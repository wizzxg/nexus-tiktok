'use strict';

const express = require('express');
const axios = require('axios');
const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const cors = require('cors');

require('events').EventEmitter.defaultMaxListeners = 250;

const CONFIG = {
  port: Number(process.env.PORT) || 3000,
  ttBlacklistPath: process.env.TT_BLACKLIST_PATH || path.join(__dirname, 'tiktok_blacklist.txt'),
  corsOrigin: process.env.CORS_ORIGIN || '*',
  eulerApiKey: process.env.EULER_API_KEY || null,
  tiktok: {
    resolveTimeoutMs: 7500,
    blockTimeoutMs: 8500,
    maxBlockRetries: 3,
    blockConcurrency: 6,
    prewarmConcurrency: 6,
    slotStaggerMs: 45,
    profileCacheTtlMs: 24 * 60 * 60 * 1000,
    shareDedupWindowMs: 1200,
  },
};

// ============================================================================
// TELEMETRY & LOGGING
// ============================================================================
const telemetryRingBuffer = [];
function pushTelemetry(level, scope, message) {
  telemetryRingBuffer.push({
    id: crypto.randomUUID(),
    at: new Date().toISOString(),
    level,
    scope,
    message,
  });
  if (telemetryRingBuffer.length > 200) telemetryRingBuffer.shift();
}

const log = {
  info: (...a) => {
    const msg = a.join(' ');
    console.log(`[${new Date().toISOString()}] [info]`, msg);
    pushTelemetry('info', 'system', msg);
  },
  warn: (...a) => {
    const msg = a.join(' ');
    console.warn(`[${new Date().toISOString()}] [warn]`, msg);
    pushTelemetry('warn', 'system', msg);
  },
  error: (...a) => {
    const msg = a.join(' ');
    console.error(`[${new Date().toISOString()}] [error]`, msg);
    pushTelemetry('error', 'system', msg);
  },
};

const delay = (ms) => new Promise((res) => setTimeout(res, ms));
const httpsAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 60000,
  maxSockets: 128,
  rejectUnauthorized: true,
});

const tiktokHttp = axios.create({
  httpsAgent,
  timeout: CONFIG.tiktok.resolveTimeoutMs,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
  },
  validateStatus: () => true,
});

function toNumericIdString(val) {
  if (val == null) return '';
  if (typeof val === 'string') {
    const t = val.trim();
    return /^\d+$/.test(t) && t !== '0' ? t : '';
  }
  if (typeof val === 'number') {
    return Number.isFinite(val) && val > 0 ? String(Math.trunc(val)) : '';
  }
  if (typeof val === 'bigint') {
    return val > 0n ? val.toString() : '';
  }
  if (typeof val === 'object') {
    if (typeof val.low === 'number' && typeof val.high === 'number') {
      try {
        const h = BigInt(val.high >>> 0);
        const l = BigInt(val.low >>> 0);
        const c = (h << 32n) | l;
        if (c > 0n) return c.toString();
      } catch (e) {}
    }
    if (typeof val.toString === 'function') {
      const s = String(val.toString()).trim();
      if (/^\d+$/.test(s) && s !== '0') return s;
    }
  }
  return '';
}

function normalizeTikTokHandle(raw) {
  return String(raw || '').trim().replace(/^@+/, '').trim();
}

// ============================================================================
// GLOBAL BLACKLIST (SHARED ACROSS ALL USERS + AUTO-PURGES 1-CHAR CORRUPTION)
// ============================================================================
class TikTokBlacklist {
  constructor(filePath) {
    this.filePath = filePath;
    this.usernames = new Set();
    this.writeChain = Promise.resolve();
  }

  normalize(u) {
    return normalizeTikTokHandle(u).toLowerCase();
  }

  isValidHandle(clean) {
    // Reject empty strings or 1-character corrupted entries
    return Boolean(clean && clean.length >= 2 && !/\s/.test(clean));
  }

  load() {
    try {
      if (!fs.existsSync(this.filePath)) {
        fs.writeFileSync(this.filePath, '');
        return;
      }
      const raw = fs.readFileSync(this.filePath, 'utf8');
      let hadCorrupted = false;
      for (const line of raw.split(/\r?\n/)) {
        const clean = this.normalize(line);
        if (!clean) continue;
        if (this.isValidHandle(clean)) {
          this.usernames.add(clean);
        } else {
          hadCorrupted = true;
        }
      }
      if (hadCorrupted) {
        log.info('Purged 1-character corrupted entries from tiktok_blacklist.txt');
        this.saveToDisk();
      }
    } catch (e) {}
  }

  has(username) {
    return this.usernames.has(this.normalize(username));
  }

  add(input) {
    // Safely accepts either an Array of usernames OR a single username string
    const list = Array.isArray(input)
      ? input
      : String(input || '').split(/\r?\n/);

    let added = 0;
    for (const item of list) {
      const clean = this.normalize(item);
      if (this.isValidHandle(clean) && !this.usernames.has(clean)) {
        this.usernames.add(clean);
        added++;
      }
    }
    if (added > 0) this.saveToDisk();
    return added;
  }

  replaceAll(input) {
    const lines = Array.isArray(input)
      ? input
      : String(input || '').split(/\r?\n/);
    this.usernames.clear();
    for (const line of lines) {
      const clean = this.normalize(line);
      if (this.isValidHandle(clean)) {
        this.usernames.add(clean);
      }
    }
    this.saveToDisk();
    return this.usernames.size;
  }

  saveToDisk() {
    const content = Array.from(this.usernames).join('\n') + (this.usernames.size ? '\n' : '');
    this.writeChain = this.writeChain
      .then(() => fsp.writeFile(this.filePath, content))
      .catch(() => {});
  }

  all() {
    return Array.from(this.usernames);
  }
}

const ttBlacklist = new TikTokBlacklist(CONFIG.ttBlacklistPath);
ttBlacklist.load();

const tiktokProfileCache = new Map();

// ============================================================================
// MULTI-TENANT SESSION MANAGER
// ============================================================================
class ClientSession {
  constructor(sessionId) {
    this.sessionId = sessionId;
    this.username = 'Operator';
    this.auth = {
      loggedIn: false,
      cookieHeader: '',
      ttCookieId: '',
      ttTargetIdc: 'useast1a',
      csrfToken: '',
      msToken: '',
      accountName: '',
      autoBlockSharers: false,
    };
    this.state = { connected: false, username: null, roomId: null, error: null };
    this.sharers = new Map();
    this.connection = null;
    this.jobs = new Map();
    this.sseClients = new Set();
    this.diagnosticHistory = [];
  }

  broadcast(payload) {
    const msg = `data: ${JSON.stringify(payload)}\n\n`;
    for (const res of this.sseClients) {
      try {
        res.write(msg);
      } catch (e) {
        this.sseClients.delete(res);
      }
    }
  }

  recordDiagnostic(entry) {
    const item = {
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      ...entry,
    };
    this.diagnosticHistory.push(item);
    if (this.diagnosticHistory.length > 250) this.diagnosticHistory.shift();
    return item;
  }
}

const sessions = new Map();
function getSession(id) {
  if (!sessions.has(id)) sessions.set(id, new ClientSession(id));
  return sessions.get(id);
}

function broadcastGlobalBlacklist() {
  const list = ttBlacklist.all();
  for (const session of sessions.values()) {
    for (const [uid, entry] of session.sharers.entries()) {
      entry.blacklisted = ttBlacklist.has(uid);
    }
    session.broadcast({ type: 'BLACKLIST', blacklist: list });
  }
}

// ============================================================================
// TIKTOK UTILS & RESOLVERS
// ============================================================================
let tiktokLib = null;
async function getTikTokLib() {
  if (!tiktokLib) tiktokLib = await import('tiktok-live-connector');
  return tiktokLib;
}

function singleFlight() {
  const inflight = new Map();
  return function run(key, fn) {
    const hit = inflight.get(key);
    if (hit) return hit;
    const p = Promise.resolve()
      .then(fn)
      .finally(() => inflight.delete(key));
    inflight.set(key, p);
    return p;
  };
}
const ttResolveFlight = singleFlight();

async function buildTikTokCookieHeader(session, rawInput) {
  const trimmed = String(rawInput || '').trim();
  if (!trimmed) {
    return { cookieHeader: '', ttCookieId: '', ttTargetIdc: 'useast1a', csrfToken: '', msToken: '' };
  }

  let ttCookieId = '';
  let ttTargetIdc = 'useast1a';
  let cookieHeader = '';
  let csrfToken = '';
  let msToken = '';

  if (trimmed.includes('=')) {
    const m =
      /sessionid=([^;\s]+)/i.exec(trimmed) ||
      /sessionid_ss=([^;\s]+)/i.exec(trimmed) ||
      /sid_tt=([^;\s]+)/i.exec(trimmed);
    ttCookieId = m ? m[1] : '';
    const idcMatch = /tt-target-idc=([^;\s]+)/i.exec(trimmed);
    if (idcMatch) ttTargetIdc = idcMatch[1];
    const cm = /(?:tt_csrf_token|csrf_session_id|passport_csrf_token)=([^;\s]+)/i.exec(trimmed);
    if (cm) csrfToken = cm[1];
    const mm = /msToken=([^;\s]+)/i.exec(trimmed);
    if (mm) msToken = mm[1];
    cookieHeader = trimmed;
  } else {
    ttCookieId = trimmed;
    csrfToken = crypto.randomBytes(16).toString('hex');
    cookieHeader = `sessionid=${trimmed}; sessionid_ss=${trimmed}; sid_tt=${trimmed}; tt_csrf_token=${csrfToken}; passport_csrf_token=${csrfToken};`;
  }

  if (!csrfToken) {
    csrfToken = crypto.randomBytes(16).toString('hex');
    cookieHeader += `; tt_csrf_token=${csrfToken}; passport_csrf_token=${csrfToken}`;
  }

  try {
    const res = await tiktokHttp.get('https://www.tiktok.com/explore', {
      headers: { Cookie: cookieHeader },
      timeout: 5000,
    });
    for (const sc of res.headers?.['set-cookie'] || []) {
      const cMatch = /(?:tt_csrf_token|passport_csrf_token)=([^;]+)/i.exec(sc);
      if (cMatch && !csrfToken) csrfToken = cMatch[1];
      const mMatch = /msToken=([^;]+)/i.exec(sc);
      if (mMatch && !msToken) msToken = mMatch[1];
      const idcMatch = /tt-target-idc=([^;]+)/i.exec(sc);
      if (idcMatch) ttTargetIdc = idcMatch[1];
    }
    if (msToken && !cookieHeader.includes('msToken=')) {
      cookieHeader += `; msToken=${msToken}`;
    }
  } catch (e) {}

  return { cookieHeader, ttCookieId, ttTargetIdc, csrfToken, msToken };
}

function isSocialSharePacket(data) {
  if (!data || typeof data !== 'object') return false;
  const rawAction = data.action?.low ?? data.action;
  if (Number(rawAction) === 3 || String(rawAction) === '3') return true;
  const displayType = String(
    data.displayType ||
      data.common?.displayText?.key ||
      data.common?.displayText?.defaultPattern ||
      data.label ||
      ''
  ).toLowerCase();
  return displayType.includes('share');
}

function extractShareUser(data) {
  const pieceUser = data?.common?.displayText?.pieces?.find((p) => p?.userValue?.user)?.userValue?.user;
  const u = data?.user || data?.userDetails || pieceUser || data || {};
  const uniqueId = normalizeTikTokHandle(u.uniqueId || u.displayId || u.username || data?.uniqueId || '');
  const nickname = String(u.nickname || u.nickName || data?.nickname || uniqueId).trim();
  const userId =
    toNumericIdString(u.userId) ||
    toNumericIdString(u.id) ||
    toNumericIdString(u.idStr) ||
    toNumericIdString(pieceUser?.id) ||
    toNumericIdString(data?.userId);
  const secUid = String(u.secUid || u.sec_uid || pieceUser?.secUid || data?.secUid || '').trim();
  const pfp =
    u.profilePictureUrl ||
    u.profilePicture?.urls?.[0] ||
    u.avatarThumb?.urlList?.[0] ||
    data?.profilePictureUrl ||
    'https://tr.rbxcdn.com/53eb9b17fe1432a809c73a13889b5006/150/150/AvatarHeadshot/Png';
  return { uniqueId, nickname, userId, secUid, profilePictureUrl: pfp };
}

async function resolveTikTokUserProfile(session, uniqueId) {
  const clean = normalizeTikTokHandle(uniqueId).toLowerCase();
  if (!clean || clean.length < 2) return { userId: '', secUid: '' };

  const cached = tiktokProfileCache.get(clean);
  if (cached && /^\d+$/.test(cached.userId) && cached.secUid) {
    return { userId: cached.userId, secUid: cached.secUid };
  }

  for (const [k, v] of session.sharers.entries()) {
    if (k.toLowerCase() === clean && /^\d+$/.test(v.userId) && v.secUid) {
      const hit = { userId: v.userId, secUid: v.secUid };
      tiktokProfileCache.set(clean, { ...hit, at: Date.now() });
      return hit;
    }
  }

  return ttResolveFlight(`tt-resolve:${clean}`, async () => {
    let foundUserId = '';
    let foundSecUid = '';

    // Layer 1: TikWM Public User Info API
    try {
      const twRes = await tiktokHttp.get(
        `https://www.tikwm.com/api/user/info?unique_id=${encodeURIComponent(clean)}`,
        { timeout: 5500, headers: { Accept: 'application/json' } }
      );
      const u = twRes.data?.data?.user;
      if (u) {
        const uid = toNumericIdString(u.id);
        const sec = String(u.secUid || '').trim();
        if (uid) foundUserId = uid;
        if (sec) foundSecUid = sec;
        if (foundUserId && foundSecUid) {
          const out = { userId: foundUserId, secUid: foundSecUid };
          tiktokProfileCache.set(clean, { ...out, at: Date.now() });
          return out;
        }
      }
    } catch (e) {}

    // Layer 2: TikTok API-Live User Room Endpoint
    try {
      const apiRes = await tiktokHttp.get(
        `https://www.tiktok.com/api-live/user/room/?aid=1988&uniqueId=${encodeURIComponent(clean)}&sourceType=54`,
        {
          timeout: 5500,
          headers: {
            Referer: `https://www.tiktok.com/@${encodeURIComponent(clean)}/live`,
            ...(session?.auth?.cookieHeader ? { Cookie: session.auth.cookieHeader } : {}),
          },
        }
      );
      const u = apiRes.data?.data?.user;
      if (u) {
        const uid = toNumericIdString(u.id || u.idStr);
        const sec = String(u.secUid || '').trim();
        if (uid && !foundUserId) foundUserId = uid;
        if (sec && !foundSecUid) foundSecUid = sec;
        if (foundUserId && foundSecUid) {
          const out = { userId: foundUserId, secUid: foundSecUid };
          tiktokProfileCache.set(clean, { ...out, at: Date.now() });
          return out;
        }
      }
    } catch (e) {}

    // Layer 3: TikTok Profile HTML Rehydration JSON Parser
    try {
      const resHtml = await tiktokHttp.get(`https://www.tiktok.com/@${encodeURIComponent(clean)}`, {
        timeout: 6000,
        headers: {
          Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          ...(session?.auth?.cookieHeader ? { Cookie: session.auth.cookieHeader } : {}),
        },
      });
      const html = typeof resHtml.data === 'string' ? resHtml.data : '';
      const scriptMatch = /<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/i.exec(html);
      if (scriptMatch && scriptMatch[1]) {
        const jsonData = JSON.parse(scriptMatch[1]);
        const userDetail = jsonData?.['__DEFAULT_SCOPE__']?.['webapp.user-detail']?.userInfo?.user;
        if (userDetail) {
          const uid = toNumericIdString(userDetail.id);
          const sec = String(userDetail.secUid || '').trim();
          if (uid && !foundUserId) foundUserId = uid;
          if (sec && !foundSecUid) foundSecUid = sec;
        }
      }
    } catch (e) {}

    const finalOut = { userId: foundUserId, secUid: foundSecUid };
    if (finalOut.userId || finalOut.secUid) {
      tiktokProfileCache.set(clean, { ...finalOut, at: Date.now() });
    }
    return finalOut;
  });
}

async function prewarmTikTokProfiles(session, usernames) {
  if (!Array.isArray(usernames) || usernames.length === 0) return;
  const cleanList = [
    ...new Set(
      usernames
        .map((u) => normalizeTikTokHandle(u).toLowerCase())
        .filter((u) => u.length >= 2)
    ),
  ].filter((uname) => !(tiktokProfileCache.get(uname)?.userId && tiktokProfileCache.get(uname)?.secUid));

  let cursor = 0;
  const worker = async () => {
    while (cursor < cleanList.length) {
      await resolveTikTokUserProfile(session, cleanList[cursor++]).catch(() => {});
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(CONFIG.tiktok.prewarmConcurrency, cleanList.length) }, () => worker())
  );
}

async function signTikTokUrlIfPossible(targetUrl) {
  try {
    const lib = await getTikTokLib();
    const TikTokLiveConnection = lib.TikTokLiveConnection || lib.WebcastPushConnection;
    const tempConn = new TikTokLiveConnection('tiktok', {
      processInitialData: false,
      enableExtendedGiftInfo: false,
      ...(CONFIG.eulerApiKey ? { signApiKey: CONFIG.eulerApiKey } : {}),
    });
    if (tempConn?.webClient?.webSigner?.signTiktokUrl) {
      const signed = await tempConn.webClient.webSigner.signTiktokUrl(targetUrl);
      return signed?.signedUrl || signed || targetUrl;
    }
  } catch (e) {}
  return targetUrl;
}

// ============================================================================
// CORE BLOCKING LOGIC (FULL PAYLOAD + AUTO-RETRY)
// ============================================================================

async function blockTikTokUserOnAccount(session, userObj) {
  const startMs = Date.now();
  const cleanUsername = normalizeTikTokHandle(userObj?.uniqueId);

  if (!cleanUsername || cleanUsername.length < 2) {
    return { blockedOnTikTok: false, note: 'Skipped invalid 1-char handle' };
  }

  if (!session.auth.loggedIn || !session.auth.cookieHeader) {
    return { blockedOnTikTok: false, note: 'Not logged into TikTok Account (Click Auth button to paste cookie)' };
  }

  let targetUserId = toNumericIdString(userObj?.userId);
  let targetSecUid = String(userObj?.secUid || '').trim();

  if (!targetUserId || !targetSecUid) {
    const cached = tiktokProfileCache.get(cleanUsername.toLowerCase());
    if (cached) {
      if (!targetUserId) targetUserId = cached.userId;
      if (!targetSecUid) targetSecUid = cached.secUid;
    }
  }

  if (!targetUserId || !targetSecUid) {
    const resolved = await resolveTikTokUserProfile(session, cleanUsername);
    if (!targetUserId && resolved.userId) targetUserId = resolved.userId;
    if (!targetSecUid && resolved.secUid) targetSecUid = resolved.secUid;
  }

  if (!targetSecUid && !targetUserId) {
    session.recordDiagnostic({
      username: cleanUsername,
      userId: '—',
      success: false,
      vector: 'RESOLVE_MISS',
      note: `User @${cleanUsername} not found`,
      elapsedMs: Date.now() - startMs,
    });
    return { blockedOnTikTok: false, note: `User @${cleanUsername} not found` };
  }

  const csrfToken = session.auth.csrfToken || crypto.randomBytes(16).toString('hex');
  const webHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    Accept: 'application/json, text/plain, */*',
    'Content-Type': 'application/x-www-form-urlencoded',
    Origin: 'https://www.tiktok.com',
    Referer: `https://www.tiktok.com/@${cleanUsername}`,
    Cookie: session.auth.cookieHeader,
    'x-secsdk-csrf-token': csrfToken,
    'tt-csrf-token': csrfToken,
    'x-tt-passport-csrf-token': csrfToken,
  };

  let lastNote = '';
  for (let attempt = 1; attempt <= CONFIG.tiktok.maxBlockRetries; attempt++) {
    try {
      const webQuery = new URLSearchParams({
        WebIdLastTime: String(Math.floor(Date.now() / 1000)),
        aid: '1988',
        app_language: 'en',
        app_name: 'tiktok_web',
        browser_language: 'en-US',
        browser_name: 'Mozilla',
        browser_online: 'true',
        browser_platform: 'Win32',
        browser_version: '5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        channel: 'tiktok_web',
        cookie_enabled: 'true',
        device_id: '7350000000000000000',
        device_platform: 'web_pc',
        focus_state: 'true',
        from_page: 'user',
        history_len: '3',
        is_fullscreen: 'false',
        is_page_visible: 'true',
        os: 'windows',
        priority_region: 'US',
        referer: '',
        region: 'US',
        screen_height: '1080',
        screen_width: '1920',
        tz_name: 'UTC',
        webcast_language: 'en',
        blockType: '1',
        block_type: '1',
        source: '0',
        ...(targetSecUid
          ? { secUid: targetSecUid, sec_user_id: targetSecUid, target_sec_uid: targetSecUid }
          : {}),
        ...(targetUserId
          ? { userId: targetUserId, user_id: targetUserId, target_uid: targetUserId }
          : {}),
        ...(session.auth.msToken ? { msToken: session.auth.msToken } : {}),
      });

      const bodyForm = new URLSearchParams({
        blockType: '1',
        block_type: '1',
        source: '0',
        ...(targetSecUid ? { secUid: targetSecUid, sec_user_id: targetSecUid } : {}),
        ...(targetUserId ? { userId: targetUserId, user_id: targetUserId } : {}),
      });

      const unsignedUrl = `https://www.tiktok.com/api/user/block/?${webQuery.toString()}`;
      // Attempt 1 uses direct fast path; Attempt 2+ uses Euler URL signer if needed
      const targetUrl = attempt === 1 ? unsignedUrl : await signTikTokUrlIfPossible(unsignedUrl);

      const res1 = await tiktokHttp.post(targetUrl, bodyForm.toString(), {
        headers: webHeaders,
        timeout: CONFIG.tiktok.blockTimeoutMs,
      });

      log.info(
        `[${session.username}] [TikTok Block] @${cleanUsername} (uid=${targetUserId}, sec=${targetSecUid ? 'yes' : 'no'}) -> HTTP ${res1.status}:`,
        JSON.stringify(res1.data || {}).slice(0, 180)
      );

      if (res1.status === 200 && (res1.data?.status_code === 0 || res1.data?.statusCode === 0)) {
        session.recordDiagnostic({
          username: cleanUsername,
          userId: targetUserId,
          success: true,
          vector: `WEB_V1_TRY_${attempt}`,
          elapsedMs: Date.now() - startMs,
        });
        return { blockedOnTikTok: true };
      }

      lastNote = res1.data?.status_msg || `code ${res1.data?.status_code ?? res1.status}`;
      if (String(lastNote).toLowerCase().includes('invalid') || String(lastNote).toLowerCase().includes('param')) {
        tiktokProfileCache.delete(cleanUsername.toLowerCase());
        const fresh = await resolveTikTokUserProfile(session, cleanUsername);
        if (fresh.userId) targetUserId = fresh.userId;
        if (fresh.secUid) targetSecUid = fresh.secUid;
      }
    } catch (err) {
      lastNote = err.message;
    }

    if (attempt === CONFIG.tiktok.maxBlockRetries) {
      try {
        const mQuery = new URLSearchParams({
          aid: '1233',
          app_name: 'musical_ly',
          version_code: '350103',
          device_platform: 'android',
          os: 'android',
          block_type: '1',
          source: '0',
          ...(targetSecUid ? { sec_user_id: targetSecUid } : {}),
          ...(targetUserId ? { user_id: targetUserId } : {}),
        });
        const resMob = await tiktokHttp.post(
          `https://api16-normal-c-useast1a.tiktokv.com/aweme/v1/user/block/?${mQuery.toString()}`,
          mQuery.toString(),
          {
            headers: {
              'User-Agent': 'com.zhiliaoapp.musically/2023501030 (Linux; U; Android 13; en_US; Pixel 8; Build/UQ1A.240205.004)',
              Accept: 'application/json',
              'Content-Type': 'application/x-www-form-urlencoded',
              Cookie: session.auth.cookieHeader,
            },
            timeout: 5500,
          }
        );
        if (resMob.status === 200 && resMob.data?.status_code === 0) {
          session.recordDiagnostic({
            username: cleanUsername,
            userId: targetUserId,
            success: true,
            vector: 'MOBILE_AWEME_V1',
            elapsedMs: Date.now() - startMs,
          });
          return { blockedOnTikTok: true };
        }
        if (resMob.data?.status_msg) lastNote = resMob.data.status_msg;
      } catch (e) {}
    }

    await delay(280 * attempt);
  }

  session.recordDiagnostic({
    username: cleanUsername,
    userId: targetUserId,
    success: false,
    vector: 'FAILED',
    note: lastNote,
    elapsedMs: Date.now() - startMs,
  });
  return { blockedOnTikTok: false, note: lastNote };
}

async function runParallelTikTokBlockBatch(session, usernames) {
  const cleanList = [
    ...new Set(
      usernames
        .map((u) => normalizeTikTokHandle(u))
        .filter((u) => u.length >= 2)
    ),
  ];

  // Pass the entire array at once (never pass a string)
  ttBlacklist.add(cleanList);
  broadcastGlobalBlacklist();

  let cursor = 0;
  let blockedOnTikTokCount = 0;
  let failedCount = 0;
  let lastNote = null;

  const worker = async (laneIdx) => {
    if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.slotStaggerMs);
    while (cursor < cleanList.length) {
      const uname = cleanList[cursor++];
      if (!uname) continue;
      const existing = session.sharers.get(uname) || { uniqueId: uname };
      existing.blacklisted = true;
      if (session.sharers.has(uname)) session.sharers.set(uname, existing);

      if (session.auth.loggedIn) {
        try {
          const result = await blockTikTokUserOnAccount(session, existing);
          if (result.blockedOnTikTok) blockedOnTikTokCount++;
          else {
            failedCount++;
            if (result.note) lastNote = result.note;
          }
        } catch (err) {
          failedCount++;
          lastNote = err.message;
        }
      } else {
        failedCount++;
        lastNote = 'TikTok Account not connected (paste cookie in TikTok Account modal)';
      }
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(CONFIG.tiktok.blockConcurrency, cleanList.length) }, (_, i) => worker(i))
  );
  return { totalBlacklisted: cleanList.length, blockedOnTikTokCount, failedCount, lastNote };
}

function recordTikTokSharer(session, data) {
  const parsed = extractShareUser(data);
  if (!parsed.uniqueId || parsed.uniqueId.length < 2) return;

  const lowerKey = parsed.uniqueId.toLowerCase();
  if (parsed.userId || parsed.secUid) {
    const prev = tiktokProfileCache.get(lowerKey) || {};
    tiktokProfileCache.set(lowerKey, {
      userId: parsed.userId || prev.userId || '',
      secUid: parsed.secUid || prev.secUid || '',
      at: Date.now(),
    });
  }

  const existing = session.sharers.get(parsed.uniqueId);
  if (existing && Date.now() - existing.timestamp < CONFIG.tiktok.shareDedupWindowMs) return;

  const shareCount = existing ? existing.shareCount + 1 : 1;
  const isBlacklisted = ttBlacklist.has(parsed.uniqueId.toLowerCase());

  const userEntry = {
    uniqueId: parsed.uniqueId,
    nickname: parsed.nickname,
    userId: parsed.userId || existing?.userId || '',
    secUid: parsed.secUid || existing?.secUid || '',
    profilePictureUrl: parsed.profilePictureUrl,
    shareCount,
    lastSharedAt: new Date().toLocaleTimeString(),
    timestamp: Date.now(),
    blacklisted: isBlacklisted,
  };
  session.sharers.set(parsed.uniqueId, userEntry);

  if (session.auth.autoBlockSharers && shareCount >= 2 && !isBlacklisted) {
    ttBlacklist.add([parsed.uniqueId.toLowerCase()]);
    broadcastGlobalBlacklist();
    userEntry.blacklisted = true;
    if (session.auth.loggedIn) blockTikTokUserOnAccount(session, userEntry).catch(() => {});
  } else if (!userEntry.secUid || !userEntry.userId) {
    resolveTikTokUserProfile(session, parsed.uniqueId)
      .then((resolved) => {
        if (resolved.userId && !userEntry.userId) userEntry.userId = resolved.userId;
        if (resolved.secUid && !userEntry.secUid) userEntry.secUid = resolved.secUid;
      })
      .catch(() => {});
  }
  session.broadcast({ type: 'SHARE', user: userEntry, totalUnique: session.sharers.size });
}

// ============================================================================
// EXPRESS APP & MIDDLEWARE
// ============================================================================

const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: CONFIG.corsOrigin }));
app.use(express.json({ limit: '4mb' }));

app.get('/', (req, res) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(htmlPath)) return res.sendFile(htmlPath);
  res.status(200).json({ ok: true, service: 'nexus-tiktok-backend-multitenant' });
});

app.use((req, res, next) => {
  if (!req.path.startsWith('/api/tiktok')) return next();
  const sessionId = req.headers['x-session-id'] || req.query.sessionId;
  if (!sessionId) return res.status(401).json({ error: 'Missing x-session-id header.' });

  req.sessionId = sessionId;
  req.session = getSession(sessionId);
  req.session.username = req.headers['x-username'] || req.query.username || 'Operator';
  next();
});

const asyncRoute = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    uptimeSec: Math.round(process.uptime()),
    activeSessions: sessions.size,
    globalBlacklisted: ttBlacklist.all().length,
  });
});

// ============================================================================
// TIKTOK MULTI-TENANT ROUTES
// ============================================================================

app.get('/api/tiktok/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();

  req.session.sseClients.add(res);

  res.write(
    `data: ${JSON.stringify({
      type: 'INIT',
      state: req.session.state,
      auth: {
        loggedIn: req.session.auth.loggedIn,
        accountName: req.session.auth.accountName,
        autoBlockSharers: req.session.auth.autoBlockSharers,
      },
      sharers: Array.from(req.session.sharers.values()),
      blacklist: ttBlacklist.all(),
    })}\n\n`
  );

  const keepAlive = setInterval(() => {
    try {
      res.write(': ping\n\n');
    } catch (e) {}
  }, 15000);
  req.on('close', () => {
    clearInterval(keepAlive);
    req.session.sseClients.delete(res);
  });
});

app.post(
  '/api/tiktok/login',
  asyncRoute(async (req, res) => {
    const { cookie, accountName } = req.body || {};
    const parsed = await buildTikTokCookieHeader(req.session, cookie);
    if (!parsed.cookieHeader) {
      return res.status(400).json({ error: 'Invalid TikTok cookie string.' });
    }

    req.session.auth.loggedIn = true;
    req.session.auth.cookieHeader = parsed.cookieHeader;
    req.session.auth.ttCookieId = parsed.ttCookieId;
    req.session.auth.ttTargetIdc = parsed.ttTargetIdc || 'useast1a';
    req.session.auth.csrfToken = parsed.csrfToken;
    req.session.auth.msToken = parsed.msToken;
    req.session.auth.accountName = normalizeTikTokHandle(accountName) || 'TikTok Account';

    prewarmTikTokProfiles(req.session, ttBlacklist.all()).catch(() => {});

    const payload = {
      loggedIn: true,
      accountName: req.session.auth.accountName,
      autoBlockSharers: req.session.auth.autoBlockSharers,
    };
    req.session.broadcast({ type: 'AUTH', auth: payload });
    res.json({ success: true, auth: payload });
  })
);

app.post('/api/tiktok/logout', (req, res) => {
  req.session.auth = {
    loggedIn: false,
    cookieHeader: '',
    ttCookieId: '',
    ttTargetIdc: 'useast1a',
    csrfToken: '',
    msToken: '',
    accountName: '',
    autoBlockSharers: false,
  };
  req.session.broadcast({ type: 'AUTH', auth: req.session.auth });
  res.json({ success: true });
});

app.post('/api/tiktok/auto-block', (req, res) => {
  req.session.auth.autoBlockSharers = Boolean(req.body?.enabled);
  req.session.broadcast({
    type: 'AUTH',
    auth: {
      loggedIn: req.session.auth.loggedIn,
      accountName: req.session.auth.accountName,
      autoBlockSharers: req.session.auth.autoBlockSharers,
    },
  });
  res.json({ success: true, autoBlockSharers: req.session.auth.autoBlockSharers });
});

app.get('/api/tiktok/blacklist', (req, res) => {
  const list = ttBlacklist.all();
  prewarmTikTokProfiles(req.session, list).catch(() => {});
  res.json({ usernames: list, text: list.join('\n') });
});

app.post('/api/tiktok/blacklist/save', (req, res) => {
  const { text, usernames } = req.body || {};
  const arr = Array.isArray(usernames) ? usernames : String(text || '').split(/\r?\n/);

  ttBlacklist.replaceAll(arr);
  broadcastGlobalBlacklist();

  const allList = ttBlacklist.all();
  prewarmTikTokProfiles(req.session, allList).catch(() => {});
  res.json({ success: true, total: allList.length, usernames: allList });
});

app.post(
  '/api/tiktok/block',
  asyncRoute(async (req, res) => {
    const { usernames } = req.body || {};
    const targets = Array.isArray(usernames) ? usernames : [];
    if (targets.length === 0) return res.status(400).json({ error: 'No usernames provided to block.' });
    const batchRes = await runParallelTikTokBlockBatch(req.session, targets);
    res.json({
      success: true,
      totalBlacklisted: batchRes.totalBlacklisted,
      blockedOnTikTokCount: batchRes.blockedOnTikTokCount,
      failedCount: batchRes.failedCount,
      loggedIn: req.session.auth.loggedIn,
      note: batchRes.lastNote,
      blacklist: ttBlacklist.all(),
    });
  })
);

app.post(
  '/api/tiktok/jobs/block',
  asyncRoute(async (req, res) => {
    const { usernames } = req.body || {};
    const list = Array.isArray(usernames)
      ? [
          ...new Set(
            usernames
              .map((u) => normalizeTikTokHandle(u).toLowerCase())
              .filter((u) => u.length >= 2)
          ),
        ]
      : [];
    if (list.length === 0) {
      return res.status(400).json({ error: 'No valid usernames provided.' });
    }

    // Add entire array at once (fixes the single-letter bug)
    ttBlacklist.add(list);
    broadcastGlobalBlacklist();

    const jobId = crypto.randomUUID();
    const jobObj = {
      id: jobId,
      status: 'running',
      total: list.length,
      processed: 0,
      blockedOnTikTokCount: 0,
      failedCount: 0,
      currentUsername: null,
      lastNote: null,
      startedAt: Date.now(),
      finishedAt: null,
      cancelRequested: false,
    };
    req.session.jobs.set(jobId, jobObj);

    (async () => {
      let cursor = 0;
      const worker = async (laneIdx) => {
        if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.slotStaggerMs);
        while (cursor < list.length) {
          if (jobObj.cancelRequested) {
            jobObj.status = 'cancelled';
            return;
          }
          const uname = list[cursor++];
          if (!uname) continue;
          jobObj.currentUsername = uname;
          const existing = req.session.sharers.get(uname) || { uniqueId: uname };
          existing.blacklisted = true;
          if (req.session.sharers.has(uname)) req.session.sharers.set(uname, existing);

          if (req.session.auth.loggedIn && req.session.auth.cookieHeader) {
            try {
              const result = await blockTikTokUserOnAccount(req.session, existing);
              if (result.blockedOnTikTok) jobObj.blockedOnTikTokCount++;
              else {
                jobObj.failedCount++;
                if (result.note) jobObj.lastNote = result.note;
              }
            } catch (err) {
              jobObj.failedCount++;
              jobObj.lastNote = err.message;
            }
          } else {
            jobObj.failedCount++;
            jobObj.lastNote = 'Cookie missing — re-save your TikTok Cookie in the Auth modal';
          }
          jobObj.processed++;
        }
      };
      await Promise.all(
        Array.from({ length: Math.min(CONFIG.tiktok.blockConcurrency, list.length) }, (_, i) => worker(i))
      );
      if (jobObj.status === 'running') jobObj.status = 'done';
      jobObj.currentUsername = null;
      jobObj.finishedAt = Date.now();
    })().catch((err) => {
      jobObj.status = 'aborted';
      jobObj.lastNote = err.message;
      jobObj.finishedAt = Date.now();
    });

    res.status(202).json(jobObj);
  })
);

app.get('/api/tiktok/jobs/:id', (req, res) => {
  const job = req.session.jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'No such TikTok job.' });
  res.json(job);
});

app.delete('/api/tiktok/jobs/:id', (req, res) => {
  const job = req.session.jobs.get(req.params.id);
  if (!job || job.status !== 'running') return res.status(404).json({ error: 'No active TikTok job.' });
  job.cancelRequested = true;
  res.json({ success: true });
});

app.get('/api/tiktok/diagnostics', (req, res) => {
  res.json({
    auth: {
      loggedIn: req.session.auth.loggedIn,
      accountName: req.session.auth.accountName,
      hasCookieId: Boolean(req.session.auth.ttCookieId),
    },
    cachedProfilesCount: tiktokProfileCache.size,
    recentAttempts: req.session.diagnosticHistory.slice().reverse(),
  });
});

app.post(
  '/api/tiktok/prewarm',
  asyncRoute(async (req, res) => {
    const { usernames } = req.body || {};
    if (!Array.isArray(usernames)) return res.status(400).json({ error: 'usernames array required' });
    prewarmTikTokProfiles(req.session, usernames).catch(() => {});
    res.json({ success: true, queued: usernames.length });
  })
);

app.post(
  '/api/tiktok/connect',
  asyncRoute(async (req, res) => {
    const rawUsername = normalizeTikTokHandle(req.body?.username);
    if (!rawUsername) return res.status(400).json({ error: 'TikTok username is required.' });

    if (req.session.connection) {
      try {
        await req.session.connection.disconnect();
      } catch (e) {}
      req.session.connection = null;
    }

    try {
      const lib = await getTikTokLib();
      const TikTokLiveConnection = lib.TikTokLiveConnection || lib.WebcastPushConnection;
      const WebcastEvent = lib.WebcastEvent || {};
      const ControlEvent = lib.ControlEvent || {};

      const options = {
        processInitialData: true,
        enableExtendedGiftInfo: false,
        enableWebsocketUpgrade: true,
        requestPollingIntervalMs: 700,
      };
      if (CONFIG.eulerApiKey) options.signApiKey = CONFIG.eulerApiKey;
      if (req.session.auth.loggedIn && req.session.auth.ttCookieId) {
        options.sessionId = req.session.auth.ttCookieId;
        options.ttTargetIdc = req.session.auth.ttTargetIdc || 'useast1a';
      }

      const conn = new TikTokLiveConnection(rawUsername, options);
      req.session.connection = conn;

      const shareEvents = new Set([WebcastEvent.SHARE, 'share', 'WebcastSocialMessage'].filter(Boolean));
      const socialEvents = new Set([WebcastEvent.SOCIAL, 'social'].filter(Boolean));

      for (const ev of shareEvents) conn.on(ev, (data) => recordTikTokSharer(req.session, data));
      for (const ev of socialEvents) {
        conn.on(ev, (data) => {
          if (isSocialSharePacket(data)) recordTikTokSharer(req.session, data);
        });
      }

      conn.on(ControlEvent.RAW_DATA || 'rawData', (type, BinaryOrObj) => {
        if (
          typeof type === 'string' &&
          type.toLowerCase().includes('social') &&
          BinaryOrObj &&
          typeof BinaryOrObj === 'object'
        ) {
          if (isSocialSharePacket(BinaryOrObj)) recordTikTokSharer(req.session, BinaryOrObj);
        }
      });

      conn.on(WebcastEvent.STREAM_END || ControlEvent.STREAM_END || 'streamEnd', () => {
        req.session.state = { connected: false, username: rawUsername, roomId: null, error: 'Stream ended' };
        req.session.broadcast({ type: 'STATUS', state: req.session.state });
      });

      conn.on(ControlEvent.DISCONNECTED || 'disconnected', () => {
        req.session.state = { connected: false, username: rawUsername, roomId: null, error: 'Disconnected' };
        req.session.broadcast({ type: 'STATUS', state: req.session.state });
      });

      conn.on('error', (err) =>
        log.warn(`[${req.session.username} | ${req.session.sessionId}] Connection error:`, err?.message || err)
      );

      const state = await conn.connect();
      req.session.state = {
        connected: true,
        username: rawUsername,
        roomId: String(state?.roomId || 'live'),
        error: null,
      };
      log.info(`[${req.session.username} | ${req.session.sessionId}] Connected to LIVE @${rawUsername}`);
      req.session.broadcast({ type: 'STATUS', state: req.session.state });
      res.json({ success: true, state: req.session.state });
    } catch (err) {
      req.session.state = { connected: false, username: rawUsername, roomId: null, error: err.message };
      req.session.broadcast({ type: 'STATUS', state: req.session.state });
      res.status(400).json({ error: err.message });
    }
  })
);

app.post(
  '/api/tiktok/disconnect',
  asyncRoute(async (req, res) => {
    if (req.session.connection) {
      try {
        await req.session.connection.disconnect();
      } catch (e) {}
      req.session.connection = null;
    }
    req.session.state = { connected: false, username: null, roomId: null, error: null };
    req.session.broadcast({ type: 'STATUS', state: req.session.state });
    res.json({ success: true, state: req.session.state });
  })
);

app.post('/api/tiktok/clear', (req, res) => {
  req.session.sharers.clear();
  req.session.broadcast({ type: 'CLEAR' });
  res.json({ success: true });
});

app.get('/api/tiktok/sharers', (req, res) => {
  res.json({
    state: req.session.state,
    auth: {
      loggedIn: req.session.auth.loggedIn,
      accountName: req.session.auth.accountName,
      autoBlockSharers: req.session.auth.autoBlockSharers,
    },
    sharers: Array.from(req.session.sharers.values()),
    blacklist: ttBlacklist.all(),
  });
});

app.use((err, req, res, _next) => {
  log.error(`500: ${err.message}`);
  res.status(500).json({ error: err.message, kind: 'UNKNOWN' });
});

const server = app.listen(CONFIG.port, () => {
  log.info(`Nexus TikTok Engine active on port ${CONFIG.port}`);
});

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => server.close(() => process.exit(0)));
}
