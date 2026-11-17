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
  ttProfilesPath: process.env.TT_PROFILES_PATH || path.join(__dirname, 'tiktok_profiles_cache.json'),
  corsOrigin: process.env.CORS_ORIGIN || '*',
  eulerApiKey: process.env.EULER_API_KEY || null,
  tiktok: {
    resolveTimeoutMs: 8500,
    blockTimeoutMs: 9000,
    maxBlockRetries: 3,
    blockConcurrency: 2,
    blockStaggerMs: 380,
    tikwmMinIntervalMs: 1150,
    profileCacheTtlMs: 7 * 24 * 60 * 60 * 1000,
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
  if (telemetryRingBuffer.length > 250) telemetryRingBuffer.shift();
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
  if (val === null || val === undefined) return '';
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
        const c = (h * 4294967296n) + l;
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
  return String(raw ? raw : '')
    .trim()
    .replace(/^@+/, '')
    .trim();
}

/**
 * Safely parses any string or array into clean TikTok usernames (min 2 chars).
 * Never iterates character-by-character over a string.
 */
function parseUsernameList(input) {
  const rawItems = Array.isArray(input) ? input : [String(input ? input : '')];
  const out = [];
  const seen = new Set();

  for (const item of rawItems) {
    const parts = String(item ? item : '').split(/[\r\n,;\s]+/);
    for (const part of parts) {
      const clean = normalizeTikTokHandle(part).toLowerCase();
      if (clean.length >= 2 && !seen.has(clean)) {
        seen.add(clean);
        out.push(clean);
      }
    }
  }
  return out;
}

// ============================================================================
// PERSISTENT PROFILE CACHE (SURVIVES RESTARTS)
// ============================================================================
const tiktokProfileCache = new Map();

function loadProfileCacheFromDisk() {
  try {
    if (!fs.existsSync(CONFIG.ttProfilesPath)) return;
    const raw = JSON.parse(fs.readFileSync(CONFIG.ttProfilesPath, 'utf8'));
    if (raw && typeof raw === 'object') {
      for (const [k, v] of Object.entries(raw)) {
        if (v && (v.userId || v.secUid)) {
          tiktokProfileCache.set(k.toLowerCase(), {
            userId: String(v.userId ? v.userId : ''),
            secUid: String(v.secUid ? v.secUid : ''),
            at: Number(v.at) || Date.now(),
          });
        }
      }
    }
  } catch (e) {}
}

let profileSaveTimer = null;
function saveProfileCacheToDisk() {
  clearTimeout(profileSaveTimer);
  profileSaveTimer = setTimeout(() => {
    try {
      const obj = {};
      for (const [k, v] of tiktokProfileCache.entries()) {
        if (v.userId || v.secUid) obj[k] = v;
      }
      fsp.writeFile(CONFIG.ttProfilesPath, JSON.stringify(obj)).catch(() => {});
    } catch (e) {}
  }, 1000);
}

function cacheTikTokProfile(uniqueId, userId, secUid) {
  const key = normalizeTikTokHandle(uniqueId).toLowerCase();
  if (!key || key.length < 2) return;
  const prev = tiktokProfileCache.get(key) || {};
  const nextUid = toNumericIdString(userId) || prev.userId || '';
  const nextSec = String(secUid ? secUid : prev.secUid ? prev.secUid : '').trim();
  if (!nextUid && !nextSec) return;

  tiktokProfileCache.set(key, {
    userId: nextUid,
    secUid: nextSec,
    at: Date.now(),
  });
  saveProfileCacheToDisk();
}

loadProfileCacheFromDisk();

// ============================================================================
// GLOBAL BLACKLIST (SHARED ACROSS ALL USERS)
// ============================================================================
class TikTokBlacklist {
  constructor(filePath) {
    this.filePath = filePath;
    this.usernames = new Set();
    this.writeChain = Promise.resolve();
  }

  load() {
    try {
      if (!fs.existsSync(this.filePath)) {
        fs.writeFileSync(this.filePath, '');
        return;
      }
      const raw = fs.readFileSync(this.filePath, 'utf8');
      const valid = parseUsernameList(raw);
      this.usernames = new Set(valid);
      this.saveToDisk();
    } catch (e) {}
  }

  has(username) {
    return this.usernames.has(normalizeTikTokHandle(username).toLowerCase());
  }

  add(input) {
    const list = parseUsernameList(input);
    let added = 0;
    for (const clean of list) {
      if (!this.usernames.has(clean)) {
        this.usernames.add(clean);
        added++;
      }
    }
    if (added > 0) this.saveToDisk();
    return added;
  }

  replaceAll(input) {
    const list = parseUsernameList(input);
    this.usernames = new Set(list);
    this.saveToDisk();
    return this.usernames.size;
  }

  clear() {
    this.usernames.clear();
    this.saveToDisk();
  }

  saveToDisk() {
    const arr = Array.from(this.usernames);
    const content = arr.length ? arr.join('\n') + '\n' : '';
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
// TIKTOK RESOLVERS & RATE-GATED TIKWM LOOKUP
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

// Paces tikwm.com calls to 1 request per 1.15s so tikwm never rate-limits
let nextTikwmSlotAt = 0;
async function acquireTikwmSlot() {
  const now = Date.now();
  const scheduled = Math.max(now, nextTikwmSlotAt);
  nextTikwmSlotAt = scheduled + CONFIG.tiktok.tikwmMinIntervalMs;
  const waitMs = scheduled - now;
  if (waitMs > 0) await delay(waitMs);
}

async function buildTikTokCookieHeader(rawInput) {
  const trimmed = String(rawInput ? rawInput : '').trim();
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
    cookieHeader = `sessionid=${trimmed}; sessionid_ss=${trimmed}; sid_tt=${trimmed}`;
  }

  try {
    const res = await tiktokHttp.get('https://www.tiktok.com/explore', {
      headers: { Cookie: cookieHeader },
      timeout: 6000,
    });
    for (const sc of res.headers?.['set-cookie'] || []) {
      const cMatch = /(?:tt_csrf_token|passport_csrf_token)=([^;]+)/i.exec(sc);
      if (cMatch && !csrfToken) csrfToken = cMatch[1];
      const mMatch = /msToken=([^;]+)/i.exec(sc);
      if (mMatch && !msToken) msToken = mMatch[1];
      const idcMatch = /tt-target-idc=([^;]+)/i.exec(sc);
      if (idcMatch) ttTargetIdc = idcMatch[1];
    }
  } catch (e) {}

  if (!csrfToken) {
    csrfToken = crypto.randomBytes(16).toString('hex');
  }
  if (!cookieHeader.includes('tt_csrf_token=')) {
    cookieHeader += `; tt_csrf_token=${csrfToken}; passport_csrf_token=${csrfToken}`;
  }
  if (msToken && !cookieHeader.includes('msToken=')) {
    cookieHeader += `; msToken=${msToken}`;
  }

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

  if (session) {
    for (const [k, v] of session.sharers.entries()) {
      if (k.toLowerCase() === clean && /^\d+$/.test(v.userId) && v.secUid) {
        cacheTikTokProfile(clean, v.userId, v.secUid);
        return { userId: v.userId, secUid: v.secUid };
      }
    }
  }

  return ttResolveFlight(`tt-resolve:${clean}`, async () => {
    let foundUserId = cached?.userId || '';
    let foundSecUid = cached?.secUid || '';

    // Layer 1: TikTok API-Live User Room (Fast, no rate-limit)
    try {
      const apiRes = await tiktokHttp.get(
        `https://www.tiktok.com/api-live/user/room/?aid=1988&uniqueId=${encodeURIComponent(clean)}&sourceType=54`,
        {
          timeout: 5000,
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
        if (uid) foundUserId = uid;
        if (sec) foundSecUid = sec;
        if (foundUserId && foundSecUid) {
          cacheTikTokProfile(clean, foundUserId, foundSecUid);
          return { userId: foundUserId, secUid: foundSecUid };
        }
      }
    } catch (e) {}

    // Layer 2: TikTok @username Profile Page HTML Parser (Fast, uses session cookie)
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
        try {
          const jsonData = JSON.parse(scriptMatch[1]);
          const userDetail = jsonData?.['__DEFAULT_SCOPE__']?.['webapp.user-detail']?.userInfo?.user;
          if (userDetail) {
            const uid = toNumericIdString(userDetail.id);
            const sec = String(userDetail.secUid || '').trim();
            if (uid && !foundUserId) foundUserId = uid;
            if (sec && !foundSecUid) foundSecUid = sec;
          }
        } catch (e) {}
      }

      if (!foundUserId || !foundSecUid) {
        const escapedClean = clean.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        const r1 = new RegExp(`"id"\\s*:\\s*"(\\d+)"[^}]{0,350}"uniqueId"\\s*:\\s*"${escapedClean}"[^}]{0,650}"secUid"\\s*:\\s*"([^"]+)"`, 'i');
        const r2 = new RegExp(`"uniqueId"\\s*:\\s*"${escapedClean}"[^}]{0,450}"id"\\s*:\\s*"(\\d+)"[^}]{0,450}"secUid"\\s*:\\s*"([^"]+)"`, 'i');
        const m = r1.exec(html) || r2.exec(html);
        if (m) {
          if (!foundUserId) foundUserId = m[1];
          if (!foundSecUid) foundSecUid = m[2];
        }
      }

      if (foundUserId && foundSecUid) {
        cacheTikTokProfile(clean, foundUserId, foundSecUid);
        return { userId: foundUserId, secUid: foundSecUid };
      }
    } catch (e) {}

    // Layer 3: Rate-Gated TikWM Public API (1 req / 1.15s with auto-retry on rate-limit)
    for (let twTry = 1; twTry <= 3; twTry++) {
      try {
        await acquireTikwmSlot();
        const twRes = await tiktokHttp.get(
          `https://www.tikwm.com/api/user/info?unique_id=${encodeURIComponent(clean)}`,
          { timeout: 6500, headers: { Accept: 'application/json' } }
        );
        if (twRes.data?.code === -1) {
          // TikWM rate-limit hit; wait and retry
          await delay(1200 * twTry);
          continue;
        }
        const u = twRes.data?.data?.user;
        if (u) {
          const uid = toNumericIdString(u.id);
          const sec = String(u.secUid || '').trim();
          if (uid) foundUserId = uid;
          if (sec) foundSecUid = sec;
        }
        break;
      } catch (e) {}
    }

    if (foundUserId || foundSecUid) {
      cacheTikTokProfile(clean, foundUserId, foundSecUid);
    }
    return { userId: foundUserId, secUid: foundSecUid };
  });
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
// CORE BLOCKING ENGINE (PROVEN V1 WEB + MOBILE AWEME FALLBACK)
// ============================================================================

async function blockTikTokUserOnAccount(session, userObj) {
  const startMs = Date.now();
  const cleanUsername = normalizeTikTokHandle(userObj?.uniqueId);

  if (!cleanUsername || cleanUsername.length < 2) {
    return { blockedOnTikTok: false, note: 'Invalid handle' };
  }

  if (!session.auth.loggedIn || !session.auth.cookieHeader) {
    return { blockedOnTikTok: false, note: 'Not logged into TikTok Account' };
  }

  let targetUserId = toNumericIdString(userObj?.userId);
  let targetSecUid = String(userObj?.secUid || '').trim();

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
      note: `Could not resolve @${cleanUsername}`,
      elapsedMs: Date.now() - startMs,
    });
    return { blockedOnTikTok: false, note: `Could not resolve @${cleanUsername}` };
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
      const signedUrl = await signTikTokUrlIfPossible(unsignedUrl);

      const res1 = await tiktokHttp.post(signedUrl, bodyForm.toString(), {
        headers: webHeaders,
        timeout: CONFIG.tiktok.blockTimeoutMs,
      });

      log.info(
        `[TikTok Block V1 Web] @${cleanUsername} (uid=${targetUserId}, sec=${targetSecUid ? 'yes' : 'no'}) -> HTTP ${res1.status}:`,
        JSON.stringify(res1.data || {}).slice(0, 180)
      );

      if (
        res1.status === 200 &&
        (res1.data?.status_code === 0 ||
          res1.data?.statusCode === 0 ||
          res1.data?.block_status === 1 ||
          res1.data?.data?.block_status === 1)
      ) {
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

    await delay(350 * attempt);
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
  const cleanList = parseUsernameList(usernames);
  ttBlacklist.add(cleanList);
  broadcastGlobalBlacklist();

  let cursor = 0;
  let blockedOnTikTokCount = 0;
  let failedCount = 0;
  let lastNote = null;

  const worker = async (laneIdx) => {
    if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.blockStaggerMs);
    while (cursor < cleanList.length) {
      const uname = cleanList[cursor++];
      if (!uname) continue;
      const existing = session.sharers.get(uname) || { uniqueId: uname };
      existing.blacklisted = true;
      if (session.sharers.has(uname)) session.sharers.set(uname, existing);

      if (session.auth.loggedIn && session.auth.cookieHeader) {
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
        await delay(CONFIG.tiktok.blockStaggerMs);
      } else {
        failedCount++;
        lastNote = 'TikTok cookie not connected';
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

  if (parsed.userId || parsed.secUid) {
    cacheTikTokProfile(parsed.uniqueId, parsed.userId, parsed.secUid);
  }

  const existing = session.sharers.get(parsed.uniqueId);
  if (existing && Date.now() - existing.timestamp < CONFIG.tiktok.shareDedupWindowMs) return;

  const shareCount = existing ? existing.shareCount + 1 : 1;
  const isBlacklisted = ttBlacklist.has(parsed.uniqueId);

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
    ttBlacklist.add([parsed.uniqueId]);
    broadcastGlobalBlacklist();
    userEntry.blacklisted = true;
    if (session.auth.loggedIn) {
      blockTikTokUserOnAccount(session, userEntry).catch(() => {});
    }
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
// EXPRESS APP & ROUTES
// ============================================================================

const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: CONFIG.corsOrigin }));
app.use(express.json({ limit: '4mb' }));

app.get('/', (req, res) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(htmlPath)) return res.sendFile(htmlPath);
  res.status(200).json({ ok: true, service: 'nexus-tiktok-engine' });
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
    cachedProfiles: tiktokProfileCache.size,
  });
});

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
    const parsed = await buildTikTokCookieHeader(cookie);
    if (!parsed.cookieHeader) {
      return res.status(400).json({ error: 'Please paste your TikTok Cookie or sessionid.' });
    }

    req.session.auth.loggedIn = true;
    req.session.auth.cookieHeader = parsed.cookieHeader;
    req.session.auth.ttCookieId = parsed.ttCookieId;
    req.session.auth.ttTargetIdc = parsed.ttTargetIdc || 'useast1a';
    req.session.auth.csrfToken = parsed.csrfToken;
    req.session.auth.msToken = parsed.msToken;
    req.session.auth.accountName = normalizeTikTokHandle(accountName) || 'TikTok Account';

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
  res.json({ usernames: list, text: list.join('\n') });
});

app.post('/api/tiktok/blacklist/save', (req, res) => {
  const { text, usernames } = req.body || {};
  const total = ttBlacklist.replaceAll(usernames ? usernames : text);
  broadcastGlobalBlacklist();
  const allList = ttBlacklist.all();
  res.json({ success: true, total, usernames: allList });
});

app.post('/api/tiktok/blacklist/clear', (req, res) => {
  ttBlacklist.clear();
  broadcastGlobalBlacklist();
  res.json({ success: true, total: 0, usernames: [] });
});

app.post(
  '/api/tiktok/block',
  asyncRoute(async (req, res) => {
    const list = parseUsernameList(req.body?.usernames);
    if (list.length === 0) return res.status(400).json({ error: 'No valid usernames provided.' });
    const batchRes = await runParallelTikTokBlockBatch(req.session, list);
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
    const list = parseUsernameList(req.body?.usernames);
    if (list.length === 0) {
      return res.status(400).json({ error: 'No valid usernames provided to block.' });
    }
    if (!req.session.auth.loggedIn || !req.session.auth.cookieHeader) {
      return res.status(400).json({
        error: 'TikTok cookie missing — click TikTok Account and save your cookie first.',
      });
    }

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
        if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.blockStaggerMs);
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
          jobObj.processed++;
          await delay(CONFIG.tiktok.blockStaggerMs);
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
