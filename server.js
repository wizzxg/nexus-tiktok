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
  ttProfilesPath: process.env.TT_PROFILES_PATH || path.join(__dirname, 'tiktok_profiles_v4.json'),
  corsOrigin: process.env.CORS_ORIGIN || '*',
  eulerApiKey: process.env.EULER_API_KEY || null,
  tiktok: {
    resolveTimeoutMs: 8500,
    blockTimeoutMs: 9000,
    blockConcurrency: 2,
    blockStaggerMs: 380,
    tikwmMinIntervalMs: 1100,
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

// Safely quotes 15+ digit integers before JSON.parse so 64-bit TikTok IDs never round or truncate
function parseJsonSafe64(rawText) {
  if (typeof rawText !== 'string') return rawText;
  const patched = rawText.replace(
    /("(?:id|user_id|userId|uid|target_uid|user_id_str|idStr|roomId|room_id)"\s*:\s*)(-?\d{15,})/g,
    '$1"$2"'
  );
  return JSON.parse(patched);
}

const tiktokHttp = axios.create({
  httpsAgent,
  timeout: CONFIG.tiktok.resolveTimeoutMs,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
  },
  transformResponse: [
    (data) => {
      if (typeof data === 'string') {
        const trimmed = data.trim();
        if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
          try {
            return parseJsonSafe64(data);
          } catch (e) {
            return data;
          }
        }
      }
      return data;
    },
  ],
  validateStatus: () => true,
});

function toNumericIdString(val) {
  if (val === null || val === undefined) return '';
  if (typeof val === 'string') {
    const t = val.trim();
    return /^\d+$/.test(t) && t !== '0' ? t : '';
  }
  if (typeof val === 'bigint') {
    return val > 0n ? val.toString() : '';
  }
  if (typeof val === 'number') {
    if (!Number.isFinite(val) || val <= 0) return '';
    try {
      return BigInt(Math.trunc(val)).toString();
    } catch (e) {
      return '';
    }
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

function isValidSecUid(sec) {
  return typeof sec === 'string' && sec.trim().startsWith('MS4wLjAB') && sec.trim().length > 20;
}

function normalizeTikTokHandle(raw) {
  return String(raw ? raw : '')
    .trim()
    .replace(/^@+/, '')
    .trim();
}

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
// PERSISTENT VERIFIED PROFILE CACHE
// ============================================================================
const tiktokProfileCache = new Map();

function loadProfileCacheFromDisk() {
  try {
    if (!fs.existsSync(CONFIG.ttProfilesPath)) return;
    const raw = parseJsonSafe64(fs.readFileSync(CONFIG.ttProfilesPath, 'utf8'));
    if (raw && typeof raw === 'object') {
      for (const [k, v] of Object.entries(raw)) {
        const uid = toNumericIdString(v?.userId);
        const sec = String(v?.secUid ? v.secUid : '').trim();
        if (uid && isValidSecUid(sec)) {
          tiktokProfileCache.set(k.toLowerCase(), {
            userId: uid,
            secUid: sec,
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
        if (v.userId && isValidSecUid(v.secUid)) obj[k] = v;
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
  const nextSec = isValidSecUid(secUid) ? String(secUid).trim() : (prev.secUid || '');
  if (!nextUid && !nextSec) return;

  tiktokProfileCache.set(key, {
    userId: nextUid,
    secUid: nextSec,
    at: Date.now(),
  });
  if (nextUid && isValidSecUid(nextSec)) {
    saveProfileCacheToDisk();
  }
}

loadProfileCacheFromDisk();

// ============================================================================
// GLOBAL BLACKLIST
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
      callerUserId: '',
      callerSecUid: '',
      passportVerified: false,
      passportNote: 'Not checked',
      accountName: '',
      autoBlockSharers: false,
    };
    this.state = { connected: false, username: null, roomId: null, error: null };
    this.sharers = new Map();
    this.connection = null;
    this.jobs = new Map();
    this.sseClients = new Set();
    this.diagnosticHistory = [];
    this.lastFailedUsername = '';
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
    if (!entry.success && entry.username) {
      this.lastFailedUsername = entry.username;
    }
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
// TIKTOK COOKIE BUILDER + PASSPORT ACCOUNT VERIFIER
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
    return {
      cookieHeader: '',
      ttCookieId: '',
      ttTargetIdc: 'useast1a',
      csrfToken: '',
      msToken: '',
      callerUserId: '',
      callerSecUid: '',
      callerUsername: '',
      passportVerified: false,
      passportNote: 'Empty cookie',
    };
  }

  let ttCookieId = '';
  let ttTargetIdc = '';
  let cookieHeader = '';
  let csrfToken = '';
  let msToken = '';
  let callerUserId = '';
  let callerSecUid = '';
  let callerUsername = '';
  let passportVerified = false;
  let passportNote = '';

  if (trimmed.includes('=')) {
    const m =
      /sessionid=([^;\s]+)/i.exec(trimmed) ||
      /sessionid_ss=([^;\s]+)/i.exec(trimmed) ||
      /sid_tt=([^;\s]+)/i.exec(trimmed);
    ttCookieId = m ? m[1] : '';
    const idcMatch = /(?:tt-target-idc|store-idc)=([^;\s]+)/i.exec(trimmed);
    if (idcMatch) ttTargetIdc = idcMatch[1];
    const cm = /(?:tt_csrf_token|csrf_session_id|passport_csrf_token)=([^;\s]+)/i.exec(trimmed);
    if (cm) csrfToken = cm[1];
    const mm = /msToken=([^;\s]+)/i.exec(trimmed);
    if (mm) msToken = mm[1];
    const uidMatch = /uid_tt=([^;\s]+)/i.exec(trimmed);
    if (uidMatch) callerUserId = toNumericIdString(uidMatch[1]);
    cookieHeader = trimmed;
  } else {
    ttCookieId = trimmed;
    cookieHeader = `sessionid=${trimmed}; sessionid_ss=${trimmed}; sid_tt=${trimmed}`;
  }

  // Step 1: Query TikTok Passport Account Info to verify sessionid & get caller's uid_tt + IDC
  try {
    const passRes = await tiktokHttp.get(
      'https://www.tiktok.com/passport/web/account/info/?aid=1459&app_language=en&app_name=tiktok_web&device_platform=web_pc',
      {
        headers: {
          Cookie: cookieHeader,
          Referer: 'https://www.tiktok.com/',
        },
        timeout: 6500,
      }
    );

    for (const sc of passRes.headers?.['set-cookie'] || []) {
      const cMatch = /(?:tt_csrf_token|passport_csrf_token)=([^;]+)/i.exec(sc);
      if (cMatch && !csrfToken) csrfToken = cMatch[1];
      const mMatch = /msToken=([^;]+)/i.exec(sc);
      if (mMatch && !msToken) msToken = mMatch[1];
      const idcMatch = /(?:tt-target-idc|store-idc)=([^;]+)/i.exec(sc);
      if (idcMatch && !ttTargetIdc) ttTargetIdc = idcMatch[1];
    }

    const headerIdc = passRes.headers?.['x-tt-store-idc'] || passRes.headers?.['x-tt-target-idc'];
    if (headerIdc && !ttTargetIdc) ttTargetIdc = String(headerIdc).trim();

    const passData = passRes.data?.data;
    if (passRes.data?.message === 'success' && passData) {
      callerUserId = toNumericIdString(passData.user_id_str || passData.user_id) || callerUserId;
      callerSecUid = String(passData.sec_user_id || '').trim();
      callerUsername = String(passData.username || passData.screen_name || '').trim();
      passportVerified = Boolean(callerUserId);
      passportNote = `Verified @${callerUsername || 'user'} (uid=${callerUserId}, idc=${ttTargetIdc || 'auto'})`;
    } else {
      passportNote = `Passport response: ${JSON.stringify(passRes.data || {}).slice(0, 140)}`;
    }
  } catch (e) {
    passportNote = `Passport check error: ${e.message}`;
  }

  // Step 2: Fetch /explore for csrfToken / msToken / tt-target-idc if still missing
  try {
    const res = await tiktokHttp.get('https://www.tiktok.com/explore', {
      headers: { Cookie: cookieHeader },
      timeout: 5500,
    });
    for (const sc of res.headers?.['set-cookie'] || []) {
      const cMatch = /(?:tt_csrf_token|passport_csrf_token)=([^;]+)/i.exec(sc);
      if (cMatch && !csrfToken) csrfToken = cMatch[1];
      const mMatch = /msToken=([^;]+)/i.exec(sc);
      if (mMatch && !msToken) msToken = mMatch[1];
      const idcMatch = /(?:tt-target-idc|store-idc)=([^;]+)/i.exec(sc);
      if (idcMatch && !ttTargetIdc) ttTargetIdc = idcMatch[1];
    }
  } catch (e) {}

  if (!ttTargetIdc) ttTargetIdc = 'useast1a';
  if (!csrfToken) csrfToken = crypto.randomBytes(16).toString('hex');

  if (callerUserId && !cookieHeader.includes('uid_tt=')) {
    cookieHeader += `; uid_tt=${callerUserId}; uid_tt_ss=${callerUserId}`;
  }
  if (!cookieHeader.includes('tt-target-idc=')) {
    cookieHeader += `; tt-target-idc=${ttTargetIdc}; store-idc=${ttTargetIdc}`;
  }
  if (!cookieHeader.includes('tt_csrf_token=')) {
    cookieHeader += `; tt_csrf_token=${csrfToken}; passport_csrf_token=${csrfToken}`;
  }
  if (msToken && !cookieHeader.includes('msToken=')) {
    cookieHeader += `; msToken=${msToken}`;
  }

  return {
    cookieHeader,
    ttCookieId,
    ttTargetIdc,
    csrfToken,
    msToken,
    callerUserId,
    callerSecUid,
    callerUsername,
    passportVerified,
    passportNote,
  };
}

// ============================================================================
// PROFILE RESOLUTION (WITH LAYER-BY-LAYER DIAGNOSTIC TRACE)
// ============================================================================
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
    toNumericIdString(u.idStr) ||
    toNumericIdString(u.userId) ||
    toNumericIdString(u.id) ||
    toNumericIdString(pieceUser?.idStr) ||
    toNumericIdString(pieceUser?.id) ||
    toNumericIdString(data?.userId);
  const rawSec = String(u.secUid || u.sec_uid || pieceUser?.secUid || data?.secUid || '').trim();
  const secUid = isValidSecUid(rawSec) ? rawSec : '';
  const pfp =
    u.profilePictureUrl ||
    u.profilePicture?.urls?.[0] ||
    u.avatarThumb?.urlList?.[0] ||
    data?.profilePictureUrl ||
    'https://tr.rbxcdn.com/53eb9b17fe1432a809c73a13889b5006/150/150/AvatarHeadshot/Png';
  return { uniqueId, nickname, userId, secUid, profilePictureUrl: pfp };
}

/**
 * Resolves a TikTok @username and returns both the resolved IDs and a detailed
 * trace of what each layer returned for the Error Scanner.
 */
async function resolveTikTokUserProfileDetailed(session, uniqueId, { bypassCache = false } = {}) {
  const clean = normalizeTikTokHandle(uniqueId).toLowerCase();
  const trace = {
    input: uniqueId,
    cleanHandle: clean,
    fromCache: false,
    layers: {},
    finalUserId: '',
    finalSecUid: '',
  };

  if (!clean || clean.length < 2) {
    trace.error = 'Username too short (< 2 chars)';
    return { userId: '', secUid: '', trace };
  }

  if (/^\d{15,20}$/.test(clean)) {
    trace.finalUserId = clean;
    trace.layers.numericInput = { ok: true, userId: clean };
    return { userId: clean, secUid: '', trace };
  }

  if (!bypassCache) {
    const cached = tiktokProfileCache.get(clean);
    if (cached && /^\d+$/.test(cached.userId) && isValidSecUid(cached.secUid)) {
      trace.fromCache = true;
      trace.finalUserId = cached.userId;
      trace.finalSecUid = cached.secUid;
      return { userId: cached.userId, secUid: cached.secUid, trace };
    }

    if (session) {
      for (const [k, v] of session.sharers.entries()) {
        if (k.toLowerCase() === clean && /^\d+$/.test(v.userId) && isValidSecUid(v.secUid)) {
          cacheTikTokProfile(clean, v.userId, v.secUid);
          trace.fromCache = true;
          trace.finalUserId = v.userId;
          trace.finalSecUid = v.secUid;
          return { userId: v.userId, secUid: v.secUid, trace };
        }
      }
    }
  }

  let foundUserId = '';
  let foundSecUid = '';

  // LAYER 1: TikWM Public User Info API
  for (let twTry = 1; twTry <= 2; twTry++) {
    try {
      await acquireTikwmSlot();
      const twStart = Date.now();
      const twRes = await tiktokHttp.get(
        `https://www.tikwm.com/api/user/info?unique_id=${encodeURIComponent(clean)}`,
        { timeout: 6500, headers: { Accept: 'application/json' } }
      );
      const u = twRes.data?.data?.user;
      const uid = toNumericIdString(u?.id);
      const sec = String(u?.secUid || '').trim();
      trace.layers[`L1_TikWM_try${twTry}`] = {
        httpStatus: twRes.status,
        apiCode: twRes.data?.code,
        apiMsg: twRes.data?.msg,
        resolvedUniqueId: u?.uniqueId || null,
        userId: uid || null,
        secUidPrefix: sec ? `${sec.slice(0, 18)}...` : null,
        elapsedMs: Date.now() - twStart,
      };
      if (twRes.data?.code === -1 && twTry === 1) {
        await delay(1150);
        continue;
      }
      if (uid) foundUserId = uid;
      if (isValidSecUid(sec)) foundSecUid = sec;
      break;
    } catch (e) {
      trace.layers[`L1_TikWM_try${twTry}`] = { error: e.message };
    }
  }

  // LAYER 2: TikTok @username HTML Rehydration JSON (using 64-bit safe parser!)
  if (!foundUserId || !foundSecUid || bypassCache) {
    try {
      const l2Start = Date.now();
      const resHtml = await tiktokHttp.get(`https://www.tiktok.com/@${encodeURIComponent(clean)}`, {
        timeout: 6500,
        headers: {
          Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        },
      });
      const html = typeof resHtml.data === 'string' ? resHtml.data : '';
      const scriptMatch = /<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/i.exec(html);
      if (scriptMatch && scriptMatch[1]) {
        const jsonData = parseJsonSafe64(scriptMatch[1]);
        const userInfo = jsonData?.['__DEFAULT_SCOPE__']?.['webapp.user-detail']?.userInfo;
        const statusCode = jsonData?.['__DEFAULT_SCOPE__']?.['webapp.user-detail']?.statusCode;
        const userDetail = userInfo?.user;
        const uid = toNumericIdString(userDetail?.idStr || userDetail?.id);
        const sec = String(userDetail?.secUid || '').trim();
        trace.layers.L2_WebRehydration = {
          httpStatus: resHtml.status,
          webappStatusCode: statusCode,
          resolvedUniqueId: userDetail?.uniqueId || null,
          userId: uid || null,
          secUidPrefix: sec ? `${sec.slice(0, 18)}...` : null,
          elapsedMs: Date.now() - l2Start,
        };
        if (uid && !foundUserId) foundUserId = uid;
        if (isValidSecUid(sec) && !foundSecUid) foundSecUid = sec;
      } else {
        trace.layers.L2_WebRehydration = {
          httpStatus: resHtml.status,
          note: 'No __UNIVERSAL_DATA_FOR_REHYDRATION__ script tag in HTML (WAF/Captcha or non-existent user)',
          htmlLength: html.length,
        };
      }
    } catch (e) {
      trace.layers.L2_WebRehydration = { error: e.message };
    }
  }

  // LAYER 3: TikTok API-Live User Room Endpoint
  if (!foundUserId || !foundSecUid || bypassCache) {
    try {
      const l3Start = Date.now();
      const apiRes = await tiktokHttp.get(
        `https://www.tiktok.com/api-live/user/room/?aid=1988&uniqueId=${encodeURIComponent(clean)}&sourceType=54`,
        {
          timeout: 5500,
          headers: {
            Referer: `https://www.tiktok.com/@${encodeURIComponent(clean)}/live`,
          },
        }
      );
      const u = apiRes.data?.data?.user;
      const uid = toNumericIdString(u?.idStr || u?.id);
      const sec = String(u?.secUid || '').trim();
      trace.layers.L3_ApiLiveRoom = {
        httpStatus: apiRes.status,
        statusCode: apiRes.data?.statusCode ?? apiRes.data?.status_code,
        resolvedUniqueId: u?.uniqueId || null,
        userId: uid || null,
        secUidPrefix: sec ? `${sec.slice(0, 18)}...` : null,
        elapsedMs: Date.now() - l3Start,
      };
      if (uid && !foundUserId) foundUserId = uid;
      if (isValidSecUid(sec) && !foundSecUid) foundSecUid = sec;
    } catch (e) {
      trace.layers.L3_ApiLiveRoom = { error: e.message };
    }
  }

  trace.finalUserId = foundUserId;
  trace.finalSecUid = foundSecUid;

  if (foundUserId || foundSecUid) {
    cacheTikTokProfile(clean, foundUserId, foundSecUid);
  }

  return { userId: foundUserId, secUid: foundSecUid, trace };
}

async function resolveTikTokUserProfile(session, uniqueId) {
  const clean = normalizeTikTokHandle(uniqueId).toLowerCase();
  if (!clean || clean.length < 2) return { userId: '', secUid: '' };
  return ttResolveFlight(`tt-resolve:${clean}`, async () => {
    const res = await resolveTikTokUserProfileDetailed(session, clean, { bypassCache: false });
    return { userId: res.userId, secUid: res.secUid, trace: res.trace };
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
// MULTI-VECTOR BLOCK ENGINE + DEEP SCANNER
// ============================================================================

function isTikTokBlockSuccess(httpStatus, data) {
  if (httpStatus !== 200 || !data || typeof data !== 'object') return false;
  if (data.status_code === 0 || data.statusCode === 0) return true;
  if (data.block_status === 1 || data.data?.block_status === 1) return true;
  return false;
}

/**
 * Executes up to 5 distinct TikTok Block API vectors.
 * If stopOnFirstSuccess is true (normal blocking), returns immediately when any vector succeeds.
 * If stopOnFirstSuccess is false (Error Scanner), tests and records results or stops after first success with full trace.
 */
async function executeBlockVectors(session, { cleanUsername, targetUserId, targetSecUid, stopOnFirstSuccess = true }) {
  const csrfToken = session.auth.csrfToken || crypto.randomBytes(16).toString('hex');
  const idc = session.auth.ttTargetIdc || 'useast1a';

  const baseWebHeaders = {
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

  const baseWebParams = {
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
    referer: `https://www.tiktok.com/@${cleanUsername}`,
    region: 'US',
    screen_height: '1080',
    screen_width: '1920',
    tz_name: 'UTC',
    webcast_language: 'en',
    ...(session.auth.msToken ? { msToken: session.auth.msToken } : {}),
  };

  const vectorResults = [];

  // Vector definitions
  const vectors = [
    {
      name: 'V1_WEB_CAMEL_CLEAN',
      description: 'TikTok Web V1 (Exact camelCase secUid + userId + blockType=1)',
      run: async () => {
        const q = new URLSearchParams({
          ...baseWebParams,
          blockType: '1',
          ...(targetSecUid ? { secUid: targetSecUid } : {}),
          ...(targetUserId ? { userId: targetUserId } : {}),
          source: '0',
        });
        const body = new URLSearchParams({
          blockType: '1',
          ...(targetSecUid ? { secUid: targetSecUid } : {}),
          ...(targetUserId ? { userId: targetUserId } : {}),
          source: '0',
        });
        const url = `https://www.tiktok.com/api/user/block/?${q.toString()}`;
        return await tiktokHttp.post(url, body.toString(), {
          headers: baseWebHeaders,
          timeout: CONFIG.tiktok.blockTimeoutMs,
        });
      },
    },
    {
      name: 'V2_WEB_SIGNED_HYBRID',
      description: 'TikTok Web V1 Signed URL (Both camelCase & snake_case params)',
      run: async () => {
        const q = new URLSearchParams({
          ...baseWebParams,
          blockType: '1',
          block_type: '1',
          source: '0',
          ...(targetSecUid ? { secUid: targetSecUid, sec_user_id: targetSecUid, target_sec_uid: targetSecUid } : {}),
          ...(targetUserId ? { userId: targetUserId, user_id: targetUserId, target_uid: targetUserId } : {}),
        });
        const body = new URLSearchParams({
          blockType: '1',
          block_type: '1',
          source: '0',
          ...(targetSecUid ? { secUid: targetSecUid, sec_user_id: targetSecUid } : {}),
          ...(targetUserId ? { userId: targetUserId, user_id: targetUserId } : {}),
        });
        const unsignedUrl = `https://www.tiktok.com/api/user/block/?${q.toString()}`;
        const signedUrl = await signTikTokUrlIfPossible(unsignedUrl);
        return await tiktokHttp.post(signedUrl, body.toString(), {
          headers: baseWebHeaders,
          timeout: CONFIG.tiktok.blockTimeoutMs,
        });
      },
    },
    {
      name: 'V3_WEB_SECUID_ONLY',
      description: 'TikTok Web V1 using ONLY secUid (omits numeric userId)',
      skip: !targetSecUid,
      run: async () => {
        const q = new URLSearchParams({
          ...baseWebParams,
          blockType: '1',
          secUid: targetSecUid,
          sec_user_id: targetSecUid,
          source: '0',
        });
        const body = new URLSearchParams({
          blockType: '1',
          secUid: targetSecUid,
          sec_user_id: targetSecUid,
          source: '0',
        });
        const url = `https://www.tiktok.com/api/user/block/?${q.toString()}`;
        return await tiktokHttp.post(url, body.toString(), {
          headers: baseWebHeaders,
          timeout: CONFIG.tiktok.blockTimeoutMs,
        });
      },
    },
    {
      name: 'V4_WEB_USERID_ONLY',
      description: 'TikTok Web V1 using ONLY numeric userId (omits secUid)',
      skip: !targetUserId,
      run: async () => {
        const q = new URLSearchParams({
          ...baseWebParams,
          blockType: '1',
          userId: targetUserId,
          user_id: targetUserId,
          source: '0',
        });
        const body = new URLSearchParams({
          blockType: '1',
          userId: targetUserId,
          user_id: targetUserId,
          source: '0',
        });
        const url = `https://www.tiktok.com/api/user/block/?${q.toString()}`;
        return await tiktokHttp.post(url, body.toString(), {
          headers: baseWebHeaders,
          timeout: CONFIG.tiktok.blockTimeoutMs,
        });
      },
    },
    {
      name: 'V5_MOBILE_AWEME_V1',
      description: `TikTok Mobile Aweme V1 (api16-normal-c-${idc} / alisg / useast1a)`,
      run: async () => {
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
        const hosts = [
          `https://api16-normal-c-${idc}.tiktokv.com`,
          'https://api22-normal-c-alisg.tiktokv.com',
          'https://api16-normal-c-useast1a.tiktokv.com',
        ];
        let lastRes = null;
        for (const host of [...new Set(hosts)]) {
          try {
            const resMob = await tiktokHttp.post(
              `${host}/aweme/v1/user/block/?${mQuery.toString()}`,
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
            lastRes = resMob;
            if (isTikTokBlockSuccess(resMob.status, resMob.data)) return resMob;
          } catch (e) {}
        }
        return lastRes || { status: 0, data: { status_msg: 'Mobile hosts unreachable' } };
      },
    },
  ];

  for (const vec of vectors) {
    if (vec.skip) {
      vectorResults.push({
        vector: vec.name,
        skipped: true,
        reason: 'Missing required ID for this vector',
      });
      continue;
    }

    const vStart = Date.now();
    try {
      const res = await vec.run();
      const ok = isTikTokBlockSuccess(res.status, res.data);
      const statusCode = res.data?.status_code ?? res.data?.statusCode ?? null;
      const statusMsg = res.data?.status_msg || res.data?.message || (ok ? 'OK' : `HTTP ${res.status}`);

      const entry = {
        vector: vec.name,
        description: vec.description,
        httpStatus: res.status,
        success: ok,
        statusCode,
        statusMsg,
        rawResponse: res.data,
        elapsedMs: Date.now() - vStart,
      };
      vectorResults.push(entry);

      log.info(
        `[${session.username}] [${vec.name}] @${cleanUsername} (uid=${targetUserId || 'none'}, sec=${targetSecUid ? 'yes' : 'no'}) -> HTTP ${res.status} code=${statusCode} msg="${statusMsg}"`
      );

      if (ok && stopOnFirstSuccess) {
        return { blockedOnTikTok: true, winningVector: vec.name, vectorResults };
      }
    } catch (err) {
      vectorResults.push({
        vector: vec.name,
        description: vec.description,
        success: false,
        error: err.message,
        elapsedMs: Date.now() - vStart,
      });
    }
    await delay(220);
  }

  const anySuccess = vectorResults.find((v) => v.success);
  if (anySuccess) {
    return { blockedOnTikTok: true, winningVector: anySuccess.vector, vectorResults };
  }

  const summaryErrors = vectorResults
    .filter((v) => !v.skipped)
    .map((v) => `${v.vector.replace('_WEB_', ':')}=${v.statusCode ?? 'ERR'}(${v.statusMsg || v.error || '?'})`)
    .join(' | ');

  return {
    blockedOnTikTok: false,
    note: summaryErrors || 'All vectors failed',
    vectorResults,
  };
}

async function blockTikTokUserOnAccount(session, userObj) {
  const startMs = Date.now();
  const cleanUsername = normalizeTikTokHandle(userObj?.uniqueId);

  if (!cleanUsername || cleanUsername.length < 2) {
    return { blockedOnTikTok: false, note: 'Invalid handle (< 2 chars)' };
  }

  if (!session.auth.loggedIn || !session.auth.cookieHeader) {
    return { blockedOnTikTok: false, note: 'Not logged into TikTok Account' };
  }

  let targetUserId = toNumericIdString(userObj?.userId);
  let targetSecUid = isValidSecUid(userObj?.secUid) ? String(userObj.secUid).trim() : '';

  if (!targetSecUid || !targetUserId) {
    const resolved = await resolveTikTokUserProfile(session, cleanUsername);
    if (resolved.userId) targetUserId = resolved.userId;
    if (isValidSecUid(resolved.secUid)) targetSecUid = resolved.secUid;
  }

  if (!targetSecUid && !targetUserId) {
    const missNote = `Resolver could not find @${cleanUsername} (check username spelling or open Error Scanner)`;
    session.recordDiagnostic({
      username: cleanUsername,
      userId: 'NONE',
      secUid: 'NONE',
      success: false,
      vector: 'RESOLVE_MISS',
      note: missNote,
      elapsedMs: Date.now() - startMs,
    });
    return { blockedOnTikTok: false, note: missNote };
  }

  const execRes = await executeBlockVectors(session, {
    cleanUsername,
    targetUserId,
    targetSecUid,
    stopOnFirstSuccess: true,
  });

  if (execRes.blockedOnTikTok) {
    session.recordDiagnostic({
      username: cleanUsername,
      userId: targetUserId || 'secUid',
      secUid: targetSecUid ? `${targetSecUid.slice(0, 14)}...` : 'none',
      success: true,
      vector: execRes.winningVector,
      note: 'BLOCKED',
      elapsedMs: Date.now() - startMs,
    });
    return { blockedOnTikTok: true };
  }

  const detailedNote = `[uid:${targetUserId || 'NONE'} sec:${targetSecUid ? 'YES' : 'NONE'} callerUid:${session.auth.callerUserId || 'NONE'}] ${execRes.note}`;
  session.recordDiagnostic({
    username: cleanUsername,
    userId: targetUserId || 'NONE',
    secUid: targetSecUid ? `${targetSecUid.slice(0, 14)}...` : 'NONE',
    success: false,
    vector: 'ALL_5_FAILED',
    note: detailedNote,
    vectorResults: execRes.vectorResults,
    elapsedMs: Date.now() - startMs,
  });

  return { blockedOnTikTok: false, note: detailedNote };
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

  if (parsed.userId && isValidSecUid(parsed.secUid)) {
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
  } else if (!isValidSecUid(userEntry.secUid) || !userEntry.userId) {
    resolveTikTokUserProfile(session, parsed.uniqueId)
      .then((resolved) => {
        if (resolved.userId && !userEntry.userId) userEntry.userId = resolved.userId;
        if (isValidSecUid(resolved.secUid) && !userEntry.secUid) userEntry.secUid = resolved.secUid;
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
        passportVerified: req.session.auth.passportVerified,
        passportNote: req.session.auth.passportNote,
        callerUserId: req.session.auth.callerUserId,
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
    req.session.auth.callerUserId = parsed.callerUserId;
    req.session.auth.callerSecUid = parsed.callerSecUid;
    req.session.auth.passportVerified = parsed.passportVerified;
    req.session.auth.passportNote = parsed.passportNote;
    req.session.auth.accountName =
      normalizeTikTokHandle(accountName) || parsed.callerUsername || 'TikTok Account';

    const payload = {
      loggedIn: true,
      accountName: req.session.auth.accountName,
      autoBlockSharers: req.session.auth.autoBlockSharers,
      passportVerified: req.session.auth.passportVerified,
      passportNote: req.session.auth.passportNote,
      callerUserId: req.session.auth.callerUserId,
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
    callerUserId: '',
    callerSecUid: '',
    passportVerified: false,
    passportNote: 'Logged out',
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
      passportVerified: req.session.auth.passportVerified,
      passportNote: req.session.auth.passportNote,
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

/**
 * DEEP ERROR SCANNER ENDPOINT
 * Tests cookie auth against TikTok Passport, runs all 3 profile resolution layers,
 * and tests all 5 block vectors against the target username, returning the full raw report.
 */
app.post(
  '/api/tiktok/scan',
  asyncRoute(async (req, res) => {
    const rawTarget = req.body?.username || req.session.lastFailedUsername || '';
    const cleanTarget = normalizeTikTokHandle(rawTarget).toLowerCase();

    // 1. Check Cookie & Passport Live
    let passportLive = null;
    if (req.session.auth.cookieHeader) {
      try {
        const pRes = await tiktokHttp.get(
          'https://www.tiktok.com/passport/web/account/info/?aid=1459&app_language=en&app_name=tiktok_web&device_platform=web_pc',
          {
            headers: { Cookie: req.session.auth.cookieHeader, Referer: 'https://www.tiktok.com/' },
            timeout: 6000,
          }
        );
        passportLive = {
          httpStatus: pRes.status,
          message: pRes.data?.message,
          user_id_str: toNumericIdString(pRes.data?.data?.user_id_str || pRes.data?.data?.user_id),
          username: pRes.data?.data?.username || pRes.data?.data?.screen_name || null,
          error_code: pRes.data?.data?.error_code ?? null,
          description: pRes.data?.data?.description ?? null,
          storeIdcHeader: pRes.headers?.['x-tt-store-idc'] || null,
        };
      } catch (e) {
        passportLive = { error: e.message };
      }
    }

    const cookieSummary = {
      loggedInFlag: req.session.auth.loggedIn,
      cookieLength: req.session.auth.cookieHeader.length,
      hasSessionId: /sessionid=/i.test(req.session.auth.cookieHeader),
      hasSidTt: /sid_tt=/i.test(req.session.auth.cookieHeader),
      hasUidTt: /uid_tt=/i.test(req.session.auth.cookieHeader),
      hasCsrf: /tt_csrf_token=/i.test(req.session.auth.cookieHeader),
      hasMsToken: /msToken=/i.test(req.session.auth.cookieHeader),
      ttTargetIdc: req.session.auth.ttTargetIdc,
      callerUserId: req.session.auth.callerUserId || null,
      passportLive,
    };

    if (!cleanTarget) {
      return res.json({
        scannedAt: new Date().toISOString(),
        cookieSummary,
        note: 'No target @username provided to scan. Type a username in the scanner input and click Run Deep Scan.',
      });
    }

    // 2. Run all 3 Profile Resolver Layers (bypassing cache to test live)
    const resolved = await resolveTikTokUserProfileDetailed(req.session, cleanTarget, {
      bypassCache: true,
    });

    // 3. Test Block Vectors if logged in and resolved
    let blockTest = null;
    if (!req.session.auth.loggedIn || !req.session.auth.cookieHeader) {
      blockTest = { skipped: true, reason: 'Not logged into TikTok Account (Cookie missing)' };
    } else if (!resolved.userId && !resolved.secUid) {
      blockTest = {
        skipped: true,
        reason: `All 3 Resolver Layers returned empty userId & secUid for @${cleanTarget}`,
      };
    } else {
      blockTest = await executeBlockVectors(req.session, {
        cleanUsername: cleanTarget,
        targetUserId: resolved.userId,
        targetSecUid: resolved.secUid,
        stopOnFirstSuccess: false,
      });
    }

    res.json({
      scannedAt: new Date().toISOString(),
      targetUsername: cleanTarget,
      cookieSummary,
      resolverReport: resolved.trace,
      blockTest,
      recentDiagnostics: req.session.diagnosticHistory.slice(-10).reverse(),
    });
  })
);

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
      passportVerified: req.session.auth.passportVerified,
      passportNote: req.session.auth.passportNote,
      callerUserId: req.session.auth.callerUserId,
      ttTargetIdc: req.session.auth.ttTargetIdc,
    },
    lastFailedUsername: req.session.lastFailedUsername || '',
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
      passportVerified: req.session.auth.passportVerified,
      passportNote: req.session.auth.passportNote,
      callerUserId: req.session.auth.callerUserId,
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
