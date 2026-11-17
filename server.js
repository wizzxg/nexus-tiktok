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
    resolveTimeoutMs: 7000,
    blockTimeoutMs: 7000,
    maxBlockRetries: 3,
    blockConcurrency: 6,
    prewarmConcurrency: 6,
    slotStaggerMs: 35,
    profileCacheTtlMs: 24 * 60 * 60 * 1000,
    shareDedupWindowMs: 1200,
  },
};

const telemetryRingBuffer = [];
function pushTelemetry(level, scope, message) {
  telemetryRingBuffer.push({ id: crypto.randomUUID(), at: new Date().toISOString(), level, scope, message });
  if (telemetryRingBuffer.length > 200) telemetryRingBuffer.shift();
}

const log = {
  info: (...a) => { const msg = a.join(' '); console.log(`[${new Date().toISOString()}] [info]`, msg); pushTelemetry('info', 'system', msg); },
  warn: (...a) => { const msg = a.join(' '); console.warn(`[${new Date().toISOString()}] [warn]`, msg); pushTelemetry('warn', 'system', msg); },
  error: (...a) => { const msg = a.join(' '); console.error(`[${new Date().toISOString()}] [error]`, msg); pushTelemetry('error', 'system', msg); }
};

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

const httpsAgent = new https.Agent({ keepAlive: true, keepAliveMsecs: 60000, maxSockets: 128, rejectUnauthorized: true });
const tiktokHttp = axios.create({ httpsAgent, timeout: CONFIG.tiktok.resolveTimeoutMs, headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36', Accept: 'application/json, text/plain, */*' }, validateStatus: () => true });

function toNumericIdString(val) {
  if (val == null) return '';
  if (typeof val === 'string') { const t = val.trim(); return /^\d+$/.test(t) && t !== '0' ? t : ''; }
  if (typeof val === 'number') return Number.isFinite(val) && val > 0 ? String(Math.trunc(val)) : '';
  if (typeof val === 'object') {
    if (typeof val.low === 'number' && typeof val.high === 'number') {
      try { const h = BigInt(val.high >>> 0); const l = BigInt(val.low >>> 0); const c = (h << 32n) | l; if (c > 0n) return c.toString(); } catch (e) {}
    }
    if (typeof val.toString === 'function') { const s = String(val.toString()).trim(); if (/^\d+$/.test(s) && s !== '0') return s; }
  }
  return '';
}

function normalizeTikTokHandle(raw) { return String(raw || '').trim().replace(/^@+/, '').trim(); }

class TikTokBlacklist {
  constructor(filePath) { this.filePath = filePath; this.usernames = new Set(); this.writeChain = Promise.resolve(); }
  load() {
    try {
      if (!fs.existsSync(this.filePath)) return fs.writeFileSync(this.filePath, '');
      for (const line of fs.readFileSync(this.filePath, 'utf8').split(/\r?\n/)) {
        const clean = normalizeTikTokHandle(line).toLowerCase();
        if (clean) this.usernames.add(clean);
      }
    } catch (e) {}
  }
  has(username) { return this.usernames.has(normalizeTikTokHandle(username).toLowerCase()); }
  add(list) {
    let added = 0;
    for (const item of list) {
      const clean = normalizeTikTokHandle(item).toLowerCase();
      if (clean && !this.usernames.has(clean)) { this.usernames.add(clean); added++; }
    }
    if (added > 0) this.saveToDisk();
    return added;
  }
  replaceAll(arr) {
    const lines = Array.isArray(arr) ? arr : String(arr || '').split(/\r?\n/);
    this.usernames.clear();
    for (const line of lines) { const clean = normalizeTikTokHandle(line).toLowerCase(); if (clean) this.usernames.add(clean); }
    this.saveToDisk(); return this.usernames.size;
  }
  saveToDisk() {
    const content = Array.from(this.usernames).join('\n') + (this.usernames.size ? '\n' : '');
    this.writeChain = this.writeChain.then(() => fsp.writeFile(this.filePath, content)).catch(() => {});
  }
  all() { return Array.from(this.usernames); }
}

const ttBlacklist = new TikTokBlacklist(CONFIG.ttBlacklistPath);
ttBlacklist.load();

let tiktokLib = null;
async function getTikTokLib() { if (!tiktokLib) tiktokLib = await import('tiktok-live-connector'); return tiktokLib; }

let tiktokConnection = null;
let currentTikTokState = { connected: false, username: null, roomId: null, error: null };
let tiktokAuthSession = { loggedIn: false, cookieHeader: '', sessionId: '', ttTargetIdc: 'useast1a', csrfToken: '', msToken: '', ttwid: '', odinTt: '', accountName: '', autoBlockSharers: false };

const tiktokSharers = new Map();
const tiktokProfileCache = new Map();
const tiktokBlockJobs = new Map();
const tiktokDiagnosticHistory = [];
let sseClients = [];

function broadcastTikTokEvent(payload) {
  const msg = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) { try { client.res.write(msg); } catch (e) {} }
}

function recordTikTokDiagnostic(entry) {
  const item = { id: crypto.randomUUID(), timestamp: new Date().toISOString(), ...entry };
  tiktokDiagnosticHistory.push(item);
  if (tiktokDiagnosticHistory.length > 250) tiktokDiagnosticHistory.shift();
  return item;
}

function singleFlight() {
  const inflight = new Map();
  return function run(key, fn) {
    const hit = inflight.get(key); if (hit) return hit;
    const p = Promise.resolve().then(fn).finally(() => inflight.delete(key));
    inflight.set(key, p); return p;
  };
}
const ttResolveFlight = singleFlight();

async function buildTikTokCookieHeader(rawInput) {
  const trimmed = String(rawInput || '').trim();
  if (!trimmed) return { cookieHeader: '', sessionId: '', ttTargetIdc: 'useast1a', csrfToken: '', msToken: '' };

  let sessionIdVal = '', ttTargetIdc = 'useast1a', cookieHeader = '', csrfToken = '', msToken = '';
  if (trimmed.includes('=')) {
    const m = /sessionid=([^;\s]+)/i.exec(trimmed) || /sessionid_ss=([^;\s]+)/i.exec(trimmed);
    sessionIdVal = m ? m[1] : '';
    const cm = /(?:tt_csrf_token|csrf_session_id|passport_csrf_token)=([^;\s]+)/i.exec(trimmed);
    if (cm) csrfToken = cm[1];
    const mm = /msToken=([^;\s]+)/i.exec(trimmed);
    if (mm) msToken = mm[1];
    cookieHeader = trimmed;
  } else {
    sessionIdVal = trimmed;
    csrfToken = crypto.randomBytes(16).toString('hex');
    cookieHeader = `sessionid=${trimmed}; sessionid_ss=${trimmed}; tt_csrf_token=${csrfToken}; passport_csrf_token=${csrfToken};`;
  }
  if (!csrfToken) { csrfToken = crypto.randomBytes(16).toString('hex'); cookieHeader += `; tt_csrf_token=${csrfToken};`; }

  tiktokHttp.get('https://www.tiktok.com/explore', { headers: { Cookie: cookieHeader }, timeout: 5000 }).then(res => {
    for (const sc of (res.headers?.['set-cookie'] || [])) {
      const mMatch = /msToken=([^;]+)/i.exec(sc);
      if (mMatch && !tiktokAuthSession.msToken) { tiktokAuthSession.msToken = mMatch[1]; tiktokAuthSession.cookieHeader += `; msToken=${mMatch[1]}`; }
      const idcMatch = /tt-target-idc=([^;]+)/i.exec(sc);
      if (idcMatch) tiktokAuthSession.ttTargetIdc = idcMatch[1];
    }
  }).catch(() => {});

  return { cookieHeader, sessionId: sessionIdVal, ttTargetIdc, csrfToken, msToken };
}

function isSocialSharePacket(data) {
  if (!data || typeof data !== 'object') return false;
  const rawAction = data.action?.low ?? data.action;
  if (Number(rawAction) === 3 || String(rawAction) === '3') return true;
  const displayType = String(data.displayType || data.common?.displayText?.key || data.common?.displayText?.defaultPattern || data.label || '').toLowerCase();
  return displayType.includes('share');
}

function extractShareUser(data) {
  const pieceUser = data?.common?.displayText?.pieces?.find(p => p?.userValue?.user)?.userValue?.user;
  const u = data?.user || data?.userDetails || pieceUser || data || {};
  const uniqueId = normalizeTikTokHandle(u.uniqueId || u.displayId || u.username || data?.uniqueId || '');
  const nickname = String(u.nickname || u.nickName || data?.nickname || uniqueId).trim();
  const userId = toNumericIdString(u.userId) || toNumericIdString(u.id) || toNumericIdString(pieceUser?.id) || toNumericIdString(data?.userId);
  const secUid = String(u.secUid || pieceUser?.secUid || data?.secUid || '').trim();
  const pfp = u.profilePictureUrl || u.profilePicture?.urls?.[0] || u.avatarThumb?.urlList?.[0] || data?.profilePictureUrl || 'https://tr.rbxcdn.com/53eb9b17fe1432a809c73a13889b5006/150/150/AvatarHeadshot/Png';
  return { uniqueId, nickname, userId, secUid, profilePictureUrl: pfp };
}

async function resolveTikTokUserProfile(uniqueId) {
  const clean = normalizeTikTokHandle(uniqueId).toLowerCase();
  if (!clean) return { userId: '', secUid: '' };

  const cached = tiktokProfileCache.get(clean);
  if (cached && /^\d+$/.test(cached.userId) && cached.secUid) return { userId: cached.userId, secUid: cached.secUid };

  for (const [k, v] of tiktokSharers.entries()) {
    if (k.toLowerCase() === clean && /^\d+$/.test(v.userId) && v.secUid) {
      const hit = { userId: v.userId, secUid: v.secUid };
      tiktokProfileCache.set(clean, { ...hit, at: Date.now() });
      return hit;
    }
  }

  return ttResolveFlight(`tt-resolve:${clean}`, async () => {
    let foundUserId = '', foundSecUid = '';
    const tikwmTask = tiktokHttp.get(`https://www.tikwm.com/api/user/info?unique_id=${encodeURIComponent(clean)}`, { timeout: 4500, headers: { Accept: 'application/json' } })
      .then(res => { const u = res.data?.data?.user; if (u?.id || u?.secUid) return { userId: toNumericIdString(u.id), secUid: String(u.secUid || '').trim() }; throw new Error('empty'); });
    
    const apiLiveTask = tiktokHttp.get(`https://www.tiktok.com/api-live/user/room/?aid=1988&uniqueId=${encodeURIComponent(clean)}&sourceType=54`, { timeout: 4500, headers: { Referer: `https://www.tiktok.com/@${encodeURIComponent(clean)}/live` } })
      .then(res => { const u = res.data?.data?.user; if (u?.id || u?.secUid) return { userId: toNumericIdString(u.id), secUid: String(u.secUid || '').trim() }; throw new Error('empty'); });

    try {
      const winner = await Promise.any([tikwmTask, apiLiveTask]);
      if (winner.userId) foundUserId = winner.userId;
      if (winner.secUid) foundSecUid = winner.secUid;
      if (foundUserId || foundSecUid) {
        const out = { userId: foundUserId, secUid: foundSecUid };
        tiktokProfileCache.set(clean, { ...out, at: Date.now() });
        return out;
      }
    } catch (e) {}

    try {
      const resHtml = await tiktokHttp.get(`https://www.tiktok.com/@${encodeURIComponent(clean)}`, { timeout: 5500, headers: { Accept: 'text/html' } });
      const html = typeof resHtml.data === 'string' ? resHtml.data : '';
      const m1 = new RegExp(`"id"\\s*:\\s*"(\\d+)"[^}]{0,300}"uniqueId"\\s*:\\s*"${clean}"[^}]{0,600}"secUid"\\s*:\\s*"([^"]+)"`, 'i').exec(html);
      if (m1) { foundUserId = m1[1]; foundSecUid = m1[2]; }
    } catch (e) {}

    const finalOut = { userId: foundUserId, secUid: foundSecUid };
    if (finalOut.userId || finalOut.secUid) tiktokProfileCache.set(clean, { ...finalOut, at: Date.now() });
    return finalOut;
  });
}

async function prewarmTikTokProfiles(usernames) {
  if (!Array.isArray(usernames) || usernames.length === 0) return;
  const cleanList = [...new Set(usernames.map(u => normalizeTikTokHandle(u).toLowerCase()).filter(Boolean))]
    .filter(uname => !(tiktokProfileCache.get(uname)?.userId));
  
  let cursor = 0;
  const worker = async () => {
    while (cursor < cleanList.length) {
      await resolveTikTokUserProfile(cleanList[cursor++]).catch(() => {});
    }
  };
  await Promise.all(Array.from({ length: Math.min(6, cleanList.length) }, () => worker()));
}

async function signTikTokUrlIfPossible(targetUrl) {
  try {
    const lib = await getTikTokLib();
    const TikTokLiveConnection = lib.TikTokLiveConnection || lib.WebcastPushConnection;
    const tempConn = new TikTokLiveConnection('tiktok', { processInitialData: false, ...(CONFIG.eulerApiKey ? { signApiKey: CONFIG.eulerApiKey } : {}) });
    if (tempConn?.webClient?.webSigner?.signTiktokUrl) {
      const signed = await tempConn.webClient.webSigner.signTiktokUrl(targetUrl);
      return signed?.signedUrl || signed || targetUrl;
    }
  } catch (e) {}
  return targetUrl;
}

async function blockTikTokUserOnAccount(userObj) {
  const startMs = Date.now();
  const cleanUsername = normalizeTikTokHandle(userObj?.uniqueId);

  if (!tiktokAuthSession.loggedIn || !tiktokAuthSession.cookieHeader) return { blockedOnTikTok: false, note: 'Not logged into TikTok Account' };

  let targetUserId = toNumericIdString(userObj?.userId);
  let targetSecUid = String(userObj?.secUid || '').trim();

  if ((!targetUserId || !targetSecUid) && cleanUsername) {
    const cached = tiktokProfileCache.get(cleanUsername.toLowerCase());
    if (cached) { if (!targetUserId) targetUserId = cached.userId; if (!targetSecUid) targetSecUid = cached.secUid; }
  }
  if ((!targetUserId && !targetSecUid) && cleanUsername) {
    const resolved = await resolveTikTokUserProfile(cleanUsername);
    if (!targetUserId) targetUserId = resolved.userId;
    if (!targetSecUid) targetSecUid = resolved.secUid;
  }

  if (!targetSecUid && !targetUserId) return { blockedOnTikTok: false, note: `Could not find @${cleanUsername}` };

  const csrfToken = tiktokAuthSession.csrfToken || crypto.randomBytes(16).toString('hex');
  const webHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded',
    Origin: 'https://www.tiktok.com', Referer: `https://www.tiktok.com/@${cleanUsername}`,
    Cookie: tiktokAuthSession.cookieHeader, 'x-secsdk-csrf-token': csrfToken, 'tt-csrf-token': csrfToken
  };

  let lastNote = '';
  for (let attempt = 1; attempt <= CONFIG.tiktok.maxBlockRetries; attempt++) {
    try {
      const webQuery = new URLSearchParams({
        aid: '1988', app_name: 'tiktok_web', browser_name: 'Mozilla', browser_platform: 'Win32', device_platform: 'web_pc', block_type: '1', source: '0',
        ...(targetSecUid ? { secUid: targetSecUid, sec_user_id: targetSecUid, target_sec_uid: targetSecUid } : {}),
        ...(targetUserId ? { userId: targetUserId, user_id: targetUserId, target_uid: targetUserId } : {}),
        ...(tiktokAuthSession.msToken ? { msToken: tiktokAuthSession.msToken } : {})
      });
      const bodyForm = new URLSearchParams({ block_type: '1', source: '0', ...(targetSecUid ? { sec_user_id: targetSecUid } : {}), ...(targetUserId ? { user_id: targetUserId } : {}) });

      const signedUrl = await signTikTokUrlIfPossible(`https://www.tiktok.com/api/user/block/?${webQuery.toString()}`);
      const res1 = await tiktokHttp.post(signedUrl, bodyForm.toString(), { headers: webHeaders, timeout: CONFIG.tiktok.blockTimeoutMs });

      if (res1.status === 200 && (res1.data?.status_code === 0 || res1.data?.statusCode === 0)) {
        recordTikTokDiagnostic({ username: cleanUsername, userId: targetUserId, success: true, vector: `WEB_V1_ATTEMPT_${attempt}`, elapsedMs: Date.now() - startMs });
        return { blockedOnTikTok: true };
      }
      lastNote = res1.data?.status_msg || `code ${res1.data?.status_code}`;
      if (String(lastNote).toLowerCase().includes('invalid') || String(lastNote).toLowerCase().includes('param')) {
        const fresh = await resolveTikTokUserProfile(cleanUsername);
        if (fresh.userId) targetUserId = fresh.userId;
        if (fresh.secUid) targetSecUid = fresh.secUid;
      }
    } catch (err) { lastNote = err.message; }

    if (attempt === CONFIG.tiktok.maxBlockRetries) {
      try {
        const mQuery = new URLSearchParams({ aid: '1233', device_platform: 'android', block_type: '1', source: '0', ...(targetSecUid ? { sec_user_id: targetSecUid } : {}), ...(targetUserId ? { user_id: targetUserId } : {}) });
        const resMob = await tiktokHttp.post(`https://api16-normal-c-useast1a.tiktokv.com/aweme/v1/user/block/?${mQuery.toString()}`, mQuery.toString(), {
          headers: { 'User-Agent': 'com.zhiliaoapp.musically/2023501030', Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded', Cookie: tiktokAuthSession.cookieHeader }, timeout: 5500
        });
        if (resMob.status === 200 && resMob.data?.status_code === 0) return { blockedOnTikTok: true };
        if (resMob.data?.status_msg) lastNote = resMob.data.status_msg;
      } catch (e) {}
    }
    await delay(300 * attempt);
  }
  recordTikTokDiagnostic({ username: cleanUsername, userId: targetUserId, success: false, vector: 'FAILED', note: lastNote, elapsedMs: Date.now() - startMs });
  return { blockedOnTikTok: false, note: lastNote };
}

async function runParallelTikTokBlockBatch(usernames) {
  const cleanList = [...new Set(usernames.map(u => normalizeTikTokHandle(u)).filter(Boolean))];
  ttBlacklist.add(cleanList);

  let cursor = 0, blockedOnTikTokCount = 0, failedCount = 0, lastNote = null;
  const worker = async (laneIdx) => {
    if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.slotStaggerMs);
    while (cursor < cleanList.length) {
      const uname = cleanList[cursor++];
      if (!uname) continue;
      const existing = tiktokSharers.get(uname) || { uniqueId: uname };
      existing.blacklisted = true;
      if (tiktokSharers.has(uname)) tiktokSharers.set(uname, existing);

      if (tiktokAuthSession.loggedIn) {
        try {
          const result = await blockTikTokUserOnAccount(existing);
          if (result.blockedOnTikTok) blockedOnTikTokCount++;
          else { failedCount++; if (result.note) lastNote = result.note; }
        } catch (err) { failedCount++; lastNote = err.message; }
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONFIG.tiktok.blockConcurrency, cleanList.length) }, (_, i) => worker(i)));
  broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() });
  return { totalBlacklisted: cleanList.length, blockedOnTikTokCount, failedCount, lastNote };
}

function recordTikTokSharer(data) {
  const parsed = extractShareUser(data);
  if (!parsed.uniqueId) return;

  const lowerKey = parsed.uniqueId.toLowerCase();
  if (parsed.userId || parsed.secUid) {
    const prev = tiktokProfileCache.get(lowerKey) || {};
    tiktokProfileCache.set(lowerKey, { userId: parsed.userId || prev.userId || '', secUid: parsed.secUid || prev.secUid || '', at: Date.now() });
  }

  const existing = tiktokSharers.get(parsed.uniqueId);
  if (existing && Date.now() - existing.timestamp < CONFIG.tiktok.shareDedupWindowMs) return;
  
  const shareCount = existing ? existing.shareCount + 1 : 1;
  const isBlacklisted = ttBlacklist.has(parsed.uniqueId);
  const userEntry = {
    uniqueId: parsed.uniqueId, nickname: parsed.nickname, userId: parsed.userId || existing?.userId || '', secUid: parsed.secUid || existing?.secUid || '',
    profilePictureUrl: parsed.profilePictureUrl, shareCount, lastSharedAt: new Date().toLocaleTimeString(), timestamp: Date.now(), blacklisted: isBlacklisted
  };
  tiktokSharers.set(parsed.uniqueId, userEntry);

  if (tiktokAuthSession.autoBlockSharers && shareCount >= 2 && !isBlacklisted) {
    ttBlacklist.add([parsed.uniqueId]);
    userEntry.blacklisted = true;
    if (tiktokAuthSession.loggedIn) blockTikTokUserOnAccount(userEntry).catch(() => {});
    broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() });
  } else if (!userEntry.secUid || !userEntry.userId) {
    resolveTikTokUserProfile(parsed.uniqueId).then(resolved => {
      if (resolved.userId && !userEntry.userId) userEntry.userId = resolved.userId;
      if (resolved.secUid && !userEntry.secUid) userEntry.secUid = resolved.secUid;
    }).catch(() => {});
  }
  broadcastTikTokEvent({ type: 'SHARE', user: userEntry, totalUnique: tiktokSharers.size });
}

const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: CONFIG.corsOrigin }));
app.use(express.json({ limit: '4mb' }));

app.get('/', (req, res) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(htmlPath)) return res.sendFile(htmlPath);
  res.status(200).json({ ok: true, service: 'nexus-tiktok-backend' });
});

const asyncRoute = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

app.get('/api/health', (req, res) => {
  res.json({ ok: true, uptimeSec: Math.round(process.uptime()), ttBlacklisted: ttBlacklist.all().length, ttProfileCacheSize: tiktokProfileCache.size });
});

app.get('/api/telemetry', (req, res) => { res.json({ logs: telemetryRingBuffer.slice(-100) }); });

app.get('/api/tiktok/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();

  const clientId = Date.now() + Math.random();
  sseClients.push({ id: clientId, res });
  res.write(`data: ${JSON.stringify({ type: 'INIT', state: currentTikTokState, auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers }, sharers: Array.from(tiktokSharers.values()), blacklist: ttBlacklist.all() })}\n\n`);

  const keepAlive = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) {} }, 15000);
  req.on('close', () => { clearInterval(keepAlive); sseClients = sseClients.filter((c) => c.id !== clientId); });
});

app.post('/api/tiktok/login', asyncRoute(async (req, res) => {
  const { cookie, accountName } = req.body || {};
  const parsed = await buildTikTokCookieHeader(cookie);
  if (!parsed.cookieHeader) return res.status(400).json({ error: 'Invalid TikTok cookie string.' });

  tiktokAuthSession.loggedIn = true;
  tiktokAuthSession.cookieHeader = parsed.cookieHeader;
  tiktokAuthSession.sessionId = parsed.sessionId;
  tiktokAuthSession.ttTargetIdc = parsed.ttTargetIdc || 'useast1a';
  tiktokAuthSession.csrfToken = parsed.csrfToken;
  tiktokAuthSession.msToken = parsed.msToken;
  tiktokAuthSession.ttwid = parsed.ttwid;
  tiktokAuthSession.odinTt = parsed.odinTt;
  tiktokAuthSession.accountName = normalizeTikTokHandle(accountName) || 'TikTok Account';

  prewarmTikTokProfiles(ttBlacklist.all()).catch(() => {});
  broadcastTikTokEvent({ type: 'AUTH', auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers } });
  res.json({ success: true, auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers } });
}));

app.post('/api/tiktok/logout', (req, res) => {
  tiktokAuthSession = { loggedIn: false, cookieHeader: '', sessionId: '', ttTargetIdc: 'useast1a', csrfToken: '', msToken: '', ttwid: '', odinTt: '', accountName: '', autoBlockSharers: false };
  broadcastTikTokEvent({ type: 'AUTH', auth: tiktokAuthSession });
  res.json({ success: true });
});

app.post('/api/tiktok/auto-block', (req, res) => {
  tiktokAuthSession.autoBlockSharers = Boolean(req.body?.enabled);
  broadcastTikTokEvent({ type: 'AUTH', auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers } });
  res.json({ success: true, autoBlockSharers: tiktokAuthSession.autoBlockSharers });
});

app.get('/api/tiktok/blacklist', (req, res) => {
  const list = ttBlacklist.all();
  prewarmTikTokProfiles(list).catch(() => {});
  res.json({ usernames: list, text: list.join('\n') });
});

app.post('/api/tiktok/blacklist/save', (req, res) => {
  const { text, usernames } = req.body || {};
  const total = ttBlacklist.replaceAll(usernames || text || '');
  const allList = ttBlacklist.all();
  for (const [uid, entry] of tiktokSharers.entries()) entry.blacklisted = ttBlacklist.has(uid);
  prewarmTikTokProfiles(allList).catch(() => {});
  broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: allList });
  res.json({ success: true, total, usernames: allList });
});

app.post('/api/tiktok/blacklist/add', (req, res) => {
  const { usernames } = req.body || {};
  if (!Array.isArray(usernames)) return res.status(400).json({ error: 'usernames must be an array.' });
  const addedCount = ttBlacklist.add(usernames);
  for (const [uid, entry] of tiktokSharers.entries()) entry.blacklisted = ttBlacklist.has(uid);
  prewarmTikTokProfiles(usernames).catch(() => {});
  broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() });
  res.json({ success: true, addedCount, total: ttBlacklist.all().length, usernames: ttBlacklist.all() });
});

app.post('/api/tiktok/block', asyncRoute(async (req, res) => {
  const { usernames } = req.body || {};
  const targets = Array.isArray(usernames) ? usernames : [];
  if (targets.length === 0) return res.status(400).json({ error: 'No usernames provided to block.' });
  const batchRes = await runParallelTikTokBlockBatch(targets);
  res.json({ success: true, totalBlacklisted: batchRes.totalBlacklisted, blockedOnTikTokCount: batchRes.blockedOnTikTokCount, failedCount: batchRes.failedCount, loggedIn: tiktokAuthSession.loggedIn, note: batchRes.lastNote, blacklist: ttBlacklist.all() });
}));

app.post('/api/tiktok/jobs/block', asyncRoute(async (req, res) => {
  const { usernames } = req.body || {};
  const list = Array.isArray(usernames) ? [...new Set(usernames.map(u => normalizeTikTokHandle(u)).filter(Boolean))] : [];
  if (list.length === 0) return res.status(400).json({ error: 'No usernames provided.' });

  ttBlacklist.add(list);
  const jobId = crypto.randomUUID();
  const jobObj = { id: jobId, status: 'running', total: list.length, processed: 0, blockedOnTikTokCount: 0, failedCount: 0, currentUsername: null, lastNote: null, startedAt: Date.now(), finishedAt: null, cancelRequested: false };
  tiktokBlockJobs.set(jobId, jobObj);

  (async () => {
    let cursor = 0;
    const worker = async (laneIdx) => {
      if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.slotStaggerMs);
      while (cursor < list.length) {
        if (jobObj.cancelRequested) { jobObj.status = 'cancelled'; return; }
        const uname = list[cursor++];
        if (!uname) continue;
        jobObj.currentUsername = uname;
        const existing = tiktokSharers.get(uname) || { uniqueId: uname };
        existing.blacklisted = true;
        if (tiktokSharers.has(uname)) tiktokSharers.set(uname, existing);
        if (tiktokAuthSession.loggedIn) {
          try {
            const result = await blockTikTokUserOnAccount(existing);
            if (result.blockedOnTikTok) jobObj.blockedOnTikTokCount++;
            else { jobObj.failedCount++; if (result.note) jobObj.lastNote = result.note; }
          } catch (err) { jobObj.failedCount++; jobObj.lastNote = err.message; }
        }
        jobObj.processed++;
      }
    };
    await Promise.all(Array.from({ length: Math.min(CONFIG.tiktok.blockConcurrency, list.length) }, (_, i) => worker(i)));
    if (jobObj.status === 'running') jobObj.status = 'done';
    jobObj.currentUsername = null; jobObj.finishedAt = Date.now();
    broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() });
  })().catch(err => { jobObj.status = 'aborted'; jobObj.lastNote = err.message; jobObj.finishedAt = Date.now(); });
  res.status(202).json(jobObj);
}));

app.get('/api/tiktok/jobs/:id', (req, res) => {
  const job = tiktokBlockJobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'No such TikTok job.' });
  res.json(job);
});

app.delete('/api/tiktok/jobs/:id', (req, res) => {
  const job = tiktokBlockJobs.get(req.params.id);
  if (!job || job.status !== 'running') return res.status(404).json({ error: 'No active TikTok job.' });
  job.cancelRequested = true;
  res.json({ success: true });
});

app.get('/api/tiktok/diagnostics', (req, res) => {
  res.json({
    auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, hasSessionId: Boolean(tiktokAuthSession.sessionId), ttTargetIdc: tiktokAuthSession.ttTargetIdc },
    cachedProfilesCount: tiktokProfileCache.size,
    recentAttempts: tiktokDiagnosticHistory.slice(-50).reverse(),
  });
});

app.post('/api/tiktok/prewarm', asyncRoute(async (req, res) => {
  const { usernames } = req.body || {};
  if (!Array.isArray(usernames)) return res.status(400).json({ error: 'usernames array required' });
  prewarmTikTokProfiles(usernames).catch(() => {});
  res.json({ success: true, queued: usernames.length });
}));

app.post('/api/tiktok/connect', asyncRoute(async (req, res) => {
  const rawUsername = normalizeTikTokHandle(req.body?.username);
  if (!rawUsername) return res.status(400).json({ error: 'TikTok username is required.' });

  if (tiktokConnection) { try { await tiktokConnection.disconnect(); } catch (e) {} tiktokConnection = null; }

  try {
    const lib = await getTikTokLib();
    const TikTokLiveConnection = lib.TikTokLiveConnection || lib.WebcastPushConnection;
    const WebcastEvent = lib.WebcastEvent || {};
    const ControlEvent = lib.ControlEvent || {};

    const options = { processInitialData: true, enableExtendedGiftInfo: false, enableWebsocketUpgrade: true, requestPollingIntervalMs: 700 };
    if (CONFIG.eulerApiKey) options.signApiKey = CONFIG.eulerApiKey;
    if (tiktokAuthSession.loggedIn && tiktokAuthSession.sessionId) {
      options.sessionId = tiktokAuthSession.sessionId;
      options.ttTargetIdc = tiktokAuthSession.ttTargetIdc || 'useast1a';
    }

    tiktokConnection = new TikTokLiveConnection(rawUsername, options);
    const shareEvents = new Set([WebcastEvent.SHARE, 'share', 'WebcastSocialMessage'].filter(Boolean));
    const socialEvents = new Set([WebcastEvent.SOCIAL, 'social'].filter(Boolean));

    for (const ev of shareEvents) tiktokConnection.on(ev, data => recordTikTokSharer(data));
    for (const ev of socialEvents) tiktokConnection.on(ev, data => { if (isSocialSharePacket(data)) recordTikTokSharer(data); });
    
    tiktokConnection.on(ControlEvent.RAW_DATA || 'rawData', (type, BinaryOrObj) => {
      if (typeof type === 'string' && type.toLowerCase().includes('social') && BinaryOrObj && typeof BinaryOrObj === 'object') {
        if (isSocialSharePacket(BinaryOrObj)) recordTikTokSharer(BinaryOrObj);
      }
    });

    tiktokConnection.on(WebcastEvent.STREAM_END || ControlEvent.STREAM_END || 'streamEnd', () => {
      currentTikTokState = { connected: false, username: rawUsername, roomId: null, error: 'Stream ended' };
      broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState });
    });

    tiktokConnection.on(ControlEvent.DISCONNECTED || 'disconnected', () => {
      currentTikTokState = { connected: false, username: rawUsername, roomId: null, error: 'Disconnected' };
      broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState });
    });

    tiktokConnection.on('error', err => log.warn('TikTok connection error:', err?.message || err));

    const state = await tiktokConnection.connect();
    currentTikTokState = { connected: true, username: rawUsername, roomId: String(state?.roomId || 'live'), error: null };
    log.info(`Connected to TikTok LIVE @${rawUsername}`);
    broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState });
    res.json({ success: true, state: currentTikTokState });
  } catch (err) {
    currentTikTokState = { connected: false, username: rawUsername, roomId: null, error: err.message };
    broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState });
    res.status(400).json({ error: err.message });
  }
}));

app.post('/api/tiktok/disconnect', asyncRoute(async (req, res) => {
  if (tiktokConnection) { try { await tiktokConnection.disconnect(); } catch (e) {} tiktokConnection = null; }
  currentTikTokState = { connected: false, username: null, roomId: null, error: null };
  broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState });
  res.json({ success: true, state: currentTikTokState });
}));

app.post('/api/tiktok/clear', (req, res) => {
  tiktokSharers.clear();
  broadcastTikTokEvent({ type: 'CLEAR' });
  res.json({ success: true });
});

app.get('/api/tiktok/sharers', (req, res) => {
  res.json({
    state: currentTikTokState,
    auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers },
    sharers: Array.from(tiktokSharers.values()),
    blacklist: ttBlacklist.all(),
  });
});

app.use((err, req, res, _next) => {
  log.error(`${req.method} ${req.path} -> 500: ${err.message}`);
  res.status(500).json({ error: err.message, kind: 'UNKNOWN' });
});

const server = app.listen(CONFIG.port, () => {
  log.info(`Nexus TikTok active on port ${CONFIG.port}`);
  prewarmTikTokProfiles(ttBlacklist.all()).catch(() => {});
});

for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => server.close(() => process.exit(0)));
