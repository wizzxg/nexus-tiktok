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
  info: (...a) => { const msg = a.join(' '); console.log(`[${new Date().toISOString()}] [info]`, msg); pushTelemetry('info', 'system', msg); }, warn: (...a) => { const msg = a.join(' '); console.warn(`[${new Date().toISOString()}] [warn]`, msg); pushTelemetry('warn', 'system', msg); },
  error: (...a) => { const msg = a.join(' '); console.error(`[${new Date().toISOString()}] [error]`, msg); pushTelemetry('error', 'system', msg); } };  const delay = (ms) => new Promise((res) => setTimeout(res, ms));  const httpsAgent = new https.Agent({ keepAlive: true, keepAliveMsecs: 60000, maxSockets: 128, rejectUnauthorized: true }); const tiktokHttp = axios.create({ httpsAgent, timeout: CONFIG.tiktok.resolveTimeoutMs, headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36', Accept: 'application/json, text/plain, */*' }, validateStatus: () => true });  function toNumericIdString(val) { if (val == null) return ''; if (typeof val === 'string') { const t = val.trim(); return /^\d+$/.test(t) && t !== '0' ? t : ''; } if (typeof val === 'number') return Number.isFinite(val) && val > 0 ? String(Math.trunc(val)) : ''; if (typeof val === 'object') { if (typeof val.low === 'number' && typeof val.high === 'number') { try { const h = BigInt(val.high >>> 0); const l = BigInt(val.low >>> 0); const c = (h << 32n) \vert{} l; if (c > 0n) return c.toString(); } catch (e) {} } if (typeof val.toString === 'function') { const s = String(val.toString()).trim(); if (/^\d+$/.test(s) && s !== '0') return s; } } return ''; }  function normalizeTikTokHandle(raw) { return String(raw \vert{}\vert{} '').trim().replace(/^@+/, '').trim(); }  class TikTokBlacklist { constructor(filePath) { this.filePath = filePath; this.usernames = new Set(); this.writeChain = Promise.resolve(); } load() { try { if (!fs.existsSync(this.filePath)) return fs.writeFileSync(this.filePath, ''); for (const line of fs.readFileSync(this.filePath, 'utf8').split(/\r?\n/)) { const clean = normalizeTikTokHandle(line).toLowerCase(); if (clean) this.usernames.add(clean); } } catch (e) {} } has(username) { return this.usernames.has(normalizeTikTokHandle(username).toLowerCase()); } add(list) { let added = 0; for (const item of list) { const clean = normalizeTikTokHandle(item).toLowerCase(); if (clean && !this.usernames.has(clean)) { this.usernames.add(clean); added++; } } if (added > 0) this.saveToDisk(); return added; } replaceAll(arr) { const lines = Array.isArray(arr) ? arr : String(arr \vert{}\vert{} '').split(/\r?\n/); this.usernames.clear(); for (const line of lines) { const clean = normalizeTikTokHandle(line).toLowerCase(); if (clean) this.usernames.add(clean); } this.saveToDisk(); return this.usernames.size; } saveToDisk() { const content = Array.from(this.usernames).join('\n') + (this.usernames.size ? '\n' : ''); this.writeChain = this.writeChain.then(() => fsp.writeFile(this.filePath, content)).catch(() => {}); } all() { return Array.from(this.usernames); } }  const ttBlacklist = new TikTokBlacklist(CONFIG.ttBlacklistPath); ttBlacklist.load();  let tiktokLib = null; async function getTikTokLib() { if (!tiktokLib) tiktokLib = await import('tiktok-live-connector'); return tiktokLib; }  let tiktokConnection = null; let currentTikTokState = { connected: false, username: null, roomId: null, error: null }; let tiktokAuthSession = { loggedIn: false, cookieHeader: '', sessionId: '', ttTargetIdc: 'useast1a', csrfToken: '', msToken: '', ttwid: '', odinTt: '', accountName: '', autoBlockSharers: false };  const tiktokSharers = new Map(); const tiktokProfileCache = new Map(); const tiktokBlockJobs = new Map(); const tiktokDiagnosticHistory = []; let sseClients = [];  function broadcastTikTokEvent(payload) { const msg = `data: ${JSON.stringify(payload)}\n\n`;
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
  if (!csrfToken) { csrfToken = crypto.randomBytes(16).toString('hex'); cookieHeader += `; tt_csrf_token=${csrfToken};`; }  tiktokHttp.get('https://www.tiktok.com/explore', { headers: { Cookie: cookieHeader }, timeout: 5000 }).then(res => { for (const sc of (res.headers?.['set-cookie'] \vert{}\vert{} [])) { const mMatch = /msToken=([^;]+)/i.exec(sc); if (mMatch && !tiktokAuthSession.msToken) { tiktokAuthSession.msToken = mMatch[1]; tiktokAuthSession.cookieHeader += `; msToken=${mMatch[1]}`; }
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

  return ttResolveFlight(`tt-resolve:${clean}`, async () => { let foundUserId = '', foundSecUid = ''; const tikwmTask = tiktokHttp.get(`https://www.tikwm.com/api/user/info?unique_id=${encodeURIComponent(clean)}`, { timeout: 4500, headers: { Accept: 'application/json' } })
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
      const resHtml = await tiktokHttp.get(`https://www.tiktok.com/@${encodeURIComponent(clean)}`, { timeout: 5500, headers: { Accept: 'text/html' } }); const html = typeof resHtml.data === 'string' ? resHtml.data : ''; const m1 = new RegExp(`"id"\\s*:\\s*"(\\d+)"[^}]{0,300}"uniqueId"\\s*:\\s*"${clean}"[^}]{0,600}"secUid"\\s*:\\s*"([^"]+)"`, 'i').exec(html);
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
    Origin: 'https://www.tiktok.com', Referer: `https://www.tiktok.com/@${cleanUsername}`, Cookie: tiktokAuthSession.cookieHeader, 'x-secsdk-csrf-token': csrfToken, 'tt-csrf-token': csrfToken };  let lastNote = ''; for (let attempt = 1; attempt <= CONFIG.tiktok.maxBlockRetries; attempt++) { try { const webQuery = new URLSearchParams({ aid: '1988', app_name: 'tiktok_web', browser_name: 'Mozilla', browser_platform: 'Win32', device_platform: 'web_pc', block_type: '1', source: '0', ...(targetSecUid ? { secUid: targetSecUid, sec_user_id: targetSecUid, target_sec_uid: targetSecUid } : {}), ...(targetUserId ? { userId: targetUserId, user_id: targetUserId, target_uid: targetUserId } : {}), ...(tiktokAuthSession.msToken ? { msToken: tiktokAuthSession.msToken } : {}) }); const bodyForm = new URLSearchParams({ block_type: '1', source: '0', ...(targetSecUid ? { sec_user_id: targetSecUid } : {}), ...(targetUserId ? { user_id: targetUserId } : {}) });  const signedUrl = await signTikTokUrlIfPossible(`https://www.tiktok.com/api/user/block/?${webQuery.toString()}`);
      const res1 = await tiktokHttp.post(signedUrl, bodyForm.toString(), { headers: webHeaders, timeout: CONFIG.tiktok.blockTimeoutMs });

      if (res1.status === 200 && (res1.data?.status_code === 0 || res1.data?.statusCode === 0)) {
        recordTikTokDiagnostic({ username: cleanUsername, userId: targetUserId, success: true, vector: `WEB_V1_ATTEMPT_${attempt}`, elapsedMs: Date.now() - startMs }); return { blockedOnTikTok: true }; } lastNote = res1.data?.status_msg \vert{}\vert{} `code ${res1.data?.status_code}`;
      if (String(lastNote).toLowerCase().includes('invalid') || String(lastNote).toLowerCase().includes('param')) {
        const fresh = await resolveTikTokUserProfile(cleanUsername);
        if (fresh.userId) targetUserId = fresh.userId;
        if (fresh.secUid) targetSecUid = fresh.secUid;
      }
    } catch (err) { lastNote = err.message; }

    if (attempt === CONFIG.tiktok.maxBlockRetries) {
      try {
        const mQuery = new URLSearchParams({ aid: '1233', device_platform: 'android', block_type: '1', source: '0', ...(targetSecUid ? { sec_user_id: targetSecUid } : {}), ...(targetUserId ? { user_id: targetUserId } : {}) });
        const resMob = await tiktokHttp.post(`https://api16-normal-c-useast1a.tiktokv.com/aweme/v1/user/block/?${mQuery.toString()}`, mQuery.toString(), { headers: { 'User-Agent': 'com.zhiliaoapp.musically/2023501030', Accept: 'application/json', 'Content-Type': 'application/x-www-form-urlencoded', Cookie: tiktokAuthSession.cookieHeader }, timeout: 5500 }); if (resMob.status === 200 && resMob.data?.status_code === 0) return { blockedOnTikTok: true }; if (resMob.data?.status_msg) lastNote = resMob.data.status_msg; } catch (e) {} } await delay(300 * attempt); } recordTikTokDiagnostic({ username: cleanUsername, userId: targetUserId, success: false, vector: 'FAILED', note: lastNote, elapsedMs: Date.now() - startMs }); return { blockedOnTikTok: false, note: lastNote }; }  async function runParallelTikTokBlockBatch(usernames) { const cleanList = [...new Set(usernames.map(u => normalizeTikTokHandle(u)).filter(Boolean))]; ttBlacklist.add(cleanList);  let cursor = 0, blockedOnTikTokCount = 0, failedCount = 0, lastNote = null; const worker = async (laneIdx) => { if (laneIdx > 0) await delay(laneIdx * CONFIG.tiktok.slotStaggerMs); while (cursor < cleanList.length) { const uname = cleanList[cursor++]; if (!uname) continue; const existing = tiktokSharers.get(uname) \vert{}\vert{} { uniqueId: uname }; existing.blacklisted = true; if (tiktokSharers.has(uname)) tiktokSharers.set(uname, existing);  if (tiktokAuthSession.loggedIn) { try { const result = await blockTikTokUserOnAccount(existing); if (result.blockedOnTikTok) blockedOnTikTokCount++; else { failedCount++; if (result.note) lastNote = result.note; } } catch (err) { failedCount++; lastNote = err.message; } } } }; await Promise.all(Array.from({ length: Math.min(CONFIG.tiktok.blockConcurrency, cleanList.length) }, (_, i) => worker(i))); broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() }); return { totalBlacklisted: cleanList.length, blockedOnTikTokCount, failedCount, lastNote }; }  function recordTikTokSharer(data) { const parsed = extractShareUser(data); if (!parsed.uniqueId) return;  const lowerKey = parsed.uniqueId.toLowerCase(); if (parsed.userId \vert{}\vert{} parsed.secUid) { const prev = tiktokProfileCache.get(lowerKey) \vert{}\vert{} {}; tiktokProfileCache.set(lowerKey, { userId: parsed.userId \vert{}\vert{} prev.userId \vert{}\vert{} '', secUid: parsed.secUid \vert{}\vert{} prev.secUid \vert{}\vert{} '', at: Date.now() }); }  const existing = tiktokSharers.get(parsed.uniqueId); if (existing && Date.now() - existing.timestamp < CONFIG.tiktok.shareDedupWindowMs) return;  const shareCount = existing ? existing.shareCount + 1 : 1; const isBlacklisted = ttBlacklist.has(parsed.uniqueId); const userEntry = { uniqueId: parsed.uniqueId, nickname: parsed.nickname, userId: parsed.userId \vert{}\vert{} existing?.userId \vert{}\vert{} '', secUid: parsed.secUid \vert{}\vert{} existing?.secUid \vert{}\vert{} '', profilePictureUrl: parsed.profilePictureUrl, shareCount, lastSharedAt: new Date().toLocaleTimeString(), timestamp: Date.now(), blacklisted: isBlacklisted }; tiktokSharers.set(parsed.uniqueId, userEntry);  if (tiktokAuthSession.autoBlockSharers && shareCount >= 2 && !isBlacklisted) { ttBlacklist.add([parsed.uniqueId]); userEntry.blacklisted = true; if (tiktokAuthSession.loggedIn) blockTikTokUserOnAccount(userEntry).catch(() => {}); broadcastTikTokEvent({ type: 'BLACKLIST', blacklist: ttBlacklist.all() }); } else if (!userEntry.secUid \vert{}\vert{} !userEntry.userId) { resolveTikTokUserProfile(parsed.uniqueId).then(resolved => { if (resolved.userId && !userEntry.userId) userEntry.userId = resolved.userId; if (resolved.secUid && !userEntry.secUid) userEntry.secUid = resolved.secUid; }).catch(() => {}); } broadcastTikTokEvent({ type: 'SHARE', user: userEntry, totalUnique: tiktokSharers.size }); }  const app = express(); app.disable('x-powered-by'); app.use(cors({ origin: CONFIG.corsOrigin })); app.use(express.json({ limit: '4mb' }));  app.get('/', (req, res) => { const htmlPath = path.join(__dirname, 'index.html'); if (fs.existsSync(htmlPath)) return res.sendFile(htmlPath); res.status(200).json({ ok: true, service: 'nexus-tiktok-backend' }); });  const asyncRoute = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);  app.get('/api/health', (req, res) => { res.json({ ok: true, uptimeSec: Math.round(process.uptime()), ttBlacklisted: ttBlacklist.all().length, ttProfileCacheSize: tiktokProfileCache.size }); });  app.get('/api/telemetry', (req, res) => { res.json({ logs: telemetryRingBuffer.slice(-100) }); });  app.get('/api/tiktok/stream', (req, res) => { res.setHeader('Content-Type', 'text/event-stream'); res.setHeader('Cache-Control', 'no-cache, no-transform'); res.setHeader('Connection', 'keep-alive'); res.setHeader('X-Accel-Buffering', 'no'); res.flushHeaders?.();  const clientId = Date.now() + Math.random(); sseClients.push({ id: clientId, res }); res.write(`data: ${JSON.stringify({ type: 'INIT', state: currentTikTokState, auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers }, sharers: Array.from(tiktokSharers.values()), blacklist: ttBlacklist.all() })}\n\n`);

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
    log.info(`Connected to TikTok LIVE @${rawUsername}`); broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState }); res.json({ success: true, state: currentTikTokState }); } catch (err) { currentTikTokState = { connected: false, username: rawUsername, roomId: null, error: err.message }; broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState }); res.status(400).json({ error: err.message }); } }));  app.post('/api/tiktok/disconnect', asyncRoute(async (req, res) => { if (tiktokConnection) { try { await tiktokConnection.disconnect(); } catch (e) {} tiktokConnection = null; } currentTikTokState = { connected: false, username: null, roomId: null, error: null }; broadcastTikTokEvent({ type: 'STATUS', state: currentTikTokState }); res.json({ success: true, state: currentTikTokState }); }));  app.post('/api/tiktok/clear', (req, res) => { tiktokSharers.clear(); broadcastTikTokEvent({ type: 'CLEAR' }); res.json({ success: true }); });  app.get('/api/tiktok/sharers', (req, res) => { res.json({ state: currentTikTokState, auth: { loggedIn: tiktokAuthSession.loggedIn, accountName: tiktokAuthSession.accountName, autoBlockSharers: tiktokAuthSession.autoBlockSharers }, sharers: Array.from(tiktokSharers.values()), blacklist: ttBlacklist.all(), }); });  app.use((err, req, res, _next) => { log.error(`${req.method} ${req.path} -> 500:${err.message}`);
  res.status(500).json({ error: err.message, kind: 'UNKNOWN' });
});

const server = app.listen(CONFIG.port, () => {
  log.info(`Nexus TikTok active on port ${CONFIG.port}`); prewarmTikTokProfiles(ttBlacklist.all()).catch(() => {}); });  for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => server.close(() => process.exit(0))); ```  ---  **3. `index.html`** ```html <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="viewport" content="width=device-width, initial-scale=1.0"> <title>NEXUS // TikTok Terminal</title> <link rel="icon" href="https://ibb.co/BKny7SJh"> <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin> <link rel="preconnect" href="https://tr.rbxcdn.com"> <style> @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap'); :root { --void: #05060b; --surface: #0a0d16; --surface-2: #0f1320; --surface-3: #161b2c; --surface-glass: rgba(12, 15, 26, 0.86); --line: rgba(148, 163, 196, 0.09); --line-2: rgba(148, 163, 196, 0.18); --text: #eef2f9; --muted: #8b95af; --faint: #555f78; --cyan: #2de2d0; --cyan-glow: rgba(45, 226, 208, 0.25); --cyan-soft: rgba(45, 226, 208, 0.10); --emerald: #10b981; --emerald-soft: rgba(16, 185, 129, 0.12); --red: #ff4769; --red-soft: rgba(255, 71, 105, 0.11); --amber: #f59e0b; --violet: #a78bfa; --r-xs: 4px; --r-md: 8px; --mono: 'JetBrains Mono', monospace; --sans: 'Inter', sans-serif; } * { box-sizing: border-box; } body { margin: 0; min-height: 100vh; color: var(--text); font-family: var(--sans); font-size: 14px; background: var(--void); background-image: radial-gradient(1100px 520px at 50\% -15\%, rgba(45,226,208,0.08), transparent 65\%), linear-gradient(180deg, #070912 0\%, var(--void) 50\%); overflow-x: hidden; } ::-webkit-scrollbar { width: 8px; height: 8px; } ::-webkit-scrollbar-track { background: var(--void); } ::-webkit-scrollbar-thumb { background: var(--surface-3); border-radius: 4px; } :focus-visible { outline: 1.5px solid var(--cyan); outline-offset: 2px; }  .btn { font-family: var(--mono); font-size: 10.5px; font-weight: 600; text-transform: uppercase; color: var(--muted); background: linear-gradient(180deg, var(--surface-2), var(--surface)); border: 1px solid var(--line-2); padding: 8px 13px; border-radius: var(--r-xs); cursor: pointer; transition: all 0.15s; display: inline-flex; align-items: center; gap: 7px; white-space: nowrap; } .btn:hover { color: var(--text); border-color: rgba(148,163,196,0.38); background: var(--surface-3); transform: translateY(-1px); } .btn-primary { color: var(--cyan); border-color: rgba(45,226,208,0.32); background: var(--cyan-soft); } .btn-primary:hover { color: #031412; background: var(--cyan); border-color: var(--cyan); box-shadow: 0 0 20px -4px var(--cyan-glow); } .btn-danger { color: #ff6b86; border-color: rgba(255,71,105,0.28); background: var(--red-soft); } .btn-danger:hover { color: #fff; background: var(--red); border-color: var(--red); } .btn-violet { color: var(--violet); border-color: rgba(167,139,250,0.28); background: rgba(167,139,250,0.12); } .btn-violet:hover { color: #0f0722; background: var(--violet); border-color: var(--violet); } .btn-warn { color: var(--amber); border-color: rgba(245,158,11,0.28); background: rgba(245,158,11,0.12); }  #dashboard-section { width: 100\%; max-width: 1480px; margin: 0 auto; padding: 0 24px 70px; } .topbar { display: flex; align-items: center; justify-content: space-between; padding: 20px 2px 14px; flex-wrap: wrap; gap: 14px; } .brand { display: flex; align-items: center; gap: 10px; } .brand-badge { width: 28px; height: 28px; border-radius: var(--r-xs); background: var(--cyan-soft); border: 1px solid rgba(45,226,208,0.35); display: flex; align-items: center; justify-content: center; color: var(--cyan); box-shadow: 0 0 16px -4px var(--cyan-glow); } .brand b { font-family: var(--mono); font-weight: 700; letter-spacing: 0.22em; font-size: 15px; color: var(--text); } .brand span { font-family: var(--mono); color: var(--faint); font-size: 11.5px; letter-spacing: 0.12em; } .status-pill { display: inline-flex; align-items: center; gap: 8px; background: var(--surface-2); border: 1px solid var(--line-2); padding: 6px 12px; border-radius: var(--r-xs); font-family: var(--mono); font-size: 10.5px; text-transform: uppercase; color: var(--muted); cursor: pointer; transition: 0.15s; } .status-pill:hover { border-color: var(--cyan); color: var(--text); } .status-dot { width: 7px; height: 7px; border-radius: 50\%; background: var(--cyan); box-shadow: 0 0 8px var(--cyan); animation: pulse 2.4s infinite; } .status-dot.offline { background: var(--faint); box-shadow: none; animation: none; } .status-dot.live { background: var(--emerald); box-shadow: 0 0 8px var(--emerald); } @keyframes pulse { 0\% { box-shadow: 0 0 0 0 rgba(45,226,208,0.5); } 70\% { box-shadow: 0 0 0 6px rgba(45,226,208,0); } 100\% { box-shadow: 0 0 0 0 rgba(45,226,208,0); } }  .sticky-header { position: sticky; top: 0; z-index: 100; padding: 8px 0 14px; background: linear-gradient(180deg, var(--void) 65\%, transparent); } .deck { background: var(--surface-glass); backdrop-filter: blur(18px); border: 1px solid var(--line-2); border-radius: var(--r-md); box-shadow: 0 20px 50px -20px rgba(0,0,0,0.85); overflow: hidden; } .deck-row { display: flex; align-items: center; gap: 12px; padding: 12px 16px; flex-wrap: wrap; } .deck-row + .deck-row { border-top: 1px solid var(--line); background: rgba(7,9,16,0.45); }  .search-wrap { position: relative; flex: 1; min-width: 200px; } .search-wrap svg { position: absolute; left: 12px; top: 50\%; transform: translateY(-50\%); color: var(--faint); pointer-events: none; } #search-input, #tt-username-input, #tt-quick-block-input { padding: 9px 13px 9px 36px; background: var(--void); color: var(--text); border: 1px solid var(--line-2); border-radius: var(--r-xs); font-family: var(--sans); font-size: 13px; outline: none; transition: 0.15s; width: 100\%; } #tt-username-input, #tt-quick-block-input { padding-left: 12px; font-family: var(--mono); font-size: 12px; width: 220px; flex: none; } #tt-quick-block-input { width: 195px; } #search-input:focus, #tt-username-input:focus, #tt-quick-block-input:focus { border-color: var(--cyan); box-shadow: 0 0 0 3px var(--cyan-soft); }  .toggle { display: inline-flex; align-items: center; gap: 9px; cursor: pointer; font-family: var(--mono); font-size: 10.5px; text-transform: uppercase; color: var(--muted); padding: 4px 8px; transition: 0.15s; } .toggle input { position: absolute; opacity: 0; } .track { width: 34px; height: 18px; border-radius: 10px; background: var(--void); border: 1px solid var(--line-2); position: relative; transition: 0.18s; } .track::after { content: ""; position: absolute; top: 2px; left: 2px; width: 12px; height: 12px; border-radius: 50\%; background: var(--faint); transition: 0.18s; } .toggle input:checked + .track { background: var(--cyan-soft); border-color: var(--cyan); } .toggle input:checked + .track::after { transform: translateX(16px); background: var(--cyan); box-shadow: 0 0 6px var(--cyan); }  .deck-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; } .divider { width: 1px; height: 20px; background: var(--line-2); margin: 0 4px; } .auto-status { display: flex; align-items: center; gap: 8px; margin-left: auto; font-family: var(--mono); font-size: 10.5px; color: var(--faint); background: var(--void); border: 1px solid var(--line); padding: 5px 11px; border-radius: var(--r-xs); } .auto-status.is-live { color: var(--cyan); border-color: rgba(45,226,208,0.28); }  #tt-metrics-bar { display: grid; grid-template-columns: repeat(auto-fit, minmax(185px, 1fr)); gap: 10px; margin-top: 12px; } .metric-tile { background: linear-gradient(180deg, var(--surface-2), var(--surface)); border: 1px solid var(--line-2); border-radius: 6px; padding: 10px 14px; display: flex; align-items: center; justify-content: space-between; } .metric-tile .m-label { font-family: var(--mono); font-size: 10px; letter-spacing: 0.1em; text-transform: uppercase; color: var(--muted); } .metric-tile .m-val { font-family: var(--mono); font-size: 14px; font-weight: 700; color: var(--cyan); }  #requests-container { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 14px; margin-top: 18px; } #requests-container.compact-mode { grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 10px; } .user-card { background: linear-gradient(180deg, var(--surface-2), var(--surface)); border: 1px solid var(--line-2); border-radius: 6px; padding: 22px 16px 16px; display: flex; flex-direction: column; align-items: center; text-align: center; transition: 0.15s; position: relative; } #requests-container.compact-mode .user-card { flex-direction: row; text-align: left; padding: 12px 14px; gap: 12px; } .user-card:hover { border-color: rgba(45,226,208,0.38); transform: translateY(-2px); box-shadow: 0 10px 20px -10px rgba(0,0,0,0.8); }  .tt-share-pill { font-family: var(--mono); font-size: 9.5px; font-weight: 700; color: var(--cyan); background: var(--cyan-soft); border: 1px solid rgba(45,226,208,0.32); padding: 2px 7px; border-radius: 3px; position: absolute; top: 10px; right: 10px; } .tt-time-pill { font-family: var(--mono); font-size: 9px; color: var(--faint); position: absolute; top: 12px; left: 11px; } .pfp-wrap { width: 74px; height: 74px; margin-bottom: 12px; flex: none; } #requests-container.compact-mode .pfp-wrap { width: 46px; height: 46px; margin-bottom: 0; } .pfp { width: 100\%; height: 100\%; border-radius: 10px; background: var(--void); border: 1px solid var(--line-2); padding: 2px; object-fit: cover; } .names { width: 100\%; margin-bottom: 14px; overflow: hidden; } #requests-container.compact-mode .names { margin-bottom: 0; flex: 1; } .names h2 { margin: 0; font-size: 14.5px; font-weight: 600; color: var(--text); text-overflow: ellipsis; overflow: hidden; white-space: nowrap; } .names p { margin: 4px 0 0; font-family: var(--mono); font-size: 11px; color: var(--muted); cursor: pointer; } .actions { display: flex; gap: 8px; width: 100\%; } #requests-container.compact-mode .actions { width: auto; flex: none; } .actions .btn { flex: 1; padding: 8px 10px; font-size: 10px; }  #empty-state { display: none; flex-direction: column; align-items: center; justify-content: center; text-align: center; padding: 84px 20px; margin-top: 18px; border: 1px dashed var(--line-2); border-radius: var(--r-md); color: var(--muted); } #empty-state h3 { margin: 0 0 6px; font-family: var(--mono); font-size: 13.5px; color: var(--text); }  .modal { display: none; position: fixed; inset: 0; z-index: 1000; background: rgba(3,4,8,0.84); backdrop-filter: blur(8px); justify-content: center; align-items: center; padding: 20px; } .modal-content { width: 100\%; max-width: 580px; background: var(--surface-2); border: 1px solid var(--line-2); border-top: 2px solid var(--cyan); border-radius: var(--r-md); padding: 24px 26px; } .modal-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; } .modal-head h2 { margin: 0; font-size: 16px; font-family: var(--mono); } .modal-close { background: none; border: 0; color: var(--muted); font-size: 16px; cursor: pointer; } textarea.modal-ta { width: 100\%; height: 260px; background: var(--void); color: var(--cyan); border: 1px solid var(--line-2); padding: 13px; font-family: var(--mono); font-size: 12px; margin-bottom: 16px; resize: vertical; outline: none; } .modal-ta:focus { border-color: var(--cyan); box-shadow: 0 0 0 3px var(--cyan-soft); } .modal-actions { display: flex; justify-content: space-between; align-items: center; }  #toast-container { position: fixed; bottom: 22px; right: 22px; display: flex; flex-direction: column; gap: 10px; z-index: 1100; max-width: 370px; } .toast { display: flex; align-items: flex-start; gap: 11px; background: rgba(15,19,32,0.96); backdrop-filter: blur(10px); border: 1px solid var(--line-2); border-left: 3px solid var(--cyan); border-radius: var(--r-xs); padding: 12px 15px; font-size: 12.5px; animation: slideIn 0.25s both; transition: 0.25s; } .toast.error { border-left-color: var(--red); } @keyframes slideIn { from { transform: translateX(110\%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }  #job-progress { position: fixed; left: 50\%; bottom: 22px; transform: translateX(-50\%); width: min(540px, calc(100vw - 32px)); background: rgba(15,19,32,0.96); backdrop-filter: blur(14px); border: 1px solid var(--line-2); border-left: 3px solid var(--cyan); border-radius: var(--r-sm); padding: 15px 18px; z-index: 1200; } .jp-head { display: flex; justify-content: space-between; margin-bottom: 10px; font-family: var(--mono); font-size: 11px; font-weight: 600; text-transform: uppercase; } .jp-track { height: 5px; background: var(--void); border-radius: 3px; overflow: hidden; } .jp-fill { height: 100\%; width: 0\%; background: var(--cyan); transition: width 0.2s linear; } .jp-meta { margin-top: 9px; font-family: var(--mono); font-size: 10.5px; color: var(--muted); } </style> </head> <body>  <div id="dashboard-section"> <div class="topbar"> <div class="brand"> <div class="brand-badge">◈</div> <b>NEXUS</b> <span>// TIKTOK TERMINAL</span> </div> <div class="telemetry-bar"> <div class="status-pill" onclick="openTikTokDiagnosticsModal()"> <span class="status-dot offline" id="top-tt-dot"></span> <span id="top-tt-text">TikTok: Standby</span> </div> </div> </div>  <div class="sticky-header"> <div class="deck"> <div class="deck-row"> <div class="search-wrap"> <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg> <input type="text" id="search-input" placeholder="Filter queue by @username…" oninput="handleFilterChange()"> </div>  <label class="toggle" title="Toggle Compact Tactical Rows"> <input type="checkbox" id="compact-view-toggle" onchange="toggleCompactMode()"> <span class="track"></span> Compact </label> </div>  <div class="deck-row deck-actions"> <input type="text" id="tt-username-input" placeholder="@tiktok_streamer (LIVE)..." onkeydown="if(event.key==='Enter')connectTikTokLive()"> <button class="btn btn-primary" onclick="connectTikTokLive()" id="btn-tt-connect">● Listen Live</button> <button class="btn btn-danger" onclick="disconnectTikTokLive()" id="btn-tt-disconnect">Stop</button>  <span class="divider"></span> <label class="toggle" title="Auto-block users who share 2x or more"> <input type="checkbox" id="tt-autoblock-toggle" onchange="toggleTikTokAutoBlock()"> <span class="track"></span> Auto-Block (2x+) </label> <button class="btn btn-danger" onclick="blockAllTikTokSharers()">Fast-Block All</button> <button class="btn btn-violet" onclick="openTikTokBlacklistModal()">Blacklist (<span id="tt-bl-count">0</span>)</button> <button class="btn" onclick="openTikTokLoginModal()" id="btn-tt-login">TikTok Account</button>  <span class="divider"></span> <input type="text" id="tt-quick-block-input" placeholder="Quick block @user..." onkeydown="if(event.key==='Enter')quickBlockTikTokInput()"> <button class="btn btn-danger" onclick="quickBlockTikTokInput()">Block @</button>  <span class="divider"></span> <button class="btn" onclick="copyAllTikTokSharers()">Copy @Users</button> <button class="btn btn-warn" onclick="clearTikTokSharers()">Clear Log</button>  <div class="auto-status" id="tt-live-status" style="margin-left:auto;"> <span class="auto-dot" id="tt-live-dot"></span> <span id="tt-live-status-text">Listener offline</span> </div> </div> </div>  <div id="tt-metrics-bar"> <div class="metric-tile"><span class="m-label">Active Sharers</span><span class="m-val" id="tt-metric-visible">0</span></div> <div class="metric-tile"><span class="m-label">Total Share Events</span><span class="m-val emerald" id="tt-metric-total-shares">0</span></div> <div class="metric-tile"><span class="m-label">Blacklisted Count</span><span class="m-val violet" id="tt-metric-blacklisted">0</span></div> </div> </div>  <div id="requests-container"></div>  <div id="empty-state"> <div class="es-glyph">◇</div> <h3 id="empty-title">No live shares collected yet</h3> <p id="empty-body">Enter your TikTok @username above and click Listen Live.</p> </div> </div>  <!-- TIKTOK BLACKLIST MODAL --> <div id="tt-blacklist-modal" class="modal"> <div class="modal-content"> <div class="modal-head"> <h2>TikTok Username Blacklist</h2> <button class="modal-close" onclick="closeTikTokBlacklistModal()">✕</button> </div> <p style="font-size:12px; color:var(--muted); margin-top:0;">One username per line (without @). Saved usernames are mass-blocked.</p> <textarea id="tt-blacklist-textarea" class="modal-ta" placeholder="username&#10;username1"></textarea> <div class="modal-actions"> <button class="btn btn-danger" onclick="blockAllListedTikTokUsers()">⚡ Fast-Block All Listed</button> <div style="display:flex; gap:8px;"> <button class="btn" onclick="closeTikTokBlacklistModal()">Close</button> <button class="btn btn-primary" onclick="saveTikTokBlacklistText()">Save Blacklist</button> </div> </div> </div> </div>  <!-- TIKTOK LOGIN MODAL --> <div id="tt-login-modal" class="modal"> <div class="modal-content"> <div class="modal-head"> <h2>TikTok Account Session</h2> <button class="modal-close" onclick="closeTikTokLoginModal()">✕</button> </div> <p style="font-size:12px; color:var(--muted); margin-top:0;">Connect your TikTok session cookie so NEXUS can auto-block users.</p> <input type="text" id="tt-account-name-input" placeholder="@your_handle" style="width:100\%; padding:10px; background:var(--void); border:1px solid var(--line-2); color:var(--text); margin-bottom:14px; outline:none;"> <input type="password" id="tt-cookie-input" placeholder="Paste full Cookie header or sessionid..." style="width:100\%; padding:10px; background:var(--void); border:1px solid var(--line-2); color:var(--cyan); margin-bottom:14px; outline:none;"> <div class="modal-actions"> <button class="btn btn-danger" onclick="logoutTikTokAccount()">Disconnect Account</button> <div style="display:flex; gap:8px;"> <button class="btn" onclick="closeTikTokLoginModal()">Cancel</button> <button class="btn btn-primary" onclick="saveTikTokLogin()">Save &amp; Connect</button> </div> </div> </div> </div>  <!-- TIKTOK DIAGNOSTICS MODAL --> <div id="tt-diag-modal" class="modal"> <div class="modal-content" style="max-width: 760px;"> <div class="modal-head"> <h2>TikTok Diagnostics</h2> <button class="modal-close" onclick="closeTikTokDiagnosticsModal()">✕</button> </div> <p id="tt-diag-summary" style="font-size:12px; color:var(--muted); margin-top:0;"></p> <div style="overflow-y:auto; max-height:360px; border:1px solid var(--line-2); background:var(--void); margin-bottom:16px;"> <table style="width:100\%; border-collapse:collapse; font-family:var(--mono); font-size:11px;" id="tt-diag-table"> <thead style="background:var(--surface-2); color:var(--muted); position:sticky; top:0;"> <tr><th style="padding:8px; text-align:left;">Time</th><th style="padding:8px; text-align:left;">User</th><th style="padding:8px; text-align:left;">ID</th><th style="padding:8px; text-align:left;">Vector</th><th style="padding:8px; text-align:left;">Result</th></tr> </thead> <tbody id="tt-diag-tbody"></tbody> </table> </div> <div class="modal-actions"> <button class="btn" onclick="refreshTikTokDiagnostics()">↻ Refresh</button> <button class="btn btn-primary" onclick="closeTikTokDiagnosticsModal()">Close</button> </div> </div> </div>  <div id="toast-container"></div>  <script> // Automatically use the current window's URL for the backend connection const BACKEND_URL = window.location.origin;  const container = document.getElementById('requests-container'); const searchInput = document.getElementById('search-input'); const compactViewToggle = document.getElementById('compact-view-toggle'); const ttBlacklistModal = document.getElementById('tt-blacklist-modal'); const ttBlacklistTa = document.getElementById('tt-blacklist-textarea'); const ttLoginModal = document.getElementById('tt-login-modal'); const ttDiagModal = document.getElementById('tt-diag-modal'); const emptyState = document.getElementById('empty-state');  let visibleIds = []; let searchDebounce = null;  let ttSharersMap = new Map(); let ttBlacklistSet = new Set(); let ttEventSource = null; let ttPollTimer = null; let ttState = { connected: false, username: null, error: null }; let ttAuth = { loggedIn: false, accountName: '', autoBlockSharers: false }; let activeTikTokJobId = null; let ttPrewarmDebounce = null;  const esc = (s) => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));  function showToast(message, type = 'info') { const toast = document.createElement('div'); toast.className = `toast ${type}`;
  const glyph = { error:'✕', success:'✓', warn:'!', info:'›' }[type] || '›';
  toast.innerHTML = `<span class="toast-glyph">${glyph}</span><span>${esc(message)}</span>`;
  document.getElementById('toast-container').appendChild(toast);
  setTimeout(() => { toast.style.opacity = '0'; setTimeout(() => toast.remove(), 300); }, 3600);
}

function toggleCompactMode() {
  const on = compactViewToggle.checked;
  localStorage.setItem('nexusCompactMode', on ? '1' : '0');
  container.classList.toggle('compact-mode', on);
}

function computeVisible() {
  const term = searchInput.value.toLowerCase().trim();
  const allSharers = Array.from(ttSharersMap.values()).filter(u => !ttBlacklistSet.has(u.uniqueId.toLowerCase())).sort((a,b) => b.timestamp - a.timestamp);
  visibleIds = term ? allSharers.filter(u => u.uniqueId.toLowerCase().includes(term)).map(u => u.uniqueId) : allSharers.map(u => u.uniqueId);
}

function setTargetCount() {
  const unblocked = Array.from(ttSharersMap.values()).filter(u => !ttBlacklistSet.has(u.uniqueId.toLowerCase())).length;
  const totalShares = Array.from(ttSharersMap.values()).reduce((acc, u) => acc + (Number(u.shareCount)||1), 0);
  document.getElementById('tt-tab-count').innerText = unblocked;
  document.getElementById('tt-bl-count').innerText = ttBlacklistSet.size;
  document.getElementById('tt-metric-visible').innerText = unblocked;
  document.getElementById('tt-metric-total-shares').innerText = totalShares;
  document.getElementById('tt-metric-blacklisted').innerText = ttBlacklistSet.size;
}

function handleFilterChange() {
  clearTimeout(searchDebounce);
  searchDebounce = setTimeout(render, 100);
}

function queueRender() {
  requestAnimationFrame(render);
}

function render() {
  computeVisible();
  setTargetCount();
  if (visibleIds.length === 0) {
    container.style.display = 'none';
    emptyState.style.display = 'flex';
    return;
  }
  container.style.display = 'grid';
  emptyState.style.display = 'none';
  const frag = document.createDocumentFragment();
  for (const id of visibleIds) frag.appendChild(buildTikTokCard(id));
  container.replaceChildren(frag);
}

function buildTikTokCard(uniqueId) {
  const u = ttSharersMap.get(uniqueId);
  const card = document.createElement('div');
  card.className = 'user-card';
  card.innerHTML = `
    <span class="tt-time-pill">${esc(u.lastSharedAt || '')}</span>
    <span class="tt-share-pill">${u.shareCount}× SHARED</span>
    <div class="pfp-wrap"><img src="${esc(u.profilePictureUrl)}" class="pfp" loading="lazy"></div>
    <div class="names">
      <h2>${esc(u.nickname)}</h2>
      <p onclick="copySingleTikTokUser('${esc(u.uniqueId)}')">@${esc(u.uniqueId)}</p>
    </div>
    <div class="actions">
      <button class="btn btn-accept" onclick="copySingleTikTokUser('${esc(u.uniqueId)}')">Copy</button>
      <button class="btn btn-reject" onclick="blockSingleTikTokUser('${esc(u.uniqueId)}')">Block</button>
    </div>`;
  return card;
}

function initTikTokSSE() {
  if (ttEventSource) ttEventSource.close();
  const savedUser = localStorage.getItem('nexusTikTokUser');
  if (savedUser) document.getElementById('tt-username-input').value = savedUser;

  fetchTikTokSnapshot();
  if (!ttPollTimer) ttPollTimer = setInterval(fetchTikTokSnapshot, 2000);

  ttEventSource = new EventSource(`${BACKEND_URL}/api/tiktok/stream`);
  ttEventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      if (data.type === 'INIT') {
        ttState = data.state || { connected: false };
        if (data.auth) ttAuth = data.auth;
        if (data.blacklist) ttBlacklistSet = new Set(data.blacklist.map(u => u.toLowerCase()));
        ttSharersMap.clear();
        (data.sharers || []).forEach(u => ttSharersMap.set(u.uniqueId, u));
        updateTikTokStatusUI(); queueRender();
      } else if (data.type === 'STATUS') {
        ttState = data.state || { connected: false }; updateTikTokStatusUI();
      } else if (data.type === 'AUTH') {
        if (data.auth) ttAuth = data.auth; updateTikTokStatusUI(); queueRender();
      } else if (data.type === 'BLACKLIST') {
        if (data.blacklist) ttBlacklistSet = new Set(data.blacklist.map(u => u.toLowerCase())); queueRender();
      } else if (data.type === 'SHARE') {
        ttSharersMap.set(data.user.uniqueId, data.user); queueRender();
      } else if (data.type === 'CLEAR') {
        ttSharersMap.clear(); queueRender();
      }
    } catch (e) {}
  };
}

async function fetchTikTokSnapshot() {
  try {
    const res = await fetch(`${BACKEND_URL}/api/tiktok/sharers`); if (!res.ok) return; const data = await res.json(); if (data.state) ttState = data.state; if (data.auth) ttAuth = data.auth; if (data.blacklist) ttBlacklistSet = new Set(data.blacklist.map(u => u.toLowerCase())); updateTikTokStatusUI(); if (Array.isArray(data.sharers)) { ttSharersMap.clear(); data.sharers.forEach(u => ttSharersMap.set(u.uniqueId, u)); queueRender(); } } catch (e) {} }  function updateTikTokStatusUI() { const wrap = document.getElementById('tt-live-status'); const txt = document.getElementById('tt-live-status-text'); const loginBtn = document.getElementById('btn-tt-login'); const topDot = document.getElementById('top-tt-dot'); const topText = document.getElementById('top-tt-text');  if (document.getElementById('tt-autoblock-toggle')) document.getElementById('tt-autoblock-toggle').checked = !!ttAuth.autoBlockSharers;  if (ttAuth.loggedIn) { loginBtn.innerText = `TT: @${ttAuth.accountName||'Connected'}`; loginBtn.classList.add('btn-primary'); } 
  else { loginBtn.innerText = 'TikTok Account'; loginBtn.classList.remove('btn-primary'); }

  if (ttState.connected) { topDot.className = 'status-dot live'; topText.innerText = `Live: @${ttState.username}`; } else if (ttAuth.loggedIn) { topDot.className = 'status-dot'; topText.innerText = `Auth: @${ttAuth.accountName}`; }
  else { topDot.className = 'status-dot offline'; topText.innerText = 'TikTok: Standby'; }

  wrap.classList.toggle('is-live', !!ttState.connected);
  if (ttState.connected) txt.innerText = `Listening to @${ttState.username}`; else if (ttState.error) txt.innerText = `Error: ${ttState.error}`;
  else txt.innerText = 'Listener offline';
}

async function connectTikTokLive() {
  const username = document.getElementById('tt-username-input').value.trim();
  if (!username) return showToast('Enter a TikTok @username first.', 'warn');
  localStorage.setItem('nexusTikTokUser', username);
  showToast(`Connecting to @${username.replace(/^@/, '')}...`, 'info'); try { const res = await fetch(`${BACKEND_URL}/api/tiktok/connect`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username }) });
    const data = await res.json();
    if (!res.ok) { ttState = { connected: false, error: data.error }; updateTikTokStatusUI(); return showToast(data.error, 'error'); }
    ttState = data.state; updateTikTokStatusUI();
    showToast(`Listening for shares on @${ttState.username}!`, 'success');
  } catch (e) { showToast('Could not reach backend.', 'error'); }
}

async function disconnectTikTokLive() {
  try {
    await fetch(`${BACKEND_URL}/api/tiktok/disconnect`, { method: 'POST' }); ttState = { connected: false, username: null, error: null }; updateTikTokStatusUI(); showToast('Stopped listener.', 'info'); } catch (e) {} }  async function clearTikTokSharers() { if (ttSharersMap.size === 0 \vert{}\vert{} !confirm('Clear log?')) return; try { await fetch(`${BACKEND_URL}/api/tiktok/clear`, { method: 'POST' }); ttSharersMap.clear(); queueRender(); showToast('Log cleared.', 'success'); } catch (e) {}
}

function copyAllTikTokSharers() {
  if (!visibleIds.length) return showToast('No users to copy.', 'warn');
  navigator.clipboard?.writeText(visibleIds.join('\n'));
  showToast(`Copied ${visibleIds.length} users!`, 'success'); } function copySingleTikTokUser(id) { navigator.clipboard?.writeText(id); showToast(`Copied @${id}`, 'success'); }

// ---- BLOCKING SYSTEM ----

function ensureProgressBar() {
  let bar = document.getElementById('job-progress'); if (bar) return bar;
  bar = document.createElement('div'); bar.id = 'job-progress';
  bar.innerHTML = `<div class="jp-head"><span id="jp-label">Working…</span><button id="jp-cancel" class="btn btn-danger">Cancel</button></div><div class="jp-track"><div id="jp-fill" class="jp-fill"></div></div><div id="jp-meta" class="jp-meta"></div>`;
  document.body.appendChild(bar); return bar;
}
function hideProgress() { const b = document.getElementById('job-progress'); if (b) b.remove(); }

async function cancelTikTokJob() {
  if (!activeTikTokJobId) return;
  try { await fetch(`${BACKEND_URL}/api/tiktok/jobs/${activeTikTokJobId}`, { method: 'DELETE' }); showToast('Stopping job…', 'info'); } catch (e) {}
}

async function runTikTokBlockJobWithProgress(usernames) {
  if (activeTikTokJobId) return showToast('Job already running.', 'warn');
  try {
    usernames.forEach(u => ttBlacklistSet.add(u.toLowerCase())); queueRender();
    const res = await fetch(`${BACKEND_URL}/api/tiktok/jobs/block`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ usernames }) }); const job = await res.json(); if (!res.ok) return showToast(job.error, 'error'); activeTikTokJobId = job.id; showToast(`Fast-blocking ${job.total} users...`, 'info');
    const poll = setInterval(async () => {
      try {
        const r = await fetch(`${BACKEND_URL}/api/tiktok/jobs/${job.id}`);
        if (!r.ok) { clearInterval(poll); activeTikTokJobId = null; return; }
        const state = await r.json();
        const bar = ensureProgressBar();
        document.getElementById('jp-cancel').onclick = cancelTikTokJob;
        document.getElementById('jp-label').innerText = `Blocking ${state.processed}/${state.total}`;
        document.getElementById('jp-fill').style.width = `${Math.round((state.processed/Math.max(1,state.total))*100)}\%`; document.getElementById('jp-meta').innerText = `${state.blockedOnTikTokCount} blocked · ${state.failedCount} failed`; if (state.status !== 'running') { clearInterval(poll); activeTikTokJobId = null; setTimeout(hideProgress, 2500); fetchTikTokSnapshot(); showToast(`Blocked ${state.blockedOnTikTokCount}/${state.total} users!`, state.blockedOnTikTokCount > 0 ? 'success' : 'warn'); } } catch (e) { clearInterval(poll); activeTikTokJobId = null; } }, 250); } catch (e) { showToast('Backend error.', 'error'); } }  async function blockSingleTikTokUser(uname) { ttBlacklistSet.add(uname.toLowerCase()); queueRender(); try { const res = await fetch(`${BACKEND_URL}/api/tiktok/block`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ usernames: [uname] }) });
    const data = await res.json();
    if (res.ok && data.success) showToast(data.blockedOnTikTokCount > 0 ? `Blocked @${uname}` : `Blacklisted @${uname}`, data.blockedOnTikTokCount > 0 ? 'success' : 'info');
  } catch (e) {}
}

async function quickBlockTikTokInput() {
  const inp = document.getElementById('tt-quick-block-input');
  const uname = inp.value.trim().replace(/^@+/, '');
  if (!uname) return showToast('Enter a username.', 'warn');
  inp.value = ''; await blockSingleTikTokUser(uname);
}

async function blockAllTikTokSharers() {
  if (visibleIds.length === 0) return showToast('No users in queue.', 'warn');
  if (confirm(`Block all ${visibleIds.length} sharers?`)) await runTikTokBlockJobWithProgress([...visibleIds]); }  async function toggleTikTokAutoBlock() { const enabled = document.getElementById('tt-autoblock-toggle').checked; try { await fetch(`${BACKEND_URL}/api/tiktok/auto-block`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled }) });
    ttAuth.autoBlockSharers = enabled; setTargetCount();
    showToast(enabled ? 'Auto-Block (2x) ON.' : 'Auto-Block OFF.', 'info');
  } catch (e) {}
}

// ---- MODALS ----
async function openTikTokBlacklistModal() {
  ttBlacklistModal.style.display = 'flex';
  try {
    const res = await fetch(`${BACKEND_URL}/api/tiktok/blacklist`); const data = await res.json(); ttBlacklistTa.value = data.text \vert{}\vert{} ''; } catch (e) { ttBlacklistTa.value = Array.from(ttBlacklistSet).join('\n'); } } function closeTikTokBlacklistModal() { ttBlacklistModal.style.display = 'none'; } async function saveTikTokBlacklistText() { try { const res = await fetch(`${BACKEND_URL}/api/tiktok/blacklist/save`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text: ttBlacklistTa.value }) });
    const data = await res.json();
    if (res.ok && data.success) {
      ttBlacklistSet = new Set((data.usernames || []).map(u => u.toLowerCase())); queueRender();
      showToast(`Saved ${data.total} usernames.`, 'success'); closeTikTokBlacklistModal(); } } catch (e) {} } async function blockAllListedTikTokUsers() { const lines = ttBlacklistTa.value.split(/\r?\n/).map(s => s.trim().replace(/^@/, '')).filter(Boolean); if (lines.length === 0) return; if (!ttAuth.loggedIn) return showToast('Log in first.', 'warn'); closeTikTokBlacklistModal(); await runTikTokBlockJobWithProgress(lines); }  function openTikTokLoginModal() { ttLoginModal.style.display = 'flex'; document.getElementById('tt-account-name-input').value = localStorage.getItem('nexusTikTokAccountName') \vert{}\vert{} ttAuth.accountName \vert{}\vert{} ''; document.getElementById('tt-cookie-input').value = localStorage.getItem('nexusTikTokCookie') \vert{}\vert{} ''; } function closeTikTokLoginModal() { ttLoginModal.style.display = 'none'; } async function saveTikTokLogin() { const accountName = document.getElementById('tt-account-name-input').value.trim(); const cookie = document.getElementById('tt-cookie-input').value.trim(); if (!cookie) return showToast('Paste cookie first.', 'warn'); localStorage.setItem('nexusTikTokAccountName', accountName); localStorage.setItem('nexusTikTokCookie', cookie); try { const res = await fetch(`${BACKEND_URL}/api/tiktok/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accountName, cookie }) });
    const data = await res.json();
    if (!res.ok) return showToast(data.error, 'error');
    ttAuth = data.auth; updateTikTokStatusUI(); closeTikTokLoginModal(); showToast('Logged In!', 'success');
  } catch (e) {}
}
async function logoutTikTokAccount() {
  localStorage.removeItem('nexusTikTokCookie'); localStorage.removeItem('nexusTikTokAccountName');
  try { await fetch(`${BACKEND_URL}/api/tiktok/logout`, { method: 'POST' }); ttAuth.loggedIn = false; updateTikTokStatusUI(); closeTikTokLoginModal(); } catch (e) {} }  async function openTikTokDiagnosticsModal() { ttDiagModal.style.display = 'flex'; await refreshTikTokDiagnostics(); } function closeTikTokDiagnosticsModal() { ttDiagModal.style.display = 'none'; } async function refreshTikTokDiagnostics() { try { const res = await fetch(`${BACKEND_URL}/api/tiktok/diagnostics`);
    const data = await res.json();
    const tbody = document.getElementById('tt-diag-tbody');
    if (!data.recentAttempts?.length) { tbody.innerHTML = '<tr><td colspan="5">No blocks yet.</td></tr>'; return; }
    tbody.innerHTML = data.recentAttempts.map(r => `<tr>
      <td style="padding:8px; border-bottom:1px solid var(--line);">${new Date(r.timestamp).toLocaleTimeString()}</td>
      <td style="padding:8px; border-bottom:1px solid var(--line); color:var(--cyan);">@${esc(r.username)}</td>
      <td style="padding:8px; border-bottom:1px solid var(--line);">${esc(r.userId)}</td>
      <td style="padding:8px; border-bottom:1px solid var(--line);">${esc(r.vector)}</td>
      <td style="padding:8px; border-bottom:1px solid var(--line); color:${r.success?'var(--emerald)':'var(--red)'};">${r.success?'BLOCKED':esc(r.note)}</td>
    </tr>`).join('');
  } catch (e) {}
}

if (ttBlacklistTa) {
  ttBlacklistTa.addEventListener('input', () => {
    clearTimeout(ttPrewarmDebounce);
    ttPrewarmDebounce = setTimeout(() => {
      const lines = ttBlacklistTa.value.split(/\r?\n/).map(s => s.trim().replace(/^@+/, '')).filter(Boolean);
      if (lines.length) fetch(`${BACKEND_URL}/api/tiktok/prewarm`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ usernames: lines }) }).catch(()=>{}); }, 300); }); }  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') { closeTikTokBlacklistModal(); closeTikTokLoginModal(); closeTikTokDiagnosticsModal(); } });  if (localStorage.getItem('nexusCompactMode') === '1') { compactViewToggle.checked = true; container.classList.add('compact-mode'); }  initTikTokSSE(); const savedCookie = localStorage.getItem('nexusTikTokCookie'); const savedName = localStorage.getItem('nexusTikTokAccountName') \vert{}\vert{} 'Account'; if (savedCookie) fetch(`${BACKEND_URL}/api/tiktok/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accountName: savedName, cookie: savedCookie }) }).then(r=>r.json()).then(d=>{if(d.auth) ttAuth = d.auth; updateTikTokStatusUI();}).catch(()=>{});

</script>
</body>
</html>