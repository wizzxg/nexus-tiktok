'use strict';

/**
 * NEXUS — TikTok LIVE share listener + fast account blocker.
 *
 * TikTok refuses block requests that aren't signed by the security script on its own pages,
 * which is why plain server-side requests always failed. This server keeps one hidden Chrome
 * tab open on tiktok.com per connected account and fires the block requests from inside it,
 * many at once, so TikTok's script signs every one. No page clicking in the normal case:
 * the profile page is only opened as a fallback if TikTok's request format changes.
 */

const path = require('path');

const express = require('express');
const crypto = require('crypto');
const fs = require('fs');
const cors = require('cors');

const DATA_DIR = process.env.DATA_DIR || __dirname;

const CONFIG = {
  port: Number(process.env.PORT) || 3000,
  corsOrigin: process.env.CORS_ORIGIN || '*',
  eulerApiKey: process.env.EULER_API_KEY || null,
  files: {
    blacklist: process.env.TT_BLACKLIST_PATH || path.join(DATA_DIR, 'tiktok_blacklist.txt'),
    profiles: path.join(DATA_DIR, 'tiktok_profiles.json'),
    ledger: path.join(DATA_DIR, 'tiktok_blocked.json'),
    templates: path.join(DATA_DIR, 'tiktok_block_template.json'),
  },
  // Low-memory mode (default) keeps everything inside ~512 MB: a single-process headless browser
  // that only runs while blocking. Set LOW_MEMORY=0 on bigger machines for a bit more speed.
  lowMemory: process.env.LOW_MEMORY !== '0',
  browser: {
    headless: process.env.HEADFUL !== '1',
    // Close the browser after this long without blocks (it reopens automatically when needed).
    idleCloseMs: Number(process.env.BROWSER_IDLE_MS) || 3 * 60 * 1000,
    // Slow hosts (e.g. Render Free, 0.1 CPU) need much longer to load TikTok's page.
    navTimeoutMs: Number(process.env.NAV_TIMEOUT_MS) || 90000,
  },
  block: {
    // Parallel block requests per account. Raise for speed, lower if TikTok starts rate-limiting.
    concurrency: Math.max(1, Math.min(24, Number(process.env.BLOCK_CONCURRENCY) || 10)),
    uiPages: process.env.LOW_MEMORY === '0' ? 2 : 1, // profile-page fallback tabs
    staggerMs: 25,
    uiReadyTimeoutMs: 45000, // wait for the profile page's buttons to start working
    uiDialogTimeoutMs: 15000,
    uiResponseTimeoutMs: 60000,
    uiAfterConfirmMs: 15000,
    uiFailuresBeforeStop: 2, // stop a job instead of grinding through the list when clicks don't work
    pauseOnLimitMs: 45000,
    maxStrikes: 3,
    apiFailuresBeforeUi: 4,
  },
  share: { dedupWindowMs: 1200, defaultThreshold: 2 },
  sessionIdleMs: 3 * 60 * 60 * 1000,
};

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';
const HANDLE_RE = /^[a-z0-9._]{2,24}$/;

// ============================================================================
// LOGGING & HELPERS
// ============================================================================
const ts = () => new Date().toISOString();
const log = {
  info: (...a) => console.log(`[${ts()}] [info]`, ...a),
  warn: (...a) => console.warn(`[${ts()}] [warn]`, ...a),
  error: (...a) => console.error(`[${ts()}] [error]`, ...a),
};
const delay = (ms) => new Promise((r) => setTimeout(r, ms));
const safeJson = (t) => {
  try {
    return JSON.parse(t);
  } catch {
    return null;
  }
};

function normalizeHandle(raw) {
  let s = String(raw || '').trim();
  const m = /tiktok\.com\/@([^/?#\s]+)/i.exec(s);
  if (m) s = m[1];
  return s.replace(/^@+/, '').trim().toLowerCase();
}
const isValidHandle = (h) => HANDLE_RE.test(h);

function cleanHandleList(input) {
  const arr = Array.isArray(input) ? input : String(input || '').split(/[\r\n,]+/);
  return [...new Set(arr.map(normalizeHandle).filter(isValidHandle))];
}

// ============================================================================
// PERSISTENCE
// ============================================================================
class JsonFile {
  constructor(file, fallback) {
    this.file = file;
    this.data = fallback;
    this.timer = null;
    try {
      if (fs.existsSync(file)) this.data = JSON.parse(fs.readFileSync(file, 'utf8')) || fallback;
    } catch (e) {
      log.warn(`Could not read ${path.basename(file)}, starting fresh:`, e.message);
    }
  }
  save() {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      const tmp = `${this.file}.tmp`;
      fs.promises
        .writeFile(tmp, JSON.stringify(this.data))
        .then(() => fs.promises.rename(tmp, this.file))
        .catch((e) => log.warn(`Write failed for ${path.basename(this.file)}:`, e.message));
    }, 400);
  }
}

class Blacklist {
  constructor(file) {
    this.file = file;
    this.set = new Set();
    this.chain = Promise.resolve();
    try {
      if (fs.existsSync(file)) {
        const raw = fs.readFileSync(file, 'utf8');
        const clean = cleanHandleList(raw);
        clean.forEach((h) => this.set.add(h));
        if (clean.length !== raw.split(/\r?\n/).filter((l) => l.trim()).length) this.persist();
      } else fs.writeFileSync(file, '');
    } catch (e) {
      log.warn('Blacklist load failed:', e.message);
    }
  }
  has(h) {
    return this.set.has(normalizeHandle(h));
  }
  add(list) {
    let n = 0;
    for (const h of cleanHandleList(list)) if (!this.set.has(h)) (this.set.add(h), n++);
    if (n) this.persist();
    return n;
  }
  replaceAll(list) {
    this.set = new Set(cleanHandleList(list));
    this.persist();
    return this.set.size;
  }
  persist() {
    const body = [...this.set].join('\n') + (this.set.size ? '\n' : '');
    this.chain = this.chain.then(() => fs.promises.writeFile(this.file, body)).catch(() => {});
  }
  all() {
    return [...this.set];
  }
}

const blacklist = new Blacklist(CONFIG.files.blacklist);
const profiles = new JsonFile(CONFIG.files.profiles, {}); // handle -> { userId, secUid, nickname, at }
const ledger = new JsonFile(CONFIG.files.ledger, {}); // accountUid -> { handle: blockedAt }
const templates = new JsonFile(CONFIG.files.templates, {}); // accountUid -> learned request

function rememberProfile(handle, info) {
  const h = normalizeHandle(handle);
  if (!isValidHandle(h)) return;
  const prev = profiles.data[h] || {};
  const next = {
    userId: String(info.userId || prev.userId || ''),
    secUid: String(info.secUid || prev.secUid || ''),
    nickname: info.nickname || prev.nickname || '',
    at: Date.now(),
  };
  if (!next.userId && !next.secUid) return;
  profiles.data[h] = next;
  const keys = Object.keys(profiles.data);
  if (keys.length > 60000) {
    keys
      .sort((a, b) => profiles.data[a].at - profiles.data[b].at)
      .slice(0, 10000)
      .forEach((k) => delete profiles.data[k]);
  }
  profiles.save();
}
const knownProfile = (h) => {
  const p = profiles.data[normalizeHandle(h)];
  return p && p.userId && p.secUid ? p : null;
};
const isInLedger = (uid, h) => Boolean(uid && ledger.data[uid]?.[h]);
function addToLedger(uid, h) {
  if (!uid) return;
  (ledger.data[uid] ||= {})[h] = Date.now();
  ledger.save();
}

// ============================================================================
// HEADLESS BROWSER ENGINE
// ============================================================================
// The server needs a Chromium-based browser on the machine it runs on (not the browser you
// use to open the dashboard). It looks for one in this order:
//   1. PUPPETEER_EXECUTABLE_PATH / CHROME_PATH env var
//   2. a Chrome that Puppeteer already downloaded
//   3. Chrome, Edge, Brave or Chromium installed on the machine
//   4. otherwise it downloads its own Chrome into ./.cache (first start only)
let puppeteer = null;
let browserPromise = null;
let browserError = null;
let browserPath = null;
let browserSetup = null;
let lastBrowserUse = Date.now();
const engineStatus = { state: 'idle', progress: 0, message: '' };

const BROWSER_CACHE = path.join(__dirname, '.cache', 'puppeteer');
const loadPuppeteer = () => (puppeteer ||= require('puppeteer'));
const loadBrowsers = () => require(require.resolve('@puppeteer/browsers', { paths: [path.dirname(require.resolve('puppeteer'))] }));

function setEngineStatus(state, message = '', progress = engineStatus.progress) {
  const changed = state !== engineStatus.state || message !== engineStatus.message || Math.abs(progress - engineStatus.progress) >= 5;
  Object.assign(engineStatus, { state, message, progress });
  if (!changed) return;
  for (const s of sessions.values()) s.broadcast({ type: 'ENGINE', engine: { ...engineStatus } });
}

function systemBrowserPaths() {
  const env = process.env;
  if (process.platform === 'win32') {
    const roots = [env.PROGRAMFILES || 'C:\\Program Files', env['PROGRAMFILES(X86)'] || 'C:\\Program Files (x86)', env.LOCALAPPDATA || ''];
    const rel = [
      'Google\\Chrome\\Application\\chrome.exe',
      'Microsoft\\Edge\\Application\\msedge.exe',
      'BraveSoftware\\Brave-Browser\\Application\\brave.exe',
      'Chromium\\Application\\chrome.exe',
    ];
    return rel.flatMap((r) => roots.filter(Boolean).map((root) => path.join(root, r)));
  }
  if (process.platform === 'darwin') {
    return [
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
      '/Applications/Brave Browser.app/Contents/MacOS/Brave Browser',
      '/Applications/Chromium.app/Contents/MacOS/Chromium',
    ];
  }
  return [
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/snap/bin/chromium',
    '/usr/bin/microsoft-edge',
    '/usr/bin/microsoft-edge-stable',
    '/usr/bin/brave-browser',
  ];
}

async function findDownloadedChrome() {
  const { getInstalledBrowsers } = loadBrowsers();
  const dirs = [
    process.env.PUPPETEER_CACHE_DIR,
    BROWSER_CACHE,
    path.join(path.dirname(require.resolve('puppeteer')), '..', '.cache', 'puppeteer'),
    path.join(require('os').homedir(), '.cache', 'puppeteer'),
  ].filter(Boolean);
  for (const cacheDir of [...new Set(dirs)]) {
    try {
      const found = (await getInstalledBrowsers({ cacheDir })).filter(
        (b) => (b.browser === 'chrome-headless-shell' || b.browser === 'chrome') && fs.existsSync(b.executablePath)
      );
      // The headless-only build uses noticeably less memory, so it wins when present.
      const rank = (b) => (b.browser === 'chrome-headless-shell' ? 1 : 0);
      found.sort((a, b) => rank(b) - rank(a) || String(b.buildId).localeCompare(String(a.buildId), undefined, { numeric: true }));
      if (found[0]) return found[0].executablePath;
    } catch {}
  }
  return null;
}

async function downloadChrome() {
  const { install, Browser, detectBrowserPlatform } = loadBrowsers();
  const revisions = loadPuppeteer().PUPPETEER_REVISIONS || require('puppeteer-core').PUPPETEER_REVISIONS;
  const browser = CONFIG.lowMemory ? Browser.CHROMEHEADLESSSHELL : Browser.CHROME;
  const buildId = CONFIG.lowMemory ? revisions['chrome-headless-shell'] : revisions.chrome;
  const platform = detectBrowserPlatform();
  if (!platform) throw new Error(`Chrome can't be downloaded for this system (${process.platform}/${process.arch}). Install Chrome or Chromium on the server.`);
  log.info(`No browser found on the server — downloading Chrome ${buildId} into .cache (first start only)…`);
  setEngineStatus('downloading', 'Downloading Chrome for the server (first start only)…', 0);
  const installed = await install({
    browser,
    buildId,
    platform,
    cacheDir: BROWSER_CACHE,
    downloadProgressCallback: (done, total) => {
      if (total) setEngineStatus('downloading', 'Downloading Chrome for the server (first start only)…', Math.round((done / total) * 100));
    },
  });
  log.info('Chrome downloaded.');
  return installed.executablePath;
}

async function resolveBrowserPath() {
  if (browserPath && fs.existsSync(browserPath)) return browserPath;
  if (browserSetup) return browserSetup;
  browserSetup = (async () => {
    setEngineStatus('finding', 'Looking for a browser on the server…', 0);
    const fromEnv = [process.env.PUPPETEER_EXECUTABLE_PATH, process.env.CHROME_PATH].find((p) => p && fs.existsSync(p));
    const downloaded = await findDownloadedChrome();
    const preferShell = CONFIG.lowMemory && downloaded && /headless-shell/i.test(downloaded);
    const found =
      fromEnv ||
      (preferShell ? downloaded : null) ||
      downloaded ||
      systemBrowserPaths().find((p) => {
        try {
          return fs.existsSync(p);
        } catch {
          return false;
        }
      }) ||
      (await downloadChrome());
    browserPath = found;
    log.info(`Using browser: ${found}`);
    return found;
  })();
  try {
    return await browserSetup;
  } catch (e) {
    setEngineStatus('error', e.message);
    throw e;
  } finally {
    browserSetup = null;
  }
}

function explainLaunchError(e) {
  const msg = String(e?.message || e);
  if (/error while loading shared libraries|libnss3|libatk|libgbm|libasound/i.test(msg)) {
    return 'Chrome is missing system libraries on this server. On Linux run: sudo apt-get install -y libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 libpango-1.0-0 libasound2';
  }
  if (/ENOSPC/i.test(msg)) return 'The server ran out of disk space while setting up Chrome.';
  if (/ENOTFOUND|ECONNRESET|ETIMEDOUT|EAI_AGAIN|getaddrinfo/i.test(msg)) return "The server couldn't download Chrome (no internet access). Install Chrome or Chromium on the server and restart.";
  return `The browser could not start on the server: ${msg.split('\n')[0]}`;
}

async function getBrowser() {
  if (browserPromise) {
    const b = await browserPromise.catch(() => null);
    if (b && b.connected) return b;
    browserPromise = null;
  }
  browserPromise = (async () => {
    const executablePath = await resolveBrowserPath();
    setEngineStatus('starting', "Starting the server's browser…", 0);
    const shell = /headless[-_]shell/i.test(executablePath);
    const lowMemArgs = CONFIG.lowMemory
      ? [
          // One process instead of ~5: saves ~60 MB. Works because low-memory mode keeps one
          // account in the browser's default profile (separate profiles need multi-process).
          '--single-process',
          '--no-zygote',
          '--renderer-process-limit=1',
          '--disable-site-isolation-trials',
          '--disk-cache-size=1048576',
          '--media-cache-size=1',
          '--aggressive-cache-discard',
          '--disable-software-rasterizer',
          '--disable-component-update',
          '--disable-domain-reliability',
          '--disable-breakpad',
          '--blink-settings=imagesEnabled=false',
        ]
      : ['--renderer-process-limit=2'];
    return loadPuppeteer().launch({
      headless: shell ? 'shell' : CONFIG.browser.headless,
      executablePath,
      args: [
        ...lowMemArgs,
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-gpu',
        '--disable-extensions',
        '--disable-sync',
        '--disable-default-apps',
        '--mute-audio',
        '--disable-background-networking',
        '--disable-features=Translate,BackForwardCache,MediaRouter,OptimizationHints,site-per-process,IsolateOrigins',
        '--lang=en-US',
        CONFIG.lowMemory ? '--window-size=1024,700' : '--window-size=1366,900',
      ],
      defaultViewport: CONFIG.lowMemory ? { width: 1024, height: 700 } : { width: 1366, height: 900 },
    });
  })();
  try {
    const b = await browserPromise;
    browserError = null;
    setEngineStatus('ready', '', 100);
    b.on('disconnected', () => {
      log.warn('Browser closed — it will reopen on the next block.');
      browserPromise = null;
      for (const s of sessions.values()) if (s.account) s.account.ready = false;
    });
    log.info('Headless browser started.');
    return b;
  } catch (e) {
    browserPromise = null;
    const friendly = explainLaunchError(e);
    browserError = friendly;
    if (browserPath && !/shared libraries|disk space/.test(friendly)) browserPath = null; // try another browser next time
    setEngineStatus('error', friendly);
    log.error(friendly);
    throw new EngineError('engine_unavailable', friendly);
  }
}

class EngineError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function parseCookieInput(raw) {
  const text = String(raw || '').trim();
  if (!text) return [];
  // JSON export from a cookie extension
  if (text.startsWith('[')) {
    const arr = safeJson(text);
    if (Array.isArray(arr)) {
      return arr
        .filter((c) => c && c.name && c.value != null && /tiktok\.com$/i.test(String(c.domain || '.tiktok.com')))
        .map((c) => ({ name: String(c.name), value: String(c.value) }));
    }
  }
  if (!text.includes('=')) {
    return ['sessionid', 'sessionid_ss', 'sid_tt'].map((name) => ({ name, value: text }));
  }
  const out = [];
  for (const part of text.replace(/^cookie:\s*/i, '').split(';')) {
    const i = part.indexOf('=');
    if (i <= 0) continue;
    const name = part.slice(0, i).trim();
    const value = part.slice(i + 1).trim().replace(/^"(.*)"$/, '$1');
    if (/^[\w.\-]+$/.test(name) && value) out.push({ name, value });
  }
  return out;
}

async function readScope(page) {
  return page
    .evaluate(() => {
      const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__');
      if (!el) return null;
      try {
        return JSON.parse(el.textContent).__DEFAULT_SCOPE__ || null;
      } catch {
        return null;
      }
    })
    .catch(() => null);
}

async function hasCaptcha(page) {
  return page
    .evaluate(() => {
      const el = document.querySelector(
        '#captcha-verify-container, #captcha_container, .captcha-verify-container, .captcha_verify_container, [class*="captcha_verify"], [id^="captcha"]'
      );
      if (!el) return false;
      const r = el.getBoundingClientRect();
      return r.width > 0 && r.height > 0;
    })
    .catch(() => false);
}

/** XHR from inside the TikTok page, so TikTok's SDK adds its signatures. */
async function pageXhr(page, { method = 'GET', url, body = null, contentType = null }) {
  return page.evaluate(
    (o) =>
      new Promise((resolve) => {
        let finalUrl = o.url;
        if (finalUrl.includes('{{msToken}}')) {
          const m = /(?:^|;\s*)msToken=([^;]+)/.exec(document.cookie);
          let tok = m ? m[1] : '';
          try {
            tok = tok || localStorage.getItem('xmst') || '';
          } catch {}
          finalUrl = tok
            ? finalUrl.replace('{{msToken}}', encodeURIComponent(decodeURIComponent(tok)))
            : finalUrl.replace(/[?&]msToken=\{\{msToken\}\}/, '');
        }
        const x = new XMLHttpRequest();
        x.open(o.method, finalUrl, true);
        x.withCredentials = true;
        if (o.contentType) x.setRequestHeader('Content-Type', o.contentType);
        x.timeout = 12000;
        x.onload = () => resolve({ status: x.status, text: x.responseText || '' });
        x.onerror = () => resolve({ status: 0, text: '', error: 'network error' });
        x.ontimeout = () => resolve({ status: 0, text: '', error: 'timeout' });
        x.send(o.body);
      }),
    { method, url, body, contentType }
  );
}

/** Finds a visible element by selector or exact text and clicks it with real mouse events. */
async function clickTarget(page, { selectors = [], texts = [], within = null, hoverOnly = false }) {
  const pt = await page
    .evaluate(
      (selectors, texts, within) => {
        const visible = (el) => {
          const r = el.getBoundingClientRect();
          const s = getComputedStyle(el);
          return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none' && s.opacity !== '0';
        };
        const roots = within ? [...document.querySelectorAll(within)].filter(visible) : [document];
        const centre = (el) => {
          el.scrollIntoView({ block: 'center', inline: 'center' });
          const r = el.getBoundingClientRect();
          return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
        };
        for (const root of roots) {
          for (const sel of selectors) {
            const el = [...root.querySelectorAll(sel)].find(visible);
            if (el) return centre(el);
          }
          if (texts.length) {
            const want = texts.map((t) => t.toLowerCase());
            const hits = [...root.querySelectorAll('button,[role="button"],[role="menuitem"],li,a,div,span,p')]
              .filter((el) => visible(el) && want.includes((el.innerText || '').trim().toLowerCase()))
              .sort((a, b) => {
                const ra = a.getBoundingClientRect();
                const rb = b.getBoundingClientRect();
                return ra.width * ra.height - rb.width * rb.height;
              });
            if (hits.length) return centre(hits[0]);
          }
        }
        return null;
      },
      selectors,
      texts,
      within
    )
    .catch(() => null);
  if (!pt) return false;
  await page.mouse.move(pt.x, pt.y, { steps: 4 });
  if (!hoverOnly) await page.mouse.click(pt.x, pt.y, { delay: 40 });
  return true;
}

/** Keeps looking for an element until it appears (slow pages), then clicks it. */
async function waitForClick(page, opts, timeoutMs) {
  const until = Date.now() + timeoutMs;
  do {
    if (await clickTarget(page, opts)) return true;
    await delay(400);
  } while (Date.now() < until);
  return false;
}

const BLOCK_WORDS = ['Block', 'Bloquear', 'Bloquer', 'Blockieren', 'Blocca', 'Blokir', 'Engelle', 'Заблокировать', 'Заблокувати', 'Zablokuj', 'Blokkeren', 'Chặn', 'ブロック', '차단', '屏蔽', '封鎖', 'حظر', 'Blockera', 'Bloker', 'Blokker', 'Blokuj'];
const UNBLOCK_WORDS = ['Unblock', 'Desbloquear', 'Débloquer', 'Blockierung aufheben', 'Sblocca', 'Buka blokir', 'Engeli kaldır', 'Разблокировать', 'Розблокувати', 'Odblokuj', 'Deblokkeren', 'Bỏ chặn', 'ブロックを解除', '차단 해제', '取消屏蔽', '解除封鎖', 'إلغاء الحظر', 'Avblockera'];
const MORE_SELECTORS = [
  '[data-e2e="user-more"]',
  '[data-e2e="user-more-btn"]',
  '[data-e2e="more-menu-icon"]',
  'button[aria-label="Actions"]',
  'button[aria-label*="more" i]',
  'div[aria-label*="more" i][role="button"]',
];

const isBlockRequest = (req) => {
  if (req.method() === 'GET' || req.method() === 'OPTIONS') return false;
  try {
    const u = new URL(req.url());
    return /tiktok/i.test(u.hostname) && /block/i.test(u.pathname) && !/unblock|block_?list/i.test(u.pathname);
  } catch {
    return false;
  }
};

function learnTemplate(req, ids) {
  const u = new URL(req.url());
  for (const k of ['X-Bogus', 'X-Gnarly', '_signature', 'X-Argus', 'X-Ladon']) u.searchParams.delete(k);
  if (u.searchParams.has('msToken')) u.searchParams.set('msToken', '__MSTOKEN__');
  const swap = (s) => {
    let out = String(s || '');
    for (const [val, tag] of [
      [ids.secUid, '{{sec}}'],
      [ids.userId, '{{uid}}'],
    ]) {
      if (!val) continue;
      out = out.split(val).join(tag).split(encodeURIComponent(val)).join(tag);
    }
    return out;
  };
  const url = swap(u.toString()).replace('__MSTOKEN__', '{{msToken}}');
  const headers = req.headers();
  return {
    url,
    body: swap(req.postData() || ''),
    contentType: headers['content-type'] || null,
    learnedAt: Date.now(),
    failures: 0,
  };
}

const classifyTikTokReply = (status, text) => {
  const j = safeJson(text);
  const code = j?.status_code ?? j?.statusCode;
  const msg = j?.status_msg || j?.statusMsg || j?.message || '';
  if (code === 0) return { ok: true };
  if (/frequen|too many|limit|try again later|slow down/i.test(msg)) return { ok: false, code: 'rate_limited', note: `TikTok rate limit: ${msg}` };
  if (!text) return { ok: false, code: 'rejected', note: `TikTok returned an empty reply (HTTP ${status})` };
  return { ok: false, code: 'rejected', note: msg ? `TikTok said: ${msg}` : `TikTok code ${code ?? 'unknown'} (HTTP ${status})` };
};

const DEFAULT_BLOCK_GUESSES = [
  {
    url: 'https://www.tiktok.com/api/user/block/?aid=1988&app_language=en&app_name=tiktok_web&channel=tiktok_web&device_platform=web_pc&user_id={{uid}}&sec_user_id={{sec}}&block_type=1&source=3&msToken={{msToken}}',
    body: '',
    contentType: null,
  },
  {
    url: 'https://www.tiktok.com/api/user/block/?aid=1988&app_language=en&app_name=tiktok_web&channel=tiktok_web&device_platform=web_pc&msToken={{msToken}}',
    body: 'user_id={{uid}}&sec_user_id={{sec}}&block_type=1&source=0',
    contentType: 'application/x-www-form-urlencoded',
  },
];

function mutex() {
  let tail = Promise.resolve();
  return (fn) => {
    const run = tail.then(fn, fn);
    tail = run.catch(() => {});
    return run;
  };
}

// Lightest first. /explore is TikTok's full web app and always has the security script.
const HOME_CANDIDATES = ['https://www.tiktok.com/legal/page/global/terms-of-service/en', 'https://www.tiktok.com/explore'];

class TikTokAccount {
  constructor(session, cookies) {
    this.session = session;
    this.cookies = cookies;
    this.context = null;
    this.apiPage = null; // stays on tiktok.com; every fast request runs here in parallel
    this.uiPages = [];
    this.uiFree = [];
    this.uiWaiters = [];
    this.learn = mutex(); // only one worker figures out the block request at a time
    this.probed = false;
    this.detailWorks = false;
    this.me = null;
    this.ready = false;
  }

  async newPage({ light = false } = {}) {
    const page = await this.context.newPage();
    page.__light = light;
    page.setDefaultNavigationTimeout(CONFIG.browser.navTimeoutMs);
    await page.setUserAgent(UA);
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });
    await page.setRequestInterception(true);
    page.on('request', (r) => {
      if (r.isInterceptResolutionHandled()) return;
      const t = r.resourceType();
      const skip =
        t === 'image' || t === 'media' || t === 'font' || t === 'manifest' || t === 'texttrack' ||
        (page.__light && t === 'stylesheet') ||
        /\.(mp4|webm|m3u8|jpe?g|png|webp|gif|woff2?)(\?|$)/i.test(r.url()) ||
        /google-analytics|googletagmanager|doubleclick|facebook\.net|analytics\.tiktok\.com|ads\.tiktok\.com/i.test(r.url());
      if (skip) r.abort().catch(() => {});
      else r.continue().catch(() => {});
    });
    return page;
  }

  /**
   * Opens a tiktok.com page so TikTok's own security script is running in the tab (it signs the
   * block requests). Lighter pages are tried first; the first one that has the script is remembered.
   */
  async loadHome(page) {
    const remembered = templates.data.__home;
    const skip = new Set(templates.data.__noScript || []);
    const candidates = [...new Set([remembered, ...HOME_CANDIDATES].filter((u) => u && !skip.has(u)))];
    for (const url of candidates) {
      await this.gotoWithRetry(page, url);
      await page.waitForSelector('#__UNIVERSAL_DATA_FOR_REHYDRATION__', { timeout: 20000 }).catch(() => {});
      const signed = await page
        .waitForFunction(() => !/\[native code\]/.test(XMLHttpRequest.prototype.open.toString()), { timeout: 20000, polling: 500 })
        .then(() => true)
        .catch(() => false);
      if (signed || url === candidates[candidates.length - 1]) {
        if (signed && templates.data.__home !== url) {
          templates.data.__home = url;
          templates.save();
        }
        await delay(500);
        return;
      }
      log.info(`TikTok's security script didn't load on ${url}; trying a fuller page.`);
      templates.data.__noScript = [...skip, url];
      templates.save();
    }
  }

  async gotoWithRetry(page, url) {
    for (let attempt = 1; ; attempt++) {
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded' });
        break;
      } catch (e) {
        if (attempt < 2 && /timeout/i.test(e.message)) continue;
        if (/timeout/i.test(e.message)) {
          throw new EngineError(
            'slow',
            `TikTok took more than ${Math.round(CONFIG.browser.navTimeoutMs / 1000)}s to load on the server. Try again — the first load after a restart is the slowest. If it keeps happening, raise NAV_TIMEOUT_MS (e.g. 180000).`
          );
        }
        throw e;
      }
    }
  }

  /**
   * Works out which account the cookie belongs to. Checks several places because TikTok moves
   * this around: the page's own data, the account-info endpoint, and the profile link in the menu.
   * Returns { info } or { info: null, report } where report says what TikTok actually showed.
   */
  async detectLogin(page) {
    const set = await this.context.cookies('https://www.tiktok.com').catch(() => []);
    const hasSession = set.some((c) => /^(sessionid|sessionid_ss|sid_tt)$/.test(c.name));
    if (!hasSession) return { info: null, report: 'The browser refused the cookie value, so check you copied the whole sessionid with nothing extra.' };

    // 1) page data
    const scope = await readScope(page);
    const ctxUser = scope?.['webapp.app-context']?.user;
    if (ctxUser?.uid) {
      return { info: { uid: String(ctxUser.uid), uniqueId: ctxUser.uniqueId, nickname: ctxUser.nickName, avatar: ctxUser.avatarUri?.[0] || '' } };
    }

    // 2) account-info endpoint (two common forms)
    let passportNote = '';
    for (const url of [
      'https://www.tiktok.com/passport/web/account/info/?aid=1459&app_name=tiktok_web&device_platform=web_pc',
      'https://www.tiktok.com/passport/web/user/info/?aid=1459&app_name=tiktok_web',
    ]) {
      const r = await pageXhr(page, { url }).catch(() => null);
      const j = safeJson(r?.text);
      const d = j?.data;
      if (d && (d.user_id_str || d.user_id) && !d.error_code) {
        return { info: { uid: String(d.user_id_str || d.user_id), uniqueId: d.username || d.unique_id || '', nickname: d.screen_name || d.name || '', avatar: d.avatar_url || '' } };
      }
      if (!passportNote) {
        passportNote = j
          ? `TikTok's account check said: "${d?.description || j.message || d?.error_code || 'no account'}".`
          : `TikTok's account check gave ${r?.status ? `HTTP ${r.status}` : 'no answer'}${r?.text ? '' : ' (empty reply)'}.`;
      }
    }

    // 3) the profile link in TikTok's menu
    await page
      .waitForSelector('[data-e2e="profile-icon"], [data-e2e="nav-profile"], [data-e2e="top-login-button"], [data-e2e="nav-login-button"]', { timeout: 12000 })
      .catch(() => {});
    const dom = await page
      .evaluate(() => {
        const prof = document.querySelector('a[data-e2e="nav-profile"], [data-e2e="profile-icon"]');
        const link = prof?.closest('a')?.getAttribute('href') || prof?.getAttribute('href') || '';
        const loginBtn = !!document.querySelector('[data-e2e="top-login-button"], [data-e2e="nav-login-button"]');
        return { link, loginBtn, title: document.title, url: location.href };
      })
      .catch(() => ({ link: '', loginBtn: false, title: '', url: '' }));
    const handle = /\/@([^/?#]+)/.exec(dom.link)?.[1];
    if (handle) {
      // Logged in but TikTok hid the numeric ID; a stable stand-in keeps per-account data apart.
      const sid = set.find((c) => c.name === 'sessionid' || c.name === 'sessionid_ss')?.value || '';
      const uid = 'h' + crypto.createHash('sha256').update(sid).digest('hex').slice(0, 15);
      return { info: { uid, uniqueId: decodeURIComponent(handle), nickname: '', avatar: '' } };
    }

    const parts = [passportNote];
    if (dom.loginBtn) parts.push('The page showed a "Log in" button.');
    if (!scope) parts.push(`The page didn't load TikTok's data (title: "${dom.title || 'blank'}").`);
    parts.push('Paste the whole cookie string instead of only sessionid (see the steps above). If it still fails, the session may be tied to your region; try a Render region closer to you, like Frankfurt.');
    return { info: null, report: parts.filter(Boolean).join(' ') };
  }

  async open() {
    lastBrowserUse = Date.now();
    const browser = await getBrowser();
    if (CONFIG.lowMemory) {
      this.context = browser.defaultBrowserContext();
      this.sharedContext = true;
      try {
        const old = await this.context.cookies();
        if (old.length) await this.context.deleteCookie(...old);
      } catch {}
    } else {
      this.context = await browser.createBrowserContext();
    }
    await this.context.setCookie(
      ...this.cookies.map((c) => ({ name: c.name, value: c.value, domain: '.tiktok.com', path: '/', secure: true, sameSite: 'None' }))
    );

    const page = await this.newPage({ light: true });
    await this.loadHome(page);
    if (await hasCaptcha(page)) {
      throw new EngineError('captcha', 'TikTok showed a captcha to the server. Wait a few minutes, or run the server from a home connection.');
    }

    const { info, report } = await this.detectLogin(page);
    if (!info) {
      log.warn(`[${this.session.username}] Login check failed: ${report}`);
      throw new EngineError('not_logged_in', `TikTok treated the server as logged out. ${report}`);
    }

    this.me = info;
    this.apiPage = page;
    this.ready = true;
    log.info(`[${this.session.username}] TikTok engine ready as @${info.uniqueId} (${info.uid})`);
    return info;
  }

  async ensureApiPage() {
    if (this.apiPage && !this.apiPage.isClosed()) return this.apiPage;
    this.apiPage = await this.newPage({ light: true });
    await this.loadHome(this.apiPage);
    return this.apiPage;
  }

  async withUiPage(fn) {
    if (CONFIG.lowMemory) {
      // One TikTok tab only. Any tiktok.com page keeps signing requests, so it can stay on the
      // profile afterwards; it just needs TikTok's styling so menus and dialogs behave normally.
      const page = await this.ensureApiPage();
      page.__light = false;
      return fn(page);
    }
    let page = this.uiFree.pop();
    if (!page) {
      if (this.uiPages.length < CONFIG.block.uiPages) {
        page = await this.newPage();
        this.uiPages.push(page);
      } else {
        page = await new Promise((res) => this.uiWaiters.push(res));
        if (!page) throw new EngineError('closed', 'Account disconnected');
      }
    }
    try {
      return await fn(page);
    } finally {
      if (page.isClosed()) this.uiPages = this.uiPages.filter((p) => p !== page);
      else {
        const w = this.uiWaiters.shift();
        if (w) w(page);
        else this.uiFree.push(page);
      }
    }
  }

  async close() {
    this.ready = false;
    const ctx = this.context;
    const pages = [this.apiPage, ...this.uiPages].filter(Boolean);
    this.context = null;
    this.apiPage = null;
    this.uiPages = [];
    this.uiFree = [];
    this.uiWaiters.splice(0).forEach((w) => w(null));
    if (ctx && this.sharedContext) {
      for (const p of pages) await p.close().catch(() => {});
      try {
        const old = await ctx.cookies();
        if (old.length) await ctx.deleteCookie(...old);
      } catch {}
    } else if (ctx) await ctx.close().catch(() => {});
  }

  get template() {
    return templates.data[this.me?.uid] || null;
  }
  get fastHealthy() {
    const t = this.template;
    return Boolean(t && t.failures < CONFIG.block.apiFailuresBeforeUi);
  }

  /** Returns ids, or { missing: note } when TikTok says the account doesn't exist, or null if unknown. */
  async resolveIds(handle) {
    const known = knownProfile(handle);
    if (known) return known;
    const page = await this.ensureApiPage();
    const url = `https://www.tiktok.com/api/user/detail/?aid=1988&app_language=en&app_name=tiktok_web&device_platform=web_pc&uniqueId=${encodeURIComponent(handle)}&secUid=&msToken={{msToken}}`;
    const r = await pageXhr(page, { url }).catch(() => null);
    const j = safeJson(r?.text);
    const u = j?.userInfo?.user;
    if (u?.id && u?.secUid) {
      this.detailWorks = true;
      rememberProfile(handle, { userId: u.id, secUid: u.secUid, nickname: u.nickname });
      return knownProfile(handle);
    }
    if (this.detailWorks && j && Number(j.statusCode) && !u?.id) {
      return { missing: `@${handle} doesn't exist or is banned (TikTok code ${j.statusCode})` };
    }
    return null;
  }

  async sendBlock(tpl, ids) {
    const page = await this.ensureApiPage();
    const fill = (s) => String(s || '').split('{{uid}}').join(ids.userId).split('{{sec}}').join(ids.secUid);
    const r = await pageXhr(page, { method: 'POST', url: fill(tpl.url), body: fill(tpl.body) || null, contentType: tpl.contentType });
    return classifyTikTokReply(r.status, r.text);
  }

  /** Tries the usual TikTok block request so the first block doesn't need the profile page. */
  async probeGuesses(ids) {
    for (const g of DEFAULT_BLOCK_GUESSES) {
      const r = await this.sendBlock(g, ids);
      if (r.ok) {
        templates.data[this.me.uid] = { ...g, learnedAt: Date.now(), failures: 0, source: 'guess' };
        templates.save();
        log.info(`[${this.session.username}] Fast block request works — no profile clicks needed.`);
        return { ...r, vector: 'fast' };
      }
      if (r.code === 'rate_limited') return { ...r, vector: 'fast' };
      log.info(`[${this.session.username}] Direct block request not accepted (${r.note}); using the profile page once to learn it.`);
    }
    return null;
  }

  async blockViaUi(page, handle) {
    const step = (t) => this.session.step(t);
    step(`Opening @${handle}'s profile (first block only; slow on small servers)…`);
    await page.goto(`https://www.tiktok.com/@${encodeURIComponent(handle)}`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('#__UNIVERSAL_DATA_FOR_REHYDRATION__', { timeout: 20000 }).catch(() => {});
    if (await hasCaptcha(page)) return { ok: false, code: 'captcha', note: 'TikTok asked for a captcha' };

    const detail = (await readScope(page))?.['webapp.user-detail'];
    const user = detail?.userInfo?.user;
    const ids = user?.id ? { userId: String(user.id), secUid: String(user.secUid || '') } : null;
    if (ids) rememberProfile(handle, { ...ids, nickname: user.nickname });
    if (!ids && detail && Number(detail.statusCode) !== 0) {
      return { ok: false, code: 'not_found', note: `@${handle} doesn't exist or is banned (TikTok code ${detail.statusCode})` };
    }

    step("Waiting for TikTok's profile page to finish loading…");
    // On slow servers TikTok's buttons exist long before they respond to clicks. Wait until the
    // page has finished wiring them up (React attaches its handlers to the element).
    await page
      .waitForFunction(
        (sels) => {
          const el = sels.map((s) => document.querySelector(s)).find(Boolean);
          return el && Object.keys(el).some((k) => k.startsWith('__reactProps') || k.startsWith('__reactFiber'));
        },
        { timeout: CONFIG.block.uiReadyTimeoutMs, polling: 500 },
        [...MORE_SELECTORS, '[data-e2e="follow-button"]']
      )
      .catch(() => {});

    if (await clickTarget(page, { texts: UNBLOCK_WORDS, hoverOnly: true })) return { ok: true, already: true, ids };

    // Watch every write request TikTok makes after we click, so a failure can say what happened.
    const seen = [];
    const onReq = (r) => {
      if (r.method() === 'GET' || r.method() === 'OPTIONS') return;
      try {
        const u = new URL(r.url());
        if (/tiktok/i.test(u.hostname) && !/log|monitor|slardar|mcs|mon\./i.test(u.hostname + u.pathname)) seen.push(u.pathname);
      } catch {}
    };
    page.on('request', onReq);
    const responseP = page
      .waitForResponse((r) => isBlockRequest(r.request()), { timeout: CONFIG.block.uiResponseTimeoutMs })
      .catch(() => null);

    try {
      step('Opening the … menu…');
      // Open the "…" menu and wait for the Block item to show up.
      if (!(await waitForClick(page, { selectors: MORE_SELECTORS, hoverOnly: true }, 8000))) {
        return { ok: false, code: 'ui_changed', note: "Couldn't find the … menu on the profile page", ids };
      }
      let clickedMenu = await waitForClick(page, { texts: BLOCK_WORDS }, 6000);
      if (!clickedMenu) {
        await clickTarget(page, { selectors: MORE_SELECTORS });
        clickedMenu = await waitForClick(page, { texts: BLOCK_WORDS }, 8000);
      }
      if (!clickedMenu) return { ok: false, code: 'ui_changed', note: "Couldn't find Block in the profile menu", ids };

      step('Confirming Block…');
      // Wait for the confirmation box, then press its Block button.
      const confirmed = await waitForClick(
        page,
        { texts: BLOCK_WORDS, within: '[role="dialog"], [class*="Modal"], [class*="modal"], [class*="Dialog"]' },
        CONFIG.block.uiDialogTimeoutMs
      );
      if (!confirmed) await clickTarget(page, { texts: BLOCK_WORDS });

      step("Waiting for TikTok's answer…");
      // The request goes out right after confirming, so don't wait long once that's done.
      const resp = await Promise.race([responseP, delay(CONFIG.block.uiAfterConfirmMs).then(() => null)]);
      if (!resp) {
        if (await hasCaptcha(page)) return { ok: false, code: 'captcha', note: 'TikTok asked for a captcha' };
        const sent = [...new Set(seen)].slice(0, 4);
        return {
          ok: false,
          code: 'no_response',
          note: `Pressed Block${confirmed ? ' and confirmed' : ' (no confirmation box appeared)'}, but TikTok sent no block request${sent.length ? `. Requests seen: ${sent.join(', ')}` : ''}`,
          ids,
        };
      }
      const verdict = classifyTikTokReply(resp.status(), await resp.text().catch(() => ''));
      if (verdict.ok && ids?.userId && ids?.secUid) {
        templates.data[this.me.uid] = { ...learnTemplate(resp.request(), ids), source: 'profile' };
        templates.save();
        log.info(`[${this.session.username}] Learned TikTok's block request (${new URL(resp.url()).pathname}) — fast mode on.`);
        step('Learned it. Blocking the rest in fast mode…');
      }
      return { ...verdict, ids };
    } finally {
      page.off('request', onReq);
    }
  }

  async uiBlock(handle) {
    const r = await this.withUiPage((page) => this.blockViaUi(page, handle));
    return { ...r, vector: r.already ? 'already' : 'profile' };
  }

  async block(handle) {
    lastBrowserUse = Date.now();
    if (this.fastHealthy) {
      const ids = await this.resolveIds(handle);
      if (ids?.missing) return { ok: false, code: 'not_found', note: ids.missing, vector: 'lookup' };
      if (ids) {
        const r = await this.sendBlock(this.template, ids);
        if (r.ok || r.code === 'rate_limited') {
          if (r.ok && this.template.failures) ((this.template.failures = 0), templates.save());
          return { ...r, vector: 'fast', ids };
        }
        this.template.failures++;
        templates.save();
      }
      if (this.fastHealthy) return this.uiBlock(handle);
    }

    // The block request isn't known (or stopped working). One worker works it out through the
    // profile page while the others wait, then they all go fast. Nothing else touches the tab meanwhile.
    return this.learn(async () => {
      if ((this.session.uiFails || 0) >= CONFIG.block.uiFailuresBeforeStop) {
        return { ok: false, code: 'stopped', note: this.session.uiStopNote || 'Stopped', vector: 'profile' };
      }
      if (!this.fastHealthy) this.session.step(`Looking up @${handle}…`);
      const ids = await this.resolveIds(handle);
      if (ids?.missing) return { ok: false, code: 'not_found', note: ids.missing, vector: 'lookup' };
      if (ids && this.fastHealthy) {
        const r = await this.sendBlock(this.template, ids);
        if (r.ok || r.code === 'rate_limited') return { ...r, vector: 'fast', ids };
      }
      if (ids && !this.probed) {
        this.probed = true;
        this.session.step('Trying a direct block request…');
        const r = await this.probeGuesses(ids);
        if (r) return { ...r, ids };
      }
      return this.uiBlock(handle);
    });
  }

}

// ============================================================================
// SESSIONS
// ============================================================================
class ClientSession {
  constructor(id) {
    this.id = id;
    this.username = 'Operator';
    this.cookies = null;
    this.account = null;
    this.opening = null;
    this.auth = { loggedIn: false, accountName: '', nickname: '', avatar: '', autoBlockSharers: false, autoBlockThreshold: CONFIG.share.defaultThreshold, engineError: null };
    this.state = { connected: false, username: null, roomId: null, error: null };
    this.sharers = new Map(); // lowercase handle -> entry
    this.connection = null;
    this.jobs = new Map();
    this.queue = [];
    this.workers = 0;
    this.pausedUntil = 0;
    this.strikes = 0;
    this.sseClients = new Set();
    this.attempts = [];
    this.lastSeen = Date.now();
  }

  publicAuth() {
    const a = this.auth;
    return {
      loggedIn: a.loggedIn,
      accountName: a.accountName,
      nickname: a.nickname,
      avatar: a.avatar,
      autoBlockSharers: a.autoBlockSharers,
      autoBlockThreshold: a.autoBlockThreshold,
      engineError: a.engineError,
      fastMode: Boolean(
        this.account?.fastHealthy ||
          (this.lastUid && templates.data[this.lastUid] && templates.data[this.lastUid].failures < CONFIG.block.apiFailuresBeforeUi)
      ),
    };
  }

  broadcast(payload) {
    const msg = `data: ${JSON.stringify(payload)}\n\n`;
    for (const res of this.sseClients) {
      try {
        res.write(msg);
      } catch {
        this.sseClients.delete(res);
      }
    }
  }

  step(text) {
    this.currentStep = text;
    this.broadcast({ type: 'STEP', step: text });
    log.info(`[${this.username}] ${text}`);
  }

  recordAttempt(entry) {
    const item = { id: crypto.randomUUID(), timestamp: ts(), ...entry };
    this.attempts.push(item);
    if (this.attempts.length > 300) this.attempts.shift();
    this.broadcast({ type: 'ATTEMPT', attempt: item });
    return item;
  }

  async ensureAccount() {
    if (this.account?.ready) return this.account;
    if (!this.cookies) return null;
    if (this.opening) return this.opening;
    this.opening = (async () => {
      if (this.account) await this.account.close();
      const acc = new TikTokAccount(this, this.cookies);
      try {
        const me = await acc.open();
        this.account = acc;
        this.lastUid = me.uid;
        Object.assign(this.auth, { loggedIn: true, accountName: me.uniqueId || '', nickname: me.nickname || '', avatar: me.avatar || '', engineError: null });
        return acc;
      } catch (err) {
        const e = err instanceof EngineError ? err : new EngineError('network', `Couldn't open tiktok.com from the server: ${String(err.message).split('\n')[0]}`);
        await acc.close();
        this.account = null;
        this.auth.engineError = e.message;
        if (e.code === 'not_logged_in') {
          this.auth.loggedIn = false;
          this.cookies = null;
        }
        throw e;
      } finally {
        this.opening = null;
        this.broadcast({ type: 'AUTH', auth: this.publicAuth() });
      }
    })();
    return this.opening;
  }
}

const sessions = new Map();
function getSession(id) {
  if (!sessions.has(id)) sessions.set(id, new ClientSession(id));
  const s = sessions.get(id);
  s.lastSeen = Date.now();
  return s;
}

function broadcastBlacklist() {
  const list = blacklist.all();
  for (const s of sessions.values()) {
    for (const [h, e] of s.sharers) e.blacklisted = blacklist.has(h);
    s.broadcast({ type: 'BLACKLIST', blacklist: list });
  }
}

// ============================================================================
// BLOCK QUEUE
// ============================================================================
function publicJob(j) {
  const { _timer, ...rest } = j;
  return rest;
}

function pushJob(session, job, force = false) {
  if (force || job.status !== 'running') {
    clearTimeout(job._timer);
    job._timer = null;
    session.broadcast({ type: 'JOB', job: publicJob(job) });
    return;
  }
  if (job._timer) return;
  job._timer = setTimeout(() => {
    job._timer = null;
    session.broadcast({ type: 'JOB', job: publicJob(job) });
  }, 120);
}

function createJob(session, handles, kind = 'manual') {
  const job = {
    id: crypto.randomUUID(),
    kind,
    status: 'running',
    total: handles.length,
    processed: 0,
    blockedOnTikTokCount: 0,
    alreadyCount: 0,
    failedCount: 0,
    cancelledCount: 0,
    currentUsername: null,
    lastNote: null,
    startedAt: Date.now(),
    finishedAt: null,
    cancelRequested: false,
    _timer: null,
  };
  session.uiFails = 0;
  session.uiStopNote = null;
  session.jobs.set(job.id, job);
  if (session.jobs.size > 50) session.jobs.delete(session.jobs.keys().next().value);

  const uid = session.account?.me?.uid || session.lastUid;
  for (const h of handles) {
    if (isInLedger(uid, h)) {
      job.processed++;
      job.alreadyCount++;
    } else session.queue.push({ handle: h, job });
  }
  finishJobIfDone(session, job);
  pushJob(session, job, true);
  ensureWorkers(session);
  return job;
}

function finishJobIfDone(session, job) {
  if (job.status !== 'running') return;
  if (job.processed + job.cancelledCount >= job.total) {
    if (session.currentStep) session.step('');
    job.status = job.cancelRequested ? 'cancelled' : 'done';
    job.currentUsername = null;
    job.finishedAt = Date.now();
  }
}

function failQueue(session, note) {
  const touched = new Set();
  for (const it of session.queue.splice(0)) {
    it.job.processed++;
    it.job.failedCount++;
    it.job.lastNote = note;
    finishJobIfDone(session, it.job);
    touched.add(it.job);
  }
  touched.forEach((j) => pushJob(session, j, true));
}

function ensureWorkers(session) {
  if (!session.queue.length) return;
  if (!session.cookies) return failQueue(session, 'Connect your TikTok account first');
  const want = Math.min(CONFIG.block.concurrency, session.queue.length);
  while (session.workers < want) {
    const lane = session.workers++;
    runWorker(session, lane)
      .catch((e) => log.error('Worker crashed:', e))
      .finally(() => {
        session.workers--;
        if (session.queue.length && session.cookies && !session.opening) setTimeout(() => ensureWorkers(session), 300);
      });
  }
}

async function runWorker(session, lane) {
  let account;
  try {
    account = await session.ensureAccount();
  } catch (e) {
    failQueue(session, e.message);
    return;
  }
  if (!account) return;
  if (lane) await delay(lane * CONFIG.block.staggerMs);

  while (session.queue.length && account.ready) {
    const wait = session.pausedUntil - Date.now();
    if (wait > 0) await delay(wait);
    const item = session.queue.shift();
    if (!item) break;
    const { handle, job } = item;
    if (job.cancelRequested) {
      job.cancelledCount++;
      finishJobIfDone(session, job);
      pushJob(session, job);
      continue;
    }
    job.currentUsername = handle;
    pushJob(session, job);

    const started = Date.now();
    let result;
    try {
      result = await account.block(handle);
    } catch (e) {
      result = { ok: false, code: 'error', note: String(e.message).split('\n')[0], vector: 'error' };
      if (/Target closed|Session closed|detached|Execution context was destroyed/i.test(e.message)) {
        session.queue.unshift(item); // page died mid-request; retry this user once the page is back
        await delay(500);
        continue;
      }
    }

    if (result.code === 'captcha' || result.code === 'rate_limited') {
      session.strikes++;
      if (session.strikes >= CONFIG.block.maxStrikes) {
        session.strikes = 0;
        job.processed++;
        job.failedCount++;
        job.lastNote = result.note;
        session.recordAttempt({ username: handle, userId: result.ids?.userId || '', success: false, vector: result.vector, note: result.note, elapsedMs: Date.now() - started });
        finishJobIfDone(session, job);
        pushJob(session, job, true);
        failQueue(session, `${result.note}. Stopped — try again in a few minutes.`);
        break;
      }
      session.pausedUntil = Date.now() + CONFIG.block.pauseOnLimitMs;
      job.lastNote = `${result.note}. Pausing ${Math.round(CONFIG.block.pauseOnLimitMs / 1000)}s`;
      session.queue.unshift(item);
      pushJob(session, job, true);
      continue;
    }
    session.strikes = 0;

    if (result.code === 'ui_changed' || result.code === 'no_response') {
      session.uiFails = (session.uiFails || 0) + 1;
    } else if (result.ok) session.uiFails = 0;

    job.processed++;
    if (result.ok) {
      addToLedger(account.me.uid, handle);
      if (result.already) job.alreadyCount++;
      else job.blockedOnTikTokCount++;
    } else {
      job.failedCount++;
      job.lastNote = result.note;
    }
    if (result.code !== 'stopped') session.recordAttempt({
      username: handle,
      userId: result.ids?.userId || knownProfile(handle)?.userId || '',
      success: Boolean(result.ok),
      vector: result.vector,
      note: result.ok ? (result.already ? 'Already blocked' : 'Blocked') : result.note,
      elapsedMs: Date.now() - started,
    });
    finishJobIfDone(session, job);
    pushJob(session, job);
    if ((session.uiFails || 0) >= CONFIG.block.uiFailuresBeforeStop && !account.fastHealthy && result.code !== 'stopped') {
      session.uiStopNote = `Stopped: TikTok's Block button didn't work ${CONFIG.block.uiFailuresBeforeStop} times in a row. Last problem: ${result.note}`;
      failQueue(session, session.uiStopNote);
      break;
    }
    if (result.ok && result.vector !== 'fast' && account.fastHealthy) session.broadcast({ type: 'AUTH', auth: session.publicAuth() });
    if (result.ok && result.vector === 'fast' && job.blockedOnTikTokCount === 1) session.broadcast({ type: 'AUTH', auth: session.publicAuth() });
  }
}

// ============================================================================
// LIVE SHARE LISTENER
// ============================================================================
let tiktokLib = null;
const getTikTokLib = async () => (tiktokLib ||= await import('tiktok-live-connector'));

function isSharePacket(data) {
  if (!data || typeof data !== 'object') return false;
  if (String(data.action ?? '') === '3') return true;
  const key = String(data.common?.displayText?.key || data.displayType || '').toLowerCase();
  return key.includes('share');
}

function extractShareUser(data) {
  const u = data?.user || {};
  const handle = normalizeHandle(u.uniqueId || u.displayId || u.username || '');
  return {
    uniqueId: handle,
    displayHandle: String(u.uniqueId || u.displayId || handle).replace(/^@+/, ''),
    nickname: String(u.nickname || u.nickName || handle).trim(),
    userId: String(u.userId || u.id || u.idStr || '').replace(/\D/g, ''),
    secUid: String(u.secUid || u.sec_uid || '').trim(),
    profilePictureUrl:
      u.profilePictureUrl || u.avatarThumb?.urlList?.[0] || u.avatarThumb?.url?.[0] || u.profilePicture?.urls?.[0] || u.profilePicture?.url?.[0] || '',
  };
}

function recordSharer(session, data) {
  const p = extractShareUser(data);
  if (!isValidHandle(p.uniqueId)) return;
  if (p.userId || p.secUid) rememberProfile(p.uniqueId, p);

  const prev = session.sharers.get(p.uniqueId);
  if (prev && Date.now() - prev.timestamp < CONFIG.share.dedupWindowMs) return;

  const entry = {
    uniqueId: p.uniqueId,
    displayHandle: p.displayHandle,
    nickname: p.nickname,
    userId: p.userId || prev?.userId || '',
    profilePictureUrl: p.profilePictureUrl || prev?.profilePictureUrl || '',
    shareCount: (prev?.shareCount || 0) + 1,
    firstSharedAt: prev?.firstSharedAt || Date.now(),
    timestamp: Date.now(),
    blacklisted: blacklist.has(p.uniqueId),
  };
  session.sharers.set(p.uniqueId, entry);

  if (session.auth.autoBlockSharers && entry.shareCount >= session.auth.autoBlockThreshold && !entry.blacklisted) {
    blacklist.add([p.uniqueId]);
    entry.blacklisted = true;
    broadcastBlacklist();
    if (session.cookies) createJob(session, [p.uniqueId], 'auto');
  }
  session.broadcast({ type: 'SHARE', user: entry });
}

// ============================================================================
// EXPRESS
// ============================================================================
const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: CONFIG.corsOrigin }));
app.use(express.json({ limit: '2mb' }));

app.get('/', (req, res) => {
  const htmlPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(htmlPath)) return res.sendFile(htmlPath);
  res.json({ ok: true, service: 'nexus-tiktok' });
});

app.get('/api/health', (req, res) => {
  res.json({
    ok: true,
    uptimeSec: Math.round(process.uptime()),
    activeSessions: sessions.size,
    globalBlacklisted: blacklist.all().length,
    knownProfiles: Object.keys(profiles.data).length,
    browser: browserError ? `error: ${browserError}` : engineStatus.state,
    browserPath,
  });
});

app.use('/api/tiktok', (req, res, next) => {
  const sessionId = String(req.headers['x-session-id'] || req.query.sessionId || '').trim();
  if (!sessionId) return res.status(401).json({ error: 'Missing session ID.' });
  req.session = getSession(sessionId);
  req.session.username = String(req.headers['x-username'] || req.query.username || 'Operator').slice(0, 40);
  next();
});

const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

function snapshot(s) {
  return {
    state: s.state,
    auth: s.publicAuth(),
    sharers: [...s.sharers.values()],
    blacklist: blacklist.all(),
    jobs: [...s.jobs.values()].filter((j) => j.kind === 'manual').slice(-1).map(publicJob),
    attempts: s.attempts.slice(-60),
    engine: { ...engineStatus },
    step: s.currentStep || '',
  };
}

app.get('/api/tiktok/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders?.();
  const s = req.session;
  s.sseClients.add(res);
  res.write(`data: ${JSON.stringify({ type: 'INIT', ...snapshot(s) })}\n\n`);
  const ping = setInterval(() => {
    try {
      res.write(': ping\n\n');
      s.lastSeen = Date.now();
    } catch {}
  }, 15000);
  req.on('close', () => {
    clearInterval(ping);
    s.sseClients.delete(res);
  });
});

app.get('/api/tiktok/sharers', (req, res) => res.json(snapshot(req.session)));

app.post(
  '/api/tiktok/login',
  wrap(async (req, res) => {
    const s = req.session;
    const cookies = parseCookieInput(req.body?.cookie);
    if (!cookies.some((c) => /^(sessionid|sessionid_ss|sid_tt)$/i.test(c.name))) {
      return res.status(400).json({ error: 'That cookie has no sessionid in it. Paste the full Cookie header from tiktok.com.' });
    }
    if (CONFIG.lowMemory) {
      const other = [...sessions.values()].find((x) => x !== s && x.cookies);
      if (other) {
        return res.status(409).json({
          error: `This server runs in low-memory mode, which fits one TikTok account at a time. @${other.auth.accountName || 'another account'} is already connected in another session. Disconnect it there first.`,
        });
      }
    }
    s.cookies = cookies;
    if (s.account) {
      await s.account.close();
      s.account = null;
    }
    try {
      await s.ensureAccount();
      res.json({ success: true, auth: s.publicAuth() });
      ensureWorkers(s);
    } catch (e) {
      res.status(e.code === 'not_logged_in' ? 401 : 503).json({ error: e.message, code: e.code, auth: s.publicAuth() });
    }
  })
);

app.post(
  '/api/tiktok/logout',
  wrap(async (req, res) => {
    const s = req.session;
    failQueue(s, 'Account disconnected');
    s.cookies = null;
    if (s.account) await s.account.close();
    s.account = null;
    Object.assign(s.auth, { loggedIn: false, accountName: '', nickname: '', avatar: '', autoBlockSharers: false, engineError: null });
    s.broadcast({ type: 'AUTH', auth: s.publicAuth() });
    res.json({ success: true });
  })
);

app.post('/api/tiktok/auto-block', (req, res) => {
  const s = req.session;
  s.auth.autoBlockSharers = Boolean(req.body?.enabled);
  const t = Number(req.body?.threshold);
  if (t >= 1 && t <= 10) s.auth.autoBlockThreshold = Math.round(t);
  s.broadcast({ type: 'AUTH', auth: s.publicAuth() });
  res.json({ success: true, auth: s.publicAuth() });
});

app.get('/api/tiktok/blacklist', (req, res) => {
  const list = blacklist.all();
  res.json({ usernames: list, text: list.join('\n') });
});

app.post('/api/tiktok/blacklist/save', (req, res) => {
  const { text, usernames } = req.body || {};
  blacklist.replaceAll(Array.isArray(usernames) ? usernames : String(text || ''));
  broadcastBlacklist();
  const list = blacklist.all();
  res.json({ success: true, total: list.length, usernames: list });
});

app.post('/api/tiktok/blacklist/remove', (req, res) => {
  const h = normalizeHandle(req.body?.username);
  if (blacklist.set.delete(h)) {
    blacklist.persist();
    broadcastBlacklist();
  }
  res.json({ success: true, total: blacklist.all().length });
});

function startBlockJob(req, res) {
  const handles = cleanHandleList(req.body?.usernames);
  if (!handles.length) return res.status(400).json({ error: 'No valid TikTok usernames to block.' });
  blacklist.add(handles);
  broadcastBlacklist();
  const s = req.session;
  if (!s.cookies) {
    return res.status(409).json({ error: 'Added to the blocklist. Connect your TikTok account to block them on TikTok.', blacklisted: handles.length });
  }
  const job = createJob(s, handles, handles.length === 1 ? 'single' : 'manual');
  res.status(202).json(publicJob(job));
}
app.post('/api/tiktok/jobs/block', startBlockJob);
app.post('/api/tiktok/block', startBlockJob);

app.get('/api/tiktok/jobs/:id', (req, res) => {
  const job = req.session.jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'No such job.' });
  res.json(publicJob(job));
});

app.delete('/api/tiktok/jobs/:id', (req, res) => {
  const s = req.session;
  const job = s.jobs.get(req.params.id);
  if (!job || job.status !== 'running') return res.status(404).json({ error: 'No running job with that ID.' });
  job.cancelRequested = true;
  const before = s.queue.length;
  s.queue = s.queue.filter((it) => it.job !== job);
  job.cancelledCount += before - s.queue.length;
  finishJobIfDone(s, job);
  s.broadcast({ type: 'JOB', job: publicJob(job) });
  res.json({ success: true });
});

app.get('/api/tiktok/diagnostics', (req, res) => {
  const s = req.session;
  res.json({
    auth: s.publicAuth(),
    knownProfiles: Object.keys(profiles.data).length,
    queued: s.queue.length,
    pausedForSec: Math.max(0, Math.round((s.pausedUntil - Date.now()) / 1000)),
    recentAttempts: s.attempts.slice().reverse(),
  });
});

app.post(
  '/api/tiktok/connect',
  wrap(async (req, res) => {
    const s = req.session;
    const target = normalizeHandle(req.body?.username);
    if (!isValidHandle(target)) return res.status(400).json({ error: 'Enter the TikTok username of the LIVE you want to watch.' });

    if (s.connection) {
      try {
        await s.connection.disconnect();
      } catch {}
      s.connection = null;
    }

    try {
      const lib = await getTikTokLib();
      const Conn = lib.TikTokLiveConnection || lib.WebcastPushConnection;
      const WebcastEvent = lib.WebcastEvent || {};
      const ControlEvent = lib.ControlEvent || {};
      const conn = new Conn(target, {
        processInitialData: false,
        enableExtendedGiftInfo: false,
        fetchRoomInfoOnConnect: true,
        ...(CONFIG.eulerApiKey ? { signApiKey: CONFIG.eulerApiKey } : {}),
      });
      s.connection = conn;

      conn.on(WebcastEvent.SHARE || 'share', (d) => recordSharer(s, d));
      conn.on(WebcastEvent.SOCIAL || 'social', (d) => {
        if (isSharePacket(d)) recordSharer(s, d);
      });
      const ended = (why) => () => {
        if (s.connection !== conn) return;
        s.state = { connected: false, username: target, roomId: null, error: why };
        s.broadcast({ type: 'STATUS', state: s.state });
      };
      conn.on(WebcastEvent.STREAM_END || 'streamEnd', ended('The LIVE ended'));
      conn.on(ControlEvent.DISCONNECTED || 'disconnected', ended('Disconnected from the LIVE'));
      conn.on('error', (err) => log.warn(`[${s.username}] Live error:`, err?.message || err));

      const st = await conn.connect();
      s.state = { connected: true, username: target, roomId: String(st?.roomId || ''), error: null };
      log.info(`[${s.username}] Listening to @${target}`);
      s.broadcast({ type: 'STATUS', state: s.state });
      res.json({ success: true, state: s.state });
    } catch (err) {
      const msg = /offline|not.*live|LIVE has ended|UserOfflineError/i.test(err?.message || '')
        ? `@${target} isn't live right now.`
        : err?.message || 'Could not connect to the LIVE.';
      s.connection = null;
      s.state = { connected: false, username: target, roomId: null, error: msg };
      s.broadcast({ type: 'STATUS', state: s.state });
      res.status(400).json({ error: msg });
    }
  })
);

app.post(
  '/api/tiktok/disconnect',
  wrap(async (req, res) => {
    const s = req.session;
    const conn = s.connection;
    s.connection = null;
    if (conn) {
      try {
        await conn.disconnect();
      } catch {}
    }
    s.state = { connected: false, username: null, roomId: null, error: null };
    s.broadcast({ type: 'STATUS', state: s.state });
    res.json({ success: true, state: s.state });
  })
);

app.post('/api/tiktok/clear', (req, res) => {
  req.session.sharers.clear();
  req.session.broadcast({ type: 'CLEAR' });
  res.json({ success: true });
});

app.use((err, req, res, _next) => {
  log.error('500:', err?.stack || err);
  res.status(500).json({ error: err?.message || 'Server error' });
});

// Low-memory mode: shut the browser down when nothing needs it. Accounts keep their cookies and
// reopen automatically on the next block. It stays open while a LIVE with auto-block is running.
setInterval(async () => {
  if (!CONFIG.lowMemory || !browserPromise) return;
  const busy = [...sessions.values()].some(
    (s) => s.queue.length || s.workers || s.opening || (s.state.connected && s.auth.autoBlockSharers && s.cookies)
  );
  if (busy) {
    lastBrowserUse = Date.now();
    return;
  }
  if (Date.now() - lastBrowserUse < CONFIG.browser.idleCloseMs) return;
  const b = await browserPromise.catch(() => null);
  browserPromise = null;
  for (const s of sessions.values()) {
    if (s.account) {
      s.account.ready = false;
      s.account = null;
    }
  }
  if (b) {
    log.info('No blocks for a while — closing the browser to free memory.');
    await b.close().catch(() => {});
  }
  setEngineStatus('idle', '', 0);
}, 20000).unref();

// Close idle browser contexts so Chrome doesn't hold memory for absent users.
setInterval(() => {
  const now = Date.now();
  for (const s of sessions.values()) {
    if (s.account && !s.sseClients.size && !s.queue.length && now - s.lastSeen > CONFIG.sessionIdleMs) {
      log.info(`[${s.username}] Idle — closing browser context.`);
      s.account.close();
      s.account = null; // cookies are kept; it reopens on the next block
    }
  }
}, 60000).unref();

const server = app.listen(CONFIG.port, () => {
  log.info(`NEXUS listening on :${CONFIG.port}`);
  // Find (or download) the browser right away so connecting an account is quick.
  resolveBrowserPath().catch((e) => log.error('Browser setup failed:', explainLaunchError(e)));
});

for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, async () => {
    server.close();
    for (const s of sessions.values()) await s.account?.close();
    const b = browserPromise && (await browserPromise.catch(() => null));
    if (b) await b.close().catch(() => {});
    process.exit(0);
  });
}
