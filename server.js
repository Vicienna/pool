// server.js — POOL chat studio · dual mode · zero dependency · Node 18.13+
'use strict';
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const os = require('node:os');
const { Readable } = require('node:stream');
const { spawn } = require('node:child_process');

if (typeof AbortSignal === 'undefined') { console.error('butuh Node 18.13+'); process.exit(1); }
if (typeof AbortSignal.timeout !== 'function') {
  AbortSignal.timeout = function(ms) { const c = new AbortController(); const t = setTimeout(() => c.abort(new Error('timeout')), ms); if (t.unref) t.unref(); return c.signal; };
}
if (typeof fetch !== 'function') { console.error('butuh Node 18.13+ (fetch)'); process.exit(1); }

const PORT = parseInt(process.env.PORT || '8080', 10);
const DIR = __dirname;
const POOL_FILE = process.env.POOL_FILE || path.join(DIR, 'pool.json');
const CHATS_FILE = path.join(DIR, 'chats.json');
const AGENTS_FILE = path.join(DIR, 'agents.json');
const PLUGINS_FILE = path.join(DIR, 'plugins.json');
const MCP_FILE = path.join(DIR, 'mcp.json');
const SANDBOX_DIR = path.join(DIR, 'sandbox');
const WORKSPACE_DIR = SANDBOX_DIR;

const DEFAULT_SETTINGS = {
  adminToken: process.env.ADMIN_TOKEN || null,
  defaultAlias: 'smart',
  upstreamTimeoutMs: parseInt(process.env.UPSTREAM_TIMEOUT_MS || '120000', 10),
  paidFallback: true, accent: 'mint', logLevel: 'info',
  probeConcurrency: 5, logBufferSize: 500, sseHeartbeatMs: 25000, maxBodyBytes: 33554432,
  enableShellTool: false, fallbackMode: 'priority',
  maxToolIterations: 8,
  maxContextChars: 2000000,
  searxngUrl: '',
  searchEngine: 'auto',
  searchPriority: 'bing,4get-ddg,yahoo,ecosia,yep,mojeek,librey,brave,marginalia,mwmbl,stract,searxng',
  searxngCustomUrls: [],
  forceThinkPrompt: false,
};

const SEED_PROVIDERS = {
  groq: { base: 'https://api.groq.com/openai/v1', tz: 'UTC', paid: false, needsAccountId: false, enabled: true, priority: 100, models: { smart: 'openai/gpt-oss-120b', fast: 'llama-3.1-8b-instant' } },
  cloudflare: { base: 'https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1', tz: 'UTC', paid: false, needsAccountId: true, enabled: true, priority: 80, models: { smart: '@cf/meta/llama-3.3-70b-instruct-fp8-fast', fast: '@cf/meta/llama-3.1-8b-instruct' } },
  zai: { base: 'https://api.z.ai/api/paas/v4', tz: 'UTC', paid: false, needsAccountId: false, enabled: true, priority: 70, models: { smart: 'glm-4.7-flash', fast: 'glm-4.5-flash' } },
  openrouter: { base: 'https://openrouter.ai/api/v1', tz: 'UTC', paid: false, needsAccountId: false, enabled: true, priority: 50, models: { smart: 'deepseek/deepseek-r1:free', fast: 'qwen/qwen3-8b:free' } },
  mistral: { base: 'https://api.mistral.ai/v1', tz: 'UTC', paid: false, needsAccountId: false, enabled: true, priority: 60, models: { smart: 'mistral-large-latest', fast: 'mistral-small-latest' } },
  deepseek: { base: 'https://api.deepseek.com/v1', tz: 'UTC', paid: true, needsAccountId: false, enabled: true, priority: 10, models: { smart: 'deepseek-reasoner', fast: 'deepseek-chat' } },
};

const CHAT_MODE_PROMPT = 'You are a helpful conversational AI assistant, similar to ChatGPT or Gemini.\n\nTools available to you:\n- builtin_web_search(query) — search the LIVE web. You MUST use this whenever the user asks about: news, current events, recent releases, prices, weather, sports scores, recent versions, "berita", "terbaru", "terkini", "sekarang", "hari ini", anything that could have changed after your training cutoff.\n- builtin_fetch_url(url) — read a webpage in full for details.\n- builtin_current_time() — current time in any timezone.\n- builtin_calculator(expression) — safe math evaluation.\n\nGuidelines:\n- Answer questions directly and conversationally.\n- Use markdown for formatting (headers, lists, code blocks, tables).\n- Be concise unless the user asks for depth.\n- NEVER say "I cannot access real-time information" or "Saya tidak bisa mengakses internet" — you CAN, via builtin_web_search. Just call it.\n- Match the user\'s language.\n- If tools cannot answer the question, say so honestly.';

const CHAT_MODE_TOOLS = ['builtin_web_search', 'builtin_fetch_url', 'builtin_current_time', 'builtin_calculator'];

const DEFAULT_AGENT = {
  id: 'ag_default', name: 'Default Assistant', description: 'Chat + agent helper.',
  systemPrompt: 'You are a helpful assistant. Use markdown. Answer in the user\'s language. Use tools when useful.',
  model: 'smart', mode: 'agent', temperature: 0.7,
  tools: ['builtin_web_search', 'builtin_fetch_url', 'builtin_current_time', 'builtin_calculator'],
  skills: [], enabled: true
};

let state = { version: 1, settings: { ...DEFAULT_SETTINGS }, providers: JSON.parse(JSON.stringify(SEED_PROVIDERS)), accounts: [], models: [] };
let chats = { conversations: [] };
let agents = { agents: [JSON.parse(JSON.stringify(DEFAULT_AGENT))], skills: [] };
let plugins = { plugins: [] };
let mcp = { servers: [] };
const pluginTools = new Map();
const mcpClients = new Map();
const mcpTools = new Map();

function atomicWrite(file, data) { const tmp = file + '.tmp'; try { fs.writeFileSync(tmp, typeof data === 'string' ? data : JSON.stringify(data, null, 2), 'utf8'); fs.renameSync(tmp, file); } catch (e) { try { fs.unlinkSync(tmp); } catch {} throw e; } }
function readJsonFile(file, fallback) { try { if (!fs.existsSync(file)) return fallback; const v = JSON.parse(fs.readFileSync(file, 'utf8')); return v && typeof v === 'object' ? v : fallback; } catch (e) { try { fs.copyFileSync(file, file + '.bak'); } catch {} return fallback; } }
let saveTimer = null, lastSaveAt = 0;
function scheduleSave(fn) { if (saveTimer) return; const delay = Math.max(50, 1000 - (Date.now() - lastSaveAt)); saveTimer = setTimeout(() => { saveTimer = null; lastSaveAt = Date.now(); try { fn(); } catch (e) { log('err','-','-','save', e.message); } }, delay); }

function loadAll() {
  if (!fs.existsSync(POOL_FILE)) atomicWrite(POOL_FILE, state);
  else {
    try {
      const parsed = readJsonFile(POOL_FILE, null);
      if (!parsed) throw new Error('corrupt');
      state = {
        version: parsed.version || 1,
        settings: { ...DEFAULT_SETTINGS, ...(parsed.settings || {}) },
        providers: Object.keys(parsed.providers || {}).length ? { ...JSON.parse(JSON.stringify(SEED_PROVIDERS)), ...parsed.providers } : JSON.parse(JSON.stringify(SEED_PROVIDERS)),
        accounts: Array.isArray(parsed.accounts) ? parsed.accounts : [],
        models: Array.isArray(parsed.models) ? parsed.models : [],
      };
    } catch (e) {
      log('warn','-','-','load-pool', 'corrupt — reseed');
      try { fs.copyFileSync(POOL_FILE, POOL_FILE + '.bak'); } catch {}
      atomicWrite(POOL_FILE, state);
    }
  }
  const c = readJsonFile(CHATS_FILE, { conversations: [] }); chats = { conversations: Array.isArray(c.conversations) ? c.conversations : [] };
  const a = readJsonFile(AGENTS_FILE, null); if (a && Array.isArray(a.agents)) agents = { agents: a.agents, skills: Array.isArray(a.skills) ? a.skills : [] }; else atomicWrite(AGENTS_FILE, agents);
  if (agents.agents.length === 0) agents.agents.push(JSON.parse(JSON.stringify(DEFAULT_AGENT)));
  for (const ag of agents.agents) { if (ag.mode !== 'chat' && ag.mode !== 'agent') ag.mode = 'agent'; }
  const p = readJsonFile(PLUGINS_FILE, { plugins: [] }); plugins = { plugins: Array.isArray(p.plugins) ? p.plugins : [] };
  const m = readJsonFile(MCP_FILE, { servers: [] }); mcp = { servers: Array.isArray(m.servers) ? m.servers : [] };
  try { fs.mkdirSync(SANDBOX_DIR, { recursive: true }); } catch {}
}

function migrateSettings() {
  var OLD_DEFAULT = 'bing,yahoo,mojeek,ddg,searxng';
  var NEW_DEFAULT = 'bing,4get-ddg,yahoo,ecosia,yep,mojeek,librey,brave,marginalia,mwmbl,stract,searxng';
  if (state.settings.searchEngine && state.settings.searchEngine !== 'auto' && (!state.settings.searchPriority || state.settings.searchPriority === OLD_DEFAULT)) {
    var all = ['bing', '4get-ddg', 'yahoo', 'ecosia', 'yep', 'mojeek', 'librey', 'brave', 'marginalia', 'mwmbl', 'stract', 'searxng'];
    var rest = all.filter(function(e) { return e !== state.settings.searchEngine; });
    state.settings.searchPriority = [state.settings.searchEngine].concat(rest).join(',');
  }
  if (!state.settings.searchPriority || state.settings.searchPriority === OLD_DEFAULT) {
    state.settings.searchPriority = NEW_DEFAULT;
  }
  if (!Array.isArray(state.settings.searxngCustomUrls)) state.settings.searxngCustomUrls = [];
}

function migrateModels() {
  if (!Array.isArray(state.models)) state.models = [];
  let _changed = false;
  for (const _m of state.models) { if (!Array.isArray(_m.labels)) { _m.labels = _m.label ? [_m.label] : ['general']; _changed = true; } }
  if (state.models.length > 0) { if (_changed) savePoolNow(); return; }
  for (const provName of Object.keys(state.providers)) {
    const prov = state.providers[provName];
    if (!prov || !prov.models) continue;
    for (const label of Object.keys(prov.models)) {
      const mid = prov.models[label];
      if (!mid) continue;
      state.models.push({
        id: 'mdl_' + crypto.randomBytes(4).toString('hex'),
        provider: provName, modelId: mid, label: label, labels: [label],
        input: label === 'vision' ? ['text', 'image'] : ['text'],
        output: ['text'],
        priority: label === 'smart' ? 100 : (label === 'fast' ? 80 : 70),
        enabled: true, contextLength: null, note: 'auto-seeded',
      });
    }
  }
  if (state.models.length > 0) savePoolNow();
}

const KNOWN_LABELS = ['smart','fast','vision','voice','coding','reasoning','translate','image-gen','general'];
function resolveRequest(spec, providerHint) {
  if (!spec || spec === 'auto') spec = state.settings.defaultAlias || 'smart';
  if (typeof spec === 'string' && spec.startsWith('mdl_')) {
    const m = (state.models || []).find(function(x){ return x.id === spec; });
    if (m) return { label: m.label, modelId: m.modelId, providerFilter: m.provider };
  }
  if (typeof spec === 'string' && spec.indexOf(':') > 0) {
    const idx = spec.indexOf(':');
    const a = spec.slice(0, idx), b = spec.slice(idx + 1);
    if (KNOWN_LABELS.indexOf(b) >= 0) return { label: b, providerFilter: a };
    return { modelId: b, providerFilter: a };
  }
  if (KNOWN_LABELS.indexOf(spec) >= 0) return { label: spec, providerFilter: providerHint || null };
  return { modelId: spec, providerFilter: providerHint || null };
}
function modelLabels(m) { if (!m) return []; if (Array.isArray(m.labels) && m.labels.length > 0) return m.labels; return m.label ? [m.label] : []; }
function modelHasLabel(m, label) { return modelLabels(m).indexOf(label) >= 0; }
function accountCanServeModel2(req, acc) {
  if (req.providerFilter && acc.provider !== req.providerFilter) return false;
  if (acc.model) return req.modelId ? acc.model === req.modelId : true;
  if (!Array.isArray(state.models)) return false;
  for (let i = 0; i < state.models.length; i++) {
    const m = state.models[i];
    if (!m.enabled || m.provider !== acc.provider) continue;
    if (req.modelId && m.modelId !== req.modelId) continue;
    if (req.label && !modelHasLabel(m, req.label)) continue;
    return true;
  }
  return false;
}
function getModelForAccount(req, acc) {
  if (acc.model) return { modelId: acc.model, label: req.label || 'override', priority: 100 };
  if (!Array.isArray(state.models)) return null;
  let best = null;
  for (let i = 0; i < state.models.length; i++) {
    const m = state.models[i];
    if (!m.enabled || m.provider !== acc.provider) continue;
    if (req.modelId && m.modelId !== req.modelId) continue;
    if (req.label && !modelHasLabel(m, req.label)) continue;
    if (!best || (m.priority || 0) > (best.priority || 0)) best = m;
  }
  return best;
}
function getModelContextLength(targetModel) {
  const req = resolveRequest(targetModel, null);
  let cl = 128000;
  const models = state.models || [];
  if (req.modelId) {
    for (let i = 0; i < models.length; i++) {
      const m = models[i];
      if (m.modelId === req.modelId && (!req.providerFilter || m.provider === req.providerFilter)) {
        if (typeof m.contextLength === 'number' && m.contextLength > 0) cl = m.contextLength;
        break;
      }
    }
  } else if (req.label) {
    let maxCl = 0;
    for (let i = 0; i < models.length; i++) {
      const m = models[i];
      if (modelHasLabel(m, req.label) && typeof m.contextLength === 'number' && m.contextLength > maxCl) maxCl = m.contextLength;
    }
    if (maxCl > 0) cl = maxCl;
  }
  return cl;
}

function savePool() { scheduleSave(() => atomicWrite(POOL_FILE, state)); }
function savePoolNow() { try { atomicWrite(POOL_FILE, state); } catch (e) { log('err','-','-','save-pool', e.message); } }
function saveChats() { scheduleSave(() => atomicWrite(CHATS_FILE, chats)); }
function saveChatsNow() { try { atomicWrite(CHATS_FILE, chats); } catch (e) { log('err','-','-','save-chats', e.message); } }
function saveAgents() { try { atomicWrite(AGENTS_FILE, agents); } catch (e) { log('err','-','-','save-agents', e.message); } }
function savePlugins() { try { atomicWrite(PLUGINS_FILE, plugins); } catch (e) { log('err','-','-','save-plugins', e.message); } }
function saveMCP() { try { atomicWrite(MCP_FILE, mcp); } catch (e) { log('err','-','-','save-mcp', e.message); } }

const logBuffer = [];
const logClients = new Set();
const LEVELS = { debug: 10, info: 20, warn: 30, err: 40 };
function log(level, provider, label, event, detail) {
  const lv = LEVELS[level] || 20;
  const cfg = (state.settings && state.settings.logLevel) || 'info';
  if (lv < (LEVELS[cfg] || 20)) return;
  const entry = { ts: new Date().toISOString(), level, provider: String(provider || '-').slice(0, 40), label: String(label || '-').slice(0, 60), event: String(event || '-').slice(0, 60), detail: detail == null ? '' : String(detail).slice(0, 400) };
  logBuffer.push(entry);
  const cap = Math.max(10, state.settings.logBufferSize || 500);
  if (logBuffer.length > cap) logBuffer.shift();
  process.stdout.write('[' + entry.ts + '] ' + entry.level + ' | ' + entry.provider + ' | ' + entry.label + ' | ' + entry.event + ' | ' + entry.detail + '\n');
  const payload = 'data: ' + JSON.stringify(entry) + '\n\n';
  for (const res of logClients) { try { res.write(payload); } catch { logClients.delete(res); } }
}

function tzOffsetMs(tz, date = new Date()) { try { const p = Object.fromEntries(new Intl.DateTimeFormat('en-US', { timeZone: tz, hour12: false, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }).formatToParts(date).map(x => [x.type, x.value])); return Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour % 24, +p.minute, +p.second) - date.getTime(); } catch { return 0; } }
function nextResetMs(tz) { const now = Date.now(), DAY = 86400000; try { const off = tzOffsetMs(tz || 'UTC', new Date(now)); const nextLocal = Math.floor((now + off) / DAY) * DAY + DAY + 5000; return nextLocal - tzOffsetMs(tz || 'UTC', new Date(nextLocal - off)); } catch { return Math.floor(now / DAY) * DAY + DAY + 5000; } }

function wakeup() { const now = Date.now(); let ch = false; for (const a of state.accounts) if (a.status === 'cooldown' && a.resetAt && a.resetAt <= now) { a.status = 'ready'; a.resetAt = 0; a.remaining = null; a.failCount = 0; ch = true; } if (ch) savePool(); }
function accountPriority(a) { if (typeof a.priority === 'number') return a.priority; const p = state.providers[a.provider]; return p ? (p.priority || 0) : 0; }
function isPaidAccount(a) { const p = state.providers[a.provider]; return !!(p && p.paid); }
function isQuota(status, h, low) { if (status === 429) return true; const r = h && h.get ? h.get('x-ratelimit-remaining-requests') : null; if (r === '0') return true; if (low) for (const kw of ['quota', 'rate limit', 'rate_limit', 'too many requests', 'exceeded', 'insufficient']) if (low.includes(kw)) return true; return false; }
function cooldown(acc, tz, reason, kind) { const paid = isPaidAccount(acc); acc.status = 'cooldown'; acc.resetAt = (kind === '5xx' && paid) ? Date.now() + 60000 : nextResetMs(tz || 'UTC'); savePool(); log('warn', acc.provider, acc.label || acc.id, 'cooldown', reason + ' until ' + new Date(acc.resetAt).toISOString()); }

function getFallbackCandidates(req, tried) {
  const scored = [];
  for (let ai = 0; ai < state.accounts.length; ai++) {
    const acc = state.accounts[ai];
    if (!acc.enabled || acc.status !== 'ready' || tried.has(acc.id)) continue;
    const prov = state.providers[acc.provider];
    if (!prov || !prov.enabled) continue;
    if (!accountCanServeModel2(req, acc)) continue;
    const model = getModelForAccount(req, acc);
    if (!model) continue;
    scored.push({ acc: acc, modelId: model.modelId, modelPriority: model.priority || 0, modelLabel: model.label || '?', modelLabels: modelLabels(model) });
  }
  let pool = scored;
  if (state.settings.paidFallback) {
    const free = scored.filter(function(x){ return !isPaidAccount(x.acc); });
    if (free.length > 0) pool = free;
  }
  const mode = state.settings.fallbackMode || 'priority';
  if (mode === 'round-robin') pool.sort(function(a, b) { return (a.acc.lastUsed || 0) - (b.acc.lastUsed || 0); });
  else if (mode === 'provider') pool.sort(function(a, b) {
    const pa = (state.providers[a.acc.provider] && state.providers[a.acc.provider].priority) || 0;
    const pb = (state.providers[b.acc.provider] && state.providers[b.acc.provider].priority) || 0;
    if (pb !== pa) return pb - pa;
    if (b.modelPriority !== a.modelPriority) return b.modelPriority - a.modelPriority;
    const aa = accountPriority(a.acc), ab = accountPriority(b.acc);
    if (ab !== aa) return ab - aa;
    return (a.acc.lastUsed || 0) - (b.acc.lastUsed || 0);
  });
  else pool.sort(function(a, b) {
    if (b.modelPriority !== a.modelPriority) return b.modelPriority - a.modelPriority;
    const aa = accountPriority(a.acc), ab = accountPriority(b.acc);
    if (ab !== aa) return ab - aa;
    return (a.acc.lastUsed || 0) - (b.acc.lastUsed || 0);
  });
  return pool;
}

async function callProvider(payload, providerHint) {
  wakeup();
  const tried = new Set(), attempted = [];
  let lastError = 'no account available';
  const maxAttempts = state.accounts.length + 5;
  for (let i = 0; i < maxAttempts; i++) {
    const _req = resolveRequest(payload.model, providerHint);
    const candidates = getFallbackCandidates(_req, tried);
    if (!candidates.length) break;
    const cand = candidates[0];
    const acc = cand.acc;
    tried.add(acc.id);
    attempted.push({ provider: acc.provider, label: acc.label || acc.id, id: acc.id, model: cand.modelId });
    const prov = state.providers[acc.provider];
    if (!prov || !prov.enabled) { lastError = 'provider ' + acc.provider + ' disabled'; continue; }
    const modelId = cand.modelId;
    if (!modelId) { lastError = 'no model'; continue; }
    let base = prov.base || '';
    if (prov.needsAccountId) { if (!acc.accountId) { lastError = 'no accountId'; continue; } base = base.replace('{account_id}', encodeURIComponent(acc.accountId)); }
    const url = base.replace(/\/+$/, '') + '/chat/completions';
    const up_payload = { ...payload, model: modelId }; delete up_payload.stream_options;
    let up;
    try {
      up = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + acc.apiKey }, body: JSON.stringify(up_payload), signal: AbortSignal.timeout(state.settings.upstreamTimeoutMs || 120000) });
    } catch (e) {
      acc.failCount = (acc.failCount || 0) + 1;
      if (acc.failCount >= 3) acc.status = 'dead'; else cooldown(acc, prov.tz, 'net: ' + e.message, '5xx');
      savePool(); lastError = 'net (' + (acc.label || acc.id) + '): ' + e.message; continue;
    }
    if (up.status >= 200 && up.status < 300) {
      acc.lastUsed = Date.now(); acc.failCount = 0;
      const rem = up.headers.get('x-ratelimit-remaining-requests');
      if (rem !== null) { const r = parseInt(rem, 10); acc.remaining = isNaN(r) ? null : r; if (!isNaN(r) && r <= 1) { acc.status = 'cooldown'; acc.resetAt = nextResetMs(prov.tz); } }
      savePool();
      return { ok: true, account: acc, model: modelId, response: up, attempted };
    }
    let text = ''; try { text = await up.text(); } catch {}
    const lower = text.toLowerCase();
    if (isQuota(up.status, up.headers, lower)) { acc.remaining = 0; cooldown(acc, prov.tz, 'quota', 'quota'); lastError = 'quota (' + (acc.label || acc.id) + ')'; continue; }
    if (up.status === 401 || up.status === 403) {
      acc.authFails = (acc.authFails || 0) + 1; acc.failCount = (acc.failCount || 0) + 1;
      acc.lastError = up.status + ' auth: ' + text.slice(0, 180);
      if (acc.authFails >= 3) acc.status = 'dead'; else { acc.status = 'cooldown'; acc.resetAt = Date.now() + 120000; }
      savePool(); lastError = 'auth ' + up.status + ' (' + (acc.label || acc.id) + ')'; continue;
    }
    if (up.status >= 500) {
      acc.failCount = (acc.failCount || 0) + 1;
      if (acc.failCount >= 3) acc.status = 'dead'; else cooldown(acc, prov.tz, 'srv ' + up.status, '5xx');
      savePool(); lastError = 'srv ' + up.status; continue;
    }
    acc.lastUsed = Date.now(); savePool();
    return { ok: false, error: 'upstream ' + up.status + ': ' + text.slice(0, 300), attempted };
  }
  return { ok: false, error: lastError, attempted };
}

function jsonResponse(res, status, body) { res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(body)); }
function readBody(req, maxSize = 33554432) { return new Promise((resolve, reject) => { const chunks = []; let size = 0; req.on('data', c => { size += c.length; if (size > maxSize) { req.destroy(); reject(new Error('body too large')); return; } chunks.push(c); }); req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8'))); req.on('error', reject); }); }
async function readJson(req) { const t = await readBody(req, state.settings.maxBodyBytes || 33554432); if (!t) return {}; try { return JSON.parse(t); } catch { throw new Error('Invalid JSON body'); } }
function genId(prefix) { return prefix + '_' + crypto.randomBytes(4).toString('hex'); }
function redactPoolState() { const c = JSON.parse(JSON.stringify(state)); for (const a of c.accounts) if (a.apiKey) a.apiKey = String(a.apiKey).slice(0, 8) + '...'; return c; }
function checkAuth(req, res, u) { const token = state.settings.adminToken; if (!token) return true; const auth = req.headers['authorization'] || ''; if (auth === 'Bearer ' + token) return true; if (u && u.searchParams.get('token') === token) return true; jsonResponse(res, 401, { error: { message: 'Unauthorized' } }); return false; }

async function handleChatCompletions(req, res, bodyStr) {
  let payload; try { payload = JSON.parse(bodyStr || '{}'); } catch { return jsonResponse(res, 400, { error: { message: 'Invalid JSON body' } }); }
  let modelSpec = payload.model || state.settings.defaultAlias || 'smart';
  let providerHint = null, explicitModel = null;
  if (typeof modelSpec === 'string' && modelSpec.indexOf(':') > 0) { const ci = modelSpec.indexOf(':'); providerHint = modelSpec.slice(0, ci); explicitModel = modelSpec.slice(ci + 1); }
  const wantStream = !!payload.stream;
  const call = await callProvider({ ...payload, model: explicitModel || modelSpec, stream: wantStream }, providerHint);
  if (!call.ok) return jsonResponse(res, 429, { error: { message: 'Semua akun habis kuota.', detail: call.error, attempted: call.attempted || [] } });
  const acc = call.account, up = call.response;
  log('info', acc.provider, acc.label || acc.id, 'success', 'model=' + payload.model + ' status=' + up.status);
  if (wantStream) {
    res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no', 'x-served-by': acc.provider, 'x-served-account': acc.label || acc.id });
    if (!up.body) { res.end(); return; }
    const ns = Readable.fromWeb(up.body);
    ns.on('error', () => { try { res.end(); } catch {} });
    res.on('close', () => { try { up.body.cancel(); } catch {} });
    ns.pipe(res); return;
  }
  const text = await up.text();
  res.writeHead(200, { 'Content-Type': up.headers.get('content-type') || 'application/json', 'x-served-by': acc.provider, 'x-served-account': acc.label || acc.id });
  res.end(text);
}

async function* parseSSE(webStream) {
  const reader = webStream.getReader(); const dec = new TextDecoder(); let buf = '';
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      let idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        const block = buf.slice(0, idx); buf = buf.slice(idx + 2);
        const lines = block.split('\n');
        for (let i = 0; i < lines.length; i++) {
          if (lines[i].startsWith('data: ')) {
            const data = lines[i].slice(6).trim();
            if (data === '[DONE]') return;
            try { yield JSON.parse(data); } catch {}
          }
        }
      }
    }
  } finally { try { reader.releaseLock(); } catch {} }
}

// ─── BUILTIN TOOLS ────────────────────────────────────────────────────────
const BUILTIN_TOOLS = new Map();
function registerBuiltin(def, handler) { BUILTIN_TOOLS.set(def.function.name, { def, handler }); }

registerBuiltin({ type: 'function', function: { name: 'builtin_calculator', description: 'Safe math expression. Supports + - * / % ( ) and decimals.', parameters: { type: 'object', properties: { expression: { type: 'string' } }, required: ['expression'] } } }, async ({ expression }) => {
  const s = String(expression || '');
  if (!/^[\d\s+\-*/().%]+$/.test(s)) throw new Error('invalid chars');
  const v = new Function('"use strict"; return (' + s + ');')();
  if (typeof v !== 'number' || !isFinite(v)) throw new Error('not finite');
  return String(v);
});
registerBuiltin({ type: 'function', function: { name: 'builtin_current_time', description: 'Current time in ISO and IANA timezone.', parameters: { type: 'object', properties: { timezone: { type: 'string' } } } } }, async ({ timezone }) => {
  const tz = timezone || 'UTC'; const now = new Date(); let local;
  try { local = now.toLocaleString('en-US', { timeZone: tz, hour12: false }); } catch { local = now.toISOString(); }
  return 'iso=' + now.toISOString() + '\nunix=' + Math.floor(now.getTime() / 1000) + '\ntz=' + tz + '\nlocal=' + local;
});
registerBuiltin({ type: 'function', function: { name: 'builtin_http_fetch', description: 'Fetch URL (GET/POST). Returns status, body (max 8KB).', parameters: { type: 'object', properties: { url: { type: 'string' }, method: { type: 'string' }, headers: { type: 'object' }, body: { type: 'string' } }, required: ['url'] } } }, async ({ url, method, headers, body }) => {
  if (!/^https?:\/\//i.test(url || '')) throw new Error('only http(s)');
  const r = await fetch(url, { method: method || 'GET', headers: headers || {}, body: body || undefined, signal: AbortSignal.timeout(15000) });
  const t = await r.text();
  return 'status=' + r.status + '\nbody:\n' + t.slice(0, 8000) + (t.length > 8000 ? '\n...truncated' : '');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_base64', description: 'Encode/decode base64.', parameters: { type: 'object', properties: { op: { type: 'string', enum: ['encode', 'decode'] }, text: { type: 'string' } }, required: ['op', 'text'] } } }, async ({ op, text }) => {
  if (op === 'encode') return Buffer.from(String(text), 'utf8').toString('base64');
  if (op === 'decode') return Buffer.from(String(text), 'base64').toString('utf8');
  throw new Error('bad op');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_json_query', description: 'Parse JSON, return path "a.b[0].c".', parameters: { type: 'object', properties: { json: { type: 'string' }, path: { type: 'string' } }, required: ['json'] } } }, async ({ json, path: p }) => {
  let v; try { v = JSON.parse(json); } catch (e) { throw new Error('bad json'); }
  if (!p) return JSON.stringify(v);
  for (const k of String(p).replace(/\[(\d+)\]/g, '.$1').split('.').filter(Boolean)) { if (v == null) return 'null'; v = v[k]; }
  return typeof v === 'string' ? v : JSON.stringify(v);
});
registerBuiltin({ type: 'function', function: { name: 'builtin_file_read', description: 'Read file inside sandbox only (legacy).', parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } } }, async ({ path: rel }) => {
  const abs = path.resolve(SANDBOX_DIR, String(rel || ''));
  if (!abs.startsWith(SANDBOX_DIR + path.sep) && abs !== SANDBOX_DIR) throw new Error('outside sandbox');
  if (!fs.existsSync(abs)) throw new Error('not found');
  const b = fs.readFileSync(abs);
  if (b.length > 65536) throw new Error('too large');
  return b.toString('utf8');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_file_write', description: 'Write file inside sandbox only (legacy).', parameters: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] } } }, async ({ path: rel, content }) => {
  const abs = path.resolve(SANDBOX_DIR, String(rel || ''));
  if (!abs.startsWith(SANDBOX_DIR + path.sep)) throw new Error('outside sandbox');
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  const c = String(content || '');
  if (c.length > 262144) throw new Error('too large');
  fs.writeFileSync(abs, c, 'utf8');
  return 'OK wrote ' + rel + ' (' + Buffer.byteLength(c) + ' bytes)';
});
registerBuiltin({ type: 'function', function: { name: 'builtin_shell', description: 'Shell cmd in sandbox. DISABLED unless enabled in Settings.', parameters: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } } }, async ({ command }) => {
  if (!state.settings.enableShellTool) throw new Error('shell tool disabled');
  const cmd = String(command || '');
  if (cmd.length > 2000) throw new Error('too long');
  return await new Promise((resolve, reject) => {
    const sh = process.platform === 'win32' ? 'cmd.exe' : '/bin/sh';
    const args = process.platform === 'win32' ? ['/c', cmd] : ['-c', cmd];
    const proc = spawn(sh, args, { cwd: SANDBOX_DIR, timeout: 20000 });
    let out = '', err = '';
    proc.stdout.on('data', d => { out += d.toString(); if (out.length > 16384) try { proc.kill(); } catch {} });
    proc.stderr.on('data', d => { err += d.toString(); if (err.length > 16384) try { proc.kill(); } catch {} });
    proc.on('close', code => resolve('exit=' + code + '\nstdout:\n' + out.slice(0, 16000) + '\nstderr:\n' + err.slice(0, 16000)));
    proc.on('error', reject);
  });
});

// ─── CODING AGENT TOOLS ──────────────────────────────────────────────────
function resolveInWorkspace(rel, mustExist) {
  const raw = String(rel == null ? '' : rel);
  if (raw.indexOf('\0') >= 0) throw new Error('invalid path');
  const abs = path.resolve(WORKSPACE_DIR, raw || '.');
  const rootWithSep = WORKSPACE_DIR + path.sep;
  if (abs !== WORKSPACE_DIR && !abs.startsWith(rootWithSep)) throw new Error('path outside workspace: ' + raw);
  try {
    if (fs.existsSync(abs)) {
      const real = fs.realpathSync(abs);
      if (real !== WORKSPACE_DIR && !real.startsWith(rootWithSep)) throw new Error('symlink escape blocked');
    } else {
      const parent = path.dirname(abs);
      if (fs.existsSync(parent)) {
        const realParent = fs.realpathSync(parent);
        if (realParent !== WORKSPACE_DIR && !realParent.startsWith(rootWithSep)) throw new Error('symlink escape blocked');
      }
    }
  } catch (e) { if (String(e.message).indexOf('escape') >= 0) throw e; }
  if (mustExist && !fs.existsSync(abs)) throw new Error('not found: ' + raw);
  return abs;
}
function globToRegex(glob) {
  let g = String(glob);
  g = g.replace(/[.+^\u0024{}()|[\]\\]/g, '\\$&');
  g = g.replace(/\*\*/g, '<<SS>>');
  g = g.replace(/\*/g, '[^/]*');
  g = g.replace(/<<SS>>/g, '.*');
  g = g.replace(/\?/g, '.');
  return new RegExp('^' + g + '\u0024');
}
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_list', description: 'List files/folders in workspace. Optional recursive (max depth 5).', parameters: { type: 'object', properties: { path: { type: 'string', description: 'relative path, default "."' }, recursive: { type: 'boolean' } }, required: [] } } }, async ({ path: p, recursive }) => {
  const abs = resolveInWorkspace(p || '.', true);
  const st = fs.statSync(abs);
  if (!st.isDirectory()) throw new Error('not a directory: ' + p);
  const out = [];
  (function walk(dir, prefix, depth) {
    if (depth > 5 || out.length > 500) return;
    let items; try { items = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const d of items) {
      if (d.name === '.git' || d.name === 'node_modules' || d.name === '.cache') continue;
      const full = path.join(dir, d.name);
      const relP = prefix ? prefix + '/' + d.name : d.name;
      let s = null; try { s = fs.statSync(full); } catch {}
      if (d.isDirectory()) { out.push({ name: relP, type: 'dir' }); if (recursive) walk(full, relP, depth + 1); }
      else out.push({ name: relP, type: 'file', size: s ? s.size : 0 });
      if (out.length > 500) return;
    }
  })(abs, '', 0);
  if (out.length === 0) return 'Folder kosong: ' + (p || '.');
  const lines = out.map(function(e) { return (e.type === 'dir' ? 'D ' : 'F ') + e.name + (e.type === 'file' ? ' (' + (e.size > 1024 ? (e.size/1024).toFixed(1)+'K' : e.size+'B') + ')' : ''); });
  return (p || '.') + ' (' + out.length + '):\n' + lines.join('\n');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_read', description: 'Read a text file with line numbers. Default 300 lines, max 500.', parameters: { type: 'object', properties: { path: { type: 'string' }, offset: { type: 'number', description: '0-based line start' }, limit: { type: 'number', description: 'max lines, default 300' } }, required: ['path'] } } }, async ({ path: p, offset, limit }) => {
  const abs = resolveInWorkspace(p, true);
  const st = fs.statSync(abs);
  if (!st.isFile()) throw new Error('not a file: ' + p);
  if (st.size > 2097152) throw new Error('file too large (max 2MB)');
  const raw = fs.readFileSync(abs, 'utf8');
  const lines = raw.split('\n');
  const off = Math.max(0, parseInt(offset, 10) || 0);
  const lim = Math.min(500, Math.max(1, parseInt(limit, 10) || 300));
  const slice = lines.slice(off, off + lim);
  const numbered = slice.map((l, i) => String(off + i + 1).padStart(4, ' ') + '| ' + l).join('\n');
  const more = lines.length > off + lim ? '\n... use offset=' + (off+lim) + ' to read more (total ' + lines.length + ' lines)' : '';
  return '[File: ' + p + ' | ' + (off+1) + '-' + (off+slice.length) + '/' + lines.length + ' lines]\n' + numbered + more;
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_write', description: 'Create or overwrite a file (creates parent dirs).', parameters: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] } } }, async ({ path: p, content }) => {
  const abs = resolveInWorkspace(p, false);
  fs.mkdirSync(path.dirname(abs), { recursive: true });
  const c = String(content == null ? '' : content);
  if (c.length > 1048576) throw new Error('content too large (max 1MB)');
  fs.writeFileSync(abs, c, 'utf8');
  return 'OK wrote ' + p + ' (' + Buffer.byteLength(c) + ' bytes)';
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_edit', description: 'Surgical edit: replace first occurrence of old_string. Fails if not found or appears >1 time (use replaceAll=true).', parameters: { type: 'object', properties: { path: { type: 'string' }, old_string: { type: 'string' }, new_string: { type: 'string' }, replaceAll: { type: 'boolean' } }, required: ['path', 'old_string', 'new_string'] } } }, async ({ path: p, old_string, new_string, replaceAll }) => {
  const abs = resolveInWorkspace(p, true);
  const st = fs.statSync(abs);
  if (!st.isFile()) throw new Error('not a file');
  if (st.size > 2097152) throw new Error('file too large');
  const orig = fs.readFileSync(abs, 'utf8');
  const oldS = String(old_string == null ? '' : old_string);
  const newS = String(new_string == null ? '' : new_string);
  if (!oldS) throw new Error('old_string required');
  if (oldS === newS) throw new Error('no-op');
  const firstIdx = orig.indexOf(oldS);
  if (firstIdx < 0) throw new Error('old_string not found in ' + p);
  const count = orig.split(oldS).length - 1;
  if (!replaceAll && count > 1) throw new Error('old_string appears ' + count + ' times — be more specific or replaceAll=true');
  let updated;
  if (replaceAll) updated = orig.split(oldS).join(newS);
  else updated = orig.slice(0, firstIdx) + newS + orig.slice(firstIdx + oldS.length);
  fs.writeFileSync(abs, updated, 'utf8');
  return 'OK edited ' + p + ' (' + (replaceAll ? count : 1) + ' replacement' + ((replaceAll ? count : 1) > 1 ? 's' : '') + ')';
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_glob', description: 'Find files by glob pattern ("*.js", "**/*.py").', parameters: { type: 'object', properties: { pattern: { type: 'string' }, maxResults: { type: 'number' } }, required: ['pattern'] } } }, async ({ pattern, maxResults }) => {
  const pat = String(pattern || '*');
  const limit = Math.min(500, Math.max(1, parseInt(maxResults, 10) || 200));
  const re = globToRegex(pat);
  const out = [];
  (function walk(dir, prefix, depth) {
    if (depth > 10 || out.length >= limit) return;
    let items; try { items = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const d of items) {
      if (d.name === '.git' || d.name === 'node_modules') continue;
      const full = path.join(dir, d.name);
      const rel = prefix ? prefix + '/' + d.name : d.name;
      if (d.isDirectory()) walk(full, rel, depth + 1);
      else if (re.test(rel)) { out.push(rel); if (out.length >= limit) return; }
    }
  })(WORKSPACE_DIR, '', 0);
  if (out.length === 0) return 'No matches for ' + pat;
  return 'Found ' + out.length + ' match' + (out.length > 1 ? 'es' : '') + ' for ' + pat + ':\n' + out.join('\n') + (out.length >= limit ? '\n... (truncated)' : '');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_grep', description: 'Search files by regex. Returns file:line:col + text.', parameters: { type: 'object', properties: { pattern: { type: 'string' }, pathFilter: { type: 'string' }, maxResults: { type: 'number' }, contextLines: { type: 'number' } }, required: ['pattern'] } } }, async ({ pattern, pathFilter, maxResults, contextLines }) => {
  let re;
  try { re = new RegExp(pattern); } catch (e) { throw new Error('invalid regex: ' + e.message); }
  const limit = Math.min(200, Math.max(1, parseInt(maxResults, 10) || 100));
  const ctx = Math.min(5, Math.max(0, parseInt(contextLines, 10) || 0));
  const pathRe = pathFilter ? globToRegex(pathFilter) : null;
  const out = [];
  (function walk(dir, prefix, depth) {
    if (depth > 10 || out.length >= limit) return;
    let items; try { items = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const d of items) {
      if (d.name === '.git' || d.name === 'node_modules') continue;
      const full = path.join(dir, d.name);
      const rel = prefix ? prefix + '/' + d.name : d.name;
      if (d.isDirectory()) { walk(full, rel, depth + 1); continue; }
      if (pathRe && !pathRe.test(rel)) continue;
      let st; try { st = fs.statSync(full); } catch { continue; }
      if (st.size > 1048576) continue;
      let txt; try { txt = fs.readFileSync(full, 'utf8'); } catch { continue; }
      const lines = txt.split('\n');
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        const m = re.exec(line);
        if (!m) continue;
        out.push(rel + ':' + (i+1) + ':' + (m.index+1) + '  ' + line.slice(0, 200));
        if (out.length >= limit) return;
      }
    }
  })(WORKSPACE_DIR, '', 0);
  if (out.length === 0) return 'No matches for ' + pattern;
  return 'Matches (max ' + limit + '):\n' + out.join('\n') + (out.length >= limit ? '\n... (truncated)' : '');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_mkdir', description: 'Create directory (recursive) inside workspace.', parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } } }, async ({ path: p }) => {
  const abs = resolveInWorkspace(p, false);
  fs.mkdirSync(abs, { recursive: true });
  return 'OK created ' + p;
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_move', description: 'Move or rename file/dir inside workspace.', parameters: { type: 'object', properties: { from: { type: 'string' }, to: { type: 'string' } }, required: ['from', 'to'] } } }, async ({ from, to }) => {
  const srcPath = resolveInWorkspace(from, true);
  const dst = resolveInWorkspace(to, false);
  fs.mkdirSync(path.dirname(dst), { recursive: true });
  fs.renameSync(srcPath, dst);
  return 'OK moved ' + from + ' -> ' + to;
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fs_rm', description: 'Delete file or empty dir. Set recursive=true for non-empty dir.', parameters: { type: 'object', properties: { path: { type: 'string' }, recursive: { type: 'boolean' } }, required: ['path'] } } }, async ({ path: p, recursive }) => {
  if (!p || p === '.' || p === '/' || p === '') throw new Error('refuse to delete workspace root');
  const abs = resolveInWorkspace(p, true);
  const st = fs.statSync(abs);
  if (st.isDirectory()) {
    if (!recursive) { if (fs.readdirSync(abs).length > 0) throw new Error('dir not empty — set recursive=true'); fs.rmdirSync(abs); }
    else fs.rmSync(abs, { recursive: true, force: true });
  } else fs.unlinkSync(abs);
  return 'OK deleted ' + p;
});
registerBuiltin({ type: 'function', function: { name: 'builtin_run', description: 'Run shell command in workspace (requires "Enable shell tool"). Returns exitCode, stdout, stderr.', parameters: { type: 'object', properties: { command: { type: 'string' }, cwd: { type: 'string' }, timeoutMs: { type: 'number' } }, required: ['command'] } } }, async ({ command, cwd, timeoutMs }) => {
  if (!state.settings.enableShellTool) throw new Error('builtin_run disabled — enable "shell tool" in Settings');
  const cmd = String(command || '').trim();
  if (!cmd) throw new Error('command required');
  if (cmd.length > 4000) throw new Error('command too long');
  if (/(?:^|\s)(rm\s+-rf\s+\/|mkfs|dd\s+if=|:\(\)\{)/.test(cmd)) throw new Error('blocked pattern');
  const cwdAbs = resolveInWorkspace(cwd || '.', true);
  const to = Math.min(120000, Math.max(1000, parseInt(timeoutMs, 10) || 30000));
  return await new Promise((resolve, reject) => {
    const sh = process.platform === 'win32' ? 'cmd.exe' : '/bin/sh';
    const args = process.platform === 'win32' ? ['/c', cmd] : ['-c', cmd];
    const proc = spawn(sh, args, { cwd: cwdAbs, timeout: to, env: { ...process.env, CI: '1' } });
    let out = '', err = '';
    proc.stdout.on('data', d => { out += d.toString(); if (out.length > 32768) try { proc.kill(); } catch {} });
    proc.stderr.on('data', d => { err += d.toString(); if (err.length > 32768) try { proc.kill(); } catch {} });
    proc.on('close', code => resolve('exit=' + code + '\nstdout:\n' + out.slice(0, 16000) + '\nstderr:\n' + err.slice(0, 16000)));
    proc.on('error', reject);
  });
});
const GIT_ALLOWED = new Set(['status', 'diff', 'log', 'show', 'branch', 'rev-parse', 'ls-files', 'blame', 'add', 'commit', 'checkout', 'stash', 'reset', 'restore', 'init', 'remote', 'fetch', 'pull', 'push', 'tag']);
registerBuiltin({ type: 'function', function: { name: 'builtin_git', description: 'Run git subcommand in workspace (requires shell enabled). Blocked: reset --hard, push --force, checkout -- ., clean -f.', parameters: { type: 'object', properties: { subcommand: { type: 'string' }, args: { type: 'array', items: { type: 'string' } } }, required: ['subcommand'] } } }, async ({ subcommand, args }) => {
  if (!state.settings.enableShellTool) throw new Error('builtin_git disabled — enable "shell tool" in Settings');
  const sub = String(subcommand || '').trim();
  if (!GIT_ALLOWED.has(sub)) throw new Error('git subcommand not allowed: ' + sub);
  const arr = Array.isArray(args) ? args.map(String) : [];
  const joined = sub + ' ' + arr.join(' ');
  if (/\breset\s+--hard\b/.test(joined)) throw new Error('blocked: reset --hard');
  if (/\bpush\s+.*--force\b/.test(joined) || /\bpush\s+-f\b/.test(joined)) throw new Error('blocked: push --force');
  if (/\bcheckout\s+--\s+\./.test(joined)) throw new Error('blocked: checkout -- .');
  if (/\bclean\s+-fdx?\b/.test(joined)) throw new Error('blocked: clean -f');
  return await new Promise((resolve, reject) => {
    const proc = spawn('git', [sub].concat(arr), { cwd: WORKSPACE_DIR, timeout: 30000 });
    let out = '', err = '';
    proc.stdout.on('data', d => { out += d.toString(); if (out.length > 32768) try { proc.kill(); } catch {} });
    proc.stderr.on('data', d => { err += d.toString(); if (err.length > 32768) try { proc.kill(); } catch {} });
    proc.on('close', code => resolve('exit=' + code + '\nstdout:\n' + out.slice(0, 16000) + '\nstderr:\n' + err.slice(0, 16000)));
    proc.on('error', reject);
  });
});
const _todos = new Map();
registerBuiltin({ type: 'function', function: { name: 'builtin_todo', description: 'Per-conversation task tracker. op=read|write|add. Tasks: {text, status: pending|in_progress|done}.', parameters: { type: 'object', properties: { op: { type: 'string', enum: ['read', 'write', 'add'] }, tasks: { type: 'array', items: { type: 'object' } } }, required: ['op'] } } }, async (a, ctx) => {
  const key = (ctx && ctx.conv && ctx.conv.id) || '_default';
  const norm = (t) => ({ text: String(t.text || '').slice(0, 500), status: ['pending', 'in_progress', 'done'].includes(t.status) ? t.status : 'pending' });
  const list = _todos.get(key) || [];
  if (a.op === 'read') return JSON.stringify(list);
  const incoming = Array.isArray(a.tasks) ? a.tasks.map(norm) : [];
  const next = a.op === 'write' ? incoming : list.concat(incoming);
  _todos.set(key, next);
  return 'OK ' + next.length + ' tasks:\n' + next.map(function(t){ return '[' + t.status + '] ' + t.text; }).join('\n');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_env', description: 'Environment info: workspace, platform, node version, safe env vars.', parameters: { type: 'object', properties: {} } } }, async () => {
  const ALLOW = ['PATH', 'HOME', 'USERPROFILE', 'USER', 'USERNAME', 'SHELL', 'OS', 'TEMP', 'TMP', 'LANG', 'COMPUTERNAME'];
  const safe = {};
  for (const k of ALLOW) if (process.env[k]) safe[k] = process.env[k];
  return 'workspace=' + WORKSPACE_DIR + '\ncwd=' + process.cwd() + '\nplatform=' + process.platform + '/' + process.arch + '\nnode=' + process.version + '\nshell_enabled=' + !!state.settings.enableShellTool + '\nenv=' + JSON.stringify(safe);
});

// ─── WEB SEARCH (all keyless) ─────────────────────────────────────────────
function decodeHtmlEntities(s) {
  return String(s || '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&#x27;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&#(\d+);/g, function(_, n) { try { return String.fromCharCode(parseInt(n, 10)); } catch { return _; } })
    .replace(/&#x([0-9a-f]+);/gi, function(_, n) { try { return String.fromCharCode(parseInt(n, 16)); } catch { return _; } });
}
function stripTags(s) { return decodeHtmlEntities(String(s || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim()); }

async function searchBing(query, maxResults) {
  const url = 'https://www.bing.com/search?q=' + encodeURIComponent(query) + '&count=' + Math.min(30, Math.max(10, maxResults)) + '&setlang=id&cc=ID';
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 'Accept': 'text/html,application/xhtml+xml', 'Accept-Language': 'id-ID,id;q=0.9,en;q=0.8' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Bing HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const parts = html.split('<li class="b_algo"').slice(1);
  for (const part of parts) {
    if (results.length >= maxResults) break;
    const linkMatch = part.match(/<h2[^>]*>\s*<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/);
    if (!linkMatch) continue;
    let href = linkMatch[1].replace(/&amp;/g, '&');
    if (href.indexOf('/ck/a?') === 0 || href.indexOf('https://www.bing.com/ck/') === 0) {
      const uMatch = href.match(/[?&]u=a1([A-Za-z0-9_-]+)/);
      if (uMatch) { try { let b64 = uMatch[1].replace(/-/g, '+').replace(/_/g, '/'); while (b64.length % 4) b64 += '='; href = Buffer.from(b64, 'base64').toString('utf8'); } catch {} }
    }
    if (!/^https?:\/\//i.test(href)) continue;
    if (href.indexOf('bing.com') >= 0) continue;
    const title = stripTags(linkMatch[2]) || href;
    const snMatch = part.match(/<p[^>]*>([\s\S]*?)<\/p>/);
    results.push({ title, url: href, snippet: snMatch ? stripTags(snMatch[1]).slice(0, 400) : '' });
  }
  return results;
}

async function searchYahoo(query, maxResults) {
  const url = 'https://id.search.yahoo.com/search?p=' + encodeURIComponent(query) + '&n=' + Math.min(20, Math.max(10, maxResults));
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Yahoo HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const parts = html.split(/<div[^>]+class="[^"]*\balgo\b[^"]*"/).slice(1);
  for (const part of parts) {
    if (results.length >= maxResults) break;
    const linkMatch = part.match(/<h3[^>]*>[\s\S]*?<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/);
    if (!linkMatch) continue;
    let href = linkMatch[1];
    if (href.indexOf('r.search.yahoo.com') >= 0 || href.indexOf('/RU=') >= 0) { const ruMatch = href.match(/\/RU=([^/]+)\//); if (ruMatch) { try { href = decodeURIComponent(ruMatch[1]); } catch {} } }
    if (!/^https?:\/\//i.test(href)) continue;
    if (href.indexOf('yahoo.com') >= 0) continue;
    const title = stripTags(linkMatch[2]) || href;
    const snMatch = part.match(/<div[^>]*class="[^"]*compText[^"]*"[^>]*>([\s\S]*?)<\/div>/);
    results.push({ title, url: href, snippet: snMatch ? stripTags(snMatch[1]).slice(0, 400) : '' });
  }
  return results;
}

async function searchMojeek(query, maxResults) {
  const url = 'https://www.mojeek.com/search?q=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Mojeek HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const re = /<li[^>]*>\s*<a[^>]+class="ob"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>\s*<p class="s">([\s\S]*?)<\/p>/g;
  let m;
  while ((m = re.exec(html)) !== null && results.length < maxResults) {
    let href = m[1]; if (href.indexOf('//') === 0) href = 'https:' + href;
    if (!/^https?:\/\//i.test(href)) continue;
    if (href.indexOf('mojeek.com') >= 0) continue;
    results.push({ title: stripTags(m[2]) || href, url: href, snippet: stripTags(m[3]).slice(0, 400) });
  }
  return results;
}

async function searchDdgLite(query, maxResults) {
  const url = 'https://lite.duckduckgo.com/lite/?q=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('DDG lite HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const parts = html.split(/<a[^>]+class=['"]result-link['"]/).slice(1);
  for (const part of parts) {
    if (results.length >= maxResults) break;
    const urlMatch = part.match(/href="([^"]+)"/); if (!urlMatch) continue;
    let href = urlMatch[1];
    if (href.indexOf('//duckduckgo.com/l/?uddg=') >= 0) { const u = href.match(/uddg=([^&]+)/); if (u) { try { href = decodeURIComponent(u[1]); } catch {} } }
    if (!/^https?:\/\//i.test(href)) continue;
    if (href.indexOf('duckduckgo.com') >= 0) continue;
    const titleMatch = part.match(/>([\s\S]*?)<\/a>/);
    const snMatch = part.match(/<td[^>]*class=['"]result-snippet['"][^>]*>([\s\S]*?)<\/td>/);
    results.push({ title: titleMatch ? stripTags(titleMatch[1]) : href, url: href, snippet: snMatch ? stripTags(snMatch[1]).slice(0, 400) : '' });
  }
  return results;
}

async function searchSearxng(baseUrl, query, maxResults) {
  const base = baseUrl.replace(/\/+$/, '');
  const url = base + '/search?q=' + encodeURIComponent(query) + '&format=json&safesearch=0';
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; pool-websearch/1.0)', 'Accept': 'application/json, text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('SearXNG HTTP ' + r.status);
  const ct = (r.headers.get('content-type') || '').toLowerCase();
  const text = await r.text();
  if (ct.includes('text/html') || text.trim().startsWith('<')) {
    const results = [];
    const parts = text.split(/<article[^>]+class="[^"]*\bresult\b[^"]*"/).slice(1);
    for (const part of parts) {
      if (results.length >= maxResults) break;
      const linkMatch = part.match(/<a[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/); if (!linkMatch) continue;
      if (!/^https?:\/\//i.test(linkMatch[1])) continue;
      const snMatch = part.match(/<p[^>]*class="[^"]*content[^"]*"[^>]*>([\s\S]*?)<\/p>/);
      results.push({ title: stripTags(linkMatch[2]), url: linkMatch[1], snippet: snMatch ? stripTags(snMatch[1]).slice(0, 400) : '' });
    }
    if (results.length > 0) return results;
    throw new Error('SearXNG returned HTML (JSON API disabled on this instance).');
  }
  let data; try { data = JSON.parse(text); } catch (e) { throw new Error('SearXNG non-JSON: ' + text.slice(0, 100)); }
  return (Array.isArray(data.results) ? data.results : []).slice(0, maxResults).map(function(item) { return { title: String(item.title || item.url || ''), url: String(item.url || ''), snippet: String(item.content || item.snippet || '') }; }).filter(function(r) { return r.url && r.url.startsWith('http'); });
}

async function search4get(query, maxResults, scraper) {
  const url = 'https://4get.ca/api/v1/web?s=' + encodeURIComponent(query) + (scraper ? '&scraper=' + scraper : '');
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('4get HTTP ' + r.status);
  const d = await r.json();
  const arr = d.web || d.results || [];
  return arr.slice(0, maxResults).map(function(x) {
    return { title: stripTags(x.title || ''), url: String(x.url || ''), snippet: stripTags(x.description || x.snippet || '').slice(0, 400) };
  }).filter(function(x) { return /^https?:\/\//i.test(x.url); });
}
async function searchStract(query, maxResults) {
  const r = await fetch('https://stract.com/beta/api/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0' },
    body: JSON.stringify({ query: query, numResults: Math.min(20, maxResults) }),
    signal: AbortSignal.timeout(15000),
  });
  if (!r.ok) throw new Error('Stract HTTP ' + r.status);
  const d = await r.json();
  const arr = d.webpages || d.results || [];
  return arr.slice(0, maxResults).map(function(x) {
    return { title: stripTags(x.title || ''), url: String(x.url || ''), snippet: stripTags(x.description || x.snippet || '').slice(0, 400) };
  }).filter(function(x) { return /^https?:\/\//i.test(x.url); });
}
async function searchEcosia(query, maxResults) {
  const url = 'https://www.ecosia.org/search?q=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Ecosia HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const parts = html.split(/<article[^>]+class="[^"]*result[^"]*"/).slice(1);
  for (const part of parts) {
    if (results.length >= maxResults) break;
    const lm = part.match(/<a[^>]+href="(https?:\/\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/);
    if (!lm) continue;
    if (lm[1].indexOf('ecosia.org') >= 0) continue;
    const sm = part.match(/<p[^>]*>([\s\S]*?)<\/p>/);
    results.push({ title: stripTags(lm[2]) || lm[1], url: lm[1], snippet: sm ? stripTags(sm[1]).slice(0, 400) : '' });
  }
  return results;
}
async function searchBraveHtml(query, maxResults) {
  const url = 'https://search.brave.com/search?q=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Brave HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const parts = html.split(/<div[^>]+class="[^"]*snippet[^"]*"/).slice(1);
  for (const part of parts) {
    if (results.length >= maxResults) break;
    const lm = part.match(/<a[^>]+href="(https?:\/\/[^"]+)"/);
    if (!lm || lm[1].indexOf('brave.com') >= 0) continue;
    const tm = part.match(/<div[^>]+class="[^"]*title[^"]*"[^>]*>([\s\S]*?)<\/div>/);
    const sm = part.match(/<div[^>]+class="[^"]*snippet-description[^"]*"[^>]*>([\s\S]*?)<\/div>/);
    results.push({ title: tm ? stripTags(tm[1]) : lm[1], url: lm[1], snippet: sm ? stripTags(sm[1]).slice(0, 400) : '' });
  }
  return results;
}
async function searchYep(query, maxResults) {
  const url = 'https://api.yep.com/fs/2/search?client=web&gl=ID&limit=' + Math.min(20, maxResults) + '&no_correct=false&q=' + encodeURIComponent(query) + '&safeSearch=off&type=web';
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Yep HTTP ' + r.status);
  const d = await r.json();
  const arr = d.results || d.web || [];
  return arr.slice(0, maxResults).map(function(x) {
    return { title: stripTags(x.title || ''), url: String(x.url || ''), snippet: stripTags(x.snippet || x.description || '').slice(0, 400) };
  }).filter(function(x) { return /^https?:\/\//i.test(x.url); });
}
async function searchLibreY(query, maxResults) {
  const url = 'https://librey.org/api.php?q=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('LibreY HTTP ' + r.status);
  const d = await r.json();
  const arr = d.results || d.web || [];
  return arr.slice(0, maxResults).map(function(x) {
    return { title: stripTags(x.title || ''), url: String(x.url || ''), snippet: stripTags(x.description || x.snippet || '').slice(0, 400) };
  }).filter(function(x) { return /^https?:\/\//i.test(x.url); });
}
async function searchMarginalia(query, maxResults) {
  const url = 'https://search.marginalia.nu/search?query=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Marginalia HTTP ' + r.status);
  const html = await r.text();
  const results = [];
  const re = /<a[^>]+href="(https?:\/\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/g;
  let m;
  while ((m = re.exec(html)) !== null && results.length < maxResults) {
    if (m[1].indexOf('marginalia.nu') >= 0 || m[1].indexOf('github.com/Margin') >= 0) continue;
    const t = stripTags(m[2]);
    if (!t || t.length < 3) continue;
    results.push({ title: t, url: m[1], snippet: '' });
  }
  return results;
}
async function searchMwmbl(query, maxResults) {
  const url = 'https://mwmbl.org/api/v1/search/?s=' + encodeURIComponent(query);
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }, signal: AbortSignal.timeout(15000) });
  if (!r.ok) throw new Error('Mwmbl HTTP ' + r.status);
  const d = await r.json();
  const arr = d.results || [];
  return arr.slice(0, maxResults).map(function(x) {
    return { title: stripTags(x.title || ''), url: String(x.url || ''), snippet: stripTags(x.extract || x.description || '').slice(0, 400) };
  }).filter(function(x) { return /^https?:\/\//i.test(x.url); });
}

const SEARCH_ENGINES = {
  bing: searchBing, yahoo: searchYahoo, mojeek: searchMojeek, ddg: searchDdgLite,
  searxng: function(q, n) { return searchSearxng(state.settings.searxngUrl, q, n); },
  '4get-ddg': function(q, n) { return search4get(q, n, 'ddg'); },
  '4get-brave': function(q, n) { return search4get(q, n, 'brave'); },
  '4get-google': function(q, n) { return search4get(q, n, 'google'); },
  stract: searchStract, ecosia: searchEcosia, brave: searchBraveHtml,
  yep: searchYep, librey: searchLibreY, marginalia: searchMarginalia, mwmbl: searchMwmbl,
};

async function webSearch(query, maxResults) {
  const priStr = state.settings.searchPriority || 'bing,yahoo,mojeek,ddg,searxng';
  const order = priStr.split(',').map(function(s) { return s.trim(); }).filter(Boolean);
  const errors = [];
  for (let i = 0; i < order.length; i++) {
    const name = order[i];
    if (name === 'searxng' && !state.settings.searxngUrl) { errors.push('searxng: no url configured'); continue; }
    const fn = SEARCH_ENGINES[name];
    if (!fn) { errors.push(name + ': unknown engine'); continue; }
    try {
      const results = await fn(query, maxResults);
      if (results && results.length > 0) return { engine: name, results };
      errors.push(name + ': 0 results');
    } catch (e) { errors.push(name + ': ' + e.message.slice(0, 80)); }
  }
  throw new Error('Semua search engine gagal:\n' + errors.join('\n'));
}

registerBuiltin({ type: 'function', function: { name: 'builtin_web_search', description: 'Search the web. No API key. Cascades through configured engines.', parameters: { type: 'object', properties: { query: { type: 'string' }, maxResults: { type: 'number', description: 'default 5, max 10' } }, required: ['query'] } } }, async ({ query, maxResults }) => {
  const q = String(query || '').trim(); if (!q) throw new Error('query required');
  const n = Math.min(10, Math.max(1, parseInt(maxResults, 10) || 5));
  const res = await webSearch(q, n);
  const lines = ['Search results (' + res.engine + ') for: ' + q, ''];
  res.results.forEach(function(r, i) {
    lines.push('[' + (i + 1) + '] ' + r.title);
    lines.push('URL: ' + r.url);
    if (r.snippet) lines.push('Snippet: ' + r.snippet.slice(0, 400));
    lines.push('');
  });
  lines.push('(To read a page, call builtin_fetch_url with the URL.)');
  return lines.join('\n');
});
registerBuiltin({ type: 'function', function: { name: 'builtin_fetch_url', description: 'Fetch a URL and return readable text (HTML stripped). Max 8KB.', parameters: { type: 'object', properties: { url: { type: 'string' }, maxChars: { type: 'number', description: 'default 8000, max 20000' } }, required: ['url'] } } }, async ({ url, maxChars }) => {
  const u = String(url || '').trim(); if (!/^https?:\/\//i.test(u)) throw new Error('only http(s) allowed');
  const cap = Math.min(20000, Math.max(500, parseInt(maxChars, 10) || 8000));
  const r = await fetch(u, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' }, signal: AbortSignal.timeout(20000) });
  if (!r.ok) throw new Error('HTTP ' + r.status);
  const ct = (r.headers.get('content-type') || '').toLowerCase();
  const raw = await r.text();
  let text;
  if (ct.indexOf('text/html') >= 0) { text = raw.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ').replace(/<noscript[\s\S]*?<\/noscript>/gi, ' ').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim(); }
  else { text = raw; }
  if (text.length > cap) text = text.slice(0, cap) + '\n... [truncated]';
  return 'URL: ' + u + '\n\n' + text;
});

// ─── PLUGIN LOADER ────────────────────────────────────────────────────────
function loadPlugins() {
  pluginTools.clear();
  for (const pl of plugins.plugins) {
    if (!pl.enabled) continue;
    try {
      const sandboxRequire = (m) => { if (m === 'node:fs' || m === 'fs') return fs; if (m === 'node:path' || m === 'path') return path; if (m === 'node:crypto' || m === 'crypto') return crypto; if (m === 'node:os' || m === 'os') return os; if (m === 'node:child_process' || m === 'child_process') return require('node:child_process'); if (m === 'node:http' || m === 'http') return http; throw new Error('module not allowed: ' + m); };
      const modObj = { exports: {} };
      const fn = new Function('module', 'exports', 'require', 'console', 'Buffer', 'fetch', 'AbortSignal', '"use strict";\n' + pl.source + '\n;return module.exports;');
      const exp = fn.call(modObj.exports, modObj, modObj.exports, sandboxRequire, console, Buffer, fetch, AbortSignal);
      if (!exp || !Array.isArray(exp.tools)) { log('warn', 'plugin', pl.name, 'load', 'no tools'); continue; }
      for (const t of exp.tools) {
        if (!t || !t.name || typeof t.handler !== 'function') continue;
        const full = 'plugin__' + pl.id.replace(/[^a-z0-9_]/gi, '') + '__' + String(t.name).replace(/[^a-z0-9_]/gi, '_');
        pluginTools.set(full, { def: { type: 'function', function: { name: full, description: t.description || '', parameters: t.parameters || { type: 'object', properties: {} } } }, handler: t.handler.bind(t), pluginId: pl.id });
      }
      log('info', 'plugin', pl.name, 'loaded', exp.tools.length + ' tools');
    } catch (e) { log('err', 'plugin', pl.name, 'load-error', e.message); }
  }
}

// ─── MCP ──────────────────────────────────────────────────────────────────
function sanitizeToolName(s) { return String(s || '').replace(/[^a-z0-9_]/gi, '_'); }
function createStdioMCPClient(cfg) {
  const proc = spawn(cfg.command, cfg.args || [], { env: { ...process.env, ...(cfg.env || {}) }, stdio: ['pipe', 'pipe', 'pipe'], shell: process.platform === 'win32' });
  let buf = '', nextId = 1, exited = false; const pending = new Map();
  proc.stdout.on('data', d => { buf += d.toString(); let idx; while ((idx = buf.indexOf('\n')) >= 0) { const line = buf.slice(0, idx).trim(); buf = buf.slice(idx + 1); if (!line) continue; try { const msg = JSON.parse(line); if (msg.id && pending.has(msg.id)) { const { resolve, reject } = pending.get(msg.id); pending.delete(msg.id); if (msg.error) reject(new Error(msg.error.message || 'err')); else resolve(msg.result); } } catch {} } });
  proc.stderr.on('data', d => log('debug', 'mcp', cfg.name, 'stderr', d.toString().slice(0, 200)));
  proc.on('exit', () => { exited = true; for (const { reject } of pending.values()) reject(new Error('mcp exited')); pending.clear(); });
  function request(method, params) { if (exited) return Promise.reject(new Error('exited')); const id = nextId++; return new Promise((resolve, reject) => { pending.set(id, { resolve, reject }); try { proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); } catch (e) { pending.delete(id); reject(e); return; } setTimeout(() => { if (pending.has(id)) { pending.delete(id); reject(new Error('timeout')); } }, 30000); }); }
  return { request, async initialize() { const r = await request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'pool', version: '1.0.0' } }); try { proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n'); } catch {} return r; }, async listTools() { const r = await request('tools/list', {}); return r.tools || []; }, async callTool(n, a) { return await request('tools/call', { name: n, arguments: a }); }, close() { try { proc.kill(); } catch {} }, get isAlive() { return !exited; } };
}
function createHttpMCPClient(cfg) {
  let nextId = 1, sessionId = null;
  async function request(method, params) { const id = nextId++; const headers = { 'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream', ...(cfg.headers || {}) }; if (sessionId) headers['Mcp-Session-Id'] = sessionId; const res = await fetch(cfg.url, { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id, method, params }), signal: AbortSignal.timeout(30000) }); const sid = res.headers.get('mcp-session-id'); if (sid) sessionId = sid; if (!res.ok) throw new Error('mcp ' + res.status); const ct = res.headers.get('content-type') || ''; if (ct.includes('text/event-stream')) { for await (const chunk of parseSSE(res.body)) if (chunk.id === id) { if (chunk.error) throw new Error(chunk.error.message); return chunk.result; } throw new Error('no resp'); } const msg = await res.json(); if (msg.error) throw new Error(msg.error.message); return msg.result; }
  return { request, async initialize() { return await request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'pool', version: '1.0.0' } }); }, async listTools() { const r = await request('tools/list', {}); return r.tools || []; }, async callTool(n, a) { return await request('tools/call', { name: n, arguments: a }); }, close() {}, get isAlive() { return true; } };
}
async function connectMCP(serverCfg) {
  if (mcpClients.has(serverCfg.id)) { try { mcpClients.get(serverCfg.id).close(); } catch {} mcpClients.delete(serverCfg.id); }
  for (const [name, t] of mcpTools) if (t.serverId === serverCfg.id) mcpTools.delete(name);
  let client;
  try {
    if (serverCfg.transport === 'stdio') client = createStdioMCPClient(serverCfg);
    else if (serverCfg.transport === 'http') client = createHttpMCPClient(serverCfg);
    else throw new Error('bad transport');
    await client.initialize();
    const tools = await client.listTools();
    const sn = sanitizeToolName(serverCfg.name);
    for (const t of tools) {
      const full = 'mcp__' + sn + '__' + sanitizeToolName(t.name);
      mcpTools.set(full, { def: { type: 'function', function: { name: full, description: t.description || '', parameters: t.inputSchema || { type: 'object', properties: {} } } }, serverId: serverCfg.id, originalName: t.name, client });
    }
    mcpClients.set(serverCfg.id, client);
    serverCfg.status = 'connected'; serverCfg.tools = tools.map(t => ({ name: t.name, fullName: 'mcp__' + sn + '__' + sanitizeToolName(t.name), description: t.description || '' })); serverCfg.lastError = null; saveMCP();
    log('info', 'mcp', serverCfg.name, 'connected', tools.length + ' tools');
    return { ok: true, tools: serverCfg.tools };
  } catch (e) {
    if (client) try { client.close(); } catch {}
    serverCfg.status = 'error'; serverCfg.lastError = e.message; serverCfg.tools = []; saveMCP();
    log('err', 'mcp', serverCfg.name, 'fail', e.message);
    return { ok: false, error: e.message };
  }
}
async function connectAllMCP() { for (const s of mcp.servers) if (s.enabled) await connectMCP(s); }

function listAllTools() {
  const out = [];
  for (const [, t] of BUILTIN_TOOLS) out.push({ ...t.def, source: 'builtin' });
  for (const [, t] of pluginTools) out.push({ ...t.def, source: 'plugin', pluginId: t.pluginId });
  for (const [, t] of mcpTools) out.push({ ...t.def, source: 'mcp', serverId: t.serverId });
  return out;
}
function resolveTools(names) {
  const out = [];
  for (const n of names) {
    if (BUILTIN_TOOLS.has(n)) out.push(BUILTIN_TOOLS.get(n).def);
    else if (pluginTools.has(n)) out.push(pluginTools.get(n).def);
    else if (mcpTools.has(n)) out.push(mcpTools.get(n).def);
  }
  return out;
}
async function executeTool(name, args, ctx) {
  if (BUILTIN_TOOLS.has(name)) return await BUILTIN_TOOLS.get(name).handler(args || {}, ctx);
  if (pluginTools.has(name)) return await pluginTools.get(name).handler(args || {}, ctx);
  if (mcpTools.has(name)) { const t = mcpTools.get(name); return await t.client.callTool(t.originalName, args || {}); }
  throw new Error('unknown tool: ' + name);
}

function buildContentParts(text, attachments) {
  if (!Array.isArray(attachments) || attachments.length === 0) return text;
  const parts = [{ type: 'text', text: text || '' }];
  let hasNonText = false; let totalSize = 0; const MAX_ATT_SIZE = 3000000;
  for (let i = 0; i < attachments.length; i++) {
    const att = attachments[i];
    if (att.type === 'image' && att.dataUrl) {
      if (totalSize + att.dataUrl.length > MAX_ATT_SIZE) { parts.push({ type: 'text', text: '\n\n[Image ' + (att.name||'') + ' skipped: too large]' }); continue; }
      parts.push({ type: 'image_url', image_url: { url: att.dataUrl } }); hasNonText = true; totalSize += att.dataUrl.length;
    } else if (att.type === 'text' && att.text) {
      const remaining = Math.max(0, MAX_ATT_SIZE - totalSize);
      const content = att.text.length > remaining ? att.text.slice(0, remaining) + '\n[truncated]' : att.text;
      parts.push({ type: 'text', text: '\n\n[File: ' + (att.name || 'file') + ']\n' + content + '\n[/File]\n' });
      totalSize += content.length;
    }
  }
  if (!hasNonText) return parts.map(function(p){ return p.text || ''; }).join('');
  return parts;
}
function sanitizeAttachmentsForHistory(attachments) {
  if (!Array.isArray(attachments) || attachments.length === 0) return undefined;
  return attachments.map(function(att) {
    return { type: att.type, name: String(att.name || '').slice(0, 200), mime: String(att.mime || ''), size: Number(att.size) || 0, dataUrl: (att.dataUrl && att.dataUrl.length < 300000) ? att.dataUrl : null, text: (att.text && att.text.length < 50000) ? att.text.slice(0, 50000) : null, truncated: (att.type === 'image' && att.dataUrl && att.dataUrl.length >= 300000) || (att.type === 'text' && att.text && att.text.length >= 50000) };
  });
}

function trimHistory(rawMsgs, budgetChars) {
  const sys = rawMsgs.filter(m => m.role === 'system');
  const nonSys = rawMsgs.filter(m => m.role !== 'system');
  for (const m of nonSys) {
    if (typeof m.content === 'string' && m.content.length > 50000) m.content = m.content.slice(0, 50000) + '\n... [truncated]';
  }
  let total = 0; let startIdx = nonSys.length;
  for (let i = nonSys.length - 1; i >= 0; i--) {
    const len = typeof nonSys[i].content === 'string' ? nonSys[i].content.length : 100;
    if (total + len > budgetChars && startIdx < nonSys.length) break;
    total += len; startIdx = i;
  }
  while (startIdx < nonSys.length && nonSys[startIdx].role === 'tool') startIdx++;
  let kept = nonSys.slice(startIdx);
  const validIds = new Set();
  for (const m of kept) if (m.role === 'assistant' && m.tool_calls) for (const tc of m.tool_calls) validIds.add(tc.id);
  kept = kept.filter(m => m.role !== 'tool' || validIds.has(m.tool_call_id));
  const resultIds = new Set(kept.filter(m => m.role === 'tool').map(m => m.tool_call_id));
  kept = kept.map(m => {
    if (m.role === 'assistant' && m.tool_calls) {
      const valid = m.tool_calls.filter(tc => resultIds.has(tc.id));
      if (valid.length === 0) return { role: 'assistant', content: m.content || '(no content)' };
      if (valid.length < m.tool_calls.length) return { ...m, tool_calls: valid };
    }
    return m;
  });
  return [...sys, ...kept];
}

// ─── ORCHESTRATOR (dual mode) ─────────────────────────────────────────────
async function orchestrateChat(req, res) {
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  const { conversationId, agentId, message, attachments, modelOverride, modeOverride } = body;
  if (!message || typeof message !== 'string') return jsonResponse(res, 400, { error: { message: 'message required' } });

  let conv = conversationId ? chats.conversations.find(c => c.id === conversationId) : null;
  if (!conv) { conv = { id: genId('cv'), title: message.slice(0, 60), agentId: agentId || 'ag_default', messages: [], createdAt: Date.now(), updatedAt: Date.now(), modeOverride: null }; chats.conversations.push(conv); }
  const agent = agents.agents.find(a => a.id === (agentId || conv.agentId)) || agents.agents[0];
  if (!agent) return jsonResponse(res, 400, { error: { message: 'no agent' } });
  conv.agentId = agent.id;

  const requestedMode = (modeOverride === 'chat' || modeOverride === 'agent') ? modeOverride : null;
  const effectiveMode = requestedMode || conv.modeOverride || agent.mode || 'agent';
  const isChatMode = effectiveMode === 'chat';
  if (requestedMode) conv.modeOverride = requestedMode;

  let systemPrompt;
  let tools;
  const FORCE_THINK_INSTRUCTION = '\n\nIMPORTANT: Before answering, think step by step. Output your internal reasoning inside \u003cthink\u003e...\u003c/think\u003e tags. Then output your final answer OUTSIDE those tags. Example: \u003cthink\u003eLet me analyze...\u003c/think\u003eHere is the answer: ...';
  if (isChatMode) {
    systemPrompt = CHAT_MODE_PROMPT;
    tools = resolveTools(CHAT_MODE_TOOLS);
  } else {
    systemPrompt = agent.systemPrompt || '';
    for (const skId of (agent.skills || [])) { const sk = agents.skills.find(s => s.id === skId); if (sk && sk.systemPrompt) systemPrompt += '\n\n## Skill: ' + sk.name + '\n' + sk.systemPrompt; }
    const toolSet = new Set(agent.tools || []);
    for (const skId of (agent.skills || [])) { const sk = agents.skills.find(s => s.id === skId); if (sk) for (const t of (sk.tools || [])) toolSet.add(t); }
    tools = resolveTools([...toolSet]);
  }

  const targetModel = modelOverride || agent.model || 'smart';
  const modelCtx = getModelContextLength(targetModel);
  const globalCap = state.settings.maxContextChars || 2000000;
  const budgetChars = Math.min(modelCtx * 3, globalCap);

  const POISON_RE = [/saya tidak akan memanggil tool/i, /i will not call (a )?tool/i, /jangan panggil tool/i, /do not call (a )?tool/i, /tidak dapat memberikan berita.*real.?time/i, /cannot provide real.?time/i, /tidak menggunakan pencarian web/i, /tidak bisa mengakses internet/i, /tidak akan memanggil tool lagi/i];
  function isPoisoned(t) { for (var _i = 0; _i < POISON_RE.length; _i++) if (POISON_RE[_i].test(String(t || ''))) return true; return false; }
  const rawMsgs = [];
  if (systemPrompt && state.settings.forceThinkPrompt) systemPrompt += FORCE_THINK_INSTRUCTION;
  if (systemPrompt) rawMsgs.push({ role: 'system', content: systemPrompt });
  for (const m of conv.messages) {
    if (m.role === 'assistant' && !m.tool_calls && isPoisoned(m.content)) continue;
    const e = { role: m.role, content: m.content };
    if (m.tool_calls) e.tool_calls = m.tool_calls;
    if (m.tool_call_id) e.tool_call_id = m.tool_call_id;
    rawMsgs.push(e);
  }
  rawMsgs.push({ role: 'user', content: buildContentParts(message, attachments) });
  const msgs = trimHistory(rawMsgs, budgetChars);

  conv.messages.push({ role: 'user', content: message, attachments: sanitizeAttachmentsForHistory(attachments), ts: Date.now(), mode: effectiveMode });

  res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
  const sse = (o) => { try { res.write('data: ' + JSON.stringify(o) + '\n\n'); } catch {} };
  sse({ type: 'start', convId: conv.id, agentId: agent.id, mode: effectiveMode, contextLength: modelCtx });

  const MAX_ITER = isChatMode ? 3 : Math.max(1, Math.min(20, parseInt(state.settings.maxToolIterations, 10) || 8));
  let forcedReply = false;
  let switchedModel = false;

  try {
    let iter = 0;
    while (iter++ < MAX_ITER) {
      const payload = { model: targetModel, messages: msgs.slice(), temperature: typeof agent.temperature === 'number' ? agent.temperature : 0.7, stream: true };
      if (tools.length > 0) { payload.tools = tools; payload.tool_choice = 'auto'; }
      const call = await callProvider(payload);
      if (!call.ok) { sse({ type: 'error', message: call.error, attempted: call.attempted }); break; }
      const acc = call.account, up = call.response;
      sse({ type: 'served', provider: acc.provider, account: acc.label || acc.id, model: call.model });

      const ct = up.headers.get('content-type') || '';
      let text = ''; let reasoning = ''; const tcMap = new Map(); let hasFirstChunk = false;
      let inThinkBlock = false;

      if (ct.includes('text/event-stream')) {
        for await (const chunk of parseSSE(up.body)) {
          hasFirstChunk = true;
          const ch = chunk.choices && chunk.choices[0]; if (!ch) continue;
          const d = ch.delta || {};
          var _rc = d.reasoning_content || d.reasoning || d.thinking || null;
          if (_rc) { reasoning += _rc; sse({ type: 'reasoning', content: _rc }); }
          if (d.content) {
            var _newContent = '', _newReasoning = '', _c = d.content, _i = 0;
            while (_i < _c.length) {
              if (!inThinkBlock) {
                var _openIdx = _c.indexOf('\u003cthink\u003e', _i);
                if (_openIdx === -1) { _newContent += _c.slice(_i); _i = _c.length; break; }
                _newContent += _c.slice(_i, _openIdx);
                inThinkBlock = true;
                _i = _openIdx + 7;
              } else {
                var _closeIdx = _c.indexOf('\u003c/think\u003e', _i);
                if (_closeIdx === -1) { _newReasoning += _c.slice(_i); _i = _c.length; break; }
                _newReasoning += _c.slice(_i, _closeIdx);
                inThinkBlock = false;
                _i = _closeIdx + 8;
              }
            }
            if (_newReasoning) { reasoning += _newReasoning; sse({ type: 'reasoning', content: _newReasoning }); }
            if (_newContent) { text += _newContent; sse({ type: 'token', content: _newContent }); }
          }
          if (d.tool_calls) {
            for (const tc of d.tool_calls) {
              const i = tc.index ?? 0;
              if (!tcMap.has(i)) tcMap.set(i, { id: '', name: '', arguments: '' });
              const cur = tcMap.get(i);
              if (tc.id) cur.id = tc.id;
              if (tc.function && tc.function.name) cur.name += tc.function.name;
              if (tc.function && tc.function.arguments) cur.arguments += tc.function.arguments;
            }
          }
        }
      } else {
        const json = await up.json(); hasFirstChunk = true;
        const ch = json.choices && json.choices[0];
        if (ch) {
          if (ch.message && (ch.message.reasoning_content || ch.message.reasoning)) {
            reasoning = ch.message.reasoning_content || ch.message.reasoning;
            sse({ type: 'reasoning', content: reasoning });
          }
          if (ch.message && ch.message.content) {
            var _ct = ch.message.content;
            var _tm = _ct.match(/^\u003cthink\u003e([\s\S]*?)\u003c\/think\u003e\s*([\s\S]*)$/);
            if (_tm) { reasoning += _tm[1]; text = _tm[2]; sse({ type: 'reasoning', content: _tm[1] }); sse({ type: 'token', content: _tm[2] }); }
            else { text = _ct; sse({ type: 'token', content: text }); }
          }
          if (ch.message && ch.message.tool_calls) ch.message.tool_calls.forEach((tc, i) => tcMap.set(i, { id: tc.id, name: tc.function.name, arguments: tc.function.arguments }));
        }
      }

      let tcList = [...tcMap.values()];
      if (isChatMode && tcList.length > 0) tcList = [];

      if (!text && tcList.length === 0) {
        if (!forcedReply) {
          forcedReply = true;
          var _plainMsgs = msgs.filter(function(m) { return m.role !== 'tool'; }).map(function(m) {
            if (m.role === 'assistant' && m.tool_calls) return { role: 'assistant', content: m.content || '(tool call skipped)' };
            return m;
          });
          _plainMsgs.push({ role: 'user', content: 'Answer the user directly in plain text using your knowledge. Do not call any tools. Be concise.' });
          sse({ type: 'token', content: '*(model blank tanpa tools, retry plain-text)*\n\n' });
          try {
            var _plainCall = await callProvider({ model: targetModel, messages: _plainMsgs, temperature: typeof agent.temperature === 'number' ? agent.temperature : 0.7, stream: true });
            if (_plainCall.ok) {
              var _pAcc = _plainCall.account, _pUp = _plainCall.response;
              var _pCt = _pUp.headers.get('content-type') || '';
              var _pText = '';
              sse({ type: 'served', provider: _pAcc.provider, account: _pAcc.label || _pAcc.id, model: _plainCall.model });
              if (_pCt.includes('text/event-stream')) {
                for await (var _pc of parseSSE(_pUp.body)) {
                  var _pch = _pc.choices && _pc.choices[0]; if (!_pch) continue;
                  var _pd = _pch.delta || {};
                  if (_pd.content) { _pText += _pd.content; sse({ type: 'token', content: _pd.content }); }
                }
              } else {
                var _pj = await _pUp.json(); var _pchj = _pj.choices && _pj.choices[0];
                if (_pchj && _pchj.message && _pchj.message.content) { _pText = _pchj.message.content; sse({ type: 'token', content: _pText }); }
              }
              if (_pText) {
                var _pm = { role: 'assistant', content: _pText, ts: Date.now(), mode: effectiveMode, note: 'plain-text fallback' };
                conv.messages.push(_pm);
                break;
              }
            }
          } catch (_e) { log('warn', '-', '-', 'plain-retry-fail', _e.message); }
        }
        if (!switchedModel) {
          switchedModel = true;
          var _fallbackModel = (state.settings.defaultAlias || 'smart');
          if (_fallbackModel === targetModel) _fallbackModel = 'fast';
          sse({ type: 'token', content: '*(model masih blank, fallback ke ' + _fallbackModel + ')*\n\n' });
          try {
            var _fbCall = await callProvider({ model: _fallbackModel, messages: msgs, temperature: typeof agent.temperature === 'number' ? agent.temperature : 0.7, stream: true });
            if (_fbCall.ok) {
              var _fbAcc = _fbCall.account, _fbUp = _fbCall.response;
              var _fbCt = _fbUp.headers.get('content-type') || '';
              var _fbText = '';
              sse({ type: 'served', provider: _fbAcc.provider, account: _fbAcc.label || _fbAcc.id, model: _fbCall.model });
              if (_fbCt.includes('text/event-stream')) {
                for await (var _fc of parseSSE(_fbUp.body)) {
                  var _fch = _fc.choices && _fc.choices[0]; if (!_fch) continue;
                  var _fd = _fch.delta || {};
                  if (_fd.content) { _fbText += _fd.content; sse({ type: 'token', content: _fd.content }); }
                }
              } else {
                var _fj = await _fbUp.json(); var _fchj = _fj.choices && _fj.choices[0];
                if (_fchj && _fchj.message && _fchj.message.content) { _fbText = _fchj.message.content; sse({ type: 'token', content: _fbText }); }
              }
              if (_fbText) {
                var _fbm = { role: 'assistant', content: _fbText, ts: Date.now(), mode: effectiveMode, note: 'model fallback: ' + _fallbackModel };
                conv.messages.push(_fbm);
                break;
              }
            }
          } catch (_e2) { log('warn', '-', '-', 'model-fallback-fail', _e2.message); }
        }
        sse({ type: 'error', message: 'Model ' + targetModel + ' blank 3x (normal → tanpa tools → fallback model). Saran: ganti Model ke openai/gpt-oss-120b atau deepseek-chat, klik Clean, atau buat New Chat.' });
        break;
      }

      const am = { role: 'assistant', content: text || null, reasoning: reasoning || null, ts: Date.now(), mode: effectiveMode };
      if (tcList.length > 0) am.tool_calls = tcList.map(tc => ({ id: tc.id || 'call_' + crypto.randomBytes(4).toString('hex'), type: 'function', function: { name: tc.name, arguments: tc.arguments } }));
      if (am.content && isPoisoned(am.content) && !am.tool_calls) { sse({ type: 'error', message: 'Response contains poison text — not saving. Klik Clean.' }); break; }
      conv.messages.push(am);
      msgs.push({ role: 'assistant', content: am.content, tool_calls: am.tool_calls });

      if (tcList.length === 0) break;

      for (let ti = 0; ti < tcList.length; ti++) {
        const tc = tcList[ti], callId = am.tool_calls[ti].id;
        sse({ type: 'tool_call', id: callId, name: tc.name, arguments: tc.arguments });
        let args = {}; try { args = JSON.parse(tc.arguments || '{}'); } catch { args = { _raw: tc.arguments }; }
        let result; try { result = await executeTool(tc.name, args, { agent, conv }); } catch (e) { result = 'ERROR: ' + e.message; }
        sse({ type: 'tool_result', id: callId, name: tc.name, result });
        const content = typeof result === 'string' ? result : JSON.stringify(result);
        const tm = { role: 'tool', tool_call_id: callId, content: content.slice(0, 8000), ts: Date.now() };
        conv.messages.push(tm);
        msgs.push({ role: 'tool', tool_call_id: callId, content: tm.content });
      }
      const reTrimmed = trimHistory(msgs, budgetChars);
      msgs.length = 0;
      msgs.push(...reTrimmed);
    }
    conv.updatedAt = Date.now(); saveChats();
    sse({ type: 'final', conversation: conv });
  } catch (e) {
    log('err', '-', '-', 'chat-orch', e.message);
    sse({ type: 'error', message: e.message });
  }
  try { res.end(); } catch {}
}

// --- END OF PART 1 ---
// ─── DASHBOARD HTML ─────────────────────────────────────────────────────────
const DASHBOARD_HTML = String.raw`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>POOL · chat studio</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Silkscreen:wght@400;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#0e1013; --bg-2:#14171c; --bg-3:#1a1f26; --line:#2b313a; --line-2:#3a424e;
  --fg:#e7eaf0; --fg-2:#a2abb8; --fg-3:#6b7482;
  --ok:#7dd3a0; --warn:#e3b341; --err:#e5645f; --info:#7aa2f7;
  --accent: var(--ok);
}
body.light {
  --bg:#f7f7f2; --bg-2:#eeeee8; --bg-3:#e2e2da;
  --line:#c0c0b8; --line-2:#888880;
  --fg:#1a1a1a; --fg-2:#4a4a4a; --fg-3:#7a7a7a;
  --ok:#2d8659; --warn:#a8741a; --err:#b83232; --info:#3058b8;
}
* { box-sizing: border-box; border-radius: 0 !important; }
html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg); height: 100%; }
body { font-family: "IBM Plex Mono", ui-monospace, monospace; font-size: 13px; line-height: 1.55; -webkit-font-smoothing: antialiased; overflow: hidden; }
button, input, select, textarea { font: inherit; color: inherit; }
button { cursor: pointer; }
a { color: var(--accent); text-decoration: none; }
h1, h2, h3, h4 { margin: 0; font-weight: 700; }
::selection { background: var(--accent); color: var(--bg); }
*:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.app { display: grid; grid-template-columns: 200px 1fr; height: 100vh; }
.rail { border-right: 2px solid var(--line); background: var(--bg-2); padding: 12px 0; display: flex; flex-direction: column; overflow-y: auto; }
.brand { font-family: "Silkscreen", monospace; font-size: 16px; font-weight: 700; text-transform: uppercase; letter-spacing: .14em; padding: 6px 16px 16px; border-bottom: 2px solid var(--line); }
.brand span { color: var(--accent); animation: blink 1s steps(2, start) infinite; }
@keyframes blink { 50% { opacity: 0; } }
.rail nav { display: flex; flex-direction: column; margin-top: 8px; }
.rail nav button { background: transparent; border: none; text-align: left; font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .1em; color: var(--fg-2); padding: 9px 16px; border-left: 3px solid transparent; }
.rail nav button:hover { color: var(--fg); background: var(--bg-3); }
.rail nav button.active { color: var(--accent); border-left-color: var(--accent); background: var(--bg-3); }
.rail .foot { margin-top: auto; padding: 8px 16px; font-size: 9px; color: var(--fg-3); border-top: 2px solid var(--line); font-family: "Silkscreen", monospace; text-transform: uppercase; letter-spacing: .1em; }
.main { overflow-y: auto; min-width: 0; }
.tab { display: none; padding: 20px 24px 32px; }
.tab.active { display: block; }
.tab.chat-tab { padding: 0; height: 100vh; }
.page-title { font-family: "Silkscreen", monospace; font-size: 26px; font-weight: 700; text-transform: uppercase; letter-spacing: .02em; margin-bottom: 6px; }
.page-sub { color: var(--fg-3); font-size: 11px; margin-bottom: 24px; }
.sec { margin-bottom: 24px; padding-bottom: 20px; border-bottom: 2px solid var(--line); }
.sec:last-child { border-bottom: none; }
.sec-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
.sec-title { font-family: "Silkscreen", monospace; font-size: 16px; font-weight: 700; text-transform: uppercase; letter-spacing: .06em; padding-bottom: 4px; border-bottom: 2px solid var(--accent); display: inline-block; }
.btn { font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .08em; background: var(--bg-3); border: 2px solid var(--line); color: var(--fg); padding: 6px 12px; box-shadow: 3px 3px 0 var(--line-2); transition: transform 120ms steps(2), box-shadow 120ms steps(2), border-color 120ms steps(2); display: inline-flex; align-items: center; gap: 6px; white-space: nowrap; }
.btn:hover { border-color: var(--fg-2); }
.btn:active { transform: translate(3px, 3px); box-shadow: none; }
.btn.primary { border-color: var(--accent); color: var(--accent); }
.btn.danger { border-color: var(--err); color: var(--err); }
.btn.warn { border-color: var(--warn); color: var(--warn); }
.btn.ghost { background: transparent; box-shadow: none; }
.btn.ghost:active { transform: none; }
.btn.small { padding: 3px 7px; font-size: 9px; box-shadow: 2px 2px 0 var(--line-2); }
.btn:disabled { opacity: .4; cursor: not-allowed; box-shadow: none; }
.field { display: flex; flex-direction: column; gap: 5px; margin-bottom: 12px; }
.field label { font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .1em; color: var(--fg-3); }
.field input, .field select, .field textarea { background: var(--bg); border: 2px solid var(--line); padding: 7px 9px; color: var(--fg); font-family: "IBM Plex Mono", monospace; font-size: 12px; }
.field input:focus, .field select:focus, .field textarea:focus { border-color: var(--accent); outline: none; }
.field textarea { resize: vertical; min-height: 90px; }
.field .hint { color: var(--fg-3); font-size: 10px; }
.form-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.tbl-wrap { overflow-x: auto; border: 2px solid var(--line); }
table { border-collapse: collapse; width: 100%; }
table th { background: var(--bg-3); font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .1em; color: var(--fg-2); padding: 8px 10px; text-align: left; border-bottom: 2px solid var(--line); white-space: nowrap; }
table td { padding: 7px 10px; border-bottom: 1px solid var(--line); border-left: 3px solid transparent; font-size: 11px; }
table tr:nth-child(even) td { background: var(--bg-2); }
table tr:hover td { border-left-color: var(--accent); }
.num { font-variant-numeric: tabular-nums; }
.badge { display: inline-block; font-family: "Silkscreen", monospace; font-size: 9px; text-transform: uppercase; letter-spacing: .08em; padding: 1px 5px; border: 1px solid currentColor; background: transparent; white-space: nowrap; }
.badge.ok { color: var(--ok); } .badge.warn { color: var(--warn); } .badge.err { color: var(--err); } .badge.info { color: var(--info); } .badge.mute { color: var(--fg-3); }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 20px; }
.stat { border: 2px solid var(--line); padding: 12px; background: var(--bg-2); }
.stat .lbl { font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .1em; color: var(--fg-3); margin-bottom: 6px; }
.stat .val { font-family: "IBM Plex Mono", monospace; font-size: 22px; font-weight: 600; font-variant-numeric: tabular-nums; }
.stat .val.ok { color: var(--ok); } .stat .val.warn { color: var(--warn); } .stat .val.err { color: var(--err); }
.panel { border: 2px solid var(--line); background: var(--bg-2); margin-bottom: 16px; }
.panel-head { background-image: repeating-linear-gradient(90deg, var(--bg-3) 0 2px, transparent 2px 4px); border-bottom: 2px solid var(--line); padding: 10px 14px; }
.panel-title { font-family: "Silkscreen", monospace; font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: .1em; }
.panel-body { padding: 12px 14px; }
#toasts { position: fixed; bottom: 14px; right: 14px; display: flex; flex-direction: column; gap: 6px; z-index: 1000; }
.toast { background: var(--bg-3); border: 2px solid var(--line-2); padding: 8px 14px; font-size: 11px; box-shadow: 3px 3px 0 var(--line-2); max-width: 320px; animation: toast-in 200ms steps(4); }
.toast.ok { border-color: var(--ok); color: var(--ok); }
.toast.err { border-color: var(--err); color: var(--err); }
.toast.warn { border-color: var(--warn); color: var(--warn); }
@keyframes toast-in { from { opacity: 0; transform: translateX(40px); } to { opacity: 1; transform: translateX(0); } }
.overlay { position: fixed; inset: 0; background: rgba(0,0,0,.8); display: flex; align-items: center; justify-content: center; z-index: 500; padding: 20px; overflow-y: auto; }
.modal { background: var(--bg); border: 2px solid var(--line-2); box-shadow: 6px 6px 0 var(--line-2); max-width: 640px; width: 100%; max-height: 90vh; overflow-y: auto; }
.modal.wide { max-width: 900px; }
.modal-head { background-image: repeating-linear-gradient(90deg, var(--bg-3) 0 2px, transparent 2px 4px); border-bottom: 2px solid var(--line); padding: 10px 14px; display: flex; justify-content: space-between; align-items: center; }
.modal-body { padding: 16px; }
.modal-foot { padding: 10px 14px; border-top: 2px solid var(--line); display: flex; justify-content: flex-end; gap: 6px; }
.chip-row { display: flex; flex-wrap: wrap; gap: 6px; }
.swatches { display: flex; gap: 6px; }
.swatch { width: 32px; height: 32px; border: 2px solid var(--line); cursor: pointer; }
.swatch.active { border-color: var(--fg); box-shadow: 2px 2px 0 var(--fg); }
.ascii { font-family: "IBM Plex Mono", monospace; font-size: 11px; line-height: 1.4; white-space: pre; color: var(--fg-3); margin: 12px 0; }
.empty { padding: 20px; text-align: center; color: var(--fg-3); font-size: 11px; }
.logs-list { background: var(--bg); border: 2px solid var(--line); padding: 6px; font-family: "IBM Plex Mono", monospace; font-size: 10px; max-height: 600px; overflow-y: auto; line-height: 1.6; }
.log-line { padding: 2px 4px; white-space: pre-wrap; word-break: break-all; }
.log-line:hover { background: var(--bg-2); }
.log-line .ts { color: var(--fg-3); }
.log-line .lv-info { color: var(--info); } .log-line .lv-warn { color: var(--warn); } .log-line .lv-err { color: var(--err); } .log-line .lv-debug { color: var(--fg-3); }
.log-line .prov { color: var(--accent); }
.sys-card { font-family: "IBM Plex Mono", monospace; font-size: 11px; line-height: 1.9; color: var(--fg-2); }
.sys-card .k { color: var(--fg-3); display: inline-block; min-width: 120px; }
.chat-layout { display: grid; grid-template-columns: 220px 1fr; height: 100vh; }
.chat-sidebar { border-right: 2px solid var(--line); background: var(--bg-2); display: flex; flex-direction: column; overflow: hidden; }
.chat-sidebar-head { padding: 10px; border-bottom: 2px solid var(--line); }
.chat-conv-list { flex: 1; overflow-y: auto; }
.conv-item { padding: 8px 10px; border-bottom: 1px solid var(--line); cursor: pointer; border-left: 3px solid transparent; }
.conv-item:hover { background: var(--bg-3); }
.conv-item.active { border-left-color: var(--accent); background: var(--bg-3); }
.conv-item .cv-title { font-size: 11px; color: var(--fg); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.conv-item .cv-meta { font-size: 9px; color: var(--fg-3); margin-top: 2px; display: flex; justify-content: space-between; }
.chat-main { display: flex; flex-direction: column; overflow: hidden; }
.chat-toolbar { border-bottom: 2px solid var(--line); padding: 8px 12px; display: flex; gap: 8px; align-items: center; background: var(--bg-2); flex-wrap: wrap; }
.chat-toolbar select { background: var(--bg-3); border: 2px solid var(--line); padding: 4px 8px; font-size: 11px; }
.chat-toolbar .spacer { flex: 1; }
.chat-thread { flex: 1; overflow-y: auto; padding: 16px 20px; }
.chat-input-row { border-top: 2px solid var(--line); padding: 10px 12px; display: flex; gap: 8px; background: var(--bg-2); align-items: flex-end; }
.chat-input-row textarea { flex: 1; background: var(--bg); border: 2px solid var(--line); padding: 8px 10px; min-height: 48px; max-height: 160px; resize: none; font-size: 12px; font-family: inherit; }
.chat-input-row textarea:focus { border-color: var(--accent); outline: none; }
.msg-waiting { display: none; padding: 4px 10px; margin: 4px 0; color: var(--fg-3); font-family: "IBM Plex Mono", monospace; font-size: 11px; font-style: italic; border-left: 2px solid var(--line); }
.msg-waiting.show { display: block; }
.msg-waiting::before { content: ""; display: inline-block; width: 8px; height: 8px; margin-right: 8px; background: var(--accent); animation: blink 1s steps(2, start) infinite; vertical-align: middle; }
.msg-reasoning { margin: 4px 0 8px; border-left: 3px solid var(--info); background: var(--bg-2); padding: 3px 10px; }
.msg-reasoning summary { cursor: pointer; font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .06em; color: var(--info); padding: 3px 0; user-select: none; list-style: none; outline: none; }
.msg-reasoning summary::-webkit-details-marker { display: none; }
.msg-reasoning summary::before { content: "\25b8 "; display: inline-block; transition: transform 120ms steps(2); }
.msg-reasoning[open] summary::before { content: "\25be "; }
.msg-reasoning summary:hover { color: var(--fg); }
.msg-reasoning .reasoning-content { font-size: 11px; color: var(--fg-2); white-space: pre-wrap; word-wrap: break-word; padding: 4px 0 6px; max-height: 320px; overflow-y: auto; line-height: 1.5; border-top: 1px dashed var(--line); margin-top: 2px; }
body.hide-reasoning .msg-reasoning { display: none; }
.moon-thinking { display: flex; align-items: center; gap: 10px; padding: 8px 12px; margin: 8px 0; background: var(--bg-2); border: 1px solid var(--line); border-left: 3px solid var(--accent); font-family: "IBM Plex Mono", monospace; font-size: 11px; color: var(--fg-2); }
@keyframes moon-pulse { 50% { opacity: 0.5; } }
.moon-text { color: var(--fg-3); font-style: italic; }
.msg-stream-group { display: block; }
.conv-item { position: relative; }
.conv-item.pinned { background: var(--bg-2); }
.conv-item .conv-actions { position: absolute; top: 4px; right: 4px; display: none; gap: 2px; }
.conv-item:hover .conv-actions { display: flex; }
.conv-action { background: var(--bg-3); border: 1px solid var(--line); color: var(--fg-3); cursor: pointer; padding: 1px 5px; font-size: 10px; font-family: "Silkscreen", monospace; }
.conv-action:hover { color: var(--accent); border-color: var(--accent); }
.conv-action.danger:hover { color: var(--err); border-color: var(--err); }
.conv-pin-badge { color: var(--warn); margin-right: 4px; font-size: 10px; }
.conv-search { width: 100%; background: var(--bg); border: 2px solid var(--line); padding: 4px 8px; font-size: 11px; margin-top: 4px; font-family: "IBM Plex Mono", monospace; }
.conv-search:focus { border-color: var(--accent); outline: none; }
.conv-sidebar-foot { padding: 6px 8px; border-top: 2px solid var(--line); display: flex; gap: 4px; background: var(--bg-2); }
.conv-sidebar-foot .btn { flex: 1; justify-content: center; }
.conv-title-input { width: 100%; background: var(--bg); border: 1px solid var(--accent); color: var(--fg); font-family: "IBM Plex Mono", monospace; font-size: 11px; padding: 1px 4px; }
.msg { position: relative; }
.msg-actions { position: absolute; top: 0; right: 0; display: none; gap: 2px; background: var(--bg-2); border: 1px solid var(--line); padding: 2px 4px; z-index: 1; }
.msg:hover .msg-actions { display: flex; }
.msg-action-btn { background: transparent; border: none; color: var(--fg-3); cursor: pointer; font-size: 10px; padding: 1px 4px; font-family: "IBM Plex Mono", monospace; }
.msg-action-btn:hover { color: var(--accent); }
.msg-action-btn.danger:hover { color: var(--err); }
#chat-info { display: flex; gap: 12px; padding: 6px 12px; background: var(--bg); border-bottom: 2px solid var(--line); font-family: "IBM Plex Mono", monospace; font-size: 10px; color: var(--fg-3); flex-wrap: wrap; min-height: 28px; align-items: center; }
.ci-item { display: inline-flex; align-items: center; gap: 4px; }
.ci-item.ci-active { color: var(--accent); animation: blink 1.2s steps(2, start) infinite; }
.ci-item.ci-err { color: var(--err); }
.ci-item.ci-dim { color: var(--fg-3); }
.md-content .code-block { background: var(--bg-3); border: 2px solid var(--line); margin: 8px 0; overflow: hidden; }
.md-content .code-block .code-head { display: flex; justify-content: space-between; align-items: center; padding: 4px 8px; background: var(--bg); border-bottom: 1px solid var(--line); }
.md-content .code-block .code-lang { font-family: "Silkscreen", monospace; font-size: 9px; text-transform: uppercase; letter-spacing: .08em; color: var(--fg-3); }
.md-content .code-block .copy-btn { background: transparent; border: 1px solid var(--line); color: var(--fg-2); padding: 2px 8px; font-size: 9px; font-family: "Silkscreen", monospace; text-transform: uppercase; letter-spacing: .08em; cursor: pointer; }
.md-content .code-block .copy-btn:hover { border-color: var(--accent); color: var(--accent); }
.md-content .code-block pre { background: transparent; border: none; padding: 10px; margin: 0; overflow-x: auto; }
.md-content .code-block pre code { background: transparent; padding: 0; color: var(--fg); font-size: 11px; line-height: 1.5; }
.md-content table.md-table { border-collapse: collapse; margin: 8px 0; width: 100%; }
.md-content table.md-table th, .md-content table.md-table td { border: 1px solid var(--line); padding: 6px 10px; font-size: 11px; }
.md-content table.md-table th { background: var(--bg-3); font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .06em; color: var(--fg-2); }
.md-content table.md-table tr:nth-child(even) td { background: var(--bg-2); }
.md-content li.task { list-style: none; margin-left: -20px; }
.md-content li.task input { margin-right: 6px; }
.msg { margin-bottom: 14px; }
.msg-role { font-family: "Silkscreen", monospace; font-size: 9px; text-transform: uppercase; letter-spacing: .08em; color: var(--fg-3); margin-bottom: 4px; }
.msg-user .msg-role { color: var(--info); }
.msg-assistant .msg-role { color: var(--accent); }
.msg-tool-block { background: var(--bg); border: 1px solid var(--line); padding: 6px 8px; margin: 4px 0; font-size: 10px; }
.msg-tool-block .tc-name { color: var(--accent); font-weight: 600; }
.msg-tool-block .tc-args { color: var(--fg-3); margin-top: 2px; white-space: pre-wrap; word-break: break-all; font-size: 9px; }
.msg-tool-block .tc-result { color: var(--fg-2); margin-top: 4px; padding-top: 4px; border-top: 1px dashed var(--line); white-space: pre-wrap; word-break: break-all; font-size: 9px; max-height: 200px; overflow-y: auto; }
.msg-error { color: var(--err); background: var(--bg-3); border: 1px solid var(--err); padding: 8px; font-size: 11px; white-space: pre-wrap; }
.msg-content.streaming::after { content: "▮"; animation: blink 1s steps(2, start) infinite; color: var(--accent); }
.md-content { font-size: 13px; line-height: 1.6; color: var(--fg); }
.md-content h1, .md-content h2, .md-content h3, .md-content h4 { font-family: "Silkscreen", monospace; text-transform: uppercase; letter-spacing: .04em; margin: 14px 0 6px; }
.md-content h1 { font-size: 18px; border-bottom: 2px solid var(--line); padding-bottom: 4px; }
.md-content h2 { font-size: 15px; }
.md-content h3 { font-size: 13px; }
.md-content p { margin: 6px 0; }
.md-content code { background: var(--bg-3); padding: 1px 5px; font-family: "IBM Plex Mono", monospace; color: var(--accent); font-size: 12px; }
.md-content pre { background: var(--bg-3); border: 1px solid var(--line); padding: 10px; overflow-x: auto; margin: 8px 0; }
.md-content pre code { background: transparent; padding: 0; color: var(--fg); font-size: 11px; line-height: 1.5; }
.md-content ul, .md-content ol { margin: 6px 0; padding-left: 22px; }
.md-content li { margin: 2px 0; }
.md-content blockquote { border-left: 3px solid var(--accent); padding: 2px 10px; margin: 8px 0; color: var(--fg-2); background: var(--bg-2); }
.md-content a { color: var(--accent); text-decoration: underline; }
.md-content hr { border: none; border-top: 2px solid var(--line); margin: 12px 0; }
.md-content strong { color: var(--fg); }
.md-content em { color: var(--fg-2); }
.tool-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; margin-top: 8px; }
.tool-card { border: 2px solid var(--line); padding: 10px; background: var(--bg-2); }
.tool-card .tc-title { font-family: "Silkscreen", monospace; font-size: 11px; text-transform: uppercase; letter-spacing: .06em; color: var(--accent); word-break: break-all; }
.tool-card .tc-desc { font-size: 10px; color: var(--fg-2); margin: 4px 0; line-height: 1.4; }
.tool-card .tc-source { font-size: 9px; color: var(--fg-3); }
.tool-card.selected { border-color: var(--accent); }
.subtabs { display: flex; gap: 0; margin-bottom: 16px; border: 2px solid var(--line); }
.subtabs button { flex: 1; font-family: "Silkscreen", monospace; font-size: 10px; text-transform: uppercase; letter-spacing: .08em; background: var(--bg-2); border: none; padding: 8px; color: var(--fg-2); border-right: 2px solid var(--line); }
.subtabs button:last-child { border-right: none; }
.subtabs button.active { background: var(--bg-3); color: var(--accent); }
.pg-out { background: var(--bg); border: 2px solid var(--line); padding: 12px; min-height: 200px; white-space: pre-wrap; font-family: "IBM Plex Mono", monospace; font-size: 12px; max-height: 500px; overflow-y: auto; }
.pg-meta { margin-top: 8px; display: flex; gap: 12px; font-size: 11px; color: var(--fg-3); flex-wrap: wrap; }
.chat-attachments { display: none; gap: 8px; padding: 8px 12px; background: var(--bg-2); border-top: 2px solid var(--line); flex-wrap: wrap; }
.attachment-chip { display: inline-flex; align-items: center; gap: 6px; background: var(--bg-3); border: 1px solid var(--line); padding: 4px 8px; font-size: 11px; }
.attachment-chip img { border: 1px solid var(--line); }
.attach-remove { background: transparent; border: none; color: var(--err); cursor: pointer; padding: 0 4px; font-size: 12px; }
.msg-attachments { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 6px; }
.msg-attach-img { max-width: 240px; max-height: 240px; border: 2px solid var(--line); }
.msg-attach-file { background: var(--bg-3); border: 1px solid var(--line); padding: 4px 8px; font-size: 10px; }
.filter-select { background: var(--bg-3); border: 2px solid var(--line); padding: 4px 8px; font-size: 11px; }
.filter-input { background: var(--bg-3); border: 2px solid var(--line); padding: 4px 8px; font-size: 11px; min-width: 180px; }
.mode-toggle { display: inline-flex; border: 2px solid var(--line); background: var(--bg-3); }
.mode-toggle button { background: transparent; border: none; padding: 4px 10px; font-family: "Silkscreen", monospace; font-size: 10px; letter-spacing: .06em; color: var(--fg-3); cursor: pointer; text-transform: uppercase; }
.mode-toggle button.active.chat { background: var(--info); color: var(--bg); }
.mode-toggle button.active.agent { background: var(--accent); color: var(--bg); }
.ic { width: 1em; height: 1em; display: inline-block; vertical-align: -0.14em; flex-shrink: 0; }
.rail nav button .ic { font-size: 13px; margin-right: 7px; vertical-align: -2px; }
.btn .ic { font-size: 12px; }
.conv-action .ic, .msg-action-btn .ic { font-size: 11px; vertical-align: -1px; }
.attach-remove .ic { font-size: 11px; vertical-align: -1px; }
@keyframes ic-spin { to { transform: rotate(360deg); } }
.ic.spin { animation: ic-spin 1s steps(8) infinite; transform-origin: center; }
.ic-moon { width: 18px; height: 18px; }
.moon-core { transition: none; }
@keyframes moon-pulse { 50% { opacity: 0.5; } }
@media (max-width: 900px) {
  .app { grid-template-columns: 1fr; }
  .rail { flex-direction: row; overflow-x: auto; padding: 0; }
  .brand { padding: 8px 12px; border-bottom: none; border-right: 2px solid var(--line); }
  .rail nav { flex-direction: row; margin: 0; }
  .rail nav button { border-left: none; border-bottom: 3px solid transparent; padding: 8px 10px; }
  .rail nav button.active { border-left: none; border-bottom-color: var(--accent); }
  .rail .foot { display: none; }
  .tab { padding: 14px; }
  .chat-layout { grid-template-columns: 1fr; }
  .chat-sidebar { display: none; }
  .form-row { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div class="app">
  <aside class="rail">
    <div class="brand">POOL<span>▮</span></div>
    <nav id="rail-nav">
      <button data-tab="chat" class="active"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M2 2h12v9H8l-4 3v-3H2z"/></svg>Chat</button>
      <button data-tab="agents"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><rect x="4" y="4" width="8" height="7"/><path d="M8 1.5V4M5.5 7h.5M10 7h.5M6 13.5h4"/></svg>Agents</button>
      <button data-tab="tools"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M9.8 2.6a3 3 0 0 0-3.8 3.8L2 10.4 3.6 12l4-4a3 3 0 0 0 3.8-3.8L9.7 5.9l-.8-.8z"/></svg>Tools</button>
      <button data-tab="playground"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M6 2v4.3L2.9 12A1.6 1.6 0 0 0 4.3 14.5h7.4A1.6 1.6 0 0 0 13.1 12L10 6.3V2M4.5 2h7M4.8 9.5h6.4"/></svg>Playground</button>
      <button data-tab="mcp"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M8 1.5v4M8 10.5v4M1.5 8h4M10.5 8h4"/><rect x="6" y="6" width="4" height="4"/></svg>MCP</button>
      <button data-tab="overview"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><rect x="2" y="2" width="5" height="5"/><rect x="9" y="2" width="5" height="5"/><rect x="2" y="9" width="5" height="5"/><rect x="9" y="9" width="5" height="5"/></svg>Overview</button>
      <button data-tab="accounts"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><circle cx="8" cy="5" r="2.5"/><path d="M3 14c0-3 2.2-4.5 5-4.5S13 11 13 14"/></svg>Accounts</button>
      <button data-tab="providers"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><ellipse cx="8" cy="3.5" rx="5.5" ry="2"/><path d="M2.5 3.5v9a5.5 2 0 0 0 11 0v-9M2.5 8a5.5 2 0 0 0 11 0"/></svg>Providers</button>
      <button data-tab="models"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M8 1.5l6 3.2-6 3.2-6-3.2zM2 8.5L8 11.7l6-3.2M2 11.5L8 14.7l6-3.2"/></svg>Models</button>
      <button data-tab="logs"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M2 4h8M2 8h12M2 12h6"/></svg>Logs</button>
      <button data-tab="settings"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><circle cx="8" cy="8" r="2.2"/><path d="M8 1.5v2.3M8 12.2v2.3M1.5 8h2.3M12.2 8h2.3M3.4 3.4l1.6 1.6M11 11l1.6 1.6M12.6 3.4L11 5M5 11l-1.6 1.6"/></svg>Settings</button>
    </nav>
    <div class="foot">
      <button id="theme-toggle" class="btn ghost small" style="width:100%;margin-bottom:6px;justify-content:center">Light Mode</button>
      <div style="text-align:center">v2 · zero dep</div>
    </div>
  </aside>
  <main class="main">
    <section class="tab active chat-tab" id="tab-chat">
      <div class="chat-layout">
        <aside class="chat-sidebar">
          <div class="chat-sidebar-head">
            <button class="btn primary small" id="chat-new" style="width:100%;margin-bottom:6px">+ New Chat</button>
            <input type="text" id="conv-search" class="conv-search" placeholder="search...">
          </div>
          <div class="chat-conv-list" id="conv-list"></div>
          <div class="conv-sidebar-foot">
            <button class="btn ghost small" id="conv-export" title="Export all conversations"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><path d="M8 2v8M4.5 6.5L8 10l3.5-3.5M2.5 13.5h11"/></svg>Export</button>
            <button class="btn ghost small" id="conv-import" title="Import conversations"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><path d="M8 10V2M4.5 5.5L8 2l3.5 3.5M2.5 13.5h11"/></svg>Import</button>
            <button class="btn ghost small danger" id="conv-delete-all" title="Delete all">Clear</button>
          </div>
        </aside>
        <div class="chat-main">
          <div class="chat-toolbar">
            <span style="font-family:'Silkscreen',monospace;font-size:10px;color:var(--fg-3);text-transform:uppercase;letter-spacing:.08em">Agent</span>
            <select id="chat-agent"></select>
            <span style="font-family:'Silkscreen',monospace;font-size:10px;color:var(--fg-3);text-transform:uppercase;letter-spacing:.08em">Model</span>
            <select id="chat-model"></select>
            <span style="font-family:'Silkscreen',monospace;font-size:10px;color:var(--fg-3);text-transform:uppercase;letter-spacing:.08em">Mode</span>
            <div class="mode-toggle">
              <button id="mode-chat" data-mode="chat">Chat</button>
              <button id="mode-agent" data-mode="agent" class="active">Agent</button>
            </div>
            <button class="btn ghost small" id="chat-clean-btn" title="Hapus pesan asisten yang mengandung artifact 'tidak akan memanggil tool'">Clean</button>
            <button class="btn ghost small" id="toggle-reasoning" title="Toggle reasoning visibility">Hide Think</button>
            <span class="spacer"></span>
          </div>
          <div id="chat-info"><span class="ci-item ci-dim">ready</span></div>
          <div class="chat-thread" id="chat-thread"></div>
          <div id="chat-attachments" class="chat-attachments"></div>
          <div class="chat-input-row">
            <button class="btn ghost small" id="chat-attach-btn" title="Attach file (max 8MB)"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><path d="M13 7.5L8 12.5a3.2 3.2 0 0 1-4.5-4.5l6-6a2.2 2.2 0 0 1 3.1 3.1l-6 6a1.2 1.2 0 0 1-1.7-1.7l5.3-5.3"/></svg></button>
            <input type="file" id="chat-attach-input" multiple accept="image/*,.txt,.md,.json,.js,.py,.html,.css,.xml,.yaml,.yml,.csv,.log,.sh,.ts,.tsx,.jsx,.sql,.rs,.go,.java,.c,.cpp,.h,.rb,.php,.toml,.ini,.conf,.env" style="display:none">
            <textarea id="chat-input" placeholder="Type a message... (Enter to send, Shift+Enter for newline)"></textarea>
            <button class="btn primary" id="chat-send">Kirim<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M2 8h11M9 4l4 4-4 4"/></svg></button>
            <button class="btn ghost" id="chat-stop" style="display:none">Stop</button>
          </div>
        </div>
      </div>
    </section>
    <section class="tab" id="tab-agents">
      <h1 class="page-title">Agents</h1>
      <div class="page-sub">system prompt · model · tools · skills · mode</div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Agents</div><div class="chip-row"><button class="btn primary small" id="btn-add-agent">+ New Agent</button><button class="btn small" id="btn-from-template">From Template</button></div></div>
        <div id="agents-table"></div>
      </div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Skills</div><button class="btn primary small" id="btn-add-skill">+ New Skill</button></div>
        <div id="skills-table"></div>
      </div>
    </section>
    <section class="tab" id="tab-tools">
      <h1 class="page-title">Tools</h1>
      <div class="page-sub">builtin + plugin + MCP</div>
      <div class="subtabs" id="tools-subtabs">
        <button data-sub="all" class="active">All</button>
        <button data-sub="builtin">Builtin</button>
        <button data-sub="plugin">Plugin</button>
        <button data-sub="mcp">MCP</button>
      </div>
      <div class="sec">
        <div class="sec-title">Tool Registry</div>
        <div class="tool-grid" id="tools-grid"></div>
      </div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Plugins</div><button class="btn primary small" id="btn-add-plugin">+ New Plugin</button></div>
        <div style="color:var(--warn);font-size:10px;margin-bottom:8px">' + ic('warn') + ' Plugin source JS dieksekusi saat load. Hanya isi plugin yang kamu percaya.</div>
        <div id="plugins-table"></div>
      </div>
    </section>
    <section class="tab" id="tab-playground">
      <h1 class="page-title">Playground</h1>
      <div class="page-sub">direct test · /v1/chat/completions · stream</div>
      <div class="form-row">
        <div class="field"><label>Model</label><select id="pg-model"></select></div>
        <div class="field"><label>Temperature</label><input id="pg-temp" type="number" value="0.7" step="0.1" min="0" max="2"></div>
      </div>
      <div class="field"><label>Message</label><textarea id="pg-input" rows="4" placeholder="Ask something..."></textarea></div>
      <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
        <button class="btn primary" id="pg-send">Kirim<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M2 8h11M9 4l4 4-4 4"/></svg></button>
        <button class="btn ghost" id="pg-clear">Clear</button>
        <button class="btn ghost" id="pg-preview"><svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M3 2.5L11 8l-8 5.5z"/></svg>Test Fallback Order</button>
      </div>
      <div class="pg-out" id="pg-out"></div>
      <div class="pg-meta" id="pg-meta"></div>
    </section>
    <section class="tab" id="tab-mcp">
      <h1 class="page-title">MCP</h1>
      <div class="page-sub">Model Context Protocol servers</div>
      <div class="sec-head"><button class="btn primary small" id="btn-add-mcp">+ Connect MCP Server</button></div>
      <div id="mcp-table"></div>
    </section>
    <section class="tab" id="tab-overview">
      <h1 class="page-title">Overview</h1>
      <div class="page-sub">rotation pool status</div>
      <div id="overview-body"></div>
    </section>
    <section class="tab" id="tab-accounts">
      <h1 class="page-title">Accounts</h1>
      <div class="page-sub">manage pool accounts</div>
      <div class="sec-head">
        <div class="chip-row">
          <button class="btn primary small" id="btn-add-acc">+ Tambah Akun</button>
          <button class="btn small" id="btn-bulk-acc">Import Massal</button>
          <button class="btn small" id="btn-probe-sel">Probe Selected</button>
          <button class="btn small" id="btn-enable-sel">Enable</button>
          <button class="btn small" id="btn-disable-sel">Disable</button>
          <button class="btn danger small" id="btn-del-sel">Hapus Selected</button>
        </div>
      </div>
      <div class="sec-head" style="gap:8px">
        <div class="chip-row">
          <select id="f-provider" style="background:var(--bg-3);border:2px solid var(--line);padding:4px 8px;font-size:11px"><option value="">All providers</option></select>
          <select id="f-status" style="background:var(--bg-3);border:2px solid var(--line);padding:4px 8px;font-size:11px"><option value="">All status</option><option value="ready">ready</option><option value="cooldown">cooldown</option><option value="dead">dead</option></select>
          <input id="f-search" placeholder="search label..." style="background:var(--bg-3);border:2px solid var(--line);padding:4px 8px;font-size:11px;min-width:180px">
        </div>
      </div>
      <div class="tbl-wrap" id="acc-table-wrap"></div>
    </section>
    <section class="tab" id="tab-providers">
      <h1 class="page-title">Providers</h1>
      <div class="page-sub">click cells to edit · Enter save · Esc cancel</div>
      <div class="sec-head"><button class="btn primary small" id="btn-add-prov">+ Tambah Provider</button></div>
      <div class="tbl-wrap" id="prov-table-wrap"></div>
    </section>
    <section class="tab" id="tab-models">
      <h1 class="page-title">Models</h1>
      <div class="page-sub">model catalog per provider · labels · capabilities · priority · context</div>
      <div class="sec-head"><div class="chip-row"><button class="btn primary small" id="btn-add-model">+ Tambah Model</button><button class="btn small" id="btn-reseed-models">Reseed from Providers</button></div></div>
      <div class="sec-head"><div class="chip-row"><select id="mdl-f-provider" class="filter-select"><option value="">All providers</option></select><select id="mdl-f-label" class="filter-select"><option value="">All labels</option></select><input id="mdl-f-search" class="filter-input" placeholder="search model id..."></div></div>
      <div class="tbl-wrap" id="mdl-table-wrap"></div>
    </section>
    <section class="tab" id="tab-logs">
      <h1 class="page-title">Logs</h1>
      <div class="page-sub">live SSE stream</div>
      <div class="sec-head">
        <div class="chip-row">
          <select id="log-level" style="background:var(--bg-3);border:2px solid var(--line);padding:4px 8px;font-size:11px"><option value="">All levels</option><option value="info">info</option><option value="warn">warn</option><option value="err">err</option><option value="debug">debug</option></select>
          <button class="btn small" id="log-pause">Pause</button>
          <button class="btn small" id="log-clear">Clear</button>
        </div>
      </div>
      <div class="logs-list" id="logs-list"></div>
    </section>
    <section class="tab" id="tab-settings">
      <h1 class="page-title">Settings</h1>
      <div class="page-sub">configuration</div>
      <div class="sec"><div class="sec-head"><div class="sec-title">System</div></div><div class="sys-card" id="sys-info">—</div></div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Auth & defaults</div></div>
        <div class="field"><label>Admin token (blank = open)</label><input id="s-adminToken"></div>
        <div class="form-row">
          <div class="field"><label>Default alias</label><select id="s-defaultAlias"><option value="smart">smart</option><option value="fast">fast</option></select></div>
          <div class="field"><label>Upstream timeout (ms)</label><input id="s-upstreamTimeoutMs" type="number" min="5000" step="1000"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>Log level</label><select id="s-logLevel"><option value="debug">debug</option><option value="info">info</option><option value="warn">warn</option><option value="err">err</option></select></div>
          <div class="field"><label>Paid fallback</label><select id="s-paidFallback"><option value="true">enabled</option><option value="false">disabled</option></select></div>
        </div>
        <div class="field">
          <label>Fallback mode</label>
          <select id="s-fallbackMode">
            <option value="priority">priority — akun priority tertinggi dulu</option>
            <option value="provider">provider — habiskan satu provider dulu</option>
            <option value="round-robin">round-robin — least recently used</option>
          </select>
        </div>
        <div class="field">
          <label>Accent</label>
          <div class="swatches" id="s-swatches">
            <div class="swatch" data-accent="mint" style="background:#7dd3a0"></div>
            <div class="swatch" data-accent="amber" style="background:#e3b341"></div>
            <div class="swatch" data-accent="blue" style="background:#7aa2f7"></div>
            <div class="swatch" data-accent="coral" style="background:#e5645f"></div>
          </div>
        </div>
      </div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Performance & Context</div></div>
        <div class="field">
          <label>Preset</label>
          <div style="display:flex;gap:6px;flex-wrap:wrap">
            <button type="button" class="btn small" id="s-preset-low">Low-Spec (2GB / 100k ctx)</button>
            <button type="button" class="btn small" id="s-preset-default">Default (128k ctx)</button>
          </div>
        </div>
        <div class="form-row">
          <div class="field"><label>Probe concurrency</label><input id="s-probeConcurrency" type="number" min="1" max="20"></div>
          <div class="field"><label>Log buffer size</label><input id="s-logBufferSize" type="number" min="10" max="5000"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>SSE heartbeat (ms)</label><input id="s-sseHeartbeatMs" type="number" min="5000" max="300000"></div>
          <div class="field"><label>Max body bytes</label><input id="s-maxBodyBytes" type="number" min="1048576"></div>
        </div>
        <div class="form-row">
          <div class="field"><label>Max tool iterations</label><input id="s-maxToolIterations" type="number" min="1" max="20"><div class="hint">Batas AI panggil tool sebelum dipaksa jawab. Default 8.</div></div>
          <div class="field"><label>Max context chars (global)</label><input id="s-maxContextChars" type="number" min="100000" step="100000"><div class="hint">Batas maksimal karakter history. Default 2.000.000.</div></div>
        </div>
        <div class="field">
          <label>Search priority (klik ▲▼ untuk urutkan, ✕ untuk skip)</label>
          <div id="s-searchPriorityList" style="display:flex;flex-direction:column;gap:4px"></div>
          <div id="s-searchPriorityAdd" style="margin-top:6px"></div>
        </div>
        <div class="form-row">
          <div class="field">
            <label>SearXNG URL (opsional)</label>
            <div style="display:flex;gap:6px">
              <input id="s-searxngUrl" placeholder="https://..." style="flex:1">
              <button class="btn small primary" id="s-setupSearxng" title="Auto-discover working SearXNG">Auto Setup</button>
            </div>
            <div id="s-searxngStatus" style="font-size:10px;color:var(--fg-3);margin-top:4px">—</div>
            <div class="hint">Publik SearXNG sering nonaktifkan JSON API. Auto Setup coba 15 instance publik + Docker lokal kalau ada.</div>
          </div>
          <div class="field">
            <label>Custom SearXNG URLs (opsional, satu per baris)</label>
            <textarea id="s-searxngCustomUrls" rows="4" placeholder="https://my-searx.be&#10;https://searx.private.example.com"></textarea>
            <div class="hint">URL yang kamu tambahkan di sini dicoba PERTAMA kali sebelum instance publik. Maksimal 20 URL.</div>
          </div>
        </div>
        <div class="field">
          <label>Test search (klik untuk cek engine mana yang jalan)</label>
          <div style="display:flex;gap:6px">
            <input id="s-testSearchQ" placeholder="hello world" value="hello world" style="flex:1">
            <button class="btn small" id="s-testSearchBtn">Test All</button>
          </div>
          <div id="s-testSearchResult" style="font-family:monospace;font-size:10px;color:var(--fg-3);white-space:pre-wrap;max-height:240px;overflow-y:auto;margin-top:6px;background:var(--bg);padding:8px;border:2px solid var(--line)"></div>
        </div>
        <div class="field">
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer;margin-bottom:8px">
            <input type="checkbox" id="s-forceThinkPrompt" style="width:auto"> Force think prompt (wrap reasoning in &lt;think&gt; tags)
          </label>
          <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
            <input type="checkbox" id="s-enableShellTool" style="width:auto"> Enable shell tool (DANGEROUS)
          </label>
          <div class="hint">Force think: inject system instruction asking model to wrap reasoning in  thinking.... Works only if model follows instructions. Gemma base does NOT.</div>
        </div>
      </div>
      <div class="sec" style="border-bottom:none"><button class="btn primary" id="s-save">Simpan Pengaturan</button></div>
      <div class="sec">
        <div class="sec-head"><div class="sec-title">Backup</div></div>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          <button class="btn" id="s-export">↓ Export pool.json</button>
          <button class="btn" id="s-import">↑ Import pool.json</button>
          <button class="btn warn" id="s-reset-all">Reset semua cooldown</button>
        </div>
      </div>
    </section>
  </main>
</div>
<div id="toasts"></div>

<script>
(function(){
'use strict';
var state = null, agentData = { agents: [], skills: [] }, toolList = [];
var currentTab = 'chat';
var currentConvId = null, currentConv = null, currentAgentId = 'ag_default';
var currentMode = 'agent';
var activeStreams = new Map();
var refreshAbort = null;
var accSelected = new Set();
var accFilter = { provider: '', status: '', search: '' };
var logSource = null, logPaused = false, logLines = [];
var toolsSub = 'all';

var ICONS = {
  send: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M2 8h11M9 4l4 4-4 4"/></svg>',
  play: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M3 2.5L11 8l-8 5.5z"/></svg>',
  refresh: '<svg class="ic spin" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9M13.5 1.5v3.5H10"/></svg>',
  regen: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9M13.5 1.5v3.5H10"/></svg>',
  starF: '<svg class="ic" viewBox="0 0 16 16" fill="currentColor" stroke="currentColor" stroke-width="1" stroke-linejoin="miter"><path d="M8 1.5l2 4.2 4.5.5-3.3 3 .8 4.4L8 11.4l-4 2.2.8-4.4-3.3-3 4.5-.5z"/></svg>',
  starE: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="miter"><path d="M8 1.5l2 4.2 4.5.5-3.3 3 .8 4.4L8 11.4l-4 2.2.8-4.4-3.3-3 4.5-.5z"/></svg>',
  pencil: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M11.5 1.5l3 3-9.5 9.5H2v-3zM9.5 3.5l3 3"/></svg>',
  x: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="square"><path d="M3.5 3.5l9 9M12.5 3.5l-9 9"/></svg>',
  warn: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M8 1.5L15 14H1zM8 6v4M8 11.6v.9"/></svg>',
  file: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M3 1.5h7L13 5v9.5H3zM10 1.5V5h3"/></svg>',
  download: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><path d="M8 2v8M4.5 6.5L8 10l3.5-3.5M2.5 13.5h11"/></svg>',
  upload: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><path d="M8 10V2M4.5 5.5L8 2l3.5 3.5M2.5 13.5h11"/></svg>',
  sun: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square"><circle cx="8" cy="8" r="3"/><path d="M8 .8v1.8M8 13.4v1.8M.8 8h1.8M13.4 8h1.8M2.9 2.9l1.3 1.3M11.8 11.8l1.3 1.3M13.1 2.9l-1.3 1.3M4.2 11.8l-1.3 1.3"/></svg>',
  moonSm: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linejoin="miter"><path d="M13.5 9.5A6 6 0 1 1 6.5 2.5a4.8 4.8 0 0 0 7 7z"/></svg>',
  chevD: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="square" stroke-linejoin="miter"><path d="M3.5 5.5L8 10l4.5-4.5"/></svg>',
  chevU: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="square" stroke-linejoin="miter"><path d="M3.5 10.5L8 6l4.5 4.5"/></svg>',
  arrowBack: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M14 8H3M7 4L3 8l4 4"/></svg>',
  tool: '<svg class="ic" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="square" stroke-linejoin="miter"><path d="M9.8 2.6a3 3 0 0 0-3.8 3.8L2 10.4 3.6 12l4-4a3 3 0 0 0 3.8-3.8L9.7 5.9l-.8-.8z"/></svg>',
};
function ic(name) { return ICONS[name] || ''; }

function esc(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, function(c) { return { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]; }); }
function el(sel) { return document.querySelector(sel); }
function els(sel) { return Array.prototype.slice.call(document.querySelectorAll(sel)); }
function getToken() { try { return localStorage.getItem('pool_token') || ''; } catch(e) { return ''; } }
function setToken(t) { try { if (t) localStorage.setItem('pool_token', t); else localStorage.removeItem('pool_token'); } catch(e) {} }

async function api(path, opts, _retried) {
  opts = opts || {};
  var headers = Object.assign({}, opts.headers || {});
  var tok = getToken();
  if (tok) headers['Authorization'] = 'Bearer ' + tok;
  var body = opts.body;
  if (body && typeof body !== 'string') { headers['Content-Type'] = 'application/json'; body = JSON.stringify(body); }
  var r = await fetch(path, Object.assign({}, opts, { headers: headers, body: body }));
  if (r.status === 401 && !_retried && !opts._noAuthPrompt) {
    var newTok = await customPrompt('Admin token required', {
      label: 'Token',
      description: 'Enter the admin token set in Settings on the host device.',
      placeholder: 'my-secret-token'
    });
    if (newTok) {
      setToken(newTok);
      return api(path, opts, true);
    }
  }
  if (!r.ok) { var t = ''; try { t = await r.text(); } catch(e) {} var err = new Error(r.status + ' ' + t.slice(0, 200)); err.status = r.status; throw err; }
  var ct = r.headers.get('content-type') || '';
  if (ct.indexOf('application/json') >= 0) return r.json();
  return r.text();
}

function toast(msg, type) { var box = el('#toasts'); var t = document.createElement('div'); t.className = 'toast ' + (type || 'ok'); t.textContent = msg; box.appendChild(t); setTimeout(function(){ t.remove(); }, 3500); }
function fmtAgo(ts) { if (!ts) return '—'; var d = Date.now() - ts; if (d < 60000) return Math.floor(d/1000) + 's'; if (d < 3600000) return Math.floor(d/60000) + 'm'; if (d < 86400000) return Math.floor(d/3600000) + 'h'; return Math.floor(d/86400000) + 'd'; }
function fmtCountdown(ts) { if (!ts) return '—'; var d = ts - Date.now(); if (d <= 0) return 'now'; var s = Math.floor(d/1000); var h = Math.floor(s/3600), m = Math.floor((s%3600)/60), ss = s%60; var o = []; if (h > 0) o.push(h + 'h'); if (m > 0 || h > 0) o.push(m + 'm'); o.push(ss + 's'); return o.join(' '); }
function fmtUptime(sec) { if (!sec) return '—'; var d = Math.floor(sec/86400), h = Math.floor((sec%86400)/3600), m = Math.floor((sec%3600)/60); if (d > 0) return d + 'd ' + h + 'h'; if (h > 0) return h + 'h ' + m + 'm'; return m + 'm'; }
function fmtSize(n) { if (!n) return '0B'; if (n < 1024) return n + 'B'; if (n < 1048576) return (n/1024).toFixed(1) + 'KB'; return (n/1048576).toFixed(1) + 'MB'; }
function setAccent(name) { var map = { mint: 'var(--ok)', amber: 'var(--warn)', blue: 'var(--info)', coral: 'var(--err)' }; document.documentElement.style.setProperty('--accent', map[name] || 'var(--ok)'); els('#s-swatches .swatch').forEach(function(s){ s.classList.toggle('active', s.dataset.accent === name); }); }

function applyTheme(mode) { if (mode === 'light') document.body.classList.add('light'); else document.body.classList.remove('light'); try { localStorage.setItem('pool_theme', mode); } catch(e) {} var btn = el('#theme-toggle'); if (btn) btn.innerHTML = (mode === 'light' ? ic('sun') : ic('moonSm')) + ' ' + (mode === 'light' ? 'Dark Mode' : 'Light Mode'); }
(function initTheme(){ var saved = 'dark'; try { saved = localStorage.getItem('pool_theme') || 'dark'; } catch(e) {} applyTheme(saved); var btn = el('#theme-toggle'); if (btn) btn.addEventListener('click', function() { var cur = document.body.classList.contains('light') ? 'dark' : 'light'; applyTheme(cur); }); })();

var showReasoning = true;
try { showReasoning = localStorage.getItem('pool_show_reasoning') !== '0'; } catch(e) {}
(function initReasoningToggle() {
  var btn = el('#toggle-reasoning');
  if (btn) {
    if (!showReasoning) { document.body.classList.add('hide-reasoning'); btn.textContent = 'Show Think'; }
    btn.addEventListener('click', function() {
      showReasoning = !showReasoning;
      try { localStorage.setItem('pool_show_reasoning', showReasoning ? '1' : '0'); } catch(e) {}
      document.body.classList.toggle('hide-reasoning', !showReasoning);
      btn.textContent = showReasoning ? 'Hide Think' : 'Show Think';
    });
  }
})();

function setMode(mode) {
  currentMode = (mode === 'chat') ? 'chat' : 'agent';
  var bChat = el('#mode-chat');
  var bAgent = el('#mode-agent');
  if (bChat) { bChat.classList.toggle('active', currentMode === 'chat'); bChat.classList.toggle('chat', currentMode === 'chat'); }
  if (bAgent) { bAgent.classList.toggle('active', currentMode === 'agent'); bAgent.classList.toggle('agent', currentMode === 'agent'); }
  var inp = el('#chat-input');
  if (inp) inp.placeholder = currentMode === 'chat' ? 'Ask anything... (Chat mode: web search, time, calc)' : 'Describe task... (Agent mode: full tools)';
}
el('#mode-chat').addEventListener('click', function() { setMode('chat'); });
el('#mode-agent').addEventListener('click', function() { setMode('agent'); });

function escHtml(s) { return String(s == null ? '' : s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
function inlineMd(s) {
  s = s.replace(/\u0060([^\u0060]+)\u0060/g, '<code>$1</code>');
  s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/(?<![a-zA-Z0-9_*])\*([^*\n]+)\*(?![a-zA-Z0-9_*])/g, '<em>$1</em>');
  s = s.replace(/~~([^~]+)~~/g, '<del>$1</del>');
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
  return s;
}
function renderCodeBlock(cb) {
  var lang = cb.lang || 'code';
  var code = escHtml(cb.code);
  return '<div class="code-block"><div class="code-head"><span class="code-lang">' + escHtml(lang) + '</span><button type="button" class="copy-btn">copy</button></div><pre><code class="lang-' + escHtml(lang) + '">' + code + '</code></pre></div>';
}
function mdRender(text) {
  if (!text) return '';
  var codeBlocks = [];
  var s = String(text);
  s = s.replace(/\u0060\u0060\u0060(\w*)\n?([\s\S]*?)\u0060\u0060\u0060/g, function(m, lang, code) { var idx = codeBlocks.length; codeBlocks.push({ lang: lang, code: code.replace(/\n$/, '') }); return '\u0000CB' + idx + '\u0000'; });
  s = s.replace(/\u0060\u0060\u0060(\w*)\n?([\s\S]*)$/g, function(m, lang, code) { var idx = codeBlocks.length; codeBlocks.push({ lang: lang, code: code, unclosed: true }); return '\u0000CB' + idx + '\u0000'; });
  s = s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  var lines = s.split('\n');
  var out = [];
  var i = 0;
  var paraBuf = [];
  function flushPara() { if (paraBuf.length) { out.push('<p>' + inlineMd(paraBuf.join(' ')) + '</p>'); paraBuf = []; } }
  while (i < lines.length) {
    var line = lines[i];
    if (/^\u0000CB\d+\u0000$/.test(line.trim())) { flushPara(); var idx = parseInt(line.trim().replace(/\u0000CB/, '').replace(/\u0000/, ''), 10); out.push(renderCodeBlock(codeBlocks[idx])); i++; continue; }
    if (/^---+ *$/.test(line)) { flushPara(); out.push('<hr>'); i++; continue; }
    var hm = line.match(/^(#{1,6})\s+(.+)$/);
    if (hm) { flushPara(); var lv = hm[1].length; out.push('<h' + lv + '>' + inlineMd(hm[2]) + '</h' + lv + '>'); i++; continue; }
    if (line.indexOf('|') >= 0 && i + 1 < lines.length) {
      var nextLine = lines[i + 1];
      if (/^\s*\|?[\s:-]+\|[\s|:-]*$/.test(nextLine)) {
        flushPara();
        var headerCells = line.replace(/^\s*\||\|\s*$/g, '').split('|').map(function(c){ return c.trim(); });
        var aligns = nextLine.replace(/^\s*\||\|\s*$/g, '').split('|').map(function(c){ c = c.trim(); if (/^:-+:$/.test(c)) return 'center'; if (/^-+:$/.test(c)) return 'right'; return 'left'; });
        i += 2;
        var rows = [];
        while (i < lines.length && lines[i].indexOf('|') >= 0 && !/^\s*$/.test(lines[i])) { rows.push(lines[i].replace(/^\s*\||\|\s*$/g, '').split('|').map(function(c){ return c.trim(); })); i++; }
        var t = '<table class="md-table"><thead><tr>';
        headerCells.forEach(function(h, ci) { t += '<th style="text-align:' + (aligns[ci] || 'left') + '">' + inlineMd(h) + '</th>'; });
        t += '</tr></thead><tbody>';
        rows.forEach(function(row) { t += '<tr>'; for (var ci2 = 0; ci2 < headerCells.length; ci2++) t += '<td style="text-align:' + (aligns[ci2] || 'left') + '">' + inlineMd(row[ci2] || '') + '</td>'; t += '</tr>'; });
        t += '</tbody></table>';
        out.push(t);
        continue;
      }
    }
    if (/^>\s?/.test(line)) {
      flushPara();
      var qLines = [];
      while (i < lines.length && /^>\s?/.test(lines[i])) { qLines.push(lines[i].replace(/^>\s?/, '')); i++; }
      out.push('<blockquote>' + inlineMd(qLines.join(' ')) + '</blockquote>');
      continue;
    }
    var ulm = line.match(/^([-*+])\s+(\[[ xX]\]\s+)?(.+)$/);
    if (ulm) {
      flushPara();
      out.push('<ul>');
      while (i < lines.length) {
        var lm = lines[i].match(/^([-*+])\s+(\[[ xX]\]\s+)?(.+)$/);
        if (!lm) break;
        var isTask = !!lm[2], checked = lm[2] && /[xX]/.test(lm[2]);
        if (isTask) out.push('<li class="task"><input type="checkbox" disabled' + (checked ? ' checked' : '') + '> ' + inlineMd(lm[3]) + '</li>');
        else out.push('<li>' + inlineMd(lm[3]) + '</li>');
        i++;
      }
      out.push('</ul>');
      continue;
    }
    var olm = line.match(/^\d+\.\s+(.+)$/);
    if (olm) {
      flushPara();
      out.push('<ol>');
      while (i < lines.length) {
        var om = lines[i].match(/^\d+\.\s+(.+)$/);
        if (!om) break;
        out.push('<li>' + inlineMd(om[1]) + '</li>');
        i++;
      }
      out.push('</ol>');
      continue;
    }
    if (/^\s*$/.test(line)) { flushPara(); i++; continue; }
    paraBuf.push(line);
    i++;
  }
  flushPara();
  return out.join('\n');
}

els('.rail nav button[data-tab]').forEach(function(b) { b.addEventListener('click', function() { switchTab(b.dataset.tab); }); });
function switchTab(name) {
  currentTab = name;
  els('.rail nav button[data-tab]').forEach(function(b) { b.classList.toggle('active', b.dataset.tab === name); });
  els('.tab').forEach(function(t) { t.classList.toggle('active', t.id === 'tab-' + name); });
  if (name === 'chat') { loadConvList(); loadAgents(); populateChatModels(); updateSendButtonState(); }
  if (name === 'agents') loadAgents();
  if (name === 'tools') loadTools();
  if (name === 'playground') populatePgModels();
  if (name === 'models') loadModels();
  if (name === 'mcp') loadMCP();
  if (name === 'overview' || name === 'accounts') refresh();
  if (name === 'providers') renderProviders();
  if (name === 'logs') startLogStream();
  if (name === 'settings') { loadSettings(); renderSystemInfo(); }
}

async function refresh() {
  if (refreshAbort) refreshAbort.abort();
  refreshAbort = new AbortController();
  try {
    var s = await api('/admin/state', { signal: refreshAbort.signal });
    state = s;
    if (state.settings && state.settings.accent) setAccent(state.settings.accent);
    renderOverview(); renderAccounts(); renderProviders();
    if (currentTab === 'settings') renderSystemInfo();
  } catch (e) { if (e.name === 'AbortError') return; if (e.status === 401) return; }
}
function renderSystemInfo() {
  var box = el('#sys-info'); if (!state || !state.system) { box.textContent = '—'; return; }
  var sy = state.system, h = '';
  h += '<div><span class="k">Node</span>' + esc(sy.node) + '</div>';
  h += '<div><span class="k">Platform</span>' + esc(sy.platform) + '/' + esc(sy.arch) + '</div>';
  h += '<div><span class="k">Uptime</span>' + fmtUptime(sy.uptimeSec) + '</div>';
  h += '<div><span class="k">RSS</span>' + sy.memRSSMB + ' MB</div>';
  h += '<div><span class="k">Heap</span>' + sy.memHeapMB + ' MB</div>';
  h += '<div><span class="k">Pool file</span><span style="color:var(--fg-3);font-size:10px">' + esc(sy.poolFile) + '</span></div>';
  box.innerHTML = h;
}

function renderOverview() {
  var box = el('#overview-body'); if (!state) { box.innerHTML = '<div class="empty">connecting...</div>'; return; }
  var total = state.accounts.length, ready = 0, cooldown = 0, dead = 0;
  state.accounts.forEach(function(a){ if (a.status === 'ready' && a.enabled) ready++; else if (a.status === 'cooldown' && a.enabled) cooldown++; else if (a.status === 'dead') dead++; });
  if (total === 0) { box.innerHTML = '<div class="panel"><div class="panel-body"><div class="ascii">  ┌─────┐\n  │  ▯  │\n  │ ▯ ▯ │  no accounts yet\n  │  ▯  │\n  └─────┘</div><p>Add your first account to start.</p><button class="btn primary" id="ov-add">+ Tambah Akun</button></div></div>'; var b = el('#ov-add'); if (b) b.addEventListener('click', function(){ switchTab('accounts'); setTimeout(function(){ el('#btn-add-acc').click(); }, 50); }); return; }
  var html = '<div class="stats-grid">';
  html += '<div class="stat"><div class="lbl">Total</div><div class="val num">' + total + '</div></div>';
  html += '<div class="stat"><div class="lbl">Ready</div><div class="val num ok">' + ready + '</div></div>';
  html += '<div class="stat"><div class="lbl">Cooldown</div><div class="val num warn">' + cooldown + '</div></div>';
  html += '<div class="stat"><div class="lbl">Dead</div><div class="val num err">' + dead + '</div></div>';
  html += '</div>';
  box.innerHTML = html;
}

function filteredAccounts() {
  if (!state) return [];
  return state.accounts.filter(function(a) {
    if (accFilter.provider && a.provider !== accFilter.provider) return false;
    if (accFilter.status && a.status !== accFilter.status) return false;
    if (accFilter.search) { var q = accFilter.search.toLowerCase(); if (!(a.label || '').toLowerCase().includes(q) && !(a.id || '').toLowerCase().includes(q)) return false; }
    return true;
  });
}
function renderAccounts() {
  if (!state) return;
  var pf = el('#f-provider'); var cur = pf.value;
  var opts = '<option value="">All providers</option>';
  Object.keys(state.providers).forEach(function(n){ opts += '<option value="' + esc(n) + '">' + esc(n) + '</option>'; });
  if (pf.innerHTML !== opts) { pf.innerHTML = opts; pf.value = cur; }
  var list = filteredAccounts(); var wrap = el('#acc-table-wrap');
  if (!list.length) { wrap.innerHTML = '<div class="empty">no accounts</div>'; return; }
  var h = '<table><thead><tr>';
  h += '<th style="width:24px"><input type="checkbox" id="acc-sel-all"></th>';
  h += '<th>provider</th><th>label / model</th><th>status</th><th class="num">sisa</th><th>reset</th><th class="num">prio</th><th>last used</th><th>aksi</th>';
  h += '</tr></thead><tbody>';
  list.forEach(function(a) {
    var prov = state.providers[a.provider]; var paid = prov && prov.paid;
    var effModel = a.model || (prov && prov.models ? (prov.models[state.settings.defaultAlias] || prov.models.smart) : null);
    var modelLine = a.model ? '<div style="color:var(--warn);font-size:9px;margin-top:2px">◆ ' + esc(a.model) + '</div>' : (effModel ? '<div style="color:var(--fg-3);font-size:9px;margin-top:2px">◇ ' + esc(effModel) + '</div>' : '');
    var errLine = a.lastError ? '<div style="color:var(--err);font-size:9px;margin-top:3px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(a.lastError) + '">' + ic('warn') + ' ' + esc(a.lastError.slice(0, 90)) + '</div>' : '';
    var sel = accSelected.has(a.id) ? ' checked' : '';
    h += '<tr' + (a.enabled ? '' : ' style="opacity:.5"') + '>';
    h += '<td><input type="checkbox" data-sel="' + esc(a.id) + '"' + sel + '></td>';
    h += '<td>' + esc(a.provider) + (paid ? ' <span class="badge warn">PAID</span>' : '') + '</td>';
    h += '<td>' + esc(a.label || '') + modelLine + errLine + '</td>';
    h += '<td><span class="badge ' + (a.status==='ready'?'ok':a.status==='cooldown'?'warn':'err') + '">' + a.status + '</span></td>';
    h += '<td class="num">' + (a.remaining == null ? '—' : a.remaining) + '</td>';
    h += '<td class="num">' + (a.status === 'cooldown' && a.resetAt ? fmtCountdown(a.resetAt) : '—') + '</td>';
    h += '<td class="num">' + (a.priority == null ? '—' : a.priority) + '</td>';
    h += '<td class="num">' + fmtAgo(a.lastUsed) + '</td>';
    h += '<td><button class="btn small ghost" data-act="probe" data-id="' + esc(a.id) + '">Probe</button> <button class="btn small ghost" data-act="reset-probe" data-id="' + esc(a.id) + '" title="Clear errors then probe">Reset+Probe</button> <button class="btn small ghost" data-act="reset" data-id="' + esc(a.id) + '">Reset</button> <button class="btn small ghost" data-act="edit" data-id="' + esc(a.id) + '">Edit</button> <button class="btn small ghost danger" data-act="del" data-id="' + esc(a.id) + '">' + ic('x') + '</button></td>';
    h += '</tr>';
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;
  wrap.querySelectorAll('button[data-act]').forEach(function(b) {
    b.addEventListener('click', async function() {
      var act = b.dataset.act, id = b.dataset.id;
      try {
        if (act === 'probe') { var r = await api('/admin/accounts/' + id + '/probe', { method: 'POST' }); var rres = r.result || {}; if (rres.ok) toast('probe OK · model=' + (rres.model || '?'), 'ok'); else toast('probe FAIL · ' + (rres.lastError || rres.error || ('status ' + rres.status)), 'err'); refresh(); return; }
        if (act === 'reset-probe') { var rr = await api('/admin/accounts/' + id + '/reset-and-probe', { method: 'POST' }); var rrres = rr.result || {}; if (rrres.ok) toast('reset+probe OK · model=' + (rrres.model || '?'), 'ok'); else toast('reset+probe FAIL · ' + (rrres.lastError || rrres.error || ('status ' + rrres.status)), 'err'); refresh(); return; }
        if (act === 'reset') { await api('/admin/accounts/' + id + '/reset', { method: 'POST' }); toast('reset', 'ok'); refresh(); return; }
        if (act === 'edit') { editAccount(id); return; }
        if (act === 'del') { if (!(await confirmModal('Hapus akun ini?'))) return; await api('/admin/accounts/' + id, { method: 'DELETE' }); toast('deleted', 'ok'); refresh(); }
      } catch (e) { toast(e.message, 'err'); }
    });
  });
  var selAll = el('#acc-sel-all');
  if (selAll) { selAll.checked = list.length > 0 && list.every(function(a) { return accSelected.has(a.id); }); selAll.addEventListener('change', function() { if (selAll.checked) list.forEach(function(a) { accSelected.add(a.id); }); else list.forEach(function(a) { accSelected.delete(a.id); }); renderAccounts(); }); }
  wrap.querySelectorAll('input[data-sel]').forEach(function(cb) { cb.addEventListener('change', function() { if (cb.checked) accSelected.add(cb.dataset.sel); else accSelected.delete(cb.dataset.sel); }); });
}

el('#f-provider').addEventListener('change', function(e){ accFilter.provider = e.target.value; renderAccounts(); });
el('#f-status').addEventListener('change', function(e){ accFilter.status = e.target.value; renderAccounts(); });
el('#f-search').addEventListener('input', function(e){ accFilter.search = e.target.value; renderAccounts(); });

function editAccount(id) {
  var a = state.accounts.find(function(x){ return x.id === id; }); if (!a) return;
  var body = '<div class="field"><label>Label</label><input id="e-label" value="' + esc(a.label || '') + '"></div>' +
    '<div class="field"><label>API Key (blank = unchanged)</label><input id="e-apikey" type="password"></div>' +
    '<div class="field"><label>Account ID</label><input id="e-accountid" value="' + esc(a.accountId || '') + '"></div>' +
    '<div class="field"><label>Model override</label><input id="e-model" value="' + esc(a.model || '') + '" placeholder="blank = provider default"></div>' +
    '<div class="field"><label>Priority</label><input id="e-priority" type="number" value="' + (a.priority == null ? '' : a.priority) + '"></div>' +
    '<div class="field"><label>Enabled</label><select id="e-enabled"><option value="true"' + (a.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!a.enabled ? ' selected' : '') + '>no</option></select></div>';
  openModal('Edit Akun', body, {
    confirmLabel: 'Simpan',
    onConfirm: async function() {
      var payload = { label: el('#e-label').value.trim(), accountId: el('#e-accountid').value.trim() || null, model: el('#e-model').value.trim() || null, enabled: el('#e-enabled').value === 'true' };
      var pv = el('#e-priority').value; payload.priority = pv !== '' ? parseInt(pv, 10) : null;
      var ak = el('#e-apikey').value.trim(); if (ak) payload.apiKey = ak;
      await api('/admin/accounts/' + id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); refresh();
    }
  });
}
el('#btn-add-acc').addEventListener('click', function() {
  if (!state) return;
  var provOpts = Object.keys(state.providers).filter(function(n){ return state.providers[n].enabled; }).map(function(n){ return '<option value="' + esc(n) + '">' + esc(n) + '</option>'; }).join('');
  var body = '<div class="field"><label>Provider</label><select id="m-provider">' + provOpts + '</select></div>' +
    '<div class="field"><label>Label</label><input id="m-label"></div>' +
    '<div class="field"><label>API Key</label><input id="m-apikey" type="password"></div>' +
    '<div class="field" id="m-accid-wrap" style="display:none"><label>Account ID</label><input id="m-accountid"></div>' +
    '<div class="field"><label>Model override (optional)</label><input id="m-model"></div>' +
    '<div class="field"><label>Priority</label><input id="m-priority" type="number"></div>';
  var m = openModal('Tambah Akun', body, {
    confirmLabel: 'Tambah',
    onConfirm: async function() {
      var payload = { provider: el('#m-provider').value, label: el('#m-label').value.trim() || 'acc-' + Date.now(), apiKey: el('#m-apikey').value.trim(), accountId: el('#m-accountid').value.trim() || null, model: el('#m-model').value.trim() || null };
      var pv = el('#m-priority').value; if (pv) payload.priority = parseInt(pv, 10);
      if (!payload.apiKey) throw new Error('apiKey required');
      await api('/admin/accounts', { method: 'POST', body: payload });
      toast('added', 'ok'); refresh();
    }
  });
  var sel = m.overlay.querySelector('#m-provider');
  sel.addEventListener('change', function() { var p = state.providers[sel.value]; m.overlay.querySelector('#m-accid-wrap').style.display = (p && p.needsAccountId) ? '' : 'none'; });
});
el('#btn-bulk-acc').addEventListener('click', function() {
  var body = '<div class="field"><label>Bulk import (provider,label,apiKey[,accountId[,model]])</label><textarea id="bi-text" rows="8" placeholder="groq,a1,gsk_xxx,,llama-3.1-8b-instant"></textarea></div>';
  openModal('Import Massal', body, {
    confirmLabel: 'Import',
    onConfirm: async function() {
      var lines = el('#bi-text').value.split('\n').map(function(l){ return l.trim(); }).filter(Boolean);
      var items = [];
      lines.forEach(function(l) { var p = l.split(',').map(function(s){ return s.trim(); }); if (p.length < 3) return; items.push({ provider: p[0], label: p[1], apiKey: p[2], accountId: p[3] || null, model: p[4] || null }); });
      if (!items.length) throw new Error('no valid lines');
      var r = await api('/admin/accounts/bulk', { method: 'POST', body: items });
      toast('imported ' + r.added.length + ', errors ' + r.errors.length, r.errors.length ? 'warn' : 'ok'); refresh();
    }
  });
});
el('#btn-probe-sel').addEventListener('click', async function() { if (!accSelected.size) return toast('select first', 'warn'); try { await api('/admin/probe-all', { method: 'POST', body: { ids: Array.from(accSelected) } }); toast('probe done', 'ok'); refresh(); } catch (e) { toast(e.message, 'err'); } });
el('#btn-enable-sel').addEventListener('click', async function() { for (var id of Array.from(accSelected)) await api('/admin/accounts/' + id, { method: 'PATCH', body: { enabled: true } }); toast('enabled', 'ok'); refresh(); });
el('#btn-disable-sel').addEventListener('click', async function() { for (var id of Array.from(accSelected)) await api('/admin/accounts/' + id, { method: 'PATCH', body: { enabled: false } }); toast('disabled', 'ok'); refresh(); });
el('#btn-del-sel').addEventListener('click', async function() { if (!accSelected.size) return toast('select first', 'warn'); if (!(await confirmModal('Hapus ' + accSelected.size + ' akun?'))) return; for (var id of Array.from(accSelected)) await api('/admin/accounts/' + id, { method: 'DELETE' }); accSelected.clear(); refresh(); });

function renderProviders() {
  if (!state) return;
  var wrap = el('#prov-table-wrap');
  var h = '<table><thead><tr><th>Name</th><th>Base URL</th><th>TZ</th><th class="num">Priority</th><th>Paid</th><th>Enabled</th><th class="num">Accts</th><th>Models</th><th>Aksi</th></tr></thead><tbody>';
  Object.keys(state.providers).forEach(function(name) {
    var p = state.providers[name]; var cnt = state.accounts.filter(function(a){ return a.provider === name; }).length;
    h += '<tr><td style="color:var(--accent);font-weight:600">' + esc(name) + '</td>';
    h += '<td style="max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(p.base) + '</td>';
    h += '<td>' + esc(p.tz || 'UTC') + '</td>';
    h += '<td class="num">' + (p.priority || 0) + '</td>';
    h += '<td>' + (p.paid ? '<span class="badge warn">PAID</span>' : '<span class="badge ok">FREE</span>') + '</td>';
    h += '<td><select data-toggle="enabled" data-prov="' + esc(name) + '" style="background:transparent;border:none;font-size:10px"><option value="true"' + (p.enabled ? ' selected' : '') + '>ON</option><option value="false"' + (!p.enabled ? ' selected' : '') + '>OFF</option></select></td>';
    h += '<td class="num">' + cnt + '</td>';
    h += '<td style="font-size:9px;color:var(--fg-3)">' + esc((p.models && p.models.smart) || '—') + '</td>';
    h += '<td><button class="btn small ghost" data-edit-prov="' + esc(name) + '">Edit</button> <button class="btn small ghost danger" data-del="' + esc(name) + '"' + (cnt > 0 ? ' disabled title="hapus akun dulu"' : '') + '>' + ic('x') + '</button></td>';
    h += '</tr>';
  });
  h += '</tbody></table>';
  wrap.innerHTML = h;
  wrap.querySelectorAll('[data-toggle]').forEach(function(sel) { sel.addEventListener('change', async function() { var name = sel.dataset.prov, field = sel.dataset.toggle, val = sel.value === 'true'; var pl = {}; pl[field] = val; try { await api('/admin/providers/' + name, { method: 'PATCH', body: pl }); toast('updated', 'ok'); refresh(); } catch (e) { toast(e.message, 'err'); } }); });
  wrap.querySelectorAll('[data-edit-prov]').forEach(function(b) { b.addEventListener('click', function() { editProviderFull(b.dataset.editProv); }); });
  wrap.querySelectorAll('[data-del]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus provider ' + b.dataset.del + '?'))) return; try { await api('/admin/providers/' + b.dataset.del, { method: 'DELETE' }); toast('deleted', 'ok'); refresh(); } catch (e) { toast(e.message, 'err'); } }); });
}
el('#btn-add-prov').addEventListener('click', function() {
  var body = '<div class="field"><label>Name (huruf kecil/angka/_/-)</label><input id="p-name" placeholder="myprovider"></div>' +
    '<div class="field"><label>Base URL</label><input id="p-base" placeholder="https://api.example.com/v1"><div class="hint">Contoh Groq: https://api.groq.com/openai/v1</div></div>' +
    '<div class="form-row"><div class="field"><label>Timezone (IANA)</label><input id="p-tz" value="UTC"></div><div class="field"><label>Priority</label><input id="p-priority" type="number" value="50"></div></div>' +
    '<div class="form-row"><div class="field"><label>Model smart</label><input id="p-smart"></div><div class="field"><label>Model fast</label><input id="p-fast"></div></div>' +
    '<div class="form-row"><div class="field"><label>Paid</label><select id="p-paid"><option value="false">no</option><option value="true">yes</option></select></div><div class="field"><label>Needs accountId</label><select id="p-needs"><option value="false">no</option><option value="true">yes (cloudflare)</option></select></div></div>';
  openModal('Tambah Provider', body, {
    confirmLabel: 'Tambah',
    onConfirm: async function() {
      var name = el('#p-name').value.trim(), base = el('#p-base').value.trim();
      if (!name) throw new Error('name wajib');
      if (!/^[a-z0-9_\-]+$/i.test(name)) throw new Error('name hanya huruf/angka/_/-');
      if (!base) throw new Error('base URL wajib');
      if (!/^https?:\/\//i.test(base)) throw new Error('base URL harus diawali http:// atau https://');
      var pl = { name: name, base: base, tz: el('#p-tz').value.trim() || 'UTC', priority: parseInt(el('#p-priority').value, 10) || 50, models: { smart: el('#p-smart').value.trim(), fast: el('#p-fast').value.trim() }, paid: el('#p-paid').value === 'true', needsAccountId: el('#p-needs').value === 'true', enabled: true };
      try { await api('/admin/providers', { method: 'POST', body: pl }); toast('provider "' + name + '" ditambahkan', 'ok'); refresh(); }
      catch (e) { throw new Error('gagal simpan: ' + e.message); }
    }
  });
});
function editProviderFull(name) {
  var p = state.providers[name]; if (!p) return;
  var body = '<div class="field"><label>Name</label><input value="' + esc(name) + '" disabled></div>' +
    '<div class="field"><label>Base URL</label><input id="ep-base" value="' + esc(p.base) + '"></div>' +
    '<div class="form-row"><div class="field"><label>Timezone</label><input id="ep-tz" value="' + esc(p.tz || 'UTC') + '"></div><div class="field"><label>Priority</label><input id="ep-priority" type="number" value="' + (p.priority || 50) + '"></div></div>' +
    '<div class="form-row"><div class="field"><label>Model smart</label><input id="ep-smart" value="' + esc((p.models && p.models.smart) || '') + '"></div><div class="field"><label>Model fast</label><input id="ep-fast" value="' + esc((p.models && p.models.fast) || '') + '"></div></div>' +
    '<div class="form-row"><div class="field"><label>Paid</label><select id="ep-paid"><option value="false"' + (p.paid ? '' : ' selected') + '>no</option><option value="true"' + (p.paid ? ' selected' : '') + '>yes</option></select></div><div class="field"><label>Needs accountId</label><select id="ep-needs"><option value="false"' + (p.needsAccountId ? '' : ' selected') + '>no</option><option value="true"' + (p.needsAccountId ? ' selected' : '') + '>yes</option></select></div></div>' +
    '<div class="field"><label>Enabled</label><select id="ep-enabled"><option value="true"' + (p.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!p.enabled ? ' selected' : '') + '>no</option></select></div>';
  openModal('Edit Provider · ' + name, body, {
    confirmLabel: 'Simpan',
    onConfirm: async function() {
      var base = el('#ep-base').value.trim();
      if (!base || !/^https?:\/\//i.test(base)) throw new Error('base URL harus http(s)://');
      var pl = { base: base, tz: el('#ep-tz').value.trim() || 'UTC', priority: parseInt(el('#ep-priority').value, 10) || 50, models: { smart: el('#ep-smart').value.trim(), fast: el('#ep-fast').value.trim() }, paid: el('#ep-paid').value === 'true', needsAccountId: el('#ep-needs').value === 'true', enabled: el('#ep-enabled').value === 'true' };
      await api('/admin/providers/' + name, { method: 'PATCH', body: pl });
      toast('provider diperbarui', 'ok'); refresh();
    }
  });
}

async function loadAgents() { try { agentData = await api('/admin/agents'); renderAgents(); renderSkills(); populateAgentSelect(); populateChatModels(); } catch (e) { toast(e.message, 'err'); } }
function renderAgents() {
  var wrap = el('#agents-table');
  if (!agentData.agents.length) { wrap.innerHTML = '<div class="empty">no agents</div>'; return; }
  var h = '<div class="tbl-wrap"><table><thead><tr><th>Name</th><th>Mode</th><th>Model</th><th class="num">Temp</th><th class="num">Tools</th><th class="num">Skills</th><th>Enabled</th><th>Aksi</th></tr></thead><tbody>';
  agentData.agents.forEach(function(a) { var modeLabel = a.mode === 'chat' ? '<span class="badge info">CHAT</span>' : '<span class="badge ok">AGENT</span>'; h += '<tr><td><b>' + esc(a.name) + '</b><div style="color:var(--fg-3);font-size:9px">' + esc(a.description || '') + '</div></td><td>' + modeLabel + '</td><td>' + esc(a.model) + '</td><td class="num">' + (a.temperature || 0) + '</td><td class="num">' + (a.tools || []).length + '</td><td class="num">' + (a.skills || []).length + '</td><td>' + (a.enabled ? '<span class="badge ok">ON</span>' : '<span class="badge mute">OFF</span>') + '</td><td><button class="btn small ghost" data-ae="' + esc(a.id) + '">Edit</button> <button class="btn small ghost danger" data-ad="' + esc(a.id) + '">' + ic('x') + '</button></td></tr>'; });
  h += '</tbody></table></div>'; wrap.innerHTML = h;
  wrap.querySelectorAll('[data-ae]').forEach(function(b) { b.addEventListener('click', function() { editAgent(b.dataset.ae); }); });
  wrap.querySelectorAll('[data-ad]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus agent?'))) return; try { await api('/admin/agents/' + b.dataset.ad, { method: 'DELETE' }); toast('deleted', 'ok'); loadAgents(); } catch (e) { toast(e.message, 'err'); } }); });
}
function renderSkills() {
  var wrap = el('#skills-table');
  if (!agentData.skills.length) { wrap.innerHTML = '<div class="empty">no skills yet</div>'; return; }
  var h = '<div class="tbl-wrap"><table><thead><tr><th>Name</th><th>Description</th><th class="num">Tools</th><th>Aksi</th></tr></thead><tbody>';
  agentData.skills.forEach(function(s) { h += '<tr><td><b>' + esc(s.name) + '</b></td><td style="color:var(--fg-3);font-size:10px">' + esc(s.description || '') + '</td><td class="num">' + (s.tools || []).length + '</td><td><button class="btn small ghost" data-se="' + esc(s.id) + '">Edit</button> <button class="btn small ghost danger" data-sd="' + esc(s.id) + '">' + ic('x') + '</button></td></tr>'; });
  h += '</tbody></table></div>'; wrap.innerHTML = h;
  wrap.querySelectorAll('[data-se]').forEach(function(b) { b.addEventListener('click', function() { editSkill(b.dataset.se); }); });
  wrap.querySelectorAll('[data-sd]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus skill?'))) return; try { await api('/admin/skills/' + b.dataset.sd, { method: 'DELETE' }); toast('deleted', 'ok'); loadAgents(); } catch (e) { toast(e.message, 'err'); } }); });
}
function populateAgentSelect() {
  var sel = el('#chat-agent'); if (!sel) return;
  var cur = sel.value || currentAgentId, h = '';
  agentData.agents.forEach(function(a) { if (a.enabled) h += '<option value="' + esc(a.id) + '">' + esc(a.name) + ' [' + (a.mode || 'agent') + ']</option>'; });
  sel.innerHTML = h;
  if (cur && Array.prototype.some.call(sel.options, function(o){ return o.value === cur; })) sel.value = cur;
}
el('#btn-add-agent').addEventListener('click', function() { editAgent(null); });
el('#btn-add-skill').addEventListener('click', function() { editSkill(null); });
function editAgent(id) {
  var a = id ? agentData.agents.find(function(x){ return x.id === id; }) : null;
  var isNew = !a;
  if (!a) a = { id: null, name: '', description: '', systemPrompt: '', model: 'smart', mode: 'agent', temperature: 0.7, tools: ['builtin_current_time'], skills: [], enabled: true };
  var tc = toolList.map(function(t) { var checked = (a.tools || []).indexOf(t.function.name) >= 0 ? ' checked' : ''; return '<label class="tool-card' + (checked ? ' selected' : '') + '" style="cursor:pointer;display:block"><input type="checkbox" data-tool="' + esc(t.function.name) + '"' + checked + '><span class="tc-title">' + esc(t.function.name) + '</span><div class="tc-desc">' + esc(t.function.description || '') + '</div><div class="tc-source">' + esc(t.source) + '</div></label>'; }).join('');
  var sc = agentData.skills.map(function(s) { var checked = (a.skills || []).indexOf(s.id) >= 0 ? ' checked' : ''; return '<label style="display:block;padding:4px 0;cursor:pointer"><input type="checkbox" data-skill="' + esc(s.id) + '"' + checked + '> ' + esc(s.name) + '</label>'; }).join('');
  var body = '<div class="form-row"><div class="field"><label>Name</label><input id="ag-name" value="' + esc(a.name) + '"></div><div class="field"><label>Mode</label><select id="ag-mode"><option value="chat"' + (a.mode === 'chat' ? ' selected' : '') + '>Chat (ChatGPT-like)</option><option value="agent"' + (a.mode === 'agent' ? ' selected' : '') + '>Agent (OpenCode-like)</option></select></div></div>' +
    '<div class="form-row"><div class="field"><label>Model</label><input id="ag-model" value="' + esc(a.model) + '"></div><div class="field"><label>Description</label><input id="ag-desc" value="' + esc(a.description) + '"></div></div>' +
    '<div class="field"><label>System Prompt</label><textarea id="ag-sys" rows="6">' + esc(a.systemPrompt) + '</textarea></div>' +
    '<div class="form-row"><div class="field"><label>Temperature</label><input id="ag-temp" type="number" min="0" max="2" step="0.1" value="' + (a.temperature || 0.7) + '"></div><div class="field"><label>Enabled</label><select id="ag-enabled"><option value="true"' + (a.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!a.enabled ? ' selected' : '') + '>no</option></select></div></div>' +
    '<div class="field"><label>Tools (only in Agent mode)</label><div class="tool-grid">' + tc + '</div></div>' +
    '<div class="field"><label>Skills</label><div style="max-height:200px;overflow-y:auto;border:2px solid var(--line);padding:8px">' + (sc || '<div style="color:var(--fg-3);font-size:11px">no skills</div>') + '</div></div>';
  var m = openModal(isNew ? 'New Agent' : 'Edit Agent', body, {
    confirmLabel: 'Save', wide: true,
    onConfirm: async function() {
      var payload = { name: el('#ag-name').value.trim(), description: el('#ag-desc').value.trim(), systemPrompt: el('#ag-sys').value, model: el('#ag-model').value.trim() || 'smart', mode: el('#ag-mode').value, temperature: parseFloat(el('#ag-temp').value) || 0.7, enabled: el('#ag-enabled').value === 'true', tools: Array.from(m.overlay.querySelectorAll('input[data-tool]:checked')).map(function(x){ return x.dataset.tool; }), skills: Array.from(m.overlay.querySelectorAll('input[data-skill]:checked')).map(function(x){ return x.dataset.skill; }) };
      if (!payload.name) throw new Error('name required');
      if (isNew) await api('/admin/agents', { method: 'POST', body: payload }); else await api('/admin/agents/' + id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); loadAgents();
    }
  });
  m.overlay.querySelectorAll('input[data-tool]').forEach(function(cb) { cb.addEventListener('change', function() { cb.closest('.tool-card').classList.toggle('selected', cb.checked); }); });
}
function editSkill(id) {
  var s = id ? agentData.skills.find(function(x){ return x.id === id; }) : null;
  var isNew = !s;
  if (!s) s = { id: null, name: '', description: '', systemPrompt: '', tools: [] };
  var tc = toolList.map(function(t) { var checked = (s.tools || []).indexOf(t.function.name) >= 0 ? ' checked' : ''; return '<label style="display:block;padding:2px 0;cursor:pointer;font-size:11px"><input type="checkbox" data-skt="' + esc(t.function.name) + '"' + checked + '> ' + esc(t.function.name) + '</label>'; }).join('');
  var body = '<div class="field"><label>Name</label><input id="sk-name" value="' + esc(s.name) + '"></div>' +
    '<div class="field"><label>Description</label><input id="sk-desc" value="' + esc(s.description) + '"></div>' +
    '<div class="field"><label>Prompt fragment</label><textarea id="sk-sys" rows="6">' + esc(s.systemPrompt) + '</textarea></div>' +
    '<div class="field"><label>Tools</label><div style="max-height:200px;overflow-y:auto;border:2px solid var(--line);padding:8px">' + tc + '</div></div>';
  var m = openModal(isNew ? 'New Skill' : 'Edit Skill', body, {
    confirmLabel: 'Save', wide: true,
    onConfirm: async function() {
      var payload = { name: el('#sk-name').value.trim(), description: el('#sk-desc').value.trim(), systemPrompt: el('#sk-sys').value, tools: Array.from(m.overlay.querySelectorAll('input[data-skt]:checked')).map(function(x){ return x.dataset.skt; }) };
      if (!payload.name) throw new Error('name required');
      if (isNew) await api('/admin/skills', { method: 'POST', body: payload }); else await api('/admin/skills/' + id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); loadAgents();
    }
  });
}

async function loadTools() { try { var r = await api('/admin/tools'); toolList = r.tools; renderTools(); loadPlugins(); } catch (e) { toast(e.message, 'err'); } }
function renderTools() {
  var grid = el('#tools-grid');
  var filtered = toolList.filter(function(t) { return toolsSub === 'all' || t.source === toolsSub; });
  if (!filtered.length) { grid.innerHTML = '<div class="empty">no tools</div>'; return; }
  grid.innerHTML = filtered.map(function(t) { var params = t.function.parameters && t.function.parameters.properties ? Object.keys(t.function.parameters.properties).join(', ') : ''; return '<div class="tool-card"><div class="tc-title">' + esc(t.function.name) + '</div><div class="tc-desc">' + esc(t.function.description || '') + '</div>' + (params ? '<div style="font-size:9px;color:var(--fg-3);margin-top:4px">params: ' + esc(params) + '</div>' : '') + '<div class="tc-source">source: ' + esc(t.source) + '</div></div>'; }).join('');
}
els('#tools-subtabs button').forEach(function(b) { b.addEventListener('click', function() { toolsSub = b.dataset.sub; els('#tools-subtabs button').forEach(function(x){ x.classList.toggle('active', x === b); }); renderTools(); }); });
async function loadPlugins() { try { var r = await api('/admin/plugins'); renderPlugins(r.plugins); } catch (e) {} }
function renderPlugins(list) {
  var wrap = el('#plugins-table');
  if (!list.length) { wrap.innerHTML = '<div class="empty">no plugins</div>'; return; }
  var h = '<div class="tbl-wrap"><table><thead><tr><th>Name</th><th>Enabled</th><th class="num">Tools</th><th>Aksi</th></tr></thead><tbody>';
  list.forEach(function(p) { h += '<tr><td><b>' + esc(p.name) + '</b></td><td>' + (p.enabled ? '<span class="badge ok">ON</span>' : '<span class="badge mute">OFF</span>') + '</td><td class="num">' + p.toolsLoaded + '</td><td><button class="btn small ghost" data-pe="' + esc(p.id) + '">Edit</button> <button class="btn small ghost danger" data-pd="' + esc(p.id) + '">' + ic('x') + '</button></td></tr>'; });
  h += '</tbody></table></div>'; wrap.innerHTML = h;
  wrap.querySelectorAll('[data-pe]').forEach(function(b) { b.addEventListener('click', function() { editPlugin(list.find(function(x){ return x.id === b.dataset.pe; })); }); });
  wrap.querySelectorAll('[data-pd]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus plugin?'))) return; try { await api('/admin/plugins/' + b.dataset.pd, { method: 'DELETE' }); toast('deleted', 'ok'); loadTools(); } catch (e) { toast(e.message, 'err'); } }); });
}
el('#btn-add-plugin').addEventListener('click', function() { editPlugin(null); });
function editPlugin(p) {
  var isNew = !p;
  if (!p) p = { id: null, name: '', enabled: true, source: '// Example plugin — exports { tools: [...] }\n// module.exports = {\n//   tools: [{\n//     name: "hello",\n//     description: "Say hello",\n//     parameters: { type: "object", properties: { name: { type: "string" } }, required: ["name"] },\n//     handler: async (args) => ({ greeting: "Hello " + args.name })\n//   }]\n// };\n' };
  var body = '<div class="field"><label>Name</label><input id="pl-name" value="' + esc(p.name) + '"></div>' +
    '<div class="field"><label>Enabled</label><select id="pl-enabled"><option value="true"' + (p.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!p.enabled ? ' selected' : '') + '>no</option></select></div>' +
    '<div class="field"><label>Source (JS module)</label><textarea id="pl-src" rows="14" style="font-family:monospace;font-size:11px">' + esc(p.source) + '</textarea><div class="hint" style="color:var(--warn)">' + ic('warn') + ' Dieksekusi saat load.</div></div>';
  openModal(isNew ? 'New Plugin' : 'Edit Plugin', body, {
    confirmLabel: 'Save', wide: true,
    onConfirm: async function() {
      var payload = { name: el('#pl-name').value.trim(), enabled: el('#pl-enabled').value === 'true', source: el('#pl-src').value };
      if (!payload.name || !payload.source) throw new Error('name + source required');
      if (isNew) await api('/admin/plugins', { method: 'POST', body: payload }); else await api('/admin/plugins/' + p.id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); loadTools();
    }
  });
}

async function loadMCP() { try { var r = await api('/admin/mcp'); renderMCP(r.servers); } catch (e) {} }
function renderMCP(list) {
  var wrap = el('#mcp-table');
  if (!list.length) { wrap.innerHTML = '<div class="empty">no MCP servers</div>'; return; }
  var h = '<div class="tbl-wrap"><table><thead><tr><th>Name</th><th>Transport</th><th>Target</th><th>Status</th><th class="num">Tools</th><th>Aksi</th></tr></thead><tbody>';
  list.forEach(function(s) {
    var target = s.transport === 'stdio' ? s.command + ' ' + (s.args || []).join(' ') : s.url;
    var sb = s.status === 'connected' ? '<span class="badge ok">connected</span>' : (s.status === 'error' ? '<span class="badge err">error</span>' : '<span class="badge mute">disconnected</span>');
    h += '<tr><td><b>' + esc(s.name) + '</b>' + (s.lastError ? '<div style="color:var(--err);font-size:9px">' + esc(s.lastError) + '</div>' : '') + '</td>';
    h += '<td>' + esc(s.transport) + '</td>';
    h += '<td style="font-size:9px;color:var(--fg-3);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(target) + '</td>';
    h += '<td>' + sb + '</td><td class="num">' + (s.tools ? s.tools.length : 0) + '</td>';
    h += '<td>' + (s.status === 'connected' ? '<button class="btn small ghost" data-mdisc="' + esc(s.id) + '">Disconnect</button> ' : '<button class="btn small ghost" data-mcon="' + esc(s.id) + '">Connect</button> ') + '<button class="btn small ghost" data-med="' + esc(s.id) + '">Edit</button> <button class="btn small ghost danger" data-mdd="' + esc(s.id) + '">' + ic('x') + '</button></td></tr>';
  });
  h += '</tbody></table></div>'; wrap.innerHTML = h;
  wrap.querySelectorAll('[data-mcon]').forEach(function(b) { b.addEventListener('click', async function() { toast('connecting...', 'warn'); try { await api('/admin/mcp/' + b.dataset.mcon + '/connect', { method: 'POST' }); toast('connected', 'ok'); loadMCP(); loadTools(); } catch(e) { toast(e.message, 'err'); } }); });
  wrap.querySelectorAll('[data-mdisc]').forEach(function(b) { b.addEventListener('click', async function() { try { await api('/admin/mcp/' + b.dataset.mdisc + '/disconnect', { method: 'POST' }); toast('disconnected', 'ok'); loadMCP(); loadTools(); } catch(e) { toast(e.message, 'err'); } }); });
  wrap.querySelectorAll('[data-med]').forEach(function(b) { b.addEventListener('click', function() { editMCP(list.find(function(x){ return x.id === b.dataset.med; })); }); });
  wrap.querySelectorAll('[data-mdd]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus MCP server?'))) return; try { await api('/admin/mcp/' + b.dataset.mdd, { method: 'DELETE' }); toast('deleted', 'ok'); loadMCP(); loadTools(); } catch(e) { toast(e.message, 'err'); } }); });
}
el('#btn-add-mcp').addEventListener('click', function() { editMCP(null); });
function editMCP(s) {
  var isNew = !s;
  if (!s) s = { id: null, name: '', transport: 'stdio', command: 'npx', args: ['-y', '@modelcontextprotocol/server-filesystem', '/tmp'], env: {}, url: '', headers: {}, enabled: true };
  var body = '<div class="field"><label>Name</label><input id="mc-name" value="' + esc(s.name) + '"></div>' +
    '<div class="field"><label>Transport</label><select id="mc-transport"><option value="stdio"' + (s.transport === 'stdio' ? ' selected' : '') + '>stdio</option><option value="http"' + (s.transport === 'http' ? ' selected' : '') + '>http (SSE)</option></select></div>' +
    '<div id="mc-stdio-fields" style="display:' + (s.transport === 'stdio' ? 'block' : 'none') + '">' +
    '<div class="field"><label>Command</label><input id="mc-command" value="' + esc(s.command) + '"></div>' +
    '<div class="field"><label>Args (one per line)</label><textarea id="mc-args" rows="4">' + esc((s.args || []).join('\n')) + '</textarea></div>' +
    '<div class="field"><label>Env (KEY=VALUE per line)</label><textarea id="mc-env" rows="3">' + esc(Object.keys(s.env || {}).map(function(k){ return k + '=' + s.env[k]; }).join('\n')) + '</textarea></div>' +
    '</div>' +
    '<div id="mc-http-fields" style="display:' + (s.transport === 'http' ? 'block' : 'none') + '">' +
    '<div class="field"><label>URL</label><input id="mc-url" value="' + esc(s.url) + '"></div>' +
    '<div class="field"><label>Headers (KEY=VALUE per line)</label><textarea id="mc-headers" rows="3">' + esc(Object.keys(s.headers || {}).map(function(k){ return k + '=' + s.headers[k]; }).join('\n')) + '</textarea></div>' +
    '</div>' +
    '<div class="field"><label>Enabled</label><select id="mc-enabled"><option value="true"' + (s.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!s.enabled ? ' selected' : '') + '>no</option></select></div>';
  var m = openModal(isNew ? 'Connect MCP' : 'Edit MCP', body, {
    confirmLabel: 'Save', wide: true,
    onConfirm: async function() {
      var transport = el('#mc-transport').value;
      function parseKV(txt) { var o = {}; txt.split('\n').forEach(function(l) { var i = l.indexOf('='); if (i > 0) o[l.slice(0, i).trim()] = l.slice(i + 1).trim(); }); return o; }
      var payload = { name: el('#mc-name').value.trim(), transport: transport, enabled: el('#mc-enabled').value === 'true', command: el('#mc-command') ? el('#mc-command').value.trim() : '', args: el('#mc-args') ? el('#mc-args').value.split('\n').map(function(x){ return x.trim(); }).filter(Boolean) : [], env: el('#mc-env') ? parseKV(el('#mc-env').value) : {}, url: el('#mc-url') ? el('#mc-url').value.trim() : '', headers: el('#mc-headers') ? parseKV(el('#mc-headers').value) : {} };
      if (!payload.name) throw new Error('name required');
      if (isNew) await api('/admin/mcp', { method: 'POST', body: payload }); else await api('/admin/mcp/' + s.id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); loadMCP(); loadTools();
    }
  });
  var tsel = m.overlay.querySelector('#mc-transport');
  tsel.addEventListener('change', function() {
    m.overlay.querySelector('#mc-stdio-fields').style.display = tsel.value === 'stdio' ? 'block' : 'none';
    m.overlay.querySelector('#mc-http-fields').style.display = tsel.value === 'http' ? 'block' : 'none';
  });
}

async function loadConvList() {
  try {
    var r = await api('/admin/chats');
    var list = el('#conv-list');
    var all = r.conversations || [];
    var q = (convSearchTerm || '').toLowerCase().trim();
    var filtered = q ? all.filter(function(c) { return (c.title || '').toLowerCase().indexOf(q) >= 0 || (c.id || '').toLowerCase().indexOf(q) >= 0; }) : all;
    if (!filtered.length) { list.innerHTML = '<div class="empty">' + (q ? 'no match' : 'no chats') + '</div>'; return; }
    list.innerHTML = filtered.map(function(c) {
      var active = c.id === currentConvId ? ' active' : '';
      var pinnedCls = c.pinned ? ' pinned' : '';
      var pinBadge = c.pinned ? '<span class="conv-pin-badge">' + ic('starF') + '</span>' : '';
      var streamBadge = activeStreams.has(c.id) ? '<span style="color:var(--accent);animation:blink 1s steps(2,start) infinite">●</span> ' : '';
      return '<div class="conv-item' + active + pinnedCls + '" data-cid="' + esc(c.id) + '">' +
        '<div class="cv-title">' + streamBadge + pinBadge + esc(c.title || '(untitled)') + '</div>' +
        '<div class="cv-meta"><span>' + (c.messageCount || 0) + ' msg</span><span>' + fmtAgo(c.updatedAt) + '</span></div>' +
        '<div class="conv-actions">' +
          '<button class="conv-action" data-cact="pin" data-cid="' + esc(c.id) + '" title="' + (c.pinned ? 'unpin' : 'pin') + '">' + (c.pinned ? ic('starF') : ic('starE')) + '</button>' +
          '<button class="conv-action" data-cact="rename" data-cid="' + esc(c.id) + '" title="rename">' + ic('pencil') + '</button>' +
          '<button class="conv-action danger" data-cact="del" data-cid="' + esc(c.id) + '" title="delete">' + ic('x') + '</button>' +
        '</div>' +
        '</div>';
    }).join('');
    list.querySelectorAll('.conv-item').forEach(function(item) {
      item.addEventListener('click', function(e) { if (e.target.closest('.conv-actions')) return; openConversation(item.dataset.cid); });
      var titleEl = item.querySelector('.cv-title');
      titleEl.addEventListener('dblclick', function(e) { e.stopPropagation(); startRenameConv(item.dataset.cid, titleEl); });
    });
    list.querySelectorAll('.conv-action').forEach(function(b) {
      b.addEventListener('click', function(e) {
        e.stopPropagation();
        var cid = b.dataset.cid;
        if (b.dataset.cact === 'pin') togglePinConv(cid);
        else if (b.dataset.cact === 'rename') { var item = b.closest('.conv-item'); startRenameConv(cid, item.querySelector('.cv-title')); }
        else if (b.dataset.cact === 'del') deleteConv(cid);
      });
    });
  } catch (e) {}
}

async function togglePinConv(cid) {
  try { var r = await api('/admin/chats/' + cid); var cur = r.conversation; await api('/admin/chats/' + cid, { method: 'PATCH', body: { pinned: !cur.pinned } }); loadConvList(); } catch (e) { toast(e.message, 'err'); }
}
async function startRenameConv(cid, titleEl) {
  if (titleEl.querySelector('input')) return;
  var currentTitle = titleEl.textContent.replace(/^★\s*/, '').replace(/\s*★$/, '').trim();
  var origHTML = titleEl.innerHTML;
  titleEl.innerHTML = '<input class="conv-title-input" value="' + esc(currentTitle) + '">';
  var inp = titleEl.querySelector('input');
  inp.focus(); inp.select();
  var cancelled = false;
  inp.addEventListener('keydown', function(e) { if (e.key === 'Enter') { e.preventDefault(); inp.blur(); } else if (e.key === 'Escape') { cancelled = true; inp.blur(); } });
  inp.addEventListener('click', function(e){ e.stopPropagation(); });
  inp.addEventListener('dblclick', function(e){ e.stopPropagation(); });
  inp.addEventListener('blur', async function() {
    if (cancelled) { titleEl.innerHTML = origHTML; return; }
    var newTitle = inp.value.trim() || 'untitled';
    try { await api('/admin/chats/' + cid, { method: 'PATCH', body: { title: newTitle } }); if (currentConv && currentConv.id === cid) currentConv.title = newTitle; loadConvList(); }
    catch (e) { titleEl.innerHTML = origHTML; toast(e.message, 'err'); }
  });
}
async function deleteConv(cid) {
  if (!(await confirmModal('Hapus percakapan ini?'))) return;
  try { await api('/admin/chats/' + cid, { method: 'DELETE' }); if (currentConvId === cid) { currentConv = null; currentConvId = null; renderThread(); } toast('deleted', 'ok'); loadConvList(); } catch (e) { toast(e.message, 'err'); }
}

var convSearchTerm = '';
el('#conv-search').addEventListener('input', function(e) { convSearchTerm = e.target.value; loadConvList(); });
el('#conv-export').addEventListener('click', async function() {
  try { var tok = getToken(); var r = await fetch('/admin/chats/export', { headers: tok ? { 'Authorization': 'Bearer ' + tok } : {} }); var blob = await r.blob(); var a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'chats.json'; a.click(); URL.revokeObjectURL(a.href); } catch (e) { toast(e.message, 'err'); }
});
el('#conv-import').addEventListener('click', function() {
  var inp = document.createElement('input'); inp.type = 'file'; inp.accept = '.json,application/json';
  inp.onchange = async function() { var f = inp.files[0]; if (!f) return; var text = await f.text();
    try { var data = JSON.parse(text); var r = await api('/admin/chats/import', { method: 'POST', body: data }); toast('imported: ' + r.added + ' new, ' + r.updated + ' updated', 'ok'); loadConvList(); } catch (e) { toast(e.message, 'err'); }
  };
  inp.click();
});
el('#conv-delete-all').addEventListener('click', async function() {
  if (!(await confirmModal('Hapus SEMUA percakapan? Tidak bisa dibatalkan.'))) return;
  try { var r = await api('/admin/chats', { method: 'DELETE' }); currentConv = null; currentConvId = null; renderThread(); toast('deleted ' + r.count + ' conversations', 'ok'); loadConvList(); } catch (e) { toast(e.message, 'err'); }
});
async function openConversation(id) {
  try {
    var r = await api('/admin/chats/' + id);
    currentConv = r.conversation; currentConvId = id;
    if (r.conversation.agentId) { currentAgentId = r.conversation.agentId; el('#chat-agent').value = currentAgentId; }
    if (r.conversation.modeOverride === 'chat' || r.conversation.modeOverride === 'agent') setMode(r.conversation.modeOverride);
    renderThread(); loadConvList(); updateSendButtonState();
  } catch (e) { toast(e.message, 'err'); }
}
function newChat() { currentConv = null; currentConvId = null; renderThread(); loadConvList(); updateSendButtonState(); }
function renderThread() {
  var box = el('#chat-thread');
  if (!currentConv || !currentConv.messages || !currentConv.messages.length) {
    box.innerHTML = '<div class="empty" style="padding:60px 20px"><div class="ascii">  ┌───────────┐\n  │  ▯ chat  ▯ │\n  │ ▯ ▯ ▯ ▯ ▯ │\n  └───────────┘</div><p>Start a conversation.</p></div>';
    if (currentConvId && activeStreams.has(currentConvId)) { var _s2 = activeStreams.get(currentConvId); if (_s2 && _s2.container) box.appendChild(_s2.container); }
    updateSendButtonState();
    return;
  }
  box.innerHTML = currentConv.messages.map(function(m, i){ return renderMsg(m, i); }).join('');
  if (currentConvId && activeStreams.has(currentConvId)) { var _s = activeStreams.get(currentConvId); if (_s && _s.container) box.appendChild(_s.container); }
  box.querySelectorAll('[data-mact]').forEach(function(b) {
    b.addEventListener('click', function(e) { e.stopPropagation(); handleMsgAction(b.dataset.mact, parseInt(b.dataset.idx, 10)); });
  });
  box.scrollTop = box.scrollHeight;
  updateSendButtonState();
}
function renderMsg(m, idx) {
  if (m.role === 'user') {
    var hu = '<div class="msg msg-user">';
    hu += '<div class="msg-actions"><button class="msg-action-btn" data-mact="copy" data-idx="' + idx + '" title="copy">copy</button><button class="msg-action-btn" data-mact="edit" data-idx="' + idx + '" title="edit & regenerate">edit</button><button class="msg-action-btn danger" data-mact="truncate" data-idx="' + idx + '" title="hapus dari sini">del</button></div>';
    hu += '<div class="msg-role">USER' + (m.mode ? ' · ' + m.mode.toUpperCase() : '') + '</div>';
    if (m.attachments && m.attachments.length) {
      hu += '<div class="msg-attachments">';
      m.attachments.forEach(function(a) {
        if (a.type === 'image' && a.dataUrl) hu += '<img class="msg-attach-img" src="' + a.dataUrl + '" alt="' + esc(a.name) + '">';
        else hu += '<div class="msg-attach-file">' + ic('file') + ' ' + esc(a.name) + ' (' + fmtSize(a.size) + ')' + (a.truncated ? ' · preview only' : '') + '</div>';
      });
      hu += '</div>';
    }
    hu += '<div class="msg-content md-content">' + mdRender(m.content || '') + '</div></div>';
    return hu;
  }
  if (m.role === 'assistant') {
    var isLast = currentConv && idx === currentConv.messages.length - 1;
    var h = '<div class="msg msg-assistant">';
    h += '<div class="msg-actions">' + (m.content ? '<button class="msg-action-btn" data-mact="copy" data-idx="' + idx + '" title="copy">copy</button>' : '') + '<button class="msg-action-btn danger" data-mact="truncate" data-idx="' + idx + '" title="hapus dari sini">del</button>' + (isLast ? '<button class="msg-action-btn" data-mact="regen" data-idx="' + idx + '" title="regenerate">' + ic('regen') + '</button>' : '') + '</div>';
    h += '<div class="msg-role">ASSISTANT' + (m.mode ? ' · ' + m.mode.toUpperCase() : '') + '</div>';
    if (m.reasoning) {
      h += '<details class="msg-reasoning"><summary>Thinking (' + m.reasoning.length + ' chars)</summary><div class="reasoning-content">' + escHtml(m.reasoning) + '</div></details>';
    }
    if (m.content) h += '<div class="msg-content md-content">' + mdRender(m.content) + '</div>';
    if (m.tool_calls) { m.tool_calls.forEach(function(tc) { h += '<div class="msg-tool-block"><div><span class="tc-name">' + ic('chevD') + ' ' + esc(tc.function.name) + '</span></div><div class="tc-args">' + esc(tc.function.arguments || '') + '</div></div>'; }); }
    h += '</div>';
    return h;
  }
  if (m.role === 'tool') return '<div class="msg msg-tool-block"><div class="tc-result" style="margin-top:0">' + ic('arrowBack') + ' ' + esc(m.content || '').slice(0, 500) + '</div></div>';
  return '';
}
function appendStreamingMsg() { var box = el('#chat-thread'); var div = document.createElement('div'); div.className = 'msg msg-assistant'; div.innerHTML = '<div class="msg-role">ASSISTANT</div><div class="msg-content streaming"></div>'; box.appendChild(div); box.scrollTop = box.scrollHeight; return div.querySelector('.msg-content'); }

function isStreamingConv(convId) {
  if (convId) return activeStreams.has(convId);
  var pending = false;
  activeStreams.forEach(function(_, k) { if (String(k).indexOf('pending_') === 0) pending = true; });
  return pending;
}
function updateSendButtonState() {
  var isStream = isStreamingConv(currentConvId);
  var sendBtn = el('#chat-send'), stopBtn = el('#chat-stop');
  if (sendBtn) sendBtn.style.display = isStream ? 'none' : '';
  if (stopBtn) stopBtn.style.display = isStream ? '' : 'none';
}
function abortCurrentStream() {
  if (currentConvId && activeStreams.has(currentConvId)) { var s = activeStreams.get(currentConvId); if (s && s.abort) s.abort.abort(); return true; }
  var aborted = false;
  activeStreams.forEach(function(s, k) { if (String(k).indexOf('pending_') === 0 && s.abort) { s.abort.abort(); aborted = true; } });
  return aborted;
}

async function handleMsgAction(act, idx) {
  if (!currentConv || !currentConv.messages) return;
  var m = currentConv.messages[idx]; if (!m) return;
  if (act === 'copy') {
    var text = m.content || ''; if (!text) return;
    if (navigator.clipboard && navigator.clipboard.writeText) { try { await navigator.clipboard.writeText(text); toast('copied', 'ok'); } catch (e) { toast('copy failed', 'err'); } }
    else { var ta = document.createElement('textarea'); ta.value = text; document.body.appendChild(ta); ta.select(); try { document.execCommand('copy'); toast('copied', 'ok'); } catch (e) { toast('copy failed', 'err'); } document.body.removeChild(ta); }
    return;
  }
  if (act === 'truncate') {
    if (!(await confirmModal('Hapus pesan ini dan semua setelahnya?'))) return;
    try { await api('/admin/chats/' + currentConvId + '/truncate', { method: 'POST', body: { fromIndex: idx } }); var r = await api('/admin/chats/' + currentConvId); currentConv = r.conversation; renderThread(); loadConvList(); } catch (e) { toast(e.message, 'err'); }
    return;
  }
  if (act === 'edit') {
    if (m.role !== 'user') return;
    var body = '<div class="field"><label>Edit pesan & regenerate</label><textarea id="edit-msg-text" rows="6">' + esc(m.content || '') + '</textarea><div class="hint">Menyimpan akan menghapus pesan setelah ini dan membuat respons baru.</div></div>';
    openModal('Edit Pesan', body, {
      confirmLabel: 'Simpan & Regenerate',
      onConfirm: async function() {
        var newContent = el('#edit-msg-text').value;
        try { await api('/admin/chats/' + currentConvId + '/truncate', { method: 'POST', body: { fromIndex: idx } }); var r = await api('/admin/chats/' + currentConvId); currentConv = r.conversation; renderThread(); await sendMessage({ explicit: true, msg: newContent, atts: m.attachments || [] }); } catch (e) { throw new Error(e.message); }
      }
    });
    return;
  }
  if (act === 'regen') { await regenerateFrom(idx); return; }
}
async function regenerateFrom(assistantIdx) {
  if (isStreamingConv(currentConvId)) { toast('Sesi ini masih streaming', 'warn'); return; }
  if (!currentConv || !currentConv.messages) return;
  var msgs = currentConv.messages;
  var lastUserIdx = -1;
  for (var i = assistantIdx - 1; i >= 0; i--) { if (msgs[i].role === 'user') { lastUserIdx = i; break; } }
  if (lastUserIdx < 0) return toast('tidak ada user message sebelumnya', 'warn');
  var lastUser = msgs[lastUserIdx];
  if (!(await confirmModal('Regenerate? Pesan setelah user terakhir akan dihapus.'))) return;
  try { await api('/admin/chats/' + currentConvId + '/truncate', { method: 'POST', body: { fromIndex: lastUserIdx } }); var r = await api('/admin/chats/' + currentConvId); currentConv = r.conversation; currentConvId = r.conversation.id; renderThread(); await sendMessage({ explicit: true, msg: lastUser.content || '', atts: lastUser.attachments || [] }); } catch (e) { toast(e.message, 'err'); }
}

el('#chat-new').addEventListener('click', newChat);
el('#chat-agent').addEventListener('change', function(e) { currentAgentId = e.target.value; });
el('#chat-send').addEventListener('click', sendMessage);
el('#chat-input').addEventListener('keydown', function(e) { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); } });
el('#chat-stop').addEventListener('click', function() { abortCurrentStream(); });
el('#chat-clean-btn').addEventListener('click', async function() {
  if (!currentConvId) return toast('no active chat', 'warn');
  if (!(await confirmModal('Hapus semua pesan asisten yang mengandung artifact "tidak akan memanggil tool"?'))) return;
  try {
    var r = await api('/admin/chats/' + currentConvId + '/clean-artifacts', { method: 'POST' });
    toast('cleaned ' + r.removed + ' artifact messages', 'ok');
    var c = await api('/admin/chats/' + currentConvId);
    currentConv = c.conversation;
    renderThread();
    loadConvList();
  } catch (e) { toast(e.message, 'err'); }
});

function updateChatInfo(info) {
  var bar = el('#chat-info'); if (!bar) return; info = info || {};
  if (info.status === 'streaming') {
    bar.innerHTML = '<span class="ci-item ci-active">\u25cf STREAMING</span>' + (info.served ? '<span class="ci-item"><b style="color:var(--accent)">' + esc(info.served.provider) + '</b> / ' + esc(info.served.account) + (info.served.model ? ' · ' + esc(info.served.model) : '') + '</span>' : '<span class="ci-item ci-dim">routing...</span>') + (info.mode ? '<span class="ci-item ci-dim">· ' + info.mode.toUpperCase() + ' mode</span>' : '');
    return;
  }
  if (info.status === 'error') { bar.innerHTML = '<span class="ci-item ci-err">' + ic('warn') + ' error</span>'; return; }
  if (info.served) {
    var bits = ['<span class="ci-item"><b style="color:var(--accent)">' + esc(info.served.provider) + '</b> / ' + esc(info.served.account) + (info.served.model ? ' · ' + esc(info.served.model) : '') + '</span>'];
    if (info.inputTokens) bits.push('<span class="ci-item">in: ' + info.inputTokens + '</span>');
    if (info.outputTokens) bits.push('<span class="ci-item">out: ' + info.outputTokens + '</span>');
    if (info.duration) bits.push('<span class="ci-item">' + Math.round(info.duration) + 'ms</span>');
    if (info.mode) bits.push('<span class="ci-item ci-dim">· ' + info.mode.toUpperCase() + '</span>');
    bar.innerHTML = bits.join('');
    return;
  }
  bar.innerHTML = '<span class="ci-item ci-dim">ready</span>';
}
document.addEventListener('click', function(e) {
  var btn = e.target && e.target.closest ? e.target.closest('.copy-btn') : null;
  if (!btn) return;
  e.preventDefault();
  var block = btn.closest('.code-block'); if (!block) return;
  var code = block.querySelector('code'); if (!code) return;
  var text = code.textContent;
  var orig = btn.textContent;
  function flash(ok) { btn.textContent = ok ? 'copied' : 'fail'; setTimeout(function() { btn.textContent = orig; }, 1500); }
  if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(text).then(function() { flash(true); }).catch(function() { flash(false); }); }
  else { var ta = document.createElement('textarea'); ta.value = text; ta.style.position = 'fixed'; ta.style.left = '-9999px'; document.body.appendChild(ta); ta.select(); var ok2 = false; try { ok2 = document.execCommand('copy'); } catch(er) { ok2 = false; } document.body.removeChild(ta); flash(ok2); }
});
function createThinkingIndicator() {
  var div = document.createElement('div');
  div.className = 'moon-thinking';
  div.innerHTML = '<svg class="ic-moon" viewBox="0 0 24 24" aria-hidden="true">' +
    '<defs><mask id="moon-mask"><rect x="0" y="0" width="24" height="24" fill="white"/><circle class="moon-shadow" cx="17" cy="8" r="9" fill="black"/></mask></defs>' +
    '<circle class="moon-core" cx="12" cy="12" r="8.5" fill="currentColor" mask="url(#moon-mask)"/>' +
    '<circle cx="12" cy="12" r="8.5" fill="none" stroke="currentColor" stroke-width="1.4"/></svg>' +
    '<span class="moon-text">Thinking... executing tool</span>';
  var shadow = div.querySelector('.moon-shadow');
  var t = 0;
  var interval = setInterval(function() {
    t = (t + 1) % 8;
    // geser bayangan melintasi piringan — siklus fase bulan
    var dx = -8 + t * 2; // dari -8 (gerhana penuh) ke +6
    shadow.setAttribute('cx', String(12 + dx));
  }, 200);
  div._cleanup = function() { clearInterval(interval); div.remove(); };
  return div;
}

async function sendMessage(opts) {
  opts = opts || {};
  if (isStreamingConv(currentConvId)) { toast('Sesi ini masih streaming, tunggu selesai atau stop', 'warn'); return; }
  var msg, atts, modelOverride;
  if (opts.explicit) {
    msg = opts.msg || ''; atts = opts.atts || []; modelOverride = opts.modelOverride !== undefined ? opts.modelOverride : null;
  } else {
    var input = el('#chat-input');
    msg = input.value.trim(); atts = pendingAttachments.slice();
    if (!msg && atts.length === 0) return;
    input.value = ''; pendingAttachments = []; renderAttachments();
    var modelSel = el('#chat-model');
    modelOverride = modelSel ? (modelSel.value || null) : null;
  }
  if (!currentConv) currentConv = { messages: [] };
  if (!opts.skipPushUser) {
    var historyAtts = atts.map(function(a){ return { type: a.type, name: a.name, mime: a.mime, size: a.size, dataUrl: (a.type === 'image' && a.dataUrl && a.dataUrl.length < 200000) ? a.dataUrl : null, text: (a.type === 'text' && a.text && a.text.length < 5000) ? a.text : null }; });
    currentConv.messages.push({ role: 'user', content: msg, attachments: historyAtts.length ? historyAtts : undefined });
    renderThread();
  }
  var groupContainer = document.createElement('div');
  groupContainer.className = 'msg-stream-group';
  var chatBox = el('#chat-thread');
  if (chatBox) chatBox.appendChild(groupContainer);
  var msgWrapper = document.createElement('div');
  msgWrapper.className = 'msg msg-assistant';
  msgWrapper.innerHTML = '<div class="msg-role">ASSISTANT</div><div class="msg-content streaming"></div>';
  groupContainer.appendChild(msgWrapper);
  var contentDiv = msgWrapper.querySelector('.msg-content');

  reasoningContainer = document.createElement('details');
  reasoningContainer.className = 'msg-reasoning';
  reasoningContainer.open = true;
  reasoningContainer.innerHTML = '<summary>Thinking...</summary><div class="reasoning-content"></div>';
  groupContainer.insertBefore(reasoningContainer, msgWrapper);
  reasoningContent = reasoningContainer.querySelector('.reasoning-content');
  reasoningContainer.style.display = 'none';

  var waitEl = document.createElement('div');
  waitEl.className = 'msg-waiting';
  waitEl.textContent = 'Waiting for model...';
  groupContainer.insertBefore(waitEl, reasoningContainer);
  var waitTimer = null;
  function showWaiting() {
    clearTimeout(waitTimer);
    waitTimer = setTimeout(function() { waitEl.classList.add('show'); }, 600);
  }
  function hideWaiting() {
    clearTimeout(waitTimer);
    waitEl.classList.remove('show');
  }
  showWaiting();

  var rawText = '';
  var reasoningText = '';
  var reasoningContainer;
  var reasoningContent;
  var reasoningCollapsed = false;
  var served = null;
  var t0 = performance.now();
  var rafPending = false;
  var lastRenderedLen = 0;
  var thinkingEl = null;
  var streamConvId = currentConvId;
  var placeholderKey = streamConvId || ('pending_' + Math.random().toString(36).slice(2));
  var streamState = { abort: new AbortController(), container: groupContainer };
  activeStreams.set(placeholderKey, streamState);
  updateSendButtonState();

  function scrollIfActive() { if (currentConvId === streamConvId) { var th = el('#chat-thread'); if (th) th.scrollTop = th.scrollHeight; } }
  function updateInfoSafe(info) { if (currentConvId === streamConvId) updateChatInfo(info); }
  function scheduleRender() {
    if (rafPending) return;
    rafPending = true;
    requestAnimationFrame(function() { rafPending = false; if (rawText.length === lastRenderedLen) return; lastRenderedLen = rawText.length; contentDiv.innerHTML = mdRender(rawText); contentDiv.classList.add('streaming'); scrollIfActive(); });
  }
  function clearThinking() { if (thinkingEl) { thinkingEl._cleanup(); thinkingEl = null; } }
  updateInfoSafe({ status: 'streaming', mode: currentMode });
  try {
    var r = await fetch('/admin/chat', {
      method: 'POST',
      headers: Object.assign({ 'Content-Type': 'application/json' }, getToken() ? { 'Authorization': 'Bearer ' + getToken() } : {}),
      body: JSON.stringify({ conversationId: currentConvId, agentId: currentAgentId, message: msg, attachments: atts, modelOverride: modelOverride, modeOverride: currentMode }),
      signal: streamState.abort.signal
    });
    if (!r.ok) { var et = await r.text(); throw new Error('server ' + r.status + ': ' + et.slice(0, 200)); }
    var reader = r.body.getReader(); var dec = new TextDecoder(); var buf = '';
    while (true) {
      var rd = await reader.read(); if (rd.done) break;
      buf += dec.decode(rd.value, { stream: true });
      var idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        var block = buf.slice(0, idx); buf = buf.slice(idx + 2);
        var lines = block.split('\n');
        for (var i = 0; i < lines.length; i++) {
          var line = lines[i]; if (line.indexOf('data: ') !== 0) continue;
          var ev; try { ev = JSON.parse(line.slice(6)); } catch (e) { continue; }
          if (ev.type === 'start') {
            if (placeholderKey !== ev.convId) {
              activeStreams.delete(placeholderKey); streamConvId = ev.convId; activeStreams.set(streamConvId, streamState); placeholderKey = streamConvId;
              if (currentConvId === null) currentConvId = ev.convId;
              updateSendButtonState();
            }
            if (ev.mode && (ev.mode === 'chat' || ev.mode === 'agent')) setMode(ev.mode);
          }
          else if (ev.type === 'served') { served = ev; updateInfoSafe({ status: 'streaming', served: ev, mode: currentMode }); showWaiting(); }
          else if (ev.type === 'reasoning') {
            hideWaiting();
            clearThinking();
            reasoningText += ev.content;
            reasoningContainer.style.display = '';
            reasoningContent.textContent = reasoningText;
            reasoningContainer.querySelector('summary').textContent = 'Thinking (' + reasoningText.length + ' chars)';
            scrollIfActive();
          }
          else if (ev.type === 'token') {
            clearThinking();
            hideWaiting();
            if (reasoningText && !reasoningCollapsed) {
              reasoningContainer.open = false;
              reasoningCollapsed = true;
            }
            rawText += ev.content;
            scheduleRender();
          }
          else if (ev.type === 'tool_call') {
            hideWaiting();
            clearThinking();
            contentDiv.innerHTML = mdRender(rawText);
            contentDiv.classList.remove('streaming');
            var tb = document.createElement('div'); tb.className = 'msg-tool-block';
            tb.innerHTML = '<div><span class="tc-name">\u25b8 ' + esc(ev.name) + '</span></div><div class="tc-args">' + esc(ev.arguments || '') + '</div>';
            groupContainer.appendChild(tb);
            thinkingEl = createThinkingIndicator();
            groupContainer.appendChild(thinkingEl);
            scrollIfActive();
          }
          else if (ev.type === 'tool_result') {
            var tbs = groupContainer.querySelectorAll('.msg-tool-block'); var last = tbs[tbs.length - 1];
            if (last) { var rdv = document.createElement('div'); rdv.className = 'tc-result'; var rs = typeof ev.result === 'string' ? ev.result : JSON.stringify(ev.result); rdv.textContent = '\u2190 ' + rs.slice(0, 500); last.appendChild(rdv); }
            var newWrapper = document.createElement('div'); newWrapper.className = 'msg msg-assistant';
            newWrapper.innerHTML = '<div class="msg-role">ASSISTANT</div><div class="msg-content streaming"></div>';
            groupContainer.appendChild(newWrapper);
            contentDiv = newWrapper.querySelector('.msg-content');
            rawText = ''; lastRenderedLen = 0;
            if (thinkingEl) groupContainer.appendChild(thinkingEl);
            scrollIfActive();
            showWaiting();
          }
          else if (ev.type === 'error') {
            clearThinking();
            contentDiv.classList.remove('streaming');
            var errMsg = '\u26a0 ' + esc(ev.message);
            if (ev.attempted && ev.attempted.length) { errMsg += '\n\nAttempted:\n'; ev.attempted.forEach(function(a) { errMsg += '  \u00b7 ' + a.provider + ' / ' + a.label + '\n'; }); }
            contentDiv.innerHTML = '<div class="msg-error">' + errMsg + '</div>';
            updateInfoSafe({ status: 'error' });
            hideWaiting();
          }
          else if (ev.type === 'final') {
            hideWaiting();
            activeStreams.delete(placeholderKey);
            if (currentConvId === ev.conversation.id) {
              currentConv = ev.conversation; currentConvId = ev.conversation.id;
              var dur = performance.now() - t0;
              updateChatInfo({ served: served, status: 'idle', duration: dur, inputTokens: Math.ceil(msg.length / 4), outputTokens: Math.ceil(rawText.length / 4), mode: currentMode });
              renderThread();
            }
            loadConvList();
            updateSendButtonState();
          }
        }
      }
    }
    contentDiv.classList.remove('streaming');
    if (rawText) contentDiv.innerHTML = mdRender(rawText);
  } catch (e) {
    if (e.name === 'AbortError') toast('stopped', 'warn');
    else { contentDiv.classList.remove('streaming'); contentDiv.innerHTML = '<div class="msg-error">\u26a0 ' + esc(e.message) + '</div>'; }
    updateChatInfo({ status: 'idle' });
  } finally {
    activeStreams.delete(placeholderKey);
    if (streamConvId) activeStreams.delete(streamConvId);
    clearThinking();
    if (typeof hideWaiting === 'function') hideWaiting();
    updateSendButtonState();
  }
}

function populatePgModels() {
  if (!state) return;
  var sel = el('#pg-model'); if (!sel) return;
  var cur = sel.value;
  var h = '<option value="smart">smart</option><option value="fast">fast</option><option value="auto">auto</option>';
  Object.keys(state.providers).forEach(function(name) { var p = state.providers[name]; if (!p.enabled) return; Object.keys(p.models || {}).forEach(function(alias) { var mid = p.models[alias]; if (mid) h += '<option value="' + esc(name + ':' + mid) + '">' + esc(name + ':' + mid) + '</option>'; }); });
  var seen = {};
  state.accounts.forEach(function(acc) { if (!acc.model) return; var key = acc.provider + ':' + acc.model; if (seen[key]) return; seen[key] = 1; h += '<option value="' + esc(key) + '">' + esc(key) + '  (acct: ' + esc(acc.label) + ')</option>'; });
  if (sel.innerHTML !== h) { sel.innerHTML = h; if (cur && Array.prototype.some.call(sel.options, function(o){ return o.value === cur; })) sel.value = cur; }
}
el('#pg-send').addEventListener('click', async function() {
  var model = el('#pg-model').value, text = el('#pg-input').value, temp = parseFloat(el('#pg-temp').value) || 0.7;
  if (!text.trim()) return;
  var out = el('#pg-out'); out.textContent = '';
  var meta = el('#pg-meta'); meta.textContent = 'sending...';
  var t0 = performance.now();
  try {
    var tok = getToken(); var headers = { 'Content-Type': 'application/json' }; if (tok) headers['Authorization'] = 'Bearer ' + tok;
    var r = await fetch('/v1/chat/completions', { method: 'POST', headers: headers, body: JSON.stringify({ model: model, messages: [{ role: 'user', content: text }], stream: true, temperature: temp }) });
    var servedBy = r.headers.get('x-served-by') || '?'; var servedAcc = r.headers.get('x-served-account') || '?';
    if (!r.ok) {
      var errText = await r.text(); var parsedErr; try { parsedErr = JSON.parse(errText); } catch(e) {}
      var msg = 'ERROR ' + r.status;
      if (parsedErr && parsedErr.error) { msg += ': ' + (parsedErr.error.message || ''); if (parsedErr.error.detail) msg += '\n\nDetail: ' + parsedErr.error.detail; if (parsedErr.error.attempted && parsedErr.error.attempted.length) { msg += '\n\nAttempted (' + parsedErr.error.attempted.length + ' akun):'; parsedErr.error.attempted.forEach(function(a) { msg += '\n  · ' + a.provider + ' / ' + a.label; }); } }
      else { msg += ': ' + errText.slice(0, 500); }
      out.textContent = msg; meta.textContent = 'failed'; return;
    }
    var reader = r.body.getReader(); var dec = new TextDecoder(); var buf = '', chunks = 0, firstByte = null;
    while (true) {
      var rd = await reader.read(); if (rd.done) break;
      buf += dec.decode(rd.value, { stream: true });
      var idx;
      while ((idx = buf.indexOf('\n\n')) >= 0) {
        var block = buf.slice(0, idx); buf = buf.slice(idx + 2);
        var lines = block.split('\n');
        for (var i = 0; i < lines.length; i++) {
          var line = lines[i]; if (line.indexOf('data:') !== 0) continue;
          var data = line.slice(5).trim(); if (data === '[DONE]') continue;
          try { var obj = JSON.parse(data); var content = (obj.choices && obj.choices[0] && obj.choices[0].delta && obj.choices[0].delta.content) || ''; if (content) { out.textContent += content; chunks++; if (firstByte === null) firstByte = performance.now() - t0; out.scrollTop = out.scrollHeight; } } catch (e) {}
        }
      }
    }
    var dur = performance.now() - t0;
    meta.innerHTML = '<span>x-served-by: <b style="color:var(--accent)">' + esc(servedBy) + '</b></span><span>x-served-account: ' + esc(servedAcc) + '</span><span>' + Math.round(dur) + 'ms</span><span>~' + chunks + ' chunks</span>' + (firstByte !== null ? '<span>first byte: ' + Math.round(firstByte) + 'ms</span>' : '');
  } catch (e) { out.textContent = 'ERROR: ' + e.message; meta.textContent = 'failed'; }
});
el('#pg-clear').addEventListener('click', function() { el('#pg-out').textContent = ''; el('#pg-meta').textContent = ''; });
el('#pg-preview').addEventListener('click', async function() {
  var model = el('#pg-model').value;
  var out = el('#pg-out'); out.textContent = 'loading fallback preview for ' + model + '...';
  var meta = el('#pg-meta'); meta.textContent = '';
  try {
    var r = await api('/admin/fallback-preview?model=' + encodeURIComponent(model));
    var lines = []; lines.push('Model: ' + r.model); lines.push('Fallback mode: ' + r.fallbackMode); lines.push('Provider hint: ' + (r.providerHint || '(none)')); lines.push('Explicit model: ' + (r.explicitModel || '(none)')); lines.push('Candidates: ' + r.count + ' akun'); lines.push('');
    if (r.count === 0) { lines.push('Tidak ada akun yang bisa serve model ini.'); lines.push('Kemungkinan:'); lines.push('  - semua akun provider dead/cooldown/disabled'); lines.push('  - tidak ada akun dari provider yang punya model ini'); lines.push('  - model tidak dikenal provider manapun'); }
    else { r.candidates.forEach(function(c, i) { lines.push((i+1) + '. [' + c.provider + '] ' + c.label + (c.paid ? ' [PAID]' : '')); lines.push('     priority=' + c.priority + '  effective model=' + c.effectiveModel + (c.modelOverride ? '  (override)' : '')); lines.push('     id=' + c.id); }); }
    out.textContent = lines.join('\n');
    meta.innerHTML = '<span>Coba kirim model <b style="color:var(--accent)">' + esc(model) + '</b></span>';
  } catch (e) { out.textContent = 'ERROR: ' + e.message; meta.textContent = 'failed'; }
});

var modelFilter = { provider: '', label: '', search: '' };
function mdlLabels(m) { if (!m) return []; if (Array.isArray(m.labels) && m.labels.length > 0) return m.labels; return m.label ? [m.label] : []; }
async function loadModels() { try { var r = await api('/admin/models'); if (!state) state = {}; state.models = r.models || []; renderModels(); } catch (e) { toast(e.message, 'err'); } }
function renderModels() {
  if (!state) return;
  var models = state.models || [];
  var pf = el('#mdl-f-provider'), lf = el('#mdl-f-label');
  if (pf) { var pOpts = '<option value="">All providers</option>'; Object.keys(state.providers || {}).forEach(function(n){ pOpts += '<option value="' + esc(n) + '">' + esc(n) + '</option>'; }); if (pf.innerHTML !== pOpts) { var pc = pf.value; pf.innerHTML = pOpts; pf.value = pc; } }
  if (lf) { var labels = {}; models.forEach(function(m){ mdlLabels(m).forEach(function(l){ labels[l] = 1; }); }); var lOpts = '<option value="">All labels</option>'; Object.keys(labels).sort().forEach(function(l){ lOpts += '<option value="' + esc(l) + '">' + esc(l) + '</option>'; }); if (lf.innerHTML !== lOpts) { var lc = lf.value; lf.innerHTML = lOpts; lf.value = lc; } }
  var list = models.filter(function(m) {
    if (modelFilter.provider && m.provider !== modelFilter.provider) return false;
    if (modelFilter.label && mdlLabels(m).indexOf(modelFilter.label) < 0) return false;
    if (modelFilter.search) { var q = modelFilter.search.toLowerCase(); if (!((m.modelId || '').toLowerCase().indexOf(q) >= 0) && !((m.label || '').toLowerCase().indexOf(q) >= 0)) return false; }
    return true;
  });
  var wrap = el('#mdl-table-wrap');
  if (!list.length) { wrap.innerHTML = '<div class="empty">no models. Klik "+ Tambah Model" atau "Reseed from Providers".</div>'; return; }
  var h = '<table><thead><tr><th>Provider</th><th>Model ID</th><th>Label</th><th>Input</th><th>Output</th><th class="num">Priority</th><th class="num">Context</th><th>Enabled</th><th>Aksi</th></tr></thead><tbody>';
  list.forEach(function(m) { h += '<tr><td style="color:var(--accent);font-weight:600">' + esc(m.provider) + '</td><td style="font-family:monospace;font-size:10px;max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(m.modelId) + '">' + esc(m.modelId) + '</td><td>' + mdlLabels(m).map(function(l){ return '<span class="badge info" style="margin-right:3px">' + esc(l) + '</span>'; }).join('') + '</td><td style="font-size:9px;color:var(--fg-3)">' + esc((m.input || []).join(',')) + '</td><td style="font-size:9px;color:var(--fg-3)">' + esc((m.output || []).join(',')) + '</td><td class="num">' + (m.priority || 0) + '</td><td class="num" style="color:var(--warn)">' + (m.contextLength || '128k') + '</td><td>' + (m.enabled ? '<span class="badge ok">ON</span>' : '<span class="badge mute">OFF</span>') + '</td><td><button class="btn small ghost" data-med="' + esc(m.id) + '">Edit</button> <button class="btn small ghost danger" data-mdl-del="' + esc(m.id) + '">' + ic('x') + '</button></td></tr>'; });
  h += '</tbody></table>';
  wrap.innerHTML = h;
  wrap.querySelectorAll('[data-med]').forEach(function(b) { b.addEventListener('click', function() { editModel(b.dataset.med); }); });
  wrap.querySelectorAll('[data-mdl-del]').forEach(function(b) { b.addEventListener('click', async function() { if (!(await confirmModal('Hapus model ini?'))) return; try { await api('/admin/models/' + b.dataset.mdlDel, { method: 'DELETE' }); toast('deleted', 'ok'); loadModels(); } catch (e) { toast(e.message, 'err'); } }); });
}
function editModel(id) {
  var m = id ? (state.models || []).find(function(x){ return x.id === id; }) : null;
  var isNew = !m;
  if (!m) m = { id: null, provider: '', modelId: '', label: 'general', input: ['text'], output: ['text'], priority: 50, enabled: true, contextLength: 128000 };
  var provOpts = Object.keys(state.providers || {}).filter(function(n){ return state.providers[n].enabled; }).map(function(n){ return '<option value="' + esc(n) + '"' + (m.provider === n ? ' selected' : '') + '>' + esc(n) + '</option>'; }).join('');
  var LABELS = ['smart','fast','vision','voice','coding','reasoning','translate','image-gen','general'];
  var currentLabels = mdlLabels(m); var labelChecks = LABELS.map(function(l){ return '<label style="display:inline-flex;align-items:center;gap:4px;margin-right:8px;font-size:11px;margin-bottom:4px"><input type="checkbox" data-lbl="' + l + '"' + (currentLabels.indexOf(l) >= 0 ? ' checked' : '') + '> ' + l + '</label>'; }).join('');
  var CAPS = ['text','image','audio','video','file'];
  var inCaps = CAPS.map(function(c){ return '<label style="display:inline-flex;align-items:center;gap:4px;margin-right:8px;font-size:11px"><input type="checkbox" data-in="' + c + '"' + ((m.input||[]).indexOf(c)>=0?' checked':'') + '> ' + c + '</label>'; }).join('');
  var outCaps = CAPS.map(function(c){ return '<label style="display:inline-flex;align-items:center;gap:4px;margin-right:8px;font-size:11px"><input type="checkbox" data-out="' + c + '"' + ((m.output||[]).indexOf(c)>=0?' checked':'') + '> ' + c + '</label>'; }).join('');
  var body = '<div class="form-row"><div class="field"><label>Provider</label><select id="mdl-provider"' + (isNew ? '' : ' disabled') + '>' + provOpts + '</select></div><div class="field"><label>Labels (multi)</label><div style="display:flex;flex-wrap:wrap;gap:4px">' + labelChecks + '</div></div></div>' +
    '<div class="field"><label>Model ID</label><input id="mdl-id" value="' + esc(m.modelId) + '" ' + (isNew ? '' : 'disabled') + '></div>' +
    '<div class="field"><label>Input capabilities</label><div>' + inCaps + '</div></div>' +
    '<div class="field"><label>Output capabilities</label><div>' + outCaps + '</div></div>' +
    '<div class="form-row"><div class="field"><label>Priority</label><input id="mdl-priority" type="number" value="' + (m.priority || 50) + '"></div><div class="field"><label>Context length (tokens)</label><input id="mdl-ctx" type="number" value="' + (m.contextLength || '') + '" placeholder="128000 (default)"></div></div>' +
    '<div class="field"><label>Enabled</label><select id="mdl-enabled"><option value="true"' + (m.enabled ? ' selected' : '') + '>yes</option><option value="false"' + (!m.enabled ? ' selected' : '') + '>no</option></select></div>';
  openModal(isNew ? 'Tambah Model' : 'Edit Model', body, {
    confirmLabel: 'Save',
    onConfirm: async function() {
      var _selLabels = Array.from(document.querySelectorAll('input[data-lbl]:checked')).map(function(x){ return x.dataset.lbl; }); if (_selLabels.length === 0) _selLabels = ['general']; var payload = { provider: el('#mdl-provider').value, modelId: el('#mdl-id').value.trim(), label: _selLabels[0], labels: _selLabels, input: Array.from(document.querySelectorAll('input[data-in]:checked')).map(function(x){ return x.dataset.in; }), output: Array.from(document.querySelectorAll('input[data-out]:checked')).map(function(x){ return x.dataset.out; }), priority: parseInt(el('#mdl-priority').value, 10) || 50, contextLength: parseInt(el('#mdl-ctx').value, 10) || null, enabled: el('#mdl-enabled').value === 'true' };
      if (!payload.provider || !payload.modelId) throw new Error('provider + modelId required');
      if (isNew) await api('/admin/models', { method: 'POST', body: payload }); else await api('/admin/models/' + id, { method: 'PATCH', body: payload });
      toast('saved', 'ok'); loadModels(); populateChatModels();
    }
  });
}
el('#btn-add-model').addEventListener('click', function() { editModel(null); });
el('#btn-reseed-models').addEventListener('click', async function() { if (!(await confirmModal('Reset catalog dari provider config?'))) return; try { await api('/admin/models/reseed', { method: 'POST' }); toast('reseeded', 'ok'); loadModels(); populateChatModels(); } catch (e) { toast(e.message, 'err'); } });
el('#mdl-f-provider').addEventListener('change', function(e){ modelFilter.provider = e.target.value; renderModels(); });
el('#mdl-f-label').addEventListener('change', function(e){ modelFilter.label = e.target.value; renderModels(); });
el('#mdl-f-search').addEventListener('input', function(e){ modelFilter.search = e.target.value; renderModels(); });

var pendingAttachments = [];
function populateChatModels() {
  var sel = el('#chat-model'); if (!sel) return;
  var cur = sel.value;
  var h = '<option value="">(agent default)</option>';
  var LABELS = ['smart','fast','vision','voice','coding','reasoning','translate','image-gen','general'];
  LABELS.forEach(function(l){ h += '<option value="' + l + '">label: ' + l + '</option>'; });
  var models = (state && state.models) || [];
  if (models.length) { h += '<optgroup label="Specific models">'; models.filter(function(m){ return m.enabled; }).forEach(function(m) { h += '<option value="' + esc(m.provider + ':' + m.modelId) + '">' + esc(m.provider + ' / ' + m.modelId + ' [' + mdlLabels(m).join(',') + ']') + '</option>'; }); h += '</optgroup>'; }
  if (sel.innerHTML !== h) { sel.innerHTML = h; if (cur && Array.prototype.some.call(sel.options, function(o){ return o.value === cur; })) sel.value = cur; }
}
function fileToDataURL(file) { return new Promise(function(resolve, reject) { var r = new FileReader(); r.onload = function() { resolve(r.result); }; r.onerror = function() { reject(new Error('read error')); }; r.readAsDataURL(file); }); }
function fileToText(file) { return new Promise(function(resolve, reject) { var r = new FileReader(); r.onload = function() { resolve(r.result); }; r.onerror = function() { reject(new Error('read error')); }; r.readAsText(file); }); }
async function addAttachment(file) {
  if (file.size > 8 * 1024 * 1024) throw new Error(file.name + ': too large (max 8MB)');
  if (pendingAttachments.length >= 5) throw new Error('max 5 attachments');
  var mime = file.type || '';
  if (mime.indexOf('image/') === 0) { var dataUrl = await fileToDataURL(file); pendingAttachments.push({ type: 'image', name: file.name, mime: mime, size: file.size, dataUrl: dataUrl }); }
  else if (mime.indexOf('text/') === 0 || /\.(txt|md|json|js|py|html|css|xml|yaml|yml|csv|log|sh|ts|tsx|jsx|sql|rs|go|java|c|cpp|h|rb|php|toml|ini|conf|env)$/i.test(file.name)) { var text = await fileToText(file); if (text.length > 100000) text = text.slice(0, 100000) + '\n\n[...truncated]'; pendingAttachments.push({ type: 'text', name: file.name, mime: mime || 'text/plain', size: file.size, text: text }); }
  else throw new Error(file.name + ': tipe tidak didukung');
}
function renderAttachments() {
  var box = el('#chat-attachments'); if (!box) return;
  if (!pendingAttachments.length) { box.innerHTML = ''; box.style.display = 'none'; return; }
  box.style.display = 'flex';
  box.innerHTML = pendingAttachments.map(function(a, i) { var thumb = (a.type === 'image' && a.dataUrl) ? '<img src="' + a.dataUrl + '" style="width:24px;height:24px;object-fit:cover;border:1px solid var(--line)">' : ic('file'); return '<div class="attachment-chip">' + thumb + ' <span>' + esc(a.name) + ' (' + fmtSize(a.size) + ')</span><button class="attach-remove" data-i="' + i + '" title="remove">' + ic('x') + '</button></div>'; }).join('');
  box.querySelectorAll('.attach-remove').forEach(function(b) { b.addEventListener('click', function() { pendingAttachments.splice(parseInt(b.dataset.i, 10), 1); renderAttachments(); }); });
}
el('#chat-attach-btn').addEventListener('click', function() { el('#chat-attach-input').click(); });
el('#chat-attach-input').addEventListener('change', async function(e) { var files = Array.from(e.target.files || []); for (var i = 0; i < files.length; i++) { try { await addAttachment(files[i]); } catch (err) { toast(err.message, 'err'); } } e.target.value = ''; renderAttachments(); });

var AGENT_TEMPLATES = [
  { id: 'chat-basic', name: 'Basic Chat', description: 'ChatGPT-like general assistant.', mode: 'chat', model: 'smart', systemPrompt: 'You are a helpful conversational assistant. Answer concisely with markdown.', tools: [], skills: [] },
  { id: 'chat-research', name: 'Research Chat', description: 'ChatGPT with web search.', mode: 'chat', model: 'smart', systemPrompt: 'You are a research assistant with web search. When the user asks about recent info or facts, call builtin_web_search, then answer with sources.', tools: [], skills: [] },
  { id: 'web', name: 'Web Builder', description: 'Full-stack web app builder.', mode: 'agent', model: 'smart', systemPrompt: 'You are an expert full-stack web developer. You have workspace tools: builtin_fs_list/read/write/edit/glob/grep/mkdir + builtin_todo. When user describes an app: 1) plan with builtin_todo, 2) create with builtin_fs_write, 3) edit with builtin_fs_edit, 4) verify by reading back. If shell enabled: use builtin_run.', tools: ['builtin_web_search', 'builtin_fetch_url', 'builtin_fs_list', 'builtin_fs_read', 'builtin_fs_write', 'builtin_fs_edit', 'builtin_fs_glob', 'builtin_fs_grep', 'builtin_fs_mkdir', 'builtin_todo', 'builtin_calculator'], skills: [] },
  { id: 'debugger', name: 'Debugger', description: 'Debug step-by-step.', mode: 'agent', model: 'smart', systemPrompt: 'You are a debugging expert. Use builtin_fs_list/read/grep/glob to explore code, builtin_run to reproduce errors, builtin_git for history. 1) Grep error, 2) Read file, 3) Hypothesize, 4) Run diagnostics, 5) Propose exact fix via builtin_fs_edit.', tools: ['builtin_web_search', 'builtin_fetch_url', 'builtin_fs_list', 'builtin_fs_read', 'builtin_fs_grep', 'builtin_fs_glob', 'builtin_fs_edit', 'builtin_run', 'builtin_git', 'builtin_todo'], skills: [] },
  { id: 'api', name: 'API Designer', description: 'REST API architect.', mode: 'agent', model: 'smart', systemPrompt: 'You are an API designer. Output: 1) endpoint list with schemas, 2) code in files via builtin_fs_write, 3) OpenAPI YAML. Use builtin_todo.', tools: ['builtin_web_search', 'builtin_fetch_url', 'builtin_fs_list', 'builtin_fs_read', 'builtin_fs_write', 'builtin_fs_edit', 'builtin_fs_mkdir', 'builtin_todo', 'builtin_run', 'builtin_calculator'], skills: [] },
  { id: 'reviewer', name: 'Code Reviewer', description: 'Senior reviewer.', mode: 'chat', model: 'smart', systemPrompt: 'You are a senior code reviewer. For pasted code: bugs by severity, performance, readability, refactored version.', tools: [], skills: [] },
  { id: 'data', name: 'Data Analyst', description: 'SQL + pandas.', mode: 'agent', model: 'smart', systemPrompt: 'You are a data analyst. Given schema, write SQL (Postgres/MySQL) and pandas. Show query, expected output, edge cases.', tools: ['builtin_web_search', 'builtin_fetch_url', 'builtin_calculator', 'builtin_json_query'], skills: [] },
  { id: 'translator', name: 'Translator', description: 'Translate preserving tone.', mode: 'chat', model: 'smart', systemPrompt: 'Translate preserving tone, formality, nuance. Indonesian: natural informal unless formal context.', tools: [], skills: [] },
  { id: 'writer', name: 'Writing Coach', description: 'Improve writing.', mode: 'chat', model: 'smart', systemPrompt: 'Improve writing. For shared text: 1) corrected version, 2) key changes, 3) tone suggestions.', tools: [], skills: [] },
  { id: 'vision', name: 'Image Describer', description: 'Describe images.', mode: 'chat', model: 'vision', systemPrompt: 'Analyze images: content, composition, OCR text, style. Suggest prompts/code if asked to recreate.', tools: [], skills: [] },
  { id: 'devops', name: 'DevOps Helper', description: 'Shell, Docker, K8s.', mode: 'agent', model: 'smart', systemPrompt: 'You are a DevOps engineer. 1) exact commands/config, 2) side effects, 3) rollback. Bash: set -euo pipefail. Docker: multi-stage. K8s: resource limits, probes.', tools: ['builtin_shell', 'builtin_file_write', 'builtin_file_read', 'builtin_run'], skills: [] },
  { id: 'sql', name: 'SQL Master', description: 'Query optimization.', mode: 'chat', model: 'smart', systemPrompt: 'You are a SQL expert. For queries: 1) query, 2) expected plan, 3) missing indexes, 4) performance issues. Use EXPLAIN ANALYZE, not guess.', tools: [], skills: [] },
  { id: 'prompt', name: 'Prompt Engineer', description: 'Craft prompts.', mode: 'chat', model: 'smart', systemPrompt: 'You are a prompt engineer. Analyze prompt, output improved prompt in code block, explain key changes. Keep under 500 tokens unless complex.', tools: [], skills: [] },
];
function fromTemplateModal() {
  var h = '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px">';
  AGENT_TEMPLATES.forEach(function(t) { var modeBadge = t.mode === 'chat' ? '<span class="badge info">CHAT</span>' : '<span class="badge ok">AGENT</span>'; h += '<div class="tool-card" style="cursor:pointer" data-tmpl="' + esc(t.id) + '"><div class="tc-title">' + esc(t.name) + ' ' + modeBadge + '</div><div class="tc-desc">' + esc(t.description) + '</div><div class="tc-source">model: ' + esc(t.model) + ' · tools: ' + (t.tools || []).length + '</div></div>'; });
  h += '</div>';
  var m = openModal('From Template', h, { wide: true });
  m.overlay.querySelectorAll('[data-tmpl]').forEach(function(c) {
    c.addEventListener('click', function() {
      var t = AGENT_TEMPLATES.find(function(x){ return x.id === c.dataset.tmpl; });
      if (!t) return;
      m.close(); editAgent(null);
      setTimeout(function() {
        var ni = el('#ag-name'); if (ni) ni.value = t.name;
        var di = el('#ag-desc'); if (di) di.value = t.description;
        var si = el('#ag-sys'); if (si) si.value = t.systemPrompt;
        var mi = el('#ag-model'); if (mi) mi.value = t.model || 'smart';
        var mo = el('#ag-mode'); if (mo) mo.value = t.mode || 'agent';
        (t.tools || []).forEach(function(tn) { var cb = document.querySelector('input[data-tool="' + tn + '"]'); if (cb) { cb.checked = true; var card = cb.closest('.tool-card'); if (card) card.classList.add('selected'); } });
      }, 80);
    });
  });
}
el('#btn-from-template').addEventListener('click', fromTemplateModal);

function startLogStream() {
  if (logSource) return;
  var tok = getToken();
  var url = '/admin/logs/stream' + (tok ? '?token=' + encodeURIComponent(tok) : '');
  logSource = new EventSource(url);
  logSource.onmessage = function(e) { if (logPaused) return; try { addLogLine(JSON.parse(e.data)); } catch (err) {} };
  logSource.onerror = function() { if (logSource) { logSource.close(); logSource = null; } setTimeout(startLogStream, 3000); };
}
function addLogLine(e) {
  if (el('#log-level').value && e.level !== el('#log-level').value) return;
  var list = el('#logs-list');
  var div = document.createElement('div'); div.className = 'log-line';
  div.innerHTML = '<span class="ts">' + esc(e.ts.slice(11, 19)) + '</span> <span class="lv-' + esc(e.level) + '">' + esc(e.level.toUpperCase()) + '</span> <span class="prov">' + esc(e.provider) + '</span> ' + esc(e.label) + ' <span style="color:var(--fg)">' + esc(e.event) + '</span> <span style="color:var(--fg-3)">' + esc(e.detail) + '</span>';
  list.appendChild(div); logLines.push(div);
  if (logLines.length > 500) { var old = logLines.shift(); if (old.parentNode) old.parentNode.removeChild(old); }
  list.scrollTop = list.scrollHeight;
}
el('#log-pause').addEventListener('click', function(e) { logPaused = !logPaused; e.target.textContent = logPaused ? 'Resume' : 'Pause'; });
el('#log-clear').addEventListener('click', function() { el('#logs-list').innerHTML = ''; logLines = []; });

var SEARCH_ENGINE_META = {
  bing: { name: 'Bing', desc: 'fast, stable, works in ID' },
  yahoo: { name: 'Yahoo', desc: 'works in ID, slower' },
  mojeek: { name: 'Mojeek', desc: 'privacy, small index' },
  ddg: { name: 'DuckDuckGo', desc: 'blocked by Kominfo in ID' },
  searxng: { name: 'SearXNG', desc: 'requires setup' },
  '4get-ddg': { name: '4get / DDG', desc: 'server proxy, works in ID' },
  '4get-brave': { name: '4get / Brave', desc: 'server proxy, works in ID' },
  '4get-google': { name: '4get / Google', desc: 'server proxy, works in ID' },
  stract: { name: 'Stract', desc: 'independent, niche' },
  ecosia: { name: 'Ecosia', desc: 'Bing-backed' },
  brave: { name: 'Brave HTML', desc: 'independent index' },
  yep: { name: 'Yep', desc: 'independent (Ahrefs)' },
  librey: { name: 'LibreY', desc: 'metasearch' },
  marginalia: { name: 'Marginalia', desc: 'indie, niche articles' },
  mwmbl: { name: 'Mwmbl', desc: 'crowdsourced' }
};
var _priorityCache = [];

function renderPriorityList() {
  var priStr = (state && state.settings && state.settings.searchPriority) || 'bing,4get-ddg,yahoo,ecosia,yep,mojeek,librey,brave,marginalia,mwmbl,stract,searxng';
  _priorityCache = priStr.split(',').map(function(s){ return s.trim(); }).filter(function(s){ return SEARCH_ENGINE_META[s]; });
  renderPriorityRows();
}

function renderPriorityRows() {
  var list = el('#s-searchPriorityList');
  var addBox = el('#s-searchPriorityAdd');
  if (!list || !addBox) return;
  list.innerHTML = _priorityCache.map(function(eng, idx) {
    var m = SEARCH_ENGINE_META[eng] || { name: eng, desc: '' };
    return '<div data-pri-name="' + esc(eng) + '" style="display:flex;gap:6px;align-items:center;padding:5px 8px;border:2px solid var(--line);background:var(--bg-2)">' +
      '<span style="font-family:Silkscreen,monospace;font-size:11px;min-width:22px;color:var(--accent)">' + (idx+1) + '.</span>' +
      '<span style="flex:1;font-size:11px">' + esc(m.name) + ' <span style="color:var(--fg-3);font-size:9px">\u00b7 ' + esc(m.desc) + '</span></span>' +
      '<button class="btn ghost small" data-pri-up="' + esc(eng) + '"' + (idx === 0 ? ' disabled' : '') + ' title="up">' + ic('chevU') + '</button>' +
      '<button class="btn ghost small" data-pri-down="' + esc(eng) + '"' + (idx === _priorityCache.length-1 ? ' disabled' : '') + ' title="down">' + ic('chevD') + '</button>' +
      '<button class="btn ghost small danger" data-pri-del="' + esc(eng) + '" title="remove">' + ic('x') + '</button>' +
      '</div>';
  }).join('');
  var missing = Object.keys(SEARCH_ENGINE_META).filter(function(k){ return _priorityCache.indexOf(k) === -1; });
  addBox.innerHTML = missing.length === 0 ? '' : '<span style="font-size:10px;color:var(--fg-3)">Add: </span>' + missing.map(function(k){
    return '<button class="btn ghost small" data-pri-add="' + esc(k) + '" style="margin:2px">+ ' + esc(SEARCH_ENGINE_META[k].name) + '</button>';
  }).join('');
  list.querySelectorAll('[data-pri-up]').forEach(function(b){ b.addEventListener('click', function(){ movePri(b.dataset.priUp, -1); }); });
  list.querySelectorAll('[data-pri-down]').forEach(function(b){ b.addEventListener('click', function(){ movePri(b.dataset.priDown, 1); }); });
  list.querySelectorAll('[data-pri-del]').forEach(function(b){ b.addEventListener('click', function(){ removePri(b.dataset.priDel); }); });
  addBox.querySelectorAll('[data-pri-add]').forEach(function(b){ b.addEventListener('click', function(){ addPri(b.dataset.priAdd); }); });
}

function persistPriority() {
  var s = _priorityCache.join(',');
  if (state && state.settings) state.settings.searchPriority = s;
  api('/admin/settings', { method: 'PATCH', body: { searchPriority: s } }).catch(function(e){ toast(e.message, 'err'); });
}

function movePri(eng, dir) {
  var i = _priorityCache.indexOf(eng);
  if (i < 0) return;
  var j = i + dir;
  if (j < 0 || j >= _priorityCache.length) return;
  var tmp = _priorityCache[i]; _priorityCache[i] = _priorityCache[j]; _priorityCache[j] = tmp;
  renderPriorityRows(); persistPriority();
}
function removePri(eng) {
  _priorityCache = _priorityCache.filter(function(e){ return e !== eng; });
  renderPriorityRows(); persistPriority();
}
function addPri(eng) {
  if (_priorityCache.indexOf(eng) >= 0) return;
  _priorityCache.push(eng);
  renderPriorityRows(); persistPriority();
}

el('#s-setupSearxng').addEventListener('click', async function() {
  var btn = el('#s-setupSearxng');
  var status = el('#s-searxngStatus');
  btn.disabled = true; btn.textContent = 'Testing...';
  status.textContent = 'testing existing + custom + docker + 15 public instances...';
  status.style.color = 'var(--fg-3)';
  try {
    var r = await api('/admin/setup-searxng', { method: 'POST' });
    var working = (r.tried || []).filter(function(t){ return t.ok; });
    var failed = (r.tried || []).filter(function(t){ return !t.ok; });
    var lines = [];
    if (r.ok) {
      lines.push('\u2713 ' + r.method + ': ' + r.url);
      lines.push('  ' + (r.message || ''));
      el('#s-searxngUrl').value = r.url;
      status.style.color = 'var(--ok)';
      toast('SearXNG ready: ' + r.url, 'ok');
      if (_priorityCache.indexOf('searxng') === -1) { _priorityCache.push('searxng'); renderPriorityRows(); persistPriority(); }
    } else {
      lines.push('\u2717 ' + (r.error || 'unknown error'));
      status.style.color = 'var(--err)';
      toast('Setup failed', 'err');
    }
    lines.push('');
    lines.push('WORKING (' + working.length + '):');
    working.forEach(function(t){ lines.push('  \u2713 ' + t.url + (t.count ? ' (' + t.count + ' results)' : '')); });
    lines.push('');
    lines.push('FAILED (' + failed.length + '):');
    failed.slice(0, 8).forEach(function(t){ lines.push('  \u2717 ' + (t.url || '?') + ' \u2014 ' + (t.error || '')); });
    if (failed.length > 8) lines.push('  ... and ' + (failed.length - 8) + ' more');
    status.textContent = lines.join('\n');
    status.style.whiteSpace = 'pre-wrap';
  } catch (e) {
    status.textContent = '\u2717 ' + e.message;
    status.style.color = 'var(--err)';
    toast('Setup error: ' + e.message, 'err');
  } finally {
    btn.disabled = false; btn.textContent = 'Auto Setup';
  }
});

el('#s-testSearchBtn').addEventListener('click', async function() {
  var q = el('#s-testSearchQ').value.trim() || 'hello world';
  var box = el('#s-testSearchResult');
  box.textContent = 'testing all engines...';
  try {
    var r = await api('/admin/test-search?q=' + encodeURIComponent(q));
    var lines = ['query: ' + r.query, ''];
    for (var name in r.engines) {
      var e = r.engines[name];
      if (e.ok) lines.push('\u2713 ' + name.padEnd(8) + ' ' + e.ms + 'ms  ' + e.count + ' hasil  ' + (e.sample ? e.sample.title.slice(0, 60) : ''));
      else lines.push('\u2717 ' + name.padEnd(8) + ' ' + e.ms + 'ms  ' + e.error);
    }
    box.textContent = lines.join('\n');
  } catch (err) {
    box.textContent = 'error: ' + err.message;
  }
});

function loadSettings() {
  if (!state) return;
  var s = state.settings;
  el('#s-adminToken').value = getToken() || s.adminToken || '';
  el('#s-defaultAlias').value = s.defaultAlias || 'smart';
  el('#s-upstreamTimeoutMs').value = s.upstreamTimeoutMs || 120000;
  el('#s-logLevel').value = s.logLevel || 'info';
  el('#s-paidFallback').value = s.paidFallback ? 'true' : 'false';
  el('#s-fallbackMode').value = s.fallbackMode || 'priority';
  el('#s-probeConcurrency').value = s.probeConcurrency || 5;
  el('#s-logBufferSize').value = s.logBufferSize || 500;
  el('#s-sseHeartbeatMs').value = s.sseHeartbeatMs || 25000;
  el('#s-maxBodyBytes').value = s.maxBodyBytes || 33554432;
  el('#s-maxToolIterations').value = s.maxToolIterations || 8;
  el('#s-maxContextChars').value = s.maxContextChars || 2000000;
  el('#s-searxngUrl').value = s.searxngUrl || '';
  if (Array.isArray(s.searxngCustomUrls)) el('#s-searxngCustomUrls').value = s.searxngCustomUrls.join('\n'); else el('#s-searxngCustomUrls').value = '';
  el('#s-enableShellTool').checked = !!s.enableShellTool;
  el('#s-forceThinkPrompt').checked = !!s.forceThinkPrompt;
  setAccent(s.accent || 'mint');
  renderPriorityList();
}
el('#s-swatches').addEventListener('click', function(e) { var sw = e.target.closest('.swatch'); if (sw) setAccent(sw.dataset.accent); });
el('#s-preset-low').addEventListener('click', function() { el('#s-probeConcurrency').value = 1; el('#s-logBufferSize').value = 100; el('#s-sseHeartbeatMs').value = 60000; el('#s-maxBodyBytes').value = 8388608; el('#s-maxToolIterations').value = 3; el('#s-maxContextChars').value = 100000; toast('low-spec loaded', 'warn'); });
el('#s-preset-default').addEventListener('click', function() { el('#s-probeConcurrency').value = 5; el('#s-logBufferSize').value = 500; el('#s-sseHeartbeatMs').value = 25000; el('#s-maxBodyBytes').value = 33554432; el('#s-maxToolIterations').value = 8; el('#s-maxContextChars').value = 2000000; toast('default loaded', 'ok'); });
el('#s-save').addEventListener('click', async function() {
  try {
    var _newToken = el('#s-adminToken').value.trim();
    var activeAccent = document.querySelector('#s-swatches .swatch.active');
    var payload = {
      adminToken: _newToken || null,
      defaultAlias: el('#s-defaultAlias').value,
      upstreamTimeoutMs: parseInt(el('#s-upstreamTimeoutMs').value, 10),
      logLevel: el('#s-logLevel').value,
      paidFallback: el('#s-paidFallback').value === 'true',
      accent: activeAccent ? activeAccent.dataset.accent : 'mint',
      probeConcurrency: parseInt(el('#s-probeConcurrency').value, 10),
      logBufferSize: parseInt(el('#s-logBufferSize').value, 10),
      sseHeartbeatMs: parseInt(el('#s-sseHeartbeatMs').value, 10),
      maxBodyBytes: parseInt(el('#s-maxBodyBytes').value, 10),
      maxToolIterations: parseInt(el('#s-maxToolIterations').value, 10),
      maxContextChars: parseInt(el('#s-maxContextChars').value, 10),
      searxngUrl: el('#s-searxngUrl').value.trim(),
      searxngCustomUrls: el('#s-searxngCustomUrls').value.split('\n').map(function(s){ return s.trim(); }).filter(function(s){ return s && /^https?:\/\//i.test(s); }),
      searchPriority: (function(){ var p = []; document.querySelectorAll('#s-searchPriorityList [data-pri-name]').forEach(function(r){ p.push(r.dataset.priName); }); return p.join(','); })(),
      enableShellTool: el('#s-enableShellTool').checked,
      forceThinkPrompt: el('#s-forceThinkPrompt').checked,
      fallbackMode: el('#s-fallbackMode').value
    };
    await api('/admin/settings', { method: 'PATCH', body: payload });
    setToken(_newToken);
    toast('saved', 'ok'); refresh();
  } catch (e) { if (e.status !== 401) toast(e.message, 'err'); }
});
el('#s-export').addEventListener('click', async function() { try { var r = await fetch('/admin/export', { headers: getToken() ? { 'Authorization': 'Bearer ' + getToken() } : {} }); var blob = await r.blob(); var a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'pool.json'; a.click(); URL.revokeObjectURL(a.href); } catch (e) { toast(e.message, 'err'); } });
el('#s-import').addEventListener('click', function() { var inp = document.createElement('input'); inp.type = 'file'; inp.accept = '.json,application/json'; inp.onchange = async function() { var f = inp.files[0]; if (!f) return; var text = await f.text(); try { var data = JSON.parse(text); await api('/admin/import', { method: 'POST', body: data }); toast('imported', 'ok'); refresh(); } catch (e) { toast(e.message, 'err'); } }; inp.click(); });
el('#s-reset-all').addEventListener('click', async function() { if (!(await confirmModal('Reset semua cooldown?'))) return; try { var r = await api('/admin/accounts/reset-all', { method: 'POST' }); toast('reset ' + r.count, 'ok'); refresh(); } catch (e) { toast(e.message, 'err'); } });

function openModal(title, body, opts) {
  opts = opts || {};
  var ov = document.createElement('div'); ov.className = 'overlay';
  ov.innerHTML = '<div class="modal' + (opts.wide ? ' wide' : '') + '"><div class="modal-head"><div class="sec-title" style="border:none;font-size:12px">' + esc(title) + '</div><button class="btn ghost small" data-act="close" title="close">' + ic('x') + '</button></div><div class="modal-body">' + body + '</div><div class="modal-foot"><button class="btn ghost" data-act="cancel">Cancel</button>' + (opts.onConfirm ? '<button class="btn primary" data-act="confirm">' + esc(opts.confirmLabel || 'Save') + '</button>' : '') + '</div></div>';
  document.body.appendChild(ov);
  var escHandler = function(e) { if (e.key === 'Escape' && document.body.contains(ov)) { try { if (opts.onCancel) opts.onCancel(); } catch(er) {} close(); } };
  document.addEventListener('keydown', escHandler);
  var close = function() { document.removeEventListener('keydown', escHandler); ov.remove(); };
  ov.addEventListener('click', async function(e) {
    var act = e.target.dataset.act;
    if (act === 'close' || act === 'cancel' || e.target === ov) { try { if (opts.onCancel) opts.onCancel(); } catch (e2) {} close(); }
    else if (act === 'confirm') { try { var res = await opts.onConfirm(ov); if (res !== false) close(); } catch (err) { toast(err.message, 'err'); } }
  });
  return { overlay: ov, close: close };
}
function customPrompt(title, opts) {
  opts = opts || {};
  return new Promise(function(resolve) {
    var inputId = '__cp_' + Math.random().toString(36).slice(2, 8);
    var body = '<div class="field"><label>' + esc(opts.label || 'Value') + '</label>' +
      '<input id="' + inputId + '" type="' + (opts.type || 'text') + '" value="' + esc(opts.defaultValue || '') + '" placeholder="' + esc(opts.placeholder || '') + '">' +
      (opts.description ? '<div class="hint" style="margin-top:4px">' + esc(opts.description) + '</div>' : '') + '</div>';
    var settled = false;
    function finish(val) { if (!settled) { settled = true; resolve(val); } }
    var m = openModal(title, body, {
      confirmLabel: opts.confirmLabel || 'OK',
      onConfirm: function(ov) { finish(ov.querySelector('#' + inputId).value); return true; },
      onCancel: function() { finish(null); }
    });
    setTimeout(function() {
      var inp = m.overlay.querySelector('#' + inputId);
      if (!inp) return;
      inp.focus();
      inp.select();
      inp.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
          e.preventDefault();
          var btn = m.overlay.querySelector('[data-act="confirm"]');
          if (btn) btn.click();
        }
      });
    }, 40);
  });
}
function customAlert(title, message, opts) {
  opts = opts || {};
  return new Promise(function(resolve) {
    var body = '<p style="font-family:IBM Plex Mono,monospace;font-size:13px;line-height:1.6;color:var(--fg);margin:0;white-space:pre-wrap">' + esc(message || '') + '</p>';
    openModal(title, body, {
      confirmLabel: opts.confirmLabel || 'OK',
      onConfirm: function() { resolve(); return true; },
      onCancel: function() { resolve(); }
    });
  });
}
function confirmModal(msg) { return new Promise(function(resolve) { var m = openModal('Confirm', '<p>' + esc(msg) + '</p>', { confirmLabel: 'Confirm', onConfirm: function(){ resolve(true); } }); m.overlay.addEventListener('click', function(e) { if (e.target === m.overlay || e.target.dataset.act === 'cancel' || e.target.dataset.act === 'close') resolve(false); }); }); }

setInterval(function() { if (currentTab === 'overview' || currentTab === 'accounts') refresh(); }, 10000);

async function boot() { await refresh(); loadAgents(); loadConvList(); populateChatModels(); setMode('agent'); }
boot();
})();
</script>
</body>
</html>`;

// ─── ROUTER ─────────────────────────────────────────────────────────────────
async function handler(req, res) {
  try {
    const u = new URL(req.url, 'http://' + (req.headers.host || 'localhost'));
    const pathname = u.pathname;
    const method = (req.method || 'GET').toUpperCase();
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Access-Control-Expose-Headers', 'x-served-by, x-served-account');
    if (method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
    if (pathname === '/' && method === 'GET') { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); res.end(DASHBOARD_HTML); return; }
    if (pathname === '/health') {
      wakeup();
      const mem = process.memoryUsage();
      return jsonResponse(res, 200, { ok: true, node: process.version, platform: process.platform, arch: process.arch, uptimeSec: Math.round(process.uptime()), rssMB: Math.round(mem.rss / 1048576), heapMB: Math.round(mem.heapUsed / 1048576), poolFile: POOL_FILE, accounts: state.accounts.length, providers: Object.keys(state.providers).length, agents: agents.agents.length, skills: agents.skills.length, models: (state.models || []).length, plugins: plugins.plugins.length, mcpServers: mcp.servers.length, tools: listAllTools().length, conversations: chats.conversations.length, fallbackMode: state.settings.fallbackMode, maxContextChars: state.settings.maxContextChars, searchPriority: state.settings.searchPriority });
    }
    if (pathname === '/v1/models' && method === 'GET') {
      const data = [{ id: 'smart', object: 'model', owned_by: 'pool' }, { id: 'fast', object: 'model', owned_by: 'pool' }, { id: 'auto', object: 'model', owned_by: 'pool' }];
      for (const [name, prov] of Object.entries(state.providers)) { if (!prov || !prov.enabled) continue; for (const mid of Object.values(prov.models || {})) if (mid) data.push({ id: name + ':' + mid, object: 'model', owned_by: name }); }
      return jsonResponse(res, 200, { object: 'list', data });
    }
    if (pathname === '/v1/chat/completions' && method === 'POST') { const body = await readBody(req, state.settings.maxBodyBytes || 33554432); return handleChatCompletions(req, res, body); }
    if (pathname.startsWith('/admin/')) {
      if (!checkAuth(req, res, u)) return;
      if (pathname === '/admin/state' && method === 'GET') return adminState(res);
      if (pathname === '/admin/settings' && method === 'PATCH') return adminSettings(req, res);
      if (pathname === '/admin/accounts' && method === 'POST') return adminAddAccount(req, res);
      if (pathname === '/admin/accounts/bulk' && method === 'POST') return adminBulkAccounts(req, res);
      if (pathname === '/admin/accounts/reset-all' && method === 'POST') return adminResetAll(res);
      if (pathname === '/admin/probe-all' && method === 'POST') return adminProbeAll(req, res);
      if (pathname === '/admin/providers' && method === 'POST') return adminAddProvider(req, res);
      if (pathname === '/admin/logs/stream' && method === 'GET') return adminLogsStream(req, res);
      if (pathname === '/admin/logs' && method === 'GET') return adminLogs(res, u);
      if (pathname === '/admin/export' && method === 'GET') return adminExport(res);
      if (pathname === '/admin/import' && method === 'POST') return adminImport(req, res);
      if (pathname === '/admin/agents' && method === 'GET') return adminGetAgents(res);
      if (pathname === '/admin/agents' && method === 'POST') return adminAddAgent(req, res);
      if (pathname === '/admin/skills' && method === 'POST') return adminAddSkill(req, res);
      if (pathname === '/admin/chats' && method === 'GET') return adminListChats(res);
      if (pathname === '/admin/chats' && method === 'DELETE') return adminDeleteAllChats(res);
      if (pathname === '/admin/chats/export' && method === 'GET') return adminExportChats(res);
      if (pathname === '/admin/chats/import' && method === 'POST') return adminImportChats(req, res);
      if (pathname === '/admin/chat' && method === 'POST') return orchestrateChat(req, res);
      if (pathname === '/admin/plugins' && method === 'GET') return adminListPlugins(res);
      if (pathname === '/admin/plugins' && method === 'POST') return adminAddPlugin(req, res);
      if (pathname === '/admin/mcp' && method === 'GET') return adminListMCP(res);
      if (pathname === '/admin/mcp' && method === 'POST') return adminAddMCP(req, res);
      if (pathname === '/admin/tools' && method === 'GET') return adminListTools(res);
      if (pathname === '/admin/models' && method === 'GET') return adminListModels(res);
      if (pathname === '/admin/models' && method === 'POST') return adminAddModel(req, res);
      if (pathname === '/admin/models/reseed' && method === 'POST') return adminReseedModels(req, res);
      if (pathname === '/admin/search-engines' && method === 'GET') {
        const available = ['bing', 'yahoo', 'mojeek', 'ddg', 'searxng', '4get-ddg', '4get-brave', '4get-google', 'stract', 'ecosia', 'brave', 'yep', 'librey', 'marginalia', 'mwmbl'];
        const meta = {
          bing: { name: 'Bing', desc: 'Fast, stable, works in ID', blocked: false },
          yahoo: { name: 'Yahoo', desc: 'Works in ID, slower', blocked: false },
          mojeek: { name: 'Mojeek', desc: 'Privacy, small index', blocked: false },
          ddg: { name: 'DuckDuckGo', desc: 'Blocked by Kominfo in ID (need VPN)', blocked: true },
          searxng: { name: 'SearXNG', desc: 'Requires setup', blocked: false },
          '4get-ddg': { name: '4get / DDG', desc: 'Server proxy, works in ID', blocked: false },
          '4get-brave': { name: '4get / Brave', desc: 'Server proxy, works in ID', blocked: false },
          '4get-google': { name: '4get / Google', desc: 'Server proxy, works in ID', blocked: false },
          stract: { name: 'Stract', desc: 'Independent, niche topics', blocked: false },
          ecosia: { name: 'Ecosia', desc: 'Bing-backed, works in ID', blocked: false },
          brave: { name: 'Brave HTML', desc: 'Independent index', blocked: false },
          yep: { name: 'Yep', desc: 'Independent (Ahrefs), JSON', blocked: false },
          librey: { name: 'LibreY', desc: 'Metasearch, keyless', blocked: false },
          marginalia: { name: 'Marginalia', desc: 'Indie, niche articles', blocked: false },
          mwmbl: { name: 'Mwmbl', desc: 'Crowdsourced index', blocked: false },
        };
        const priority = (state.settings.searchPriority || 'bing,yahoo,mojeek,ddg,searxng').split(',').map(function(s){ return s.trim(); }).filter(Boolean);
        return jsonResponse(res, 200, { available: available, meta: meta, priority: priority, searxngUrl: state.settings.searxngUrl || '' });
      }
      if (pathname === '/admin/setup-searxng' && method === 'POST') {
        const tried = [];
        if (state.settings.searxngUrl) {
          try {
            const testUrl = state.settings.searxngUrl.replace(/\/+$/, '') + '/search?q=test&format=json';
            const r = await fetch(testUrl, { headers: { 'User-Agent': 'Mozilla/5.0' }, signal: AbortSignal.timeout(5000) });
            if (r.ok) {
              const ct = (r.headers.get('content-type') || '').toLowerCase();
              if (ct.includes('json')) {
                const d = await r.json();
                if (Array.isArray(d.results)) {
                  return jsonResponse(res, 200, { ok: true, method: 'existing', url: state.settings.searxngUrl, message: 'Existing URL works, kept.' });
                }
              }
            }
            tried.push({ url: state.settings.searxngUrl, ok: false, error: 'existing URL failed test' });
          } catch (e) { tried.push({ url: state.settings.searxngUrl, ok: false, error: e.message }); }
        }
        try {
          const dockerOk = await new Promise(function(resolve) {
            const proc = spawn('docker', ['--version'], { timeout: 3000 });
            proc.on('close', function(c) { resolve(c === 0); });
            proc.on('error', function() { resolve(false); });
          });
          if (dockerOk) {
            log('info', 'searxng', '-', 'setup', 'docker available, trying local container');
            const exists = await new Promise(function(resolve) {
              const proc = spawn('docker', ['ps', '-a', '--filter', 'name=pool-searxng', '--format', '{{.Names}}'], { timeout: 5000 });
              let out = '';
              proc.stdout.on('data', function(d) { out += d.toString(); });
              proc.on('close', function() { resolve(out.trim() === 'pool-searxng'); });
              proc.on('error', function() { resolve(false); });
            });
            if (exists) {
              await new Promise(function(resolve) {
                const proc = spawn('docker', ['start', 'pool-searxng'], { timeout: 10000 });
                proc.on('close', function() { resolve(); });
                proc.on('error', function() { resolve(); });
              });
              for (let i = 0; i < 10; i++) {
                await new Promise(function(r) { setTimeout(r, 2000); });
                try {
                  const r = await fetch('http://localhost:8081/search?q=test&format=json', { signal: AbortSignal.timeout(3000) });
                  if (r.ok) {
                    const d = await r.json();
                    if (Array.isArray(d.results)) {
                      state.settings.searxngUrl = 'http://localhost:8081';
                      savePoolNow();
                      return jsonResponse(res, 200, { ok: true, method: 'docker', url: 'http://localhost:8081', message: 'Local docker container is running.', tried: tried });
                    }
                  }
                } catch (e) { /* not ready yet */ }
              }
              tried.push({ url: 'http://localhost:8081', ok: false, error: 'docker container not responding after 20s' });
            } else {
              tried.push({ url: 'docker', ok: false, error: 'container not installed (run: docker run -d --name pool-searxng -p 8081:8080 searxng/searxng)' });
            }
          } else {
            tried.push({ url: 'docker', ok: false, error: 'docker not available' });
          }
        } catch (e) { tried.push({ url: 'docker', ok: false, error: e.message }); }

        const customUrls = Array.isArray(state.settings.searxngCustomUrls) ? state.settings.searxngCustomUrls.slice() : [];
        if (customUrls.length > 0) {
          log('info', 'searxng', '-', 'setup', 'trying ' + customUrls.length + ' custom URL(s)');
          const customTests = await Promise.all(customUrls.map(async function(baseUrl) {
            const url = baseUrl.replace(/\/+$/, '');
            try {
              const r = await fetch(url + '/search?q=test&format=json', {
                headers: { 'User-Agent': 'Mozilla/5.0 (compatible; pool-setup/1.0)' },
                signal: AbortSignal.timeout(6000),
              });
              if (!r.ok) return { url: url, ok: false, error: 'HTTP ' + r.status, custom: true };
              const ct = (r.headers.get('content-type') || '').toLowerCase();
              if (!ct.includes('json')) return { url: url, ok: false, error: 'not json (' + ct.slice(0, 30) + ')', custom: true };
              const d = await r.json();
              if (!Array.isArray(d.results)) return { url: url, ok: false, error: 'no results array', custom: true };
              return { url: url, ok: true, count: d.results.length, custom: true };
            } catch (e) {
              return { url: url, ok: false, error: e.message.slice(0, 60), custom: true };
            }
          }));
          const customOk = customTests.filter(function(t) { return t.ok; });
          customTests.forEach(function(t) { tried.push(t); });
          if (customOk.length > 0) {
            state.settings.searxngUrl = customOk[0].url;
            savePoolNow();
            log('info', 'searxng', '-', 'setup', 'custom ' + customOk[0].url);
            return jsonResponse(res, 200, { ok: true, method: 'custom', url: customOk[0].url, message: 'Using user-provided SearXNG: ' + customOk[0].url, tried: tried });
          }
          log('warn', 'searxng', '-', 'setup', 'all custom URLs failed, falling back');
        }
        const candidates = [
          'https://searx.be', 'https://searx.tiekoetter.com', 'https://search.inetol.net', 'https://baresearch.org',
          'https://searxng.site', 'https://priv.au', 'https://searx.work', 'https://opnxng.com',
          'https://search.hbubli.cc', 'https://searx.oloke.xyz', 'https://paulgo.io', 'https://searx.rhscz.eu',
          'https://searx.itsdumping.com', 'https://searxng.world', 'https://searx.namejeff.xyz',
        ];
        const tests = await Promise.all(candidates.map(async function(baseUrl) {
          const url = baseUrl.replace(/\/+$/, '');
          try {
            const r = await fetch(url + '/search?q=test&format=json', {
              headers: { 'User-Agent': 'Mozilla/5.0 (compatible; pool-setup/1.0)' },
              signal: AbortSignal.timeout(6000),
            });
            if (!r.ok) return { url: url, ok: false, error: 'HTTP ' + r.status };
            const ct = (r.headers.get('content-type') || '').toLowerCase();
            if (!ct.includes('json')) return { url: url, ok: false, error: 'not json (' + ct.slice(0, 30) + ')' };
            const d = await r.json();
            if (!Array.isArray(d.results)) return { url: url, ok: false, error: 'no results array' };
            return { url: url, ok: true, count: d.results.length };
          } catch (e) {
            return { url: url, ok: false, error: e.message.slice(0, 60) };
          }
        }));
        const working = tests.filter(function(t) { return t.ok; });
        working.forEach(function(w) { tried.push(w); });
        tests.filter(function(t) { return !t.ok; }).forEach(function(t) { tried.push(t); });
        if (working.length === 0) {
          return jsonResponse(res, 200, { ok: false, method: 'public', error: 'No working public SearXNG instance found. Try Bing/Yahoo instead.', tried: tried });
        }
        state.settings.searxngUrl = working[0].url;
        savePoolNow();
        log('info', 'searxng', '-', 'setup', 'saved ' + working[0].url);
        return jsonResponse(res, 200, { ok: true, method: 'public', url: working[0].url, message: 'Found ' + working.length + ' working instance(s), using first.', tried: tried });
      }
      if (pathname === '/admin/test-search' && method === 'GET') {
        const q = u.searchParams.get('q') || 'hello world';
        const list = ['bing', 'yahoo', 'mojeek', 'ddg', '4get-ddg', 'ecosia', 'yep', 'librey', 'brave', 'marginalia', 'mwmbl', 'stract'];
        if (state.settings.searxngUrl) list.unshift('searxng');
        const report = { query: q, engines: {} };
        for (const name of list) {
          const t0 = Date.now();
          try {
            const fn = SEARCH_ENGINES[name];
            const r = await fn(q, 3);
            report.engines[name] = { ok: true, count: r.length, ms: Date.now() - t0, sample: r[0] || null };
          } catch (e) {
            report.engines[name] = { ok: false, error: e.message, ms: Date.now() - t0 };
          }
        }
        return jsonResponse(res, 200, report);
      }
      if (pathname === '/admin/fallback-preview' && method === 'GET') {
        const modelQ = u.searchParams.get('model') || state.settings.defaultAlias || 'smart';
        const req = resolveRequest(modelQ, null);
        const cands = getFallbackCandidates(req, new Set());
        return jsonResponse(res, 200, {
          model: modelQ, resolved: req, fallbackMode: state.settings.fallbackMode || 'priority', count: cands.length,
          candidates: cands.map(function(c) { return { id: c.acc.id, provider: c.acc.provider, label: c.acc.label, accountPriority: accountPriority(c.acc), paid: isPaidAccount(c.acc), modelId: c.modelId, modelLabel: c.modelLabel, modelLabels: c.modelLabels || [], modelPriority: c.modelPriority }; }),
        });
      }
      let m;
      m = pathname.match(/^\/admin\/accounts\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchAccount(req, res, id); if (method === 'DELETE') return adminDeleteAccount(res, id); }
      m = pathname.match(/^\/admin\/accounts\/([^/]+)\/reset-and-probe$/);
      if (m && method === 'POST') return adminResetAndProbe(req, res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/accounts\/([^/]+)\/reset$/);
      if (m && method === 'POST') return adminResetAccount(res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/accounts\/([^/]+)\/probe$/);
      if (m && method === 'POST') return adminProbeAccount(req, res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/models\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchModel(req, res, id); if (method === 'DELETE') return adminDeleteModel(res, id); }
      m = pathname.match(/^\/admin\/providers\/([^/]+)$/);
      if (m) { const name = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchProvider(req, res, name); if (method === 'DELETE') return adminDeleteProvider(res, name); }
      m = pathname.match(/^\/admin\/agents\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchAgent(req, res, id); if (method === 'DELETE') return adminDeleteAgent(res, id); }
      m = pathname.match(/^\/admin\/skills\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchSkill(req, res, id); if (method === 'DELETE') return adminDeleteSkill(res, id); }
      m = pathname.match(/^\/admin\/chats\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'GET') return adminGetChat(res, id); if (method === 'DELETE') return adminDeleteChat(res, id); if (method === 'PATCH') return adminPatchChat(req, res, id); }
      m = pathname.match(/^\/admin\/chats\/([^/]+)\/clean-artifacts$/);
      if (m && method === 'POST') return adminCleanChatArtifacts(req, res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/chats\/([^/]+)\/truncate$/);
      if (m && method === 'POST') return adminTruncateChat(req, res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/chats\/([^/]+)\/messages\/(\d+)$/);
      if (m && method === 'PATCH') return adminPatchChatMessage(req, res, decodeURIComponent(m[1]), m[2]);
      m = pathname.match(/^\/admin\/plugins\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchPlugin(req, res, id); if (method === 'DELETE') return adminDeletePlugin(res, id); }
      m = pathname.match(/^\/admin\/mcp\/([^/]+)$/);
      if (m) { const id = decodeURIComponent(m[1]); if (method === 'PATCH') return adminPatchMCP(req, res, id); if (method === 'DELETE') return adminDeleteMCP(res, id); }
      m = pathname.match(/^\/admin\/mcp\/([^/]+)\/connect$/);
      if (m && method === 'POST') return adminConnectMCP(req, res, decodeURIComponent(m[1]));
      m = pathname.match(/^\/admin\/mcp\/([^/]+)\/disconnect$/);
      if (m && method === 'POST') return adminDisconnectMCP(res, decodeURIComponent(m[1]));
    }
    jsonResponse(res, 404, { error: { message: 'Not found', path: pathname } });
  } catch (e) {
    log('err', '-', '-', 'handler', e.message);
    try { jsonResponse(res, 500, { error: { message: 'Internal error', detail: e.message } }); } catch {}
  }
}

// ─── ADMIN HANDLERS ─────────────────────────────────────────────────────────
function adminState(res) { wakeup(); const c = redactPoolState(); const mem = process.memoryUsage(); c.system = { node: process.version, platform: process.platform, arch: process.arch, uptimeSec: Math.round(process.uptime()), memRSSMB: Math.round(mem.rss / 1048576), memHeapMB: Math.round(mem.heapUsed / 1048576), poolFile: POOL_FILE }; jsonResponse(res, 200, c); }
async function adminSettings(req, res) {
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  const allowed = ['adminToken','defaultAlias','upstreamTimeoutMs','paidFallback','accent','logLevel','probeConcurrency','logBufferSize','sseHeartbeatMs','maxBodyBytes','enableShellTool','fallbackMode','maxToolIterations','maxContextChars','searxngUrl','searchEngine','searchPriority','searxngCustomUrls','forceThinkPrompt'];
  for (const k of Object.keys(body)) if (allowed.includes(k)) state.settings[k] = body[k];
  if (state.settings.adminToken === '') state.settings.adminToken = null;
  state.settings.upstreamTimeoutMs = Math.max(5000, Math.min(600000, parseInt(state.settings.upstreamTimeoutMs, 10) || 120000));
  state.settings.probeConcurrency = Math.max(1, Math.min(20, parseInt(state.settings.probeConcurrency, 10) || 5));
  state.settings.logBufferSize = Math.max(10, Math.min(5000, parseInt(state.settings.logBufferSize, 10) || 500));
  state.settings.sseHeartbeatMs = Math.max(5000, Math.min(300000, parseInt(state.settings.sseHeartbeatMs, 10) || 25000));
  state.settings.maxBodyBytes = Math.max(1048576, Math.min(268435456, parseInt(state.settings.maxBodyBytes, 10) || 33554432));
  state.settings.enableShellTool = !!state.settings.enableShellTool;
  state.settings.forceThinkPrompt = !!state.settings.forceThinkPrompt;
  state.settings.maxToolIterations = Math.max(1, Math.min(20, parseInt(state.settings.maxToolIterations, 10) || 8));
  state.settings.maxContextChars = Math.max(100000, Math.min(10000000, parseInt(state.settings.maxContextChars, 10) || 2000000));
  if (!['priority','provider','round-robin'].includes(state.settings.fallbackMode)) state.settings.fallbackMode = 'priority';
  if (Array.isArray(state.settings.searxngCustomUrls)) {
    state.settings.searxngCustomUrls = state.settings.searxngCustomUrls.map(function(u) { return String(u || '').trim().replace(/\/+$/, ''); }).filter(function(u) { return /^https?:\/\//i.test(u); }).slice(0, 20);
  } else {
    state.settings.searxngCustomUrls = [];
  }
  savePoolNow();
  log('info', '-', '-', 'settings-updated', JSON.stringify(Object.keys(body)));
  jsonResponse(res, 200, { ok: true, settings: state.settings });
}
async function adminAddAccount(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const { provider, label, apiKey, accountId, priority, enabled, note, model } = body; if (!provider || !apiKey) return jsonResponse(res, 400, { error: { message: 'provider + apiKey required' } }); if (!state.providers[provider]) return jsonResponse(res, 400, { error: { message: 'unknown provider' } }); const acc = { id: genId('ac'), provider, label: label || provider + '-' + Date.now(), apiKey, accountId: accountId || null, model: model || null, status: 'ready', resetAt: 0, remaining: null, priority: typeof priority === 'number' ? priority : null, lastUsed: 0, failCount: 0, authFails: 0, enabled: enabled !== false, note: note || '' }; state.accounts.push(acc); savePoolNow(); log('info', provider, acc.label, 'account-added', acc.id); jsonResponse(res, 200, { ok: true, account: { ...acc, apiKey: acc.apiKey.slice(0, 8) + '...' } }); }
async function adminBulkAccounts(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const list = Array.isArray(body) ? body : body.accounts; if (!Array.isArray(list)) return jsonResponse(res, 400, { error: { message: 'expected array' } }); const added = [], errors = []; for (const item of list) { if (!item || !item.provider || !item.apiKey) { errors.push({ item, error: 'missing' }); continue; } if (!state.providers[item.provider]) { errors.push({ item, error: 'unknown provider' }); continue; } const acc = { id: genId('ac'), provider: item.provider, label: item.label || item.provider + '-' + Date.now(), apiKey: item.apiKey, accountId: item.accountId || null, model: item.model || null, status: 'ready', resetAt: 0, remaining: null, priority: typeof item.priority === 'number' ? item.priority : null, lastUsed: 0, failCount: 0, authFails: 0, enabled: item.enabled !== false, note: item.note || '' }; state.accounts.push(acc); added.push({ id: acc.id, label: acc.label }); } savePoolNow(); jsonResponse(res, 200, { ok: true, added, errors }); }
async function adminPatchAccount(req, res, id) { const acc = state.accounts.find(a => a.id === id); if (!acc) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['label','priority','enabled','note','apiKey','accountId','model']) if (f in body) acc[f] = body[f]; savePoolNow(); jsonResponse(res, 200, { ok: true, account: { ...acc, apiKey: acc.apiKey ? acc.apiKey.slice(0, 8) + '...' : '' } }); }
function adminDeleteAccount(res, id) { const idx = state.accounts.findIndex(a => a.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); state.accounts.splice(idx, 1); savePoolNow(); jsonResponse(res, 200, { ok: true }); }
function adminResetAccount(res, id) { const acc = state.accounts.find(a => a.id === id); if (!acc) return jsonResponse(res, 404, { error: { message: 'not found' } }); acc.status = 'ready'; acc.resetAt = 0; acc.failCount = 0; acc.remaining = null; acc.authFails = 0; acc.lastError = null; acc.lastProbe = null; savePoolNow(); jsonResponse(res, 200, { ok: true }); }
async function adminResetAndProbe(req, res, id) { const acc = state.accounts.find(a => a.id === id); if (!acc) return jsonResponse(res, 404, { error: { message: 'not found' } }); acc.status = 'ready'; acc.resetAt = 0; acc.failCount = 0; acc.authFails = 0; acc.remaining = null; acc.lastError = null; savePoolNow(); const r = await probeAccount(acc); jsonResponse(res, 200, { ok: true, result: r, account: { ...acc, apiKey: undefined } }); }
function adminResetAll(res) { let n = 0; for (const acc of state.accounts) if (acc.status === 'cooldown') { acc.status = 'ready'; acc.resetAt = 0; acc.failCount = 0; acc.remaining = null; acc.authFails = 0; acc.lastError = null; n++; } savePoolNow(); jsonResponse(res, 200, { ok: true, count: n }); }
async function adminProbeAccount(req, res, id) { const acc = state.accounts.find(a => a.id === id); if (!acc) return jsonResponse(res, 404, { error: { message: 'not found' } }); const r = await probeAccount(acc); jsonResponse(res, 200, { ok: true, result: r }); }
async function adminProbeAll(req, res) { let body = {}; try { body = await readJson(req); } catch {} const list = state.accounts.filter(a => !body.ids || body.ids.includes(a.id)); const queue = list.slice(); const results = []; const CONC = Math.max(1, Math.min(20, state.settings.probeConcurrency || 5)); const workers = []; for (let i = 0; i < CONC; i++) workers.push((async () => { while (queue.length) { const a = queue.shift(); if (!a) break; const r = await probeAccount(a); results.push({ id: a.id, label: a.label, ...r }); } })()); await Promise.all(workers); jsonResponse(res, 200, { ok: true, results }); }
async function adminAddProvider(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const { name } = body; if (!name || !/^[a-z0-9_\-]+$/i.test(name)) return jsonResponse(res, 400, { error: { message: 'invalid name' } }); if (state.providers[name]) return jsonResponse(res, 400, { error: { message: 'exists' } }); const base = String(body.base || '').trim(); if (!base || !/^https?:\/\//i.test(base)) return jsonResponse(res, 400, { error: { message: 'base URL harus http(s)://' } }); const models = body.models || {}; state.providers[name] = { base: base.replace(/\/+$/, ''), tz: String(body.tz || 'UTC').trim() || 'UTC', paid: !!body.paid, needsAccountId: !!body.needsAccountId, enabled: body.enabled !== false, priority: typeof body.priority === 'number' ? body.priority : 50, models: { smart: String(models.smart || '').trim(), fast: String(models.fast || '').trim() } }; savePoolNow(); log('info', name, '-', 'provider-added', name); jsonResponse(res, 200, { ok: true, provider: state.providers[name] }); }
async function adminPatchProvider(req, res, name) { const prov = state.providers[name]; if (!prov) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['base','tz','paid','needsAccountId','enabled','priority']) if (f in body) prov[f] = body[f]; if (body.models && typeof body.models === 'object') prov.models = { ...(prov.models || {}), ...body.models }; savePoolNow(); jsonResponse(res, 200, { ok: true, provider: prov }); }
function adminDeleteProvider(res, name) { if (!state.providers[name]) return jsonResponse(res, 404, { error: { message: 'not found' } }); const cnt = state.accounts.filter(a => a.provider === name).length; if (cnt > 0) return jsonResponse(res, 400, { error: { message: 'has ' + cnt + ' accounts' } }); delete state.providers[name]; savePoolNow(); jsonResponse(res, 200, { ok: true }); }
function adminLogs(res, u) { const limit = Math.max(1, Math.min(500, parseInt(u.searchParams.get('limit') || '200', 10))); jsonResponse(res, 200, { logs: logBuffer.slice(-limit) }); }
function adminLogsStream(req, res) { res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' }); for (const e of logBuffer.slice(-50)) res.write('data: ' + JSON.stringify(e) + '\n\n'); logClients.add(res); const hb = setInterval(() => { try { res.write(': ping\n\n'); } catch {} }, Math.max(5000, state.settings.sseHeartbeatMs || 25000)); req.on('close', () => { clearInterval(hb); logClients.delete(res); }); }
function adminExport(res) { res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="pool.json"' }); res.end(JSON.stringify(state, null, 2)); }
async function adminImport(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const parsed = body && body.pool ? body.pool : body; if (!parsed || !parsed.providers || !Array.isArray(parsed.accounts)) return jsonResponse(res, 400, { error: { message: 'invalid pool' } }); state = { version: 1, settings: { ...DEFAULT_SETTINGS, ...(parsed.settings || {}) }, providers: parsed.providers, accounts: parsed.accounts, models: Array.isArray(parsed.models) ? parsed.models : [] }; savePoolNow(); jsonResponse(res, 200, { ok: true, accounts: state.accounts.length }); }

function adminGetAgents(res) { jsonResponse(res, 200, { agents: agents.agents, skills: agents.skills }); }
async function adminAddAgent(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const agent = { id: genId('ag'), name: String(body.name || 'Untitled').slice(0, 80), description: String(body.description || '').slice(0, 300), systemPrompt: String(body.systemPrompt || ''), model: String(body.model || 'smart'), mode: (body.mode === 'chat' || body.mode === 'agent') ? body.mode : 'agent', temperature: typeof body.temperature === 'number' ? body.temperature : 0.7, tools: Array.isArray(body.tools) ? body.tools.filter(x => typeof x === 'string') : [], skills: Array.isArray(body.skills) ? body.skills.filter(x => typeof x === 'string') : [], enabled: body.enabled !== false }; agents.agents.push(agent); saveAgents(); log('info', 'agent', agent.name, 'created', agent.id); jsonResponse(res, 200, { ok: true, agent }); }
async function adminPatchAgent(req, res, id) { const a = agents.agents.find(x => x.id === id); if (!a) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['name','description','systemPrompt','model','mode','temperature','tools','skills','enabled']) if (f in body) a[f] = body[f]; saveAgents(); jsonResponse(res, 200, { ok: true, agent: a }); }
function adminDeleteAgent(res, id) { const idx = agents.agents.findIndex(x => x.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); if (agents.agents.length <= 1) return jsonResponse(res, 400, { error: { message: 'cannot delete last agent' } }); agents.agents.splice(idx, 1); saveAgents(); jsonResponse(res, 200, { ok: true }); }
async function adminAddSkill(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } const skill = { id: genId('sk'), name: String(body.name || 'Untitled').slice(0, 80), description: String(body.description || '').slice(0, 300), systemPrompt: String(body.systemPrompt || ''), tools: Array.isArray(body.tools) ? body.tools.filter(x => typeof x === 'string') : [] }; agents.skills.push(skill); saveAgents(); jsonResponse(res, 200, { ok: true, skill }); }
async function adminPatchSkill(req, res, id) { const s = agents.skills.find(x => x.id === id); if (!s) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['name','description','systemPrompt','tools']) if (f in body) s[f] = body[f]; saveAgents(); jsonResponse(res, 200, { ok: true, skill: s }); }
function adminDeleteSkill(res, id) { const idx = agents.skills.findIndex(x => x.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); agents.skills.splice(idx, 1); for (const a of agents.agents) if (Array.isArray(a.skills)) a.skills = a.skills.filter(x => x !== id); saveAgents(); jsonResponse(res, 200, { ok: true }); }

function adminListChats(res) { const out = chats.conversations.slice().sort((a, b) => { const ap = a.pinned ? 1 : 0, bp = b.pinned ? 1 : 0; if (bp !== ap) return bp - ap; return (b.updatedAt || 0) - (a.updatedAt || 0); }).map(c => ({ id: c.id, title: c.title, agentId: c.agentId, modeOverride: c.modeOverride || null, updatedAt: c.updatedAt, createdAt: c.createdAt, messageCount: (c.messages || []).length, pinned: !!c.pinned })); jsonResponse(res, 200, { conversations: out }); }
async function adminPatchChat(req, res, id) {
  const c = chats.conversations.find(x => x.id === id);
  if (!c) return jsonResponse(res, 404, { error: { message: 'not found' } });
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  if (typeof body.title === 'string') c.title = String(body.title).slice(0, 200);
  if (typeof body.pinned === 'boolean') c.pinned = body.pinned;
  if (typeof body.agentId === 'string') c.agentId = body.agentId;
  if (body.modeOverride === 'chat' || body.modeOverride === 'agent' || body.modeOverride === null) c.modeOverride = body.modeOverride;
  c.updatedAt = Date.now();
  saveChatsNow();
  jsonResponse(res, 200, { ok: true, conversation: c });
}
function adminDeleteAllChats(res) {
  const n = chats.conversations.length;
  chats.conversations = [];
  saveChatsNow();
  log('info', 'chat', '-', 'delete-all', n + ' conversations');
  jsonResponse(res, 200, { ok: true, count: n });
}
async function adminTruncateChat(req, res, id) {
  const c = chats.conversations.find(x => x.id === id);
  if (!c) return jsonResponse(res, 404, { error: { message: 'not found' } });
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  const fromIndex = parseInt(body.fromIndex, 10);
  if (!Number.isFinite(fromIndex) || fromIndex < 0 || !c.messages || fromIndex >= c.messages.length) return jsonResponse(res, 400, { error: { message: 'invalid fromIndex' } });
  const removed = c.messages.length - fromIndex;
  c.messages = c.messages.slice(0, fromIndex);
  c.updatedAt = Date.now();
  saveChatsNow();
  jsonResponse(res, 200, { ok: true, removed, remaining: c.messages.length });
}
async function adminPatchChatMessage(req, res, id, idx) {
  const c = chats.conversations.find(x => x.id === id);
  if (!c) return jsonResponse(res, 404, { error: { message: 'chat not found' } });
  const i = parseInt(idx, 10);
  if (!Number.isFinite(i) || i < 0 || i >= (c.messages || []).length) return jsonResponse(res, 400, { error: { message: 'invalid index' } });
  const m = c.messages[i];
  if (m.role !== 'user' && m.role !== 'assistant') return jsonResponse(res, 400, { error: { message: 'can only edit user or assistant messages' } });
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  if (typeof body.content === 'string') m.content = body.content.slice(0, 100000);
  c.updatedAt = Date.now();
  saveChatsNow();
  jsonResponse(res, 200, { ok: true, message: m });
}
async function adminCleanChatArtifacts(req, res, id) {
  const c = chats.conversations.find(x => x.id === id);
  if (!c) return jsonResponse(res, 404, { error: { message: 'chat not found' } });
  const POISON = [/saya tidak akan memanggil tool/i, /i will not call (a )?tool/i, /jangan panggil tool/i, /do not call (a )?tool/i, /tidak dapat memberikan berita.*real.?time/i, /cannot provide real.?time/i, /tidak menggunakan pencarian web/i, /tidak bisa mengakses internet/i, /tidak akan memanggil tool lagi/i];
  let removed = 0;
  c.messages = (c.messages || []).filter(function(m) {
    if (m.role !== 'assistant') return true;
    var t = String(m.content || '');
    for (var i = 0; i < POISON.length; i++) if (POISON[i].test(t)) { removed++; return false; }
    return true;
  });
  c.updatedAt = Date.now();
  saveChatsNow();
  log('info', 'chat', id, 'clean-artifacts', removed + ' removed');
  jsonResponse(res, 200, { ok: true, removed, remaining: c.messages.length });
}
function adminExportChats(res) {
  res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Disposition': 'attachment; filename="chats.json"' });
  res.end(JSON.stringify(chats, null, 2));
}
async function adminImportChats(req, res) {
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  if (!body || !Array.isArray(body.conversations)) return jsonResponse(res, 400, { error: { message: 'invalid chats file' } });
  const existing = new Set(chats.conversations.map(c => c.id));
  let added = 0, updated = 0;
  for (const incoming of body.conversations) {
    if (!incoming || !incoming.id) continue;
    if (existing.has(incoming.id)) {
      const idx = chats.conversations.findIndex(c => c.id === incoming.id);
      chats.conversations[idx] = incoming;
      updated++;
    } else {
      chats.conversations.push(incoming);
      added++;
    }
  }
  saveChatsNow();
  log('info', 'chat', '-', 'import', added + ' added, ' + updated + ' updated');
  jsonResponse(res, 200, { ok: true, added, updated, total: chats.conversations.length });
}
function adminGetChat(res, id) { const c = chats.conversations.find(x => x.id === id); if (!c) return jsonResponse(res, 404, { error: { message: 'not found' } }); jsonResponse(res, 200, { conversation: c }); }
function adminDeleteChat(res, id) { const idx = chats.conversations.findIndex(x => x.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); chats.conversations.splice(idx, 1); saveChatsNow(); jsonResponse(res, 200, { ok: true }); }

function adminListPlugins(res) { jsonResponse(res, 200, { plugins: plugins.plugins.map(p => ({ id: p.id, name: p.name, enabled: p.enabled, source: p.source, toolsLoaded: (function(){ let n = 0; for (const [, t] of pluginTools) if (t.pluginId === p.id) n++; return n; })() })) }); }
async function adminAddPlugin(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } if (!body.name || !body.source) return jsonResponse(res, 400, { error: { message: 'name + source' } }); const pl = { id: genId('pl'), name: String(body.name).slice(0, 80), enabled: body.enabled !== false, source: String(body.source) }; plugins.plugins.push(pl); savePlugins(); loadPlugins(); jsonResponse(res, 200, { ok: true, plugin: pl }); }
async function adminPatchPlugin(req, res, id) { const p = plugins.plugins.find(x => x.id === id); if (!p) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['name','enabled','source']) if (f in body) p[f] = body[f]; savePlugins(); loadPlugins(); jsonResponse(res, 200, { ok: true, plugin: p }); }
function adminDeletePlugin(res, id) { const idx = plugins.plugins.findIndex(x => x.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); plugins.plugins.splice(idx, 1); savePlugins(); loadPlugins(); jsonResponse(res, 200, { ok: true }); }

function adminListMCP(res) { jsonResponse(res, 200, { servers: mcp.servers.map(s => ({ id: s.id, name: s.name, transport: s.transport, command: s.command, args: s.args, env: s.env, url: s.url, headers: s.headers, enabled: s.enabled, status: mcpClients.has(s.id) ? 'connected' : (s.status || 'disconnected'), tools: s.tools || [], lastError: s.lastError || null })) }); }
async function adminAddMCP(req, res) { let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } if (!body.name || !body.transport) return jsonResponse(res, 400, { error: { message: 'name + transport' } }); const s = { id: genId('mcp'), name: String(body.name).slice(0, 80), transport: body.transport, command: body.command || '', args: Array.isArray(body.args) ? body.args : [], env: body.env || {}, url: body.url || '', headers: body.headers || {}, enabled: body.enabled !== false, status: 'disconnected', tools: [], lastError: null }; mcp.servers.push(s); saveMCP(); if (s.enabled) { const r = await connectMCP(s); jsonResponse(res, 200, { ok: true, server: s, connect: r }); } else jsonResponse(res, 200, { ok: true, server: s }); }
async function adminPatchMCP(req, res, id) { const s = mcp.servers.find(x => x.id === id); if (!s) return jsonResponse(res, 404, { error: { message: 'not found' } }); let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); } for (const f of ['name','enabled','command','args','env','url','headers']) if (f in body) s[f] = body[f]; saveMCP(); if (s.enabled) await connectMCP(s); else if (mcpClients.has(s.id)) { try { mcpClients.get(s.id).close(); } catch {} mcpClients.delete(s.id); } jsonResponse(res, 200, { ok: true, server: s }); }
function adminDeleteMCP(res, id) { const idx = mcp.servers.findIndex(x => x.id === id); if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } }); if (mcpClients.has(id)) { try { mcpClients.get(id).close(); } catch {} mcpClients.delete(id); } for (const [name, t] of mcpTools) if (t.serverId === id) mcpTools.delete(name); mcp.servers.splice(idx, 1); saveMCP(); jsonResponse(res, 200, { ok: true }); }
async function adminConnectMCP(req, res, id) { const s = mcp.servers.find(x => x.id === id); if (!s) return jsonResponse(res, 404, { error: { message: 'not found' } }); const r = await connectMCP(s); jsonResponse(res, 200, { ok: r.ok, server: s, result: r }); }
function adminDisconnectMCP(res, id) { if (mcpClients.has(id)) { try { mcpClients.get(id).close(); } catch {} mcpClients.delete(id); } for (const [name, t] of mcpTools) if (t.serverId === id) mcpTools.delete(name); const s = mcp.servers.find(x => x.id === id); if (s) { s.status = 'disconnected'; s.tools = []; saveMCP(); } jsonResponse(res, 200, { ok: true }); }
function adminListTools(res) { jsonResponse(res, 200, { tools: listAllTools() }); }

// ─── PROBE ──────────────────────────────────────────────────────────────────
async function probeAccount(acc) {
  const prov = state.providers[acc.provider]; if (!prov) return { ok: false, error: 'provider missing' };
  let base = prov.base || '';
  if (prov.needsAccountId) { if (!acc.accountId) return { ok: false, error: 'missing accountId' }; base = base.replace('{account_id}', encodeURIComponent(acc.accountId)); }
  const url = base.replace(/\/+$/, '') + '/chat/completions';
  const candidates = [];
  if (acc.model) candidates.push(acc.model);
  if (prov.models && prov.models.fast) candidates.push(prov.models.fast);
  if (prov.models && prov.models.smart) candidates.push(prov.models.smart);
  if (acc.provider === 'groq') { candidates.push('llama-3.1-8b-instant'); candidates.push('llama-3.3-70b-versatile'); }
  if (acc.provider === 'openai') { candidates.push('gpt-4o-mini'); candidates.push('gpt-3.5-turbo'); }
  if (acc.provider === 'mistral') { candidates.push('mistral-small-latest'); }
  if (acc.provider === 'deepseek') { candidates.push('deepseek-chat'); }
  if (acc.provider === 'openrouter') { candidates.push('meta-llama/llama-3.1-8b-instruct:free'); }
  const seen = {}; const uniq = [];
  for (const m of candidates) if (m && !seen[m]) { seen[m] = 1; uniq.push(m); }
  let lastDetail = 'no model tried';
  for (const modelId of uniq) {
    try {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + acc.apiKey }, body: JSON.stringify({ model: modelId, messages: [{ role: 'user', content: 'hi' }], max_tokens: 5, stream: false }), signal: AbortSignal.timeout(20000) });
      const text = await r.text();
      const rh = r.headers.get('x-ratelimit-remaining-requests');
      if (rh !== null) { const rem = parseInt(rh, 10); if (!isNaN(rem)) acc.remaining = rem; }
      if (r.status >= 200 && r.status < 300) { acc.status = 'ready'; acc.resetAt = 0; acc.failCount = 0; acc.authFails = 0; acc.lastError = null; acc.lastProbe = Date.now(); savePool(); log('info', acc.provider, acc.label || acc.id, 'probe-ok', `model=${modelId}`); return { ok: true, status: r.status, model: modelId, body: text.slice(0, 200) }; }
      if (isQuota(r.status, r.headers, text.toLowerCase())) { cooldown(acc, prov.tz, 'probe quota', 'quota'); acc.lastError = 'quota: ' + text.slice(0, 180); acc.lastProbe = Date.now(); savePool(); return { ok: false, status: r.status, error: 'quota', body: text.slice(0, 300), lastError: acc.lastError }; }
      if (r.status === 401 || r.status === 403) {
        acc.authFails = (acc.authFails || 0) + 1; acc.failCount = (acc.failCount || 0) + 1;
        acc.lastError = `${r.status} auth: ${text.slice(0, 180)}`; acc.lastProbe = Date.now();
        if (acc.authFails >= 3) { acc.status = 'dead'; log('err', acc.provider, acc.label || acc.id, 'probe-dead', acc.lastError); } else log('warn', acc.provider, acc.label || acc.id, 'probe-auth-warn', `${r.status} (${acc.authFails}/3)`);
        savePool(); return { ok: false, status: r.status, error: 'auth', body: text.slice(0, 300), lastError: acc.lastError, authFails: acc.authFails };
      }
      const lower = text.toLowerCase();
      if (r.status === 404 || (lower.indexOf('model') >= 0 && (lower.indexOf('not found') >= 0 || lower.indexOf('not exist') >= 0 || lower.indexOf('invalid') >= 0 || lower.indexOf('unknown') >= 0 || lower.indexOf('does not exist') >= 0))) { lastDetail = 'model "' + modelId + '" not available: ' + text.slice(0, 140); continue; }
      acc.lastError = `${r.status}: ${text.slice(0, 200)}`; acc.lastProbe = Date.now(); savePool();
      log('warn', acc.provider, acc.label || acc.id, 'probe-fail', acc.lastError);
      return { ok: false, status: r.status, body: text.slice(0, 300), lastError: acc.lastError };
    } catch (e) { acc.lastError = 'network: ' + e.message; acc.lastProbe = Date.now(); savePool(); log('warn', acc.provider, acc.label || acc.id, 'probe-err', e.message); return { ok: false, error: e.message, lastError: acc.lastError }; }
  }
  acc.lastError = 'all candidate models rejected: ' + lastDetail; acc.lastProbe = Date.now(); savePool();
  return { ok: false, error: 'model mismatch', lastError: acc.lastError, detail: lastDetail };
}

// ─── MODELS CRUD ────────────────────────────────────────────────────────────
function adminListModels(res) {
  if (!Array.isArray(state.models)) state.models = [];
  jsonResponse(res, 200, { models: state.models });
}
async function adminAddModel(req, res) {
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  if (!body.provider || !body.modelId) return jsonResponse(res, 400, { error: { message: 'provider + modelId required' } });
  if (!state.providers[body.provider]) return jsonResponse(res, 400, { error: { message: 'unknown provider' } });
  const _labels = Array.isArray(body.labels) ? body.labels.map(function(x){ return String(x).slice(0, 40); }).filter(Boolean) : (body.label ? [String(body.label).slice(0, 40)] : ['general']); if (_labels.length === 0) _labels.push('general'); const model = { id: genId('mdl'), provider: body.provider, modelId: String(body.modelId).slice(0, 200), label: _labels[0], labels: _labels, input: Array.isArray(body.input) ? body.input : ['text'], output: Array.isArray(body.output) ? body.output : ['text'], priority: typeof body.priority === 'number' ? body.priority : 50, enabled: body.enabled !== false, contextLength: Number(body.contextLength) || null, note: String(body.note || '').slice(0, 200) };
  if (!Array.isArray(state.models)) state.models = [];
  state.models.push(model); savePoolNow();
  jsonResponse(res, 200, { ok: true, model });
}
async function adminPatchModel(req, res, id) {
  if (!Array.isArray(state.models)) state.models = [];
  const m = state.models.find(function(x){ return x.id === id; });
  if (!m) return jsonResponse(res, 404, { error: { message: 'not found' } });
  let body; try { body = await readJson(req); } catch (e) { return jsonResponse(res, 400, { error: { message: e.message } }); }
  for (const f of ['modelId','label','input','output','priority','enabled','contextLength','note']) if (f in body) m[f] = body[f]; if ('labels' in body && Array.isArray(body.labels)) { m.labels = body.labels.map(function(x){ return String(x).slice(0, 40); }).filter(Boolean); if (m.labels.length > 0) m.label = m.labels[0]; }
  savePoolNow();
  jsonResponse(res, 200, { ok: true, model: m });
}
function adminDeleteModel(res, id) {
  if (!Array.isArray(state.models)) state.models = [];
  const idx = state.models.findIndex(function(x){ return x.id === id; });
  if (idx < 0) return jsonResponse(res, 404, { error: { message: 'not found' } });
  state.models.splice(idx, 1); savePoolNow();
  jsonResponse(res, 200, { ok: true });
}
async function adminReseedModels(req, res) {
  state.models = [];
  for (const provName of Object.keys(state.providers)) {
    const prov = state.providers[provName];
    if (!prov || !prov.models) continue;
    for (const label of Object.keys(prov.models)) {
      const mid = prov.models[label];
      if (!mid) continue;
      state.models.push({ id: genId('mdl'), provider: provName, modelId: mid, label: label, labels: [label], input: label === 'vision' ? ['text', 'image'] : ['text'], output: ['text'], priority: label === 'smart' ? 100 : (label === 'fast' ? 80 : 70), enabled: true, contextLength: null, note: 'reseeded' });
    }
  }
  savePoolNow();
  jsonResponse(res, 200, { ok: true, models: state.models });
}

// ─── BOOT ───────────────────────────────────────────────────────────────────
loadAll();
migrateSettings();
migrateModels();
loadPlugins();
setTimeout(() => { connectAllMCP().catch(e => log('err', '-', '-', 'mcp-boot', e.message)); }, 100);

process.on('uncaughtException', (e) => log('err', '-', '-', 'uncaught', e.stack || e.message));
process.on('unhandledRejection', (e) => log('err', '-', '-', 'rejection', String(e)));

const server = http.createServer(handler);
server.listen(PORT, () => {
  const mem = process.memoryUsage();
  log('info', '-', '-', 'listen', 'http://localhost:' + PORT);
  console.log('\n  POOL · chat studio');
  console.log('  → http://localhost:' + PORT + '\n');
  console.log('  Node:    ' + process.version + ' (' + process.platform + '/' + process.arch + ')');
  console.log('  RSS:     ' + Math.round(mem.rss / 1048576) + ' MB');
  console.log('  pool:    ' + POOL_FILE);
  console.log('  agents:  ' + agents.agents.length + ' · skills: ' + agents.skills.length);
  console.log('  plugins: ' + plugins.plugins.length + ' · MCP: ' + mcp.servers.length);
  console.log('  tools:   ' + listAllTools().length + ' · fallback: ' + state.settings.fallbackMode);
  console.log('  auth:    ' + (state.settings.adminToken ? 'ENABLED' : 'OPEN (dev)') + '\n');
  if (process.arch === 'ia32' || process.arch === 'arm') console.log('  ⚠ 32-bit. Settings → Low-Spec (2GB) jika RAM ≤ 2GB.\n');
  console.log('  Ctrl+C untuk stop.\n');
});
function shutdown(sig) { log('info', '-', '-', 'shutdown', sig); try { savePoolNow(); saveChatsNow(); saveAgents(); savePlugins(); saveMCP(); } catch {} for (const [, c] of mcpClients) { try { c.close(); } catch {} } try { server.close(() => process.exit(0)); } catch { process.exit(0); } setTimeout(() => process.exit(0), 1500); }
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));