#!/usr/bin/env node
/* Bloxatron server: accounts, saves, live updates and game rooms. No dependencies, Node 18+.
 *   node server.js                (PORT=3000, data in ./bloxatron-data.json)
 * Put index.html next to this file and the same server also hosts the site.  */
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 3000, DATA = process.env.DATA_FILE || path.join(__dirname, 'bloxatron-data.json');
const ORIGIN = process.env.ALLOW_ORIGIN || '*';
const COLS = new Set(['users', 'games', 'groups', 'catalog', 'playing', 'ds', 'reports']);
const MAXBODY = 14 * 1024 * 1024;

/* ---------- storage ---------- */
let store = { seq: 1000, accounts: {}, sessions: {}, docs: {} };
try { const o = JSON.parse(fs.readFileSync(DATA, 'utf8')); store = Object.assign(store, o); } catch (e) { /* first run */ }
for (const c of COLS) store.docs[c] = store.docs[c] || {};
let saveT = null;
/* Optional cloud storage so nothing is wiped when a free host restarts: set SUPABASE_URL and SUPABASE_SERVICE_KEY (kept secret, only on this server). */
const SB = process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_KEY ? { url: process.env.SUPABASE_URL.replace(/\/+$/, '') + '/rest/v1/' + (process.env.SUPABASE_TABLE || 'bv_store'), key: process.env.SUPABASE_SERVICE_KEY } : null;
const sbH = SB ? { apikey: SB.key, Authorization: 'Bearer ' + SB.key, 'Content-Type': 'application/json' } : {};
const sbReq = async (u, o) => { const r = await fetch(u, Object.assign({}, o, { headers: Object.assign({}, sbH, (o && o.headers) || {}) })); if (!r.ok) throw new Error('cloud storage ' + r.status + ' ' + (await r.text()).slice(0, 160)); return r; };
const rowCache = new Map(); let sbBusy = false, sbDirty = false;
async function sbLoad() {
  store.accounts = {}; store.sessions = {}; store.sexp = {}; for (const c of COLS) store.docs[c] = {}; /* the cloud copy is the truth */
  for (let from = 0; ; from += 500) {
    const r = await (await sbReq(SB.url + '?select=id,data&order=id', { headers: { Range: from + '-' + (from + 499), 'Range-Unit': 'items' } })).json();
    for (const x of r) { if (x.id === 'meta') Object.assign(store, x.data); else { const i = x.id.indexOf('/'), c = x.id.slice(0, i), k = x.id.slice(i + 1); if (COLS.has(c)) store.docs[c][k] = x.data.v; } rowCache.set(x.id, JSON.stringify(x.data)); }
    if (r.length < 500) break;
  }
  for (const c of COLS) store.docs[c] = store.docs[c] || {};
}
async function sbFlush() {
  if (sbBusy) { sbDirty = true; return; } sbBusy = true;
  try { do { sbDirty = false; const rows = { meta: { seq: store.seq, accounts: store.accounts, sessions: store.sessions, sexp: store.sexp || {} } };
      for (const c of COLS) for (const [k, v] of Object.entries(store.docs[c])) rows[c + '/' + k] = { c, k, v };
      const up = []; for (const [id, d] of Object.entries(rows)) { const str = JSON.stringify(d); if (rowCache.get(id) !== str) up.push([id, d, str]); }
      for (let i = 0; i < up.length; i += 5) { const b = up.slice(i, i + 5); await sbReq(SB.url + '?on_conflict=id', { method: 'POST', headers: { Prefer: 'resolution=merge-duplicates,return=minimal' }, body: JSON.stringify(b.map(([id, d]) => ({ id, data: d }))) }); b.forEach(([id, , str]) => rowCache.set(id, str)); }
      for (const id of [...rowCache.keys()]) if (!(id in rows)) { await sbReq(SB.url + '?id=eq.' + encodeURIComponent(id), { method: 'DELETE' }); rowCache.delete(id); }
    } while (sbDirty); }
  catch (e) { console.error('cloud save failed:', e.message); setTimeout(() => { sbFlush(); }, 10000); }
  sbBusy = false;
}
function persist() { clearTimeout(saveT); saveT = setTimeout(() => { const tmp = DATA + '.tmp'; try { fs.writeFileSync(tmp, JSON.stringify(store)); fs.renameSync(tmp, DATA); } catch (e) { if (!SB) console.error('save failed', e.message); } if (SB) sbFlush(); }, 800); }
async function bye() { try { fs.writeFileSync(DATA, JSON.stringify(store)); } catch (e) {} if (SB) { clearTimeout(saveT); await sbFlush(); for (let n = 0; sbBusy && n < 250; n++) await new Promise((r) => setTimeout(r, 100)); } process.exit(0); }
process.on('SIGINT', bye); process.on('SIGTERM', bye);

/* ---------- helpers ---------- */
const lc = (s) => String(s).toLowerCase();
const rnd = (n) => crypto.randomBytes(n).toString('hex');
function hashPw(pw, salt) { return crypto.scryptSync(pw, salt, 32).toString('hex'); }
function send(res, code, obj) { const b = JSON.stringify(obj); res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(b); }
function body(req) { return new Promise((ok, no) => { let n = 0; const ch = []; req.on('data', (d) => { n += d.length; if (n > MAXBODY) { no(new Error('too big')); req.destroy(); } else ch.push(d); }); req.on('end', () => { try { ok(ch.length ? JSON.parse(Buffer.concat(ch).toString('utf8')) : {}); } catch (e) { no(e); } }); req.on('error', no); }); }
function who(req, url) { const a = req.headers.authorization, t = a && a.startsWith('Bearer ') ? a.slice(7) : url.searchParams.get('token'); const s = t && store.sessions[t] && (store.sexp[t] || 0) > Date.now() ? store.sessions[t] : null; return s && store.accounts[s] ? store.accounts[s].name : null; }
const attempts = new Map();
function tooMany(ip) { const now = Date.now(), a = (attempts.get(ip) || []).filter((t) => now - t < 60000); a.push(now); attempts.set(ip, a); return a.length > 12; }
/* per-address limits: bucket name -> [max, window ms]. Behind a host's proxy the real address is the first X-Forwarded-For entry. */
const LIM = { signup: [5, 3600000], write: [180, 60000], room: [1500, 60000] }, hits = new Map();
function limited(ip, kind) { const [mx, win] = LIM[kind], k = kind + '|' + ip, now = Date.now(), a = (hits.get(k) || []).filter((t) => now - t < win); a.push(now); hits.set(k, a); return a.length > mx; }
setInterval(() => { const now = Date.now(); for (const [k, a] of hits) if (!a.length || now - a[a.length - 1] > 3600000) hits.delete(k); for (const [k, a] of attempts) if (!a.length || now - a[a.length - 1] > 60000) attempts.delete(k); }, 60000);
const SESSION_MS = 30 * 86400000; store.sexp = store.sexp || {};
function newSession(user) { const t = rnd(24); store.sessions[t] = user; store.sexp[t] = Date.now() + SESSION_MS; return t; }
setInterval(() => { const now = Date.now(); for (const t of Object.keys(store.sessions)) if ((store.sexp[t] || 0) < now) { delete store.sessions[t]; delete store.sexp[t]; } }, 3600000);
function canSeeReport(name, r) { if (!name || !r) return false; const n = lc(name); if (lc(r.by || '') === n) return true; const gm = store.docs.games[String(r.game)]; if (!gm) return false; if (lc(gm.ownerName) === n) return true; if (gm.groupId) { const gr = store.docs.groups[gm.groupId]; const ro = gr && gr.roles && gr.roles.find((x) => x.name === (gr.members || {})[n]); return !!(ro && ro.rank >= 200); } return false; }
const b0sid = (u) => String(u.searchParams.get('sid') || '').replace(/[^A-Za-z0-9]/g, '').slice(0, 12);
const clean = (c, v) => { if (c === 'users' && v && typeof v === 'object') { const o = Object.assign({}, v); delete o.pass; delete o.salt; return o; } return v; };

/* ---------- live streams ---------- */
const streams = new Set();
function emit(ev) { const d = 'data: ' + JSON.stringify(ev) + '\n\n'; for (const r of streams) { if (ev.c === 'reports' && !(ev.op === 'set' && canSeeReport(r._me, ev.v))) continue; try { r.write(d); } catch (e) {} } }
const MAXP = 12; const rooms = new Map(); /* 'gameId~serverId' -> Map(key -> player) */
const serversOf = (g) => [...rooms.entries()].filter(([k]) => k.startsWith(g + '~') && !k.split('~')[1].startsWith('PV')).map(([k, R]) => ({ sid: k.split('~')[1], count: R.size, max: MAXP, players: [...R.values()].map((q) => q.name) }));
function roomEmit(g, ev, exceptKey) { const R = rooms.get(g); if (!R) return; const d = 'data: ' + JSON.stringify(ev) + '\n\n'; for (const [k, p] of R) if (k !== exceptKey) { try { p.res.write(d); } catch (e) {} } }
setInterval(() => { for (const [g, R] of rooms) for (const [k, p] of R) { if (Date.now() - p.t > 45000) { try { p.res.end(); } catch (e) {} } else { try { p.res.write(': ping\n\n'); } catch (e) {} } } for (const r of streams) { try { r.write(': ping\n\n'); } catch (e) {} } }, 15000);

/* ---------- routes ---------- */
async function api(req, res, url) {
  const p = url.pathname, m = req.method, ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (m !== 'GET' && !p.startsWith('/api/room/') && limited(ip, 'write')) return send(res, 429, { error: 'Slow down a little.' });
  if (p.startsWith('/api/room/') && m === 'POST' && limited(ip, 'room')) return send(res, 429, { error: 'Slow down a little.' });
  if (p === '/api/ping') return send(res, 200, { ok: true, name: 'Bloxatron', users: Object.keys(store.accounts).length });
  if (p === '/api/signup' && m === 'POST') {
    if (tooMany(ip) || limited(ip, 'signup')) return send(res, 429, { error: 'Too many tries. Wait a while.' });
    const b = await body(req), name = String(b.name || ''), pw = String(b.pass || '');
    if (!/^[A-Za-z0-9_]{3,20}$/.test(name)) return send(res, 400, { error: 'Usernames use 3 to 20 letters, numbers or underscores.' });
    if (pw.length < 6 || pw.length > 200) return send(res, 400, { error: 'Choose a password with at least 6 characters.' });
    if (store.accounts[lc(name)]) return send(res, 409, { error: 'That username is taken. Try another one.' });
    const salt = rnd(16); store.accounts[lc(name)] = { name, salt, hash: hashPw(pw, salt), created: Date.now() };
    const token = newSession(lc(name));
    const user = clean('users', b.user || {}); user.name = name; user.id = ++store.seq; store.docs.users[lc(name)] = user; persist();
    emit({ op: 'set', c: 'users', k: lc(name), v: user });
    return send(res, 200, { token, name, user });
  }
  if (p === '/api/login' && m === 'POST') {
    if (tooMany(ip)) return send(res, 429, { error: 'Too many tries. Wait a minute.' });
    const b = await body(req), a = store.accounts[lc(b.name || '')];
    if (!a || hashPw(String(b.pass || ''), a.salt) !== a.hash) return send(res, 401, { error: 'Incorrect username or password.' });
    const token = newSession(lc(a.name)); persist(); return send(res, 200, { token, name: a.name });
  }
  if (p === '/api/logout' && m === 'POST') { const a = req.headers.authorization; if (a) { delete store.sessions[a.slice(7)]; delete store.sexp[a.slice(7)]; } persist(); return send(res, 200, { ok: true }); }
  if (p === '/api/me') { const n = who(req, url); return n ? send(res, 200, { name: n }) : send(res, 401, { error: 'not signed in' }); }
  if (p === '/api/all') { const out = {}; const me0 = who(req, url); for (const c of COLS) out[c] = store.docs[c]; out.reports = {}; for (const [k, r] of Object.entries(store.docs.reports)) if (canSeeReport(me0, r)) out.reports[k] = r; return send(res, 200, { docs: out, seq: store.seq }); }
  if (p === '/api/ids') { const n = Math.min(200, Math.max(1, +url.searchParams.get('n') || 20)); const start = store.seq + 1; store.seq += n; persist(); return send(res, 200, { start, n }); }
  let mm = /^\/api\/doc\/(\w+)\/([^/]+)$/.exec(p);
  if (mm && (m === 'PUT' || m === 'DELETE')) {
    const me = who(req, url); if (!me) return send(res, 401, { error: 'Log in first.' });
    const c = mm[1], k = decodeURIComponent(mm[2]); if (!COLS.has(c) || k.length > 120) return send(res, 400, { error: 'bad doc' });
    if (c === 'users' && k === lc(me) === false && m === 'DELETE') return send(res, 403, { error: 'Not yours.' });
    /* ownership: only a game's creator (or an admin of the group that owns it) can change or delete it; a catalog item only its owner. Visitors may only bump the visit counter. */
    if (c === 'games' || c === 'catalog') {
      const ex = store.docs[c][k], mine = (d) => d && lc(c === 'games' ? d.ownerName : d.owner) === lc(me), grp = (d) => { const gr = c === 'games' && d && d.groupId ? store.docs.groups[d.groupId] : null; if (!gr || !gr.members || !gr.roles) return false; const r = gr.roles.find((x) => x.name === gr.members[lc(me)]); return !!r && r.rank >= 200; };
      const collab = (d) => c === 'games' && d && Array.isArray(d.collab) && d.collab.some((n) => lc(n) === lc(me));
      if (ex && m === 'DELETE' && collab(ex) && !mine(ex) && !grp(ex)) return send(res, 403, { error: 'Only the creator can delete this.' });
      if (ex && !mine(ex) && !grp(ex) && !collab(ex)) {
        if (m === 'DELETE') return send(res, 403, { error: 'Only the creator can delete this.' });
        const nv = clean(c, await body(req)), keep = c === 'games' ? ['visits', 'favA', 'favR'] : ['sales', 'sold', 'count']; const merged = Object.assign({}, ex);
        for (const f of keep) if (typeof nv[f] === 'number' && nv[f] >= (ex[f] || 0) && nv[f] - (ex[f] || 0) <= 5) merged[f] = nv[f];
        store.docs[c][k] = merged; emit({ op: 'set', c, k, v: merged, from: me }); persist(); return send(res, 200, { ok: true, limited: true });
      }
    }
    if (c === 'reports') {
      /* anyone signed in can file a report (under their own name); only the game's creator or a group admin of its group can mark one handled; nothing is deleted here */
      if (m === 'DELETE') return send(res, 403, { error: 'Reports are kept.' });
      const raw = await body(req), ex = store.docs.reports[k], gm = store.docs.games[String(ex ? ex.game : raw.game)]; const meL = lc(me);
      const canMod = gm && (lc(gm.ownerName) === meL || (gm.groupId && (() => { const gr = store.docs.groups[gm.groupId]; const r = gr && gr.roles && gr.roles.find((x) => x.name === (gr.members || {})[meL]); return r && r.rank >= 200; })()));
      if (!ex) { raw.by = store.accounts[meL].name; raw.done = false; raw.chat = (raw.chat || []).slice(0, 8); req._body = raw; }
      else if (canMod) req._body = Object.assign({}, ex, { done: !!raw.done });
      else return send(res, 403, { error: 'Only the creator can handle reports.' });
    }
    if (m === 'PUT' && (c === 'users' || c === 'groups')) {
      /* trust rules for profiles and groups: you may rewrite your own profile; for anyone else's you may only do what the site's social
         actions need (friend lists touching you, a notification, a Token payment into their balance). Group admins run their group;
         everyone else may only join or leave themselves, request to join, or add to the group's funds. */
      const raw = await body(req); req._body = raw; const meL = lc(me), ex = store.docs[c][k], arr = (a) => Array.isArray(a) ? a : [];
      const onlyMe = (o, n) => [...arr(o).filter((x) => !arr(n).includes(x)), ...arr(n).filter((x) => !arr(o).includes(x))].every((x) => lc(String(x)) === meL);
      if (c === 'users' && ex && k !== meL) {
        const out = Object.assign({}, ex);
        for (const f of ['friends', 'incoming', 'outgoing']) if (Array.isArray(raw[f]) && onlyMe(ex[f], raw[f])) out[f] = raw[f].slice(0, 2000);
        if (Array.isArray(raw.notes)) { const have = new Set(arr(ex.notes).map((x) => x.id)); out.notes = raw.notes.filter((x) => x && !have.has(x.id) && typeof x.text === 'string').slice(0, 20).map((x) => ({ id: x.id, t: x.t, text: String(x.text).slice(0, 300), link: x.link || null, read: false })).concat(arr(ex.notes)).slice(0, 100); }
        if (typeof raw.bux === 'number' && raw.bux > (ex.bux || 0) && isFinite(raw.bux)) out.bux = raw.bux;
        req._body = out;
      }
      if (c === 'groups' && ex && ex.members && ex.roles) {
        const rank = (n) => { const r = arr(ex.roles).find((x) => x.name === ex.members[lc(n)]); return r ? r.rank : 0; };
        if (rank(me) < 200) {
          const out = Object.assign({}, ex); const mem = Object.assign({}, ex.members);
          const wasIn = mem[meL], nowIn = raw.members && raw.members[meL];
          if (raw.members && !nowIn && wasIn && rank(me) < 255) delete mem[meL];
          else if (raw.members && nowIn && !wasIn && ex.approval !== 'request' && arr(ex.roles).some((r) => r.name === nowIn && r.rank <= 1)) mem[meL] = nowIn;
          out.members = mem;
          if (typeof raw.funds === 'number' && raw.funds > (ex.funds || 0) && isFinite(raw.funds)) out.funds = raw.funds;
          if (raw.requests && typeof raw.requests === 'object') { const rq = Object.assign({}, ex.requests || {}); if (raw.requests[meL] && ex.approval === 'request' && !wasIn) rq[meL] = raw.requests[meL]; else if (!raw.requests[meL]) delete rq[meL]; out.requests = rq; }
          req._body = out;
        } else { const out = Object.assign({}, raw); out.owner = ex.owner; out.ownerName = ex.ownerName; req._body = out; }  /* admins cannot take the group from its founder */
      } else if (c === 'groups' && !ex) { raw.owner = meL; raw.ownerName = store.accounts[meL].name; raw.members = { [meL]: 'Owner' }; raw.funds = 0; req._body = raw; }
    }
    if (m === 'PUT') { const v = clean(c, req._body !== undefined ? req._body : await body(req)); if (c === 'users' && k === lc(me)) { v.name = store.accounts[lc(me)].name; } if (c === 'users' && !store.docs.users[k]) return send(res, 403, { error: 'Accounts are made by signing up.' }); if (c === 'games' && store.docs.games[k] && !(lc(store.docs.games[k].ownerName) === lc(me))) { const ex0 = store.docs.games[k]; v.ownerName = ex0.ownerName; v.collab = ex0.collab; if (ex0.groupId !== undefined) v.groupId = ex0.groupId; }
    if (c === 'games' && !store.docs.games[k]) v.ownerName = store.accounts[lc(me)].name; if (c === 'catalog' && !store.docs.catalog[k]) v.owner = store.accounts[lc(me)].name; store.docs[c][k] = v; emit({ op: 'set', c, k, v, from: me }); }
    else { if (c === 'users') return send(res, 403, { error: 'Accounts cannot be deleted here.' }); delete store.docs[c][k]; emit({ op: 'del', c, k, from: me }); }
    persist(); return send(res, 200, { ok: true });
  }
  if (p === '/api/stream') {
    res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' }); res.write('retry: 3000\n\n'); res._me = who(req, url); streams.add(res); req.on('close', () => streams.delete(res)); return;
  }
  /* rooms */
  mm = /^\/api\/servers\/(\d+)$/.exec(p); if (mm) return send(res, 200, { servers: serversOf(mm[1]) });
  mm = /^\/api\/room\/((?:st)?\d+)\/(stream|state|chat|dmg|rep)$/.exec(p);
  if (mm) {
    const g0 = mm[1], act = mm[2]; let g = g0;
    if (act === 'stream') {
      const acc = who(req, url); let name = acc; const want = String(url.searchParams.get('name') || '');
      if (!name) { name = /^Guest\d{3,6}$/.test(want) ? want : 'Guest' + (1000 + Math.floor(Math.random() * 9000)); }
      if (/^st\d+$/.test(g0)) { const gm = store.docs.games[g0.slice(2)]; const okc = acc && gm && (lc(gm.ownerName) === lc(acc) || (Array.isArray(gm.collab) && gm.collab.some((n) => lc(n) === lc(acc)))); if (!okc) { res.writeHead(403, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ error: 'Only the creator and invited collaborators can join this editing session.' })); } }
      let sid = String(url.searchParams.get('sid') || 'auto').replace(/[^A-Za-z0-9]/g, '').slice(0, 12);
      const free = serversOf(g0).filter((x) => x.count < MAXP).sort((a, b) => b.count - a.count)[0];
      if (sid === 'auto' || sid === '') sid = free ? free.sid : rnd(3); else if (sid === 'new') sid = rnd(3);
      else if (rooms.has(g0 + '~' + sid) && rooms.get(g0 + '~' + sid).size >= MAXP) { res.writeHead(409, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ error: 'That server is full.' })); }
      g = g0 + '~' + sid; let R = rooms.get(g); if (!R) rooms.set(g, R = new Map());
      for (const [k, q] of R) if (lc(q.name) === lc(name)) { try { q.res.end(); } catch (e) {} R.delete(k); }
      const key = rnd(10), me = { name, res, t: Date.now(), s: null, acc: !!acc }; R.set(key, me);
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write('data: ' + JSON.stringify({ t: 'hello', key, name, sid, roster: [...R.values()].filter((q) => q !== me).map((q) => ({ name: q.name, s: q.s })) }) + '\n\n');
      roomEmit(g, { t: 'join', name }, key);
      req.on('close', () => { const R2 = rooms.get(g); if (R2 && R2.get(key) === me) { R2.delete(key); roomEmit(g, { t: 'leave', name }); if (!R2.size) rooms.delete(g); } });
      return;
    }
    g = g0 + '~' + String(b0sid(url) || '');
    const b = await body(req), R = rooms.get(g), me = R && R.get(String(b.key || '')); if (!me) return send(res, 403, { error: 'not in room' });
    me.t = Date.now();
    if (act === 'state') { const part = { pos: b.pos, yaw: b.yaw, pose: b.pose, dead: !!b.dead, hs: b.hs ? 1 : 0, pj: b.pj ? 1 : 0, tl: b.tl ? String(b.tl).slice(0, 40) : null, hp: Number(b.hp) || 0, mh: Number(b.mh) || 100, team: b.team && typeof b.team.n === 'string' ? { n: b.team.n.slice(0, 40), c: Array.isArray(b.team.c) ? b.team.c.slice(0, 3).map(Number) : [1, 1, 1] } : null }; if (b.av) part.av = b.av; me.s = Object.assign(me.s || {}, part); roomEmit(g, { t: 'state', name: me.name, s: part }, b.key); return send(res, 200, { ok: true }); }
    if (act === 'rep') { if (b.d && typeof b.d === 'object') roomEmit(g, { t: 'rep', name: me.name, d: b.d }, b.key); return send(res, 200, { ok: true }); }
    if (act === 'dmg') { const n = Math.max(0, Math.min(200, Number(b.n) || 0)); if (n && b.to) roomEmit(g, { t: 'dmg', to: String(b.to).slice(0, 20), from: me.name, n }, b.key); return send(res, 200, { ok: true }); }
    if (act === 'chat') { const text = String(b.text || '').slice(0, 200); if (text) roomEmit(g, { t: 'chat', name: me.name, text, team: b.team ? String(b.team).slice(0, 40) : null }, b.key); return send(res, 200, { ok: true }); }
  }
  return send(res, 404, { error: 'no such route' });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('Access-Control-Allow-Origin', ORIGIN);
  if (ORIGIN !== '*' && req.headers.origin && req.headers.origin !== ORIGIN && req.url.startsWith('/api/')) { res.writeHead(403, { 'Content-Type': 'application/json' }); return res.end('{"error":"This site is not allowed to use this server."}'); } res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type'); res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  try {
    if (url.pathname.startsWith('/api/')) return await api(req, res, url);
    const f = path.join(__dirname, url.pathname === '/' ? 'index.html' : path.basename(url.pathname));
    if (fs.existsSync(f) && fs.statSync(f).isFile() && /\.(html|js|css|png|jpg|gif|ico)$/.test(f)) { const ext = path.extname(f).slice(1); res.writeHead(200, { 'Content-Type': { html: 'text/html; charset=utf-8', js: 'text/javascript', css: 'text/css', png: 'image/png', jpg: 'image/jpeg', gif: 'image/gif', ico: 'image/x-icon' }[ext] }); return fs.createReadStream(f).pipe(res); }
    res.writeHead(404); res.end('Not found');
  } catch (e) { try { send(res, 400, { error: String(e.message || e) }); } catch (er) {} }
});
(async () => {
  if (SB) { try { await sbLoad(); console.log('Loaded ' + rowCache.size + ' records from cloud storage.'); } catch (e) { console.error('Could not read cloud storage, not starting (so nothing gets overwritten):', e.message); process.exit(1); } }
  server.listen(PORT, () => console.log('Bloxatron server on http://localhost:' + PORT + '  (data: ' + (SB ? 'cloud storage' : DATA) + ')'));
})();
