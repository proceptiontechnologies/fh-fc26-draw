// FH tournaments: draw + knockout bracket. One row per tournament in Postgres, live updates over SSE.
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;
const MASTER_KEY = process.env.ADMIN_KEY || ""; // creates tournaments, runs any of them
const SIZES = [4, 8, 16];

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const clients = new Map(); // slug -> Set of SSE responses

const fill = (a, n) => Array.from({length:n}, (_, i) => (typeof (a && a[i]) === "string" && a[i]) || "");
const roundCount = size => Math.log2(size);

// "qf2", "sf1", "final", "r16-3": what scores are keyed by, matching the client.
function matchKey(size, r, m){
  const left = size >> r;
  if (left === 2) return "final";
  const code = left === 4 ? "sf" : left === 8 ? "qf" : "r" + left + "-";
  return code + (m + 1);
}

// Always hand out the same shape, whatever is stored.
function normalize(d){
  d = d || {};
  const size = SIZES.includes(d.size) ? d.size : 8;
  const rounds = [];
  for (let r = 0; r < roundCount(size); r++) rounds.push(fill(d.rounds && d.rounds[r], size >> (r + 1)));
  const keys = new Set(rounds.flatMap((ms, r) => ms.map((_, m) => matchKey(size, r, m))));
  const scores = {};
  for (const k in d.scores || {}) if (keys.has(k) && typeof d.scores[k] === "string" && d.scores[k]) scores[k] = d.scores[k];
  return {
    size,
    players: (Array.isArray(d.players) ? d.players : []).filter(p => typeof p === "string" && p).slice(0, size),
    slots: fill(d.slots, size),
    rounds,
    scores
  };
}

async function init(){
  await pool.query(`CREATE TABLE IF NOT EXISTS tournaments (
    slug text PRIMARY KEY,
    name text NOT NULL,
    admin_key text NOT NULL,
    data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
  )`);
}

class Refused extends Error {}
class Missing extends Error {}

async function load(slug){
  const { rows } = await pool.query("SELECT slug, name, admin_key, data FROM tournaments WHERE slug = $1", [slug]);
  if (!rows[0]) throw new Missing("No such tournament");
  return rows[0];
}

const view = t => Object.assign({ slug: t.slug, name: t.name }, normalize(t.data));

// Lock the row, let fn change the state (or throw Refused), save, tell everyone watching.
async function mutate(slug, fn){
  const db = await pool.connect();
  let t, out;
  try {
    await db.query("BEGIN");
    const { rows } = await db.query("SELECT slug, name, admin_key, data FROM tournaments WHERE slug = $1 FOR UPDATE", [slug]);
    if (!rows[0]) throw new Missing("No such tournament");
    t = rows[0];
    const state = normalize(t.data);
    out = fn(state, t);
    t.data = normalize(state);
    await db.query("UPDATE tournaments SET data = $2, name = $3 WHERE slug = $1", [slug, t.data, t.name]);
    await db.query("COMMIT");
  } catch (e) {
    await db.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    db.release();
  }
  broadcast(slug, view(t));
  return out;
}

function broadcast(slug, state){
  const msg = "data: " + JSON.stringify(state) + "\n\n";
  for (const res of clients.get(slug) || []) res.write(msg);
}

function sameKey(given, want){
  const a = Buffer.from(String(given || "")), b = Buffer.from(String(want || ""));
  return b.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
}
const keyOf = req => req.headers["x-admin-key"];
const isMaster = req => sameKey(keyOf(req), MASTER_KEY);
const isOrganizer = (req, t) => isMaster(req) || sameKey(keyOf(req), t.admin_key);

function send(res, code, body){
  res.writeHead(code, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(body));
}

function body(req){
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", c => { raw += c; if (raw.length > 64e3) { reject(new Refused("Too large")); req.destroy(); } });
    req.on("end", () => { try { resolve(raw ? JSON.parse(raw) : {}); } catch { reject(new Refused("Bad JSON")); } });
  });
}

const str = (v, max) => typeof v === "string" ? v.trim().slice(0, max) : "";

function slugify(name){
  const base = name.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 32) || "tournament";
  return base + "-" + crypto.randomBytes(2).toString("hex");
}

// Champion, worked out the same way the bracket page does (byes go straight through).
function champion(s){
  const allDrawn = s.players.length > 0 && s.players.every(p => s.slots.includes(p));
  let sides = s.slots.map(v => v ? { name: v } : { name: "", bye: allDrawn });
  for (const picks of s.rounds){
    sides = picks.map((pick, m) => {
      const a = sides[m * 2], b = sides[m * 2 + 1];
      if (a.bye && b.bye) return { name: "", bye: true };
      if (a.bye) return b;
      if (b.bye) return a;
      return a.name && b.name && (pick === a.name || pick === b.name) ? { name: pick } : { name: "" };
    });
  }
  return sides[0].name;
}

// Routes inside one tournament: /api/t/<slug>/<action>
const actions = {
  "GET state": async (req, slug) => view(await load(slug)),

  "POST draw": async (req, slug) => {
    const name = str((await body(req)).name, 40);
    return mutate(slug, s => {
      if (!s.players.includes(name)) throw new Refused("Unknown player");
      if (s.slots.includes(name)) throw new Refused("Already drawn");
      const free = s.slots.map((v, i) => v ? -1 : i).filter(i => i >= 0);
      if (!free.length) throw new Refused("No spots left");
      const slot = free[crypto.randomInt(free.length)];
      s.slots[slot] = name;
      return { slot };
    });
  },

  "POST admin/check": async () => ({ ok: true }),

  "POST admin/player": async (req, slug) => {
    const name = str((await body(req)).name, 40);
    return mutate(slug, s => {
      if (!name || s.players.includes(name) || s.players.length >= s.size) throw new Refused("Can't add that player");
      s.players.push(name);
      return { ok: true };
    });
  },

  "POST admin/player/remove": async (req, slug) => {
    const name = str((await body(req)).name, 40);
    return mutate(slug, s => {
      if (s.slots.includes(name)) throw new Refused("That player is already drawn");
      s.players = s.players.filter(p => p !== name);
      return { ok: true };
    });
  },

  // Tapping the current winner again clears it; later results that depended on it are cleared too.
  "POST admin/winner": async (req, slug) => {
    const b = await body(req), name = str(b.name, 40), r = b.round | 0, m = b.match | 0;
    return mutate(slug, s => {
      if (!s.rounds[r] || m < 0 || m >= s.rounds[r].length) throw new Refused("Unknown match");
      s.rounds[r][m] = s.rounds[r][m] === name ? "" : name;
      for (let k = r + 1; k < s.rounds.length; k++) s.rounds[k][m >> (k - r)] = "";
      return { ok: true };
    });
  },

  "POST admin/score": async (req, slug) => {
    const b = await body(req), key = str(b.key, 12), score = str(b.score, 60);
    return mutate(slug, s => {
      if (score) s.scores[key] = score; else delete s.scores[key]; // unknown keys are dropped by normalize
      return { ok: true };
    });
  },

  "POST admin/reset": async (req, slug) =>
    mutate(slug, s => { s.slots = []; s.rounds = []; s.scores = {}; return { ok: true }; }),

  // Replace the whole state, used for migrating a tournament in.
  "PUT admin/state": async (req, slug) => {
    const next = normalize(await body(req));
    return mutate(slug, s => { Object.assign(s, next); return { ok: true }; });
  }
};

async function listTournaments(){
  const { rows } = await pool.query("SELECT slug, name, data, created_at FROM tournaments ORDER BY created_at DESC");
  return rows.map(t => {
    const s = normalize(t.data);
    return { slug: t.slug, name: t.name, size: s.size, players: s.players.length, champion: champion(s), created: t.created_at };
  });
}

async function createTournament(req){
  const b = await body(req);
  const name = str(b.name, 60);
  const size = SIZES.includes(b.size) ? b.size : 8;
  const players = [...new Set((Array.isArray(b.players) ? b.players : []).map(p => str(p, 40)).filter(Boolean))];
  if (!name) throw new Refused("Give the tournament a name");
  if (players.length > size) throw new Refused("More players than spots");
  const slug = str(b.slug, 40).toLowerCase().replace(/[^a-z0-9-]/g, "") || slugify(name);
  const adminKey = crypto.randomBytes(8).toString("hex");
  const { rowCount } = await pool.query(
    "INSERT INTO tournaments (slug, name, admin_key, data) VALUES ($1, $2, $3, $4) ON CONFLICT (slug) DO NOTHING",
    [slug, name, adminKey, normalize({ size, players })]);
  if (!rowCount) throw new Refused("That link is taken");
  return { slug, adminKey };
}

const TYPES = { ".html":"text/html; charset=utf-8", ".js":"text/javascript", ".css":"text/css", ".png":"image/png", ".svg":"image/svg+xml", ".ico":"image/x-icon" };
const PUBLIC = path.join(__dirname, "public");

function serveFile(res, rel){
  const file = path.join(PUBLIC, rel);
  if (!file.startsWith(PUBLIC + path.sep)) return send(res, 404, { error: "Not found" });
  fs.readFile(file, (err, buf) => {
    if (err) return send(res, 404, { error: "Not found" });
    res.writeHead(200, { "content-type": TYPES[path.extname(file)] || "application/octet-stream", "cache-control": "no-cache" });
    res.end(buf);
  });
}

http.createServer(async (req, res) => {
  const url = req.url.split("?")[0];
  try {
    if (url === "/health") return send(res, 200, { ok: true });

    if (url === "/api/tournaments"){
      if (req.method === "GET") return send(res, 200, await listTournaments());
      if (req.method === "POST"){
        if (!isMaster(req)) return send(res, 403, { error: "Master key needed" });
        return send(res, 200, await createTournament(req));
      }
    }
    if (url === "/api/master/check" && req.method === "POST")
      return isMaster(req) ? send(res, 200, { ok: true }) : send(res, 403, { error: "Master key needed" });

    const m = url.match(/^\/api\/t\/([a-z0-9-]+)\/(.+)$/);
    if (m){
      const [, slug, action] = m;
      if (action === "events" && req.method === "GET"){
        const state = view(await load(slug));
        res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store", connection: "keep-alive", "x-accel-buffering": "no" });
        res.write("data: " + JSON.stringify(state) + "\n\n");
        if (!clients.has(slug)) clients.set(slug, new Set());
        clients.get(slug).add(res);
        req.on("close", () => clients.get(slug).delete(res));
        return;
      }
      const fn = actions[req.method + " " + action];
      if (!fn) return send(res, 404, { error: "Not found" });
      if (action.startsWith("admin/") && !isOrganizer(req, await load(slug))) return send(res, 403, { error: "Organizer key needed" });
      return send(res, 200, await fn(req, slug));
    }
    if (url.startsWith("/api/")) return send(res, 404, { error: "Not found" });

    if (url === "/") return serveFile(res, "index.html");
    if (/^\/t\/[a-z0-9-]+\/?$/.test(url)) return serveFile(res, "tournament.html");
    serveFile(res, decodeURIComponent(url));
  } catch (e) {
    if (e instanceof Missing) return send(res, 404, { error: e.message });
    if (e instanceof Refused) return send(res, 409, { error: e.message });
    console.error(e);
    send(res, 500, { error: "Server error" });
  }
}).listen(PORT, async () => {
  await init();
  console.log("Listening on " + PORT);
});

// Keep SSE connections open through proxies.
setInterval(() => { for (const set of clients.values()) for (const res of set) res.write(": ping\n\n"); }, 25000);
