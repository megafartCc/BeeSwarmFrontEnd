// Simple Railway-ready backend for Bee Swarm stats.
// Endpoints:
//   GET  /api/stats?period=24h            -> { honey: [{t,v}], pollen: [{t,v}], backpack: [{t,v}], nectar: { Comforting: [{t,v}], ... } }
//   POST /api/ingest (body: {honey, pollen, backpack, nectar, at?}) with x-api-key header
//   Both endpoints require x-user-key to scope data per user.
// Auth:
//   Read (GET):   x-client-key must match CLIENT_KEY (if set) AND x-user-key present
//   Write (POST): x-api-key must match API_KEY (required) AND x-user-key present
//
// Persistence:
//   Uses MySQL when configured (recommended). Falls back to in-memory buckets if MySQL env is not set.

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "replace-this-api-key";          // used by your script to push samples
const CLIENT_KEY = process.env.CLIENT_KEY || "replace-this-client-key"; // used by frontend to read

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

// In-memory stores keyed by userKey:
// samples: { honey: [{t,v}], pollen: [{t,v}], backpack: [{t,v}], nectar: {Type:[{t,v}]}, currentHoney: 0 }
// controlStates: { state, at }
// controlCommands: [ {command, at} ]
const samples = {};
const controlStates = {};
const controlCommands = {};
const configs = {};
const publicIdToUserKey = {};
const NECTAR_TYPES = ["Comforting", "Motivating", "Satisfying", "Refreshing", "Invigorating"];
const BASE_METRICS = ["honey", "pollen", "backpack", "backpack_capacity"];
const nectarMetricForType = (type) => `nectar_${type.toLowerCase()}`;
const NECTAR_METRICS = NECTAR_TYPES.map((type) => nectarMetricForType(type));
const ALL_METRICS = [...BASE_METRICS, ...NECTAR_METRICS];
const memorySessions = {};
const nectarTypeFromMetric = (metric) => {
  if (!metric || !metric.startsWith("nectar_")) return null;
  const slug = metric.slice("nectar_".length);
  return NECTAR_TYPES.find((type) => type.toLowerCase() === slug) || null;
};
const createEmptyNectarBucket = () => {
  const obj = {};
  NECTAR_TYPES.forEach((type) => {
    obj[type] = [];
  });
  return obj;
};

const cachePublicMapping = (publicId, userKey, username) => {
  if (!publicId || !userKey) return;
  publicIdToUserKey[publicId] = { userKey, username: username || null };
};

const normalizePlayerId = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.max(0, Math.floor(value));
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed)) {
      return Math.max(0, Math.floor(parsed));
    }
  }
  return null;
};

const getSessionPublicId = (userKey, playerId) => {
  if (!userKey || !playerId) return null;
  return crypto.createHash("sha1").update(`${userKey}:${playerId}`).digest("hex").slice(0, 16);
};

const recordMemorySession = (userKey, playerId, username, lastSeen, currentHoney) => {
  if (!userKey || !playerId) return;
  if (!memorySessions[userKey]) memorySessions[userKey] = {};
  const publicId = getSessionPublicId(userKey, playerId);
  memorySessions[userKey][playerId] = {
    playerId,
    username: username || "Player",
    lastSeen,
    currentHoney: currentHoney || 0,
    publicId
  };
  cachePublicMapping(publicId, userKey, username);
};

async function recordDbSession(userKey, playerId, username, lastSeen, currentHoney) {
  if (!USE_DB || !dbPool || !userKey || !playerId) return;
  const publicId = getSessionPublicId(userKey, playerId);
  try {
    await dbPool.query(
      `
        INSERT INTO ${PLAYER_SESSIONS_TABLE} (session_public_id, user_key, player_id, username, last_seen, current_honey)
        VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          username = COALESCE(VALUES(username), username),
          last_seen = VALUES(last_seen),
          current_honey = COALESCE(VALUES(current_honey), current_honey)
      `,
      [publicId, userKey, playerId, username || null, lastSeen, typeof currentHoney === "number" ? currentHoney : null]
    );
    cachePublicMapping(publicId, userKey, username);
  } catch (err) {
    console.error("Failed to upsert player session:", err);
  }
}

// MySQL config (set these env vars on Railway to enable DB mode)
const MYSQL_HOST = process.env.MYSQL_HOST || process.env.MYSQLHOST;
const MYSQL_USER = process.env.MYSQL_USER || process.env.MYSQLUSER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || process.env.MYSQLPASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE || process.env.MYSQLDATABASE;
const USE_DB = !!(MYSQL_HOST && MYSQL_USER && MYSQL_PASSWORD && MYSQL_DATABASE);
const CONFIG_TABLE = "configs";
const ONLINE_TIMEOUT = 120; // seconds to consider player online
const PLAYER_SESSIONS_TABLE = "player_sessions";
let dbPool = null;

async function initDb() {
  if (!USE_DB) return;
  dbPool = mysql.createPool({
    host: MYSQL_HOST,
    user: MYSQL_USER,
    password: MYSQL_PASSWORD,
    database: MYSQL_DATABASE,
    connectionLimit: 5
  });
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS users (
      user_key VARCHAR(128) PRIMARY KEY,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
  `);
  const addColumn = async (sql) => {
    try { await dbPool.query(sql); } catch (e) { if (e && e.code !== "ER_DUP_FIELDNAME") throw e; }
  };
  await addColumn(`ALTER TABLE users ADD COLUMN total_honey BIGINT DEFAULT 0`);
  await addColumn(`ALTER TABLE users ADD COLUMN last_activity INT DEFAULT 0`);
  await addColumn(`ALTER TABLE users ADD COLUMN current_honey BIGINT DEFAULT 0`);
  await addColumn(`ALTER TABLE users ADD COLUMN username VARCHAR(64) DEFAULT NULL`);
  await addColumn(`ALTER TABLE users ADD COLUMN public_id VARCHAR(32) UNIQUE`);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS samples (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      metric ENUM(${ALL_METRICS.map((m) => `'${m}'`).join(",")}) NOT NULL,
      t INT NOT NULL,
      v DOUBLE NOT NULL,
      INDEX idx_user_time (user_key, t),
      FOREIGN KEY (user_key) REFERENCES users(user_key) ON DELETE CASCADE
    ) ENGINE=InnoDB;
  `);
  // Ensure ENUM includes 'backpack' even if table already existed
  try {
    await dbPool.query(`ALTER TABLE samples MODIFY COLUMN metric ENUM(${ALL_METRICS.map((m) => `'${m}'`).join(",")}) NOT NULL`);
  } catch (e) {
    // ignore; column may already be in desired shape
  }
  // tokens/buffs tables omitted (feature removed)
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS ${CONFIG_TABLE} (
      config_key VARCHAR(64) PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      payload JSON NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS ${PLAYER_SESSIONS_TABLE} (
      session_public_id VARCHAR(32) PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      player_id BIGINT NOT NULL,
      username VARCHAR(64),
      last_seen INT DEFAULT 0,
      current_honey BIGINT DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uniq_user_player (user_key, player_id),
      FOREIGN KEY (user_key) REFERENCES users(user_key) ON DELETE CASCADE
    ) ENGINE=InnoDB;
  `);
}

// Helpers
const nowSec = () => Math.floor(Date.now() / 1000);
const toSeconds = (period) => {
  if (!period) return 86400; // default 24h
  const m = /^(\d+)([smhd])$/.exec(period);
  if (!m) return 86400;
  const n = Number(m[1]);
  const unit = m[2];
  switch (unit) {
    case "s": return Math.min(n, 86400 * 30);
    case "m": return Math.min(n * 60, 86400 * 30);
    case "h": return Math.min(n * 3600, 86400 * 30);
    case "d": return Math.min(n * 86400, 86400 * 30);
    default: return 86400;
  }
};

const requireReadKey = (req, res, next) => {
  const userKey = req.header("x-user-key");
  if (!userKey) return res.status(400).json({ error: "x-user-key required" });
  if (!CLIENT_KEY || CLIENT_KEY === "replace-this-client-key") {
    req.userKey = userKey;
    return next(); // not enforced if left default
  }
  const key = req.header("x-client-key");
  if (key !== CLIENT_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  req.userKey = userKey;
  next();
};

const requireWriteKey = (req, res, next) => {
  const userKey = req.header("x-user-key");
  if (!userKey) return res.status(400).json({ error: "x-user-key required" });
  const key = req.header("x-api-key");
  if (key !== API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  req.userKey = userKey;
  next();
};

const requireConfigReadKey = (req, res, next) => {
  if (!CLIENT_KEY || CLIENT_KEY === "replace-this-client-key") {
    return next();
  }
  const key = req.header("x-client-key") || req.header("x-api-key");
  if (key !== CLIENT_KEY && key !== API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
};

const requireViewerKey = (req, res, next) => {
  if (!CLIENT_KEY || CLIENT_KEY === "replace-this-client-key") {
    return res.status(400).json({ error: "viewer disabled" });
  }
  const key = req.header("x-client-key");
  if (key !== CLIENT_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
};

const CONFIG_JSON_LIMIT = 256 * 1024; // bytes
const configKeyRegex = /^[A-Z0-9\[\]-]{10,32}$/;
const KEY_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]";
function generateConfigKey(len = 20) {
  let out = "";
  for (let i = 0; i < len; i++) {
    const idx = crypto.randomInt(0, KEY_ALPHABET.length);
    out += KEY_ALPHABET[idx];
  }
  return out;
}

const sanitizeUsername = (name) => {
  if (typeof name !== "string") return null;
  return name.trim().slice(0, 32) || null;
};

const clampPercent = (value) => {
  if (!isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
};

const getPublicId = (userKey) => {
  return crypto.createHash("sha1").update(String(userKey)).digest("hex").slice(0, 12);
};

function getBucket(userKey) {
  if (!samples[userKey]) {
    samples[userKey] = {
      honey: [],
      pollen: [],
      backpack: [],
      nectar: createEmptyNectarBucket(),
      currentHoney: 0,
      lastCapacity: 0,
      username: null,
      publicId: getPublicId(userKey),
      lastSeen: 0
    };
  }
  if (!samples[userKey].nectar) {
    samples[userKey].nectar = createEmptyNectarBucket();
  }
  if (!samples[userKey].publicId) {
    samples[userKey].publicId = getPublicId(userKey);
  }
  cachePublicMapping(samples[userKey].publicId, userKey, samples[userKey].username);
  return samples[userKey];
}

const shapeBackpackEntries = (entries) => {
  return entries.map((entry) => {
    const pct =
      typeof entry.pct === "number"
        ? clampPercent(entry.pct)
        : entry.cap && entry.cap > 0
          ? clampPercent((entry.v / entry.cap) * 100)
          : 0;
    return { t: entry.t, v: entry.v, pct };
  });
};

const mergeBackpackSeries = (backpackEntries, capacityEntries) => {
  const capacityByTime = new Map();
  let lastCap = 0;
  capacityEntries.forEach((entry) => {
    capacityByTime.set(entry.t, entry.v);
  });
  return backpackEntries.map((entry) => {
    const cap = capacityByTime.get(entry.t) ?? lastCap;
    if (cap && cap > 0) {
      lastCap = cap;
    }
    const pct = cap && cap > 0 ? clampPercent((entry.v / cap) * 100) : 0;
    return { t: entry.t, v: entry.v, pct };
  });
};

function collectMemoryStats(userKey, cutoff) {
  const bucket = getBucket(userKey);
  cachePublicMapping(bucket.publicId, userKey, bucket.username);
  const honey = bucket.honey.filter((p) => p.t >= cutoff);
  const pollen = bucket.pollen.filter((p) => p.t >= cutoff);
  const backpack = shapeBackpackEntries(bucket.backpack.filter((p) => p.t >= cutoff));
  const nectar = {};
  NECTAR_TYPES.forEach((type) => {
    nectar[type] = (bucket.nectar[type] || []).filter((p) => p.t >= cutoff);
  });
  return {
    honey,
    pollen,
    backpack,
    nectar,
    currentHoney: bucket.currentHoney || 0,
    player: {
      username: bucket.username || "Player",
      id: bucket.publicId
    }
  };
}

async function collectDbStats(userKey, cutoff) {
  const [rows] = await dbPool.query(
    "SELECT metric, t, v FROM samples WHERE user_key = ? AND t >= ? ORDER BY t ASC",
    [userKey, cutoff]
  );
  const honey = [];
  const pollen = [];
  const backpackRaw = [];
  const capacityEntries = [];
  const nectar = {};
  NECTAR_TYPES.forEach((type) => {
    nectar[type] = [];
  });
  for (const row of rows) {
    if (row.metric === "honey") honey.push({ t: row.t, v: row.v });
    else if (row.metric === "pollen") pollen.push({ t: row.t, v: row.v });
    else if (row.metric === "backpack") backpackRaw.push({ t: row.t, v: row.v });
    else if (row.metric === "backpack_capacity") capacityEntries.push({ t: row.t, v: row.v });
    else {
      const nectarType = nectarTypeFromMetric(row.metric);
      if (nectarType) {
        nectar[nectarType].push({ t: row.t, v: row.v });
      }
    }
  }
  const backpack = mergeBackpackSeries(backpackRaw, capacityEntries);
  const [userRows] = await dbPool.query(
    "SELECT current_honey, username, public_id FROM users WHERE user_key = ? LIMIT 1",
    [userKey]
  );
  const info = userRows && userRows[0] ? userRows[0] : {};
  const resolvedPublicId = info.public_id || getPublicId(userKey);
  cachePublicMapping(resolvedPublicId, userKey, info.username);
  return {
    honey,
    pollen,
    backpack,
    nectar,
    currentHoney: info.current_honey || 0,
    player: {
      username: info.username || "Player",
      id: resolvedPublicId
    }
  };
}

async function sendStatsResponse(userKey, periodSec, res, overrides) {
  const cutoff = nowSec() - periodSec;
  if (!USE_DB) {
    const data = collectMemoryStats(userKey, cutoff);
    if (overrides) {
      if (overrides.username) data.player.username = overrides.username;
      if (overrides.publicId) data.player.id = overrides.publicId;
    }
    return res.json(data);
  }
  try {
    const data = await collectDbStats(userKey, cutoff);
    if (overrides) {
      if (overrides.username) data.player.username = overrides.username;
      if (overrides.publicId) data.player.id = overrides.publicId;
    }
    res.json(data);
  } catch (err) {
    console.error(err);
    const fallback = collectMemoryStats(userKey, cutoff);
    if (overrides) {
      if (overrides.username) fallback.player.username = overrides.username;
      if (overrides.publicId) fallback.player.id = overrides.publicId;
    }
    res.json(fallback);
  }
}

async function resolveUserKeyFromPublicId(publicId) {
  if (!publicId) return null;
  const cached = publicIdToUserKey[publicId];
  if (cached) {
    return { userKey: cached.userKey, username: cached.username || null, publicId };
  }
  if (!USE_DB) {
    for (const [userKey, bucket] of Object.entries(samples)) {
      if (bucket && bucket.publicId === publicId) {
        cachePublicMapping(publicId, userKey, bucket.username);
        return { userKey, username: bucket.username || null, publicId };
      }
    }
    for (const [userKey, sessions] of Object.entries(memorySessions)) {
      for (const session of Object.values(sessions)) {
        if (session && session.publicId === publicId) {
          cachePublicMapping(publicId, userKey, session.username);
          return { userKey, username: session.username || null, publicId };
        }
      }
    }
    return null;
  }
  const [rows] = await dbPool.query(
    "SELECT user_key, username FROM users WHERE public_id = ? LIMIT 1",
    [publicId]
  );
  if (rows && rows[0]) {
    cachePublicMapping(publicId, rows[0].user_key, rows[0].username);
    return { userKey: rows[0].user_key, username: rows[0].username || null, publicId };
  }
  const [sessionRows] = await dbPool.query(
    `SELECT user_key, username FROM ${PLAYER_SESSIONS_TABLE} WHERE session_public_id = ? LIMIT 1`,
    [publicId]
  );
  if (sessionRows && sessionRows[0]) {
    cachePublicMapping(publicId, sessionRows[0].user_key, sessionRows[0].username);
    return { userKey: sessionRows[0].user_key, username: sessionRows[0].username || null, publicId };
  }
  return null;
}

async function ensureUser(userKey) {
  if (!USE_DB) return;
  const publicId = getPublicId(userKey);
  await dbPool.query(
    "INSERT INTO users (user_key, public_id) VALUES (?, ?) ON DUPLICATE KEY UPDATE public_id = COALESCE(public_id, VALUES(public_id))",
    [userKey, publicId]
  );
}

// GET stats
app.get("/api/stats", requireReadKey, (req, res) => {
  const periodSec = toSeconds(req.query.period || "24h");
  sendStatsResponse(req.userKey, periodSec, res);
});

app.get("/api/players", requireViewerKey, async (_req, res) => {
  const now = nowSec();
  const list = [];
  if (!USE_DB) {
    const seenKeys = new Set();
    Object.entries(memorySessions).forEach(([userKey, sessions]) => {
      Object.values(sessions).forEach((session) => {
        if (!session || !session.lastSeen) return;
        if (now - session.lastSeen > ONLINE_TIMEOUT) return;
        list.push({
          id: session.publicId,
          username: session.username || "Player",
          lastSeen: session.lastSeen,
          currentHoney: session.currentHoney || 0
        });
        seenKeys.add(userKey);
      });
    });
    Object.entries(samples).forEach(([userKey, bucket]) => {
      if (seenKeys.has(userKey)) return;
      if (!bucket || !bucket.lastSeen) return;
      if (now - bucket.lastSeen > ONLINE_TIMEOUT) return;
      list.push({
        id: bucket.publicId,
        username: bucket.username || "Player",
        lastSeen: bucket.lastSeen,
        currentHoney: bucket.currentHoney || 0
      });
    });
    list.sort((a, b) => {
      const nameA = (a.username || "").toLowerCase();
      const nameB = (b.username || "").toLowerCase();
      return nameA.localeCompare(nameB);
    });
    return res.json({ players: list });
  }
  try {
    const cutoff = now - ONLINE_TIMEOUT;
    const seenKeys = new Set();
    const [sessionRows] = await dbPool.query(
      `
        SELECT session_public_id, user_key, username, current_honey, last_seen
        FROM ${PLAYER_SESSIONS_TABLE}
        WHERE last_seen >= ?
      `,
      [cutoff]
    );
    sessionRows.forEach((row) => {
      if (!row) return;
      cachePublicMapping(row.session_public_id, row.user_key, row.username);
      seenKeys.add(row.user_key);
      list.push({
        id: row.session_public_id,
        username: row.username || "Player",
        lastSeen: row.last_seen || now,
        currentHoney: row.current_honey || 0
      });
    });
    const [rows] = await dbPool.query(
      "SELECT user_key, username, current_honey, public_id, last_activity FROM users WHERE last_activity >= ? ORDER BY username ASC",
      [cutoff]
    );
    rows.forEach((row) => {
      if (!row.public_id) return;
      if (seenKeys.has(row.user_key)) return;
      cachePublicMapping(row.public_id, row.user_key, row.username);
      list.push({
        id: row.public_id,
        username: row.username || "Player",
        lastSeen: row.last_activity || now,
        currentHoney: row.current_honey || 0
      });
    });
    list.sort((a, b) => {
      const nameA = (a.username || "").toLowerCase();
      const nameB = (b.username || "").toLowerCase();
      return nameA.localeCompare(nameB);
    });
    res.json({ players: list });
  } catch (err) {
    console.error(err);
    res.json({ players: [] });
  }
});

app.get("/api/player/:publicId/stats", requireViewerKey, async (req, res) => {
  const resolved = await resolveUserKeyFromPublicId(req.params.publicId);
  if (!resolved) {
    return res.status(404).json({ error: "not found" });
  }
  const periodSec = toSeconds(req.query.period || "24h");
  sendStatsResponse(resolved.userKey, periodSec, res, {
    username: resolved.username || null,
    publicId: resolved.publicId || req.params.publicId
  });
});

// Config sharing (in-memory)
app.post("/api/configs", requireWriteKey, (req, res) => {
  const config = req.body && req.body.config;
  if (!config || typeof config !== "object" || Array.isArray(config)) {
    return res.status(400).json({ error: "config object required" });
  }
  let json;
  try {
    json = JSON.stringify(config);
  } catch (e) {
    return res.status(400).json({ error: "config not serializable" });
  }
  if (!json || json.length > CONFIG_JSON_LIMIT) {
    return res.status(413).json({ error: "config too large" });
  }
  let key = req.body && req.body.key;
  if (typeof key !== "string" || !configKeyRegex.test(key)) {
    key = generateConfigKey();
  }
  const entry = {
    config,
    owner: req.userKey,
    at: nowSec(),
  };
  configs[key] = entry;
  if (!USE_DB) {
    return res.json({ ok: true, key, mode: "memory" });
  }
  (async () => {
    try {
      await dbPool.query(
        `REPLACE INTO ${CONFIG_TABLE} (config_key, user_key, payload, created_at) VALUES (?, ?, ?, FROM_UNIXTIME(?))`,
        [key, req.userKey, JSON.stringify(config), entry.at]
      );
      res.json({ ok: true, key, mode: "mysql" });
    } catch (e) {
      console.error("Failed to write config to DB:", e);
      res.json({ ok: true, key, mode: "memory-fallback" });
    }
  })();
});

app.get("/api/configs/:key", requireConfigReadKey, (req, res) => {
  const key = req.params.key;
  const entry = key && configs[key];
  const respond = (record) => {
    if (!record) {
      return res.status(404).json({ error: "not found" });
    }
    res.json({
      config: record.config,
      owner: record.owner || null,
      at: record.at || nowSec(),
    });
  };
  if (!USE_DB) {
    return respond(entry);
  }
  (async () => {
    try {
      const [rows] = await dbPool.query(
        `SELECT config_key, user_key, payload, UNIX_TIMESTAMP(created_at) AS at FROM ${CONFIG_TABLE} WHERE config_key = ? LIMIT 1`,
        [key]
      );
      if (rows && rows[0]) {
        const row = rows[0];
        let parsed = {};
        try { parsed = JSON.parse(row.payload); } catch (e) { parsed = {}; }
        respond({ config: parsed, owner: row.user_key, at: row.at || nowSec() });
      } else {
        respond(entry);
      }
    } catch (e) {
      console.error("Failed to fetch config from DB:", e);
      respond(entry);
    }
  })();
});

// POST ingest
app.post("/api/ingest", requireWriteKey, (req, res) => {
  const {
    honey,
    pollen,
    backpack,
    backpackCapacity,
    nectar,
    at,
    currentHoney,
    username,
    playerId
  } = req.body || {};
  const t = typeof at === "number" ? Math.floor(at) : nowSec();
  const cleanedName = sanitizeUsername(username);
  const numericPlayerId = normalizePlayerId(playerId);
  const hasNectar =
    nectar &&
    typeof nectar === "object" &&
    NECTAR_TYPES.some((type) => typeof nectar[type] === "number" && isFinite(nectar[type]));

  if (
    typeof honey !== "number" &&
    typeof pollen !== "number" &&
    typeof backpack !== "number" &&
    typeof currentHoney !== "number" &&
    !hasNectar
  ) {
    return res.status(400).json({ error: "no metrics provided" });
  }

  const writeMemory = () => {
    const bucket = getBucket(req.userKey);
    bucket.lastSeen = t;
    if (cleanedName) {
      bucket.username = cleanedName;
    }
    cachePublicMapping(bucket.publicId, req.userKey, bucket.username);
    if (typeof honey === "number" && isFinite(honey)) {
      bucket.honey.push({ t, v: honey });
    }
    if (typeof pollen === "number" && isFinite(pollen)) {
      bucket.pollen.push({ t, v: pollen });
    }
    if (typeof backpack === "number" && isFinite(backpack)) {
      const entry = { t, v: backpack };
      let capValue = bucket.lastCapacity || 0;
      if (typeof backpackCapacity === "number" && isFinite(backpackCapacity)) {
        capValue = backpackCapacity;
        bucket.lastCapacity = backpackCapacity;
      }
      if (capValue > 0) {
        entry.cap = capValue;
        entry.pct = clampPercent((backpack / capValue) * 100);
      } else {
        entry.cap = null;
        entry.pct = 0;
      }
      bucket.backpack.push(entry);
    }
    if (typeof backpackCapacity === "number" && isFinite(backpackCapacity)) {
      bucket.lastCapacity = backpackCapacity;
    }
    if (typeof currentHoney === "number") {
      bucket.currentHoney = currentHoney;
    }
    const sessionHoney = typeof currentHoney === "number" ? currentHoney : bucket.currentHoney || 0;
    if (hasNectar) {
      NECTAR_TYPES.forEach((type) => {
        const value = nectar[type];
        if (typeof value === "number" && isFinite(value)) {
          bucket.nectar[type].push({ t, v: value });
        }
      });
    }
    const cutoff = nowSec() - 86400 * 30;
    bucket.honey = bucket.honey.filter((p) => p.t >= cutoff);
    bucket.pollen = bucket.pollen.filter((p) => p.t >= cutoff);
    bucket.backpack = bucket.backpack.filter((p) => p.t >= cutoff);
    NECTAR_TYPES.forEach((type) => {
      bucket.nectar[type] = bucket.nectar[type].filter((p) => p.t >= cutoff);
    });
    if (numericPlayerId) {
      recordMemorySession(
        req.userKey,
        numericPlayerId,
        bucket.username || cleanedName || "Player",
        t,
        sessionHoney
      );
    }
  };

  if (!USE_DB) {
    writeMemory();
    return res.json({ ok: true, mode: "memory" });
  }

  (async () => {
    try {
      await ensureUser(req.userKey);
      cachePublicMapping(getPublicId(req.userKey), req.userKey, cleanedName);
      const inserts = [];
      if (typeof honey === "number" && isFinite(honey)) {
        inserts.push(["honey", t, honey]);
      }
      if (typeof pollen === "number" && isFinite(pollen)) {
        inserts.push(["pollen", t, pollen]);
      }
      if (typeof backpack === "number" && isFinite(backpack)) {
        inserts.push(["backpack", t, backpack]);
      }
      if (typeof backpackCapacity === "number" && isFinite(backpackCapacity)) {
        inserts.push(["backpack_capacity", t, backpackCapacity]);
      }
      if (hasNectar) {
        NECTAR_TYPES.forEach((type) => {
          const value = nectar[type];
          if (typeof value === "number" && isFinite(value)) {
            inserts.push([nectarMetricForType(type), t, value]);
          }
        });
      }
      if (inserts.length) {
        await dbPool.query(
          "INSERT INTO samples (user_key, metric, t, v) VALUES ?",
          [inserts.map(([metric, tt, vv]) => [req.userKey, metric, tt, vv])]
        );
      }

      await dbPool.query(
        "UPDATE users SET current_honey = COALESCE(?, current_honey), last_activity = ?, username = COALESCE(?, username) WHERE user_key = ?",
        [
          typeof currentHoney === "number" ? currentHoney : null,
          t,
          cleanedName,
          req.userKey
        ]
      );
      if (numericPlayerId) {
        await recordDbSession(
          req.userKey,
          numericPlayerId,
          cleanedName || null,
          t,
          typeof currentHoney === "number" ? currentHoney : null
        );
      }

      res.json({ ok: true, mode: "mysql" });
    } catch (err) {
      console.error(err);
      writeMemory();
      res.json({ ok: true, mode: "memory-fallback" });
    }
  })();
});

// Control sync endpoints (in-memory)
app.post("/api/controls/state", requireWriteKey, (req, res) => {
  const state = req.body && req.body.state;
  const at = req.body && req.body.at;
  if (!state) {
    return res.status(400).json({ error: "state required" });
  }
  controlStates[req.userKey] = { state, at: typeof at === "number" ? at : nowSec() };
  res.json({ ok: true });
});

app.get("/api/controls/state", requireReadKey, (req, res) => {
  const entry = controlStates[req.userKey];
  if (entry) {
    res.json({ state: entry.state, at: entry.at || nowSec() });
  } else {
    res.json({ state: null, at: nowSec() });
  }
});

app.post("/api/controls/commands", requireReadKey, (req, res) => {
  const cmds = Array.isArray(req.body && req.body.commands) ? req.body.commands : null;
  if (!cmds || !cmds.length) {
    return res.status(400).json({ error: "commands array required" });
  }
  if (!controlCommands[req.userKey]) controlCommands[req.userKey] = [];
  const stamped = cmds.map((c) => ({ ...c, at: nowSec() }));
  controlCommands[req.userKey].push(...stamped);
  // trim to last 100
  if (controlCommands[req.userKey].length > 100) {
    controlCommands[req.userKey] = controlCommands[req.userKey].slice(-100);
  }
  res.json({ ok: true, queued: controlCommands[req.userKey].length });
});

app.get("/api/controls/commands", requireWriteKey, (req, res) => {
  const list = controlCommands[req.userKey] || [];
  controlCommands[req.userKey] = [];
  res.json({ commands: list });
});

(async () => {
  try {
    await initDb();
  } catch (err) {
    console.error("DB init failed, falling back to memory:", err.message);
  }
  app.get("/health", (_req, res) => res.json({ ok: true, mode: USE_DB ? "mysql" : "memory" }));
  app.listen(PORT, () => {
    console.log(`Bee stats backend listening on :${PORT} (${USE_DB ? "mysql" : "memory"})`);
  });
})();
