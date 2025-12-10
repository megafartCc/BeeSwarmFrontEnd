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
const NECTAR_TYPES = ["Comforting", "Motivating", "Satisfying", "Refreshing", "Invigorating"];
const BASE_METRICS = ["honey", "pollen", "backpack"];
const nectarMetricForType = (type) => `nectar_${type.toLowerCase()}`;
const NECTAR_METRICS = NECTAR_TYPES.map((type) => nectarMetricForType(type));
const ALL_METRICS = [...BASE_METRICS, ...NECTAR_METRICS];
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

// MySQL config (set these env vars on Railway to enable DB mode)
const MYSQL_HOST = process.env.MYSQL_HOST || process.env.MYSQLHOST;
const MYSQL_USER = process.env.MYSQL_USER || process.env.MYSQLUSER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD || process.env.MYSQLPASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE || process.env.MYSQLDATABASE;
const USE_DB = !!(MYSQL_HOST && MYSQL_USER && MYSQL_PASSWORD && MYSQL_DATABASE);
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

function getBucket(userKey) {
  if (!samples[userKey]) {
    samples[userKey] = {
      honey: [],
      pollen: [],
      backpack: [],
      nectar: createEmptyNectarBucket(),
      currentHoney: 0
    };
  }
  if (!samples[userKey].nectar) {
    samples[userKey].nectar = createEmptyNectarBucket();
  }
  return samples[userKey];
}

async function ensureUser(userKey) {
  if (!USE_DB) return;
  await dbPool.query("INSERT IGNORE INTO users (user_key) VALUES (?)", [userKey]);
}

// GET stats
app.get("/api/stats", requireReadKey, (req, res) => {
  const periodSec = toSeconds(req.query.period || "24h");
  const cutoff = nowSec() - periodSec;

  const respondFromMemory = () => {
    const bucket = getBucket(req.userKey);
    const honey = bucket.honey.filter((p) => p.t >= cutoff);
    const pollen = bucket.pollen.filter((p) => p.t >= cutoff);
    const backpack = bucket.backpack.filter((p) => p.t >= cutoff);
    const nectar = {};
    NECTAR_TYPES.forEach((type) => {
      nectar[type] = (bucket.nectar[type] || []).filter((p) => p.t >= cutoff);
    });
    res.json({
      honey,
      pollen,
      backpack,
      nectar,
      currentHoney: bucket.currentHoney || 0
    });
  };

  if (!USE_DB) return respondFromMemory();

  (async () => {
    try {
      const [rows] = await dbPool.query(
        "SELECT metric, t, v FROM samples WHERE user_key = ? AND t >= ? ORDER BY t ASC",
        [req.userKey, cutoff]
      );
      const honey = [];
      const pollen = [];
      const backpack = [];
      const nectar = {};
      NECTAR_TYPES.forEach((type) => {
        nectar[type] = [];
      });
      for (const row of rows) {
        if (row.metric === "honey") honey.push({ t: row.t, v: row.v });
        else if (row.metric === "pollen") pollen.push({ t: row.t, v: row.v });
        else if (row.metric === "backpack") backpack.push({ t: row.t, v: row.v });
        else {
          const nectarType = nectarTypeFromMetric(row.metric);
          if (nectarType) {
            nectar[nectarType].push({ t: row.t, v: row.v });
          }
        }
      }
      const [userRows] = await dbPool.query("SELECT current_honey FROM users WHERE user_key = ? LIMIT 1", [req.userKey]);
      const currentHoney = userRows && userRows[0] ? userRows[0].current_honey || 0 : 0;
      res.json({ honey, pollen, backpack, nectar, currentHoney });
    } catch (err) {
      console.error(err);
      respondFromMemory();
    }
  })();
});

// POST ingest
app.post("/api/ingest", requireWriteKey, (req, res) => {
  const { honey, pollen, backpack, nectar, at, currentHoney } = req.body || {};
  const t = typeof at === "number" ? Math.floor(at) : nowSec();
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
    if (typeof honey === "number" && isFinite(honey)) {
      bucket.honey.push({ t, v: honey });
    }
    if (typeof pollen === "number" && isFinite(pollen)) {
      bucket.pollen.push({ t, v: pollen });
    }
    if (typeof backpack === "number" && isFinite(backpack)) {
      bucket.backpack.push({ t, v: backpack });
    }
    if (typeof currentHoney === "number") {
      bucket.currentHoney = currentHoney;
    }
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
  };

  if (!USE_DB) {
    writeMemory();
    return res.json({ ok: true, mode: "memory" });
  }

  (async () => {
    try {
      await ensureUser(req.userKey);
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
      if (typeof currentHoney === "number") {
        await dbPool.query(
          "UPDATE users SET current_honey = ?, last_activity = ? WHERE user_key = ?",
          [currentHoney, t, req.userKey]
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
