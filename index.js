// Simple Railway-ready backend for Bee Swarm stats.
// Endpoints:
//   GET  /api/stats?period=24h            -> { honey: [{t,v}], pollen: [{t,v}] }
//   POST /api/ingest (body: {honey, pollen, at?}) with x-api-key header
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

// In-memory sample store keyed by userKey:
// { [userKey]: { honey: [{t,v}], pollen: [{t,v}] } }
const samples = {};

// MySQL config (set these env vars on Railway to enable DB mode)
const MYSQL_HOST = process.env.MYSQL_HOST;
const MYSQL_USER = process.env.MYSQL_USER;
const MYSQL_PASSWORD = process.env.MYSQL_PASSWORD;
const MYSQL_DATABASE = process.env.MYSQL_DATABASE;
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
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS samples (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      metric ENUM('honey','pollen') NOT NULL,
      t INT NOT NULL,
      v DOUBLE NOT NULL,
      INDEX idx_user_time (user_key, t),
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

function getBucket(userKey) {
  if (!samples[userKey]) {
    samples[userKey] = { honey: [], pollen: [] };
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
    res.json({
      honey: bucket.honey.filter((p) => p.t >= cutoff),
      pollen: bucket.pollen.filter((p) => p.t >= cutoff)
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
      for (const row of rows) {
        if (row.metric === "honey") honey.push({ t: row.t, v: row.v });
        else if (row.metric === "pollen") pollen.push({ t: row.t, v: row.v });
      }
      res.json({ honey, pollen });
    } catch (err) {
      console.error(err);
      respondFromMemory();
    }
  })();
});

// POST ingest
app.post("/api/ingest", requireWriteKey, (req, res) => {
  const { honey, pollen, at } = req.body || {};
  const t = typeof at === "number" ? Math.floor(at) : nowSec();

  if (typeof honey !== "number" && typeof pollen !== "number") {
    return res.status(400).json({ error: "honey or pollen is required" });
  }

  const writeMemory = () => {
    const bucket = getBucket(req.userKey);
    if (typeof honey === "number" && isFinite(honey)) {
      bucket.honey.push({ t, v: honey });
    }
    if (typeof pollen === "number" && isFinite(pollen)) {
      bucket.pollen.push({ t, v: pollen });
    }
    const cutoff = nowSec() - 86400 * 30;
    bucket.honey = bucket.honey.filter((p) => p.t >= cutoff);
    bucket.pollen = bucket.pollen.filter((p) => p.t >= cutoff);
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
      if (inserts.length) {
        await dbPool.query(
          "INSERT INTO samples (user_key, metric, t, v) VALUES ?",
          [inserts.map(([metric, tt, vv]) => [req.userKey, metric, tt, vv])]
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
    samples.honey.push({ t, v: honey });
  }
  if (typeof pollen === "number" && isFinite(pollen)) {
    samples.pollen.push({ t, v: pollen });
  }

  // Keep only 30 days of data in memory
  const cutoff = nowSec() - 86400 * 30;
  samples.honey = samples.honey.filter((p) => p.t >= cutoff);
  samples.pollen = samples.pollen.filter((p) => p.t >= cutoff);

  res.json({ ok: true });
});

app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(PORT, () => {
  console.log(`Bee stats backend listening on :${PORT}`);
});
