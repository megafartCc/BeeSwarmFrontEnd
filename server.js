// Simple Railway-ready backend for Bee Swarm stats.
// Endpoints:
//   GET  /api/stats?period=24h             -> { honey: [{t,v}], pollen: [{t,v}] }
//   GET  /api/history?date=YYYY-MM-DD&user_key=KEY  -> Historical 24h graph data
//   GET  /api/leaderboard?metric=total_honey&limit=20  -> Global ranking data
//   POST /api/ingest (body: {honey, pollen, at?}) with x-api-key header
//   Both GET/POST endpoints require x-user-key to scope data per user.

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const moment = require("moment-timezone"); // Used for date/time conversion

const PORT = process.env.PORT || 3000;
// Note: These must match the values used in your environment variables for production
const API_KEY = process.env.API_KEY || "99fds8fdsjkxckkxuihdshufdsZXbccxn"; // Default set to your provided key
const CLIENT_KEY = process.env.CLIENT_KEY || "Xjcxjzx3123Zccxvjfdsfd"; // Default set to your provided key

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

// In-memory store (Fallbacks)
const samples = {};
const nowSec = () => Math.floor(Date.now() / 1000);

// MySQL Config
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
  
  // UPDATED USERS TABLE: Added last_activity and total_honey
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS users (
      user_key VARCHAR(128) PRIMARY KEY,
      last_activity INT NOT NULL DEFAULT 0,
      total_honey DOUBLE NOT NULL DEFAULT 0,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB;
  `);
  // Existing tables remain the same (samples, tokens, buffs)
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
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS tokens (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      t INT NOT NULL,
      token VARCHAR(255) NOT NULL,
      INDEX idx_token_user_time (user_key, t),
      FOREIGN KEY (user_key) REFERENCES users(user_key) ON DELETE CASCADE
    ) ENGINE=InnoDB;
  `);
  await dbPool.query(`
    CREATE TABLE IF NOT EXISTS buffs (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      user_key VARCHAR(128) NOT NULL,
      name VARCHAR(128) NOT NULL,
      t INT NOT NULL,
      v DOUBLE NOT NULL,
      INDEX idx_buff_user_time (user_key, name, t),
      FOREIGN KEY (user_key) REFERENCES users(user_key) ON DELETE CASCADE
    ) ENGINE=InnoDB;
  `);
}

// Helpers
const toSeconds = (period) => {
  if (!period) return 86400;
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

// FIX: Corrected logic to properly check the x-client-key against the environment variable.
const requireReadKey = (req, res, next) => {
  const userKey = req.header("x-user-key") || req.query.user_key; 
  if (!userKey) return res.status(400).json({ error: "x-user-key required" });

  const clientKey = req.header("x-client-key");
  
  // 1. Check if the incoming client key matches the expected CLIENT_KEY
  if (clientKey !== CLIENT_KEY) {
    // Optional: Allow if CLIENT_KEY is set to a placeholder AND no key is provided
    // but since the frontend ALWAYS provides the key, we must enforce the match.
    return res.status(401).json({ error: "unauthorized: invalid x-client-key" });
  }

  req.userKey = userKey;
  next();
};

const requireWriteKey = (req, res, next) => {
  const userKey = req.header("x-user-key");
  if (!userKey) return res.status(400).json({ error: "x-user-key required" });
  const key = req.header("x-api-key");
  if (key !== API_KEY) {
    return res.status(401).json({ error: "unauthorized: invalid x-api-key" });
  }
  req.userKey = userKey;
  next();
};

function getBucket(userKey) {
  if (!samples[userKey]) {
    samples[userKey] = { honey: [], pollen: [], tokens: [], buffs: {} };
  }
  return samples[userKey];
}

async function ensureUser(userKey) {
  if (!USE_DB) return;
  await dbPool.query("INSERT IGNORE INTO users (user_key) VALUES (?)", [userKey]);
}

// NEW ENDPOINT: GET Historical Data for a specific 24h period (e.g., calendar click)
app.get("/api/history", requireReadKey, async (req, res) => {
    if (!USE_DB) return res.status(501).json({ error: "Database required for history." });

    const dateStr = req.query.date; // Expects YYYY-MM-DD
    const date = moment.tz(dateStr, "YYYY-MM-DD", "UTC"); // Treat midnight UTC as the start of the day
    
    if (!date.isValid()) {
        return res.status(400).json({ error: "Invalid date format. Use YYYY-MM-DD." });
    }

    const startT = date.unix();
    const endT = date.clone().add(1, 'day').unix();

    try {
        const [rows] = await dbPool.query(
            "SELECT metric, t, v FROM samples WHERE user_key = ? AND t >= ? AND t < ? ORDER BY t ASC",
            [req.userKey, startT, endT]
        );
        
        const honey = [];
        const pollen = [];
        let totalHoney = 0;
        
        for (const row of rows) {
            if (row.metric === "honey") {
                honey.push({ t: row.t, v: row.v });
                totalHoney += row.v; // Summing the deltas for the day's total
            } else if (row.metric === "pollen") {
                pollen.push({ t: row.t, v: row.v });
            }
        }

        // Calculate Average Rate: Honey/Day * Honey_per_second
        const dayDuration = endT - startT;
        const avgRate = totalHoney / dayDuration * 3600; // Average Honey/Hour
        
        res.json({ honey, pollen, summary: { total_honey: totalHoney, avg_honey_rate: avgRate } });

    } catch (err) {
        console.error("History query error:", err);
        res.status(500).json({ error: "Database error fetching history." });
    }
});


// NEW ENDPOINT: GET Leaderboard
app.get("/api/leaderboard", requireReadKey, async (req, res) => {
    if (!USE_DB) return res.status(501).json({ error: "Database required for leaderboard." });
    
    // For simplicity, we rank by total_honey and filter by last activity (active in the last 7 days)
    const cutoffT = nowSec() - (86400 * 7); // Active in the last 7 days
    const limit = Math.min(parseInt(req.query.limit) || 25, 100);

    try {
        const [rows] = await dbPool.query(
            `SELECT 
                user_key, 
                total_honey, 
                last_activity 
            FROM users 
            WHERE last_activity > ? 
            ORDER BY total_honey DESC 
            LIMIT ?`,
            [cutoffT, limit]
        );
        
        // Calculate Daily Rate Estimate for ranking display (Crude estimate: last 24 hours of samples)
        const leaderboard = [];
        for (const user of rows) {
            const [dailyRateRows] = await dbPool.query(
                `SELECT SUM(v) AS daily_honey FROM samples 
                 WHERE user_key = ? AND metric = 'honey' AND t >= ?`,
                [user.user_key, nowSec() - 86400]
            );

            const dailyHoney = dailyRateRows[0]?.daily_honey || 0;
            const hourlyRate = dailyHoney / 24;

            leaderboard.push({
                user_key: user.user_key,
                total_honey: user.total_honey,
                daily_honey: dailyHoney,
                hourly_rate: hourlyRate,
                last_activity: user.last_activity
            });
        }

        // Final sort by daily rate to make it current/competitive
        leaderboard.sort((a, b) => b.hourly_rate - a.hourly_rate);
        
        res.json(leaderboard);

    } catch (err) {
        console.error("Leaderboard query error:", err);
        res.status(500).json({ error: "Database error fetching leaderboard." });
    }
});


// GET stats (Live Data - same as before)
app.get("/api/stats", requireReadKey, async (req, res) => {
  const periodSec = toSeconds(req.query.period || "24h");
  const cutoff = nowSec() - periodSec;

  const respondFromMemory = () => {
    const bucket = getBucket(req.userKey);
    // ... [existing memory logic remains] ...
    const honey = bucket.honey.filter((p) => p.t >= cutoff);
    const pollen = bucket.pollen.filter((p) => p.t >= cutoff);
    const tokens = bucket.tokens.filter((p) => p.t >= cutoff).slice(-200);
    const buffs = {};
    for (const name in bucket.buffs) {
      buffs[name] = bucket.buffs[name].filter((p) => p.t >= cutoff).slice(-200);
    }
    res.json({ honey, pollen, tokens, buffs });
  };

  if (!USE_DB) return respondFromMemory();

  try {
    const [rows] = await dbPool.query(
      "SELECT metric, t, v FROM samples WHERE user_key = ? AND t >= ? ORDER BY t ASC",
      [req.userKey, cutoff]
    );
    const [tokenRows] = await dbPool.query(
      "SELECT t, token FROM tokens WHERE user_key = ? AND t >= ? ORDER BY t ASC LIMIT 400",
      [req.userKey, cutoff]
    );
    const [buffRows] = await dbPool.query(
      "SELECT name, t, v FROM buffs WHERE user_key = ? AND t >= ? ORDER BY t ASC LIMIT 1000",
      [req.userKey, cutoff]
    );
    
    // ... rest of DB retrieval logic ...
    const honey = [];
    const pollen = [];
    for (const row of rows) {
      if (row.metric === "honey") honey.push({ t: row.t, v: row.v });
      else if (row.metric === "pollen") pollen.push({ t: row.t, v: row.v });
    }
    const tokens = tokenRows.map(r => ({ t: r.t, token: r.token }));
    const buffs = {};
    for (const row of buffRows) {
      buffs[row.name] = buffs[row.name] || [];
      buffs[row.name].push({ t: row.t, v: row.v });
    }
    res.json({ honey, pollen, tokens, buffs });

  } catch (err) {
    console.error(err);
    respondFromMemory();
  }
});


// POST ingest (Write Data - updated to include total_honey update)
app.post("/api/ingest", requireWriteKey, async (req, res) => {
  const { honey, pollen, at, tokens, buffs } = req.body || {};
  const t = typeof at === "number" ? Math.floor(at) : nowSec();

  if (
    typeof honey !== "number" &&
    typeof pollen !== "number" &&
    !Array.isArray(tokens) &&
    typeof buffs !== "object"
  ) {
    return res.status(400).json({ error: "no metrics provided" });
  }
  
  const writeMemory = () => { 
        const bucket = getBucket(req.userKey);
        if (typeof honey === "number") bucket.honey.push({t, v: honey});
        if (typeof pollen === "number") bucket.pollen.push({t, v: pollen});
        if (Array.isArray(tokens)) {
            tokens.filter(tok => typeof tok === "string").forEach(tok => bucket.tokens.push({t, token: tok}));
        }
        if (buffs && typeof buffs === "object") {
            for (const name in buffs) {
                if (typeof buffs[name] === "number") {
                    bucket.buffs[name] = bucket.buffs[name] || [];
                    bucket.buffs[name].push({t, v: buffs[name]});
                }
            }
        }
    };

  if (!USE_DB) {
    writeMemory();
    return res.json({ ok: true, mode: "memory" });
  }

  try {
    await ensureUser(req.userKey);
    
    // 1. Log Samples, Tokens, Buffs (as before)
    // 2. Update User totals and activity
    
    const inserts = [];
    let honeyDelta = 0;

    if (typeof honey === "number" && isFinite(honey)) {
      inserts.push(["honey", t, honey]);
      honeyDelta = honey; // Honey is the delta from the script
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
    
    // Update Users table: total_honey and last_activity
    if (honeyDelta > 0) {
        await dbPool.query(
            `UPDATE users 
             SET total_honey = total_honey + ?, last_activity = ? 
             WHERE user_key = ?`,
            [honeyDelta, t, req.userKey]
        );
    } else {
        await dbPool.query(
             `UPDATE users 
              SET last_activity = ? 
              WHERE user_key = ?`,
             [t, req.userKey]
         );
    }

    // Insert Tokens
    if (Array.isArray(tokens) && tokens.length) {
      const tokenRows = tokens.filter(tok => typeof tok === "string").map(tok => [req.userKey, t, tok]);
      if (tokenRows.length) {
        await dbPool.query("INSERT INTO tokens (user_key, t, token) VALUES ?", [tokenRows]);
      }
    }
    
    // Insert Buffs
    if (buffs && typeof buffs === "object") {
      const buffRows = [];
      for (const name in buffs) {
        const val = buffs[name];
        if (typeof val === "number") {
          buffRows.push([req.userKey, name, t, val]);
        }
      }
      if (buffRows.length) {
        await dbPool.query("INSERT INTO buffs (user_key, name, t, v) VALUES ?", [buffRows]);
      }
    }
    
    res.json({ ok: true, mode: "mysql" });
    
  } catch (err) {
    console.error(err);
    writeMemory();
    res.json({ ok: true, mode: "memory-fallback" });
  }
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
