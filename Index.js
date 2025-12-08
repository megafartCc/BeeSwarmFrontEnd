// Simple Railway-ready backend for Bee Swarm stats.
// Endpoints:
//   GET  /api/stats?period=24h            -> { honey: [{t,v}], pollen: [{t,v}] }
//   POST /api/ingest (body: {honey, pollen, at?}) with x-api-key header
// Auth:
//   Read (GET):   x-client-key must match CLIENT_KEY (if set)
//   Write (POST): x-api-key must match API_KEY (required)
//
// Persistence:
//   This example keeps samples in-memory. Swap `samples` with a database
//   (e.g., Railway Postgres) for real persistence.

const express = require("express");
const cors = require("cors");

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || "replace-this-api-key";          // used by your script to push samples
const CLIENT_KEY = process.env.CLIENT_KEY || "replace-this-client-key"; // used by frontend to read

const app = express();
app.use(cors());
app.use(express.json({ limit: "256kb" }));

// In-memory sample store: { honey: [{t,v}], pollen: [{t,v}] }
const samples = {
  honey: [],
  pollen: []
};

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
  if (!CLIENT_KEY || CLIENT_KEY === "replace-this-client-key") {
    return next(); // not enforced if left default
  }
  const key = req.header("x-client-key");
  if (key !== CLIENT_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
};

const requireWriteKey = (req, res, next) => {
  const key = req.header("x-api-key");
  if (key !== API_KEY) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
};

// GET stats
app.get("/api/stats", requireReadKey, (req, res) => {
  const periodSec = toSeconds(req.query.period || "24h");
  const cutoff = nowSec() - periodSec;
  const payload = {
    honey: samples.honey.filter((p) => p.t >= cutoff),
    pollen: samples.pollen.filter((p) => p.t >= cutoff)
  };
  res.json(payload);
});

// POST ingest
app.post("/api/ingest", requireWriteKey, (req, res) => {
  const { honey, pollen, at } = req.body || {};
  const t = typeof at === "number" ? Math.floor(at) : nowSec();

  if (typeof honey !== "number" && typeof pollen !== "number") {
    return res.status(400).json({ error: "honey or pollen is required" });
  }

  if (typeof honey === "number" && isFinite(honey)) {
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
