// db.js — Postgres helper for los_data table (assumes table already exists)
require('dotenv').config();
const { Pool } = require('pg');

const {
  PGHOST,
  PGPORT,
  PGDATABASE,
  PGUSER,
  PGPASSWORD,
  DATABASE_URL
} = process.env;

const pool = new Pool({
  connectionString: DATABASE_URL,
  host: PGHOST,
  port: PGPORT ? Number(PGPORT) : undefined,
  database: PGDATABASE,
  user: PGUSER,
  password: PGPASSWORD,
});

/**
 * Insert a LoS reading into los_data table.
 * Expects losObj keys like:
 *   "LoS-Temp(c)", "LoS-Rx Light", "LoS- R2", "LoS-HeartBeat", "LoS - PPM"
 *
 * @param {string} topic
 * @param {object|string} rawPayload
 * @param {object} losObj
 * @param {string|Date} receivedAt
 * @returns {Promise<object|null>} inserted row id or null on error
 */
async function insertLosData(topic, rawPayload, losObj, receivedAt) {
  const recv = receivedAt ? new Date(receivedAt) : new Date();

  // normalize values - try to coerce to number where possible, otherwise null
  function numOrNull(v) {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  // Map expected column keys (matching your table)
  const colTemp = losObj['LoS-Temp(c)'] ?? losObj['LoS-Temp(c)'.trim()] ?? null;
  const colRxLight = losObj['LoS-Rx Light'] ?? losObj['LoS-Rx Light'.trim()] ?? null;
  const colR2 = losObj['LoS- R2'] ?? losObj['LoS-R2'] ?? losObj['LoS - R2'] ?? null;
  const colHeart = losObj['LoS-HeartBeat'] ?? losObj['LoS- HeartBeat'] ?? null;
  const colPpm = losObj['LoS - PPM'] ?? losObj['LoS- PPM'] ?? losObj['LoS-PPM'] ?? null;

  const sql = `
    INSERT INTO los_data ("LoS-Temp(c)", "LoS-Rx Light", "LoS- R2", "LoS-HeartBeat", "LoS - PPM", recorded_at)
    VALUES ($1, $2, $3, $4, $5, $6)
    RETURNING id;
  `;
  const values = [
    numOrNull(colTemp),
    numOrNull(colRxLight),
    numOrNull(colR2),
    numOrNull(colHeart),
    numOrNull(colPpm),
    recv.toISOString()
  ];

  try {
    const res = await pool.query(sql, values);
    return res.rows[0] || null;
  } catch (err) {
    console.error('Error inserting into los_data:', err && err.message ? err.message : err);
    return null;
  }
}

/**
 * Fetch rows from los_data in a time range (uses recorded_at)
 * @param {string|Date|null} from inclusive start
 * @param {string|Date|null} to inclusive end
 * @param {number} limit
 * @returns {Promise<Array>} rows
 */
// Add "offset" parameter with default 0
async function fetchLosData(from, to, limit = 500, offset = 0) {
  const clauses = [];
  const values = [];
  let idx = 1;

  if (from) {
    clauses.push(`recorded_at >= $${idx++}`);
    values.push(new Date(from).toISOString());
  }
  if (to) {
    clauses.push(`recorded_at <= $${idx++}`);
    values.push(new Date(to).toISOString());
  }

  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  // Add OFFSET $idx
  const sql = `
    SELECT id,
          "LoS-Temp(c)" AS los_temp,
          "LoS-Rx Light" AS los_rx_light,
          "LoS- R2" AS los_r2,
          "LoS-HeartBeat" AS los_heartbeat,
          "LoS - PPM" AS los_ppm,
          to_char(recorded_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai', 'YYYY-MM-DD HH24:MI:SS') AS recorded_at_str,
          recorded_at
    FROM los_data
    ${where}
    ORDER BY recorded_at DESC
    LIMIT $${idx++}
    OFFSET $${idx++};
  `;
  values.push(Number(limit), Number(offset));

  try {
    const res = await pool.query(sql, values);
    return res.rows;
  } catch (err) {
    console.error('Error fetching from los_data:', err && err.message ? err.message : err);
    throw err;
  }
}

module.exports = {
  pool,
  insertLosData,
  fetchLosData
};
