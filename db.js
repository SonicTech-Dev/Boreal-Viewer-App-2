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
 * Accepts losObj keys in either normalized form (los_temp, los_ppm, etc)
 * or the original DB column names ("LoS-Temp(c)", "LoS - PPM", ...).
 *
 * @param {string} topic
 * @param {object|string} rawPayload
 * @param {object} losObj
 * @param {string|Date} receivedAt
 * @param {string|null} serialNumber  OPTIONAL: device serial number to store
 * @returns {Promise<object|null>} inserted row id or null on error
 */
async function insertLosData(topic, rawPayload, losObj, receivedAt, serialNumber = null) {
  const recv = receivedAt ? new Date(receivedAt) : new Date();

  // normalize values - try to coerce to number where possible, otherwise null
  function numOrNull(v) {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }

  // helper: pick first defined key from losObj (accepts many variants)
  function pick(...keys) {
    for (const k of keys) {
      if (losObj && Object.prototype.hasOwnProperty.call(losObj, k) && losObj[k] !== undefined) return losObj[k];
    }
    return undefined;
  }

  // Try many common variants including normalized names produced by server code
  const colTemp = pick(
    'los_temp',
    'LoS-Temp(c)', 'LoS-Temp(C)', 'LoS-Temp', 'LoS Temp', 'lostemp', 'los_temp'
  );
  const colRxLight = pick(
    'los_rx_light',
    'LoS-Rx Light', 'LoS-RxLight', 'LoS Rx Light', 'losrxlight', 'los_rx_light'
  );
  const colR2 = pick(
    'los_r2',
    'LoS- R2', 'LoS-R2', 'LoS - R2', 'losr2', 'los_r2'
  );
  const colHeart = pick(
    'los_heartbeat',
    'LoS-HeartBeat', 'LoS- HeartBeat', 'losheartbeat', 'los_heartbeat'
  );
  const colPpm = pick(
    'los_ppm',
    'LoS - PPM', 'LoS- PPM', 'LoS-PPM', 'los_ppm', 'ppm', 'losppm', 'ppm_mlo'
  );

  const sql = `
    INSERT INTO los_data ("LoS-Temp(c)", "LoS-Rx Light", "LoS- R2", "LoS-HeartBeat", "LoS - PPM", recorded_at, serial_number)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    RETURNING id;
  `;
  const values = [
    numOrNull(colTemp),
    numOrNull(colRxLight),
    numOrNull(colR2),
    numOrNull(colHeart),
    numOrNull(colPpm),
    recv.toISOString(),
    serialNumber || null
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
 * @param {number} offset
 * @param {string|null} serial_number  OPTIONAL: filter by serial_number if provided
 * @returns {Promise<Array>} rows
 */
async function fetchLosData(from, to, limit = 500, offset = 0, serial_number = null) {
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

  if (serial_number) {
    clauses.push(`serial_number = $${idx++}`);
    values.push(String(serial_number));
  }

  const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
  const sql = `
    SELECT id,
          "LoS-Temp(c)" AS los_temp,
          "LoS-Rx Light" AS los_rx_light,
          "LoS- R2" AS los_r2,
          "LoS-HeartBeat" AS los_heartbeat,
          "LoS - PPM" AS los_ppm,
          to_char(recorded_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai', 'YYYY-MM-DD HH24:MI:SS') AS recorded_at_str,
          recorded_at,
          serial_number
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
