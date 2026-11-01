const express = require('express');
const { Pool } = require('pg');
const Redis = require('ioredis');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const MEALS = ['breakfast', 'lunch', 'snacks', 'dinner'];
const PORT = process.env.PORT || 3000;
const HOST = '0.0.0.0';
const SALT = process.env.VOTER_SALT || 'change-me';

if (!process.env.DATABASE_URL || !process.env.REDIS_URL) {
  console.error('Missing DATABASE_URL or REDIS_URL environment variable');
  process.exit(1);
}

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 5 });
const redis = new Redis(process.env.REDIS_URL);
redis.on('error', (e) => console.error('redis error:', e.message));

const app = express();
app.set('trust proxy', 1);
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Date helpers in India time
const today = () => new Date().toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' }); // YYYY-MM-DD
const weekday = (d) => new Date(d + 'T00:00:00Z').getUTCDay(); // 0 = Sunday

// Voter identity: random cookie, stored only as a salted hash
function getVoter(req, res) {
  const m = (req.headers.cookie || '').match(/(?:^|;\s*)mm_voter=([a-f0-9]{32})/);
  let id = m && m[1];
  if (!id) {
    id = crypto.randomBytes(16).toString('hex');
    res.setHeader('Set-Cookie', `mm_voter=${id}; Max-Age=31536000; Path=/; HttpOnly; Secure; SameSite=Lax`);
  }
  return crypto.createHash('sha256').update(id + SALT).digest('hex').slice(0, 32);
}

async function init() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS menu (
      id SERIAL PRIMARY KEY,
      day SMALLINT NOT NULL,
      meal TEXT NOT NULL,
      items TEXT NOT NULL,
      UNIQUE (day, meal)
    );
    CREATE TABLE IF NOT EXISTS votes (
      id BIGSERIAL PRIMARY KEY,
      vote_date DATE NOT NULL,
      meal TEXT NOT NULL,
      vote SMALLINT NOT NULL CHECK (vote IN (1, -1)),
      voter TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS votes_date_meal ON votes (vote_date, meal);
  `);

  // Load the real menu from data/menu.json (safe to run on every start)
  const rows = JSON.parse(fs.readFileSync(path.join(__dirname, 'data', 'menu.json'), 'utf8'));
  let loaded = 0;
  for (const r of rows) {
    if (!MEALS.includes(r.meal) || r.day < 0 || r.day > 6) continue;
    if (!r.items || /^REPLACE/i.test(r.items)) continue; // skip placeholders
    await pool.query(
      `INSERT INTO menu (day, meal, items) VALUES ($1, $2, $3)
       ON CONFLICT (day, meal) DO UPDATE SET items = EXCLUDED.items`,
      [r.day, r.meal, r.items]
    );
    loaded++;
  }
  for (let d = 0; d < 7; d++) await redis.del(`menu:${d}`); // clear cache after reload
  console.log(`menu rows loaded: ${loaded}`);
}

// Live tally lives in Redis; rebuilt from Postgres if Redis is empty
async function tally(date, meal) {
  const key = `tally:${date}:${meal}`;
  if (!(await redis.exists(key))) {
    const { rows } = await pool.query(
      'SELECT vote, COUNT(*)::int AS c FROM votes WHERE vote_date = $1 AND meal = $2 GROUP BY vote',
      [date, meal]
    );
    const up = rows.find((r) => r.vote === 1)?.c || 0;
    const down = rows.find((r) => r.vote === -1)?.c || 0;
    await redis.hset(key, 'up', up, 'down', down);
    await redis.expire(key, 172800);
  }
  const t = await redis.hgetall(key);
  return { up: Number(t.up) || 0, down: Number(t.down) || 0 };
}

app.get('/api/today', async (req, res) => {
  try {
    const date = today();
    const day = weekday(date);
    const voter = getVoter(req, res);

    // Menu cache in Redis, source of truth in Postgres
    const cacheKey = `menu:${day}`;
    let menu = JSON.parse((await redis.get(cacheKey)) || 'null');
    if (!menu) {
      const { rows } = await pool.query('SELECT meal, items FROM menu WHERE day = $1', [day]);
      menu = rows;
      await redis.set(cacheKey, JSON.stringify(menu), 'EX', 3600);
    }

    const meals = [];
    for (const meal of MEALS) {
      const item = menu.find((m) => m.meal === meal);
      if (!item) continue;
      const voted = (await redis.exists(`lock:${date}:${meal}:${voter}`)) === 1;
      meals.push({ meal, items: item.items, voted, ...(await tally(date, meal)) });
    }
    res.json({ date, meals });
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'server error' });
  }
});

app.post('/api/vote', async (req, res) => {
  try {
    const { meal, vote } = req.body || {};
    if (!MEALS.includes(meal) || !['up', 'down'].includes(vote)) {
      return res.status(400).json({ error: 'bad request' });
    }
    const date = today();
    const voter = getVoter(req, res);

    // Redis job: one vote per person per meal per day, expires by itself
    const lockKey = `lock:${date}:${meal}:${voter}`;
    const locked = await redis.set(lockKey, '1', 'EX', 90000, 'NX');
    if (!locked) return res.status(429).json({ error: 'You already voted for this meal today' });

    // Postgres job: permanent record
    try {
      await pool.query(
        'INSERT INTO votes (vote_date, meal, vote, voter) VALUES ($1, $2, $3, $4)',
        [date, meal, vote === 'up' ? 1 : -1, voter]
      );
    } catch (err) {
      await redis.del(lockKey); // let them retry if the save failed
      throw err;
    }

    // Redis job: fast live counter
    const key = `tally:${date}:${meal}`;
    if (await redis.exists(key)) await redis.hincrby(key, vote, 1);
    res.json(await tally(date, meal));
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'server error' });
  }
});

// Postgres job: 7-day history for the mess committee
app.get('/api/week', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT to_char(vote_date, 'YYYY-MM-DD') AS date, meal,
             COUNT(*) FILTER (WHERE vote = 1)::int AS up,
             COUNT(*) FILTER (WHERE vote = -1)::int AS down
      FROM votes
      WHERE vote_date > (now() AT TIME ZONE 'Asia/Kolkata')::date - 7
      GROUP BY vote_date, meal
      ORDER BY vote_date DESC, meal`);
    res.json(rows);
  } catch (e) {
    console.error(e);
    res.status(500).json({ error: 'server error' });
  }
});

app.get('/healthz', (_req, res) => res.send('ok'));

init()
  .then(() => app.listen(PORT, HOST, () => console.log(`mess-meter listening on http://${HOST}:${PORT}`)))
  .catch((e) => { console.error('startup failed:', e); process.exit(1); });
