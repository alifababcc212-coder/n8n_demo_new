const express = require('express');
const Redis = require('ioredis');
const app = express();
app.use(express.json());

const redis = new Redis(process.env.REDIS_URL);
redis.on('error', (err) => console.error('Redis error:', err.message));
redis.on('connect', () => console.log('✅ Redis connected'));

const DEBOUNCE_MS = 5000;
const IDSTAR_WEBHOOK = process.env.IDSTAR_WEBHOOK;
const PORT = process.env.PORT || 3000;

const getKey = (nomorWA) => `buffer:${nomorWA}`;

app.post('/', async (req, res) => {
  const { nomorWA, pesanMasuk, namaPengirim, chatId, session } = req.body;
  if (!nomorWA || !pesanMasuk) {
    return res.status(400).json({ error: 'nomorWA dan pesanMasuk wajib' });
  }

  const key = getKey(nomorWA);
  const messageData = {
    pesanMasuk: pesanMasuk.trim(),
    namaPengirim: namaPengirim || 'User',
    chatId: chatId || nomorWA,
    session: session || 'default'
  };

  await redis.rpush(key, JSON.stringify(messageData));
  await redis.pexpire(key, DEBOUNCE_MS);

  console.log(`[${nomorWA}] pesan masuk (queued)`);
  res.json({ success: true });
});

async function scanKeys(pattern) {
  const keys = [];
  let cursor = '0';
  do {
    const [nextCursor, found] = await redis.scan(
      cursor, 'MATCH', pattern, 'COUNT', 100
    );
    cursor = nextCursor;
    keys.push(...found);
  } while (cursor !== '0');
  return keys;
}

async function forwardWithRetry(nomorWA, payload, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(IDSTAR_WEBHOOK, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (response.ok) {
        console.log(`[${nomorWA}] ✅ sent (${response.status})`);
        return;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (err) {
      if (i < retries - 1) {
        console.warn(`[${nomorWA}] retry ${i + 1}/${retries}:`, err.message);
        await new Promise(r => setTimeout(r, 1000));
      } else {
        console.error(`[${nomorWA}] ❌ gagal setelah ${retries} retry:`, err.message);
      }
    }
  }
}

setInterval(async () => {
  try {
    const keys = await scanKeys('buffer:*');

    for (const key of keys) {
      const ttl = await redis.pttl(key);

      if (ttl > 0 && ttl < 1500) {
        // Atomic: ambil semua pesan dan hapus key sekaligus
        const pipeline = redis.pipeline();
        pipeline.lrange(key, 0, -1);
        pipeline.del(key);
        const [[, messagesRaw]] = await pipeline.exec();

        if (!messagesRaw || messagesRaw.length === 0) continue;

        const messages = messagesRaw.map(m => JSON.parse(m));
        const nomorWA = key.replace('buffer:', '');
        const pesanGabungan = messages.map(m => m.pesanMasuk).join('\n');
        const first = messages[0];

        console.log(`[${nomorWA}] mengirim ${messages.length} pesan`);

        await forwardWithRetry(nomorWA, {
          channel_id: first.chatId,
          user_id: nomorWA,
          user_name: first.namaPengirim,
          message: pesanGabungan,
          response_url: ''
        });
      }
    }
  } catch (err) {
    console.error('Polling error:', err.message);
  }
}, 1000);

app.get('/health', (_, res) => res.json({ status: 'ok' }));
app.listen(PORT, () => console.log(`✅ Worker running on port ${PORT}`));