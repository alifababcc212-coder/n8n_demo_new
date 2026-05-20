const express = require('express');
const Redis = require('ioredis');
const app = express();
app.use(express.json());

const redis = new Redis(process.env.REDIS_URL);
redis.on('error', (err) => console.error('Redis error:', err.message));

// State untuk menyimpan timer debounce di memory Node.js
const debounceTimers = new Map();

const DEBOUNCE_MS = 9000;
const IDSTAR_WEBHOOK = process.env.IDSTAR_WEBHOOK;
const PORT = process.env.PORT || 3000;

const getKey = (nomorWA) => `buffer:${nomorWA}`;

// Fungsi untuk mengeksekusi pengiriman dan menghapus data di Redis
async function processBuffer(nomorWA) {
  const key = getKey(nomorWA);
  try {
    // Atomic: ambil semua pesan dan hapus key sekaligus
    const pipeline = redis.pipeline();
    pipeline.lrange(key, 0, -1);
    pipeline.del(key);
    const [[, messagesRaw]] = await pipeline.exec();

    if (!messagesRaw || messagesRaw.length === 0) return;

    const messages = messagesRaw.map(m => JSON.parse(m));
    const pesanGabungan = messages.map(m => m.pesanMasuk).join('\n');
    const first = messages[0];

    console.log(`[${nomorWA}] mengirim ${messages.length} pesan gabungan`);

    await forwardWithRetry(nomorWA, {
      channel_id: first.chatId,
      user_id: nomorWA,
      user_name: first.namaPengirim,
      message: pesanGabungan,
      response_url: ''
    });
  } catch (err) {
    console.error(`[${nomorWA}] Error processing buffer:`, err.message);
  } finally {
    // Bersihkan state timer dari memory
    debounceTimers.delete(nomorWA);
  }
}

// Fungsi webhook penerima dari WhatsApp
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

  // 1. Simpan payload ke Redis agar tidak hilang
  await redis.rpush(key, JSON.stringify(messageData));
  
  console.log(`[${nomorWA}] pesan masuk (queued)`);

  // 2. Reset timer debounce setiap kali ada pesan baru dari nomor ini
  if (debounceTimers.has(nomorWA)) {
    clearTimeout(debounceTimers.get(nomorWA));
  }

  // 3. Set timer. Jika tidak ada pesan baru selama DEBOUNCE_MS, pesan dikirim.
  const timer = setTimeout(() => {
    processBuffer(nomorWA);
  }, DEBOUNCE_MS);

  debounceTimers.set(nomorWA, timer);

  res.json({ success: true });
});

// Fungsi pengiriman ke webhook utama dengan retry
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

// Fungsi recovery untuk menangani pesan yang tertinggal jika Railway restart
async function recoverOrphanedMessages() {
  console.log('Mengecek pesan yang tertinggal di Redis saat server startup...');
  try {
    let cursor = '0';
    let count = 0;
    do {
      const [nextCursor, found] = await redis.scan(cursor, 'MATCH', 'buffer:*', 'COUNT', 100);
      cursor = nextCursor;
      for (const key of found) {
        const nomorWA = key.replace('buffer:', '');
        console.log(`[Recovery] Memproses sisa pesan untuk ${nomorWA}`);
        // Kirim sisa pesan yang tertinggal di Redis
        processBuffer(nomorWA);
        count++;
      }
    } while (cursor !== '0');
    console.log(`[Recovery] Selesai. Menemukan ${count} antrean yang tertinggal.`);
  } catch (error) {
    console.error('[Recovery] Gagal memulihkan pesan:', error.message);
  }
}

// Gunakan .once agar recovery hanya berjalan satu kali saat booting pertama
redis.once('connect', () => {
  console.log('✅ Redis connected');
  recoverOrphanedMessages();
});

app.get('/health', (_, res) => res.json({ status: 'ok' }));

app.listen(PORT, () => {
  console.log(`✅ Worker running on port ${PORT}`);
});