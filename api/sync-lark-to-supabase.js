// api/sync-lark-to-supabase.js
import { createClient } from '@supabase/supabase-js';

const LARK_API_BASE = 'https://open.larksuite.com/open-apis';

/**
 * Chuẩn hóa text:
 * - Array -> lấy phần tử đầu
 * - Object có {name} hoặc {text} -> lấy field đó
 * - Chuỗi dạng '["USSH"]' -> parse JSON -> 'USSH'
 * - Trim khoảng trắng, rỗng -> null
 */
function normalizeText(value) {
  if (value == null) return null;

  // Nếu là mảng, lấy phần tử đầu và tiếp tục xử lý
  if (Array.isArray(value)) {
    if (value.length === 0) return null;
    return normalizeText(value[0]);
  }

  // Nếu là object kiểu { name: 'USSH' } hoặc { text: 'USSH' }
  if (typeof value === 'object') {
    if ('name' in value) return normalizeText(value.name);
    if ('text' in value) return normalizeText(value.text);
  }

  // Mọi thứ khác convert sang string
  let s = String(value).trim();
  if (!s) return null;

  // Thử parse chuỗi JSON kiểu '["USSH"]'
  if (s.startsWith('[') && s.endsWith(']')) {
    try {
      const parsed = JSON.parse(s);
      if (Array.isArray(parsed) && parsed.length > 0) {
        return normalizeText(parsed[0]);
      }
    } catch (_) {
      // nếu parse lỗi thì thôi, trả lại chuỗi cũ
    }
  }

  return s;
}

/**
 * Chuẩn hóa thời gian:
 * nhận number / string / Date -> { time: "YYYY/MM/DD", timeRaw: millis }
 */
function normalizeTime(raw) {
  if (raw == null) return { time: null, timeRaw: null };

  let ms = null;

  if (typeof raw === 'number') {
    ms = raw;
  } else if (raw instanceof Date) {
    ms = raw.getTime();
  } else if (typeof raw === 'string') {
    const trimmed = raw.trim();

    // chuỗi toàn số -> xem như millis
    if (/^\d{10,}$/.test(trimmed)) {
      ms = Number(trimmed);
    } else {
      const d = new Date(trimmed);
      if (!Number.isNaN(d.getTime())) {
        ms = d.getTime();
      }
    }
  }

  if (!ms) return { time: null, timeRaw: null };

  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, '0');
  const time = `${d.getFullYear()}/${pad(d.getMonth() + 1)}/${pad(d.getDate())}`;

  return { time, timeRaw: ms };
}

/**
 * Map 1 record từ Lark -> 1 row Supabase
 * Cố gắng support nhiều kiểu đặt tên field (Time/time, Group/group, …)
 */
function mapRecordToSupabaseRow(record) {
  const f = record.fields || {};

  const { time, timeRaw } = normalizeTime(
    f.time ??
      f.Time ??
      f['Thời gian'] ??
      f['thoigian'] ??
      null
  );

  const name = normalizeText(f.name ?? f.Name);
  const description = normalizeText(f.description ?? f.Description);
  const image =
    normalizeText(f.HinhAnhURL ?? f.hinhanhurl ?? f.image ?? f.Image) || null;

  const type = normalizeText(f.type ?? f.Type);
  const group = normalizeText(f.group ?? f.Group);
  const docType = normalizeText(f.docType ?? f.DocType);
  const khuVuc = normalizeText(f.khuVuc ?? f.KhuVuc);

  const isPinnedRaw = f.isPinned ?? f.IsPinned ?? false;
  const isPinned = Boolean(isPinnedRaw);

  const latitude = f.latitude ?? f.Latitude ?? null;
  const longitude = f.longitude ?? f.Longitude ?? null;

  return {
    record_id: record.record_id || record.id,
    name,
    description,
    image,
    type,
    group,
    docType,
    khuVuc,
    time,
    timeRaw,
    isPinned,
    latitude,
    longitude,
    // Các cột “_” để dễ debug / filter
    _name: name,
    _group: group,
    _docType: docType,
    _khuVuc: khuVuc,
  };
}

// Lấy access token của Lark
async function fetchLarkAccessToken() {
  const res = await fetch(`${LARK_API_BASE}/auth/v3/tenant_access_token/internal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      app_id: process.env.LARK_APP_ID,
      app_secret: process.env.LARK_APP_SECRET,
    }),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Lark token error: ${res.status} ${text}`);
  }

  const data = await res.json();
  if (data.code !== 0) {
    throw new Error(`Lark token error code=${data.code} msg=${data.msg}`);
  }

  return data.tenant_access_token;
}

// Lấy toàn bộ record từ Lark (phân trang)
async function fetchAllLarkRecords(accessToken) {
  const records = [];
  let pageToken = null;

  while (true) {
    const url = new URL(
      `${LARK_API_BASE}/bitable/v1/apps/${process.env.LARK_BASE_TOKEN}/tables/${process.env.LARK_TABLE_ID}/records`
    );
    url.searchParams.set('page_size', '500');
    if (pageToken) url.searchParams.set('page_token', pageToken);

    const res = await fetch(url, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
      },
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Lark records error: ${res.status} ${text}`);
    }

    const data = await res.json();
    if (data.code !== 0) {
      throw new Error(`Lark records error code=${data.code} msg=${data.msg}`);
    }

    const pageRecords = data.data?.items ?? [];
    for (const r of pageRecords) {
      records.push({
        record_id: r.record_id,
        fields: r.fields || {},
      });
    }

    pageToken = data.data?.page_token;
    const hasMore = data.data?.has_more;
    if (!hasMore || !pageToken) break;
  }

  return records;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ success: false, error: 'Method not allowed' });
  }

  try {
    // 1. Lấy token Lark
    const accessToken = await fetchLarkAccessToken();

    // 2. Lấy toàn bộ dữ liệu Lark
    const larkRecords = await fetchAllLarkRecords(accessToken);

    // 3. Map sang định dạng Supabase
    const rows = larkRecords.map(mapRecordToSupabaseRow);

    // 4. Upsert vào Supabase
    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_SERVICE_ROLE_KEY
    );

    const { data: upserted, error: upsertError } = await supabase
      .from('public_posts')
      .upsert(rows, {
        onConflict: 'record_id',
      });

    if (upsertError) {
      throw upsertError;
    }

    // 5. Xoá những record đã bị xóa ở Lark
    const larkIds = new Set(rows.map((r) => r.record_id));

    const { data: existing, error: fetchExistingError } = await supabase
      .from('public_posts')
      .select('record_id');

    if (fetchExistingError) {
      throw fetchExistingError;
    }

    const toDelete = (existing || [])
      .map((r) => r.record_id)
      .filter((id) => !larkIds.has(id));

    if (toDelete.length > 0) {
      const { error: deleteError } = await supabase
        .from('public_posts')
        .delete()
        .in('record_id', toDelete);

      if (deleteError) {
        throw deleteError;
      }
    }

    return res.status(200).json({
      success: true,
      synced: rows.length,
      deleted: toDelete.length,
    });
  } catch (err) {
    console.error('sync-lark-to-supabase error', err);
    return res.status(500).json({
      success: false,
      error: String(err.message || err),
    });
  }
}
