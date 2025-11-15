// api/sync-lark-to-supabase.js
// Đồng bộ Lark Bitable -> Supabase

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

const LARK_APP_ID = process.env.LARK_APP_ID;
const LARK_APP_SECRET = process.env.LARK_APP_SECRET;
const LARK_BASE_TOKEN = process.env.LARK_BASE_TOKEN;
const LARK_TABLE_ID = process.env.LARK_TABLE_ID;

/* -------------------------------------------------------------------------- */
/*  Helpers chuẩn hoá dữ liệu                                                */
/* -------------------------------------------------------------------------- */

function firstOrNull(value) {
  // Xử lý null/undefined/empty
  if (value === undefined || value === "" || value === null) return null;
  
  // Nếu là mảng
  if (Array.isArray(value)) {
    if (value.length === 0) return null;
    
    const first = value[0];
    
    // Nếu phần tử đầu cũng là mảng -> đệ quy
    if (Array.isArray(first)) {
      return firstOrNull(first);
    }
    
    // Nếu là object có .text (Lark select)
    if (first && typeof first === 'object' && 'text' in first) {
      return first.text;
    }
    
    // Nếu là object có .name
    if (first && typeof first === 'object' && 'name' in first) {
      return first.name;
    }
    
    return first;
  }
  
  // Nếu là object có .text
  if (value && typeof value === 'object' && 'text' in value) {
    return value.text;
  }
  
  // Nếu là object có .name
  if (value && typeof value === 'object' && 'name' in value) {
    return value.name;
  }
  
  return value;
}

function normalizeText(value) {
  const v = firstOrNull(value);
  if (v == null) return null;
  
  // Chuyển sang string và normalize
  const str = String(v).trim();
  
  // Loại bỏ space dư thừa
  return str.replace(/\s+/g, " ") || null;
}

function extractImageUrl(imageField) {
  if (!imageField) return null;
  
  // Bóc mảng nếu có
  let value = imageField;
  if (Array.isArray(value)) {
    if (value.length === 0) return null;
    value = value[0];
  }
  
  // Nếu là string -> return
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || null;
  }
  
  // Nếu là object
  if (value && typeof value === 'object') {
    return value.url || value.tmp_url || value.link || null;
  }
  
  return null;
}

function formatTimeFromRaw(timeRaw, fallback) {
  // Ưu tiên timeRaw (timestamp milliseconds)
  let timestamp = null;
  
  if (typeof timeRaw === "number" && !Number.isNaN(timeRaw) && timeRaw > 0) {
    timestamp = timeRaw;
  } else if (typeof timeRaw === "string") {
    const parsed = Number(timeRaw);
    if (!isNaN(parsed) && parsed > 0) {
      timestamp = parsed;
    }
  }
  
  if (timestamp) {
    const d = new Date(timestamp);
    if (!isNaN(d.getTime())) {
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${y}/${m}/${day}`;
    }
  }
  
  // Fallback
  if (fallback) {
    const fb = firstOrNull(fallback);
    if (fb && typeof fb === 'string') {
      // Nếu fallback đã là định dạng yyyy/MM/dd
      if (/^\d{4}\/\d{2}\/\d{2}$/.test(fb)) {
        return fb;
      }
      
      // Thử parse fallback
      const parsed = new Date(fb);
      if (!isNaN(parsed.getTime())) {
        const y = parsed.getFullYear();
        const m = String(parsed.getMonth() + 1).padStart(2, "0");
        const day = String(parsed.getDate()).padStart(2, "0");
        return `${y}/${m}/${day}`;
      }
    }
  }
  
  return null;
}

/**
 * Map record Lark -> row Supabase
 */
function mapRecordToSupabaseRow(larkRecord) {
  const f = larkRecord.fields || {};

  // Tên
  const name = normalizeText(f.TieuDe || f.name);

  // Mô tả
  const description = normalizeText(f.MoTa || f.description);

  // Ảnh
  const image = extractImageUrl(f.HinhAnhURL || f.HinhAnh || f.image);

  // Trạng thái
  const type = normalizeText(f.TrangThai || f.type) || "found";

  // Select fields
  const group = normalizeText(f.Group);
  const docType = normalizeText(f.LoaiDo);
  const khuVuc = normalizeText(f.KhuVuc);

  // Thời gian
  let timeRaw = null;
  if (typeof f.ThoiGianRaw === "number") {
    timeRaw = f.ThoiGianRaw;
  } else if (typeof f.timeRaw === "number") {
    timeRaw = f.timeRaw;
  } else if (typeof f.ThoiGian === "number") {
    timeRaw = f.ThoiGian;
  } else if (typeof f.time === "number") {
    timeRaw = f.time;
  }
  
  const time = formatTimeFromRaw(timeRaw, f.ThoiGian || f.time);

  // Ghim
  const isPinned = !!(f.Ghim || f.isPinned);

  // Toạ độ
  const latitude =
    typeof f.Latitude === "number" ? f.Latitude : 
    typeof f.latitude === "number" ? f.latitude : null;
  const longitude =
    typeof f.Longitude === "number" ? f.Longitude : 
    typeof f.longitude === "number" ? f.longitude : null;

  return {
    record_id: larkRecord.record_id,
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
    _name: name,
    _group: group,
    _docType: docType,
    _khuVuc: khuVuc,
  };
}

/* -------------------------------------------------------------------------- */
/*  Lark API                                                                  */
/* -------------------------------------------------------------------------- */

async function getTenantAccessToken() {
  if (LARK_BASE_TOKEN && !LARK_APP_ID && !LARK_APP_SECRET) {
    return LARK_BASE_TOKEN;
  }

  if (!LARK_APP_ID || !LARK_APP_SECRET) {
    throw new Error("Missing LARK_APP_ID or LARK_APP_SECRET");
  }

  const resp = await fetch(
    "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        app_id: LARK_APP_ID,
        app_secret: LARK_APP_SECRET,
      }),
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Failed to get tenant_access_token: ${resp.status} ${text}`
    );
  }

  const data = await resp.json();
  if (data.code !== 0) {
    throw new Error(
      `Lark auth error: code=${data.code}, msg=${data.msg || data.message}`
    );
  }

  return data.tenant_access_token;
}

async function fetchAllLarkRecords() {
  if (!LARK_BASE_TOKEN || !LARK_TABLE_ID) {
    throw new Error("Missing LARK_BASE_TOKEN or LARK_TABLE_ID");
  }

  const tenantToken = await getTenantAccessToken();

  const all = [];
  let pageToken = undefined;

  while (true) {
    const url = new URL(
      `https://open.larksuite.com/open-apis/bitable/v1/apps/${encodeURIComponent(
        LARK_BASE_TOKEN
      )}/tables/${encodeURIComponent(LARK_TABLE_ID)}/records`
    );
    url.searchParams.set("page_size", "500");
    if (pageToken) url.searchParams.set("page_token", pageToken);

    const resp = await fetch(url.toString(), {
      headers: {
        Authorization: `Bearer ${tenantToken}`,
        "Content-Type": "application/json",
      },
    });

    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(
        `Lark fetch error: ${resp.status} ${resp.statusText} - ${text}`
      );
    }

    const data = await resp.json();
    if (data.code !== 0) {
      throw new Error(
        `Lark error: code=${data.code}, msg=${data.msg || data.message}`
      );
    }

    const items = data.data?.items || data.data?.records || [];
    for (const r of items) {
      all.push({
        record_id: r.record_id,
        fields: r.fields || {},
      });
    }

    const next = data.data?.page_token;
    if (!next) break;
    pageToken = next;
  }

  return all;
}

/* -------------------------------------------------------------------------- */
/*  Supabase REST                                                             */
/* -------------------------------------------------------------------------- */

function supabaseHeaders() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  }
  return {
    apikey: SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
    "Content-Type": "application/json",
  };
}

async function fetchExistingRecordIds() {
  const resp = await fetch(
    `${SUPABASE_URL}/rest/v1/TimDoSinhVien?select=record_id`,
    {
      headers: supabaseHeaders(),
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Supabase fetch error: ${resp.status} ${text || resp.statusText}`
    );
  }

  const rows = await resp.json();
  return rows.map((r) => r.record_id);
}

async function upsertRows(rows) {
  if (!rows.length) return;

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/TimDoSinhVien`, {
    method: "POST",
    headers: {
      ...supabaseHeaders(),
      Prefer: "resolution=merge-duplicates",
    },
    body: JSON.stringify(rows),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Supabase upsert error: ${resp.status} ${text || resp.statusText}`
    );
  }
}

async function deleteMissingRows(missingIds) {
  if (!missingIds.length) return;

  const idList = missingIds.map((id) => `"${id}"`).join(",");
  const url = `${SUPABASE_URL}/rest/v1/TimDoSinhVien?record_id=in.(${encodeURIComponent(
    idList
  )})`;

  const resp = await fetch(url, {
    method: "DELETE",
    headers: supabaseHeaders(),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Supabase delete error: ${resp.status} ${text || resp.statusText}`
    );
  }
}

/* -------------------------------------------------------------------------- */
/*  Vercel handler                                                            */
/* -------------------------------------------------------------------------- */

export default async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ success: false, error: "Method not allowed" });
  }

  try {
    const larkRecords = await fetchAllLarkRecords();
    const supabaseRows = larkRecords.map(mapRecordToSupabaseRow);
    
    // Debug log - chỉ log 2 dòng đầu
    if (supabaseRows.length > 0) {
      console.log("=== Sample data (first 2 rows) ===");
      supabaseRows.slice(0, 2).forEach((row, i) => {
        console.log(`Row ${i}:`, {
          record_id: row.record_id,
          name: row.name,
          group: row.group,
          docType: row.docType,
          image: row.image,
          time: row.time,
          timeRaw: row.timeRaw,
        });
      });
    }

    const existingIds = await fetchExistingRecordIds();
    const currentIds = new Set(larkRecords.map((r) => r.record_id));
    const missingIds = existingIds.filter((id) => !currentIds.has(id));

    await upsertRows(supabaseRows);
    await deleteMissingRows(missingIds);

    return res.status(200).json({
      success: true,
      synced: supabaseRows.length,
      deleted: missingIds.length,
    });
  } catch (err) {
    console.error("Sync error:", err);
    return res.status(500).json({
      success: false,
      error: err?.message || "Unknown error",
    });
  }
}
