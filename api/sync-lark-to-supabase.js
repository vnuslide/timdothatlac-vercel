// api/sync-lark-to-supabase.js
// Đồng bộ Lark Bitable -> Supabase, có chuẩn hoá dữ liệu giống Apps Script cũ

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Lark
// LARK_APP_ID / LARK_APP_SECRET: dùng để lấy tenant_access_token
// LARK_BASE_TOKEN: nếu bạn đã tự lấy được tenant_access_token thì có thể
//    set luôn biến này và bỏ qua bước auth.
// LARK_BASE_TOKEN ở đây chính là app_token của Bitable (database)
// LARK_TABLE_ID: id bảng trong Bitable
const LARK_APP_ID = process.env.LARK_APP_ID;
const LARK_APP_SECRET = process.env.LARK_APP_SECRET;
const LARK_BASE_TOKEN = process.env.LARK_BASE_TOKEN; // app_token / base token
const LARK_TABLE_ID = process.env.LARK_TABLE_ID;

/* -------------------------------------------------------------------------- */
/*  Helpers chuẩn hoá dữ liệu – giống các hàm trong Apps Script cũ           */
/* -------------------------------------------------------------------------- */

function firstOrNull(value) {
  if (Array.isArray(value)) {
    return value.length ? value[0] : null;
  }
  if (value === undefined || value === "") return null;
  return value ?? null;
}

function normalizeText(value) {
  const v = firstOrNull(value);
  if (v == null) return null;
  return String(v).trim().replace(/\s+/g, " ");
}

function formatTimeFromRaw(timeRaw, fallback) {
  if (typeof timeRaw === "number" && !Number.isNaN(timeRaw)) {
    const d = new Date(timeRaw);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    // Apps Script format kiểu yyyy/MM/dd
    return `${y}/${m}/${day}`;
  }
  return fallback || null;
}

/**
 * Map một ghi Lark -> 1 row trong Supabase (public_posts)
 *  - Bóc phần tử đầu của các field dạng mảng (Group, LoaiDo, KhuVuc,…)
 *  - Chuẩn hoá text
 *  - timeRaw: số timestamp; time: chuỗi yyyy/MM/dd
 */
function mapRecordToSupabaseRow(larkRecord) {
  const f = larkRecord.fields || {};

  // Tiêu đề / tên
  const name = normalizeText(f.TieuDe || f.name);

  // Mô tả
  const description = normalizeText(f.MoTa || f.description);

  // Ảnh: dùng HinhAnhURL trong Lark
  const image = firstOrNull(f.HinhAnhURL || f.image) || null;

  // Trạng thái: "Cần tìm" / "Nhặt được" → cột type
  const type = normalizeText(f.TrangThai || f.type) || "found";

  // Các lựa chọn (select / multi-select) – Lark trả mảng → bóc phần tử đầu
  const group = normalizeText(f.Group);
  const docType = normalizeText(f.LoaiDo);
  const khuVuc = normalizeText(f.KhuVuc);

  // Thời gian
  const timeRaw =
    typeof f.ThoiGianRaw === "number"
      ? f.ThoiGianRaw
      : Number(f.timeRaw) || null;
  const time = formatTimeFromRaw(timeRaw, firstOrNull(f.ThoiGian || f.time));

  // Ghim bài
  const isPinned = !!(f.Ghim || f.isPinned);

  // Toạ độ
  const latitude =
    typeof f.Latitude === "number" ? f.Latitude : Number(f.latitude) || null;
  const longitude =
    typeof f.Longitude === "number" ? f.Longitude : Number(f.longitude) || null;

  return {
    // Khóa chính
    record_id: larkRecord.record_id,

    // Các cột chính của bảng public_posts
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

    // Các cột “raw” để phục vụ search / backup
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
  // Nếu bạn đã tự set sẵn tenant_access_token vào LARK_BASE_TOKEN
  // thì dùng luôn, khỏi gọi auth.
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
      `Failed to get tenant_access_token from Lark: ${resp.status} ${text}`
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
    throw new Error("Missing LARK_BASE_TOKEN or LARK_TABLE_ID env");
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
        `Lark fetch records error: ${resp.status} ${resp.statusText} - ${text}`
      );
    }

    const data = await resp.json();
    if (data.code !== 0) {
      throw new Error(
        `Lark records error: code=${data.code}, msg=${data.msg || data.message}`
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
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  }
  return {
    apikey: SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
    "Content-Type": "application/json",
  };
}

async function fetchExistingRecordIds() {
  const resp = await fetch(
    `${SUPABASE_URL}/rest/v1/public_posts?select=record_id`,
    {
      headers: supabaseHeaders(),
    }
  );

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(
      `Supabase fetch existing error: ${resp.status} ${text || resp.statusText}`
    );
  }

  const rows = await resp.json();
  return rows.map((r) => r.record_id);
}

async function upsertRows(rows) {
  if (!rows.length) return;

  const resp = await fetch(`${SUPABASE_URL}/rest/v1/public_posts`, {
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

  // record_id=in.(id1,id2,...)
  const idList = missingIds.map((id) => `"${id}"`).join(",");
  const url = `${SUPABASE_URL}/rest/v1/public_posts?record_id=in.(${encodeURIComponent(
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
    // 1. Lấy toàn bộ record từ Lark
    const larkRecords = await fetchAllLarkRecords();

    // 2. Map sang cấu trúc Supabase & chuẩn hoá
    const supabaseRows = larkRecords.map(mapRecordToSupabaseRow);

    // 3. Fetch list record_id đang có ở Supabase
    const existingIds = await fetchExistingRecordIds();
    const currentIds = new Set(larkRecords.map((r) => r.record_id));
    const missingIds = existingIds.filter((id) => !currentIds.has(id));

    // 4. Upsert & delete
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
