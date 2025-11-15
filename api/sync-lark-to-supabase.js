import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const LARK_APP_ID = process.env.LARK_APP_ID;
const LARK_APP_SECRET = process.env.LARK_APP_SECRET;
const LARK_APP_TOKEN = process.env.LARK_APP_TOKEN;
const LARK_TABLE_ID = process.env.LARK_TABLE_ID;

// Chuẩn hoá text giống GAS: bỏ dấu, lower-case, trim
function normalizeText(input) {
  if (!input) return "";
  return input
    .toString()
    .normalize("NFD")
    .replace(/[̀-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

// Lấy tenant_access_token của Lark
async function getLarkToken() {
  const res = await fetch(
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

  if (!res.ok) {
    throw new Error(`Lark auth error: ${res.status} ${await res.text()}`);
  }
  const data = await res.json();
  return data.tenant_access_token;
}

// Lấy toàn bộ record từ Lark Bitable
async function fetchAllLarkRecords(token) {
  const records = [];
  let pageToken = undefined;

  while (true) {
    const url = new URL(
      `https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_TABLE_ID}/records`
    );
    url.searchParams.set("page_size", "500");
    if (pageToken) url.searchParams.set("page_token", pageToken);

    const res = await fetch(url.toString(), {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (!res.ok) {
      throw new Error(`Lark fetch error: ${res.status} ${await res.text()}`);
    }

    const data = await res.json();
    if (!data?.data?.items) break;

    records.push(...data.data.items);
    pageToken = data.data.page_token;
    if (!pageToken) break;
  }

  return records;
}

// Map một record Lark sang một row trong bảng TimDoSinhVien (giống GAS)
function mapRecordToSupabaseRow(record) {
  const f = record.fields || {};

  // Giống helper bên GAS: bóc multi-select / object -> text thường
  const unwrapField = (value) => {
    // Array (multi-select)
    if (Array.isArray(value)) {
      const texts = value
        .map((v) => {
          if (typeof v === "string") return v;
          if (v && typeof v === "object") {
            if (typeof v.text === "string") return v.text;
            if (typeof v.name === "string") return v.name;
          }
          return "";
        })
        .filter(Boolean);
      return texts.join(", ") || "";
    }

    // Object đơn lẻ
    if (value && typeof value === "object") {
      if (typeof value.text === "string") return value.text;
      if (typeof value.name === "string") return value.name;
    }

    // String thường
    return typeof value === "string" ? value : "";
  };

  const recordId = f.record_id || record.record_id || record.id || "";
  const name = f.Ten || "";
  const description = f.MoTa || "";

  // Ảnh gốc từ Lark (đã là link Drive dạng uc?id=...)
  const originalImage = (f.HinhAnhURL || "").trim();

  // Những field bị thành ["Thẻ sinh viên"] bữa giờ
  const type = unwrapField(f.LoaiDo);
  const group = unwrapField(f.Group);
  const docType = unwrapField(f.DocType);
  const khuVuc = unwrapField(f.KhuVuc);

  const time = f.ThoiGian || "";
  const timeRaw = f.TimeRaw || null;
  const isPinned = Boolean(f.Pin || f.isPinned || false);

  const latitude = f.Lat || f.latitude || null;
  const longitude = f.Long || f.longitude || null;

  // Field search / slug giống GAS
  const normalizedName = normalizeText(name);
  const normalizedGroup = normalizeText(group);
  const normalizedDocType = normalizeText(docType);
  const normalizedKhuVuc = normalizeText(khuVuc);

  return {
    record_id: recordId,
    name,
    description,
    image: originalImage,
    type,
    group,
    docType,
    khuVuc,
    time,
    timeRaw: timeRaw || null,
    isPinned,
    latitude,
    longitude,
    _name: normalizedName,
    _group: normalizedGroup,
    _docType: normalizedDocType,
    _khuVuc: normalizedKhuVuc,
  };
}

export default async function handler(req, res) {
  if (req.method !== "GET") {
    return res.status(405).json({ success: false, error: "Method not allowed" });
  }

  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
      throw new Error("supabaseUrl/supabaseKey is required.");
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    // 1. Lấy toàn bộ record hiện có trong Supabase
    const { data: existingRows, error: existingError } = await supabase
      .from("TimDoSinhVien")
      .select("record_id");
    if (existingError) throw existingError;

    const existingIds = new Set(existingRows.map((r) => r.record_id));

    // 2. Lấy dữ liệu từ Lark
    const token = await getLarkToken();
    const larkRecords = await fetchAllLarkRecords(token);

    const rows = [];
    const larkIds = new Set();

    for (const rec of larkRecords) {
      const row = mapRecordToSupabaseRow(rec);
      if (!row.record_id) continue;
      larkIds.add(row.record_id);
      rows.push(row);
    }

    // 3. Xoá các record Supabase không còn trong Lark
    const idsToDelete = Array.from(existingIds).filter((id) => !larkIds.has(id));

    if (idsToDelete.length > 0) {
      await supabase.from("TimDoSinhVien").delete().in("record_id", idsToDelete);
    }

    // 4. Upsert dữ liệu mới
    if (rows.length > 0) {
      await supabase.from("TimDoSinhVien").upsert(rows, { onConflict: "record_id" });
    }

    return res
      .status(200)
      .json({ success: true, synced: rows.length, deleted: idsToDelete.length });
  } catch (err) {
    return res.status(500).json({ success: false, error: String(err) });
  }
}
