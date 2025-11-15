import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const LARK_APP_ID = process.env.LARK_APP_ID;
const LARK_APP_SECRET = process.env.LARK_APP_SECRET;
const LARK_APP_TOKEN = process.env.LARK_APP_TOKEN; // Bitable app/base token
const LARK_TABLE_ID = process.env.LARK_TABLE_ID;

// Chuẩn hóa text giống hàm normalizeText trong GAS
function normalizeText(input) {
  if (!input) return "";
  let s = input.toString().toLowerCase();
  // bỏ dấu tiếng Việt
  s = s.normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/đ/g, "d");
  return s.trim();
}

// Lấy phần tử đầu nếu là array, nếu không thì trả về chính nó
function pickFirst(value) {
  if (Array.isArray(value)) {
    return value.length > 0 ? value[0] : "";
  }
  return value ?? "";
}

function formatDateYMD(value) {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}/${m}/${day}`;
}

// === Lấy tenant_access_token từ Lark ===
async function getLarkToken() {
  const res = await fetch(
    "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
    {
      method: "POST",
      headers: { "Content-Type": "application/json; charset=utf-8" },
      body: JSON.stringify({
        app_id: LARK_APP_ID,
        app_secret: LARK_APP_SECRET,
      }),
    }
  );

  const data = await res.json();
  if (data.code !== 0 || !data.tenant_access_token) {
    throw new Error("Lark auth error: " + JSON.stringify(data));
  }
  return data.tenant_access_token;
}

// === Lấy tất cả bản ghi TrangThai = "Đã duyệt" từ Lark Bitable ===
async function fetchApprovedRecordsFromLark(token) {
  const allItems = [];
  let pageToken = "";

  do {
    const params = new URLSearchParams();
    params.set('filter', 'CurrentValue.[TrangThai] = "Đã duyệt"');
    params.set("page_size", "500");
    if (pageToken) params.set("page_token", pageToken);

    const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_TABLE_ID}/records?${params.toString()}`;

    const res = await fetch(url, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const data = await res.json();
    if (data.code !== 0) {
      throw new Error("Lark fetch error: " + JSON.stringify(data));
    }

    const items = data.data?.items ?? [];
    allItems.push(...items);
    pageToken = data.data?.has_more ? data.data.page_token : "";
  } while (pageToken);

  return allItems;
}

// === Map 1 record từ Lark sang đúng cấu trúc bảng TimDoSinhVien (giống GAS) ===
function recordToPublicSupabase(rec) {
  const f = rec.fields || {};

  // time, timeRaw
  let timeStr = "";
  if (f.ThoiGian) {
    timeStr = formatDateYMD(f.ThoiGian);
  }
  const timeRaw = timeStr ? new Date(timeStr).getTime() : 0;

  // type: found / lost
  const VN_MAP = {
    FOUND: ["nhặt được", "nhat duoc", "found"],
    LOST: ["mất", "mat", "lost", "tìm đồ", "tim do"],
  };
  const typeRaw = (f.LoaiTin || "").toString().toLowerCase();
  const isFound = VN_MAP.FOUND.some((x) => typeRaw.includes(x));
  const type = isFound ? "found" : "lost";

  const name = f.TieuDe || "";
  const group = pickFirst(f.Group); // Group là array → lấy phần tử đầu
  const description = f.MoTa || "";

  const loaiDoArray = Array.isArray(f.LoaiDo) ? f.LoaiDo : [];
  const docType = loaiDoArray.join(", ");

  const khuVuc = f.KhuVuc || "";

  const originalImage = f.HinhAnhURL || null;

  // Tọa độ
  const lat =
    f.Latitude !== undefined && f.Latitude !== null && f.Latitude !== ""
      ? Number(f.Latitude)
      : null;
  const lng =
    f.Longitude !== undefined && f.Longitude !== null && f.Longitude !== ""
      ? Number(f.Longitude)
      : null;

  return {
    record_id: rec.record_id,
    time: timeStr,
    timeRaw,
    name,
    group, // map thẳng sang cột "group" trong Supabase
    description,
    docType,
    khuVuc,
    image: originalImage,
    type,
    isPinned: f.Ghim === true,
    latitude: Number.isNaN(lat) ? null : lat,
    longitude: Number.isNaN(lng) ? null : lng,
    _name: normalizeText(name),
    _group: normalizeText(group),
    _docType: normalizeText(docType),
    _khuVuc: normalizeText(khuVuc),
  };
}

export default async function handler(req, res) {
  try {
    if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
      return res
        .status(500)
        .json({ success: false, error: "Supabase env missing" });
    }
    if (!LARK_APP_ID || !LARK_APP_SECRET || !LARK_APP_TOKEN || !LARK_TABLE_ID) {
      return res
        .status(500)
        .json({ success: false, error: "Lark env missing" });
    }

    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    // 1. Lấy token Lark và tất cả records đã duyệt
    const token = await getLarkToken();
    const items = await fetchApprovedRecordsFromLark(token);

    // 2. Map dữ liệu giống hệt GAS
    const rows = items.map(recordToPublicSupabase);
    const larkIdSet = new Set(rows.map((r) => r.record_id));

    // 3. Lấy danh sách record_id hiện có trong Supabase (bảng TimDoSinhVien)
    const { data: existing, error: existingError } = await supabase
      .from("TimDoSinhVien")
      .select("record_id");

    if (existingError) {
      throw existingError;
    }

    const idsToDelete =
      existing
        ?.filter((row) => row.record_id && !larkIdSet.has(row.record_id))
        .map((row) => row.record_id) ?? [];

    // 4. Xoá bản ghi thừa (đã xoá khỏi Lark)
    if (idsToDelete.length > 0) {
      await supabase.from("TimDoSinhVien").delete().in("record_id", idsToDelete);
    }

    // 5. Upsert toàn bộ data từ Lark
    if (rows.length > 0) {
      const { error: upsertError } = await supabase
        .from("TimDoSinhVien")
        .upsert(rows, { onConflict: "record_id" });

      if (upsertError) {
        throw upsertError;
      }
    }

    return res
      .status(200)
      .json({ success: true, synced: rows.length, deleted: idsToDelete.length });
  } catch (err) {
    console.error("sync-lark-to-supabase error:", err);
    return res.status(500).json({ success: false, error: String(err) });
  }
}
