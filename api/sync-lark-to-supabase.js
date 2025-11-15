import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const LARK_APP_ID = process.env.LARK_APP_ID;
const LARK_APP_SECRET = process.env.LARK_APP_SECRET;
const LARK_APP_TOKEN = process.env.LARK_APP_TOKEN;
const LARK_TABLE_ID = process.env.LARK_TABLE_ID;

function normalizeText(input) {
  if (!input) return "";
  return input
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

// Lấy phần tử đầu nếu là array, còn không thì trả về chính nó
function pickFirst(value) {
  if (Array.isArray(value)) {
    return value.length > 0 ? value[0] : null;
  }
  if (value === undefined || value === null || value === "") return null;
  return value;
}

async function getLarkToken() {
  const res = await fetch(
    "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
    {
      method: "POST",
      headers: { "Content-Type": "application/json; charset=utf-8" },
      body: JSON.stringify({ app_id: LARK_APP_ID, app_secret: LARK_APP_SECRET }),
    }
  );

  const data = await res.json();
  if (data.code !== 0) throw new Error("Lark auth error: " + data.msg);
  return data.tenant_access_token;
}

async function fetchApprovedRecordsFromLark(token) {
  const items = [];
  let pageToken = "";

  do {
    const params = new URLSearchParams();
    params.set('filter', 'CurrentValue.[TrangThai] = "Đã duyệt"');
    params.set("page_size", "500");
    if (pageToken) params.set("page_token", pageToken);

    const url = `https://open.larksuite.com/open-apis/bitable/v1/apps/${LARK_APP_TOKEN}/tables/${LARK_TABLE_ID}/records?${params.toString()}`;

    const res = await fetch(url, {
      method: "GET",
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await res.json();
    if (data.code !== 0) throw new Error("Lark fetch error: " + data.msg);

    items.push(...(data.data.items || []));
    pageToken = data.data.has_more ? data.data.page_token : "";
  } while (pageToken);

  return items;
}

function mapRecordToSupabaseRow(rec) {
  const f = rec.fields || {};

  const title = pickFirst(f.TieuDe) || "";
  const loaiTinRaw = pickFirst(f.LoaiTin) || "";

  let type = null;
  if (loaiTinRaw === "Nhặt được") type = "found";
  else if (loaiTinRaw === "Mất đồ") type = "lost";
  else type = loaiTinRaw || null;

  let timeRaw = null;
  const timeText = pickFirst(f.ThoiGian);
  if (timeText) {
    const t = Date.parse(timeText);
    if (!Number.isNaN(t)) timeRaw = t;
  }

  // Multi-select / text fields
  const groupRaw = pickFirst(f.Group);
  const docTypeRaw = pickFirst(f.LoaiDo);
  const khuVucRaw = pickFirst(f.KhuVuc);

  // Tọa độ
  let lat = null;
  let lng = null;
  if (f.Latitude !== undefined && f.Latitude !== null && f.Latitude !== "") {
    const v = Number(f.Latitude);
    if (!Number.isNaN(v)) lat = v;
  }
  if (f.Longitude !== undefined && f.Longitude !== null && f.Longitude !== "") {
    const v = Number(f.Longitude);
    if (!Number.isNaN(v)) lng = v;
  }

  return {
    record_id: rec.record_id,
    name: title || null,
    description: pickFirst(f.MoTa) || null,
    image: pickFirst(f.HinhAnhURL) || null, // ảnh từ Lark
    type,
    group: groupRaw || null,
    docType: docTypeRaw || null,
    khuVuc: khuVucRaw || null,
    time: timeText || null,
    timeRaw,
    isPinned: !!f.Ghim,
    latitude: lat,
    longitude: lng,
    _name: normalizeText(title),
    _group: normalizeText(groupRaw),
    _docType: normalizeText(docTypeRaw),
    _khuVuc: normalizeText(khuVucRaw),
  };
}

export default async function handler(req, res) {
  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    const token = await getLarkToken();
    const items = await fetchApprovedRecordsFromLark(token);
    const rows = items.map(mapRecordToSupabaseRow);

    const larkIdSet = new Set(rows.map((r) => r.record_id));

    // 👉 Sync sang bảng cũ public_posts
    const { data: existing } = await supabase
      .from("public_posts")
      .select("record_id");

    const idsToDelete =
      existing?.filter((e) => !larkIdSet.has(e.record_id)).map((e) => e.record_id) ||
      [];

    if (idsToDelete.length > 0) {
      await supabase.from("public_posts").delete().in("record_id", idsToDelete);
    }

    if (rows.length > 0) {
      await supabase
        .from("public_posts")
        .upsert(rows, { onConflict: "record_id" });
    }

    return res
      .status(200)
      .json({ success: true, synced: rows.length, deleted: idsToDelete.length });
  } catch (err) {
    return res.status(500).json({ success: false, error: String(err) });
  }
}
