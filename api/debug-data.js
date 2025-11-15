// api/debug-data.js
// Endpoint để xem sample data sau khi sync

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

export default async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    // Lấy 5 records đầu tiên từ Supabase
    const resp = await fetch(
      `${SUPABASE_URL}/rest/v1/TimDoSinhVien?select=*&limit=5`,
      {
        headers: {
          apikey: SUPABASE_SERVICE_KEY,
          Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
          "Content-Type": "application/json",
        },
      }
    );

    if (!resp.ok) {
      const text = await resp.text();
      return res.status(500).json({
        error: `Supabase error: ${resp.status} ${text}`,
      });
    }

    const data = await resp.json();

    // Phân tích data types
    const analysis = data.map((row) => ({
      record_id: row.record_id,
      name: row.name,
      group: {
        value: row.group,
        type: typeof row.group,
        isArray: Array.isArray(row.group),
      },
      docType: {
        value: row.docType,
        type: typeof row.docType,
        isArray: Array.isArray(row.docType),
      },
      khuVuc: {
        value: row.khuVuc,
        type: typeof row.khuVuc,
        isArray: Array.isArray(row.khuVuc),
      },
      image: {
        value: row.image,
        type: typeof row.image,
        hasValue: !!row.image,
      },
      time: {
        value: row.time,
        type: typeof row.time,
      },
      timeRaw: {
        value: row.timeRaw,
        type: typeof row.timeRaw,
      },
    }));

    return res.status(200).json({
      success: true,
      count: data.length,
      rawData: data,
      analysis,
    });
  } catch (err) {
    console.error("Debug error:", err);
    return res.status(500).json({
      error: err?.message || "Unknown error",
    });
  }
}
