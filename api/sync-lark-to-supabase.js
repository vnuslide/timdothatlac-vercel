// File: /api/sync.js (ĐÃ SỬA LỖI DỌN DẸP DỮ LIỆU)

// Cần cài đặt: npm install node-fetch@2
const fetch = require('node-fetch');

// Lấy biến môi trường từ Vercel
const CFG = {
    APP_ID: process.env.LARK_APP_ID,
    APP_SECRET: process.env.LARK_APP_SECRET,
    BASE_TOKEN: process.env.LARK_BASE_TOKEN,
    TABLE_ID: process.env.LARK_TABLE_ID,
    HOST: 'https://open.larksuite.com',
    
    // (XÓA GIST) Chúng ta không cần Gist nữa nếu Trang Bản Đồ cũng đọc từ Supabase
    // GITHUB_TOKEN: process.env.GITHUB_TOKEN,
    // GIST_ID: process.env.GIST_ID,
    // GIST_FILENAME: process.env.GIST_FILENAME,
    
    TZ: 'Asia/Ho_Chi_Minh', 
    SUPABASE_URL: process.env.SUPABASE_URL,
    SUPABASE_KEY: process.env.SUPABASE_SERVICE_KEY,
    SUPABASE_TABLE: 'TimDoSinhVien' // Tên bảng của bạn
};

// Biến cache token (chỉ hoạt động trong 1 lần chạy)
let larkTokenCache = null;
let larkTokenExp = 0;

/* ------------------ (NODE.JS) CÁC HÀM LARKBASE ------------------- */
async function getTenantAccessToken_() {
    const now = Date.now();
    if (larkTokenCache && now < larkTokenExp) {
        return larkTokenCache;
    }
    
    const url = `${CFG.HOST}/open-apis/auth/v3/tenant_access_token/internal`;
    const payload = { app_id: CFG.APP_ID, app_secret: CFG.APP_SECRET };
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    const j = await res.json();
    if (j.code !== 0 || !j.tenant_access_token) {
        throw new Error('Lark auth error: ' + j.msg);
    }
    const token = j.tenant_access_token;
    const ttl = (j.expire || j.expire_in || 3600) - 120;
    larkTokenCache = token;
    larkTokenExp = now + ttl * 1000;
    return token;
}

async function bitableListAll_() {
    const token = await getTenantAccessToken_();
    let out = [];
    let pt = '';
    do {
        const base = `${CFG.HOST}/open-apis/bitable/v1/apps/${CFG.BASE_TOKEN}/tables/${CFG.TABLE_ID}/records`;
        const qs = [`page_size=500`]; 
        if (pt) qs.push(`page_token=${encodeURIComponent(pt)}`);
        const url = base + '?' + qs.join('&');
        
        const res = await fetch(url, {
            method: 'GET',
            headers: { 'Authorization': `Bearer ${token}` },
        });
        const j = await res.json();
        if (j.code !== 0) throw new Error('bitableListAll_ error: ' + j.msg);
        
        out = out.concat(j.data.items || []);
        pt = j.data.has_more ? j.data.page_token : '';
    } while (pt);
    return { items: out };
}

/* ------------------ (NODE.JS) CÁC HÀM HELPER (ĐÃ SỬA) ------------------- */
function normalizeText_(s) {
  if (!s) return '';
  s = String(s).toLowerCase();
  s = s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/đ/g, 'd');
  return s;
}

function formatDateYMD_(timestamp) {
    if (!timestamp) return '';
    const date = new Date(timestamp);
    // Vercel chạy ở UTC, chúng ta phải buộc múi giờ Việt Nam
    const options = { timeZone: CFG.TZ, year: 'numeric', month: '2-digit', day: '2-digit' };
    // Định dạng en-CA (yyyy-MM-dd) sau đó thay thế
    return new Intl.DateTimeFormat('en-CA', options).format(date).replace(/-/g, '/');
}

const VN_MAP = {
  FOUND: ['nhặt được', 'nhat duoc', 'found'],
  LOST:  ['mất', 'mat', 'lost', 'tìm đồ', 'tim do']
};

/**
 * (ĐÃ SỬA) Hàm chuyển đổi cho Supabase
 */
function publicRecordForSupabase_(rec) {
  const f = rec.fields || {};

  // (FIX 3) Xử lý Time/TimeRaw
  const timeStr = f.ThoiGian ? formatDateYMD_(f.ThoiGian) : '';
  const timeRaw = f.ThoiGian || (timeStr ? new Date(timeStr).getTime() : 0);
  
  const typeRaw = (f.LoaiTin || '').toString().toLowerCase();
  const type = VN_MAP.FOUND.some(x => typeRaw.includes(x)) ? 'found' : 'lost';

  const name = f.TieuDe || '';
  
  // (FIX 1) Flatten mảng Group
  const group = (f.Group && Array.isArray(f.Group) && f.Group.length > 0) ? f.Group[0] : '';
  
  const description = f.MoTa || '';
  
  // (FIX 2) Join mảng LoaiDo
  const loaiDoArray = (f.LoaiDo && Array.isArray(f.LoaiDo)) ? f.LoaiDo : [];
  const docType = loaiDoArray.join(', '); 

  const khuVuc = f.KhuVuc || '';
  const originalImage = f.HinhAnhURL || null; 

  return {
    record_id: rec.record_id, 
    time: timeStr, // Chuỗi (VD: 2025/11/14)
    timeRaw: timeRaw, // Số (VD: 1743696600000)
    name,
    "group": group, // Chuỗi (VD: "USSH")
    description: description,
    docType: docType, // Chuỗi (VD: "Thẻ sinh viên, Ví")
    khuVuc: khuVuc,
    image: originalImage, // Link ảnh (Frontend đọc cột này)
    type: type,
    isPinned: f.Ghim === true,
    latitude: f.Latitude || null,
    longitude: f.Longitude || null,
    _name: normalizeText_(name),
    _group: normalizeText_(group),
    _docType: normalizeText_(docType),
    _khuVuc: normalizeText_(khuVuc),
    // (MỚI) Thêm các cột bị thiếu (nếu bạn đã thêm chúng)
    status: f.TrangThai || 'Chờ duyệt',
    email: f.EmailNguoiDang || null,
    lienHe: f.LienHe || null,
    linkFacebook: f.LinkFacebook || null
  };
}

/* ------------------ (NODE.JS) HÀM GỌI API BÊN NGOÀI ------------------- */
async function supabaseFetch(endpoint, options) {
    const url = `${CFG.SUPABASE_URL}/rest/v1/${endpoint}`;
    const headers = {
        'apikey': CFG.SUPABASE_KEY,
        'Authorization': `Bearer ${CFG.SUPABASE_KEY}`,
        'Content-Type': 'application/json',
        ...options.headers,
    };
    const res = await fetch(url, { ...options, headers });
    return res;
}

/* ------------------ (NODE.JS) HÀM SYNC CHÍNH (FIX LỖI 23505) ------------------- */
// Đây là hàm được Vercel gọi mỗi 5 phút
export default async function handler(request, response) {
    console.log('🚀 Bắt đầu đồng bộ Larkbase -> Supabase (Vercel)');
    
    try {
        // 1. LẤY TẤT CẢ DỮ LIỆU TỪ LARKBASE
        const allLarkItems = await bitableListAll_();
        const larkData = allLarkItems.items || [];
        console.log(`Lấy được ${larkData.length} tin từ Larkbase.`);

        // 2. CHUẨN BỊ DỮ LIỆU ĐỂ SYNC (Đã dùng hàm dọn dẹp mới)
        const dataToSync = larkData.map(publicRecordForSupabase_);
        const larkIds = new Set(dataToSync.map(r => r.record_id));
        
        // 3. LẤY ID HIỆN CÓ TRONG SUPABASE
        const res = await supabaseFetch(`${CFG.SUPABASE_TABLE}?select=record_id`, { method: 'GET' });
        if (!res.ok) throw new Error(await res.text());
        const existingRows = await res.json();
        const supabaseIds = new Set(existingRows.map(r => r.record_id));
        
        // 4. TÌM BẢN GHI CẦN XÓA (Có trong Supabase nhưng không có trong Lark)
        const idsToDelete = [...supabaseIds].filter(id => !larkIds.has(id));
        
        // 5. THỰC HIỆN XÓA (NẾU CẦN)
        if (idsToDelete.length > 0) {
            console.log(`Đang xóa ${idsToDelete.length} bản ghi thừa...`);
            const deleteRes = await supabaseFetch(
                `${CFG.SUPABASE_TABLE}?record_id=in.(${idsToDelete.join(',')})`, 
                { method: 'DELETE' }
            );
            if (!deleteRes.ok) {
                 console.error('Lỗi khi xóa Supabase:', await deleteRes.text());
            }
        }

        // 6. THỰC HIỆN UPSERT (Cập nhật hoặc Thêm mới)
        if (dataToSync.length > 0) {
            console.log(`Đang UPSERT ${dataToSync.length} bản ghi...`);
            const upsertRes = await supabaseFetch(CFG.SUPABASE_TABLE, {
                method: 'POST',
                headers: { 'Prefer': 'resolution=merge-duplicates' }, // Tự động cập nhật nếu record_id tồn tại
                body: JSON.stringify(dataToSync)
            });
            if (!upsertRes.ok) {
                // Lỗi này (23505) sẽ không xảy ra nữa vì chúng ta dùng UPSERT
                console.error('Lỗi khi UPSERT Supabase:', await upsertRes.text());
            }
        }

        console.log('✅ Đồng bộ Vercel hoàn tất.');
        response.status(200).send({ success: true, message: 'Sync complete.' });

    } catch (e) {
        console.error('❌ Lỗi nghiêm trọng trong Vercel Sync:', e);
        response.status(500).send({ success: false, error: e.message });
    }
}
