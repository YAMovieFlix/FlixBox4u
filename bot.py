import os, sys, re, asyncio, threading, random, string, time, requests, json, base64, socket, io, traceback, gzip, hashlib, secrets, html
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from datetime import datetime, timedelta
try:
    from PIL import Image, ImageDraw, ImageFont
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

sys.stdout.reconfigure(line_buffering=True)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from pyrogram import Client, filters, idle, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, MenuButtonWebApp, BotCommand
from pyrogram.errors import UserNotParticipant, FloodWait
from flask import Flask, render_template_string, jsonify, request, Response, redirect, send_file
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from bson.objectid import ObjectId

processed_msgs, processed_cbs = set(), set()

def is_unique_func(_, __, m):
    if not hasattr(m, "id") or not hasattr(m, "chat"): return True
    k = (m.chat.id, m.id)
    if k in processed_msgs: return False
    processed_msgs.add(k)
    if len(processed_msgs) > 10000: processed_msgs.clear()
    return True
unique_msg = filters.create(is_unique_func)

def is_unique_cb(_, __, q):
    if not hasattr(q, "id"): return True
    if q.id in processed_cbs: return False
    processed_cbs.add(q.id)
    if len(processed_cbs) > 10000: processed_cbs.clear()
    return True
unique_cb = filters.create(is_unique_cb)

# ==========================================
# 1. CONFIGURATION & AUTO WEB_URL
# ==========================================
API_ID = int(os.environ.get("API_ID", 29904834))
API_HASH = os.environ.get("API_HASH", "8b4fd9ef578af114502feeafa2d31938")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://drama:drama@cluster0.sa4kvgu.mongodb.net/?appName=Cluster0")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 7120801813))

def detect_web_url():
    for k in ["WEB_URL", "RENDER_EXTERNAL_URL"]:
        if os.environ.get(k, "").strip(): return os.environ[k].strip().rstrip("/")
    if os.environ.get("KOYEB_PUBLIC_DOMAIN"): return f"https://{os.environ['KOYEB_PUBLIC_DOMAIN'].strip()}".rstrip("/")
    if os.environ.get("VERCEL_URL"): return f"https://{os.environ['VERCEL_URL'].strip()}".rstrip("/")
    return "https://resident-chris-akashvaikh-0a1f684c.apps.mistral.run"

WEB_URL = detect_web_url()
DEFAULT_ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "Sudo_king")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "FlixBox4uBot")
DEFAULT_ZONE_ID = os.environ.get("MONETAG_ZONE_ID", "").strip()

# ==========================================
# 2. DATABASE & MULTI-MONGO (NewBot Preserved)
# ==========================================
app = Client("shilacall_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN or "0:dummy", in_memory=True)
web = Flask(__name__)
admin_steps = {}

db_client, db = None, None
users_col, files_col, cats_col, langs_col, tasks_col, titles_col = None, None, None, None, None, None
pkgs_col, links_col, config_col, channels_col, coupons_col, mongos_col = None, None, None, None, None, None
auto_channels_col, promotions_col, zone_ids_col, upcoming_col = None, None, None, None
posters_col, short_links_col, tutorials_col = None, None, None
sync_db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)["NewBot"]

POSTER_CACHE = {}
BRANDED_POSTER_CACHE = {}
BRANDED_COVER_ID_CACHE = {}
EXTRA_URI_CACHE = {"uris": None, "ts": 0}
HOME_DATA_CACHE = {"data": None, "ts": 0, "ver": 0}
SHELL_DATA_CACHE = {"data": None, "ts": 0, "ver": 0}
AD_TOKENS = {}
AD_TOKENS_LOCK = threading.Lock()
WEB_POOL = ThreadPoolExecutor(max_workers=10)
TUTORIAL_DL_LOCK = threading.Lock()
HOME_CACHE_LOCK = threading.Lock()
TG_HTTP = requests.Session()
NON_VIDEO_FILE_IDS = set()
FILE_DB_IDX_CACHE = {}
bg_task_ref = None

def get_db_cache_ver():
    try:
        c = sync_db["config"].find_one({"_id": "settings"}, {"cache_ver": 1}) or {}
        return float(c.get("cache_ver", 0) or 0)
    except:
        return 0.0

def invalidate_home_cache():
    now = time.time()
    HOME_DATA_CACHE["data"] = None
    HOME_DATA_CACHE["ts"] = 0
    HOME_DATA_CACHE["ver"] = 0
    SHELL_DATA_CACHE["data"] = None
    SHELL_DATA_CACHE["ts"] = 0
    SHELL_DATA_CACHE["ver"] = 0
    try:
        sync_db["config"].update_one({"_id": "settings"}, {"$set": {"cache_ver": now}}, upsert=True)
    except:
        pass

def poster_cache_put(pid, raw_bytes, mime="image/jpeg", ver=""):
    if not pid or not raw_bytes: return
    if len(POSTER_CACHE) >= 250:
        try: POSTER_CACHE.pop(next(iter(POSTER_CACHE)))
        except: POSTER_CACHE.clear()
    POSTER_CACHE[pid] = (raw_bytes, mime, str(ver or ""))

def poster_cache_del(pid):
    POSTER_CACHE.pop(pid, None)

def get_live_web_url(conf=None):
    for k in ["WEB_URL", "RENDER_EXTERNAL_URL"]:
        if os.environ.get(k, "").strip(): return os.environ[k].strip().rstrip("/")
    if conf and conf.get("auto_web_url"): return conf["auto_web_url"].rstrip("/")
    return WEB_URL

def get_relock_seconds(conf):
    if not conf:
        return 60
    if "vid_relock_sec" in conf and conf["vid_relock_sec"] is not None:
        return int(conf["vid_relock_sec"])
    if conf.get("vid_relock_min", 0) > 0:
        return int(conf["vid_relock_min"]) * 60
    return 0

@web.before_request
def capture_live_host():
    try:
        if request.path.startswith("/poster/") or request.path in ("/tutorial_video", "/site_logo", "/api/welcome_audio"):
            return
        host = request.headers.get("X-Forwarded-Host") or request.host
        if host and "127.0.0.1" not in host and "localhost" not in host:
            proto = request.headers.get("X-Forwarded-Proto", "https")
            if proto != "https": proto = "https"
            live = f"{proto}://{host}".rstrip("/")
            global WEB_URL
            if live != WEB_URL:
                WEB_URL = live
                sync_db["config"].update_one({"_id": "settings"}, {"$set": {"auto_web_url": live}}, upsert=True)
    except: pass

@web.after_request
def compress_response(response):
    try:
        if (
            response.status_code == 200
            and not response.direct_passthrough
            and response.mimetype in ("text/html", "application/json", "image/svg+xml")
            and "gzip" in request.headers.get("Accept-Encoding", "").lower()
            and "Content-Encoding" not in response.headers
        ):
            data = response.get_data()
            if len(data) > 512:
                gz = gzip.compress(data, compresslevel=5)
                response.set_data(gz)
                response.headers["Content-Encoding"] = "gzip"
                response.headers["Content-Length"] = str(len(gz))
                response.headers["Vary"] = "Accept-Encoding"
    except: pass
    return response

async def get_db():
    global db_client, db, users_col, files_col, cats_col, langs_col, pkgs_col, links_col, config_col
    global channels_col, coupons_col, mongos_col, tasks_col, titles_col, auto_channels_col, promotions_col, zone_ids_col, upcoming_col
    global posters_col, short_links_col, tutorials_col
    if db_client is None:
        db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000); db = db_client["NewBot"]
        users_col, files_col, cats_col, langs_col = db["users"], db["files"], db["categories"], db["languages"]
        pkgs_col, links_col, config_col = db["packages"], db["ad_links"], db["config"]
        channels_col, coupons_col, mongos_col = db["channels"], db["coupons"], db["mongos"]
        tasks_col, titles_col = db["custom_tasks"], db["auto_titles"]
        auto_channels_col, promotions_col, zone_ids_col, upcoming_col = db["auto_channels"], db["promotions"], db["zone_ids"], db["upcoming"]
        posters_col, short_links_col, tutorials_col = db["posters"], db["short_links"], db["tutorials"]
    return db

async def get_config():
    await get_db()
    conf = await config_col.find_one({"_id": "settings"})
    if not conf:
        conf = {
            "_id": "settings", "ref_coin": 10, "ref_on": True, "auto_del_time": 0,
            "autodel_text": "⏳ ফাইলটি নির্দিষ্ট সময় পর অটো ডিলিট হয়ে যাবে।", "frotect": False,
            "ads_on": True, "direk_wait": [5], "prem_vid_wait": 10, "task_wait_sec": 10, "monetag_wait": 4,
            "strict_ad": True, "blur_sec": 20, "brand_name": "",
            "prstep": 1, "regstep": 1, "task_step": 1,
            "start_logo": None, "start_text": "", "payment_admin": DEFAULT_ADMIN_USERNAME,
            "autovid_msg_id": None, "autovid_chat_id": None, "autovid_time_min": 0,
            "autopost_time_hr": 0, "autopost_idx": 0, "site_name": "Moviee Link",
            "premium_vid_coin": 50, "vid_relock_sec": 60, "vid_relock_min": 0, "spin_coin_min": 10, "spin_coin_max": 50,
            "task_coin": 20, "spin_limit": 5, "task_limit": 5, "autopost_channel": None,
            "autopost_minute": 0, "autopost_channel_idx": 0, "tutorial_vid": None,
            "rec_group_link": "", "cache_ver": time.time(),
            "marquee_text": "VLC Player অথবা MX Player দিয়ে চালিয়ে দেখুন। ✅ সাউন্ড/অডিও সমস্যা হলে ভিডিওটি ডাউনলোড করে দেখুন।"
        }
        await config_col.insert_one(conf)
    return conf

async def delete_msg_later(client, chat_id, msg_id, delay):
    await asyncio.sleep(delay)
    try: await client.delete_messages(chat_id, msg_id)
    except: pass

def get_expiry_str(expiry_date):
    if not expiry_date or expiry_date <= datetime.now(): return None
    sec = int((expiry_date - datetime.now()).total_seconds())
    y, sec = divmod(sec, 31536000); mo, sec = divmod(sec, 2592000); w, sec = divmod(sec, 604800)
    d, sec = divmod(sec, 86400); h, sec = divmod(sec, 3600); m, sec = divmod(sec, 60)
    res = []
    if y: res.append(f"{y} বছর/y")
    if mo: res.append(f"{mo} মাস/mo")
    if w: res.append(f"{w} সপ্তাহ/w")
    if d: res.append(f"{d} দিন/d")
    if h: res.append(f"{h} ঘণ্টা/h")
    if m: res.append(f"{m} মিনিট/m")
    if sec or not res: res.append(f"{sec} সেকেন্ড/s")
    return " ".join(res)

def send_tg_api_fallback(chat_id, text):
    try:
        if getattr(app, "is_connected", False):
            asyncio.run_coroutine_threadsafe(app.send_message(chat_id, text), loop); return
    except: pass
    if BOT_TOKEN:
        try: requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=5)
        except: pass

def send_tg_tutorial_sync(chat_id, vid, title="Tutorial Video"):
    if not vid:
        return False
    cap = f"🎬 **{title}**\n\n✅ কিভাবে ভিডিও দেখবেন ও ডাউনলোড করবেন তা এখানে দেখানো হয়েছে।"
    try:
        if getattr(app, "is_connected", False):
            if str(vid).startswith("http"):
                asyncio.run_coroutine_threadsafe(app.send_message(chat_id, f"{cap}\n🔗 {vid}"), loop)
            else:
                asyncio.run_coroutine_threadsafe(app.send_video(chat_id, video=vid, caption=cap), loop)
            return True
    except: pass
    if BOT_TOKEN:
        try:
            if str(vid).startswith("http"):
                r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": f"{cap}\n{vid}"}, timeout=8).json()
            else:
                r = requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", json={"chat_id": chat_id, "video": vid, "caption": cap}, timeout=10).json()
            return bool(r.get("ok"))
        except: pass
    return False

def keep_alive():
    while True:
        try: time.sleep(60); requests.get(WEB_URL, timeout=10)
        except: pass

async_mongo_clients, sync_mongo_clients = {}, {}

async def get_extra_dbs_async():
    await get_db(); dbs = [db]
    async for m in mongos_col.find():
        uri = m["uri"]
        if uri not in async_mongo_clients:
            try: async_mongo_clients[uri] = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=3000)["NewBot"]
            except: continue
        dbs.append(async_mongo_clients[uri])
    return dbs

def get_extra_dbs_sync():
    dbs = [sync_db]
    try:
        now = time.time()
        if EXTRA_URI_CACHE["uris"] is None or (now - EXTRA_URI_CACHE["ts"]) > 30:
            EXTRA_URI_CACHE["uris"] = [m["uri"] for m in sync_db["mongos"].find()]
            EXTRA_URI_CACHE["ts"] = now
        for uri in EXTRA_URI_CACHE["uris"]:
            if uri not in sync_mongo_clients:
                try: sync_mongo_clients[uri] = MongoClient(uri, serverSelectionTimeoutMS=3000)["NewBot"]
                except: continue
            dbs.append(sync_mongo_clients[uri])
    except: pass
    return dbs

async def find_file_in_all_dbs(f_id):
    dbs = await get_extra_dbs_async()
    key = str(f_id)

    async def _find_in(d):
        try:
            doc = await d["files"].find_one({"_id": f_id})
            if doc is not None:
                return doc, d
            if isinstance(f_id, str) and ObjectId.is_valid(f_id):
                doc = await d["files"].find_one({"_id": ObjectId(f_id)})
                if doc is not None:
                    return doc, d
        except:
            pass
        return None, None

    cached_idx = FILE_DB_IDX_CACHE.get(key)
    if cached_idx is not None and 0 <= cached_idx < len(dbs):
        doc, d = await _find_in(dbs[cached_idx])
        if doc is not None:
            return doc, d

    results = await asyncio.gather(*[_find_in(d) for d in dbs])
    for idx, (doc, d) in enumerate(results):
        if doc is not None:
            if len(FILE_DB_IDX_CACHE) >= 3000:
                FILE_DB_IDX_CACHE.clear()
            FILE_DB_IDX_CACHE[key] = idx
            return doc, d
    return None, None

async def get_all_files_across_dbs(query_filter=None):
    all_files, q = [], query_filter or {}
    for d in await get_extra_dbs_async():
        try: all_files.extend(await d["files"].find(q).sort("_id", -1).to_list(None))
        except: pass
    return all_files

def _bot_api_download_sync(file_id):
    gf = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": file_id}, timeout=10).json()
    if not gf.get("ok") or not gf.get("result", {}).get("file_path"):
        return None
    fpath = gf["result"]["file_path"]
    r = requests.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fpath}", timeout=15)
    if r.status_code == 200 and r.content:
        return r.content
    return None

def ensure_tutorial_cached_sync(vid):
    if not vid or str(vid).startswith("http"):
        return None
    vid_hash = hashlib.md5(str(vid).encode()).hexdigest()[:14]
    cache_path = f"/tmp/tutorial_{vid_hash}.mp4"
    if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
        return cache_path
    with TUTORIAL_DL_LOCK:
        if os.path.exists(cache_path) and os.path.getsize(cache_path) > 0:
            return cache_path
        tmp_mp4 = f"/tmp/tut_tmp_{vid_hash}.mp4"
        if BOT_TOKEN:
            try:
                gf = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile", params={"file_id": vid}, timeout=15).json()
                if gf.get("ok") and gf["result"].get("file_path"):
                    fpath = gf["result"]["file_path"]
                    vr = requests.get(f"https://api.telegram.org/file/bot{BOT_TOKEN}/{fpath}", stream=True, timeout=120)
                    if vr.status_code == 200:
                        with open(tmp_mp4, "wb") as f:
                            for ch in vr.iter_content(chunk_size=65536):
                                if ch: f.write(ch)
                        if os.path.exists(tmp_mp4) and os.path.getsize(tmp_mp4) > 0:
                            os.replace(tmp_mp4, cache_path)
                            return cache_path
            except Exception as e:
                print("Bot API tutorial download fallback:", e, flush=True)
        if getattr(app, "is_connected", False):
            try:
                fut = asyncio.run_coroutine_threadsafe(app.download_media(vid, file_name=tmp_mp4), loop)
                res_p = fut.result(timeout=180)
                if res_p and os.path.exists(res_p) and os.path.getsize(res_p) > 0:
                    os.replace(res_p, cache_path)
                    return cache_path
            except Exception as e:
                print("Pyrogram tutorial download error:", e, flush=True)
    return cache_path if (os.path.exists(cache_path) and os.path.getsize(cache_path) > 0) else None

async def record_daily_unique_view(tdb, f_id, uid):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        u = await users_col.find_one({"_id": uid}, {"daily_viewed": 1}) or {}
        dv = u.get("daily_viewed", {})
        today_list = dv.get(today, [])
        if f_id in today_list:
            return False
        new_dv = {today: today_list + [f_id]}
        await users_col.update_one({"_id": uid}, {"$set": {"daily_viewed": new_dv}}, upsert=True)
        await tdb["files"].update_one({"_id": f_id}, {"$inc": {"views": 1}})
        if HOME_DATA_CACHE["data"] and HOME_DATA_CACHE["data"].get("files"):
            for it in HOME_DATA_CACHE["data"]["files"]:
                if it.get("_id") == f_id:
                    it["views"] = (it.get("views", 0) or 0) + 1
                    break
        return True
    except Exception as e:
        print("record_daily_unique_view error:", e, flush=True)
        return False

async def extract_image_from_message(client, msg):
    try:
        media = msg.photo or msg.document
        if not media:
            return None, "image/jpeg", None, "❌ ছবি পাওয়া যায়নি!"
        if msg.document and msg.document.mime_type and not msg.document.mime_type.startswith("image/"):
            return None, "image/jpeg", None, "❌ দয়া করে একটি ছবি (Photo/Image) পাঠান!"
        raw = None
        if BOT_TOKEN:
            try:
                raw = await asyncio.to_thread(_bot_api_download_sync, media.file_id)
            except Exception as e0:
                print("Bot API photo download fallback:", e0, flush=True)
        if not raw:
            try:
                bio = await asyncio.wait_for(client.download_media(media.file_id, in_memory=True), timeout=20)
                if bio is not None:
                    raw = bio.getvalue() if hasattr(bio, "getvalue") else (bytes(bio) if isinstance(bio, (bytes, bytearray)) else None)
            except Exception as e1:
                print("Pyrogram in_memory download fallback:", e1, flush=True)
        if not raw:
            try:
                path = await asyncio.wait_for(msg.download(), timeout=20)
                if path and os.path.exists(path):
                    with open(path, "rb") as f: raw = f.read()
                    try: os.remove(path)
                    except: pass
            except Exception as e2:
                print("Pyrogram disk download fallback:", e2, flush=True)
        if not raw:
            return None, "image/jpeg", None, "❌ ছবি ডাউনলোড করা যায়নি, আবার চেষ্টা করুন!"
        if len(raw) > 10 * 1024 * 1024:
            return None, "image/jpeg", None, "❌ ছবির সাইজ 10MB এর বেশি হতে পারবে না!"
        mime = "image/jpeg"
        if msg.document and msg.document.mime_type:
            mime = "image/jpeg" if msg.document.mime_type == "image/jpg" else msg.document.mime_type
        tg_file_id = msg.photo.file_id if msg.photo else None
        return base64.b64encode(raw).decode("utf-8"), mime, tg_file_id, None
    except Exception as e:
        print("extract_image_from_message error:", e, flush=True)
        traceback.print_exc()
        return None, "image/jpeg", None, f"❌ ইমেজ প্রসেসিং এরর: {e}"

async def save_poster_to_mongo(target_db, poster_id, b64_data, mime="image/jpeg", live_url=None):
    if mime == "image/jpg": mime = "image/jpeg"
    ver = int(time.time())
    payload = {"data": b64_data, "mime": mime, "ver": ver, "created_at": datetime.utcnow().isoformat() + "Z"}
    for d in await get_extra_dbs_async():
        if d is not target_db:
            try: await d["posters"].delete_one({"_id": poster_id})
            except: pass
    try:
        await target_db["posters"].update_one({"_id": poster_id}, {"$set": payload}, upsert=True)
    except Exception as e:
        print("save_poster_to_mongo target_db error, falling back to main db:", e, flush=True)
        await get_db()
        await db["posters"].update_one({"_id": poster_id}, {"$set": payload}, upsert=True)
    poster_cache_del(poster_id)
    try:
        raw, _ = decode_b64_to_bytes(b64_data)
        if raw: poster_cache_put(poster_id, raw, mime, ver=str(ver))
    except: pass
    invalidate_home_cache()
    base = (live_url or get_live_web_url()).rstrip("/")
    return f"{base}/poster/{poster_id}?v={ver}"

def decode_b64_to_bytes(val):
    if not val: return None, "image/jpeg"
    mime = "image/jpeg"
    if isinstance(val, (bytes, bytearray)):
        return bytes(val), mime
    if isinstance(val, str) and val.startswith("data:image"):
        try:
            head, b64 = val.split(",", 1)
            m = re.search(r"data:(image/[^;]+);", head)
            if m: mime = "image/jpeg" if m.group(1) == "image/jpg" else m.group(1)
            return base64.b64decode(b64), mime
        except: return None, mime
    try: return base64.b64decode(val), mime
    except: return None, mime

def make_clean_placeholder_svg():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="640" height="360" viewBox="0 0 640 360">
  <defs><linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#141624"/><stop offset="100%" stop-color="#271b3d"/></linearGradient></defs>
  <rect width="640" height="360" fill="url(#g)"/>
</svg>"""
    return svg.encode("utf-8")

def _load_brand_font(size):
    for fp in (
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
    ):
        if os.path.exists(fp):
            try: return ImageFont.truetype(fp, size)
            except: pass
    try: return ImageFont.load_default(size=size)
    except: return ImageFont.load_default()

def apply_brand_watermark_sync(raw_bytes, mime, brand_name):
    text = (brand_name or "").strip()
    if not raw_bytes or not text or not HAS_PIL:
        return raw_bytes, mime
    try:
        img = Image.open(io.BytesIO(raw_bytes)).convert("RGBA")
        w, h = img.size
        if w < 60 or h < 40:
            return raw_bytes, mime

        overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
        od = ImageDraw.Draw(overlay)
        margin_x = max(8, int(w * 0.022))
        margin_y = max(8, int(h * 0.032))
        font_size = max(14, int(h * 0.045))

        while font_size > 10:
            font = _load_brand_font(font_size)
            bbox = od.textbbox((0, 0), text, font=font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            pad_x = max(8, int(font_size * 0.65))
            dot_r = max(3, int(font_size * 0.22))
            gap = max(5, int(font_size * 0.42))
            box_w = pad_x + (dot_r * 2) + gap + tw + pad_x
            if box_w <= w - (margin_x * 2):
                break
            font_size -= 2

        pad_y = max(5, int(font_size * 0.42))
        box_h = pad_y + th + pad_y
        x1 = margin_x
        y2 = h - margin_y
        y1 = max(2, y2 - box_h)
        x2 = min(w - 2, x1 + box_w)
        rad = max(8, int(box_h * 0.45))

        od.rounded_rectangle([x1 - 2, y1 - 2, x2 + 2, y2 + 2], radius=rad + 2, fill=(124, 58, 237, 70))
        od.rounded_rectangle(
            [x1, y1, x2, y2],
            radius=rad,
            fill=(10, 12, 24, 212),
            outline=(255, 215, 0, 200),
            width=max(1, int(font_size * 0.08))
        )

        dot_cx = x1 + pad_x + dot_r
        dot_cy = (y1 + y2) // 2
        od.ellipse([dot_cx - dot_r - 2, dot_cy - dot_r - 2, dot_cx + dot_r + 2, dot_cy + dot_r + 2], fill=(255, 183, 3, 95))
        od.ellipse([dot_cx - dot_r, dot_cy - dot_r, dot_cx + dot_r, dot_cy + dot_r], fill=(255, 95, 45, 255))

        tx = dot_cx + dot_r + gap - bbox[0]
        ty = ((y1 + y2 - th) // 2) - bbox[1]
        od.text((tx, ty), text, font=font, fill=(255, 255, 255, 255))

        out = Image.alpha_composite(img, overlay).convert("RGB")
        buf = io.BytesIO()
        out.save(buf, format="JPEG", quality=90)
        return buf.getvalue(), "image/jpeg"
    except:
        return raw_bytes, mime

def get_poster_bytes_sync(file_doc):
    if not file_doc:
        return None, "image/jpeg"
    fid = str(file_doc.get("_id") or "")
    thumb = file_doc.get("thumb_url") or ""
    pid = thumb.split("/poster/", 1)[1].split("?")[0] if "/poster/" in thumb else fid
    if pid and pid != "default":
        req_v = thumb.split("?v=", 1)[1].split("&")[0] if "?v=" in thumb else ""
        if pid in POSTER_CACHE:
            c_item = POSTER_CACHE[pid]
            if not req_v or (len(c_item) > 2 and c_item[2] == req_v):
                return c_item[0], c_item[1]
        for d in get_extra_dbs_sync():
            try:
                pdoc = d["posters"].find_one({"_id": pid})
                if pdoc and pdoc.get("data"):
                    raw, mime = decode_b64_to_bytes(pdoc["data"])
                    if raw:
                        final_mime = pdoc.get("mime", mime)
                        poster_cache_put(pid, raw, final_mime, ver=str(pdoc.get("ver", "")))
                        return raw, final_mime
            except: pass
    if thumb.startswith("data:image"):
        raw, mime = decode_b64_to_bytes(thumb)
        if raw:
            return raw, mime
    if thumb.startswith("http") and "/poster/" not in thumb:
        try:
            r = requests.get(thumb, timeout=8)
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if r.status_code == 200 and r.content and ct.startswith("image/"):
                return r.content, ("image/jpeg" if ct == "image/jpg" else ct)
        except: pass
    if file_doc.get("tg_thumb_id") and BOT_TOKEN:
        try:
            raw = _bot_api_download_sync(file_doc["tg_thumb_id"])
            if raw:
                return raw, "image/jpeg"
        except: pass
    return None, "image/jpeg"

def send_video_with_cover_sync(uid, f_id_send, cap_html, protect, file_doc, brand_name=""):
    if not BOT_TOKEN or not file_doc or f_id_send in NON_VIDEO_FILE_IDS:
        return None
    brand = (brand_name or "").strip()
    fid = str(file_doc.get("_id") or "")
    thumb_ver = str(file_doc.get("thumb_url") or file_doc.get("tg_thumb_id") or "")
    cache_key = (fid, brand, thumb_ver)

    def _post_json_cover(cover_id):
        payload = {
            "chat_id": uid,
            "video": f_id_send,
            "cover": cover_id,
            "caption": cap_html,
            "parse_mode": "HTML",
            "supports_streaming": True,
            "protect_content": bool(protect)
        }
        r = TG_HTTP.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", json=payload, timeout=12).json()
        if r.get("ok") and r.get("result", {}).get("message_id"):
            return r["result"]["message_id"], False
        desc = str(r.get("description", "")).lower()
        if r.get("error_code") == 400 and any(k in desc for k in ("document", "audio", "wrong", "type", "video")):
            if "cover" not in desc:
                NON_VIDEO_FILE_IDS.add(f_id_send)
                return None, True
        return None, False

    def _post_multipart_cover(raw_b, mime_t):
        ext = "png" if mime_t == "image/png" else "jpg"
        data = {
            "chat_id": str(uid),
            "video": f_id_send,
            "cover": "attach://cover",
            "caption": cap_html,
            "parse_mode": "HTML",
            "supports_streaming": "true",
            "protect_content": "true" if protect else "false"
        }
        files = {"cover": (f"cover.{ext}", raw_b, mime_t)}
        r = TG_HTTP.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendVideo", data=data, files=files, timeout=15).json()
        if r.get("ok") and r.get("result", {}).get("message_id"):
            covers = r.get("result", {}).get("video", {}).get("cover")
            if isinstance(covers, list) and covers and covers[-1].get("file_id"):
                if len(BRANDED_COVER_ID_CACHE) >= 500:
                    try: BRANDED_COVER_ID_CACHE.pop(next(iter(BRANDED_COVER_ID_CACHE)))
                    except: BRANDED_COVER_ID_CACHE.clear()
                BRANDED_COVER_ID_CACHE[cache_key] = covers[-1]["file_id"]
            return r["result"]["message_id"]
        desc = str(r.get("description", "")).lower()
        if r.get("error_code") == 400 and any(k in desc for k in ("document", "audio", "wrong", "type", "video")):
            if "cover" not in desc:
                NON_VIDEO_FILE_IDS.add(f_id_send)
        return None

    cached_cov = BRANDED_COVER_ID_CACHE.get(cache_key)
    if cached_cov:
        try:
            mid, is_non_vid = _post_json_cover(cached_cov)
            if mid or is_non_vid:
                return mid
            BRANDED_COVER_ID_CACHE.pop(cache_key, None)
        except:
            pass

    if brand and HAS_PIL:
        b_item = BRANDED_POSTER_CACHE.get(cache_key)
        if not b_item:
            raw_bytes, mime = get_poster_bytes_sync(file_doc)
            if raw_bytes:
                b_raw, b_mime = apply_brand_watermark_sync(raw_bytes, mime, brand)
                if len(BRANDED_POSTER_CACHE) >= 200:
                    try: BRANDED_POSTER_CACHE.pop(next(iter(BRANDED_POSTER_CACHE)))
                    except: BRANDED_POSTER_CACHE.clear()
                b_item = (b_raw, b_mime)
                BRANDED_POSTER_CACHE[cache_key] = b_item
        if b_item:
            try:
                return _post_multipart_cover(b_item[0], b_item[1])
            except:
                pass
        return None

    tg_cover = file_doc.get("tg_thumb_id")
    if tg_cover:
        try:
            mid, is_non_vid = _post_json_cover(tg_cover)
            if mid or is_non_vid:
                return mid
        except:
            pass

    raw_bytes, mime = get_poster_bytes_sync(file_doc)
    if raw_bytes:
        try:
            return _post_multipart_cover(raw_bytes, mime)
        except:
            pass
    return None

def get_tg_photo(thumb, tg_thumb_id=None, brand_name=""):
    brand = (brand_name or "").strip()
    if brand and HAS_PIL:
        raw_bytes, mime = None, "image/jpeg"
        if thumb and "/poster/" in thumb and not thumb.endswith("/poster/default"):
            pid = thumb.split("/poster/", 1)[1].split("?")[0]
            ck = (pid, brand, thumb)
            if ck in BRANDED_POSTER_CACHE:
                img = io.BytesIO(BRANDED_POSTER_CACHE[ck][0]); img.name = "thumb.jpg"; return img
            if pid in POSTER_CACHE:
                raw_bytes, mime = POSTER_CACHE[pid][0], POSTER_CACHE[pid][1]
            else:
                for d in get_extra_dbs_sync():
                    try:
                        pdoc = d["posters"].find_one({"_id": pid})
                        if pdoc and pdoc.get("data"):
                            r_b, m_b = decode_b64_to_bytes(pdoc["data"])
                            if r_b:
                                raw_bytes, mime = r_b, pdoc.get("mime", m_b)
                                poster_cache_put(pid, raw_bytes, mime, ver=str(pdoc.get("ver", "")))
                                break
                    except: pass
        elif thumb and thumb.startswith("data:image"):
            raw_bytes, mime = decode_b64_to_bytes(thumb)
        if not raw_bytes and tg_thumb_id and BOT_TOKEN:
            try: raw_bytes = _bot_api_download_sync(tg_thumb_id)
            except: pass
        if raw_bytes:
            b_raw, b_mime = apply_brand_watermark_sync(raw_bytes, mime, brand)
            img = io.BytesIO(b_raw); img.name = "thumb.jpg"; return img

    if tg_thumb_id:
        return tg_thumb_id
    if not thumb or thumb.endswith("/poster/default"):
        return None
    if "/poster/" in thumb:
        pid = thumb.split("/poster/", 1)[1].split("?")[0]
        if pid in POSTER_CACHE:
            img = io.BytesIO(POSTER_CACHE[pid][0]); img.name = "thumb.jpg"; return img
        for d in get_extra_dbs_sync():
            try:
                pdoc = d["posters"].find_one({"_id": pid})
                if pdoc and pdoc.get("data"):
                    raw, mime = decode_b64_to_bytes(pdoc["data"])
                    if raw:
                        poster_cache_put(pid, raw, pdoc.get("mime", mime), ver=str(pdoc.get("ver", "")))
                        img = io.BytesIO(raw); img.name = "thumb.jpg"; return img
            except: pass
        for d in get_extra_dbs_sync():
            try:
                fdoc = d["files"].find_one({"_id": pid})
                if fdoc and fdoc.get("thumb_url", "").startswith("data:image"):
                    raw, _ = decode_b64_to_bytes(fdoc["thumb_url"])
                    if raw: img = io.BytesIO(raw); img.name = "thumb.jpg"; return img
            except: pass
        return thumb if thumb.startswith("http") else f"{get_live_web_url()}{thumb}"
    if thumb.startswith("data:image"):
        raw, _ = decode_b64_to_bytes(thumb)
        if raw: img = io.BytesIO(raw); img.name = "thumb.jpg"; return img
        return None
    return thumb

def parse_chat_target(cid):
    s = str(cid or "").strip()
    if re.match(r"^-?\d+$", s):
        return int(s)
    return s

def parse_minutes_val(val):
    s = re.sub(r"(?:minutes?|mins?|m)$", "", str(val or "").strip(), flags=re.IGNORECASE).strip()
    v = float(s)
    return int(v) if v.is_integer() else v

async def schedule_channel_delete(chat_id, msg_id, delete_min):
    try:
        dmin = float(delete_min or 0)
    except:
        dmin = 0
    if dmin <= 0 or not msg_id:
        return
    await get_db()
    await db["pending_deletes"].insert_one({
        "chat_id": str(chat_id),
        "msg_id": int(msg_id),
        "delete_at": time.time() + (dmin * 60.0)
    })

async def send_post_to_chat(client, chat_id, file_doc, reply_markup, brand_name=""):
    cap = f"🔥 **New Trending Upload!**\n\n🎬 **{file_doc['title']}**"
    photo = await asyncio.to_thread(get_tg_photo, file_doc.get("thumb_url"), file_doc.get("tg_thumb_id"), brand_name)
    if photo:
        try:
            return await client.send_photo(chat_id, photo=photo, caption=cap, reply_markup=reply_markup)
        except Exception as e:
            print("send_photo fallback to send_message:", e, flush=True)
    return await client.send_message(chat_id, cap, reply_markup=reply_markup)

async def notify_new_post(client, file_doc, config):
    ch_id = config.get("autopost_channel")
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]])
    bname = config.get("brand_name", "")
    if ch_id:
        try:
            await send_post_to_chat(client, parse_chat_target(ch_id), file_doc, markup, brand_name=bname)
        except Exception as e:
            print(f"notify_new_post main channel error: {e}", flush=True)
    await get_db()
    async for ch in auto_channels_col.find():
        try:
            target = parse_chat_target(ch["chat_id"])
            msg = await send_post_to_chat(client, target, file_doc, markup, brand_name=bname)
            if msg and getattr(msg, "id", None) and float(ch.get("delete_min", 0) or 0) > 0:
                await schedule_channel_delete(target, msg.id, ch["delete_min"])
        except Exception as e:
            print(f"notify_new_post auto_channel ({ch.get('chat_id')}) error: {e}", flush=True)

# ==========================================
# 3. BACKGROUND TASKS
# ==========================================
async def background_tasks():
    last_av, last_ap, last_cp = time.time(), time.time(), time.time()
    while True:
        await asyncio.sleep(5)
        if not getattr(app, "is_connected", False):
            continue
        now = time.time()
        try:
            config = await get_config()
            live_url = get_live_web_url(config)
            bname = config.get("brand_name", "")

            async for pd in db["pending_deletes"].find({"delete_at": {"$lte": now}}):
                try:
                    await app.delete_messages(parse_chat_target(pd["chat_id"]), int(pd["msg_id"]))
                    await db["pending_deletes"].delete_one({"_id": pd["_id"]})
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                    break
                except Exception as de:
                    print(f"Auto Delete Error ({pd.get('chat_id')}): {de}", flush=True)
                    await db["pending_deletes"].delete_one({"_id": pd["_id"]})

            if config.get("autovid_time_min", 0) > 0 and config.get("autovid_msg_id") and (now - last_av) >= (config["autovid_time_min"] * 60):
                last_av = time.time()
                fc = config.get("autovid_chat_id", ADMIN_ID)
                async for u in users_col.find():
                    if u.get("last_autovid_msg"):
                        try: await app.delete_messages(u["_id"], u["last_autovid_msg"])
                        except: pass
                    try:
                        msg = await app.copy_message(u["_id"], fc, config["autovid_msg_id"])
                        await users_col.update_one({"_id": u["_id"]}, {"$set": {"last_autovid_msg": msg.id}})
                        await asyncio.sleep(0.05)
                    except: pass

            if config.get("autopost_time_hr", 0) > 0 and (now - last_ap) >= (config["autopost_time_hr"] * 3600):
                last_ap = time.time()
                af = await get_all_files_across_dbs()
                if af:
                    idx = config.get("autopost_idx", 0) % len(af)
                    f = af[idx]
                    photo = await asyncio.to_thread(get_tg_photo, f.get("thumb_url"), f.get("tg_thumb_id"), bname)
                    async for u in users_col.find():
                        try:
                            btn = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", web_app=WebAppInfo(url=f"{live_url}/?uid={u['_id']}"))]])
                            if photo:
                                if hasattr(photo, "seek"): photo.seek(0)
                                await app.send_photo(u["_id"], photo=photo, caption=f"🔥 **New Video!**\n\nTitle: {f['title']}", reply_markup=btn)
                            else:
                                await app.send_message(u["_id"], f"🔥 **New Video!**\n\nTitle: {f['title']}", reply_markup=btn)
                            await asyncio.sleep(0.05)
                        except: pass
                    await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_idx": idx + 1}})

            if config.get("autopost_channel") and config.get("autopost_minute", 0) > 0 and (now - last_cp) >= (config["autopost_minute"] * 60):
                af = await get_all_files_across_dbs()
                if af:
                    ci = config.get("autopost_channel_idx", 0) % len(af)
                    f = af[ci]
                    try:
                        await send_post_to_chat(app, parse_chat_target(config["autopost_channel"]), f, InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]]), brand_name=bname)
                        last_cp = now
                        await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_channel_idx": ci + 1}})
                    except Exception as e:
                        print(f"Main autopost error: {e}", flush=True)

            async for ch in auto_channels_col.find():
                c_id = ch.get("chat_id")
                try:
                    c_int = float(ch.get("interval", 0) or 0)
                except:
                    c_int = 0
                if not c_id or c_int <= 0:
                    continue

                next_post = ch.get("next_post_time")
                if next_post is None:
                    last_pt = float(ch.get("last_post_time", 0) or 0)
                    next_post = (last_pt + (c_int * 60.0)) if last_pt > 0 else 0

                if now < next_post:
                    continue

                af = await get_all_files_across_dbs()
                if not af:
                    continue

                ci = int(ch.get("post_idx", 0) or 0) % len(af)
                f = af[ci]
                target = parse_chat_target(c_id)
                try:
                    msg = await send_post_to_chat(
                        app, target, f,
                        InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]]),
                        brand_name=bname
                    )
                    next_t = now + (c_int * 60.0)
                    await auto_channels_col.update_one(
                        {"_id": ch["_id"]},
                        {"$set": {"last_post_time": now, "next_post_time": next_t, "post_idx": ci + 1}}
                    )
                    d_min = float(ch.get("delete_min", 0) or 0)
                    if d_min > 0 and msg and getattr(msg, "id", None):
                        await schedule_channel_delete(target, msg.id, d_min)
                except FloodWait as fw:
                    await asyncio.sleep(fw.value)
                except Exception as e:
                    print(f"AutoChannel Post Error ({c_id}): {e}", flush=True)
        except Exception as e:
            print("BG Task Error:", e, flush=True)

# ==========================================
# 4. HELPER BUILDERS (100% DYNAMIC MONGODB)
# ==========================================
QUALITIES = ["360p", "480p", "720p", "1080p", "4K", "HDRip"]

def clean_season_no(val):
    s = str(val or "1").strip()
    s = re.sub(r"^(?:season|s)\s*[-_:]?\s*", "", s, flags=re.IGNORECASE).strip()
    return s or "1"

def clean_ep_no(val):
    s = str(val or "1").strip()
    s = re.sub(r"^(?:episode|ep|e)\s*[-_:]?\s*", "", s, flags=re.IGNORECASE).strip()
    return s or "1"

def natural_sort_key(val):
    s = str(val or "").strip()
    m = re.search(r"\d+", s)
    if m:
        return (0, int(m.group(0)), s.lower())
    return (1, 0, s.lower())

async def get_categories_list():
    await get_db(); return [c["name"] for c in await cats_col.find().to_list(100)]

async def get_languages_list():
    await get_db(); return [l["name"] for l in await langs_col.find().to_list(100)]

def parse_name_year(raw_text, cmd_prefix):
    txt = raw_text.strip()
    if cmd_prefix:
        mc = re.match(rf"^{re.escape(cmd_prefix)}(?:@\w+)?(?:\s+(.*))?$", txt, re.IGNORECASE | re.DOTALL)
        if mc: txt = (mc.group(1) or "").strip()
    if txt.upper().startswith("NAME YEAR "): txt = txt[10:].strip()
    elif txt.upper().startswith("NAME "): txt = txt[5:].strip()
    if not txt: return "", ""
    m = re.match(r"^(.*?)\s+(\d{4})$", txt)
    return (m.group(1).strip(), m.group(2).strip()) if m else (txt, "")

async def get_cat_markup(prefix, skip_cb, back_cb=None):
    cats = await get_categories_list(); btns, row = [], []
    for i, cn in enumerate(cats):
        row.append(InlineKeyboardButton(f"📂 {cn}", callback_data=f"{prefix}_{i}"))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([InlineKeyboardButton("⏭ Skip Category", callback_data=skip_cb)])
    if back_cb: btns.append([InlineKeyboardButton("⬅️ Back", callback_data=back_cb)])
    return InlineKeyboardMarkup(btns), cats

async def get_lang_markup(prefix, skip_cb, back_cb=None):
    langs = await get_languages_list(); btns, row = [], []
    for i, ln in enumerate(langs):
        row.append(InlineKeyboardButton(f"🌐 {ln}", callback_data=f"{prefix}_{i}"))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([InlineKeyboardButton("⏭ Skip Language", callback_data=skip_cb)])
    if back_cb: btns.append([InlineKeyboardButton("⬅️ Back", callback_data=back_cb)])
    return InlineKeyboardMarkup(btns), langs

def extract_file_meta(m):
    media = m.video or m.document or m.audio
    fname = getattr(media, "file_name", None) or ""
    fsize = getattr(media, "file_size", 0) or 0
    cap = (m.caption or "").strip().replace("\n", " ")
    raw_lbl = cap[:38] if cap else (fname[:38] if fname else "Video File")
    label = re.sub(r"[`*_]", " ", raw_lbl).strip() or "Video File"
    if fsize > 0:
        mb = fsize / (1024 * 1024)
        sz = f"{mb:.1f} MB" if mb < 1024 else f"{mb/1024:.2f} GB"
        return f"{label} ({sz})"
    return label

def build_lang_pick_rows(langs, cur_lang, cb_prefix):
    if not langs:
        return []
    rows, row = [], []
    for i, ln in enumerate(langs):
        mark = " ✅" if ln == cur_lang else ""
        row.append(InlineKeyboardButton(f"🌐 {ln}{mark}", callback_data=f"{cb_prefix}_{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    none_mark = " ✅" if not cur_lang else ""
    rows.append([InlineKeyboardButton(f"🌐 No Lang{none_mark}", callback_data=f"{cb_prefix}_none")])
    return rows

def get_quality_markup(prefix, skip_cb, finish_label="⏭ Skip / Finish", back_cb=None, extra_rows=None, top_rows=None):
    btns = list(top_rows) if top_rows else []
    row = []
    for q in QUALITIES:
        row.append(InlineKeyboardButton(f"🎬 {q}", callback_data=f"{prefix}_{q}"))
        if len(row) == 3: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([InlineKeyboardButton("➕ Custom Quality", callback_data=f"{prefix}_custom")])
    if extra_rows:
        btns.extend(extra_rows)
    if finish_label and skip_cb:
        btns.append([InlineKeyboardButton(finish_label, callback_data=skip_cb)])
    if back_cb: btns.append([InlineKeyboardButton("⬅️ Back", callback_data=back_cb)])
    return InlineKeyboardMarkup(btns)

def collect_doc_languages(doc):
    res = []
    def _add(v):
        s = str(v or "").strip()
        if s and s not in res:
            res.append(s)
    for x in (doc.get("languages") or []):
        _add(x)
    _add(doc.get("language"))
    for q in (doc.get("qualities") or []):
        _add(q.get("lang"))
    for s in (doc.get("seasons") or []):
        for e in (s.get("episodes") or []):
            for q in (e.get("qualities") or []):
                _add(q.get("lang"))
    return res

def is_in_step(step_name):
    return filters.create(lambda _, __, m: admin_steps.get(m.from_user.id, {}).get("step") == step_name)

async def deliver_cached_file(client, uid, f_id_send, title, config, file_doc=None):
    try:
        dt = config.get("auto_del_time", 0)
        cap = f"🎬 **{title}**"
        cap_html = f"🎬 <b>{html.escape(str(title))}</b>"
        if dt > 0:
            mn, sc = divmod(dt, 60)
            del_str = f"{mn} মিনিট {sc} সেকেন্ড" if mn > 0 else f"{sc} সেকেন্ড"
            warn_txt = config.get("autodel_text") or ""
            cap += f"\n\n⏳ **ভিডিওটি {del_str} পর অটো ডিলিট হয়ে যাবে!**"
            cap_html += f"\n\n⏳ <b>ভিডিওটি {del_str} পর অটো ডিলিট হয়ে যাবে!</b>"
            if warn_txt:
                cap += f"\n⚠️ {warn_txt}"
                cap_html += f"\n⚠️ {html.escape(str(warn_txt))}"

        sent_msg_id = None
        if file_doc and BOT_TOKEN and f_id_send not in NON_VIDEO_FILE_IDS:
            sent_msg_id = await asyncio.to_thread(
                send_video_with_cover_sync, uid, f_id_send, cap_html, config.get("frotect", False), file_doc, config.get("brand_name", "")
            )

        if not sent_msg_id:
            sm = await client.send_cached_media(
                chat_id=uid,
                file_id=f_id_send,
                caption=cap,
                protect_content=config.get("frotect", False)
            )
            sent_msg_id = sm.id

        if dt > 0 and sent_msg_id:
            asyncio.create_task(delete_msg_later(client, uid, sent_msg_id, dt))
    except Exception as e:
        await client.send_message(uid, f"❌ ফাইল পাঠাতে সমস্যা হয়েছে! {e}")

# ==================== END OF PART 1 ====================

👉 এবার লিখুন: Part 2 (বাকি ৩ ভাগের ২য় অংশ) ভাই, সম্পূর্ণ কোডটি প্রায় ১৫০ KB
হওয়ায় ৩ পার্টে ভাগ করলে প্রতি পার্ট ~৫০–৬০ KB হয়ে যায় এবং শেষে আবার কেটে যাওয়ার
ঝুঁকি থাকে। তাই কোনো লাইন যেন না কাটে এবং আপনার পেস্ট করতেও কোনো ঝামেলা না হয়,
মোট ৪টি পার্টে (Part 1 + আর মাত্র ৩টি পার্ট) সম্পূর্ণ কোড দিচ্ছি। প্রতিটি
পার্টের শেষে 0 স্পেসে # === END === দেওয়া আছে, তাই কোনো Auto-Indent এরর আসবে না।

নিচে Part 2 / 4 দেওয়া হলো। এটি Part 1-এর ঠিক নিচে পেস্ট করে আমাকে Part 3 লিখুন।

📦 PART 2 / 4

# ==========================================
# 5. BOT COMMANDS (START + STATS + SIMPLE CONFIG + STRICT AD + BLUR + BRAND + TUTORIALS)
# ==========================================
@app.on_message(filters.command("cancel", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_cancel(c, m): admin_steps.pop(m.from_user.id, None); await m.reply("✅ প্রসেস বাতিল করা হয়েছে!")

@app.on_message(filters.command("myid", case_sensitive=False) & unique_msg)
async def cmd_myid(c, m): await m.reply(f"✅ বট কাজ করছে!\n🆔 আইডি: `{m.from_user.id}`")

@app.on_message(filters.command("cmd", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_list(c, m):
    part1 = """🛠 **সকল কমান্ড ও ব্যবহারের উদাহরণ (পর্ব ১/২):**
💡 _কমান্ডে ক্লিক করলে সরাসরি বটে রান হবে, আর কোড বক্সে ক্লিক করলে কপি হবে!_

🎬 **Movies, Series, Brand & 18+ Blur:**
• /movies — নতুন মুভি এড
  কপি: `/movies NAME RRR 2024`
• /editemovies — মুভি এডিট (কপি: `/editemovies RRR`)
• /deletedmovies — মুভি ডিলিট লিস্ট
• /addseries — নতুন ওয়েব সিরিজ এড
  কপি: `/addseries Bachelor Point 2026`
• /editeseries — সিরিজ এডিট (কপি: `/editeseries Bachelor Point`)
• /deleteseries — সিরিজ ডিলিট লিস্ট
• /addupcoming — আপকামিং শিডিউল এড (কপি: `/addupcoming Pushpa 3 | আজ রাত ৮টা`)
• /delupcoming — আপকামিং ডিলিট
• /setcategory — নতুন ক্যাটাগরি এড (কপি: `/setcategory 18+` অথবা `/setcategory Action`)
• /deletecategory — ক্যাটাগরি ডিলিট
• /blar — ১৮+ পোস্টার ট্যাপ করে দেখার পর কত সেকেন্ডে অটো ব্লার হবে
  কপি: `/blar 20s` (বন্ধ করতে `/blar 0`)
• /brand — সব পোস্টারের নিচে বাম কোনায় ব্র্যান্ড নাম সেট
  কপি: `/brand Flixbix4u` | রিমুভ: `/delbrand`
• /setlanguage — ভাষা এড (কপি: `/setlanguage Bangla`) | /deletelanguage
• /setrecgroup — Recommendation গ্রুপ (কপি: `/setrecgroup https://t.me/my_group`) | /delrecgroup
• /cancel — চলমান প্রসেস বাতিল"""

    part2 = """🛠 **সকল কমান্ড ও ব্যবহারের উদাহরণ (পর্ব ২/২):**

💰 **Strict Ad, Wait Time, Monetag, Steps, Redeem & Telegram Tasks:**
• /strictad — অ্যাড শেষ না করে ব্যাক দিলে কিছু পাবে না (`on` / `off`)
  কপি: `/strictad on`
• /regwait — রেগুলার ডাইক্ট অ্যাডের সময় (সেকেন্ড)
  কপি: `/regwait 10` অথবা `/regwait 5-15`
• /prparadd — প্রিমিয়াম ডাইরেক্ট অ্যাডের সময় (কপি: `/prparadd 10`)
• /taskwait — স্পিন ও টাস্ক ডাইরেক্ট অ্যাডের সময় (কপি: `/taskwait 10`)
• /monetagwait — Monetag অ্যাডের ন্যূনতম দেখার সময় (কপি: `/monetagwait 4`)
• /addzoonid — Monetag Zone ID এড (কপি: `/addzoonid 12242829`) | /deletezoonid
• /addlink — Adsterra / Direct Link এড (কপি: `/addlink https://example.com`) | /delink
• /prstep (কপি: `/prstep 2`) | /regstep (কপি: `/regstep 2`) | /taskstep (কপি: `/taskstep 2`)
• /tgtask — টেলিগ্রাম চ্যানেল/গ্রুপ জয়েন টাস্ক (বট অ্যাডমিন থাকতে হবে)
  কপি: `/tgtask -100123456789 50` | ডিলিট: `/deltgtask`
• /addtask — কাস্টম টাস্ক (কপি: `/addtask Join | https://t.me/ch | 50`)
• /addzonetask (কপি: `/addzonetask Monetag | 12242829 | 30`) | /addlinktask | /deltask
• /addredeem (কপি: `/addredeem 1day 10 VIP10`) | /allred (কপি: `/allred 100 10-50`)
• /Premiumuserlist | /deletepremium | /addrdiem

⚙️ **Tutorials (Unlimited + Multi-Language), Site & Channels:**
• /addtutorial — ভাষা অনুযায়ী আনলিমিটেড টিউটোরিয়াল এড (ভিডিওতে রিপ্লাই)
  কপি: `/addtutorial Bangla | ডাউনলোডের নিয়ম`
• /deltutorial — টিউটোরিয়াল ডিলিট | /tutorial — টেলিগ্রামে টিউটোরিয়াল দেখা
• /settutorial — ডিফল্ট টিউটোরিয়াল ভিডিও সেট (ভিডিওতে রিপ্লাই)
• /stats | /name (কপি: `/name Moviee Link`) | /setnotice | /logo | /addtex | /deltex
• /setpremcoin (কপি: `/setpremcoin 50`) | /relocktime (কপি: `/relocktime 30`) | /delrelocktime | /lockall
• /spincoin (কপি: `/spincoin 10-50`) | /spinlimit 5 | /taskcoin 20 | /tasklimit 5
• /refbonous 20 | /refbonousoff | /usd 100 5 | /bdt 100 50 | /addcred 500 = 7 d | /delcred
• /addcnl | /delcnl | /vercnl | /delvrcnl | /autochannel | /delautochannel
• /brodcast | /cnlbdcst | /autodel | /frotect | /addadmin | /mongo | /delmongo | /mongostats | /addfile | /delfile | /delall | /vidtitel | /deltitel | /auto | /auto2 | /promot | /delpromot"""

    kb1 = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 /movies", callback_data="cmdrun_movies"), InlineKeyboardButton("📺 /addseries", callback_data="cmdrun_addseries")],
        [InlineKeyboardButton("✏️ /editemovies", callback_data="cmdrun_editemovies"), InlineKeyboardButton("✏️ /editeseries", callback_data="cmdrun_editeseries")],
        [InlineKeyboardButton("📂 /setcategory", callback_data="cmdrun_setcategory"), InlineKeyboardButton("🌐 /setlanguage", callback_data="cmdrun_setlanguage")],
        [InlineKeyboardButton("📅 /addupcoming", callback_data="cmdrun_addupcoming"), InlineKeyboardButton("❌ /cancel", callback_data="cmdrun_cancel")]
    ])
    kb2 = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 /stats", callback_data="cmdrun_stats"), InlineKeyboardButton("🆔 /addzoonid", callback_data="cmdrun_addzoonid")],
        [InlineKeyboardButton("💎 /Premiumuserlist", callback_data="cmdrun_premiumuserlist"), InlineKeyboardButton("🗄 /mongostats", callback_data="cmdrun_mongostats")]
    ])
    await m.reply(part1, reply_markup=kb1)
    await m.reply(part2, reply_markup=kb2)

@app.on_callback_query(filters.regex(r"^cmdrun_") & filters.user(ADMIN_ID) & unique_cb)
async def cmdrun_cb(c, q):
    act = q.data.split("_", 1)[1]
    await q.answer()
    class FakeCmdMsg:
        def __init__(self, u, txt):
            self.from_user = u; self.text = txt; self.chat = q.message.chat; self.reply_to_message = None
        async def reply(self, *a, **k): return await c.send_message(q.from_user.id, *a, **k)
    fm = FakeCmdMsg(q.from_user, f"/{act}")
    dispatch = {
        "movies": cmd_movies, "addseries": cmd_addseries, "editemovies": cmd_edit_movies,
        "editeseries": cmd_edit_series, "setcategory": cmd_setcategory, "setlanguage": cmd_setlanguage,
        "addupcoming": cmd_addupcoming, "addzoonid": cmd_addzoonid, "stats": cmd_stats,
        "premiumuserlist": cmd_premiumuserlist, "mongostats": cmd_legacy_router, "cancel": cmd_cancel
    }
    fn = dispatch.get(act)
    if fn: await fn(c, fm)

@app.on_message(filters.command("stats", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_stats(c, m):
    try:
        await get_db(); conf = await get_config(); live_url = get_live_web_url(conf)
        tu = await users_col.count_documents({})
        pu = await users_col.count_documents({"premium_until": {"$gt": datetime.now()}})
        dbs = await get_extra_dbs_async(); tf = 0
        for d in dbs:
            try: tf += await d["files"].count_documents({})
            except: pass
        await m.reply(
            f"📊 **Bot Statistics:**\n\n"
            f"👥 Total Users: `{tu}`\n🎬 Total Movies/Series: `{tf}`\n"
            f"💎 Premium Users: `{pu}`\n👤 Regular Users: `{max(0, tu - pu)}`\n"
            f"🗄 MongoDBs: `{len(dbs)}`\n"
            f"🛡 Strict Ad: `{'ON' if conf.get('strict_ad', True) else 'OFF'}` | 🔞 Blur: `{conf.get('blur_sec', 20)}s`\n"
            f"🏷 Brand: `{conf.get('brand_name') or 'None'}`\n"
            f"🌐 Mini App URL: `{live_url}`"
        )
    except Exception as e:
        await m.reply(f"❌ Stats Error: `{e}`")

SIMPLE_CONF = {
    "name": ("site_name", "str", "/name Moviee Link"),
    "setnotice": ("marquee_text", "str", "/setnotice নতুন মুভি পেতে চ্যানেলে জয়েন করুন"),
    "setpremcoin": ("premium_vid_coin", "int", "/setpremcoin 50"),
    "relocktime": ("vid_relock_sec", "int", "/relocktime 30"),
    "taskcoin": ("task_coin", "int", "/taskcoin 20"),
    "spinlimit": ("spin_limit", "int", "/spinlimit 5"),
    "tasklimit": ("task_limit", "int", "/tasklimit 5"),
    "prstep": ("prstep", "int", "/prstep 2"),
    "regstep": ("regstep", "int", "/regstep 2"),
    "taskstep": ("task_step", "int", "/taskstep 2"),
    "addadmin": ("payment_admin", "admin", "/addadmin Sudo_king"),
    "addtex": ("start_text", "str", "/addtex স্বাগতম আমাদের বটে!"),
    "autvidti": ("autovid_time_min", "int", "/autvidti 30"),
    "autpost": ("autopost_time_hr", "int", "/autpost 2"),
    "autodel": ("auto_del_time", "int", "/autodel 600"),
    "autex": ("autodel_text", "str", "/autex ভিডিওটি ১০ মিনিট পর ডিলিট হবে।"),
    "prparadd": ("prem_vid_wait", "int", "/prparadd 10"),
    "taskwait": ("task_wait_sec", "int", "/taskwait 10"),
    "monetagwait": ("monetag_wait", "int", "/monetagwait 4")
}

@app.on_message(filters.command(list(SIMPLE_CONF.keys()) + ["delrelocktime", "deltex", "refbonous", "refbonousoff", "frotect", "strictad", "regwait", "blar", "blur", "brand", "delbrand"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_simple_config(c, m):
    await get_db(); parts = m.text.split(None, 1); cmd = parts[0].lstrip("/").lower().split("@")[0]; arg = parts[1].strip() if len(parts) > 1 else ""
    if cmd == "delrelocktime": await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_sec": -1, "vid_relock_min": 0}}); invalidate_home_cache(); return await m.reply("✅ Relock Time Disabled (একবার আনলক করলে আর লক হবে না)!")
    if cmd == "deltex": await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": ""}}); invalidate_home_cache(); return await m.reply("✅ Start Text Deleted!")
    if cmd == "refbonousoff": await config_col.update_one({"_id": "settings"}, {"$set": {"ref_on": False}}); invalidate_home_cache(); return await m.reply("✅ Refer Bonus OFF!")
    if cmd == "refbonous":
        if not arg or not arg.isdigit(): return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/refbonous 20`")
        await config_col.update_one({"_id": "settings"}, {"$set": {"ref_coin": int(arg), "ref_on": True}}); invalidate_home_cache(); return await m.reply(f"✅ Refer Bonus Set: `{arg}` Coins")
    if cmd == "frotect":
        if arg.lower() not in ["on", "off"]: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/frotect on` অথবা `/frotect off`")
        await config_col.update_one({"_id": "settings"}, {"$set": {"frotect": arg.lower() == "on"}}); invalidate_home_cache(); return await m.reply(f"✅ Protect Content: `{arg.upper()}`")
    if cmd == "strictad":
        if arg.lower() not in ["on", "off"]: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/strictad on` (আগে ব্যাক করলে কিছুই পাবে না)\n`/strictad off` (বন্ধ করতে)")
        is_on = (arg.lower() == "on")
        await config_col.update_one({"_id": "settings"}, {"$set": {"strict_ad": is_on}}, upsert=True); invalidate_home_cache()
        return await m.reply(f"✅ **Strict Ad Verification:** `{'ON 🔒' if is_on else 'OFF 🔓'}`\n_(এখন অ্যাডের সময় শেষ হওয়ার আগে ব্যাক করলে ভিডিও, শর্ট লিংক, টাস্ক বা স্পিন কয়েন পাবে না!)_" if is_on else "✅ Strict Ad OFF করা হয়েছে।")
    if cmd == "regwait":
        if not arg: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/regwait 10` অথবা রেঞ্জ `/regwait 5-15`")
        if "-" in arg:
            try:
                a, b = [max(1, int(x.strip())) for x in arg.split("-", 1)]
                lo, hi = min(a, b), max(a, b)
                vals = [lo, (lo + hi) // 2, hi] if hi - lo >= 2 else [lo, hi]
                await config_col.update_one({"_id": "settings"}, {"$set": {"direk_wait": vals}}, upsert=True); invalidate_home_cache()
                return await m.reply(f"✅ Regular Ad Wait Time Set: `{lo}-{hi}` সেকেন্ড")
            except: return await m.reply("❌ উদাহরণ: `/regwait 10` অথবা `/regwait 5-15`")
        try:
            sec = max(1, int(re.sub(r"[^0-9]", "", arg)))
            await config_col.update_one({"_id": "settings"}, {"$set": {"direk_wait": [sec]}}, upsert=True); invalidate_home_cache()
            return await m.reply(f"✅ Regular Ad Wait Time Set: `{sec}` সেকেন্ড")
        except: return await m.reply("❌ উদাহরণ: `/regwait 10`")
    if cmd in ("blar", "blur"):
        if not arg: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/blar 20s` (২০ সেকেন্ড পর আবার অটো ব্লার হবে)\n`/blar 0` (অটো রিব্লার বন্ধ)")
        nums = re.sub(r"[^0-9]", "", arg)
        if not nums: return await m.reply("❌ উদাহরণ: `/blar 20s`")
        sec = int(nums)
        await config_col.update_one({"_id": "settings"}, {"$set": {"blur_sec": sec}}, upsert=True); invalidate_home_cache()
        return await m.reply(f"✅ 18+ পোস্টার অটো-ব্লার সময় সেট হয়েছে: `{sec}s`" if sec > 0 else "✅ 18+ অটো রিব্লার `0s` সেট করা হয়েছে!")
    if cmd == "brand":
        if not arg:
            admin_steps[m.from_user.id] = {"step": "set_brand_wait"}
            return await m.reply("🏷 পোস্টারে প্রদর্শনের জন্য আপনার **Brand Name** লিখুন:\nউদাহরণ: `Flixbix4u`")
        await config_col.update_one({"_id": "settings"}, {"$set": {"brand_name": arg}}, upsert=True); invalidate_home_cache()
        return await m.reply(f"✅ Brand Watermark সেট হয়েছে: **{arg}**\n_(সব মুভি, সিরিজ ও ডিটেইল পোস্টারের নিচে বাম কোনায় দেখাবে!)_")
    if cmd == "delbrand":
        await config_col.update_one({"_id": "settings"}, {"$set": {"brand_name": ""}}, upsert=True); invalidate_home_cache()
        return await m.reply("✅ Brand Watermark রিমুভ করা হয়েছে!")
    k, tp, ex = SIMPLE_CONF[cmd]
    if not arg: return await m.reply(f"❌ **সঠিক নিয়ম ও উদাহরণ:**\n`{ex}`")
    try:
        val = int(re.sub(r"[^0-9-]", "", arg)) if tp == "int" else (arg.replace("@", "") if tp == "admin" else arg)
        if tp == "int" and cmd in ("prstep", "regstep", "taskstep") and val < 1: val = 1
        if tp == "int" and cmd in ("prparadd", "taskwait") and val < 1: val = 1
        if tp == "int" and cmd == "monetagwait" and val < 0: val = 0
        await config_col.update_one({"_id": "settings"}, {"$set": {k: val}}, upsert=True); invalidate_home_cache()
        if cmd == "relocktime":
            extra = " (প্রতি বাটন ক্লিকে অ্যাড আসবে)" if val == 0 else f" ({val} সেকেন্ড পর আবার লক হবে)"
            return await m.reply(f"✅ Relock Time সেট হয়েছে: `{val}` সেকেন্ড{extra}")
        await m.reply(f"✅ সেট হয়েছে: **{k}** = `{val}`")
    except: await m.reply(f"❌ ভুল ইনপুট! উদাহরণ:\n`{ex}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("set_brand_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_set_brand_text(c, m):
    bn = m.text.strip()
    await get_db()
    await config_col.update_one({"_id": "settings"}, {"$set": {"brand_name": bn}}, upsert=True)
    admin_steps.pop(m.from_user.id, None)
    invalidate_home_cache()
    await m.reply(f"✅ Brand Watermark সেট হয়েছে: **{bn}**")

@app.on_message(filters.command("setrecgroup", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setrecgroup(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2:
        admin_steps[m.from_user.id] = {"step": "set_rec_group_wait"}
        return await m.reply("👥 আপনার **Movie Recommendation গ্রুপের লিংক** দিন:\nউদাহরণ: `https://t.me/my_movie_group`")
    link = parts[1].strip()
    if not link.startswith("http"):
        link = "https://t.me/" + link.lstrip("@")
    await get_db()
    await config_col.update_one({"_id": "settings"}, {"$set": {"rec_group_link": link}}, upsert=True)
    invalidate_home_cache()
    await m.reply(f"✅ Movie Recommendation গ্রুপ লিংক সেট হয়েছে:\n🔗 `{link}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("set_rec_group_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_set_rec_group_text(c, m):
    link = m.text.strip()
    if not link.startswith("http"):
        link = "https://t.me/" + link.lstrip("@")
    await get_db()
    await config_col.update_one({"_id": "settings"}, {"$set": {"rec_group_link": link}}, upsert=True)
    admin_steps.pop(m.from_user.id, None)
    invalidate_home_cache()
    await m.reply(f"✅ Movie Recommendation গ্রুপ লিংক সেট হয়েছে:\n🔗 `{link}`")

@app.on_message(filters.command("delrecgroup", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delrecgroup(c, m):
    await get_db()
    await config_col.update_one({"_id": "settings"}, {"$set": {"rec_group_link": ""}}, upsert=True)
    invalidate_home_cache()
    await m.reply("✅ Movie Recommendation গ্রুপ লিংক রিমুভ করা হয়েছে!")

@app.on_message(filters.command("settutorial", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_settutorial(c, m):
    await get_db()
    parts = m.text.split(None, 1)
    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.animation or m.reply_to_message.document):
        media = m.reply_to_message.video or m.reply_to_message.animation or m.reply_to_message.document
        vid_id = media.file_id
        await config_col.update_one({"_id": "settings"}, {"$set": {"tutorial_vid": vid_id}}, upsert=True)
        invalidate_home_cache()
        status_msg = await m.reply("⏳ টিউটোরিয়াল ভিডিও সার্ভারে ক্যাশ করা হচ্ছে, অপেক্ষা করুন...")
        try:
            vid_hash = hashlib.md5(str(vid_id).encode()).hexdigest()[:14]
            cache_path = f"/tmp/tutorial_{vid_hash}.mp4"
            dl_path = await c.download_media(m.reply_to_message, file_name=cache_path + ".part")
            if dl_path and os.path.exists(dl_path) and os.path.getsize(dl_path) > 0:
                os.replace(dl_path, cache_path)
            await status_msg.edit_text("✅ ডিফল্ট টিউটোরিয়াল ভিডিও সেট ও ক্যাশ হয়েছে! _(ভাষা অনুযায়ী আনলিমিটেড টিউটোরিয়াল এড করতে `/addtutorial` ব্যবহার করুন)_")
        except Exception:
            await status_msg.edit_text("✅ ডিফল্ট টিউটোরিয়াল ভিডিও সেট হয়েছে! (স্ট্রিমিং ব্যাকআপ সক্রিয়)")
    elif len(parts) > 1 and parts[1].strip().startswith("http"):
        await config_col.update_one({"_id": "settings"}, {"$set": {"tutorial_vid": parts[1].strip()}}, upsert=True)
        invalidate_home_cache()
        await m.reply("✅ ডিফল্ট টিউটোরিয়াল ভিডিও লিংক সেট হয়েছে!")
    else:
        await m.reply("❌ **সঠিক নিয়ম:** কোনো ভিডিওতে রিপ্লাই (Reply) করে `/settutorial` লিখুন অথবা `/settutorial https://...mp4` দিন।\n💡 ভাষা অনুযায়ী আনলিমিটেড টিউটোরিয়াল এড করতে `/addtutorial` ব্যবহার করুন!")

@app.on_message(filters.command("addtutorial", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addtutorial(c, m):
    await get_db()
    clean = re.sub(r"^/addtutorial(?:@\w+)?\s*", "", m.text, count=1, flags=re.IGNORECASE).strip()
    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.animation or m.reply_to_message.document):
        media = m.reply_to_message.video or m.reply_to_message.animation or m.reply_to_message.document
        vid_id = media.file_id
        if "|" in clean:
            lang, title = [x.strip() for x in clean.split("|", 1)]
        elif clean:
            lang, title = clean, f"{clean} Tutorial"
        else:
            admin_steps[m.from_user.id] = {"step": "tut_lang_only", "video_id": vid_id, "is_url": False}
            return await m.reply("🌐 এই টিউটোরিয়াল ভিডিওর **ভাষা ও টাইটেল** লিখুন:\nউদাহরণ: `Bangla | কিভাবে ডাউনলোড করবেন`")
        doc = {"lang": lang, "title": title, "video_id": vid_id, "is_url": False, "created_at": datetime.utcnow().isoformat() + "Z"}
        await tutorials_col.insert_one(doc)
        invalidate_home_cache()
        st_m = await m.reply(f"⏳ **{lang}** টিউটোরিয়াল ক্যাশ হচ্ছে...")
        try:
            vid_hash = hashlib.md5(str(vid_id).encode()).hexdigest()[:14]
            cp = f"/tmp/tutorial_{vid_hash}.mp4"
            dp = await c.download_media(m.reply_to_message, file_name=cp + ".part")
            if dp and os.path.exists(dp) and os.path.getsize(dp) > 0: os.replace(dp, cp)
        except: pass
        return await st_m.edit_text(f"✅ টিউটোরিয়াল এড হয়েছে!\n🌐 ভাষা: **{lang}**\n🎬 টাইটেল: **{title}**")

    if "|" in clean:
        segs = [x.strip() for x in clean.split("|")]
        if len(segs) >= 3 and segs[2].startswith("http"):
            await tutorials_col.insert_one({"lang": segs[0], "title": segs[1], "video_id": segs[2], "is_url": True, "created_at": datetime.utcnow().isoformat() + "Z"})
            invalidate_home_cache()
            return await m.reply(f"✅ টিউটোরিয়াল লিংক এড হয়েছে!\n🌐 ভাষা: **{segs[0]}** | 🎬 **{segs[1]}**")
        admin_steps[m.from_user.id] = {"step": "tut_video_wait", "lang": segs[0], "title": segs[1]}
        return await m.reply(f"🌐 ভাষা: **{segs[0]}** | 🎬 **{segs[1]}**\n\n📤 এবার **টিউটোরিয়াল ভিডিও ফাইল** অথবা ভিডিও লিংক (`https://...mp4`) পাঠান:")

    admin_steps[m.from_user.id] = {"step": "tut_lang_title"}
    await m.reply("🌐 টিউটোরিয়ালের **ভাষা ও টাইটেল** লিখুন:\nউদাহরণ: `Bangla | মুভি ডাউনলোড করার নিয়ম`\nঅথবা `Hindi | Download Guide`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("tut_lang_only") & ~filters.regex(r"^/") & unique_msg)
async def handle_tut_lang_only(c, m):
    st = admin_steps.pop(m.from_user.id)
    if "|" in m.text: lang, title = [x.strip() for x in m.text.split("|", 1)]
    else: lang, title = m.text.strip(), f"{m.text.strip()} Tutorial"
    await get_db()
    await tutorials_col.insert_one({"lang": lang, "title": title, "video_id": st["video_id"], "is_url": st["is_url"], "created_at": datetime.utcnow().isoformat() + "Z"})
    invalidate_home_cache()
    threading.Thread(target=ensure_tutorial_cached_sync, args=(st["video_id"],), daemon=True).start()
    await m.reply(f"✅ টিউটোরিয়াল এড হয়েছে!\n🌐 ভাষা: **{lang}** | 🎬 **{title}**")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("tut_lang_title") & ~filters.regex(r"^/") & unique_msg)
async def handle_tut_lang_title(c, m):
    if "|" in m.text: lang, title = [x.strip() for x in m.text.split("|", 1)]
    else: lang, title = m.text.strip(), f"{m.text.strip()} Tutorial"
    admin_steps[m.from_user.id] = {"step": "tut_video_wait", "lang": lang, "title": title}
    await m.reply(f"🌐 ভাষা: **{lang}** | 🎬 **{title}**\n\n📤 এবার **টিউটোরিয়াল ভিডিও** অথবা লিংক পাঠান:")

@app.on_message((filters.video | filters.animation | filters.document | filters.text) & filters.user(ADMIN_ID) & filters.private & is_in_step("tut_video_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_tut_video_wait(c, m):
    st = admin_steps.get(m.from_user.id)
    if not st: return
    if m.text:
        v = m.text.strip()
        if not v.startswith("http"):
            return await m.reply("❌ দয়া করে ভিডিও ফাইল অথবা `https://...` লিংক পাঠান।")
        vid_id, is_url = v, True
    else:
        media = m.video or m.animation or m.document
        vid_id, is_url = media.file_id, False
    admin_steps.pop(m.from_user.id, None)
    await get_db()
    await tutorials_col.insert_one({"lang": st["lang"], "title": st["title"], "video_id": vid_id, "is_url": is_url, "created_at": datetime.utcnow().isoformat() + "Z"})
    invalidate_home_cache()
    if not is_url:
        st_m = await m.reply("⏳ টিউটোরিয়াল ভিডিও ক্যাশ হচ্ছে...")
        try:
            vid_hash = hashlib.md5(str(vid_id).encode()).hexdigest()[:14]
            cp = f"/tmp/tutorial_{vid_hash}.mp4"
            dp = await c.download_media(m, file_name=cp + ".part")
            if dp and os.path.exists(dp) and os.path.getsize(dp) > 0: os.replace(dp, cp)
        except: pass
        return await st_m.edit_text(f"✅ টিউটোরিয়াল এড হয়েছে!\n🌐 ভাষা: **{st['lang']}** | 🎬 **{st['title']}**")
    await m.reply(f"✅ টিউটোরিয়াল লিংক এড হয়েছে!\n🌐 ভাষা: **{st['lang']}** | 🎬 **{st['title']}**")

@app.on_message(filters.command("deltutorial", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltutorial(c, m):
    await get_db(); conf = await get_config(); ts = await tutorials_col.find().to_list(100)
    btns = []
    if conf.get("tutorial_vid"):
        btns.append([InlineKeyboardButton("❌ [Main / Default] Tutorial Video", callback_data="deltut_main")])
    for t in ts:
        btns.append([InlineKeyboardButton(f"❌ [{t['lang']}] {t['title'][:24]}", callback_data=f"deltut_{t['_id']}")])
    if not btns:
        return await m.reply("❌ কোনো টিউটোরিয়াল সেট করা নেই!")
    await m.reply("🗑 ডিলিট করতে টিউটোরিয়ালে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^deltut_") & filters.user(ADMIN_ID) & unique_cb)
async def deltut_cb(c, q):
    await get_db()
    val = q.data.split("_", 1)[1]
    if val == "main":
        await config_col.update_one({"_id": "settings"}, {"$set": {"tutorial_vid": None}})
    else:
        await tutorials_col.delete_one({"_id": ObjectId(val)})
    invalidate_home_cache()
    await q.message.edit_text("✅ টিউটোরিয়াল ডিলিট হয়েছে!")

@app.on_message(filters.command("tutorial", case_sensitive=False) & filters.private & unique_msg)
async def cmd_user_tutorial(c, m):
    await get_db(); conf = await get_config(); ts = await tutorials_col.find().to_list(100)
    if ts:
        btns = [[InlineKeyboardButton(f"🎬 {t['lang']} — {t['title'][:26]}", callback_data=f"getut_{t['_id']}")] for t in ts]
        return await m.reply("🌐 **আপনার পছন্দের ভাষায় টিউটোরিয়াল ভিডিও দেখুন:**", reply_markup=InlineKeyboardMarkup(btns))
    if conf.get("tutorial_vid"):
        vid = conf["tutorial_vid"]
        if str(vid).startswith("http"):
            return await m.reply(f"🎬 **Tutorial Video:**\n🔗 {vid}")
        return await c.send_video(m.from_user.id, video=vid, caption="🎬 **কিভাবে মুভি/সিরিজ দেখবেন ও ডাউনলোড করবেন**")
    await m.reply("⚠️ এখনো কোনো টিউটোরিয়াল ভিডিও সেট করা হয়নি।")

@app.on_callback_query(filters.regex(r"^getut_") & unique_cb)
async def getut_cb(c, q):
    await get_db()
    tid = q.data.split("_", 1)[1]
    t = await tutorials_col.find_one({"_id": ObjectId(tid)})
    if not t: return await q.answer("❌ টিউটোরিয়াল পাওয়া যায়নি!", show_alert=True)
    await q.answer("⏳ টিউটোরিয়াল পাঠানো হচ্ছে...")
    cap = f"🎬 **{t['title']}**\n🌐 Language: **{t['lang']}**"
    if t.get("is_url") or str(t["video_id"]).startswith("http"):
        await c.send_message(q.from_user.id, f"{cap}\n🔗 {t['video_id']}")
    else:
        await c.send_video(q.from_user.id, video=t["video_id"], caption=cap)

@app.on_message(filters.command("start", case_sensitive=False) & filters.private & unique_msg)
async def start_cmd(client, message):
    try:
        await get_db()
        uid = message.from_user.id
        args = message.text.split()
        config, user, must_join_chs = await asyncio.gather(
            get_config(),
            users_col.find_one({"_id": uid}),
            channels_col.find({"type": "must_join"}).to_list(100)
        )
        live_url = get_live_web_url(config)
        ref_by, pf = None, None
        if len(args) > 1:
            v = args[1]
            if v.startswith("tut_"):
                tid = v[4:]
                if tid == "default":
                    vid = config.get("tutorial_vid")
                    if vid:
                        if str(vid).startswith("http"): return await message.reply(f"🎬 **Tutorial Video:**\n🔗 {vid}")
                        return await client.send_video(uid, video=vid, caption="🎬 **Tutorial Video**")
                    return await message.reply("❌ টিউটোরিয়াল পাওয়া যায়নি!")
                try:
                    t = await tutorials_col.find_one({"_id": ObjectId(tid)})
                    if t:
                        cap = f"🎬 **{t['title']}**\n🌐 Language: **{t['lang']}**"
                        if t.get("is_url") or str(t["video_id"]).startswith("http"):
                            return await message.reply(f"{cap}\n🔗 {t['video_id']}")
                        return await client.send_video(uid, video=t["video_id"], caption=cap)
                except: pass
                return await message.reply("❌ টিউটোরিয়াল পাওয়া যায়নি!")
            if v.isdigit(): ref_by = int(v)
            elif v.startswith(("file_", "mfile_", "sfile_")): pf = v
            elif v.startswith("ref") and "file" in v:
                try: p = v.replace("ref", "").split("file"); ref_by, pf = int(p[0]), "file_" + p[1]
                except: pass

        if not user:
            user = {"_id": uid, "name": message.from_user.first_name, "balance": 0, "pending_file": None, "premium_until": None, "history": [], "favorites": [], "unlocked_files": {}, "spin_count": 0, "task_count": 0, "last_activity_date": None, "custom_tasks_done": [], "daily_viewed": {}}
            await users_col.insert_one(user)
            if ref_by and ref_by != uid and config.get("ref_on", True):
                await users_col.update_one({"_id": ref_by}, {"$inc": {"balance": config.get("ref_coin", 10)}})
                try: await client.send_message(ref_by, f"🎉 রেফার বোনাস! +{config.get('ref_coin', 10)} Coins")
                except: pass

        async def _chk_ch(ch):
            try:
                await client.get_chat_member(ch["chat_id"], uid)
                return None
            except UserNotParticipant:
                return ch["link"]
            except:
                return None

        not_joined = []
        if must_join_chs:
            res_ch = await asyncio.gather(*[_chk_ch(ch) for ch in must_join_chs])
            not_joined = [lnk for lnk in res_ch if lnk]

        if not_joined:
            if pf:
                await users_col.update_one({"_id": uid}, {"$set": {"pending_file": pf}})
            btns = [[InlineKeyboardButton("📢 Join Channel", url=l)] for l in not_joined] + [[InlineKeyboardButton("✅ Joined", callback_data="check_join")]]
            return await message.reply("❌ আগে আমাদের চ্যানেলগুলোতে জয়েন করুন:", reply_markup=InlineKeyboardMarkup(btns))

        pf_val = pf or user.get("pending_file")
        if pf_val:
            if user.get("pending_file"):
                asyncio.create_task(users_col.update_one({"_id": uid}, {"$set": {"pending_file": None}}))
            if pf_val.startswith("mfile_"):
                _, f_id, q_idx = pf_val.split("_")
                fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    asyncio.create_task(record_daily_unique_view(tdb, f_id, uid))
                    qs = fd.get("qualities", []); qi = int(q_idx)
                    if 0 <= qi < len(qs):
                        qo = qs[qi]
                        qlbl = f"{qo['lang']} • {qo['name']}" if qo.get("lang") else qo["name"]
                        return await deliver_cached_file(client, uid, qo["file_id"], f"{fd['title']} [{qlbl}]", config, file_doc=fd)
                    elif fd.get("file_id"): return await deliver_cached_file(client, uid, fd["file_id"], fd["title"], config, file_doc=fd)
                return await message.reply("❌ ফাইল পাওয়া যায়নি!")
            elif pf_val.startswith("sfile_"):
                _, f_id, si, ei, qi = pf_val.split("_")
                fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    asyncio.create_task(record_daily_unique_view(tdb, f_id, uid))
                    try:
                        ssn = fd["seasons"][int(si)]; ep = ssn["episodes"][int(ei)]; qo = ep["qualities"][int(qi)]
                        qlbl = f"{qo['lang']} • {qo['name']}" if qo.get("lang") else qo["name"]
                        return await deliver_cached_file(client, uid, qo["file_id"], f"{fd['title']} - S{clean_season_no(ssn['season_no'])} E{clean_ep_no(ep['ep_no'])} [{qlbl}]", config, file_doc=fd)
                    except: pass
                return await message.reply("❌ এপিসোড ফাইল পাওয়া যায়নি!")
            else:
                f_id = pf_val.replace("file_", "")
                fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    asyncio.create_task(record_daily_unique_view(tdb, f_id, uid))
                    fs = fd.get("file_id") or (fd["qualities"][0]["file_id"] if fd.get("qualities") else None)
                    if fs: return await deliver_cached_file(client, uid, fs, fd["title"], config, file_doc=fd)
                return await message.reply("❌ ফাইল পাওয়া যায়নি!")

        try: await client.set_chat_menu_button(chat_id=uid, menu_button=MenuButtonWebApp(text="Watch Now 🎬", web_app=WebAppInfo(url=f"{live_url}/?uid={uid}")))
        except: pass

        full = f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
        sn = config.get("site_name", "Moviee Link")
        txt = f"👋 **হ্যালো {full}!**\n\n🎬 উপরের টিউটোরিয়াল ভিডিওটি অন্তত একবার দেখে নিন (অথবা /tutorial লিখুন)\n\n🍿 এখানে পাবেন **নিত্য নতুন মুভি ও সিরিজ** কালেকশন — নিচের Watch Now বাটনে ক্লিক করো!"
        if config.get("start_text"): txt += f"\n\n📝 {config.get('start_text')}"
        ichs = await channels_col.find({"type": "inline"}).to_list(100)
        jurl = ichs[0]["link"] if ichs else f"https://t.me/{BOT_USERNAME}"
        surl = f"https://t.me/share/url?url={quote(f'https://t.me/{BOT_USERNAME}?start={uid}')}&text={quote(f'Join {sn}!')}"
        btns = [[InlineKeyboardButton("🍿 WATCH NOW", web_app=WebAppInfo(url=f"{live_url}/?uid={uid}"))], [InlineKeyboardButton("🎁 Share Friend", url=surl), InlineKeyboardButton("📡 Join All Channel", url=jurl)]]
        for ch in ichs[1:]:
            if ch.get("name") and ch.get("link"): btns.append([InlineKeyboardButton(ch["name"], url=ch["link"])])
        if config.get("tutorial_vid") and not str(config["tutorial_vid"]).startswith("http"):
            try: return await client.send_video(uid, video=config["tutorial_vid"], caption=txt, reply_markup=InlineKeyboardMarkup(btns))
            except: pass
        if config.get("start_logo"):
            try: return await client.send_photo(uid, photo=config["start_logo"], caption=txt, reply_markup=InlineKeyboardMarkup(btns))
            except: pass
        await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))
    except Exception as e:
        print("Start Error:", e, flush=True); traceback.print_exc()
        try: await message.reply(f"⚠️ Start Error: `{e}`")
        except: pass

@app.on_callback_query(filters.regex("check_join") & unique_cb)
async def check_join_cb(c, q):
    await get_db()
    for ch in await channels_col.find({"type": "must_join"}).to_list(100):
        try: await c.get_chat_member(ch["chat_id"], q.from_user.id)
        except: return await q.answer("❌ সব চ্যানেলে জয়েন করুন!", show_alert=True)
    await q.message.delete()
    class FakeMsg:
        def __init__(self, u): self.from_user = u; self.text = "/start"
        async def reply(self, *a, **k): return await c.send_message(q.from_user.id, *a, **k)
    await start_cmd(c, FakeMsg(q.from_user))

# ==========================================
# 6. CATEGORY, LANGUAGE, UPCOMING, ZONE ID, REDEEM, PREMIUM, TG TASK
# ==========================================
@app.on_message(filters.command("setcategory", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setcategory(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2: admin_steps[m.from_user.id] = {"step": "add_cat_wait"}; return await m.reply("📂 নতুন ক্যাটাগরির নাম লিখুন:\nউদাহরণ: `18+` অথবা `Action` অথবা `Bachelor Point`")
    cat = parts[1].strip(); await get_db(); await cats_col.update_one({"name": cat}, {"$set": {"name": cat}}, upsert=True); invalidate_home_cache(); await m.reply(f"✅ ক্যাটাগরি অ্যাড হয়েছে: **{cat}**")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("add_cat_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_add_cat_text(c, m):
    cat = m.text.strip(); await get_db(); await cats_col.update_one({"name": cat}, {"$set": {"name": cat}}, upsert=True); admin_steps.pop(m.from_user.id, None); invalidate_home_cache(); await m.reply(f"✅ ক্যাটাগরি অ্যাড হয়েছে: **{cat}**")

@app.on_message(filters.command("deletecategory", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletecategory(c, m):
    await get_db(); cats = await cats_col.find().to_list(100)
    if not cats: return await m.reply("❌ কোনো ক্যাটাগরি নেই! উদাহরণ: `/setcategory Action`")
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {cat['name']}", callback_data=f"delcat_{cat['_id']}")] for cat in cats]))

@app.on_callback_query(filters.regex(r"^delcat_") & filters.user(ADMIN_ID) & unique_cb)
async def delcat_cb(c, q): await get_db(); await cats_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); invalidate_home_cache(); await q.message.edit_text("✅ ক্যাটাগরি ডিলিট হয়েছে!")

@app.on_message(filters.command("setlanguage", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setlanguage(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2: admin_steps[m.from_user.id] = {"step": "add_lang_wait"}; return await m.reply("🌐 নতুন ভাষার নাম লিখুন:\nউদাহরণ: `Bangla` অথবা `Hindi Dubbed`")
    lang = parts[1].strip(); await get_db(); await langs_col.update_one({"name": lang}, {"$set": {"name": lang}}, upsert=True); invalidate_home_cache(); await m.reply(f"✅ ভাষা অ্যাড হয়েছে: **{lang}**")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("add_lang_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_add_lang_text(c, m):
    lang = m.text.strip(); await get_db(); await langs_col.update_one({"name": lang}, {"$set": {"name": lang}}, upsert=True); admin_steps.pop(m.from_user.id, None); invalidate_home_cache(); await m.reply(f"✅ ভাষা অ্যাড হয়েছে: **{lang}**")

@app.on_message(filters.command("deletelanguage", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletelanguage(c, m):
    await get_db(); ls = await langs_col.find().to_list(100)
    if not ls: return await m.reply("❌ কোনো ভাষা সেভ করা নেই! উদাহরণ: `/setlanguage Bangla`")
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {l['name']}", callback_data=f"dellang_{l['_id']}")] for l in ls]))

@app.on_callback_query(filters.regex(r"^dellang_") & filters.user(ADMIN_ID) & unique_cb)
async def dellang_cb(c, q): await get_db(); await langs_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); invalidate_home_cache(); await q.message.edit_text("✅ ভাষা ডিলিট হয়েছে!")

@app.on_message(filters.command("addupcoming", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addupcoming(c, m):
    txt = re.sub(r"^/addupcoming(?:@\w+)?\s*", "", m.text, count=1, flags=re.IGNORECASE).strip()
    if "|" not in txt: admin_steps[m.from_user.id] = {"step": "upc_title_time"}; return await m.reply("📅 আপকামিং মুভি/সিরিজের নাম ও সময় লিখুন:\nউদাহরণ: `Bachelor Point Ep 140 | আজ সন্ধ্যা ৭টা`")
    title, rtime = [x.strip() for x in txt.split("|", 1)]
    await get_db(); conf = await get_config(); live_url = get_live_web_url(conf)
    pid = "upc_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    thumb = ""
    if m.reply_to_message and (m.reply_to_message.photo or m.reply_to_message.document):
        enc, mime, _, err = await extract_image_from_message(c, m.reply_to_message)
        if enc: thumb = await save_poster_to_mongo(db, pid, enc, mime, live_url)
    await upcoming_col.insert_one({"title": title, "release_time": rtime, "thumb_url": thumb, "created_at": datetime.utcnow().isoformat() + "Z"})
    invalidate_home_cache()
    await m.reply(f"✅ আপকামিং অ্যাড হয়েছে!\n🎬 **{title}** | 🕒 {rtime}")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("upc_title_time") & ~filters.regex(r"^/") & unique_msg)
async def handle_upc_text(c, m):
    title, rtime = [x.strip() for x in m.text.split("|", 1)] if "|" in m.text else (m.text.strip(), "Coming Soon")
    admin_steps[m.from_user.id] = {"step": "upc_photo", "title": title, "release_time": rtime}
    await m.reply("🖼 ল্যান্ডস্কোপ পোস্টার ছবি পাঠান:")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("upc_photo") & unique_msg)
async def handle_upc_photo(c, m):
    msg = await m.reply("⏳ আপকামিং পোস্টার MongoDB-তে সেভ হচ্ছে...")
    try:
        enc, mime, _, err = await extract_image_from_message(c, m)
        if err or not enc:
            return await msg.edit_text(err or "❌ ছবি ডাউনলোড করা যায়নি, আবার ছবি পাঠান।")
        st = admin_steps.pop(m.from_user.id); await get_db(); conf = await get_config(); live_url = get_live_web_url(conf)
        pid = "upc_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        thumb = await save_poster_to_mongo(db, pid, enc, mime, live_url)
        await upcoming_col.insert_one({"title": st["title"], "release_time": st["release_time"], "thumb_url": thumb, "created_at": datetime.utcnow().isoformat() + "Z"})
        invalidate_home_cache()
        await msg.edit_text(f"✅ আপকামিং ও পোস্টার MongoDB-তে সেভ হয়েছে: **{st['title']}**")
    except Exception as e:
        await msg.edit_text(f"❌ এরর: `{e}`")

@app.on_message(filters.command("delupcoming", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delupcoming(c, m):
    await get_db(); items = await upcoming_col.find().to_list(100)
    if not items: return await m.reply("❌ কোনো আপকামিং নেই! উদাহরণ: `/addupcoming KGF 3 | কাল রাত ৮টা`")
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {i['title'][:25]}", callback_data=f"delupc_{i['_id']}")] for i in items]))

@app.on_callback_query(filters.regex(r"^delupc_") & filters.user(ADMIN_ID) & unique_cb)
async def delupc_cb(c, q): await get_db(); await upcoming_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); invalidate_home_cache(); await q.message.edit_text("✅ ডিলিট হয়েছে!")

@app.on_message(filters.command("addzoonid", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addzoonid(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2: admin_steps[m.from_user.id] = {"step": "add_zone_wait"}; return await m.reply("🆔 আপনার Monetag Zone ID দিন:\nউদাহরণ: `12242829`")
    zid = parts[1].strip(); await get_db(); await zone_ids_col.update_one({"zone_id": zid}, {"$set": {"zone_id": zid}}, upsert=True); invalidate_home_cache(); await m.reply(f"✅ Zone ID অ্যাড হয়েছে: `{zid}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("add_zone_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_add_zone_text(c, m):
    zid = m.text.strip(); await get_db(); await zone_ids_col.update_one({"zone_id": zid}, {"$set": {"zone_id": zid}}, upsert=True); admin_steps.pop(m.from_user.id, None); invalidate_home_cache(); await m.reply(f"✅ Zone ID অ্যাড হয়েছে: `{zid}`")

@app.on_message(filters.command("deletezoonid", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletezoonid(c, m):
    await get_db(); zids = await zone_ids_col.find().to_list(100)
    if not zids: return await m.reply("❌ কোনো Zone ID নেই! উদাহরণ: `/addzoonid 12242829`")
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {z['zone_id']}", callback_data=f"delzone_{z['_id']}")] for z in zids]))

@app.on_callback_query(filters.regex(r"^delzone_") & filters.user(ADMIN_ID) & unique_cb)
async def delzone_cb(c, q): await get_db(); await zone_ids_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); invalidate_home_cache(); await q.message.edit_text("✅ ডিলিট হয়েছে!")

async def save_telegram_join_task(c, m, target_raw, coin, custom_title=None, custom_link=None):
    await get_db()
    target = int(target_raw) if re.match(r"^-?\d+$", str(target_raw).strip()) else str(target_raw).strip()
    try:
        chat = await c.get_chat(target)
    except Exception as e:
        return await m.reply(f"❌ চ্যানেল/গ্রুপ পাওয়া যায়নি (`{target_raw}`)!\nবটকে আগে ওই চ্যানেল/গ্রুপে **Admin** বানিয়ে তারপর চেষ্টা করুন।\nError: `{e}`")
    is_admin = False
    try:
        me_mem = await c.get_chat_member(chat.id, "me")
        is_admin = me_mem.status in (enums.ChatMemberStatus.ADMINISTRATOR, enums.ChatMemberStatus.OWNER)
    except: pass
    invite_link = custom_link or ""
    if not invite_link:
        if getattr(chat, "username", None):
            invite_link = f"https://t.me/{chat.username}"
        elif getattr(chat, "invite_link", None):
            invite_link = chat.invite_link
        else:
            try: invite_link = await c.export_chat_invite_link(chat.id)
            except: pass
    if not invite_link:
        return await m.reply(f"❌ চ্যানেলের Invite Link পাওয়া যায়নি! বটকে Admin দিন অথবা এভাবে লিংক সহ দিন:\n`/tgtask {chat.id} | {chat.title or 'Join Channel'} | https://t.me/+invite | {coin}`")
    title = custom_title or getattr(chat, "title", None) or getattr(chat, "username", None) or "Join Telegram"
    await tasks_col.insert_one({
        "title": title,
        "task_type": "tg_join",
        "chat_id": str(chat.id),
        "zone_id": "",
        "link": invite_link,
        "coin": int(coin),
        "steps": 0
    })
    warn = "" if is_admin else "\n⚠️ **সতর্কতা:** বট এখনো ওই চ্যানেল/গ্রুপে Admin না হলে Verify কাজ করবে না—বটকে অবশ্যই Admin রাখবেন!"
    await m.reply(f"✅ **Telegram Join Task Added!**\n📢 **{title}** (`{chat.id}`)\n🔗 {invite_link}\n🪙 Reward: `{coin}` Coins{warn}")

@app.on_message(filters.command("tgtask", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_tgtask(c, m):
    clean = re.sub(r"^/tgtask(?:@\w+)?\s*", "", m.text, count=1, flags=re.IGNORECASE).strip()
    if not clean:
        admin_steps[m.from_user.id] = {"step": "tgtask_wait"}
        return await m.reply("📢 টেলিগ্রাম চ্যানেল/গ্রুপের **ID (বা @username) ও কয়েন** দিন:\nউদাহরণ: `-100123456789 50`\nঅথবা: `-100123456789 | My Channel | https://t.me/+abc | 50`\n\n⚠️ _বটকে ওই চ্যানেল/গ্রুপে অবশ্যই Admin থাকতে হবে!_")
    if "|" in clean:
        segs = [x.strip() for x in clean.split("|")]
        if len(segs) == 4 and segs[3].isdigit():
            return await save_telegram_join_task(c, m, segs[0], int(segs[3]), segs[1], segs[2])
        if len(segs) == 3 and segs[2].isdigit():
            return await save_telegram_join_task(c, m, segs[0], int(segs[2]), segs[1])
        if len(segs) == 2 and segs[1].isdigit():
            return await save_telegram_join_task(c, m, segs[0], int(segs[1]))
        return await m.reply("❌ উদাহরণ: `/tgtask -100123456789 50`")
    ps = clean.split()
    if len(ps) < 2 or not ps[-1].isdigit():
        return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/tgtask -100123456789 50`\nঅথবা `/tgtask @mychannel 50`")
    return await save_telegram_join_task(c, m, ps[0], int(ps[-1]))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("tgtask_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_tgtask_wait(c, m):
    txt = m.text.strip(); admin_steps.pop(m.from_user.id, None)
    if "|" in txt:
        segs = [x.strip() for x in txt.split("|")]
        if len(segs) >= 4 and segs[3].isdigit(): return await save_telegram_join_task(c, m, segs[0], int(segs[3]), segs[1], segs[2])
        if len(segs) >= 2 and segs[-1].isdigit(): return await save_telegram_join_task(c, m, segs[0], int(segs[-1]), segs[1] if len(segs) > 2 else None)
    ps = txt.split()
    if len(ps) >= 2 and ps[-1].isdigit():
        return await save_telegram_join_task(c, m, ps[0], int(ps[-1]))
    await m.reply("❌ ভুল ফরম্যাট! উদাহরণ: `-100123456789 50`")

@app.on_message(filters.command("deltgtask", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltgtask(c, m):
    await get_db(); ts = await tasks_col.find({"task_type": "tg_join"}).to_list(100)
    if not ts: return await m.reply("❌ কোনো Telegram Join Task নেই! উদাহরণ: `/tgtask -100123456789 50`")
    btns = [[InlineKeyboardButton(f"❌ {t['title'][:24]} (+{t.get('coin',0)})", callback_data=f"deltask_{t['_id']}")] for t in ts]
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

def parse_duration_seconds(dur_str):
    m = re.match(r"^(\d+)\s*([a-zA-Z]+)$", dur_str.strip().lower())
    if not m: return None
    val, u = int(m.group(1)), m.group(2)
    if u.startswith("s"): return val
    if u.startswith("mi") or u == "m": return val * 60
    if u.startswith("h"): return val * 3600
    if u.startswith("d"): return val * 86400
    if u.startswith("w"): return val * 604800
    if u.startswith("mo"): return val * 2592000
    if u.startswith("y"): return val * 31536000
    return None

@app.on_message(filters.command("addredeem", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addredeem(c, m):
    args = m.text.split()
    if len(args) < 4: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n• প্রিমিয়াম কোড: `/addredeem 1day 10 VIP10`\n• কয়েন কোড: `/addredeem 50 10 COIN50`")
    dur, lim_str, code = args[1], args[2], args[3]
    try: lim = int(lim_str)
    except: return await m.reply("❌ Limit অবশ্যই সংখ্যা হতে হবে! উদাহরণ: `/addredeem 1day 10 VIP10`")
    await get_db(); secs = parse_duration_seconds(dur)
    if secs:
        await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_premium_code": True, "duration_seconds": secs, "duration_label": dur})
        await m.reply(f"✅ **Premium Code Created!**\n🎟 Code: `{code}` | ⏳ `{dur}` | 👥 Limit: `{lim}`")
    elif dur.isdigit():
        await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_random": False, "coins": int(dur)})
        await m.reply(f"✅ **Coin Code Created!**\n🎟 Code: `{code}` | 🪙 `{dur}` | 👥 Limit: `{lim}`")
    else: await m.reply("❌ Duration সঠিক নয়! উদাহরণ: `1day`, `7days`, `1month`, `12h` অথবা কয়েন `50`")

@app.on_message(filters.command("premiumuserlist", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_premiumuserlist(c, m):
    await get_db(); pu = await users_col.find({"premium_until": {"$gt": datetime.now()}}).to_list(None)
    if not pu: return await m.reply("👤 বর্তমানে কোনো Premium User নেই।")
    txt = f"💎 **Premium Users ({len(pu)}):**\n\n"; btns = []
    for u in pu[:30]:
        txt += f"• **{u.get('name','User')}** (`{u['_id']}`) — ⏳ {get_expiry_str(u.get('premium_until')) or 'Active'}\n"
        btns.append([InlineKeyboardButton(f"🗑 Remove {u.get('name','User')} ({u['_id']})", callback_data=f"delpremu_{u['_id']}")])
    await m.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delpremu_") & filters.user(ADMIN_ID) & unique_cb)
async def delpremu_cb(c, q):
    uid = int(q.data.split("_")[1]); await get_db(); await users_col.update_one({"_id": uid}, {"$set": {"premium_until": None}})
    await q.answer("✅ Removed!", show_alert=True); await q.message.edit_text(f"✅ User `{uid}` রিমুভ হয়েছে!")

@app.on_message(filters.command(["deletepremium", "delpremium"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletepremium(c, m):
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit(): return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/deletepremium 12345678`")
    await get_db(); await users_col.update_one({"_id": int(args[1])}, {"$set": {"premium_until": None}}); await m.reply(f"✅ User `{args[1]}` এর প্রিমিয়াম রিমুভ হয়েছে!")

