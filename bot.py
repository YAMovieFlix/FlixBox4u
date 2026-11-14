import os, sys, re, asyncio, threading, random, string, time, requests, json, base64, socket, io, traceback, gzip, hashlib
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from datetime import datetime, timedelta

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
    return "https://flixbox4ubot.onrender.com"

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
posters_col, short_links_col = None, None
sync_db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=8000)["NewBot"]

POSTER_CACHE = {}
EXTRA_URI_CACHE = {"uris": None, "ts": 0}
HOME_DATA_CACHE = {"data": None, "ts": 0}
WEB_POOL = ThreadPoolExecutor(max_workers=8)
TUTORIAL_DL_LOCK = threading.Lock()

def invalidate_home_cache():
    HOME_DATA_CACHE["data"] = None
    HOME_DATA_CACHE["ts"] = 0

def poster_cache_put(pid, raw_bytes, mime="image/jpeg"):
    if not pid or not raw_bytes: return
    if len(POSTER_CACHE) >= 200:
        try: POSTER_CACHE.pop(next(iter(POSTER_CACHE)))
        except: POSTER_CACHE.clear()
    POSTER_CACHE[pid] = (raw_bytes, mime)

def poster_cache_del(pid):
    POSTER_CACHE.pop(pid, None)

def get_live_web_url(conf=None):
    for k in ["WEB_URL", "RENDER_EXTERNAL_URL"]:
        if os.environ.get(k, "").strip(): return os.environ[k].strip().rstrip("/")
    if conf and conf.get("auto_web_url"): return conf["auto_web_url"].rstrip("/")
    return WEB_URL

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
    global posters_col, short_links_col
    if db_client is None:
        db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000); db = db_client["NewBot"]
        users_col, files_col, cats_col, langs_col = db["users"], db["files"], db["categories"], db["languages"]
        pkgs_col, links_col, config_col = db["packages"], db["ad_links"], db["config"]
        channels_col, coupons_col, mongos_col = db["channels"], db["coupons"], db["mongos"]
        tasks_col, titles_col = db["custom_tasks"], db["auto_titles"]
        auto_channels_col, promotions_col, zone_ids_col, upcoming_col = db["auto_channels"], db["promotions"], db["zone_ids"], db["upcoming"]
        posters_col, short_links_col = db["posters"], db["short_links"]
    return db

async def get_config():
    await get_db()
    conf = await config_col.find_one({"_id": "settings"})
    if not conf:
        conf = {
            "_id": "settings", "ref_coin": 10, "ref_on": True, "auto_del_time": 0,
            "autodel_text": "⏳ ফাইলটি নির্দিষ্ট সময় পর অটো ডিলিট হয়ে যাবে।", "frotect": False,
            "ads_on": True, "direk_wait": [5], "prem_vid_wait": 10, "prstep": 1, "regstep": 1,
            "start_logo": None, "start_text": "", "payment_admin": DEFAULT_ADMIN_USERNAME,
            "autovid_msg_id": None, "autovid_chat_id": None, "autovid_time_min": 0,
            "autopost_time_hr": 0, "autopost_idx": 0, "site_name": "Moviee Link",
            "premium_vid_coin": 50, "vid_relock_min": 1440, "spin_coin_min": 10, "spin_coin_max": 50,
            "task_coin": 20, "spin_limit": 5, "task_limit": 5, "autopost_channel": None,
            "autopost_minute": 0, "autopost_channel_idx": 0, "tutorial_vid": None,
            "rec_group_link": "",
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
    for d in await get_extra_dbs_async():
        try:
            doc = await d["files"].find_one({"_id": f_id})
            if doc is not None: return doc, d
        except: pass
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
        invalidate_home_cache()
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
    payload = {"data": b64_data, "mime": mime, "created_at": datetime.utcnow().isoformat() + "Z"}
    try:
        await target_db["posters"].update_one({"_id": poster_id}, {"$set": payload}, upsert=True)
    except Exception as e:
        print("save_poster_to_mongo target_db error, falling back to main db:", e, flush=True)
        await get_db()
        await db["posters"].update_one({"_id": poster_id}, {"$set": payload}, upsert=True)
    try:
        raw, _ = decode_b64_to_bytes(b64_data)
        if raw: poster_cache_put(poster_id, raw, mime)
    except: pass
    invalidate_home_cache()
    base = (live_url or get_live_web_url()).rstrip("/")
    return f"{base}/poster/{poster_id}"

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

def get_tg_photo(thumb, tg_thumb_id=None):
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
                        poster_cache_put(pid, raw, pdoc.get("mime", mime))
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

async def send_post_to_chat(client, chat_id, file_doc, reply_markup):
    cap = f"🔥 **New Trending Upload!**\n\n🎬 **{file_doc['title']}**"
    photo = get_tg_photo(file_doc.get("thumb_url"), file_doc.get("tg_thumb_id"))
    if photo:
        try:
            return await client.send_photo(chat_id, photo=photo, caption=cap, reply_markup=reply_markup)
        except Exception as e:
            print("send_photo fallback to send_message:", e, flush=True)
    return await client.send_message(chat_id, cap, reply_markup=reply_markup)

async def notify_new_post(client, file_doc, config):
    ch_id = config.get("autopost_channel"); markup = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]])
    if ch_id:
        try: await send_post_to_chat(client, int(ch_id), file_doc, markup)
        except: pass
    await get_db()
    async for ch in auto_channels_col.find():
        try:
            msg = await send_post_to_chat(client, int(ch["chat_id"]), file_doc, markup)
            if ch.get("delete_min", 0) > 0: asyncio.create_task(delete_msg_later(client, int(ch["chat_id"]), msg.id, ch["delete_min"] * 60))
        except: pass

# ==========================================
# 3. BACKGROUND TASKS
# ==========================================
async def background_tasks():
    last_av, last_ap, last_cp = time.time(), time.time(), time.time()
    while True:
        await asyncio.sleep(60); now = time.time()
        try:
            config = await get_config(); live_url = get_live_web_url(config)
            if config.get("autovid_time_min", 0) > 0 and config.get("autovid_msg_id") and (now - last_av) >= (config["autovid_time_min"] * 60):
                last_av = time.time(); fc = config.get("autovid_chat_id", ADMIN_ID)
                async for u in users_col.find():
                    if u.get("last_autovid_msg"):
                        try: await app.delete_messages(u["_id"], u["last_autovid_msg"])
                        except: pass
                    try:
                        msg = await app.copy_message(u["_id"], fc, config["autovid_msg_id"])
                        await users_col.update_one({"_id": u["_id"]}, {"$set": {"last_autovid_msg": msg.id}}); await asyncio.sleep(0.05)
                    except: pass

            if config.get("autopost_time_hr", 0) > 0 and (now - last_ap) >= (config["autopost_time_hr"] * 3600):
                last_ap = time.time(); af = await get_all_files_across_dbs()
                if af:
                    idx = config.get("autopost_idx", 0) % len(af); f = af[idx]
                    async for u in users_col.find():
                        try:
                            btn = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", web_app=WebAppInfo(url=f"{live_url}/?uid={u['_id']}"))]])
                            photo = get_tg_photo(f.get("thumb_url"), f.get("tg_thumb_id"))
                            if photo:
                                await app.send_photo(u["_id"], photo=photo, caption=f"🔥 **New Video!**\n\nTitle: {f['title']}", reply_markup=btn)
                            else:
                                await app.send_message(u["_id"], f"🔥 **New Video!**\n\nTitle: {f['title']}", reply_markup=btn)
                            await asyncio.sleep(0.05)
                        except: pass
                    await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_idx": idx + 1}})

            if config.get("autopost_channel") and config.get("autopost_minute", 0) > 0 and (now - last_cp) >= (config["autopost_minute"] * 60):
                last_cp = time.time(); af = await get_all_files_across_dbs()
                if af:
                    ci = config.get("autopost_channel_idx", 0) % len(af); f = af[ci]
                    try:
                        await send_post_to_chat(app, int(config["autopost_channel"]), f, InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]]))
                        await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_channel_idx": ci + 1}})
                    except: pass

            async for ch in auto_channels_col.find():
                c_id, c_int = ch.get("chat_id"), ch.get("interval", 0)
                if not c_id or c_int <= 0 or (now - ch.get("last_post_time", 0)) < (c_int * 60): continue
                af = await get_all_files_across_dbs()
                if af:
                    ci = ch.get("post_idx", 0) % len(af); f = af[ci]
                    try:
                        msg = await send_post_to_chat(app, int(c_id), f, InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]]))
                        await auto_channels_col.update_one({"_id": ch["_id"]}, {"$set": {"last_post_time": time.time(), "post_idx": ci + 1}})
                        if ch.get("delete_min", 0) > 0: asyncio.create_task(delete_msg_later(app, int(c_id), msg.id, ch["delete_min"] * 60))
                    except: pass
        except Exception as e: print("BG Task Error:", e)
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

def get_quality_markup(prefix, skip_cb, finish_label="⏭ Skip / Finish", back_cb=None):
    btns, row = [], []
    for q in QUALITIES:
        row.append(InlineKeyboardButton(f"🎬 {q}", callback_data=f"{prefix}_{q}"))
        if len(row) == 3: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([InlineKeyboardButton("➕ Custom Quality", callback_data=f"{prefix}_custom")])
    btns.append([InlineKeyboardButton(finish_label, callback_data=skip_cb)])
    if back_cb: btns.append([InlineKeyboardButton("⬅️ Back", callback_data=back_cb)])
    return InlineKeyboardMarkup(btns)

def is_in_step(step_name):
    return filters.create(lambda _, __, m: admin_steps.get(m.from_user.id, {}).get("step") == step_name)

async def deliver_cached_file(client, uid, f_id_send, title, config):
    msg = await client.send_message(uid, "⏳ আপনার ফাইল পাঠানো হচ্ছে...")
    try:
        dt = config.get("auto_del_time", 0); cap = f"🎬 **{title}**"
        if dt > 0:
            mn, sc = divmod(dt, 60)
            cap += f"\n\n⏳ **ভিডিওটি {f'{mn} মিনিট {sc} সেকেন্ড' if mn > 0 else f'{sc} সেকেন্ড'} পর অটো ডিলিট হয়ে যাবে!**"
        sm = await client.send_cached_media(chat_id=uid, file_id=f_id_send, caption=cap, protect_content=config.get("frotect", False))
        await msg.delete()
        if dt > 0:
            w = await client.send_message(uid, f"⚠️ {config.get('autodel_text')}")
            asyncio.create_task(delete_msg_later(client, uid, sm.id, dt))
            asyncio.create_task(delete_msg_later(client, uid, w.id, dt))
    except Exception as e: await msg.edit_text(f"❌ ফাইল পাঠাতে সমস্যা হয়েছে! {e}")

# ==========================================
# 5. BOT COMMANDS (START + STATS + SIMPLE CONFIG)
# ==========================================
@app.on_message(filters.command("cancel", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_cancel(c, m): admin_steps.pop(m.from_user.id, None); await m.reply("✅ প্রসেস বাতিল করা হয়েছে!")

@app.on_message(filters.command("myid", case_sensitive=False) & unique_msg)
async def cmd_myid(c, m): await m.reply(f"✅ বট কাজ করছে!\n🆔 আইডি: `{m.from_user.id}`")

@app.on_message(filters.command("cmd", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_list(c, m):
    part1 = """🛠 **সকল কমান্ড ও ব্যবহারের উদাহরণ (পর্ব ১/২):**
💡 _কমান্ডে ক্লিক করলে সরাসরি বটে রান হবে, আর কোড বক্সে ক্লিক করলে কপি হবে!_

🎬 **Movies & Series:**
• /movies — নতুন মুভি এড
  কপি: `/movies NAME RRR 2024`
• /editemovies — মুভি এডিট (নাম লিখে সার্চ করুন)
  কপি: `/editemovies RRR`
• /deletedmovies — মুভি ডিলিট লিস্ট
• /addseries — নতুন ওয়েব সিরিজ এড
  কপি: `/addseries Bachelor Point 2026`
• /editeseries — সিরিজ এডিট (নাম লিখে সার্চ করুন)
  কপি: `/editeseries Bachelor Point`
• /deleteseries — সিরিজ ডিলিট লিস্ট
• /addupcoming — আপকামিং শিডিউল এড (ছবিতে রিপ্লাই করেও করা যাবে)
  কপি: `/addupcoming Pushpa 3 | আজ রাত ৮টা`
• /delupcoming — আপকামিং ডিলিট
• /setcategory — নতুন ক্যাটাগরি এড
  কপি: `/setcategory Action` অথবা `/setcategory 18+`
• /deletecategory — ক্যাটাগরি ডিলিট
• /setlanguage — নতুন ভাষা এড
  কপি: `/setlanguage Bangla`
• /deletelanguage — ভাষা ডিলিট
• /setrecgroup — Movie Recommendation গ্রুপ লিংক সেট
  কপি: `/setrecgroup https://t.me/my_movie_group`
• /delrecgroup — Recommendation গ্রুপ রিমুভ
• /cancel — চলমান প্রসেস বাতিল"""

    part2 = """🛠 **সকল কমান্ড ও ব্যবহারের উদাহরণ (পর্ব ২/২):**

💰 **Monetag, Adsterra, Redeem & Premium:**
• /addzoonid — Monetag Zone ID এড
  কপি: `/addzoonid 12242829`
• /deletezoonid — Zone ID ডিলিট
• /addlink — Adsterra / Direct Ad Link এড
  কপি: `/addlink https://example.com/ad`
• /delink — Ad Link ডিলিট
• /addredeem — প্রিমিয়াম বা কয়েন রিডিম কোড তৈরি
  কপি: `/addredeem 1day 10 VIP10` অথবা `/addredeem 50 10 COIN50`
• /allred — র‍্যান্ডম কয়েন কোড তৈরি
  কপি: `/allred 100 10-50`
• /Premiumuserlist — প্রিমিয়াম ইউজার লিস্ট
• /deletepremium — প্রিমিয়াম রিমুভ
  কপি: `/deletepremium 12345678`
• /addrdiem — নির্দিষ্ট ইউজারকে প্রিমিয়াম দেওয়া
  কপি: `/addrdiem 12345678 30 d`

⚙️ **Site, Earning & Channel Settings:**
• /stats — বট স্ট্যাটিস্টিকস
• /name — সাইটের নাম (কপি: `/name Moviee Link`)
• /setnotice — লাইভ স্ক্রলিং নোটিশ (কপি: `/setnotice নতুন মুভি পেতে জয়েন করুন`)
• /settutorial — ভিডিওতে রিপ্লাই করে টিউটোরিয়াল সেট
• /logo — ছবিতে রিপ্লাই করে স্টার্ট ও মিনি অ্যাপ লোগো সেট
• /addtex — স্টার্ট এক্সট্রা টেক্সট (কপি: `/addtex স্বাগতম!`) | /deltex
• /setpremcoin — আনলক কয়েন সেট (কপি: `/setpremcoin 50`)
• /relocktime — আনলক মেয়াদ মিনিট (কপি: `/relocktime 1440`) | /delrelocktime
• /lockall — সবার আনলক ভিডিও রিসেট
• /spincoin — স্পিন কয়েন (কপি: `/spincoin 10-50`) | কপি: `/spinlimit 5`
• /taskcoin — টাস্ক কয়েন (কপি: `/taskcoin 20`) | কপি: `/tasklimit 5`
• /addtask — কাস্টম টাস্ক (কপি: `/addtask Join Channel | https://t.me/ch | 50`) | /deltask
• /refbonous — রেফার বোনাস (কপি: `/refbonous 20`) | /refbonousoff
• /usd — USD প্যাকেজ (কপি: `/usd 100 5`) | /bdt (কপি: `/bdt 100 50`)
• /addcred — কয়েন প্যাকেজ (কপি: `/addcred 500 = 7 d`) | /delcred
• /addcnl — ইনলাইন চ্যানেল (কপি: `/addcnl Backup https://t.me/ch`) | /delcnl
• /vercnl — Must Join চ্যানেল (কপি: `/vercnl -100123456 https://t.me/ch`) | /delvrcnl
• /autochannel — অটো পোস্ট চ্যানেল (কপি: `/autochannel -100123456 30 60`) | /delautochannel
• /brodcast ও /cnlbdcst — মেসেজে রিপ্লাই করে ব্রডকাস্ট
• /autodel (কপি: `/autodel 600`) | /frotect (কপি: `/frotect on`) | /addadmin (কপি: `/addadmin Sudo_king`)
• /mongo (কপি: `/mongo <uri>`) | /delmongo | /mongostats
• /addfile | /delfile (কপি: `/delfile <id>`) | /delall | /vidtitel (কপি: `/vidtitel <title>`) | /deltitel | /auto | /auto2 | /promot (কপি: `/promot Title | https://t.me/...`) | /delpromot"""

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
        await m.reply(f"📊 **Bot Statistics:**\n\n👥 Total Users: `{tu}`\n🎬 Total Movies/Series: `{tf}`\n💎 Premium Users: `{pu}`\n👤 Regular Users: `{max(0, tu - pu)}`\n🗄 MongoDBs: `{len(dbs)}`\n🌐 Mini App URL: `{live_url}`")
    except Exception as e:
        await m.reply(f"❌ Stats Error: `{e}`")

SIMPLE_CONF = {
    "name": ("site_name", "str", "/name Moviee Link"),
    "setnotice": ("marquee_text", "str", "/setnotice নতুন মুভি পেতে চ্যানেলে জয়েন করুন"),
    "setpremcoin": ("premium_vid_coin", "int", "/setpremcoin 50"),
    "relocktime": ("vid_relock_min", "int", "/relocktime 1440"),
    "taskcoin": ("task_coin", "int", "/taskcoin 20"),
    "spinlimit": ("spin_limit", "int", "/spinlimit 5"),
    "tasklimit": ("task_limit", "int", "/tasklimit 5"),
    "prstep": ("prstep", "int", "/prstep 2"),
    "regstep": ("regstep", "int", "/regstep 1"),
    "addadmin": ("payment_admin", "admin", "/addadmin Sudo_king"),
    "addtex": ("start_text", "str", "/addtex স্বাগতম আমাদের বটে!"),
    "autvidti": ("autovid_time_min", "int", "/autvidti 30"),
    "autpost": ("autopost_time_hr", "int", "/autpost 2"),
    "autodel": ("auto_del_time", "int", "/autodel 600"),
    "autex": ("autodel_text", "str", "/autex ভিডিওটি ১০ মিনিট পর ডিলিট হবে।"),
    "prparadd": ("prem_vid_wait", "int", "/prparadd 10")
}

@app.on_message(filters.command(list(SIMPLE_CONF.keys()) + ["delrelocktime", "deltex", "refbonous", "refbonousoff", "frotect"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_simple_config(c, m):
    await get_db(); parts = m.text.split(None, 1); cmd = parts[0].lstrip("/").lower().split("@")[0]; arg = parts[1].strip() if len(parts) > 1 else ""
    if cmd == "delrelocktime": await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_min": 0}}); invalidate_home_cache(); return await m.reply("✅ Relock Time Deleted!")
    if cmd == "deltex": await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": ""}}); invalidate_home_cache(); return await m.reply("✅ Start Text Deleted!")
    if cmd == "refbonousoff": await config_col.update_one({"_id": "settings"}, {"$set": {"ref_on": False}}); invalidate_home_cache(); return await m.reply("✅ Refer Bonus OFF!")
    if cmd == "refbonous":
        if not arg or not arg.isdigit(): return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/refbonous 20`")
        await config_col.update_one({"_id": "settings"}, {"$set": {"ref_coin": int(arg), "ref_on": True}}); invalidate_home_cache(); return await m.reply(f"✅ Refer Bonus Set: `{arg}` Coins")
    if cmd == "frotect":
        if arg.lower() not in ["on", "off"]: return await m.reply("❌ **সঠিক নিয়ম ও উদাহরণ:**\n`/frotect on` অথবা `/frotect off`")
        await config_col.update_one({"_id": "settings"}, {"$set": {"frotect": arg.lower() == "on"}}); invalidate_home_cache(); return await m.reply(f"✅ Protect Content: `{arg.upper()}`")
    k, tp, ex = SIMPLE_CONF[cmd]
    if not arg: return await m.reply(f"❌ **সঠিক নিয়ম ও উদাহরণ:**\n`{ex}`")
    try:
        val = int(arg) if tp == "int" else (arg.replace("@", "") if tp == "admin" else arg)
        await config_col.update_one({"_id": "settings"}, {"$set": {k: val}}); invalidate_home_cache(); await m.reply(f"✅ সেট হয়েছে: **{k}** = `{val}`")
    except: await m.reply(f"❌ ভুল ইনপুট! উদাহরণ:\n`{ex}`")

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
            await status_msg.edit_text("✅ টিউটোরিয়াল ভিডিও সেট ও ক্যাশ হয়েছে! এখন মিনি অ্যাপে সাথে সাথে প্লে হবে।")
        except Exception as e:
            await status_msg.edit_text(f"✅ টিউটোরিয়াল ভিডিও সেট হয়েছে! (স্ট্রিমিং ব্যাকআপ সক্রিয়)")
    elif len(parts) > 1 and parts[1].strip().startswith("http"):
        await config_col.update_one({"_id": "settings"}, {"$set": {"tutorial_vid": parts[1].strip()}}, upsert=True)
        invalidate_home_cache()
        await m.reply("✅ টিউটোরিয়াল ভিডিও লিংক সেট হয়েছে!")
    else:
        await m.reply("❌ **সঠিক নিয়ম:** কোনো ভিডিওতে রিপ্লাই (Reply) করে `/settutorial` লিখুন অথবা `/settutorial https://...mp4` দিন।")

@app.on_message(filters.command("start", case_sensitive=False) & filters.private & unique_msg)
async def start_cmd(client, message):
    try:
        await get_db(); uid = message.from_user.id; args = message.text.split(); config = await get_config()
        live_url = get_live_web_url(config)
        ref_by, pf = None, None
        if len(args) > 1:
            v = args[1]
            if v.isdigit(): ref_by = int(v)
            elif v.startswith(("file_", "mfile_", "sfile_")): pf = v
            elif v.startswith("ref") and "file" in v:
                try: p = v.replace("ref", "").split("file"); ref_by, pf = int(p[0]), "file_" + p[1]
                except: pass

        user = await users_col.find_one({"_id": uid})
        if not user:
            await users_col.insert_one({"_id": uid, "name": message.from_user.first_name, "balance": 0, "pending_file": None, "premium_until": None, "history": [], "favorites": [], "unlocked_files": {}, "spin_count": 0, "task_count": 0, "last_activity_date": None, "custom_tasks_done": [], "daily_viewed": {}})
            if ref_by and ref_by != uid and config.get("ref_on", True):
                await users_col.update_one({"_id": ref_by}, {"$inc": {"balance": config.get("ref_coin", 10)}})
                try: await client.send_message(ref_by, f"🎉 রেফার বোনাস! +{config.get('ref_coin', 10)} Coins")
                except: pass
            user = await users_col.find_one({"_id": uid})

        if pf: await users_col.update_one({"_id": uid}, {"$set": {"pending_file": pf}}); user["pending_file"] = pf

        not_joined = []
        for ch in await channels_col.find({"type": "must_join"}).to_list(100):
            try: await client.get_chat_member(ch["chat_id"], uid)
            except UserNotParticipant: not_joined.append(ch["link"])
            except: pass
        if not_joined:
            btns = [[InlineKeyboardButton("📢 Join Channel", url=l)] for l in not_joined] + [[InlineKeyboardButton("✅ Joined", callback_data="check_join")]]
            return await message.reply("❌ আগে আমাদের চ্যানেলগুলোতে জয়েন করুন:", reply_markup=InlineKeyboardMarkup(btns))

        if user.get("pending_file"):
            pf_val = user["pending_file"]; await users_col.update_one({"_id": uid}, {"$set": {"pending_file": None}})
            if pf_val.startswith("mfile_"):
                _, f_id, q_idx = pf_val.split("_"); fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    await record_daily_unique_view(tdb, f_id, uid); qs = fd.get("qualities", []); qi = int(q_idx)
                    if 0 <= qi < len(qs): return await deliver_cached_file(client, uid, qs[qi]["file_id"], f"{fd['title']} [{qs[qi]['name']}]", config)
                    elif fd.get("file_id"): return await deliver_cached_file(client, uid, fd["file_id"], fd["title"], config)
                return await message.reply("❌ ফাইল পাওয়া যায়নি!")
            elif pf_val.startswith("sfile_"):
                _, f_id, si, ei, qi = pf_val.split("_"); fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    await record_daily_unique_view(tdb, f_id, uid)
                    try:
                        ssn = fd["seasons"][int(si)]; ep = ssn["episodes"][int(ei)]; qo = ep["qualities"][int(qi)]
                        return await deliver_cached_file(client, uid, qo["file_id"], f"{fd['title']} - S{clean_season_no(ssn['season_no'])} E{clean_ep_no(ep['ep_no'])} [{qo['name']}]", config)
                    except: pass
                return await message.reply("❌ এপিসোড ফাইল পাওয়া যায়নি!")
            else:
                f_id = pf_val.replace("file_", ""); fd, tdb = await find_file_in_all_dbs(f_id)
                if fd is not None and tdb is not None:
                    await record_daily_unique_view(tdb, f_id, uid)
                    fs = fd.get("file_id") or (fd["qualities"][0]["file_id"] if fd.get("qualities") else None)
                    if fs: return await deliver_cached_file(client, uid, fs, fd["title"], config)
                return await message.reply("❌ ফাইল পাওয়া যায়নি!")

        try: await client.set_chat_menu_button(chat_id=uid, menu_button=MenuButtonWebApp(text="Watch Now 🎬", web_app=WebAppInfo(url=f"{live_url}/?uid={uid}")))
        except: pass

        full = f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
        sn = config.get("site_name", "Moviee Link")
        txt = f"👋 **হ্যালো {full}!**\n\n🎬 উপরের টিউটোরিয়াল ভিডিওটি অন্তত একবার দেখে নিন\n\n🍿 এখানে পাবেন **নিত্য নতুন মুভি ও সিরিজ** কালেকশন — নিচের Watch Now বাটনে ক্লিক করো!"
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
# 6. CATEGORY, LANGUAGE, UPCOMING, ZONE ID, REDEEM, PREMIUM
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
    # ==========================================
# 7. MOVIES WORKFLOW (POSTER FIRST IN MONGODB + SEARCH EDIT + BACK BUTTONS)
# ==========================================
async def show_mv_poster_prompt(mo, st, edit=True):
    st["step"] = "mv_poster"; ps = "💎 Premium" if st.get("is_premium") else "👤 Regular"
    txt = f"✅ Language: **{st.get('language') or 'None'}**\n\n৩. 🖼 **ল্যান্ডস্কোপ (16:9) পোস্টার ছবি অথবা ছবির লিংক পাঠান**\n_(আপনার MongoDB ও সাইট URL-এ হোস্ট হবে)_\n🔰 টাইপ: **{ps}**"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("💎 Toggle Premium/Regular", callback_data="mvprem_toggle")], [InlineKeyboardButton("⬅️ Back to Language", callback_data="mvback_lang")]])
    if edit: await mo.edit_text(txt, reply_markup=kb)
    else: await mo.reply(txt, reply_markup=kb)

@app.on_message(filters.command("movies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_movies(c, m):
    name, year = parse_name_year(m.text, "/movies")
    if not name: admin_steps[m.from_user.id] = {"step": "mv_name_year"}; return await m.reply("🎬 **মুভির নাম ও সাল দিন:**\nউদাহরণ: `RRR 2024`")
    admin_steps[m.from_user.id] = {"step": "mv_cat", "name": name, "year": year, "qualities": [], "is_premium": False, "thumb_url": None, "poster_b64": None, "poster_mime": "image/jpeg", "tg_thumb_id": None}
    mk, _ = await get_cat_markup("mvcat", "mvcat_skip"); await m.reply(f"🎬 **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_name_year") & ~filters.regex(r"^/") & unique_msg)
async def handle_mv_name_year(c, m):
    name, year = parse_name_year(m.text, "")
    admin_steps[m.from_user.id] = {"step": "mv_cat", "name": name, "year": year, "qualities": [], "is_premium": False, "thumb_url": None, "poster_b64": None, "poster_mime": "image/jpeg", "tg_thumb_id": None}
    mk, _ = await get_cat_markup("mvcat", "mvcat_skip"); await m.reply(f"🎬 **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)

@app.on_callback_query(filters.regex(r"^mvback_") & filters.user(ADMIN_ID) & unique_cb)
async def mvback_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    target = q.data.split("_", 1)[1]
    if target == "cat":
        st["step"] = "mv_cat"; mk, _ = await get_cat_markup("mvcat", "mvcat_skip")
        return await q.message.edit_text(f"🎬 **{st['name']} {st.get('year','')}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)
    elif target == "lang":
        st["step"] = "mv_lang"; lm, _ = await get_lang_markup("mvlang", "mvlang_skip", "mvback_cat")
        return await q.message.edit_text(f"✅ Category: **{st.get('category','All')}**\n\n২. **Language** সিলেক্ট করুন:", reply_markup=lm)
    elif target == "poster":
        return await show_mv_poster_prompt(q.message, st, edit=True)
    elif target == "qual":
        st["step"] = "mv_qual_wait"
        qs = ", ".join([x["name"] for x in st.get("qualities", [])]) or "None"
        return await q.message.edit_text(f"🎞 অ্যাড করা কোয়ালিটি: `{qs}`\n\n৪. এবার মুভির **Quality** সিলেক্ট করে ভিডিও ফাইল দিন:", reply_markup=get_quality_markup("mvqual", "mvsave_final", "✅ Save Movie Now", "mvback_poster"))

@app.on_callback_query(filters.regex(r"^mvcat_") & filters.user(ADMIN_ID) & unique_cb)
async def mvcat_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_")[1]
    if val == "skip": st["category"] = "All"
    else: cats = await get_categories_list(); st["category"] = cats[int(val)] if int(val) < len(cats) else "All"
    st["step"] = "mv_lang"; lm, _ = await get_lang_markup("mvlang", "mvlang_skip", "mvback_cat")
    await q.message.edit_text(f"✅ Category: **{st['category']}**\n\n২. **Language** সিলেক্ট করুন:", reply_markup=lm)

@app.on_callback_query(filters.regex(r"^mvlang_") & filters.user(ADMIN_ID) & unique_cb)
async def mvlang_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_")[1]
    if val == "skip": st["language"] = ""
    else: langs = await get_languages_list(); st["language"] = langs[int(val)] if int(val) < len(langs) else ""
    await show_mv_poster_prompt(q.message, st, edit=True)

@app.on_callback_query(filters.regex(r"^mvprem_toggle$") & filters.user(ADMIN_ID) & unique_cb)
async def mvprem_toggle_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if st:
        st["is_premium"] = not st.get("is_premium", False)
        await show_mv_poster_prompt(q.message, st, edit=True)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_poster") & unique_msg)
async def handle_mv_poster(c, m):
    msg = await m.reply("⏳ ল্যান্ডস্কোপ পোস্টার নেওয়া হচ্ছে...")
    try:
        enc, mime, tg_fid, err = await extract_image_from_message(c, m)
        if err or not enc:
            return await msg.edit_text(f"{err or '❌ পোস্টার রিসিভ করা যায়নি!'} দয়া করে আবার ছবি পাঠান।")
        st = admin_steps.get(m.from_user.id)
        if not st: return await msg.edit_text("❌ সেশন এক্সপায়ার হয়েছে! আবার `/movies` দিন।")
        st["poster_b64"] = enc; st["poster_mime"] = mime; st["tg_thumb_id"] = tg_fid; st["step"] = "mv_qual_wait"
        await msg.edit_text("✅ **পোস্টার রেডি!**\n\n৪. এবার মুভির **Quality** সিলেক্ট করে ভিডিও ফাইল দিন (শেষে **Save Movie** বাটনে ক্লিক করুন):", reply_markup=get_quality_markup("mvqual", "mvsave_final", "✅ Save Movie Now", "mvback_poster"))
    except Exception as e:
        await msg.edit_text(f"❌ পোস্টার এরর: `{e}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_poster") & ~filters.regex(r"^/") & unique_msg)
async def handle_mv_poster_url(c, m):
    url = m.text.strip()
    if not url.startswith("http"):
        return await m.reply("❌ দয়া করে একটি ল্যান্ডস্কোপ ছবি (Photo) অথবা সঠিক ইমেজ লিংক (`https://...`) পাঠান।")
    msg = await m.reply("⏳ ইমেজ লিংক প্রসেস হচ্ছে...")
    try:
        st = admin_steps.get(m.from_user.id)
        if not st: return await msg.edit_text("❌ সেশন এক্সপায়ার হয়েছে! আবার `/movies` দিন।")
        try:
            r = await asyncio.to_thread(requests.get, url, timeout=10)
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if r.status_code == 200 and r.content and ct.startswith("image/"):
                st["poster_b64"] = base64.b64encode(r.content).decode("utf-8")
                st["poster_mime"] = "image/jpeg" if ct == "image/jpg" else ct
            else:
                st["thumb_url"] = url
        except:
            st["thumb_url"] = url
        st["step"] = "mv_qual_wait"
        await msg.edit_text("✅ **পোস্টার লিংক রেডি!**\n\n৪. এবার মুভির **Quality** সিলেক্ট করে ভিডিও ফাইল দিন:", reply_markup=get_quality_markup("mvqual", "mvsave_final", "✅ Save Movie Now", "mvback_poster"))
    except Exception as e:
        await msg.edit_text(f"❌ এরর: `{e}`")

@app.on_callback_query(filters.regex(r"^mvqual_") & filters.user(ADMIN_ID) & unique_cb)
async def mvqual_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_", 1)[1]
    if val == "custom":
        st["step"] = "mv_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:\nউদাহরণ: `1080p HEVC`", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data="mvback_qual")]]))
    st["current_quality"] = val; st["step"] = "mv_qual_file"
    await q.message.edit_text(f"📤 এবার **{val}** ফাইল পাঠান:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data="mvback_qual")]]))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_mv_custom_qname(c, m):
    st = admin_steps[m.from_user.id]; st["current_quality"] = m.text.strip(); st["step"] = "mv_qual_file"
    await m.reply(f"📤 এবার **{st['current_quality']}** ফাইল পাঠান:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data="mvback_qual")]]))

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_qual_file") & unique_msg)
async def handle_mv_qual_file(c, m):
    st = admin_steps[m.from_user.id]; f_id = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    q_name = st.get("current_quality", "HD"); st["qualities"].append({"name": q_name, "file_id": f_id}); st["step"] = "mv_qual_wait"
    await m.reply(f"✅ **{q_name}** অ্যাড হয়েছে! (`{', '.join([q['name'] for q in st['qualities']])}`)\nআরও কোয়ালিটি যোগ করুন অথবা সেভ করুন:", reply_markup=get_quality_markup("mvqual", "mvsave_final", "✅ Save Movie Now", "mvback_poster"))

async def finalize_save_movie(c, uid, msg_obj):
    st = admin_steps.pop(uid, None)
    if not st: return
    try:
        await msg_obj.edit_text("⏳ MongoDB-তে মুভি ও পোস্টার সেভ হচ্ছে...")
        short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        dbs = await get_extra_dbs_async(); target_db, min_sz = dbs[0], float('inf')
        for d in dbs:
            try:
                s = await d.command("dbstats")
                if s["dataSize"] < min_sz: min_sz = s["dataSize"]; target_db = d
            except: pass
        conf = await get_config(); live_url = get_live_web_url(conf)
        thumb_url = ""
        if st.get("poster_b64"):
            thumb_url = await save_poster_to_mongo(target_db, short_id, st["poster_b64"], st.get("poster_mime", "image/jpeg"), live_url)
        elif st.get("thumb_url"):
            thumb_url = st["thumb_url"]
        ft = f"{st['name']} ({st['year']})" if st.get("year") else st["name"]
        nm = {"_id": short_id, "type": "movie", "title": ft, "name": st["name"], "year": st.get("year", ""), "category": st.get("category", "All"), "language": st.get("language", ""), "is_premium": st.get("is_premium", False), "file_id": st["qualities"][0]["file_id"] if st.get("qualities") else None, "qualities": st.get("qualities", []), "thumb_url": thumb_url, "tg_thumb_id": st.get("tg_thumb_id"), "pinned": False, "ratings": [], "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
        await target_db["files"].insert_one(nm); invalidate_home_cache(); await notify_new_post(c, nm, conf)
        await msg_obj.edit_text(f"✅ **মুভি ও পোস্টার MongoDB-তে সেভ হয়েছে!**\n🎬 **{ft}** | 🆔 `{short_id}`\n🖼 Poster URL: `{thumb_url or 'None'}`")
    except Exception as e:
        traceback.print_exc()
        await msg_obj.edit_text(f"❌ সেভ করতে সমস্যা হয়েছে: `{e}`")

@app.on_callback_query(filters.regex(r"^mvsave_final$") & filters.user(ADMIN_ID) & unique_cb)
async def mvsave_final_cb(c, q): await finalize_save_movie(c, q.from_user.id, q.message)

@app.on_message(filters.command("deletedmovies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deleted_movies(c, m): await send_del_page(m, {"type": {"$ne": "series"}}, "delmv", 0, edit=False)

async def send_del_page(mq, qflt, pfx, page=0, edit=True):
    docs = await get_all_files_across_dbs(qflt)
    if not docs: txt = "❌ কোনো আইটেম নেই!"; return await (mq.message.edit_text(txt) if edit else mq.reply(txt))
    pp = 10; st = page * pp; ch = docs[st:st + pp]
    btns = [[InlineKeyboardButton(f"🗑 {d['title'][:32]} [{d['_id']}]", callback_data=f"{pfx}_{d['_id']}_{page}")] for d in ch]
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{pfx}pg_{page-1}"))
    if st + pp < len(docs): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{pfx}pg_{page+1}"))
    if nav: btns.append(nav)
    txt = f"🗑 **ডিলিট লিস্ট (Total: {len(docs)}):**"
    if edit: await mq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mq.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delmvpg_") & filters.user(ADMIN_ID) & unique_cb)
async def delmvpg_cb(c, q): await send_del_page(q, {"type": {"$ne": "series"}}, "delmv", int(q.data.split("_")[1]), edit=True)

@app.on_callback_query(filters.regex(r"^delmv_") & filters.user(ADMIN_ID) & unique_cb)
async def delmv_cb(c, q):
    _, f_id, pg = q.data.split("_"); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        await tdb["files"].delete_one({"_id": f_id})
        await tdb["posters"].delete_one({"_id": f_id})
        poster_cache_del(f_id)
        invalidate_home_cache()
        await q.answer(f"✅ {doc['title']} ডিলিট হয়েছে!", show_alert=True)
    await send_del_page(q, {"type": {"$ne": "series"}}, "delmv", int(pg), edit=True)

@app.on_message(filters.command("editemovies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_edit_movies(c, m):
    name, year = parse_name_year(m.text, "/editemovies")
    if not name:
        admin_steps[m.from_user.id] = {"step": "edmv_search_wait"}
        return await m.reply("🔍 **যে মুভি এডিট করতে চান, তার নাম লিখুন:**\nউদাহরণ: `RRR` অথবা `RRR 2022`")
    all_m = await get_all_files_across_dbs({"type": {"$ne": "series"}})
    ms = [d for d in all_m if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
    if len(ms) == 1: return await show_edit_movie_panel(m, ms[0]["_id"], edit=False)
    elif ms: return await m.reply(f"🔍 '{name}' এর ফলাফল ({len(ms)} টি):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edmvpick_{d['_id']}")] for d in ms[:20]]))
    return await m.reply(f"❌ '{name}' নামে কোনো মুভি পাওয়া যায়নি!")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_search_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_search(c, m):
    admin_steps.pop(m.from_user.id, None)
    name, year = parse_name_year(m.text, "")
    all_m = await get_all_files_across_dbs({"type": {"$ne": "series"}})
    ms = [d for d in all_m if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
    if len(ms) == 1: return await show_edit_movie_panel(m, ms[0]["_id"], edit=False)
    elif ms: return await m.reply(f"🔍 '{name}' এর ফলাফল ({len(ms)} টি):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edmvpick_{d['_id']}")] for d in ms[:20]]))
    return await m.reply(f"❌ '{name}' নামে কোনো মুভি পাওয়া যায়নি! আবার `/editemovies` দিয়ে সার্চ করুন।")

@app.on_callback_query(filters.regex(r"^edmvpick_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvpick_cb(c, q): await show_edit_movie_panel(q, q.data.split("_")[1], edit=True)

async def show_edit_movie_panel(mq, f_id, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if doc is None: txt = "❌ পাওয়া যায়নি!"; return await (mq.message.edit_text(txt) if edit else mq.reply(txt))
    qs = doc.get("qualities", []); q_str = ", ".join([x["name"] for x in qs]) if qs else ("Single File" if doc.get("file_id") else "None")
    txt = f"✏️ **Edit Movie:** `{doc['title']}`\n🆔 `{f_id}` | 📂 `{doc.get('category','All')}` | 🌐 `{doc.get('language','None')}`\n📌 Pin: `{'Yes' if doc.get('pinned') else 'No'}` | 💎 VIP: `{'Yes' if doc.get('is_premium') else 'No'}`\n🎞 Qualities: `{q_str}`"
    btns = [[InlineKeyboardButton("✏️ Name/Year", callback_data=f"edmv_name_{f_id}"), InlineKeyboardButton("🖼 Poster", callback_data=f"edmv_poster_{f_id}")],
            [InlineKeyboardButton("📂 Category", callback_data=f"edmv_cat_{f_id}"), InlineKeyboardButton("🌐 Language", callback_data=f"edmv_lang_{f_id}")],
            [InlineKeyboardButton("➕ Add Quality", callback_data=f"edmv_addq_{f_id}"), InlineKeyboardButton("🗑 Delete Quality", callback_data=f"edmv_delq_{f_id}")],
            [InlineKeyboardButton("📌 Pin", callback_data=f"edmv_pin_{f_id}"), InlineKeyboardButton("💎 Premium", callback_data=f"edmv_prem_{f_id}")],
            [InlineKeyboardButton("🗑 Delete Movie", callback_data=f"edmv_del_{f_id}")]]
    if edit: await mq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mq.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edmv_") & filters.user(ADMIN_ID) & unique_cb)
async def edmv_cb(c, q):
    _, act, f_id = q.data.split("_", 2); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is None or tdb is None: return await q.answer("❌ পাওয়া যায়নি!", show_alert=True)
    if act == "pin": await tdb["files"].update_one({"_id": f_id}, {"$set": {"pinned": not doc.get("pinned", False)}}); invalidate_home_cache(); return await show_edit_movie_panel(q, f_id, edit=True)
    elif act == "prem": await tdb["files"].update_one({"_id": f_id}, {"$set": {"is_premium": not doc.get("is_premium", False)}}); invalidate_home_cache(); return await show_edit_movie_panel(q, f_id, edit=True)
    elif act == "del": await tdb["files"].delete_one({"_id": f_id}); await tdb["posters"].delete_one({"_id": f_id}); poster_cache_del(f_id); invalidate_home_cache(); return await q.message.edit_text(f"✅ **{doc['title']}** ডিলিট হয়েছে!")
    elif act == "name": admin_steps[q.from_user.id] = {"step": "edmv_name_wait", "f_id": f_id}; return await q.message.edit_text("✏️ নতুন নাম ও সাল দিন (যেমন: `RRR 2024`):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")]]))
    elif act == "poster": admin_steps[q.from_user.id] = {"step": "edmv_poster_wait", "f_id": f_id}; return await q.message.edit_text("🖼 নতুন ল্যান্ডস্কোপ পোস্টার ছবি অথবা ছবির লিংক পাঠান:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")]]))
    elif act == "cat":
        cats = await get_categories_list()
        if not cats: return await q.answer("❌ আগে /setcategory দিয়ে ক্যাটাগরি অ্যাড করুন!", show_alert=True)
        btns = [[InlineKeyboardButton(f"📂 {cn}", callback_data=f"edmvcat_{f_id}_{i}")] for i, cn in enumerate(cats)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")]]
        return await q.message.edit_text("📂 Category সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif act == "lang":
        ls = await get_languages_list()
        if not ls: return await q.answer("❌ আগে /setlanguage দিয়ে ভাষা অ্যাড করুন!", show_alert=True)
        btns = [[InlineKeyboardButton(f"🌐 {ln}", callback_data=f"edmvlang_{f_id}_{i}")] for i, ln in enumerate(ls)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")]]
        return await q.message.edit_text("🌐 Language সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif act == "addq": admin_steps[q.from_user.id] = {"step": "edmv_addq_pick", "f_id": f_id}; return await q.message.edit_text("➕ Quality সিলেক্ট করুন:", reply_markup=get_quality_markup("edmvqadd", f"edmvpick_{f_id}", "⬅️ Back"))
    elif act == "delq":
        qs = doc.get("qualities", [])
        if not qs: return await q.answer("❌ কোনো কোয়ালিটি নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"❌ {ql['name']}", callback_data=f"edmvqdel_{f_id}_{i}")] for i, ql in enumerate(qs)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")]]
        return await q.message.edit_text("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edmvcat_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvcat_cb(c, q):
    _, f_id, idx = q.data.split("_"); cats = await get_categories_list(); _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"category": cats[int(idx)]}}); invalidate_home_cache()
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvlang_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvlang_cb(c, q):
    _, f_id, idx = q.data.split("_"); ls = await get_languages_list(); _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"language": ls[int(idx)]}}); invalidate_home_cache()
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvqdel_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvqdel_cb(c, q):
    _, f_id, idx = q.data.split("_"); idx = int(idx); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        qs = doc.get("qualities", [])
        if 0 <= idx < len(qs): qs.pop(idx)
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"qualities": qs}}); invalidate_home_cache()
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvqadd_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvqadd_cb(c, q):
    val = q.data.split("_", 1)[1]; st = admin_steps.get(q.from_user.id)
    if not st or "f_id" not in st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    f_id = st["f_id"]
    if val == "custom":
        st["step"] = "edmv_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data=f"edmv_addq_{f_id}")]]))
    st["current_quality"] = val; st["step"] = "edmv_addq_file"
    await q.message.edit_text(f"📤 এবার **{val}** ফাইল পাঠান:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data=f"edmv_addq_{f_id}")]]))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_cq(c, m):
    st = admin_steps[m.from_user.id]; st["current_quality"] = m.text.strip(); st["step"] = "edmv_addq_file"
    await m.reply(f"📤 এবার **{m.text.strip()}** ফাইল পাঠান:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data=f"edmv_addq_{st['f_id']}")]]))

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_addq_file") & unique_msg)
async def handle_edmv_qf(c, m):
    st = admin_steps.get(m.from_user.id); f_id, qn = st["f_id"], st["current_quality"]
    fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        await tdb["files"].update_one({"_id": f_id}, {"$push": {"qualities": {"name": qn, "file_id": fid}}})
        if not doc.get("file_id"): await tdb["files"].update_one({"_id": f_id}, {"$set": {"file_id": fid}})
        invalidate_home_cache()
    st["step"] = "edmv_addq_pick"; await m.reply(f"✅ **{qn}** অ্যাড হয়েছে!", reply_markup=get_quality_markup("edmvqadd", f"edmvpick_{f_id}", "⬅️ Back"))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_name_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_name(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; n, y = parse_name_year(m.text, ""); ft = f"{n} ({y})" if y else n; _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"name": n, "year": y, "title": ft}}); invalidate_home_cache()
    await show_edit_movie_panel(m, f_id, edit=False)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_poster_wait") & unique_msg)
async def handle_edmv_poster(c, m):
    f_id = admin_steps[m.from_user.id]["f_id"]; msg = await m.reply("⏳ MongoDB-তে পোস্টার আপডেট হচ্ছে...")
    try:
        enc, mime, tg_fid, err = await extract_image_from_message(c, m)
        if err or not enc:
            return await msg.edit_text(err or "❌ পোস্টার আপডেট ব্যর্থ! আবার ছবি পাঠান বা `/cancel` দিন।")
        _, tdb = await find_file_in_all_dbs(f_id)
        if tdb is None:
            admin_steps.pop(m.from_user.id, None)
            return await msg.edit_text("❌ মুভিটি ডাটাবেজে পাওয়া যায়নি!")
        conf = await get_config(); live_url = get_live_web_url(conf)
        purl = await save_poster_to_mongo(tdb, f_id, enc, mime, live_url)
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": purl, "tg_thumb_id": tg_fid}})
        invalidate_home_cache()
        admin_steps.pop(m.from_user.id, None)
        await msg.delete(); await show_edit_movie_panel(m, f_id, edit=False)
    except Exception as e:
        traceback.print_exc()
        await msg.edit_text(f"❌ পোস্টার আপডেট এরর: `{e}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_poster_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_poster_url(c, m):
    url = m.text.strip()
    if not url.startswith("http"):
        return await m.reply("❌ দয়া করে ছবি অথবা সঠিক ইমেজ লিংক (`https://...`) পাঠান।")
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; msg = await m.reply("⏳ MongoDB-তে পোস্টার আপডেট হচ্ছে...")
    try:
        _, tdb = await find_file_in_all_dbs(f_id)
        if tdb is None: return await msg.edit_text("❌ মুভিটি ডাটাবেজে পাওয়া যায়নি!")
        conf = await get_config(); live_url = get_live_web_url(conf)
        purl = url
        try:
            r = await asyncio.to_thread(requests.get, url, timeout=10)
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if r.status_code == 200 and r.content and ct.startswith("image/"):
                purl = await save_poster_to_mongo(tdb, f_id, base64.b64encode(r.content).decode("utf-8"), "image/jpeg" if ct == "image/jpg" else ct, live_url)
        except: pass
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": purl}})
        invalidate_home_cache()
        await msg.delete(); await show_edit_movie_panel(m, f_id, edit=False)
    except Exception as e:
        await msg.edit_text(f"❌ এরর: `{e}`")

# ==========================================
# 8. SERIES WORKFLOW (POSTER FIRST IN MONGODB + SEARCH EDIT + BACK BUTTONS)
# ==========================================
async def show_sr_poster_prompt(mo, st, edit=True):
    st["step"] = "sr_poster"; ps = "💎 Premium" if st.get("is_premium") else "👤 Regular"
    txt = f"✅ Language: **{st.get('language') or 'None'}**\n\n৩. 🖼 **ল্যান্ডস্কোপ (16:9) পোস্টার ছবি অথবা ছবির লিংক পাঠান**\n_(আপনার MongoDB ও সাইট URL-এ হোস্ট হবে)_\n🔰 টাইপ: **{ps}**"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("💎 Toggle Premium/Regular", callback_data="srprem_toggle")], [InlineKeyboardButton("⬅️ Back to Language", callback_data="srback_lang")]])
    if edit: await mo.edit_text(txt, reply_markup=kb)
    else: await mo.reply(txt, reply_markup=kb)

@app.on_message(filters.command("addseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addseries(c, m):
    name, year = parse_name_year(m.text, "/addseries")
    if not name: admin_steps[m.from_user.id] = {"step": "sr_name_year"}; return await m.reply("📺 **সিরিজের নাম ও সাল দিন:**\nউদাহরণ: `Bachelor Point 2026`")
    admin_steps[m.from_user.id] = {"step": "sr_cat", "name": name, "year": year, "seasons": [], "is_premium": False, "thumb_url": None, "poster_b64": None, "poster_mime": "image/jpeg", "tg_thumb_id": None}
    mk, _ = await get_cat_markup("srcat", "srcat_skip"); await m.reply(f"📺 **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_name_year") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_name_year(c, m):
    name, year = parse_name_year(m.text, "")
    admin_steps[m.from_user.id] = {"step": "sr_cat", "name": name, "year": year, "seasons": [], "is_premium": False, "thumb_url": None, "poster_b64": None, "poster_mime": "image/jpeg", "tg_thumb_id": None}
    mk, _ = await get_cat_markup("srcat", "srcat_skip"); await m.reply(f"📺 **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)

@app.on_callback_query(filters.regex(r"^srcat_") & filters.user(ADMIN_ID) & unique_cb)
async def srcat_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_")[1]
    if val == "skip": st["category"] = "All"
    else: cats = await get_categories_list(); st["category"] = cats[int(val)] if int(val) < len(cats) else "All"
    st["step"] = "sr_lang"; lm, _ = await get_lang_markup("srlang", "srlang_skip", "srback_cat")
    await q.message.edit_text(f"✅ Category: **{st['category']}**\n\n২. **Language** সিলেক্ট করুন:", reply_markup=lm)

@app.on_callback_query(filters.regex(r"^srlang_") & filters.user(ADMIN_ID) & unique_cb)
async def srlang_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_")[1]
    if val == "skip": st["language"] = ""
    else: langs = await get_languages_list(); st["language"] = langs[int(val)] if int(val) < len(langs) else ""
    await show_sr_poster_prompt(q.message, st, edit=True)

@app.on_callback_query(filters.regex(r"^srprem_toggle$") & filters.user(ADMIN_ID) & unique_cb)
async def srprem_toggle_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if st:
        st["is_premium"] = not st.get("is_premium", False)
        await show_sr_poster_prompt(q.message, st, edit=True)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_poster") & unique_msg)
async def handle_sr_poster(c, m):
    msg = await m.reply("⏳ ল্যান্ডস্কোপ পোস্টার নেওয়া হচ্ছে...")
    try:
        enc, mime, tg_fid, err = await extract_image_from_message(c, m)
        if err or not enc:
            return await msg.edit_text(f"{err or '❌ পোস্টার রিসিভ করা যায়নি!'} দয়া করে আবার ছবি পাঠান।")
        st = admin_steps.get(m.from_user.id)
        if not st: return await msg.edit_text("❌ সেশন এক্সপায়ার হয়েছে! আবার `/addseries` দিন।")
        st["poster_b64"] = enc; st["poster_mime"] = mime; st["tg_thumb_id"] = tg_fid
        await prompt_series_season(msg, st, edit=True)
    except Exception as e:
        await msg.edit_text(f"❌ পোস্টার এরর: `{e}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_poster") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_poster_url(c, m):
    url = m.text.strip()
    if not url.startswith("http"):
        return await m.reply("❌ দয়া করে একটি ল্যান্ডস্কোপ ছবি (Photo) অথবা সঠিক ইমেজ লিংক (`https://...`) পাঠান।")
    msg = await m.reply("⏳ ইমেজ লিংক প্রসেস হচ্ছে...")
    try:
        st = admin_steps.get(m.from_user.id)
        if not st: return await msg.edit_text("❌ সেশন এক্সপায়ার হয়েছে! আবার `/addseries` দিন।")
        try:
            r = await asyncio.to_thread(requests.get, url, timeout=10)
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if r.status_code == 200 and r.content and ct.startswith("image/"):
                st["poster_b64"] = base64.b64encode(r.content).decode("utf-8")
                st["poster_mime"] = "image/jpeg" if ct == "image/jpg" else ct
            else:
                st["thumb_url"] = url
        except:
            st["thumb_url"] = url
        await prompt_series_season(msg, st, edit=True)
    except Exception as e:
        await msg.edit_text(f"❌ এরর: `{e}`")

async def prompt_series_season(mo, st, edit=True):
    st["step"] = "sr_season_no"; ns = len(st["seasons"]) + 1
    btns = [[InlineKeyboardButton(f"📺 Season {ns}", callback_data=f"srssn_{ns}")]]
    for idx, s in enumerate(st["seasons"]):
        btns.append([InlineKeyboardButton(f"📂 Edit Season {s['season_no']} ({len(s.get('episodes', []))} Eps)", callback_data=f"srssn_{s['season_no']}")])
    btns.append([InlineKeyboardButton("✅ Save Entire Series", callback_data="srsave_final")])
    btns.append([InlineKeyboardButton("⬅️ Back to Poster", callback_data="srback_poster")])
    txt = f"✅ পোস্টার রেডি!\n\n৪. **Season নম্বর দিন** (যেমন: `1` বা `5`):"
    if edit: await mo.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mo.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

def get_or_create_season(st, sno):
    sno = clean_season_no(sno)
    for i, s in enumerate(st["seasons"]):
        if clean_season_no(s["season_no"]).lower() == sno.lower(): return i
    st["seasons"].append({"season_no": sno, "episodes": []}); return len(st["seasons"]) - 1

def get_or_create_episode(st, si, eno):
    eno = clean_ep_no(eno)
    eps = st["seasons"][si]["episodes"]
    for i, e in enumerate(eps):
        if clean_ep_no(e["ep_no"]).lower() == eno.lower(): return i
    eps.append({"ep_no": eno, "qualities": []}); return len(eps) - 1

async def prompt_series_episode(mo, st, edit=True):
    st["step"] = "sr_ep_no"; ssn = st["seasons"][st["cur_s_idx"]]; ne = len(ssn["episodes"]) + 1
    btns = [[InlineKeyboardButton(f"🎬 Episode {ne}", callback_data=f"srep_{ne}")]]
    btns.append([InlineKeyboardButton("➕ Add Another Season", callback_data="srssn_new"), InlineKeyboardButton("⬅️ Back to Season", callback_data="srback_season")])
    btns.append([InlineKeyboardButton("✅ Save Entire Series", callback_data="srsave_final")])
    txt = f"📺 **Season {ssn['season_no']}**\nএবার **Episode নম্বর দিন** (যেমন: `1` বা `134`):"
    if edit: await mo.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mo.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

async def show_sr_quality_prompt(mo, st, edit=True):
    st["step"] = "sr_qual_wait"
    ssn = st["seasons"][st["cur_s_idx"]]; ep = ssn["episodes"][st["cur_e_idx"]]
    added = ", ".join([q["name"] for q in ep.get("qualities", [])]) or "None"
    btns = get_quality_markup("srqual", "srep_next", "⏭ Next Episode", "srback_ep").inline_keyboard + [[InlineKeyboardButton("✅ Save Entire Series", callback_data="srsave_final")]]
    txt = f"📺 S{ssn['season_no']} | 🎬 E{ep['ep_no']}\n🎞 Added: `{added}`\n\nআনলিমিটেড **Quality** অ্যাড করুন:"
    if edit: await mo.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mo.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^srback_") & filters.user(ADMIN_ID) & unique_cb)
async def srback_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    target = q.data.split("_", 1)[1]
    if target == "cat":
        st["step"] = "sr_cat"; mk, _ = await get_cat_markup("srcat", "srcat_skip")
        return await q.message.edit_text(f"📺 **{st['name']} {st.get('year','')}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=mk)
    elif target == "lang":
        st["step"] = "sr_lang"; lm, _ = await get_lang_markup("srlang", "srlang_skip", "srback_cat")
        return await q.message.edit_text(f"✅ Category: **{st.get('category','All')}**\n\n২. **Language** সিলেক্ট করুন:", reply_markup=lm)
    elif target == "poster":
        return await show_sr_poster_prompt(q.message, st, edit=True)
    elif target == "season":
        return await prompt_series_season(q.message, st, edit=True)
    elif target == "ep":
        return await prompt_series_episode(q.message, st, edit=True)
    elif target == "qual":
        return await show_sr_quality_prompt(q.message, st, edit=True)

@app.on_callback_query(filters.regex(r"^srssn_") & filters.user(ADMIN_ID) & unique_cb)
async def srssn_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    v = q.data.split("_", 1)[1]
    if v == "new": return await prompt_series_season(q.message, st, edit=True)
    st["cur_s_idx"] = get_or_create_season(st, v); await prompt_series_episode(q.message, st, edit=True)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_season_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_sno(c, m): st = admin_steps[m.from_user.id]; st["cur_s_idx"] = get_or_create_season(st, m.text.strip()); await prompt_series_episode(m, st, edit=False)

@app.on_callback_query(filters.regex(r"^srep_") & filters.user(ADMIN_ID) & unique_cb)
async def srep_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    v = q.data.split("_", 1)[1]
    if v == "next": return await prompt_series_episode(q.message, st, edit=True)
    st["cur_e_idx"] = get_or_create_episode(st, st["cur_s_idx"], v)
    await show_sr_quality_prompt(q.message, st, edit=True)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_ep_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_eno(c, m):
    st = admin_steps[m.from_user.id]; st["cur_e_idx"] = get_or_create_episode(st, st["cur_s_idx"], m.text.strip())
    await show_sr_quality_prompt(m, st, edit=False)

@app.on_callback_query(filters.regex(r"^srqual_") & filters.user(ADMIN_ID) & unique_cb)
async def srqual_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    v = q.data.split("_", 1)[1]
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data="srback_qual")], [InlineKeyboardButton("⏭ Next Episode", callback_data="srep_next"), InlineKeyboardButton("✅ Save Series", callback_data="srsave_final")]])
    if v == "custom":
        st["step"] = "sr_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:", reply_markup=back_kb)
    st["current_quality"] = v; st["step"] = "sr_qual_file"
    await q.message.edit_text(f"📤 এবার **{v}** এপিসোড ফাইল পাঠান:", reply_markup=back_kb)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_cq(c, m):
    st = admin_steps[m.from_user.id]; st["current_quality"] = m.text.strip(); st["step"] = "sr_qual_file"
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data="srback_qual")], [InlineKeyboardButton("⏭ Next Episode", callback_data="srep_next"), InlineKeyboardButton("✅ Save Series", callback_data="srsave_final")]])
    await m.reply(f"📤 এবার **{st['current_quality']}** ফাইল পাঠান:", reply_markup=back_kb)

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_qual_file") & unique_msg)
async def handle_sr_qf(c, m):
    st = admin_steps[m.from_user.id]; fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    qn = st.get("current_quality", "HD"); ssn = st["seasons"][st["cur_s_idx"]]; ep = ssn["episodes"][st["cur_e_idx"]]
    ep["qualities"].append({"name": qn, "file_id": fid}); st["step"] = "sr_qual_wait"
    btns = get_quality_markup("srqual", "srep_next", "⏭ Next Episode", "srback_ep").inline_keyboard + [[InlineKeyboardButton("✅ Save Entire Series", callback_data="srsave_final")]]
    await m.reply(f"✅ S{ssn['season_no']} E{ep['ep_no']} - **{qn}** অ্যাড হয়েছে!", reply_markup=InlineKeyboardMarkup(btns))

async def finalize_save_series(c, uid, mo):
    st = admin_steps.pop(uid, None)
    if not st: return
    try:
        await mo.edit_text("⏳ MongoDB-তে সিরিজ ও পোস্টার সেভ হচ্ছে...")
        sid = ''.join(random.choices(string.ascii_letters + string.digits, k=8)); dbs = await get_extra_dbs_async(); tdb, ms = dbs[0], float('inf')
        for d in dbs:
            try:
                s = await d.command("dbstats")
                if s["dataSize"] < ms: ms = s["dataSize"]; tdb = d
            except: pass
        conf = await get_config(); live_url = get_live_web_url(conf)
        thumb_url = ""
        if st.get("poster_b64"):
            thumb_url = await save_poster_to_mongo(tdb, sid, st["poster_b64"], st.get("poster_mime", "image/jpeg"), live_url)
        elif st.get("thumb_url"):
            thumb_url = st["thumb_url"]
        ft = f"{st['name']} ({st['year']})" if st.get("year") else st["name"]
        ns = {"_id": sid, "type": "series", "title": ft, "name": st["name"], "year": st.get("year", ""), "category": st.get("category", "All"), "language": st.get("language", ""), "is_premium": st.get("is_premium", False), "file_id": None, "seasons": st.get("seasons", []), "thumb_url": thumb_url, "tg_thumb_id": st.get("tg_thumb_id"), "pinned": False, "ratings": [], "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
        await tdb["files"].insert_one(ns); invalidate_home_cache(); await notify_new_post(c, ns, conf)
        await mo.edit_text(f"✅ **সিরিজ ও পোস্টার MongoDB-তে সেভ হয়েছে!**\n📺 **{ft}** | 🆔 `{sid}`\n🖼 Poster URL: `{thumb_url or 'None'}`")
    except Exception as e:
        traceback.print_exc()
        await mo.edit_text(f"❌ সেভ করতে সমস্যা হয়েছে: `{e}`")

@app.on_callback_query(filters.regex(r"^srsave_final$") & filters.user(ADMIN_ID) & unique_cb)
async def srsave_final_cb(c, q): await finalize_save_series(c, q.from_user.id, q.message)

@app.on_message(filters.command("deleteseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delete_series(c, m): await send_del_page(m, {"type": "series"}, "delsr", 0, edit=False)

@app.on_callback_query(filters.regex(r"^delsrpg_") & filters.user(ADMIN_ID) & unique_cb)
async def delsrpg_cb(c, q): await send_del_page(q, {"type": "series"}, "delsr", int(q.data.split("_")[1]), edit=True)

@app.on_callback_query(filters.regex(r"^delsr_") & filters.user(ADMIN_ID) & unique_cb)
async def delsr_cb(c, q):
    _, f_id, pg = q.data.split("_"); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        await tdb["files"].delete_one({"_id": f_id})
        await tdb["posters"].delete_one({"_id": f_id})
        poster_cache_del(f_id)
        invalidate_home_cache()
        await q.answer(f"✅ {doc['title']} ডিলিট হয়েছে!", show_alert=True)
    await send_del_page(q, {"type": "series"}, "delsr", int(pg), edit=True)

@app.on_message(filters.command("editeseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_edit_series(c, m):
    name, year = parse_name_year(m.text, "/editeseries")
    if not name:
        admin_steps[m.from_user.id] = {"step": "edsr_search_wait"}
        return await m.reply("🔍 **যে ওয়েব সিরিজ এডিট করতে চান, তার নাম লিখুন:**\nউদাহরণ: `Bachelor Point`")
    alls = await get_all_files_across_dbs({"type": "series"})
    ms = [d for d in alls if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
    if len(ms) == 1: return await show_edit_series_panel(m, ms[0]["_id"], edit=False)
    elif ms: return await m.reply(f"🔍 '{name}' এর ফলাফল ({len(ms)} টি):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edsrpick_{d['_id']}")] for d in ms[:20]]))
    return await m.reply(f"❌ '{name}' নামে কোনো সিরিজ পাওয়া যায়নি!")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_search_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_search(c, m):
    admin_steps.pop(m.from_user.id, None)
    name, year = parse_name_year(m.text, "")
    alls = await get_all_files_across_dbs({"type": "series"})
    ms = [d for d in alls if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
    if len(ms) == 1: return await show_edit_series_panel(m, ms[0]["_id"], edit=False)
    elif ms: return await m.reply(f"🔍 '{name}' এর ফলাফল ({len(ms)} টি):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edsrpick_{d['_id']}")] for d in ms[:20]]))
    return await m.reply(f"❌ '{name}' নামে কোনো সিরিজ পাওয়া যায়নি! আবার `/editeseries` দিয়ে সার্চ করুন।")

@app.on_callback_query(filters.regex(r"^edsrpick_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrpick_cb(c, q): await show_edit_series_panel(q, q.data.split("_")[1], edit=True)

async def show_edit_series_panel(mq, f_id, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if doc is None: txt = "❌ পাওয়া যায়নি!"; return await (mq.message.edit_text(txt) if edit else mq.reply(txt))
    ssns = doc.get("seasons", []); te = sum(len(s.get("episodes", [])) for s in ssns)
    txt = f"📺 **Edit Series:** `{doc['title']}`\n🆔 `{f_id}` | 📂 `{doc.get('category','All')}` | 🌐 `{doc.get('language','None')}`\n📌 Pin: `{'Yes' if doc.get('pinned') else 'No'}` | 💎 VIP: `{'Yes' if doc.get('is_premium') else 'No'}`\n📚 Seasons: `{len(ssns)}` | Eps: `{te}`"
    btns = [[InlineKeyboardButton("✏️ Name/Year", callback_data=f"edsr_name_{f_id}"), InlineKeyboardButton("🖼 Poster", callback_data=f"edsr_poster_{f_id}")],
            [InlineKeyboardButton("📂 Category", callback_data=f"edsr_cat_{f_id}"), InlineKeyboardButton("🌐 Language", callback_data=f"edsr_lang_{f_id}")],
            [InlineKeyboardButton("➕ Add Season", callback_data=f"edsr_addssn_{f_id}"), InlineKeyboardButton("📚 Manage Seasons", callback_data=f"edsr_mng_{f_id}")],
            [InlineKeyboardButton("📌 Pin", callback_data=f"edsr_pin_{f_id}"), InlineKeyboardButton("💎 Premium", callback_data=f"edsr_prem_{f_id}")],
            [InlineKeyboardButton("🗑 Delete Series", callback_data=f"edsr_del_{f_id}")]]
    if edit: await mq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mq.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsr_") & filters.user(ADMIN_ID) & unique_cb)
async def edsr_cb(c, q):
    _, act, f_id = q.data.split("_", 2); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is None or tdb is None: return await q.answer("❌ পাওয়া যায়নি!", show_alert=True)
    if act == "pin": await tdb["files"].update_one({"_id": f_id}, {"$set": {"pinned": not doc.get("pinned", False)}}); invalidate_home_cache(); return await show_edit_series_panel(q, f_id, edit=True)
    elif act == "prem": await tdb["files"].update_one({"_id": f_id}, {"$set": {"is_premium": not doc.get("is_premium", False)}}); invalidate_home_cache(); return await show_edit_series_panel(q, f_id, edit=True)
    elif act == "del": await tdb["files"].delete_one({"_id": f_id}); await tdb["posters"].delete_one({"_id": f_id}); poster_cache_del(f_id); invalidate_home_cache(); return await q.message.edit_text(f"✅ **{doc['title']}** ডিলিট হয়েছে!")
    elif act == "name": admin_steps[q.from_user.id] = {"step": "edsr_name_wait", "f_id": f_id}; return await q.message.edit_text("✏️ নতুন নাম ও সাল দিন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]))
    elif act == "poster": admin_steps[q.from_user.id] = {"step": "edsr_poster_wait", "f_id": f_id}; return await q.message.edit_text("🖼 নতুন ল্যান্ডস্কোপ পোস্টার ছবি অথবা ছবির লিংক দিন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]))
    elif act == "cat":
        cats = await get_categories_list()
        if not cats: return await q.answer("❌ আগে /setcategory দিয়ে ক্যাটাগরি অ্যাড করুন!", show_alert=True)
        btns = [[InlineKeyboardButton(f"📂 {cn}", callback_data=f"edsrcat_{f_id}_{i}")] for i, cn in enumerate(cats)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]
        return await q.message.edit_text("📂 Category সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif act == "lang":
        ls = await get_languages_list()
        if not ls: return await q.answer("❌ আগে /setlanguage দিয়ে ভাষা অ্যাড করুন!", show_alert=True)
        btns = [[InlineKeyboardButton(f"🌐 {ln}", callback_data=f"edsrlang_{f_id}_{i}")] for i, ln in enumerate(ls)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]
        return await q.message.edit_text("🌐 Language সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif act == "addssn": admin_steps[q.from_user.id] = {"step": "edsr_addssn_no", "f_id": f_id}; return await q.message.edit_text("📺 নতুন Season নম্বর লিখুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]))
    elif act == "mng":
        ssns = doc.get("seasons", [])
        if not ssns: return await q.answer("❌ কোনো সিজন নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"📺 Season {clean_season_no(s['season_no'])} ({len(s.get('episodes', []))} Eps)", callback_data=f"edsrs_{f_id}_{i}")] for i, s in enumerate(ssns)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")]]
        return await q.message.edit_text("📚 সিজন সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsrcat_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrcat_cb(c, q):
    _, f_id, idx = q.data.split("_"); cats = await get_categories_list(); _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"category": cats[int(idx)]}}); invalidate_home_cache()
    await show_edit_series_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edsrlang_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrlang_cb(c, q):
    _, f_id, idx = q.data.split("_"); ls = await get_languages_list(); _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"language": ls[int(idx)]}}); invalidate_home_cache()
    await show_edit_series_panel(q, f_id, edit=True)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_name_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_name(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; n, y = parse_name_year(m.text, ""); ft = f"{n} ({y})" if y else n; _, tdb = await find_file_in_all_dbs(f_id)
    if tdb is not None: await tdb["files"].update_one({"_id": f_id}, {"$set": {"name": n, "year": y, "title": ft}}); invalidate_home_cache()
    await show_edit_series_panel(m, f_id, edit=False)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_poster_wait") & unique_msg)
async def handle_edsr_poster(c, m):
    f_id = admin_steps[m.from_user.id]["f_id"]; msg = await m.reply("⏳ MongoDB-তে পোস্টার আপডেট হচ্ছে...")
    try:
        enc, mime, tg_fid, err = await extract_image_from_message(c, m)
        if err or not enc:
            return await msg.edit_text(err or "❌ পোস্টার আপডেট ব্যর্থ! আবার ছবি পাঠান বা `/cancel` দিন।")
        _, tdb = await find_file_in_all_dbs(f_id)
        if tdb is None:
            admin_steps.pop(m.from_user.id, None)
            return await msg.edit_text("❌ সিরিজটি ডাটাবেজে পাওয়া যায়নি!")
        conf = await get_config(); live_url = get_live_web_url(conf)
        purl = await save_poster_to_mongo(tdb, f_id, enc, mime, live_url)
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": purl, "tg_thumb_id": tg_fid}})
        invalidate_home_cache()
        admin_steps.pop(m.from_user.id, None)
        await msg.delete(); await show_edit_series_panel(m, f_id, edit=False)
    except Exception as e:
        traceback.print_exc()
        await msg.edit_text(f"❌ পোস্টার আপডেট এরর: `{e}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_poster_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_poster_url(c, m):
    url = m.text.strip()
    if not url.startswith("http"):
        return await m.reply("❌ দয়া করে ছবি অথবা সঠিক ইমেজ লিংক (`https://...`) পাঠান।")
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; msg = await m.reply("⏳ MongoDB-তে পোস্টার আপডেট হচ্ছে...")
    try:
        _, tdb = await find_file_in_all_dbs(f_id)
        if tdb is None: return await msg.edit_text("❌ সিরিজটি ডাটাবেজে পাওয়া যায়নি!")
        conf = await get_config(); live_url = get_live_web_url(conf)
        purl = url
        try:
            r = await asyncio.to_thread(requests.get, url, timeout=10)
            ct = r.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            if r.status_code == 200 and r.content and ct.startswith("image/"):
                purl = await save_poster_to_mongo(tdb, f_id, base64.b64encode(r.content).decode("utf-8"), "image/jpeg" if ct == "image/jpg" else ct, live_url)
        except: pass
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": purl}})
        invalidate_home_cache()
        await msg.delete(); await show_edit_series_panel(m, f_id, edit=False)
    except Exception as e:
        await msg.edit_text(f"❌ এরর: `{e}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addssn_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_addssn(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; sno = clean_season_no(m.text.strip()); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        ssns = doc.get("seasons", []); si = next((i for i, s in enumerate(ssns) if clean_season_no(s["season_no"]).lower() == sno.lower()), None)
        if si is None: ssns.append({"season_no": sno, "episodes": []}); si = len(ssns) - 1; await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
        await show_edit_season_panel(m, f_id, si, edit=False)

async def show_edit_season_panel(mq, f_id, si, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if doc is None or si >= len(doc.get("seasons", [])): return
    ssn = doc["seasons"][si]
    btns = [[InlineKeyboardButton(f"🎬 Ep {clean_ep_no(e['ep_no'])} ({len(e.get('qualities', []))} Q)", callback_data=f"edsrep_view_{f_id}_{si}_{ei}")] for ei, e in enumerate(ssn.get("episodes", []))]
    btns += [[InlineKeyboardButton("➕ Add Episode", callback_data=f"edsrep_add_{f_id}_{si}")], [InlineKeyboardButton("🗑 Delete Season", callback_data=f"edsrep_delssn_{f_id}_{si}")], [InlineKeyboardButton("⬅️ Back", callback_data=f"edsr_mng_{f_id}")]]
    txt = f"📺 **{doc['title']} — Season {clean_season_no(ssn['season_no'])}**"
    if edit: await mq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mq.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsrs_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrs_cb(c, q): _, f_id, si = q.data.split("_"); await show_edit_season_panel(q, f_id, int(si), edit=True)

@app.on_callback_query(filters.regex(r"^edsrep_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrep_cb(c, q):
    ps = q.data.split("_"); act = ps[1]
    if act == "view": return await show_edit_ep_panel(q, ps[2], int(ps[3]), int(ps[4]), edit=True)
    elif act == "add": admin_steps[q.from_user.id] = {"step": "edsr_addep_no", "f_id": ps[2], "s_idx": int(ps[3])}; return await q.message.edit_text("🎬 নতুন Episode নম্বর লিখুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrs_{ps[2]}_{ps[3]}")]]))
    elif act == "delssn":
        f_id, si = ps[2], int(ps[3]); doc, tdb = await find_file_in_all_dbs(f_id)
        if doc is not None and tdb is not None:
            ssns = doc.get("seasons", [])
            if 0 <= si < len(ssns): ssns.pop(si)
            await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
        return await show_edit_series_panel(q, f_id, edit=True)
    elif act == "delep":
        f_id, si, ei = ps[2], int(ps[3]), int(ps[4]); doc, tdb = await find_file_in_all_dbs(f_id)
        if doc is not None and tdb is not None:
            ssns = doc.get("seasons", [])
            if 0 <= si < len(ssns) and 0 <= ei < len(ssns[si]["episodes"]): ssns[si]["episodes"].pop(ei)
            await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
        return await show_edit_season_panel(q, f_id, si, edit=True)
    elif act == "addq":
        f_id, si, ei = ps[2], int(ps[3]), int(ps[4]); admin_steps[q.from_user.id] = {"step": "edsr_addq_pick", "f_id": f_id, "s_idx": si, "e_idx": ei}
        return await q.message.edit_text("➕ Quality সিলেক্ট করুন:", reply_markup=get_quality_markup("edsrqadd", f"edsrep_view_{f_id}_{si}_{ei}", "⬅️ Back"))
    elif act == "delqmenu":
        f_id, si, ei = ps[2], int(ps[3]), int(ps[4]); doc, _ = await find_file_in_all_dbs(f_id); qs = doc["seasons"][si]["episodes"][ei].get("qualities", [])
        if not qs: return await q.answer("❌ কোনো কোয়ালিটি নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"❌ {ql['name']}", callback_data=f"edsrqdel_{f_id}_{si}_{ei}_{qi}")] for qi, ql in enumerate(qs)] + [[InlineKeyboardButton("⬅️ Back", callback_data=f"edsrep_view_{f_id}_{si}_{ei}")]]
        return await q.message.edit_text("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

async def show_edit_ep_panel(mq, f_id, si, ei, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id); ssn = doc["seasons"][si]; ep = ssn["episodes"][ei]
    q_str = ", ".join([x["name"] for x in ep.get("qualities", [])]) or "None"
    txt = f"🎬 **{doc['title']}** | S{clean_season_no(ssn['season_no'])} E{clean_ep_no(ep['ep_no'])}\n🎞 Qualities: `{q_str}`"
    btns = [[InlineKeyboardButton("➕ Add Quality", callback_data=f"edsrep_addq_{f_id}_{si}_{ei}"), InlineKeyboardButton("🗑 Delete Quality", callback_data=f"edsrep_delqmenu_{f_id}_{si}_{ei}")],
            [InlineKeyboardButton("🗑 Delete Episode", callback_data=f"edsrep_delep_{f_id}_{si}_{ei}")], [InlineKeyboardButton("⬅️ Back", callback_data=f"edsrs_{f_id}_{si}")]]
    if edit: await mq.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await mq.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addep_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_addep(c, m):
    st = admin_steps.pop(m.from_user.id); f_id, si, eno = st["f_id"], st["s_idx"], clean_ep_no(m.text.strip()); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        ssns = doc.get("seasons", []); eps = ssns[si]["episodes"]; ei = next((i for i, e in enumerate(eps) if clean_ep_no(e["ep_no"]).lower() == eno.lower()), None)
        if ei is None: eps.append({"ep_no": eno, "qualities": []}); ei = len(eps) - 1; await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
        await show_edit_ep_panel(m, f_id, si, ei, edit=False)

@app.on_callback_query(filters.regex(r"^edsrqdel_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrqdel_cb(c, q):
    _, f_id, si, ei, qi = q.data.split("_"); si, ei, qi = int(si), int(ei), int(qi); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None:
        ssns = doc.get("seasons", []); qs = ssns[si]["episodes"][ei]["qualities"]
        if 0 <= qi < len(qs): qs.pop(qi)
        await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
    await show_edit_ep_panel(q, f_id, si, ei, edit=True)

@app.on_callback_query(filters.regex(r"^edsrqadd_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrqadd_cb(c, q):
    v = q.data.split("_", 1)[1]; st = admin_steps.get(q.from_user.id)
    if not st or "f_id" not in st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    f_id, si, ei = st["f_id"], st["s_idx"], st["e_idx"]
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data=f"edsrep_addq_{f_id}_{si}_{ei}")]])
    if v == "custom": st["step"] = "edsr_custom_qname"; return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:", reply_markup=back_kb)
    st["current_quality"] = v; st["step"] = "edsr_addq_file"; await q.message.edit_text(f"📤 এবার **{v}** ফাইল পাঠান:", reply_markup=back_kb)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_cq(c, m):
    st = admin_steps[m.from_user.id]; st["current_quality"] = m.text.strip(); st["step"] = "edsr_addq_file"
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Quality", callback_data=f"edsrep_addq_{st['f_id']}_{st['s_idx']}_{st['e_idx']}")]])
    await m.reply(f"📤 এবার **{m.text.strip()}** ফাইল পাঠান:", reply_markup=back_kb)

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addq_file") & unique_msg)
async def handle_edsr_qf(c, m):
    st = admin_steps.get(m.from_user.id); f_id, si, ei, qn = st["f_id"], st["s_idx"], st["e_idx"], st["current_quality"]
    fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id); doc, tdb = await find_file_in_all_dbs(f_id)
    if doc is not None and tdb is not None: ssns = doc.get("seasons", []); ssns[si]["episodes"][ei]["qualities"].append({"name": qn, "file_id": fid}); await tdb["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}}); invalidate_home_cache()
    st["step"] = "edsr_addq_pick"; await m.reply(f"✅ **{qn}** অ্যাড হয়েছে!", reply_markup=get_quality_markup("edsrqadd", f"edsrep_view_{f_id}_{si}_{ei}", "⬅️ Back"))

# ==========================================
# 9. LEGACY COMMANDS (WITH EXAMPLES WHEN EMPTY)
# ==========================================
@app.on_message(filters.command("addfile", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addfile(c, m): admin_steps[m.from_user.id] = {"step": "name"}; await m.reply("১. ফাইলের নাম/টাইটেল দিন (যেমন: `Pushpa 2`):")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("name") & ~filters.regex(r"^/") & unique_msg)
async def handle_admin_name(c, m): admin_steps[m.from_user.id]["title"] = m.text; admin_steps[m.from_user.id]["step"] = "thumb"; await m.reply("২. ফাইলের ল্যান্ডস্কোপ ছবি (Photo) দিন:")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("thumb") & unique_msg)
async def handle_admin_photo(c, m):
    msg = await m.reply("⏳ সেভ হচ্ছে...")
    enc, mime, tg_fid, err = await extract_image_from_message(c, m)
    if err or not enc:
        return await msg.edit_text(err or "❌ ছবি সেভ করা যায়নি, আবার ছবি পাঠান!")
    admin_steps[m.from_user.id]["poster_b64"] = enc
    admin_steps[m.from_user.id]["poster_mime"] = mime
    admin_steps[m.from_user.id]["tg_thumb_id"] = tg_fid
    await msg.edit_text("ভিডিও টাইপ?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="ftype_prem")], [InlineKeyboardButton("👤 Regular", callback_data="ftype_reg")]]))

@app.on_callback_query(filters.regex(r"^ftype_") & filters.user(ADMIN_ID) & unique_cb)
async def filetype_cb(c, q): admin_steps[q.from_user.id]["is_premium"] = (q.data == "ftype_prem"); admin_steps[q.from_user.id]["step"] = "file"; await q.message.edit_text("✅ ৩. মূল ফাইল দিন:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("file") & unique_msg)
async def handle_admin_file(c, m):
    msg = await m.reply("⏳ সেভ হচ্ছে..."); sid = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    dbs = await get_extra_dbs_async(); tdb, ms = dbs[0], float('inf')
    for d in dbs:
        try:
            st = await d.command("dbstats")
            if st["dataSize"] < ms: ms = st["dataSize"]; tdb = d
        except: pass
    st_data = admin_steps[m.from_user.id]
    conf = await get_config(); live_url = get_live_web_url(conf)
    thumb_url = ""
    if st_data.get("poster_b64"):
        thumb_url = await save_poster_to_mongo(tdb, sid, st_data["poster_b64"], st_data.get("poster_mime", "image/jpeg"), live_url)
    nf = {"_id": sid, "title": st_data["title"], "category": "All", "is_premium": st_data.get("is_premium", False), "file_id": fid, "thumb_url": thumb_url, "tg_thumb_id": st_data.get("tg_thumb_id"), "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
    await tdb["files"].insert_one(nf); del admin_steps[m.from_user.id]; invalidate_home_cache(); await notify_new_post(c, nf, conf); await msg.edit_text(f"✅ ফাইল ও পোস্টার MongoDB-তে অ্যাড হয়েছে! ID: `{sid}`")

LEGACY_EXAMPLES = {
    "vidtitel": "`/vidtitel নতুন বাংলা নাটক`", "delfile": "`/delfile AbCd1234`",
    "mongo": "`/mongo mongodb+srv://user:pass@cluster.mongodb.net/`",
    "autochannel": "`/autochannel -100123456789 30 60`", "spincoin": "`/spincoin 10-50`",
    "addtask": "`/addtask Join Channel | https://t.me/mychannel | 50`", "addlink": "`/addlink https://example.com`",
    "addcnl": "`/addcnl Backup Channel https://t.me/mychannel`", "vercnl": "`/vercnl -100123456789 https://t.me/mychannel`",
    "usd": "`/usd 100 5` (100 Coins = 5 USD)", "bdt": "`/bdt 100 50` (100 Coins = 50 BDT)",
    "addcred": "`/addcred 500 = 7 d`", "addrdiem": "`/addrdiem 12345678 30 d`", "allred": "`/allred 100 10-50`"
}

@app.on_message(filters.command(["vidtitel", "deltitel", "delpromot", "delfile", "delall", "mongo", "delmongo", "mongostats", "lockall", "autochannel", "delautochannel", "spincoin", "addtask", "deltask", "addlink", "delink", "addcnl", "delcnl", "vercnl", "delvrcnl", "logo", "autvid", "usd", "bdt", "addcred", "delcred", "addrdiem", "brodcast", "cnlbdcst", "allred"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_legacy_router(c, m):
    try:
        await get_db(); parts = m.text.split(); cmd = parts[0].lstrip("/").lower().split("@")[0]; arg = m.text.replace(parts[0], "", 1).strip()
        if cmd == "vidtitel":
            if not arg: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['vidtitel']}")
            await titles_col.insert_one({"title": arg}); return await m.reply(f"✅ Added: **{arg}**")
        if cmd == "deltitel": ts = await titles_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {t['title'][:25]}", callback_data=f"delttl_{t['_id']}")] for t in ts])) if ts else await m.reply("❌ কোনো টাইটেল নেই!")
        if cmd == "delpromot": ps = await promotions_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {p['title'][:25]}", callback_data=f"delpromo_{p['_id']}")] for p in ps])) if ps else await m.reply("❌ কোনো প্রোমোট নেই!")
        if cmd == "delfile":
            if len(parts) < 2: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['delfile']}")
            for d in await get_extra_dbs_async():
                if (await d["files"].delete_one({"_id": parts[1]})).deleted_count > 0:
                    await d["posters"].delete_one({"_id": parts[1]})
                    poster_cache_del(parts[1])
                    invalidate_home_cache()
                    return await m.reply("✅ ফাইল ডিলিট হয়েছে!")
            return await m.reply("❌ ফাইল পাওয়া যায়নি!")
        if cmd == "delall":
            for d in await get_extra_dbs_async():
                await d["files"].delete_many({})
                await d["posters"].delete_many({})
            POSTER_CACHE.clear()
            invalidate_home_cache()
            return await m.reply("✅ সব ফাইল ডিলিট হয়েছে!")
        if cmd == "mongo":
            if len(parts) < 2: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['mongo']}")
            await AsyncIOMotorClient(parts[1], serverSelectionTimeoutMS=3000).server_info(); await mongos_col.insert_one({"uri": parts[1]}); EXTRA_URI_CACHE["uris"] = None; invalidate_home_cache(); return await m.reply("✅ MongoDB যুক্ত হয়েছে!")
        if cmd == "delmongo": dbs = await mongos_col.find().to_list(None); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {d['uri'][:25]}", callback_data=f"delmongo_{d['_id']}")] for d in dbs])) if dbs else await m.reply("❌ কোনো এক্সট্রা MongoDB নেই!")
        if cmd == "mongostats":
            dbs = await get_extra_dbs_async(); txt = "📊 **MongoDB Stats:**\n\n"
            for i, d in enumerate(dbs):
                try: st = await d.command("dbstats"); fc = await d["files"].count_documents({}); txt += f"{'🔹 Main' if i==0 else f'🔸 Extra {i}'}: {fc} files | {st['dataSize']/(1024*1024):.2f}/512 MB\n"
                except: txt += f"❌ DB {i} Error\n"
            return await m.reply(txt)
        if cmd == "lockall": await users_col.update_many({}, {"$set": {"unlocked_files": {}}}); return await m.reply("✅ সবার ভিডিও লক করা হয়েছে!")
        if cmd == "autochannel":
            if len(parts) < 4: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['autochannel']}")
            await auto_channels_col.update_one({"chat_id": parts[1]}, {"$set": {"chat_id": parts[1], "interval": int(parts[2]), "delete_min": int(parts[3]), "last_post_time": 0, "post_idx": 0}}, upsert=True); return await m.reply("✅ Auto Channel Added!")
        if cmd == "delautochannel": chs = await auto_channels_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delautoch_{ch['_id']}")] for ch in chs])) if chs else await m.reply("❌ কোনো অটো চ্যানেল নেই!")
        if cmd == "spincoin":
            if "-" not in arg: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['spincoin']}")
            mn, mx = map(int, parts[1].split("-")); await config_col.update_one({"_id": "settings"}, {"$set": {"spin_coin_min": mn, "spin_coin_max": mx}}); return await m.reply("✅ Spin Coin Set!")
        if cmd == "addtask":
            if "|" not in arg: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['addtask']}")
            t, l, co = [x.strip() for x in arg.split("|")]; await tasks_col.insert_one({"title": t, "link": l, "coin": int(co)}); return await m.reply("✅ Task Added!")
        if cmd == "deltask": ts = await tasks_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {t['title']}", callback_data=f"deltask_{t['_id']}")] for t in ts])) if ts else await m.reply("❌ কোনো টাস্ক নেই!")
        if cmd == "addlink":
            if len(parts) < 2: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['addlink']}")
            await links_col.insert_one({"link": parts[1]}); invalidate_home_cache(); return await m.reply("✅ Ad Link Added!")
        if cmd == "delink": ls = await links_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {l['link'][:20]}", callback_data=f"dellink_{l['_id']}")] for l in ls])) if ls else await m.reply("❌ কোনো লিংক নেই!")
        if cmd == "addcnl":
            if len(parts) < 3: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['addcnl']}")
            nm, lk = arg.rsplit(None, 1); await channels_col.update_one({"link": lk}, {"$set": {"name": nm, "link": lk, "type": "inline"}}, upsert=True); return await m.reply(f"✅ Added: {nm}")
        if cmd == "delcnl": chs = await channels_col.find({"type": "inline"}).to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {ch['name']}", callback_data=f"delch_{ch['_id']}")] for ch in chs])) if chs else await m.reply("❌ কোনো চ্যানেল নেই!")
        if cmd == "vercnl":
            if len(parts) < 3: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['vercnl']}")
            await channels_col.insert_one({"type": "must_join", "chat_id": int(parts[1]), "link": parts[2]}); return await m.reply("✅ Must Join Added!")
        if cmd == "delvrcnl": chs = await channels_col.find({"type": "must_join"}).to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delch_{ch['_id']}")] for ch in chs])) if chs else await m.reply("❌ কোনো Must Join চ্যানেল নেই!")
        if cmd == "logo":
            if m.reply_to_message and (m.reply_to_message.photo or m.reply_to_message.document):
                enc, mime, tg_fid, err = await extract_image_from_message(c, m.reply_to_message)
                logo_fid = tg_fid or (m.reply_to_message.photo.file_id if m.reply_to_message.photo else m.reply_to_message.document.file_id)
                await config_col.update_one({"_id": "settings"}, {"$set": {"start_logo": logo_fid}}, upsert=True)
                if enc:
                    await save_poster_to_mongo(db, "site_logo", enc, mime)
                invalidate_home_cache()
                return await m.reply("✅ লোগো সেট হয়েছে! (বট স্টার্ট ও মিনি অ্যাপ ওয়েলকামে দেখাবে)")
            if arg and arg.startswith("http"):
                await config_col.update_one({"_id": "settings"}, {"$set": {"start_logo": arg}}, upsert=True)
                poster_cache_del("site_logo")
                invalidate_home_cache()
                return await m.reply("✅ লোগো লিংক সেট হয়েছে!")
            return await m.reply("❌ **নিয়ম:** কোনো ছবিতে রিপ্লাই করে `/logo` লিখুন অথবা `/logo https://...` দিন।")
        if cmd == "autvid":
            if m.reply_to_message: await config_col.update_one({"_id": "settings"}, {"$set": {"autovid_msg_id": m.reply_to_message.id, "autovid_chat_id": m.chat.id}}); return await m.reply("✅ Auto Video Set!")
            return await m.reply("❌ **নিয়ম:** কোনো ভিডিওতে রিপ্লাই করে `/autvid` লিখুন।")
        if cmd == "usd":
            if len(parts) < 3: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['usd']}")
            await pkgs_col.insert_one({"type": "usd", "coins": int(parts[1]), "details": f"{parts[1]} Coins={parts[2]} USD"}); invalidate_home_cache(); return await m.reply("✅ USD Package Added!")
        if cmd == "bdt":
            if len(parts) < 3: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['bdt']}")
            await pkgs_col.insert_one({"type": "bkash", "coins": int(parts[1]), "details": f"{parts[1]} Coins={parts[2]} BDT"}); invalidate_home_cache(); return await m.reply("✅ BDT Package Added!")
        if cmd == "addcred":
            if len(parts) < 5: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['addcred']}")
            await pkgs_col.insert_one({"type": "coin", "coins": int(parts[1]), "amount": int(parts[3]), "unit": parts[4], "details": f"{parts[1]} Coins={parts[3]} {parts[4]}"}); invalidate_home_cache(); return await m.reply("✅ Coin Package Added!")
        if cmd == "delcred": ps = await pkgs_col.find().to_list(100); return await m.reply("ডিলিট:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {p['details']}", callback_data=f"delpkg_{p['_id']}")] for p in ps])) if ps else await m.reply("❌ কোনো প্যাকেজ নেই!")
        if cmd == "addrdiem":
            if len(parts) < 4: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['addrdiem']}")
            secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(parts[3].lower(), 0)
            await users_col.update_one({"_id": int(parts[1])}, {"$set": {"premium_until": datetime.now() + timedelta(seconds=int(parts[2]) * secs)}}); return await m.reply("✅ Premium Added!")
        if cmd == "allred":
            if len(parts) < 3 or "-" not in parts[2]: return await m.reply(f"❌ **উদাহরণ:** {LEGACY_EXAMPLES['allred']}")
            rng = parts[2].split("-"); code = "RND" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
            await coupons_col.insert_one({"code": code, "limit": int(parts[1]), "used_by": [], "is_random": True, "min": int(rng[0]), "max": int(rng[1])}); return await m.reply(f"✅ Code: `{code}`")
        if cmd == "brodcast":
            if not m.reply_to_message: return await m.reply("❌ **নিয়ম:** কোনো মেসেজে রিপ্লাই করে `/brodcast` লিখুন।")
            msg = await m.reply("⏳ Broadcasting..."); s = 0
            for u in await users_col.find().to_list(None):
                try: await m.reply_to_message.copy(u["_id"], reply_markup=m.reply_to_message.reply_markup); s += 1; await asyncio.sleep(0.05)
                except: pass
            return await msg.edit_text(f"✅ Sent to {s} users.")
        if cmd == "cnlbdcst":
            if not m.reply_to_message: return await m.reply("❌ **নিয়ম:** কোনো মেসেজে রিপ্লাই করে `/cnlbdcst` লিখুন।")
            msg = await m.reply("⏳ Broadcasting..."); s = 0; conf = await get_config(); chs = set()
            if conf.get("autopost_channel"): chs.add(conf["autopost_channel"])
            async for ch in auto_channels_col.find(): chs.add(int(ch["chat_id"]))
            async for ch in channels_col.find({"type": "must_join"}): chs.add(ch["chat_id"])
            for cid in chs:
                try: await m.reply_to_message.copy(cid, reply_markup=m.reply_to_message.reply_markup); s += 1; await asyncio.sleep(0.5)
                except: pass
            return await msg.edit_text(f"✅ Sent to {s} Channels!")
    except Exception as e:
        await m.reply(f"❌ Command Error: `{e}`")

@app.on_callback_query(filters.regex(r"^(delttl|delpromo|delmongo|delautoch|deltask|dellink|delch|delpkg)_") & filters.user(ADMIN_ID) & unique_cb)
async def legacy_del_cb(c, q):
    await get_db(); pfx, oid = q.data.split("_", 1)
    col_map = {"delttl": titles_col, "delpromo": promotions_col, "delmongo": mongos_col, "delautoch": auto_channels_col, "deltask": tasks_col, "dellink": links_col, "delch": channels_col, "delpkg": pkgs_col}
    await col_map[pfx].delete_one({"_id": ObjectId(oid)})
    if pfx == "delmongo": EXTRA_URI_CACHE["uris"] = None
    invalidate_home_cache()
    await q.message.edit_text("✅ ডিলিট করা হয়েছে!")

@app.on_message(filters.command(["auto", "auto2", "promot"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_auto_promo(c, m):
    cmd = m.text.split()[0].lstrip("/").lower().split("@")[0]
    if cmd == "promot":
        if not m.reply_to_message or not (m.reply_to_message.photo or m.reply_to_message.document) or "|" not in m.text:
            return await m.reply("❌ **নিয়ম:** ফটোতে রিপ্লাই করে `/promot Title | https://t.me/...` লিখুন।")
        clean_txt = re.sub(r"^/promot(?:@\w+)?\s*", "", m.text, count=1, flags=re.IGNORECASE).strip()
        t, l = [x.strip() for x in clean_txt.split("|", 1)]
        enc, mime, _, err = await extract_image_from_message(c, m.reply_to_message)
        if err or not enc: return await m.reply(err or "❌ ছবি ডাউনলোড করা যায়নি!")
        await get_db(); conf = await get_config(); live_url = get_live_web_url(conf)
        pid = "promo_" + ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        purl = await save_poster_to_mongo(db, pid, enc, mime, live_url)
        await promotions_col.insert_one({"title": t, "link": l, "image": purl}); invalidate_home_cache(); return await m.reply(f"✅ প্রোমোট ও পোস্টার MongoDB-তে অ্যাড হয়েছে: **{t}**")
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document): return await m.reply("❌ ভিডিওতে রিপ্লাই করুন।")
    fid = (m.reply_to_message.video or m.reply_to_message.document).file_id
    if cmd == "auto":
        await get_db(); ts = await titles_col.find().to_list(100)
        if not ts: return await m.reply("❌ আগে `/vidtitel` দিয়ে টাইটেল অ্যাড করুন।")
        admin_steps[m.from_user.id] = {"file_id": fid}; return await m.reply("টাইটেল সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t['title'], callback_data=f"selttl_{t['_id']}")] for t in ts]))
    title = re.sub(r"^/auto2(?:@\w+)?\s*", "", m.text, count=1, flags=re.IGNORECASE).strip()
    if not title: return await m.reply("❌ **উদাহরণ:** ভিডিওতে রিপ্লাই করে `/auto2 নতুন নাটক` লিখুন।")
    admin_steps[m.from_user.id] = {"step": "auto_wait", "title": title, "file_id": fid, "thumb_url": ""}
    await m.reply(f"✅ **{title}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="auto_prem")], [InlineKeyboardButton("👤 Regular", callback_data="auto_reg")]]))

@app.on_callback_query(filters.regex(r"^(selttl|auto)_") & filters.user(ADMIN_ID) & unique_cb)
async def legacy_auto_cb(c, q):
    sd = admin_steps.get(q.from_user.id)
    if not sd: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    if q.data.startswith("selttl_"):
        await get_db(); td = await titles_col.find_one({"_id": ObjectId(q.data.split("_")[1])})
        admin_steps[q.from_user.id] = {"step": "auto_wait", "title": td["title"], "file_id": sd["file_id"], "thumb_url": ""}
        return await q.message.edit_text(f"✅ **{td['title']}**", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="auto_prem")], [InlineKeyboardButton("👤 Regular", callback_data="auto_reg")]]))
    sid = ''.join(random.choices(string.ascii_letters + string.digits, k=8)); dbs = await get_extra_dbs_async()
    nf = {"_id": sid, "title": sd["title"], "category": "All", "is_premium": (q.data == "auto_prem"), "file_id": sd["file_id"], "thumb_url": sd["thumb_url"], "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
    await dbs[0]["files"].insert_one(nf); del admin_steps[q.from_user.id]; invalidate_home_cache(); await notify_new_post(c, nf, await get_config()); await q.message.edit_text(f"✅ আপলোড সফল! ID: `{sid}`")
    # ==========================================
# 10. FLASK WEB API & MONGODB POSTER HOSTING + LOGO + WELCOME AUDIO + HTTP 206 TUTORIAL + SHORTENER
# ==========================================
@web.route('/poster/<poster_id>')
def serve_poster_from_mongo(poster_id):
    if poster_id == "default":
        return Response(make_clean_placeholder_svg(), mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=604800"})

    if poster_id in POSTER_CACHE:
        raw, mime = POSTER_CACHE[poster_id]
        return Response(raw, mimetype=mime, headers={"Cache-Control": "public, max-age=604800"})

    for d in get_extra_dbs_sync():
        try:
            pdoc = d["posters"].find_one({"_id": poster_id})
            if pdoc and pdoc.get("data"):
                raw, mime = decode_b64_to_bytes(pdoc["data"])
                final_mime = pdoc.get("mime", mime)
                if final_mime == "image/jpg": final_mime = "image/jpeg"
                if raw:
                    poster_cache_put(poster_id, raw, final_mime)
                    return Response(raw, mimetype=final_mime, headers={"Cache-Control": "public, max-age=604800"})
        except: pass

    if poster_id.startswith("promo_"):
        try:
            pr = sync_db["promotions"].find_one({"_id": ObjectId(poster_id[6:])})
            if pr and pr.get("image"):
                if pr["image"].startswith("data:image"):
                    raw, mime = decode_b64_to_bytes(pr["image"])
                    if raw:
                        poster_cache_put(poster_id, raw, mime)
                        return Response(raw, mimetype=mime, headers={"Cache-Control": "public, max-age=604800"})
                elif pr["image"].startswith("http"):
                    return redirect(pr["image"])
        except: pass

    if poster_id.startswith("upc_"):
        try:
            up = sync_db["upcoming"].find_one({"_id": ObjectId(poster_id[4:])})
            if up and up.get("thumb_url"):
                if up["thumb_url"].startswith("data:image"):
                    raw, mime = decode_b64_to_bytes(up["thumb_url"])
                    if raw:
                        poster_cache_put(poster_id, raw, mime)
                        return Response(raw, mimetype=mime, headers={"Cache-Control": "public, max-age=604800"})
                elif up["thumb_url"].startswith("http"):
                    return redirect(up["thumb_url"])
        except: pass

    for d in get_extra_dbs_sync():
        try:
            fdoc = d["files"].find_one({"_id": poster_id})
            if fdoc and fdoc.get("thumb_url"):
                tu = fdoc["thumb_url"]
                if tu.startswith("data:image"):
                    raw, mime = decode_b64_to_bytes(tu)
                    if raw:
                        poster_cache_put(poster_id, raw, mime)
                        return Response(raw, mimetype=mime, headers={"Cache-Control": "public, max-age=604800"})
                elif tu.startswith("http") and "/poster/" not in tu:
                    return redirect(tu)
        except: pass

    return Response(make_clean_placeholder_svg(), mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=3600"})

@web.route('/site_logo')
def serve_site_logo():
    if "site_logo" in POSTER_CACHE:
        raw, mime = POSTER_CACHE["site_logo"]
        return Response(raw, mimetype=mime, headers={"Cache-Control": "public, max-age=3600"})
    try:
        pdoc = sync_db["posters"].find_one({"_id": "site_logo"})
        if pdoc and pdoc.get("data"):
            raw, mime = decode_b64_to_bytes(pdoc["data"])
            final_mime = pdoc.get("mime", mime)
            if raw:
                poster_cache_put("site_logo", raw, final_mime)
                return Response(raw, mimetype=final_mime, headers={"Cache-Control": "public, max-age=3600"})
    except: pass
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}
    logo = conf.get("start_logo")
    if logo:
        if str(logo).startswith("http"):
            return redirect(logo)
        if BOT_TOKEN:
            try:
                raw = _bot_api_download_sync(logo)
                if raw:
                    poster_cache_put("site_logo", raw, "image/jpeg")
                    try:
                        sync_db["posters"].update_one({"_id": "site_logo"}, {"$set": {"data": base64.b64encode(raw).decode("utf-8"), "mime": "image/jpeg"}}, upsert=True)
                    except: pass
                    return Response(raw, mimetype="image/jpeg", headers={"Cache-Control": "public, max-age=3600"})
            except: pass
    sn = (conf.get("site_name") or "FB").strip()
    initials = "".join([w[0].upper() for w in sn.split()[:2]]) or "FB"
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" viewBox="0 0 200 200">
  <defs><linearGradient id="lg" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#ff4d4d"/><stop offset="50%" stop-color="#7c3aed"/><stop offset="100%" stop-color="#06b6d4"/></linearGradient></defs>
  <rect width="200" height="200" rx="46" fill="url(#lg)"/>
  <text x="100" y="118" font-family="sans-serif" font-weight="800" font-size="72" fill="#fff" text-anchor="middle">{initials}</text>
</svg>"""
    return Response(svg.encode("utf-8"), mimetype="image/svg+xml", headers={"Cache-Control": "public, max-age=600"})

@web.route('/api/welcome_audio')
def serve_welcome_audio():
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}
    sn = (conf.get("site_name") or "Moviee Link").strip()
    clean_name = re.sub(r"\bbot\b", "", sn, flags=re.IGNORECASE).strip() or sn
    phrase = f"Welcome to our {clean_name} Bot"
    h = hashlib.md5(phrase.encode("utf-8")).hexdigest()[:12]
    mp3_path = f"/tmp/welcome_{h}.mp3"
    if not os.path.exists(mp3_path) or os.path.getsize(mp3_path) < 300:
        try:
            r = requests.get(
                "https://translate.google.com/translate_tts",
                params={"ie": "UTF-8", "q": phrase, "tl": "en", "client": "tw-ob"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=8
            )
            if r.status_code == 200 and len(r.content) > 300:
                with open(mp3_path, "wb") as f:
                    f.write(r.content)
        except Exception as e:
            print("Welcome TTS error:", e, flush=True)
    if os.path.exists(mp3_path) and os.path.getsize(mp3_path) > 300:
        return send_file(mp3_path, mimetype="audio/mpeg")
    return "", 204

@web.route('/tutorial_video')
def serve_tutorial_video():
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}
    vid = conf.get("tutorial_vid")
    if not vid:
        return "Tutorial video not set", 404
    if str(vid).startswith("http"):
        return redirect(vid)

    cache_path = ensure_tutorial_cached_sync(vid)
    if not cache_path or not os.path.exists(cache_path) or os.path.getsize(cache_path) == 0:
        return "Tutorial video unavailable", 404

    file_size = os.path.getsize(cache_path)
    range_header = request.headers.get("Range", None)
    if range_header:
        byte1, byte2 = 0, None
        m = re.search(r"bytes=(\d+)-(\d*)", range_header)
        if m:
            byte1 = int(m.group(1))
            if m.group(2):
                byte2 = int(m.group(2))
        if byte2 is None or byte2 >= file_size:
            byte2 = file_size - 1
        if byte1 >= file_size:
            return Response(status=416, headers={"Content-Range": f"bytes */{file_size}"})
        length = byte2 - byte1 + 1

        def generate_range():
            with open(cache_path, "rb") as f:
                f.seek(byte1)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(65536, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        rv = Response(generate_range(), 206, mimetype="video/mp4", direct_passthrough=True)
        rv.headers["Content-Range"] = f"bytes {byte1}-{byte2}/{file_size}"
        rv.headers["Accept-Ranges"] = "bytes"
        rv.headers["Content-Length"] = str(length)
        rv.headers["Cache-Control"] = "public, max-age=86400"
        return rv

    def generate_full():
        with open(cache_path, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                yield chunk

    rv = Response(generate_full(), 200, mimetype="video/mp4", direct_passthrough=True)
    rv.headers["Accept-Ranges"] = "bytes"
    rv.headers["Content-Length"] = str(file_size)
    rv.headers["Cache-Control"] = "public, max-age=86400"
    return rv

@web.route('/api/shorten', methods=['POST'])
def api_shorten():
    url = (request.json.get("url") or "").strip()
    if not url.startswith("http"): return jsonify({"status": "error", "msg": "সঠিক URL দিন (https://...)"})
    try:
        code = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
        while sync_db["short_links"].find_one({"_id": code}):
            code = ''.join(random.choices(string.ascii_letters + string.digits, k=6))
        sync_db["short_links"].insert_one({
            "_id": code,
            "url": url,
            "clicks": 0,
            "created_at": datetime.utcnow().isoformat() + "Z"
        })
        conf = sync_db["config"].find_one({"_id": "settings"}) or {}
        live_url = get_live_web_url(conf)
        return jsonify({"status": "success", "short_url": f"{live_url}/s/{code}"})
    except Exception as e:
        return jsonify({"status": "error", "msg": f"Shortener ত্রুটি: {e}"})

SHORT_VERIFY_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>{{ site_name }} — Link Verification</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Hind+Siliguri:wght@400;500;600;700&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
{% for z in zone_ids %}<script src="https://libtl.com/sdk.js" data-zone="{{ z }}" data-sdk="show_{{ z }}" defer></script>{% endfor %}
<style>
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:linear-gradient(135deg,#0f172a,#1e1b4b);color:#fff;font-family:'Hind Siliguri',system-ui,sans-serif;display:flex;align-items:center;justify-content:center;padding:18px}
.card{background:#fff;color:#0f172a;width:100%;max-width:420px;border-radius:22px;padding:24px 20px;text-align:center;box-shadow:0 20px 50px rgba(0,0,0,.35)}
.badge{display:inline-block;background:#eef2ff;color:#4f46e5;font-weight:700;font-size:12px;padding:4px 12px;border-radius:20px;margin-bottom:10px}
h2{margin:0 0 8px 0;font-size:21px}
p{margin:0 0 18px 0;font-size:13.5px;color:#64748b}
.btn{width:100%;border:none;padding:13px 16px;border-radius:14px;font-size:15px;font-weight:700;cursor:pointer;margin-bottom:10px;display:flex;align-items:center;justify-content:center;gap:8px;transition:transform .2s}
.btn:active{transform:scale(.95)}
.btn-monetag{background:linear-gradient(90deg,#6c5ce7,#4f46e5);color:#fff}
.btn-link{background:linear-gradient(90deg,#ff512f,#dd2476);color:#fff}
.btn-go{background:linear-gradient(90deg,#10b981,#059669);color:#fff;display:none;text-decoration:none}
.timer-box{display:none;background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:14px;margin-bottom:12px}
.timer-num{font-size:38px;font-weight:800;color:#e11d48;line-height:1.1}
</style>
</head>
<body>
<div class="card">
  <div class="badge">🔗 {{ site_name }} Safe Link</div>
  <h2>লিংকটি ভেরিফাই করুন</h2>
  <p>আপনার সুবিধামতো যেকোনো একটি অ্যাড ভেরিফিকেশন সম্পন্ন করে আসল লিংকে যান।</p>

  <div id="verify-options">
    {% if zone_ids %}
    <button class="btn btn-monetag" onclick="verifyMonetag()">📺 Monetag Ad দিয়ে Verify করুন</button>
    {% endif %}
    {% if ad_link %}
    <button class="btn btn-link" onclick="verifyLinkAd()">🔗 Link Ad (Adsterra) দিয়ে Verify করুন</button>
    {% endif %}
    {% if not zone_ids and not ad_link %}
    <button class="btn btn-monetag" onclick="startDirectWait()">⚡ Verify & Continue</button>
    {% endif %}
  </div>

  <div class="timer-box" id="timer-box">
    <div style="font-size:13px;color:#475569;">অনুগ্রহ করে অপেক্ষা করুন...</div>
    <div class="timer-num" id="t-num">{{ wait_time }}</div>
  </div>

  <a id="go-btn" class="btn btn-go" href="{{ target_url }}">✅ লিংক আনলক হয়েছে — এখানে ক্লিক করুন</a>
</div>
<script>
const targetUrl={{ target_url_json | safe }};
const adLink={{ ad_link_json | safe }};
const zoneIds={{ zone_ids_json | safe }};
const waitTime={{ wait_time }};

function unlockTarget(){
  document.getElementById('verify-options').style.display='none';
  document.getElementById('timer-box').style.display='none';
  const g=document.getElementById('go-btn');
  g.style.display='flex';
  g.href=targetUrl;
}
async function verifyMonetag(){
  const zid=zoneIds[0];
  if(zid && typeof window['show_'+zid]==='function'){
    try{ await window['show_'+zid](); unlockTarget(); return; }catch(e){}
  }
  startCountdown(3);
}
function verifyLinkAd(){
  if(adLink){ try{ window.open(adLink,'_blank'); }catch(e){} }
  startCountdown(waitTime||5);
}
function startDirectWait(){ startCountdown(3); }
function startCountdown(sec){
  document.getElementById('verify-options').style.display='none';
  const tb=document.getElementById('timer-box'), tn=document.getElementById('t-num');
  tb.style.display='block'; tn.innerText=sec;
  let t=sec;
  const iv=setInterval(()=>{
    t--; tn.innerText=Math.max(0,t);
    if(t<=0){ clearInterval(iv); unlockTarget(); }
  },1000);
}
</script>
</body>
</html>"""

@web.route('/s/<code>')
def short_link_gate(code):
    doc = sync_db["short_links"].find_one({"_id": code})
    if not doc:
        return "<h2 style='font-family:sans-serif;text-align:center;margin-top:50px;'>❌ লিংকটি পাওয়া যায়নি বা মেয়াদ শেষ হয়েছে!</h2>", 404
    sync_db["short_links"].update_one({"_id": code}, {"$inc": {"clicks": 1}})
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}
    links = list(sync_db["ad_links"].find())
    zones = [z["zone_id"] for z in sync_db["zone_ids"].find()] or ([DEFAULT_ZONE_ID] if DEFAULT_ZONE_ID else [])
    ad_link = random.choice(links)["link"] if links else None
    wait_time = random.choice(conf.get("direk_wait", [5]))
    return render_template_string(
        SHORT_VERIFY_HTML,
        site_name=conf.get("site_name", "Moviee Link"),
        target_url=doc["url"],
        target_url_json=json.dumps(doc["url"]),
        ad_link=ad_link,
        ad_link_json=json.dumps(ad_link),
        zone_ids=zones,
        zone_ids_json=json.dumps(zones),
        wait_time=wait_time
    )

@web.route('/api/rates')
def api_rates():
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=4).json()
        if r.get("rates"): return jsonify(r["rates"])
    except: pass
    return jsonify({"USD": 1, "BDT": 120.5, "EUR": 0.92, "GBP": 0.79, "SAR": 3.75, "AED": 3.67, "INR": 84.4, "MYR": 4.45})

@web.route('/api/get_ad/<int:user_id>/<file_id>')
def get_ad_api(user_id, file_id):
    config = sync_db["config"].find_one({"_id": "settings"}) or {}; user = sync_db["users"].find_one({"_id": user_id}) or {}; fd = None
    for d in get_extra_dbs_sync():
        fd = d["files"].find_one({"_id": file_id}, {"is_premium": 1})
        if fd: break
    if user.get("premium_until") and user["premium_until"] > datetime.now():
        return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})
    if file_id in user.get("unlocked_files", {}):
        try:
            if datetime.fromisoformat(user["unlocked_files"][file_id].replace("Z", "")) > datetime.utcnow():
                return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})
        except: pass
    zones = [z["zone_id"] for z in sync_db["zone_ids"].find()] or ([DEFAULT_ZONE_ID] if DEFAULT_ZONE_ID else [])
    links = list(sync_db["ad_links"].find())
    coin_price = config.get("premium_vid_coin", 50)
    is_prem_file = bool(fd and fd.get("is_premium"))
    ads_on = config.get("ads_on", True)
    if is_prem_file or (ads_on and (links or zones)):
        return jsonify({
            "show_ad": True,
            "is_unlocked": False,
            "is_premium_file": is_prem_file,
            "requires_coin": is_prem_file,
            "coin_price": coin_price,
            "user_balance": user.get("balance", 0),
            "ad_link": random.choice(links)["link"] if links else None,
            "zone_id": random.choice(zones) if zones else None,
            "wait_time": random.choice(config.get("direk_wait", [5]))
        })
    return jsonify({"show_ad": False, "is_unlocked": True})

@web.route('/api/unlock_file', methods=['POST'])
def unlock_file():
    d = request.json; uid, fid, m = d['uid'], d['file_id'], d['method']
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}; u = sync_db["users"].find_one({"_id": uid}) or {}
    if m == "coin":
        cost = conf.get("premium_vid_coin", 50)
        if u.get("balance", 0) < cost: return jsonify({"status": "error", "msg": "❌ পর্যাপ্ত কয়েন নেই!"})
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -cost}})
    rl = conf.get("vid_relock_min", 1440)
    exp = (datetime.utcnow() + timedelta(minutes=rl if rl > 0 else 52560000)).isoformat() + "Z"
    sync_db["users"].update_one({"_id": uid}, {"$set": {f"unlocked_files.{fid}": exp}}, upsert=True)
    return jsonify({"status": "success", "expiry": exp})

@web.route('/api/earn_info/<int:uid>')
def get_earn_info(uid):
    conf = sync_db["config"].find_one({"_id": "settings"}) or {}; u = sync_db["users"].find_one({"_id": uid}) or {}; td = datetime.now().strftime("%Y-%m-%d")
    if u.get("last_activity_date") != td:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": td, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}}, upsert=True)
        u["spin_count"], u["task_count"], u["custom_tasks_done"] = 0, 0, []
    cts = list(sync_db["custom_tasks"].find())
    for t in cts: t["_id"] = str(t["_id"])
    return jsonify({"spins_done": u.get("spin_count", 0), "spin_limit": conf.get("spin_limit", 5), "tasks_done": u.get("task_count", 0), "task_limit": conf.get("task_limit", 5), "custom_tasks": cts, "custom_tasks_done": u.get("custom_tasks_done", [])})

@web.route('/api/get_earn_ad/<ad_type>')
def get_earn_ad_dynamic(ad_type):
    links = list(sync_db["ad_links"].find()); zones = [z["zone_id"] for z in sync_db["zone_ids"].find()] or ([DEFAULT_ZONE_ID] if DEFAULT_ZONE_ID else [])
    return jsonify({"ad_link": random.choice(links)["link"] if links else None, "zone_id": random.choice(zones) if zones else None, "wait_time": 10})

@web.route('/api/claim_earn', methods=['POST'])
def claim_earn():
    d = request.json; uid, tt = d['uid'], d['type']; conf = sync_db["config"].find_one({"_id": "settings"}) or {}; u = sync_db["users"].find_one({"_id": uid}) or {}; td = datetime.now().strftime("%Y-%m-%d")
    if u.get("last_activity_date") != td:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": td, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}}, upsert=True)
        u["spin_count"], u["task_count"], u["custom_tasks_done"] = 0, 0, []
    if tt == "spin":
        if u.get("spin_count", 0) >= conf.get("spin_limit", 5): return jsonify({"status": "error", "msg": "❌ স্পিন লিমিট শেষ!"})
        c = random.randint(conf.get("spin_coin_min", 10), conf.get("spin_coin_max", 50))
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": c, "spin_count": 1}}); return jsonify({"status": "success", "msg": f"🎉 +{c} কয়েন পেয়েছেন!"})
    if tt == "task":
        if u.get("task_count", 0) >= conf.get("task_limit", 5): return jsonify({"status": "error", "msg": "❌ টাস্ক লিমিট শেষ!"})
        c = conf.get("task_coin", 20); sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": c, "task_count": 1}}); return jsonify({"status": "success", "msg": f"🎉 +{c} কয়েন পেয়েছেন!"})
    if tt.startswith("custom_"):
        tid = tt.split("_")[1]
        if tid in u.get("custom_tasks_done", []): return jsonify({"status": "error", "msg": "❌ আগেই করেছেন!"})
        t = sync_db["custom_tasks"].find_one({"_id": ObjectId(tid)})
        if t: sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": t["coin"]}, "$push": {"custom_tasks_done": tid}}); return jsonify({"status": "success", "msg": f"🎉 +{t['coin']} কয়েন পেয়েছেন!"})
    return jsonify({"status": "error", "msg": "Invalid"})

@web.route('/api/redeem', methods=['POST'])
def redeem_coupon():
    d = request.json; uid, code = d['uid'], d['code'].strip(); c = sync_db["coupons"].find_one({"code": code})
    if not c: return jsonify({"status": "error", "msg": "❌ কোডটি সঠিক নয়!"})
    if uid in c.get("used_by", []): return jsonify({"status": "error", "msg": "❌ আপনি এটি ব্যবহার করেছেন!"})
    if len(c.get("used_by", [])) >= c.get("limit", 0): return jsonify({"status": "error", "msg": "❌ কোডের মেয়াদ শেষ!"})
    sync_db["coupons"].update_one({"code": code}, {"$push": {"used_by": uid}})
    if c.get("is_premium_code"):
        secs = c.get("duration_seconds", 86400); u = sync_db["users"].find_one({"_id": uid}) or {}
        base = u.get("premium_until") if u.get("premium_until") and u["premium_until"] > datetime.now() else datetime.now()
        sync_db["users"].update_one({"_id": uid}, {"$set": {"premium_until": base + timedelta(seconds=secs)}}, upsert=True)
        lbl = c.get("duration_label", "Premium"); send_tg_api_fallback(uid, f"🎉 **{lbl}** প্রিমিয়াম চালু হয়েছে!")
        return jsonify({"status": "success", "msg": f"✅ Activated {lbl} Premium!"})
    coin = random.randint(c["min"], c["max"]) if c.get("is_random") else c.get("coins", 0)
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin}}, upsert=True); send_tg_api_fallback(uid, f"🎉 +{coin} Coins")
    return jsonify({"status": "success", "msg": f"✅ Redeemed {coin} coins!"})

@web.route('/api/buy_with_coin', methods=['POST'])
def buy_with_coin():
    d = request.json; uid, pid = d['uid'], d['pkg_id']; u = sync_db["users"].find_one({"_id": uid}); p = sync_db["packages"].find_one({"_id": ObjectId(pid)})
    if not u or not p: return jsonify({"status": "error", "msg": "Invalid"})
    if u.get("balance", 0) < p["coins"]: return jsonify({"status": "error", "msg": "❌ পর্যাপ্ত কয়েন নেই!"})
    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(p.get("unit", "d").lower()[0], 86400)
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -p["coins"]}, "$set": {"premium_until": datetime.now() + timedelta(seconds=p["amount"] * secs)}})
    send_tg_api_fallback(uid, f"🎉 {p['coins']} কয়েন দিয়ে প্রিমিয়াম কিনেছেন!"); return jsonify({"status": "success", "msg": "✅ Premium Purchased!"})

@web.route('/api/request_movie', methods=['POST'])
def request_movie():
    d = request.json; name = (d.get("movie_name") or "").strip()
    if not name: return jsonify({"status": "error", "msg": "❌ মুভির নাম লিখুন!"})
    send_tg_api_fallback(ADMIN_ID, f"📥 **New Request!**\n🎬 **{name}**\n👤 `{d.get('uid')}`"); return jsonify({"status": "success", "msg": "✅ রিকোয়েস্ট পাঠানো হয়েছে!"})

@web.route('/api/action', methods=['POST'])
def handle_actions():
    d = request.json; act, uid, fid = d.get('action'), d.get('uid'), d.get('file_id')
    if act == "history": sync_db["users"].update_one({"_id": uid}, {"$addToSet": {"history": fid}}, upsert=True)
    elif act == "clear_history": sync_db["users"].update_one({"_id": uid}, {"$set": {"history": []}}, upsert=True); return jsonify({"status": "success"})
    elif act == "favorite":
        u = sync_db["users"].find_one({"_id": uid}) or {}
        if fid in u.get("favorites", []): sync_db["users"].update_one({"_id": uid}, {"$pull": {"favorites": fid}}); return jsonify({"status": "success", "saved": False})
        sync_db["users"].update_one({"_id": uid}, {"$addToSet": {"favorites": fid}}, upsert=True); return jsonify({"status": "success", "saved": True})
    elif act == "like":
        for dbx in get_extra_dbs_sync():
            if dbx["files"].update_one({"_id": fid}, {"$addToSet": {"likes": uid}}).modified_count > 0: invalidate_home_cache(); break
    elif act == "rate":
        st = max(1, min(5, int(d.get("stars", 5))))
        for dbx in get_extra_dbs_sync():
            doc = dbx["files"].find_one({"_id": fid})
            if doc:
                rs = [r for r in doc.get("ratings", []) if r.get("uid") != uid] + [{"uid": uid, "stars": st}]
                dbx["files"].update_one({"_id": fid}, {"$set": {"ratings": rs}}); invalidate_home_cache(); return jsonify({"status": "success"})
    elif act == "comment":
        cmt = {"uid": uid, "text": d.get("text"), "time": datetime.now().strftime("%Y-%m-%d %H:%M")}
        for dbx in get_extra_dbs_sync():
            if dbx["files"].update_one({"_id": fid}, {"$push": {"comments": cmt}}).modified_count > 0: invalidate_home_cache(); break
        return jsonify({"status": "success", "comment": cmt})
    return jsonify({"status": "success"})

def build_user_state_dict(user_id):
    u = sync_db["users"].find_one({"_id": user_id}); is_p, exp = False, None
    if u and u.get("premium_until") and u["premium_until"] > datetime.now(): is_p, exp = True, get_expiry_str(u["premium_until"])
    return {"balance": u.get("balance", 0) if u else 0, "is_premium": is_p, "expiry": exp, "history": u.get("history", []) if u else [], "favorites": u.get("favorites", []) if u else [], "unlocked_files": u.get("unlocked_files", {}) if u else {}}

@web.route('/api/user/<int:user_id>')
def get_user(user_id):
    return jsonify(build_user_state_dict(user_id))
    # ==========================================
# 11. HTML MINI APP UI & SERVER RUNNER
# ==========================================
HTML_TEMPLATE = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>{{ site_name }}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Hind+Siliguri:wght@400;500;600;700&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
{% for z in zone_ids %}<script src="https://libtl.com/sdk.js" data-zone="{{ z }}" data-sdk="show_{{ z }}" defer></script>{% endfor %}
<style>
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{background:#f4f5f7;color:#111;font-family:'Hind Siliguri',system-ui,-apple-system,sans-serif;margin:0;padding-bottom:94px;min-height:100vh}
@keyframes btnShine{0%{left:-120%}100%{left:150%}}
@keyframes gradFlow{0%{background-position:0% 50%}50%{background-position:100% 50%}100%{background-position:0% 50%}}
@keyframes fadeInSlide{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
@keyframes popIn{from{opacity:0;transform:scale(.88)}to{opacity:1;transform:scale(1)}}
@keyframes pulseGlow{0%,100%{box-shadow:0 0 0 0 rgba(108,92,231,.45)}50%{box-shadow:0 0 0 8px rgba(108,92,231,0)}}
@keyframes floatLogo{0%,100%{transform:translateY(0) scale(1)}50%{transform:translateY(-5px) scale(1.04)}}
@keyframes spinRing{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
@keyframes dotBlink{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.35;transform:scale(.75)}}
@keyframes navPop{0%{transform:translateY(0) scale(1)}50%{transform:translateY(-4px) scale(1.18)}100%{transform:translateY(-1px) scale(1.06)}}
@keyframes rippleAnim{to{transform:scale(4);opacity:0}}
@keyframes gentleBob{0%,100%{transform:translateY(0)}50%{transform:translateY(-2px)}}

.anim-btn,button,.cat-pill,.lang-pill,.watch-pill,.q-btn,.s-tab,.btn-p,.unlock-opt,.p-like,.p-dl,.p-save,.tut-btn,.back-btn,.shr-c{
  transition:transform .18s cubic-bezier(.34,1.56,.64,1),box-shadow .22s ease,background .25s ease;
  position:relative;overflow:hidden;
}
button:active,.cat-pill:active,.lang-pill:active,.nav-i:active,.watch-pill:active,.q-btn:active,.s-tab:active,.btn-p:active,.unlock-opt:active,.p-like:active,.p-dl:active,.p-save:active,.tut-btn:active{
  transform:scale(.92)!important;
}
.btn-p::after,.q-btn::after,.p-dl::after,.unlock-opt::after,.watch-pill::after,.tut-btn::after,.cat-pill.active::after{
  content:"";position:absolute;top:0;left:-120%;width:55%;height:100%;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.38),transparent);
  transform:skewX(-20deg);animation:btnShine 2.6s infinite ease-in-out;pointer-events:none;
}
.ripple-fx{position:absolute;border-radius:50%;background:rgba(255,255,255,.45);transform:scale(0);animation:rippleAnim .5s linear;pointer-events:none}

.top-header{position:sticky;top:0;z-index:90;background:#fff;padding:10px 14px;display:flex;justify-content:space-between;align-items:center;box-shadow:0 2px 10px rgba(0,0,0,.06)}
.brand{display:flex;align-items:center;gap:7px;font-size:18.5px;font-weight:700}
.hdr-logo{width:30px;height:30px;border-radius:9px;object-fit:cover;box-shadow:0 2px 8px rgba(108,92,231,.25);animation:gentleBob 3s ease-in-out infinite}
.brand-badge{background:linear-gradient(90deg,#d90429,#ef233c,#ff4d6d);background-size:200% 200%;animation:gradFlow 4s ease infinite;color:#fff;font-size:12px;padding:2px 8px;border-radius:6px}
.hdr-r{display:flex;align-items:center;gap:8px}.coin-pill{background:#fff3cd;color:#856404;border:1px solid #ffeeba;padding:4px 10px;border-radius:16px;font-size:12.5px;font-weight:700;animation:pulseGlow 2.8s infinite}
.r-search{width:36px;height:36px;border-radius:50%;position:relative;display:flex;align-items:center;justify-content:center;cursor:pointer}
.r-search::before,.float-search::before{content:"";position:absolute;inset:0;border-radius:50%;background:conic-gradient(#ff4d4d,#ffb703,#00d4ff,#6c5ce7,#ff4d4d);animation:spinRing 3.5s linear infinite}
.r-search-in{position:relative;z-index:2;width:30px;height:30px;background:#fff;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:14px}
.float-search{position:fixed;bottom:82px;right:14px;z-index:95;width:46px;height:46px;border-radius:50%;display:flex;align-items:center;justify-content:center;box-shadow:0 6px 18px rgba(0,0,0,.22);cursor:pointer}
.float-search .r-search-in{width:38px;height:38px;font-size:16px}

.live-bar{background:linear-gradient(90deg,#4b39ef,#7c3aed,#ec4899);background-size:200% 200%;animation:gradFlow 6s ease infinite;color:#fff;display:flex;align-items:center;height:34px;overflow:hidden}
.live-pill{display:flex;align-items:center;gap:5px;padding:0 10px;font-size:11px;font-weight:700;background:rgba(0,0,0,.25);height:100%;white-space:nowrap}
.live-dot{width:8px;height:8px;background:#ff3b30;border-radius:50%;animation:dotBlink 1s infinite}.marquee-wrap{flex:1;overflow:hidden;white-space:nowrap}
.marquee-txt{display:inline-block;padding-left:100%;font-size:12.5px;animation:scr 18s linear infinite}@keyframes scr{0%{transform:translateX(0)}100%{transform:translateX(-100%)}}
.search-box-wrap{display:none;padding:10px 14px;background:#fff;border-bottom:1px solid #eee}.search-box-wrap.open{display:block;animation:fadeInSlide .22s ease}
.s-input{width:100%;padding:10px 14px;border-radius:12px;border:1.5px solid #e2e8f0;background:#f8fafc;font-size:14px;outline:none;color:#111}

.cat-scroll{display:flex;flex-wrap:nowrap;gap:8px;overflow-x:auto;overflow-y:hidden;-webkit-overflow-scrolling:touch;touch-action:pan-x;padding:10px 14px 5px 14px;scrollbar-width:none}
.cat-scroll::-webkit-scrollbar{display:none}
.cat-pill{flex:0 0 auto;width:max-content;background:#fff;color:#333;padding:7px 15px;border-radius:20px;font-size:13px;font-weight:600;white-space:nowrap;cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,.06);border:1px solid #ececec}
.cat-pill.active{background:linear-gradient(90deg,#ff512f,#dd2476,#ff512f);background-size:200% 200%;animation:gradFlow 3s ease infinite;color:#fff;border-color:transparent;box-shadow:0 4px 12px rgba(221,36,118,.3)}
.cat-pill.adult{background:#5e0b2b;color:#fff;border-color:transparent}
.lang-pill{flex:0 0 auto;width:max-content;background:#eef2ff;color:#4338ca;padding:5px 13px;border-radius:14px;font-size:12px;font-weight:600;white-space:nowrap;cursor:pointer}
.lang-pill.active{background:linear-gradient(90deg,#4338ca,#6366f1);color:#fff;box-shadow:0 3px 8px rgba(67,56,202,.25)}

.page{display:none}.page.active{display:block;animation:fadeInSlide .26s ease}
.sec-head{padding:10px 14px 6px 14px;font-size:15.5px;font-weight:700;display:flex;justify-content:space-between;align-items:center}
.pin-scroll{display:flex;flex-wrap:nowrap;gap:10px;overflow-x:auto;-webkit-overflow-scrolling:touch;touch-action:pan-x;padding:0 14px 10px 14px;scrollbar-width:none}.pin-scroll::-webkit-scrollbar{display:none}
.pin-card{flex:0 0 auto;min-width:168px;max-width:168px;cursor:pointer;transition:transform .2s ease;animation:fadeInSlide .3s ease}.pin-card:active{transform:scale(.94)}
.pin-img-w{position:relative;width:100%;aspect-ratio:16/9;border-radius:12px;overflow:hidden;box-shadow:0 4px 12px rgba(0,0,0,.1);background:#141624}.pin-img-w img{width:100%;height:100%;object-fit:cover;display:block}
.pin-tag{position:absolute;top:6px;left:6px;background:linear-gradient(90deg,#ff512f,#dd2476);color:#fff;font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;box-shadow:0 2px 6px rgba(0,0,0,.3)}
.pin-views{position:absolute;bottom:6px;right:6px;background:rgba(0,0,0,.72);color:#fff;font-size:10px;padding:2px 6px;border-radius:8px}
.pin-t{font-size:12.5px;font-weight:600;margin-top:5px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}

.feed-wrap{padding:4px 14px}.feed-card{background:#fff;border-radius:16px;margin-bottom:15px;overflow:hidden;border:1.5px solid #7c6cf6;box-shadow:0 4px 14px rgba(108,92,231,.1);cursor:pointer;transition:transform .22s ease,box-shadow .22s ease;animation:fadeInSlide .3s ease}
.feed-card:active{transform:scale(.96)}
.feed-img-w{position:relative;width:100%;aspect-ratio:16/9;background:#141624}.feed-img-w img{width:100%;height:100%;object-fit:cover;display:block}
.b-new{position:absolute;top:10px;left:10px;background:linear-gradient(90deg,#2979ff,#00b0ff);color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:8px}.b-type{position:absolute;top:10px;right:10px;background:rgba(0,0,0,.75);color:#fff;font-size:10.5px;padding:3px 8px;border-radius:8px}
.feed-body{padding:10px 12px 12px 12px}.feed-title{font-size:15px;font-weight:700;margin-bottom:3px}.feed-meta{font-size:12px;color:#777;display:flex;justify-content:space-between}
.car-wrap{display:flex;flex-wrap:nowrap;gap:12px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding:4px 0 12px 0;scrollbar-width:none}.car-wrap::-webkit-scrollbar{display:none}
.p-slide{flex:0 0 auto;min-width:78%;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,.08);border:1px solid #eee}.p-slide img{width:100%;aspect-ratio:16/9;object-fit:cover;display:block}.p-slide-b{padding:10px 12px}
.watch-pill{background:linear-gradient(90deg,#e63946,#d90429);color:#fff;border:none;padding:7px 17px;border-radius:20px;font-size:12.5px;font-weight:700;cursor:pointer}

.det-hdr{background:#fff;padding:12px 14px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #eee;position:sticky;top:0;z-index:95}.back-btn{width:34px;height:34px;border-radius:50%;background:#f1f3f5;border:none;font-size:18px;cursor:pointer}
.det-img-w{position:relative;width:100%;aspect-ratio:16/9;max-height:260px;background:#141624}.det-img-w img{width:100%;height:100%;object-fit:cover;display:block}.hd-b{position:absolute;top:12px;left:12px;background:#ffb703;color:#000;font-weight:800;font-size:11px;padding:3px 9px;border-radius:6px}
.det-body{background:#fff;padding:14px 16px}.det-title{font-size:19px;font-weight:700;margin:0 0 8px 0}.d-tags{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px}.d-tag{background:#f1f5f9;color:#475569;padding:3px 10px;border-radius:12px;font-size:11.5px;font-weight:600}
.stats-r{display:flex;justify-content:space-around;align-items:center;padding:10px 0;border-top:1px solid #f1f5f9;border-bottom:1px solid #f1f5f9;margin-bottom:12px}.stat-i{text-align:center;font-size:12px;color:#64748b}.stat-i b{display:block;font-size:15px;color:#111}
.shr-c{width:40px;height:40px;border-radius:50%;background:#eef2ff;color:#4f46e5;border:1px solid #c7d2fe;font-size:16px;cursor:pointer;animation:pulseGlow 2.8s infinite}.tip-ban{background:#fff8eb;border:1px solid #fde68a;color:#92400e;padding:9px 12px;border-radius:10px;font-size:13px;font-weight:600;text-align:center;margin-bottom:10px}
.tut-btn{display:block;margin:0 auto 12px auto;background:linear-gradient(90deg,#1d4ed8,#4f46e5,#1d4ed8);background-size:200% 200%;animation:gradFlow 3s ease infinite;color:#fff;border:none;padding:9px 20px;border-radius:22px;font-size:13px;font-weight:700;cursor:pointer;box-shadow:0 4px 14px rgba(29,78,216,.3)}
.tut-box{display:none;margin:0 auto 14px auto;background:#000;border-radius:14px;overflow:hidden;box-shadow:0 6px 20px rgba(0,0,0,.22);position:relative}
.tut-box video{width:100%;max-height:250px;display:block;background:#000}
.tut-status{color:#e2e8f0;font-size:12px;text-align:center;padding:6px;background:rgba(15,23,42,.85)}

.act-r{display:flex;gap:10px;margin-bottom:14px}
.p-like{flex:1;background:#fff;border:1.5px solid #fda4af;color:#e11d48;padding:10px;border-radius:25px;font-weight:700;cursor:pointer}.p-dl{flex:1.2;background:linear-gradient(90deg,#0f172a,#334155,#0f172a);background-size:200% 200%;animation:gradFlow 4s ease infinite;border:none;color:#fff;padding:10px;border-radius:25px;font-weight:700;cursor:pointer;box-shadow:0 4px 12px rgba(15,23,42,.25)}.p-save{flex:1;background:#fff;border:1.5px solid #fed7aa;color:#d97706;padding:10px;border-radius:25px;font-weight:700;cursor:pointer}
.sel-box{background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:12px;margin-bottom:14px}.q-grid{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}.q-btn{background:linear-gradient(90deg,#4f46e5,#7c3aed,#6366f1);background-size:200% 200%;animation:gradFlow 3.5s ease infinite;color:#fff;border:none;padding:9px 14px;border-radius:11px;font-size:13px;font-weight:700;cursor:pointer;box-shadow:0 4px 12px rgba(79,70,229,.28)}
.s-tab{flex:0 0 auto;width:max-content;background:#fff;border:1px solid #cbd5e1;color:#334155;padding:7px 14px;border-radius:10px;font-size:12.5px;font-weight:700;cursor:pointer;white-space:nowrap}.s-tab.active{background:linear-gradient(90deg,#111827,#334155);color:#fff;border-color:#111;box-shadow:0 3px 10px rgba(0,0,0,.2)}.ep-card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:10px;margin-top:8px;animation:fadeInSlide .22s ease}
.rate-c{background:#fffbeb;border:1px solid #fde68a;border-radius:14px;padding:10px 14px;display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;font-size:13px;font-weight:600}.star-b{font-size:20px;color:#cbd5e1;background:none;border:none;cursor:pointer;padding:0 2px}.star-b.lit{color:#f59e0b}
.cmt-sec{background:#fff;margin-top:10px;padding:14px 16px 80px 16px}.cmt-bar{position:fixed;bottom:0;left:0;width:100%;background:#fff;padding:10px 14px;border-top:1px solid #eee;display:none;gap:10px;z-index:120}.cmt-bar input{flex:1;background:#f1f5f9;border:none;border-radius:22px;padding:10px 16px;outline:none;font-size:14px}.cmt-bar button{width:40px;height:40px;border-radius:50%;background:#111;color:#fff;border:none;font-size:16px;cursor:pointer}
.card-box{background:#fff;border-radius:16px;padding:16px;margin:12px 14px;box-shadow:0 2px 10px rgba(0,0,0,.05);border:1px solid #eee;animation:fadeInSlide .25s ease}.btn-p{width:100%;background:linear-gradient(90deg,#6c5ce7,#4f46e5,#7c3aed);background-size:200% 200%;animation:gradFlow 3.5s ease infinite;color:#fff;border:none;padding:11px;border-radius:12px;font-weight:700;font-size:14px;cursor:pointer;box-shadow:0 4px 12px rgba(79,70,229,.25)}

.tools-hero{text-align:center;padding:12px 14px 4px 14px}.tools-stats{display:flex;gap:10px;padding:10px 14px}.t-stat{flex:1;background:#fff;border-radius:16px;padding:12px 8px;text-align:center;box-shadow:0 3px 10px rgba(0,0,0,.05)}.t-stat b{font-size:20px;display:block}
.t-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:4px 14px 10px 14px}.t-card{background:#fff;border-radius:18px;padding:14px;position:relative;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,.06);cursor:pointer;border:1px solid #f1f5f9;transition:transform .2s ease;animation:fadeInSlide .25s ease}
.t-card:active{transform:scale(.94)}
.t-blob{position:absolute;top:-16px;right:-16px;width:62px;height:62px;border-radius:50%;opacity:.55}.t-new{position:absolute;top:8px;right:8px;background:#10b981;color:#fff;font-size:9.5px;font-weight:700;padding:2px 7px;border-radius:10px;z-index:2}
.t-ico{width:42px;height:42px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:21px;margin-bottom:10px}.t-name{font-size:13.5px;font-weight:700;color:#0f172a;margin-bottom:3px}.t-desc{font-size:11px;color:#64748b;line-height:1.35}
.tool-lbl{display:block;text-align:left;font-size:12px;font-weight:600;color:#475569;margin:6px 0 3px 2px}.tool-res{background:#f1f5f9;border-radius:12px;padding:12px;margin-top:10px;font-size:13.5px;text-align:left;word-break:break-word}

.bottom-nav{position:fixed;bottom:8px;left:6px;right:6px;background:rgba(255,255,255,.97);backdrop-filter:blur(12px);border-radius:24px;display:grid;grid-template-columns:repeat(7,1fr);align-items:center;padding:6px 2px;box-shadow:0 6px 24px rgba(0,0,0,.14);z-index:100;border:1px solid rgba(255,255,255,.95)}
.nav-i{display:flex;flex-direction:column;align-items:center;justify-content:center;font-size:9.5px;color:#64748b;cursor:pointer;font-weight:600;padding:3px 1px;letter-spacing:-0.2px;white-space:nowrap;transition:transform .2s ease,color .2s ease}
.nav-i.active{color:#d90429;font-weight:700;animation:navPop .35s cubic-bezier(.34,1.56,.64,1)}
.nav-i.active span{animation:gentleBob 1.8s ease-in-out infinite}
.nav-i span{font-size:17px;margin-bottom:1px;line-height:1.1}
.av-badge{width:20px;height:20px;border-radius:50%;background:linear-gradient(135deg,#f43f5e,#8b5cf6);color:#fff;font-size:9px;display:flex;align-items:center;justify-content:center;font-weight:700;margin-bottom:2px}
.modal-ov{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.65);z-index:9999;justify-content:center;align-items:center;backdrop-filter:blur(4px);padding:14px}
.modal-card{background:#fff;color:#111;border-radius:22px;padding:22px 18px;width:100%;max-width:390px;text-align:center;box-shadow:0 15px 40px rgba(0,0,0,.25);max-height:88vh;overflow-y:auto;position:relative;animation:popIn .26s cubic-bezier(.34,1.56,.64,1)}
.age-card{background:#1c1d2b;color:#fff;border-radius:22px;padding:24px 20px;width:100%;max-width:370px;text-align:center;border:1px solid rgba(255,255,255,.1);animation:popIn .24s ease}.age-warn{background:rgba(239,68,68,.12);border:1px solid #ef4444;color:#fca5a5;padding:12px;border-radius:12px;font-size:13px;margin:14px 0}
.wel-logo-ring{width:88px;height:88px;margin:0 auto 12px auto;border-radius:24px;padding:3px;background:linear-gradient(135deg,#ff4d4d,#7c3aed,#06b6d4);box-shadow:0 8px 24px rgba(124,58,237,.35);animation:floatLogo 2.5s ease-in-out infinite}
.wel-logo-ring img{width:100%;height:100%;border-radius:21px;object-fit:cover;display:block;background:#141624}
.wel-voice-btn{background:#eef2ff;color:#4f46e5;border:1px solid #c7d2fe;border-radius:16px;padding:6px 14px;font-size:12px;font-weight:700;cursor:pointer;margin-bottom:10px;display:inline-flex;align-items:center;gap:5px}
.wel-item{text-align:left;font-size:13.5px;line-height:1.55;margin-bottom:8px;color:#334155}
.unlock-opt{width:100%;border:none;border-radius:14px;padding:13px 14px;font-size:14px;font-weight:700;cursor:pointer;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;color:#fff;box-shadow:0 4px 12px rgba(0,0,0,.15)}
.clr-hist-btn{background:#fee2e2;color:#dc2626;border:none;padding:5px 12px;border-radius:12px;font-size:11.5px;font-weight:700;cursor:pointer}
</style>
</head>
<body>

<audio id="welcome-audio" preload="auto" playsinline style="display:none;"></audio>

<div id="main-chrome">
  <div class="top-header"><div class="brand"><img class="hdr-logo" src="/site_logo?v={{ logo_ver }}" alt="Logo"><span id="brand-title"></span></div><div class="hdr-r"><div class="coin-pill">🪙 <span id="hdr-bal">0</span></div><div class="r-search" onclick="toggleSearch()"><div class="r-search-in">🔍</div></div></div></div>
  <div class="live-bar"><div class="live-pill"><span class="live-dot"></span> LIVE</div><div class="marquee-wrap"><div class="marquee-txt">{{ marquee_text }}</div></div></div>
  <div class="search-box-wrap" id="search-drawer"><input type="text" id="search-inp" class="s-input" placeholder="🔍 মুভি বা সিরিজ খুঁজুন..." onkeyup="feedLimit=15;renderFeed()"></div>
</div>

<div id="welcome-modal" class="modal-ov" onclick="ensureWelcomeVoice()"><div class="modal-card">
  <div class="wel-logo-ring"><img id="wel-logo-img" src="/site_logo?v={{ logo_ver }}" alt="{{ site_name }}"></div>
  <h2 style="margin:0 0 8px 0;font-size:20px;">👋 Welcome to<br>{{ site_name }} Bot!</h2>
  <button class="wel-voice-btn" onclick="event.stopPropagation();playWelcomeVoice(true)">🔊 Play Welcome Voice</button>
  <div class="wel-item">🎬 এখানে আপনি হাজারো Movie, Web Series, Drama ও Anime দেখতে পারবেন।</div>
  <div class="wel-item">🔍 আপনার পছন্দের Movie খুঁজতে Search ব্যবহার করুন।</div>
  <div class="wel-item">🎵 ভিডিওতে সাউন্ড সমস্যা হলে VLC Player অথবা MX Player দিয়ে প্লে করুন।</div>
  <div class="wel-item">📥 কোনো Movie খুঁজে না পেলে <b>Profile</b> ট্যাব থেকে Movie Request পাঠান।</div>
  <div class="wel-item">🍿 Happy Watching! ❤️</div>
  <button class="btn-p" style="margin-top:8px;" onclick="event.stopPropagation();closeWelcome()">OK ✅</button>
</div></div>

<div id="age-modal" class="modal-ov"><div class="age-card">
  <div style="font-size:52px;margin-bottom:8px;">🔞</div><h2 style="margin:0 0 10px 0;">বয়স নিশ্চিতকরণ</h2>
  <p style="font-size:13.5px;color:#cbd5e1;margin:0;">এই সেকশনের কনটেন্ট শুধুমাত্র <b style="color:#00d4ff;">১৮ বছর বা তার বেশি বয়সী</b> ব্যবহারকারীদের জন্য প্রযোজ্য।</p>
  <div class="age-warn">⚠️ আপনার বয়স ১৮+ না হলে এই ক্যাটাগরি ব্যবহার করবেন না।</div>
  <button class="btn-p" style="background:linear-gradient(90deg,#00e5a3,#00d4ff);color:#000;margin-bottom:10px;" onclick="confirm18()">✅ হ্যাঁ, আমার বয়স ১৮+ বছর</button>
  <button class="btn-p" style="background:transparent;border:1px solid #475569;color:#94a3b8;" onclick="deny18()">❌ না, বের হয়ে যান</button>
</div></div>

<div id="unlock-modal" class="modal-ov"><div class="modal-card">
  <button onclick="document.getElementById('unlock-modal').style.display='none'" style="position:absolute;top:12px;right:14px;background:#f1f5f9;border:none;width:30px;height:30px;border-radius:50%;font-size:16px;cursor:pointer;">✕</button>
  <div style="font-size:38px;margin-bottom:4px;">🔓</div>
  <h3 style="margin:0 0 6px 0;color:#0f172a;">ফাইল আনলক করুন</h3>
  <p style="font-size:13px;color:#64748b;margin:0 0 14px 0;">আপনার যেটি সহজ মনে হয়, সেটি দিয়ে আনলক করে ডাউনলোড করুন:</p>
  <div id="unlock-options-area"></div>
</div></div>

<div id="ad-modal" class="modal-ov"><div class="modal-card"><h3 style="margin-top:0;color:#4f46e5;">🚀 Ad Verification</h3><p style="font-size:13px;color:#64748b;">সম্পূর্ণ অ্যাড দেখুন</p><h1 id="ad-timer" style="font-size:46px;color:#e11d48;margin:10px 0;">5</h1><button id="ad-action-btn" class="btn-p" style="display:none;">✅ Continue</button></div></div>

<div id="tool-modal" class="modal-ov"><div class="modal-card"><button onclick="document.getElementById('tool-modal').style.display='none'" style="position:absolute;top:12px;right:14px;background:#f1f5f9;border:none;width:30px;height:30px;border-radius:50%;font-size:16px;cursor:pointer;">✕</button><h3 id="tool-m-title" style="margin:0 0 10px 0;"></h3><div id="tool-m-body"></div></div></div>

<div id="page-home" class="page active">
  <div class="cat-scroll" id="cat-pills"></div><div class="cat-scroll" id="lang-pills" style="padding-top:2px;"></div>
  <div id="pinned-section"><div class="sec-head" id="home-pinned-head">🔥 Top Trending</div><div class="pin-scroll" id="pinned-list"></div></div>
  <div class="feed-wrap" id="feed-list"></div>
</div>

<div id="page-details" class="page">
  <div class="det-hdr"><button class="back-btn" onclick="closeDetails()">❮</button><b style="font-size:16px;">Video Details</b></div>
  <div class="det-img-w"><img id="det-poster" src=""><div class="hd-b">HD</div></div>
  <div class="det-body">
    <h2 class="det-title" id="det-title"></h2><div class="d-tags" id="det-tags"></div>
    <div class="stats-r"><div class="stat-i">❤️<b id="det-likes">0</b>Like</div><div class="stat-i">⬇️<b id="det-views">0</b>Download</div><div class="stat-i">💬<b id="det-cmts-cnt">0</b>Comments</div><button class="shr-c" onclick="shareCurrent()">🔗</button></div>
    <div class="tip-ban">💡 Tap the button below to download easily</div>
    <button class="tut-btn" id="tut-toggle-btn" onclick="toggleInlineTutorial()">❓ Need help? Watch the tutorial</button>
    <div class="tut-box" id="inline-tut-box">
      <div class="tut-status" id="tut-status">⏳ টিউটোরিয়াল ভিডিও লোড হচ্ছে...</div>
      <video id="det-tut-vid" controls playsinline webkit-playsinline preload="auto"><source id="det-tut-src" type="video/mp4"></video>
    </div>
    <div class="act-r"><button class="p-like" id="btn-like" onclick="likeCurrent()">🤍 Like</button><button class="p-dl" onclick="scrollToDownload()">⬇️ Download</button><button class="p-save" id="btn-save" onclick="saveCurrent()">🔖 Save</button></div>
    <div id="det-selector-area"></div>
    <div class="rate-c"><span>Rate it: <span id="star-row"></span></span><span id="rate-avg">⭐ 0.0 (0)</span></div>
  </div>
  <div class="cmt-sec"><h4 style="margin:0 0 10px 0;">Comments (<span id="cmt-cnt-h">0</span>)</h4><div id="det-cmt-list"></div></div>
</div>
<div class="cmt-bar" id="cmt-bar"><input type="text" id="cmt-inp" placeholder="Add a public comment..."><button onclick="sendComment()">➤</button></div>

<div id="page-favorite" class="page">
  <div class="sec-head">🔖 Saved Favorites</div>
  <div class="feed-wrap" id="fav-list"></div>
  <div class="sec-head"><span>🕒 Watch History</span><button class="clr-hist-btn" onclick="clearHistory()">🗑 Clear</button></div>
  <div class="feed-wrap" id="hist-list"></div>
</div>

<div id="page-tools" class="page">
  <div class="tools-hero"><div style="font-size:20px;font-weight:700;color:#6366f1;">🛠️ Smart Tools</div><div style="font-size:12px;color:#64748b;margin-bottom:10px;">All the essential tools in one place ✨</div><input type="text" id="tool-search" class="s-input" placeholder="🔍 Search tools (যেমন: Age, BMI, QR)..." onkeyup="renderTools()"></div>
  <div class="tools-stats"><div class="t-stat"><b style="color:#8b5cf6;">20</b><small>Total Tools</small></div><div class="t-stat"><b style="color:#0ea5e9;">7</b><small>Newly added</small></div><div class="t-stat"><b style="color:#10b981;">100%</b><small>Free to use</small></div></div>
  <div id="tools-container"></div>
</div>

<div id="page-upcoming" class="page"><div class="sec-head">📅 Upcoming Upload Schedule</div><div class="feed-wrap" id="upcoming-list"></div><div class="sec-head">⭐ Featured Promotions</div><div class="feed-wrap" id="promo-full-list"></div></div>

<div id="page-profile" class="page">
  <div class="card-box" style="text-align:center;">
    <img id="u-pic" src="/poster/default" style="width:76px;height:76px;border-radius:50%;object-fit:cover;border:3px solid #6c5ce7;">
    <h3 id="u-name" style="margin:8px 0 2px 0;">User</h3><div style="font-size:12px;color:#64748b;">ID: <span id="u-id"></span></div>
    <div id="u-prem-status" style="display:inline-block;margin-top:8px;padding:4px 12px;border-radius:14px;font-size:12px;font-weight:700;background:#f1f5f9;">👤 Regular Member</div>
    <div id="u-prem-exp" style="font-size:11.5px;color:#d97706;margin-top:4px;"></div><h2 style="margin:12px 0 0 0;color:#4f46e5;">🪙 <span id="u-bal">0</span> Coins</h2>
  </div>
  <div class="card-box"><h4 style="margin:0 0 8px 0;">🎁 Refer & Earn ({{ ref_coin }} Coins)</h4><div style="display:flex;gap:8px;"><input type="text" id="ref-inp" class="s-input" readonly><button class="btn-p" style="width:auto;padding:0 16px;" onclick="copyRef()">Copy</button></div></div>
  <div class="card-box"><h3 style="margin:0 0 8px 0;">📥 Movie / Series Request</h3><div style="display:flex;gap:8px;"><input type="text" id="req-inp" class="s-input" placeholder="যেমন: Pushpa 2 (2024)"><button class="btn-p" style="width:auto;padding:0 16px;" onclick="sendMovieRequest()">Send</button></div></div>
  <div class="card-box"><h3 style="margin:0 0 8px 0;">🎟 Redeem Code</h3><div style="display:flex;gap:8px;"><input type="text" id="coupon-inp" class="s-input" placeholder="যেমন: VIP10 বা RND123"><button class="btn-p" style="width:auto;padding:0 16px;" onclick="redeemCoupon()">Redeem</button></div></div>
  <div id="earn-area"></div>
  <div class="card-box"><h3 style="margin-top:0;">👑 Buy VIP / Coins</h3>
    {% for pkg in pkgs %}
    <div style="padding:10px;border:1px solid #eee;border-radius:12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;">
      <div><b>{{ pkg.details }}</b><br><small style="color:#64748b;">{{ pkg.type|upper }}</small></div>
      {% if pkg.type == 'coin' %}<button class="watch-pill" style="background:#6c5ce7;" onclick="buyWithCoin('{{ pkg._id }}', {{ pkg.coins }})">Exchange</button>
      {% else %}<button class="watch-pill" onclick="openTgLink('https://t.me/{{ admin_username }}')">Buy Now</button>{% endif %}
    </div>
    {% endfor %}
  </div>
</div>

<div class="float-search" id="float-search-btn" onclick="toggleSearch()"><div class="r-search-in">🔍</div></div>

<div class="bottom-nav" id="bottom-nav">
  <div class="nav-i active" data-tab="home" onclick="switchTab('home', this, 'all')"><span>🏠</span>Home</div>
  <div class="nav-i" data-tab="movie" onclick="switchTab('home', this, 'movie')"><span>🎬</span>Movie</div>
  <div class="nav-i" data-tab="series" onclick="switchTab('home', this, 'series')"><span>📺</span>Series</div>
  <div class="nav-i" data-tab="favorite" onclick="switchTab('favorite', this)"><span>🔖</span>Fav</div>
  <div class="nav-i" data-tab="tools" onclick="switchTab('tools', this)"><span>🛠️</span>Tools</div>
  <div class="nav-i" data-tab="upcoming" onclick="switchTab('upcoming', this)"><span>📅</span>Upcoming</div>
  <div class="nav-i" data-tab="profile" onclick="switchTab('profile', this)"><div class="av-badge" id="nav-av">U</div>Profile</div>
</div>

<script>
const tg=window.Telegram.WebApp; tg.expand();
const botUsername="{{ bot_username }}", siteName="{{ site_name }}", recGroupLink={{ rec_group_link_json | safe }};
const hasTutorial={{ has_tutorial_json | safe }}, tutorialVer={{ tutorial_ver_json | safe }}, siteNameHash={{ site_name_hash_json | safe }};
const allFiles={{ files_json | safe }}, categories={{ categories_json | safe }}, dbLanguages={{ languages_json | safe }};
const promotions={{ promotions_json | safe }}, upcomingList={{ upcoming_json | safe }}, zoneIds={{ zone_ids_json | safe }};
const adConfig={{ ad_config_json | safe }};
const initialUserState={{ initial_user_json | safe }};

(function(){const p=siteName.trim().split(/\s+/),e=document.getElementById('brand-title');e.innerHTML=p.length>1?`${p[0]} <span class="brand-badge">${p.slice(1).join(' ')}</span>`:`${siteName} <span class="brand-badge">Bot</span>`;})();

document.addEventListener('click',function(e){
  const btn=e.target.closest('button,.cat-pill,.lang-pill,.watch-pill,.q-btn,.s-tab,.unlock-opt');
  if(!btn)return;
  const r=document.createElement('span'); r.className='ripple-fx';
  const rect=btn.getBoundingClientRect(), sz=Math.max(rect.width,rect.height);
  r.style.width=r.style.height=sz+'px';
  r.style.left=(e.clientX-rect.left-sz/2)+'px'; r.style.top=(e.clientY-rect.top-sz/2)+'px';
  btn.appendChild(r); setTimeout(()=>r.remove(),520);
});

let userId=123456789; const urlP=new URLSearchParams(window.location.search);
if(tg?.initDataUnsafe?.user){const u=tg.initDataUnsafe.user;userId=u.id;localStorage.setItem("tg_user_id",userId);const fn=(u.first_name+" "+(u.last_name||"")).trim();document.getElementById('u-name').innerText=fn;document.getElementById('nav-av').innerText=fn.slice(0,2).toUpperCase();if(u.photo_url)document.getElementById('u-pic').src=u.photo_url;}
else if(urlP.get('uid')){userId=parseInt(urlP.get('uid'));localStorage.setItem("tg_user_id",userId);}
else if(localStorage.getItem("tg_user_id"))userId=parseInt(localStorage.getItem("tg_user_id"));
document.getElementById('u-id').innerText=userId; document.getElementById('ref-inp').value=`https://t.me/${botUsername}?start=${userId}`;

let welcomeVoicePlayed=false;
function getWelcomePhrase(){
  let cn=siteName.replace(/\bbot\b/gi,'').trim()||siteName;
  return `Welcome to our ${cn} Bot`;
}
function playWelcomeVoice(forceReplay){
  if(welcomeVoicePlayed&&!forceReplay)return;
  welcomeVoicePlayed=true;
  const aud=document.getElementById('welcome-audio');
  if(!aud.getAttribute('src'))aud.src=`/api/welcome_audio?v=${siteNameHash}`;
  aud.currentTime=0;
  aud.play().catch(()=>{
    welcomeVoicePlayed=false;
    try{
      if('speechSynthesis' in window){
        window.speechSynthesis.cancel();
        const u=new SpeechSynthesisUtterance(getWelcomePhrase());
        u.lang='en-US'; u.rate=0.96;
        window.speechSynthesis.speak(u);
        welcomeVoicePlayed=true;
      }
    }catch(e){}
  });
}
function ensureWelcomeVoice(){
  if(!welcomeVoicePlayed)playWelcomeVoice(false);
}
if(!sessionStorage.getItem('wel_seen')){
  document.getElementById('welcome-modal').style.display='flex';
  setTimeout(()=>playWelcomeVoice(false),250);
  document.addEventListener('pointerdown',()=>ensureWelcomeVoice(),{once:true});
}
function closeWelcome(){
  ensureWelcomeVoice();
  sessionStorage.setItem('wel_seen','1');
  document.getElementById('welcome-modal').style.display='none';
}

let userState=initialUserState||{balance:0,is_premium:false,expiry:null,history:[],favorites:[],unlocked_files:{}};
let activeCat='All',activeLang='All',activeType='all',pendingCat=null,curFile=null,curSeasonIdx=0,feedLimit=15,lastFilteredLen=0;
function openTgLink(u){
  if(!u)return;
  try{
    if(tg&&tg.openTelegramLink&&/^https?:\/\/(t\.me|telegram\.me)\//i.test(u)){tg.openTelegramLink(u);return;}
    if(tg&&tg.openLink){tg.openLink(u);return;}
  }catch(e){}
  window.open(u,'_blank')||(window.location.href=u);
}
function applyUserStateUI(){
  document.getElementById('hdr-bal').innerText=userState.balance||0;
  document.getElementById('u-bal').innerText=userState.balance||0;
  if(userState.is_premium){
    document.getElementById('u-prem-status').innerHTML='💎 Premium Member';
    document.getElementById('u-prem-status').style.background='#ede9fe';
    document.getElementById('u-prem-status').style.color='#5b21b6';
    document.getElementById('u-prem-exp').innerText='Expiry: '+(userState.expiry||'');
  }
  renderFavs();
}
async function loadUser(){try{const r=await fetch('/api/user/'+userId);userState=await r.json();applyUserStateUI();}catch(e){}}
applyUserStateUI();
loadUser();

function toggleSearch(){const d=document.getElementById('search-drawer');d.classList.toggle('open');if(d.classList.contains('open'))document.getElementById('search-inp').focus();}

function renderFilters(){
  let cH=`<div class="cat-pill ${activeCat==='All'?'active':''}" onclick="setCat('All')">All</div>`;
  categories.forEach(c=>{const i18=c.includes('18');cH+=`<div class="cat-pill ${activeCat===c?'active':(i18?'adult':'')}" onclick="setCat('${c.replace(/'/g,"\\'")}')">${i18?'🔞 ':''}${c}</div>`;});
  document.getElementById('cat-pills').innerHTML=cH;
  if(dbLanguages.length>0){
    document.getElementById('lang-pills').style.display='flex';
    document.getElementById('lang-pills').innerHTML=['All',...dbLanguages].map(l=>`<div class="lang-pill ${activeLang===l?'active':''}" onclick="setLang('${l.replace(/'/g,"\\'")}')">${l==='All'?'All Languages':l}</div>`).join('');
  } else document.getElementById('lang-pills').style.display='none';
}
function setCat(c){if(c.includes('18')&&sessionStorage.getItem('age_18_ok')!=='1'){pendingCat=c;document.getElementById('age-modal').style.display='flex';return;}activeCat=c;feedLimit=15;renderFilters();renderFeed();}
function confirm18(){sessionStorage.setItem('age_18_ok','1');document.getElementById('age-modal').style.display='none';if(pendingCat){activeCat=pendingCat;pendingCat=null;feedLimit=15;renderFilters();renderFeed();}}
function deny18(){pendingCat=null;document.getElementById('age-modal').style.display='none';activeCat='All';feedLimit=15;renderFilters();renderFeed();}
function setLang(l){activeLang=l;feedLimit=15;renderFilters();renderFeed();}
renderFilters();

function matchesType(f){
  if(activeType==='movie')return f.type!=='series';
  if(activeType==='series')return f.type==='series';
  return true;
}

function topScore(f){
  return ((f.views||0)*3)+((f.likes||[]).length*5)+((f.ratings||[]).length*2);
}

function renderTopSlider(){
  let base=allFiles.filter(f=>matchesType(f)&&(!(f.category||'').includes('18')||sessionStorage.getItem('age_18_ok')==='1'));
  let tops=[...base].sort((a,b)=>topScore(b)-topScore(a)).slice(0,10);
  const sec=document.getElementById('pinned-section');
  if(!tops.length){sec.style.display='none';return;}
  sec.style.display='block';
  document.getElementById('home-pinned-head').innerText=activeType==='movie'?'🔥 Top Movies':(activeType==='series'?'🔥 Top Series':'🔥 Top Movies & Series');
  document.getElementById('pinned-list').innerHTML=tops.map((f,idx)=>`<div class="pin-card" onclick="openDetails('${f._id}')"><div class="pin-img-w"><img loading="${idx<3?'eager':'lazy'}" decoding="async" src="${f.thumb_url||'/poster/default'}"><div class="pin-tag">🔥 Top #${idx+1}</div><div class="pin-views">⬇️ ${f.views||0}</div></div><div class="pin-t">${f.title}</div></div>`).join('');
}
renderTopSlider();

function timeAgo(iso){if(!iso)return 'Recently';const s=Math.floor((new Date()-new Date(iso))/1000);if(s<3600)return Math.max(1,Math.floor(s/60))+' min ago';if(s<86400)return Math.floor(s/3600)+' hr ago';return new Date(iso).toLocaleDateString();}

function renderFeed(){
  const q=(document.getElementById('search-inp').value||'').toLowerCase();
  const list=allFiles.filter(f=>{
    if(!matchesType(f))return false;
    if((f.category||'').includes('18')&&activeCat==='All'&&sessionStorage.getItem('age_18_ok')!=='1')return false;
    if(activeCat!=='All'&&(f.category||'All')!==activeCat)return false;
    if(activeLang!=='All'&&(f.language||'')!==activeLang)return false;
    if(q&&!f.title.toLowerCase().includes(q))return false; return true;
  });
  lastFilteredLen=list.length;
  const shown=list.slice(0,feedLimit);
  let h='';
  shown.forEach((f,i)=>{
    const tl=f.type==='series'?'📺 Series':(f.is_premium?'💎 VIP':'🎬 Movie');
    h+=`<div class="feed-card" onclick="openDetails('${f._id}')"><div class="feed-img-w"><img loading="${i<2?'eager':'lazy'}" decoding="async" src="${f.thumb_url||'/poster/default'}"><div class="b-new">🆕 New</div><div class="b-type">${tl}</div></div><div class="feed-body"><div class="feed-title">${f.title}</div><div class="feed-meta"><span>${timeAgo(f.created_at)}</span><span>${f.category||''} ${f.language?'• '+f.language:''}</span></div></div></div>`;
    if((i+1)%3===0&&promotions.length>0)h+=`<div class="car-wrap">`+promotions.map(p=>`<div class="p-slide" onclick="openTgLink('${p.link}')"><img loading="lazy" decoding="async" src="${p.image||'/poster/default'}"><div class="p-slide-b"><div style="font-size:14px;font-weight:700;margin-bottom:6px;">${p.title}</div><button class="watch-pill">Watch Now</button></div></div>`).join('')+`</div>`;
  });
  if(list.length>feedLimit){
    h+=`<button class="btn-p" style="margin:6px 0 16px 0;" onclick="loadMoreFeed()">⬇️ Load More (${list.length-feedLimit} more)</button>`;
  }
  document.getElementById('feed-list').innerHTML=h||"<p style='text-align:center;color:#777;padding:30px;'>কোনো ভিডিও পাওয়া যায়নি!</p>";
}
function loadMoreFeed(){feedLimit+=15;renderFeed();}
window.addEventListener('scroll',()=>{
  if(document.getElementById('page-home').classList.contains('active')&&feedLimit<lastFilteredLen&&(window.innerHeight+window.scrollY)>=document.body.offsetHeight-350){
    loadMoreFeed();
  }
},{passive:true});
renderFeed();

function renderFavs(){
  const favIds=[...(userState.favorites||[])];
  const favIts=allFiles.filter(f=>favIds.includes(f._id));
  document.getElementById('fav-list').innerHTML=favIts.map(f=>`<div class="feed-card" onclick="openDetails('${f._id}')"><div class="feed-img-w"><img loading="lazy" decoding="async" src="${f.thumb_url||'/poster/default'}"><div class="b-new">🔖 Saved</div></div><div class="feed-body"><div class="feed-title">${f.title}</div></div></div>`).join('')||"<p style='text-align:center;color:#777;padding:20px;'>কোনো ফেভারিট ভিডিও সেভ করা নেই!</p>";

  const histIds=(userState.history||[]).filter(id=>!favIds.includes(id));
  const histIts=allFiles.filter(f=>histIds.includes(f._id));
  document.getElementById('hist-list').innerHTML=histIts.map(f=>`<div class="feed-card" onclick="openDetails('${f._id}')"><div class="feed-img-w"><img loading="lazy" decoding="async" src="${f.thumb_url||'/poster/default'}"><div class="b-new" style="background:#475569;">🕒 Watched</div></div><div class="feed-body"><div class="feed-title">${f.title}</div></div></div>`).join('')||"<p style='text-align:center;color:#777;padding:18px;'>কোনো ওয়াচ হিস্টোরি নেই!</p>";
}
async function clearHistory(){
  userState.history=[];
  renderFavs();
  await fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'clear_history',uid:userId})});
}

function renderUpcoming(){document.getElementById('upcoming-list').innerHTML=upcomingList.map(u=>`<div class="feed-card"><div class="feed-img-w"><img loading="lazy" decoding="async" src="${u.thumb_url||'/poster/default'}"><div class="b-new">📅 UPCOMING</div><div class="b-type">🕒 ${u.release_time}</div></div><div class="feed-body"><div class="feed-title">${u.title}</div><div class="feed-meta"><span>Release: ${u.release_time}</span></div></div></div>`).join('')||"<p style='text-align:center;color:#777;padding:20px;'>কোনো আপকামিং শিডিউল নেই!</p>";document.getElementById('promo-full-list').innerHTML=promotions.map(p=>`<div class="feed-card" onclick="openTgLink('${p.link}')"><div class="feed-img-w"><img loading="lazy" decoding="async" src="${p.image||'/poster/default'}"><div class="b-new">⭐ PROMO</div></div><div class="feed-body"><div class="feed-title">${p.title}</div></div></div>`).join('');}
renderUpcoming();

function openDetails(id){
  curFile=allFiles.find(f=>f._id===id); if(!curFile)return;
  if((curFile.category||'').includes('18')&&sessionStorage.getItem('age_18_ok')!=='1'){pendingCat=curFile.category;document.getElementById('age-modal').style.display='flex';return;}
  curSeasonIdx=0; document.getElementById('main-chrome').style.display='none'; document.getElementById('bottom-nav').style.display='none'; document.getElementById('float-search-btn').style.display='none';
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active')); document.getElementById('page-details').classList.add('active'); document.getElementById('cmt-bar').style.display='flex'; window.scrollTo(0,0);
  const tb=document.getElementById('inline-tut-box'), tv=document.getElementById('det-tut-vid'), tbtn=document.getElementById('tut-toggle-btn');
  tv.pause(); tb.style.display='none'; tbtn.innerText='❓ Need help? Watch the tutorial';
  document.getElementById('det-poster').src=curFile.thumb_url||'/poster/default'; document.getElementById('det-title').innerText=curFile.title;
  document.getElementById('det-likes').innerText=(curFile.likes||[]).length; document.getElementById('det-views').innerText=curFile.views||0;
  document.getElementById('det-cmts-cnt').innerText=(curFile.comments||[]).length; document.getElementById('cmt-cnt-h').innerText=(curFile.comments||[]).length;
  let tags=[]; if(curFile.year)tags.push(curFile.year); if(curFile.category)tags.push(curFile.category); if(curFile.language)tags.push(curFile.language);
  document.getElementById('det-tags').innerHTML=tags.map(t=>`<span class="d-tag">${t}</span>`).join('');
  document.getElementById('btn-save').innerText=(userState.favorites||[]).includes(id)?'✅ Saved':'🔖 Save';
  document.getElementById('btn-like').innerText=(curFile.likes||[]).includes(userId)?'❤️ Liked':'🤍 Like';
  renderRating(); renderComments(); renderSelectors();
  if(!(userState.history||[]).includes(id)){
    if(!userState.history)userState.history=[];
    userState.history.push(id);
  }
  fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'history',uid:userId,file_id:id})});
}
function closeDetails(){
  const tb=document.getElementById('inline-tut-box'), tv=document.getElementById('det-tut-vid');
  tv.pause(); tb.style.display='none';
  document.getElementById('main-chrome').style.display='block';document.getElementById('bottom-nav').style.display='grid';document.getElementById('float-search-btn').style.display='flex';document.getElementById('cmt-bar').style.display='none';document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));document.getElementById('page-home').classList.add('active');
}

function toggleInlineTutorial(){
  const box=document.getElementById('inline-tut-box'), vid=document.getElementById('det-tut-vid'), src=document.getElementById('det-tut-src'), st=document.getElementById('tut-status'), btn=document.getElementById('tut-toggle-btn');
  if(!hasTutorial){alert('⚠️ টিউটোরিয়াল ভিডিও এখনো সেট করা হয়নি!');return;}
  if(box.style.display==='none'||!box.style.display){
    box.style.display='block';
    const vUrl='/tutorial_video?v='+tutorialVer;
    if(!src.getAttribute('src')){
      st.style.display='block';
      st.innerText='⏳ টিউটোরিয়াল ভিডিও লোড হচ্ছে...';
      src.setAttribute('src',vUrl);
      vid.oncanplay=()=>{st.style.display='none';};
      vid.onplaying=()=>{st.style.display='none';};
      vid.onerror=()=>{st.style.display='block';st.innerText='⚠️ ভিডিও লোড হতে সময় নিচ্ছে, অনুগ্রহ করে কয়েক সেকেন্ড পর আবার চেষ্টা করুন।';};
      vid.load();
    }
    vid.play().catch(()=>{});
    btn.innerText='🔼 Hide tutorial video';
  } else {
    vid.pause();
    box.style.display='none';
    btn.innerText='❓ Need help? Watch the tutorial';
  }
}

function pad2(v){
  const s=String(v||'1').replace(/^(?:episode|ep|e|season|s)\s*[-_:]?\s*/i,'').trim();
  const m=s.match(/^(\d+)$/);
  if(m){return m[1].length<2?'0'+m[1]:m[1];}
  return s;
}
function sortNumKey(v){
  const m=String(v||'').match(/\d+/);
  return m?parseInt(m[0],10):999999;
}

function renderSelectors(){
  const area=document.getElementById('det-selector-area');
  if(curFile.type==='series'&&curFile.seasons?.length){
    const ssns=[...curFile.seasons].sort((a,b)=>sortNumKey(a.season_no)-sortNumKey(b.season_no));
    if(curSeasonIdx>=ssns.length)curSeasonIdx=0;
    const curS=ssns[curSeasonIdx];
    const sOrig=(curS.orig_idx!==undefined)?curS.orig_idx:curSeasonIdx;
    let h=`<div class="sel-box"><div style="font-size:13.5px;font-weight:700;margin-bottom:8px;">📺 Select Season & Episode</div><div style="display:flex;gap:6px;overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px;">`;
    ssns.forEach((s,si)=>{
      const sLbl=pad2(s.season_no);
      h+=`<button class="s-tab ${si===curSeasonIdx?'active':''}" onclick="curSeasonIdx=${si};renderSelectors()">Season ${sLbl}</button>`;
    });
    h+=`</div>`;
    const eps=[...(curS?.episodes||[])].sort((a,b)=>sortNumKey(a.ep_no)-sortNumKey(b.ep_no));
    eps.forEach((e,ei)=>{
      const eOrig=(e.orig_idx!==undefined)?e.orig_idx:ei;
      const eLbl=pad2(e.ep_no);
      h+=`<div class="ep-card"><div style="font-size:13px;font-weight:700;margin-bottom:6px;">🎬 Episode ${eLbl}</div><div class="q-grid">`;
      (e.qualities||[]).forEach((q,qi)=>{
        h+=`<button class="q-btn" onclick="triggerDownload('sfile_${curFile._id}_${sOrig}_${eOrig}_${qi}')">⬇️ Episode ${eLbl} ${q.name}</button>`;
      });
      h+=`</div></div>`;
    });
    area.innerHTML=h+`</div>`;
  } else if(curFile.qualities?.length){
    area.innerHTML=`<div class="sel-box"><div style="font-size:13.5px;font-weight:700;">🎞 Select Quality</div><div class="q-grid">`+curFile.qualities.map((q,qi)=>`<button class="q-btn" onclick="triggerDownload('mfile_${curFile._id}_${qi}')">⬇️ Download ${q.name}</button>`).join('')+`</div></div>`;
  } else { area.innerHTML=`<div class="sel-box"><button class="btn-p" onclick="triggerDownload('file_${curFile._id}')">⬇️ Download Movie</button></div>`; }
}
function scrollToDownload(){document.getElementById('det-selector-area')?.scrollIntoView({behavior:'smooth'});}

function isFileLocallyUnlocked(fid){
  if(userState.is_premium)return true;
  const exp=(userState.unlocked_files||{})[fid];
  if(!exp)return false;
  try{return new Date(exp)>new Date();}catch(e){return false;}
}

let pendingUnlockData=null;
function triggerDownload(sp){
  const dl=`https://t.me/${botUsername}?start=${sp}`;
  if(isFileLocallyUnlocked(curFile._id)){openTgLink(dl);return;}
  const adLinks=adConfig.ad_links||[];
  const zList=adConfig.zone_ids||zoneIds||[];
  const needsAd=curFile.is_premium||(adConfig.ads_on&&(adLinks.length>0||zList.length>0));
  if(!needsAd){openTgLink(dl);return;}

  const waitList=adConfig.wait_times||[5];
  const waitT=waitList[Math.floor(Math.random()*waitList.length)]||5;
  const randLink=adLinks.length?adLinks[Math.floor(Math.random()*adLinks.length)]:null;
  const randZone=zList.length?zList[Math.floor(Math.random()*zList.length)]:null;
  const coinPrice=adConfig.coin_price||50;

  pendingUnlockData={dl,ad:{ad_link:randLink,zone_id:randZone,wait_time:waitT,coin_price:coinPrice},fileId:curFile._id};
  let h=`<button class="unlock-opt" style="background:linear-gradient(90deg,#f59e0b,#d97706);" onclick="unlockWithCoin()"><span>🪙 কয়েন দিয়ে আনলক (${coinPrice} Coins)</span><span>আপনার: ${userState.balance||0}</span></button>`;
  if(randZone){
    h+=`<button class="unlock-opt" style="background:linear-gradient(90deg,#6c5ce7,#4f46e5);" onclick="unlockWithMonetag('${randZone}')"><span>📺 Monetag Zone Ad দিয়ে আনলক</span><span>Easy ⚡</span></button>`;
  }
  if(randLink){
    h+=`<button class="unlock-opt" style="background:linear-gradient(90deg,#ff512f,#dd2476);" onclick="unlockWithAdsterra()"><span>🔗 Adsterra / Link Ad দিয়ে আনলক</span><span>${waitT}s ⏳</span></button>`;
  }
  document.getElementById('unlock-options-area').innerHTML=h;
  document.getElementById('unlock-modal').style.display='flex';
}

async function unlockWithCoin(){
  if(!pendingUnlockData)return;
  const {dl,fileId}=pendingUnlockData;
  const r2=await fetch('/api/unlock_file',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,file_id:fileId,method:'coin'})});
  const d2=await r2.json();
  if(d2.status==='success'){
    if(!userState.unlocked_files)userState.unlocked_files={};
    userState.unlocked_files[fileId]=d2.expiry;
    document.getElementById('unlock-modal').style.display='none';
    loadUser();
    openTgLink(dl);
  } else alert(d2.msg);
}

async function unlockWithMonetag(zid){
  if(!pendingUnlockData)return;
  const {dl,fileId}=pendingUnlockData;
  document.getElementById('unlock-modal').style.display='none';
  if(zid&&typeof window['show_'+zid]==='function'){
    try{
      await window['show_'+zid]();
      const r=await fetch('/api/unlock_file',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,file_id:fileId,method:'monetag'})});
      const d=await r.json();
      if(d.expiry){if(!userState.unlocked_files)userState.unlocked_files={};userState.unlocked_files[fileId]=d.expiry;}
      openTgLink(dl);
      return;
    }catch(e){}
  }
  alert('⚠️ Monetag অ্যাড লোড হয়নি, অন্য মাধ্যম বা কয়েক সেকেন্ড পর আবার চেষ্টা করুন।');
}

function unlockWithAdsterra(){
  if(!pendingUnlockData)return;
  const {dl,ad,fileId}=pendingUnlockData;
  document.getElementById('unlock-modal').style.display='none';
  runAdTimer(ad.ad_link,ad.wait_time||5,async()=>{
    const r=await fetch('/api/unlock_file',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,file_id:fileId,method:'adsterra'})});
    const d=await r.json();
    if(d.expiry){if(!userState.unlocked_files)userState.unlocked_files={};userState.unlocked_files[fileId]=d.expiry;}
    openTgLink(dl);
  });
}

function runAdTimer(link,sec,onDone){
  const ov=document.getElementById('ad-modal'),tm=document.getElementById('ad-timer'),btn=document.getElementById('ad-action-btn');
  ov.style.display='flex';btn.style.display='none';tm.innerText=sec;
  try{if(tg?.openLink)tg.openLink(link);else window.open(link,'_blank');}catch(e){}
  let t=sec,iv=setInterval(()=>{t--;tm.innerText=Math.max(0,t);if(t<=0){clearInterval(iv);btn.style.display='block';btn.onclick=()=>{ov.style.display='none';onDone();};}},1000);
}

function likeCurrent(){if(!curFile.likes)curFile.likes=[];if(!curFile.likes.includes(userId)){curFile.likes.push(userId);document.getElementById('det-likes').innerText=curFile.likes.length;document.getElementById('btn-like').innerText='❤️ Liked';fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'like',uid:userId,file_id:curFile._id})});}}
async function saveCurrent(){
  const fid=curFile._id;
  if(!userState.favorites)userState.favorites=[];
  const wasSaved=userState.favorites.includes(fid);
  if(wasSaved){userState.favorites=userState.favorites.filter(x=>x!==fid);document.getElementById('btn-save').innerText='🔖 Save';}
  else{userState.favorites.push(fid);document.getElementById('btn-save').innerText='✅ Saved';}
  renderFavs();
  const r=await fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'favorite',uid:userId,file_id:fid})});
  const d=await r.json();
  document.getElementById('btn-save').innerText=d.saved?'✅ Saved':'🔖 Save';
  loadUser();
}
function shareCurrent(){
  const refUrl=`https://t.me/${botUsername}?start=${userId}`;
  const shareTxt=`🔥 Watch "${curFile.title}" & thousands of Movies/Series on ${siteName}!`;
  openTgLink(`https://t.me/share/url?url=${encodeURIComponent(refUrl)}&text=${encodeURIComponent(shareTxt)}`);
}
function renderRating(){const rs=curFile.ratings||[],avg=rs.length?(rs.reduce((a,b)=>a+b.stars,0)/rs.length).toFixed(1):'0.0';document.getElementById('rate-avg').innerText=`⭐ ${avg} (${rs.length})`;const my=(rs.find(r=>r.uid===userId)||{}).stars||0;document.getElementById('star-row').innerHTML=[1,2,3,4,5].map(n=>`<button class="star-b ${n<=my?'lit':''}" onclick="rateVideo(${n})">★</button>`).join('');}
async function rateVideo(stars){await fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'rate',uid:userId,file_id:curFile._id,stars})});if(!curFile.ratings)curFile.ratings=[];curFile.ratings=curFile.ratings.filter(x=>x.uid!==userId);curFile.ratings.push({uid:userId,stars});renderRating();}
function renderComments(){const cs=curFile.comments||[];document.getElementById('det-cmt-list').innerHTML=cs.map(c=>`<div style="padding:8px 0;border-bottom:1px solid #f1f5f9;font-size:13.5px;"><b>User ${c.uid}</b> <small style="color:#94a3b8;">${c.time}</small><br>${c.text}</div>`).join('')||"<p style='color:#94a3b8;font-size:13px;'>No comments yet.</p>";}
async function sendComment(){const i=document.getElementById('cmt-inp'),t=i.value.trim();if(!t||!curFile)return;i.value='';const r=await fetch('/api/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'comment',uid:userId,file_id:curFile._id,text:t})});const d=await r.json();if(!curFile.comments)curFile.comments=[];curFile.comments.push(d.comment);document.getElementById('det-cmts-cnt').innerText=curFile.comments.length;document.getElementById('cmt-cnt-h').innerText=curFile.comments.length;renderComments();}

function switchTab(id,el,tp){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('page-'+id).classList.add('active');
  document.querySelectorAll('.nav-i').forEach(n=>n.classList.remove('active'));
  if(el)el.classList.add('active');
  if(id==='home'){
    activeType=tp||'all';
    feedLimit=15;
    renderTopSlider();
    renderFeed();
    window.scrollTo({top:0,behavior:'smooth'});
  }
  if(id==='favorite')renderFavs();
  if(id==='profile'){loadUser();loadEarn();}
  if(id==='tools')renderTools();
}

const TOOLS=[
  {id:'age',sec:'🔥 Popular Tools',ico:'🎂',bg:'#fef3c7',bl:'#ddd6fe',name:'Age Calculator',desc:'Calculate exact age'},
  {id:'bmi',sec:'🔥 Popular Tools',ico:'⚖️',bg:'#e0e7ff',bl:'#fbcfe8',name:'BMI Calculator',desc:'Body Mass Index'},
  {id:'curr',sec:'🔥 Popular Tools',ico:'💱',bg:'#d1fae5',bl:'#a7f3d0',name:'Currency Converter',desc:'Convert currencies'},
  {id:'love',sec:'🔥 Popular Tools',ico:'💘',bg:'#ffe4e6',bl:'#fecdd3',name:'Love Match',desc:'Love compatibility'},
  {id:'pass',sec:'🛡️ Utility Tools',ico:'🔐',bg:'#e0f2fe',bl:'#bae6fd',name:'Password Generator',desc:'Strong passwords'},
  {id:'blood',sec:'🛡️ Utility Tools',ico:'🩸',bg:'#fee2e2',bl:'#fecaca',name:'Blood Group Finder',desc:'Donor compatibility'},
  {id:'qr',sec:'🛡️ Utility Tools',ico:'📱',bg:'#f3e8ff',bl:'#e9d5ff',name:'QR Code Generator',desc:'Create QR for link'},
  {id:'rand',sec:'🛡️ Utility Tools',ico:'🎲',bg:'#fef9c3',bl:'#fde68a',name:'Random Picker',desc:'Pick random item'},
  {id:'style',sec:'⚡ Exclusive Tools',ico:'✨',bg:'#fae8ff',bl:'#e9d5ff',name:'Stylish Text Maker',desc:'Stylish Unicode fonts'},
  {id:'tasbih',sec:'⚡ Exclusive Tools',ico:'📿',bg:'#dcfce7',bl:'#bbf7d0',name:'Tasbih Counter',desc:'Digital tasbih counter'},
  {id:'tz',sec:'⚡ Exclusive Tools',ico:'🌍',bg:'#dbeafe',bl:'#bfdbfe',name:'Time Zone Converter',desc:'World live times'},
  {id:'unit',sec:'🧰 More Useful Tools',ico:'📏',bg:'#e0f2fe',bl:'#bae6fd',name:'Unit Converter',desc:'Length, weight, temp',n:1},
  {id:'pct',sec:'🧰 More Useful Tools',ico:'📊',bg:'#fef3c7',bl:'#fde68a',name:'Percentage Calculator',desc:'Calculate percentage',n:1},
  {id:'emi',sec:'🧰 More Useful Tools',ico:'💰',bg:'#d1fae5',bl:'#a7f3d0',name:'EMI Calculator',desc:'Monthly loan EMI',n:1},
  {id:'ddiff',sec:'🧰 More Useful Tools',ico:'📅',bg:'#f3e8ff',bl:'#e9d5ff',name:'Date Difference',desc:'Days between dates',n:1},
  {id:'word',sec:'🧰 More Useful Tools',ico:'🔤',bg:'#e0e7ff',bl:'#fecdd3',name:'Text Case & Word Count',desc:'Case & word count',n:1},
  {id:'zakat',sec:'🧰 More Useful Tools',ico:'🕌',bg:'#fef9c3',bl:'#bbf7d0',name:'Zakat Calculator',desc:'2.5% Zakat calc',n:1},
  {id:'color',sec:'🧰 More Useful Tools',ico:'🎨',bg:'#ffe4e6',bl:'#fbcfe8',name:'Color Picker',desc:'HEX, RGB, HSL codes',n:1},
  {id:'short',sec:'🌟 More Tools',ico:'🔗',bg:'#e0f2fe',bl:'#ddd6fe',name:'Link Shortener',desc:'Shorten with Ad Gate'},
  {id:'mrec',sec:'🌟 More Tools',ico:'🎬',bg:'#f1f5f9',bl:'#fecaca',name:'Movie Recommendation',desc:'Join Movie Group'}
];

function renderTools(){
  const q=(document.getElementById('tool-search').value||'').toLowerCase(), secs=[...new Set(TOOLS.map(t=>t.sec))]; let h='';
  secs.forEach(s=>{
    const its=TOOLS.filter(t=>t.sec===s&&(!q||t.name.toLowerCase().includes(q)||t.desc.toLowerCase().includes(q))); if(!its.length)return;
    h+=`<div class="sec-head"><span>${s}</span></div><div class="t-grid">`+its.map(t=>`<div class="t-card" onclick="openTool('${t.id}')"><div class="t-blob" style="background:${t.bl};"></div>${t.n?'<span class="t-new">NEW</span>':''}<div class="t-ico" style="background:${t.bg};">${t.ico}</div><div class="t-name">${t.name}</div><div class="t-desc">${t.desc}</div></div>`).join('')+`</div>`;
  });
  document.getElementById('tools-container').innerHTML=h;
}

let tasbihCnt=0;
function openTool(id){
  const t=TOOLS.find(x=>x.id===id); document.getElementById('tool-m-title').innerText=t.ico+' '+t.name;
  const b=document.getElementById('tool-m-body'); document.getElementById('tool-modal').style.display='flex';
  const ui={
    age:`<span class="tool-lbl">জন্মতারিখ (যেমন: 2002-05-15)</span><input type="date" id="ti1" class="s-input" value="2002-05-15"><br><br><button class="btn-p" onclick="calcAge()">Calculate</button><div id="tr" class="tool-res"></div>`,
    bmi:`<span class="tool-lbl">ওজন কেজিতে (যেমন: 65)</span><input type="number" id="ti1" class="s-input" placeholder="যেমন: 65"><span class="tool-lbl">উচ্চতা সেমি-তে (যেমন: 170)</span><input type="number" id="ti2" class="s-input" placeholder="যেমন: 170"><br><br><button class="btn-p" onclick="calcBMI()">Calculate BMI</button><div id="tr" class="tool-res"></div>`,
    curr:`<span class="tool-lbl">পরিমাণ (যেমন: 100)</span><input type="number" id="ti1" class="s-input" value="100" placeholder="যেমন: 100"><span class="tool-lbl">From</span><select id="ti2" class="s-input"><option>USD</option><option>BDT</option><option>SAR</option><option>AED</option><option>INR</option><option>EUR</option><option>GBP</option><option>MYR</option></select><span class="tool-lbl">To</span><select id="ti3" class="s-input"><option>BDT</option><option>USD</option><option>SAR</option><option>AED</option><option>INR</option><option>EUR</option></select><br><br><button class="btn-p" onclick="calcCurr()">Convert</button><div id="tr" class="tool-res"></div>`,
    love:`<span class="tool-lbl">আপনার নাম (যেমন: Rahim)</span><input id="ti1" class="s-input" placeholder="যেমন: Rahim"><span class="tool-lbl">পার্টনারের নাম (যেমন: Karim)</span><input id="ti2" class="s-input" placeholder="যেমন: Karim"><br><br><button class="btn-p" onclick="calcLove()">Check Match 💘</button><div id="tr" class="tool-res"></div>`,
    pass:`<span class="tool-lbl">পাসওয়ার্ড দৈর্ঘ্য (যেমন: 12)</span><input type="number" id="ti1" class="s-input" value="12" placeholder="যেমন: 12" min="6" max="32"><br><br><button class="btn-p" onclick="genPass()">Generate</button><div id="tr" class="tool-res"></div>`,
    blood:`<span class="tool-lbl">ব্লাড গ্রুপ সিলেক্ট করুন (যেমন: O+)</span><select id="ti1" class="s-input" onchange="showBlood()"><option value="O+">O+</option><option value="O-">O-</option><option value="A+">A+</option><option value="A-">A-</option><option value="B+">B+</option><option value="B-">B-</option><option value="AB+">AB+</option><option value="AB-">AB-</option></select><div id="tr" class="tool-res"></div>`,
    qr:`<span class="tool-lbl">যেকোনো লিংক বা লেখা (যেমন: https://t.me/${botUsername})</span><input id="ti1" class="s-input" placeholder="যেমন: https://google.com"><br><br><button class="btn-p" onclick="genQR()">Generate QR</button><div id="tr" class="tool-res" style="text-align:center;"></div>`,
    rand:`<span class="tool-lbl">কমা দিয়ে অপশন লিখুন (যেমন: Apple, Mango, Banana)</span><textarea id="ti1" class="s-input" rows="3" placeholder="যেমন: Ali, Karim, Rahim"></textarea><br><br><button class="btn-p" onclick="pickRand()">Pick Random</button><div id="tr" class="tool-res"></div>`,
    style:`<span class="tool-lbl">ইংরেজি লেখা লিখুন (যেমন: Moviee Link)</span><input id="ti1" class="s-input" placeholder="যেমন: Moviee Link" onkeyup="genStyle()"><div id="tr" class="tool-res"></div>`,
    tasbih:`<h1 id="tcnt" style="font-size:48px;color:#10b981;margin:8px 0;">${tasbihCnt}</h1><button class="btn-p" onclick="tasbihCnt++;document.getElementById('tcnt').innerText=tasbihCnt">Count (+1)</button><br><br><button class="btn-p" style="background:#64748b;" onclick="tasbihCnt=0;document.getElementById('tcnt').innerText=0">Reset</button>`,
    tz:`<div id="tr" class="tool-res"></div>`,
    unit:`<span class="tool-lbl">সংখ্যা দিন (যেমন: 5)</span><input type="number" id="ti1" class="s-input" value="5" placeholder="যেমন: 5"><span class="tool-lbl">রূপান্তর</span><select id="ti2" class="s-input"><option value="km_m">KM ➔ Meter</option><option value="kg_g">KG ➔ Gram</option><option value="kg_lb">KG ➔ Pound</option><option value="c_f">°C ➔ °F</option><option value="f_c">°F ➔ °C</option></select><br><br><button class="btn-p" onclick="calcUnit()">Convert</button><div id="tr" class="tool-res"></div>`,
    pct:`<span class="tool-lbl">শতকরা হার % (যেমন: 15)</span><input type="number" id="ti1" class="s-input" placeholder="যেমন: 15"><span class="tool-lbl">মোট সংখ্যা (যেমন: 1000)</span><input type="number" id="ti2" class="s-input" placeholder="যেমন: 1000"><br><br><button class="btn-p" onclick="calcPct()">Calculate</button><div id="tr" class="tool-res"></div>`,
    emi:`<span class="tool-lbl">লোনের পরিমাণ (যেমন: 100000)</span><input type="number" id="ti1" class="s-input" placeholder="যেমন: 100000"><span class="tool-lbl">বার্ষিক সুদ % (যেমন: 10)</span><input type="number" id="ti2" class="s-input" placeholder="যেমন: 10"><span class="tool-lbl">মাস (যেমন: 12)</span><input type="number" id="ti3" class="s-input" placeholder="যেমন: 12"><br><br><button class="btn-p" onclick="calcEMI()">Calculate EMI</button><div id="tr" class="tool-res"></div>`,
    ddiff:`<span class="tool-lbl">শুরুর তারিখ</span><input type="date" id="ti1" class="s-input" value="2026-01-01"><span class="tool-lbl">শেষের তারিখ</span><input type="date" id="ti2" class="s-input" value="2026-12-31"><br><br><button class="btn-p" onclick="calcDDiff()">Calculate Days</button><div id="tr" class="tool-res"></div>`,
    word:`<span class="tool-lbl">যেকোনো লেখা লিখুন (যেমন: Hello World 2026)</span><textarea id="ti1" class="s-input" rows="3" placeholder="যেমন: Hello World 2026" onkeyup="calcWord()"></textarea><div style="display:flex;gap:6px;margin-top:8px;"><button class="btn-p" onclick="ti1.value=ti1.value.toUpperCase();calcWord()">UPPER</button><button class="btn-p" onclick="ti1.value=ti1.value.toLowerCase();calcWord()">lower</button></div><div id="tr" class="tool-res"></div>`,
    zakat:`<span class="tool-lbl">মোট সম্পদ/টাকা/সোনা (যেমন: 500000)</span><input type="number" id="ti1" class="s-input" placeholder="যেমন: 500000"><span class="tool-lbl">ঋণ থাকলে লিখুন (যেমন: 0)</span><input type="number" id="ti2" class="s-input" placeholder="যেমন: 0" value="0"><br><br><button class="btn-p" onclick="calcZakat()">Calculate 2.5%</button><div id="tr" class="tool-res"></div>`,
    color:`<span class="tool-lbl">যেকোনো কালার সিলেক্ট করুন</span><input type="color" id="ti1" value="#6c5ce7" style="width:100%;height:50px;border:none;cursor:pointer;" oninput="pickCol()"><div id="tr" class="tool-res"></div>`,
    short:`<span class="tool-lbl">বড় লিংক দিন (যেমন: https://google.com/search?q=movies)</span><input id="ti1" class="s-input" placeholder="যেমন: https://google.com"><br><br><button class="btn-p" onclick="shortUrl()">Shorten Link (With Ads)</button><div id="tr" class="tool-res"></div>`,
    mrec: recGroupLink
      ? `<p style="font-size:13.5px;color:#475569;margin-bottom:12px;">মুভি রিকোয়েস্ট ও সাজেশন পেতে আমাদের অফিসিয়াল গ্রুপে জয়েন করুন!</p><div class="tool-res" style="margin-bottom:12px;text-align:center;">🔗 <b>${recGroupLink}</b></div><button class="btn-p" onclick="openTgLink('${recGroupLink}')">👥 Join Movie Group</button><br><br><button class="btn-p" style="background:#0f172a;" onclick="navigator.clipboard.writeText('${recGroupLink}');alert('✅ Group Link Copied!')">📋 Copy Group Link</button>`
      : `<div class="tool-res" style="text-align:center;">⚠️ গ্রুপ লিংক এখনো সেট করা হয়নি!<br><small>অ্যাডমিন বটে <b>/setrecgroup https://t.me/...</b> দিয়ে গ্রুপ লিংক সেট করুন।</small></div>`
  };
  b.innerHTML=ui[id];
  if(id==='blood')showBlood();
  if(id==='tz'){const zs=[['🇧🇩 Dhaka','Asia/Dhaka'],['🇸🇦 Riyadh','Asia/Riyadh'],['🇦🇪 Dubai','Asia/Dubai'],['🇮🇳 Delhi','Asia/Kolkata'],['🇬🇧 London','Europe/London'],['🇺🇸 New York','America/New_York']];document.getElementById('tr').innerHTML=zs.map(z=>`<b>${z[0]}:</b> ${new Date().toLocaleTimeString('en-US',{timeZone:z[1]})}`).join('<br>');}
  if(id==='color')pickCol();
}

function calcAge(){const d=new Date(ti1.value),n=new Date();if(isNaN(d))return;let y=n.getFullYear()-d.getFullYear(),m=n.getMonth()-d.getMonth(),dd=n.getDate()-d.getDate();if(dd<0){m--;dd+=30;}if(m<0){y--;m+=12;}tr.innerHTML=`🎂 <b>${y} Years, ${m} Months, ${dd} Days</b><br>Total Days: ${Math.floor((n-d)/86400000)}`;}
function calcBMI(){const w=+ti1.value,h=+ti2.value/100;if(!w||!h)return;const bmi=(w/(h*h)).toFixed(1);tr.innerHTML=`⚖️ BMI: <b>${bmi}</b> (${bmi<18.5?'Underweight':bmi<25?'Normal':bmi<30?'Overweight':'Obese'})`;}
async function calcCurr(){const r=await(await fetch('/api/rates')).json(),v=+ti1.value*(r[ti3.value]/r[ti2.value]);tr.innerHTML=`💱 <b>${ti1.value} ${ti2.value} = ${v.toFixed(2)} ${ti3.value}</b>`;}
function calcLove(){const s=(ti1.value+ti2.value).toLowerCase().replace(/\s/g,'');if(!s)return;let h=0;for(let i=0;i<s.length;i++)h=(h*31+s.charCodeAt(i))%101;tr.innerHTML=`💘 Compatibility: <b>${Math.max(45,h)}%</b> ❤️`;}
function genPass(){const cs="ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789!@#$%&*",n=Math.min(32,Math.max(6,+ti1.value||12));let p="";for(let i=0;i<n;i++)p+=cs[Math.floor(Math.random()*cs.length)];tr.innerHTML=`🔐 <b>${p}</b>`;}
function showBlood(){const m={'O+':'Give: O+,A+,B+,AB+ | Take: O+,O-','O-':'Universal Donor! | Take: O-','A+':'Give: A+,AB+ | Take: A+,A-,O+,O-','A-':'Give: A+,A-,AB+,AB- | Take: A-,O-','B+':'Give: B+,AB+ | Take: B+,B-,O+,O-','B-':'Give: B+,B-,AB+,AB- | Take: B-,O-','AB+':'Give: AB+ | Take: All (Universal Receiver)','AB-':'Give: AB+,AB- | Take: AB-,A-,B-,O-'};tr.innerHTML=`🩸 <b>${ti1.value}:</b><br>${m[ti1.value]}`;}
function genQR(){if(!ti1.value)return;tr.innerHTML=`<img src="https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=${encodeURIComponent(ti1.value)}" style="border-radius:10px;">`;}
function pickRand(){const a=ti1.value.split(/[,\n]+/).map(x=>x.trim()).filter(Boolean);if(a.length)tr.innerHTML=`🎲 Winner: <b>${a[Math.floor(Math.random()*a.length)]}</b>`;}
function genStyle(){const t=ti1.value;if(!t){tr.innerHTML='';return;}const mp=(a,b)=>[...t].map(c=>{const i=c.charCodeAt(0);if(i>=65&&i<=90)return String.fromCodePoint(a+i-65);if(i>=97&&i<=122)return String.fromCodePoint(b+i-97);return c;}).join('');tr.innerHTML=[mp(0x1D400,0x1D41A),mp(0x1D4D0,0x1D4EA),mp(0x1D5D4,0x1D5EE),mp(0x1D670,0x1D68A),`★彡 ${t} 彡★`].map(x=>`<div style="padding:4px 0;border-bottom:1px solid #ddd;">${x}</div>`).join('');}
function calcUnit(){const v=+ti1.value,k=ti2.value,r=k==='km_m'?v*1000:k==='kg_g'?v*1000:k==='kg_lb'?v*2.20462:k==='c_f'?(v*9/5)+32:(v-32)*5/9;tr.innerHTML=`📏 Result: <b>${r.toFixed(2)}</b>`;}
function calcPct(){tr.innerHTML=`📊 ${ti1.value}% of ${ti2.value} = <b>${((+ti1.value/100)*+ti2.value).toFixed(2)}</b>`;}
function calcEMI(){const P=+ti1.value,r=(+ti2.value/12)/100,n=+ti3.value;if(!P||!n)return;const e=r?(P*r*Math.pow(1+r,n))/(Math.pow(1+r,n)-1):P/n;tr.innerHTML=`💰 Monthly EMI: <b>${e.toFixed(2)}</b><br>Total: ${(e*n).toFixed(2)}`;}
function calcDDiff(){const d1=new Date(ti1.value),d2=new Date(ti2.value);if(isNaN(d1)||isNaN(d2))return;tr.innerHTML=`📅 Difference: <b>${Math.abs(Math.round((d2-d1)/86400000))} Days</b>`;}
function calcWord(){const t=ti1.value;tr.innerHTML=`Words: <b>${t.trim()?t.trim().split(/\s+/).length:0}</b> | Chars: <b>${t.length}</b>`;}
function calcZakat(){const net=Math.max(0,(+ti1.value||0)-(+ti2.value||0));tr.innerHTML=`🕌 Net Wealth: <b>${net}</b><br>Zakat (2.5%): <b>${(net*0.025).toFixed(2)}</b>`;}
function pickCol(){const h=ti1.value,r=parseInt(h.slice(1,3),16),g=parseInt(h.slice(3,5),16),b=parseInt(h.slice(5,7),16);tr.innerHTML=`HEX: <b>${h}</b><br>RGB: <b>rgb(${r},${g},${b})</b>`;}
async function shortUrl(){const res=await(await fetch('/api/shorten',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({url:ti1.value})})).json();if(res.status==='success'){tr.innerHTML=`🔗 <a href="${res.short_url}" target="_blank">${res.short_url}</a><br><br><button class="btn-p" onclick="navigator.clipboard.writeText('${res.short_url}');alert('✅ Short Link Copied!')">📋 Copy Short Link</button>`;}else{tr.innerHTML=res.msg;}}

async function loadEarn(){
  const r=await fetch('/api/earn_info/'+userId),d=await r.json();
  let h=`<div class="card-box"><b>🎡 Daily Spin (${d.spins_done}/${d.spin_limit})</b><br><br><button class="btn-p" onclick="doEarn('spin')" ${d.spins_done>=d.spin_limit?'disabled':''}>Spin & Earn</button></div>`;
  h+=`<div class="card-box"><b>📱 Daily Task (${d.tasks_done}/${d.task_limit})</b><br><br><button class="btn-p" onclick="doEarn('task')" ${d.tasks_done>=d.task_limit?'disabled':''}>Complete Task</button></div>`;
  (d.custom_tasks||[]).forEach(t=>{const dn=(d.custom_tasks_done||[]).includes(t._id);h+=`<div class="card-box"><b>💎 ${t.title} (+${t.coin}C)</b><br><br><button class="btn-p" onclick="doCustomEarn('${t._id}','${t.link}')" ${dn?'disabled':''}>${dn?'Completed':'Complete'}</button></div>`;});
  document.getElementById('earn-area').innerHTML=h;
}
async function doEarn(tp){
  const ad=await(await fetch('/api/get_earn_ad/'+tp)).json();
  if(ad.zone_id&&typeof window['show_'+ad.zone_id]==='function'){try{await window['show_'+ad.zone_id]();return claimEarn(tp);}catch(e){}}
  if(ad.ad_link)runAdTimer(ad.ad_link,ad.wait_time||10,()=>claimEarn(tp)); else claimEarn(tp);
}
function doCustomEarn(id,lk){runAdTimer(lk,10,()=>claimEarn('custom_'+id));}
async function claimEarn(tp){const d=await(await fetch('/api/claim_earn',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,type:tp})})).json();alert(d.msg);loadUser();loadEarn();}
async function redeemCoupon(){const c=document.getElementById('coupon-inp').value.trim();if(!c)return;const d=await(await fetch('/api/redeem',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,code:c})})).json();alert(d.msg);if(d.status==='success'){document.getElementById('coupon-inp').value='';loadUser();}}
async function sendMovieRequest(){const n=document.getElementById('req-inp').value.trim();if(!n)return;const d=await(await fetch('/api/request_movie',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,movie_name:n})})).json();alert(d.msg);document.getElementById('req-inp').value='';}
async function buyWithCoin(pid,cost){if(!confirm(`${cost} কয়েন দিয়ে কিনবেন?`))return;const d=await(await fetch('/api/buy_with_coin',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({uid:userId,pkg_id:pid})})).json();alert(d.msg);loadUser();}
function copyRef(){const i=document.getElementById('ref-inp');i.select();navigator.clipboard.writeText(i.value);alert('✅ Link Copied!');}
</script>
</body>
</html>"""

def _fetch_home_payload():
    now = time.time()
    if HOME_DATA_CACHE["data"] is not None and (now - HOME_DATA_CACHE["ts"]) < 6:
        return HOME_DATA_CACHE["data"]

    def q_config():
        try: return sync_db["config"].find_one({"_id": "settings"}) or {}
        except: return {}

    def q_files():
        res = []
        for d in get_extra_dbs_sync():
            try: res.extend(list(d["files"].find().sort("_id", -1)))
            except: pass
        return res

    def q_cats():
        try: return [c["name"] for c in sync_db["categories"].find()]
        except: return []

    def q_langs():
        try: return [l["name"] for l in sync_db["languages"].find()]
        except: return []

    def q_promos():
        try: return list(sync_db["promotions"].find())
        except: return []

    def q_upcs():
        try: return list(sync_db["upcoming"].find().sort("_id", -1))
        except: return []

    def q_pkgs():
        try: return list(sync_db["packages"].find())
        except: return []

    def q_zones():
        try: return [z["zone_id"] for z in sync_db["zone_ids"].find()]
        except: return []

    def q_ad_links():
        try: return [l["link"] for l in sync_db["ad_links"].find() if l.get("link")]
        except: return []

    f_cfg = WEB_POOL.submit(q_config)
    f_files = WEB_POOL.submit(q_files)
    f_cats = WEB_POOL.submit(q_cats)
    f_langs = WEB_POOL.submit(q_langs)
    f_promos = WEB_POOL.submit(q_promos)
    f_upcs = WEB_POOL.submit(q_upcs)
    f_pkgs = WEB_POOL.submit(q_pkgs)
    f_zones = WEB_POOL.submit(q_zones)
    f_links = WEB_POOL.submit(q_ad_links)

    config = f_cfg.result()
    raw_files = f_files.result()
    cats = f_cats.result()
    langs = f_langs.result()
    promos = f_promos.result()
    upcs = f_upcs.result()
    pkgs = f_pkgs.result()
    zone_ids = f_zones.result()
    ad_links = f_links.result()

    if not zone_ids and DEFAULT_ZONE_ID:
        zone_ids = [DEFAULT_ZONE_ID]

    live_url = get_live_web_url(config)
    files = []
    for doc in raw_files:
        fid = str(doc["_id"])
        tu = doc.get("thumb_url") or ""
        if "/poster/" in tu:
            tu = f"{live_url}/poster/{tu.split('/poster/', 1)[1]}"
        elif tu.startswith("data:image"):
            tu = f"{live_url}/poster/{fid}"
        elif not tu or "placehold.co" in tu:
            tu = f"{live_url}/poster/default"

        indexed_seasons = []
        for si, s in enumerate(doc.get("seasons", [])):
            indexed_eps = []
            for ei, e in enumerate(s.get("episodes", [])):
                ep_lbl = clean_ep_no(e.get("ep_no", "1"))
                indexed_eps.append({
                    "ep_no": ep_lbl,
                    "orig_idx": ei,
                    "qualities": [{"name": q.get("name", "HD")} for q in e.get("qualities", [])]
                })
            indexed_eps.sort(key=lambda x: natural_sort_key(x["ep_no"]))
            ssn_lbl = clean_season_no(s.get("season_no", "1"))
            indexed_seasons.append({
                "season_no": ssn_lbl,
                "orig_idx": si,
                "episodes": indexed_eps
            })
        indexed_seasons.sort(key=lambda x: natural_sort_key(x["season_no"]))

        clean_doc = {
            "_id": fid,
            "type": doc.get("type", "movie"),
            "title": doc.get("title", ""),
            "name": doc.get("name", ""),
            "year": doc.get("year", ""),
            "category": doc.get("category", "All"),
            "language": doc.get("language", ""),
            "is_premium": bool(doc.get("is_premium", False)),
            "thumb_url": tu,
            "pinned": bool(doc.get("pinned", False)),
            "ratings": doc.get("ratings", []),
            "created_at": doc.get("created_at", ""),
            "views": doc.get("views", 0),
            "likes": doc.get("likes", []),
            "comments": doc.get("comments", []),
            "qualities": [{"name": q.get("name", "HD")} for q in doc.get("qualities", [])],
            "seasons": indexed_seasons
        }
        files.append(clean_doc)

    for p in promos:
        p["_id"] = str(p["_id"])
        img = p.get("image") or ""
        if "/poster/" in img:
            p["image"] = f"{live_url}/poster/{img.split('/poster/', 1)[1]}"
        elif img.startswith("data:image"):
            p["image"] = f"{live_url}/poster/promo_{p['_id']}"

    for u in upcs:
        u["_id"] = str(u["_id"])
        tu = u.get("thumb_url") or ""
        if "/poster/" in tu:
            u["thumb_url"] = f"{live_url}/poster/{tu.split('/poster/', 1)[1]}"
        elif tu.startswith("data:image"):
            u["thumb_url"] = f"{live_url}/poster/upc_{u['_id']}"

    payload = {
        "config": config,
        "files": files,
        "cats": cats,
        "langs": langs,
        "promos": promos,
        "upcs": upcs,
        "pkgs": pkgs,
        "zone_ids": zone_ids,
        "ad_links": ad_links
    }
    HOME_DATA_CACHE["data"] = payload
    HOME_DATA_CACHE["ts"] = time.time()
    return payload

@web.route('/')
def home():
    p = _fetch_home_payload()
    config = p["config"]
    tut_vid = config.get("tutorial_vid") or ""
    tut_ver = hashlib.md5(str(tut_vid).encode()).hexdigest()[:8] if tut_vid else "0"
    logo_val = config.get("start_logo") or config.get("site_name", "FB")
    logo_ver = hashlib.md5(str(logo_val).encode()).hexdigest()[:8]
    sn = config.get("site_name", "Moviee Link")
    site_name_hash = hashlib.md5(sn.encode("utf-8")).hexdigest()[:8]

    uid_arg = request.args.get("uid", "")
    initial_user = None
    if uid_arg.isdigit():
        try: initial_user = build_user_state_dict(int(uid_arg))
        except: pass

    ad_config = {
        "ads_on": config.get("ads_on", True),
        "coin_price": config.get("premium_vid_coin", 50),
        "ad_links": p["ad_links"],
        "zone_ids": p["zone_ids"],
        "wait_times": config.get("direk_wait", [5])
    }

    return render_template_string(
        HTML_TEMPLATE,
        files_json=json.dumps(p["files"], default=str),
        categories_json=json.dumps(p["cats"]),
        languages_json=json.dumps(p["langs"]),
        promotions_json=json.dumps(p["promos"], default=str),
        upcoming_json=json.dumps(p["upcs"], default=str),
        zone_ids=p["zone_ids"],
        zone_ids_json=json.dumps(p["zone_ids"]),
        ad_config_json=json.dumps(ad_config),
        initial_user_json=json.dumps(initial_user),
        pkgs=p["pkgs"],
        bot_username=BOT_USERNAME,
        admin_username=config.get("payment_admin", DEFAULT_ADMIN_USERNAME),
        site_name=sn,
        logo_ver=logo_ver,
        site_name_hash_json=json.dumps(site_name_hash),
        rec_group_link_json=json.dumps(config.get("rec_group_link", "")),
        has_tutorial_json=json.dumps(bool(tut_vid)),
        tutorial_ver_json=json.dumps(tut_ver),
        marquee_text=config.get("marquee_text", "VLC Player অথবা MX Player দিয়ে চালিয়ে দেখুন। ✅ সাউন্ড/অডিও সমস্যা হলে ভিডিওটি ডাউনলোড করে দেখুন।"),
        ref_coin=config.get("ref_coin", 10)
    )

app_flask = web

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    print(f"🌐 Starting Flask Web on port {port} | WEB_URL={WEB_URL}", flush=True)
    web.run(host="0.0.0.0", port=port, debug=False, threaded=True, use_reloader=False)

async def main_bot():
    global BOT_USERNAME
    if not BOT_TOKEN:
        print("❌ ERROR: BOT_TOKEN environment variable is NOT set! Bot skipped.", flush=True)
        while True: await asyncio.sleep(3600)
    print("⏳ Connecting to MongoDB & Telegram...", flush=True)
    await get_db()
    conf = await get_config()
    live_url = get_live_web_url(conf)
    await app.start()
    BOT_USERNAME = app.me.username
    print(f"✅ Bot Started Successfully! (@{BOT_USERNAME})", flush=True)
    if conf.get("tutorial_vid") and not str(conf["tutorial_vid"]).startswith("http"):
        threading.Thread(target=ensure_tutorial_cached_sync, args=(conf["tutorial_vid"],), daemon=True).start()
    try:
        commands = [
            BotCommand("start", "বট স্টার্ট করুন"),
            BotCommand("cmd", "সকল কমান্ড ও উদাহরণ (Admin)"),
            BotCommand("stats", "বট স্ট্যাটিস্টিকস (Admin)"),
            BotCommand("movies", "নতুন মুভি এড (Admin)"),
            BotCommand("editemovies", "মুভি এডিট (Admin)"),
            BotCommand("deletedmovies", "মুভি ডিলিট (Admin)"),
            BotCommand("addseries", "নতুন ওয়েব সিরিজ এড (Admin)"),
            BotCommand("editeseries", "ওয়েব সিরিজ এডিট (Admin)"),
            BotCommand("deleteseries", "সিরিজ ডিলিট (Admin)"),
            BotCommand("addupcoming", "আপকামিং শিডিউল এড (Admin)"),
            BotCommand("delupcoming", "আপকামিং ডিলিট (Admin)"),
            BotCommand("setcategory", "ক্যাটাগরি এড (Admin)"),
            BotCommand("deletecategory", "ক্যাটাগরি ডিলিট (Admin)"),
            BotCommand("setlanguage", "ভাষা এড (Admin)"),
            BotCommand("deletelanguage", "ভাষা ডিলিট (Admin)"),
            BotCommand("setrecgroup", "Recommendation গ্রুপ সেট (Admin)"),
            BotCommand("delrecgroup", "Recommendation গ্রুপ রিমুভ (Admin)"),
            BotCommand("addzoonid", "Monetag Zone ID এড (Admin)"),
            BotCommand("deletezoonid", "Zone ID ডিলিট (Admin)"),
            BotCommand("addredeem", "রিডিম কোড তৈরি (Admin)"),
            BotCommand("premiumuserlist", "প্রিমিয়াম ইউজার লিস্ট (Admin)"),
            BotCommand("deletepremium", "প্রিমিয়াম রিমুভ (Admin)"),
            BotCommand("settutorial", "টিউটোরিয়াল ভিডিও সেট (Admin)"),
            BotCommand("setnotice", "স্ক্রলিং নোটিশ সেট (Admin)"),
            BotCommand("cancel", "চলমান প্রসেস বাতিল (Admin)")
        ]
        await app.set_bot_commands(commands)
        await app.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Watch Now 🎬", web_app=WebAppInfo(url=live_url)))
        print(f"✅ Menu WebApp URL set to: {live_url}", flush=True)
    except Exception as e:
        print(f"⚠️ Menu/Commands Setup Error: {e}", flush=True)
    asyncio.create_task(background_tasks())
    await idle()
    await app.stop()

if __name__ == "__main__":
    try:
        instance_lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        instance_lock.bind(("127.0.0.1", 9876))
    except socket.error:
        print("⚠️ বট আগে থেকেই রান হচ্ছে!", flush=True)
        sys.exit(1)

    if BOT_TOKEN:
        try: requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True", timeout=5)
        except: pass

    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=run_flask, daemon=True).start()
    while True:
        try: loop.run_until_complete(main_bot())
        except FloodWait as e: time.sleep(e.value)
        except Exception as e:
            print(f"❌ Bot Error: {e}", flush=True)
            traceback.print_exc()
            time.sleep(5)
