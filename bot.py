import os, sys, re, asyncio, threading, random, string, time, requests, json, base64, socket, io
from datetime import datetime, timedelta

try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from pyrogram import Client, filters, idle, enums
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo, MenuButtonWebApp, BotCommand
from pyrogram.errors import UserNotParticipant, FloodWait
from flask import Flask, render_template_string, jsonify, request
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient
from bson.objectid import ObjectId

# ==========================================
# Anti-Duplicate Filter
# ==========================================
processed_msgs, processed_cbs = set(), set()

def is_unique_func(_, __, m):
    if not hasattr(m, "id"): return True
    if m.id in processed_msgs: return False
    processed_msgs.add(m.id)
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
# 1. CONFIGURATION
# ==========================================
API_ID = int(os.environ.get("API_ID", 29904834))
API_HASH = os.environ.get("API_HASH", "8b4fd9ef578af114502feeafa2d31938")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://drama:drama@cluster0.sa4kvgu.mongodb.net/?appName=Cluster0")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 7120801813))
WEB_URL = os.environ.get("WEB_URL", "https://flixbox4ubot.onrender.com").rstrip("/")

DEFAULT_ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "Sudo_king")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "FlixBox4uBot")

# ==========================================
# 2. DATABASE & SYSTEM INIT (NewBot Schema Preserved)
# ==========================================
app = Client("shilacall_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
web = Flask(__name__)
admin_steps = {}

db_client, db = None, None
users_col, files_col, cats_col, tasks_col, titles_col = None, None, None, None, None
pkgs_col, links_col, config_col, channels_col, coupons_col, mongos_col = None, None, None, None, None, None
auto_channels_col, promotions_col, zone_ids_col = None, None, None

sync_db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)["NewBot"]

async def get_db():
    global db_client, db, users_col, files_col, cats_col, pkgs_col, links_col, config_col
    global channels_col, coupons_col, mongos_col, tasks_col, titles_col, auto_channels_col, promotions_col, zone_ids_col
    if db_client is None:
        db_client = AsyncIOMotorClient(MONGO_URI)
        db = db_client["NewBot"]
        users_col, files_col, cats_col = db["users"], db["files"], db["categories"]
        pkgs_col, links_col, config_col = db["packages"], db["ad_links"], db["config"]
        channels_col, coupons_col, mongos_col = db["channels"], db["coupons"], db["mongos"]
        tasks_col, titles_col = db["custom_tasks"], db["auto_titles"]
        auto_channels_col, promotions_col, zone_ids_col = db["auto_channels"], db["promotions"], db["zone_ids"]
    return db

async def get_config():
    await get_db()
    conf = await config_col.find_one({"_id": "settings"})
    if not conf:
        conf = {
            "_id": "settings", "ref_coin": 10, "ref_on": True, "auto_del_time": 0,
            "autodel_text": "⏳ ফাইলটি নির্দিষ্ট সময় পর অটো ডিলিট হয়ে যাবে।",
            "frotect": False, "ads_on": True, "direk_wait": [5], "prem_vid_wait": 10,
            "prstep": 1, "regstep": 1, "start_logo": None, "start_text": "",
            "payment_admin": DEFAULT_ADMIN_USERNAME, "autovid_msg_id": None, "autovid_chat_id": None,
            "autovid_time_min": 0, "autopost_time_hr": 0, "autopost_idx": 0,
            "site_name": "Moviee Link", "premium_vid_coin": 50, "vid_relock_min": 1440,
            "spin_coin_min": 10, "spin_coin_max": 50, "task_coin": 20, "spin_limit": 5, "task_limit": 5,
            "autopost_channel": None, "autopost_minute": 0, "autopost_channel_idx": 0,
            "tutorial_vid": None,
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
    y, sec = divmod(sec, 31536000); mo, sec = divmod(sec, 2592000)
    w, sec = divmod(sec, 604800); d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600); m, sec = divmod(sec, 60)
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
            asyncio.run_coroutine_threadsafe(app.send_message(chat_id, text), loop)
            return
    except: pass
    if BOT_TOKEN:
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                          json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=5)
        except: pass

def keep_alive():
    while True:
        try: time.sleep(60); requests.get(WEB_URL, timeout=10)
        except: pass

# ==========================================
# 2.5 MULTI-MONGODB MANAGER
# ==========================================
async_mongo_clients, sync_mongo_clients = {}, {}

async def get_extra_dbs_async():
    await get_db()
    dbs = [db]
    async for m in mongos_col.find():
        uri = m["uri"]
        if uri not in async_mongo_clients:
            try: async_mongo_clients[uri] = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=2000)["NewBot"]
            except: continue
        dbs.append(async_mongo_clients[uri])
    return dbs

def get_extra_dbs_sync():
    dbs = [sync_db]
    try:
        for m in sync_db["mongos"].find():
            uri = m["uri"]
            if uri not in sync_mongo_clients:
                try: sync_mongo_clients[uri] = MongoClient(uri, serverSelectionTimeoutMS=2000)["NewBot"]
                except: continue
            dbs.append(sync_mongo_clients[uri])
    except: pass
    return dbs

async def find_file_in_all_dbs(f_id):
    for d in await get_extra_dbs_async():
        doc = await d["files"].find_one({"_id": f_id})
        if doc: return doc, d
    return None, None

async def get_all_files_across_dbs(query_filter=None):
    all_files = []
    q = query_filter or {}
    for d in await get_extra_dbs_async():
        try:
            docs = await d["files"].find(q).sort("_id", -1).to_list(None)
            all_files.extend(docs)
        except: pass
    return all_files

def get_tg_photo(thumb):
    if not thumb: return "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    if thumb.startswith("data:image"):
        try:
            img = io.BytesIO(base64.b64decode(thumb.split(",")[1]))
            img.name = "thumb.jpg"
            return img
        except: return "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    return thumb

async def notify_new_post(client, file_doc, config):
    ch_id = config.get("autopost_channel")
    btn = [InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]
    if ch_id:
        try:
            await client.send_photo(int(ch_id), photo=get_tg_photo(file_doc.get("thumb_url")),
                                    caption=f"🔥 **New Trending Upload!**\n\n🎬 **{file_doc['title']}**\n\n👇 নিচের বাটনে ক্লিক করে এখনই দেখুন!",
                                    reply_markup=InlineKeyboardMarkup([btn]))
        except Exception as e: print(f"Channel Autopost Error: {e}")

    await get_db()
    async for ch in auto_channels_col.find():
        try:
            msg = await client.send_photo(int(ch["chat_id"]), photo=get_tg_photo(file_doc.get("thumb_url")),
                                          caption=f"🔥 **New Trending Upload!**\n\n🎬 **{file_doc['title']}**\n\n👇 নিচের বাটনে ক্লিক করে এখনই দেখুন!",
                                          reply_markup=InlineKeyboardMarkup([btn]))
            if ch.get("delete_min", 0) > 0:
                asyncio.create_task(delete_msg_later(client, int(ch["chat_id"]), msg.id, ch["delete_min"] * 60))
        except Exception as e: print(f"Auto Channel Error: {e}")

# ==========================================
# 3. BACKGROUND TASKS
# ==========================================
async def background_tasks():
    last_autovid, last_autopost, last_channel_post = time.time(), time.time(), time.time()
    while True:
        await asyncio.sleep(60)
        now = time.time()
        try:
            config = await get_config()
            av_min = config.get("autovid_time_min", 0)
            if av_min > 0 and config.get("autovid_msg_id") and (now - last_autovid) >= (av_min * 60):
                last_autovid = time.time()
                from_chat = config.get("autovid_chat_id", ADMIN_ID)
                async for u in users_col.find():
                    if u.get("last_autovid_msg"):
                        try: await app.delete_messages(u["_id"], u["last_autovid_msg"])
                        except: pass
                    try:
                        msg = await app.copy_message(u["_id"], from_chat, config["autovid_msg_id"])
                        await users_col.update_one({"_id": u["_id"]}, {"$set": {"last_autovid_msg": msg.id}})
                        await asyncio.sleep(0.05)
                    except: pass

            ap_hr = config.get("autopost_time_hr", 0)
            if ap_hr > 0 and (now - last_autopost) >= (ap_hr * 3600):
                last_autopost = time.time()
                all_files = await get_all_files_across_dbs()
                if all_files:
                    idx = config.get("autopost_idx", 0)
                    if idx >= len(all_files): idx = 0
                    f = all_files[idx]
                    async for u in users_col.find():
                        try:
                            btn = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", web_app=WebAppInfo(url=f"{WEB_URL}/?uid={u['_id']}"))]])
                            await app.send_photo(u["_id"], photo=get_tg_photo(f.get("thumb_url")), caption=f"🔥 **New Video!**\n\nTitle: {f['title']}", reply_markup=btn)
                            await asyncio.sleep(0.05)
                        except: pass
                    await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_idx": idx + 1}})

            ch_id, ch_min = config.get("autopost_channel"), config.get("autopost_minute", 0)
            if ch_id and ch_min > 0 and (now - last_channel_post) >= (ch_min * 60):
                last_channel_post = time.time()
                all_files = await get_all_files_across_dbs()
                if all_files:
                    c_idx = config.get("autopost_channel_idx", 0)
                    if c_idx >= len(all_files): c_idx = 0
                    f = all_files[c_idx]
                    btn = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]])
                    try:
                        await app.send_photo(int(ch_id), photo=get_tg_photo(f.get("thumb_url")), caption=f"🔥 **New Trending Video!**\n\n🎬 **{f['title']}**", reply_markup=btn)
                        await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_channel_idx": c_idx + 1}})
                    except Exception as e: print("Ch Post Err:", e)

            async for ch in auto_channels_col.find():
                c_id, c_int = ch.get("chat_id"), ch.get("interval", 0)
                if not c_id or c_int <= 0: continue
                if (now - ch.get("last_post_time", 0)) >= (c_int * 60):
                    all_fs = await get_all_files_across_dbs()
                    if all_fs:
                        c_idx = ch.get("post_idx", 0)
                        if c_idx >= len(all_fs): c_idx = 0
                        f = all_fs[c_idx]
                        btn = InlineKeyboardMarkup([[InlineKeyboardButton("🍿 WATCH NOW", url=f"https://t.me/{BOT_USERNAME}?start=home")]])
                        try:
                            msg = await app.send_photo(int(c_id), photo=get_tg_photo(f.get("thumb_url")), caption=f"🔥 **New Trending Video!**\n\n🎬 **{f['title']}**", reply_markup=btn)
                            await auto_channels_col.update_one({"_id": ch["_id"]}, {"$set": {"last_post_time": time.time(), "post_idx": c_idx + 1}})
                            if ch.get("delete_min", 0) > 0:
                                asyncio.create_task(delete_msg_later(app, int(c_id), msg.id, ch["delete_min"] * 60))
                        except Exception as e: print("AutoCh Err:", e)
        except Exception as e: print("BG Task Error:", e)

# ==========================================
# 4. HELPER BUILDERS
# ==========================================
LANGUAGES = ["Bangla", "Hindi", "Hindi Dubbed", "English", "Dual Audio", "Tamil", "Korean", "Other"]
QUALITIES = ["360p", "480p", "720p", "1080p", "4K", "HDRip"]

async def get_categories_list():
    await get_db()
    cats = await cats_col.find().to_list(100)
    if not cats: return ["Movie", "Web Series", "18+", "Bachelor Point", "Natok", "Anime"]
    return [c["name"] for c in cats]

def parse_name_year(raw_text, cmd_prefix):
    txt = raw_text.strip()
    if cmd_prefix and txt.lower().startswith(cmd_prefix.lower()):
        txt = txt[len(cmd_prefix):].strip()
    if txt.upper().startswith("NAME YEAR "): txt = txt[10:].strip()
    elif txt.upper().startswith("NAME "): txt = txt[5:].strip()
    if not txt: return "", ""
    m = re.match(r"^(.*?)\s+(\d{4})$", txt)
    return (m.group(1).strip(), m.group(2).strip()) if m else (txt, "")

async def get_cat_markup(prefix):
    cats = await get_categories_list()
    btns, row = [], []
    for idx, c_name in enumerate(cats):
        row.append(InlineKeyboardButton(f"📂 {c_name}", callback_data=f"{prefix}_{idx}"))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    return InlineKeyboardMarkup(btns), cats

def get_lang_markup(prefix):
    btns, row = [], []
    for idx, l_name in enumerate(LANGUAGES):
        row.append(InlineKeyboardButton(f"🌐 {l_name}", callback_data=f"{prefix}_{idx}"))
        if len(row) == 2: btns.append(row); row = []
    if row: btns.append(row)
    return InlineKeyboardMarkup(btns)

def get_quality_markup(prefix, skip_cb):
    btns, row = [], []
    for q in QUALITIES:
        row.append(InlineKeyboardButton(f"🎬 {q}", callback_data=f"{prefix}_{q}"))
        if len(row) == 3: btns.append(row); row = []
    if row: btns.append(row)
    btns.append([InlineKeyboardButton("➕ Custom Quality", callback_data=f"{prefix}_custom")])
    btns.append([InlineKeyboardButton("⏭ Skip / Finish", callback_data=skip_cb)])
    return InlineKeyboardMarkup(btns)

def is_in_step(step_name):
    return filters.create(lambda _, __, m: admin_steps.get(m.from_user.id, {}).get("step") == step_name)

async def deliver_cached_file(client, uid, file_id_to_send, caption_title, config):
    msg = await client.send_message(uid, "⏳ আপনার ফাইল পাঠানো হচ্ছে... / Sending your file...")
    try:
        del_time = config.get("auto_del_time", 0)
        caption = f"🎬 **{caption_title}**"
        if del_time > 0:
            mins, secs = divmod(del_time, 60)
            time_str = f"{mins} মিনিট {secs} সেকেন্ড" if mins > 0 else f"{secs} সেকেন্ড"
            caption += f"\n\n⏳ **ভিডিওটি {time_str} পর অটো ডিলিট হয়ে যাবে!**"
        sent_msg = await client.send_cached_media(chat_id=uid, file_id=file_id_to_send, caption=caption, protect_content=config.get("frotect", False))
        await msg.delete()
        if del_time > 0:
            warn = await client.send_message(uid, f"⚠️ {config.get('autodel_text')}")
            asyncio.create_task(delete_msg_later(client, uid, sent_msg.id, del_time))
            asyncio.create_task(delete_msg_later(client, uid, warn.id, del_time))
    except Exception as e:
        await msg.edit_text(f"❌ ফাইল পাঠাতে সমস্যা হয়েছে! {e}")

# ==========================================
# 5. BOT COMMANDS (START + BASIC)
# ==========================================
@app.on_message(filters.command("cancel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_cancel(c, m):
    admin_steps.pop(m.from_user.id, None)
    await m.reply("✅ চলমান প্রসেস বাতিল করা হয়েছে!")

@app.on_message(filters.command("myid") & unique_msg)
async def cmd_myid(c, m):
    await m.reply(f"✅ বট কাজ করছে!\n🆔 আপনার আইডি: `{m.from_user.id}`")

@app.on_message(filters.command("cmd") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_list(c, m):
    await m.reply("""🛠 **সকল কমান্ড লিস্ট:**

🎬 **Movies & Series:**
/movies NAME RRR 2024 - নতুন মুভি এড
/editemovies Name Year RRR 2024 - মুভি ও কোয়ালিটি এডিট
/deletedMovies - মুভি ডিলিট
/addseries Batchlor point 2026 - নতুন সিরিজ এড
/editeseries Batchlor point 2026 - সিরিজ এডিট
/deleteseries - সিরিজ ডিলিট
/setcategory - ক্যাটাগরি এড
/deletecategory - ক্যাটাগরি ডিলিট
/cancel - প্রসেস বাতিল

💰 **Monetag, Redeem & Premium:**
/addzoonid - Zone ID এড
/deletezoonid - Zone ID ডিলিট
/addredeem 1day 10 code - রিডিম কোড
/Premiumuserlist - প্রিমিয়াম লিস্ট
/deletepremium userid - প্রিমিয়াম রিমুভ
/settutorial - টিউটোরিয়াল ভিডিও সেট
/setnotice - স্ক্রলিং নোটিশ সেট

🔸 **Other Commands:**
/start, /stats, /name, /addfile, /vidtitel, /deltitel, /auto, /auto2, /promot, /delpromot, /delfile, /delall, /mongo, /delmongo, /mongostats, /spinlimit, /tasklimit, /addtask, /deltask, /setpremcoin, /spincoin, /taskcoin, /relocktime, /delrelocktime, /lockall, /addlink, /delink, /prstep, /regstep, /addcnl, /vercnl, /delcnl, /delvrcnl, /brodcast, /cnlbdcst, /autochannel, /delautochannel, /allred, /addcred, /bdt, /usd, /delcred, /autodel, /frotect, /addadmin, /addtex, /deltex, /logo, /autvid, /autvidti, /autpost, /refbonous, /refbonousoff""")

@app.on_message(filters.command("stats") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_stats(c, m):
    await get_db()
    t_u = await users_col.count_documents({})
    p_u = await users_col.count_documents({"premium_until": {"$gt": datetime.now()}})
    dbs = await get_extra_dbs_async()
    t_f = sum([await d["files"].count_documents({}) for d in dbs])
    await m.reply(f"📊 **Statistics:**\n\n👥 Users: {t_u}\n🎬 Total Files: {t_f}\n💎 Premium: {p_u}\n👤 Regular: {t_u - p_u}\n🗄 MongoDBs: {len(dbs)}")

@app.on_message(filters.command("name") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_name(c, m):
    new_name = m.text.replace("/name", "").strip()
    if not new_name: return await m.reply("❌ নিয়ম: `/name Moviee Link`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"site_name": new_name}})
    await m.reply(f"✅ সাইটের নাম পরিবর্তন হয়েছে: **{new_name}**")

@app.on_message(filters.command("settutorial") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_settutorial(c, m):
    await get_db()
    if m.reply_to_message and (m.reply_to_message.video or m.reply_to_message.animation):
        vid_id = (m.reply_to_message.video or m.reply_to_message.animation).file_id
        await config_col.update_one({"_id": "settings"}, {"$set": {"tutorial_vid": vid_id}})
        await m.reply("✅ টিউটোরিয়াল ভিডিও সেট করা হয়েছে!")
    else: await m.reply("❌ কোনো ভিডিওতে রিপ্লাই করে `/settutorial` দিন।")

@app.on_message(filters.command("setnotice") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setnotice(c, m):
    tex = m.text.replace("/setnotice", "").strip()
    if not tex: return await m.reply("❌ নিয়ম: `/setnotice <Text>`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"marquee_text": tex}})
    await m.reply("✅ লাইভ স্ক্রলিং নোটিশ সেট করা হয়েছে!")

@app.on_message(filters.command("start") & filters.private & unique_msg)
async def start_cmd(client, message):
    await get_db()
    uid = message.from_user.id
    args = message.text.split()
    config = await get_config()

    ref_by, pf = None, None
    if len(args) > 1:
        val = args[1]
        if val.isdigit(): ref_by = int(val)
        elif val.startswith("file_") or val.startswith("mfile_") or val.startswith("sfile_"): pf = val
        elif val.startswith("ref") and "file" in val:
            try:
                parts = val.replace("ref", "").split("file")
                ref_by, pf = int(parts[0]), "file_" + parts[1]
            except: pass

    user = await users_col.find_one({"_id": uid})
    if not user:
        await users_col.insert_one({
            "_id": uid, "name": message.from_user.first_name, "balance": 0,
            "pending_file": None, "premium_until": None, "history": [], "favorites": [],
            "unlocked_files": {}, "spin_count": 0, "task_count": 0,
            "last_activity_date": None, "custom_tasks_done": []
        })
        if ref_by and ref_by != uid and config.get("ref_on", True):
            await users_col.update_one({"_id": ref_by}, {"$inc": {"balance": config.get("ref_coin", 10)}})
            try: await client.send_message(ref_by, f"🎉 রেফার বোনাস! +{config.get('ref_coin', 10)} Coins")
            except: pass
        user = await users_col.find_one({"_id": uid})

    if pf:
        await users_col.update_one({"_id": uid}, {"$set": {"pending_file": pf}})
        user["pending_file"] = pf

    not_joined = []
    for ch in await channels_col.find({"type": "must_join"}).to_list(100):
        try: await client.get_chat_member(ch["chat_id"], uid)
        except UserNotParticipant: not_joined.append(ch["link"])
        except: pass

    if not_joined:
        btns = [[InlineKeyboardButton("📢 Join Channel", url=link)] for link in not_joined]
        btns.append([InlineKeyboardButton("✅ Joined", callback_data="check_join")])
        return await message.reply("❌ আগে আমাদের চ্যানেলগুলোতে জয়েন করুন:", reply_markup=InlineKeyboardMarkup(btns))

    if user.get("pending_file"):
        pf_val = user["pending_file"]
        await users_col.update_one({"_id": uid}, {"$set": {"pending_file": None}})
        if pf_val.startswith("mfile_"):
            _, f_id, q_idx = pf_val.split("_")
            file_data, target_db = await find_file_in_all_dbs(f_id)
            if file_data:
                await target_db["files"].update_one({"_id": f_id}, {"$inc": {"views": 1}})
                quals = file_data.get("qualities", [])
                qi = int(q_idx)
                if 0 <= qi < len(quals):
                    return await deliver_cached_file(client, uid, quals[qi]["file_id"], f"{file_data['title']} [{quals[qi]['name']}]", config)
                elif file_data.get("file_id"):
                    return await deliver_cached_file(client, uid, file_data["file_id"], file_data["title"], config)
            return await message.reply("❌ ফাইলটি পাওয়া যায়নি!")
        elif pf_val.startswith("sfile_"):
            _, f_id, s_idx, e_idx, q_idx = pf_val.split("_")
            file_data, target_db = await find_file_in_all_dbs(f_id)
            if file_data:
                await target_db["files"].update_one({"_id": f_id}, {"$inc": {"views": 1}})
                try:
                    ssn = file_data["seasons"][int(s_idx)]
                    ep = ssn["episodes"][int(e_idx)]
                    q_obj = ep["qualities"][int(q_idx)]
                    return await deliver_cached_file(client, uid, q_obj["file_id"], f"{file_data['title']} - S{ssn['season_no']} E{ep['ep_no']} [{q_obj['name']}]", config)
                except: pass
            return await message.reply("❌ এপিসোড ফাইলটি পাওয়া যায়নি!")
        else:
            f_id = pf_val.replace("file_", "")
            file_data, target_db = await find_file_in_all_dbs(f_id)
            if file_data:
                await target_db["files"].update_one({"_id": f_id}, {"$inc": {"views": 1}})
                f_send = file_data.get("file_id") or (file_data["qualities"][0]["file_id"] if file_data.get("qualities") else None)
                if f_send: return await deliver_cached_file(client, uid, f_send, file_data["title"], config)
            return await message.reply("❌ ফাইলটি পাওয়া যায়নি!")

    full = f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
    site_name = config.get("site_name", "Moviee Link")
    txt = f"👋 **হ্যালো {full}!**\n\n🎬 উপরের টিউটোরিয়াল ভিডিওটি অন্তত একবার দেখে নিন\n\n🍿 এখানে পাবেন **নিত্য নতুন মুভি ও সিরিজ** কালেকশন — দেরি না করে নিচের Watch Now বাটনে ক্লিক করো!"
    if config.get("start_text"): txt += f"\n\n📝 {config.get('start_text')}"

    inline_chs = await channels_col.find({"type": "inline"}).to_list(100)
    join_url = inline_chs[0]["link"] if inline_chs else f"https://t.me/{BOT_USERNAME}"
    share_url = f"https://t.me/share/url?url=https://t.me/{BOT_USERNAME}?start={uid}&text=Join {site_name}!"
    btns = [
        [InlineKeyboardButton("🍿 WATCH NOW", web_app=WebAppInfo(url=f"{WEB_URL}/?uid={uid}"))],
        [InlineKeyboardButton("🎁 Share Friend", url=share_url), InlineKeyboardButton("📡 Join All Channel", url=join_url)]
    ]
    for ch in inline_chs[1:]:
        if ch.get("name") and ch.get("link"): btns.append([InlineKeyboardButton(ch["name"], url=ch["link"])])

    if config.get("tutorial_vid"):
        try: return await client.send_video(uid, video=config["tutorial_vid"], caption=txt, reply_markup=InlineKeyboardMarkup(btns))
        except: pass
    if config.get("start_logo"):
        try: return await client.send_photo(uid, photo=config["start_logo"], caption=txt, reply_markup=InlineKeyboardMarkup(btns))
        except: pass
    await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

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
# 6. CATEGORY, ZONE ID, REDEEM, PREMIUM LIST
# ==========================================
@app.on_message(filters.command("setcategory", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setcategory(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2:
        admin_steps[m.from_user.id] = {"step": "add_cat_wait"}
        return await m.reply("📂 নতুন ক্যাটাগরির নাম লিখুন (যেমন: `Action` বা `18+` বা `Bachelor Point`):")
    cat = parts[1].strip()
    await get_db(); await cats_col.update_one({"name": cat}, {"$set": {"name": cat}}, upsert=True)
    await m.reply(f"✅ ক্যাটাগরি অ্যাড হয়েছে: **{cat}**")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("add_cat_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_add_cat_text(c, m):
    cat = m.text.strip()
    await get_db(); await cats_col.update_one({"name": cat}, {"$set": {"name": cat}}, upsert=True)
    admin_steps.pop(m.from_user.id, None)
    await m.reply(f"✅ ক্যাটাগরি অ্যাড হয়েছে: **{cat}**")

@app.on_message(filters.command("deletecategory", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletecategory(c, m):
    await get_db()
    cats = await cats_col.find().to_list(100)
    if not cats: return await m.reply("❌ কোনো ক্যাটাগরি সেভ করা নেই!")
    btns = [[InlineKeyboardButton(f"❌ {cat['name']}", callback_data=f"delcat_{cat['_id']}")] for cat in cats]
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delcat_") & filters.user(ADMIN_ID) & unique_cb)
async def delcat_cb(c, q):
    await get_db(); await cats_col.delete_one({"_id": ObjectId(q.data.split("_")[1])})
    await q.message.edit_text("✅ ক্যাটাগরি ডিলিট করা হয়েছে!")

@app.on_message(filters.command("addzoonid", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addzoonid(c, m):
    parts = m.text.split(None, 1)
    if len(parts) < 2:
        admin_steps[m.from_user.id] = {"step": "add_zone_wait"}
        return await m.reply("🆔 আপনার Monetag Zone ID দিন (যেমন: `12242829`):")
    zid = parts[1].strip()
    await get_db(); await zone_ids_col.update_one({"zone_id": zid}, {"$set": {"zone_id": zid}}, upsert=True)
    await m.reply(f"✅ Zone ID অ্যাড হয়েছে: `{zid}`")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("add_zone_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_add_zone_text(c, m):
    zid = m.text.strip()
    await get_db(); await zone_ids_col.update_one({"zone_id": zid}, {"$set": {"zone_id": zid}}, upsert=True)
    admin_steps.pop(m.from_user.id, None)
    await m.reply(f"✅ Zone ID অ্যাড হয়েছে: `{zid}`")

@app.on_message(filters.command("deletezoonid", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletezoonid(c, m):
    await get_db()
    zids = await zone_ids_col.find().to_list(100)
    if not zids: return await m.reply("❌ কোনো Zone ID নেই!")
    btns = [[InlineKeyboardButton(f"❌ Zone: {z['zone_id']}", callback_data=f"delzone_{z['_id']}")] for z in zids]
    await m.reply("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delzone_") & filters.user(ADMIN_ID) & unique_cb)
async def delzone_cb(c, q):
    await get_db(); await zone_ids_col.delete_one({"_id": ObjectId(q.data.split("_")[1])})
    await q.message.edit_text("✅ Zone ID ডিলিট করা হয়েছে!")

def parse_duration_seconds(dur_str):
    m = re.match(r"^(\d+)\s*([a-zA-Z]+)$", dur_str.strip().lower())
    if not m: return None
    val, unit = int(m.group(1)), m.group(2)
    if unit.startswith("s"): return val
    if unit.startswith("mi") or unit == "m": return val * 60
    if unit.startswith("h"): return val * 3600
    if unit.startswith("d"): return val * 86400
    if unit.startswith("w"): return val * 604800
    if unit.startswith("mo"): return val * 2592000
    if unit.startswith("y"): return val * 31536000
    return None

@app.on_message(filters.command("addredeem", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addredeem(c, m):
    args = m.text.split()
    if len(args) < 4:
        return await m.reply("❌ নিয়ম: `/addredeem <Duration/Coins> <Limit> <Code>`\nউদাহরণ: `/addredeem 1day 10 code`")
    dur_or_coin, lim_str, code = args[1], args[2], args[3]
    try: lim = int(lim_str)
    except: return await m.reply("❌ Limit সংখ্যা হতে হবে!")
    await get_db()
    secs = parse_duration_seconds(dur_or_coin)
    if secs:
        await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_premium_code": True, "duration_seconds": secs, "duration_label": dur_or_coin})
        await m.reply(f"✅ **Premium Redeem Code Created!**\n🎟 Code: `{code}`\n⏳ Duration: `{dur_or_coin}`\n👥 Limit: `{lim}`")
    elif dur_or_coin.isdigit():
        await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_random": False, "coins": int(dur_or_coin)})
        await m.reply(f"✅ **Coin Redeem Code Created!**\n🎟 Code: `{code}`\n🪙 Coins: `{dur_or_coin}`\n👥 Limit: `{lim}`")
    else:
        await m.reply("❌ Duration সঠিক নয়! উদাহরণ: `1day`, `7days`, `1month`, `12h`")

@app.on_message(filters.command("premiumuserlist", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_premiumuserlist(c, m):
    await get_db()
    prem_users = await users_col.find({"premium_until": {"$gt": datetime.now()}}).to_list(None)
    if not prem_users: return await m.reply("👤 বর্তমানে কোনো Premium User নেই।")
    txt = f"💎 **Premium Users ({len(prem_users)}):**\n\n"
    btns = []
    for u in prem_users[:30]:
        exp = get_expiry_str(u.get("premium_until")) or "Active"
        txt += f"• **{u.get('name', 'User')}** (`{u['_id']}`) — ⏳ {exp}\n"
        btns.append([InlineKeyboardButton(f"🗑 Remove {u.get('name', 'User')} ({u['_id']})", callback_data=f"delpremu_{u['_id']}")])
    await m.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delpremu_") & filters.user(ADMIN_ID) & unique_cb)
async def delpremu_cb(c, q):
    uid = int(q.data.split("_")[1])
    await get_db(); await users_col.update_one({"_id": uid}, {"$set": {"premium_until": None}})
    await q.answer("✅ Premium Removed!", show_alert=True)
    await q.message.edit_text(f"✅ User `{uid}` এর প্রিমিয়াম রিমুভ করা হয়েছে!")

@app.on_message(filters.command(["deletepremium", "delpremium"], case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deletepremium(c, m):
    args = m.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return await m.reply("❌ নিয়ম: `/deletepremium <userid>`")
    uid = int(args[1])
    await get_db(); await users_col.update_one({"_id": uid}, {"$set": {"premium_until": None}})
    await m.reply(f"✅ User `{uid}` এর প্রিমিয়াম রিমুভ করা হয়েছে!")

# ==========================================
# 7. MOVIES WORKFLOW (/movies, /editemovies, /deletedMovies)
# ==========================================
@app.on_message(filters.command("movies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_movies(c, m):
    name, year = parse_name_year(m.text, "/movies")
    if not name:
        admin_steps[m.from_user.id] = {"step": "mv_name_year"}
        return await m.reply("🎬 **মুভির নাম এবং সাল দিন:** (যেমন: `RRR 2024`)")
    admin_steps[m.from_user.id] = {"step": "mv_cat", "name": name, "year": year, "qualities": [], "is_premium": False, "thumb_url": None}
    markup, _ = await get_cat_markup("mvcat")
    await m.reply(f"🎬 মুভি: **{name} {year}**\n\n১. মুভির **Category** সিলেক্ট করুন:", reply_markup=markup)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_name_year") & ~filters.regex(r"^/") & unique_msg)
async def handle_mv_name_year(c, m):
    name, year = parse_name_year(m.text, "")
    admin_steps[m.from_user.id] = {"step": "mv_cat", "name": name, "year": year, "qualities": [], "is_premium": False, "thumb_url": None}
    markup, _ = await get_cat_markup("mvcat")
    await m.reply(f"🎬 মুভি: **{name} {year}**\n\n১. মুভির **Category** সিলেক্ট করুন:", reply_markup=markup)

@app.on_callback_query(filters.regex(r"^mvcat_") & filters.user(ADMIN_ID) & unique_cb)
async def mvcat_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    cats = await get_categories_list()
    st["category"] = cats[int(q.data.split("_")[1])]
    st["step"] = "mv_lang"
    await q.message.edit_text(f"✅ Category: **{st['category']}**\n\n২. এবার মুভির **Language** সিলেক্ট করুন:", reply_markup=get_lang_markup("mvlang"))

@app.on_callback_query(filters.regex(r"^mvlang_") & filters.user(ADMIN_ID) & unique_cb)
async def mvlang_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    st["language"] = LANGUAGES[int(q.data.split("_")[1])]
    st["step"] = "mv_qual_wait"
    await q.message.edit_text(f"✅ Language: **{st['language']}**\n\n৩. এবার আনলিমিটেড **Quality** অ্যাড করুন (বা Skip করুন):",
                              reply_markup=get_quality_markup("mvqual", "mvsave_ask"))

@app.on_callback_query(filters.regex(r"^mvqual_") & filters.user(ADMIN_ID) & unique_cb)
async def mvqual_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_", 1)[1]
    if val == "custom":
        st["step"] = "mv_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন (যেমন: `1080p HEVC`):")
    st["current_quality"] = val
    st["step"] = "mv_qual_file"
    await q.message.edit_text(f"📤 এবার **{val}** কোয়ালিটির ভিডিও বা ডকুমেন্ট ফাইল পাঠান:")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_mv_custom_qname(c, m):
    st = admin_steps[m.from_user.id]
    st["current_quality"] = m.text.strip()
    st["step"] = "mv_qual_file"
    await m.reply(f"📤 এবার **{st['current_quality']}** কোয়ালিটির ফাইল পাঠান:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_qual_file") & unique_msg)
async def handle_mv_qual_file(c, m):
    st = admin_steps[m.from_user.id]
    f_id = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    q_name = st.get("current_quality", "HD")
    st["qualities"].append({"name": q_name, "file_id": f_id})
    st["step"] = "mv_qual_wait"
    added = ", ".join([q["name"] for q in st["qualities"]])
    await m.reply(f"✅ **{q_name}** অ্যাড হয়েছে!\n📦 কোয়ালিটি: `{added}`\n\nআরও কোয়ালিটি অ্যাড করুন বা Skip দিন:",
                  reply_markup=get_quality_markup("mvqual", "mvsave_ask"))

@app.on_callback_query(filters.regex(r"^mvsave_ask$") & filters.user(ADMIN_ID) & unique_cb)
async def mvsave_ask_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    st["step"] = "mv_poster"
    p_status = "💎 Premium" if st.get("is_premium") else "👤 Regular"
    btns = [[InlineKeyboardButton("💎 Premium / Regular টগল", callback_data="mvprem_toggle")],
            [InlineKeyboardButton("⏭ Skip Poster & Save Movie", callback_data="mvsave_final")]]
    await q.message.edit_text(f"🖼 **মুভির পোস্টার ছবি দিন** (অথবা Skip করুন)।\n🔰 টাইপ: **{p_status}**", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^mvprem_toggle$") & filters.user(ADMIN_ID) & unique_cb)
async def mvprem_toggle_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return
    st["is_premium"] = not st.get("is_premium", False)
    p_status = "💎 Premium" if st["is_premium"] else "👤 Regular"
    btns = [[InlineKeyboardButton("💎 Premium / Regular টগল", callback_data="mvprem_toggle")],
            [InlineKeyboardButton("⏭ Skip Poster & Save Movie", callback_data="mvsave_final")]]
    await q.message.edit_text(f"🖼 **মুভির পোস্টার ছবি দিন** (অথবা Skip করুন)।\n🔰 টাইপ: **{p_status}**", reply_markup=InlineKeyboardMarkup(btns))

async def finalize_save_movie(c, uid, msg_obj):
    st = admin_steps.pop(uid, None)
    if not st: return
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    dbs = await get_extra_dbs_async()
    target_db, min_size = dbs[0], float('inf')
    for d in dbs:
        try:
            stat = await d.command("dbstats")
            if stat["dataSize"] < min_size: min_size = stat["dataSize"]; target_db = d
        except: pass
    full_title = f"{st['name']} ({st['year']})" if st.get("year") else st["name"]
    new_movie = {
        "_id": short_id, "type": "movie", "title": full_title, "name": st["name"], "year": st.get("year", ""),
        "category": st.get("category", "Movie"), "language": st.get("language", "Bangla"),
        "is_premium": st.get("is_premium", False),
        "file_id": st["qualities"][0]["file_id"] if st.get("qualities") else None,
        "qualities": st.get("qualities", []),
        "thumb_url": st.get("thumb_url") or "https://placehold.co/600x400/1c1c24/ff007f?text=Movie",
        "pinned": False, "ratings": [], "created_at": datetime.utcnow().isoformat() + "Z",
        "views": 0, "likes": [], "comments": []
    }
    await target_db["files"].insert_one(new_movie)
    await notify_new_post(c, new_movie, await get_config())
    await msg_obj.edit_text(f"✅ **মুভি সেভ হয়েছে!**\n🎬 **{full_title}**\n📂 `{new_movie['category']}` | 🌐 `{new_movie['language']}`\n📦 Qualities: `{len(new_movie['qualities'])}` | 🆔 `{short_id}`")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("mv_poster") & unique_msg)
async def handle_mv_poster(c, m):
    msg = await m.reply("⏳ পোস্টার সেভ হচ্ছে...")
    path = await m.download()
    try:
        with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg'] else "image/jpeg"
        admin_steps[m.from_user.id]["thumb_url"] = f"data:{mime};base64,{encoded}"
        os.remove(path)
    except Exception as e: print("Poster err:", e)
    await finalize_save_movie(c, m.from_user.id, msg)

@app.on_callback_query(filters.regex(r"^mvsave_final$") & filters.user(ADMIN_ID) & unique_cb)
async def mvsave_final_cb(c, q):
    await finalize_save_movie(c, q.from_user.id, q.message)

@app.on_message(filters.command("deletedmovies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deleted_movies(c, m):
    await send_movie_delete_page(m, 0, edit=False)

async def send_movie_delete_page(msg_or_q, page=0, edit=True):
    all_docs = await get_all_files_across_dbs({"type": {"$ne": "series"}})
    if not all_docs:
        txt = "❌ কোনো মুভি নেই!"
        return await (msg_or_q.message.edit_text(txt) if edit else msg_or_q.reply(txt))
    per_page = 10; start = page * per_page; chunk = all_docs[start:start + per_page]
    btns = [[InlineKeyboardButton(f"🗑 {d['title'][:32]} [{d['_id']}]", callback_data=f"delmv_{d['_id']}_{page}")] for d in chunk]
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"delmvpg_{page-1}"))
    if start + per_page < len(all_docs): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"delmvpg_{page+1}"))
    if nav: btns.append(nav)
    txt = f"🗑 **মুভি ডিলিট লিস্ট (Total: {len(all_docs)}):**"
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delmvpg_") & filters.user(ADMIN_ID) & unique_cb)
async def delmvpg_cb(c, q): await send_movie_delete_page(q, int(q.data.split("_")[1]), edit=True)

@app.on_callback_query(filters.regex(r"^delmv_") & filters.user(ADMIN_ID) & unique_cb)
async def delmv_cb(c, q):
    _, f_id, page = q.data.split("_")
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        await target_db["files"].delete_one({"_id": f_id})
        await q.answer(f"✅ {doc['title']} ডিলিট হয়েছে!", show_alert=True)
    await send_movie_delete_page(q, int(page), edit=True)

@app.on_message(filters.command("editemovies", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_edit_movies(c, m):
    name, year = parse_name_year(m.text, "/editemovies")
    all_movies = await get_all_files_across_dbs({"type": {"$ne": "series"}})
    if name:
        matches = [d for d in all_movies if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
        if len(matches) == 1: return await show_edit_movie_panel(m, matches[0]["_id"], edit=False)
        elif matches:
            btns = [[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edmvpick_{d['_id']}")] for d in matches[:15]]
            return await m.reply("🔍 ম্যাচ হওয়া মুভি:", reply_markup=InlineKeyboardMarkup(btns))
        return await m.reply(f"❌ '{name}' পাওয়া যায়নি!")
    if not all_movies: return await m.reply("❌ কোনো মুভি নেই!")
    btns = [[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edmvpick_{d['_id']}")] for d in all_movies[:15]]
    await m.reply("✏️ এডিট করার জন্য মুভি সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edmvpick_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvpick_cb(c, q): await show_edit_movie_panel(q, q.data.split("_")[1], edit=True)

async def show_edit_movie_panel(msg_or_q, f_id, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if not doc:
        txt = "❌ মুভিটি পাওয়া যায়নি!"
        return await (msg_or_q.message.edit_text(txt) if edit else msg_or_q.reply(txt))
    quals = doc.get("qualities", [])
    q_str = ", ".join([x["name"] for x in quals]) if quals else ("Single File" if doc.get("file_id") else "None")
    txt = f"✏️ **Edit Movie:** `{doc['title']}`\n🆔 `{f_id}` | 📂 `{doc.get('category', 'All')}` | 🌐 `{doc.get('language', 'N/A')}`\n📌 Pin: `{'Yes' if doc.get('pinned') else 'No'}` | 💎 VIP: `{'Yes' if doc.get('is_premium') else 'No'}`\n🎞 Qualities: `{q_str}`"
    btns = [
        [InlineKeyboardButton("✏️ Name/Year", callback_data=f"edmv_name_{f_id}"), InlineKeyboardButton("🖼 Poster", callback_data=f"edmv_poster_{f_id}")],
        [InlineKeyboardButton("📂 Category", callback_data=f"edmv_cat_{f_id}"), InlineKeyboardButton("🌐 Language", callback_data=f"edmv_lang_{f_id}")],
        [InlineKeyboardButton("➕ Add Quality", callback_data=f"edmv_addq_{f_id}"), InlineKeyboardButton("🗑 Delete Quality", callback_data=f"edmv_delq_{f_id}")],
        [InlineKeyboardButton("📌 Pin", callback_data=f"edmv_pin_{f_id}"), InlineKeyboardButton("💎 Premium", callback_data=f"edmv_prem_{f_id}")],
        [InlineKeyboardButton("🗑 Delete Movie", callback_data=f"edmv_del_{f_id}")]
    ]
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edmv_") & filters.user(ADMIN_ID) & unique_cb)
async def edmv_cb(c, q):
    _, action, f_id = q.data.split("_", 2)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if not doc: return await q.answer("❌ মুভি পাওয়া যায়নি!", show_alert=True)
    if action == "pin":
        await target_db["files"].update_one({"_id": f_id}, {"$set": {"pinned": not doc.get("pinned", False)}})
        return await show_edit_movie_panel(q, f_id, edit=True)
    elif action == "prem":
        await target_db["files"].update_one({"_id": f_id}, {"$set": {"is_premium": not doc.get("is_premium", False)}})
        return await show_edit_movie_panel(q, f_id, edit=True)
    elif action == "del":
        await target_db["files"].delete_one({"_id": f_id})
        return await q.message.edit_text(f"✅ **{doc['title']}** ডিলিট হয়েছে!")
    elif action == "name":
        admin_steps[q.from_user.id] = {"step": "edmv_name_wait", "f_id": f_id}
        return await q.message.edit_text("✏️ নতুন নাম ও সাল দিন (যেমন: `RRR 2024`):")
    elif action == "poster":
        admin_steps[q.from_user.id] = {"step": "edmv_poster_wait", "f_id": f_id}
        return await q.message.edit_text("🖼 নতুন পোস্টার ছবি পাঠান:")
    elif action == "cat":
        cats = await get_categories_list()
        btns = [[InlineKeyboardButton(f"📂 {cn}", callback_data=f"edmvcat_{f_id}_{i}")] for i, cn in enumerate(cats)]
        return await q.message.edit_text("📂 Category সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif action == "lang":
        btns = [[InlineKeyboardButton(f"🌐 {ln}", callback_data=f"edmvlang_{f_id}_{i}")] for i, ln in enumerate(LANGUAGES)]
        return await q.message.edit_text("🌐 Language সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif action == "addq":
        admin_steps[q.from_user.id] = {"step": "edmv_addq_pick", "f_id": f_id}
        return await q.message.edit_text("➕ Quality সিলেক্ট করুন:", reply_markup=get_quality_markup("edmvqadd", f"edmvpick_{f_id}"))
    elif action == "delq":
        quals = doc.get("qualities", [])
        if not quals: return await q.answer("❌ কোনো কোয়ালিটি নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"❌ {ql['name']}", callback_data=f"edmvqdel_{f_id}_{i}")] for i, ql in enumerate(quals)]
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"edmvpick_{f_id}")])
        return await q.message.edit_text("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edmvcat_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvcat_cb(c, q):
    _, f_id, idx = q.data.split("_"); cats = await get_categories_list()
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"category": cats[int(idx)]}})
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvlang_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvlang_cb(c, q):
    _, f_id, idx = q.data.split("_")
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"language": LANGUAGES[int(idx)]}})
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvqdel_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvqdel_cb(c, q):
    _, f_id, idx = q.data.split("_"); idx = int(idx)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        quals = doc.get("qualities", [])
        if 0 <= idx < len(quals):
            quals.pop(idx)
            await target_db["files"].update_one({"_id": f_id}, {"$set": {"qualities": quals}})
            await q.answer("✅ ডিলিট হয়েছে!", show_alert=True)
    await show_edit_movie_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edmvqadd_") & filters.user(ADMIN_ID) & unique_cb)
async def edmvqadd_cb(c, q):
    val = q.data.split("_", 1)[1]; st = admin_steps.get(q.from_user.id)
    if not st or "f_id" not in st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    if val == "custom":
        st["step"] = "edmv_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:")
    st["current_quality"] = val; st["step"] = "edmv_addq_file"
    await q.message.edit_text(f"📤 এবার **{val}** কোয়ালিটির ফাইল পাঠান:")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_custom_qname(c, m):
    admin_steps[m.from_user.id]["current_quality"] = m.text.strip()
    admin_steps[m.from_user.id]["step"] = "edmv_addq_file"
    await m.reply(f"📤 এবার **{m.text.strip()}** কোয়ালিটির ফাইল পাঠান:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_addq_file") & unique_msg)
async def handle_edmv_addq_file(c, m):
    st = admin_steps.get(m.from_user.id); f_id, q_name = st["f_id"], st["current_quality"]
    tg_fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        await target_db["files"].update_one({"_id": f_id}, {"$push": {"qualities": {"name": q_name, "file_id": tg_fid}}})
        if not doc.get("file_id"): await target_db["files"].update_one({"_id": f_id}, {"$set": {"file_id": tg_fid}})
    st["step"] = "edmv_addq_pick"
    await m.reply(f"✅ **{q_name}** অ্যাড হয়েছে!", reply_markup=get_quality_markup("edmvqadd", f"edmvpick_{f_id}"))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_name_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edmv_name(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]
    name, year = parse_name_year(m.text, "")
    full_title = f"{name} ({year})" if year else name
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"name": name, "year": year, "title": full_title}})
    await show_edit_movie_panel(m, f_id, edit=False)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("edmv_poster_wait") & unique_msg)
async def handle_edmv_poster(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; msg = await m.reply("⏳ আপডেট হচ্ছে...")
    path = await m.download()
    try:
        with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg'] else "image/jpeg"
        os.remove(path)
        _, target_db = await find_file_in_all_dbs(f_id)
        if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": f"data:{mime};base64,{encoded}"}})
    except Exception as e: print("Edit poster err:", e)
    await msg.delete(); await show_edit_movie_panel(m, f_id, edit=False)

# ==========================================
# 8. SERIES WORKFLOW (/addseries, /editeseries, /deleteseries)
# ==========================================
@app.on_message(filters.command("addseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addseries(c, m):
    name, year = parse_name_year(m.text, "/addseries")
    if not name:
        admin_steps[m.from_user.id] = {"step": "sr_name_year"}
        return await m.reply("📺 **সিরিজের নাম ও সাল দিন:** (যেমন: `Bachelor Point 2026`)")
    admin_steps[m.from_user.id] = {"step": "sr_cat", "name": name, "year": year, "seasons": [], "is_premium": False, "thumb_url": None}
    markup, _ = await get_cat_markup("srcat")
    await m.reply(f"📺 সিরিজ: **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=markup)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_name_year") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_name_year(c, m):
    name, year = parse_name_year(m.text, "")
    admin_steps[m.from_user.id] = {"step": "sr_cat", "name": name, "year": year, "seasons": [], "is_premium": False, "thumb_url": None}
    markup, _ = await get_cat_markup("srcat")
    await m.reply(f"📺 সিরিজ: **{name} {year}**\n\n১. **Category** সিলেক্ট করুন:", reply_markup=markup)

@app.on_callback_query(filters.regex(r"^srcat_") & filters.user(ADMIN_ID) & unique_cb)
async def srcat_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    cats = await get_categories_list()
    st["category"] = cats[int(q.data.split("_")[1])]
    st["step"] = "sr_lang"
    await q.message.edit_text(f"✅ Category: **{st['category']}**\n\n২. **Language** সিলেক্ট করুন:", reply_markup=get_lang_markup("srlang"))

@app.on_callback_query(filters.regex(r"^srlang_") & filters.user(ADMIN_ID) & unique_cb)
async def srlang_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    st["language"] = LANGUAGES[int(q.data.split("_")[1])]
    await prompt_series_season(q.message, st, edit=True)

async def prompt_series_season(msg_obj, st, edit=True):
    st["step"] = "sr_season_no"
    next_s = len(st["seasons"]) + 1
    btns = [[InlineKeyboardButton(f"📺 Season {next_s}", callback_data=f"srssn_{next_s}")],
            [InlineKeyboardButton("⏭ Skip / Save Series", callback_data="srsave_ask")]]
    txt = f"🌐 Language: **{st['language']}**\n\n৩. এবার **Season নম্বর দিন** (যেমন: `1` বা `5`, অথবা বাটনে ক্লিক করুন):"
    if edit: await msg_obj.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_obj.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

def get_or_create_season(st, season_no):
    for idx, s in enumerate(st["seasons"]):
        if str(s["season_no"]).lower() == str(season_no).lower(): return idx
    st["seasons"].append({"season_no": str(season_no), "episodes": []})
    return len(st["seasons"]) - 1

def get_or_create_episode(st, s_idx, ep_no):
    eps = st["seasons"][s_idx]["episodes"]
    for idx, e in enumerate(eps):
        if str(e["ep_no"]).lower() == str(ep_no).lower(): return idx
    eps.append({"ep_no": str(ep_no), "qualities": []})
    return len(eps) - 1

async def prompt_series_episode(msg_obj, st, edit=True):
    st["step"] = "sr_ep_no"
    ssn = st["seasons"][st["cur_s_idx"]]
    next_ep = len(ssn["episodes"]) + 1
    btns = [[InlineKeyboardButton(f"🎬 Episode {next_ep}", callback_data=f"srep_{next_ep}")],
            [InlineKeyboardButton("➕ Add Another Season", callback_data="srssn_new")],
            [InlineKeyboardButton("⏭ Skip / Save Series", callback_data="srsave_ask")]]
    txt = f"📺 **Season {ssn['season_no']}** সিলেক্টেড!\nএবার **Episode নম্বর দিন** (লিখে পাঠান যেমন: `1` বা `134`, বা বাটনে ক্লিক করুন):"
    if edit: await msg_obj.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_obj.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^srssn_") & filters.user(ADMIN_ID) & unique_cb)
async def srssn_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_", 1)[1]
    if val == "new": return await prompt_series_season(q.message, st, edit=True)
    st["cur_s_idx"] = get_or_create_season(st, val)
    await prompt_series_episode(q.message, st, edit=True)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_season_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_season_no(c, m):
    st = admin_steps[m.from_user.id]
    st["cur_s_idx"] = get_or_create_season(st, m.text.strip())
    await prompt_series_episode(m, st, edit=False)

@app.on_callback_query(filters.regex(r"^srep_") & filters.user(ADMIN_ID) & unique_cb)
async def srep_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_", 1)[1]
    if val == "next": return await prompt_series_episode(q.message, st, edit=True)
    st["cur_e_idx"] = get_or_create_episode(st, st["cur_s_idx"], val)
    st["step"] = "sr_qual_wait"
    ssn = st["seasons"][st["cur_s_idx"]]; ep = ssn["episodes"][st["cur_e_idx"]]
    await q.message.edit_text(f"📺 Season {ssn['season_no']} | 🎬 Episode {ep['ep_no']}\n\nআনলিমিটেড **Quality** অ্যাড করুন (বা Skip করুন):",
                              reply_markup=get_quality_markup("srqual", "srep_next"))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_ep_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_ep_no(c, m):
    st = admin_steps[m.from_user.id]
    st["cur_e_idx"] = get_or_create_episode(st, st["cur_s_idx"], m.text.strip())
    st["step"] = "sr_qual_wait"
    ssn = st["seasons"][st["cur_s_idx"]]; ep = ssn["episodes"][st["cur_e_idx"]]
    await m.reply(f"📺 Season {ssn['season_no']} | 🎬 Episode {ep['ep_no']}\n\nআনলিমিটেড **Quality** অ্যাড করুন (বা Skip করুন):",
                  reply_markup=get_quality_markup("srqual", "srep_next"))

@app.on_callback_query(filters.regex(r"^srqual_") & filters.user(ADMIN_ID) & unique_cb)
async def srqual_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    val = q.data.split("_", 1)[1]
    if val == "custom":
        st["step"] = "sr_custom_qname"
        return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:")
    st["current_quality"] = val; st["step"] = "sr_qual_file"
    await q.message.edit_text(f"📤 এবার **{val}** কোয়ালিটির এপিসোড ফাইল পাঠান:")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_sr_custom_qname(c, m):
    st = admin_steps[m.from_user.id]
    st["current_quality"] = m.text.strip(); st["step"] = "sr_qual_file"
    await m.reply(f"📤 এবার **{st['current_quality']}** কোয়ালিটির এপিসোড ফাইল পাঠান:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_qual_file") & unique_msg)
async def handle_sr_qual_file(c, m):
    st = admin_steps[m.from_user.id]
    f_id = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    q_name = st.get("current_quality", "HD")
    ssn = st["seasons"][st["cur_s_idx"]]; ep = ssn["episodes"][st["cur_e_idx"]]
    ep["qualities"].append({"name": q_name, "file_id": f_id})
    st["step"] = "sr_qual_wait"
    added = ", ".join([q["name"] for q in ep["qualities"]])
    btns = get_quality_markup("srqual", "srep_next").inline_keyboard
    btns.append([InlineKeyboardButton("✅ Save Entire Series", callback_data="srsave_ask")])
    await m.reply(f"✅ S{ssn['season_no']} E{ep['ep_no']} - **{q_name}** অ্যাড হয়েছে!\n📦 কোয়ালিটি: `{added}`", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^srsave_ask$") & filters.user(ADMIN_ID) & unique_cb)
async def srsave_ask_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    st["step"] = "sr_poster"
    p_status = "💎 Premium" if st.get("is_premium") else "👤 Regular"
    btns = [[InlineKeyboardButton("💎 Premium / Regular টগল", callback_data="srprem_toggle")],
            [InlineKeyboardButton("⏭ Skip Poster & Save Series", callback_data="srsave_final")]]
    await q.message.edit_text(f"🖼 **সিরিজের পোস্টার ছবি দিন** (অথবা Skip করুন)।\n🔰 টাইপ: **{p_status}**", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^srprem_toggle$") & filters.user(ADMIN_ID) & unique_cb)
async def srprem_toggle_cb(c, q):
    st = admin_steps.get(q.from_user.id)
    if not st: return
    st["is_premium"] = not st.get("is_premium", False)
    p_status = "💎 Premium" if st["is_premium"] else "👤 Regular"
    btns = [[InlineKeyboardButton("💎 Premium / Regular টগল", callback_data="srprem_toggle")],
            [InlineKeyboardButton("⏭ Skip Poster & Save Series", callback_data="srsave_final")]]
    await q.message.edit_text(f"🖼 **সিরিজের পোস্টার ছবি দিন** (অথবা Skip করুন)।\n🔰 টাইপ: **{p_status}**", reply_markup=InlineKeyboardMarkup(btns))

async def finalize_save_series(c, uid, msg_obj):
    st = admin_steps.pop(uid, None)
    if not st: return
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    dbs = await get_extra_dbs_async()
    target_db, min_size = dbs[0], float('inf')
    for d in dbs:
        try:
            stat = await d.command("dbstats")
            if stat["dataSize"] < min_size: min_size = stat["dataSize"]; target_db = d
        except: pass
    full_title = f"{st['name']} ({st['year']})" if st.get("year") else st["name"]
    total_eps = sum(len(s["episodes"]) for s in st["seasons"])
    new_series = {
        "_id": short_id, "type": "series", "title": full_title, "name": st["name"], "year": st.get("year", ""),
        "category": st.get("category", "Web Series"), "language": st.get("language", "Bangla"),
        "is_premium": st.get("is_premium", False), "file_id": None, "seasons": st.get("seasons", []),
        "thumb_url": st.get("thumb_url") or "https://placehold.co/600x400/1c1c24/ff007f?text=Series",
        "pinned": False, "ratings": [], "created_at": datetime.utcnow().isoformat() + "Z",
        "views": 0, "likes": [], "comments": []
    }
    await target_db["files"].insert_one(new_series)
    await notify_new_post(c, new_series, await get_config())
    await msg_obj.edit_text(f"✅ **সিরিজ সেভ হয়েছে!**\n📺 **{full_title}**\n📚 Seasons: `{len(new_series['seasons'])}` | Episodes: `{total_eps}` | 🆔 `{short_id}`")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("sr_poster") & unique_msg)
async def handle_sr_poster(c, m):
    msg = await m.reply("⏳ পোস্টার সেভ হচ্ছে...")
    path = await m.download()
    try:
        with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg'] else "image/jpeg"
        admin_steps[m.from_user.id]["thumb_url"] = f"data:{mime};base64,{encoded}"
        os.remove(path)
    except Exception as e: print("Series poster err:", e)
    await finalize_save_series(c, m.from_user.id, msg)

@app.on_callback_query(filters.regex(r"^srsave_final$") & filters.user(ADMIN_ID) & unique_cb)
async def srsave_final_cb(c, q): await finalize_save_series(c, q.from_user.id, q.message)

@app.on_message(filters.command("deleteseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delete_series(c, m): await send_series_delete_page(m, 0, edit=False)

async def send_series_delete_page(msg_or_q, page=0, edit=True):
    all_docs = await get_all_files_across_dbs({"type": "series"})
    if not all_docs:
        txt = "❌ কোনো সিরিজ নেই!"
        return await (msg_or_q.message.edit_text(txt) if edit else msg_or_q.reply(txt))
    per_page = 10; start = page * per_page; chunk = all_docs[start:start + per_page]
    btns = [[InlineKeyboardButton(f"🗑 {d['title'][:32]} [{d['_id']}]", callback_data=f"delsr_{d['_id']}_{page}")] for d in chunk]
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"delsrpg_{page-1}"))
    if start + per_page < len(all_docs): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"delsrpg_{page+1}"))
    if nav: btns.append(nav)
    txt = f"🗑 **সিরিজ ডিলিট লিস্ট (Total: {len(all_docs)}):**"
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delsrpg_") & filters.user(ADMIN_ID) & unique_cb)
async def delsrpg_cb(c, q): await send_series_delete_page(q, int(q.data.split("_")[1]), edit=True)

@app.on_callback_query(filters.regex(r"^delsr_") & filters.user(ADMIN_ID) & unique_cb)
async def delsr_cb(c, q):
    _, f_id, page = q.data.split("_")
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        await target_db["files"].delete_one({"_id": f_id})
        await q.answer(f"✅ {doc['title']} ডিলিট হয়েছে!", show_alert=True)
    await send_series_delete_page(q, int(page), edit=True)

@app.on_message(filters.command("editeseries", case_sensitive=False) & filters.user(ADMIN_ID) & unique_msg)
async def cmd_edit_series(c, m):
    name, year = parse_name_year(m.text, "/editeseries")
    all_series = await get_all_files_across_dbs({"type": "series"})
    if name:
        matches = [d for d in all_series if name.lower() in d.get("title", "").lower() and (not year or year in d.get("title", ""))]
        if len(matches) == 1: return await show_edit_series_panel(m, matches[0]["_id"], edit=False)
        elif matches:
            btns = [[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edsrpick_{d['_id']}")] for d in matches[:15]]
            return await m.reply("🔍 ম্যাচ হওয়া সিরিজ:", reply_markup=InlineKeyboardMarkup(btns))
        return await m.reply(f"❌ '{name}' পাওয়া যায়নি!")
    if not all_series: return await m.reply("❌ কোনো সিরিজ নেই!")
    btns = [[InlineKeyboardButton(f"✏️ {d['title'][:35]}", callback_data=f"edsrpick_{d['_id']}")] for d in all_series[:15]]
    await m.reply("✏️ এডিট করার জন্য সিরিজ সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsrpick_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrpick_cb(c, q): await show_edit_series_panel(q, q.data.split("_")[1], edit=True)

async def show_edit_series_panel(msg_or_q, f_id, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if not doc:
        txt = "❌ সিরিজটি পাওয়া যায়নি!"
        return await (msg_or_q.message.edit_text(txt) if edit else msg_or_q.reply(txt))
    ssns = doc.get("seasons", [])
    total_eps = sum(len(s.get("episodes", [])) for s in ssns)
    txt = f"📺 **Edit Series:** `{doc['title']}`\n🆔 `{f_id}` | 📂 `{doc.get('category', 'Web Series')}` | 🌐 `{doc.get('language', 'N/A')}`\n📌 Pin: `{'Yes' if doc.get('pinned') else 'No'}` | 💎 VIP: `{'Yes' if doc.get('is_premium') else 'No'}`\n📚 Seasons: `{len(ssns)}` | Episodes: `{total_eps}`"
    btns = [
        [InlineKeyboardButton("✏️ Name/Year", callback_data=f"edsr_name_{f_id}"), InlineKeyboardButton("🖼 Poster", callback_data=f"edsr_poster_{f_id}")],
        [InlineKeyboardButton("📂 Category", callback_data=f"edsr_cat_{f_id}"), InlineKeyboardButton("🌐 Language", callback_data=f"edsr_lang_{f_id}")],
        [InlineKeyboardButton("➕ Add Season", callback_data=f"edsr_addssn_{f_id}"), InlineKeyboardButton("📚 Manage Seasons", callback_data=f"edsr_mng_{f_id}")],
        [InlineKeyboardButton("📌 Pin", callback_data=f"edsr_pin_{f_id}"), InlineKeyboardButton("💎 Premium", callback_data=f"edsr_prem_{f_id}")],
        [InlineKeyboardButton("🗑 Delete Series", callback_data=f"edsr_del_{f_id}")]
    ]
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsr_") & filters.user(ADMIN_ID) & unique_cb)
async def edsr_cb(c, q):
    _, action, f_id = q.data.split("_", 2)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if not doc: return await q.answer("❌ পাওয়া যায়নি!", show_alert=True)
    if action == "pin":
        await target_db["files"].update_one({"_id": f_id}, {"$set": {"pinned": not doc.get("pinned", False)}})
        return await show_edit_series_panel(q, f_id, edit=True)
    elif action == "prem":
        await target_db["files"].update_one({"_id": f_id}, {"$set": {"is_premium": not doc.get("is_premium", False)}})
        return await show_edit_series_panel(q, f_id, edit=True)
    elif action == "del":
        await target_db["files"].delete_one({"_id": f_id})
        return await q.message.edit_text(f"✅ **{doc['title']}** ডিলিট হয়েছে!")
    elif action == "name":
        admin_steps[q.from_user.id] = {"step": "edsr_name_wait", "f_id": f_id}
        return await q.message.edit_text("✏️ সিরিজের নতুন নাম ও সাল দিন:")
    elif action == "poster":
        admin_steps[q.from_user.id] = {"step": "edsr_poster_wait", "f_id": f_id}
        return await q.message.edit_text("🖼 নতুন পোস্টার ছবি দিন:")
    elif action == "cat":
        cats = await get_categories_list()
        btns = [[InlineKeyboardButton(f"📂 {cn}", callback_data=f"edsrcat_{f_id}_{i}")] for i, cn in enumerate(cats)]
        return await q.message.edit_text("📂 Category সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif action == "lang":
        btns = [[InlineKeyboardButton(f"🌐 {ln}", callback_data=f"edsrlang_{f_id}_{i}")] for i, ln in enumerate(LANGUAGES)]
        return await q.message.edit_text("🌐 Language সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))
    elif action == "addssn":
        admin_steps[q.from_user.id] = {"step": "edsr_addssn_no", "f_id": f_id}
        return await q.message.edit_text("📺 নতুন Season নম্বর লিখুন (যেমন: `2`):")
    elif action == "mng":
        ssns = doc.get("seasons", [])
        if not ssns: return await q.answer("❌ কোনো সিজন নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"📺 Season {s['season_no']} ({len(s.get('episodes', []))} Eps)", callback_data=f"edsrs_{f_id}_{i}")] for i, s in enumerate(ssns)]
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"edsrpick_{f_id}")])
        return await q.message.edit_text("📚 সিজন সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsrcat_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrcat_cb(c, q):
    _, f_id, idx = q.data.split("_"); cats = await get_categories_list()
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"category": cats[int(idx)]}})
    await show_edit_series_panel(q, f_id, edit=True)

@app.on_callback_query(filters.regex(r"^edsrlang_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrlang_cb(c, q):
    _, f_id, idx = q.data.split("_")
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"language": LANGUAGES[int(idx)]}})
    await show_edit_series_panel(q, f_id, edit=True)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_name_wait") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_name(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]
    name, year = parse_name_year(m.text, "")
    full_title = f"{name} ({year})" if year else name
    _, target_db = await find_file_in_all_dbs(f_id)
    if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"name": name, "year": year, "title": full_title}})
    await show_edit_series_panel(m, f_id, edit=False)

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_poster_wait") & unique_msg)
async def handle_edsr_poster(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; msg = await m.reply("⏳ আপডেট হচ্ছে...")
    path = await m.download()
    try:
        with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg'] else "image/jpeg"
        os.remove(path)
        _, target_db = await find_file_in_all_dbs(f_id)
        if target_db: await target_db["files"].update_one({"_id": f_id}, {"$set": {"thumb_url": f"data:{mime};base64,{encoded}"}})
    except Exception as e: print("Edit sr poster err:", e)
    await msg.delete(); await show_edit_series_panel(m, f_id, edit=False)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addssn_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_addssn_no(c, m):
    f_id = admin_steps.pop(m.from_user.id)["f_id"]; s_no = m.text.strip()
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        ssns = doc.get("seasons", [])
        s_idx = next((i for i, s in enumerate(ssns) if str(s["season_no"]).lower() == s_no.lower()), None)
        if s_idx is None:
            ssns.append({"season_no": s_no, "episodes": []}); s_idx = len(ssns) - 1
            await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
        await show_edit_season_panel(m, f_id, s_idx, edit=False)

async def show_edit_season_panel(msg_or_q, f_id, s_idx, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    if not doc or s_idx >= len(doc.get("seasons", [])): return
    ssn = doc["seasons"][s_idx]
    btns = [[InlineKeyboardButton(f"🎬 Ep {e['ep_no']} ({len(e.get('qualities', []))} Q)", callback_data=f"edsrep_view_{f_id}_{s_idx}_{ei}")] for ei, e in enumerate(ssn.get("episodes", []))]
    btns.append([InlineKeyboardButton("➕ Add New Episode", callback_data=f"edsrep_add_{f_id}_{s_idx}")])
    btns.append([InlineKeyboardButton("🗑 Delete Season", callback_data=f"edsrep_delssn_{f_id}_{s_idx}")])
    btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"edsr_mng_{f_id}")])
    txt = f"📺 **{doc['title']} — Season {ssn['season_no']}**"
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^edsrs_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrs_cb(c, q):
    _, f_id, s_idx = q.data.split("_"); await show_edit_season_panel(q, f_id, int(s_idx), edit=True)

@app.on_callback_query(filters.regex(r"^edsrep_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrep_cb(c, q):
    parts = q.data.split("_"); act = parts[1]
    if act == "view":
        return await show_edit_episode_panel(q, parts[2], int(parts[3]), int(parts[4]), edit=True)
    elif act == "add":
        admin_steps[q.from_user.id] = {"step": "edsr_addep_no", "f_id": parts[2], "s_idx": int(parts[3])}
        return await q.message.edit_text("🎬 নতুন Episode নম্বর লিখুন:")
    elif act == "delssn":
        f_id, s_idx = parts[2], int(parts[3]); doc, target_db = await find_file_in_all_dbs(f_id)
        if doc and target_db:
            ssns = doc.get("seasons", [])
            if 0 <= s_idx < len(ssns): ssns.pop(s_idx); await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
        return await show_edit_series_panel(q, f_id, edit=True)
    elif act == "delep":
        f_id, s_idx, e_idx = parts[2], int(parts[3]), int(parts[4]); doc, target_db = await find_file_in_all_dbs(f_id)
        if doc and target_db:
            ssns = doc.get("seasons", [])
            if 0 <= s_idx < len(ssns) and 0 <= e_idx < len(ssns[s_idx]["episodes"]):
                ssns[s_idx]["episodes"].pop(e_idx); await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
        return await show_edit_season_panel(q, f_id, s_idx, edit=True)
    elif act == "addq":
        f_id, s_idx, e_idx = parts[2], int(parts[3]), int(parts[4])
        admin_steps[q.from_user.id] = {"step": "edsr_addq_pick", "f_id": f_id, "s_idx": s_idx, "e_idx": e_idx}
        return await q.message.edit_text("➕ Quality সিলেক্ট করুন:", reply_markup=get_quality_markup("edsrqadd", f"edsrep_view_{f_id}_{s_idx}_{e_idx}"))
    elif act == "delqmenu":
        f_id, s_idx, e_idx = parts[2], int(parts[3]), int(parts[4]); doc, _ = await find_file_in_all_dbs(f_id)
        quals = doc["seasons"][s_idx]["episodes"][e_idx].get("qualities", [])
        if not quals: return await q.answer("❌ কোনো কোয়ালিটি নেই!", show_alert=True)
        btns = [[InlineKeyboardButton(f"❌ {ql['name']}", callback_data=f"edsrqdel_{f_id}_{s_idx}_{e_idx}_{qi}")] for qi, ql in enumerate(quals)]
        btns.append([InlineKeyboardButton("⬅️ Back", callback_data=f"edsrep_view_{f_id}_{s_idx}_{e_idx}")])
        return await q.message.edit_text("🗑 ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

async def show_edit_episode_panel(msg_or_q, f_id, s_idx, e_idx, edit=True):
    doc, _ = await find_file_in_all_dbs(f_id)
    ssn = doc["seasons"][s_idx]; ep = ssn["episodes"][e_idx]
    q_str = ", ".join([x["name"] for x in ep.get("qualities", [])]) or "None"
    txt = f"🎬 **{doc['title']}** | S{ssn['season_no']} E{ep['ep_no']}\n🎞 Qualities: `{q_str}`"
    btns = [
        [InlineKeyboardButton("➕ Add Quality", callback_data=f"edsrep_addq_{f_id}_{s_idx}_{e_idx}"),
         InlineKeyboardButton("🗑 Delete Quality", callback_data=f"edsrep_delqmenu_{f_id}_{s_idx}_{e_idx}")],
        [InlineKeyboardButton("🗑 Delete Episode", callback_data=f"edsrep_delep_{f_id}_{s_idx}_{e_idx}")],
        [InlineKeyboardButton("⬅️ Back", callback_data=f"edsrs_{f_id}_{s_idx}")]
    ]
    if edit: await msg_or_q.message.edit_text(txt, reply_markup=InlineKeyboardMarkup(btns))
    else: await msg_or_q.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addep_no") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_addep_no(c, m):
    st = admin_steps.pop(m.from_user.id); f_id, s_idx = st["f_id"], st["s_idx"]; ep_no = m.text.strip()
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        ssns = doc.get("seasons", []); eps = ssns[s_idx]["episodes"]
        e_idx = next((i for i, e in enumerate(eps) if str(e["ep_no"]).lower() == ep_no.lower()), None)
        if e_idx is None:
            eps.append({"ep_no": ep_no, "qualities": []}); e_idx = len(eps) - 1
            await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
        await show_edit_episode_panel(m, f_id, s_idx, e_idx, edit=False)

@app.on_callback_query(filters.regex(r"^edsrqdel_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrqdel_cb(c, q):
    _, f_id, s_idx, e_idx, qi = q.data.split("_"); s_idx, e_idx, qi = int(s_idx), int(e_idx), int(qi)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        ssns = doc.get("seasons", []); quals = ssns[s_idx]["episodes"][e_idx]["qualities"]
        if 0 <= qi < len(quals): quals.pop(qi); await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
    await show_edit_episode_panel(q, f_id, s_idx, e_idx, edit=True)

@app.on_callback_query(filters.regex(r"^edsrqadd_") & filters.user(ADMIN_ID) & unique_cb)
async def edsrqadd_cb(c, q):
    val = q.data.split("_", 1)[1]; st = admin_steps.get(q.from_user.id)
    if not st or "f_id" not in st: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    if val == "custom":
        st["step"] = "edsr_custom_qname"; return await q.message.edit_text("✏️ কাস্টম কোয়ালিটির নাম লিখুন:")
    st["current_quality"] = val; st["step"] = "edsr_addq_file"
    await q.message.edit_text(f"📤 এবার **{val}** কোয়ালিটির এপিসোড ফাইল পাঠান:")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_custom_qname") & ~filters.regex(r"^/") & unique_msg)
async def handle_edsr_custom_qname(c, m):
    admin_steps[m.from_user.id]["current_quality"] = m.text.strip(); admin_steps[m.from_user.id]["step"] = "edsr_addq_file"
    await m.reply(f"📤 এবার **{m.text.strip()}** কোয়ালিটির এপিসোড ফাইল পাঠান:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("edsr_addq_file") & unique_msg)
async def handle_edsr_addq_file(c, m):
    st = admin_steps.get(m.from_user.id); f_id, s_idx, e_idx, q_name = st["f_id"], st["s_idx"], st["e_idx"], st["current_quality"]
    tg_fid = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    doc, target_db = await find_file_in_all_dbs(f_id)
    if doc and target_db:
        ssns = doc.get("seasons", []); ssns[s_idx]["episodes"][e_idx]["qualities"].append({"name": q_name, "file_id": tg_fid})
        await target_db["files"].update_one({"_id": f_id}, {"$set": {"seasons": ssns}})
    st["step"] = "edsr_addq_pick"
    await m.reply(f"✅ **{q_name}** অ্যাড হয়েছে!", reply_markup=get_quality_markup("edsrqadd", f"edsrep_view_{f_id}_{s_idx}_{e_idx}"))

# ==========================================
# 9. LEGACY COMMANDS (PRESERVED 100%)
# ==========================================
@app.on_message(filters.command("addfile") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addfile(c, m): admin_steps[m.from_user.id] = {"step": "name"}; await m.reply("১. ফাইলের নাম/টাইটেল দিন:")

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("name") & ~filters.regex(r"^/") & unique_msg)
async def handle_admin_name(c, m): admin_steps[m.from_user.id]["title"] = m.text; admin_steps[m.from_user.id]["step"] = "thumb"; await m.reply("২. ফাইলের ছবি দিন:")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("thumb") & unique_msg)
async def handle_admin_photo(c, m):
    msg = await m.reply("⏳ সেভ হচ্ছে..."); path = await m.download()
    try:
        with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        admin_steps[m.from_user.id]["thumb_url"] = f"data:image/{ext if ext in ['png','webp','gif','jpeg','jpg'] else 'jpeg'};base64,{encoded}"
        os.remove(path)
    except: admin_steps[m.from_user.id]["thumb_url"] = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    await msg.edit_text("ভিডিওটি কি প্রিমিয়াম নাকি রেগুলার?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="ftype_prem")], [InlineKeyboardButton("👤 Regular", callback_data="ftype_reg")]]))

@app.on_callback_query(filters.regex(r"^ftype_") & filters.user(ADMIN_ID) & unique_cb)
async def filetype_cb(c, q): admin_steps[q.from_user.id]["is_premium"] = (q.data == "ftype_prem"); admin_steps[q.from_user.id]["step"] = "file"; await q.message.edit_text("✅ ৩. এবার মূল ফাইল দিন:")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("file") & unique_msg)
async def handle_admin_file(c, m):
    msg = await m.reply("⏳ সেভ হচ্ছে...")
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    f_id = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id)
    dbs = await get_extra_dbs_async(); target_db, min_size = dbs[0], float('inf')
    for d in dbs:
        try:
            st = await d.command("dbstats")
            if st["dataSize"] < min_size: min_size = st["dataSize"]; target_db = d
        except: pass
    new_file = {"_id": short_id, "title": admin_steps[m.from_user.id]["title"], "category": "All", "is_premium": admin_steps[m.from_user.id].get("is_premium", False), "file_id": f_id, "thumb_url": admin_steps[m.from_user.id].get("thumb_url"), "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
    await target_db["files"].insert_one(new_file); del admin_steps[m.from_user.id]
    await notify_new_post(c, new_file, await get_config())
    await msg.edit_text(f"✅ ফাইল অ্যাড হয়েছে! ID: `{short_id}`")

@app.on_message(filters.command("vidtitel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_vidtitel(c, m):
    t = m.text.replace("/vidtitel", "").strip()
    if not t: return await m.reply("❌ নিয়ম: `/vidtitel <Title>`")
    await get_db(); await titles_col.insert_one({"title": t}); await m.reply(f"✅ টাইটেল অ্যাড হয়েছে: **{t}**")

@app.on_message(filters.command("deltitel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltitel(c, m):
    await get_db(); ts = await titles_col.find().to_list(100)
    if not ts: return await m.reply("❌ কোনো টাইটেল নেই!")
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {t['title'][:25]}", callback_data=f"delttl_{t['_id']}")] for t in ts]))

@app.on_callback_query(filters.regex(r"^delttl_") & filters.user(ADMIN_ID) & unique_cb)
async def delttl_cb(c, q): await get_db(); await titles_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ ডিলিট হয়েছে!")

@app.on_message(filters.command("auto") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_auto_upload(c, m):
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document): return await m.reply("❌ ভিডিওতে রিপ্লাই করে `/auto` দিন।")
    await get_db(); ts = await titles_col.find().to_list(100)
    if not ts: return await m.reply("❌ `/vidtitel` দিয়ে টাইটেল অ্যাড করুন।")
    admin_steps[m.from_user.id] = {"file_id": (m.reply_to_message.video or m.reply_to_message.document).file_id}
    await m.reply("টাইটেল সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t['title'], callback_data=f"selttl_{t['_id']}")] for t in ts]))

@app.on_callback_query(filters.regex(r"^selttl_") & filters.user(ADMIN_ID) & unique_cb)
async def selttl_cb(c, q):
    sd = admin_steps.get(q.from_user.id)
    if not sd or "file_id" not in sd: return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    await get_db(); td = await titles_col.find_one({"_id": ObjectId(q.data.split("_")[1])})
    if not td: return await q.answer("❌ পাওয়া যায়নি!", show_alert=True)
    msg = await q.message.edit_text(f"⏳ **{td['title']}** প্রসেস হচ্ছে...")
    thumb_url = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    try:
        vp = await c.download_media(sd["file_id"]); gp = f"{vp}_grid.jpg"
        p = await asyncio.create_subprocess_shell(f'ffmpeg -y -i "{vp}" -vf "thumbnail=n=20,scale=320:-1,tile=4x2" -frames:v 1 -q:v 2 "{gp}"', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await p.communicate()
        if os.path.exists(gp):
            with open(gp, "rb") as f: thumb_url = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode('utf-8')}"
            os.remove(gp)
        if os.path.exists(vp): os.remove(vp)
    except: pass
    admin_steps[q.from_user.id] = {"step": "auto_wait", "title": td["title"], "file_id": sd["file_id"], "thumb_url": thumb_url}
    await msg.edit_text(f"✅ **{td['title']}**\nকোথায় অ্যাড হবে?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="auto_prem")], [InlineKeyboardButton("👤 Regular", callback_data="auto_reg")]]))

@app.on_message(filters.command("auto2") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_auto2(c, m):
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document): return await m.reply("❌ ভিডিওতে রিপ্লাই করে `/auto2` দিন।")
    title = m.text.replace("/auto2", "").strip()
    if not title: return await m.reply("❌ নিয়ম: `/auto2 <Title>`")
    msg = await m.reply(f"⏳ **{title}** প্রসেস হচ্ছে..."); fid = (m.reply_to_message.video or m.reply_to_message.document).file_id
    thumb_url = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    try:
        vp = await c.download_media(fid); gp = f"{vp}_grid.jpg"
        p = await asyncio.create_subprocess_shell(f'ffmpeg -y -i "{vp}" -vf "thumbnail=n=20,scale=320:-1,tile=4x2" -frames:v 1 -q:v 2 "{gp}"', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await p.communicate()
        if os.path.exists(gp):
            with open(gp, "rb") as f: thumb_url = f"data:image/jpeg;base64,{base64.b64encode(f.read()).decode('utf-8')}"
            os.remove(gp)
        if os.path.exists(vp): os.remove(vp)
    except: pass
    admin_steps[m.from_user.id] = {"step": "auto_wait", "title": title, "file_id": fid, "thumb_url": thumb_url}
    await msg.edit_text(f"✅ **{title}**\nকোথায় অ্যাড হবে?", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💎 Premium", callback_data="auto_prem")], [InlineKeyboardButton("👤 Regular", callback_data="auto_reg")]]))

@app.on_callback_query(filters.regex(r"^auto_") & filters.user(ADMIN_ID) & unique_cb)
async def auto_type_cb(c, q):
    sd = admin_steps.get(q.from_user.id)
    if not sd or sd.get("step") != "auto_wait": return await q.answer("❌ সেশন এক্সপায়ার!", show_alert=True)
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    dbs = await get_extra_dbs_async(); target_db, min_size = dbs[0], float('inf')
    for d in dbs:
        try:
            st = await d.command("dbstats")
            if st["dataSize"] < min_size: min_size = st["dataSize"]; target_db = d
        except: pass
    nf = {"_id": short_id, "title": sd["title"], "category": "All", "is_premium": (q.data == "auto_prem"), "file_id": sd["file_id"], "thumb_url": sd["thumb_url"], "created_at": datetime.utcnow().isoformat() + "Z", "views": 0, "likes": [], "comments": []}
    await target_db["files"].insert_one(nf); del admin_steps[q.from_user.id]
    await notify_new_post(c, nf, await get_config())
    await q.message.edit_text(f"✅ অটো আপলোড সফল! ID: `{short_id}`")

@app.on_message(filters.command("promot") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_promot(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("❌ ফটোতে রিপ্লাই করে `/promot Title | Link` দিন।")
    text = m.text.replace("/promot", "").strip()
    if "|" not in text: return await m.reply("❌ নিয়ম: `/promot Title | Link`")
    title, link = [x.strip() for x in text.split("|", 1)]
    msg = await m.reply("⏳ সেভ হচ্ছে..."); path = await m.reply_to_message.download()
    with open(path, "rb") as f: encoded = base64.b64encode(f.read()).decode('utf-8')
    ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
    os.remove(path); await get_db()
    await promotions_col.insert_one({"title": title, "link": link, "image": f"data:image/{ext if ext in ['png','webp','gif','jpeg','jpg'] else 'jpeg'};base64,{encoded}"})
    await msg.edit_text(f"✅ প্রোমোট অ্যাড হয়েছে! **{title}**")

@app.on_message(filters.command("delpromot") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delpromot(c, m):
    await get_db(); ps = await promotions_col.find().to_list(100)
    if not ps: return await m.reply("❌ কোনো প্রোমোট নেই!")
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {p['title'][:25]}", callback_data=f"delpromo_{p['_id']}")] for p in ps]))

@app.on_callback_query(filters.regex(r"^delpromo_") & filters.user(ADMIN_ID) & unique_cb)
async def delpromo_cb(c, q): await get_db(); await promotions_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ ডিলিট হয়েছে!")

@app.on_message(filters.command("delfile") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delfile(c, m):
    if len(m.text.split()) < 2: return await m.reply("❌ নিয়ম: `/delfile <file_id>`")
    f_id = m.text.split()[1]
    for d in await get_extra_dbs_async():
        res = await d["files"].delete_one({"_id": f_id})
        if res.deleted_count > 0: return await m.reply("✅ ফাইল ডিলিট হয়েছে!")
    await m.reply("❌ ফাইল পাওয়া যায়নি!")

@app.on_message(filters.command("delall") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delall(c, m):
    for d in await get_extra_dbs_async(): await d["files"].delete_many({})
    await m.reply("✅ সকল ফাইল ডিলিট হয়েছে!")

@app.on_message(filters.command("mongo") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_mongo(c, m):
    args = m.text.split()
    if len(args) < 2: return await m.reply("❌ নিয়ম: `/mongo <uri>`")
    msg = await m.reply("⏳ কানেক্ট হচ্ছে...")
    try:
        tc = AsyncIOMotorClient(args[1], serverSelectionTimeoutMS=3000); await tc.server_info()
        await get_db(); await mongos_col.insert_one({"uri": args[1]})
        await msg.edit_text("✅ নতুন MongoDB যুক্ত হয়েছে! 🚀")
    except Exception as e: await msg.edit_text(f"❌ ব্যর্থ! {e}")

@app.on_message(filters.command("delmongo") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delmongo(c, m):
    await get_db(); dbs = await mongos_col.find().to_list(None)
    if not dbs: return await m.reply("❌ কোনো এক্সট্রা MongoDB নেই!")
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {db['uri'][:25]}...", callback_data=f"delmongo_{db['_id']}")] for db in dbs]))

@app.on_callback_query(filters.regex(r"^delmongo_") & filters.user(ADMIN_ID) & unique_cb)
async def delmongo_cb(c, q):
    await get_db(); doc = await mongos_col.find_one({"_id": ObjectId(q.data.split("_")[1])})
    if doc:
        await mongos_col.delete_one({"_id": doc["_id"]})
        async_mongo_clients.pop(doc["uri"], None); sync_mongo_clients.pop(doc["uri"], None)
        await q.message.edit_text("✅ MongoDB ডিলিট হয়েছে!")

@app.on_message(filters.command("mongostats") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_mongostats(c, m):
    msg = await m.reply("⏳ চেক হচ্ছে..."); dbs = await get_extra_dbs_async(); text = "📊 **MongoDB Stats:**\n\n"
    for i, d in enumerate(dbs):
        try:
            st = await d.command("dbstats"); fc = await d["files"].count_documents({})
            sz = st["dataSize"] / (1024 * 1024)
            text += f"{'🔹 Main DB' if i==0 else f'🔸 Extra DB {i}'}\n🎬 Files: {fc} | 📦 {sz:.2f}/512 MB\n\n"
        except: text += f"❌ **DB {i}** Error!\n\n"
    await msg.edit_text(text)

@app.on_message(filters.command("lockall") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_lockall(c, m): await get_db(); await users_col.update_many({}, {"$set": {"unlocked_files": {}}}); await m.reply("✅ রিস্টার্ট/লক করা হয়েছে!")

@app.on_message(filters.command("autochannel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autochannel(c, m):
    args = m.text.split()
    if len(args) < 4: return await m.reply("❌ নিয়ম: `/autochannel <ChatID> <Interval> <DelMin>`")
    await get_db(); await auto_channels_col.update_one({"chat_id": args[1]}, {"$set": {"chat_id": args[1], "interval": int(args[2]), "delete_min": int(args[3]), "last_post_time": 0, "post_idx": 0}}, upsert=True)
    await m.reply("✅ Auto Channel Added!")

@app.on_message(filters.command("delautochannel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delautochannel(c, m):
    await get_db(); chs = await auto_channels_col.find().to_list(100)
    if not chs: return await m.reply("❌ কোনো অটো চ্যানেল নেই!")
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delautoch_{ch['_id']}")] for ch in chs]))

@app.on_callback_query(filters.regex(r"^delautoch_") & filters.user(ADMIN_ID) & unique_cb)
async def delautoch_cb(c, q): await get_db(); await auto_channels_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ রিমুভ হয়েছে!")

@app.on_message(filters.command("setpremcoin") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_setpremcoin(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"premium_vid_coin": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("relocktime") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_relocktime(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_min": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("delrelocktime") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delrelocktime(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_min": 0}}); await m.reply("✅ Deleted!")

@app.on_message(filters.command("spincoin") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_spincoin(c, m):
    mn, mx = map(int, m.text.split()[1].split("-"))
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"spin_coin_min": mn, "spin_coin_max": mx}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("taskcoin") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_taskcoin(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"task_coin": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("spinlimit") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_spinlimit(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"spin_limit": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("tasklimit") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_tasklimit(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"task_limit": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("addtask") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addtask(c, m):
    try:
        t, l, co = [x.strip() for x in m.text.replace("/addtask", "").split("|")]
        await get_db(); await tasks_col.insert_one({"title": t, "link": l, "coin": int(co)}); await m.reply("✅ Task Added!")
    except: await m.reply("❌ নিয়ম: `/addtask Title | Link | Coins`")

@app.on_message(filters.command("deltask") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltask(c, m):
    await get_db(); ts = await tasks_col.find().to_list(100)
    if not ts: return await m.reply("❌ কোনো টাস্ক নেই!")
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(f"❌ {t['title']}", callback_data=f"deltask_{t['_id']}")] for t in ts]))

@app.on_callback_query(filters.regex(r"^deltask_") & filters.user(ADMIN_ID) & unique_cb)
async def deltask_cb(c, q): await get_db(); await tasks_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Deleted!")

@app.on_message(filters.command("prstep") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_prstep(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"prstep": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("regstep") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_regstep(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"regstep": int(m.text.split()[1])}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("addadmin") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addadmin(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"payment_admin": m.text.split()[1].replace("@", "")}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("addlink") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addlink(c, m): await get_db(); await links_col.insert_one({"link": m.text.split()[1]}); await m.reply("✅ Ad Link Added!")

@app.on_message(filters.command("delink") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_dellink(c, m):
    await get_db(); btns = [[InlineKeyboardButton(f"❌ {l['link'][:20]}...", callback_data=f"dellink_{l['_id']}")] for l in await links_col.find().to_list(100)]
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^dellink_") & filters.user(ADMIN_ID) & unique_cb)
async def dellink_cb(c, q): await get_db(); await links_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Deleted!")

@app.on_message(filters.command("addcnl") & filters.user(ADMIN_ID))
async def cmd_addcnl(c, m):
    try:
        name, link = m.text.split(None, 1)[1].rsplit(None, 1)
        await get_db(); await channels_col.update_one({"link": link}, {"$set": {"name": name, "link": link, "type": "inline"}}, upsert=True)
        await m.reply(f"✅ চ্যানেল যোগ হয়েছে: **{name}**")
    except: await m.reply("❌ নিয়ম: `/addcnl <Name> <Link>`")

@app.on_message(filters.command("delcnl") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delcnl(c, m):
    await get_db(); btns = [[InlineKeyboardButton(f"❌ {ch['name']}", callback_data=f"delch_{ch['_id']}")] for ch in await channels_col.find({"type": "inline"}).to_list(100)]
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_message(filters.command("vercnl") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_vercnl(c, m):
    p = m.text.split(); await get_db(); await channels_col.insert_one({"type": "must_join", "chat_id": int(p[1]), "link": p[2]}); await m.reply("✅ Must Join Added!")

@app.on_message(filters.command("delvrcnl") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delvrcnl(c, m):
    await get_db(); btns = [[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delch_{ch['_id']}")] for ch in await channels_col.find({"type": "must_join"}).to_list(100)]
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^delch_") & filters.user(ADMIN_ID) & unique_cb)
async def delch_cb(c, q): await get_db(); await channels_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Deleted!")

@app.on_message(filters.command("addtex") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addtex(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": m.text.replace("/addtex", "").strip()}}); await m.reply("✅ Updated!")

@app.on_message(filters.command("deltex") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltex(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": ""}}); await m.reply("✅ Deleted!")

@app.on_message(filters.command("logo") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_logo(c, m):
    await get_db()
    if m.reply_to_message and m.reply_to_message.photo: await config_col.update_one({"_id": "settings"}, {"$set": {"start_logo": m.reply_to_message.photo.file_id}}); await m.reply("✅ Logo Set!")
    else: await m.reply("❌ ছবিতে রিপ্লাই করুন।")

@app.on_message(filters.command("autvid") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autvid(c, m):
    await get_db()
    if m.reply_to_message: await config_col.update_one({"_id": "settings"}, {"$set": {"autovid_msg_id": m.reply_to_message.id, "autovid_chat_id": m.chat.id}}); await m.reply("✅ Set!")

@app.on_message(filters.command("autvidti") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autvidti(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autovid_time_min": int(m.text.split()[1])}}); await m.reply("✅ Set!")

@app.on_message(filters.command("autpost") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autpost(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_time_hr": int(m.text.split()[1])}}); await m.reply("✅ Set!")

@app.on_message(filters.command("refbonous") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_refbonous(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"ref_coin": int(m.text.split()[1]), "ref_on": True}}); await m.reply("✅ Set!")

@app.on_message(filters.command("refbonousoff") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_refbonousoff(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"ref_on": False}}); await m.reply("✅ OFF!")

@app.on_message(filters.command("frotect") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_frotect(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"frotect": m.text.split()[1].lower() == "on"}}); await m.reply("✅ Set!")

@app.on_message(filters.command("autodel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autodel(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"auto_del_time": int(m.text.split()[1])}}); await m.reply("✅ Set!")

@app.on_message(filters.command("autex") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autex(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autodel_text": m.text.replace("/autex", "").strip()}}); await m.reply("✅ Set!")

@app.on_message(filters.command("usd") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_usd(c, m): p = m.text.split(); await get_db(); await pkgs_col.insert_one({"type": "usd", "coins": int(p[1]), "details": f"{p[1]} Coins={p[2]} USD"}); await m.reply("✅ Added!")

@app.on_message(filters.command("bdt") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_bdt(c, m): p = m.text.split(); await get_db(); await pkgs_col.insert_one({"type": "bkash", "coins": int(p[1]), "details": f"{p[1]} Coins={p[2]} BDT"}); await m.reply("✅ Added!")

@app.on_message(filters.command("addcred") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addcred(c, m): p = m.text.split(); await get_db(); await pkgs_col.insert_one({"type": "coin", "coins": int(p[1]), "amount": int(p[3]), "unit": p[4], "details": f"{p[1]} Coins={p[3]} {p[4]}"}); await m.reply("✅ Added!")

@app.on_message(filters.command("delcred") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delcred(c, m):
    await get_db(); btns = [[InlineKeyboardButton(f"❌ {p.get('type','').upper()} | {p['details']}", callback_data=f"delpkg_{p['_id']}")] for p in await pkgs_col.find().to_list(100)]
    await m.reply("ডিলিট করুন:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^delpkg_") & filters.user(ADMIN_ID) & unique_cb)
async def delpkg_cb(c, q): await get_db(); await pkgs_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Deleted!")

@app.on_message(filters.command("prparadd") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_prparadd(c, m): await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"prem_vid_wait": int(m.text.split()[1])}}); await m.reply("✅ Set!")

@app.on_message(filters.command("addrdiem") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addrdiem(c, m):
    await get_db(); p = m.text.split(); u_id, amt, unit = int(p[1]), int(p[2]), p[3].lower()
    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(unit, 0)
    await users_col.update_one({"_id": u_id}, {"$set": {"premium_until": datetime.now() + timedelta(seconds=amt * secs)}})
    await m.reply(f"✅ User {u_id} made Premium!")

@app.on_message(filters.command("brodcast") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_brodcast(c, m):
    await get_db()
    if not m.reply_to_message: return await m.reply("❌ মেসেজে রিপ্লাই করে দিন।")
    msg = await m.reply("⏳ Broadcasting..."); s = 0
    for u in await users_col.find().to_list(None):
        try: await m.reply_to_message.copy(u["_id"], reply_markup=m.reply_to_message.reply_markup); s += 1; await asyncio.sleep(0.05)
        except: pass
    await msg.edit_text(f"✅ Sent to {s} users.")

@app.on_message(filters.command("cnlbdcst") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_cnlbdcst(c, m):
    if not m.reply_to_message: return await m.reply("❌ রিপ্লাই করে /cnlbdcst দিন।")
    msg = await m.reply("⏳ Broadcasting..."); s = 0; await get_db(); conf = await get_config()
    chs = set()
    if conf.get("autopost_channel"): chs.add(conf["autopost_channel"])
    async for ch in auto_channels_col.find(): chs.add(int(ch["chat_id"]))
    async for ch in channels_col.find({"type": "must_join"}): chs.add(ch["chat_id"])
    for cid in chs:
        try: await m.reply_to_message.copy(cid, reply_markup=m.reply_to_message.reply_markup); s += 1; await asyncio.sleep(0.5)
        except: pass
    await msg.edit_text(f"✅ Sent to {s} Channels!")

@app.on_message(filters.command("allred") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_allred(c, m):
    await get_db(); p = m.text.split(); lim, rng = int(p[1]), p[2].split("-")
    code = "RND" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_random": True, "min": int(rng[0]), "max": int(rng[1])})
    await m.reply(f"✅ Code: `{code}` | Limit: {lim}")

# ==========================================
# 10. FLASK WEB API
# ==========================================
@web.route('/api/get_ad/<int:user_id>/<file_id>')
def get_ad_api(user_id, file_id):
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": user_id}) or {}
    file_data = None
    for d in get_extra_dbs_sync():
        file_data = d["files"].find_one({"_id": file_id})
        if file_data: break
    if user.get("premium_until") and user["premium_until"] > datetime.now():
        return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})
    if file_id in user.get("unlocked_files", {}):
        try:
            if datetime.fromisoformat(user["unlocked_files"][file_id].replace("Z", "")) > datetime.utcnow():
                return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})
        except: pass
    if file_data and file_data.get("is_premium"):
        return jsonify({"show_ad": False, "is_unlocked": False, "requires_coin": True, "coin_price": config.get("premium_vid_coin", 50)})
    zones = [z["zone_id"] for z in sync_db["zone_ids"].find()]
    links = list(sync_db["ad_links"].find())
    if config.get("ads_on", True) and (links or zones):
        return jsonify({"show_ad": True, "ad_link": random.choice(links)["link"] if links else None, "zone_id": random.choice(zones) if zones else None, "wait_time": random.choice(config.get("direk_wait", [5])), "steps": config.get("regstep", 1), "requires_coin": False, "coin_price": config.get("premium_vid_coin", 50)})
    return jsonify({"show_ad": False, "is_unlocked": True})

@web.route('/api/unlock_file', methods=['POST'])
def unlock_file():
    data = request.json; uid, f_id, method = data['uid'], data['file_id'], data['method']
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid}) or {}
    if method == "coin":
        cost = config.get("premium_vid_coin", 50)
        if user.get("balance", 0) < cost: return jsonify({"status": "error", "msg": "❌ পর্যাপ্ত কয়েন নেই!"})
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -cost}})
    relock = config.get("vid_relock_min", 1440)
    exp = (datetime.utcnow() + timedelta(minutes=relock if relock > 0 else 52560000)).isoformat() + "Z"
    sync_db["users"].update_one({"_id": uid}, {"$set": {f"unlocked_files.{f_id}": exp}}, upsert=True)
    return jsonify({"status": "success", "msg": "Unlocked!", "expiry": exp})

@web.route('/api/earn_info/<int:uid>')
def get_earn_info(uid):
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid}) or {}
    today = datetime.now().strftime("%Y-%m-%d")
    if user.get("last_activity_date") != today:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": today, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}}, upsert=True)
        user["spin_count"], user["task_count"], user["custom_tasks_done"] = 0, 0, []
    c_tasks = list(sync_db["custom_tasks"].find())
    for t in c_tasks: t["_id"] = str(t["_id"])
    return jsonify({"spins_done": user.get("spin_count", 0), "spin_limit": config.get("spin_limit", 5), "tasks_done": user.get("task_count", 0), "task_limit": config.get("task_limit", 5), "custom_tasks": c_tasks, "custom_tasks_done": user.get("custom_tasks_done", [])})

@web.route('/api/get_earn_ad/<ad_type>')
def get_earn_ad_dynamic(ad_type):
    links = list(sync_db["ad_links"].find())
    zones = [z["zone_id"] for z in sync_db["zone_ids"].find()]
    return jsonify({"ad_link": random.choice(links)["link"] if links else None, "zone_id": random.choice(zones) if zones else None, "wait_time": 10})

@web.route('/api/claim_earn', methods=['POST'])
def claim_earn():
    data = request.json; uid, tt = data['uid'], data['type']
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid}) or {}
    today = datetime.now().strftime("%Y-%m-%d")
    if user.get("last_activity_date") != today:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": today, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}}, upsert=True)
        user["spin_count"], user["task_count"], user["custom_tasks_done"] = 0, 0, []
    if tt == "spin":
        if user.get("spin_count", 0) >= config.get("spin_limit", 5): return jsonify({"status": "error", "msg": "❌ আজকের স্পিন লিমিট শেষ!"})
        coin = random.randint(config.get("spin_coin_min", 10), config.get("spin_coin_max", 50))
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin, "spin_count": 1}})
        return jsonify({"status": "success", "msg": f"🎉 স্পিন করে {coin} কয়েন পেয়েছেন!"})
    elif tt == "task":
        if user.get("task_count", 0) >= config.get("task_limit", 5): return jsonify({"status": "error", "msg": "❌ আজকের টাস্ক লিমিট শেষ!"})
        coin = config.get("task_coin", 20)
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin, "task_count": 1}})
        return jsonify({"status": "success", "msg": f"🎉 {coin} কয়েন পেয়েছেন!"})
    elif tt.startswith("custom_"):
        tid = tt.split("_")[1]
        if tid in user.get("custom_tasks_done", []): return jsonify({"status": "error", "msg": "❌ আগেই করেছেন!"})
        td = sync_db["custom_tasks"].find_one({"_id": ObjectId(tid)})
        if td:
            sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": td["coin"]}, "$push": {"custom_tasks_done": tid}})
            return jsonify({"status": "success", "msg": f"🎉 {td['coin']} কয়েন পেয়েছেন!"})
    return jsonify({"status": "error", "msg": "Invalid Task"})

@web.route('/api/redeem', methods=['POST'])
def redeem_coupon():
    data = request.json; uid, code = data['uid'], data['code'].strip()
    c = sync_db["coupons"].find_one({"code": code})
    if not c: return jsonify({"status": "error", "msg": "❌ কোডটি সঠিক নয়!"})
    if uid in c.get("used_by", []): return jsonify({"status": "error", "msg": "❌ আপনি এটি ব্যবহার করেছেন!"})
    if len(c.get("used_by", [])) >= c.get("limit", 0): return jsonify({"status": "error", "msg": "❌ কোডের মেয়াদ শেষ!"})
    sync_db["coupons"].update_one({"code": code}, {"$push": {"used_by": uid}})
    if c.get("is_premium_code"):
        secs = c.get("duration_seconds", 86400); user = sync_db["users"].find_one({"_id": uid}) or {}
        base = user.get("premium_until") if user.get("premium_until") and user["premium_until"] > datetime.now() else datetime.now()
        sync_db["users"].update_one({"_id": uid}, {"$set": {"premium_until": base + timedelta(seconds=secs)}}, upsert=True)
        lbl = c.get("duration_label", "Premium"); send_tg_api_fallback(uid, f"🎉 রিডিম সফল! **{lbl}** প্রিমিয়াম চালু হয়েছে!")
        return jsonify({"status": "success", "msg": f"✅ Activated {lbl} Premium!"})
    coin = random.randint(c["min"], c["max"]) if c.get("is_random") else c.get("coins", 0)
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin}}, upsert=True)
    send_tg_api_fallback(uid, f"🎉 কুপন রিডিম সফল! +{coin} Coins")
    return jsonify({"status": "success", "msg": f"✅ Redeemed {coin} coins!"})

@web.route('/api/buy_with_coin', methods=['POST'])
def buy_with_coin():
    data = request.json; uid, pkg_id = data['uid'], data['pkg_id']
    user, pkg = sync_db["users"].find_one({"_id": uid}), sync_db["packages"].find_one({"_id": ObjectId(pkg_id)})
    if not user or not pkg: return jsonify({"status": "error", "msg": "Invalid Request"})
    if user.get("balance", 0) < pkg["coins"]: return jsonify({"status": "error", "msg": "❌ পর্যাপ্ত কয়েন নেই!"})
    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(pkg.get("unit", "d").lower()[0], 86400)
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -pkg["coins"]}, "$set": {"premium_until": datetime.now() + timedelta(seconds=pkg["amount"] * secs)}})
    send_tg_api_fallback(uid, f"🎉 {pkg['coins']} কয়েন দিয়ে প্রিমিয়াম কিনেছেন!")
    return jsonify({"status": "success", "msg": "✅ Premium Purchased!"})

@web.route('/api/request_movie', methods=['POST'])
def request_movie():
    data = request.json; name = (data.get("movie_name") or "").strip()
    if not name: return jsonify({"status": "error", "msg": "❌ মুভির নাম লিখুন!"})
    send_tg_api_fallback(ADMIN_ID, f"📥 **New Request!**\n🎬 **{name}**\n👤 `{data.get('uid')}`")
    return jsonify({"status": "success", "msg": "✅ রিকোয়েস্ট পাঠানো হয়েছে!"})

@web.route('/api/action', methods=['POST'])
def handle_actions():
    data = request.json; action, uid, f_id = data.get('action'), data.get('uid'), data.get('file_id')
    if action == "history": sync_db["users"].update_one({"_id": uid}, {"$addToSet": {"history": f_id}}, upsert=True)
    elif action == "favorite":
        user = sync_db["users"].find_one({"_id": uid}) or {}
        if f_id in user.get("favorites", []):
            sync_db["users"].update_one({"_id": uid}, {"$pull": {"favorites": f_id}}); return jsonify({"status": "success", "saved": False})
        sync_db["users"].update_one({"_id": uid}, {"$addToSet": {"favorites": f_id}}, upsert=True); return jsonify({"status": "success", "saved": True})
    elif action == "like":
        for d in get_extra_dbs_sync():
            if d["files"].update_one({"_id": f_id}, {"$addToSet": {"likes": uid}}).modified_count > 0: break
    elif action == "rate":
        stars = max(1, min(5, int(data.get("stars", 5))))
        for d in get_extra_dbs_sync():
            doc = d["files"].find_one({"_id": f_id})
            if doc:
                ratings = [r for r in doc.get("ratings", []) if r.get("uid") != uid]
                ratings.append({"uid": uid, "stars": stars})
                d["files"].update_one({"_id": f_id}, {"$set": {"ratings": ratings}})
                return jsonify({"status": "success", "avg": round(sum(r["stars"] for r in ratings)/len(ratings), 1), "count": len(ratings)})
    elif action == "comment":
        cmt = {"uid": uid, "text": data.get("text"), "time": datetime.now().strftime("%Y-%m-%d %H:%M")}
        for d in get_extra_dbs_sync():
            if d["files"].update_one({"_id": f_id}, {"$push": {"comments": cmt}}).modified_count > 0: break
        return jsonify({"status": "success", "comment": cmt})
    return jsonify({"status": "success"})

@web.route('/api/user/<int:user_id>')
def get_user(user_id):
    user = sync_db["users"].find_one({"_id": user_id})
    is_prem, expiry_str = False, None
    if user and user.get("premium_until") and user["premium_until"] > datetime.now():
        is_prem = True; expiry_str = get_expiry_str(user["premium_until"])
    return jsonify({"balance": user.get("balance", 0) if user else 0, "is_premium": is_prem, "expiry": expiry_str, "history": user.get("history", []) if user else [], "favorites": user.get("favorites", []) if user else [], "unlocked_files": user.get("unlocked_files", {}) if user else {}})

# ==========================================
# 11. HTML TEMPLATE (MATCHING SCREENSHOTS 1, 2, 3, 6 + MONETAG)
# ==========================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>{{ site_name }}</title>
<link href="https://fonts.googleapis.com/css2?family=Hind+Siliguri:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://telegram.org/js/telegram-web-app.js"></script>
{% for z in zone_ids %}<script src='//libtl.com/sdk.js' data-zone='{{ z }}' data-sdk='show_{{ z }}'></script>{% endfor %}
<style>
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
body{background:#f4f5f7;color:#111;font-family:'Hind Siliguri',sans-serif;margin:0;padding-bottom:92px;min-height:100vh}
.top-header{position:sticky;top:0;z-index:90;background:#fff;padding:10px 16px;display:flex;justify-content:space-between;align-items:center;box-shadow:0 1px 6px rgba(0,0,0,.05)}
.brand{display:flex;align-items:center;gap:6px;font-size:20px;font-weight:700}
.brand-badge{background:#d90429;color:#fff;font-size:13px;padding:2px 8px;border-radius:6px}
.hdr-r{display:flex;align-items:center;gap:8px}
.coin-pill{background:#fff3cd;color:#856404;border:1px solid #ffeeba;padding:4px 10px;border-radius:16px;font-size:12.5px;font-weight:700}
.r-search{width:38px;height:38px;border-radius:50%;padding:2.5px;background:conic-gradient(#ff4d4d,#ffb703,#00d4ff,#6c5ce7,#ff4d4d);display:flex;align-items:center;justify-content:center;cursor:pointer}
.r-search-in{width:100%;height:100%;background:#fff;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:16px}
.live-bar{background:linear-gradient(90deg,#4b39ef,#7c3aed);color:#fff;display:flex;align-items:center;height:34px;overflow:hidden}
.live-pill{display:flex;align-items:center;gap:5px;padding:0 10px;font-size:11px;font-weight:700;background:rgba(0,0,0,.22);height:100%;white-space:nowrap}
.live-dot{width:8px;height:8px;background:#ff3b30;border-radius:50%}
.marquee-wrap{flex:1;overflow:hidden;white-space:nowrap}
.marquee-txt{display:inline-block;padding-left:100%;font-size:12.5px;animation:scr 18s linear infinite}
@keyframes scr{0%{transform:translateX(0)}100%{transform:translateX(-100%)}}
.search-box-wrap{display:none;padding:10px 14px;background:#fff;border-bottom:1px solid #eee}
.search-box-wrap.open{display:block}
.s-input{width:100%;padding:10px 14px;border-radius:12px;border:1.5px solid #e2e8f0;background:#f8fafc;font-size:14px;outline:none}
.cat-scroll{display:flex;gap:8px;overflow-x:auto;padding:10px 14px 4px 14px;scrollbar-width:none}
.cat-scroll::-webkit-scrollbar{display:none}
.cat-pill{background:#fff;color:#333;padding:7px 15px;border-radius:20px;font-size:13px;font-weight:600;white-space:nowrap;cursor:pointer;box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #ececec}
.cat-pill.active{background:linear-gradient(90deg,#ff512f,#dd2476);color:#fff;border-color:transparent}
.cat-pill.adult{background:#5e0b2b;color:#fff;border-color:transparent}
.lang-pill{background:#eef2ff;color:#4338ca;padding:4px 11px;border-radius:14px;font-size:11.5px;font-weight:600;white-space:nowrap;cursor:pointer}
.lang-pill.active{background:#4338ca;color:#fff}
.page{display:none}.page.active{display:block}
.sec-head{padding:8px 14px 6px 14px;font-size:15px;font-weight:700}
.pin-scroll{display:flex;gap:10px;overflow-x:auto;padding:0 14px 10px 14px;scrollbar-width:none}
.pin-scroll::-webkit-scrollbar{display:none}
.pin-card{min-width:160px;max-width:160px;cursor:pointer}
.pin-img-w{position:relative;width:100%;height:94px;border-radius:12px;overflow:hidden;box-shadow:0 3px 10px rgba(0,0,0,.08)}
.pin-img-w img{width:100%;height:100%;object-fit:cover}
.pin-tag{position:absolute;top:6px;left:6px;background:rgba(0,0,0,.72);color:#fff;font-size:10px;padding:2px 7px;border-radius:10px}
.pin-t{font-size:12.5px;font-weight:600;margin-top:5px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.feed-wrap{padding:4px 14px}
.feed-card{background:#fff;border-radius:16px;margin-bottom:15px;overflow:hidden;border:1.5px solid #7c6cf6;box-shadow:0 4px 14px rgba(108,92,231,.08);cursor:pointer}
.feed-img-w{position:relative;width:100%;height:200px;background:#111}
.feed-img-w img{width:100%;height:100%;object-fit:cover}
.b-new{position:absolute;top:10px;left:10px;background:#2979ff;color:#fff;font-size:11px;font-weight:700;padding:3px 9px;border-radius:8px}
.b-type{position:absolute;top:10px;right:10px;background:rgba(0,0,0,.7);color:#fff;font-size:10.5px;padding:3px 8px;border-radius:8px}
.feed-body{padding:10px 12px 12px 12px}
.feed-title{font-size:15px;font-weight:700;margin-bottom:3px}
.feed-meta{font-size:12px;color:#777;display:flex;justify-content:space-between}
.car-wrap{display:flex;gap:12px;overflow-x:auto;padding:4px 0 12px 0;scrollbar-width:none}
.car-wrap::-webkit-scrollbar{display:none}
.p-slide{min-width:78%;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,.08);border:1px solid #eee}
.p-slide img{width:100%;height:140px;object-fit:cover}
.p-slide-b{padding:10px 12px}
.watch-pill{background:#e63946;color:#fff;border:none;padding:6px 16px;border-radius:20px;font-size:12.5px;font-weight:700;cursor:pointer}
.det-hdr{background:#fff;padding:12px 14px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #eee;position:sticky;top:0;z-index:95}
.back-btn{width:34px;height:34px;border-radius:50%;background:#f1f3f5;border:none;font-size:18px;cursor:pointer}
.det-img-w{position:relative;width:100%;height:220px;background:#000}
.det-img-w img{width:100%;height:100%;object-fit:cover}
.hd-b{position:absolute;top:12px;left:12px;background:#ffb703;color:#000;font-weight:800;font-size:11px;padding:3px 9px;border-radius:6px}
.det-body{background:#fff;padding:14px 16px}
.det-title{font-size:19px;font-weight:700;margin:0 0 8px 0}
.d-tags{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px}
.d-tag{background:#f1f5f9;color:#475569;padding:3px 10px;border-radius:12px;font-size:11.5px;font-weight:600}
.stats-r{display:flex;justify-content:space-around;align-items:center;padding:10px 0;border-top:1px solid #f1f5f9;border-bottom:1px solid #f1f5f9;margin-bottom:12px}
.stat-i{text-align:center;font-size:12px;color:#64748b}.stat-i b{display:block;font-size:15px;color:#111}
.shr-c{width:38px;height:38px;border-radius:50%;background:#eef2ff;color:#4f46e5;border:1px solid #c7d2fe;font-size:16px;cursor:pointer}
.tip-ban{background:#fff8eb;border:1px solid #fde68a;color:#92400e;padding:9px 12px;border-radius:10px;font-size:13px;font-weight:600;text-align:center;margin-bottom:10px}
.tut-btn{display:block;margin:0 auto 14px auto;background:#1d4ed8;color:#fff;border:none;padding:8px 18px;border-radius:22px;font-size:13px;font-weight:600;cursor:pointer}
.act-r{display:flex;gap:10px;margin-bottom:14px}
.p-like{flex:1;background:#fff;border:1.5px solid #fda4af;color:#e11d48;padding:10px;border-radius:25px;font-weight:700;cursor:pointer}
.p-dl{flex:1.2;background:#0f172a;border:none;color:#fff;padding:10px;border-radius:25px;font-weight:700;cursor:pointer}
.p-save{flex:1;background:#fff;border:1.5px solid #fed7aa;color:#d97706;padding:10px;border-radius:25px;font-weight:700;cursor:pointer}
.sel-box{background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:12px;margin-bottom:14px}
.q-grid{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.q-btn{background:linear-gradient(90deg,#4f46e5,#7c3aed);color:#fff;border:none;padding:8px 14px;border-radius:10px;font-size:13px;font-weight:700;cursor:pointer}
.s-tab{background:#fff;border:1px solid #cbd5e1;color:#334155;padding:6px 12px;border-radius:10px;font-size:12.5px;font-weight:700;cursor:pointer}
.s-tab.active{background:#111;color:#fff;border-color:#111}
.ep-card{background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:10px;margin-top:8px}
.rate-c{background:#fffbeb;border:1px solid #fde68a;border-radius:14px;padding:10px 14px;display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;font-size:13px;font-weight:600}
.star-b{font-size:20px;color:#cbd5e1;background:none;border:none;cursor:pointer;padding:0 2px}.star-b.lit{color:#f59e0b}
.cmt-sec{background:#fff;margin-top:10px;padding:14px 16px 80px 16px}
.cmt-bar{position:fixed;bottom:0;left:0;width:100%;background:#fff;padding:10px 14px;border-top:1px solid #eee;display:none;gap:10px;z-index:120}
.cmt-bar input{flex:1;background:#f1f5f9;border:none;border-radius:22px;padding:10px 16px;outline:none;font-size:14px}
.cmt-bar button{width:40px;height:40px;border-radius:50%;background:#111;color:#fff;border:none;font-size:16px;cursor:pointer}
.card-box{background:#fff;border-radius:16px;padding:16px;margin:12px 14px;box-shadow:0 2px 10px rgba(0,0,0,.05);border:1px solid #eee}
.btn-p{width:100%;background:linear-gradient(90deg,#6c5ce7,#4f46e5);color:#fff;border:none;padding:12px;border-radius:12px;font-weight:700;font-size:14.5px;cursor:pointer}
.float-search{position:fixed;bottom:84px;right:16px;z-index:95;width:46px;height:46px;border-radius:50%;padding:3px;background:conic-gradient(#ff4d4d,#ffb703,#00d4ff,#6c5ce7,#ff4d4d);box-shadow:0 4px 14px rgba(0,0,0,.2);cursor:pointer}
.bottom-nav{position:fixed;bottom:10px;left:12px;right:12px;background:rgba(255,255,255,.94);backdrop-filter:blur(12px);border-radius:28px;display:flex;justify-content:space-around;padding:8px 6px;box-shadow:0 6px 24px rgba(0,0,0,.12);z-index:100;border:1px solid rgba(255,255,255,.8)}
.nav-i{display:flex;flex-direction:column;align-items:center;font-size:11px;color:#64748b;cursor:pointer;font-weight:600;padding:2px 8px}
.nav-i.active{color:#d90429}.nav-i span{font-size:20px;margin-bottom:2px}
.av-badge{width:24px;height:24px;border-radius:50%;background:linear-gradient(135deg,#f43f5e,#8b5cf6);color:#fff;font-size:10px;display:flex;align-items:center;justify-content:center;font-weight:700;margin-bottom:2px}
.modal-ov{display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);z-index:9999;justify-content:center;align-items:center;backdrop-filter:blur(4px)}
.modal-card{background:#fff;color:#111;border-radius:22px;padding:24px 20px;width:90%;max-width:380px;text-align:center;box-shadow:0 15px 40px rgba(0,0,0,.2)}
.wel-item{text-align:left;font-size:13.5px;line-height:1.55;margin-bottom:11px;color:#334155}
</style>
</head>
<body>

<div id="main-chrome">
  <div class="top-header">
    <div class="brand" id="brand-title"></div>
    <div class="hdr-r">
      <div class="coin-pill">🪙 <span id="hdr-bal">0</span></div>
      <div class="r-search" onclick="toggleSearch()"><div class="r-search-in">🔍</div></div>
    </div>
  </div>
  <div class="live-bar">
    <div class="live-pill"><span class="live-dot"></span> LIVE</div>
    <div class="marquee-wrap"><div class="marquee-txt">{{ marquee_text }}</div></div>
  </div>
  <div class="search-box-wrap" id="search-drawer">
    <input type="text" id="search-inp" class="s-input" placeholder="🔍 মুভি বা সিরিজ খুঁজুন..." onkeyup="renderFeed()">
  </div>
</div>

<!-- Welcome Modal (Screenshot 1) -->
<div id="welcome-modal" class="modal-ov">
  <div class="modal-card">
    <h2 style="margin:0 0 14px 0;font-size:22px;">👋 Welcome to<br>{{ site_name }} Bot!</h2>
    <div class="wel-item">🎬 এখানে আপনি হাজারো Movie, Web Series, Drama ও Anime দেখতে পারবেন।</div>
    <div class="wel-item">🔍 আপনার পছন্দের Movie খুঁজতে Search ব্যবহার করুন।</div>
    <div class="wel-item">🎵 ভিডিওতে সাউন্ড বা অডিও ট্র্যাক সংক্রান্ত সমস্যা হলে, ভিডিওটি ডাউনলোড করে VLC Player অথবা MX Player দিয়ে প্লে করুন।</div>
    <div class="wel-item">📥 কোনো Movie খুঁজে না পেলে Tools থেকে Movie Request পাঠান।</div>
    <div class="wel-item">🍿 Happy Watching! ❤️</div>
    <button class="btn-p" style="margin-top:8px;" onclick="closeWelcome()">OK ✅</button>
  </div>
</div>

<!-- Ad Timer Fallback Modal -->
<div id="ad-modal" class="modal-ov">
  <div class="modal-card">
    <h3 style="margin-top:0;color:#4f46e5;">🚀 Ad Verification</h3>
    <p id="ad-step-txt" style="font-size:13px;color:#64748b;">Step 1</p>
    <h1 id="ad-timer" style="font-size:46px;color:#e11d48;margin:10px 0;">5</h1>
    <p style="font-size:12.5px;color:#64748b;">সম্পূর্ণ অ্যাড দেখার পর নিচের বাটনে ক্লিক করুন</p>
    <button id="ad-action-btn" class="btn-p" style="display:none;">Continue</button>
  </div>
</div>

<!-- Home Page (Screenshots 3 & 6) -->
<div id="page-home" class="page active">
  <div class="cat-scroll" id="cat-pills"></div>
  <div class="cat-scroll" id="lang-pills" style="padding-top:2px;"></div>
  <div id="pinned-section">
    <div class="sec-head">📌 Pinned for you</div>
    <div class="pin-scroll" id="pinned-list"></div>
  </div>
  <div class="feed-wrap" id="feed-list"></div>
</div>

<!-- Video Details Page (Screenshot 2) -->
<div id="page-details" class="page">
  <div class="det-hdr">
    <button class="back-btn" onclick="closeDetails()">❮</button>
    <b style="font-size:16px;">Video Details</b>
  </div>
  <div class="det-img-w">
    <img id="det-poster" src="">
    <div class="hd-b">HD</div>
  </div>
  <div class="det-body">
    <h2 class="det-title" id="det-title"></h2>
    <div class="d-tags" id="det-tags"></div>
    <div class="stats-r">
      <div class="stat-i">❤️<b id="det-likes">0</b>Like</div>
      <div class="stat-i">⬇️<b id="det-views">0</b>Download</div>
      <div class="stat-i">💬<b id="det-cmts-cnt">0</b>Comments</div>
      <button class="shr-c" onclick="shareCurrent()">🔗</button>
    </div>
    <div class="tip-ban">💡 Tap the button below to download easily</div>
    <button class="tut-btn" onclick="openBotTutorial()">❓ Need help? Watch the tutorial</button>
    <div class="act-r">
      <button class="p-like" id="btn-like" onclick="likeCurrent()">🤍 Like</button>
      <button class="p-dl" onclick="scrollToDownload()">⬇️ Download</button>
      <button class="p-save" id="btn-save" onclick="saveCurrent()">🔖 Save</button>
    </div>
    <div id="det-selector-area"></div>
    <div class="rate-c">
      <span>Rate it: <span id="star-row"></span></span>
      <span id="rate-avg">⭐ 0.0 (0)</span>
    </div>
  </div>
  <div class="cmt-sec">
    <h4 style="margin:0 0 10px 0;">Comments (<span id="cmt-cnt-h">0</span>)</h4>
    <div id="det-cmt-list"></div>
  </div>
</div>
<div class="cmt-bar" id="cmt-bar">
  <input type="text" id="cmt-inp" placeholder="Add a public comment...">
  <button onclick="sendComment()">➤</button>
</div>

<!-- Favorite Page -->
<div id="page-favorite" class="page">
  <div class="sec-head">🔖 Saved Favorites & Watch History</div>
  <div class="feed-wrap" id="fav-list"></div>
</div>

<!-- Tools Page (Earn Coins + Movie Request + Redeem) -->
<div id="page-tools" class="page">
  <div class="card-box">
    <h3 style="margin:0 0 8px 0;">📥 Movie / Series Request</h3>
    <div style="display:flex;gap:8px;">
      <input type="text" id="req-inp" class="s-input" placeholder="নাম ও সাল লিখুন...">
      <button class="btn-p" style="width:auto;padding:0 18px;" onclick="sendMovieRequest()">Send</button>
    </div>
  </div>
  <div class="card-box">
    <h3 style="margin:0 0 8px 0;">🎟 Redeem Code</h3>
    <div style="display:flex;gap:8px;">
      <input type="text" id="coupon-inp" class="s-input" placeholder="কোড লিখুন...">
      <button class="btn-p" style="width:auto;padding:0 18px;" onclick="redeemCoupon()">Redeem</button>
    </div>
  </div>
  <div id="earn-area"></div>
</div>

<!-- Upcoming / Premium Packages Page -->
<div id="page-upcoming" class="page">
  <div class="sec-head">📅 Promotions & VIP Packages</div>
  <div class="feed-wrap" id="promo-full-list"></div>
  <div class="card-box">
    <h3 style="margin-top:0;">👑 Buy VIP / Coins</h3>
    {% for pkg in pkgs %}
    <div style="padding:10px;border:1px solid #eee;border-radius:12px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center;">
      <div><b>{{ pkg.details }}</b><br><small style="color:#64748b;">{{ pkg.type|upper }}</small></div>
      {% if pkg.type == 'coin' %}
      <button class="watch-pill" style="background:#6c5ce7;" onclick="buyWithCoin('{{ pkg._id }}', {{ pkg.coins }})">Exchange</button>
      {% else %}
      <button class="watch-pill" onclick="openTgLink('https://t.me/{{ admin_username }}')">Buy Now</button>
      {% endif %}
    </div>
    {% endfor %}
  </div>
</div>

<!-- Setting / Profile Page -->
<div id="page-setting" class="page">
  <div class="card-box" style="text-align:center;">
    <img id="u-pic" src="https://placehold.co/100x100/6c5ce7/ffffff?text=User" style="width:76px;height:76px;border-radius:50%;object-fit:cover;border:3px solid #6c5ce7;">
    <h3 id="u-name" style="margin:8px 0 2px 0;">User</h3>
    <div style="font-size:12px;color:#64748b;">ID: <span id="u-id"></span></div>
    <div id="u-prem-status" style="display:inline-block;margin-top:8px;padding:4px 12px;border-radius:14px;font-size:12px;font-weight:700;background:#f1f5f9;">👤 Regular Member</div>
    <div id="u-prem-exp" style="font-size:11.5px;color:#d97706;margin-top:4px;"></div>
    <h2 style="margin:12px 0 0 0;color:#4f46e5;">🪙 <span id="u-bal">0</span> Coins</h2>
  </div>
  <div class="card-box">
    <h4 style="margin:0 0 8px 0;">🎁 Refer & Earn ({{ ref_coin }} Coins)</h4>
    <div style="display:flex;gap:8px;">
      <input type="text" id="ref-inp" class="s-input" readonly>
      <button class="btn-p" style="width:auto;padding:0 16px;" onclick="copyRef()">Copy</button>
    </div>
  </div>
</div>

<div class="float-search" id="float-search-btn" onclick="toggleSearch()"><div class="r-search-in">🔍</div></div>

<div class="bottom-nav" id="bottom-nav">
  <div class="nav-i active" onclick="switchTab('home', this)"><span>🏠</span>Home</div>
  <div class="nav-i" onclick="switchTab('favorite', this)"><span>🔖</span>Favorite</div>
  <div class="nav-i" onclick="switchTab('tools', this)"><span>🛠️</span>Tools</div>
  <div class="nav-i" onclick="switchTab('upcoming', this)"><span>📅</span>Upcoming</div>
  <div class="nav-i" onclick="switchTab('setting', this)"><div class="av-badge" id="nav-av">U</div>Setting</div>
</div>

<script>
const tg = window.Telegram.WebApp;
tg.expand();
const botUsername = "{{ bot_username }}";
const siteName = "{{ site_name }}";
const allFiles = {{ files_json | safe }};
const categories = {{ categories_json | safe }};
const promotions = {{ promotions_json | safe }};
const zoneIds = {{ zone_ids_json | safe }};

// Brand split styling (Screenshot 1 & 3)
(function(){
  const parts = siteName.trim().split(/\s+/);
  const el = document.getElementById('brand-title');
  if(parts.length > 1){
    el.innerHTML = `${parts[0]} <span class="brand-badge">${parts.slice(1).join(' ')}</span>`;
  } else {
    el.innerHTML = `${siteName} <span class="brand-badge">Bot</span>`;
  }
})();

let userId = 123456789;
const urlP = new URLSearchParams(window.location.search);
if(tg?.initDataUnsafe?.user){
  const u = tg.initDataUnsafe.user;
  userId = u.id;
  localStorage.setItem("tg_user_id", userId);
  const fn = (u.first_name + " " + (u.last_name || "")).trim();
  document.getElementById('u-name').innerText = fn;
  document.getElementById('nav-av').innerText = fn.slice(0,2).toUpperCase();
  if(u.photo_url) document.getElementById('u-pic').src = u.photo_url;
} else if(urlP.get('uid')) {
  userId = parseInt(urlP.get('uid')); localStorage.setItem("tg_user_id", userId);
} else if(localStorage.getItem("tg_user_id")) {
  userId = parseInt(localStorage.getItem("tg_user_id"));
}
document.getElementById('u-id').innerText = userId;
document.getElementById('ref-inp').value = `https://t.me/${botUsername}?start=${userId}`;

if(!sessionStorage.getItem('wel_seen')) document.getElementById('welcome-modal').style.display = 'flex';
function closeWelcome(){ sessionStorage.setItem('wel_seen','1'); document.getElementById('welcome-modal').style.display = 'none'; }

let userState = {balance:0, is_premium:false, expiry:null, history:[], favorites:[], unlocked_files:{}};
let activeCat = 'All', activeLang = 'All', curFile = null, curSeasonIdx = 0;

function openTgLink(url){
  try { if(tg?.initDataUnsafe?.user) tg.openTelegramLink(url); else window.location.href = url; }
  catch(e){ window.location.href = url; }
}

async function loadUser(){
  try{
    const r = await fetch('/api/user/' + userId);
    userState = await r.json();
    document.getElementById('hdr-bal').innerText = userState.balance;
    document.getElementById('u-bal').innerText = userState.balance;
    if(userState.is_premium){
      document.getElementById('u-prem-status').innerHTML = '💎 Premium Member';
      document.getElementById('u-prem-status').style.background = '#ede9fe';
      document.getElementById('u-prem-status').style.color = '#5b21b6';
      document.getElementById('u-prem-exp').innerText = 'Expiry: ' + (userState.expiry || '');
    }
    renderFavs();
  }catch(e){}
}
loadUser();

function toggleSearch(){
  const d = document.getElementById('search-drawer');
  d.classList.toggle('open');
  if(d.classList.contains('open')) document.getElementById('search-inp').focus();
}

function renderFilters(){
  let cHtml = `<div class="cat-pill ${activeCat==='All'?'active':''}" onclick="setCat('All')">All</div>`;
  categories.forEach(c => {
    const is18 = c.includes('18');
    cHtml += `<div class="cat-pill ${activeCat===c?'active':(is18?'adult':'')}" onclick="setCat('${c.replace(/'/g,"\\'")}')">${is18?'🔞 ':''}${c}</div>`;
  });
  document.getElementById('cat-pills').innerHTML = cHtml;

  const langs = ['All','Bangla','Hindi','Hindi Dubbed','English','Dual Audio','Tamil','Korean'];
  document.getElementById('lang-pills').innerHTML = langs.map(l =>
    `<div class="lang-pill ${activeLang===l?'active':''}" onclick="setLang('${l}')">${l==='All'?'All Languages':l}</div>`
  ).join('');
}
function setCat(c){ activeCat = c; renderFilters(); renderFeed(); }
function setLang(l){ activeLang = l; renderFilters(); renderFeed(); }
renderFilters();

function renderPinned(){
  let pins = allFiles.filter(f => f.pinned);
  if(pins.length === 0) pins = allFiles.slice(0, 6);
  document.getElementById('pinned-list').innerHTML = pins.map(f => `
    <div class="pin-card" onclick="openDetails('${f._id}')">
      <div class="pin-img-w">
        <img src="${f.thumb_url || 'https://placehold.co/300x180?text=Video'}">
        <div class="pin-tag">📌 Pinned</div>
      </div>
      <div class="pin-t">${f.title}</div>
    </div>`).join('');
}
renderPinned();

function timeAgo(iso){
  if(!iso) return 'Recently';
  const sec = Math.floor((new Date() - new Date(iso))/1000);
  if(sec < 3600) return Math.max(1, Math.floor(sec/60)) + ' min ago';
  if(sec < 86400) return Math.floor(sec/3600) + ' hr ago';
  return new Date(iso).toLocaleDateString();
}

function renderFeed(){
  const q = (document.getElementById('search-inp').value || '').toLowerCase();
  const list = allFiles.filter(f => {
    if(activeCat !== 'All' && (f.category || 'All') !== activeCat) return false;
    if(activeLang !== 'All' && (f.language || '') !== activeLang) return false;
    if(q && !f.title.toLowerCase().includes(q)) return false;
    return true;
  });
  let html = '';
  list.forEach((f, idx) => {
    const tLabel = f.type === 'series' ? '📺 Series' : (f.is_premium ? '💎 VIP' : '🎬 Movie');
    html += `
    <div class="feed-card" onclick="openDetails('${f._id}')">
      <div class="feed-img-w">
        <img src="${f.thumb_url || 'https://placehold.co/600x340?text=Video'}">
        <div class="b-new">🆕 New</div>
        <div class="b-type">${tLabel}</div>
      </div>
      <div class="feed-body">
        <div class="feed-title">${f.title}</div>
        <div class="feed-meta"><span>${timeAgo(f.created_at)}</span><span>${f.category || ''} ${f.language ? '• '+f.language : ''}</span></div>
      </div>
    </div>`;
    if((idx + 1) % 3 === 0 && promotions.length > 0){
      html += `<div class="car-wrap">` + promotions.map(p => `
        <div class="p-slide" onclick="openTgLink('${p.link}')">
          <img src="${p.image}">
          <div class="p-slide-b">
            <div class="promo-slide-title">${p.title}</div>
            <button class="watch-pill">Watch Now</button>
          </div>
        </div>`).join('') + `</div>`;
    }
  });
  document.getElementById('feed-list').innerHTML = html || "<p style='text-align:center;color:#777;padding:30px;'>কোনো ভিডিও পাওয়া যায়নি!</p>";
}
renderFeed();

function renderFavs(){
  const ids = [...new Set([...(userState.favorites||[]), ...(userState.history||[])])];
  const items = allFiles.filter(f => ids.includes(f._id));
  document.getElementById('fav-list').innerHTML = items.map(f => `
    <div class="feed-card" onclick="openDetails('${f._id}')">
      <div class="feed-img-w"><img src="${f.thumb_url}"><div class="b-new">🔖 Saved</div></div>
      <div class="feed-body"><div class="feed-title">${f.title}</div></div>
    </div>`).join('') || "<p style='text-align:center;color:#777;padding:30px;'>কোনো সেভ করা ভিডিও নেই!</p>";
}

function renderPromos(){
  document.getElementById('promo-full-list').innerHTML = promotions.map(p => `
    <div class="feed-card" onclick="openTgLink('${p.link}')">
      <div class="feed-img-w"><img src="${p.image}"><div class="b-new">⭐ PROMO</div></div>
      <div class="feed-body"><div class="feed-title">${p.title}</div></div>
    </div>`).join('');
}
renderPromos();

function openDetails(id){
  curFile = allFiles.find(f => f._id === id);
  if(!curFile) return;
  curSeasonIdx = 0;
  document.getElementById('main-chrome').style.display = 'none';
  document.getElementById('bottom-nav').style.display = 'none';
  document.getElementById('float-search-btn').style.display = 'none';
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page-details').classList.add('active');
  document.getElementById('cmt-bar').style.display = 'flex';
  window.scrollTo(0, 0);

  document.getElementById('det-poster').src = curFile.thumb_url || 'https://placehold.co/600x340?text=Video';
  document.getElementById('det-title').innerText = curFile.title;
  document.getElementById('det-likes').innerText = (curFile.likes || []).length;
  document.getElementById('det-views').innerText = curFile.views || 0;
  document.getElementById('det-cmts-cnt').innerText = (curFile.comments || []).length;
  document.getElementById('cmt-cnt-h').innerText = (curFile.comments || []).length;

  let tags = [];
  if(curFile.year) tags.push(curFile.year);
  if(curFile.category) tags.push(curFile.category);
  if(curFile.language) tags.push(curFile.language);
  document.getElementById('det-tags').innerHTML = tags.map(t => `<span class="d-tag">${t}</span>`).join('');

  const isFav = (userState.favorites || []).includes(id);
  document.getElementById('btn-save').innerText = isFav ? '✅ Saved' : '🔖 Save';
  const isLiked = (curFile.likes || []).includes(userId);
  document.getElementById('btn-like').innerText = isLiked ? '❤️ Liked' : '🤍 Like';

  renderRating();
  renderComments();
  renderSelectors();
  fetch('/api/action', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'history', uid:userId, file_id:id})});
}

function closeDetails(){
  document.getElementById('main-chrome').style.display = 'block';
  document.getElementById('bottom-nav').style.display = 'flex';
  document.getElementById('float-search-btn').style.display = 'flex';
  document.getElementById('cmt-bar').style.display = 'none';
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page-home').classList.add('active');
}

function renderSelectors(){
  const area = document.getElementById('det-selector-area');
  if(curFile.type === 'series' && curFile.seasons && curFile.seasons.length > 0){
    const ssns = curFile.seasons;
    let html = `<div class="sel-box"><div class="selector-title">📺 Select Season & Episode</div><div style="display:flex;gap:6px;overflow-x:auto;">`;
    ssns.forEach((s, si) => {
      html += `<button class="s-tab ${si===curSeasonIdx?'active':''}" onclick="curSeasonIdx=${si};renderSelectors()">Season ${s.season_no}</button>`;
    });
    html += `</div>`;
    const eps = (ssns[curSeasonIdx] && ssns[curSeasonIdx].episodes) || [];
    eps.forEach((e, ei) => {
      html += `<div class="ep-card"><div class="ep-header">🎬 Episode ${e.ep_no}</div><div class="q-grid">`;
      (e.qualities || []).forEach((q, qi) => {
        html += `<button class="q-btn" onclick="triggerDownload('sfile_${curFile._id}_${curSeasonIdx}_${ei}_${qi}')">⬇️ ${q.name}</button>`;
      });
      html += `</div></div>`;
    });
    area.innerHTML = html + `</div>`;
  } else if(curFile.qualities && curFile.qualities.length > 0){
    let html = `<div class="sel-box"><div class="selector-title">🎞 Select Quality to Download</div><div class="q-grid">`;
    curFile.qualities.forEach((q, qi) => {
      html += `<button class="q-btn" onclick="triggerDownload('mfile_${curFile._id}_${qi}')">⬇️ ${q.name}</button>`;
    });
    area.innerHTML = html + `</div></div>`;
  } else {
    area.innerHTML = `<div class="sel-box"><button class="btn-p" onclick="triggerDownload('file_${curFile._id}')">⬇️ Get File Now</button></div>`;
  }
}

function scrollToDownload(){
  const el = document.getElementById('det-selector-area');
  if(el) el.scrollIntoView({behavior:'smooth'});
}

function openBotTutorial(){ openTgLink(`https://t.me/${botUsername}?start=home`); }

async function triggerDownload(startParam){
  const deepLink = `https://t.me/${botUsername}?start=${startParam}`;
  const res = await fetch(`/api/get_ad/${userId}/${curFile._id}`);
  const ad = await res.json();

  if(ad.requires_coin){
    if(confirm(`এই প্রিমিয়াম ভিডিও আনলক করতে ${ad.coin_price} কয়েন লাগবে। আনলক করবেন?`)){
      const r2 = await fetch('/api/unlock_file', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, file_id:curFile._id, method:'coin'})});
      const d2 = await r2.json();
      if(d2.status === 'success'){ loadUser(); openTgLink(deepLink); }
      else alert(d2.msg);
    }
    return;
  }

  if(!ad.show_ad){ openTgLink(deepLink); return; }

  // Monetag Zone ID SDK Priority
  const zid = ad.zone_id || (zoneIds.length > 0 ? zoneIds[0] : null);
  if(zid && typeof window['show_' + zid] === 'function'){
    try {
      await window['show_' + zid]();
      await fetch('/api/unlock_file', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, file_id:curFile._id, method:'ad'})});
      openTgLink(deepLink);
      return;
    } catch(e){}
  }

  // Fallback Ad Link Timer
  if(ad.ad_link){
    runAdTimer(ad.ad_link, ad.wait_time || 5, async () => {
      await fetch('/api/unlock_file', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, file_id:curFile._id, method:'ad'})});
      openTgLink(deepLink);
    });
  } else {
    openTgLink(deepLink);
  }
}

function runAdTimer(link, sec, onDone){
  const ov = document.getElementById('ad-modal'), tm = document.getElementById('ad-timer'), btn = document.getElementById('ad-action-btn');
  ov.style.display = 'flex'; btn.style.display = 'none'; tm.innerText = sec;
  try { if(tg?.openLink) tg.openLink(link); else window.open(link, '_blank'); } catch(e){}
  let t = sec;
  const iv = setInterval(() => {
    t--; tm.innerText = Math.max(0, t);
    if(t <= 0){
      clearInterval(iv);
      btn.style.display = 'block'; btn.innerText = '✅ Get File';
      btn.onclick = () => { ov.style.display = 'none'; onDone(); };
    }
  }, 1000);
}

function likeCurrent(){
  if(!curFile.likes) curFile.likes = [];
  if(!curFile.likes.includes(userId)){
    curFile.likes.push(userId);
    document.getElementById('det-likes').innerText = curFile.likes.length;
    document.getElementById('btn-like').innerText = '❤️ Liked';
    fetch('/api/action', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'like', uid:userId, file_id:curFile._id})});
  }
}
async function saveCurrent(){
  const r = await fetch('/api/action', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'favorite', uid:userId, file_id:curFile._id})});
  const d = await r.json();
  document.getElementById('btn-save').innerText = d.saved ? '✅ Saved' : '🔖 Save';
  loadUser();
}
function shareCurrent(){
  const l = `https://t.me/${botUsername}?start=ref${userId}file${curFile._id}`;
  openTgLink(`https://t.me/share/url?url=${encodeURIComponent(l)}&text=${encodeURIComponent(curFile.title)}`);
}

function renderRating(){
  const rs = curFile.ratings || [];
  const avg = rs.length ? (rs.reduce((a,b)=>a+b.stars,0)/rs.length).toFixed(1) : '0.0';
  document.getElementById('rate-avg').innerText = `⭐ ${avg} (${rs.length})`;
  const my = (rs.find(r=>r.uid===userId) || {}).stars || 0;
  document.getElementById('star-row').innerHTML = [1,2,3,4,5].map(n =>
    `<button class="star-b ${n<=my?'lit':''}" onclick="rateVideo(${n})">★</button>`).join('');
}
async function rateVideo(stars){
  const r = await fetch('/api/action', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'rate', uid:userId, file_id:curFile._id, stars})});
  const d = await r.json();
  if(!curFile.ratings) curFile.ratings = [];
  curFile.ratings = curFile.ratings.filter(x=>x.uid!==userId);
  curFile.ratings.push({uid:userId, stars});
  renderRating();
}

function renderComments(){
  const cs = curFile.comments || [];
  document.getElementById('det-cmt-list').innerHTML = cs.map(c =>
    `<div style="padding:8px 0;border-bottom:1px solid #f1f5f9;font-size:13.5px;"><b>User ${c.uid}</b> <small style="color:#94a3b8;">${c.time}</small><br>${c.text}</div>`
  ).join('') || "<p style='color:#94a3b8;font-size:13px;'>No comments yet.</p>";
}
async function sendComment(){
  const inp = document.getElementById('cmt-inp'), txt = inp.value.trim();
  if(!txt || !curFile) return;
  inp.value = '';
  const r = await fetch('/api/action', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({action:'comment', uid:userId, file_id:curFile._id, text:txt})});
  const d = await r.json();
  if(!curFile.comments) curFile.comments = [];
  curFile.comments.push(d.comment);
  document.getElementById('det-cmts-cnt').innerText = curFile.comments.length;
  document.getElementById('cmt-cnt-h').innerText = curFile.comments.length;
  renderComments();
}

function switchTab(id, el){
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  document.querySelectorAll('.nav-i').forEach(n => n.classList.remove('active'));
  if(el) el.classList.add('active');
  if(id === 'tools') loadEarn();
}

async function loadEarn(){
  const r = await fetch('/api/earn_info/' + userId); const d = await r.json();
  let h = `<div class="card-box"><b>🎡 Daily Spin (${d.spins_done}/${d.spin_limit})</b><br><br><button class="btn-p" onclick="doEarn('spin')" ${d.spins_done>=d.spin_limit?'disabled':''}>Spin & Earn</button></div>`;
  h += `<div class="card-box"><b>📱 Daily Task (${d.tasks_done}/${d.task_limit})</b><br><br><button class="btn-p" onclick="doEarn('task')" ${d.tasks_done>=d.task_limit?'disabled':''}>Complete Task</button></div>`;
  (d.custom_tasks||[]).forEach(t => {
    const done = (d.custom_tasks_done||[]).includes(t._id);
    h += `<div class="card-box"><b>💎 ${t.title} (+${t.coin}C)</b><br><br><button class="btn-p" onclick="doCustomEarn('${t._id}','${t.link}')" ${done?'disabled':''}>${done?'Completed':'Complete'}</button></div>`;
  });
  document.getElementById('earn-area').innerHTML = h;
}

async function doEarn(tp){
  const r = await fetch('/api/get_earn_ad/' + tp); const ad = await r.json();
  if(ad.zone_id && typeof window['show_' + ad.zone_id] === 'function'){
    try { await window['show_' + ad.zone_id](); return claimEarn(tp); } catch(e){}
  }
  if(ad.ad_link) runAdTimer(ad.ad_link, ad.wait_time||10, () => claimEarn(tp));
  else claimEarn(tp);
}
function doCustomEarn(id, link){ runAdTimer(link, 10, () => claimEarn('custom_' + id)); }
async function claimEarn(tp){
  const r = await fetch('/api/claim_earn', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, type:tp})});
  const d = await r.json(); alert(d.msg); loadUser(); loadEarn();
}

async function redeemCoupon(){
  const code = document.getElementById('coupon-inp').value.trim(); if(!code) return;
  const r = await fetch('/api/redeem', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, code})});
  const d = await r.json(); alert(d.msg); if(d.status==='success'){ document.getElementById('coupon-inp').value=''; loadUser(); }
}
async function sendMovieRequest(){
  const name = document.getElementById('req-inp').value.trim(); if(!name) return;
  const r = await fetch('/api/request_movie', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, movie_name:name})});
  const d = await r.json(); alert(d.msg); document.getElementById('req-inp').value='';
}
async function buyWithCoin(pkgId, cost){
  if(!confirm(`${cost} কয়েন দিয়ে কিনবেন?`)) return;
  const r = await fetch('/api/buy_with_coin', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({uid:userId, pkg_id:pkgId})});
  const d = await r.json(); alert(d.msg); loadUser();
}
function copyRef(){ const i = document.getElementById('ref-inp'); i.select(); navigator.clipboard.writeText(i.value); alert('✅ Link Copied!'); }
</script>
</body>
</html>"""

@web.route('/')
def home():
    files = []
    for d in get_extra_dbs_sync():
        try: files.extend(list(d["files"].find().sort("_id", -1)))
        except: pass
    for f in files: f["_id"] = str(f["_id"])

    try: config = sync_db["config"].find_one({"_id": "settings"}) or {}
    except: config = {}

    try:
        cats = [c["name"] for c in sync_db["categories"].find()]
        if not cats: cats = ["Movie", "Web Series", "18+", "Bachelor Point", "Natok", "Anime"]
    except: cats = ["Movie", "Web Series", "18+", "Bachelor Point"]

    try:
        promos = list(sync_db["promotions"].find())
        for p in promos: p["_id"] = str(p["_id"])
    except: promos = []

    try: pkgs = list(sync_db["packages"].find())
    except: pkgs = []

    try: zone_ids = [z["zone_id"] for z in sync_db["zone_ids"].find()]
    except: zone_ids = []

    return render_template_string(
        HTML_TEMPLATE,
        files_json=json.dumps(files, default=str),
        categories_json=json.dumps(cats),
        promotions_json=json.dumps(promos, default=str),
        zone_ids=zone_ids,
        zone_ids_json=json.dumps(zone_ids),
        pkgs=pkgs,
        bot_username=BOT_USERNAME,
        admin_username=config.get("payment_admin", DEFAULT_ADMIN_USERNAME),
        site_name=config.get("site_name", "Moviee Link"),
        marquee_text=config.get("marquee_text", "VLC Player অথবা MX Player দিয়ে চালিয়ে দেখুন। ✅ সাউন্ড/অডিও সমস্যা হলে ভিডিওটি ডাউনলোড করে দেখুন।"),
        ref_coin=config.get("ref_coin", 10)
    )

app_flask = web

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    web.run(host="0.0.0.0", port=port, debug=False, threaded=True, use_reloader=False)

async def main_bot():
    global BOT_USERNAME
    await get_db()
    await get_config()
    await app.start()
    BOT_USERNAME = app.me.username
    print(f"✅ Bot Started Successfully! (@{BOT_USERNAME})")
    try:
        commands = [
            BotCommand("start", "বট স্টার্ট করুন"),
            BotCommand("cmd", "সকল কমান্ড লিস্ট (Admin)"),
            BotCommand("movies", "নতুন মুভি এড (Admin)"),
            BotCommand("editemovies", "মুভি এডিট (Admin)"),
            BotCommand("deletedmovies", "মুভি ডিলিট (Admin)"),
            BotCommand("addseries", "নতুন ওয়েব সিরিজ এড (Admin)"),
            BotCommand("editeseries", "ওয়েব সিরিজ এডিট (Admin)"),
            BotCommand("deleteseries", "সিরিজ ডিলিট (Admin)"),
            BotCommand("setcategory", "ক্যাটাগরি এড (Admin)"),
            BotCommand("deletecategory", "ক্যাটাগরি ডিলিট (Admin)"),
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
        await app.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Watch Now 🎬", web_app=WebAppInfo(url=WEB_URL)))
    except Exception as e: print(f"⚠️ Setup Error: {e}")
    asyncio.create_task(background_tasks())
    await idle()
    await app.stop()

if __name__ == "__main__":
    try:
        instance_lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        instance_lock.bind(("127.0.0.1", 9876))
    except socket.error:
        print("⚠️ বট আগে থেকেই রান হচ্ছে!")
        sys.exit(1)

    try: requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True", timeout=5)
    except: pass

    threading.Thread(target=keep_alive, daemon=True).start()
    threading.Thread(target=run_flask, daemon=True).start()
    while True:
        try: loop.run_until_complete(main_bot())
        except FloodWait as e: time.sleep(e.value)
        except Exception as e: print(f"❌ Error: {e}"); time.sleep(5)

