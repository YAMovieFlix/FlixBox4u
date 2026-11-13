import os, sys, asyncio, threading, random, string, time, requests, json, base64, socket, io
from datetime import datetime, timedelta

# ==========================================
# 🛑 ANTI-DUPLICATE LOCK (ডাবল মেসেজ ফিক্স)
# ==========================================
try:
    instance_lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    instance_lock.bind(("127.0.0.1", 9876))
except socket.error:
    print("⚠️ বট আগে থেকেই রান হচ্ছে! ডাবল মেসেজ বন্ধ করতে নতুন প্রসেস বাতিল করা হলো।")
    sys.exit(1)

# ==========================================
# 🛑 PYROGRAM PYTHON 3.14 FIX 🛑
# ==========================================
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
# 🛑 Pyrogram Double Message Filter 🛑
# ==========================================
processed_msgs = set()
def is_unique_func(_, __, m):
    if not hasattr(m, "id"): return True
    if m.id in processed_msgs: return False
    processed_msgs.add(m.id)
    if len(processed_msgs) > 10000: processed_msgs.clear()
    return True
unique_msg = filters.create(is_unique_func)

processed_cbs = set()
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
MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://akash:akash@cluster0.etisrpx.mongodb.net/?appName=Cluster0")
ADMIN_ID = int(os.environ.get("ADMIN_ID", 7120801813)) 
WEB_URL = os.environ.get("WEB_URL", "https://flixbox4ubot.onrender.com")

DEFAULT_ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "Sudo_king")
BOT_USERNAME = "FlixBox4uBot"

try:
    requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook?drop_pending_updates=True")
except: pass

# ==========================================
# 2. DATABASE & SYSTEM INIT
# ==========================================
app = Client("shilacall_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True) 
web = Flask(__name__) 
admin_steps = {}

db_client, db = None, None 
users_col, files_col, cats_col, tasks_col, titles_col = None, None, None, None, None
pkgs_col, links_col, config_col, channels_col, coupons_col, mongos_col = None, None, None, None, None, None 
auto_channels_col, promotions_col = None, None
sync_db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)["NewBot"]

async def get_db(): 
    global db_client, db, users_col, files_col, cats_col, pkgs_col, links_col, config_col, channels_col, coupons_col, mongos_col, tasks_col, titles_col
    global auto_channels_col, promotions_col
    if db_client is None:
        db_client = AsyncIOMotorClient(MONGO_URI) 
        db = db_client["NewBot"]
        users_col, files_col, cats_col = db["users"], db["files"], db["categories"]
        pkgs_col, links_col, config_col = db["packages"], db["ad_links"], db["config"]
        channels_col, coupons_col, mongos_col = db["channels"], db["coupons"], db["mongos"] 
        tasks_col = db["custom_tasks"]
        titles_col = db["auto_titles"]
        auto_channels_col = db["auto_channels"]
        promotions_col = db["promotions"]
    return db

async def get_config(): 
    await get_db() 
    conf = await config_col.find_one({"_id": "settings"}) 
    if not conf: 
        conf = { "_id": "settings", "ref_coin": 10, "ref_on": True, "auto_del_time": 0, "autodel_text": "⏳ ফাইলটি নির্দিষ্ট সময় পর অটো ডিলিট হয়ে যাবে। / File will be auto-deleted after some time.", "frotect": False, "ads_on": True, "direk_wait": [5], "prem_vid_wait": 10, "prstep": 1, "regstep": 1, "start_logo": None, "start_text": "", "payment_admin": DEFAULT_ADMIN_USERNAME, "autovid_msg_id": None, "autovid_chat_id": None, "autovid_time_min": 0, "autopost_time_hr": 0, "autopost_idx": 0, "site_name": "Glow Top", 
                 "premium_vid_coin": 50, "vid_relock_min": 1440, "spin_coin_min": 10, "spin_coin_max": 50, "task_coin": 20, "spin_limit": 5, "task_limit": 5,
                 "autopost_channel": None, "autopost_minute": 0, "autopost_channel_idx": 0 } 
        await config_col.insert_one(conf) 
    return conf

async def delete_msg_later(client, chat_id, msg_id, delay): 
    await asyncio.sleep(delay) 
    try: await client.delete_messages(chat_id, msg_id) 
    except: pass

def get_expiry_str(expiry_date): 
    if not expiry_date: return None 
    now = datetime.now() 
    if expiry_date <= now: return None 
    sec = int((expiry_date - now).total_seconds())
    y, sec = divmod(sec, 31536000)
    mo, sec = divmod(sec, 2592000)
    w, sec = divmod(sec, 604800)
    d, sec = divmod(sec, 86400)
    h, sec = divmod(sec, 3600)
    m, sec = divmod(sec, 60)
    res = []
    if y: res.append(f"{y} বছর/Years")
    if mo: res.append(f"{mo} মাস/Months")
    if w: res.append(f"{w} সপ্তাহ/Weeks")
    if d: res.append(f"{d} দিন/Days")
    if h: res.append(f"{h} ঘণ্টা/Hours")
    if m: res.append(f"{m} মিনিট/Mins")
    if sec or not res: res.append(f"{sec} সেকেন্ড/Secs")
    return " ".join(res)

def keep_alive(): 
    while True: 
        try: 
            time.sleep(60) 
            requests.get(WEB_URL) 
        except: pass 
threading.Thread(target=keep_alive, daemon=True).start()

# ==========================================
# 2.5 MULTI-MONGODB MANAGER
# ==========================================
async_mongo_clients = {}
sync_mongo_clients = {}

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

# 🛑 Base64 কনভার্টার (টেলিগ্রামে ছবি পাঠানোর জন্য)
def get_tg_photo(thumb):
    if not thumb: return "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    if thumb.startswith("data:image"):
        try:
            b64 = thumb.split(",")[1]
            img = io.BytesIO(base64.b64decode(b64))
            img.name = "thumb.jpg"
            return img
        except: return "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    return thumb

# 🛑 নতুন অ্যাড করা পোস্ট সাথে সাথে চ্যানেলে পাঠানোর ফাংশন
async def notify_new_post(client, file_doc, config):
    ch_id = config.get("autopost_channel")
    btn = [InlineKeyboardButton("🎬 Watch Now / দেখুন", url=f"https://t.me/{BOT_USERNAME}?start=home")]
    if ch_id:
        try:
            await client.send_photo(
                int(ch_id),
                photo=get_tg_photo(file_doc.get("thumb_url")),
                caption=f"🔥 **New Trending Video!**\n\n🎬 **{file_doc['title']}**\n\n👇 নিচের লিংকে বা বাটনে ক্লিক করে সম্পূর্ণ ভিডিও দেখুন!",
                reply_markup=InlineKeyboardMarkup([btn])
            )
        except Exception as e:
            print(f"Channel Autopost New Post Error: {e}")
            
    # আনলিমিটেড চ্যানেলে পোস্ট পাঠানোর লজিক
    await get_db()
    async for ch in auto_channels_col.find():
        try:
            msg = await client.send_photo(
                int(ch["chat_id"]),
                photo=get_tg_photo(file_doc.get("thumb_url")),
                caption=f"🔥 **New Trending Video!**\n\n🎬 **{file_doc['title']}**\n\n👇 নিচের লিংকে বা বাটনে ক্লিক করে সম্পূর্ণ ভিডিও দেখুন!",
                reply_markup=InlineKeyboardMarkup([btn])
            )
            del_min = ch.get("delete_min", 0)
            if del_min > 0:
                asyncio.create_task(delete_msg_later(client, int(ch["chat_id"]), msg.id, del_min * 60))
        except Exception as e:
            print(f"New Channel Autopost Error: {e}")

# ==========================================
# 3. BACKGROUND TASKS
# ==========================================
async def background_tasks(): 
    last_autovid = time.time()
    last_autopost = time.time() 
    last_channel_post = time.time() 

    while True: 
        await asyncio.sleep(60) 
        now = time.time() 
        try: 
            config = await get_config() 
            av_min = config.get("autovid_time_min", 0) 
            if av_min > 0 and config.get("autovid_msg_id") and (now - last_autovid) >= (av_min * 60): 
                last_autovid = time.time() 
                users = await users_col.find().to_list(None) 
                from_chat = config.get("autovid_chat_id", ADMIN_ID)
                for u in users: 
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
                idx = config.get("autopost_idx", 0)
                all_files = []
                for d in await get_extra_dbs_async():
                    all_files.extend(await d["files"].find().sort("_id", 1).to_list(None))
                
                if all_files:
                    if idx >= len(all_files): idx = 0
                    f = all_files[idx]
                    users = await users_col.find().to_list(None)
                    btn = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 Watch Now / দেখুন", web_app=WebAppInfo(url=f"{WEB_URL}/?uid={u['_id']}"))]])
                    for u in users:
                        try:
                            await app.send_photo(u["_id"], photo=get_tg_photo(f.get("thumb_url")), caption=f"🔥 **New Video / নতুন ভিডিও!**\n\nTitle: {f['title']}\n\n👇 Click below to watch / নিচে ক্লিক করে দেখুন!", reply_markup=btn)
                            await asyncio.sleep(0.05)
                        except: pass
                    await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_idx": idx + 1}})

            ch_id = config.get("autopost_channel")
            ch_min = config.get("autopost_minute", 0)
            if ch_id and ch_min > 0 and (now - last_channel_post) >= (ch_min * 60):
                last_channel_post = time.time()
                c_idx = config.get("autopost_channel_idx", 0)
                all_files = []
                for d in await get_extra_dbs_async():
                    all_files.extend(await d["files"].find().sort("_id", 1).to_list(None))

                if all_files:
                    if c_idx >= len(all_files): c_idx = 0
                    f = all_files[c_idx]
                    btn = InlineKeyboardMarkup(
    [
        [InlineKeyboardButton("🎬 Watch Now / দেখুন", url=f"https://t.me/{BOT_USERNAME}?start=home")]
    ]
)
                    try:
                        await app.send_photo(
                            int(ch_id),
                            photo=get_tg_photo(f.get("thumb_url")),
                            caption=f"🔥 **New Trending Video!**\n\n🎬 **{f['title']}**\n\n👇 নিচের লিংকে বা বাটনে ক্লিক করে সম্পূর্ণ ভিডিও দেখুন!",
                            reply_markup=btn
                        )
                        await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_channel_idx": c_idx + 1}})
                    except Exception as e:
                        print(f"Channel Autopost Error: {e}")
            
            # আনলিমিটেড অটো চ্যানেলের ব্যাকগ্রাউন্ড টাস্ক লজিক
            async for ch in auto_channels_col.find():
                c_id = ch.get("chat_id")
                c_int = ch.get("interval", 0)
                if not c_id or c_int <= 0: continue
                
                last_p = ch.get("last_post_time", 0)
                if (now - last_p) >= (c_int * 60):
                    all_fs = []
                    for d in await get_extra_dbs_async():
                        all_fs.extend(await d["files"].find().sort("_id", 1).to_list(None))
                    
                    if all_fs:
                        c_idx = ch.get("post_idx", 0)
                        if c_idx >= len(all_fs): c_idx = 0
                        f = all_fs[c_idx]
                        btn = InlineKeyboardMarkup([[InlineKeyboardButton("🎬 Watch Now / দেখুন", url=f"https://t.me/{BOT_USERNAME}?start=home")]])
                        try:
                            msg = await app.send_photo(int(c_id), photo=get_tg_photo(f.get("thumb_url")), caption=f"🔥 **New Trending Video!**\n\n🎬 **{f['title']}**\n\n👇 নিচের লিংকে বা বাটনে ক্লিক করে সম্পূর্ণ ভিডিও দেখুন!", reply_markup=btn)
                            await auto_channels_col.update_one({"_id": ch["_id"]}, {"$set": {"last_post_time": time.time(), "post_idx": c_idx + 1}})
                            del_m = ch.get("delete_min", 0)
                            if del_m > 0:
                                asyncio.create_task(delete_msg_later(app, int(c_id), msg.id, del_m * 60))
                        except Exception as e:
                            print("Unlimited Channel Autopost Error:", e)

        except Exception as e: print("BG Task Error:", e)

# ==========================================
# 4. BOT COMMANDS
# ==========================================
@app.on_message(filters.command("myid") & unique_msg) 
async def cmd_myid(c, m): 
    await m.reply(f"✅ বট একদম ঠিকভাবে কাজ করছে! / Bot is working perfectly!\n🆔 আপনার আইডি / Your ID: {m.from_user.id}")

@app.on_message(filters.command("cmd") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_list(c, m):
    text = """
🛠 **সকল কমান্ড লিস্ট / All Commands List:**

🔸 **Basic Commands:**
/start - বট রিস্টার্ট করুন
/myid - আইডি দেখতে
/cmd - সকল কমান্ড লিস্ট
/stats - ইউজারের স্ট্যাটিস্টিকস
/name - ওয়েবসাইটের নাম পরিবর্তন

🔸 **File Management:**
/addfile - ম্যানুয়ালি ফাইল যোগ করুন
/vidtitel - অটো ভিডিও টাইটেল অ্যাড
/deltitel - সেভ করা টাইটেল ডিলিট
/auto - অটো ফাইল + থাম্বনেইল
/auto2 - ডাইরেক্ট অটো ফাইল অ্যাড
/promot - প্রোমোট পোস্টার অ্যাড করুন
/delpromot - প্রোমোট ডিলিট করুন
/delfile - ফাইল ডিলিট
/delall - সব ডাটাবেসের সব ফাইল ডিলিট

🔸 **Tasks & Spin limits:**
/spinlimit - ডেইলি স্পিন লিমিট
/tasklimit - ডেইলি টাস্ক লিমিট
/addtask - কাস্টম আনলিমিটেড টাস্ক অ্যাড
/deltask - কাস্টম টাস্ক ডিলিট

🔸 **Database (MongoDB) Manager:**
/mongo - নতুন ডাটাবেস যোগ
/delmongo - এক্সট্রা ডাটাবেস রিমুভ
/mongostats - ডাটাবেস স্টোরেজ চেক

🔸 **Coins & Pricing:**
/setpremcoin - প্রিমিয়াম ভিডিওর দাম
/spincoin - স্পিন কয়েনের রেঞ্জ
/taskcoin - টাস্ক কমপ্লিট করার কয়েন
/relocktime - ভিডিও আনলক টাইমার
/delrelocktime - আনলক টাইমার ডিলিট
/lockall - আনলক করা ভিডিও রিস্টার্ট

🔸 **Links & Ads:**
/addlink - ডাইরেক্ট অ্যাড লিংক যোগ
/delink - অ্যাড লিংক রিমুভ
/prstep - প্রিমিয়াম অ্যাড স্টেপ
/regstep - রেগুলার অ্যাড স্টেপ

🔸 **Channels & Broadcast:**
/addcnl - ইনলাইন চ্যানেল অ্যাড
/vercnl - Must Join চ্যানেল অ্যাড
/delcnl - ইনলাইন চ্যানেল ডিলিট
/delvrcnl - Must Join চ্যানেল ডিলিট
/brodcast - ইউজারদের মেসেজ পাঠান
/cnlbdcst - চ্যানেল/গ্রুপে মেসেজ পাঠান
/autochannel - চ্যানেলে অটোমেটিক ভিডিও পোস্ট
/delautochannel - অটো চ্যানেল ডিলিট করুন

🔸 **Coupons & Packages:**
/allred - অটো রেন্ডম কুপন তৈরি
/addcred - কয়েন প্যাকেজ তৈরি
/bdt - বিকাশ প্যাকেজ তৈরি
/usd - USD প্যাকেজ তৈরি
/delcred - যেকোনো প্যাকেজ ডিলিট

🔸 **Auto Delete & Others:**
/autodel - ভিডিও অটো ডিলিট টাইম
/frotect - ফরওয়ার্ড/সেভ অফ করতে
/addadmin - পেমেন্ট অ্যাডমিন সেট
/addtex - স্টার্ট টেক্সট সেট করতে
/deltex - স্টার্ট টেক্সট ডিলিট করতে
/logo - স্টার্ট লোগো সেট করতে
/autvid - অটো ভিডিও মেসেজ সেট করতে
/autvidti - অটো ভিডিও ইন্টারভাল সেট
/autpost - অটো পোস্ট ইন্টারভাল সেট
/refbonous - রেফার বোনাস সেট করতে
/refbonousoff - রেফার বোনাস অফ করতে
"""
    await m.reply(text)

@app.on_message(filters.command("stats") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_stats(c, m): 
    await get_db() 
    t_u = await users_col.count_documents({}) 
    p_u = await users_col.count_documents({"premium_until": {"$gt": datetime.now()}}) 
    
    t_f = 0
    dbs = await get_extra_dbs_async()
    for d in dbs: t_f += await d["files"].count_documents({})
    
    await m.reply(f"📊 Statistics:\n\n👥 Users: {t_u}\n🎬 Total Files: {t_f}\n💎 Premium: {p_u}\n👤 Regular: {t_u - p_u}\n🗄 Attached MongoDBs: {len(dbs)}")

@app.on_message(filters.command("name") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_name(c, m):
    new_name = m.text.replace("/name", "").strip()
    if not new_name: return await m.reply("❌ সঠিক নিয়ম: `/name <Site_Name>`\nউদাহরণ: `/name MySite`")
    await get_db()
    await config_col.update_one({"_id": "settings"}, {"$set": {"site_name": new_name}})
    await m.reply(f"✅ সাইটের নাম পরিবর্তন করা হয়েছে / Site name changed to: {new_name}")

@app.on_message(filters.command("start") & filters.private & unique_msg) 
async def start_cmd(client, message): 
    await get_db() 
    uid = message.from_user.id 
    args = message.text.split() 
    config = await get_config()

    ref_by = None
    pf = None
    if len(args) > 1:
        val = args[1]
        if val.isdigit():
            ref_by = int(val)
        elif val.startswith("file_"):
            pf = val.replace("file_", "")
        elif val.startswith("ref") and "file" in val:
            try:
                parts = val.replace("ref", "").split("file")
                ref_by = int(parts[0])
                pf = parts[1]
            except: pass

    user = await users_col.find_one({"_id": uid})
    if not user:
        await users_col.insert_one({
            "_id": uid, "name": message.from_user.first_name, 
            "balance": 0, "pending_file": None, "premium_until": None, 
            "history": [], "unlocked_files": {}, 
            "spin_count": 0, "task_count": 0, "last_activity_date": None,
            "custom_tasks_done": []
        })
        if ref_by and ref_by != uid:
            if config.get("ref_on", True):
                await users_col.update_one({"_id": ref_by}, {"$inc": {"balance": config.get("ref_coin", 10)}})
                try: await client.send_message(ref_by, f"🎉 আপনার রেফারে একজন জয়েন করেছে! / Someone joined using your refer link! +{config.get('ref_coin', 10)} Coins")
                except: pass
        user = await users_col.find_one({"_id": uid})

    if pf:
        await users_col.update_one({"_id": uid}, {"$set": {"pending_file": pf}})
        user["pending_file"] = pf

    channels = await channels_col.find({"type": "must_join"}).to_list(100)
    not_joined = []
    for ch in channels:
        try: await client.get_chat_member(ch["chat_id"], uid)
        except UserNotParticipant: not_joined.append(ch["link"])
        except: pass

    if not_joined:
        btns = [[InlineKeyboardButton("📢 Join Channel / চ্যানেল জয়েন করুন", url=link)] for link in not_joined]
        btns.append([InlineKeyboardButton("✅ Joined / জয়েন করেছি", callback_data="check_join")])
        return await message.reply("❌ আপনাকে আগে আমাদের চ্যানেলগুলোতে জয়েন করতে হবে / You must join our channels first:", reply_markup=InlineKeyboardMarkup(btns))

    if user.get("pending_file"):
        f_id = user["pending_file"]
        await users_col.update_one({"_id": uid}, {"$set": {"pending_file": None}})
        
        file_data = None
        target_db = None
        for d in await get_extra_dbs_async():
            file_data = await d["files"].find_one({"_id": f_id})
            if file_data:
                target_db = d
                break
        
        if file_data:
            await target_db["files"].update_one({"_id": f_id}, {"$inc": {"views": 1}})
            msg = await message.reply("⏳ আপনার ফাইল পাঠানো হচ্ছে... / Sending your file...")
            try:
                del_time = config.get("auto_del_time", 0)
                caption = f"🎬 **{file_data['title']}**"
                if del_time > 0:
                    mins, secs = divmod(del_time, 60)
                    time_str = f"{mins} মিনিট {secs} সেকেন্ড" if mins > 0 else f"{secs} সেকেন্ড"
                    caption += f"\n\n⏳ **ভিডিওটি {time_str} পর অটো ডিলিট হয়ে যাবে!**"

                sent_msg = await client.send_cached_media(chat_id=uid, file_id=file_data["file_id"], caption=caption, protect_content=config.get("frotect", False))
                await msg.delete()
                
                if del_time > 0:
                    warn = await client.send_message(uid, f"⚠️ {config.get('autodel_text')}")
                    asyncio.create_task(delete_msg_later(client, uid, sent_msg.id, del_time))
                    asyncio.create_task(delete_msg_later(client, uid, warn.id, del_time))
            except Exception as e: await msg.edit_text(f"❌ ফাইল পাঠাতে সমস্যা হয়েছে! / Failed to send file! {e}")
        else: await message.reply("❌ ফাইলটি পাওয়া যায়নি! / File not found!")
        return

    full = f"{message.from_user.first_name} {message.from_user.last_name or ''}".strip()
    uname = f"@{message.from_user.username}" if message.from_user.username else "N/A"

    expiry_str = get_expiry_str(user.get("premium_until"))
    mem_status = f"💎 **Premium** (মেয়াদ / Expiry: {expiry_str})" if expiry_str else "👤 **Regular Member**"

    txt = f"👋 **স্বাগতম / Welcome to {config.get('site_name', 'Glow Top')}!**\n\n👤 **Name:** {full}\n🆔 **User ID:** `{uid}`\n🌐 **Username:** {uname}\n💰 **Balance:** {user.get('balance', 0)} Coins\n🔰 **Membership:** {mem_status}\n"
    if config.get("start_text"): txt += f"\n📝 {config.get('start_text')}\n"
    txt += "\n👇 নিচের বাটন থেকে অ্যাপ ওপেন করুন / Click below to open app:"

    btns = []
    try:
        async for ch in channels_col.find({"type": "inline"}):
            if ch.get("name") and ch.get("link"):
                btns.append([InlineKeyboardButton(ch["name"], url=ch["link"])])
    except Exception as e:
        print("Channel Buttons Error:", e)
    
    btns.insert(0, [InlineKeyboardButton(f"🔥 Open / ওপেন {config.get('site_name', 'Glow Top')}", web_app=WebAppInfo(url=f"{WEB_URL}/?uid={uid}"))])

    if config.get("start_logo"):
        try: await client.send_photo(uid, photo=config.get("start_logo"), caption=txt, reply_markup=InlineKeyboardMarkup(btns))
        except: await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))
    else:
        try:
            photos = []
            async for photo in client.get_chat_photos(uid, limit=1):
                photos.append(photo)
            if photos:
                await client.send_photo(uid, photo=photos[0].file_id, caption=txt, reply_markup=InlineKeyboardMarkup(btns))
            else:
                await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))
        except Exception:
            await message.reply(txt, reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex("check_join") & unique_cb) 
async def check_join_cb(c, q): 
    await get_db() 
    for ch in await channels_col.find({"type": "must_join"}).to_list(100): 
        try: await c.get_chat_member(ch["chat_id"], q.from_user.id) 
        except: return await q.answer("❌ আপনি এখনো সব চ্যানেলে জয়েন করেননি! / You haven't joined all channels yet!", show_alert=True) 
    
    try: await q.answer()
    except: pass
    await q.message.delete() 
    class FakeMsg: 
        def __init__(self, u): self.from_user = u; self.text = "/start" 
        async def reply(self, *a, **k): return await c.send_message(q.from_user.id, *a, **k) 
    await start_cmd(c, FakeMsg(q.from_user))

# ==========================================
# 5. ADMIN FILE UPLOAD & MULTI-MONGO
# ==========================================
@app.on_message(filters.command("addfile") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addfile(c, m): 
    admin_steps[m.from_user.id] = {"step": "name"} 
    await m.reply("১. ফাইলের নাম/টাইটেল দিন: \n(1. Enter File Name/Title:)")

def is_in_step(step): 
    return filters.create(lambda _, __, m: admin_steps.get(m.from_user.id, {}).get("step") == step)

@app.on_message(filters.text & filters.user(ADMIN_ID) & filters.private & is_in_step("name") & ~filters.command(["start", "myid", "cmd"]) & unique_msg) 
async def handle_admin_name(c, m): 
    admin_steps[m.from_user.id]["title"] = m.text 
    admin_steps[m.from_user.id]["step"] = "thumb" 
    await m.reply("২. এবার ফাইলের ছবি বা লোগো দিন (Photo বা Document আকারে সেন্ড করুন):\n(2. Now send File Thumbnail/Photo/Document:)")

@app.on_message((filters.photo | filters.document) & filters.user(ADMIN_ID) & filters.private & is_in_step("thumb") & unique_msg) 
async def handle_admin_photo(c, m): 
    msg = await m.reply("⏳ ছবি ডাটাবেসে সেভ করা হচ্ছে... / Saving image to Database...") 
    path = await m.download() 
    
    try:
        with open(path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg', 'heic'] else "image/jpeg"
        base64_url = f"data:{mime};base64,{encoded_string}"
        admin_steps[m.from_user.id]["thumb_url"] = base64_url
        os.remove(path)
    except Exception as e:
        print("Image processing error:", e)
        admin_steps[m.from_user.id]["thumb_url"] = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"

    btns = [[InlineKeyboardButton("💎 Premium Video", callback_data="ftype_prem")], [InlineKeyboardButton("👤 Regular Video", callback_data="ftype_reg")]]
    await msg.edit_text("ভিডিওটি কি প্রিমিয়াম নাকি রেগুলার? / Is it Premium or Regular?", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^ftype_") & filters.user(ADMIN_ID) & unique_cb) 
async def filetype_cb(c, q): 
    admin_steps[q.from_user.id]["is_premium"] = (q.data == "ftype_prem") 
    admin_steps[q.from_user.id]["step"] = "file" 
    await q.message.edit_text("✅ ৩. এবার মূল ফাইল (Video/Document) দিন:\n(3. Now send the main Video/File:)")

@app.on_message((filters.video | filters.document | filters.audio) & filters.user(ADMIN_ID) & filters.private & is_in_step("file") & unique_msg) 
async def handle_admin_file(c, m): 
    msg = await m.reply("⏳ ফাইল সেভ করা হচ্ছে... / Saving file...") 
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8)) 
    f_id = m.video.file_id if m.video else (m.document.file_id if m.document else m.audio.file_id) 
    
    dbs = await get_extra_dbs_async()
    target_db = dbs[0]
    min_size = float('inf')
    
    for d in dbs:
        try:
            st = await d.command("dbstats")
            if st["dataSize"] < min_size:
                min_size = st["dataSize"]
                target_db = d
        except: pass

    new_file = { "_id": short_id, "title": admin_steps[m.from_user.id]["title"], "category": "All", "is_premium": admin_steps[m.from_user.id].get("is_premium", False), "file_id": f_id, "thumb_url": admin_steps[m.from_user.id].get("thumb_url"), "views": 0, "likes": [], "comments": [] }
    await target_db["files"].insert_one(new_file) 
    del admin_steps[m.from_user.id] 
    
    config = await get_config()
    await notify_new_post(c, new_file, config)
    
    db_name = "Main DB" if target_db == dbs[0] else "Extra DB"
    await msg.edit_text(f"✅ ফাইল সফলভাবে অ্যাড হয়েছে! / File Added Successfully!\nID: `{short_id}`\n🗄 Saved in: {db_name}")


# ==========================================
# 🛑 AUTO TITLE MANAGEMENT & AUTO UPLOAD 🛑
# ==========================================
@app.on_message(filters.command("vidtitel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_vidtitel(c, m):
    title = m.text.replace("/vidtitel", "").strip()
    if not title: return await m.reply("❌ সঠিক নিয়ম: `/vidtitel <Title>`\nউদাহরণ: `/vidtitel নিউ ভাইরাল সেক্স ভিডিও`")
    await get_db()
    await titles_col.insert_one({"title": title})
    await m.reply(f"✅ নতুন টাইটেল অ্যাড হয়েছে:\n**{title}**")

@app.on_message(filters.command("deltitel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltitel(c, m):
    await get_db()
    titles = await titles_col.find().to_list(100)
    if not titles: return await m.reply("❌ কোনো সেভ করা টাইটেল নেই!")
    btns = [[InlineKeyboardButton(f"❌ {t['title'][:25]}", callback_data=f"delttl_{t['_id']}")] for t in titles]
    await m.reply("ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delttl_") & filters.user(ADMIN_ID) & unique_cb)
async def delttl_cb(c, q):
    await get_db()
    await titles_col.delete_one({"_id": ObjectId(q.data.split("_")[1])})
    await q.message.edit_text("✅ টাইটেল ডিলিট করা হয়েছে!")

@app.on_message(filters.command("auto") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_auto_upload(c, m):
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        return await m.reply("❌ কোনো ভিডিও বা ডকুমেন্টে রিপ্লাই করে `/auto` দিন।\n(Reply to a video with /auto)")
    
    await get_db()
    titles = await titles_col.find().to_list(100)
    if not titles: 
        return await m.reply("❌ কোনো টাইটেল সেভ করা নেই! অটো আপলোড করার আগে `/vidtitel <Title>` কমান্ড দিয়ে কিছু টাইটেল অ্যাড করে নিন।")
    
    btns = [[InlineKeyboardButton(t['title'], callback_data=f"selttl_{t['_id']}")] for t in titles]
    admin_steps[m.from_user.id] = {"file_id": (m.reply_to_message.video or m.reply_to_message.document).file_id}
    await m.reply("👇 ভিডিওর জন্য একটি টাইটেল সিলেক্ট করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^selttl_") & filters.user(ADMIN_ID) & unique_cb)
async def selttl_cb(c, q):
    step_data = admin_steps.get(q.from_user.id)
    if not step_data or "file_id" not in step_data:
        return await q.answer("❌ সেশন এক্সপায়ার! আবার ভিডিওতে রিপ্লাই করে /auto দিন।", show_alert=True)
        
    await get_db()
    title_doc = await titles_col.find_one({"_id": ObjectId(q.data.split("_")[1])})
    if not title_doc: return await q.answer("❌ টাইটেলটি পাওয়া যায়নি!", show_alert=True)
    
    title = title_doc["title"]
    msg = await q.message.edit_text(f"⏳ টাইটেল: **{title}**\n\nঅটো প্রসেস ও স্ক্রিনশট গ্রিড তৈরি করা হচ্ছে (এতে কিছুক্ষণ সময় লাগতে পারে)...")
    
    thumb_url = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    
    try:
        video_path = await c.download_media(step_data["file_id"])
        grid_path = f"{video_path}_grid.jpg"
        
        cmd_safe = f'ffmpeg -y -i "{video_path}" -vf "thumbnail=n=20,scale=320:-1,tile=4x2" -frames:v 1 -q:v 2 "{grid_path}"'
        process = await asyncio.create_subprocess_shell(cmd_safe, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()
        
        if os.path.exists(grid_path):
            with open(grid_path, "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode('utf-8')
            thumb_url = f"data:image/jpeg;base64,{encoded}"
            os.remove(grid_path)
            
        if os.path.exists(video_path):
            os.remove(video_path)
    except Exception as e:
        print("Auto Grid Error:", e)

    admin_steps[q.from_user.id] = {
        "step": "auto_wait",
        "title": title,
        "file_id": step_data["file_id"],
        "thumb_url": thumb_url
    }
    
    btns = [[InlineKeyboardButton("💎 Premium Video", callback_data="auto_prem")], 
            [InlineKeyboardButton("👤 Regular Video", callback_data="auto_reg")]]
    await msg.edit_text(f"✅ **নাম:** {title}\n\nভিডিওটি কি প্রিমিয়াম নাকি রেগুলার কোথায় অ্যাড হবে? / Where to add this video?", reply_markup=InlineKeyboardMarkup(btns))

@app.on_message(filters.command("auto2") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_auto2(c, m):
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        return await m.reply("❌ কোনো ভিডিও বা ডকুমেন্টে রিপ্লাই করে `/auto2` দিন।")
    
    title = m.text.replace("/auto2", "").strip()
    if not title: return await m.reply("❌ টাইটেল লিখে দেননি! সঠিক নিয়ম: `/auto2 <Title>`")

    msg = await m.reply(f"⏳ টাইটেল: **{title}**\n\nঅটো প্রসেস ও স্ক্রিনশট গ্রিড তৈরি করা হচ্ছে (এতে কিছুক্ষণ সময় লাগতে পারে)...")
    file_id = (m.reply_to_message.video or m.reply_to_message.document).file_id
    thumb_url = "https://placehold.co/600x400/1c1c24/ff007f?text=Media"
    
    try:
        video_path = await c.download_media(file_id)
        grid_path = f"{video_path}_grid.jpg"
        cmd_safe = f'ffmpeg -y -i "{video_path}" -vf "thumbnail=n=20,scale=320:-1,tile=4x2" -frames:v 1 -q:v 2 "{grid_path}"'
        process = await asyncio.create_subprocess_shell(cmd_safe, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await process.communicate()
        
        if os.path.exists(grid_path):
            with open(grid_path, "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode('utf-8')
            thumb_url = f"data:image/jpeg;base64,{encoded}"
            os.remove(grid_path)
        if os.path.exists(video_path): os.remove(video_path)
    except: pass

    admin_steps[m.from_user.id] = {"step": "auto_wait", "title": title, "file_id": file_id, "thumb_url": thumb_url}
    btns = [[InlineKeyboardButton("💎 Premium Video", callback_data="auto_prem")], [InlineKeyboardButton("👤 Regular Video", callback_data="auto_reg")]]
    await msg.edit_text(f"✅ **নাম:** {title}\n\nভিডিওটি কি প্রিমিয়াম নাকি রেগুলার কোথায় অ্যাড হবে?", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^auto_") & filters.user(ADMIN_ID) & unique_cb)
async def auto_type_cb(c, q):
    step_data = admin_steps.get(q.from_user.id)
    if not step_data or step_data.get("step") != "auto_wait":
        return await q.answer("❌ সেশন এক্সপায়ার! / Session Expired!", show_alert=True)
        
    is_premium = (q.data == "auto_prem")
    short_id = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
    
    dbs = await get_extra_dbs_async()
    target_db = dbs[0]
    min_size = float('inf')
    for d in dbs:
        try:
            st = await d.command("dbstats")
            if st["dataSize"] < min_size:
                min_size = st["dataSize"]
                target_db = d
        except: pass
        
    new_file = {
        "_id": short_id, "title": step_data["title"], "category": "All",
        "is_premium": is_premium, "file_id": step_data["file_id"],
        "thumb_url": step_data["thumb_url"], "views": 0, "likes": [], "comments": []
    }
    await target_db["files"].insert_one(new_file)
    del admin_steps[q.from_user.id]
    
    config = await get_config()
    await notify_new_post(c, new_file, config)
    
    db_name = "Main DB" if target_db == dbs[0] else "Extra DB"
    await q.message.edit_text(f"✅ অটো আপলোড সফল! / Auto Upload Success!\nID: `{short_id}`\n🗄 Saved in: {db_name}")

@app.on_message(filters.command("promot") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_promot(c, m):
    if not m.reply_to_message or not m.reply_to_message.photo: return await m.reply("❌ একটি ল্যান্ডস্কেপ ফটো (ব্যানার) তে রিপ্লাই করে `/promot` দিন।")
    text = m.text.replace("/promot", "").strip()
    if "|" not in text: return await m.reply("❌ সঠিক নিয়ম: `/promot Title | Link`")
    
    parts = text.split("|"); title = parts[0].strip(); link = parts[1].strip()
    msg = await m.reply("⏳ প্রোমোট ব্যানার সেভ করা হচ্ছে...")
    path = await m.reply_to_message.download()
    
    try:
        with open(path, "rb") as image_file: encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        ext = path.split('.')[-1].lower() if '.' in path else 'jpg'
        mime = f"image/{ext}" if ext in ['png', 'webp', 'gif', 'jpeg', 'jpg', 'heic'] else "image/jpeg"
        base64_url = f"data:{mime};base64,{encoded_string}"
        os.remove(path)
    except Exception as e: return await msg.edit_text(f"❌ ইমেজ সেভ করতে সমস্যা হয়েছে: {e}")

    await get_db()
    await promotions_col.insert_one({"title": title, "link": link, "image": base64_url})
    await msg.edit_text(f"✅ প্রোমোট ব্যানার সফলভাবে অ্যাড হয়েছে!\n**Title:** {title}\n**Link:** {link}")

@app.on_message(filters.command("delpromot") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delpromot(c, m):
    await get_db()
    promos = await promotions_col.find().to_list(100)
    if not promos: return await m.reply("❌ কোনো প্রোমোট ব্যানার নেই!")
    btns = [[InlineKeyboardButton(f"❌ {p['title'][:25]}", callback_data=f"delpromo_{p['_id']}")] for p in promos]
    await m.reply("ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delpromo_") & filters.user(ADMIN_ID) & unique_cb)
async def delpromo_cb(c, q):
    await get_db(); await promotions_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ প্রোমোট ডিলিট করা হয়েছে!")

@app.on_message(filters.command("delfile") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delfile(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/delfile <file_id>`\nউদাহরণ: `/delfile AbCd123`")
    f_id = m.text.split()[1]
    deleted = False
    for d in await get_extra_dbs_async():
        res = await d["files"].delete_one({"_id": f_id})
        if res.deleted_count > 0:
            deleted = True
            break
    if deleted: await m.reply("✅ ফাইল ডিলিট হয়েছে! / File Deleted!") 
    else: await m.reply("❌ ফাইল পাওয়া যায়নি! / File Not Found!")

@app.on_message(filters.command("delall") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delall(c, m): 
    for d in await get_extra_dbs_async(): await d["files"].delete_many({})
    await m.reply("✅ সকল ফাইল ডিলিট করা হয়েছে! / All files deleted from all DBs!")

# --- MONGODB MANAGER COMMANDS ---
@app.on_message(filters.command("mongo") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_mongo(c, m):
    args = m.text.split()
    if len(args) < 2: return await m.reply("❌ সঠিক নিয়ম: `/mongo <mongodb_uri>`\nউদাহরণ: `/mongo mongodb+srv://...`")
    uri = args[1]
    msg = await m.reply("⏳ কানেক্ট করা হচ্ছে... / Connecting...")
    try:
        test_client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=3000)
        await test_client.server_info()
        await get_db()
        await mongos_col.insert_one({"uri": uri})
        await msg.edit_text("✅ নতুন MongoDB সফলভাবে যুক্ত করা হয়েছে! স্টোরেজ লিমিট বেড়ে গেছে! 🚀")
    except Exception as e:
        await msg.edit_text(f"❌ MongoDB কানেক্ট করতে ব্যর্থ হয়েছে!\nError: {e}")

@app.on_message(filters.command("delmongo") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delmongo(c, m):
    await get_db()
    dbs = await mongos_col.find().to_list(None)
    if not dbs: return await m.reply("❌ কোনো এক্সট্রা MongoDB নেই! / No extra MongoDB found!")
    btns = [[InlineKeyboardButton(f"❌ {db['uri'][:25]}...", callback_data=f"delmongo_{db['_id']}")] for db in dbs]
    await m.reply("ডিলিট করতে ক্লিক করুন / Click to delete:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delmongo_") & filters.user(ADMIN_ID) & unique_cb)
async def delmongo_cb(c, q):
    await get_db()
    db_id = q.data.split("_")[1]
    doc = await mongos_col.find_one({"_id": ObjectId(db_id)})
    if doc:
        await mongos_col.delete_one({"_id": ObjectId(db_id)})
        if doc["uri"] in async_mongo_clients: del async_mongo_clients[doc["uri"]]
        if doc["uri"] in sync_mongo_clients: del sync_mongo_clients[doc["uri"]]
        await q.message.edit_text("✅ MongoDB ডিলিট করা হয়েছে! / MongoDB Deleted!")
    else:
        await q.message.edit_text("❌ পাওয়া যায়নি! / Not found!")

@app.on_message(filters.command("mongostats") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_mongostats(c, m):
    msg = await m.reply("⏳ তথ্য সংগ্রহ করা হচ্ছে... / Fetching stats...")
    dbs = await get_extra_dbs_async()
    text = "📊 **MongoDB Storage Stats:**\n\n"
    for i, d in enumerate(dbs):
        try:
            stats = await d.command("dbstats")
            files_count = await d["files"].count_documents({})
            size_mb = stats["dataSize"] / (1024 * 1024)
            limit_mb = 512
            free_mb = limit_mb - size_mb
            pct = (size_mb / limit_mb) * 100
            
            name = "🔹 Main Database (Default)" if i == 0 else f"🔸 Extra DB {i}"
            text += f"{name}\n"
            text += f"🎬 Files Stored: {files_count}\n"
            text += f"📦 Storage Used: {size_mb:.2f} MB ({pct:.1f}%)\n"
            text += f"🟢 Free Space: {free_mb:.2f} MB\n\n"
        except Exception as e:
            text += f"❌ **DB {i}** - Connection Error!\n\n"
    await msg.edit_text(text)

# ==========================================
# 6. OTHER ADMIN COMMANDS
# ==========================================
@app.on_message(filters.command("lockall") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_lockall(c, m): 
    await get_db()
    await users_col.update_many({}, {"$set": {"unlocked_files": {}}})
    await m.reply("✅ সবার আনলক করা ভিডিওগুলো রিস্টার্ট/লক করা হয়েছে! / All unlocked videos have been reset/locked for everyone!")

@app.on_message(filters.command("autochannel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_autochannel(c, m):
    args = m.text.split()
    if len(args) < 4: return await m.reply("❌ সঠিক নিয়ম: `/autochannel <ChatID> <Interval_Minutes> <Delete_Minutes>`\nউদাহরণ: `/autochannel -10012345678 30 60`\n(০ দিলে ডিলিট হবে না)")
    await get_db(); chat_id = args[1]; interval = int(args[2]); delete_min = int(args[3])
    await auto_channels_col.update_one({"chat_id": chat_id}, {"$set": {"chat_id": chat_id, "interval": interval, "delete_min": delete_min, "last_post_time": 0, "post_idx": 0}}, upsert=True)
    await m.reply(f"✅ Auto Channel Added Successfully!\nChat ID: {chat_id}\nInterval: {interval} Min\nAuto Delete: {delete_min} Min")

@app.on_message(filters.command("delautochannel") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_delautochannel(c, m):
    await get_db()
    chs = await auto_channels_col.find().to_list(100)
    if not chs: return await m.reply("❌ কোনো অটো চ্যানেল অ্যাড করা নেই!")
    btns = [[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delautoch_{ch['_id']}")] for ch in chs]
    await m.reply("ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^delautoch_") & filters.user(ADMIN_ID) & unique_cb)
async def delautoch_cb(c, q):
    await get_db(); await auto_channels_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ অটো চ্যানেল রিমুভ করা হয়েছে!")

@app.on_message(filters.command("setpremcoin") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_setpremcoin(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/setpremcoin <Coin>`\nউদাহরণ: `/setpremcoin 50`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"premium_vid_coin": int(m.text.split()[1])}}); await m.reply("✅ Premium Video Coin Price Set!")

@app.on_message(filters.command("relocktime") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_relocktime(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/relocktime <Minutes>`\nউদাহরণ: `/relocktime 1440` (1440 min = 24 hrs)")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_min": int(m.text.split()[1])}}); await m.reply("✅ Video Relock Time Set!")

@app.on_message(filters.command("delrelocktime") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delrelocktime(c, m): 
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"vid_relock_min": 0}}); await m.reply("✅ Video Relock Time Deleted! (ভিডিও আর কখনো লক হবেণিক)")

@app.on_message(filters.command("spincoin") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_spincoin(c, m): 
    if len(m.text.split()) < 2 or "-" not in m.text: return await m.reply("❌ সঠিক নিয়ম: `/spincoin <Min-Max>`\nউদাহরণ: `/spincoin 10-50`")
    mn, mx = map(int, m.text.split()[1].split("-"))
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"spin_coin_min": mn, "spin_coin_max": mx}}); await m.reply("✅ Spin Coin Range Set!")

@app.on_message(filters.command("taskcoin") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_taskcoin(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/taskcoin <Coin>`\nউদাহরণ: `/taskcoin 20`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"task_coin": int(m.text.split()[1])}}); await m.reply("✅ Task Coin Reward Set!")

@app.on_message(filters.command("spinlimit") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_spinlimit(c, m):
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/spinlimit <Num>`\nউদাহরণ: `/spinlimit 5`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"spin_limit": int(m.text.split()[1])}}); await m.reply("✅ User Daily Spin Limit Set!")

@app.on_message(filters.command("tasklimit") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_tasklimit(c, m):
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/tasklimit <Num>`\nউদাহরণ: `/tasklimit 5`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"task_limit": int(m.text.split()[1])}}); await m.reply("✅ User Daily Default Task Limit Set!")

@app.on_message(filters.command("addtask") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_addtask(c, m):
    try:
        parts = m.text.replace("/addtask", "").strip().split("|")
        title, link, coin = parts[0].strip(), parts[1].strip(), int(parts[2].strip())
        await get_db()
        await tasks_col.insert_one({"title": title, "link": link, "coin": coin})
        await m.reply(f"✅ কাস্টম আনলিমিটেড টাস্ক অ্যাড হয়েছে!\nTitle: {title}\nCoin: {coin}")
    except:
        await m.reply("❌ সঠিক নিয়ম: `/addtask <Title> | <Link> | <Coins>`\nউদাহরণ: `/addtask Join Telegram | https://t.me/abc | 50`")

@app.on_message(filters.command("deltask") & filters.user(ADMIN_ID) & unique_msg)
async def cmd_deltask(c, m):
    await get_db()
    tsks = await tasks_col.find().to_list(100)
    if not tsks: return await m.reply("❌ কোনো টাস্ক নেই!")
    btns = [[InlineKeyboardButton(f"❌ {t['title']} ({t['coin']}C)", callback_data=f"deltask_{t['_id']}")] for t in tsks]
    await m.reply("ডিলিট করতে ক্লিক করুন:", reply_markup=InlineKeyboardMarkup(btns))

@app.on_callback_query(filters.regex(r"^deltask_") & filters.user(ADMIN_ID) & unique_cb)
async def deltask_cb(c, q):
    await get_db(); await tasks_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Task Deleted!")

@app.on_message(filters.command("prstep") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_prstep(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/prstep <step>`\nউদাহরণ: `/prstep 2`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"prstep": int(m.text.split()[1])}}); await m.reply("✅ Premium Ad Steps Set!")

@app.on_message(filters.command("regstep") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_regstep(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/regstep <step>`\nউদাহরণ: `/regstep 1`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"regstep": int(m.text.split()[1])}}); await m.reply("✅ Regular Ad Steps Set!")

@app.on_message(filters.command("addadmin") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addadmin(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/addadmin <username>`\nউদাহরণ: `/addadmin Sudo_king`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"payment_admin": m.text.split()[1].replace("@", "")}}); await m.reply("✅ Payment Admin Set!")

@app.on_message(filters.command("addlink") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addlink(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/addlink <link>`\nউদাহরণ: `/addlink https://adlink.com/...`")
    await get_db(); await links_col.insert_one({"link": m.text.split()[1]}); await m.reply("✅ Ad Link Added!")

@app.on_message(filters.command("delink") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_dellink(c, m): 
    await get_db() 
    btns = [[InlineKeyboardButton(f"❌ {l['link'][:20]}...", callback_data=f"dellink_{l['_id']}")] for l in await links_col.find().to_list(100)] 
    await m.reply("ডিলিট করতে ক্লিক করুন / Click to delete:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^dellink_") & filters.user(ADMIN_ID) & unique_cb)
async def dellink_cb(c, q): 
    await get_db(); await links_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Ad Link Deleted!")

@app.on_message(filters.command("addcnl") & filters.user(ADMIN_ID))
async def cmd_addcnl(c, m):
    try:
        text = m.text.split(None, 1)
        if len(text) < 2:
            return await m.reply("❌ সঠিক নিয়ম: `/addcnl <Name> <Link>`\nউদাহরণ: `/addcnl Backup Channel https://t.me/...`")
        
        args = text[1].rsplit(None, 1)
        if len(args) < 2:
            return await m.reply("❌ সঠিক নিয়ম: চ্যানেলের নামের পর অবশ্যই লিংক দিন।")
            
        name = args[0]  # এখানে স্পেসসহ পুরো নাম থাকবে (যেমন: Backup Channel)
        link = args[1]  # এখানে লিংক আলাদা হয়ে যাবে
        
        await get_db()
        await channels_col.update_one(
            {"link": link},
            {"$set": {"name": name, "link": link, "type": "inline"}},
            upsert=True
        )
        await m.reply(f"✅ সফলভাবে ইনলাইন চ্যানেল যোগ করা হয়েছে!\n\n📌 **নাম:** {name}\n🔗 **লিংক:** {link}")
    except Exception as e:
        await m.reply(f"❌ ত্রুটি ঘটেছে: {e}")

@app.on_message(filters.command("delcnl") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delcnl(c, m): 
    await get_db() 
    btns = [[InlineKeyboardButton(f"❌ {ch['name']}", callback_data=f"delch_{ch['_id']}")] for ch in await channels_col.find({"type": "inline"}).to_list(100)] 
    await m.reply("ডিলিট করতে ক্লিক করুন / Click to delete:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_message(filters.command("vercnl") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_vercnl(c, m): 
    if len(m.text.split()) < 3: return await m.reply("❌ সঠিক নিয়ম: `/vercnl <ChatID> <Link>`\nউদাহরণ: `/vercnl -10012345678 https://t.me/...`")
    await get_db(); p = m.text.split(); await channels_col.insert_one({"type": "must_join", "chat_id": int(p[1]), "link": p[2]}); await m.reply("✅ Must Join Channel Added!")

@app.on_message(filters.command("delvrcnl") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delvrcnl(c, m): 
    await get_db() 
    btns = [[InlineKeyboardButton(f"❌ {ch['chat_id']}", callback_data=f"delch_{ch['_id']}")] for ch in await channels_col.find({"type": "must_join"}).to_list(100)] 
    await m.reply("ডিলিট করতে ক্লিক করুন / Click to delete:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^delch_") & filters.user(ADMIN_ID) & unique_cb) 
async def delch_cb(c, q): 
    await get_db(); await channels_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Channel Deleted!")

@app.on_message(filters.command("addtex") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addtex(c, m): 
    tex = m.text.replace("/addtex", "").strip()
    if not tex: return await m.reply("❌ সঠিক নিয়ম: `/addtex <Text>`\nউদাহরণ: `/addtex স্বাগতম!`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": tex}}); await m.reply("✅ Start Text Set!")

@app.on_message(filters.command("deltex") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_deltex(c, m): 
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"start_text": ""}}); await m.reply("✅ Start Text Deleted!")

@app.on_message(filters.command("logo") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_logo(c, m): 
    await get_db() 
    if m.reply_to_message and m.reply_to_message.photo: 
        await config_col.update_one({"_id": "settings"}, {"$set": {"start_logo": m.reply_to_message.photo.file_id}}); await m.reply("✅ Start Logo Set!")
    else: await m.reply("❌ কোনো ছবির সাথে রিপ্লাই করে `/logo` দিন।")

@app.on_message(filters.command("autvid") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_autvid(c, m): 
    await get_db(); 
    if m.reply_to_message: 
        await config_col.update_one({"_id": "settings"}, {"$set": {"autovid_msg_id": m.reply_to_message.id, "autovid_chat_id": m.chat.id}})
        await m.reply("✅ Auto Vid Msg Set!")
    else: await m.reply("❌ কোনো মেসেজ/ভিডিওতে রিপ্লাই করে `/autvid` দিন।")

@app.on_message(filters.command("autvidti") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_autvidti(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/autvidti <minutes>`\nউদাহরণ: `/autvidti 30`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autovid_time_min": int(m.text.split()[1])}}); await m.reply("✅ Auto Vid Interval Set!")

@app.on_message(filters.command("autpost") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_autpost(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/autpost <hours>`\nউদাহরণ: `/autpost 2`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autopost_time_hr": int(m.text.split()[1])}}); await m.reply("✅ Auto Post Interval Set!")

@app.on_message(filters.command("refbonous") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_refbonous(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/refbonous <coin>`\nউদাহরণ: `/refbonous 20`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"ref_coin": int(m.text.split()[1]), "ref_on": True}}); await m.reply("✅ Ref Bonus Set!")

@app.on_message(filters.command("refbonousoff") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_refbonousoff(c, m): 
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"ref_on": False}}); await m.reply("✅ Ref Bonus OFF!")

@app.on_message(filters.command("frotect") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_frotect(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/frotect <on/off>`\nউদাহরণ: `/frotect on`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"frotect": m.text.split()[1].lower() == "on"}}); await m.reply("✅ Protect Config Set!")

@app.on_message(filters.command("autodel") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_autodel(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/autodel <seconds>`\nউদাহরণ: `/autodel 600`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"auto_del_time": int(m.text.split()[1])}}); await m.reply("✅ Auto Delete Set!")

@app.on_message(filters.command("autex") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_autex(c, m): 
    tex = m.text.replace("/autex", "").strip()
    if not tex: return await m.reply("❌ সঠিক নিয়ম: `/autex <Text>`\nউদাহরণ: `/autex নির্দিষ্ট সময় পর ডিলিট হবে।`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"autodel_text": tex}}); await m.reply("✅ Auto Delete Text Set!")

@app.on_message(filters.command("usd") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_usd(c, m): 
    if len(m.text.split()) < 3: return await m.reply("❌ সঠিক নিয়ম: `/usd <Coins> <Price>`\nউদাহরণ: `/usd 5 30` (5 Coins = 30 USD)")
    await get_db()
    p = m.text.split()
    await pkgs_col.insert_one({"type": "usd", "coins": int(p[1]), "details": f"{p[1]} Coins={p[2]} USD"})
    await m.reply(f"✅ USD Package Added! ({p[1]} Coins for {p[2]} USD)")

@app.on_message(filters.command("bdt") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_bdt(c, m): 
    if len(m.text.split()) < 3: return await m.reply("❌ সঠিক নিয়ম: `/bdt <Coins> <Price>`\nউদাহরণ: `/bdt 100 30` (100 Coins = 30 BDT)")
    await get_db()
    p = m.text.split()
    await pkgs_col.insert_one({"type": "bkash", "coins": int(p[1]), "details": f"{p[1]} Coins={p[2]} BDT"})
    await m.reply(f"✅ BDT Package Added! ({p[1]} Coins for {p[2]} BDT)")

@app.on_message(filters.command("addcred") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addcred(c, m): 
    if len(m.text.split()) < 5: return await m.reply("❌ সঠিক নিয়ম: `/addcred <Coins> = <Amount> <Unit>`\nউদাহরণ: `/addcred 500 = 7 d`")
    await get_db(); p=m.text.split(); await pkgs_col.insert_one({"type": "coin", "coins": int(p[1]), "amount": int(p[3]), "unit": p[4], "details": f"{p[1]} Coins={p[3]} {p[4]}"}); await m.reply("✅ Coin Package Added!")

@app.on_message(filters.command("delcred") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delcred(c, m): 
    await get_db() 
    btns = [[InlineKeyboardButton(f"❌ {p.get('type', '').upper()} | {p['details']}", callback_data=f"delpkg_{p['_id']}")] for p in await pkgs_col.find().to_list(100)] 
    await m.reply("প্যাকেজ ডিলিট করতে নিচে ক্লিক করুন / Click to delete:", reply_markup=InlineKeyboardMarkup(btns) if btns else None)

@app.on_callback_query(filters.regex(r"^delpkg_") & filters.user(ADMIN_ID) & unique_cb)
async def delpkg_cb(c, q): 
    await get_db(); await pkgs_col.delete_one({"_id": ObjectId(q.data.split("_")[1])}); await q.message.edit_text("✅ Package Deleted!")

@app.on_message(filters.command("prparadd") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_prparadd(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/prparadd <seconds>`\nউদাহরণ: `/prparadd 10`")
    await get_db(); await config_col.update_one({"_id": "settings"}, {"$set": {"prem_vid_wait": int(m.text.split()[1])}}); await m.reply("✅ Premium Vid Wait Time Set!")

@app.on_message(filters.command("addrdiem") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_addrdiem(c, m): 
    if len(m.text.split()) < 4: return await m.reply("❌ সঠিক নিয়ম: `/addrdiem <UserID> <Amount> <Unit>`\nউদাহরণ: `/addrdiem 12345678 30 d`")
    await get_db() 
    p = m.text.split(); u_id, amt, unit = int(p[1]), int(p[2]), p[3].lower() 
    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(unit, 0) 
    await users_col.update_one({"_id": u_id}, {"$set": {"premium_until": datetime.now() + timedelta(seconds=amt * secs)}}) 
    await m.reply(f"✅ User {u_id} made Premium!"); await c.send_message(u_id, f"🎉 You received {amt} {unit} Premium!")

@app.on_message(filters.command("delpremium") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_delpremium(c, m): 
    if len(m.text.split()) < 2: return await m.reply("❌ সঠিক নিয়ম: `/delpremium <UserID>`\nউদাহরণ: `/delpremium 12345678`")
    await get_db(); await users_col.update_one({"_id": int(m.text.split()[1])}, {"$set": {"premium_until": None}}); await m.reply("✅ Premium Removed!")

@app.on_message(filters.command("brodcast") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_brodcast(c, m): 
    await get_db() 
    if not m.reply_to_message: return await m.reply("❌ Reply to a message (text, image, video, button) / কোনো মেসেজে রিপ্লাই করে দিন।") 
    msg = await m.reply("⏳ Broadcasting to all users..."); success = 0 
    for u in await users_col.find().to_list(None): 
        try: 
            await m.reply_to_message.copy(u["_id"], reply_markup=m.reply_to_message.reply_markup)
            success += 1
            await asyncio.sleep(0.05) 
        except: pass 
    await msg.edit_text(f"✅ Sent successfully to {success} users.")

@app.on_message(filters.command("cnlbdcst") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_cnlbdcst(c, m): 
    if not m.reply_to_message: return await m.reply("❌ Reply to a message (text, media, button) / কোনো মেসেজে রিপ্লাই করে /cnlbdcst দিন।") 
    msg = await m.reply("⏳ Broadcasting to channels/groups...")
    success = 0
    await get_db()
    config = await get_config()
    
    # সংগৃহীত সকল চ্যানেল/গ্রুপ এর আইডি সেট
    ch_set = set()
    if config.get("autopost_channel"): ch_set.add(config["autopost_channel"])
    async for ch in auto_channels_col.find(): ch_set.add(int(ch["chat_id"]))
    async for ch in channels_col.find({"type": "must_join"}): ch_set.add(ch["chat_id"])
    
    # ডাটাবেসে থাকা চ্যানেলগুলোতে পাঠানো
    for ch_id in ch_set:
        try:
            await m.reply_to_message.copy(ch_id, reply_markup=m.reply_to_message.reply_markup)
            success += 1
            await asyncio.sleep(0.5)
        except: pass
        
    # ডায়ালগ লিস্ট থেকে পাওয়া এডমিন থাকা অন্য চ্যানেলগুলোতেও পাঠানো
    try:
        async for dialog in c.get_dialogs():
            if dialog.chat.type in [enums.ChatType.CHANNEL, enums.ChatType.SUPERGROUP, enums.ChatType.GROUP]:
                if dialog.chat.id not in ch_set:
                    try:
                        await m.reply_to_message.copy(dialog.chat.id, reply_markup=m.reply_to_message.reply_markup)
                        success += 1
                        ch_set.add(dialog.chat.id)
                        await asyncio.sleep(0.5)
                    except: pass
    except Exception as e: print("Dialogs error:", e)
    
    await msg.edit_text(f"✅ Message Broadcasted successfully to {success} Channels/Groups!")

@app.on_message(filters.command("allred") & filters.user(ADMIN_ID) & unique_msg) 
async def cmd_allred(c, m): 
    if len(m.text.split()) < 3: return await m.reply("❌ সঠিক নিয়ম: `/allred <Limit> <Min-Max>`\nউদাহরণ: `/allred 100 10-50`")
    await get_db() 
    p = m.text.split(); lim, rng = int(p[1]), p[2].split("-") 
    code = "RND" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)) 
    await coupons_col.insert_one({"code": code, "limit": lim, "used_by": [], "is_random": True, "min": int(rng[0]), "max": int(rng[1])}) 
    await m.reply(f"✅ Code: {code}\nLimit: {lim}\nCoins: {rng[0]}-{rng[1]}")

# ==========================================
# 7. FLASK WEB API
# ==========================================
@web.route('/api/get_ad/<int:user_id>/<file_id>') 
def get_ad_api(user_id, file_id): 
    config = sync_db["config"].find_one({"_id": "settings"}) or {} 
    user = sync_db["users"].find_one({"_id": user_id}) 
    
    file_data = None
    for d in get_extra_dbs_sync():
        file_data = d["files"].find_one({"_id": file_id})
        if file_data: break

    if user and user.get("premium_until") and user["premium_until"] > datetime.now(): 
        return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})

    unlocked_files = user.get("unlocked_files", {})
    if file_id in unlocked_files:
        try:
            date_str = unlocked_files[file_id].replace("Z", "")
            expiry_date = datetime.fromisoformat(date_str)
            if expiry_date > datetime.utcnow():
                return jsonify({"show_ad": False, "is_unlocked": True, "requires_coin": False})
        except Exception as e: pass
        
    is_prem_vid = file_data and file_data.get("is_premium")
    if is_prem_vid:
        return jsonify({"show_ad": False, "is_unlocked": False, "requires_coin": True, "coin_price": config.get("premium_vid_coin", 50)})
        
    if config.get("ads_on", True):
        links = list(sync_db["ad_links"].find())
        if links:
            ad_link = random.choice(links)["link"]
            wait_time = random.choice(config.get("direk_wait", [5]))
            steps = config.get("regstep", 1)
            return jsonify({"show_ad": True, "ad_link": ad_link, "wait_time": wait_time, "steps": steps, "requires_coin": False, "coin_price": config.get("premium_vid_coin", 50)})
            
    return jsonify({"show_ad": False, "is_unlocked": True})

@web.route('/api/unlock_file', methods=['POST'])
def unlock_file():
    data = request.json
    uid, f_id, method = data['uid'], data['file_id'], data['method']
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid})
    
    if method == "coin":
        cost = config.get("premium_vid_coin", 50)
        if user.get("balance", 0) < cost:
            return jsonify({"status": "error", "msg": "❌ আপনার পর্যাপ্ত কয়েন নেই! / Insufficient Coins!"})
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -cost}})
        
    relock_min = config.get("vid_relock_min", 1440)
    if relock_min > 0:
        expiry = (datetime.utcnow() + timedelta(minutes=relock_min)).isoformat() + "Z"
    else:
        expiry = (datetime.utcnow() + timedelta(days=36500)).isoformat() + "Z"

    sync_db["users"].update_one({"_id": uid}, {"$set": {f"unlocked_files.{f_id}": expiry}})
    return jsonify({"status": "success", "msg": "Unlocked!", "expiry": expiry})

@web.route('/api/earn_info/<int:uid>')
def get_earn_info(uid):
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid})
    today = datetime.now().strftime("%Y-%m-%d")
    
    if user.get("last_activity_date") != today:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": today, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}})
        user["spin_count"] = 0
        user["task_count"] = 0
        user["custom_tasks_done"] = []

    spin_lim = config.get("spin_limit", 5)
    task_lim = config.get("task_limit", 5)
    
    c_tasks = list(sync_db["custom_tasks"].find())
    for t in c_tasks: t["_id"] = str(t["_id"])
    
    return jsonify({
        "spins_done": user.get("spin_count", 0), "spin_limit": spin_lim,
        "tasks_done": user.get("task_count", 0), "task_limit": task_lim,
        "custom_tasks": c_tasks,
        "custom_tasks_done": user.get("custom_tasks_done", [])
    })

@web.route('/api/get_earn_ad/<ad_type>')
def get_earn_ad_dynamic(ad_type):
    links = list(sync_db["ad_links"].find())
    if not links: return jsonify({"ad_link": None})
    return jsonify({"ad_link": random.choice(links)["link"], "wait_time": 15})

@web.route('/api/claim_earn', methods=['POST'])
def claim_earn():
    data = request.json
    uid, task_type = data['uid'], data['type']
    config = sync_db["config"].find_one({"_id": "settings"}) or {}
    user = sync_db["users"].find_one({"_id": uid})
    today = datetime.now().strftime("%Y-%m-%d")
    
    if user.get("last_activity_date") != today:
        sync_db["users"].update_one({"_id": uid}, {"$set": {"last_activity_date": today, "spin_count": 0, "task_count": 0, "custom_tasks_done": []}})
        user["spin_count"] = 0; user["task_count"] = 0; user["custom_tasks_done"] = []

    if task_type == "spin":
        limit = config.get("spin_limit", 5)
        if user.get("spin_count", 0) >= limit:
            return jsonify({"status": "error", "msg": "❌ আজকের স্পিন লিমিট শেষ! / Daily Spin Limit Reached!"})
        
        coin = random.randint(config.get("spin_coin_min", 10), config.get("spin_coin_max", 50))
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin, "spin_count": 1}})
        return jsonify({"status": "success", "msg": f"🎉 আপনি স্পিন করে {coin} কয়েন পেয়েছেন!"})
        
    elif task_type == "task":
        limit = config.get("task_limit", 5)
        if user.get("task_count", 0) >= limit:
            return jsonify({"status": "error", "msg": "❌ আজকের টাস্ক লিমিট শেষ! / Daily Task Limit Reached!"})
            
        coin = config.get("task_coin", 20)
        sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin, "task_count": 1}})
        return jsonify({"status": "success", "msg": f"🎉 টাস্ক কমপ্লিট করে {coin} কয়েন পেয়েছেন!"})

    elif task_type.startswith("custom_"):
        tid = task_type.split("_")[1]
        if tid in user.get("custom_tasks_done", []):
            return jsonify({"status": "error", "msg": "❌ আপনি এই টাস্কটি আগেই করেছেন! / Task already done!"})
            
        task_data = sync_db["custom_tasks"].find_one({"_id": ObjectId(tid)})
        if task_data:
            sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": task_data["coin"]}, "$push": {"custom_tasks_done": tid}})
            return jsonify({"status": "success", "msg": f"🎉 টাস্ক করে {task_data['coin']} কয়েন পেয়েছেন!"})
        
    return jsonify({"status": "error", "msg": "Invalid Task"})

@web.route('/api/redeem', methods=['POST']) 
def redeem_coupon(): 
    data = request.json 
    uid, code = data['uid'], data['code'] 
    coupon = sync_db["coupons"].find_one({"code": code})

    if not coupon: return jsonify({"status": "error", "msg": "❌ Invalid Coupon Code! / কোডটি সঠিক নয়!"})
    if uid in coupon.get("used_by", []): return jsonify({"status": "error", "msg": "❌ You already used this! / আপনি এটি ব্যবহার করেছেন!"})
    if len(coupon.get("used_by", [])) >= coupon.get("limit", 0): return jsonify({"status": "error", "msg": "❌ Coupon limit reached! / কুপনের মেয়াদ শেষ!"})

    coin_to_add = random.randint(coupon["min"], coupon["max"]) if coupon.get("is_random") else coupon.get("coins", 0)
    sync_db["coupons"].update_one({"code": code}, {"$push": {"used_by": uid}})
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": coin_to_add}})

    try: asyncio.run_coroutine_threadsafe(app.send_message(uid, f"🎉 কুপন রিডিম সফল! / Redeemed Successfully! +{coin_to_add} Coins"), loop)
    except: pass
    return jsonify({"status": "success", "msg": f"✅ Successfully redeemed {coin_to_add} coins!"})

@web.route('/api/buy_with_coin', methods=['POST']) 
def buy_with_coin(): 
    data = request.json 
    uid, pkg_id = data['uid'], data['pkg_id'] 
    user = sync_db["users"].find_one({"_id": uid}) 
    pkg = sync_db["packages"].find_one({"_id": ObjectId(pkg_id)})

    if not user or not pkg: return jsonify({"status": "error", "msg": "Invalid Request"})
    if user.get("balance", 0) < pkg["coins"]: return jsonify({"status": "error", "msg": "❌ Insufficient Coins! / পর্যাপ্ত কয়েন নেই!"})

    secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "y": 31536000}.get(pkg.get("unit", "d").lower()[0], 86400)
    sync_db["users"].update_one({"_id": uid}, {"$inc": {"balance": -pkg["coins"]}, "$set": {"premium_until": datetime.now() + timedelta(seconds=pkg["amount"] * secs)}})
    try: asyncio.run_coroutine_threadsafe(app.send_message(uid, f"🎉 আপনি {pkg['coins']} কয়েন দিয়ে প্রিমিয়াম কিনেছেন! / You bought Premium!"), loop)
    except: pass
    return jsonify({"status": "success", "msg": "✅ Premium Purchased Successfully!"})

@web.route('/api/action', methods=['POST'])
def handle_actions():
    data = request.json
    action, uid, f_id = data.get('action'), data.get('uid'), data.get('file_id')
    if action == "history":
        sync_db["users"].update_one({"_id": uid}, {"$addToSet": {"history": f_id}})
    elif action == "like":
        for d in get_extra_dbs_sync():
            res = d["files"].update_one({"_id": f_id}, {"$addToSet": {"likes": uid}})
            if res.modified_count > 0: break
    elif action == "comment":
        comment = {"uid": uid, "text": data.get("text"), "time": datetime.now().strftime("%Y-%m-%d %H:%M")}
        for d in get_extra_dbs_sync():
            res = d["files"].update_one({"_id": f_id}, {"$push": {"comments": comment}})
            if res.modified_count > 0: break
        return jsonify({"status": "success", "comment": comment})
    return jsonify({"status": "success"})

@web.route('/api/user/<int:user_id>') 
def get_user(user_id): 
    user = sync_db["users"].find_one({"_id": user_id}) 
    is_prem = False; expiry_str = None 
    if user and user.get("premium_until"): 
        now = datetime.now() 
        if user["premium_until"] > now: 
            is_prem = True 
            sec = int((user["premium_until"] - now).total_seconds()) 
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
            expiry_str = " ".join(res)

    return jsonify({
        "balance": user.get("balance", 0) if user else 0, 
        "is_premium": is_prem, 
        "expiry": expiry_str, 
        "history": user.get("history", []) if user else [],
        "unlocked_files": user.get("unlocked_files", {}) if user else {}
    })

# ==========================================
# 8. HTML UI & LOGIC 
# ==========================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <script src="https://telegram.org/js/telegram-web-app.js"></script>
    <style>
        body { background: linear-gradient(180deg, #18091c 0%, #081016 100%); color: #fff; font-family: 'Hind Siliguri', sans-serif; margin: 0; padding-bottom: 90px; min-height: 100vh; }
        * { box-sizing: border-box; }
        .header { display: flex; justify-content: space-between; padding: 15px 20px; align-items: center; }
        .logo { font-size: 20px; font-weight: bold; }
        .coin-pill { background: #ffb703; color: #000; padding: 5px 12px; border-radius: 20px; font-weight: bold; font-size: 14px; }
        .lang-btn { background: rgba(255,255,255,0.1); color: white; border: 1px solid #fff; padding: 4px 10px; border-radius: 12px; font-size: 12px; cursor: pointer; margin-right: 10px; }
        .prem-badge { background: #c72cff; padding: 2px 8px; border-radius: 10px; font-size: 10px; display:none; margin-left: 5px; }
        .page { display: none; padding: 15px; }
        .page.active { display: block; animation: fadeIn 0.3s ease-in-out; }
        @keyframes fadeIn { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
        
        .bottom-nav { position: fixed; bottom: 0; width: 100%; background: rgba(14,20,30,0.95); display: flex; justify-content: space-around; padding: 10px 0; border-top: 1px solid rgba(255,255,255,0.05); backdrop-filter: blur(10px); z-index: 100; overflow-x: auto; }
        .nav-item { display: flex; flex-direction: column; align-items: center; font-size: 11px; color: #777; cursor: pointer; padding: 5px 10px; border-radius: 12px; min-width: 60px; }
        .nav-item.active { color: #fff; background: rgba(255,255,255,0.05); }
        .nav-item span { font-size: 22px; margin-bottom: 2px; filter: grayscale(100%); }
        .nav-item.active span { filter: grayscale(0%); }

        .search-box { width: 100%; padding: 14px 20px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.1); background: rgba(0,0,0,0.4); color: white; outline: none; margin-bottom: 15px; font-size: 15px; }
        .slider-title { font-size: 16px; font-weight: bold; margin-bottom: 10px; color: #ffb703; }
        .slider-container { display: flex; overflow-x: auto; gap: 12px; padding-bottom: 10px; margin-bottom: 20px; scrollbar-width: none; }
        .slider-container::-webkit-scrollbar { display: none; }
        .slider-card { min-width: 150px; background: rgba(25,25,35,0.8); border-radius: 12px; position: relative; border: 1px solid rgba(255,183,3,0.3); overflow: hidden; }
        .slider-card img { width: 100%; height: 90px; object-fit: cover; }
        .slider-card .s-title { font-size: 12px; padding: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .slider-play { position: absolute; top: 25%; left: 50%; transform: translate(-50%, -50%); width: 40px; height: 40px; background: rgba(0,123,255,0.8); border-radius: 50%; display: flex; justify-content: center; align-items: center; cursor: pointer; box-shadow: 0 0 10px rgba(0,123,255,0.5); }
        .slider-play::after { content: '▶'; color: white; font-size: 16px; margin-left: 3px; }

        .video-card { background: rgba(25,25,35,0.8); border-radius: 12px; margin-bottom: 20px; overflow: hidden; position: relative; border: 1px solid rgba(255,255,255,0.05); }
        .video-card img { width: 100%; height: 210px; object-fit: cover; }
        .tag-premium { position: absolute; top: 12px; left: 12px; background: #c72cff; padding: 4px 12px; border-radius: 15px; font-size: 11px; font-weight: bold; box-shadow: 0 2px 10px rgba(199,44,255,0.5); z-index: 10; }
        .tag-regular { position: absolute; top: 12px; left: 12px; background: #00d4ff; color: black; padding: 4px 12px; border-radius: 15px; font-size: 11px; font-weight: bold; box-shadow: 0 2px 10px rgba(0,212,255,0.5); z-index: 10; }
        .play-btn-overlay { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 55px; height: 55px; background: rgba(0,123,255,0.8); border-radius: 50%; display: flex; justify-content: center; align-items: center; cursor: pointer; backdrop-filter: blur(5px); box-shadow: 0 0 15px rgba(0,123,255,0.4); z-index:15; }
        .play-btn-overlay::after { content: '▶'; color: white; font-size: 22px; margin-left: 4px; }
        .video-info { padding: 15px; }
        
        .video-actions { display: flex; justify-content: space-between; padding: 10px 15px; border-top: 1px solid rgba(255,255,255,0.05); background: rgba(0,0,0,0.2); }
        .video-actions button { background: rgba(255,255,255,0.08); border: none; color: white; padding: 8px 12px; border-radius: 8px; cursor: pointer; font-size: 12px; display:flex; align-items:center; gap:5px; transition: 0.2s; }

        .pagination { display: flex; justify-content: center; gap: 5px; flex-wrap: wrap; margin-top: 15px; }
        .page-btn { background: rgba(255,255,255,0.1); color: white; border: none; padding: 8px 12px; border-radius: 8px; cursor: pointer; font-weight: bold; }
        .page-btn.active { background: linear-gradient(90deg, #f02d73, #ff6b6b); }
        .page-btn:disabled { opacity: 0.5; cursor: not-allowed; }

        .cat-btn { background: rgba(255,255,255,0.05); padding: 8px 18px; border-radius: 25px; font-size: 13px; cursor: pointer; white-space: nowrap; border: 1px solid rgba(255,255,255,0.1); }
        .cat-btn.active { background: linear-gradient(90deg, #f02d73, #00d4ff); color: white; border: none; font-weight: bold; }
        .balance-card { background: linear-gradient(135deg, #4b2354, #1b1c29); border-radius: 15px; padding: 25px; text-align: center; margin-bottom: 20px; border: 1px solid rgba(255,255,255,0.05); }
        .set-item { display: flex; align-items: center; background: rgba(255,255,255,0.03); padding: 15px; border-radius: 12px; border: 1px solid rgba(255,255,255,0.05); margin-bottom: 12px; cursor: pointer; }
        .set-icon { width: 45px; height: 45px; border-radius: 12px; display: flex; justify-content: center; align-items: center; font-size: 20px; margin-right: 15px; }
        .share-banner { background: rgba(0,255,100,0.05); border: 1px solid rgba(0,255,100,0.2); padding: 20px; border-radius: 12px; font-size: 14px; line-height: 1.6; margin-bottom: 25px; }
        .ref-box { display: flex; background: rgba(255,255,255,0.05); border-radius: 10px; border: 1px solid rgba(255,255,255,0.1); margin-bottom: 25px; overflow: hidden; }
        .ref-box input { flex: 1; background: transparent; border: none; color: white; padding: 15px; font-size: 14px; outline: none; }
        .ref-box button { background: rgba(255,255,255,0.1); color: #00d4ff; border: none; padding: 0 20px; font-weight: bold; cursor: pointer; }

        .btn-main { width: 100%; background: linear-gradient(90deg, #f02d73, #ff6b6b); padding: 16px; border-radius: 12px; font-weight: bold; border: none; color: white; font-size: 16px; cursor: pointer; }
        .btn-main:disabled { opacity: 0.5; cursor: not-allowed; }
        
        .modal-overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(8,16,22,0.98); z-index: 9999; justify-content: center; align-items: center; backdrop-filter: blur(5px); }
        .modal-box { background: rgba(30, 30, 45, 0.95); padding: 30px 25px; border-radius: 20px; text-align: center; width: 90%; max-width: 400px; border: 1px solid rgba(255,255,255,0.05); position:relative;}
        .close-btn { position: absolute; top:10px; right:15px; background:transparent; border:none; color:white; font-size:20px; cursor:pointer;}
        .alert-box { background: rgba(255,0,0,0.1); border: 1px solid #ff4d4d; color: #ffb3b3; padding: 15px; border-radius: 12px; font-size: 13px; margin: 15px 0; }
        .lang-en { display: none; }
        
        #comment-list { max-height:200px; overflow-y:auto; text-align:left; margin-bottom:15px; background:rgba(0,0,0,0.3); padding:10px; border-radius:10px;}
        .cmt-item { border-bottom: 1px solid rgba(255,255,255,0.05); padding:5px 0; font-size:13px; }
        .cmt-item:last-child { border:none; }
        
        .earn-card { background: rgba(25,25,35,0.8); border: 1px solid rgba(0,212,255,0.3); padding:20px; border-radius:15px; text-align:center; margin-bottom:15px; position:relative; }
        .limit-badge { position: absolute; top:10px; right:10px; background: rgba(0,0,0,0.5); padding: 3px 8px; border-radius: 10px; font-size:11px; border: 1px solid rgba(255,255,255,0.2); }
        
        .wheel-container { position: relative; width: 120px; height: 120px; margin: 0 auto 15px auto; }
        .wheel { width: 100%; height: 100%; border-radius: 50%; border: 4px solid #ffb703; background: conic-gradient(#ff6b6b 0% 16.6%, #f02d73 16.6% 33.3%, #c72cff 33.3% 50%, #00d4ff 50% 66.6%, #00ffcc 66.6% 83.3%, #ffb703 83.3% 100%); transition: transform 3s cubic-bezier(0.25, 1, 0.5, 1); transform: rotate(0deg); }
        .wheel-pointer { position: absolute; top: -10px; left: 50%; transform: translateX(-50%); width: 0; height: 0; border-left: 10px solid transparent; border-right: 10px solid transparent; border-top: 15px solid white; z-index: 10; }
        .wheel-center { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 30px; height: 30px; background: #fff; border-radius: 50%; z-index: 5; box-shadow: 0 0 5px rgba(0,0,0,0.5); }
    </style>
</head>
<body>

<div class="header">
    <div class="logo">{{ site_name }}<span class="prem-badge" id="prem-badge">Premium</span></div>
    <div style="display:flex; align-items:center;">
        <button class="lang-btn" onclick="toggleLanguage()" id="lang-btn">English</button>
        <div class="coin-pill">🏛 <span id="hdr-balance">0</span></div>
    </div>
</div>

<div id="age-modal" class="modal-overlay">
    <div class="modal-box">
        <div style="font-size: 55px; margin-bottom:10px;">🔞</div>
        <h2 style="margin-top:0;"><span class="lang-bn">বয়স নিশ্চিতকরণ</span><span class="lang-en">Age Verification</span></h2>
        <p style="font-size:14px; color:#aaa;">
            <span class="lang-bn">এই ওয়েবসাইটের কনটেন্ট শুধুমাত্র <b style="color:#00d4ff;">১৮ বছর বা তার বেশি বয়সী</b> ব্যবহারকারীদের জন্য প্রযোজ্য।</span>
            <span class="lang-en">This website content is strictly for users who are <b style="color:#00d4ff;">18 years of age or older</b>.</span>
        </p>
        <div class="alert-box"><span class="lang-bn">⚠️ আপনার বয়স ১৮+ না হলে সাইটটি ব্যবহার করবেন খন।</span><span class="lang-en">⚠️ Do not enter if you are under 18.</span></div>
        <button class="btn-main" style="background: linear-gradient(90deg, #00d4ff, #00ffcc); color:black; margin-bottom:10px;" onclick="confirmAge()"><span class="lang-bn">✅ হ্যাঁ, আমার বয়স ১৮+ বছর</span><span class="lang-en">✅ Yes, I am 18+</span></button>
        <button class="btn-main" style="background: transparent; border: 1px solid #555; color: #888;" onclick="window.close()"><span class="lang-bn">❌ না, বের হয়ে যান</span><span class="lang-en">❌ No, Exit</span></button>
    </div>
</div>

<div id="regular-choice-modal" class="modal-overlay">
    <div class="modal-box">
        <button class="close-btn" onclick="document.getElementById('regular-choice-modal').style.display='none'">✖</button>
        <div style="font-size: 40px; margin-bottom:10px;">🎬</div>
        <h3 style="margin-top:0;">Unlock Video</h3>
        <p style="color:#aaa; font-size:14px;"><span class="lang-bn">আপনি কিভাবে ভিডিওটি আনলক করতে চান?</span><span class="lang-en">How do you want to unlock?</span></p>
        <button class="btn-main" style="background: linear-gradient(90deg, #00d4ff, #00ffcc); color:black; margin-top:10px;" onclick="document.getElementById('regular-choice-modal').style.display='none'; startAdProcess();">📺 Watch Ad (Free)</button>
        <button class="btn-main" style="background: linear-gradient(90deg, #c72cff, #ff007f); margin-top:10px;" onclick="document.getElementById('regular-choice-modal').style.display='none'; unlockWithCoin();">💎 Use <span id="reg-coin-price"></span> Coins</button>
    </div>
</div>

<div id="ad-overlay" class="modal-overlay">
    <div class="modal-box" style="background: rgba(20,20,30,0.95); border: 2px solid #00d4ff; box-shadow: 0 0 20px rgba(0,212,255,0.4); border-radius: 20px;">
        <h2 style="color: #00d4ff; margin-top: 0;">🚀 Ad Verification</h2>
        <div style="background: rgba(0,0,0,0.4); padding: 10px; border-radius: 12px; margin-bottom: 5px; font-weight:bold; color: #fff; font-size: 15px; border: 1px solid rgba(255,255,255,0.1);" id="step-info">
            Step 1 of 1
        </div>
        <div id="duration-info" style="font-size: 12px; color: #aaa; margin-bottom: 15px;">⏳ Duration: 5 Seconds / Step</div>
        <div style="position: relative; width: 120px; height: 120px; margin: 0 auto 15px auto; border-radius: 50%; border: 4px solid rgba(255,255,255,0.1); display: flex; align-items: center; justify-content: center; background: radial-gradient(circle, rgba(255,0,127,0.2) 0%, transparent 70%);">
            <h1 id="timer-count" style="font-size:50px; color:#f02d73; margin:0;">5</h1>
            <span style="position: absolute; bottom: 15px; font-size: 11px; color: #aaa;">Seconds</span>
        </div>
        <p style="font-size:14px; color:#aaa; margin-bottom: 20px;">
            <span class="lang-bn">ফাইলটি পেতে সম্পূর্ণ অ্যাডটি দেখুন। ব্যাক দিলে টাইম স্টপ হয়ে যাবে।</span>
            <span class="lang-en">Watch the ad completely. Timer pauses if you go back.</span>
        </p>
        <button id="get-file-btn" class="btn-main" style="background: linear-gradient(90deg, #00d4ff, #00ffcc); color:black; display:none; font-weight: bold; font-size: 18px; padding: 12px;">Next Step</button>
    </div>
</div>

<div id="coin-unlock-modal" class="modal-overlay">
    <div class="modal-box">
        <button class="close-btn" onclick="document.getElementById('coin-unlock-modal').style.display='none'">✖</button>
        <div style="font-size: 40px; margin-bottom:10px;">💎</div>
        <h3 style="margin-top:0;">Premium Video Unlock</h3>
        <p style="color:#aaa; font-size:14px;"><span class="lang-bn">এই প্রিমিয়াম ভিডিওটি দেখতে <b><span id="coin-unlock-price">0</span> কয়েন</b> প্রয়োজন।</span><span class="lang-en">You need <b><span id="coin-unlock-price-en">0</span> coins</b> to unlock this premium video.</span></p>
        <button class="btn-main" style="background: linear-gradient(90deg, #c72cff, #ff007f); margin-top:10px;" onclick="unlockWithCoin()">Unlock Now</button>
    </div>
</div>

<div id="comment-modal" class="modal-overlay">
    <div class="modal-box">
        <button class="close-btn" onclick="document.getElementById('comment-modal').style.display='none'">✖</button>
        <h3>💬 Comments</h3>
        <div id="comment-list"></div>
        <div style="display:flex; gap:10px;">
            <input type="text" id="cmt-input" class="search-box" style="margin:0;" placeholder="Write a comment...">
            <button class="btn-main" style="width:auto; padding:0 20px;" onclick="sendComment()">Send</button>
        </div>
    </div>
</div>

<div id="page-home" class="page active">
    <div class="slider-title">🔥 <span class="lang-bn">টপ ট্রেন্ডিং ভিডিও</span><span class="lang-en">Top Trending Videos</span></div>
    <div class="slider-container" id="top-slider"></div>
    <input type="text" id="search-bar" class="search-box bn-pl" placeholder="🔍 Search all videos..." onkeyup="handleSearch('home')">
    <div id="home-video-list"></div>
    <div class="pagination" id="home-pagination"></div>
</div>

<div id="page-regvids" class="page">
    <input type="text" id="search-bar-reg" class="search-box bn-pl" placeholder="🔍 Search Regular Videos..." onkeyup="handleSearch('reg')">
    <div id="reg-video-list"></div>
    <div class="pagination" id="reg-pagination"></div>
</div>

<div id="page-premvids" class="page">
    <input type="text" id="search-bar-prem" class="search-box bn-pl" placeholder="🔍 Search Premium Videos..." onkeyup="handleSearch('prem')">
    <div id="prem-video-list"></div>
    <div class="pagination" id="prem-pagination"></div>
</div>

<div id="page-history" class="page">
    <h2 style="margin-top:0;">🕒 <span class="lang-bn">আপনার দেখা ভিডিও</span><span class="lang-en">Watch History</span></h2>
    <div id="history-video-list"></div>
</div>

<div id="page-earn" class="page">
    <h2 style="margin-top:0;">🎯 <span class="lang-bn">কয়েন আয় করুন</span><span class="lang-en">Earn Coins</span></h2>
    <div id="earn-content">Loading...</div>
</div>

<div id="page-premium" class="page">
    <h2 style="margin-top:0;">Premium / Buy Coins</h2>
    <div style="display:flex; gap:10px; margin-bottom:20px;">
        <div class="cat-btn active" style="flex:1; text-align:center; border-radius:12px;" onclick="togglePkg('bks', this)">📱 BDT</div>
        <div class="cat-btn" style="flex:1; text-align:center; border-radius:12px;" onclick="togglePkg('usd', this)">⚡ USD</div>
        <div class="cat-btn" style="flex:1; text-align:center; border-radius:12px;" onclick="togglePkg('coin', this)">🪙 Coins</div>
    </div>
    
    <div id="pkg-bks">
        {% for pkg in pkgs if pkg.type == 'bkash' %}
        <div style="background: rgba(255,255,255,0.03); border: 1px solid #ffb703; padding: 20px; border-radius: 15px; margin-bottom: 15px; text-align: center; position: relative;">
            <div style="position: absolute; top: -12px; left: 50%; transform: translateX(-50%); background: #ffb703; color: black; font-size: 11px; font-weight: bold; padding: 4px 15px; border-radius: 12px;">⭐ BDT Premium</div>
            <div style="font-size:35px; margin-bottom:10px;">🏛</div>
            <h2 style="margin:0 0 5px 0; font-size:24px;">{{ pkg.details.split('=')[0] if '=' in pkg.details else pkg.details }}</h2>
            <p style="color:#aaa; font-size:14px; margin:0 0 15px 0;">{{ pkg.details.split('=')[1] if '=' in pkg.details else pkg.details }}</p>
            <button class="btn-main" style="margin:0; padding:12px;" onclick="reqBuy()"><span class="lang-bn">কিনুন</span><span class="lang-en">Buy Now</span></button>
        </div>
        {% endfor %}
    </div>
    
    <div id="pkg-usd" style="display:none;">
        {% for pkg in pkgs if pkg.type == 'usd' %}
        <div style="background: rgba(255,255,255,0.03); border: 1px solid #00d4ff; padding: 20px; border-radius: 15px; margin-bottom: 15px; text-align: center; position: relative;">
            <div style="position: absolute; top: -12px; left: 50%; transform: translateX(-50%); background: #00d4ff; color: black; font-size: 11px; font-weight: bold; padding: 4px 15px; border-radius: 12px;">⚡ USD Premium</div>
            <div style="font-size:35px; margin-bottom:10px;">💲</div>
            <h2 style="margin:0 0 5px 0; font-size:24px;">{{ pkg.details.split('=')[0] if '=' in pkg.details else pkg.details }}</h2>
            <p style="color:#aaa; font-size:14px; margin:0 0 15px 0;">{{ pkg.details.split('=')[1] if '=' in pkg.details else pkg.details }}</p>
            <button class="btn-main" style="margin:0; padding:12px; background:linear-gradient(90deg, #00d4ff, #00ffcc); color:black;" onclick="reqBuy()"><span class="lang-bn">কিনুন</span><span class="lang-en">Buy Now</span></button>
        </div>
        {% endfor %}
    </div>

    <div id="pkg-coin" style="display:none;">
        {% for pkg in pkgs if pkg.type == 'coin' %}
        <div style="background: rgba(255,255,255,0.03); border: 1px solid #c72cff; padding: 20px; border-radius: 15px; margin-bottom: 15px; text-align: center; position: relative;">
            <div style="position: absolute; top: -12px; left: 50%; transform: translateX(-50%); background: #c72cff; color: white; font-size: 11px; font-weight: bold; padding: 4px 15px; border-radius: 12px;">🪙 Buy with Coin</div>
            <div style="font-size:35px; margin-bottom:10px;">💎</div>
            <h2 style="margin:0 0 5px 0; font-size:24px;">{{ pkg.details.split('=')[0] if '=' in pkg.details else pkg.details }}</h2>
            <p style="color:#aaa; font-size:14px; margin:0 0 15px 0;">{{ pkg.details.split('=')[1] if '=' in pkg.details else pkg.details }} VIP (No Ads)</p>
            <button class="btn-main" style="margin:0; padding:12px; background:#c72cff;" onclick="buyWithCoin('{{ pkg._id }}', {{ pkg.coins }})"><span class="lang-bn">কয়েন দিয়ে নিন</span><span class="lang-en">Exchange Coin</span></button>
        </div>
        {% endfor %}
    </div>
</div>

<div id="page-settings" class="page">
    <div style="text-align: center; margin-bottom: 20px;">
        <img id="user-profile-pic" src="https://placehold.co/100x100/1c1c24/ff007f?text=User" style="width: 85px; height: 85px; border-radius: 50%; border: 3px solid #00d4ff; object-fit: cover; margin-bottom: 10px; box-shadow: 0 0 15px rgba(0,212,255,0.3);">
        <h2 id="user-full-name" style="margin: 0; font-size: 22px; color: #fff;">User Name</h2>
        <p id="user-username" style="margin: 5px 0 0 0; color: #aaa; font-size: 14px;">@username</p>
        <p style="margin: 5px 0 0 0; color: #666; font-size: 12px;">ID: <span id="set-id"></span></p>
        <div id="mem-status" style="display:inline-block; padding:5px 15px; border-radius:15px; font-size:12px; font-weight:bold; background:rgba(255,255,255,0.1); margin-top:10px;">👤 Regular Member</div>
        <div id="mem-expiry" style="color:#ffb703; font-size:11px; margin-top:5px; display:none;"></div>
    </div>

    <div class="balance-card">
        <p style="margin:0; color:#aaa; font-size:12px; letter-spacing:1px;"><span class="lang-bn">আপনার ব্যালেন্স</span><span class="lang-en">YOUR BALANCE</span></p>
        <h1 style="color:#ffb703; margin:15px 0 10px 0; font-size:48px;">🏛 <span id="set-balance">0</span></h1>
    </div>
    
    <div class="set-item" onclick="switchNav('coupon')">
        <div class="set-icon" style="background: linear-gradient(135deg, #a18cd1, #fbc2eb);">🎟</div>
        <div>
            <b style="display:block; font-size:16px;"><span class="lang-bn">কুপন কোড (রিডিম)</span><span class="lang-en">Redeem Coupon</span></b>
            <span style="color:#aaa; font-size:12px;"><span class="lang-bn">কোড দিয়ে ফ্রি কয়েন বা VIP নিন</span><span class="lang-en">Redeem to get free coins/VIP</span></span>
        </div>
    </div>
    <div class="set-item" onclick="switchNav('share')">
        <div class="set-icon" style="background: linear-gradient(135deg, #ffecd2, #fcb69f);">🎁</div>
        <div>
            <b style="display:block; font-size:16px;"><span class="lang-bn">বন্ধুকে শেয়ার করুন</span><span class="lang-en">Share with Friends</span></b>
            <span style="color:#aaa; font-size:12px;"><span class="lang-bn">ইনভাইট করে বোনাস জিতুন</span><span class="lang-en">Invite and win bonus</span></span>
        </div>
    </div>
</div>

<div id="page-coupon" class="page">
    <h2 style="margin-top:0;">🎟 <span class="lang-bn">কুপন কোড</span><span class="lang-en">Coupon Code</span></h2>
    <p style="font-size:13px; color:#aaa; margin-bottom:20px;">
        <span class="lang-bn">অ্যাডমিনের দেওয়া সিক্রেট কোড বসালে আপনি <b>ফ্রি কয়েন</b> অথবা <b>VIP</b> পাবেন!</span>
        <span class="lang-en">Enter secret code to instantly receive <b>Free Coins</b> or <b>VIP</b>!</span>
    </p>
    <div style="display:flex; gap:10px; margin-bottom:20px;">
        <input type="text" id="coupon-input" class="search-box" style="margin:0; border-radius:12px;" placeholder="Enter coupon">
        <button class="btn-main" style="width:auto; margin:0; padding:0 25px;" onclick="redeemCoupon()">Redeem</button>
    </div>
</div>

<div id="page-share" class="page">
    <h2 style="margin-top:0;">🎁 <span class="lang-bn">শেয়ার করুন</span><span class="lang-en">Share</span></h2>
    <div class="share-banner">
        <span class="lang-bn">🥳 বন্ধু আপনার লিংক দিয়ে স্টার্ট করলেই <b style="color:#ffb703;">{{ ref_coin }} কয়েন</b> বোনাস পাবেন!</span>
        <span class="lang-en">🥳 Get <b style="color:#ffb703;">{{ ref_coin }} Coins</b> bonus when a friend starts using your link!</span>
    </div>
    <div class="ref-box">
        <input type="text" id="ref-link" readonly>
        <button onclick="copyRef()">📋 Copy</button>
    </div>
</div>

<div class="bottom-nav">
    <div class="nav-item active" onclick="switchNav('home', this)"><span>🏠</span> Home</div>
    <div class="nav-item" onclick="switchNav('regvids', this)"><span>👤</span> Regular</div>
    <div class="nav-item" onclick="switchNav('premvids', this)"><span>💎</span> Premium Vids</div>
    <div class="nav-item" onclick="switchNav('earn', this)"><span>🎯</span> Earn</div>
    <div class="nav-item" id="nav-premium" onclick="switchNav('premium', this)"><span>👑</span> Premium</div>
    <div class="nav-item" onclick="switchNav('settings', this)"><span>⚙️</span> Profile</div>
</div>

<script>
    let tg = window.Telegram.WebApp;
    tg.expand();
    let botUsername = "{{ bot_username }}";
    let adminUsername = "{{ config.payment_admin }}";
    
    let userId = 123456789;
    let urlParams = new URLSearchParams(window.location.search);
    let uidFromUrl = urlParams.get('uid');

    if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user) {
        let u = tg.initDataUnsafe.user;
        userId = u.id;
        localStorage.setItem("tg_user_id", userId);
        
        document.getElementById('user-full-name').innerText = (u.first_name + " " + (u.last_name || "")).trim();
        if (u.username) document.getElementById('user-username').innerText = "@" + u.username;
        else document.getElementById('user-username').style.display = 'none';
        
        if (u.photo_url) document.getElementById('user-profile-pic').src = u.photo_url;
        
    } else if (uidFromUrl) {
        userId = parseInt(uidFromUrl);
        localStorage.setItem("tg_user_id", userId);
        document.getElementById('user-full-name').innerText = "User " + userId;
        document.getElementById('user-username').style.display = 'none';
    } else if (localStorage.getItem("tg_user_id")) {
        userId = parseInt(localStorage.getItem("tg_user_id"));
        document.getElementById('user-full-name').innerText = "User " + userId;
        document.getElementById('user-username').style.display = 'none';
    }
    
    document.getElementById('set-id').innerText = userId;
    document.getElementById('ref-link').value = `https://t.me/${botUsername}?start=${userId}`;
    
    let userBalance = 0;
    let userHistory = []; 
    let userUnlockedFiles = {};
    let promotions = {{ promotions_json | safe }};
    
    function openTgLink(url) {
        try {
            if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user) {
                tg.openTelegramLink(url);
            } else { window.location.href = url; }
        } catch(e) { window.location.href = url; }
    }

    let currentLang = localStorage.getItem('appLang') || 'bn';
    function applyLanguage() {
        let isBn = currentLang === 'bn';
        document.querySelectorAll('.lang-bn').forEach(el => el.style.display = isBn ? 'inline-block' : 'none');
        document.querySelectorAll('.lang-en').forEach(el => el.style.display = isBn ? 'none' : 'inline-block');
        document.getElementById('lang-btn').innerText = isBn ? 'English' : 'বাংলা';
        localStorage.setItem('appLang', currentLang);
    }
    function toggleLanguage() { currentLang = currentLang === 'bn' ? 'en' : 'bn'; applyLanguage(); }
    applyLanguage();

    async function loadUser() {
        let res = await fetch('/api/user/' + userId);
        let data = await res.json();
        userBalance = data.balance;
        userHistory = data.history || []; 
        userUnlockedFiles = data.unlocked_files || {};
        
        document.getElementById('hdr-balance').innerText = userBalance;
        document.getElementById('set-balance').innerText = userBalance;
        
        if(data.is_premium) {
            document.getElementById('prem-badge').style.display = 'inline-block';
            let memBox = document.getElementById('mem-status');
            memBox.innerHTML = '💎 Premium Member'; memBox.style.background = 'linear-gradient(90deg, #c72cff, #ff007f)'; memBox.style.color = '#fff';
            document.getElementById('mem-expiry').style.display = 'block';
            document.getElementById('mem-expiry').innerText = (currentLang==='bn'?"মেয়াদ: ":"Expiry: ") + data.expiry;
        } else {
            document.getElementById('prem-badge').style.display = 'none';
            let memBox = document.getElementById('mem-status');
            memBox.innerHTML = '👤 Regular Member'; memBox.style.background = 'rgba(255,255,255,0.1)'; memBox.style.color = '#fff';
            document.getElementById('mem-expiry').style.display = 'none';
        }
        renderHistory();
        loadEarnData();
        updateCountdowns();
    }
    loadUser();

    function updateCountdowns() {
        let now = new Date();
        allFiles.forEach(f => {
            let fId = f._id;
            let badges = document.querySelectorAll(`.lock-status-${fId}`);
            if(badges.length === 0) return;

            let expiryStr = userUnlockedFiles[fId];
            let isUnlocked = false;
            let t = "🔓 ";
            
            if (expiryStr) {
                let safeStr = expiryStr.endsWith('Z') ? expiryStr : expiryStr + 'Z';
                let expiry = new Date(safeStr);
                let diff = expiry - now;
                
                if (diff > 0) {
                    isUnlocked = true;
                    if (diff > 315360000000) {
                        t = currentLang === 'bn' ? "🔓 আজীবন" : "🔓 Forever";
                    } else {
                        let d = Math.floor(diff / 86400000);
                        let h = Math.floor((diff / 3600000) % 24);
                        let m = Math.floor((diff / 60000) % 60);
                        let s = Math.floor((diff / 1000) % 60);
                        
                        let y = Math.floor(d / 365); d = d % 365;
                        let mo = Math.floor(d / 30); d = d % 30;

                        if(y>0) t += y + (currentLang==='bn'?"ব ":"y ");
                        if(mo>0) t += mo + (currentLang==='bn'?"মা ":"mo ");
                        if(d>0) t += d + (currentLang==='bn'?"দি ":"d ");
                        if(h>0) t += h + (currentLang==='bn'?"ঘ ":"h ");
                        if(m>0) t += m + (currentLang==='bn'?"মি ":"m ");
                        t += s + (currentLang==='bn'?"সে":"s");
                    }
                }
            }
            
            badges.forEach(badge => {
                if(isUnlocked) {
                    badge.style.border = '1px solid #00d4ff';
                    badge.style.color = '#00ffcc';
                    badge.style.background = 'rgba(0,212,255,0.1)';
                    badge.innerHTML = t;
                } else {
                    badge.style.border = '1px solid #ff4d4d';
                    badge.style.color = '#ff4d4d';
                    badge.style.background = 'rgba(0,0,0,0.7)';
                    badge.innerHTML = '🔒 Locked';
                }
            });
        });
    }
    setInterval(updateCountdowns, 1000);

    document.getElementById('age-modal').style.display = 'flex';
    function confirmAge() { document.getElementById('age-modal').style.display = 'none'; }

    function switchNav(pageId, element=null) {
        document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
        document.getElementById('page-' + pageId).classList.add('active');
        if(element) {
            document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
            element.classList.add('active');
        }
        if(pageId === 'earn') loadEarnData();
    }
    
    function togglePkg(type, element) {
        document.querySelectorAll('.cat-btn').forEach(t => t.classList.remove('active'));
        element.classList.add('active');
        document.getElementById('pkg-bks').style.display = type === 'bks' ? 'block' : 'none';
        document.getElementById('pkg-usd').style.display = type === 'usd' ? 'block' : 'none';
        document.getElementById('pkg-coin').style.display = type === 'coin' ? 'block' : 'none';
    }

    let allFiles = {{ files_json | safe }};
    let state = { home: { p: 1, lim: 20, q: "" }, reg: { p: 1, lim: 20, q: "" }, prem: { p: 1, lim: 20, q: "" } };

    function createPromoCard(p) {
        return `<div class="video-card promo-card" onclick="openTgLink('${p.link}')" style="border: 1px solid #ffb703; cursor: pointer;">
            <img src="${p.image}" style="width: 100%; height: 180px; object-fit: cover;">
            <div class="tag-premium" style="background:#ffb703; color:black; font-weight:bold;">⭐ AD/PROMO</div>
            <div class="video-info" style="padding: 10px;">
                <b style="font-size:16px; color:#ffb703; display:block;">${p.title}</b>
            </div>
        </div>`;
    }

    function renderSlider() {
        let sorted = [...allFiles].sort((a,b) => (b.views||0) - (a.views||0)).slice(0, 8);
        let html = "";
        
        if(promotions.length > 0) {
            let p = promotions[0];
            html += `<div class="slider-card" onclick="openTgLink('${p.link}')" style="border-color:#ffb703; cursor:pointer;">
                <div style="position:absolute; top:5px; left:5px; background:#ffb703; color:black; font-size:9px; padding:2px 5px; border-radius:5px; z-index:10; font-weight:bold;">AD</div>
                <img src="${p.image}">
                <div class="s-title" style="color:#ffb703; font-weight:bold;">${p.title}</div>
            </div>`;
        }

        sorted.forEach(f => {
            html += `<div class="slider-card">
                <img src="${f.thumb_url || 'https://placehold.co/600x400/1c1c24/ff007f?text=Media'}">
                <div class="slider-play" onclick="playVideo('${f._id}')"></div>
                <div class="s-title">${f.title}</div>
            </div>`;
        });
        document.getElementById('top-slider').innerHTML = html;
    }
    renderSlider();

    function renderPagination(tab, total) {
        let s = state[tab];
        let maxP = Math.ceil(total / s.lim) || 1;
        if(s.p > maxP) s.p = maxP;
        let html = `<button class="page-btn" onclick="chgP('${tab}', -1)" ${s.p===1?'disabled':''}>Prev</button>`;
        let start = Math.max(1, s.p - 1);
        let end = Math.min(maxP, start + 2);
        if(end - start < 2) start = Math.max(1, end - 2);
        for(let i=start; i<=end; i++){ html += `<button class="page-btn ${s.p===i?'active':''}" onclick="setP('${tab}', ${i})">${i}</button>`; }
        html += `<button class="page-btn" onclick="chgP('${tab}', 1)" ${s.p>=maxP?'disabled':''}>Next</button>`;
        html += `<button class="page-btn" style="background:#444;" onclick="setLim('${tab}', 100)">All</button>`;
        document.getElementById(`${tab}-pagination`).innerHTML = html;
    }

    function createCard(f) {
        let tag = f.is_premium ? '<div class="tag-premium">💎 Premium</div>' : '<div class="tag-regular">👤 Regular</div>';
        let likesArr = Array.isArray(f.likes) ? f.likes : [];
        let likeCount = likesArr.length;
        let isLiked = likesArr.includes(userId);
        let heartColor = isLiked ? '#ff4d4d' : 'white';

        let unlockTime = userUnlockedFiles[f._id];
        let isUnlocked = false;
        if(unlockTime) {
            let safeStr = unlockTime.endsWith('Z') ? unlockTime : unlockTime + 'Z';
            if(new Date(safeStr) > new Date()) isUnlocked = true;
        }

        return `<div class="video-card">
            <div style="position: relative;">
                <img src="${f.thumb_url || 'https://placehold.co/600x400/1c1c24/ff007f?text=Media'}">
                ${tag}
                <div class="lock-status-badge lock-status-${f._id}" style="position:absolute; bottom:10px; right:10px; background:rgba(0,0,0,0.7); border:1px solid ${isUnlocked?'#00d4ff':'#ff4d4d'}; color:${isUnlocked?'#00ffcc':'#ff4d4d'}; padding:4px 8px; border-radius:10px; font-size:11px; font-weight:bold; backdrop-filter: blur(5px); z-index:10;">
                    ${isUnlocked ? '🔓' : '🔒 Locked'}
                </div>
                <div class="play-btn-overlay" onclick="playVideo('${f._id}')"></div>
            </div>
            <div class="video-info">
                <b style="font-size:15px; display:block; margin-bottom:5px;">${f.title} <small style="color:#00d4ff;">[ID: ${f._id}]</small></b>
                <small style="color:#aaa;">👁 ${f.views || 0} views</small>
            </div>
            <div class="video-actions">
                <button id="like-btn-${f._id}" class="like-btn-cls-${f._id}" style="color:${heartColor};" onclick="likeVideo('${f._id}')">❤️ <span id="like-${f._id}" class="like-span-cls-${f._id}">${likeCount}</span></button>
                <button onclick="openComments('${f._id}')">💬 Comment</button>
                <button onclick="shareVideo('${f._id}')">↗️ Share</button>
            </div>
        </div>`;
    }

    function renderList(tab) {
        let s = state[tab];
        let filtered = allFiles.filter(f => {
            if(tab === 'prem' && !f.is_premium) return false;
            if(tab === 'reg' && f.is_premium) return false;
            return f.title.toLowerCase().includes(s.q);
        });
        let start = (s.p - 1) * s.lim;
        let pageFiles = filtered.slice(start, start + s.lim);
        
        let html = "";
        
        if(s.p === 1 && promotions.length > 0 && s.q === "") {
            html += createPromoCard(promotions[0]);
        }

        if(pageFiles.length === 0 && html === "") {
            html = "<p style='text-align:center; color:#666;'>No videos found!</p>";
        }

        pageFiles.forEach((f, idx) => { 
            html += createCard(f); 
            if ((idx + 1) % 10 === 0 && promotions.length > 0 && s.q === "") {
                let pIdx = ((idx + 1) / 10) % promotions.length;
                html += createPromoCard(promotions[pIdx]);
            }
        });
        
        document.getElementById(`${tab}-video-list`).innerHTML = html;
        renderPagination(tab, filtered.length);
        updateCountdowns();
    }

    function renderHistory() {
        let histFiles = allFiles.filter(f => userHistory.includes(f._id));
        let html = histFiles.length === 0 ? "<p style='text-align:center; color:#666;'>No history yet!</p>" : "";
        histFiles.reverse().forEach(f => { html += createCard(f); });
        document.getElementById('history-video-list').innerHTML = html;
        updateCountdowns();
    }

    function handleSearch(tab) { let id = tab==='home'? 'search-bar' : 'search-bar-'+tab; state[tab].q = document.getElementById(id).value.toLowerCase(); state[tab].p = 1; renderList(tab); }
    function chgP(tab, dir) { state[tab].p += dir; renderList(tab); }
    function setP(tab, num) { state[tab].p = num; renderList(tab); }
    function setLim(tab, lim) { state[tab].lim = lim; state[tab].p = 1; renderList(tab); }

    renderList('home'); renderList('reg'); renderList('prem');

    function likeVideo(id) {
        let btns = document.querySelectorAll(`.like-btn-cls-${id}`);
        let spans = document.querySelectorAll(`.like-span-cls-${id}`);
        let alreadyLiked = false;
        
        btns.forEach(btn => {
            if (btn.style.color === 'rgb(255, 77, 77)' || btn.style.color === '#ff4d4d') alreadyLiked = true;
        });
        if (alreadyLiked) return;
        
        btns.forEach(btn => btn.style.color = '#ff4d4d');
        spans.forEach(span => span.innerText = parseInt(span.innerText || 0) + 1);
        
        let file = allFiles.find(f => f._id === id);
        if(file) {
            if(!Array.isArray(file.likes)) file.likes = [];
            if(!file.likes.includes(userId)) file.likes.push(userId);
        }
        
        fetch('/api/action', { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({action:'like', uid:userId, file_id:id}) });
    }
    
    function shareVideo(id) {
        let link = `https://t.me/${botUsername}?start=ref${userId}file${id}`;
        let text = currentLang === 'bn' ? "🔥 এই দারুণ ভিডিওটি দেখুন!" : "🔥 Watch this awesome video!";
        let shareUrl = `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${encodeURIComponent(text)}`;
        openTgLink(shareUrl);
    }
    
    let currentCommentId = null;
    function openComments(id) {
        currentCommentId = id;
        let file = allFiles.find(f => f._id === id);
        let list = document.getElementById('comment-list');
        list.innerHTML = "";
        let comments = file.comments || [];
        if(comments.length === 0) list.innerHTML = "<p style='color:#777; font-size:12px;'>No comments yet.</p>";
        comments.forEach(c => { list.innerHTML += `<div class="cmt-item"><b>User ${c.uid}:</b> ${c.text} <br><small style="color:#666;">${c.time}</small></div>`; });
        document.getElementById('comment-modal').style.display = 'flex';
    }
    
    async function sendComment() {
        let text = document.getElementById('cmt-input').value;
        if(!text) return;
        document.getElementById('cmt-input').value = "";
        let res = await fetch('/api/action', { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({action:'comment', uid:userId, file_id:currentCommentId, text:text}) });
        let data = await res.json();
        let file = allFiles.find(f => f._id === currentCommentId);
        if(!file.comments) file.comments = [];
        file.comments.push(data.comment);
        openComments(currentCommentId);
    }

    let currentDeepLink = "";
    let adDataGlobal = null;
    let timerInterval = null;
    let isAdRunning = false;
    let adLinkGlobal = "";
    let cFileId = null;

    let currentAdType = null; 
    let earnTypeGlobal = null; 
    let earnLinkGlobal = null;
    let earnTimeLeft = 0;

    async function playVideo(fileId) {
        cFileId = fileId;
        currentDeepLink = `https://t.me/${botUsername}?start=file_${fileId}`;

        if(!userHistory.includes(fileId)) {
            userHistory.push(fileId);
            fetch('/api/action', { method: 'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({action:'history', uid:userId, file_id:fileId}) });
            renderHistory();
        }

        let res = await fetch(`/api/get_ad/${userId}/${fileId}`);
        adDataGlobal = await res.json();

        if (adDataGlobal.requires_coin) {
            document.getElementById('coin-unlock-price').innerText = adDataGlobal.coin_price;
            document.getElementById('coin-unlock-price-en').innerText = adDataGlobal.coin_price;
            document.getElementById('coin-unlock-modal').style.display = 'flex';
        } else if (adDataGlobal.show_ad) { 
            document.getElementById('reg-coin-price').innerText = adDataGlobal.coin_price || 30;
            document.getElementById('regular-choice-modal').style.display = 'flex';
        } else { 
            openTgLink(currentDeepLink); setTimeout(() => {if(tg && tg.close) tg.close();}, 500); 
        }
    }
    
    async function unlockWithCoin() {
        let res = await fetch('/api/unlock_file', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({uid: userId, file_id: cFileId, method: "coin"})
        });
        let data = await res.json();
        document.getElementById('coin-unlock-modal').style.display = 'none';
        
        if (data.status === 'success') {
            if(data.expiry) userUnlockedFiles[cFileId] = data.expiry; 
            updateCountdowns();
            loadUser();
            openTgLink(currentDeepLink);
            setTimeout(() => {if(tg && tg.close) tg.close();}, 500);
        } else {
            alert(data.msg);
            if(data.msg.includes("পর্যাপ্ত কয়েন নেই") || data.msg.includes("Insufficient")) {
                switchNav('premium', document.getElementById('nav-premium'));
            }
        }
    }

    function handleAppFocus() {
        if (isAdRunning) {
            let timerElement = document.getElementById('timer-count');
            let timerValue = parseInt(timerElement.innerText);
            
            if (timerValue > 0 && timerElement.style.display !== 'none') {
                isAdRunning = false;
                clearInterval(timerInterval);
                timerElement.style.color = 'red';
                
                let btn = document.getElementById('get-file-btn');
                btn.innerText = currentLang === 'bn' ? "▶️ Resume Ad / অ্যাড আবার শুরু করুন" : "▶️ Resume Ad";
                btn.style.display = 'block';
                
                btn.onclick = () => { 
                    if (currentAdType === 'video') {
                        resumeAd();
                    } else {
                        resumeGenericAd();
                    }
                };
            }
        }
    }

    function triggerPause() {
        if (isAdRunning) handleAppFocus();
    }

    document.addEventListener("visibilitychange", () => {
        if (!document.hidden) triggerPause();
    });
    window.addEventListener("focus", triggerPause);

    function startFocusChecker() {
        setTimeout(() => {
            let focusCheck = setInterval(() => {
                if (!isAdRunning) {
                    clearInterval(focusCheck);
                    return;
                }
                if (document.visibilityState === 'visible' && document.hasFocus()) {
                    triggerPause();
                    clearInterval(focusCheck);
                }
            }, 500);
        }, 2000); 
    }

    function startAdProcess() {
        currentAdType = 'video';
        document.getElementById('ad-overlay').style.display = 'flex';
        document.getElementById('get-file-btn').style.display = 'none';
        document.getElementById('timer-count').style.display = 'block';
        document.getElementById('timer-count').style.color = '#f02d73';

        let adState = JSON.parse(localStorage.getItem('ad_state_' + cFileId));
        adLinkGlobal = adDataGlobal.ad_link;

        if (adState && adState.timeLeft > 0 && adState.timeLeft < adDataGlobal.wait_time) {
            document.getElementById('step-info').innerHTML = `🔹 <b>Step ${adState.step}</b> of ${adDataGlobal.steps}`;
            document.getElementById('duration-info').innerHTML = `⏳ Duration: ${adDataGlobal.wait_time} Seconds / Step`;
            document.getElementById('timer-count').innerText = adState.timeLeft;
            document.getElementById('timer-count').style.color = 'red';
            
            let btn = document.getElementById('get-file-btn');
            btn.innerText = currentLang === 'bn' ? "▶️ Resume Ad / অ্যাড আবার শুরু করুন" : "▶️ Resume Ad";
            btn.style.display = 'block';
            btn.onclick = () => { resumeAd(); };
        } else {
            if (!adState || adState.timeLeft <= 0) {
                adState = { step: 1, timeLeft: adDataGlobal.wait_time };
            }
            document.getElementById('step-info').innerHTML = `🔹 <b>Step ${adState.step}</b> of ${adDataGlobal.steps}`;
            document.getElementById('duration-info').innerHTML = `⏳ Duration: ${adDataGlobal.wait_time} Seconds / Step`;
            document.getElementById('timer-count').innerText = adState.timeLeft;
            resumeAd(); 
        }
    }

    function resumeAd() {
        isAdRunning = true;
        document.getElementById('get-file-btn').style.display = 'none';
        document.getElementById('timer-count').style.color = '#f02d73';
        
        if (tg && tg.openLink) {
            tg.openLink(adLinkGlobal);
        } else {
            window.open(adLinkGlobal, '_blank');
        }

        startFocusChecker();

        clearInterval(timerInterval); 
        let adState = JSON.parse(localStorage.getItem('ad_state_' + cFileId)) || { step: 1, timeLeft: adDataGlobal.wait_time };

        timerInterval = setInterval(() => {
            if (isAdRunning) {
                adState.timeLeft--;
                localStorage.setItem('ad_state_' + cFileId, JSON.stringify(adState));
                
                if (adState.timeLeft <= 0) {
                    clearInterval(timerInterval);
                    isAdRunning = false;
                    document.getElementById('timer-count').style.display = 'none';
                    
                    if (adState.step < adDataGlobal.steps) {
                        let btn = document.getElementById('get-file-btn');
                        btn.innerText = "Next Step";
                        btn.style.display = 'block';
                        btn.onclick = () => {
                            adState.step++; adState.timeLeft = adDataGlobal.wait_time;
                            localStorage.setItem('ad_state_' + cFileId, JSON.stringify(adState));
                            startAdProcess();
                        };
                    } else {
                        let btn = document.getElementById('get-file-btn');
                        btn.innerText = currentLang === 'bn' ? "ফাইল আনলক করুন (Unlock Video)" : "Unlock Video";
                        btn.style.display = 'block';
                        btn.onclick = async () => {
                            localStorage.removeItem('ad_state_' + cFileId);
                            let response = await fetch('/api/unlock_file', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({uid: userId, file_id: cFileId, method: "ad"})
                            });
                            let d = await response.json();
                            if(d.expiry) userUnlockedFiles[cFileId] = d.expiry;
                            updateCountdowns(); 
                            loadUser();
                            document.getElementById('ad-overlay').style.display = 'none';
                            openTgLink(currentDeepLink); 
                            setTimeout(()=>{ if(tg && tg.close) tg.close(); }, 500);
                        };
                    }
                } else {
                    document.getElementById('timer-count').innerText = adState.timeLeft;
                }
            }
        }, 1000);
    }
    
    async function loadEarnData() {
        let res = await fetch('/api/earn_info/' + userId);
        let data = await res.json();
        
        let html = `
        <div class="earn-card">
            <div class="limit-badge">${data.spins_done}/${data.spin_limit} Today</div>
            <div class="wheel-container">
                <div class="wheel-pointer"></div>
                <div class="wheel" id="spin-wheel"></div>
                <div class="wheel-center"></div>
            </div>
            <h3>Daily Spin Bonus</h3>
            <p style="font-size:12px; color:#aaa; margin-bottom:15px;">Watch an ad to spin and win random coins!</p>
            <button id="spin-btn" class="btn-main" style="background: linear-gradient(90deg, #ffb703, #ff6b6b);" onclick="startEarnAd('spin')" ${data.spins_done>=data.spin_limit?'disabled':''}>Spin & Earn</button>
        </div>
        `;
        
        let isTaskDone = data.tasks_done >= data.task_limit;
        let taskBtnStyle = isTaskDone ? "background: #555; color: #888;" : "background: linear-gradient(90deg, #00d4ff, #00ffcc); color:black;";
        let taskBtnText = isTaskDone ? "Completed" : "Complete Task";

        html += `
        <div class="earn-card">
            <div class="limit-badge">${data.tasks_done}/${data.task_limit} Today</div>
            <div style="font-size:40px;">📱</div>
            <h3>Daily Click Task</h3>
            <p style="font-size:12px; color:#aaa; margin-bottom:15px;">Click and visit the ad completely to get fixed coins!</p>
            <button class="btn-main" style="${taskBtnStyle}" onclick="startEarnAd('task')" ${isTaskDone?'disabled':''}>${taskBtnText}</button>
        </div>`;
        
        if(data.custom_tasks && data.custom_tasks.length > 0) {
            html += `<h3 style="margin-top:25px;">⚡ Unlimited Custom Tasks</h3>`;
            let doneTasks = data.custom_tasks_done || [];
            data.custom_tasks.forEach(t => {
                let isDone = doneTasks.includes(t._id);
                let btnStyle = isDone ? "background: #555; color: #888; cursor: not-allowed;" : "background: linear-gradient(90deg, #c72cff, #ff007f);";
                let btnText = isDone ? "Completed" : `Complete for ${t.coin}C`;
                html += `
                <div class="earn-card" style="padding:15px;">
                    <div style="font-size:25px;">💎</div>
                    <h4 style="margin:5px 0;">${t.title}</h4>
                    <p style="font-size:12px; color:#aaa; margin-bottom:10px;">Reward: ${t.coin} Coins</p>
                    <button class="btn-main" style="${btnStyle} padding:10px;" onclick="startCustomTask('${t._id}', '${t.link}')" ${isDone?'disabled':''}>${btnText}</button>
                </div>`;
            });
        }
        document.getElementById('earn-content').innerHTML = html;
    }

    async function startEarnAd(type) {
        let res = await fetch('/api/get_earn_ad/' + type);
        let data = await res.json();
        if (!data.ad_link) return alert(currentLang==='bn'?"বর্তমানে কোনো অ্যাড নেই!":"No ads available right now!");
        
        currentAdType = 'earn';
        earnTypeGlobal = type;
        earnLinkGlobal = data.ad_link;

        document.getElementById('ad-overlay').style.display = 'flex';
        document.getElementById('step-info').innerHTML = type === 'spin' ? 'Bonus Spin Ad' : 'Bonus Task Ad';
        document.getElementById('duration-info').innerHTML = `⏳ Duration: ${data.wait_time} Seconds`;
        earnTimeLeft = data.wait_time;
        document.getElementById('timer-count').innerText = earnTimeLeft;
        document.getElementById('timer-count').style.display = 'block';
        document.getElementById('get-file-btn').style.display = 'none';
        
        resumeGenericAd();
    }
    
    async function startCustomTask(taskId, link) {
        currentAdType = 'custom';
        earnTypeGlobal = 'custom_' + taskId;
        earnLinkGlobal = link;

        document.getElementById('ad-overlay').style.display = 'flex';
        document.getElementById('step-info').innerHTML = 'Custom Task';
        document.getElementById('duration-info').innerHTML = `⏳ Duration: 15 Seconds`;
        earnTimeLeft = 15;
        document.getElementById('timer-count').innerText = earnTimeLeft;
        document.getElementById('timer-count').style.display = 'block';
        document.getElementById('get-file-btn').style.display = 'none';
        
        resumeGenericAd();
    }

    function resumeGenericAd() {
        isAdRunning = true;
        document.getElementById('get-file-btn').style.display = 'none';
        document.getElementById('timer-count').style.color = '#f02d73';
        
        if (tg && tg.openLink) {
            tg.openLink(earnLinkGlobal);
        } else {
            window.open(earnLinkGlobal, '_blank');
        }
        
        startFocusChecker();
        
        clearInterval(timerInterval);
        timerInterval = setInterval(() => {
            if (isAdRunning) {
                earnTimeLeft--;
                if (earnTimeLeft <= 0) {
                    clearInterval(timerInterval);
                    isAdRunning = false;
                    document.getElementById('timer-count').style.display = 'none';
                    let btn = document.getElementById('get-file-btn');
                    btn.innerText = "Claim Reward";
                    btn.style.display = 'block';
                    btn.onclick = async () => {
                        let res2 = await fetch('/api/claim_earn', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({uid: userId, type: earnTypeGlobal})
                        });
                        let result = await res2.json();
                        
                        if(earnTypeGlobal === 'spin' && result.status === 'success') {
                            document.getElementById('ad-overlay').style.display = 'none';
                            let wheel = document.getElementById('spin-wheel');
                            let randomDegree = Math.floor(Math.random() * 360) + 1440; 
                            wheel.style.transform = `rotate(${randomDegree}deg)`;
                            setTimeout(() => {
                                alert(result.msg);
                                loadUser();
                            }, 3000);
                        } else {
                            alert(result.msg);
                            if (result.status === 'success') loadUser();
                            document.getElementById('ad-overlay').style.display = 'none';
                        }
                    };
                } else {
                    document.getElementById('timer-count').innerText = earnTimeLeft;
                }
            }
        }, 1000);
    }

    async function redeemCoupon() {
        let code = document.getElementById('coupon-input').value;
        if(!code) return alert(currentLang==='bn'?"কোড লিখুন!":"Enter Code!");
        let res = await fetch('/api/redeem', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uid: userId, code: code }) });
        let data = await res.json();
        alert(data.msg);
        if(data.status === 'success') { loadUser(); document.getElementById('coupon-input').value = ""; }
    }
    
    async function buyWithCoin(pkgId, cost) {
        if(userBalance < cost) return alert(currentLang==='bn'?"❌ আপনার পর্যাপ্ত কয়েন নেই!":"❌ Insufficient Coins!");
        let confirmText = currentLang === 'bn' ? `আপনি কি ${cost} কয়েন দিয়ে প্রিমিয়াম নিতে চান?` : `Buy Premium with ${cost} coins?`;
        if(confirm(confirmText)) {
            let res = await fetch('/api/buy_with_coin', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ uid: userId, pkg_id: pkgId }) });
            let data = await res.json();
            alert(data.msg); loadUser();
        }
    }

    function copyRef() { 
        let c = document.getElementById("ref-link"); c.select(); navigator.clipboard.writeText(c.value); 
        alert(currentLang==='bn'?"✅ রেফার লিংক কপি হয়েছে!":"✅ Link Copied!"); 
    }
    
    function reqBuy() { 
        openTgLink(`https://t.me/${adminUsername}`); 
        alert(currentLang==='bn'?"✅ পেমেন্ট করতে অ্যাডমিনকে ইনবক্সে মেসেজ দিন।":"✅ Inbox Admin to pay."); 
    }
</script>
</body>
</html>
"""

@web.route('/') 
def home(): 
    files = []
    for d in get_extra_dbs_sync():
        try:
            files.extend(list(d["files"].find().sort("_id", -1)))
        except: pass
        
    for f in files: f["_id"] = str(f["_id"]) 
    
    try:
        config = sync_db["config"].find_one({"_id": "settings"}) or {}
    except:
        config = {}
        
    promos = []
    try:
        promos = list(sync_db["promotions"].find())
        for p in promos: p["_id"] = str(p["_id"])
    except: pass
    
    pkgs = []
    try:
        pkgs = list(sync_db["packages"].find())
    except: pass
    
    return render_template_string(
        HTML_TEMPLATE, 
        files_json=json.dumps(files), 
        pkgs=pkgs, 
        promotions_json=json.dumps(promos),
        bot_username=BOT_USERNAME, 
        config=config, 
        site_name=config.get("site_name", "Glow Top"), 
        ref_coin=config.get("ref_coin", 10)
    )

app_flask = web 

def run_flask(): 
    port = int(os.environ.get("PORT", 8080))
    # Render এ থ্রেড ব্লক এড়াতে threaded=True এবং use_reloader=False দেওয়া হলো
    web.run(host="0.0.0.0", port=port, debug=False, threaded=True, use_reloader=False)

async def main_bot():
    global BOT_USERNAME
    await get_db() 
    await get_config() 
    await app.start()
    
    BOT_USERNAME = app.me.username
    print(f"✅ Bot Started Successfully! (@{BOT_USERNAME})") 
    
    # ==========================================
    # 🛑 COMMAND MENU & MINI APP AUTO SETUP
    # ==========================================
    try:
        commands = [
            BotCommand("start", "বট স্টার্ট করুন"),
            BotCommand("myid", "আপনার আইডি দেখুন"),
            BotCommand("cmd", "সকল কমান্ড লিস্ট (Admin)"),
            BotCommand("stats", "ইউজারের স্ট্যাটিস্টিকস (Admin)"),
            BotCommand("name", "ওয়েবসাইটের নাম পরিবর্তন (Admin)"),
            BotCommand("addfile", "ম্যানুয়ালি ফাইল যোগ (Admin)"),
            BotCommand("vidtitel", "অটো ভিডিও টাইটেল অ্যাড (Admin)"),
            BotCommand("deltitel", "সেভ করা টাইটেল ডিলিট (Admin)"),
            BotCommand("auto", "অটো ফাইল + থাম্বনেইল (Admin)"),
            BotCommand("delfile", "ফাইল ডিলিট (Admin)"),
            BotCommand("delall", "সব ডাটাবেসের সব ফাইল ডিলিট (Admin)"),
            BotCommand("spinlimit", "ডেইলি স্পিন লিমিট (Admin)"),
            BotCommand("tasklimit", "ডেইলি টাস্ক লিমিট (Admin)"),
            BotCommand("addtask", "কাস্টম আনলিমিটেড টাস্ক অ্যাড (Admin)"),
            BotCommand("deltask", "কাস্টম টাস্ক ডিলিট (Admin)"),
            BotCommand("mongo", "নতুন ডাটাবেস যোগ (Admin)"),
            BotCommand("delmongo", "এক্সট্রা ডাটাবেস রিমুভ (Admin)"),
            BotCommand("mongostats", "ডাটাবেস স্টোরেজ চেক (Admin)"),
            BotCommand("setpremcoin", "প্রিমিয়াম ভিডিওর দাম (Admin)"),
            BotCommand("spincoin", "স্পিন কয়েনের রেঞ্জ (Admin)"),
            BotCommand("taskcoin", "টাস্ক কমপ্লিট করার কয়েন (Admin)"),
            BotCommand("relocktime", "ভিডিও আনলক টাইমার (Admin)"),
            BotCommand("delrelocktime", "আনলক টাইমার ডিলিট (Admin)"),
            BotCommand("lockall", "আনলক করা ভিডিও রিস্টার্ট (Admin)"),
            BotCommand("addlink", "ডাইরেক্ট অ্যাড লিংক যোগ (Admin)"),
            BotCommand("delink", "অ্যাড লিংক রিমুভ (Admin)"),
            BotCommand("prstep", "প্রিমিয়াম অ্যাড স্টেপ (Admin)"),
            BotCommand("regstep", "রেগুলার অ্যাড স্টেপ (Admin)"),
            BotCommand("addcnl", "ইনলাইন চ্যানেল অ্যাড (Admin)"),
            BotCommand("vercnl", "Must Join চ্যানেল অ্যাড (Admin)"),
            BotCommand("delcnl", "ইনলাইন চ্যানেল ডিলিট (Admin)"),
            BotCommand("delvrcnl", "Must Join চ্যানেল ডিলিট (Admin)"),
            BotCommand("brodcast", "ইউজারদের মেসেজ পাঠান (Admin)"),
            BotCommand("cnlbdcst", "চ্যানেল/গ্রুপে মেসেজ পাঠান (Admin)"),
            BotCommand("autochannel", "চ্যানেলে অটোমেটিক ভিডিও পোস্ট (Admin)"),
            BotCommand("allred", "অটো রেন্ডম কুপন তৈরি (Admin)"),
            BotCommand("addcred", "কয়েন প্যাকেজ তৈরি (Admin)"),
            BotCommand("bdt", "বিকাশ প্যাকেজ তৈরি (Admin)"),
            BotCommand("usd", "USD প্যাকেজ তৈরি (Admin)"),
            BotCommand("delcred", "যেকোনো প্যাকেজ ডিলিট (Admin)"),
            BotCommand("autodel", "ভিডিও অটো ডিলিট টাইম (Admin)"),
            BotCommand("frotect", "ফরওয়ার্ড/সেভ অফ করতে (Admin)"),
            BotCommand("addadmin", "পেমেন্ট অ্যাডমিন সেট (Admin)"),
            BotCommand("addtex", "স্টার্ট টেক্সট সেট করতে (Admin)"),
            BotCommand("deltex", "স্টার্ট টেক্সট ডিলিট করতে (Admin)"),
            BotCommand("logo", "স্টার্ট লোগো সেট করতে (Admin)"),
            BotCommand("autvid", "অটো ভিডিও মেসেজ সেট করতে (Admin)"),
            BotCommand("autvidti", "অটো ভিডিও ইন্টারভাল সেট (Admin)"),
            BotCommand("autpost", "অটো পোস্ট ইন্টারভাল সেট (Admin)"),
            BotCommand("refbonous", "রেফার বোনাস সেট করতে (Admin)"),
            BotCommand("refbonousoff", "রেফার বোনাস অফ করতে (Admin)"),
            BotCommand("auto2", "টাইটেল লিখে অটো ফাইল অ্যাড (Admin)"),
            BotCommand("promot", "প্রোমোশনাল পোস্টার অ্যাড (Admin)"),
            BotCommand("delpromot", "প্রোমোট ডিলিট (Admin)"),
            BotCommand("delautochannel", "অটো চ্যানেল ডিলিট (Admin)")
        ]
        await app.set_bot_commands(commands)
        
        await app.set_chat_menu_button(
            menu_button=MenuButtonWebApp(
                text="🔥 Open App",
                web_app=WebAppInfo(url=WEB_URL)
            )
        )
        print("✅ Commands and Menu Setup Successful!")
    except Exception as e:
        print(f"⚠️ Setup Error: {e}")

    asyncio.create_task(background_tasks()) 
    await idle() 
    await app.stop()

if __name__ == "__main__": 
    threading.Thread(target=run_flask, daemon=True).start() 
    while True: 
        try: 
            loop.run_until_complete(main_bot()) 
        except FloodWait as e: 
            print(f"⚠️ Rate Limit: Waiting {e.value}s...") 
            time.sleep(e.value) 
        except Exception as e: 
            print(f"❌ Error: {e}") 
            time.sleep(5)
