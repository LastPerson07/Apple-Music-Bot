import asyncio
import os
import re
import tempfile
import urllib.parse

import httpx
from pyrogram import Client, filters
from pyrogram.enums import ParseMode, ButtonStyle
from pyrogram.errors import FloodWait
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery,
    ReplyParameters,
)

from PIL import Image

import config
from mongodb import db
from ap import (
    init_session, get_token, get_all_track_forms, get_links_for_track,
    HEADERS, TIMEOUT,
)

# ── Bot Client ────────────────────────────────────────────────────────────────

app = Client(
    "apl_bot",
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN,
)

PLAYLIST_UPLOAD_DELAY = 4.0
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB

# ── Concurrency control ───────────────────────────────────────────────────────
GLOBAL_AP_SEMAPHORE = asyncio.Semaphore(15)

_USER_LOCKS: dict[int, asyncio.Lock] = {}

def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _USER_LOCKS:
        _USER_LOCKS[user_id] = asyncio.Lock()
    return _USER_LOCKS[user_id]


# ── Logging ───────────────────────────────────────────────────────────────────

async def log_new_user(bot: Client, user) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    username = f"@{user.username}" if user.username else "<i>None</i>"
    text = (
        "<blockquote>"
        "🆕 <b>New User</b>\n\n"
        f"<b>Name     :</b>  <b>{name}</b>\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n"
        f"<b>Username :</b>  {username}"
        "</blockquote>"
    )
    try:
        await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Log] Failed to send new-user log: {e}")


async def log_download_summary(bot: Client, user, first_track: str, total: int) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    username = f"@{user.username}" if user.username else "<i>None</i>"
    text = (
        "<blockquote>"
        "⬇️ <b>Download Complete</b>\n\n"
        f"<b>User     :</b>  <b>{name}</b>\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n"
        f"<b>Username :</b>  {username}\n"
        f"<b>Track    :</b>  {first_track}\n"
        f"<b>Total    :</b>  {total} track{'s' if total > 1 else ''}"
        "</blockquote>"
    )
    try:
        await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Log] Failed to send download summary: {e}")


async def log_track_to_channel(
    bot: Client, user, song_name: str, idx: int, total: int,
    audio_path: str, thumb_path: str | None,
) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    caption = (
        "<blockquote>"
        f"🎵 <b>{song_name}</b>\n"
        f"<b>By      :</b> {name} (<code>{user.id}</code>)\n"
        f"<b>Track   :</b> {idx}/{total}"
        "</blockquote>"
    )
    try:
        if thumb_path:
            await bot.send_photo(
                config.LOG_CHANNEL, photo=thumb_path,
                caption=caption, parse_mode=ParseMode.HTML,
            )
        await bot.send_audio(
            config.LOG_CHANNEL, audio=audio_path,
            title=song_name, file_name=f"{song_name}.mp3",
            caption=caption if not thumb_path else None,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"[Log] Failed to send track to log channel: {e}")


# ── Helpers ───────────────────────────────────────────────────────────────────

async def flood_safe(coro_func, *args, **kwargs):
    while True:
        try:
            return await coro_func(*args, **kwargs)
        except FloodWait as e:
            wait = e.value + 1
            print(f"[FloodWait] sleeping {wait}s")
            await asyncio.sleep(wait)


def parse_filename(response: httpx.Response, fallback: str) -> str:
    cd = response.headers.get("content-disposition", "")
    m = re.search(r"filename\*=UTF-8''([^\s;]+)", cd, re.IGNORECASE)
    if m:
        return urllib.parse.unquote(m.group(1))
    m = re.search(r"""filename=["']?([^"';]+)["']?""", cd, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return fallback


async def download_file(
    client: httpx.AsyncClient, url: str, dest_dir: str, fallback_name: str
) -> tuple[bool, str]:
    try:
        async with client.stream("GET", url, headers=HEADERS, follow_redirects=True) as r:
            r.raise_for_status()
            fname = parse_filename(r, fallback_name)
            fname = re.sub(r'[\\/*?:"<>|]', "_", fname)
            dest = os.path.join(dest_dir, fname)
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes(CHUNK_SIZE):
                    f.write(chunk)
        return True, dest
    except Exception as e:
        print(f"[Download] {url} failed: {e}")
        return False, ""


# ── Name cleaning ─────────────────────────────────────────────────────────────

_EMOJI_RE = re.compile(
    "["
    "\U0001F000-\U0001FFFF"
    "\U00002700-\U000027BF"
    "\u2600-\u26FF"
    "\u2300-\u23FF"
    "]+",
    flags=re.UNICODE,
)
_DOMAIN_PREFIX_RE = re.compile(
    r'^(?:[\w\-]+\.)+[a-zA-Z]{2,10}[\s\-\u2013\u2014]*',
    re.IGNORECASE,
)
_APLMATE_RE = re.compile(
    r'(?i)'
    r'apl\s*mate\s*'
    r'(?:\.com?)?\s*'
    r'[-\u2013\u2014\s]*',
)

def clean_song_name(raw: str) -> str:
    name = raw
    name = _APLMATE_RE.sub('', name).strip()
    for _ in range(2):
        name = _DOMAIN_PREFIX_RE.sub('', name).strip()
    name = re.sub(
        r'^[^\w\u0400-\u04FF]*Download\s+\S+\s*[-\u2013]?\s*',
        '', name, flags=re.IGNORECASE,
    ).strip()
    name = _EMOJI_RE.sub('', name).strip()
    name = re.sub(r'^[\s\-\u2013\u2014_•·]+', '', name).strip()
    name = re.sub(r'\s*_\s*', ' ', name).strip()
    return name


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '_', name).strip()


def split_links(links: list[dict]) -> tuple[dict | None, dict | None]:
    """
    Separate the MP3 link from the Cover link.
    Handles both old label format and new 'Download Mp3' / 'Download Cover [HD]' labels.
    """
    audio = None
    cover = None
    for item in links:
        label = item["quality"].lower()
        if "cover" in label:
            cover = item
        elif audio is None:
            audio = item
    return audio, cover


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Dev", url=config.DEV_URL, style=ButtonStyle.PRIMARY),
        InlineKeyboardButton("Credits", callback_data="credits", style=ButtonStyle.PRIMARY),
    ]])


# ── Per-track download + upload ───────────────────────────────────────────────

async def handle_track(
    client: Client,
    message,
    dl: httpx.AsyncClient,
    idx: int,
    total: int,
    links: list[dict],
    album_thumb_path: str | None,
    first_track_ref: list,
    user,
):
    audio_entry, cover_entry = split_links(links)

    if not audio_entry:
        await flood_safe(
            message.reply_text,
            f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> No audio link found.</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=message.id),
        )
        return

    with tempfile.TemporaryDirectory() as tmp:
        print(f"\n[Upload] Track {idx}/{total} → downloading audio…")
        ok, raw_audio_path = await download_file(dl, audio_entry["link"], tmp, f"track_{idx}.mp3")
        if not ok:
            await flood_safe(
                message.reply_text,
                f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> Audio download failed.</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=message.id),
            )
            return

        raw_name = os.path.splitext(os.path.basename(raw_audio_path))[0]
        song_name = clean_song_name(raw_name)
        print(f"[Upload] Raw : {raw_name}")
        print(f"[Upload] Name: {song_name}")

        safe_fname = sanitize_filename(song_name) + ".mp3"
        audio_path = os.path.join(tmp, safe_fname)
        os.rename(raw_audio_path, audio_path)

        if first_track_ref[0] is None:
            first_track_ref[0] = song_name

        # Thumbnail
        thumb_path = album_thumb_path
        if cover_entry:
            c_ok, cover_path = await download_file(dl, cover_entry["link"], tmp, f"cover_{idx}.jpg")
            if c_ok:
                if not cover_path.lower().endswith((".jpg", ".jpeg")):
                    jpg_path = cover_path + ".jpg"
                    os.rename(cover_path, jpg_path)
                    cover_path = jpg_path
                thumb_path = cover_path
                print(f"[Thumb] Per-track cover → {thumb_path}")

        print(f"[Upload] Thumb: {thumb_path or 'none'}")

        caption = (
            f"<blockquote>"
            f"🎵 <b>{song_name}</b>"
            f"</blockquote>"
        )

        try:
            if thumb_path:
                await flood_safe(
                    client.send_photo,
                    chat_id=message.chat.id,
                    photo=thumb_path,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=message.id),
                )
                print(f"[Upload] ✓ Photo sent.")

            await flood_safe(
                client.send_audio,
                chat_id=message.chat.id,
                audio=audio_path,
                title=song_name,
                file_name=safe_fname,
                reply_parameters=ReplyParameters(message_id=message.id),
            )
            print(f"[Upload] ✓ Track {idx}/{total} sent.")

            await log_track_to_channel(client, user, song_name, idx, total, audio_path, thumb_path)

        except Exception as e:
            print(f"[Upload] ✗ Track {idx}/{total} failed: {e}")
            await flood_safe(
                message.reply_text,
                f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> Upload failed — <i>{e}</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=message.id),
            )


# ── Handlers ──────────────────────────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private)
async def cmd_start(client: Client, message):
    user = message.from_user
    is_new = await db.add_user(user.id)
    if is_new:
        await log_new_user(client, user)

    await message.reply_text(
        "<blockquote>\n"
        "<b>Hey 👋</b>\n"
        "<b>I can download Apple Music tracks for you.</b>\n\n"
        "<i>Just paste any Apple Music link below - single track, album, or playlist.</i>\n"
        "</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=start_keyboard(),
        reply_parameters=ReplyParameters(message_id=message.id),
    )


@app.on_callback_query(filters.regex("^credits$"))
async def cb_credits(client: Client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.reply_text(
        "<blockquote>\n"
        "<b>Credits</b>\n\n"
        "<i>This bot is made by</i> <b>Mr. D</b>, <b>Mark</b>, <b>Abhinai</b>.\n\n"
        "<i>Mr. D and Mark did most of the heavy lifting - if it helps you, just give credit. That's all.</i>\n"
        "</blockquote>",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.private & filters.text & ~filters.command(["start"]))
async def handle_link(client: Client, message):
    url = message.text.strip()
    if "music.apple.com" not in url:
        return

    user = message.from_user

    user_lock = _get_user_lock(user.id)
    if user_lock.locked():
        await message.reply_text(
            "<blockquote>ᯓ➤<b>You already have a download in progress • • •</b>\n"
            "<i>Please wait for it to finish.</i></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=message.id),
        )
        return

    async with user_lock:
        is_new = await db.add_user(user.id)
        if is_new:
            await log_new_user(client, user)

        status = await message.reply_text(
            "<blockquote>ᯓ➤<b>Processing • • •</b></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=message.id),
        )

        is_playlist = False

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=TIMEOUT) as fetch_cl:
                await init_session(fetch_cl)
                token = await get_token(fetch_cl, url)
                track_forms, album_thumb = await get_all_track_forms(fetch_cl, url, token)

                total = len(track_forms)
                print(f"\n[Flow] URL: {url} | Tracks: {total} | Thumb: {album_thumb}")

                if total == 0:
                    await status.edit_text(
                        "<blockquote>❌ <b>No tracks found for that link.</b></blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                    return

                is_playlist = total > 1
                label = "playlist" if is_playlist else "track"
                await status.edit_text(
                    f"<blockquote>•ᴗ•<b> Processing {label}</b> ◌ {total} track{'s' if total > 1 else ''}…"
                    + ("\n<i>Playlist detected - please wait…</i>" if is_playlist else "")
                    + "</blockquote>",
                    parse_mode=ParseMode.HTML,
                )

                print(f"[Bot] User: {user.id} | {user.first_name} | Tracks: {total}")

                # Download album thumbnail
                _thumb_tmp = tempfile.mkdtemp()
                album_thumb_path = None
                if album_thumb:
                    try:
                        async with fetch_cl.stream("GET", album_thumb) as _r:
                            _r.raise_for_status()
                            _ap = os.path.join(_thumb_tmp, "album_thumb.jpg")
                            with open(_ap, "wb") as _f:
                                async for _chunk in _r.aiter_bytes(CHUNK_SIZE):
                                    _f.write(_chunk)
                        album_thumb_path = _ap
                        print(f"[Thumb] Album art → {album_thumb_path}")
                    except Exception as e:
                        print(f"[Thumb] Album art failed: {e}")

                per_request_sem = asyncio.Semaphore(3)

                async def _get_links_guarded(f, i):
                    async with GLOBAL_AP_SEMAPHORE:
                        return await get_links_for_track(fetch_cl, f, per_request_sem, i)

                link_tasks = [
                    asyncio.ensure_future(_get_links_guarded(f, i + 1))
                    for i, f in enumerate(track_forms)
                ]

                first_track_ref = [None]

                async with httpx.AsyncClient(
                    follow_redirects=True,
                    timeout=TIMEOUT,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                ) as dl:
                    for coro in asyncio.as_completed(link_tasks):
                        idx, links = await coro
                        await handle_track(
                            client, message, dl,
                            idx, total, links,
                            album_thumb_path, first_track_ref,
                            user,
                        )
                        if is_playlist and idx < total:
                            await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

        except Exception as e:
            await status.edit_text(
                f"<blockquote>❌ <b>Failed.</b>\n<i>{e}</i></blockquote>",
                parse_mode=ParseMode.HTML,
            )
            return

        if is_playlist:
            await flood_safe(
                message.reply_text,
                f"<blockquote>"
                f"✅ <b>Playlist done!</b>\n\n"
                f"<b>Total :</b>  {total} track{'s' if total > 1 else ''} sent 🎶"
                f"</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=message.id),
            )

        await log_download_summary(client, user, first_track_ref[0] or "Unknown", total)
        await status.delete()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import time
    while True:
        try:
            print("Bot starting…")
            app.run()
            break
        except FloodWait as e:
            wait = e.value + 5
            print(f"[Startup] FloodWait — sleeping {wait}s…")
            time.sleep(wait)
        except KeyboardInterrupt:
            print("Stopped.")
            break
