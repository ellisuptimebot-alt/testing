# -*- coding: utf-8 -*-
import asyncio
import base64
import datetime
import hashlib
import json
import os
import random
import re
import string
import time
import uuid

import aiohttp
import yt_dlp
from telethon import events
from telethon.errors import FloodWaitError, MessageNotModifiedError

from client import client
from config import OWNER_ID, MEDIA_DIR, MAX_FILE_SIZE_MB, MAX_COOLDOWN_ENTRIES, logger
from core import (
    db, state, owner_filter, respond, command_cooldown, reply_cooldown,
    _get_translator,
)
from helpers import (
    resolve_entity, entity_label, build_status_card,
    safe_flood, shadow_delete_msg,
)
from utils import fmt_time, progress_bar, safe_eval, caesar, morse_enc, gen_pwd, vigenere
from downloaders import run_download, send_and_clean
from notifier import notify
import ui

_log = logger

from downloaders import set_max_file_size


@client.on(events.NewMessage(pattern=r'!ytshow\s+(.+)', func=owner_filter))
async def ytshow_cmd(e):

    raw = e.pattern_match.group(1).strip()
    parts = raw.rsplit(None, 1)
    if len(parts) == 2 and parts[1].isdigit():
        url = parts[0]
        height = int(parts[1])
    else:
        url = raw
        height = None

    status_msg = await respond(e, ui.card("⏳ Загрузка",
                                          ui.rows(("Качество", f"{'авто' if not height else f'{height}p'}"))))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    filename = await run_download(edit_fn, url, mode='video', quality=height, timeout=600)
    if filename:
        await send_and_clean(edit_fn, client, e.chat_id, filename, f"🎬 YouTube: {url}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!dl(?:\s+(.+))?$', func=owner_filter))
async def dl_cmd(e):

    url = (e.pattern_match.group(1) or '').strip()
    if not url:
        # !dl ответом на сообщение со ссылкой — URL копировать не нужно
        if e.reply_to_msg_id:
            r = await e.get_reply_message()
            m = re.search(r'https?://\S+', (r.raw_text or '')) if r else None
            if m:
                url = m.group(0).rstrip(').,')
    if not url:
        await respond(e, ui.usage("!dl <URL>",
                                  "!dl (ответом на сообщение со ссылкой)",
                                  title="Загрузка"))
        db.bump_stat('cmds')
        return

    status_msg = await respond(e, ui.card("⏳ Загрузка", "запрос отправлен…"))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    try:
        filename = await run_download(edit_fn, url, mode='video', timeout=600)
        if filename:
            await send_and_clean(edit_fn, client, e.chat_id, filename)
    except Exception as ex:
        try:
            await status_msg.edit(ui.err(f"{ex}", title="Загрузка"))
        except Exception:
            pass
        _log.error(f"dl error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!playlist\s+(.+?)(?:\s+(\d+)(?:-(\d+))?)?$', func=owner_filter))
async def playlist_cmd(e):

    g = e.pattern_match
    url = g.group(1).strip()
    start_num = None
    end_num = None
    if g.group(2):
        start_num = int(g.group(2))
        end_num = int(g.group(3)) if g.group(3) else start_num

    msg = await respond(e, ui.card("⏳ Плейлист", "получаю информацию…"))
    try:
        import yt_dlp

        def _get_playlist_info():
            opts = {
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'nocheckcertificate': True,
                'cachedir': False,
                'extract_flat': True,
                'force_generic_extractor': False,
                'cookiefile': os.path.abspath(os.path.join(os.path.dirname(__file__), 'cookies.txt')),
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                title = info.get('title', '')
                entries = info.get('entries', [])
                video_urls = []
                for entry in entries:
                    if entry and entry.get('url'):
                        video_urls.append(entry['url'])
                    elif entry and entry.get('webpage_url'):
                        video_urls.append(entry['webpage_url'])
                    elif entry and entry.get('id'):
                        video_urls.append(f'https://youtube.com/watch?v={entry["id"]}')
                return title, video_urls

        title, video_urls = await asyncio.to_thread(_get_playlist_info)

        if not video_urls:
            await msg.edit(ui.err("Плейлист пуст или недоступен.", title="Плейлист"))
            return

        total = len(video_urls)
        if start_num:
            s = max(1, start_num)
            e_idx = min(total, end_num) if end_num else min(total, s)
            selected = video_urls[s - 1:e_idx]
        else:
            selected = video_urls[:50]

        await msg.edit(ui.card("📋 Плейлист",
                               ui.rows(("Название", f"**{title or '?'}**"),
                                       ("Видео", f"`{len(selected)}/{total}`")),
                               footer="⏳ Начинаю загрузку…"))

        for i, video_url in enumerate(selected, 1):
            vid_msg = await respond(e, ui.card("⏳ Загрузка видео",
                                               ui.rows(("Позиция", f"`{i}/{len(selected)}`"))))

            async def edit_vid(text, vm=vid_msg):
                try:
                    await vm.edit(text)
                except Exception:
                    pass

            filename = await run_download(edit_vid, video_url, mode='video', timeout=600)
            if filename:
                await send_and_clean(edit_vid, client, e.chat_id, filename, f"🎬 [{i}/{len(selected)}]")
                await asyncio.sleep(2)

        await respond(e, ui.ok(ui.kv("Загружено", f"**{len(selected)}/{total}** видео"),
                               title="Плейлист загружен"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Плейлист"))
        _log.error(f"playlist error: {ex}")
    db.bump_stat('cmds')


async def safe_edit(msg, text):
    try:
        await msg.edit(text)
    except Exception:
        pass


@client.on(events.NewMessage(pattern=r'!audio\s+(.+)', func=owner_filter))
async def audio_cmd(e):

    raw = e.pattern_match.group(1).strip()
    # опции: !audio <URL> [128|192|320] [voice|file]
    #   voice — отправить голосовым сообщением, file — всегда документом
    bitrate = None
    as_voice = as_document = False
    parts = []
    for tok in raw.split():
        low = tok.lower()
        if low in ('voice', 'голос'):
            as_voice = True
        elif low in ('file', 'документ', 'doc'):
            as_document = True
        elif tok.isdigit() and 8 <= int(tok) <= 320:
            bitrate = int(tok)
        else:
            parts.append(tok)
    url = ' '.join(parts)

    modes = []
    if bitrate:
        modes.append(f"битрейт `{bitrate}k`")
    modes.append("голосовое" if as_voice else ("документ" if as_document else "аудио-файл"))
    status_msg = await respond(e, ui.card("⏳ Загрузка аудио",
                                          ui.rows(("Режим", ", ".join(modes)),
                                                  ("Запрос", "отправлен…"))))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    try:
        filename = await run_download(edit_fn, url, mode='audio', quality=bitrate, timeout=600)
        if filename:
            await send_and_clean(edit_fn, client, e.chat_id, filename, f"🎵 Аудио",
                                 as_voice=as_voice, as_document=as_document)
    except Exception as ex:
        await safe_edit(status_msg, ui.err(f"{ex}", title="Аудио"))
        _log.error(f"audio error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!ytupdate$', func=owner_filter))
async def ytupdate_cmd(e):
    """Принудительное обновление yt-dlp: YouTube ломает скачивание регулярно."""
    import subprocess
    import sys as _sys

    status = await respond(e, ui.card("🔄 yt-dlp", ui.rows(("Статус", "обновляю…"))))

    def _work():
        def _ver():
            try:
                r = subprocess.run([_sys.executable, '-m', 'yt_dlp', '--version'],
                                   capture_output=True, text=True, timeout=30)
                return (r.stdout or '').strip() or '?'
            except Exception:
                return '?'

        old = _ver()
        try:
            subprocess.run([_sys.executable, '-m', 'pip', 'install', '--upgrade',
                            '--quiet', 'yt-dlp'], capture_output=True, timeout=120)
        except Exception:
            pass
        return old, _ver()

    old, new = await asyncio.to_thread(_work)
    if old == new:
        await safe_edit(status, ui.ok(ui.kv("Версия", f"`{new}`"),
                                      title="yt-dlp уже актуален"))
    else:
        await safe_edit(status, ui.ok(ui.rows(("Было", f"`{old}`"), ("Стало", f"`{new}`")),
                                      title="yt-dlp обновлён",
                                      hint="Перезапусти бота (`!restart`), чтобы подхватилась новая версия"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!sub\s+(.+?)(?:\s+(\w{2}))?$', func=owner_filter))
async def sub_cmd(e):

    g = e.pattern_match
    url = g.group(1).strip()
    lang = (g.group(2) or 'ru').lower()

    msg = await respond(e, ui.card("⏳ Субтитры", ui.rows(("Язык", f"`{lang}`"))))
    try:
        def _get_captions():
            out_dir = os.path.join(MEDIA_DIR, 'subtmp')
            os.makedirs(out_dir, exist_ok=True)
            opts = {
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'nocheckcertificate': True,
                'cachedir': False,
                'cookiefile': os.path.abspath(os.path.join(os.path.dirname(__file__), 'cookies.txt')),
                'writesubtitles': True,
                'subtitleslangs': [lang],
                'subtitlesformat': 'srt',
                'skip_download': True,
                'outtmpl': os.path.join(out_dir, '%(id)s'),
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                subs = info.get('subtitles') or info.get('requested_subtitles') or {}
                if lang not in subs:
                    for code in subs:
                        if code.startswith(lang):
                            lang_found = code
                            break
                    else:
                        return None
                else:
                    lang_found = lang
                sub_data = subs[lang_found]
                sub_url = None
                if isinstance(sub_data, list):
                    for entry in sub_data:
                        if entry.get('ext') == 'srt' and entry.get('url'):
                            sub_url = entry['url']
                            break
                    if not sub_url and sub_data and sub_data[0].get('url'):
                        sub_url = sub_data[0]['url']
                elif isinstance(sub_data, dict):
                    sub_url = sub_data.get('url')
                if sub_url:
                    from urllib.request import Request, urlopen
                    req = Request(sub_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urlopen(req, timeout=30) as resp:
                        return resp.read().decode('utf-8', 'ignore')
                return None

        srt_content = await asyncio.to_thread(_get_captions)

        if srt_content:
            sub_path = os.path.join(MEDIA_DIR, f'sub_{lang}.srt')
            with open(sub_path, 'w', encoding='utf-8') as f:
                f.write(srt_content)
            await msg.edit(ui.card("📤 Субтитры", ui.rows(("Язык", f"`{lang}`"),
                                                          ("Статус", "отправляю…"))))
            await client.send_file(e.chat_id, sub_path, caption=f"📝 Субтитры ({lang})")
            await asyncio.sleep(3)
            try:
                os.remove(sub_path)
            except OSError:
                pass
            await msg.edit(ui.ok(ui.kv("Язык", f"`{lang}`"), title="Субтитры отправлены"))
        else:
            await msg.edit(ui.err(f"Субтитры ({lang}) не найдены для этого видео.",
                                  title="Субтитры"))
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="Субтитры"))
        logger.error(f"sub error: {ex}")
    db.bump_stat('cmds')
