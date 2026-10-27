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

import qrcode
from PIL import Image, ImageDraw


@client.on(events.NewMessage(pattern=r'!calc (.+)', func=owner_filter))
async def calc_cmd(e):

    expr = e.pattern_match.group(1).strip()
    r = safe_eval(expr)
    if r is not None:
        await respond(e, ui.card("🧮 Калькулятор",
                                 ui.rows(("Выражение", f"`{expr}`"),
                                         ("Результат", f"**{r}**"))))
    else:
        await respond(e, ui.err("Проверь выражение.",
                                title="Ошибка выражения",
                                hint="Разрешены: `+ - * / % sqrt sin cos tan log abs pow pi e factorial ceil floor round`"))
    db.bump_stat('cmds')


async def reminder_worker():
    """Опрос БД каждые 10с: напоминания переживают рестарт бота.
    Запускается в main при старте (см. spawn_task)."""
    while True:
        try:
            for r in db.get_due_reminders(time.time()):
                db.del_reminder(r['id'])
                try:
                    await client.send_message(
                        r['chat_id'], ui.card("⏰ Напоминание", f"_{r['text']}"))
                except Exception as ex:
                    logger.error(f"Ошибка напоминания: {ex}")
        except Exception as ex:
            logger.error(f"reminder_worker error: {ex}")
        await asyncio.sleep(10)

@client.on(events.NewMessage(pattern=r'!remind (\d+)\s+(.+)', func=owner_filter))
async def remind_cmd(e):

    delay = int(e.pattern_match.group(1))
    text = e.pattern_match.group(2).strip()
    db.add_reminder(e.chat_id, text, time.time() + delay)
    await respond(e, ui.card("⏰ Напоминание",
                             ui.rows(("Через", f"**{fmt_time(delay)}**"),
                                     ("Текст", f"_{text}_")),
                             hint="Сохранено в БД — переживёт рестарт бота"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!search (.+)', func=owner_filter))
async def search_cmd(e):
    query = _to_english(e.pattern_match.group(1).strip())
    msg = await respond(e, ui.card("⏳ Wikipedia", "ищу…"))
    try:
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': query,
            'format': 'json',
            'srlimit': 5,
        }
        async with aiohttp.ClientSession() as s:
            async with s.get('https://en.wikipedia.org/w/api.php', params=params) as resp:
                data = await resp.json()
        results = data.get('query', {}).get('search', [])
        if not results:
            await msg.edit(ui.err("Ничего не найдено", title="Wikipedia",
                                  hint="Попробуйте другой запрос"))
            return
        lines = [f"🔍 **Результаты (Wikipedia):** _{query}_\n"]
        for i, r in enumerate(results, 1):
            title = r.get('title', '?')
            href = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
            snippet = r.get('snippet', '').replace('<span class="searchmatch">', '**').replace('</span>', '**')[:120]
            lines.append(f"{i}. [{title}]({href})")
            if snippet:
                lines.append(f"   _{snippet}_")
        await msg.edit(ui.card(f"🔍 Wikipedia: {query}", "\n".join(lines[1:]) or "—"))
    except Exception as ex:
        await msg.edit(ui.err(f"Ошибка поиска: {ex}", title="Wikipedia"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!shorten (.+)', func=owner_filter))
async def shorten_cmd(e):

    url = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ Сокращение", "сокращаю ссылку…"))
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://is.gd/create.php",
                             params={"format": "json", "url": url},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        if 'shorturl' in data:
            await msg.edit(ui.card("✂️ Ссылка сокращена",
                                   ui.rows(("Оригинал", f"`{url[:55]}{'…' if len(url) > 55 else ''}`"),
                                           ("Короткая", data['shorturl']))))
        else:
            await msg.edit(ui.err(data.get('errormessage', 'Неверный URL'), title="Сокращение ссылки"))
    except Exception:
        await msg.edit(ui.err("Проверь URL.", title="Сокращение ссылки"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!weather (.+)', func=owner_filter))
async def weather_cmd(e):

    city = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ Погода", "запрашиваю данные…"))
    try:
        city_en = _to_english(city)
        from urllib.parse import quote
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://wttr.in/{quote(city_en)}?format=j1",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    raise ConnectionError(f"HTTP {r.status}")
                data = await r.json()
        cc = data.get('current_condition')
        loc = data.get('nearest_area')
        if not cc or not loc:
            await msg.edit(ui.err("Город не найден. Попробуй на латинице или уточни (страна/регион).",
                                  title="Погода"))
            return
        cc = cc[0]
        loc = loc[0]
        city_name = loc['areaName'][0]['value']
        country = loc['country'][0]['value']
        temp = cc['temp_C']
        feels = cc['FeelsLikeC']
        humidity = cc['humidity']
        wind = cc['windspeedKmph']
        desc = cc['weatherDesc'][0]['value']
        await msg.edit(
            ui.card(f"🌤 Погода: {city_name}, {country}",
                    ui.rows(("Температура", f"**{temp}°C** (ощущается как {feels}°C)", "🌡"),
                            ("Влажность", f"**{humidity}%**", "💧"),
                            ("Ветер", f"**{wind} км/ч**", "💨"),
                            ("Описание", desc, "📋")))
        )
    except (aiohttp.ClientError, ConnectionError) as ex:
        logger.warning(f"wttr.in failed: {ex}, trying Open-Meteo...")
        async with aiohttp.ClientSession() as s:
            geo_r = await s.get(
                f"https://geocoding-api.open-meteo.com/v1/search?name={quote(city_en)}&count=1",
                timeout=aiohttp.ClientTimeout(total=10)
            )
            if geo_r.status != 200:
                await msg.edit(ui.err("Город не найден. Попробуй на латинице.", title="Погода"))
                return
            geo = await geo_r.json()
            results = geo.get('results')
            if not results:
                await msg.edit(ui.err("Город не найден. Попробуй на латинице.", title="Погода"))
                return
            lat = results[0]['latitude']
            lon = results[0]['longitude']
            city_name = results[0].get('name', city)
            country = results[0].get('country', '')
            weather_r = await s.get(
                f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
                f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
                f"wind_speed_10m,weather_code&timezone=auto",
                timeout=aiohttp.ClientTimeout(total=10)
            )
            if weather_r.status != 200:
                await msg.edit(ui.err("Не удалось получить погоду. Попробуй позже.", title="Погода"))
                return
            wdata = await weather_r.json()
            cur = wdata.get('current', {})
            temp = cur.get('temperature_2m', '?')
            feels = cur.get('apparent_temperature', '?')
            humidity = cur.get('relative_humidity_2m', '?')
            wind = cur.get('wind_speed_10m', '?')
            wcode = cur.get('weather_code', 0)
            descs = {0:'Ясно', 1:'Преимущественно ясно', 2:'Переменная облачность', 3:'Пасмурно',
                     45:'Туман', 48:'Иней', 51:'Морось', 53:'Морось', 55:'Морось',
                     61:'Дождь', 63:'Дождь', 65:'Дождь', 71:'Снег', 73:'Снег', 75:'Снег',
                     80:'Ливень', 81:'Ливень', 82:'Ливень', 95:'Гроза', 96:'Гроза', 99:'Гроза'}
            desc = descs.get(wcode, f'Код {wcode}')
            await msg.edit(
                ui.card(f"🌤 Погода: {city_name}, {country}",
                        ui.rows(("Температура", f"**{temp}°C** (ощущается как {feels}°C)", "🌡"),
                                ("Влажность", f"**{humidity}%**", "💧"),
                                ("Ветер", f"**{wind} км/ч**", "💨"),
                                ("Описание", desc, "📋")))
            )
    except Exception as ex:
        logger.warning(f"Weather error: {ex}", exc_info=True)
        await msg.edit(ui.err("Ошибка получения погоды. Попробуй позже.", title="Погода"))
    db.bump_stat('cmds')


def _to_english(text):
    latin = sum(1 for c in text if 'a' <= c <= 'z' or 'A' <= c <= 'Z')
    if latin / max(len(text), 1) > 0.5:
        return text
    try:
        return _get_translator('en').translate(text)
    except Exception:
        return text


@client.on(events.NewMessage(pattern=r'!translate(?: ([a-z]{2}))? (.+)', func=owner_filter))
async def translate_cmd(e):

    target_lang = e.pattern_match.group(1) or 'ru'
    text = e.pattern_match.group(2).strip()
    msg = await respond(e, ui.card("⏳ Переводчик", "перевожу…"))
    try:
        t = await asyncio.to_thread(_get_translator, target_lang)
        result = await asyncio.to_thread(t.translate, text)
        await msg.edit(ui.card(f"🌐 Перевод ({target_lang})", f"{result}"))
    except Exception as ex:
        await msg.edit(ui.err(f"Ошибка перевода: {ex}", title="Переводчик"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!trhelp$', func=owner_filter))
async def trhelp_cmd(e):

    langs = ("ru, en, de, fr, es, it, pt, tr, zh, ja, ko, ar, hi, uk, pl, nl, "
             "sv, fi, no, da, cs, el, he, th, vi, id, ms, bg, ro, hu, sr, hr, "
             "sk, sl, et, lv, lt, ka, hy, az, kk, uz, mn")
    await respond(e, ui.card("🌐 Языки перевода",
                             "`!translate <код_языка> <текст>`\n" +
                             ui.kv("Коды ISO 639-1", f"`{langs}`"),
                             hint="Без кода — перевод на русский по умолчанию"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!base64 (encode|decode) (.+)', func=owner_filter))
async def base64_cmd(e):

    mode, text = e.pattern_match.group(1), e.pattern_match.group(2).strip()
    try:
        if mode == 'encode':
            res = base64.b64encode(text.encode()).decode()
            await respond(e, ui.card("🔐 Base64 encode", f"`{res}`"))
        else:
            res = base64.b64decode(text.encode()).decode()
            await respond(e, ui.card("🔓 Base64 decode", f"`{res}`"))
    except Exception as ex:
        logger.error(f"base64 error: {ex}")
        await respond(e, ui.err("Проверь данные.", title="Base64"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!hash (.+)', func=owner_filter))
async def hash_cmd(e):

    text = e.pattern_match.group(1).strip().encode()
    await respond(e, ui.card(
        "#️⃣ Хэши",
        ui.rows(
            ("MD5", f"`{hashlib.md5(text).hexdigest()}`"),
            ("SHA1", f"`{hashlib.sha1(text).hexdigest()}`"),
            ("SHA256", f"`{hashlib.sha256(text).hexdigest()}`"),
            ("SHA512", f"`{hashlib.sha512(text).hexdigest()[:64]}…`"),
        ),
    ))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!morse (.+)', func=owner_filter))
async def morse_cmd(e):

    text = e.pattern_match.group(1).strip()
    await respond(e, ui.card("📡 Азбука Морзе",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Код", f"`{morse_enc(text)}`"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!caesar (encode|decode) (\d+) (.+)', func=owner_filter))
async def caesar_cmd(e):

    mode, shift, text = e.pattern_match.group(1), int(e.pattern_match.group(2)), e.pattern_match.group(3)
    res = caesar(text, shift, dec=(mode == 'decode'))
    await respond(e, ui.card(f"{'🔒' if mode == 'encode' else '🔓'} Цезарь (сдвиг {shift})",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Результат", f"`{res}`"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!vigenere (encode|decode) (\S+) (.+)', func=owner_filter))
async def vigenere_cmd(e):

    mode, key, text = e.pattern_match.group(1), e.pattern_match.group(2), e.pattern_match.group(3)
    res = vigenere(text, key, dec=(mode == 'decode'))
    await respond(e, ui.card(f"{'🔒' if mode == 'encode' else '🔓'} Виженер (ключ {key})",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Результат", f"`{res}`"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!password(?:\s+(\d+))?(?:\s+(simple))?$', func=owner_filter))
async def password_cmd(e):

    length = max(4, min(int(e.pattern_match.group(1) or 16), 128))
    sym = not e.pattern_match.group(2)
    pwd = gen_pwd(length, sym)
    s = "🔴 Слабый" if length < 8 else "🟡 Средний" if length < 12 else "🟢 Сильный" if length < 20 else "💎 Очень сильный"
    await respond(e, ui.card(f"🔑 Пароль ({length} симв.)",
                             f"`{pwd}`\n\n"
                             + ui.rows(("Сила", s),
                                       ("Символы", '✅' if sym else '❌'))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!qr (.+)', func=owner_filter))
async def qr_cmd(e):

    text = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ QR-код", "генерирую…"))
    try:
        os.makedirs(MEDIA_DIR, exist_ok=True)
        path = os.path.join(MEDIA_DIR, f'qr_{int(time.time())}.png')
        img = await asyncio.to_thread(qrcode.make, text)
        await asyncio.to_thread(img.save, path)
        await client.send_file(e.chat_id, path,
            caption=ui.card("📱 QR-код", f"`{text[:80]}{'...' if len(text) > 80 else ''}`"))
        await msg.delete()
        os.remove(path)
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="QR-код"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!uuid$', func=owner_filter))
async def uuid_cmd(e):

    ids = [str(uuid.uuid4()) for _ in range(5)]
    out = "\n".join(f"`{u}`" for u in ids)
    await respond(e, ui.card("🆔 UUID v4", out))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!color (.+)', func=owner_filter))
async def color_cmd(e):

    raw = e.pattern_match.group(1).strip().replace(' ', '')
    if raw.startswith('#'):
        h = raw.lstrip('#')
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        hex_val = raw.upper()
    else:
        raw = raw.removeprefix('rgb(').removesuffix(')')
        r, g, b = map(int, raw.split(','))
        hex_val = f"#{r:02X}{g:02X}{b:02X}"
    msg = await respond(e, ui.card("⏳ Цвет", "генерирую образец…"))
    try:
        os.makedirs(MEDIA_DIR, exist_ok=True)
        path = os.path.join(MEDIA_DIR, f'color_{int(time.time())}.png')
        img = await asyncio.to_thread(Image.new, 'RGB', (300, 200), (r, g, b))
        brightness = (r * 299 + g * 587 + b * 114) / 1000
        text_color = 'white' if brightness < 128 else 'black'
        draw = await asyncio.to_thread(ImageDraw.Draw, img)
        await asyncio.to_thread(draw.text, (10, 10), hex_val, fill=text_color)
        await asyncio.to_thread(img.save, path)
        await client.send_file(e.chat_id, path,
            caption=ui.card(f"🎨 {hex_val}", ui.rows(("RGB", f"`rgb({r}, {g}, {b})`"))))
        await msg.delete()
        os.remove(path)
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="Цвет"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!ascii (.+)', func=owner_filter))
async def ascii_cmd(e):

    text = e.pattern_match.group(1).strip()
    codes = ' '.join(str(ord(c)) for c in text)
    back = ''.join(chr(int(x)) for x in codes.split())
    await respond(e, ui.card("🔢 ASCII коды",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Коды", f"`{codes}`"),
                                     ("Обратно", f"`{back}`"))))
    db.bump_stat('cmds')
