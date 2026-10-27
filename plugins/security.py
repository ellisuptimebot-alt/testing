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

from telethon.tl.functions.account import GetAuthorizationsRequest

import core


@client.on(events.NewMessage(pattern=r'!watch\s+(on|off)$', func=owner_filter))
async def watch_cmd(e):
    arg = e.pattern_match.group(1)
    if arg == 'on':
        if core._watch_task and not core._watch_task.done():
            await respond(e, ui.warn("Мониторинг уже запущен.", title="Watch"))
            return
        db.clear_sessions()
        try:
            result = await client(GetAuthorizationsRequest())
            for auth in result.authorizations:
                h = hashlib.md5(f"{auth.hash}{auth.device_model}{auth.platform}".encode()).hexdigest()
                db.save_session(h, json.dumps({"device": auth.device_model, "platform": auth.platform, "ip": auth.ip, "date": str(auth.date_created)}))
        except Exception as ex:
            logger.warning(f"Init sessions: {ex}")

        core._watch_task = asyncio.create_task(sessions_monitor())
        db.set_state('watch_enabled', 'True')
        await respond(e, ui.card("👁️ Мониторинг сессий",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Проверка", "каждые 5 мин")),
                                 footer="Уведомления о новых входах — во внешний бот."))
    else:
        if core._watch_task and not core._watch_task.done():
            core._watch_task.cancel()
            core._watch_task = None
        db.set_state('watch_enabled', 'False')
        await respond(e, ui.card("👁️ Мониторинг сессий",
                                 ui.rows(("Статус", "выключен ❌"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!check_email\s+(\S+)', func=owner_filter))
async def check_email_cmd(e):

    email = e.pattern_match.group(1).strip().lower()
    msg = await respond(e, ui.card("🔍 Проверка email", ui.rows(("Email", f"`{email}`"))))
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f'https://api.xposedornot.com/v1/check-email/{email}',
                params={'details': 'true'},
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 404:
                    await msg.edit(ui.ok(ui.kv("Email", f"`{email}`"),
                                         title="В утечках не найден"))
                elif resp.status == 200:
                    data = await resp.json()
                    breaches = data.get('breaches', [])
                    if not breaches:
                        await msg.edit(ui.ok(ui.kv("Email", f"`{email}`"),
                                             title="В утечках не найден"))
                        return
                    lines = []
                    for b in breaches[:15]:
                        name = b.get('Name', b) if isinstance(b, dict) else b
                        date = b.get('BreachDate', '') if isinstance(b, dict) else ''
                        lines.append(f"• {name}" + (f" ({date})" if date else ""))
                    if len(breaches) > 15:
                        lines.append(f"… и ещё {len(breaches) - 15}")
                    await msg.edit(ui.warn(
                        ui.rows(("Email", f"`{email}`"), ("Утечек", f"**{len(breaches)}**"))
                        + "\n\n" + "\n".join(lines),
                        title="Найден в утечках"))
                elif resp.status == 429:
                    await msg.edit(ui.err("Слишком много запросов. Лимит: 100/день на IP.",
                                          title="Проверка email"))
                else:
                    await msg.edit(ui.err(f"Ошибка API: HTTP {resp.status}", title="Проверка email"))
    except asyncio.TimeoutError:
        await msg.edit(ui.err("Таймаут запроса к API.", title="Проверка email"))
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="Проверка email"))
        logger.error(f"check_email error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'^!protect(?:\s+(on|off))?$', func=owner_filter))
async def protect_cmd(e):
    arg = e.pattern_match.group(1) if e.pattern_match else None
    logger.info(f"[PROTECT] вход: sender={e.sender_id} text={repr((e.raw_text or '').strip())} cmd={arg}")
    if not arg:
        if core._protect_task and not core._protect_task.done():
            await respond(e, ui.card("🔒 Защита", ui.rows(("Статус", "включён ✅")),
                                     hint="`!protect off` — отключить"))
        else:
            await respond(e, ui.card("🔓 Защита", ui.rows(("Статус", "выключен ❌")),
                                     hint="`!protect on` — включить"))
        db.bump_stat('cmds')
        return
    if arg == 'on':
        if core._protect_task and not core._protect_task.done():
            await respond(e, ui.warn("Защита уже включена.", title="Protect"))
            return
        dialogs = await client.get_dialogs(limit=1000)
        db.clear_protected_chats()
        for d in dialogs:
            db.add_protected_chat(d.id)
        await respond(e, ui.card("🔒 Защита включена",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Чатов", f"`{len(dialogs)}`"),
                                         ("Интервал проверки", "`120с`")),
                                 footer="Уведомления о пропавших чатах — во внешний бот."))

        core._protect_task = asyncio.create_task(protect_monitor())
        db.set_state('protect_enabled', 'True')
    else:
        if core._protect_task and not core._protect_task.done():
            core._protect_task.cancel()
            core._protect_task = None
        db.clear_protected_chats()
        db.set_state('protect_enabled', 'False')
        await respond(e, ui.card("🔓 Защита выключена",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Чаты", "список очищен"))))
    db.bump_stat('cmds')


# ── Фоновые мониторинги вынесены на уровень модуля: их перезапускает
#    main при старте, если флаг watch_enabled/protect_enabled сохранён в БД ──

async def sessions_monitor():
    while True:
        try:
            result = await client(GetAuthorizationsRequest())
            known = db.all_sessions()
            for auth in result.authorizations:
                h = hashlib.md5(f"{auth.hash}{auth.device_model}{auth.platform}".encode()).hexdigest()
                if h not in known:
                    db.save_session(h, json.dumps({"device": auth.device_model, "platform": auth.platform, "ip": auth.ip, "date": str(auth.date_created)}))
                    try:
                        await notify.session_alert(auth.device_model, auth.platform, auth.ip, str(auth.date_created))
                    except Exception:
                        try:
                            me = await client.get_me()
                            await client.send_message(me.id, f"⚠️ **Новый вход**\nУстройство: {auth.device_model}\nПлатформа: {auth.platform}\nIP: {auth.ip}\nДата: {auth.date_created}")
                        except Exception:
                            pass
                    logger.warning(f"New session: {auth.device_model} {auth.ip}")
        except Exception as ex:
            logger.error(f"Watch error: {ex}")
            try:
                await notify.error_alert("Watch monitor", str(ex))
            except Exception:
                pass
        await asyncio.sleep(300)


async def protect_monitor():
    while True:
        try:
            current = {d.id for d in await client.get_dialogs(limit=1000)}
            protected = set(db.get_protected_chat_ids())
            missing = protected - current
            for cid in missing:
                try:
                    await notify.protect_alert(cid)
                except Exception:
                    try:
                        me = await client.get_me()
                        await client.send_message(me.id, f"⚠️ **Удалён чат**\nID: `{cid}`\nЧат был удалён или вы из него вышли.")
                    except Exception:
                        pass
                db.del_protected_chat(cid)
        except Exception as ex:
            logger.error(f"Protect monitor error: {ex}")
            try:
                await notify.error_alert("Protect monitor", str(ex))
            except Exception:
                pass
        await asyncio.sleep(120)
