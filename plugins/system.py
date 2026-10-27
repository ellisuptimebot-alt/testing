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


@client.on(events.NewMessage(pattern=r'!sleep$', func=owner_filter))
async def sleep_cmd(e):

    state.toggle_auto_reply(True)
    await respond(e, ui.card("💤 Автоответчик",
                             ui.rows(("Статус", "включён ✅"),
                                     ("Что делает", "отвечает на входящие ЛС"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!wake$', func=owner_filter))
async def wake_cmd(e):

    state.toggle_auto_reply(False)
    await respond(e, ui.card("💤 Автоответчик",
                             ui.rows(("Статус", "выключен ❌"),
                                     ("Что делает", "ЛС без ответа"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!setreply(?:\s+(@?\w+))?(?:\s+(.+))?', func=owner_filter))
async def setreply_cmd(e):

    g = e.pattern_match
    target, text = g.group(1), g.group(2)
    # Одиночный аргумент без '@' и не число/не default — это текст глобального
    # автоответа ('!setreply привет'), а не ник. Так работало и раньше.
    if target and not (target.startswith('@') or target.isdigit() or target.lower() == 'default'):
        text = f"{target} {text}".strip() if text else target
        target = None
    if target and target.lower().lstrip('@') == 'default':
        db.set_default_reply(text or '')
        await respond(e, ui.card("💬 Дефолтный ответ",
                                 ui.rows(("Текст", text or "пусто")),
                                 hint="!setreply default <текст>"))
    elif target:
        try:
            ent = await resolve_entity(target)
        except Exception as ex:
            await respond(e, ui.err(f"Не удалось найти `{target}`: {ex}",
                                    title="Пользователь не найден",
                                    hint="Можно указать числовой ID: `!setreply 12345 текст`"))
            db.bump_stat('cmds')
            return
        # автоответчик ищет ответ по числовому id отправителя, поэтому
        # храним именно id, а не ник (иначе правило никогда не срабатывало)
        db.set_reply_text(ent.id, text or '')
        await respond(e, ui.card("💬 Ответ установлен",
                                 ui.rows(("Кому", entity_label(ent, target.lstrip('@'))),
                                         ("Текст", text or "пусто"))))
    elif text:
        state.set_auto_reply_text(text)
        await respond(e, ui.card("💬 Автоответчик",
                                 ui.rows(("Текст", text))))
    else:
        await respond(e, ui.usage("!setreply текст",
                                  "!setreply @username|ID текст",
                                  "!setreply default текст"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!status$', func=owner_filter))
async def status_cmd(e):
    await respond(e, await build_status_card())
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!time$', func=owner_filter))
async def time_cmd(e):

    now = datetime.datetime.now()
    utc = datetime.datetime.utcnow()
    week_days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
    await respond(e, ui.card(
        "🕐 Время и дата",
        ui.rows(
            ("Локальное", f"`{now.strftime('%H:%M:%S')}`", "🏠"),
            ("UTC", f"`{utc.strftime('%H:%M:%S')}`", "🌍"),
            ("Дата", f"`{now.strftime('%d.%m.%Y')}`", "📅"),
            ("День", week_days[now.weekday()], "📆"),
        ),
    ))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!ping$', func=owner_filter))
async def ping_cmd(e):

    t0 = time.monotonic()
    await respond(e, "🏓 ...")
    ms = (time.monotonic() - t0) * 1000
    q = "🟢 Отлично" if ms < 150 else "🟡 Нормально" if ms < 400 else "🔴 Высокая"
    await respond(e, ui.card("🏓 Понг!",
                             ui.rows(("Задержка", f"`{ms:.1f} мс`", "⚡"),
                                     ("Качество", q, "📶"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!id$', func=owner_filter))
async def id_cmd(e):

    chat = await e.get_chat()
    lines = [ui.kv("ID чата", f"`{chat.id}`", "🆔")]
    if e.reply_to_msg_id:
        r = await e.get_reply_message()
        lines += [
            ui.kv("ID отправителя", f"`{r.sender_id}`", "👤"),
            ui.kv("ID сообщения", f"`{r.id}`", "📨"),
        ]
        if r.sender and getattr(r.sender, 'username', None):
            lines.append(ui.kv("Username", f"@{r.sender.username}", "🔖"))
    else:
        me = await client.get_me()
        lines.append(ui.kv("Мой ID", f"`{me.id}`", "👤"))
    await respond(e, ui.card("🆔 ID", "\n".join(lines),
                             hint="Ответьте на сообщение, чтобы узнать его ID"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!info$', func=owner_filter))
async def info_cmd(e):

    me = await client.get_me()
    dialogs = await client.get_dialogs()
    await respond(e, ui.card(
        "🚀 UserBot Info",
        ui.rows(
            ("Имя", f"{me.first_name} {me.last_name or ''}".strip(), "👤"),
            ("ID", f"`{me.id}`", "🆔"),
            ("Username", f"@{me.username}" if me.username else "нет", "🔰"),
            ("Телефон", f"`{me.phone}`" if me.phone else "скрыт", "📱"),
            ("Чатов", f"`{len(dialogs)}`", "💬"),
            ("Аптайм", f"`{state.uptime}`", "⏱"),
            ("Статус", "активен ✅", "⚡"),
        ),
        hint="`!status` — детальный статус режимов",
    ))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!restart$', func=owner_filter))
async def restart_cmd(e):

    await respond(e, ui.card("🔄 Перезапуск", "Бот перезупускается…"))
    await asyncio.sleep(2)
    await client.disconnect()
    os._exit(0)


@client.on(events.NewMessage(pattern=r'!sudo(?:\s+(on|off)\s+(\S+))?\s*$', func=owner_filter))
async def sudo_cmd(e):

    g = e.pattern_match
    action = g.group(1)
    target = g.group(2)
    if not action:
        if state.sudo_users:
            lines = []
            for uid in list(state.sudo_users):
                try:
                    ent = await client.get_entity(uid)
                    name = getattr(ent, 'first_name', '') or str(uid)
                    uname = f" @{ent.username}" if getattr(ent, 'username', None) else ''
                    lines.append(f"• {name}{uname} (`{uid}`)")
                except Exception:
                    lines.append(f"• `{uid}`")
            await respond(e, ui.card("👑 Sudo-пользователи", "\n".join(lines),
                                     hint="`!sudo off @username|ID` — снять доступ"))
        else:
            await respond(e, ui.card("👑 Sudo-пользователи", "список пуст",
                                     hint="`!sudo on @username|ID` — выдать доступ"))
        db.bump_stat('cmds')
        return
    try:
        ent = await resolve_entity(target)
    except Exception as ex:
        await respond(e, ui.err(f"Пользователь `{target}` не найден: {ex}",
                                title="Sudo",
                                hint="Формат: `!sudo on @username|ID`"))
        db.bump_stat('cmds')
        return
    name = getattr(ent, 'first_name', '') or str(ent.id)
    if action == 'on':
        state.add_sudo(ent.id)
        await respond(e, ui.ok(ui.kv("Пользователь", f"**{name}**"), title="Добавлен в sudo"))
    else:
        state.remove_sudo(ent.id)
        await respond(e, ui.ok(ui.kv("Пользователь", f"**{name}**"), title="Удалён из sudo"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!status_reset$', func=owner_filter))
async def status_reset_cmd(e):
    state.reset_stealth()
    await respond(e, ui.ok("Выключены: cover, silent, shadow, lock, mute.", title="Режимы сброшены"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!resetdata$', func=owner_filter))
async def resetdata_cmd(e):

    db.clear_all()
    state.auto_reply_enabled = False
    state.auto_reply_text = '💫 Я автоответчик, хозяин скоро ответит! Спасибо за терпение 😘'
    state.ghost_mode = False
    state.afk_start_time = None
    state.afk_reason = ''
    state.sudo_users.clear()
    state._save()
    await respond(e, ui.warn("База, автоответчик, AFK и sudo очищены.",
                             title="Данные сброшены"))
    db.bump_stat('cmds')
