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


@client.on(events.NewMessage(pattern=r'!me$', func=owner_filter))
async def me_cmd(e):

    me = await client.get_me()
    photos = await client.get_profile_photos(me.id, limit=1)
    await respond(e, ui.card(
        "👤 Мой профиль",
        ui.rows(
            ("Имя", f"{me.first_name} {me.last_name or ''}".strip(), "📛"),
            ("ID", f"`{me.id}`", "🆔"),
            ("Username", f"@{me.username}" if me.username else "нет", "🔰"),
            ("Телефон", f"`{me.phone}`" if me.phone else "скрыт", "📱"),
            ("Аватар", "есть ✅" if photos else "нет ❌", "🖼"),
            ("Verified", "да ✅" if me.verified else "нет ❌", "✔️"),
            ("Бот", "да ✅" if me.bot else "нет ❌", "🤖"),
        ),
        hint="`!avatar`, `!name`, `!lastname`, `!bio` — изменить профиль",
    ))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!avatar$', func=owner_filter))
async def avatar_cmd(e):

    if e.reply_to_msg_id:
        r = await e.get_reply_message()
        uid = r.sender_id
    else:
        uid = (await client.get_me()).id
    photos = await client.get_profile_photos(uid, limit=1)
    if photos:
        await e.reply(file=photos[0])
        await e.delete()
    else:
        await respond(e, ui.err("Аватарка не найдена",
                                hint="Ответьте на чужое сообщение, чтобы получить его аватарку"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!name (.+)', func=owner_filter))
async def name_cmd(e):

    n = e.pattern_match.group(1).strip()
    await client.edit_profile(first_name=n)
    await respond(e, ui.ok(ui.kv("Имя", f"**{n}**"), title="Имя изменено"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!lastname(?:\s+(.+))?$', func=owner_filter))
async def lastname_cmd(e):

    n = (e.pattern_match.group(1) or '').strip()
    await client.edit_profile(last_name=n)
    await respond(e, ui.ok(ui.kv("Фамилия", f"**{n}**"), title="Фамилия изменена")
                   if n else ui.ok("Фамилия удалена", title="Профиль"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!bio(?:\s+(.+))?$', func=owner_filter))
async def bio_cmd(e):

    t = (e.pattern_match.group(1) or '').strip()
    await client.edit_profile(about=t)
    await respond(e, ui.ok(ui.kv("Био", f"_{t}_"), title="Био обновлено")
                   if t else ui.ok("Био очищено", title="Профиль"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!whois (.+)', func=owner_filter))
async def whois_cmd(e):

    target = e.pattern_match.group(1).strip()
    try:
        ent = await resolve_entity(target)
        name = f"{getattr(ent, 'first_name', '') or ''} {getattr(ent, 'last_name', '') or ''}".strip() \
               or getattr(ent, 'title', '?')
        uname = f"@{ent.username}" if getattr(ent, 'username', None) else "нет"
        bot_ = "✅" if getattr(ent, 'bot', False) else "❌"
        ver = "✅" if getattr(ent, 'verified', False) else "❌"
        await respond(e, ui.card(
            "🔍 Пользователь",
            ui.rows(
                ("Имя", f"**{name}**", "📛"),
                ("ID", f"`{ent.id}`", "🆔"),
                ("Username", uname, "🔰"),
                ("Бот", bot_, "🤖"),
                ("Verified", ver, "✔️"),
            ),
            hint="`!username_check @ник|ID` — проверить username",
        ))
    except Exception as ex:
        await respond(e, ui.err(f"Не найден `{target}`: {ex}",
                                title="Пользователь",
                                hint="Формат: `!whois @username|ID`"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!username_check (.+)', func=owner_filter))
async def username_check_cmd(e):

    raw = e.pattern_match.group(1).strip()
    if raw.lstrip('-').isdigit():
        # числовой ID: смотрим, есть ли у этого пользователя юзернейм
        try:
            ent = await resolve_entity(raw)
            name = getattr(ent, 'first_name', None) or getattr(ent, 'title', '?')
            uname = getattr(ent, 'username', None)
            await respond(e, ui.card(
                "🔍 Проверка username",
                ui.rows(
                    ("ID", f"`{ent.id}`", "🆔"),
                    ("Имя", name, "👤"),
                    ("Username", f"@{uname}" if uname else "отсутствует", "🔰"),
                    ("Статус", "занят ⚠️" if uname else "свободен ✅", "ℹ️"),
                ),
            ))
        except Exception as ex:
            await respond(e, ui.err(f"ID `{raw}` не найден: {ex}", title="Пользователь"))
        db.bump_stat('cmds')
        return
    try:
        ent = await resolve_entity(raw)
        name = getattr(ent, 'first_name', None) or getattr(ent, 'title', '?')
        await respond(e, ui.card(
            "🔍 Проверка username",
            ui.rows(
                ("Username", entity_label(ent, raw.lstrip('@')), "🔰"),
                ("Статус", "занят ✅", "ℹ️"),
                ("Имя", name, "👤"),
                ("ID", f"`{ent.id}`", "🆔"),
            ),
        ))
    except Exception:
        await respond(e, ui.card("🔍 Проверка username",
                                 ui.rows(("Username", raw, "🔰"),
                                         ("Статус", "свободен ✅", "ℹ️"))))
    db.bump_stat('cmds')
