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

from telethon.tl import types


@client.on(events.NewMessage(pattern=r'^!save$', func=owner_filter))
async def save_media_cmd(e):

    if not e.reply_to_msg_id:
        await respond(e, ui.usage("!save", "!save <ключ> <значение>", title="Сохранение"))
        return
    replied = await e.get_reply_message()
    if not replied.media:
        await respond(e, ui.err("В ответном сообщении нет медиа.", title="Сохранение"))
        return
    os.makedirs(MEDIA_DIR, exist_ok=True)
    try:
        path = await replied.download_media(os.path.join(MEDIA_DIR, ''))
        name = os.path.basename(path) if path else 'unknown'
        db.set_saved(f'_media_{int(time.time())}', name)
        await respond(e, ui.ok(ui.kv("Файл", f"`{name}`"), title="Медиа сохранено"))
        logger.info(f"Media saved: {name}")
    except Exception as ex:
        await respond(e, ui.err(f"Ошибка сохранения: {ex}", title="Сохранение"))
        logger.error(f"Save media error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!save (\S+) (.+)', func=owner_filter))
async def save_cmd(e):

    k, v = e.pattern_match.group(1), e.pattern_match.group(2)
    db.set_saved(k, v)
    await respond(e, ui.ok(ui.kv(k, f"_{v}_"), title="Сохранено"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!get (\S+)', func=owner_filter))
async def get_cmd(e):

    k = e.pattern_match.group(1)
    v = db.get_saved(k)
    await respond(e, ui.card("📦 Данные", ui.kv(k, f"_{v}_")) if v
                   else ui.err(f"Ключ `{k}` не найден", title="Данные",
                               hint="`!list` — все ключи"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!del (\S+)', func=owner_filter))
async def del_cmd(e):

    k = e.pattern_match.group(1)
    v = db.get_saved(k)
    if v is not None:
        db.del_saved(k)
        await respond(e, ui.ok(ui.kv("Ключ", f"`{k}`"), title="Удалено"))
    else:
        await respond(e, ui.err(f"Ключ `{k}` не найден", title="Данные"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!list$', func=owner_filter))
async def list_cmd(e):

    d = db.all_saved()
    if not d:
        await respond(e, ui.card("📭 Данные", "список пуст",
                                 hint="`!save <ключ> <значение>` — сохранить"))
        db.bump_stat('cmds')
        return
    items = "\n".join(f"• `{k}` — _{v[:40]}{'…' if len(v) > 40 else ''}_" for k, v in d.items())
    await respond(e, ui.card(f"📦 Сохранено ({len(d)})", items))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!find (.+)', func=owner_filter))
async def find_cmd(e):

    query = e.pattern_match.group(1).strip().lower()
    saved_results = db.search_saved(query)
    notes_results = db.search_notes(query)
    lines = []
    if saved_results:
        lines.append(f"📦 **В сохранённом:**")
        for row in saved_results:
            lines.append(f"  • `{row['key']}` — _{row['value'][:40]}{'…' if len(row['value']) > 40 else ''}_")
    if notes_results:
        lines.append(f"📝 **В заметках:**")
        for row in notes_results:
            lines.append(f"  • `{row['key']}` — _{row['value'][:40]}{'…' if len(row['value']) > 40 else ''}_")
    if not lines:
        await respond(e, ui.card("🔍 Поиск", f"по запросу `{query}` ничего не найдено",
                                 hint="`!list` и `!notes` — все данные"))
    else:
        await respond(e, ui.card(f"🔍 Результаты: {query}", "\n".join(lines)))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!note (\S+)(?: (.+))?', func=owner_filter))
async def note_cmd(e):

    k = e.pattern_match.group(1)
    t = e.pattern_match.group(2) or ""
    if e.reply_to_msg_id:
        r = await e.get_reply_message()
        t = r.text or t
    if not t:
        await respond(e, ui.usage("!note <название> <текст>", title="Заметка"))
        return
    db.set_note(k, t)
    await respond(e, ui.ok(ui.kv("Название", f"`{k}`"), title="Заметка сохранена"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!getnote (\S+)', func=owner_filter))
async def getnote_cmd(e):

    k = e.pattern_match.group(1)
    v = db.get_note(k)
    if v is not None:
        await respond(e, ui.card(f"📝 {k}", v))
    else:
        await respond(e, ui.err(f"Заметка `{k}` не найдена", title="Заметки",
                                hint="`!notes` — список всех"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!delnote (\S+)', func=owner_filter))
async def delnote_cmd(e):

    k = e.pattern_match.group(1)
    v = db.get_note(k)
    if v is not None:
        db.del_note(k)
        await respond(e, ui.ok(ui.kv("Название", f"`{k}`"), title="Заметка удалена"))
    else:
        await respond(e, ui.err(f"Заметка `{k}` не найдена", title="Заметки"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!notes$', func=owner_filter))
async def notes_cmd(e):

    d = db.all_notes()
    if not d:
        await respond(e, ui.card("📭 Заметки", "список пуст",
                                 hint="`!note <название> <текст>` — добавить"))
        db.bump_stat('cmds')
        return
    items = "\n".join(f"• `{k}` — _{v[:40]}{'…' if len(v) > 40 else ''}_" for k, v in d.items())
    await respond(e, ui.card(f"📝 Заметки ({len(d)})", items))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!todo (.+)', func=owner_filter))
async def todo_add_cmd(e):

    task = e.pattern_match.group(1).strip()
    db.add_todo(task)
    todos = db.get_todos()
    await respond(e, ui.ok(ui.kv("Задача", f"_{task}_"), title="Задача добавлена")
                   + f"\n{ui.SEP}\n📋 Всего: **{len(todos)}**")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!todos$', func=owner_filter))
async def todos_cmd(e):

    todos = db.get_todos()
    if not todos:
        await respond(e, ui.card("📋 Задачи", "список пуст",
                                 hint="`!todo <текст>` — добавить задачу"))
        db.bump_stat('cmds')
        return
    lines = []
    for i, t in enumerate(todos, 1):
        mark = "✅" if t['done'] else "⬜"
        lines.append(f"{mark} {i}. _{t['text']}_")
    done = sum(1 for t in todos if t['done'])
    await respond(e, ui.card(f"📋 Задачи ({done}/{len(todos)} выполнено)", "\n".join(lines),
                             hint="`!done <№>` — выполнено · `!undone <№>` — вернуть"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!done (\d+)', func=owner_filter))
async def done_cmd(e):

    idx = int(e.pattern_match.group(1)) - 1
    todos = db.get_todos()
    if 0 <= idx < len(todos):
        db.update_todo(todos[idx]['id'], done=True)
        await respond(e, ui.ok(ui.kv("Задача", f"_{todos[idx]['text']}_"), title="Выполнено"))
    else:
        await respond(e, ui.err(f"Задача #{idx + 1} не найдена", title="Задачи",
                                hint="`!todos` — список задач"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!undone (\d+)', func=owner_filter))
async def undone_cmd(e):

    idx = int(e.pattern_match.group(1)) - 1
    todos = db.get_todos()
    if 0 <= idx < len(todos):
        db.update_todo(todos[idx]['id'], done=False)
        await respond(e, ui.ok(ui.kv("Задача", f"_{todos[idx]['text']}_"), title="Отметка снята"))
    else:
        await respond(e, ui.err(f"Задача #{idx + 1} не найдена", title="Задачи",
                                hint="`!todos` — список задач"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!deltodo (\d+)', func=owner_filter))
async def deltodo_cmd(e):

    idx = int(e.pattern_match.group(1)) - 1
    todos = db.get_todos()
    if 0 <= idx < len(todos):
        db.del_todo(todos[idx]['id'])
        await respond(e, ui.ok(ui.kv("Задача", f"_{todos[idx]['text']}_"), title="Задача удалена"))
    else:
        await respond(e, ui.err(f"Задача #{idx + 1} не найдена", title="Задачи",
                                hint="`!todos` — список задач"))
    db.bump_stat('cmds')
