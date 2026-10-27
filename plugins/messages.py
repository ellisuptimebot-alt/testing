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

from telethon import utils as tgutils
from telethon.tl import types
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji


@client.on(events.NewMessage(pattern=r'!type(?:\s+(fast|slow|matrix|glitch))?\s+(.+)', func=owner_filter))
async def type_cmd(e):

    mode = e.pattern_match.group(1) or 'normal'
    text = e.pattern_match.group(2).strip()
    if mode == 'fast':
        msg = await respond(e, "▌")
        for i in range(0, len(text), 2):
            chunk = text[:i + 2]
            await msg.edit(chunk + ("▌" if i + 2 < len(text) else ""))
            await asyncio.sleep(0.04)
        await msg.edit(text)
    elif mode == 'slow':
        msg = await respond(e, "▌")
        shown = ""
        for ch in text:
            shown += ch
            await msg.edit(shown + "▌")
            pause = 0.3 if ch in '.!?…' else 0.12 if ch in ',;:' else 0.07
            await asyncio.sleep(pause)
        await msg.edit(text)
    elif mode == 'matrix':
        CHARS = string.ascii_letters + string.digits + "@#%&"
        msg = await respond(e, "▓" * len(text))
        for step in range(len(text)):
            parts = list(text[:step])
            for _ in range(len(text) - step):
                parts.append(random.choice(CHARS))
            await msg.edit(''.join(parts))
            await asyncio.sleep(0.07)
        await msg.edit(text)
    elif mode == 'glitch':
        GLITCH = "░▒▓█▄▀■□▪▫"
        msg = await respond(e, "".join(random.choice(GLITCH) for _ in text))
        for _ in range(6):
            glitched = "".join(
                c if random.random() > 0.4 else random.choice(GLITCH)
                for c in text
            )
            await msg.edit(glitched)
            await asyncio.sleep(0.12)
        await msg.edit(text)
    else:
        msg = await respond(e, "▌")
        shown = ""
        for i, ch in enumerate(text):
            shown += ch
            if i % 2 == 0 or i == len(text) - 1:
                await msg.edit(shown + ("▌" if i < len(text) - 1 else ""))
                await asyncio.sleep(0.05)
        await msg.edit(text)
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!echo (.+)', func=owner_filter))
async def echo_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, e.pattern_match.group(1).strip())
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!bold (.+)', func=owner_filter))
async def bold_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, f"**{e.pattern_match.group(1).strip()}**")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!italic (.+)', func=owner_filter))
async def italic_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, f"__{e.pattern_match.group(1).strip()}__")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!mono (.+)', func=owner_filter))
async def mono_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, f"`{e.pattern_match.group(1).strip()}`")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!clean(?:\s+(\d+))?$', func=owner_filter))
async def clean_cmd(e):

    limit = int(e.pattern_match.group(1) or 10)
    my_id = (await client.get_me()).id
    await e.delete()
    count = 0
    async for msg in client.iter_messages(e.chat_id, limit=limit):
        if msg.out or (msg.from_id and getattr(msg.from_id, 'user_id', None) == my_id):
            await msg.delete()
            count += 1
            await asyncio.sleep(0.1)
    info = await client.send_message(e.chat_id, ui.ok(f"Удалено **{count}** своих сообщений"))
    await asyncio.sleep(3)
    await info.delete()
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!purge(?:\s+(\d+))?$', func=owner_filter))
async def purge_cmd(e):

    limit = int(e.pattern_match.group(1) or 10)
    await e.delete()
    count = 0
    async for msg in client.iter_messages(e.chat_id, limit=limit):
        await msg.delete()
        count += 1
        await asyncio.sleep(0.04)
    info = await client.send_message(e.chat_id, ui.warn(f"Удалено **{count}** сообщений", title="Очистка"))
    await asyncio.sleep(3)
    await info.delete()
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!spam (\d+) (.+)', func=owner_filter))
async def spam_cmd(e):

    count, text = int(e.pattern_match.group(1)), e.pattern_match.group(2).strip()
    MAX_SPAM = 50
    if count < 1 or count > MAX_SPAM:
        await respond(e, ui.err(f"Допустимо от 1 до {MAX_SPAM} сообщений.", title="Спам"))
        return
    cooldown_key = f'spam_{e.chat_id}'
    remaining = command_cooldown.get(cooldown_key, 0) - time.time()
    if remaining > 0:
        await respond(e, ui.err(f"Подождите {int(remaining)} сек перед повторным спамом.",
                                title="Кулдаун"))
        return
    command_cooldown[cooldown_key] = time.time() + 30
    await e.delete()
    for _ in range(count):
        await client.send_message(e.chat_id, text)
        await asyncio.sleep(0.35)
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!forward (-?\d+)', func=owner_filter))
async def forward_cmd(e):

    if not e.reply_to_msg_id:
        await respond(e, ui.usage("!forward <chat_id>", title="Пересылка"))
        return
    try:
        msg = await e.get_reply_message()
        await client.forward_messages(int(e.pattern_match.group(1)), msg)
        await respond(e, ui.ok(ui.kv("Чат", f"`{e.pattern_match.group(1)}`"), title="Переслано"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Пересылка"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!pin$', func=owner_filter))
async def pin_cmd(e):

    if not e.reply_to_msg_id:
        await respond(e, ui.usage("!pin", title="Закреп"))
        return
    await (await e.get_reply_message()).pin(notify=False)
    await e.delete()
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!unpin$', func=owner_filter))
async def unpin_cmd(e):

    if e.reply_to_msg_id:
        await (await e.get_reply_message()).unpin()
    else:
        await client.unpin_message(e.chat_id)
    await e.delete()
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!copyall (\d+) (-?\d+)', func=owner_filter))
async def copyall_cmd(e):

    count, target = int(e.pattern_match.group(1)), int(e.pattern_match.group(2))
    await respond(e, ui.card("⏳ Копирование", ui.rows(("Сообщений", f"`{count}`"))))
    msgs = []
    async for m in client.iter_messages(e.chat_id, limit=count):
        msgs.append(m)
    msgs.reverse()
    copied = 0
    for m in msgs:
        try:
            await client.forward_messages(target, m)
            copied += 1
            await asyncio.sleep(0.4)
        except Exception:
            pass
    await respond(e, ui.ok(ui.rows(("Скопировано", f"**{copied}/{count}**"),
                                   ("Чат", f"`{target}`")), title="Копирование завершено"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!react (.+)', func=owner_filter))
async def react_cmd(e):

    if not e.reply_to_msg_id:
        await respond(e, ui.usage("!react 👍", title="Реакция"))
        return
    emoji = e.pattern_match.group(1).strip()
    try:
        await client(SendReactionRequest(
            peer=e.chat_id,
            msg_id=e.reply_to_msg_id,
            reaction=[ReactionEmoji(emoticon=emoji)]
        ))
        await e.delete()
    except Exception as ex:
        await respond(e, ui.err(f"Не удалось поставить реакцию: {ex}", title="Реакция"))
    db.bump_stat('cmds')
