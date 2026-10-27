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

from rp_commands import (
    RP_COMMANDS, format_rp_action, get_rp_reply,
    get_all_categories, get_category_commands,
)


@client.on(events.NewMessage(outgoing=True, func=lambda e: e.is_private and e.reply_to_msg_id))
async def rp_outgoing_handler(event):
    if event.sender_id != OWNER_ID and event.sender_id not in state.sudo_users:
        return
    try:
        reply_msg = await event.get_reply_message()
        if not reply_msg or not reply_msg.sender_id:
            return
        raw_text = event.raw_text.strip()
        lines = raw_text.split('\n', 1)
        cmd_part = lines[0].strip().lower()
        if cmd_part not in RP_COMMANDS:
            return
        custom_reply = lines[1].strip() if len(lines) > 1 else None
        target_entity = await client.get_entity(reply_msg.sender_id)
        target_name = target_entity.first_name or "пользователь"
        me = await client.get_me()
        user_name = me.first_name or "Кто-то"
        action_text = format_rp_action(cmd_part, user_name, target_name)
        reply_text = custom_reply or get_rp_reply(cmd_part)
        if state.typing_enabled:
            async with client.action(event.chat_id, 'typing'):
                await asyncio.sleep(0.8)
        sent = await safe_flood(lambda: event.reply(f"{action_text}\n{reply_text}"))
        logger.info(f"✅ RP: {user_name} -> {target_name} ({cmd_part}) | реплика: {'кастом' if custom_reply else 'рандом'}")
        if state.ghost_mode or state.shadow_enabled:
            asyncio.create_task(shadow_delete_msg(sent))
        if state.autodel_enabled:
            asyncio.create_task(shadow_delete_msg(sent, state.autodel_delay))
        db.bump_stat('cmds')
    except Exception as e:
        logger.error(f"RP error: {e}")


@client.on(events.NewMessage(pattern=r'^!rphelp$', func=lambda e: e.is_private))
async def rphelp_cmd(event):
    sender = await event.get_sender()
    if not sender:
        return
    lines = []
    for category in get_all_categories():
        cmds = get_category_commands(category)
        if cmds:
            lines.append(f"**{category.upper()}**: {', '.join(f'`{c}`' for c in cmds)}")
    await event.reply(ui.card("🎭 RP-команды", "\n".join(lines),
                              footer="Напишите команду в ответ на сообщение.\nЧерез Enter можно добавить свою реплику."))
    db.bump_stat('cmds')
