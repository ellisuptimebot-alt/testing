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


@client.on(events.NewMessage(pattern=r'!afk(?:\s+(.+))?$', func=owner_filter))
async def afk_cmd(e):

    reason = (e.pattern_match.group(1) or '').strip()
    state.set_afk(reason)
    r = ui.kv("Причина", f"_{reason}_", "📝") if reason else ui.kv("Причина", "не указана", "📝")
    await respond(e, ui.card("😴 AFK включён", r,
                             hint="`!unafk` — выключить"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!unafk$', func=owner_filter))
async def unafk_cmd(e):

    dur = state.clear_afk()
    if dur is not None:
        await respond(e, ui.card("☀️ AFK выключен",
                                 ui.rows(("Отсутствовал", f"_{fmt_time(dur)}_"))))
    else:
        await respond(e, ui.info("AFK не был включён", title="AFK"))
    db.bump_stat('cmds')
