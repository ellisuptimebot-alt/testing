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


@client.on(events.NewMessage(pattern=r'!ghost$', func=owner_filter))
async def ghost_cmd(e):

    state.toggle_ghost()
    if state.ghost_mode:
        await respond(e, ui.card("👻 Ghost-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "команды удаляются мгновенно"))))
        await asyncio.sleep(2)
        await e.delete()
    else:
        await respond(e, ui.card("👻 Ghost-режим", ui.rows(("Статус", "выключен ❌"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!cover(?:\s+(off|on))?$', func=owner_filter))
async def cover_cmd(e):
    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_cover(False)
        await respond(e, ui.card("🛡️ Cover-режим",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "команды снова работают"))))
    else:
        state.set_cover(True)
        await respond(e, ui.card("🛡️ Cover-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "все команды, кроме `!cover off`, игнорируются"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!silent\s*(on|off)?$', func=owner_filter))
async def silent_cmd(e):
    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_silent(False)
        await respond(e, ui.card("🤐 Silent-режим",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "ответы снова отправляются"))))
    else:
        state.set_silent(True)
        await respond(e, ui.card("🤐 Silent-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "бот молчит в ЛС"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!shadow(?:\s+(\d+))?$', func=owner_filter))
async def shadow_cmd(e):
    delay = e.pattern_match.group(1)
    if delay:
        d = int(delay)
        state.set_shadow(True, max(1, d))
        await respond(e, ui.card("👤 Shadow-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Удаление ответов", f"через `{max(1, d)}с`"))))
    elif state.shadow_enabled:
        state.set_shadow(False)
        await respond(e, ui.card("👤 Shadow-режим",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "автоудаление ответов отключено"))))
    else:
        state.set_shadow(True)
        await respond(e, ui.card("👤 Shadow-режим",
                                 ui.rows(("Статус", "включён ✅ (по умолчанию)"),
                                         ("Удаление ответов", "через `5с`"),
                                         ("Смена", "`!shadow <сек>`"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!lock(?:\s+(on|off))?$', func=owner_filter))
async def lock_cmd(e):
    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_lock(False)
        await respond(e, ui.card("🔒 Lock-режим",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "ЛС от всех открыты"))))
    else:
        state.set_lock(True)
        await respond(e, ui.card("🔒 Lock-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "бот отвечает только контактам"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!mute(?:\s+(on|off))?$', func=owner_filter))
async def mute_cmd(e):
    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_mute(False)
        await respond(e, ui.card("🔇 Mute-режим",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "ЛС принимаются"))))
    else:
        state.set_mute(True)
        await respond(e, ui.card("🔇 Mute-режим",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "все ЛС игнорируются"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!typing(?:\s+(on|off))?$', func=owner_filter))
async def typing_cmd(e):

    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_typing(False)
        await respond(e, ui.card("⌨️ Тайпинг",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Что делает", "индикатор «печатает…» не показывается"))))
    else:
        state.set_typing(True)
        await respond(e, ui.card("⌨️ Тайпинг",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Что делает", "перед ответом показывается «печатает…»"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!autodel(?:\s+(on|off))?(?:\s+(\d+))?$', func=owner_filter))
async def autodel_cmd(e):

    arg = e.pattern_match.group(1)
    delay_str = e.pattern_match.group(2)
    if arg == 'off':
        state.set_autodel(False)
        await respond(e, ui.card("🗑️ Автоудаление",
                                 ui.rows(("Статус", "выключено ❌"),
                                         ("Что делает", "сообщения не удаляются"))))
    else:
        d = int(delay_str) if delay_str else 10
        state.set_autodel(True, max(3, d))
        await respond(e, ui.card("🗑️ Автоудаление",
                                 ui.rows(("Статус", "включено ✅"),
                                         ("Удаление", f"через `{max(3, d)}с`"),
                                         ("Смена", "`!autodel on|off [сек]`"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!delay\s+(\d+)', func=owner_filter))
async def delay_cmd(e):

    sec = int(e.pattern_match.group(1))
    state.set_reply_delay(min(sec, 30))
    if state.reply_delay > 0:
        await respond(e, ui.card("⏳ Задержка ответа",
                                 ui.rows(("Задержка", f"`{state.reply_delay}с`"),
                                         ("Что делает", "имитация живого ответа"))))
    else:
        await respond(e, ui.card("⏳ Задержка ответа",
                                 ui.rows(("Статус", "выключена ❌"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!readreceipt(?:\s+(on|off))?$', func=owner_filter))
async def readreceipt_cmd(e):

    arg = e.pattern_match.group(1)
    if arg == 'off':
        state.set_readreceipt(False)
        await respond(e, ui.card("👁️ Прочтение",
                                 ui.rows(("Статус", "выключено ❌"),
                                         ("Что делает", "сообщения остаются непрочитанными"))))
    else:
        state.set_readreceipt(True)
        await respond(e, ui.card("👁️ Прочтение",
                                 ui.rows(("Статус", "включено ✅"),
                                         ("Что делает", "сообщения отмечаются прочитанными"))))
    db.bump_stat('cmds')
