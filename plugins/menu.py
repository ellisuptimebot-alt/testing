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


BACK_BTN = {"text": "◀️ Меню", "callback_data": "h:back"}


def _persist_business_connection(bc):
    """Сохраняет id бизнес-соединения в БД — переживает рестарт юзербота
    (Telegram присылает business_connection только при подключении/изменении)."""
    try:
        if bc.get('is_enabled', True) and bc.get('id'):
            db.set_saved('_biz_conn', bc['id'])
        else:
            db.del_saved('_biz_conn')
    except Exception as ex:
        logger.warning(f"persist business_connection: {ex}")


notify.on_business_connection = _persist_business_connection


async def _pult_toggle(action):
    """Переключает режим пульта. Возвращает текст-описание действия (или None)."""
    if action == 'ghost':
        state.toggle_ghost()
        return ("Ghost включён — команды удаляются мгновенно" if state.ghost_mode
                else "Ghost выключен")
    if action == 'cover':
        state.set_cover(not state.cover_enabled)
        return ("Cover включён — работают только `!cover off` и `!status_reset`"
                if state.cover_enabled else "Cover выключен")
    if action == 'silent':
        state.set_silent(not state.silent_enabled)
        return ("Silent включён — бот молчит в ЛС"
                if state.silent_enabled else "Silent выключен")
    if action == 'mute':
        state.set_mute(not state.mute_enabled)
        return ("Mute включён — все ЛС игнорируются"
                if state.mute_enabled else "Mute выключен")
    if action == 'lock':
        state.set_lock(not state.lock_enabled)
        return ("Lock включён — бот отвечает только контактам"
                if state.lock_enabled else "Lock выключен")
    return None


async def _cb_render(mid, chat_id, biz, html, buttons):
    """Обновляет карточку по нажатию кнопки. Если редактирование не удалось
    (не тот chat_id / нет business_connection_id) — шлёт новую карточку
    в тот же чат, чтобы нажатие не выглядело «мёртвым»."""
    ok = await notify.edit(mid, html, buttons=buttons,
                           business_connection_id=biz, chat_id=chat_id)
    if ok:
        return True
    logger.warning(f"[BOT-CB] edit не удался: chat={chat_id} biz={bool(biz)} — пробую новое сообщение")
    if not chat_id:
        return False
    return await notify.send(html, buttons=buttons,
                             business_connection_id=biz, chat_id=chat_id)


async def handle_bot_callback(q):
    """callback_query от внешнего бота: пульт (c:*) и справка (h:*).
    Для сообщений, отправленных от имени аккаунта (Telegram Business),
    нужен и chat_id чата, и business_connection_id — иначе edit не сработает."""
    data = q.get('data') or ''
    qid = q.get('id')
    uid = (q.get('from') or {}).get('id')
    # сообщение, на котором висит кнопка: если оно отправлено от имени аккаунта
    # (Telegram Business) — в нём есть business_connection_id, он обязателен для edit
    msg = q.get('message') or {}
    mid = msg.get('message_id')
    chat_id = (msg.get('chat') or {}).get('id')
    biz = msg.get('business_connection_id')
    if not biz and chat_id is not None and str(chat_id) != str(notify.chat_id):
        # бизнес-сообщение в чужом чате, но поле не доехало — берём из реконнекта
        biz = notify.business_connection_id
    logger.info(f"[BOT-CB] нажатие: data={data!r} from={uid} chat={chat_id} msg={mid} biz={biz}")

    if uid != OWNER_ID:
        await notify.answer_callback(qid, "⛔ Нет доступа", alert=True)
        return
    await notify.answer_callback(qid)
    if not mid:
        return

    # пульт: статус / пинг — отдельные карточки с кнопкой «назад»
    if data in ('c:status', 'c:ping'):
        if data == 'c:status':
            card = await build_status_card()
        else:
            t0 = time.monotonic()
            try:
                await client.get_me()
            except Exception:
                pass
            ms = (time.monotonic() - t0) * 1000
            q_show = "🟢 Отлично" if ms < 300 else "🟡 Нормально" if ms < 700 else "🔴 Высокая"
            card = ui.card("🏓 Пинг (MTProto)",
                           ui.rows(("Задержка", f"`{ms:.1f} мс`", "⚡"),
                                   ("Качество", q_show, "📶")))
        await _cb_render(mid, chat_id, biz, ui.to_html(card), [[dict(BACK_BTN)]])
        return

    # пульт: переключение режимов → перерисовать меню с новым состоянием
    if data.startswith('c:'):
        last = await _pult_toggle(data[2:])
        text, buttons = help_data.menu_view(last_action=last)
        await _cb_render(mid, chat_id, biz, ui.to_html(text), buttons)
        return

    # справка: навигация по разделам (h:back / h:all / h:<раздел>)
    text, buttons = help_data.help_view(data)
    await _cb_render(mid, chat_id, biz, ui.to_html(text), buttons)


@client.on(events.NewMessage(pattern=r'!menu$', func=owner_filter))
async def menu_cmd(e):
    text, buttons = help_data.menu_view()
    html = ui.to_html(text)
    target = e.chat_id
    biz = notify.business_connection_id
    same_chat = str(target) == str(notify.chat_id)
    sent, via = False, None

    # 1) Меню — прямо в тот чат, где набрана команда. В чужое ЛС бот сам
    #    зайти не может, поэтому шлём от имени аккаунта (Telegram Business).
    if biz and not same_chat:
        sent = await notify.send(html, buttons=buttons,
                                 business_connection_id=biz, chat_id=target)
        via = 'business' if sent else None
    # 2) Фолбэк: обычное сообщение бота в чат с ботом (или текущий, если это он)
    if not sent:
        sent = await notify.send(html, buttons=buttons)
        via = 'bot' if sent else None

    if via == 'business':
        await respond(e, ui.ok("Меню отправлено в этот чат от имени аккаунта (Telegram Business).",
                               title="Меню", hint="Кнопки кликабельны — жми прямо здесь"))
    elif via == 'bot':
        if same_chat:
            await respond(e, ui.ok("Меню с кнопками отправлено.", title="Меню",
                                   hint="Кнопки кликабельны"))
        else:
            await respond(e, ui.ok("Меню отправлено в чат внешнего бота.", title="Меню",
                                   hint="Business-соединение неактивно или чат вне списка получателей — в этот чат бот зайти не может. Подключение: Настройки → Бизнес → Чатботы."))
    else:
        await respond(e, ui.err("Не удалось отправить меню.\nПроверь `NOTIFY_BOT_TOKEN`, `NOTIFY_CHAT_ID` и бизнес-соединение.",
                                title="Меню"))
    db.bump_stat('cmds')


async def handle_bot_message(m):
    """Приём обычных сообщений бота (message-апдейты в поллинге).
    Меню приходит только когда владелец пишет боту в ЛС."""
    try:
        chat = m.get('chat') or {}
        if chat.get('type') != 'private':
            return
        if (m.get('from') or {}).get('id') != OWNER_ID:
            return
        if not (m.get('text') or '').strip():
            return
        text, buttons = help_data.menu_view()
        await notify.send(ui.to_html(text), buttons=buttons,
                          chat_id=chat.get('id'))
    except Exception as ex:
        logger.warning(f"bot message handler: {ex}", exc_info=True)


import help_data
