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

_watch_err_logged = set()

from telethon import utils as tgutils
from telethon.tl import types
from telethon.tl.types import (
    UserStatusOnline, UserStatusOffline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth,
)


_watch_count_logged = None
_watched_ids = set()          # tid отслеживаемых — быстрый фильтр без запроса в БД
_last_activity = {}           # tid → ts последней активности (сообщение/typing)
_ACTIVITY_QUIET = 600         # тишина ≥10 мин = «ушёл»; новая активность = «появился в сети»


def _status_name(status):
    """Ключ статуса: online / offline / recently / last_week / ... / unknown."""
    if isinstance(status, UserStatusOnline):
        return 'online'
    if isinstance(status, UserStatusOffline):
        return 'offline'
    if isinstance(status, UserStatusRecently):
        return 'recently'
    if isinstance(status, UserStatusLastWeek):
        return 'last_week'
    if isinstance(status, UserStatusLastMonth):
        return 'last_month'
    return 'unknown'


def _profile_name(ent=None, username=None):
    """Имя профиля (без markdown-символов), иначе @username, иначе ''."""
    if ent is not None:
        disp = re.sub(r'[*_`]', '', (tgutils.get_display_name(ent) or '').strip())
        if disp:
            return disp
        u = getattr(ent, 'username', None)
        if u:
            return f'@{u}'
    u = (username or '').strip().lstrip('@')
    return f'@{u}' if u else ''


def watch_label(target_id, username=None, ent=None):
    """«Имя Фамилия [`123`]» — цель так, как просят: имя профиля + ID."""
    name = _profile_name(ent, username) or f'id {target_id}'
    tid = getattr(ent, 'id', None) or target_id
    return f'**{name}** [`{tid}`]'


async def awatch_label(target_id, username=None):
    ent = None
    try:
        ent = await client.get_entity(int(target_id))
    except Exception:
        pass
    return watch_label(target_id, username, ent)


async def _watch_now(target_id):
    """Какой статус цели видит Telegram API прямо сейчас (диагностика)."""
    try:
        ent = await client.get_entity(int(target_id))
        return _status_name(getattr(ent, 'status', None))
    except Exception as ex:
        return f'err: {str(ex)[:40]}'


async def _onwatch_fire(w, source):
    """Цель появилась в сети → сообщение таргету + уведомление во внешнем боте."""
    if (w.get('last_status') or 'unknown') == 'online':
        return False
    # статус помечаем до await — поллер и апдейт-хендлер не отправят дважды
    db.update_online_watch_status(w['id'], 'online')
    tid = w['target_id']
    uname = w.get('target_username') or ''
    ent = None
    try:
        ent = await client.get_entity(int(tid))
    except Exception:
        ent = None
    name = _profile_name(ent, uname) or f'id {tid}'
    plain = f"{name} [{tid}]"
    logger.info(f"[ONWATCH] цель появилась в сети ({source}): {watch_label(tid, uname, ent)}")
    try:
        await client.send_message(tid, w['message'])
    except Exception as ex:
        logger.warning(f"[ONWATCH] не удалось отправить сообщение {plain}: {ex}")
        try:
            await notify.error_alert("ONWATCH", f"Не удалось отправить сообщение {plain}: {ex}")
        except Exception:
            pass
        return False
    try:
        await notify.online_watch(uname, tid, w['message'], name=name)
    except Exception as ex:
        logger.warning(f"[ONWATCH] уведомление не отправлено: {ex}")
    return True


@client.on(events.NewMessage(func=owner_filter))
async def onwatch_unified(e):
    try:
        text = (e.raw_text or '').strip()
        if not text:
            return
        low = text.lower()
        if not low.startswith('!onwatch'):
            return

        if low == '!onwatchlist' or low.startswith('!onwatchlist '):
            rows = db.get_online_watches()
            if not rows:
                await respond(e, ui.card("📡 ONWATCH", "список пуст",
                                         hint="`!onwatch @username|ID <текст>` — добавить"))
                db.bump_stat('cmds')
                return
            lines = []
            for r in rows:
                label = await awatch_label(r['target_id'], r['target_username'])
                now = await _watch_now(r['target_id'])
                lines.append(
                    f"• {label} | was: `{r['last_status']}` → now: `{now}`\n"
                    f"  _{r['message'][:40]}{'...' if len(r['message']) > 40 else ''}_"
                )
            await respond(e, ui.card(f"📡 ONWATCH ({len(rows)})", "\n".join(lines),
                                     hint="`!unonwatch @username|ID` — снять трек"))
            db.bump_stat('cmds')
            return

        if low.startswith('!unonwatch'):
            m = re.match(r'^!unonwatch\s+(@?[\w\d_]+)$', text, re.IGNORECASE)
            if not m:
                await respond(e, ui.usage("!unonwatch @username|ID", title="Снять отслеживание"))
                db.bump_stat('cmds')
                return
            target_raw = m.group(1)
            try:
                ent = await resolve_entity(target_raw)
            except Exception as ex:
                await respond(e, ui.err(f"Не найден: {ex}", title="ONWATCH",
                                        hint="Формат: `!unonwatch @username|ID`"))
                db.bump_stat('cmds')
                return
            db.remove_online_watch(target_id=ent.id)
            _watched_ids.discard(ent.id)
            _last_activity.pop(ent.id, None)
            logger.info(f"[ONWATCH] трек снят: {watch_label(ent.id, getattr(ent, 'username', None), ent=ent)}")
            await respond(e, ui.ok(ui.kv("Цель", watch_label(ent.id, getattr(ent, 'username', None), ent=ent)),
                                   title="Отслеживание снято"))
            db.bump_stat('cmds')
            return

        if low.startswith('!onwatch '):
            m = re.match(r'^!onwatch\s+(@?[\w\d_]+)\s+(.+)$', text, re.IGNORECASE | re.DOTALL)
            if not m:
                await respond(e, ui.usage("!onwatch @username|ID <текст сообщения>",
                                          title="ONWATCH"))
                db.bump_stat('cmds')
                return
            target_raw = m.group(1)
            msg_text = m.group(2).strip()
            try:
                ent = await resolve_entity(target_raw)
            except Exception as ex:
                await respond(e, ui.err(f"Не удалось найти пользователя: {ex}",
                                        title="ONWATCH",
                                        hint="Можно указать числовой ID: `!onwatch 12345 Привет`"))
                db.bump_stat('cmds')
                return
            if getattr(ent, 'bot', False):
                await respond(e, ui.err("За ботами следить нельзя.", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            try:
                db.add_online_watch(ent.id, getattr(ent, 'username', None) or '', msg_text, OWNER_ID, e.chat_id)
            except Exception as ex:
                await respond(e, ui.err(f"Ошибка БД: {ex}", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            logger.info(f"[ONWATCH] трек добавлен: {watch_label(ent.id, getattr(ent, 'username', None), ent=ent)} "
                        f"chat={e.chat_id} статус={_status_name(getattr(ent, 'status', None))}")
            _watched_ids.add(ent.id)
            _last_activity.pop(ent.id, None)
            await respond(e, ui.card(
                "✅ ONWATCH добавлен",
                ui.rows(
                    ("Цель", watch_label(ent.id, getattr(ent, 'username', None), ent=ent)),
                    ("Статус сейчас", f"`{_status_name(getattr(ent, 'status', None))}`"),
                    ("Сообщение", f"_{msg_text}_"),
                ),
                footer="Как только цель окажется в сети после оффлайна — отправлю сообщение и уведомлю во внешнем боте.\nТрек работает раз за разом.",
            ))
            db.bump_stat('cmds')
            return

        await respond(e, ui.usage("!onwatch @username|ID <текст>",
                                  "!onwatchlist",
                                  "!unonwatch @username|ID",
                                  title="ONWATCH"))
        db.bump_stat('cmds')
        return
    except Exception as ex:
        logger.error(f"onwatch_unified err: {ex}")
        try:
            await respond(e, ui.err(f"Ошибка: {ex}", title="ONWATCH"))
        except Exception:
            pass


async def online_watcher():
    global _watch_count_logged, _watched_ids
    logger.info("[ONWATCH] поллер статусов запущен (5–10с для 1–5 целей, до 35с)")
    interval = 10
    interval_logged = None
    while True:
        try:
            watches = db.get_online_watches()
            _watched_ids = {w['target_id'] for w in watches}
            if _watch_count_logged != len(watches):
                _watch_count_logged = len(watches)
                logger.info(f"[ONWATCH] активных треков: {len(watches)}")
            if not watches:
                await asyncio.sleep(40)
                continue
            n = len(watches)
            # до 2 целей — каждые 5с (максимум точности), до 5 — 10с,
            # дальше растим интервал, чтобы не упереться в FloodWait на GetUsers
            interval = 5 if n <= 2 else (10 if n <= 5 else min(35, 10 + 5 * (n - 5)))
            if interval_logged != interval:
                interval_logged = interval
                logger.info(f"[ONWATCH] интервал опроса: {interval}с (целей: {n})")
            for w in watches:
                try:
                    ent = await client.get_entity(w['target_id'])
                    cur = _status_name(getattr(ent, 'status', None))
                    prev = w['last_status'] or 'unknown'
                    if cur != prev:
                        logger.info(f"[ONWATCH] {w['target_id']}: {prev} → {cur}")
                        _watch_err_logged.discard(w['target_id'])
                    if prev != 'online' and cur == 'online':
                        await _onwatch_fire(w, 'poll')
                        continue
                    if prev != cur:
                        db.update_online_watch_status(w['id'], cur)
                    continue
                except FloodWaitError as fe:
                    logger.warning(f"[ONWATCH] FloodWait {fe.seconds}s — пауза")
                    await asyncio.sleep(fe.seconds + 5)
                    break
                except Exception as ex:
                    if w['target_id'] not in _watch_err_logged:
                        _watch_err_logged.add(w['target_id'])
                        logger.warning(f"[ONWATCH] не удалось проверить {w['target_id']}: {ex}")
                    else:
                        logger.debug(f"[ONWATCH] skip {w['target_id']}: {ex}")
                    continue
        except Exception as ex:
            logger.warning(f"[ONWATCH] loop err: {ex}")
        await asyncio.sleep(interval)


@client.on(events.UserUpdate)
async def onwatch_status_event(e):
    """Мгновенная реакция: смена статуса (updateUserStatus) ИЛИ активность
    (typing-апдейты) — ловит появление в сети между опросами.

    ВАЖНО: у целей со скрытым last-seen («был(а) недавно» — плейсхолдер
    приватности) сервер НИКОГДА не вернёт online и не пришлёт статус-апдейт.
    Для них работают только каналы активности: typing и входящие сообщения
    (см. onwatch_activity_msg)."""
    try:
        tid = e.user_id
        if not tid or tid == OWNER_ID:
            return
        if e.status is None and e.typing is None:
            return  # прочие user-update без статуса и без активности
        if tid not in _watched_ids:
            return
        w = db.get_online_watch_by_target(tid)
        if not w:
            return
        if e.status is not None:
            cur = _status_name(e.status)
            prev = w['last_status'] or 'unknown'
            logger.info(f"[ONWATCH] апдейт статуса: id={tid} {prev} → {cur}")
            if cur == 'online':
                await _onwatch_fire(w, 'update')
            elif cur != 'unknown' and cur != prev:
                db.update_online_watch_status(w['id'], cur)
            return
        # typing/choose_sticker/speak = пользователь реально в сети
        now = time.time()
        gap = now - _last_activity.get(tid, 0)
        _last_activity[tid] = now
        if gap >= _ACTIVITY_QUIET and (w['last_status'] or 'unknown') != 'online':
            logger.info(f"[ONWATCH] активность (typing): id={tid}")
            await _onwatch_fire(w, 'typing')
    except Exception as ex:
        logger.warning(f"[ONWATCH] status-event err: {ex}")


@client.on(events.NewMessage(incoming=True))
async def onwatch_activity_msg(e):
    """Цель написала сообщение (в ЛС или общей группе) = она в сети.
    Работает даже когда online-статус скрыт настройками приватности."""
    try:
        sid = e.sender_id
        if not sid or sid == OWNER_ID or sid not in _watched_ids:
            return
        w = db.get_online_watch_by_target(sid)
        if not w:
            return
        now = time.time()
        gap = now - _last_activity.get(sid, 0)
        _last_activity[sid] = now
        if gap >= _ACTIVITY_QUIET and (w['last_status'] or 'unknown') != 'online':
            logger.info(f"[ONWATCH] активность (сообщение): id={sid}")
            await _onwatch_fire(w, 'message')
    except Exception as ex:
        logger.warning(f"[ONWATCH] activity-msg err: {ex}")


@client.on(events.MessageRead)
async def onwatch_read_receipt(e):
    """Цель прочитала мои сообщения = она в сети. Двойные галочки
    (UpdateReadHistoryOutbox) приходят НЕЗАВИСИМО от скрытого last-seen —
    это основной канал, когда online-статус цели скрыт приватностью.
    Ограничение Telegram: если у цели отключены read-receipts, событие не приходит."""
    try:
        tid = e.chat_id
        if not tid or tid <= 0 or tid == OWNER_ID:
            return   # только ЛС (chat_id положителен и равен id юзера)
        if tid not in _watched_ids:
            return
        w = db.get_online_watch_by_target(tid)
        if not w:
            return
        now = time.time()
        gap = now - _last_activity.get(tid, 0)
        _last_activity[tid] = now
        if gap >= _ACTIVITY_QUIET and (w['last_status'] or 'unknown') != 'online':
            logger.info(f"[ONWATCH] активность (прочитал мои сообщения): id={tid}")
            await _onwatch_fire(w, 'read')
    except Exception as ex:
        logger.warning(f"[ONWATCH] read-receipt err: {ex}")
