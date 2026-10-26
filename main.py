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
from collections import defaultdict

import aiohttp
from flask import Flask
from PIL import Image, ImageDraw

from telethon import events
from telethon.errors import FloodWaitError, MessageNotModifiedError
from client import client
from telethon.tl.functions.account import (
    GetAuthorizationsRequest,
)
from telethon.tl import types
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import (
    ChannelParticipantsAdmins, ChannelParticipantsBots, InputMediaDice,
    ReactionEmoji,
    UserStatusOnline, UserStatusOffline, UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth,
)

import qrcode

from config import (
    API_ID, API_HASH, PORT, OWNER_ID,
    MEDIA_DIR, MAX_COOLDOWN_ENTRIES,
    MAX_FILE_SIZE_MB, logger,
)
from notifier import notify
from utils import fmt_time, progress_bar, safe_eval, caesar, morse_enc, gen_pwd, vigenere
from downloaders import run_download, send_and_clean, set_max_file_size
from rp_commands import (
    RP_COMMANDS, format_rp_action, get_all_categories,
    get_category_commands, get_rp_reply,
)
from core import db, state, owner_filter, respond, _get_translator
import ui

set_max_file_size(MAX_FILE_SIZE_MB)

_log = logger
command_cooldown = defaultdict(float)
reply_cooldown = {}
_download_lock = asyncio.Lock()
_watch_task = None
_protect_task = None

# ── Очередь для последовательной обработки удалений ЛС ──
_pm_delete_queue = asyncio.Queue()
_pm_delete_worker_started = False

# Дедупликация: одно удаление может прийти и как MessageEdited(empty),
# и как RAW UpdateDeleteMessages. Ключ — msg_id (уникален в рамках аккаунта).
_pm_delete_seen = {}


def _pm_delete_is_new(msg_id, ttl=20):
    now = time.monotonic()
    for k in [k for k, v in _pm_delete_seen.items() if now - v > ttl]:
        _pm_delete_seen.pop(k, None)
    mid = int(msg_id)
    if mid in _pm_delete_seen:
        return False
    _pm_delete_seen[mid] = now
    return True


# Удаления, которые сделал сам юзербот (ghost/autodel) — о них уведомлять не надо
_pm_suppress_delete = set()


def suppress_pm_delete(msg_id):
    try:
        _pm_suppress_delete.add(int(msg_id))
    except Exception:
        return
    if len(_pm_suppress_delete) > 5000:
        _pm_suppress_delete.clear()


async def _enqueue_pm_delete(chat_id, msg_id, old=None, username=None, event_sender_id=0, source="?"):
    """Единая точка постановки удаления в очередь (raw / edit / сверка)."""
    if not chat_id:
        return
    try:
        mid = int(msg_id)
    except Exception:
        return
    if mid in _pm_suppress_delete:
        _pm_suppress_delete.discard(mid)
        logger.info(f"[PM-DEL] msg={mid} удалён самим ботом — пропуск")
        return
    if old is None:
        try:
            old = db.get_pm_cached(chat_id, mid)
        except Exception:
            old = None
    if not _pm_delete_is_new(mid):
        logger.info(f"[PM-DEL] msg={mid} уже обработан, дедуп")
        return
    await ensure_pm_delete_worker()
    logger.info(f"[PM-DEL] {source}: chat={chat_id} msg={mid} has_old={bool(old)}")
    await _pm_delete_queue.put({
        'chat_id': chat_id,
        'msg_id': mid,
        'old': old,
        'username': username,
        'event_sender_id': event_sender_id,
    })


# id владельца аккаунта (кэш, чтобы не дёргать get_me на каждое удаление)
_me_id_cache = None


async def _get_me_id():
    global _me_id_cache
    if _me_id_cache is None:
        _me_id_cache = (await client.get_me()).id
    return _me_id_cache


@client.on(events.Raw)
async def pm_delete_raw_update(e):
    try:
        # events.Raw передаёт сам объект Update (а не обёртку с .update)
        u = getattr(e, 'update', e)
        if not isinstance(u, types.UpdateDeleteMessages):
            return
        msg_ids = list(u.messages or [])
        if not msg_ids:
            return
        # ВАЖНО: у UpdateDeleteMessages НЕТ поля peer — Telegram не сообщает,
        # в каком чате удалили сообщение. Поэтому chat_id берём из своего кэша.
        logger.info(f"[PM-DEL-RAW] UpdateDeleteMessages count={len(msg_ids)} ids={msg_ids}")
        for msg_id in msg_ids:
            old = db.get_pm_cached_by_msg(msg_id)
            if not old:
                logger.info(f"[PM-DEL-RAW] msg={msg_id} нет в кэше ЛС — пропуск (не ЛС или старое)")
                continue
            # своё сообщение (исходящее) — уведомление не отправляем,
            # но протухшую строку кэша убираем
            if old.get('sender_id'):
                try:
                    _me = await _get_me_id()
                except Exception:
                    _me = None
                if _me and old['sender_id'] == _me:
                    logger.info(f"[PM-DEL-RAW] msg={msg_id} своё сообщение — пропуск")
                    try:
                        db.delete_pm_cached(old.get('chat_id'), msg_id)
                    except Exception:
                        pass
                    continue
            username = None
            if old.get('sender_id'):
                try:
                    s = await client.get_entity(old['sender_id'])
                    username = getattr(s, 'username', None)
                except Exception:
                    pass
            await _enqueue_pm_delete(old.get('chat_id'), msg_id, old=old, username=username, source="raw")
            await asyncio.sleep(0.01)
    except Exception as ex:
        logger.warning(f"[PM-DEL-RAW] err: {ex}", exc_info=True)


async def _pm_delete_worker():
    while True:
        try:
            item = await _pm_delete_queue.get()
            chat_id = item.get('chat_id')
            msg_id = item['msg_id']
            if not chat_id:
                logger.warning(f"[PM-DEL-WORKER] chat_id пустой для msg={msg_id} — пропуск")
                continue
            logger.info(f"[PM-DEL-WORKER] Получено из очереди: chat={chat_id} msg_id={msg_id} size~{_pm_delete_queue.qsize()}")

            old = item.get('old')
            if old is None:
                old = db.get_pm_cached(chat_id, msg_id)
                logger.info(f"[PM-DEL-WORKER] Кэша не было в очереди, взяли из БД: {bool(old)}")
            else:
                logger.info(f"[PM-DEL-WORKER] Используем снапшот кэша из очереди")

            username = item.get('username')
            event_sid = item.get('event_sender_id', 0)

            if old:
                sender_id = old.get('sender_id') or event_sid or 0
                sender_name = old.get('sender_name') or 'пользователь'
                text = old.get('text') or ''
                has_media = bool(old.get('has_media'))
                media_type = old.get('media_type')
            else:
                sender_id = event_sid or 0
                sender_name = 'пользователь'
                text = ''
                has_media = False
                media_type = None

            logger.info(f"[PM-DEL-WORKER] Отправляю уведомление: from={sender_id} name={sender_name} txt_len={len(text)} cache={bool(old)}")
            res = await notify.pm_deleted(chat_id, sender_id, sender_name, text, has_media, media_type, username=username, cached=bool(old))
            logger.info(f"[PM-DEL-WORKER] Результат отправки: {res}")

            try:
                db.delete_pm_cached(chat_id, msg_id)
            except Exception as e:
                logger.debug(f"[PM-DEL-WORKER] delete cache err: {e}")

            await asyncio.sleep(0.5)
        except Exception as ex:
            logger.error(f"Ошибка в воркере pm_delete_worker: {ex}")
        finally:
            try:
                _pm_delete_queue.task_done()
            except Exception:
                pass


async def ensure_pm_delete_worker():
    global _pm_delete_worker_started
    if not _pm_delete_worker_started:
        _pm_delete_worker_started = True
        asyncio.create_task(_pm_delete_worker())
        logger.info("[TASK] pm_delete_worker запущен (from handler)")
    else:
        logger.debug("[TASK] pm_delete_worker уже запущен")


# ── Сверка кэша ЛС с реальными сообщениями ──
# Telegram не всегда присылает апдейты об удалении (особенно в ЛС), поэтому
# раз в N секунд проверяем по ID, какие сообщения из кэша ещё существуют.

async def pm_presence_watcher(interval=45, max_age=259200, chats_per_cycle=20, ids_per_chat=100):
    await asyncio.sleep(20)  # даём клиенту подняться
    while True:
        try:
            chats = db.get_pm_chats_for_check(max_age=max_age, limit=chats_per_cycle)
            for cid in chats:
                if not cid:
                    continue
                try:
                    rows = db.get_pm_rows_for_check(cid, max_age=max_age, limit=ids_per_chat)
                    if not rows:
                        continue
                    ids = [int(r['msg_id']) for r in rows]
                    try:
                        msgs = await client.get_messages(cid, ids=ids)
                    except FloodWaitError as fe:
                        logger.warning(f"[PM-CHECK] FloodWait {fe.seconds}s — пауза")
                        await asyncio.sleep(fe.seconds + 5)
                        break
                    except Exception as ex:
                        logger.debug(f"[PM-CHECK] get_messages chat={cid}: {ex}")
                        continue

                    alive = []
                    for row, m in zip(rows, msgs):
                        exists = m is not None and not isinstance(m, types.MessageEmpty)
                        if exists:
                            alive.append(row['msg_id'])
                            continue
                        # сообщение отсутствует в чате
                        if row.get('verified'):
                            logger.info(
                                f"[PM-CHECK] ПРОПАЛО: chat={cid} msg={row['msg_id']} "
                                f"from={row.get('sender_name')} txt={(row.get('text') or '')[:30]!r}"
                            )
                            username = None
                            if row.get('sender_id'):
                                try:
                                    s = await client.get_entity(row['sender_id'])
                                    username = getattr(s, 'username', None)
                                except Exception:
                                    pass
                            await _enqueue_pm_delete(
                                cid, row['msg_id'], old=row, username=username, source="check"
                            )
                        else:
                            # первая сверка (базовая линия) — тихо чистим
                            try:
                                db.delete_pm_cached(cid, row['msg_id'])
                            except Exception:
                                pass

                    db.mark_pm_alive(cid, alive)
                    await asyncio.sleep(1.2)
                except Exception as ex:
                    logger.debug(f"[PM-CHECK] chat={cid} err: {ex}")
                    continue
        except Exception as ex:
            logger.warning(f"[PM-CHECK] loop err: {ex}")
        await asyncio.sleep(interval)



# ── Разрешение цели команды: '@user' / 'user' / числовой ID ──
# (у многих юзернейма нет — тогда target указывается числом)

async def resolve_entity(target):
    t = (target or '').strip()
    if not t:
        raise ValueError('не указан пользователь')
    if re.fullmatch(r'-?\d+', t):
        try:
            return await client.get_entity(int(t))
        except Exception:
            # id может не найтись в кэше сессии — пробуем строковым ключом
            return await client.get_entity(t)
    uname = t.lstrip('@').lower()
    try:
        # сначала штатно (имя, юзернейм, телефон, id) — работает если юзер уже в диалогах
        return await client.get_entity(t.lstrip('@'))
    except Exception:
        pass
    try:
        # @lucky_m_01 vs lucky_M_01: Telegram сравнивает username в диалогах
        # регистрозависимо, поэтому ищем вручную по нижнему регистру
        async for d in client.iter_dialogs():
            u = getattr(d.entity, 'username', None)
            if u and u.lower() == uname:
                return d.entity
    except Exception:
        pass
    raise ValueError(f'пользователь {t} не найден (нет в диалогах или ошибка сети)')


def entity_label(ent, fallback=None):
    """@username, а если его нет — id."""
    u = getattr(ent, 'username', None)
    if u:
        return f'@{u}'
    if fallback:
        return fallback
    return f'id{getattr(ent, "id", "")}'


app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 UserBot работает 24/7!"

def run_web():
    app.run(host=os.environ.get('HOST', '0.0.0.0'), port=PORT)

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

async def build_status_card():
    me = await client.get_me()
    dialogs = await client.get_dialogs()
    s = db.all_stats()
    afk_status = f"вкл — {state.afk_reason or 'без причины'}" if state.afk_start_time else "выкл ❌"

    def _yn(v):
        return "вкл ✅" if v else "выкл ❌"

    return ui.card(
        "📊 Статус UserBot",
        ui.rows(
            ("Профиль", f"{me.first_name} {me.last_name or ''}".strip(), "👤"),
            ("Чатов", f"`{len(dialogs)}`", "💬"),
            ("Автоответчик", ("💤 " if state.auto_reply_enabled else "☀️ ") + _yn(state.auto_reply_enabled), "🤖"),
            ("Ghost", _yn(state.ghost_mode), "👻"),
            ("Cover", _yn(state.cover_enabled), "🛡️"),
            ("Silent", _yn(state.silent_enabled), "🤐"),
            ("Shadow", _yn(state.shadow_enabled), "👤"),
            ("Lock", _yn(state.lock_enabled), "🔒"),
            ("Mute", _yn(state.mute_enabled), "🔇"),
            ("Тайпинг", _yn(state.typing_enabled), "⌨️"),
            ("Автоудаление", _yn(state.autodel_enabled), "🗑️"),
            ("Задержка ответа", f"`{state.reply_delay}с`", "⏳"),
            ("Прочтение", _yn(state.readreceipt_enabled), "👁️"),
            ("AFK", afk_status, "😴"),
            ("Sudo", f"`{len(state.sudo_users)}`", "👑"),
            ("Аптайм", f"`{state.uptime}`", "⏱"),
            ("Команд выполнено", f"`{s.get('cmds', 0)}`", "📨"),
        ),
        hint="`!help` — справка по командам",
    )


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

@client.on(events.NewMessage(pattern=r'!dice$', func=owner_filter))
async def dice_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎲'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!dart$', func=owner_filter))
async def dart_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎯'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!basket$', func=owner_filter))
async def basket_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🏀'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!football$', func=owner_filter))
async def football_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('⚽'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!bowling$', func=owner_filter))
async def bowling_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎳'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!casino$', func=owner_filter))
async def casino_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎰'))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!coin$', func=owner_filter))
async def coin_cmd(e):

    sides = ["Орёл 🦅", "Решка 💰"]
    r = random.choice(sides)
    flips = random.randint(3, 9)
    await respond(e, ui.card("🪙 Монета",
                             ui.rows(("Бросков", f"`{flips}`"),
                                     ("Результат", f"**{r}**"))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!rand(?:\s+(-?\d+)(?:\s+(-?\d+))?)?$', func=owner_filter))
async def rand_cmd(e):

    g = e.pattern_match
    a, b = g.group(1), g.group(2)
    if a and b:
        lo, hi = sorted([int(a), int(b)])
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", f"`{lo}` … `{hi}`"),
                                         ("Результат", f"**{random.randint(lo, hi)}**"))))
    elif a:
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", f"`1` … `{a}`"),
                                         ("Результат", f"**{random.randint(1, int(a))}**"))))
    else:
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", "`1` … `100`"),
                                         ("Результат", f"**{random.randint(1, 100)}**"))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!8ball(?:\s+(.+))?$', func=owner_filter))
async def eightball_cmd(e):

    ANSWERS = {
        'pos': [
            ("Определённо да", "✅", "Вселенная согласна с тобой."),
            ("Без сомнений", "💯", "Это решено раньше, чем ты спросил."),
            ("Скорее всего да", "👍", "Всё складывается в твою пользу."),
            ("Хорошие перспективы", "🌟", "Будущее выглядит светлым."),
            ("Знаки говорят «да»", "🔮", "Мистические силы на твоей стороне."),
            ("Всё указывает на «да»", "💫", "Судьба уже всё решила."),
            ("Да, и поскорее", "🚀", "Не медли — действуй прямо сейчас."),
            ("Абсолютно точно", "🏆", "Лучшего ответа не существует."),
            ("Это неизбежно", "⚡", "Ничто не остановит это."),
            ("Да, если сделаешь шаг", "🦶", "Действие — ключ к результату."),
            ("Вселенная шепчет: да", "🌌", "Даже звёзды кивают."),
            ("Смело иди вперёд", "🎯", "Ты уже знал ответ — я лишь подтверждаю."),
        ],
        'neu': [
            ("Пока не ясно", "🤔", "Туман будущего слишком густой."),
            ("Спроси позже", "⏰", "Момент ещё не настал."),
            ("Не могу предсказать", "🌫", "Слишком много переменных."),
            ("Сосредоточься и повтори", "🧘", "Твой разум мешает ответу."),
            ("Лучше не рассказывать", "🤫", "Некоторые тайны лучше хранить."),
            ("Трудно сказать", "😶", "Даже я не всесилен."),
            ("Возможно, но не сейчас", "🌙", "Подожди подходящего момента."),
            ("Ответ где-то рядом", "🔭", "Смотри внимательнее вокруг себя."),
        ],
        'neg': [
            ("Мой ответ — нет", "🚫", "Прими это спокойно."),
            ("Перспективы не очень", "😕", "Стоит пересмотреть планы."),
            ("Весьма сомнительно", "🙄", "Интуиция говорит «осторожно»."),
            ("Точно нет", "💀", "Даже не думай об этом."),
            ("Не рассчитывай", "❌", "Лучше найди другой путь."),
            ("Категорически нет", "🔴", "Вселенная против."),
            ("Всё против этого", "⛈", "Сейчас не лучшее время."),
            ("Откажись от идеи", "🗑", "Это дорога в никуда."),
            ("Шансы ничтожны", "🎰", "Даже удача отвернулась."),
        ],
    }
    question = (e.pattern_match.group(1) or '').strip()
    spin = ["🎱", "🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘", "🎱"]
    msg = await respond(e, "🎱 Шар вращается...")
    for frame in spin:
        await msg.edit(f"{frame} Шар вращается...")
        await asyncio.sleep(0.15)
    pool_key = random.choices(['pos', 'neu', 'neg'], weights=[38, 27, 35])[0]
    answer, emoji, comment = random.choice(ANSWERS[pool_key])
    color = {"pos": "🟢", "neu": "🟡", "neg": "🔴"}[pool_key]
    label = {"pos": "ПОЗИТИВНЫЙ", "neu": "НЕЙТРАЛЬНЫЙ", "neg": "НЕГАТИВНЫЙ"}[pool_key]
    confidence = random.randint(55, 99)
    bar = progress_bar(confidence, 100, 10)
    q_line = f"❓ _{question}\n\n" if question else ""
    await msg.edit(
        ui.card("🎱 Магический шар",
                f"{q_line}"
                f"{emoji}  **{answer}**\n"
                f"💬 _{comment}_\n\n"
                f"{color} {label}\n"
                f"[{bar}] **{confidence}%** уверенности")
    )
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!rps(?:\s+(.+))?$', func=owner_filter))
async def rps_cmd(e):

    MAP = {'к': '🪨 Камень', 'камень': '🪨 Камень', 'н': '✂️ Ножницы', 'ножницы': '✂️ Ножницы', 'б': '📄 Бумага', 'бумага': '📄 Бумага'}
    BOT = ['🪨 Камень', '✂️ Ножницы', '📄 Бумага']
    WIN = {'🪨 Камень': '✂️ Ножницы', '✂️ Ножницы': '📄 Бумага', '📄 Бумага': '🪨 Камень'}
    arg = (e.pattern_match.group(1) or '').lower().strip()
    if not arg or arg not in MAP:
        await respond(e, ui.card("✊✌️🖐 КНБ",
                                 "`!rps камень` / `ножницы` / `бумага`",
                                 hint="Коротко: `к`, `н`, `б`"))
        return
    uc, bc = MAP[arg], random.choice(BOT)
    if uc == bc:
        res = "🤝 **Ничья!**"
    elif WIN[uc] == bc:
        res = "🏆 **Ты победил!**"
    else:
        res = "💀 **Бот победил!**"
    await respond(e, ui.card("✊✌️🖐 КНБ",
                             ui.rows(("Ты", uc, "👤"), ("Бот", bc, "🤖")),
                             footer=res))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!slot$', func=owner_filter))
async def slot_cmd(e):

    SYM = ['🍒', '🍋', '🍊', '🍇', '🍉', '⭐', '💎', '7️⃣', '🔔', '🍀']
    msg = await respond(e, "🎰 [ ▓ | ▓ | ▓ ]")
    for _ in range(4):
        s = [random.choice(SYM) for _ in range(3)]
        await msg.edit(f"🎰 [ {s[0]} | {s[1]} | {s[2]} ]")
        await asyncio.sleep(0.3)
    s = [random.choice(SYM) for _ in range(3)]
    if s[0] == s[1] == s[2]:
        res = "💰💰💰 **ДЖЕКПОТ!**" if s[0] in ('💎', '7️⃣') else "🎊 **Выигрыш! Три одинаковых!**"
    elif len(set(s)) < 3:
        res = "😅 Почти! Два одинаковых — ещё раз!"
    else:
        res = "💸 Не повезло. Попробуй снова!"
    await msg.edit(ui.card("🎰 Слот-машина",
                           f"[ {s[0]} | {s[1]} | {s[2]} ]\n\n{res}"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!lucky$', func=owner_filter))
async def lucky_cmd(e):

    pct = random.randint(0, 100)
    bar = progress_bar(pct, 100, 12)
    tips = {
        (90, 100): "🌟 АБСОЛЮТНАЯ УДАЧА! Сегодня твой день!",
        (70, 89): "🍀 Очень удачный день — действуй!",
        (50, 69): "😊 Неплохо — удача на твоей стороне",
        (30, 49): "😐 Средний день, будь осторожен",
        (10, 29): "😬 Не лучший день...",
        (0, 9): "💀 Сиди дома и не высовывайся!",
    }
    msg = next(v for (a, b), v in tips.items() if a <= pct <= b)
    await respond(e, ui.card("🔮 Индекс удачи",
                             f"[{bar}] **{pct}%**\n\n{msg}"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!choose (.+)', func=owner_filter))
async def choose_cmd(e):

    raw = e.pattern_match.group(1)
    opts = [o.strip() for o in re.split(r'[,|/]', raw) if o.strip()]
    if len(opts) < 2:
        await respond(e, ui.usage("!choose пицца, суши, бургер",
                                  title="Случайный выбор"))
        return
    winner = random.choice(opts)
    listed = "\n".join(f"{'➡️' if o == winner else '  •'} {o}" for o in opts)
    await respond(e, ui.card(f"🤔 Выбираю из {len(opts)} вариантов",
                             f"{listed}\n\n✅ **Выбор: {winner}**"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!quiz$', func=owner_filter))
async def quiz_cmd(e):

    QUESTIONS = [
        ("Столица Австралии?", ["Сидней", "Мельбурн", "Канберра", "Перт"], 2),
        ("Сколько планет в Солнечной системе?", ["7", "8", "9", "10"], 1),
        ("Кто написал «Гамлета»?", ["Диккенс", "Толстой", "Шекспир", "Гёте"], 2),
        ("Химический символ золота?", ["Go", "Gd", "Au", "Ag"], 2),
        ("Год основания Google?", ["1996", "1998", "2000", "2002"], 1),
        ("Самая длинная река мира?", ["Амазонка", "Янцзы", "Нил", "Конго"], 2),
        ("Сколько байт в килобайте?", ["512", "1024", "2048", "4096"], 1),
        ("Скорость света (км/с)?", ["150 000", "300 000", "450 000", "600 000"], 1),
    ]
    q, opts, ans_idx = random.choice(QUESTIONS)
    letters = ['A', 'B', 'C', 'D']
    opts_text = "\n".join(f"{letters[i]}. {o}" for i, o in enumerate(opts))
    correct = f"{letters[ans_idx]}. {opts[ans_idx]}"
    await respond(e,
        ui.card("🧠 Викторина",
                f"_{q}_\n\n{opts_text}\n\n"
                f"||✅ Ответ: **{correct}**||")
    )
    db.bump_stat('cmds')

# (functions moved to utils.py: safe_eval, caesar, morse_enc, gen_pwd, vigenere)

@client.on(events.NewMessage(pattern=r'!calc (.+)', func=owner_filter))
async def calc_cmd(e):

    expr = e.pattern_match.group(1).strip()
    r = safe_eval(expr)
    if r is not None:
        await respond(e, ui.card("🧮 Калькулятор",
                                 ui.rows(("Выражение", f"`{expr}`"),
                                         ("Результат", f"**{r}**"))))
    else:
        await respond(e, ui.err("Проверь выражение.",
                                title="Ошибка выражения",
                                hint="Разрешены: `+ - * / % sqrt sin cos tan log abs pow pi e factorial ceil floor round`"))
    db.bump_stat('cmds')

async def send_reminder(chat_id, msg_text, delay):
    await asyncio.sleep(delay)
    try:
        await client.send_message(chat_id, ui.card("⏰ Напоминание", f"_{msg_text}"))
    except Exception as e:
        logger.error(f"Ошибка напоминания: {e}")

@client.on(events.NewMessage(pattern=r'!remind (\d+)\s+(.+)', func=owner_filter))
async def remind_cmd(e):

    delay = int(e.pattern_match.group(1))
    text = e.pattern_match.group(2).strip()
    await respond(e, ui.card("⏰ Напоминание",
                             ui.rows(("Через", f"**{fmt_time(delay)}**"),
                                     ("Текст", f"_{text}_"))))
    asyncio.create_task(send_reminder(e.chat_id, text, delay))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!search (.+)', func=owner_filter))
async def search_cmd(e):
    query = _to_english(e.pattern_match.group(1).strip())
    msg = await respond(e, ui.card("⏳ Wikipedia", "ищу…"))
    try:
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': query,
            'format': 'json',
            'srlimit': 5,
        }
        async with aiohttp.ClientSession() as s:
            async with s.get('https://en.wikipedia.org/w/api.php', params=params) as resp:
                data = await resp.json()
        results = data.get('query', {}).get('search', [])
        if not results:
            await msg.edit(ui.err("Ничего не найдено", title="Wikipedia",
                                  hint="Попробуйте другой запрос"))
            return
        lines = [f"🔍 **Результаты (Wikipedia):** _{query}_\n"]
        for i, r in enumerate(results, 1):
            title = r.get('title', '?')
            href = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"
            snippet = r.get('snippet', '').replace('<span class="searchmatch">', '**').replace('</span>', '**')[:120]
            lines.append(f"{i}. [{title}]({href})")
            if snippet:
                lines.append(f"   _{snippet}_")
        await msg.edit(ui.card(f"🔍 Wikipedia: {query}", "\n".join(lines[1:]) or "—"))
    except Exception as ex:
        await msg.edit(ui.err(f"Ошибка поиска: {ex}", title="Wikipedia"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!shorten (.+)', func=owner_filter))
async def shorten_cmd(e):

    url = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ Сокращение", "сокращаю ссылку…"))
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get("https://is.gd/create.php",
                             params={"format": "json", "url": url},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
        if 'shorturl' in data:
            await msg.edit(ui.card("✂️ Ссылка сокращена",
                                   ui.rows(("Оригинал", f"`{url[:55]}{'…' if len(url) > 55 else ''}`"),
                                           ("Короткая", data['shorturl']))))
        else:
            await msg.edit(ui.err(data.get('errormessage', 'Неверный URL'), title="Сокращение ссылки"))
    except Exception:
        await msg.edit(ui.err("Проверь URL.", title="Сокращение ссылки"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!weather (.+)', func=owner_filter))
async def weather_cmd(e):

    city = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ Погода", "запрашиваю данные…"))
    try:
        city_en = _to_english(city)
        from urllib.parse import quote
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://wttr.in/{quote(city_en)}?format=j1",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status != 200:
                    raise ConnectionError(f"HTTP {r.status}")
                data = await r.json()
        cc = data.get('current_condition')
        loc = data.get('nearest_area')
        if not cc or not loc:
            await msg.edit(ui.err("Город не найден. Попробуй на латинице или уточни (страна/регион).",
                                  title="Погода"))
            return
        cc = cc[0]
        loc = loc[0]
        city_name = loc['areaName'][0]['value']
        country = loc['country'][0]['value']
        temp = cc['temp_C']
        feels = cc['FeelsLikeC']
        humidity = cc['humidity']
        wind = cc['windspeedKmph']
        desc = cc['weatherDesc'][0]['value']
        await msg.edit(
            ui.card(f"🌤 Погода: {city_name}, {country}",
                    ui.rows(("Температура", f"**{temp}°C** (ощущается как {feels}°C)", "🌡"),
                            ("Влажность", f"**{humidity}%**", "💧"),
                            ("Ветер", f"**{wind} км/ч**", "💨"),
                            ("Описание", desc, "📋")))
        )
    except (aiohttp.ClientError, ConnectionError) as ex:
        logger.warning(f"wttr.in failed: {ex}, trying Open-Meteo...")
        async with aiohttp.ClientSession() as s:
            geo_r = await s.get(
                f"https://geocoding-api.open-meteo.com/v1/search?name={quote(city_en)}&count=1",
                timeout=aiohttp.ClientTimeout(total=10)
            )
            if geo_r.status != 200:
                await msg.edit(ui.err("Город не найден. Попробуй на латинице.", title="Погода"))
                return
            geo = await geo_r.json()
            results = geo.get('results')
            if not results:
                await msg.edit(ui.err("Город не найден. Попробуй на латинице.", title="Погода"))
                return
            lat = results[0]['latitude']
            lon = results[0]['longitude']
            city_name = results[0].get('name', city)
            country = results[0].get('country', '')
            weather_r = await s.get(
                f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
                f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
                f"wind_speed_10m,weather_code&timezone=auto",
                timeout=aiohttp.ClientTimeout(total=10)
            )
            if weather_r.status != 200:
                await msg.edit(ui.err("Не удалось получить погоду. Попробуй позже.", title="Погода"))
                return
            wdata = await weather_r.json()
            cur = wdata.get('current', {})
            temp = cur.get('temperature_2m', '?')
            feels = cur.get('apparent_temperature', '?')
            humidity = cur.get('relative_humidity_2m', '?')
            wind = cur.get('wind_speed_10m', '?')
            wcode = cur.get('weather_code', 0)
            descs = {0:'Ясно', 1:'Преимущественно ясно', 2:'Переменная облачность', 3:'Пасмурно',
                     45:'Туман', 48:'Иней', 51:'Морось', 53:'Морось', 55:'Морось',
                     61:'Дождь', 63:'Дождь', 65:'Дождь', 71:'Снег', 73:'Снег', 75:'Снег',
                     80:'Ливень', 81:'Ливень', 82:'Ливень', 95:'Гроза', 96:'Гроза', 99:'Гроза'}
            desc = descs.get(wcode, f'Код {wcode}')
            await msg.edit(
                ui.card(f"🌤 Погода: {city_name}, {country}",
                        ui.rows(("Температура", f"**{temp}°C** (ощущается как {feels}°C)", "🌡"),
                                ("Влажность", f"**{humidity}%**", "💧"),
                                ("Ветер", f"**{wind} км/ч**", "💨"),
                                ("Описание", desc, "📋")))
            )
    except Exception as ex:
        logger.warning(f"Weather error: {ex}", exc_info=True)
        await msg.edit(ui.err("Ошибка получения погоды. Попробуй позже.", title="Погода"))
    db.bump_stat('cmds')

def _to_english(text):
    latin = sum(1 for c in text if 'a' <= c <= 'z' or 'A' <= c <= 'Z')
    if latin / max(len(text), 1) > 0.5:
        return text
    try:
        return _get_translator('en').translate(text)
    except Exception:
        return text

@client.on(events.NewMessage(pattern=r'!translate(?: ([a-z]{2}))? (.+)', func=owner_filter))
async def translate_cmd(e):

    target_lang = e.pattern_match.group(1) or 'ru'
    text = e.pattern_match.group(2).strip()
    msg = await respond(e, ui.card("⏳ Переводчик", "перевожу…"))
    try:
        t = await asyncio.to_thread(_get_translator, target_lang)
        result = await asyncio.to_thread(t.translate, text)
        await msg.edit(ui.card(f"🌐 Перевод ({target_lang})", f"{result}"))
    except Exception as ex:
        await msg.edit(ui.err(f"Ошибка перевода: {ex}", title="Переводчик"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!base64 (encode|decode) (.+)', func=owner_filter))
async def base64_cmd(e):

    mode, text = e.pattern_match.group(1), e.pattern_match.group(2).strip()
    try:
        if mode == 'encode':
            res = base64.b64encode(text.encode()).decode()
            await respond(e, ui.card("🔐 Base64 encode", f"`{res}`"))
        else:
            res = base64.b64decode(text.encode()).decode()
            await respond(e, ui.card("🔓 Base64 decode", f"`{res}`"))
    except Exception as ex:
        logger.error(f"base64 error: {ex}")
        await respond(e, ui.err("Проверь данные.", title="Base64"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!hash (.+)', func=owner_filter))
async def hash_cmd(e):

    text = e.pattern_match.group(1).strip().encode()
    await respond(e, ui.card(
        "#️⃣ Хэши",
        ui.rows(
            ("MD5", f"`{hashlib.md5(text).hexdigest()}`"),
            ("SHA1", f"`{hashlib.sha1(text).hexdigest()}`"),
            ("SHA256", f"`{hashlib.sha256(text).hexdigest()}`"),
            ("SHA512", f"`{hashlib.sha512(text).hexdigest()[:64]}…`"),
        ),
    ))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!morse (.+)', func=owner_filter))
async def morse_cmd(e):

    text = e.pattern_match.group(1).strip()
    await respond(e, ui.card("📡 Азбука Морзе",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Код", f"`{morse_enc(text)}`"))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!caesar (encode|decode) (\d+) (.+)', func=owner_filter))
async def caesar_cmd(e):

    mode, shift, text = e.pattern_match.group(1), int(e.pattern_match.group(2)), e.pattern_match.group(3)
    res = caesar(text, shift, dec=(mode == 'decode'))
    await respond(e, ui.card(f"{'🔒' if mode == 'encode' else '🔓'} Цезарь (сдвиг {shift})",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Результат", f"`{res}`"))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!vigenere (encode|decode) (\S+) (.+)', func=owner_filter))
async def vigenere_cmd(e):

    mode, key, text = e.pattern_match.group(1), e.pattern_match.group(2), e.pattern_match.group(3)
    res = vigenere(text, key, dec=(mode == 'decode'))
    await respond(e, ui.card(f"{'🔒' if mode == 'encode' else '🔓'} Виженер (ключ {key})",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Результат", f"`{res}`"))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!password(?:\s+(\d+))?(?:\s+(simple))?$', func=owner_filter))
async def password_cmd(e):

    length = max(4, min(int(e.pattern_match.group(1) or 16), 128))
    sym = not e.pattern_match.group(2)
    pwd = gen_pwd(length, sym)
    s = "🔴 Слабый" if length < 8 else "🟡 Средний" if length < 12 else "🟢 Сильный" if length < 20 else "💎 Очень сильный"
    await respond(e, ui.card(f"🔑 Пароль ({length} симв.)",
                             f"`{pwd}`\n\n"
                             + ui.rows(("Сила", s),
                                       ("Символы", '✅' if sym else '❌'))))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!qr (.+)', func=owner_filter))
async def qr_cmd(e):

    text = e.pattern_match.group(1).strip()
    msg = await respond(e, ui.card("⏳ QR-код", "генерирую…"))
    try:
        os.makedirs(MEDIA_DIR, exist_ok=True)
        path = os.path.join(MEDIA_DIR, f'qr_{int(time.time())}.png')
        img = await asyncio.to_thread(qrcode.make, text)
        await asyncio.to_thread(img.save, path)
        await client.send_file(e.chat_id, path,
            caption=ui.card("📱 QR-код", f"`{text[:80]}{'...' if len(text) > 80 else ''}`"))
        await msg.delete()
        os.remove(path)
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="QR-код"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!uuid$', func=owner_filter))
async def uuid_cmd(e):

    ids = [str(uuid.uuid4()) for _ in range(5)]
    out = "\n".join(f"`{u}`" for u in ids)
    await respond(e, ui.card("🆔 UUID v4", out))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!color (.+)', func=owner_filter))
async def color_cmd(e):

    raw = e.pattern_match.group(1).strip().replace(' ', '')
    if raw.startswith('#'):
        h = raw.lstrip('#')
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        hex_val = raw.upper()
    else:
        raw = raw.removeprefix('rgb(').removesuffix(')')
        r, g, b = map(int, raw.split(','))
        hex_val = f"#{r:02X}{g:02X}{b:02X}"
    msg = await respond(e, ui.card("⏳ Цвет", "генерирую образец…"))
    try:
        os.makedirs(MEDIA_DIR, exist_ok=True)
        path = os.path.join(MEDIA_DIR, f'color_{int(time.time())}.png')
        img = await asyncio.to_thread(Image.new, 'RGB', (300, 200), (r, g, b))
        brightness = (r * 299 + g * 587 + b * 114) / 1000
        text_color = 'white' if brightness < 128 else 'black'
        draw = await asyncio.to_thread(ImageDraw.Draw, img)
        await asyncio.to_thread(draw.text, (10, 10), hex_val, fill=text_color)
        await asyncio.to_thread(img.save, path)
        await client.send_file(e.chat_id, path,
            caption=ui.card(f"🎨 {hex_val}", ui.rows(("RGB", f"`rgb({r}, {g}, {b})`"))))
        await msg.delete()
        os.remove(path)
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="Цвет"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!ascii (.+)', func=owner_filter))
async def ascii_cmd(e):

    text = e.pattern_match.group(1).strip()
    codes = ' '.join(str(ord(c)) for c in text)
    back = ''.join(chr(int(x)) for x in codes.split())
    await respond(e, ui.card("🔢 ASCII коды",
                             ui.rows(("Текст", f"_{text}_"),
                                     ("Коды", f"`{codes}`"),
                                     ("Обратно", f"`{back}`"))))
    db.bump_stat('cmds')

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

@client.on(events.NewMessage(pattern=r'!chatinfo$', func=owner_filter))
async def chatinfo_cmd(e):

    chat = await e.get_chat()
    name = getattr(chat, 'title', None) or f"{getattr(chat, 'first_name', '')} {getattr(chat, 'last_name', '')}".strip()
    uname = getattr(chat, 'username', None)
    members = getattr(chat, 'participants_count', None)
    lines = [
        ui.kv("Название", f"**{name}**", "📛"),
        ui.kv("ID", f"`{e.chat_id}`", "🆔"),
        ui.kv("Username", f"@{uname}" if uname else "нет", "🔖"),
        ui.kv("Тип", f"`{type(chat).__name__}`", "👥"),
    ]
    if members:
        lines.append(ui.kv("Участников", f"`{members}`", "👤"))
    await respond(e, ui.card("📊 Чат", "\n".join(lines)))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!members$', func=owner_filter))
async def members_cmd(e):

    try:
        p = await client.get_participants(e.chat_id)
        bots = sum(1 for x in p if x.bot)
        await respond(e, ui.card("👥 Участники",
                                 ui.rows(("Всего", f"`{len(p)}`"),
                                         ("Людей", f"`{len(p) - bots}`", "👤"),
                                         ("Ботов", f"`{bots}`", "🤖"))))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Участники"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!admins$', func=owner_filter))
async def admins_cmd(e):

    try:
        admins = await client.get_participants(e.chat_id, filter=ChannelParticipantsAdmins())
        lines = []
        for a in admins[:25]:
            name = f"{a.first_name or ''} {a.last_name or ''}".strip()
            lines.append(f"• {name} — {'@' + a.username if a.username else '`' + str(a.id) + '`'}")
        await respond(e, ui.card(f"👑 Администраторы ({len(admins)})", "\n".join(lines) or "—"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Администраторы"))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!top(?:\s+(\d+))?$', func=owner_filter))
async def top_cmd(e):

    limit = int(e.pattern_match.group(1) or 200)
    await respond(e, ui.card("⏳ Топ активных", "анализирую сообщения…"))
    cnt, names = defaultdict(int), {}
    async for msg in client.iter_messages(e.chat_id, limit=limit):
        if msg.sender_id:
            cnt[msg.sender_id] += 1
            if msg.sender_id not in names:
                s = await msg.get_sender()
                if s:
                    n = f"{getattr(s, 'first_name', '') or ''} {getattr(s, 'last_name', '') or ''}".strip()
                    names[msg.sender_id] = n or str(msg.sender_id)
    top = sorted(cnt.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    lines = [f"🏆 **Топ активных** (из {limit} сообщ.)\n"]
    for i, (uid, c) in enumerate(top):
        lines.append(f"{medals[i]} {names.get(uid, uid)} — `{c}` сообщ.")
    await respond(e, ui.card("🏆 Топ активных",
                             f"из `{limit}` сообщ.\n\n" + "\n".join(lines[1:])))
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!bots$', func=owner_filter))
async def bots_cmd(e):

    try:
        bots = await client.get_participants(e.chat_id, filter=ChannelParticipantsBots())
        lines = []
        for b in bots[:20]:
            lines.append(f"• @{b.username or b.id}")
        await respond(e, ui.card(f"🤖 Боты в чате ({len(bots)})", "\n".join(lines) or "—"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Боты"))
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

@client.on(events.NewMessage(pattern=r'!ytshow\s+(.+)', func=owner_filter))
async def ytshow_cmd(e):

    raw = e.pattern_match.group(1).strip()
    parts = raw.rsplit(None, 1)
    if len(parts) == 2 and parts[1].isdigit():
        url = parts[0]
        height = int(parts[1])
    else:
        url = raw
        height = None

    status_msg = await respond(e, ui.card("⏳ Загрузка",
                                          ui.rows(("Качество", f"{'авто' if not height else f'{height}p'}"))))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    filename = await run_download(edit_fn, url, mode='video', quality=height, timeout=600)
    if filename:
        await send_and_clean(edit_fn, client, e.chat_id, filename, f"🎬 YouTube: {url}")
    db.bump_stat('cmds')

@client.on(events.NewMessage(pattern=r'!dl\s+(.+)', func=owner_filter))
async def dl_cmd(e):

    url = e.pattern_match.group(1).strip()

    status_msg = await respond(e, ui.card("⏳ Загрузка", "запрос отправлен…"))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    try:
        filename = await run_download(edit_fn, url, mode='video', timeout=600)
        if filename:
            await send_and_clean(edit_fn, client, e.chat_id, filename)
    except Exception as ex:
        try:
            await status_msg.edit(ui.err(f"{ex}", title="Загрузка"))
        except Exception:
            pass
        _log.error(f"dl error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!playlist\s+(.+?)(?:\s+(\d+)(?:-(\d+))?)?$', func=owner_filter))
async def playlist_cmd(e):

    g = e.pattern_match
    url = g.group(1).strip()
    start_num = None
    end_num = None
    if g.group(2):
        start_num = int(g.group(2))
        end_num = int(g.group(3)) if g.group(3) else start_num

    msg = await respond(e, ui.card("⏳ Плейлист", "получаю информацию…"))
    try:
        import yt_dlp

        def _get_playlist_info():
            opts = {
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'nocheckcertificate': True,
                'cachedir': False,
                'extract_flat': True,
                'force_generic_extractor': False,
                'cookiefile': os.path.abspath(os.path.join(os.path.dirname(__file__), 'cookies.txt')),
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                title = info.get('title', '')
                entries = info.get('entries', [])
                video_urls = []
                for entry in entries:
                    if entry and entry.get('url'):
                        video_urls.append(entry['url'])
                    elif entry and entry.get('webpage_url'):
                        video_urls.append(entry['webpage_url'])
                    elif entry and entry.get('id'):
                        video_urls.append(f'https://youtube.com/watch?v={entry["id"]}')
                return title, video_urls

        title, video_urls = await asyncio.to_thread(_get_playlist_info)

        if not video_urls:
            await msg.edit(ui.err("Плейлист пуст или недоступен.", title="Плейлист"))
            return

        total = len(video_urls)
        if start_num:
            s = max(1, start_num)
            e_idx = min(total, end_num) if end_num else min(total, s)
            selected = video_urls[s - 1:e_idx]
        else:
            selected = video_urls[:50]

        await msg.edit(ui.card("📋 Плейлист",
                               ui.rows(("Название", f"**{title or '?'}**"),
                                       ("Видео", f"`{len(selected)}/{total}`")),
                               footer="⏳ Начинаю загрузку…"))

        for i, video_url in enumerate(selected, 1):
            vid_msg = await respond(e, ui.card("⏳ Загрузка видео",
                                               ui.rows(("Позиция", f"`{i}/{len(selected)}`"))))

            async def edit_vid(text, vm=vid_msg):
                try:
                    await vm.edit(text)
                except Exception:
                    pass

            filename = await run_download(edit_vid, video_url, mode='video', timeout=600)
            if filename:
                await send_and_clean(edit_vid, client, e.chat_id, filename, f"🎬 [{i}/{len(selected)}]")
                await asyncio.sleep(2)

        await respond(e, ui.ok(ui.kv("Загружено", f"**{len(selected)}/{total}** видео"),
                               title="Плейлист загружен"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Плейлист"))
        _log.error(f"playlist error: {ex}")
    db.bump_stat('cmds')


async def safe_edit(msg, text):
    try:
        await msg.edit(text)
    except Exception:
        pass


@client.on(events.NewMessage(pattern=r'!audio\s+(.+)', func=owner_filter))
async def audio_cmd(e):

    url = e.pattern_match.group(1).strip()

    status_msg = await respond(e, ui.card("⏳ Загрузка аудио", "запрос отправлен…"))

    async def edit_fn(text):
        try:
            await status_msg.edit(text)
        except Exception:
            pass

    try:
        filename = await run_download(edit_fn, url, mode='audio', timeout=600)
        if filename:
            await send_and_clean(edit_fn, client, e.chat_id, filename, f"🎵 Аудио")
    except Exception as ex:
        await safe_edit(status_msg, ui.err(f"{ex}", title="Аудио"))
        _log.error(f"audio error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!sub\s+(.+?)(?:\s+(\w{2}))?$', func=owner_filter))
async def sub_cmd(e):

    g = e.pattern_match
    url = g.group(1).strip()
    lang = (g.group(2) or 'ru').lower()

    msg = await respond(e, ui.card("⏳ Субтитры", ui.rows(("Язык", f"`{lang}`"))))
    try:
        def _get_captions():
            out_dir = os.path.join(MEDIA_DIR, 'subtmp')
            os.makedirs(out_dir, exist_ok=True)
            opts = {
                'quiet': True,
                'no_warnings': True,
                'noplaylist': True,
                'nocheckcertificate': True,
                'cachedir': False,
                'cookiefile': os.path.abspath(os.path.join(os.path.dirname(__file__), 'cookies.txt')),
                'writesubtitles': True,
                'subtitleslangs': [lang],
                'subtitlesformat': 'srt',
                'skip_download': True,
                'outtmpl': os.path.join(out_dir, '%(id)s'),
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
                subs = info.get('subtitles') or info.get('requested_subtitles') or {}
                if lang not in subs:
                    for code in subs:
                        if code.startswith(lang):
                            lang_found = code
                            break
                    else:
                        return None
                else:
                    lang_found = lang
                sub_data = subs[lang_found]
                sub_url = None
                if isinstance(sub_data, list):
                    for entry in sub_data:
                        if entry.get('ext') == 'srt' and entry.get('url'):
                            sub_url = entry['url']
                            break
                    if not sub_url and sub_data and sub_data[0].get('url'):
                        sub_url = sub_data[0]['url']
                elif isinstance(sub_data, dict):
                    sub_url = sub_data.get('url')
                if sub_url:
                    from urllib.request import Request, urlopen
                    req = Request(sub_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urlopen(req, timeout=30) as resp:
                        return resp.read().decode('utf-8', 'ignore')
                return None

        srt_content = await asyncio.to_thread(_get_captions)

        if srt_content:
            sub_path = os.path.join(MEDIA_DIR, f'sub_{lang}.srt')
            with open(sub_path, 'w', encoding='utf-8') as f:
                f.write(srt_content)
            await msg.edit(ui.card("📤 Субтитры", ui.rows(("Язык", f"`{lang}`"),
                                                          ("Статус", "отправляю…"))))
            await client.send_file(e.chat_id, sub_path, caption=f"📝 Субтитры ({lang})")
            await asyncio.sleep(3)
            try:
                os.remove(sub_path)
            except OSError:
                pass
            await msg.edit(ui.ok(ui.kv("Язык", f"`{lang}`"), title="Субтитры отправлены"))
        else:
            await msg.edit(ui.err(f"Субтитры ({lang}) не найдены для этого видео.",
                                  title="Субтитры"))
    except Exception as ex:
        await msg.edit(ui.err(f"{ex}", title="Субтитры"))
        logger.error(f"sub error: {ex}")
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!watch\s+(on|off)$', func=owner_filter))
async def watch_cmd(e):
    global _watch_task
    arg = e.pattern_match.group(1)
    if arg == 'on':
        if _watch_task and not _watch_task.done():
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

        async def monitor():
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

        _watch_task = asyncio.create_task(monitor())
        await respond(e, ui.card("👁️ Мониторинг сессий",
                                 ui.rows(("Статус", "включён ✅"),
                                         ("Проверка", "каждые 5 мин")),
                                 footer="Уведомления о новых входах — во внешний бот."))
    else:
        if _watch_task and not _watch_task.done():
            _watch_task.cancel()
            _watch_task = None
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
    global _protect_task
    arg = e.pattern_match.group(1) if e.pattern_match else None
    logger.info(f"[PROTECT] вход: sender={e.sender_id} text={repr((e.raw_text or '').strip())} cmd={arg}")
    if not arg:
        if _protect_task and not _protect_task.done():
            await respond(e, ui.card("🔒 Защита", ui.rows(("Статус", "включён ✅")),
                                     hint="`!protect off` — отключить"))
        else:
            await respond(e, ui.card("🔓 Защита", ui.rows(("Статус", "выключен ❌")),
                                     hint="`!protect on` — включить"))
        db.bump_stat('cmds')
        return
    if arg == 'on':
        if _protect_task and not _protect_task.done():
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

        async def monitor():
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

        _protect_task = asyncio.create_task(monitor())
    else:
        if _protect_task and not _protect_task.done():
            _protect_task.cancel()
            _protect_task = None
        db.clear_protected_chats()
        await respond(e, ui.card("🔓 Защита выключена",
                                 ui.rows(("Статус", "выключен ❌"),
                                         ("Чаты", "список очищен"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(func=lambda e: e.is_private))
async def private_handler(event):
    global reply_cooldown
    sender = await event.get_sender()
    if not sender:
        return

    _log.info(f"📩 [ЛС] От {sender.first_name} (id:{sender.id})")
    if event.reply_to_msg_id:
        _log.info(f"↩️ Ответ на сообщение ID:{event.reply_to_msg_id}")

    uid = sender.id
    now = time.time()

    if len(reply_cooldown) > MAX_COOLDOWN_ENTRIES:
        cutoff = now - 3600
        reply_cooldown = {k: v for k, v in reply_cooldown.items() if v > cutoff}

    if state.mute_enabled:
        logger.info(f"🔇 Mute — игнорирую {uid}")
        return

    if state.lock_enabled:
        try:
            me = await client.get_me()
            if uid == me.id:
                pass
            else:
                is_contact = db.get_saved(f'_lock_cache_{uid}')
                if is_contact is None:
                    is_contact = '0'
                    try:
                        contact = await client.get_entity(uid)
                        if getattr(contact, 'contact', False):
                            is_contact = '1'
                    except Exception:
                        pass
                    if is_contact == '0':
                        try:
                            common = await client.get_common_chats(uid)
                            if common:
                                is_contact = '1'
                        except Exception:
                            pass
                    db.set_saved(f'_lock_cache_{uid}', is_contact)
                if is_contact == '0':
                    logger.info(f"🔒 Lock: {uid} не контакт и нет общих чатов — игнорирую")
                    return
        except Exception as ex:
            logger.warning(f"Lock check error for {uid}: {ex}")

    silent_mode = state.silent_enabled

    if state.readreceipt_enabled:
        try:
            await client.send_read_acknowledge(event.chat_id, event.message)
        except Exception:
            pass

    if event.reply_to_msg_id and not event.out:
        try:
            reply_msg = await event.get_reply_message()
            if reply_msg and reply_msg.sender_id:
                raw_text = event.raw_text.strip()
                lines = raw_text.split('\n', 1)
                cmd_part = lines[0].strip().lower()
                custom_reply = lines[1].strip() if len(lines) > 1 else None
                if cmd_part in RP_COMMANDS:
                    target_entity = await client.get_entity(reply_msg.sender_id)
                    target_name = target_entity.first_name or "пользователь"
                    user_name = sender.first_name or "Кто-то"
                    action_text = format_rp_action(cmd_part, user_name, target_name)
                    reply_text = custom_reply or get_rp_reply(cmd_part)
                    if state.typing_enabled:
                        async with client.action(event.chat_id, 'typing'):
                            await asyncio.sleep(0.8)
                    sent = await safe_flood(lambda: event.reply(f"{action_text}\n{reply_text}"))
                    logger.info(f"✅ RP (in): {user_name} -> {target_name} ({cmd_part})")
                    if state.ghost_mode or state.shadow_enabled:
                        asyncio.create_task(shadow_delete_msg(sent))
                    if state.autodel_enabled:
                        asyncio.create_task(shadow_delete_msg(sent, state.autodel_delay))
                    db.bump_stat('cmds')
                    return
        except Exception as e:
            logger.error(f"RP error (in): {e}")

    if state.afk_start_time and now - reply_cooldown.get(f'afk_{uid}', 0) > 60:
        if silent_mode:
            logger.info(f"🔇 Silent — AFK ответ скрыт для {uid}")
        else:
            dur = fmt_time(now - state.afk_start_time)
            reason_part = f"\n📝 _{state.afk_reason}_" if state.afk_reason else ""
            reply_cooldown[f'afk_{uid}'] = now
            if state.typing_enabled:
                async with client.action(event.chat_id, 'typing'):
                    await asyncio.sleep(0.8)
            sent = await safe_flood(lambda: event.reply(f"😴 Хозяин AFK уже **{dur}**{reason_part}"))
            if state.ghost_mode or state.shadow_enabled:
                asyncio.create_task(shadow_delete_msg(sent))
            if state.autodel_enabled:
                asyncio.create_task(shadow_delete_msg(sent, state.autodel_delay))

    if state.auto_reply_enabled and now - reply_cooldown.get(uid, 0) > 10:
        reply_text = db.get_reply_text(uid)
        if reply_text is None:
            reply_text = db.get_default_reply()
        if reply_text is None:
            reply_text = state.auto_reply_text if state.auto_reply_text else None
        if reply_text and not silent_mode:
            reply_cooldown[uid] = now
            if state.reply_delay:
                await asyncio.sleep(state.reply_delay)
            if state.typing_enabled:
                async with client.action(event.chat_id, 'typing'):
                    await asyncio.sleep(0.8)
            sent = await safe_flood(lambda: event.reply(reply_text))
            if state.ghost_mode or state.shadow_enabled:
                asyncio.create_task(shadow_delete_msg(sent))
            if state.autodel_enabled:
                asyncio.create_task(shadow_delete_msg(sent, state.autodel_delay))
        elif silent_mode:
            logger.info(f"🔇 Silent — автоответ скрыт для {uid}")

async def safe_flood(coro_factory, max_retries=3):
    for attempt in range(max_retries):
        try:
            return await coro_factory()
        except FloodWaitError as e:
            logger.warning(f"FloodWait: {e.seconds}s, попытка {attempt + 1}/{max_retries}")
            try:
                await notify.error_alert("FloodWait", f"{e.seconds} секунд")
            except Exception:
                pass
            if attempt < max_retries - 1:
                await asyncio.sleep(e.seconds + 1)
            else:
                raise
        except Exception as ex:
            try:
                await notify.error_alert("Ошибка отправки", str(ex))
            except Exception:
                pass
            raise


async def shadow_delete_msg(msg, delay=None):
    d = delay or state.shadow_delay or 5
    await asyncio.sleep(d)
    try:
        suppress_pm_delete(getattr(msg, 'id', 0))
        await msg.delete()
    except Exception:
        pass


# ── PM cache + edit/delete notify ──

@client.on(events.NewMessage(incoming=True))
async def pm_cache_incoming(e):
    if not e.is_private:
        return
    try:
        msg = e.message
        text = msg.text or msg.message or ''
        has_media = bool(msg.media)
        media_type = None
        if getattr(msg, 'photo', None):
            media_type = 'photo'
        elif getattr(msg, 'video_note', None):
            media_type = 'video_note'
        elif getattr(msg, 'video', None):
            media_type = 'video'
        elif getattr(msg, 'document', None):
            media_type = 'document'
        elif getattr(msg, 'voice', None):
            media_type = 'voice'
        elif getattr(msg, 'audio', None):
            media_type = 'audio'
        elif getattr(msg, 'sticker', None):
            media_type = 'sticker'
        elif getattr(msg, 'gif', None):
            media_type = 'gif'
        sender = await e.get_sender()
        sender_name = f"{getattr(sender, 'first_name', '') or ''} {getattr(sender, 'last_name', '') or ''}".strip() or str(e.sender_id)
        db.cache_pm_message(e.chat_id, e.id, e.sender_id, sender_name, text, has_media, media_type)
        db.clean_pm_cache()
    except Exception as ex:
        logger.debug(f"pm_cache_incoming err: {ex}")


@client.on(events.NewMessage(outgoing=True))
async def pm_cache_outgoing(e):
    if not e.is_private:
        return
    try:
        msg = e.message
        text = msg.text or msg.message or ''
        has_media = bool(msg.media)
        media_type = None
        if getattr(msg, 'photo', None):
            media_type = 'photo'
        elif getattr(msg, 'video_note', None):
            media_type = 'video_note'
        elif getattr(msg, 'video', None):
            media_type = 'video'
        elif getattr(msg, 'document', None):
            media_type = 'document'
        elif getattr(msg, 'voice', None):
            media_type = 'voice'
        elif getattr(msg, 'audio', None):
            media_type = 'audio'
        elif getattr(msg, 'sticker', None):
            media_type = 'sticker'
        elif getattr(msg, 'gif', None):
            media_type = 'gif'
        me = await client.get_me()
        sender_name = f"{getattr(me, 'first_name', '') or ''} {getattr(me, 'last_name', '') or ''}".strip() or 'Ты'
        db.cache_pm_message(e.chat_id, e.id, me.id, sender_name, text, has_media, media_type)
        db.clean_pm_cache()
    except Exception as ex:
        logger.debug(f"pm_cache_outgoing err: {ex}")


@client.on(events.MessageEdited())
async def pm_edited_handler(e):
    try:
        if not e.is_private:
            return
        # Свои исходящие сообщения не оповещаем — фича следит за тем,
        # что делает собеседник (правки/удаления в ЛС), а не за своими.
        if getattr(e.message, 'out', False):
            logger.info(f"[PM-EDIT] исходящее (своё) — пропуск msg={e.id}")
            return
        new_text = e.message.text or e.message.message or ''
        old = db.get_pm_cached(e.chat_id, e.id)
        logger.info(f"[PM-EDIT] chat={e.chat_id} msg_id={e.id} new_empty={not new_text.strip()} has_old={bool(old)}")

        # 1) Текст не изменился — это реакции/закреп/перепин, а не правка.
        #    Раньше эта проверка стояла ПОСЛЕ проверки на пустой текст, из-за чего
        #    реакция на фото/стикере (без подписи) считалась удалением.
        if old is not None and (old['text'] or '') == (new_text or ''):
            logger.info(f"[PM-EDIT] текст не менялся — пропуск (реакция/пин)")
            return

        # 2) Сообщения нет в кэше (пришло до старта бота) — сравнивать не с чем,
        #    отличить правку от реакции нельзя → молчим вместо ложного алерта.
        if old is None:
            logger.info(f"[PM-EDIT] нет базы в кэше — пропуск")
            return

        # 3) Текст очистили — трактуем как удаление (дедуп с raw UpdateDeleteMessages).
        if not new_text.strip():
            username = None
            if old.get('sender_id'):
                try:
                    s = await client.get_entity(old['sender_id'])
                    username = getattr(s, 'username', None)
                except Exception:
                    pass
            await _enqueue_pm_delete(
                e.chat_id, e.id, old=old, username=username,
                event_sender_id=e.sender_id if e.sender_id else 0, source="edit-empty"
            )
            return

        sender = None
        try:
            sender = await e.get_sender()
        except Exception:
            sender = None

        username = getattr(sender, 'username', None) if sender else None
        if sender:
            sender_name = f"{getattr(sender, 'first_name', '') or ''} {getattr(sender, 'last_name', '') or ''}".strip()
        else:
            sender_name = old['sender_name'] or ''

        if not sender_name:
            sender_name = username or str(e.sender_id)

        # 4) Настоящая правка текста.
        await notify.pm_edited(e.chat_id, e.sender_id, sender_name, old['text'] or '', new_text, username=username)

        db.cache_pm_message(e.chat_id, e.id, e.sender_id, sender_name,
                             new_text, bool(old['has_media']), old['media_type'])
    except Exception as ex:
        logger.warning(f"pm_edited_handler err: {ex}", exc_info=True)





# ── Online watch ──

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
                uname = r['target_username']
                tgt = f"@{uname}" if uname else f"id `{r['target_id']}`"
                lines.append(f"• **{tgt}** [`{r['target_id']}`] | last: `{r['last_status']}`\n  _{r['message'][:40]}{'...' if len(r['message'])>40 else ''}_")
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
            await respond(e, ui.ok(ui.kv("Цель", f"**{entity_label(ent, target_raw.lstrip('@'))}**"),
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
            await respond(e, ui.card(
                "✅ ONWATCH добавлен",
                ui.rows(
                    ("Цель", f"**{entity_label(ent, target_raw.lstrip('@'))}**"),
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
    while True:
        try:
            watches = db.get_online_watches()
            if not watches:
                await asyncio.sleep(40)
                continue
            for w in watches:
                try:
                    ent = await client.get_entity(w['target_id'])
                    status = ent.status
                    cur = 'unknown'
                    if isinstance(status, UserStatusOnline):
                        cur = 'online'
                    elif isinstance(status, UserStatusOffline):
                        cur = 'offline'
                    elif isinstance(status, UserStatusRecently):
                        cur = 'recently'
                    elif isinstance(status, UserStatusLastWeek):
                        cur = 'last_week'
                    elif isinstance(status, UserStatusLastMonth):
                        cur = 'last_month'
                    else:
                        cur = 'unknown'
                    prev = w['last_status'] or 'unknown'
                    if prev != 'online' and cur == 'online':
                        try:
                            await client.send_message(w['target_id'], w['message'])
                        except Exception as ex:
                            logger.warning(f"[ONWATCH] Не удалось отправить сообщение {w['target_id']}: {ex}")
                            try:
                                await notify.error_alert("ONWATCH", f"Не удалось отправить сообщение @{w['target_username']} [{w['target_id']}]: {ex}")
                            except Exception:
                                pass
                            db.update_online_watch_status(w['id'], cur)
                            continue
                        try:
                            await notify.online_watch(w['target_username'] or str(w['target_id']), w['target_id'], w['message'])
                        except Exception:
                            pass
                        db.update_online_watch_status(w['id'], cur)
                        continue
                    if prev == 'online' and cur != 'online':
                        db.update_online_watch_status(w['id'], cur)
                        continue
                    if prev != cur:
                        db.update_online_watch_status(w['id'], cur)
                        continue
                except Exception as ex:
                    logger.debug(f"[ONWATCH] skip {w.get('target_id')}: {ex}")
                    continue
        except Exception as ex:
            logger.warning(f"[ONWATCH] loop err: {ex}")
        await asyncio.sleep(35)

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

# ── Внешний бот (@nemusechkabot): !menu и приём нажатий инлайн-кнопок ──
# Связка: юзербот шлёт боту сообщения с кнопками, сам опрашивает getUpdates
# (см. notifier.poll_updates) и редактирует сообщения бота по нажатиям.

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


async def handle_bot_callback(q):
    """callback_query от внешнего бота: пульт (c:*) и справка (h:*)."""
    data = q.get('data') or ''
    qid = q.get('id')
    uid = (q.get('from') or {}).get('id')
    # сообщение, на котором висит кнопка: если оно отправлено от имени аккаунта
    # (Telegram Business) — в нём есть business_connection_id, он обязателен для edit
    msg = q.get('message') or {}
    mid = msg.get('message_id')
    biz = msg.get('business_connection_id')
    logger.info(f"[BOT-CB] нажатие: data={data!r} from={uid} msg={mid} biz={biz}")

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
        await notify.edit(mid, ui.to_html(card), buttons=[[dict(BACK_BTN)]],
                          business_connection_id=biz)
        return

    # пульт: переключение режимов → перерисовать меню с новым состоянием
    if data.startswith('c:'):
        last = await _pult_toggle(data[2:])
        text, buttons = help_data.menu_view(last_action=last)
        await notify.edit(mid, ui.to_html(text), buttons=buttons,
                          business_connection_id=biz)
        return

    # справка: навигация по разделам (h:back / h:all / h:<раздел>)
    text, buttons = help_data.help_view(data)
    await notify.edit(mid, ui.to_html(text), buttons=buttons,
                      business_connection_id=biz)


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


# ── Внешний бот: любое сообщение владельца в ЛС с ботом → меню с кнопками ──

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

if __name__ == "__main__":
    import glob as _glob
    import threading
    from telethon.errors import AuthKeyDuplicatedError

    print("🚀 Запуск UserBot...")
    os.makedirs(MEDIA_DIR, exist_ok=True)
    threading.Thread(target=run_web, daemon=True).start()

    _base = os.path.dirname(os.path.abspath(__file__))
    _lock = os.path.join(_base, 'bot.lock')
    if os.path.exists(_lock):
        print("⚠️ Найден bot.lock — возможно, другой экземпляр бота уже работает с той же сессией.")
        print("   Это частая причина AuthKeyDuplicatedError. Останови второй экземпляр, затем удали bot.lock.")
    with open(_lock, 'w') as _f:
        _f.write(str(os.getpid()))
    try:
        os.remove(_lock)
    except OSError:
        pass

    try:
        client.start()
    except AuthKeyDuplicatedError:
        print("❌ AuthKeyDuplicatedError: сессия использовалась под двумя разными IP одновременно и отозвана.")
        for _f in _glob.glob(os.path.join(_base, 'my_userbot.session*')):
            try:
                os.remove(_f)
                print(f"   Удалена устаревшая сессия: {_f}")
            except OSError:
                pass
        try:
            client.session.close()
        except Exception:
            pass
        print("ℹ️ Сессия сброшена. Будет предложена новая авторизация: введи телефон и код.")
        print("   Совет: останови второй экземпляр бота (локальный или на Render), иначе — снова блок.")
        print("   Для удельной смены окружения задай STRING_SESSION в .env.")
        client.start()
    try:
        client.loop.create_task(online_watcher())
        logger.info("[TASK] online_watcher запущен")
    except Exception as e:
        logger.warning(f"[TASK] online_watcher не запущен: {e}")

    try:
        if not _pm_delete_worker_started:
            globals()['_pm_delete_worker_started'] = True
            client.loop.create_task(_pm_delete_worker())
            logger.info("[TASK] pm_delete_worker запущен")
    except Exception as e:
        logger.warning(f"[TASK] pm_delete_worker не запущен: {e}")

    # приём нажатий инлайн-кнопок внешнего бота (getUpdates long-poll)
    try:
        saved_biz = db.get_saved('_biz_conn')
        if saved_biz:
            notify.business_connection_id = saved_biz
            logger.info(f"[TASK] business_connection_id восстановлен из БД: {saved_biz}")
    except Exception as e:
        logger.warning(f"[TASK] business_connection не восстановлен: {e}")

    try:
        if notify.enabled:
            client.loop.create_task(
                notify.poll_updates(handle_bot_callback, message_handler=handle_bot_message))
            logger.info("[TASK] external_bot_poll запущен")
        else:
            logger.info("[TASK] external_bot_poll пропущен: NOTIFY_* не заданы")
    except Exception as e:
        logger.warning(f"[TASK] external_bot_poll не запущен: {e}")

    try:
        # Базовая линия: первая сверка только фиксирует наличие сообщений,
        # чтобы не завалить уведомлениями за время оффлайна
        db.reset_pm_verified()
        client.loop.create_task(pm_presence_watcher())
        logger.info("[TASK] pm_presence_watcher запущен")
    except Exception as e:
        logger.warning(f"[TASK] pm_presence_watcher не запущен: {e}")

    # Принудительно подгружаем диалоги в память клиента, чтобы сервер присылал сырые апдейты
    async def preload_dialogs():
        try:
            async for dialog in client.iter_dialogs(limit=50):
                pass
            logger.info("[INIT] Диалоги подгружены в память для отслеживания сырых апдейтов.")
        except Exception as ex:
            logger.warning(f"[INIT] Ошибка подгрузки диалогов: {ex}")

    client.loop.create_task(preload_dialogs())
    print("✅ Бот запущен!")
    client.run_until_disconnected()
