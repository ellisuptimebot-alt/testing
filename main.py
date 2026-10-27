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
from telethon import utils as tgutils
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
import core
from core import (
    db, state, owner_filter, respond, _get_translator,
    command_cooldown, reply_cooldown,
)
import ui

from helpers import (
    suppress_pm_delete, _pm_suppress_delete, resolve_entity, entity_label,
    safe_flood, shadow_delete_msg, build_status_card,
)

set_max_file_size(MAX_FILE_SIZE_MB)


_log = logger


_pm_delete_queue = asyncio.Queue()


_pm_delete_worker_started = False


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


app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 UserBot работает 24/7!"


@app.route('/stats')
def stats_route():
    """Дашборд статистики: команды, активные треки, напоминания, размер media/."""
    s = db.all_stats()
    media_bytes = 0
    try:
        for root, _dirs, files in os.walk(MEDIA_DIR):
            for fn in files:
                try:
                    media_bytes += os.path.getsize(os.path.join(root, fn))
                except OSError:
                    pass
    except OSError:
        pass
    return {
        'status': 'ok',
        'uptime': state.uptime,
        'commands_total': s.get('cmds', 0),
        'stats': s,
        'onwatch_tracks': len(db.get_online_watches()),
        'pending_reminders': len(db.get_pending_reminders()),
        'protected_chats': len(db.get_protected_chat_ids()),
        'media_files_mb': round(media_bytes / 1024 / 1024, 1),
        'max_file_mb': MAX_FILE_SIZE_MB,
    }


def run_web():
    app.run(host=os.environ.get('HOST', '0.0.0.0'), port=PORT)


@client.on(events.NewMessage(func=lambda e: e.is_private))
async def private_handler(event):
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
        kept = {k: v for k, v in reply_cooldown.items() if v > cutoff}
        reply_cooldown.clear()
        reply_cooldown.update(kept)

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


# ── Плагины: регистрация всех команд из plugins/*.py ──
import plugins
plugins.load_all()

from plugins.watch import online_watcher
from plugins.menu import handle_bot_callback, handle_bot_message
from plugins.toolbox import reminder_worker


def spawn_task(name, coro):
    """Фоновая задача: исключение логируется, а не теряется молча."""
    task = client.loop.create_task(coro)

    def _done(t):
        if t.cancelled():
            return
        ex = t.exception()
        if ex is not None:
            logger.error(f"[TASK] {name} завершилась с ошибкой: {ex}", exc_info=ex)

    task.add_done_callback(_done)
    return task

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
        spawn_task('online_watcher', online_watcher())
        logger.info("[TASK] online_watcher запущен")
    except Exception as e:
        logger.warning(f"[TASK] online_watcher не запущен: {e}")

    # Напоминания: воркер БД + восстановление отложенных после рестарта
    try:
        spawn_task('reminder_worker', reminder_worker())
        pending = db.get_pending_reminders()
        if pending:
            logger.info(f"[TASK] восстановлено напоминаний из БД: {len(pending)}")
    except Exception as e:
        logger.warning(f"[TASK] reminder_worker не запущен: {e}")

    # Восстановление фоновых мониторингов, включённых до рестарта
    try:
        if db.get_state('watch_enabled') == 'True':
            from plugins.security import sessions_monitor
            core._watch_task = spawn_task('watch_restore', sessions_monitor())
            logger.info("[TASK] watch восстановлен из БД")
        if db.get_state('protect_enabled') == 'True':
            from plugins.security import protect_monitor
            core._protect_task = spawn_task('protect_restore', protect_monitor())
            logger.info("[TASK] protect восстановлен из БД")
    except Exception as e:
        logger.warning(f"[TASK] restore watch/protect: {e}")

    try:
        if not _pm_delete_worker_started:
            globals()['_pm_delete_worker_started'] = True
            spawn_task('pm_delete_worker', _pm_delete_worker())
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
            spawn_task(
                'external_bot_poll',
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
        spawn_task('pm_presence_watcher', pm_presence_watcher())
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

    spawn_task('preload_dialogs', preload_dialogs())
    print("✅ Бот запущен!")
    client.run_until_disconnected()
