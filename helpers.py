# helpers.py — общие функции, используемые и main.py, и плагинами.
# Единая точка: сущности Telegram, карточка статуса, защита от FloodWait,
# отложенное удаление сообщений, подавление уведомлений о своих удалениях.

import asyncio
import re

from telethon.errors import FloodWaitError

from client import client
from config import OWNER_ID, BOT_LANG, logger
from core import db, state
from notifier import notify
import ui

_log = logger
logger = logger


def t(ru, en):
    """Локализация: t('Загрузка', 'Download'). Строки — по ключу BOT_LANG в .env.
    По умолчанию ru; при BOT_LANG=en возвращается английский вариант."""
    return ru if BOT_LANG != 'en' else en


# ── Удаления, которые сделал сам юзербот (ghost/autodel) — о них уведомлять не надо ──
_pm_suppress_delete = set()


def suppress_pm_delete(msg_id):
    try:
        _pm_suppress_delete.add(int(msg_id))
    except Exception:
        return
    if len(_pm_suppress_delete) > 5000:
        _pm_suppress_delete.clear()


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


async def build_status_card():
    me = await client.get_me()
    dialogs = await client.get_dialogs()
    s = db.all_stats()
    afk_status = f"вкл — {state.afk_reason or 'без причины'}" if state.afk_start_time else "выкл ❌"

    def _yn(v):
        return "вкл ✅" if v else "выкл ❌"

    return ui.card(
        t("📊 Статус UserBot", "📊 Bot Status"),
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
        hint=t("`!help` — справка по командам", "`!help` — command reference"),
    )


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
