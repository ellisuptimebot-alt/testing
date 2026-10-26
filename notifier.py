import asyncio
import json

import aiohttp
from config import (
    NOTIFY_BOT_TOKEN,
    NOTIFY_CHAT_ID,
    logger,
    NOTIFY_ONLINE_WATCH,
    NOTIFY_SESSIONS,
    NOTIFY_PROTECT,
    NOTIFY_PM_DELETE,
    NOTIFY_PM_EDIT,
    NOTIFY_ERRORS,
)


class Notifier:
    def __init__(self):
        # Telegram Business: id соединения «бот подключён к аккаунту».
        # Нужен для send/edit сообщений от имени аккаунта (иначе Telegram
        # не даст редактировать сообщения, отправленные через бизнес-соединение).
        self.business_connection_id = None
        # хук main.py — сохранять соединение в БД (после рестарта Telegram
        # business_connection не переотправляет, апдейт приходит только при изменении)
        self.on_business_connection = None
        self._offset = 0
        self.enabled = bool(NOTIFY_BOT_TOKEN and NOTIFY_CHAT_ID)
        if self.enabled:
            self._api_base = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}"
            self.url = f"{self._api_base}/sendMessage"
            try:
                self.chat_id = int(NOTIFY_CHAT_ID)
            except Exception:
                self.chat_id = NOTIFY_CHAT_ID
            logger.info("[NOTIFY] Внешний бот уведомлений включён")
        else:
            self._api_base = None
            self.url = None
            self.chat_id = None
            logger.warning("[NOTIFY] NOTIFY_BOT_TOKEN/NOTIFY_CHAT_ID не заданы. Уведомления через внешний бот отключены.")

    def _format_user(self, uid, username=None, name=None):
        try:
            uid_int = int(uid) if uid else 0
        except Exception:
            uid_int = 0

        uname = (username or '').strip().lstrip('@')
        if uname:
            if uid_int:
                return f"@{uname} [<code>{uid_int}</code>]"
            return f"@{uname}"

        disp = (name or '').strip()
        if not disp:
            disp = 'пользователь'

        if uid_int:
            return f'<a href="tg://user?id={uid_int}">{disp}</a> [<code>{uid_int}</code>]'
        return disp

    @staticmethod
    def _markup(buttons):
        """Инлайн-клавиатура: [[{text, callback_data}, ...], ...] → JSON."""
        if not buttons:
            return None
        return json.dumps({"inline_keyboard": buttons}, ensure_ascii=False)

    async def _api(self, method, payload=None, timeout=15):
        """Универсальный вызов Bot API. Возвращает parsed json или None."""
        if not self.enabled or not self._api_base:
            return None
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
                async with session.post(f"{self._api_base}/{method}", json=payload or {}) as resp:
                    data = await resp.json(content_type=None)
                    if not data.get('ok'):
                        desc = str(data.get('description') or '')
                        if 'message is not modified' not in desc:
                            logger.warning(f"[NOTIFY] {method}: HTTP {resp.status} {desc[:200]}")
                    return data
        except Exception as ex:
            logger.warning(f"[NOTIFY] {method}: {ex}")
            return None

    async def send(self, text: str, preview: bool = False, buttons=None,
                   business_connection_id=None, chat_id=None):
        if not self.enabled or not self.url or not self.chat_id:
            logger.warning(f"[NOTIFY] Не отправлено - disabled/enabled={self.enabled} url={bool(self.url)} chat_id={self.chat_id}")
            return False
        target = chat_id if chat_id is not None else self.chat_id
        payload = {
            "chat_id": target,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": not preview,
        }
        if business_connection_id:
            # отправка от имени аккаунта (Telegram Business)
            payload["business_connection_id"] = business_connection_id
        markup = self._markup(buttons)
        if markup:
            payload["reply_markup"] = markup
        logger.info(f"[NOTIFY] Отправка в {target} (len={len(text)}, buttons={bool(buttons)})")
        data = await self._api('sendMessage', payload)
        ok = bool(data and data.get('ok'))
        if ok:
            logger.info(f"[NOTIFY] Успешно отправлено")
        return ok

    async def edit(self, message_id: int, text: str, buttons=None, preview: bool = False,
                   business_connection_id=None):
        """Редактирует сообщение бота. Для сообщений, отправленных от имени
        аккаунта (Telegram Business), обязателен business_connection_id."""
        if not self.enabled or not self.chat_id:
            return False
        payload = {
            "chat_id": self.chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": not preview,
        }
        if business_connection_id:
            payload["business_connection_id"] = business_connection_id
        markup = self._markup(buttons)
        if markup:
            payload["reply_markup"] = markup
        data = await self._api('editMessageText', payload)
        return bool(data and data.get('ok'))

    async def answer_callback(self, query_id, text=None, alert=False):
        """Гасит крутилку у кнопки; text — всплывающее подтверждение."""
        payload = {"callback_query_id": query_id, "show_alert": bool(alert)}
        if text:
            payload["text"] = text
        data = await self._api('answerCallbackQuery', payload)
        return bool(data and data.get('ok'))

    async def _set_business_connection(self, bc):
        """Апдейт business_connection: бот подключён/отключён от аккаунта."""
        cid = bc.get('id')
        enabled = bc.get('is_enabled', True)
        user = (bc.get('user') or {}).get('username') or (bc.get('user') or {}).get('id')
        if cid and enabled:
            self.business_connection_id = cid
            logger.info(f"[NOTIFY] Business-подключение активно: {cid} (user={user})")
        else:
            self.business_connection_id = None
            logger.info(f"[NOTIFY] Business-подключение закрыто (user={user})")
        if self.on_business_connection:
            try:
                res = self.on_business_connection(bc)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as ex:
                logger.warning(f"[NOTIFY] on_business_connection: {ex}")

    async def _process_updates(self, result, handler, message_handler=None):
        """Разбор батча getUpdates: business_connection → своё хранилище,
        message → message_handler (ЛС владельца), callback_query → handler.
        Возвращает новый offset."""
        offset = self._offset
        for upd in result or []:
            try:
                offset = int(upd.get('update_id', offset)) + 1
            except Exception:
                continue
            bc = upd.get('business_connection')
            if bc:
                await self._set_business_connection(bc)
                continue
            mq = upd.get('message')
            if mq:
                if message_handler:
                    try:
                        await message_handler(mq)
                    except Exception as ex:
                        logger.warning(f"[NOTIFY] message error: {ex}", exc_info=True)
                continue
            cq = upd.get('callback_query')
            if not cq:
                continue
            try:
                await handler(cq)
            except Exception as ex:
                logger.warning(f"[NOTIFY] callback error: {ex}", exc_info=True)
        self._offset = offset
        return offset

    async def poll_updates(self, handler, message_handler=None, poll_timeout=50):
        """Long-poll getUpdates в процессе юзербота.

        Принимает нажатия инлайн-кнопок (callback_query → handler), обычные
        сообщения боту (message → message_handler, например меню по слову
        владельца) и business_connection (id соединения для send/edit
        от имени аккаунта).
        Работает только если этим токеном не опрашивает никто ещё.
        """
        if not self.enabled:
            logger.warning("[NOTIFY] Поллинг не запущен: NOTIFY_BOT_TOKEN/NOTIFY_CHAT_ID не заданы")
            return
        logger.info("[NOTIFY] Поллинг внешнего бота запущен (callback_query, message, business_connection)")
        while True:
            data = await self._api(
                'getUpdates',
                {"timeout": poll_timeout, "offset": self._offset,
                 "allowed_updates": ["callback_query", "message", "business_connection"]},
                timeout=poll_timeout + 10,
            )
            if data is None:
                await asyncio.sleep(3)
                continue
            await self._process_updates(data.get('result'), handler, message_handler)

    async def online_watch(self, username: str, target_id: int, sent_msg: str):
        if not NOTIFY_ONLINE_WATCH:
            return
        user_str = self._format_user(target_id, username=username)
        msg = (
            f"⚡ <b>ONLINE WATCH</b>\n\n"
            f"👤 Пользователь: {user_str}\n"
            f"🟢 Статус: <b>появился в сети</b>\n\n"
            f"✉️ <b>Сообщение отправлено таргету:</b>\n"
            f"<blockquote>{sent_msg}</blockquote>"
        )
        await self.send(msg)

    # Человекочитаемые типы медиа (ключи совпадают с media_type в pm_msg_cache)
    _MEDIA_LABELS = {
        'photo': 'Фото',
        'video': 'Видео',
        'video_note': 'Кружок',
        'voice': 'Голосовое сообщение',
        'audio': 'Аудио',
        'sticker': 'Стикер',
        'gif': 'GIF',
        'document': 'Файл',
    }

    async def pm_deleted(self, chat_id: int, sender_id: int, sender_name: str, text: str, has_media: bool, media_type: str, username: str = None, cached: bool = True):
        if not NOTIFY_PM_DELETE:
            return
        label = self._MEDIA_LABELS.get(media_type or '', 'Медиа')
        if text and text.strip():
            body = text.strip()
        elif not cached:
            # строки в кэше не было вовсе — сообщение пришло до старта бота
            body = "Сообщение не найдено в кэше юзербота. Вероятно, оно было отправлено до запуска бота или уже не закэшировано."
        elif has_media:
            # медиа без подписи — раньше тут ошибочно показывалось «не найдено в кэше»
            body = f"<i>{label} без подписи</i>"
        else:
            body = "<i>(пусто)</i>"
        user_str = self._format_user(sender_id, username=username, name=sender_name)
        media_info = ""
        if has_media:
            media_info = f"📎 {label}\n" if media_type else "📎 Медиа: да\n"
        msg = (
            f"🗑 <b>ЛС | СООБЩЕНИЕ УДАЛЕНО</b>\n\n"
            f"💬 Чат: <code>{chat_id}</code>\n"
            f"👤 От: {user_str}\n"
            f"{media_info}\n"
            f"<b>Текст:</b>\n<blockquote>{body}</blockquote>"
        )
        await self.send(msg)

    async def pm_edited(self, chat_id: int, sender_id: int, sender_name: str, old_text: str, new_text: str, username: str = None):
        if not NOTIFY_PM_EDIT:
            return
        if old_text and old_text.strip():
            old = old_text.strip()
        else:
            old = "Сообщение не найдено в кэше юзербота. Редактирование зафиксировано, но изначальный текст неизвестен."
        if new_text and new_text.strip():
            new = new_text.strip()
        else:
            new = "(пусто)"
        user_str = self._format_user(sender_id, username=username, name=sender_name)
        msg = (
            f"✏️ <b>ЛС | СООБЩЕНИЕ ИЗМЕНЕНО</b>\n\n"
            f"💬 Чат: <code>{chat_id}</code>\n"
            f"👤 От: {user_str}\n\n"
            f"<b>Было:</b>\n<blockquote>{old}</blockquote>\n\n"
            f"<b>Стало:</b>\n<blockquote>{new}</blockquote>"
        )
        await self.send(msg)

    async def session_alert(self, device: str, platform: str, ip: str, date: str):
        if not NOTIFY_SESSIONS:
            return
        msg = (
            f"🔐 <b>НОВАЯ СЕССИЯ</b>\n\n"
            f"📱 Устройство: <b>{device}</b>\n"
            f"💻 Платформа: <b>{platform}</b>\n"
            f"🌐 IP: <code>{ip}</code>\n"
            f"🕒 Дата: <code>{date}</code>"
        )
        await self.send(msg)

    async def protect_alert(self, chat_id: int):
        if not NOTIFY_PROTECT:
            return
        msg = (
            f"🛡 <b>ЗАЩИТА ЧАТОВ</b>\n\n"
            f"⚠️ Защищённый диалог пропал из списка диалогов\n"
            f"🆔 Chat ID: <code>{chat_id}</code>\n\n"
            f"<i>Возможно удалён, либо ты вышел из него</i>"
        )
        await self.send(msg)

    async def error_alert(self, title: str, details: str):
        if not NOTIFY_ERRORS:
            return
        det = details.strip()
        if len(det) > 1000:
            det = det[:1000] + "..."
        msg = (
            f"⚠️ <b>ОШИБКА ЮЗЕРБОТА</b>\n\n"
            f"📌 {title}\n\n"
            f"<code>{det}</code>"
        )
        await self.send(msg)


notify = Notifier()
