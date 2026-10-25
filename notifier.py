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
        self.enabled = bool(NOTIFY_BOT_TOKEN and NOTIFY_CHAT_ID)
        if self.enabled:
            self.url = f"https://api.telegram.org/bot{NOTIFY_BOT_TOKEN}/sendMessage"
            try:
                self.chat_id = int(NOTIFY_CHAT_ID)
            except Exception:
                self.chat_id = NOTIFY_CHAT_ID
            logger.info("[NOTIFY] Внешний бот уведомлений включён")
        else:
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

    async def send(self, text: str, preview: bool = False):
        if not self.enabled or not self.url or not self.chat_id:
            return False
        try:
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": not preview,
            }
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.post(self.url, json=payload) as resp:
                    if resp.status != 200:
                        data = await resp.text()
                        logger.debug(f"[NOTIFY] HTTP {resp.status}: {data[:150]}")
                    return resp.status == 200
        except Exception as e:
            logger.warning(f"[NOTIFY] Ошибка отправки: {e}")
            return False

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

    async def pm_deleted(self, chat_id: int, sender_id: int, sender_name: str, text: str, has_media: bool, media_type: str, username: str = None):
        if not NOTIFY_PM_DELETE:
            return
        if text and text.strip():
            body = text.strip()
        else:
            body = "Сообщение не найдено в кэше юзербота. Вероятно, оно было отправлено до запуска бота или уже не закэшировано."
        user_str = self._format_user(sender_id, username=username, name=sender_name)
        media_info = ""
        if has_media:
            media_info = f"📎 Медиа: да ({media_type})\n" if media_type else "📎 Медиа: да\n"
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
