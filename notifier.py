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
        msg = (
            f"⚡ <b>ONLINE WATCH</b>\n\n"
            f"👤 Пользователь: <b>@{username or target_id}</b> [<code>{target_id}</code>]\n"
            f"🟢 Статус: <b>появился в сети</b>\n\n"
            f"✉️ <b>Сообщение отправлено таргету:</b>\n"
            f"<i>{sent_msg}</i>"
        )
        await self.send(msg)

    async def pm_deleted(self, chat_id: int, sender_id: int, sender_name: str, text: str, has_media: bool, media_type: str):
        if not NOTIFY_PM_DELETE:
            return
        body = text.strip() if text and text.strip() else "(пусто/только медиа)"
        if len(body) > 900:
            body = body[:900] + "..."
        media_info = ""
        if has_media:
            media_info = f"📎 Медиа: да ({media_type})\n" if media_type else "📎 Медиа: да\n"
        msg = (
            f"🗑 <b>ЛС | СООБЩЕНИЕ УДАЛЕНО</b>\n\n"
            f"💬 Чат: <code>{chat_id}</code>\n"
            f"👤 От: <b>{sender_name}</b> [<code>{sender_id if sender_id else '0'}</code>]\n"
            f"{media_info}\n"
            f"<b>Текст:</b>\n{body}"
        )
        await self.send(msg)

    async def pm_edited(self, chat_id: int, sender_id: int, sender_name: str, old_text: str, new_text: str):
        if not NOTIFY_PM_EDIT:
            return
        old = old_text.strip() if old_text and old_text.strip() else "(неизвестно)"
        new = new_text.strip() if new_text and new_text.strip() else "(пусто)"
        if len(old) > 700:
            old = old[:700] + "..."
        if len(new) > 700:
            new = new[:700] + "..."
        msg = (
            f"✏️ <b>ЛС | СООБЩЕНИЕ ИЗМЕНЕНО</b>\n\n"
            f"💬 Чат: <code>{chat_id}</code>\n"
            f"👤 От: <b>{sender_name}</b> [<code>{sender_id if sender_id else '0'}</code>]\n\n"
            f"<b>Было:</b>\n{old}\n\n"
            f"<b>Стало:</b>\n{new}"
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
