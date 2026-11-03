from telethon import TelegramClient
from telethon.sessions import StringSession
from config import API_ID, API_HASH, STRING_SESSION

# sequential_updates=False (значение по умолчанию — фиксируем явно): обновления
# разбираются параллельно, поэтому медленный send_message в одном обработчике
# не задерживает реакцию караула на следующий typing/статус.
# catch_up=False — осознанный выбор для ONWATCH: пачка апдейтов, накопленных
# за оффлайн, пришла бы с опозданием и переписала бы статус задним числом,
# а пропущенную сессию караул определяет по was_online (flicker).
_KW = dict(sequential_updates=False, catch_up=False)

if STRING_SESSION:
    client = TelegramClient(StringSession(STRING_SESSION), int(API_ID), API_HASH, **_KW)
else:
    client = TelegramClient('my_userbot', int(API_ID), API_HASH, **_KW)
