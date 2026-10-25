import os
import logging

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')
PORT = int(os.environ.get('PORT', 8080))
STRING_SESSION = os.environ.get('STRING_SESSION')
OWNER_ID_RAW = os.environ.get('OWNER_ID')
MEDIA_DIR = os.environ.get('MEDIA_DIR', './media')

if not API_ID or not API_HASH:
    raise ValueError("API_ID и API_HASH обязательны. Укажите их в .env")
if not OWNER_ID_RAW:
    raise ValueError("OWNER_ID обязателен. Укажите Telegram ID владельца в .env")
OWNER_ID = int(OWNER_ID_RAW)

PROXY = os.environ.get('PROXY')
PROXY_LIST = [p.strip() for p in os.environ.get('PROXY_LIST', '').split(',') if p.strip()]

MAX_COOLDOWN_ENTRIES = 500
MAX_FILE_SIZE_MB = 1500

# ===== EXTERNAL NOTIFY BOT =====
NOTIFY_BOT_TOKEN = os.environ.get('NOTIFY_BOT_TOKEN')
NOTIFY_CHAT_ID = os.environ.get('NOTIFY_CHAT_ID')


def _bool(v, default=False):
    if v is None:
        return default
    return str(v).strip().lower() in ('1', 'true', 'yes', 'on', 'y')


NOTIFY_ONLINE_WATCH = _bool(os.environ.get('NOTIFY_ONLINE_WATCH'), True)
NOTIFY_SESSIONS = _bool(os.environ.get('NOTIFY_SESSIONS'), True)
NOTIFY_PROTECT = _bool(os.environ.get('NOTIFY_PROTECT'), True)
NOTIFY_PM_DELETE = _bool(os.environ.get('NOTIFY_PM_DELETE'), True)
NOTIFY_PM_EDIT = _bool(os.environ.get('NOTIFY_PM_EDIT'), True)
NOTIFY_ERRORS = _bool(os.environ.get('NOTIFY_ERRORS'), True)
