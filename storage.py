import sqlite3
import threading
import time
import logging

logger = logging.getLogger(__name__)


_VALID_TABLES = frozenset({
    'bot_state', 'saved_data', 'notes', 'todos', 'stats',
    'reply_settings', 'sessions', 'protected_chats',
    'online_watch', 'pm_msg_cache',
})


def _validate_table(table):
    if table not in _VALID_TABLES:
        raise ValueError(f"Invalid table name: {table}")
    return table


class Storage:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path='userbot.db'):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init(db_path)
        return cls._instance

    def _init(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self._create_tables()

    def _create_tables(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.executescript('''
                CREATE TABLE IF NOT EXISTS bot_state (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS saved_data (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS notes (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, text TEXT, done INTEGER DEFAULT 0, created INTEGER);
                CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER DEFAULT 0);
                CREATE TABLE IF NOT EXISTS reply_settings (key TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS sessions (hash TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS protected_chats (chat_id TEXT PRIMARY KEY, value TEXT);
                CREATE TABLE IF NOT EXISTS online_watch (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_id INTEGER NOT NULL UNIQUE,
                    target_username TEXT,
                    message TEXT NOT NULL,
                    owner_id INTEGER NOT NULL,
                    chat_id INTEGER,
                    last_status TEXT DEFAULT 'unknown',
                    enabled INTEGER DEFAULT 1,
                    created_at INTEGER
                );
                CREATE TABLE IF NOT EXISTS pm_msg_cache (
                    chat_id INTEGER,
                    msg_id INTEGER,
                    sender_id INTEGER,
                    sender_name TEXT,
                    text TEXT,
                    has_media INTEGER DEFAULT 0,
                    media_type TEXT,
                    date INTEGER,
                    verified INTEGER DEFAULT 1,
                    last_seen INTEGER DEFAULT 0,
                    PRIMARY KEY(chat_id, msg_id)
                );
            ''')
            # Миграция для существующих баз: добавляем отсутствующие колонки
            cur.execute('PRAGMA table_info(pm_msg_cache)')
            cols = {row[1] for row in cur.fetchall()}
            if 'verified' not in cols:
                cur.execute('ALTER TABLE pm_msg_cache ADD COLUMN verified INTEGER DEFAULT 1')
            if 'last_seen' not in cols:
                cur.execute('ALTER TABLE pm_msg_cache ADD COLUMN last_seen INTEGER DEFAULT 0')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_pm_msg_id ON pm_msg_cache(msg_id)')
            cur.execute('CREATE INDEX IF NOT EXISTS idx_pm_chat_date ON pm_msg_cache(chat_id, date)')
            self.conn.commit()

    # ── Generic helpers ──

    def _get(self, table, key, default=None):
        _validate_table(table)
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(f'SELECT value FROM {table} WHERE key = ?', (key,))
            row = cur.fetchone()
            return row[0] if row else default

    def _set(self, table, key, value):
        _validate_table(table)
        with self.lock:
            self.conn.execute(f'INSERT OR REPLACE INTO {table} (key, value) VALUES (?, ?)', (key, value))
            self.conn.commit()

    def _delete(self, table, key):
        _validate_table(table)
        with self.lock:
            self.conn.execute(f'DELETE FROM {table} WHERE key = ?', (key,))
            self.conn.commit()

    def _get_all(self, table):
        _validate_table(table)
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(f'SELECT key, value FROM {table}')
            return {row[0]: row[1] for row in cur.fetchall()}

    def _delete_all(self, table):
        _validate_table(table)
        with self.lock:
            self.conn.execute(f'DELETE FROM {table}')
            self.conn.commit()

    def _keys(self, table):
        _validate_table(table)
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(f'SELECT key FROM {table}')
            return [row[0] for row in cur.fetchall()]

    def _search(self, table, query):
        _validate_table(table)
        q = f'%{query.lower()}%'
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(f'SELECT key, value FROM {table} WHERE LOWER(key) LIKE ? OR LOWER(value) LIKE ?', (q, q))
            return cur.fetchall()

    # ── Bot state ──

    def get_state(self, key, default=None):
        return self._get('bot_state', key, default)

    def set_state(self, key, value):
        self._set('bot_state', key, str(value))

    # ── Saved data ──

    def get_saved(self, key, default=None):
        return self._get('saved_data', key, default)

    def set_saved(self, key, value):
        self._set('saved_data', key, value)

    def del_saved(self, key):
        self._delete('saved_data', key)

    def all_saved(self):
        return self._get_all('saved_data')

    def search_saved(self, query):
        return self._search('saved_data', query)

    # ── Notes ──

    def get_note(self, key, default=None):
        return self._get('notes', key, default)

    def set_note(self, key, value):
        self._set('notes', key, value)

    def del_note(self, key):
        self._delete('notes', key)

    def all_notes(self):
        return self._get_all('notes')

    def search_notes(self, query):
        return self._search('notes', query)

    # ── Todos ──

    def add_todo(self, text):
        tid = int(time.time())
        with self.lock:
            self.conn.execute(
                'INSERT INTO todos (id, text, done, created) VALUES (?, ?, 0, ?)',
                (tid, text, tid)
            )
            self.conn.commit()
        return tid

    def get_todos(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT id, text, done, created FROM todos ORDER BY created')
            return [dict(row) for row in cur.fetchall()]

    def update_todo(self, tid, done=True):
        with self.lock:
            self.conn.execute('UPDATE todos SET done = ? WHERE id = ?', (1 if done else 0, tid))
            self.conn.commit()

    def del_todo(self, tid):
        with self.lock:
            self.conn.execute('DELETE FROM todos WHERE id = ?', (tid,))
            self.conn.commit()

    # ── Stats ──

    def bump_stat(self, key, n=1):
        with self.lock:
            self.conn.execute(
                'INSERT INTO stats (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = value + ?',
                (key, n, n)
            )
            self.conn.commit()

    def all_stats(self):
        raw = self._get_all('stats')
        return {k: int(v) for k, v in raw.items()}

    def clear_all(self):
        with self.lock:
            for t in _VALID_TABLES:
                self.conn.execute(f'DELETE FROM {t}')
            self.conn.commit()

    # ── Reply settings (per-user + default) ──

    def get_reply_text(self, uid):
        return self._get('reply_settings', str(uid))

    def set_reply_text(self, uid, text):
        self._set('reply_settings', str(uid), text)

    def get_default_reply(self):
        return self._get('reply_settings', 'default')

    def set_default_reply(self, text):
        self._set('reply_settings', 'default', text)

    # ── Sessions (!watch) ──

    def all_sessions(self):
        raw = self._get_all('sessions')
        return {k: v for k, v in raw.items()}

    def save_session(self, hash_key, data_json):
        self._set('sessions', hash_key, data_json)

    def clear_sessions(self):
        self._delete_all('sessions')

    # ── Protected chats (!protect) ──

    # В таблице первичный ключ называется chat_id (а не key), поэтому
    # generic-хелперы _set/_keys/_delete тут не подходят — используем явный SQL.
    def add_protected_chat(self, chat_id):
        with self.lock:
            self.conn.execute(
                'INSERT OR REPLACE INTO protected_chats (chat_id, value) VALUES (?, ?)',
                (str(chat_id), str(int(time.time())))
            )
            self.conn.commit()

    def get_protected_chat_ids(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT chat_id FROM protected_chats')
            out = []
            for row in cur.fetchall():
                try:
                    out.append(int(row[0]))
                except (TypeError, ValueError):
                    continue
            return out

    def del_protected_chat(self, chat_id):
        with self.lock:
            self.conn.execute('DELETE FROM protected_chats WHERE chat_id = ?', (str(chat_id),))
            self.conn.commit()

    def clear_protected_chats(self):
        with self.lock:
            self.conn.execute('DELETE FROM protected_chats')
            self.conn.commit()

    # ── Online watch ──

    def add_online_watch(self, target_id, target_username, message, owner_id, chat_id):
        with self.lock:
            self.conn.execute(
                'INSERT OR REPLACE INTO online_watch (target_id, target_username, message, owner_id, chat_id, last_status, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, 1, ?)',
                (int(target_id), target_username or '', message, int(owner_id), int(chat_id), 'unknown', int(time.time()))
            )
            self.conn.commit()

    def get_online_watches(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM online_watch WHERE enabled = 1')
            return [dict(row) for row in cur.fetchall()]

    def update_online_watch_status(self, watch_id, last_status):
        with self.lock:
            self.conn.execute('UPDATE online_watch SET last_status = ? WHERE id = ?', (last_status, watch_id))
            self.conn.commit()

    def remove_online_watch(self, target_id=None, watch_id=None):
        with self.lock:
            if watch_id is not None:
                self.conn.execute('DELETE FROM online_watch WHERE id = ?', (watch_id,))
            elif target_id is not None:
                self.conn.execute('DELETE FROM online_watch WHERE target_id = ?', (int(target_id),))
            self.conn.commit()

    def get_online_watch_by_target(self, target_id):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM online_watch WHERE target_id = ?', (int(target_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    # ── PM message cache ──

    def cache_pm_message(self, chat_id, msg_id, sender_id, sender_name, text, has_media, media_type):
        now = int(time.time())
        with self.lock:
            self.conn.execute(
                'INSERT OR REPLACE INTO pm_msg_cache (chat_id, msg_id, sender_id, sender_name, text, has_media, media_type, date, verified, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?)',
                (int(chat_id), int(msg_id), int(sender_id) if sender_id else 0, sender_name or '', text or '', 1 if has_media else 0, media_type or '', now, now)
            )
            self.conn.commit()

    def get_pm_cached(self, chat_id, msg_id):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM pm_msg_cache WHERE chat_id = ? AND msg_id = ?', (int(chat_id), int(msg_id)))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_pm_cached_by_msg(self, msg_id):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM pm_msg_cache WHERE msg_id = ? ORDER BY date DESC LIMIT 1', (int(msg_id),))
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_pm_cached(self, chat_id, msg_id):
        with self.lock:
            self.conn.execute('DELETE FROM pm_msg_cache WHERE chat_id = ? AND msg_id = ?', (int(chat_id), int(msg_id)))
            self.conn.commit()

    def get_pm_chats_for_check(self, max_age=259200, limit=20):
        """Чаты с самыми свежими сообщениями в кэше (для сверки)."""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                'SELECT chat_id, MAX(date) AS d FROM pm_msg_cache WHERE date >= ? '
                'GROUP BY chat_id ORDER BY d DESC LIMIT ?',
                (int(time.time()) - int(max_age), int(limit))
            )
            return [row['chat_id'] for row in cur.fetchall()]

    def get_pm_rows_for_check(self, chat_id, max_age=259200, limit=100):
        """Сообщения конкретного чата, которые надо проверить на существование."""
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                'SELECT * FROM pm_msg_cache WHERE chat_id = ? AND date >= ? '
                'ORDER BY msg_id DESC LIMIT ?',
                (int(chat_id), int(time.time()) - int(max_age), int(limit))
            )
            return [dict(row) for row in cur.fetchall()]

    def mark_pm_alive(self, chat_id, msg_ids):
        if not msg_ids:
            return
        ids = [int(i) for i in msg_ids]
        placeholders = ','.join('?' * len(ids))
        with self.lock:
            self.conn.execute(
                f'UPDATE pm_msg_cache SET verified = 1, last_seen = ? '
                f'WHERE chat_id = ? AND msg_id IN ({placeholders})',
                [int(time.time()), int(chat_id), *ids]
            )
            self.conn.commit()

    def reset_pm_verified(self):
        """Сброс базы на старте: первая сверка только фиксирует наличие,
        чтобы не заваливать уведомлениями за время оффлайна."""
        with self.lock:
            self.conn.execute('UPDATE pm_msg_cache SET verified = 0')
            self.conn.commit()

    def clean_pm_cache(self, limit=4000):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT COUNT(*) as c FROM pm_msg_cache')
            row = cur.fetchone()
            cnt = row['c'] if row else 0
            if cnt > limit:
                cur.execute('DELETE FROM pm_msg_cache WHERE rowid IN (SELECT rowid FROM pm_msg_cache ORDER BY date ASC LIMIT ?)', (cnt - limit,))
                self.conn.commit()
