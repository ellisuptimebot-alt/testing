# -*- coding: utf-8 -*-
"""Юнит-тесты «онлайн-караула»: чистые функции (интервалы, триггеры,
окна), дедупликация _onwatch_fire — событие + поллер = одна отправка,
логика пропуска опроса и обработчики активности (typing/сообщения/галочки).

Функции достаются из plugins/watch.py через ast, без импорта модуля:
он тянет client/core/notifier и создаёт файл сессии."""

import ast
import asyncio
import logging
import os
import pathlib
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone

from telethon import utils as tgutils
from telethon import events
from telethon.errors import FloodWaitError
from telethon.extensions import BinaryReader
from telethon.tl.types import (
    Message, MessageMediaEmpty, MessageReactions, PeerChannel, PeerUser,
    MessagePeerReaction, PhoneCallProtocol, PhoneCallRequested,
    ReactionCount, ReactionEmoji,
    SendMessageTypingAction, StoryItem, UpdateEditMessage,
    UpdateMessageReactions, UpdatePhoneCall, UpdateReadHistoryOutbox,
    UpdateStory, UpdateUserName, UpdateUserStatus, UpdateUserTyping,
    UserStatusOffline, UserStatusOnline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth,
)

from storage import Storage

ROOT = pathlib.Path(__file__).resolve().parent.parent
WATCH_SRC = (ROOT / 'plugins' / 'watch.py').read_text(encoding='utf-8')
NOTIFY_SRC = (ROOT / 'notifier.py').read_text(encoding='utf-8')


def load_notify_badge():
    """watch_source_badge из notifier.py без импорта модуля (он тянет config
    и требует API_ID/API_HASH из окружения)."""
    tree = ast.parse(NOTIFY_SRC)
    nodes = [n for n in tree.body
             if (isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name)
                         and t.id == '_WATCH_SOURCE_LABELS'
                         for t in n.targets))
             or (isinstance(n, ast.FunctionDef)
                 and n.name == 'watch_source_badge')]
    assert len(nodes) == 2, f'не найдены узлы: {len(nodes)}'
    ns = {}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'notifier.py',
                 'exec'), ns)
    return ns['watch_source_badge']


def load_notify_labels():
    """Словарь источник → (эмодзи, подпись) из notifier.py."""
    tree = ast.parse(NOTIFY_SRC)
    for n in tree.body:
        if (isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name)
                        and t.id == '_WATCH_SOURCE_LABELS' for t in n.targets)):
            ns = {}
            exec(compile(ast.Module(body=[n], type_ignores=[]), 'notifier.py',
                         'exec'), ns)
            return ns['_WATCH_SOURCE_LABELS']
    raise AssertionError('_WATCH_SOURCE_LABELS не найден')

_WANT = {
    '_status_name', '_profile_name', 'watch_label', '_fmt_dur',
    '_parse_window', '_fmt_win', '_poll_gap', '_fresh_was_online',
    '_next_action', '_onwatch_fire', '_skip_due_to_event', '_poll_base',
    '_flicker_window', '_session_active', '_apply_status', '_force_refresh',
    '_parse_onwatch_args', '_raw_activity', '_raw_log_candidates',
    'onwatch_raw_activity',
    '_mark_reconnect', '_refresh_delay', '_wants_refresh', '_apply_schedule',
    'onwatch_status_event', 'onwatch_activity_msg', 'onwatch_read_receipt',
}


def load_watch(db=None, client=None, notify=None):
    """Изолированное пространство имён с функциями watch.py."""
    tree = ast.parse(WATCH_SRC)
    # константы берём из самого модуля, а не дублируем в тестах: иначе
    # новый источник активности разойдётся с набором для истории
    consts = [n for n in tree.body
              if isinstance(n, ast.Assign)
              and any(isinstance(t, ast.Name) and t.id == '_ACTIVITY_LOG_SOURCES'
                      for t in n.targets)]
    nodes = [n for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
             and n.name in _WANT]
    assert {n.name for n in nodes} == _WANT, \
        f'не найдены функции: {_WANT - {n.name for n in nodes}}'
    for n in nodes:            # декораторы @client.on(...) не должны
        n.decorator_list = []  # вычисляться при exec в тестовом ns
    ns = {
        'db': db, 'client': client, 'notify': notify,
        'logger': logging.getLogger('onwatch-test'),
        'time': time, 're': __import__('re'), 'tgutils': tgutils,
        'datetime': __import__('datetime'),
        '_FIRE_COOLDOWN': 120, '_FAIL_COOLDOWN': 60, 'FLICKER_WINDOW': 180,
        'FLICKER_WINDOW_MAX': 600, '_SESSION_TIMEOUT': 900,
        '_SESSION_TOUCH_MIN': 30, '_watch_err_logged': set(),
        '_next_due': {}, '_conn_up': True, '_warmed': True,
        '_refresh_reason': 'старт', '_REFRESH_MIN_GAP': 20,
        '_last_refresh': 0.0, '_schedule_gen': 0, '_last_poll': {},
        '_refresh_fails': 0, '_next_refresh_try': 0.0,
        '_REFRESH_FALLBACK_RETRY': 300, 'FloodWaitError': FloodWaitError,
        'OWNER_ID': 1,
        'UserStatusOnline': UserStatusOnline,
        'UserStatusOffline': UserStatusOffline,
        'UserStatusRecently': UserStatusRecently,
        'UserStatusLastWeek': UserStatusLastWeek,
        'UserStatusLastMonth': UserStatusLastMonth,
        # типы для raw-каналов активности (см. _raw_activity)
        'UpdateUserName': UpdateUserName, 'UpdateStory': UpdateStory,
        'UpdateMessageReactions': UpdateMessageReactions,
        'UpdatePhoneCall': UpdatePhoneCall, 'UpdateEditMessage': UpdateEditMessage,
        'PhoneCallRequested': PhoneCallRequested, 'PeerUser': PeerUser,
        'Message': Message,
    }
    if consts:
        exec(compile(ast.Module(body=consts, type_ignores=[]), 'watch.py', 'exec'), ns)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'watch.py', 'exec'), ns)
    return ns


class FakeUser:
    def __init__(self, uid, status):
        self.id = uid
        self.status = status


class FakeClient:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail
        self.batches = []

    async def get_entity(self, tid):
        return None

    async def get_users(self, ids):
        self.batches.append(list(ids))
        return [FakeUser(i, UserStatusOnline(int(time.time()) + 60))
                for i in ids]

    async def send_message(self, tid, text, parse_mode=None):
        if self.fail:
            raise RuntimeError('privacy: user restricts messaging')
        self.sent.append((tid, text, parse_mode))


class FakeNotify:
    def __init__(self):
        self.watch_calls = []

    async def online_watch(self, *a, **k):
        self.watch_calls.append((a, k))

    async def error_alert(self, *a, **k):
        pass


def ev(**kw):
    """Event-подобный объект для обработчиков watch.py."""
    class _E:
        pass
    e = _E()
    e.__dict__.update(kw)
    return e


class TestPollGap(unittest.TestCase):
    """Инвариант: чем реже статус, тем длиннее шаг опроса."""

    def setUp(self):
        self.ns = load_watch()

    def test_no_inversion_offline_never_faster_than_online(self):
        gap = self.ns['_poll_gap']
        for base in (5, 10, 15, 30, 35):
            online = gap(base, 'online')
            offline = gap(base, 'offline')
            self.assertGreater(offline, online,
                               f'base={base}: offline {offline}s опрашивается '
                               f'чаще, чем online {online}s — инверсия')

    def test_ordering_and_bounds(self):
        gap = self.ns['_poll_gap']
        for base in (5, 10, 15, 30, 35):
            self.assertEqual(gap(base, 'online'), base)
            self.assertEqual(gap(base, 'recently'), base)
            self.assertEqual(gap(base, 'unknown'), base * 2)
            last = gap(base, 'last_week')
            self.assertGreaterEqual(last, gap(base, 'offline'))
            self.assertGreaterEqual(last, 90)
            # холодный старт (unknown) не уводит трек в долгий сон
            self.assertLessEqual(gap(base, 'unknown'), 2 * 35)


class TestPollBase(unittest.TestCase):
    """Базовый интервал по числу целей — та же формула у поллера и статуса."""

    def test_values(self):
        base = load_watch()['_poll_base']
        self.assertEqual(base(0), 5)
        self.assertEqual(base(2), 5)
        self.assertEqual(base(3), 10)
        self.assertEqual(base(5), 10)
        self.assertEqual(base(6), 15)
        self.assertEqual(base(10), 35)
        self.assertEqual(base(500), 35)   # потолок против FloodWait


class TestSkipDueToEvent(unittest.TestCase):
    """Пропуск дублирующего GetUsers: событие уже записало свежий статус."""

    def setUp(self):
        self.ns = load_watch()
        self.skip = self.ns['_skip_due_to_event']

    def test_fresh_event_skips_poll(self):
        # событие записало статус ПОСЛЕ последнего опроса — GetUsers не нужен
        self.assertTrue(self.skip(status_since=200, last_poll_ts=100))

    def test_stale_event_still_polls(self):
        # опрос был позже события — опрашиваем снова (нет ложного skip)
        self.assertFalse(self.skip(status_since=100, last_poll_ts=200))

    def test_no_event_still_polls(self):
        self.assertFalse(self.skip(status_since=0, last_poll_ts=100))
        self.assertFalse(self.skip(status_since=None, last_poll_ts=None))

    def test_per_target_isolation(self):
        # словарь _last_poll пер-целевой: решение по цели B считается только
        # из её собственных меток — событие по A не должно пропустить B
        _last_poll = {'A': 100, 'B': 200}
        self.assertTrue(self.skip(150, _last_poll.get('A', 0)))    # A: skip
        self.assertFalse(self.skip(50, _last_poll.get('B', 0)))    # B: опрос
        # у B нет ни своего события, ни опроса — тоже опрашиваем
        _last_poll.pop('B')
        self.assertFalse(self.skip(0, _last_poll.get('B', 0)))


class TestNextAction(unittest.TestCase):
    """Триггеры: offline→online, offline→recently, recently→offline, flicker."""

    def setUp(self):
        self.ns = load_watch()
        self.act = self.ns['_next_action']

    def test_classic_online_transition(self):
        self.assertEqual(self.act('offline', 'online'), 'fire')
        self.assertEqual(self.act('recently', 'online'), 'fire')
        self.assertEqual(self.act('unknown', 'online'), 'fire')

    def test_online_renewal_not_refired(self):
        # повторные online-продления от Telegram не шлём второй раз
        self.assertEqual(self.act('online', 'online'), 'none')

    def test_entry_into_recently_is_trigger(self):
        # сервер мог спрятать online за плейсхолдером приватности:
        # offline/last_week → recently = человек пришёл в сеть
        self.assertEqual(self.act('offline', 'recently'), 'fire')
        self.assertEqual(self.act('last_week', 'recently'), 'fire')
        self.assertEqual(self.act('last_month', 'recently'), 'fire')
        # cold start: unknown → recently — только базовая запись, не fire
        self.assertEqual(self.act('unknown', 'recently'), 'update')

    def test_recently_to_offline_is_trigger(self):
        # скрытый last-seen: уход из «недавно» = человек только что был онлайн
        self.assertEqual(self.act('recently', 'offline'), 'fire-recently')

    def test_flicker_only_when_session_was_missed(self):
        # was_online свежий, а мы думали, что он offline — сессия пропущена
        self.assertEqual(self.act('offline', 'offline', flicker=True), 'fire-flicker')
        # сессию мы и так видели — при уходе просто фиксируем статус
        self.assertEqual(self.act('online', 'offline', flicker=True), 'update')
        self.assertEqual(self.act('recently', 'offline', flicker=True), 'fire-recently')

    def test_plain_status_changes(self):
        self.assertEqual(self.act('offline', 'last_week'), 'update')
        self.assertEqual(self.act('online', 'online'), 'none')
        self.assertEqual(self.act('offline', 'offline'), 'none')


class TestFreshWasOnline(unittest.TestCase):
    def test_fresh_offline_ts(self):
        ns = load_watch()
        now = int(time.time())
        self.assertIsNotNone(ns['_fresh_was_online'](UserStatusOffline(was_online=now - 10), now=now))
        self.assertIsNone(ns['_fresh_was_online'](UserStatusOffline(was_online=now - 9999), now=now))
        self.assertIsNone(ns['_fresh_was_online'](UserStatusOffline(was_online=0), now=now))

    def test_none_field_no_crash(self):
        # жёсткая приватность: поле может отсутствовать вовсе
        ns = load_watch()
        self.assertIsNone(ns['_fresh_was_online'](UserStatusOffline(was_online=None)))
        # объект без атрибута (сервер прислал неполный статус) — тоже не падаем
        st = UserStatusOffline(was_online=0)
        del st.was_online
        self.assertIsNone(ns['_fresh_was_online'](st))

    def test_datetime_values_normalised(self):
        # Telethon отдаёт int, но оба варианта datetime не должны падать:
        # aware — UTC, naive трактуем как UTC (сервер хранит UTC)
        ns = load_watch()
        now = time.time()
        aware = datetime.now(timezone.utc) - timedelta(seconds=10)
        naive = aware.replace(tzinfo=None)   # naive = UTC по договорённости
        st = UserStatusOffline(was_online=now - 10)
        st.was_online = aware
        self.assertIsNotNone(ns['_fresh_was_online'](st, now=now))
        st.was_online = naive
        self.assertIsNotNone(ns['_fresh_was_online'](st, now=now))
        st.was_online = datetime.now(timezone.utc) - timedelta(hours=5)
        self.assertIsNone(ns['_fresh_was_online'](st, now=now))
        st.was_online = 'мусор'
        self.assertIsNone(ns['_fresh_was_online'](st, now=now))

    def test_window_param_respected(self):
        ns = load_watch()
        now = time.time()
        st = UserStatusOffline(was_online=now - 50)
        self.assertIsNotNone(ns['_fresh_was_online'](st, now=now))            # окно 180
        self.assertIsNone(ns['_fresh_was_online'](st, now=now, window=10))    # сузили
        self.assertIsNotNone(ns['_fresh_was_online'](st, now=now, window=60))  # хватило


class TestFlickerWindow(unittest.TestCase):
    """Окно flicker обязано быть больше максимального шага опроса."""

    def test_window_exceeds_max_gap(self):
        ns = load_watch()
        floor = ns['FLICKER_WINDOW']
        gap = ns['_poll_gap']
        max_gap = max(gap(35, s) for s in
                      ('online', 'recently', 'offline', 'unknown',
                       'last_week', 'last_month'))
        self.assertGreater(floor, max_gap,
                           f'окно {floor}s не покрывает шаг {max_gap}s — '
                           f'пропущенные сессии будут теряться')

    def test_adaptive_window_at_least_double_gap(self):
        # поллер передаёт _flicker_window(шаг): для редких статусов окно
        # растёт вместе с шагом, но не выше капа
        ns = load_watch()
        floor, gap = ns['FLICKER_WINDOW'], ns['_poll_gap']
        win = ns['_flicker_window']
        for base in (5, 35):
            for status in ('offline', 'last_week', 'last_month'):
                g = gap(base, status)
                self.assertGreaterEqual(win(g), min(max(floor, 2 * g),
                                                   ns['FLICKER_WINDOW_MAX']))

    def test_window_upper_cap(self):
        # рост _poll_gap в будущем не должен раздувать окно бесконечно
        ns = load_watch()
        win, cap = ns['_flicker_window'], ns['FLICKER_WINDOW_MAX']
        self.assertEqual(win(5), ns['FLICKER_WINDOW'])   # 180 > 2*5
        self.assertEqual(win(35), 180)                   # 2*35 < 180
        self.assertEqual(win(200), 400)                  # 2× шага
        self.assertEqual(win(400), cap)                  # кап
        self.assertEqual(win(10 ** 6), cap)


class TestOnwatchArgs(unittest.TestCase):
    """Аргументы !onwatch: -m и числовой ID совместимы в любом порядке."""

    def setUp(self):
        self.parse = load_watch()['_parse_onwatch_args']

    def test_flag_before_numeric_id(self):
        self.assertEqual(self.parse('!onwatch -m 12345 привет'),
                         (True, '12345', 'привет'))

    def test_flag_after_numeric_id(self):
        # частый ввод: сначала цель, потом флаг
        self.assertEqual(self.parse('!onwatch 12345 -m привет'),
                         (True, '12345', 'привет'))

    def test_flag_before_username(self):
        self.assertEqual(self.parse('!onwatch -m @user привет'),
                         (True, '@user', 'привет'))

    def test_without_flag(self):
        self.assertEqual(self.parse('!onwatch 12345 привет'),
                         (False, '12345', 'привет'))
        self.assertEqual(self.parse('!onwatch @user привет'),
                         (False, '@user', 'привет'))

    def test_markdown_text_preserved(self):
        self.assertEqual(
            self.parse('!onwatch -m 12345 *Жирный* [ссылка](http://x) _к_'),
            (True, '12345', '*Жирный* [ссылка](http://x) _к_'))

    def test_multiline_text(self):
        text = '!onwatch -m 12345 первая' + chr(10) + 'вторая'
        self.assertEqual(self.parse(text),
                         (True, '12345', 'первая' + chr(10) + 'вторая'))

    def test_case_insensitive(self):
        self.assertEqual(self.parse('!ONWATCH -M 12345 Привет'),
                         (True, '12345', 'Привет'))

    def test_missing_message_is_not_a_command(self):
        self.assertIsNone(self.parse('!onwatch 12345'))
        self.assertIsNone(self.parse('!onwatch -m 12345'))
        self.assertIsNone(self.parse('!onwatch -m'))
        self.assertIsNone(self.parse('!onwatch'))


class TestWindowHelpers(unittest.TestCase):
    def test_parse_window(self):
        parse = load_watch()['_parse_window']
        self.assertEqual(parse('7d'), 7 * 86400)
        self.assertEqual(parse('12h'), 12 * 3600)
        self.assertEqual(parse('30m'), 30 * 60)
        self.assertEqual(parse('45s'), 45)
        self.assertEqual(parse(None), 86400)
        self.assertEqual(parse('мусор'), 86400)

    def test_fmt_win(self):
        fmt = load_watch()['_fmt_win']
        self.assertEqual(fmt(7 * 86400), '7д')
        self.assertEqual(fmt(12 * 3600), '12ч')
        self.assertEqual(fmt(30 * 60), '30м')


class TestFireDedup(unittest.TestCase):
    """Событие и поллер на один переход → ровно одна отправка."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_test_')
        self.db = Storage(os.path.join(self.tmpdir, 'watch.db'))
        self.db.add_online_watch(555000111, 'target', 'привет', 1, -100)
        self.row = self.db.get_online_watch_by_target(555000111)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _fire(self, ns, source):
        return asyncio.run(ns['_onwatch_fire'](self.row, source))

    def _expire(self, ns, source='poll'):
        """Закрыть сессию и кулдаун — имитация новой сессии после паузы."""
        row = self.db.get_online_watch_by_target(555000111)
        self.db.close_online_watch_session(row['id'])
        self.db.touch_online_watch_fired(
            row['id'], time.time() - ns['_FIRE_COOLDOWN'] - 1)
        return self._fire(ns, source)

    def test_event_then_poll_sends_once(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'update'))      # событие выиграло
        self.assertFalse(self._fire(ns, 'poll'))       # поллер увидел кулдаун
        self.assertEqual(len(client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 1)
        self.assertEqual(row['last_source'], 'update')
        self.assertIsNone(row['last_error'])
        # тишина дольше SESSION_TIMEOUT — это новый вход, снова шлём
        self.assertTrue(self._expire(ns, 'poll'))
        self.assertEqual(len(client.sent), 2)

    def test_flicker_and_recently_sources_counted(self):
        # метрика «событие vs поллер» растёт для ВСЕХ типов триггеров
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'poll-flicker'))
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 1)
        self.assertEqual(row['last_source'], 'poll-flicker')
        self.assertTrue(self._expire(ns, 'update-recently'))
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 2)
        self.assertEqual(row['last_source'], 'update-recently')
        self.assertEqual(len(client.sent), 2)

    def test_fail_cooldown_and_error_metric(self):
        client = FakeClient(fail=True)
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertFalse(self._fire(ns, 'poll'))
        row = self.db.get_online_watch_by_target(555000111)
        # сбой не сжигает полный кулдаун: повтор через _FAIL_COOLDOWN
        left = time.time() - (row['last_fired'] or 0)
        cd, fc = ns['_FIRE_COOLDOWN'], ns['_FAIL_COOLDOWN']
        self.assertGreaterEqual(left, fc - 2)
        self.assertLess(left, cd)
        self.assertIn('privacy', row['last_error'] or '')
        self.assertEqual(row['fires'], 0)  # неудачная отправка не считается
        self.assertEqual(row['last_source'], 'poll')
        # сбой не открывает сессию — следующая попытка повторит доставку
        self.assertFalse(row.get('last_activity'))

    def test_default_sends_raw_md_flag_sends_markdown(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self._fire(ns, 'update')
        self.assertIsNone(client.sent[0][2])           # parse_mode=None по умолчанию
        # трек с флагом -m
        self.db.add_online_watch(555000222, 't2', 'hi', 1, -100, md=1)
        row2 = self.db.get_online_watch_by_target(555000222)
        asyncio.run(ns['_onwatch_fire'](row2, 'update'))
        self.assertEqual(client.sent[1][2], 'md')


class TestNotifyTone(unittest.TestCase):
    """Тело уведомления зависит от канала: звонок ≠ профиль ≠ правка."""

    def setUp(self):
        self.badge = load_notify_badge()

    def test_channels_have_distinct_badges(self):
        call = self.badge('update-call')
        profile = self.badge('update-profile')
        edit = self.badge('update-edit')
        self.assertEqual(call[0], '📞')
        self.assertEqual(profile[0], '📝')
        self.assertEqual(edit[0], '✏️')
        self.assertEqual(len({call, profile, edit}), 3)

    def test_typing_and_reaction_are_visually_distinct(self):
        typing = self.badge('typing')
        reaction = self.badge('update-reaction')
        self.assertNotEqual(typing, reaction)
        self.assertNotEqual(typing[0], reaction[0])   # разные эмодзи
        self.assertNotEqual(typing[1], reaction[1])   # разные подписи
        self.assertIn('печатает', typing[1])
        self.assertIn('отреагировал', reaction[1])

    def test_every_source_has_unique_badge(self):
        # никакие два канала не должны выглядеть одинаково в уведомлении
        labels = load_notify_labels()
        pairs = list(labels.values())
        self.assertEqual(len(pairs), len(set(pairs)),
                         f'дубли бейджей: {pairs}')
        self.assertEqual(len({p[0] for p in pairs}), len(pairs),
                         'одинаковые эмодзи у разных каналов')

    def test_poll_and_flicker_are_neutral(self):
        # обычный вход и flicker — нейтральное «появился в сети»
        for src in ('poll', 'update', 'poll-flicker', 'update-recently', None,
                    'неизвестный'):
            self.assertEqual(self.badge(src)[0], '⚡', src)
        self.assertEqual(self.badge('update-call')[1], 'звонил вам')


class TestSessionModel(unittest.TestCase):
    """Уведомление — одно на непрерывную сессию, а не на каждый триггер."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_sess_')
        self.db = Storage(os.path.join(self.tmpdir, 'sess.db'))
        self.db.add_online_watch(555000111, 'target', 'привет', 1, -100)
        self.row = self.db.get_online_watch_by_target(555000111)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _fire(self, ns, source):
        return asyncio.run(ns['_onwatch_fire'](self.row, source))

    def test_continuous_activity_single_fire(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'typing'))
        # 30 минут переписки: триггеры только продлевают активность
        for _ in range(60):
            self.db.touch_online_watch_session(self.row['id'], time.time())
            self.assertFalse(self._fire(ns, 'typing'))
            self.assertFalse(self._fire(ns, 'message'))
            self.assertFalse(self._fire(ns, 'read'))
        self.assertEqual(len(client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 1)

    def test_silence_opens_new_session(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'typing'))
        # тишина дольше SESSION_TIMEOUT + кулдаун истёк → новый вход
        self.db.touch_online_watch_session(
            self.row['id'], time.time() - ns['_SESSION_TIMEOUT'] - 1)
        self.db.touch_online_watch_fired(
            self.row['id'], time.time() - ns['_FIRE_COOLDOWN'] - 1)
        self.assertTrue(self._fire(ns, 'message'))
        self.assertEqual(len(client.sent), 2)

    def test_twenty_minutes_silence_is_new_session(self):
        # 20 мин тишины > SESSION_TIMEOUT (15 мин) и > кулдауна (120 с):
        # несмотря на недавнюю отправку, это новый вход → новое уведомление
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'typing'))
        quiet = 20 * 60
        self.assertGreater(quiet, ns['_SESSION_TIMEOUT'])
        self.assertGreater(quiet, ns['_FIRE_COOLDOWN'])
        # 20 минут назад были и активность, и отправка
        self.db.touch_online_watch_session(
            self.row['id'], time.time() - quiet)
        self.db.touch_online_watch_fired(
            self.row['id'], time.time() - quiet)
        self.assertTrue(self._fire(ns, 'message'))
        self.assertEqual(len(client.sent), 2)
        self.assertEqual(self.db.get_online_watch_by_target(555000111)['fires'], 2)

    def test_cooldown_blocks_within_short_gap(self):
        # новая сессия (тишина), но кулдаун ещё идёт — отправки нет
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'typing'))
        self.db.close_online_watch_session(self.row['id'])
        self.db.touch_online_watch_fired(self.row['id'], time.time() - 5)
        self.assertFalse(self._fire(ns, 'message'))
        self.assertEqual(len(client.sent), 1)

    def test_session_active_helper_boundaries(self):
        active = load_watch()['_session_active']
        now = time.time()
        self.assertFalse(active(0, now))            # записи нет — сессии нет
        self.assertTrue(active(now - 10, now))
        self.assertTrue(active(now - 900, now))     # ровно на границе
        self.assertFalse(active(now - 901, now))

    def test_recently_exit_closes_session_without_second_send(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'update'))
        self.assertFalse(self._fire(ns, 'update-recently'))
        self.assertEqual(len(client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertFalse(row.get('last_activity'))   # сессия закрыта

    def test_online_to_offline_no_send(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        online = UserStatusOnline(int(time.time()) + 60)
        asyncio.run(ns['_apply_status'](self.row, online, 'update'))
        self.assertEqual(len(client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        action = asyncio.run(ns['_apply_status'](row, UserStatusOffline(
            was_online=int(time.time()) - 7200), 'update'))
        self.assertEqual(action, 'update')
        self.assertEqual(len(client.sent), 1)


class TestRefreshWants(unittest.TestCase):
    """После отката батч пробуется снова — деградация не постоянная."""

    def setUp(self):
        self.wants = load_watch()['_wants_refresh']

    def test_reset_always_wants(self):
        self.assertTrue(self.wants(False, 0))
        self.assertTrue(self.wants(False, time.time() + 999))

    def test_warmed_without_timer_no_batch(self):
        self.assertFalse(self.wants(True, 0))

    def test_warmed_waits_for_retry_timer(self):
        now = time.time()
        self.assertFalse(self.wants(True, now + 300, now))
        self.assertTrue(self.wants(True, now - 1, now))
        self.assertTrue(self.wants(True, now, now))


class TestApplySchedule(unittest.TestCase):
    """Расписание пишется только если поколение не сменилось."""

    def test_stale_schedule_not_written(self):
        ns = load_watch()
        ns['_next_due'].update({1: 111.0})
        ns['_schedule_gen'] = 5
        # gen снят до реконнекта (4), поколение уже 5 — старые сроки не пишем
        ok = ns['_apply_schedule']([{'target_id': 1}, {'target_id': 2}],
                                   {1: None, 2: None}, 5, 4)
        self.assertFalse(ok)
        self.assertEqual(ns['_next_due'], {1: 111.0})   # ничего не добавилось

    def test_fresh_schedule_written(self):
        ns = load_watch()
        ok = ns['_apply_schedule']([{'target_id': 7}], {}, 10, 0)
        self.assertTrue(ok)
        self.assertIn(7, ns['_next_due'])
        self.assertIn(7, ns['_last_poll'])


class TestRefreshDebounce(unittest.TestCase):
    """Флаппинг связи: не чаще одного батч-опроса за _REFRESH_MIN_GAP."""

    def setUp(self):
        self.delay = load_watch()['_refresh_delay']

    def test_start_never_delayed(self):
        self.assertEqual(self.delay('старт', time.time()), 0.0)
        self.assertEqual(self.delay('старт', 0), 0.0)

    def test_reconnect_within_gap_delayed(self):
        now = time.time()
        self.assertEqual(self.delay('реконнект', now - 5, now), 15)
        self.assertEqual(self.delay('реконнект', now, now), 20)

    def test_reconnect_after_gap_not_delayed(self):
        now = time.time()
        self.assertEqual(self.delay('реконнект', now - 20, now), 0.0)
        self.assertEqual(self.delay('реконнект', now - 300, now), 0.0)

    def test_first_reconnect_without_history(self):
        self.assertEqual(self.delay('реконнект', 0, time.time()), 0.0)


class TestReconnectReset(unittest.TestCase):
    """Реконнект: расписание сбрасывается, причина — «реконнект»."""

    def test_mark_reconnect_clears_schedule(self):
        ns = load_watch()
        ns['_next_due'].update({1: 100.0, 2: 200.0})
        ns['_conn_up'] = False
        ns['_mark_reconnect']()
        self.assertEqual(ns['_next_due'], {})          # все треки — вне графика
        self.assertTrue(ns['_conn_up'])
        self.assertFalse(ns['_warmed'])                # прогрев будет запущен
        self.assertEqual(ns['_refresh_reason'], 'реконнект')
        # поколение растёт: поллер прервёт текущий цикл, а не будет
        # опрашивать треки поштучно по устаревшим срокам
        self.assertEqual(ns['_schedule_gen'], 1)
        ns['_mark_reconnect']()
        self.assertEqual(ns['_schedule_gen'], 2)


class TestForceRefresh(unittest.TestCase):
    """Прогрев/реконнект: статусы всех треков — одним батчем get_users."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_ref_')
        self.db = Storage(os.path.join(self.tmpdir, 'ref.db'))
        self.db.add_online_watch(555000111, 'a', 'раз', 1, -100)
        self.db.add_online_watch(555000222, 'b', 'два', 1, -100)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_single_batch_for_all_targets(self):
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        statuses = asyncio.run(ns['_force_refresh']('тест'))
        # один сетевой вызов на оба трека, а не два get_entity
        self.assertEqual(len(client.batches), 1)
        self.assertEqual(sorted(client.batches[0]), [555000111, 555000222])
        self.assertEqual(len(statuses), 2)
        # статусы применены и уведомления отправлены по одному на цель
        for tid in (555000111, 555000222):
            self.assertEqual(self.db.get_online_watch_by_target(tid)['last_status'],
                             'online')
        self.assertEqual(sorted(t for t, _, _ in client.sent),
                         [555000111, 555000222])
        # повторный прогрев не пересылает: статус уже online
        client2 = FakeClient()
        ns2 = load_watch(db=self.db, client=client2, notify=FakeNotify())
        asyncio.run(ns2['_force_refresh']('тест'))
        self.assertEqual(client2.sent, [])


class TestLastRefreshStamp(unittest.TestCase):
    """_last_refresh фиксируется и при частичном/полном сбое get_users:
    иначе дебаунс флаппинга не узнает, что попытка была."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_stamp_')
        self.db = Storage(os.path.join(self.tmpdir, 'stamp.db'))
        self.db.add_online_watch(555000111, 'a', 'раз', 1, -100)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_stamped_after_all_chunks_fail(self):
        class FailingClient:
            async def get_users(self, ids):
                raise RuntimeError('network down')

        ns = load_watch(db=self.db, client=FailingClient(),
                        notify=FakeNotify())
        before = time.time()
        statuses = asyncio.run(ns['_force_refresh']('реконнект'))
        self.assertEqual(statuses, {})            # ни одного статуса
        self.assertGreaterEqual(ns['_last_refresh'], before)
        self.assertLessEqual(ns['_last_refresh'], time.time())

    def test_not_stamped_without_tracks(self):
        # пустая база: сетевых попыток нет, метку не двигаем
        empty = Storage(os.path.join(self.tmpdir, 'empty.db'))
        try:
            ns = load_watch(db=empty, client=FakeClient(), notify=FakeNotify())
            self.assertEqual(asyncio.run(ns['_force_refresh']('старт')), {})
            self.assertEqual(ns['_last_refresh'], 0.0)
        finally:
            empty.close()


class TestForceRefreshInvalidIds(unittest.TestCase):
    """Невалидный ID в ответе get_users не ломает прогрев остальных треков."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_bad_')
        self.db = Storage(os.path.join(self.tmpdir, 'bad.db'))
        self.db.add_online_watch(555000111, 'a', 'раз', 1, -100)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_none_and_idless_entries_skipped(self):
        class BadClient:
            def __init__(self):
                self.sent = []

            async def get_users(self, ids):
                return [None, object(), FakeUser(555000111, UserStatusOnline(
                    int(time.time()) + 60))]

            async def send_message(self, tid, text, parse_mode=None):
                self.sent.append(tid)

        client = BadClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        statuses = asyncio.run(ns['_force_refresh']('тест'))
        self.assertEqual(len(statuses), 1)             # только валидный id
        self.assertEqual(client.sent, [555000111])


class TestWireEvents(unittest.TestCase):
    """Смоук обработчиков на объектах, прошедших реальную (де)сериализацию TL:
    так приходит updateUserStatus с провода (класс, типы полей, was_online)."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_wire_')
        self.db = Storage(os.path.join(self.tmpdir, 'wire.db'))
        self.db.add_online_watch(555000111, 'target', 'привет', 1, -100)
        self.row = self.db.get_online_watch_by_target(555000111)
        self.client = FakeClient()
        self.notify = FakeNotify()
        self.ns = load_watch(db=self.db, client=self.client,
                             notify=self.notify)

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _last_source(self):
        return self.notify.watch_calls[-1][1].get('source')

    @staticmethod
    def _wire(update):
        """Сериализуем и разбираем обратно — ровно путь апдейта с сервера.
        Тело читается без 4-байтового constructor id: так же, как Telethon
        разбирает updates из контейнера (date-поля становятся aware datetime)."""
        return update.__class__.from_reader(BinaryReader(bytes(update)[4:]))

    def _call(self, update):
        u = self._wire(update)
        asyncio.run(self.ns['onwatch_status_event'](
            ev(user_id=u.user_id, status=u.status, typing=None)))

    def test_offline_flicker_from_wire(self):
        self.db.update_online_watch_status(self.row['id'], 'offline')
        wo = int(time.time()) - 5
        self._call(UpdateUserStatus(
            user_id=555000111, status=UserStatusOffline(was_online=wo)))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'update-flicker')
        logs = self.db.get_online_watch_log(555000111, 0)
        flick = [l for l in logs if l['source'] == 'flicker']
        self.assertEqual(len(flick), 1)
        self.assertEqual(flick[0]['ts'], wo)      # реальное время выхода

    def test_recently_from_wire(self):
        self.db.update_online_watch_status(self.row['id'], 'offline')
        self._call(UpdateUserStatus(
            user_id=555000111, status=UserStatusRecently()))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_status'], 'recently')
        self.assertEqual(row['last_source'], 'update')

    def test_online_from_wire_cold_start(self):
        self._call(UpdateUserStatus(
            user_id=555000111,
            status=UserStatusOnline(int(time.time()) + 120)))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_status'], 'online')

    def _typing_event(self):
        raw = UpdateUserTyping(user_id=555000111,
                               action=SendMessageTypingAction())
        parsed = UpdateUserTyping.from_reader(BinaryReader(bytes(raw)[4:]))
        return events.UserUpdate.build(parsed)

    def _read_event(self):
        raw = UpdateReadHistoryOutbox(peer=PeerUser(555000111), max_id=10,
                                      pts=1, pts_count=1)
        parsed = UpdateReadHistoryOutbox.from_reader(
            BinaryReader(bytes(raw)[4:]))
        return events.MessageRead.build(parsed)

    def test_typing_event_from_wire(self):
        e = self._typing_event()
        # событие собрано Telethon: приватный typing → chat_id положителен
        self.assertEqual(e.user_id, 555000111)
        self.assertEqual(e.chat_id, 555000111)
        self.assertTrue(e.typing)
        asyncio.run(self.ns['onwatch_status_event'](e))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'typing')
        logs = self.db.get_online_watch_log(555000111, 0)
        self.assertEqual([l['source'] for l in logs], ['typing'])

    def test_read_outbox_event_from_wire(self):
        e = self._read_event()
        # UpdateReadHistoryOutbox = цель прочитала ваши сообщения
        self.assertEqual(e.chat_id, 555000111)
        self.assertFalse(e.inbox)
        asyncio.run(self.ns['onwatch_read_receipt'](e))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'read')
        logs = self.db.get_online_watch_log(555000111, 0)
        self.assertEqual([l['source'] for l in logs], ['read'])

    def _raw(self, update):
        asyncio.run(self.ns['onwatch_raw_activity'](self._wire(update)))
        return self.db.get_online_watch_by_target(555000111)

    def test_profile_change_from_wire(self):
        row = self._raw(UpdateUserName(user_id=555000111, first_name='Имя',
                                       last_name='', usernames=[]))
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual(row['last_source'], 'update-profile')
        # уведомление во внешний бот получает тот же источник (тон отличается)
        self.assertEqual(self._last_source(), 'update-profile')
        self.assertEqual([l['source'] for l in
                          self.db.get_online_watch_log(555000111, 0)],
                         ['update-profile'])

    def test_untracked_profile_ignored(self):
        # смена имени НЕтреканным пользователем не должна давать fire
        self._raw(UpdateUserName(user_id=555000999, first_name='Чужой',
                                 last_name='', usernames=[]))
        self.assertEqual(len(self.client.sent), 0)
        self.assertEqual(self.db.get_online_watch_log(555000999, 0), [])
        self.assertEqual(self.notify.watch_calls, [])

    def test_userupdate_builder_ignores_profile(self):
        # контракт Telethon 1.44: UserUpdate не фаерит на UpdateUserName,
        # поэтому raw-канал и статус-обработчик не дают двойной fire
        raw = self._wire(UpdateUserName(user_id=555000111, first_name='Имя',
                                        last_name='', usernames=[]))
        self.assertIsNone(events.UserUpdate.build(raw))

    def test_story_from_wire(self):
        now = datetime.now(timezone.utc)
        story = StoryItem(id=7, date=now, expire_date=now + timedelta(days=1),
                          media=MessageMediaEmpty())
        row = self._raw(UpdateStory(peer=PeerUser(555000111), story=story))
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual(row['last_source'], 'update-story')
        self.assertEqual(self._last_source(), 'update-story')

    def test_story_of_channel_ignored(self):
        now = datetime.now(timezone.utc)
        story = StoryItem(id=8, date=now, expire_date=now + timedelta(days=1),
                          media=MessageMediaEmpty())
        self._raw(UpdateStory(peer=PeerChannel(777), story=story))
        self.assertEqual(len(self.client.sent), 0)

    def test_reaction_in_pm_from_wire(self):
        reactions = MessageReactions(results=[ReactionCount(
            reaction=ReactionEmoji(emoticon='👍'), count=1)])
        row = self._raw(UpdateMessageReactions(
            peer=PeerUser(555000111), msg_id=5, reactions=reactions))
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual(row['last_source'], 'update-reaction')
        self.assertEqual(self._last_source(), 'update-reaction')

    def test_reaction_in_channel_ignored(self):
        reactions = MessageReactions(results=[ReactionCount(
            reaction=ReactionEmoji(emoticon='👍'), count=1)])
        self._raw(UpdateMessageReactions(
            peer=PeerChannel(777), msg_id=5, reactions=reactions))
        self.assertEqual(len(self.client.sent), 0)

    def test_incoming_call_from_wire(self):
        protocol = PhoneCallProtocol(min_layer=92, max_layer=92,
                                     library_versions=['1.0'])
        call = PhoneCallRequested(
            id=1, access_hash=2, date=datetime.now(timezone.utc),
            admin_id=555000111, participant_id=1,
            g_a_hash=b'' * 16, protocol=protocol)
        row = self._raw(UpdatePhoneCall(phone_call=call))
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual(row['last_source'], 'update-call')
        self.assertEqual(self._last_source(), 'update-call')
        self.assertEqual([l['source'] for l in
                          self.db.get_online_watch_log(555000111, 0)],
                         ['update-call'])

    def test_edit_of_target_message_from_wire(self):
        msg = Message(id=9, peer_id=PeerUser(555000111),
                      from_id=PeerUser(555000111), message='исправил',
                      out=False)
        row = self._raw(UpdateEditMessage(message=msg, pts=1, pts_count=1))
        self.assertEqual(len(self.client.sent), 1)
        self.assertEqual(row['last_source'], 'update-edit')
        self.assertEqual(self._last_source(), 'update-edit')

    def test_own_edit_not_activity(self):
        msg = Message(id=10, peer_id=PeerUser(555000111),
                      from_id=PeerUser(1), message='моя правка', out=True)
        self._raw(UpdateEditMessage(message=msg, pts=1, pts_count=1))
        self.assertEqual(len(self.client.sent), 0)

    def test_group_reaction_logged_only(self):
        # реакция в общем чате: fire не делаем (PM-only), но в историю пишем
        now = datetime.now(timezone.utc)
        reactions = MessageReactions(
            results=[ReactionCount(reaction=ReactionEmoji(emoticon='👍'),
                                   count=1)],
            recent_reactions=[MessagePeerReaction(
                peer_id=PeerUser(555000111), date=now,
                reaction=ReactionEmoji(emoticon='👍'))])
        self._raw(UpdateMessageReactions(peer=PeerChannel(777), msg_id=5,
                                         reactions=reactions))
        self.assertEqual(len(self.client.sent), 0)
        self.assertEqual(self.notify.watch_calls, [])
        logs = self.db.get_online_watch_log(555000111, 0)
        self.assertEqual([l['source'] for l in logs], ['reaction-group'])

    def test_channel_story_logged_only(self):
        now = datetime.now(timezone.utc)
        story = StoryItem(id=11, date=now, expire_date=now + timedelta(days=1),
                          media=MessageMediaEmpty(),
                          from_id=PeerUser(555000111))
        self._raw(UpdateStory(peer=PeerChannel(777), story=story))
        self.assertEqual(len(self.client.sent), 0)
        self.assertEqual(self.notify.watch_calls, [])
        logs = self.db.get_online_watch_log(555000111, 0)
        self.assertEqual([l['source'] for l in logs], ['story-channel'])

    def test_raw_channels_dedup_by_session(self):
        # профиль и сторис в пределах одной сессии — одно уведомление
        self._raw(UpdateUserName(user_id=555000111, first_name='Имя',
                                 last_name='', usernames=[]))
        now = datetime.now(timezone.utc)
        story = StoryItem(id=9, date=now, expire_date=now + timedelta(days=1),
                          media=MessageMediaEmpty())
        self._raw(UpdateStory(peer=PeerUser(555000111), story=story))
        self.assertEqual(len(self.client.sent), 1)

    def test_stale_was_online_not_flicker(self):
        # выход 2 часа назад: это не пропущенная сессия, отправки нет
        self.db.update_online_watch_status(self.row['id'], 'offline')
        self._call(UpdateUserStatus(
            user_id=555000111,
            status=UserStatusOffline(was_online=int(time.time()) - 7200)))
        self.assertEqual(len(self.client.sent), 0)


class TestStatusLog(unittest.TestCase):
    """История сессий: одна строка лога на реальную смену статуса."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_log_')
        self.db = Storage(os.path.join(self.tmpdir, 'log.db'))

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_log_written_only_on_real_change(self):
        self.assertTrue(self.db.add_online_watch(666000111, 't', 'm', 1, -100))
        row = self.db.get_online_watch_by_target(666000111)
        self.assertTrue(self.db.update_online_watch_status(row['id'], 'online'))
        # тот же статус повторно — ни строки лога, ни сдвига status_since
        s1 = self.db.get_online_watch_by_target(666000111)['status_since']
        self.assertFalse(self.db.update_online_watch_status(row['id'], 'online'))
        time.sleep(1.1)
        self.assertTrue(self.db.update_online_watch_status(row['id'], 'offline'))
        row2 = self.db.get_online_watch_by_target(666000111)
        self.assertGreater(row2['status_since'], s1)
        logs = self.db.get_online_watch_log(666000111, 0)
        self.assertEqual([l['status'] for l in logs], ['online', 'offline'])
        # окно фильтрует старые переходы
        self.assertEqual(self.db.get_online_watch_log(666000111, int(time.time()) + 10), [])

    def test_flicker_session_appended_without_status_change(self):
        # flicker не меняет last_status — сессия пишется отдельной строкой
        self.db.add_online_watch(666000222, 't2', 'm', 1, -100)
        now = int(time.time())
        self.db.append_online_watch_log(666000222, 'online', now - 30,
                                        source='flicker')
        logs = self.db.get_online_watch_log(666000222, 0)
        self.assertEqual([l['status'] for l in logs], ['online'])
        # в логе видно, что это пропущенная сессия, а не реальный вход
        self.assertEqual(logs[0]['source'], 'flicker')
        # статус при этом не сдвинулся
        row = self.db.get_online_watch_by_target(666000222)
        self.assertEqual(row['last_status'], 'unknown')
        self.assertFalse(row.get('status_since'))

    def test_log_retention_prunes_old_rows(self):
        self.db.add_online_watch(666000333, 't3', 'm', 1, -100)
        old_ts = int(time.time()) - 91 * 86400
        self.db.append_online_watch_log(666000333, 'online', old_ts)
        self.db.append_online_watch_log(666000333, 'online', int(time.time()))
        # ретеншн — периодическая задача поллера, не путь отправки
        self.assertEqual(self.db.prune_online_watch_log(), 1)
        logs = self.db.get_online_watch_log(666000333, 0)
        self.assertEqual([l['status'] for l in logs], ['online'])


class TestActivityHandlers(unittest.TestCase):
    """typing/сообщения/галочки и событийные триггеры → одна отправка."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix='onwatch_h_')
        self.db = Storage(os.path.join(self.tmpdir, 'h.db'))
        self.db.add_online_watch(555000111, 'target', 'привет', 1, -100)
        self.row = self.db.get_online_watch_by_target(555000111)
        self.client = FakeClient()
        self.ns = load_watch(db=self.db, client=self.client, notify=FakeNotify())

    def tearDown(self):
        try:
            self.db.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _call(self, fn, e):
        asyncio.run(self.ns[fn](e))

    def test_typing_fires(self):
        self._call('onwatch_status_event',
                   ev(user_id=555000111, status=None, typing=object()))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'typing')

    def test_message_fires(self):
        self._call('onwatch_activity_msg', ev(sender_id=555000111,
                                              is_private=True))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'message')

    def test_message_in_group_ignored(self):
        # написал в общую группу: from_id атрибутируется, но караул по ЛС
        self._call('onwatch_activity_msg', ev(sender_id=555000111,
                                              is_private=False))
        self.assertEqual(len(self.client.sent), 0)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 0)

    def test_typing_in_group_ignored(self):
        # UpdateChatUserTyping приходит с отрицательным chat_id
        self._call('onwatch_status_event',
                   ev(user_id=555000111, status=None, typing=object(),
                      chat_id=-100123456))
        self.assertEqual(len(self.client.sent), 0)

    def test_read_receipt_fires_in_pm_only(self):
        self._call('onwatch_read_receipt', ev(chat_id=555000111))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'read')
        # сообщение в группе (chat_id < 0) нельзя атрибутировать цели
        self.db.touch_online_watch_fired(row['id'], time.time() - 601)
        self._call('onwatch_read_receipt', ev(chat_id=-100123456))
        self.assertEqual(len(self.client.sent), 1)

    def test_owner_and_untracked_ignored(self):
        self._call('onwatch_activity_msg', ev(sender_id=1))            # OWNER_ID
        self._call('onwatch_activity_msg', ev(sender_id=555000999))    # не трекан
        self._call('onwatch_status_event', ev(user_id=1, status=None, typing=object()))
        self.assertEqual(len(self.client.sent), 0)

    def test_status_event_offline_to_recently_fires(self):
        # offline → recently = вход в сеть, которого сервер не показал
        self.db.update_online_watch_status(self.row['id'], 'offline')
        self._call('onwatch_status_event',
                   ev(user_id=555000111, status=UserStatusRecently(), typing=None))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'update')
        self.assertEqual(row['last_status'], 'recently')

    def test_status_event_flicker_sends_and_logs_session(self):
        # статус не менялся (offline → offline), но was_online свежий:
        # отправка + строка в истории сессий
        self.db.update_online_watch_status(self.row['id'], 'offline')
        wo = int(time.time()) - 5
        status = UserStatusOffline(was_online=wo)
        self._call('onwatch_status_event',
                   ev(user_id=555000111, status=status, typing=None))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'update-flicker')
        logs = self.db.get_online_watch_log(555000111, 0)
        flicker_rows = [l for l in logs if l['status'] == 'online']
        self.assertTrue(flicker_rows, 'flicker-сессия не попала в лог')
        self.assertEqual(flicker_rows[0]['ts'], wo)

    def test_duplicate_activity_within_cooldown_deduped(self):
        self._call('onwatch_status_event',
                   ev(user_id=555000111, status=None, typing=object()))
        self._call('onwatch_activity_msg', ev(sender_id=555000111))
        self._call('onwatch_read_receipt', ev(chat_id=555000111))
        # три канала активности в пределах кулдауна → одна отправка
        self.assertEqual(len(self.client.sent), 1)


if __name__ == '__main__':
    unittest.main()
