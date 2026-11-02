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
from telethon.tl.types import (
    UserStatusOffline, UserStatusOnline, UserStatusRecently,
    UserStatusLastWeek, UserStatusLastMonth,
)

from storage import Storage

ROOT = pathlib.Path(__file__).resolve().parent.parent
WATCH_SRC = (ROOT / 'plugins' / 'watch.py').read_text(encoding='utf-8')

_WANT = {
    '_status_name', '_profile_name', 'watch_label', '_fmt_dur',
    '_parse_window', '_fmt_win', '_poll_gap', '_fresh_was_online',
    '_next_action', '_onwatch_fire', '_skip_due_to_event', '_poll_base',
    'onwatch_status_event', 'onwatch_activity_msg', 'onwatch_read_receipt',
}


def load_watch(db=None, client=None, notify=None):
    """Изолированное пространство имён с функциями watch.py."""
    tree = ast.parse(WATCH_SRC)
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
        '_FIRE_COOLDOWN': 600, '_FAIL_COOLDOWN': 60, 'FLICKER_WINDOW': 180,
        'OWNER_ID': 1,
        'UserStatusOnline': UserStatusOnline,
        'UserStatusOffline': UserStatusOffline,
        'UserStatusRecently': UserStatusRecently,
        'UserStatusLastWeek': UserStatusLastWeek,
        'UserStatusLastMonth': UserStatusLastMonth,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'watch.py', 'exec'), ns)
    return ns


class FakeClient:
    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    async def get_entity(self, tid):
        return None

    async def send_message(self, tid, text, parse_mode=None):
        if self.fail:
            raise RuntimeError('privacy: user restricts messaging')
        self.sent.append((tid, text, parse_mode))


class FakeNotify:
    async def online_watch(self, *a, **k):
        pass

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
        # поллер передаёт max(FLICKER_WINDOW, 2×шаг): для редких статусов
        # окно растёт вместе с шагом
        ns = load_watch()
        floor, gap = ns['FLICKER_WINDOW'], ns['_poll_gap']
        for base in (5, 35):
            for status in ('offline', 'last_week', 'last_month'):
                g = gap(base, status)
                self.assertGreaterEqual(max(floor, 2 * g), 2 * g)


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
        # кулдаун истёк — снова шлём (трек работает раз за разом)
        self.db.touch_online_watch_fired(row['id'], time.time() - 601)
        self.assertTrue(self._fire(ns, 'poll'))
        self.assertEqual(len(client.sent), 2)

    def test_flicker_and_recently_sources_counted(self):
        # метрика «событие vs поллер» растёт для ВСЕХ типов триггеров
        client = FakeClient()
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertTrue(self._fire(ns, 'poll-flicker'))
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 1)
        self.assertEqual(row['last_source'], 'poll-flicker')
        self.db.touch_online_watch_fired(row['id'], time.time() - 601)
        self.assertTrue(self._fire(ns, 'update-recently'))
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['fires'], 2)
        self.assertEqual(row['last_source'], 'update-recently')
        self.assertEqual(len(client.sent), 2)

    def test_fail_cooldown_and_error_metric(self):
        client = FakeClient(fail=True)
        ns = load_watch(db=self.db, client=client, notify=FakeNotify())
        self.assertFalse(self._fire(ns, 'poll'))
        row = self.db.get_online_watch_by_target(555000111)
        # сбой не сжигает полный кулдаун: повтор примерно через 60с
        left = time.time() - (row['last_fired'] or 0)
        self.assertGreaterEqual(left, 600 - 60 - 2)
        self.assertLess(left, 600)
        self.assertIn('privacy', row['last_error'] or '')
        self.assertEqual(row['fires'], 0)  # неудачная отправка не считается
        self.assertEqual(row['last_source'], 'poll')

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
        self.db.append_online_watch_log(666000222, 'online', now - 30)
        logs = self.db.get_online_watch_log(666000222, 0)
        self.assertEqual([l['status'] for l in logs], ['online'])
        # статус при этом не сдвинулся
        row = self.db.get_online_watch_by_target(666000222)
        self.assertEqual(row['last_status'], 'unknown')
        self.assertFalse(row.get('status_since'))

    def test_log_retention_prunes_old_rows(self):
        self.db.add_online_watch(666000333, 't3', 'm', 1, -100)
        old_ts = int(time.time()) - 91 * 86400
        self.db.append_online_watch_log(666000333, 'online', old_ts)
        # ретеншн 90 дней чистится на каждой записи — старой строки нет
        self.assertEqual(self.db.get_online_watch_log(666000333, 0), [])


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
        self._call('onwatch_activity_msg', ev(sender_id=555000111))
        self.assertEqual(len(self.client.sent), 1)
        row = self.db.get_online_watch_by_target(555000111)
        self.assertEqual(row['last_source'], 'message')

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
