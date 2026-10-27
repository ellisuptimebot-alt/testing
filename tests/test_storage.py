import os
import shutil
import tempfile
import time
import unittest


class TestStorage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp(prefix='ubtest_')
        # Storage — синглтон: первый вызов в процессе фиксирует путь.
        # Тесты должны выполняться в чистом процессе (unittest discover).
        from storage import Storage
        cls.db = Storage(os.path.join(cls.tmpdir, 'test.db'))
        assert cls.db.db_path.endswith('test.db'), \
            f"Синглтон уже привязан к {cls.db.db_path} — запусти тесты отдельным процессом"

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.conn.close()
        except Exception:
            pass
        shutil.rmtree(cls.tmpdir, ignore_errors=True)

    # ── Напоминания (персистентный !remind) ──

    def test_reminder_lifecycle(self):
        now = int(time.time())
        rid = self.db.add_reminder(111, 'проверить чайник', now + 60)
        pending = self.db.get_pending_reminders()
        self.assertTrue(any(r['id'] == rid for r in pending))

        due = self.db.get_due_reminders(now + 120)
        self.assertTrue(any(r['id'] == rid for r in due))

        self.db.del_reminder(rid)
        pending = self.db.get_pending_reminders()
        self.assertFalse(any(r['id'] == rid for r in pending))

    def test_due_reminders_ordering_and_cutoff(self):
        now = int(time.time())
        a = self.db.add_reminder(222, 'раньше', now - 10)
        b = self.db.add_reminder(222, 'позже', now + 3600)
        try:
            due = [r['id'] for r in self.db.get_due_reminders(now)]
            self.assertIn(a, due)
            self.assertNotIn(b, due)
        finally:
            self.db.del_reminder(a)
            self.db.del_reminder(b)

    # ── Todos ──

    def test_todo_lifecycle(self):
        tid = self.db.add_todo('купить молоко')
        try:
            todos = self.db.get_todos()
            self.assertTrue(any(t['id'] == tid and t['done'] == 0 for t in todos))
            self.db.update_todo(tid, done=True)
            todos = self.db.get_todos()
            self.assertTrue(any(t['id'] == tid and t['done'] == 1 for t in todos))
        finally:
            self.db.del_todo(tid)
        todos = self.db.get_todos()
        self.assertFalse(any(t['id'] == tid for t in todos))

    # ── Stats ──

    def test_bump_and_read_stats(self):
        before = self.db.all_stats().get('test_cmds', 0)
        self.db.bump_stat('test_cmds', 3)
        after = self.db.all_stats().get('test_cmds', 0)
        self.assertEqual(after, before + 3)

    # ── Заметки ──

    def test_notes(self):
        self.db.set_note('тест-заметка', 'значение')
        try:
            self.assertEqual(self.db.get_note('тест-заметка'), 'значение')
            hits = self.db.search_notes('заметка')
            self.assertTrue(len(hits) >= 1)
        finally:
            self.db.del_note('тест-заметка')
        self.assertIsNone(self.db.get_note('тест-заметка'))


if __name__ == '__main__':
    unittest.main()
