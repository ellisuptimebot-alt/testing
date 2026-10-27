# Синхронизация справки (help_data) с реальными командами в плагинах.
# Разбираем исходники через ast/regex — без импорта проекта (нет зависимости
# от .env, Telethon и синглтона БД).

import ast
import os
import re
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_literals(rel, names):
    """Достаёт именованные константы из модуля как literal (без импорта)."""
    with open(os.path.join(ROOT, rel), encoding='utf-8') as f:
        tree = ast.parse(f.read())
    out = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in names:
                    out[tgt.id] = ast.literal_eval(node.value)
    return out


def _plugin_command_patterns():
    """Все !команды из pattern=... в плагинах (первый токен команды)."""
    found = set()
    plugins_dir = os.path.join(ROOT, 'plugins')
    pat = re.compile(r"pattern=r['\"](?:\^)?!([a-z0-9_]+)")
    for fn in os.listdir(plugins_dir):
        if not fn.endswith('.py'):
            continue
        with open(os.path.join(plugins_dir, fn), encoding='utf-8') as f:
            for m in pat.finditer(f.read()):
                found.add(m.group(1))
    return found


class TestHelpSync(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        lits = _load_literals('help_data.py', {'CMD_DESCS', 'COMMANDS_LIST'})
        cls.cmd_descs = lits['CMD_DESCS']
        cls.commands_list = lits['COMMANDS_LIST']
        cls.plugin_cmds = _plugin_command_patterns()

    def test_every_listed_command_has_description(self):
        missing = []
        for cat, cmds in self.commands_list.items():
            for c in cmds:
                key = c.lstrip('!')
                if key not in self.cmd_descs:
                    missing.append(f'{c} (раздел {cat})')
        self.assertFalse(missing, f"В COMMANDS_LIST есть команды без описания в CMD_DESCS: {missing}")

    def test_every_pattern_command_documented(self):
        undocumented = sorted(self.plugin_cmds - set(self.cmd_descs))
        self.assertFalse(undocumented,
                         f"Команды в плагинах отсутствуют в справке help_data: {undocumented}")

    def test_no_duplicate_commands_in_lists(self):
        seen = set()
        dupes = []
        for cat, cmds in self.commands_list.items():
            for c in cmds:
                if c in seen:
                    dupes.append(c)
                seen.add(c)
        self.assertFalse(dupes, f"Команды задублированы в разделах: {dupes}")

    def test_new_features_documented(self):
        # регрессия этого рефакторинга: новые возможности должны быть в справке
        self.assertIn('ytupdate', self.cmd_descs)
        self.assertIn('!ytupdate', self.commands_list.get('youtube', []))
        self.assertIn('voice', self.cmd_descs['audio']['syntax'])
        self.assertIn('remind', self.cmd_descs)
        self.assertIn('trhelp', self.cmd_descs)


if __name__ == '__main__':
    unittest.main()
