import unittest

from utils import safe_eval


class TestSafeEval(unittest.TestCase):
    def test_basic_arithmetic(self):
        self.assertEqual(safe_eval('2+2*2'), 6)
        self.assertEqual(safe_eval('(10-4)/3'), 2.0)
        self.assertEqual(safe_eval('7 % 3'), 1)
        self.assertEqual(safe_eval('-5 + 2'), -3)

    def test_floats_and_rounding(self):
        self.assertEqual(safe_eval('9/2'), 4.5)
        self.assertEqual(safe_eval('1/3'), 0.3333333333)  # round(x, 10)

    def test_allowed_functions(self):
        self.assertEqual(safe_eval('sqrt(16)'), 4.0)
        self.assertEqual(safe_eval('abs(-7)'), 7)
        self.assertEqual(safe_eval('factorial(5)'), 120)
        self.assertAlmostEqual(safe_eval('sin(0)'), 0.0)

    def test_constants(self):
        self.assertAlmostEqual(safe_eval('pi'), 3.141592653589793)
        self.assertAlmostEqual(safe_eval('e'), 2.718281828459045)

    def test_big_exponent_ok(self):
        self.assertEqual(safe_eval('2**10'), 1024)

    def test_attribute_access_rejected(self):
        self.assertIsNone(safe_eval('(1).__class__'))

    def test_import_rejected(self):
        self.assertIsNone(safe_eval("__import__('os')"))

    def test_open_rejected(self):
        self.assertIsNone(safe_eval("open('/etc/passwd')"))

    def test_names_rejected(self):
        self.assertIsNone(safe_eval('eval(1)'))
        self.assertIsNone(safe_eval('exec(1)'))
        self.assertIsNone(safe_eval('globals()'))

    def test_string_ops_rejected(self):
        # вызов недоступной функции не входит в белый список —
        # TypeError внутри _SAFE_OPS ведёт к None, а не к выполнению
        self.assertIsNone(safe_eval("'a'+str(1)"))

    def test_division_by_zero_is_none(self):
        self.assertIsNone(safe_eval('1/0'))

    def test_garbage_is_none(self):
        self.assertIsNone(safe_eval('2+'))
        self.assertIsNone(safe_eval(''))


if __name__ == '__main__':
    unittest.main()
