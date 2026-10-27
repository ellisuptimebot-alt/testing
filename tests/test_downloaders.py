# Тесты маршрутизации загрузчика: ключ платформы, определение медиа, YT id.

import os
import re
import unittest

os.environ.setdefault('API_ID', '1')
os.environ.setdefault('API_HASH', 'test')
os.environ.setdefault('OWNER_ID', '1')

from downloaders import _platform_key, _yt_id, _is_direct_media  # noqa: E402


class TestPlatformRouting(unittest.TestCase):
    def test_youtube(self):
        self.assertEqual(_platform_key('https://www.youtube.com/watch?v=dQw4w9WgXcQ'), 'youtube')
        self.assertEqual(_platform_key('https://youtu.be/dQw4w9WgXcQ'), 'youtube')

    def test_tiktok(self):
        self.assertEqual(_platform_key('https://www.tiktok.com/@u/video/123'), 'tiktok')

    def test_instagram(self):
        self.assertEqual(_platform_key('https://www.instagram.com/p/ABC/'), 'instagram')

    def test_pinterest(self):
        self.assertEqual(_platform_key('https://www.pinterest.com/pin/1/'), 'pinterest')
        self.assertEqual(_platform_key('https://pin.it/xyz'), 'pinterest')

    def test_generic(self):
        self.assertEqual(_platform_key('https://example.com/video.mp4'), 'generic')


class TestYtId(unittest.TestCase):
    def test_watch_url(self):
        self.assertEqual(_yt_id('https://www.youtube.com/watch?v=dQw4w9WgXcQ'), 'dQw4w9WgXcQ')

    def test_short_url(self):
        self.assertEqual(_yt_id('https://youtu.be/dQw4w9WgXcQ?t=5'), 'dQw4w9WgXcQ')

    def test_no_id(self):
        self.assertIsNone(_yt_id('https://example.com/'))


class TestDirectMedia(unittest.TestCase):
    def test_direct_extensions(self):
        self.assertTrue(_is_direct_media('https://example.com/v.mp4'))
        self.assertTrue(_is_direct_media('https://example.com/a.webm?x=1'))
        self.assertTrue(_is_direct_media('https://cdn.example.com/track.mp3'))

    def test_pages_are_not_direct(self):
        self.assertFalse(_is_direct_media('https://example.com/page'))
        self.assertFalse(_is_direct_media('https://youtube.com/watch?v=dQw4w9WgXcQ'))


if __name__ == '__main__':
    unittest.main()
