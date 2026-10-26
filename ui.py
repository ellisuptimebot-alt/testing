# ui.py — единое оформление ответов бота (стиль «карточка»).

import re
import html as _html
#
# Структура карточки:
#   **📊 Заголовок**
#   ────────────────
#   поля «Ключ: значение»
#   ────────────────
#   💡 подсказка (необязательно)
#
# Всё генерится под parse_mode='md' (Markdown v1 Telethon):
# жирный — **текст**, код — `текст`.

SEP = '─' * 16


def card(title: str, body: str = '', hint: str = '', footer: str = '') -> str:
    """Готовая карточка: заголовок → разделитель → поля → подсказка."""
    parts = [f"**{title}**", SEP]
    body = (body or '').strip('\n')
    if body:
        parts.append(body)
    text = "\n".join(parts)
    tail = []
    if footer:
        tail.append(footer.strip('\n'))
    if hint:
        tail.append(f"💡 {hint}")
    if tail:
        text += "\n" + SEP + "\n" + "\n".join(tail)
    return text


def kv(label: str, value, icon: str = '') -> str:
    """Одно поле: `**Имя**: значение` (с опциональным эмодзи-иконкой)."""
    prefix = f"{icon} " if icon else ''
    return f"{prefix}**{label}**: {value}"


def rows(*pairs) -> str:
    """Поля из пар ('Имя', 'X') или троек ('Имя', 'X', '👤')."""
    out = []
    for p in pairs:
        if len(p) == 3:
            out.append(kv(p[0], p[1], p[2]))
        else:
            out.append(kv(p[0], p[1]))
    return "\n".join(out)


def ok(body: str = '', title: str = 'Готово', hint: str = '') -> str:
    return card(f"✅ {title}", body, hint)


def err(body: str = '', title: str = 'Ошибка', hint: str = '') -> str:
    return card(f"❌ {title}", body, hint)


def warn(body: str = '', title: str = 'Внимание', hint: str = '') -> str:
    return card(f"⚠️ {title}", body, hint)


def info(body: str = '', title: str = 'Информация', hint: str = '') -> str:
    return card(f"ℹ️ {title}", body, hint)


def usage(*syntaxes: str, title: str = 'Использование') -> str:
    """Подсказка по синтаксису: каждая строка — вариант вызова в коде."""
    body = "\n".join(f"`{s}`" for s in syntaxes)
    return card(f"ℹ️ {title}", body)


# ── Экспорт карточек в HTML (Bot API внешнего бота) ──
# Карточки пишутся в разметке Telethon-MD (**жирный**, `код`, _курсив_,
# ||спойлер||, [текст](url)); Bot API её не понимает — конвертируем здесь.

def to_html(text) -> str:
    s = '' if text is None else str(text)
    s = _html.escape(s, quote=False)
    s = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', s, flags=re.S)
    s = re.sub(r'`([^`]+)`', r'<code>\1</code>', s)
    s = re.sub(r'\|\|(.+?)\|\|', r'<span class="tg-spoiler">\1</span>', s, flags=re.S)
    s = re.sub(r'(?<!\w)_(.+?)_(?!\w)', r'<i>\1</i>', s, flags=re.S)
    s = re.sub(r'\[([^\]]+)\]\((https?://[^)]+)\)', r'<a href="\2">\1</a>', s)
    return s
