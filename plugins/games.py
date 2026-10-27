# -*- coding: utf-8 -*-
import asyncio
import base64
import datetime
import hashlib
import json
import os
import random
import re
import string
import time
import uuid

import aiohttp
from telethon import events
from telethon.errors import FloodWaitError, MessageNotModifiedError

from client import client
from config import OWNER_ID, MEDIA_DIR, MAX_FILE_SIZE_MB, MAX_COOLDOWN_ENTRIES, logger
from core import (
    db, state, owner_filter, respond, command_cooldown, reply_cooldown,
    _get_translator,
)
from helpers import (
    resolve_entity, entity_label, build_status_card,
    safe_flood, shadow_delete_msg,
)
from utils import fmt_time, progress_bar, safe_eval, caesar, morse_enc, gen_pwd, vigenere
from downloaders import run_download, send_and_clean
from notifier import notify
import ui

_log = logger

from telethon.tl.types import InputMediaDice


@client.on(events.NewMessage(pattern=r'!dice$', func=owner_filter))
async def dice_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎲'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!dart$', func=owner_filter))
async def dart_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎯'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!basket$', func=owner_filter))
async def basket_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🏀'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!football$', func=owner_filter))
async def football_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('⚽'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!bowling$', func=owner_filter))
async def bowling_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎳'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!casino$', func=owner_filter))
async def casino_cmd(e):

    await e.delete()
    await client.send_message(e.chat_id, file=InputMediaDice('🎰'))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!coin$', func=owner_filter))
async def coin_cmd(e):

    sides = ["Орёл 🦅", "Решка 💰"]
    r = random.choice(sides)
    flips = random.randint(3, 9)
    await respond(e, ui.card("🪙 Монета",
                             ui.rows(("Бросков", f"`{flips}`"),
                                     ("Результат", f"**{r}**"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!rand(?:\s+(-?\d+)(?:\s+(-?\d+))?)?$', func=owner_filter))
async def rand_cmd(e):

    g = e.pattern_match
    a, b = g.group(1), g.group(2)
    if a and b:
        lo, hi = sorted([int(a), int(b)])
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", f"`{lo}` … `{hi}`"),
                                         ("Результат", f"**{random.randint(lo, hi)}**"))))
    elif a:
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", f"`1` … `{a}`"),
                                         ("Результат", f"**{random.randint(1, int(a))}**"))))
    else:
        await respond(e, ui.card("🎲 Случайное число",
                                 ui.rows(("Диапазон", "`1` … `100`"),
                                         ("Результат", f"**{random.randint(1, 100)}**"))))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!8ball(?:\s+(.+))?$', func=owner_filter))
async def eightball_cmd(e):

    ANSWERS = {
        'pos': [
            ("Определённо да", "✅", "Вселенная согласна с тобой."),
            ("Без сомнений", "💯", "Это решено раньше, чем ты спросил."),
            ("Скорее всего да", "👍", "Всё складывается в твою пользу."),
            ("Хорошие перспективы", "🌟", "Будущее выглядит светлым."),
            ("Знаки говорят «да»", "🔮", "Мистические силы на твоей стороне."),
            ("Всё указывает на «да»", "💫", "Судьба уже всё решила."),
            ("Да, и поскорее", "🚀", "Не медли — действуй прямо сейчас."),
            ("Абсолютно точно", "🏆", "Лучшего ответа не существует."),
            ("Это неизбежно", "⚡", "Ничто не остановит это."),
            ("Да, если сделаешь шаг", "🦶", "Действие — ключ к результату."),
            ("Вселенная шепчет: да", "🌌", "Даже звёзды кивают."),
            ("Смело иди вперёд", "🎯", "Ты уже знал ответ — я лишь подтверждаю."),
        ],
        'neu': [
            ("Пока не ясно", "🤔", "Туман будущего слишком густой."),
            ("Спроси позже", "⏰", "Момент ещё не настал."),
            ("Не могу предсказать", "🌫", "Слишком много переменных."),
            ("Сосредоточься и повтори", "🧘", "Твой разум мешает ответу."),
            ("Лучше не рассказывать", "🤫", "Некоторые тайны лучше хранить."),
            ("Трудно сказать", "😶", "Даже я не всесилен."),
            ("Возможно, но не сейчас", "🌙", "Подожди подходящего момента."),
            ("Ответ где-то рядом", "🔭", "Смотри внимательнее вокруг себя."),
        ],
        'neg': [
            ("Мой ответ — нет", "🚫", "Прими это спокойно."),
            ("Перспективы не очень", "😕", "Стоит пересмотреть планы."),
            ("Весьма сомнительно", "🙄", "Интуиция говорит «осторожно»."),
            ("Точно нет", "💀", "Даже не думай об этом."),
            ("Не рассчитывай", "❌", "Лучше найди другой путь."),
            ("Категорически нет", "🔴", "Вселенная против."),
            ("Всё против этого", "⛈", "Сейчас не лучшее время."),
            ("Откажись от идеи", "🗑", "Это дорога в никуда."),
            ("Шансы ничтожны", "🎰", "Даже удача отвернулась."),
        ],
    }
    question = (e.pattern_match.group(1) or '').strip()
    spin = ["🎱", "🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘", "🎱"]
    msg = await respond(e, "🎱 Шар вращается...")
    for frame in spin:
        await msg.edit(f"{frame} Шар вращается...")
        await asyncio.sleep(0.15)
    pool_key = random.choices(['pos', 'neu', 'neg'], weights=[38, 27, 35])[0]
    answer, emoji, comment = random.choice(ANSWERS[pool_key])
    color = {"pos": "🟢", "neu": "🟡", "neg": "🔴"}[pool_key]
    label = {"pos": "ПОЗИТИВНЫЙ", "neu": "НЕЙТРАЛЬНЫЙ", "neg": "НЕГАТИВНЫЙ"}[pool_key]
    confidence = random.randint(55, 99)
    bar = progress_bar(confidence, 100, 10)
    q_line = f"❓ _{question}\n\n" if question else ""
    await msg.edit(
        ui.card("🎱 Магический шар",
                f"{q_line}"
                f"{emoji}  **{answer}**\n"
                f"💬 _{comment}_\n\n"
                f"{color} {label}\n"
                f"[{bar}] **{confidence}%** уверенности")
    )
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!rps(?:\s+(.+))?$', func=owner_filter))
async def rps_cmd(e):

    MAP = {'к': '🪨 Камень', 'камень': '🪨 Камень', 'н': '✂️ Ножницы', 'ножницы': '✂️ Ножницы', 'б': '📄 Бумага', 'бумага': '📄 Бумага'}
    BOT = ['🪨 Камень', '✂️ Ножницы', '📄 Бумага']
    WIN = {'🪨 Камень': '✂️ Ножницы', '✂️ Ножницы': '📄 Бумага', '📄 Бумага': '🪨 Камень'}
    arg = (e.pattern_match.group(1) or '').lower().strip()
    if not arg or arg not in MAP:
        await respond(e, ui.card("✊✌️🖐 КНБ",
                                 "`!rps камень` / `ножницы` / `бумага`",
                                 hint="Коротко: `к`, `н`, `б`"))
        return
    uc, bc = MAP[arg], random.choice(BOT)
    if uc == bc:
        res = "🤝 **Ничья!**"
    elif WIN[uc] == bc:
        res = "🏆 **Ты победил!**"
    else:
        res = "💀 **Бот победил!**"
    await respond(e, ui.card("✊✌️🖐 КНБ",
                             ui.rows(("Ты", uc, "👤"), ("Бот", bc, "🤖")),
                             footer=res))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!slot$', func=owner_filter))
async def slot_cmd(e):

    SYM = ['🍒', '🍋', '🍊', '🍇', '🍉', '⭐', '💎', '7️⃣', '🔔', '🍀']
    msg = await respond(e, "🎰 [ ▓ | ▓ | ▓ ]")
    for _ in range(4):
        s = [random.choice(SYM) for _ in range(3)]
        await msg.edit(f"🎰 [ {s[0]} | {s[1]} | {s[2]} ]")
        await asyncio.sleep(0.3)
    s = [random.choice(SYM) for _ in range(3)]
    if s[0] == s[1] == s[2]:
        res = "💰💰💰 **ДЖЕКПОТ!**" if s[0] in ('💎', '7️⃣') else "🎊 **Выигрыш! Три одинаковых!**"
    elif len(set(s)) < 3:
        res = "😅 Почти! Два одинаковых — ещё раз!"
    else:
        res = "💸 Не повезло. Попробуй снова!"
    await msg.edit(ui.card("🎰 Слот-машина",
                           f"[ {s[0]} | {s[1]} | {s[2]} ]\n\n{res}"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!lucky$', func=owner_filter))
async def lucky_cmd(e):

    pct = random.randint(0, 100)
    bar = progress_bar(pct, 100, 12)
    tips = {
        (90, 100): "🌟 АБСОЛЮТНАЯ УДАЧА! Сегодня твой день!",
        (70, 89): "🍀 Очень удачный день — действуй!",
        (50, 69): "😊 Неплохо — удача на твоей стороне",
        (30, 49): "😐 Средний день, будь осторожен",
        (10, 29): "😬 Не лучший день...",
        (0, 9): "💀 Сиди дома и не высовывайся!",
    }
    msg = next(v for (a, b), v in tips.items() if a <= pct <= b)
    await respond(e, ui.card("🔮 Индекс удачи",
                             f"[{bar}] **{pct}%**\n\n{msg}"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!choose (.+)', func=owner_filter))
async def choose_cmd(e):

    raw = e.pattern_match.group(1)
    opts = [o.strip() for o in re.split(r'[,|/]', raw) if o.strip()]
    if len(opts) < 2:
        await respond(e, ui.usage("!choose пицца, суши, бургер",
                                  title="Случайный выбор"))
        return
    winner = random.choice(opts)
    listed = "\n".join(f"{'➡️' if o == winner else '  •'} {o}" for o in opts)
    await respond(e, ui.card(f"🤔 Выбираю из {len(opts)} вариантов",
                             f"{listed}\n\n✅ **Выбор: {winner}**"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!quiz$', func=owner_filter))
async def quiz_cmd(e):

    QUESTIONS = [
        ("Столица Австралии?", ["Сидней", "Мельбурн", "Канберра", "Перт"], 2),
        ("Сколько планет в Солнечной системе?", ["7", "8", "9", "10"], 1),
        ("Кто написал «Гамлета»?", ["Диккенс", "Толстой", "Шекспир", "Гёте"], 2),
        ("Химический символ золота?", ["Go", "Gd", "Au", "Ag"], 2),
        ("Год основания Google?", ["1996", "1998", "2000", "2002"], 1),
        ("Самая длинная река мира?", ["Амазонка", "Янцзы", "Нил", "Конго"], 2),
        ("Сколько байт в килобайте?", ["512", "1024", "2048", "4096"], 1),
        ("Скорость света (км/с)?", ["150 000", "300 000", "450 000", "600 000"], 1),
    ]
    q, opts, ans_idx = random.choice(QUESTIONS)
    letters = ['A', 'B', 'C', 'D']
    opts_text = "\n".join(f"{letters[i]}. {o}" for i, o in enumerate(opts))
    correct = f"{letters[ans_idx]}. {opts[ans_idx]}"
    await respond(e,
        ui.card("🧠 Викторина",
                f"_{q}_\n\n{opts_text}\n\n"
                f"||✅ Ответ: **{correct}**||")
    )
    db.bump_stat('cmds')
