# -*- coding: utf-8 -*-

from telethon import events

from client import client
from core import db, state, owner_filter, respond
from utils import fmt_time
import ui



@client.on(events.NewMessage(pattern=r'!afk(?:\s+(.+))?$', func=owner_filter))
async def afk_cmd(e):

    reason = (e.pattern_match.group(1) or '').strip()
    state.set_afk(reason)
    r = ui.kv("Причина", f"_{reason}_", "📝") if reason else ui.kv("Причина", "не указана", "📝")
    await respond(e, ui.card("😴 AFK включён", r,
                             hint="`!unafk` — выключить"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!unafk$', func=owner_filter))
async def unafk_cmd(e):

    dur = state.clear_afk()
    if dur is not None:
        await respond(e, ui.card("☀️ AFK выключен",
                                 ui.rows(("Отсутствовал", f"_{fmt_time(dur)}_"))))
    else:
        await respond(e, ui.info("AFK не был включён", title="AFK"))
    db.bump_stat('cmds')
