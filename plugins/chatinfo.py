# -*- coding: utf-8 -*-
from collections import defaultdict

from telethon import events

from client import client
from core import db, owner_filter, respond
import ui


from telethon.tl.types import ChannelParticipantsAdmins, ChannelParticipantsBots


@client.on(events.NewMessage(pattern=r'!chatinfo$', func=owner_filter))
async def chatinfo_cmd(e):

    chat = await e.get_chat()
    name = getattr(chat, 'title', None) or f"{getattr(chat, 'first_name', '')} {getattr(chat, 'last_name', '')}".strip()
    uname = getattr(chat, 'username', None)
    members = getattr(chat, 'participants_count', None)
    lines = [
        ui.kv("Название", f"**{name}**", "📛"),
        ui.kv("ID", f"`{e.chat_id}`", "🆔"),
        ui.kv("Username", f"@{uname}" if uname else "нет", "🔖"),
        ui.kv("Тип", f"`{type(chat).__name__}`", "👥"),
    ]
    if members:
        lines.append(ui.kv("Участников", f"`{members}`", "👤"))
    await respond(e, ui.card("📊 Чат", "\n".join(lines)))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!members$', func=owner_filter))
async def members_cmd(e):

    try:
        p = await client.get_participants(e.chat_id)
        bots = sum(1 for x in p if x.bot)
        await respond(e, ui.card("👥 Участники",
                                 ui.rows(("Всего", f"`{len(p)}`"),
                                         ("Людей", f"`{len(p) - bots}`", "👤"),
                                         ("Ботов", f"`{bots}`", "🤖"))))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Участники"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!admins$', func=owner_filter))
async def admins_cmd(e):

    try:
        admins = await client.get_participants(e.chat_id, filter=ChannelParticipantsAdmins())
        lines = []
        for a in admins[:25]:
            name = f"{a.first_name or ''} {a.last_name or ''}".strip()
            lines.append(f"• {name} — {'@' + a.username if a.username else '`' + str(a.id) + '`'}")
        await respond(e, ui.card(f"👑 Администраторы ({len(admins)})", "\n".join(lines) or "—"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Администраторы"))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!top(?:\s+(\d+))?$', func=owner_filter))
async def top_cmd(e):

    limit = int(e.pattern_match.group(1) or 200)
    await respond(e, ui.card("⏳ Топ активных", "анализирую сообщения…"))
    cnt, names = defaultdict(int), {}
    async for msg in client.iter_messages(e.chat_id, limit=limit):
        if msg.sender_id:
            cnt[msg.sender_id] += 1
            if msg.sender_id not in names:
                s = await msg.get_sender()
                if s:
                    n = f"{getattr(s, 'first_name', '') or ''} {getattr(s, 'last_name', '') or ''}".strip()
                    names[msg.sender_id] = n or str(msg.sender_id)
    top = sorted(cnt.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    lines = [f"🏆 **Топ активных** (из {limit} сообщ.)\n"]
    for i, (uid, c) in enumerate(top):
        lines.append(f"{medals[i]} {names.get(uid, uid)} — `{c}` сообщ.")
    await respond(e, ui.card("🏆 Топ активных",
                             f"из `{limit}` сообщ.\n\n" + "\n".join(lines[1:])))
    db.bump_stat('cmds')


@client.on(events.NewMessage(pattern=r'!bots$', func=owner_filter))
async def bots_cmd(e):

    try:
        bots = await client.get_participants(e.chat_id, filter=ChannelParticipantsBots())
        lines = []
        for b in bots[:20]:
            lines.append(f"• @{b.username or b.id}")
        await respond(e, ui.card(f"🤖 Боты в чате ({len(bots)})", "\n".join(lines) or "—"))
    except Exception as ex:
        await respond(e, ui.err(f"{ex}", title="Боты"))
    db.bump_stat('cmds')
