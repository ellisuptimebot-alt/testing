# -*- coding: utf-8 -*-
import asyncio
import datetime
import random
import re
import time

from telethon import events
from telethon.errors import FloodWaitError

from client import client
from config import OWNER_ID, logger
from core import db, owner_filter, respond
from helpers import resolve_entity
from notifier import notify
import ui


_watch_err_logged = set()
# Расписание опроса делят поллер и !onwatchstatus (модульный уровень):
# _next_due — когда цель проверить снова, _last_poll — когда реально
# выполнялся последний GetUsers (для пропуска дублирующего опроса).
_next_due = {}
_last_poll = {}

from telethon import utils as tgutils
from telethon.tl.types import (
    Message, PeerUser, PhoneCallRequested, UpdateChannelUserTyping,
    UpdateChatUserTyping, UpdateEditMessage, UpdateMessageReactions,
    UpdatePhoneCall, UpdateStory, UpdateUserName, UpdateUserStatus,
    UpdateUserTyping, User, UserStatusOnline, UserStatusOffline,
    UserStatusRecently, UserStatusLastWeek, UserStatusLastMonth,
)


# Активность вне обычных событий (raw updates): смена имени/username, новая
# сторис, реакция на ваше сообщение, входящий звонок, правка сообщения в ЛС.
# Всё это бесплатно по API и означает «цель только что действовала».
_RAW_ACTIVITY_TYPES = (
    UpdateUserName, UpdateStory, UpdateMessageReactions,
    UpdatePhoneCall, UpdateEditMessage,
)

# Источники, которые пишутся строкой в историю сессий (!onwatchlog).
_ACTIVITY_LOG_SOURCES = frozenset({
    'typing', 'message', 'read',
    'update-profile', 'update-story', 'update-reaction',
    'update-call', 'update-edit',
})

_watch_count_logged = None
# Антиспам-кулдаун от последней ОТПРАВКИ (last_fired в БД): дребезг статуса
# offline→online→offline и повтор после сбоя. Основную роль «одно уведомление
# на непрерывную сессию» играет SESSION_TIMEOUT, а не этот таймер.
_FIRE_COOLDOWN = 120
# Сбой отправки (приватность, удалённый аккаунт) не должен сжигать полный
# кулдаун — следующая попытка уведомления раньше, через столько секунд.
_FAIL_COOLDOWN = 60
# Тишина дольше этого (ни typing, ни сообщений, ни галочек, ни смены статуса)
# закрывает сессию: следующая активность — новое уведомление.
_SESSION_TIMEOUT = 900
# Не чаще этого интервала продлеваем last_activity в БД: typing шлётся
# каждые ~5с, строку на каждый апдейт писать не нужно.
_SESSION_TOUCH_MIN = 30
# was_online (UserStatusOffline) свежее этого окна = «мигающая» сессия,
# пропущенная между опросами: человек был онлайн и уже вышел. Адаптивно
# расширяется до 2× шага опроса, но не выше FLICKER_WINDOW_MAX.
FLICKER_WINDOW = 180
FLICKER_WINDOW_MAX = 600
# Первый опрос после старта/реконнекта размазывается по джиттеру: окно
# ~0.5с на трек (потолок 60с), чтобы N треков не дали N GetUsers разом.
_START_JITTER_MAX = 60
# Флаги цикла поллера: соединение, прогрев кеша, последний prune истории.
_conn_up = True
_warmed = False
_last_prune = 0.0
_refresh_reason = 'старт'
# Как часто сторож соединения проверяет link (поллер спит до 35 с — слишком
# долго для первого входа после разрыва).
_CONN_CHECK = 2
# Флаппинг связи: не чаще одного батч-опроса за _REFRESH_MIN_GAP секунд.
_REFRESH_MIN_GAP = 20
# Неудачный прогрев (сеть/сессия): пауза и повтор, после N неудач — обычный
# поштучный опрос, чтобы не крутить get_users бесконечно.
_REFRESH_RETRY_DELAY = 15
_REFRESH_FALLBACK_AFTER = 3
# После отката на поштучный опрос батч пробуется снова не позже этого интервала:
# деградация временная, а не постоянная.
_REFRESH_FALLBACK_RETRY = 300
_last_refresh = 0.0
_refresh_fails = 0
_next_refresh_try = 0.0
_conn_warned = False
# Поколение расписания: растёт при сбросе (реконнект), чтобы поллер не
# продолжал цикл по устаревшим срокам и не опрашивал треки поштучно.
_schedule_gen = 0


def _status_name(status):
    """Ключ статуса: online / offline / recently / last_week / ... / unknown."""
    if isinstance(status, UserStatusOnline):
        return 'online'
    if isinstance(status, UserStatusOffline):
        return 'offline'
    if isinstance(status, UserStatusRecently):
        return 'recently'
    if isinstance(status, UserStatusLastWeek):
        return 'last_week'
    if isinstance(status, UserStatusLastMonth):
        return 'last_month'
    return 'unknown'


def _profile_name(ent=None, username=None):
    """Имя профиля (без markdown-символов), иначе @username, иначе ''."""
    if ent is not None:
        disp = re.sub(r'[*_`]', '', (tgutils.get_display_name(ent) or '').strip())
        if disp:
            return disp
        u = getattr(ent, 'username', None)
        if u:
            return f'@{u}'
    u = (username or '').strip().lstrip('@')
    return f'@{u}' if u else ''


def watch_label(target_id, username=None, ent=None):
    """«Имя Фамилия [`123`]» — цель так, как просят: имя профиля + ID."""
    name = _profile_name(ent, username) or f'id {target_id}'
    tid = getattr(ent, 'id', None) or target_id
    return f'**{name}** [`{tid}`]'


async def _onwatch_fire(w, source):
    """Сработал триггер караула → сообщение таргету + уведомление во внешнем боте.

    Сессионная модель: уведомление уходит ОДИН раз на непрерывную сессию.
    Пока now - last_activity <= SESSION_TIMEOUT (typing/сообщения/галочки
    и успешный fire продлевают таймер), новые триггеры только обновляют
    активность. Кулдаун _FIRE_COOLDOWN (120 с) — вторая линия против дребезга
    статуса и повторов после сбоя; при сбое таймер сессии НЕ открывается,
    чтобы следующий триггер повторил доставку."""
    fresh = db.get_online_watch_by_target(w.get('target_id'))
    row = fresh or w
    now = time.time()
    la = row.get('last_activity') or 0
    active = _session_active(la, now)
    if source.endswith('-recently'):
        # уход из «недавно»: если сессия была открыта — просто закрываем
        # (уведомление ушло при входе); если не видели — разовое уведомление
        # о пропущенной сессии, сессию не открываем
        if active:
            db.close_online_watch_session(row['id'])
            logger.debug(f"[ONWATCH] {row['target_id']}: уход из «недавно» — "
                         "сессия закрыта без уведомления")
            return False
    elif active:
        # открытая сессия: только продлеваем активность, без отправки
        if now - la >= _SESSION_TOUCH_MIN:
            db.touch_online_watch_session(row['id'], now)
        return False
    if now - (row.get('last_fired') or 0) < _FIRE_COOLDOWN:
        return False
    # отметка — до await: между чтением last_fired и записью нет переключения
    # корутины, поэтому параллельный триггер (событие/поллер) увидит уже новый
    # кулдаун и не отправит дважды. Статус цели здесь не пишем — его записывает
    # вызывающий код, по фактическому статусу, до вызова этой функции.
    db.touch_online_watch_fired(row['id'], now)
    tid = row['target_id']
    uname = row.get('target_username') or ''
    ent = None
    try:
        ent = await client.get_entity(int(tid))
    except Exception:
        ent = None
    name = _profile_name(ent, uname) or f'id {tid}'
    plain = f"{name} [{tid}]"
    logger.info(f"[ONWATCH] сработал триггер {source}: {watch_label(tid, uname, ent)}")
    try:
        # по умолчанию parse_mode=None — текст уходит ровно как набран;
        # трек с флагом -m шлём с markdown-форматированием
        await client.send_message(tid, row['message'],
                                  parse_mode='md' if row.get('md') else None)
    except Exception as ex:
        # сбой не сжигает полный кулдаун — повтор через _FAIL_COOLDOWN
        db.touch_online_watch_fired(row['id'], now - _FIRE_COOLDOWN + _FAIL_COOLDOWN)
        db.mark_online_watch_fire(row['id'], source, ok=False, error=str(ex))
        logger.warning(f"[ONWATCH] не удалось отправить сообщение {plain}: {ex}")
        try:
            await notify.error_alert("ONWATCH", f"Не удалось отправить сообщение {plain}: {ex}")
        except Exception:
            pass
        return False
    db.mark_online_watch_fire(row['id'], source, ok=True)
    if not source.endswith('-recently'):
        # сессия открыта: следующие триггеры в пределах SESSION_TIMEOUT
        # только продлевают таймер ('-recently' не открывает — цель вышла)
        db.touch_online_watch_session(row['id'], now)
    if source in _ACTIVITY_LOG_SOURCES:
        # активность в ЛС — строкой в историю: для скрытого last-seen это
        # единственный видимый след сессии
        db.append_online_watch_log(tid, 'online', int(now), source=source)
    try:
        await notify.online_watch(uname, tid, row['message'], name=name,
                                  source=source)
    except Exception as ex:
        logger.warning(f"[ONWATCH] уведомление не отправлено: {ex}")
    return True


async def _apply_status(w, status, source='poll', base=None):
    """Один снимок статуса цели: лог смены, запись в БД и триггер караула.

    Общий путь для поллера, события UpdateUserStatus и принудительного опроса
    после реконнекта — логика переходов живёт здесь, а не в трёх копиях.
    Статус пишется ДО await: параллельный обработчик увидит свежее состояние
    и не отправит второе уведомление. base — шаг поллера, от него зависит
    окно flicker; у события окно фиксированное (FLICKER_WINDOW)."""
    tid = w['target_id']
    cur = _status_name(status)
    prev = w['last_status'] or 'unknown'
    wo_ts = _fresh_was_online(status,
                              window=_flicker_window(_poll_gap(base, cur))
                              if base else None)
    action = _next_action(prev, cur, wo_ts is not None)
    if cur != prev:
        since = w.get('status_since') or 0
        dur = f" (в статусе {_fmt_dur(int(time.time()) - since)})" if since else ''
        logger.info(f"[ONWATCH] {tid}: {prev} → {cur}{dur} [{action}] ({source})")
        _watch_err_logged.discard(tid)
    elif action != 'none':
        logger.info(f"[ONWATCH] {tid}: {action} (was_online свежий, статус {cur})")
    if action != 'none' and cur != 'unknown':
        db.update_online_watch_status(w['id'], cur)
    if action == 'fire':
        await _onwatch_fire(w, source)
    elif action == 'fire-recently':
        await _onwatch_fire(w, source + '-recently')
    elif action == 'fire-flicker':
        if await _onwatch_fire(w, source + '-flicker'):
            # сессия не меняла last_status — фиксируем её отдельной строкой,
            # иначе в !onwatchlog пропущенной сессии не видно
            db.append_online_watch_log(tid, 'online', int(wo_ts or time.time()),
                                       source='flicker')
    return action


def _mark_reconnect():
    """Связь была потеряна: после восстановления нужен внеплановый опрос всех
    треков — difference-апдейты приходят с задержкой, а вход цели мог быть
    пропущен. Расписание сбрасывается, прогрев выполнит поллер."""
    global _conn_up, _warmed, _refresh_reason, _schedule_gen
    _conn_up = True
    _warmed = False
    _refresh_reason = 'реконнект'
    _schedule_gen += 1
    _next_due.clear()


def _refresh_delay(reason, last_refresh, now=None):
    '''Пауза перед батч-опросом: при флаппинге связи (реконнект за реконнектом)
    не чаще одного _force_refresh за _REFRESH_MIN_GAP. Старт не задерживаем.'''
    if reason != 'реконнект' or not last_refresh:
        return 0.0
    now = time.time() if now is None else now
    return max(0.0, _REFRESH_MIN_GAP - (now - last_refresh))


async def connection_watcher():
    """Быстрый детект разрыва: поллер просыпается по расписанию (до 35 с),
    а вход после реконнекта важно увидеть сразу. Проверка link раз в 2 с
    почти ничего не стоит; сам опрос делает поллер батчем."""
    global _conn_up, _conn_warned
    while True:
        try:
            connected = client.is_connected()
            if connected and not _conn_up:
                _mark_reconnect()
                logger.info("[ONWATCH] соединение восстановлено — "
                            "запрошен внеплановый опрос")
            elif not connected:
                _conn_up = False
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            # сторож не должен умирать молча; в затяжном сбое не спамим лог —
            # одно предупреждение, дальше debug до первого успешного чека
            if not _conn_warned:
                _conn_warned = True
                logger.warning(f"[ONWATCH] conn watcher: {ex}")
            else:
                logger.debug(f"[ONWATCH] conn watcher: {ex}")
            await asyncio.sleep(5)
            continue
        _conn_warned = False
        await asyncio.sleep(_CONN_CHECK)


def _wants_refresh(warmed, next_try, now=None):
    '''Пора ли пробовать батч-прогрев: сразу после сброса (штарт/реконнект)
    или по расписанию повторной попытки после отката на поштучный опрос.'''
    if not warmed:
        return True
    if not next_try:
        return False
    now = time.time() if now is None else now
    return now >= next_try


def _apply_schedule(watches, statuses, base, gen):
    '''Раскладывает _next_due по фактическим статусам. False — если расписание
    успело сброситься (реконнект во время прогрева): тогда устаревшие сроки
    НЕ пишутся, и цикл начнётся заново с нового батча.'''
    if _schedule_gen != gen:
        return False
    stamp = time.monotonic()
    for w0 in watches:
        tid = w0['target_id']
        _last_poll[tid] = time.time()
        _next_due[tid] = stamp + _poll_gap(
            base, _status_name(statuses.get(int(tid))))
    return True


async def _force_refresh(reason='старт'):
    """Батч-опрос всех треков одним get_users (по 200): после старта и
    восстановления соединения difference-апдейты приходят с задержкой, а N
    отдельных get_entity дали бы N сетевых вызовов подряд. Возвращает
    {target_id: status} — поллер ставит по нему расписание."""
    global _last_refresh
    try:
        watches = db.get_online_watches()
    except Exception as ex:
        logger.warning(f"[ONWATCH] refresh: {ex}")
        return {}
    if not watches:
        return {}
    _last_refresh = time.time()
    ids = [int(w['target_id']) for w in watches]
    statuses = {}
    for i in range(0, len(ids), 200):
        chunk = ids[i:i + 200]
        try:
            users = await client.get_users(chunk) or []
            for u in users:
                # отвечать может None или объект без id (удалённый/невалидный
                # ID) — такие цели просто пропускаем, батч не падает
                uid = getattr(u, 'id', None) if u is not None else None
                if uid is None:
                    continue
                statuses[int(uid)] = getattr(u, 'status', None)
        except FloodWaitError as fe:
            logger.warning(f"[ONWATCH] FloodWait {fe.seconds}s на прогрев — пауза")
            await asyncio.sleep(fe.seconds + 5)
        except Exception as ex:
            logger.warning(f"[ONWATCH] прогрев {len(chunk)} целей: {ex}")
    done = 0
    for w in watches:
        tid = int(w['target_id'])
        if tid not in statuses:
            continue
        try:
            await _apply_status(w, statuses[tid], 'poll')
            done += 1
        except Exception as ex:
            logger.debug(f"[ONWATCH] refresh {tid}: {ex}")
    logger.info(f"[ONWATCH] принудительный опрос ({reason}): "
                f"{done}/{len(watches)} целей")
    return statuses


@client.on(events.NewMessage(func=owner_filter))
async def onwatch_unified(e):
    """Команды владельца (!onwatch…): owner_filter отсекает чужие сообщения,
    поэтому с onwatch_activity_msg (входящие ЛС цели) обработчики не
    пересекаются — там sender это цель, здесь только владелец и sudo."""
    try:
        text = (e.raw_text or '').strip()
        if not text:
            return
        low = text.lower()
        # !unonwatch тоже должен проходить ранний return
        if not (low.startswith('!onwatch') or low.startswith('!unonwatch')):
            return

        if low == '!onwatchlist' or low.startswith('!onwatchlist '):
            rows = db.get_online_watches()
            if not rows:
                await respond(e, ui.card("📡 ONWATCH", "список пуст",
                                         hint="`!onwatch @username|ID <текст>` — добавить"))
                db.bump_stat('cmds')
                return
            lines = []
            now_ts = int(time.time())
            for r in rows:
                # одно get_entity на строку — и для лейбла, и для статуса
                try:
                    ent = await client.get_entity(int(r['target_id']))
                    now = _status_name(getattr(ent, 'status', None))
                except Exception as ex:
                    ent = None
                    now = f'err: {str(ex)[:40]}'
                label = watch_label(r['target_id'], r['target_username'], ent)
                fired = r.get('last_fired') or 0
                ago = now_ts - fired
                if not fired:
                    sent_txt = 'никогда'
                elif ago < _FIRE_COOLDOWN:
                    sent_txt = f'через {_FIRE_COOLDOWN - ago}с'
                else:
                    sent_txt = f'{ago // 60} мин назад'
                src = r.get('last_source') or '—'
                lines.append(
                    f"• {label} | was: `{r['last_status']}` → now: `{now}` | отправка: `{sent_txt}`\n"
                    f"  отправок: {r.get('fires') or 0} (последний триггер: `{src}`) · "
                    f"  _{r['message'][:40]}{'...' if len(r['message']) > 40 else ''}_"
                )
            await respond(e, ui.card(f"📡 ONWATCH ({len(rows)})", "\n".join(lines),
                                     hint="`!unonwatch @username|ID` — снять трек"))
            db.bump_stat('cmds')
            return

        if low.startswith('!unonwatch'):
            m = re.match(r'^!unonwatch\s+(@?[\w\d_]+)$', text, re.IGNORECASE)
            if not m:
                await respond(e, ui.usage("!unonwatch @username|ID", title="Снять отслеживание"))
                db.bump_stat('cmds')
                return
            target_raw = m.group(1)
            try:
                ent = await resolve_entity(target_raw)
            except Exception:
                # аккаунт мог быть удалён — снимаем трек по числовому ID
                if target_raw.isdigit():
                    db.remove_online_watch(target_id=int(target_raw))
                    logger.info(f"[ONWATCH] трек снят (без резолва): id {target_raw}")
                    await respond(e, ui.ok(ui.kv("Цель", f"`{target_raw}`"),
                                           title="Отслеживание снято"))
                    db.bump_stat('cmds')
                    return
                await respond(e, ui.err(f"Не найден: {target_raw}", title="ONWATCH",
                                        hint="Формат: `!unonwatch @username|ID`"))
                db.bump_stat('cmds')
                return
            db.remove_online_watch(target_id=ent.id)
            logger.info(f"[ONWATCH] трек снят: {watch_label(ent.id, getattr(ent, 'username', None), ent=ent)}")
            await respond(e, ui.ok(ui.kv("Цель", watch_label(ent.id, getattr(ent, 'username', None), ent=ent)),
                                   title="Отслеживание снято"))
            db.bump_stat('cmds')
            return

        if low.startswith('!onwatchstatus'):
            m = re.match(r'^!onwatchstatus\s+(@?[\w\d_]+)$', text, re.IGNORECASE)
            if not m:
                await respond(e, ui.usage("!onwatchstatus @username|ID", title="ONWATCH-статус"))
                db.bump_stat('cmds')
                return
            target_raw = m.group(1)
            ent = None
            tid = None
            try:
                ent = await resolve_entity(target_raw)
                tid = ent.id
            except Exception:
                if target_raw.isdigit():
                    tid = int(target_raw)
            if tid is None:
                await respond(e, ui.err(f"Не найден: {target_raw}", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            row = db.get_online_watch_by_target(tid)
            if not row:
                await respond(e, ui.err(f"Трек на `{target_raw}` не установлен", title="ONWATCH",
                                        hint="`!onwatch @user <текст>` — добавить"))
                db.bump_stat('cmds')
                return
            now_name = '—'
            try:
                live = ent or await client.get_entity(int(tid))
                now_name = _status_name(getattr(live, 'status', None))
            except Exception as ex:
                now_name = f'err: {str(ex)[:30]}'
            label = watch_label(tid, row.get('target_username'), ent)
            ts_now = int(time.time())
            since = row.get('status_since') or 0
            in_status = _fmt_dur(ts_now - since) if since else '—'
            fired = row.get('last_fired') or 0
            ago = ts_now - fired
            if not fired:
                cd = 'отправлялось никогда'
            elif ago < _FIRE_COOLDOWN:
                cd = f'кулдаун: через {_FIRE_COOLDOWN - ago}с'
            else:
                cd = 'кулдаун свободен'
            due = _next_due.get(tid)
            nxt = f'через {int(due - time.monotonic())}с' if due and due > time.monotonic() else 'сейчас'
            gap = _poll_gap(_poll_base(len(db.get_online_watches())),
                            row.get('last_status') or 'unknown')
            la = row.get('last_activity') or 0
            if _session_active(la, ts_now):
                sess = f'открыта · активность {_fmt_dur(ts_now - la)} назад'
            elif la:
                sess = f'закрыта · {_fmt_dur(ts_now - la)} тишины'
            else:
                sess = 'нет · следующая активность отправит уведомление'
            await respond(e, ui.card(
                f"🔍 ONWATCH-статус {label}",
                ui.rows(
                    ("Статус сейчас", f"`{now_name}`, в статусе {in_status}"),
                    ("Отправок", f"{row.get('fires') or 0}"),
                    ("Последний триггер", f"`{row.get('last_source') or '—'}`"),
                    ("Последняя ошибка", f"`{row.get('last_error') or '—'}`"),
                    ("Отправка", cd),
                    ("Следующий опрос", nxt),
                    ("Сессия", sess),
                    ("Такт опроса", f"{gap}с · flicker-окно {_flicker_window(gap)}с"),
                    ("Формат текста", "markdown (-m)" if row.get('md') else "обычный"),
                ),
                footer=f"_{row['message'][:60]}{'…' if len(row['message']) > 60 else ''}_",
            ))
            db.bump_stat('cmds')
            return

        if low.startswith('!onwatchlog'):
            m = re.match(r'^!onwatchlog\s+(@?[\w\d_]+)(?:\s+(\d+[smhd]))?$', text, re.IGNORECASE)
            if not m:
                await respond(e, ui.usage("!onwatchlog @username|ID [7d|12h|30m]", title="ONWATCH-лог"))
                db.bump_stat('cmds')
                return
            target_raw = m.group(1)
            ent = None
            tid = None
            try:
                ent = await resolve_entity(target_raw)
                tid = ent.id
            except Exception:
                if target_raw.isdigit():
                    tid = int(target_raw)
            if tid is None:
                await respond(e, ui.err(f"Не найден: {target_raw}", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            win = _parse_window(m.group(2))
            logs = db.get_online_watch_log(tid, int(time.time()) - win)
            label = watch_label(tid, getattr(ent, 'username', None) if ent else None, ent)
            if not logs:
                await respond(e, ui.card(f"📜 ONWATCH-лог {label}",
                                         f"за {_fmt_win(win)} — сессий за окно нет",
                                         hint="время местное · `!onwatchlog @user 30d` — другое окно"))
                db.bump_stat('cmds')
                return
            now_ts = int(time.time())
            entries = []
            for i, r in enumerate(logs):
                end = logs[i + 1]['ts'] if i + 1 < len(logs) else now_ts
                # fromtimestamp без tz = местное время машины бота
                t = datetime.datetime.fromtimestamp(r['ts']).strftime('%d.%m %H:%M')
                src = _log_source_tag(r.get('source'))
                entries.append(f"`{t}` → `{r['status']}`{src} — "
                               f"{_fmt_dur(end - r['ts'])}")
            total = len(entries)
            if total > 40:
                entries = entries[-40:]
                entries.insert(0, f"_…показаны последние 40 из {total}_")
            entries[-1] += " _(сейчас)_"
            await respond(e, ui.card(f"📜 ONWATCH-лог {label} · {_fmt_win(win)}",
                                     "\n".join(entries),
                                     hint="время местное · `!onwatchlog @user 30d` — другое окно"))
            db.bump_stat('cmds')
            return

        if low.startswith('!onwatch '):
            parsed = _parse_onwatch_args(text)
            if not parsed:
                await respond(e, ui.usage("!onwatch [-m] @username|ID <текст сообщения>",
                                          title="ONWATCH"))
                db.bump_stat('cmds')
                return
            md_flag, target_raw, msg_text = parsed
            if len(msg_text) > 4096:
                await respond(e, ui.err("Сообщение слишком длинное (макс. 4096 символов).",
                                        title="ONWATCH"))
                db.bump_stat('cmds')
                return
            try:
                ent = await resolve_entity(target_raw)
            except Exception as ex:
                await respond(e, ui.err(f"Не удалось найти пользователя: {ex}",
                                        title="ONWATCH",
                                        hint="Можно указать числовой ID, флаг -m работает с ним так же: "
                                             "`!onwatch -m 12345 Привет`"))
                db.bump_stat('cmds')
                return
            if getattr(ent, 'bot', False):
                await respond(e, ui.err("За ботами следить нельзя.", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            if not isinstance(ent, User):
                # send_message в группу/канал упал бы вашим аккаунтом внутрь чата
                await respond(e, ui.err("Караулить можно только пользователей — не группы и каналы.",
                                        title="ONWATCH"))
                db.bump_stat('cmds')
                return
            if ent.id == OWNER_ID:
                await respond(e, ui.err("Караулить себя бессмысленно 😄", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            try:
                is_new = db.add_online_watch(ent.id, getattr(ent, 'username', None) or '', msg_text,
                                             OWNER_ID, e.chat_id, md=1 if md_flag else 0)
            except Exception as ex:
                await respond(e, ui.err(f"Ошибка БД: {ex}", title="ONWATCH"))
                db.bump_stat('cmds')
                return
            logger.info(f"[ONWATCH] трек {'добавлен' if is_new else 'обновлён'}: {watch_label(ent.id, getattr(ent, 'username', None), ent=ent)} "
                        f"chat={e.chat_id} статус={_status_name(getattr(ent, 'status', None))}")
            # счётчик треков пересчитает поллер на следующем цикле
            await respond(e, ui.card(
                "✅ ONWATCH добавлен" if is_new else "🔁 ONWATCH обновлён",
                ui.rows(
                    ("Цель", watch_label(ent.id, getattr(ent, 'username', None), ent=ent)),
                    ("Статус сейчас", f"`{_status_name(getattr(ent, 'status', None))}`"),
                    ("Сообщение", f"_{msg_text}_"),
                    *((("Формат", "markdown (флаг -m)"),) if md_flag else ()),
                ),
                footer=("Как только цель окажется в сети после оффлайна — отправлю сообщение и уведомлю во внешнем боте.\nТрек работает раз за разом."
                        if is_new else
                        "Текст трека обновлён — кулдаун и история сохранены."),
            ))
            db.bump_stat('cmds')
            return

        await respond(e, ui.usage("!onwatch [-m] @username|ID <текст>",
                                  "!onwatchlist",
                                  "!onwatchstatus @username|ID",
                                  "!onwatchlog @username|ID [7d]",
                                  "!unonwatch @username|ID",
                                  title="ONWATCH"))
        db.bump_stat('cmds')
        return
    except Exception as ex:
        logger.error(f"onwatch_unified err: {ex}")
        try:
            await respond(e, ui.err(f"Ошибка: {ex}", title="ONWATCH"))
        except Exception:
            pass


def _fmt_dur(sec):
    '''Человекочитаемая длительность: 45с / 5м 30с / 2ч 5м.'''
    sec = max(0, int(sec))
    if sec < 60:
        return f'{sec}с'
    if sec < 3600:
        return f'{sec // 60}м {sec % 60}с'
    return f'{sec // 3600}ч {(sec % 3600) // 60}м'


def _parse_window(s):
    '''Окно истории !onwatchlog: «7d»/«12h»/«30m»/«45s» → секунды;
    пусто/мусор — сутки по умолчанию.'''
    m = re.fullmatch(r'\d+([smhd])', (s or '').strip().lower())
    if not m:
        return 86400
    return int(m.group(0)[:-1]) * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}[m.group(1)]


def _fmt_win(sec):
    '''Окно в человекочитаемом виде: 7д / 12ч / 30м.'''
    if sec % 86400 == 0:
        return f'{sec // 86400}д'
    if sec % 3600 == 0:
        return f'{sec // 3600}ч'
    return f'{sec // 60}м'


def _poll_gap(base, status):
    '''Адаптивный интервал проверки ОДНОЙ цели. Инвариант (тестируется):
    чем реже статус, тем длиннее шаг — online/recently (активна) = base;
    offline и unknown (статус ещё не получен: холодный старт, UserStatusEmpty)
    = 2×base без кэпа — прежний кэп 30с инвертировал порядок при базе 35с;
    last_week/last_month = 4×base, но не меньше 90с.'''
    if status in ('online', 'recently'):
        return base
    if status in ('offline', 'unknown'):
        return base * 2
    return max(base * 4, 90)  # last_week / last_month


def _fresh_was_online(status, now=None, window=None):
    '''UserStatusOffline.was_online — момент последнего выхода из сети.
    Сервер отдаёт его не всегда (приватность/premium), но когда отдаёт —
    свежий was_online доказывает недавний онлайн даже для скрытого last-seen.
    Тип нормализуем: int (норма Telethon) или datetime (aware, либо naive =
    UTC); None/мусор → None, без исключений. window — окно свежести: поллер
    передаёт сюда max(FLICKER_WINDOW, 2×шаг опроса цели), чтобы медленный
    get_entity не «съедал» пропущенную сессию.'''
    if not isinstance(status, UserStatusOffline):
        return None
    wo = getattr(status, 'was_online', None)
    if not wo:
        return None
    if isinstance(wo, datetime.datetime):
        if wo.tzinfo is None:
            wo = wo.replace(tzinfo=datetime.timezone.utc)
        wo = wo.timestamp()
    else:
        try:
            wo = int(wo)
        except (TypeError, ValueError):
            return None
    now = time.time() if now is None else now
    if window is None:
        window = FLICKER_WINDOW
    return wo if now - wo <= window else None


def _flicker_window(gap):
    '''Окно flicker для цели: не уже 2× её шага опроса, но не шире
    FLICKER_WINDOW_MAX — будущий рост _poll_gap не должен раздувать окно.'''
    return min(max(FLICKER_WINDOW, 2 * gap), FLICKER_WINDOW_MAX)


def _next_action(prev, cur, flicker=False):
    '''Что делать при известной смене статуса — чистая функция (юнит-тесты).

    fire          — вход в активность: переход в online ИЛИ в «недавно»
                    (сервер мог спрятать online за плейсхолдером приватности:
                    offline/last_week/last_month → recently = пришёл;
                    cold start unknown → recently — только базовая запись);
    fire-recently — скрытый last-seen: «недавно» → «не в сети» = человек
                    только что вышел, онлайн-статус сервер не показывал;
                    отдельный source (poll-/update-recently) виден в
                    !onwatchstatus — для диагностики ложных срабатываний;
    fire-flicker  — пропущенная сессия: статус был не online/recently,
                    а was_online свежий — между опросами была сессия;
    update        — записать статус; none — изменений нет.'''
    if cur == 'online' and prev != 'online':
        return 'fire'
    if cur == 'recently' and prev in ('offline', 'last_week', 'last_month'):
        return 'fire'
    if prev == 'recently' and cur == 'offline':
        return 'fire-recently'
    if flicker and cur == 'offline' and prev not in ('online', 'recently'):
        return 'fire-flicker'
    if prev != cur:
        return 'update'
    return 'none'


def _skip_due_to_event(status_since, last_poll_ts):
    '''True, если событие записало статус ПОСЛЕ последнего GetUsers ЭТОЙ цели:
    опрос ничего нового не узнал бы — пропускаем. Пер-целевой ключ: сверяется
    только своё время _last_poll[tid], чужие треки на решение не влияют.'''
    return bool(status_since) and status_since > (last_poll_ts or 0)


def _poll_base(n):
    '''Базовый интервал поллера по числу целей: до 2 — 5с, до 5 — 10с,
    дальше растём против FloodWait на GetUsers (потолок 35с).'''
    return 5 if n <= 2 else (10 if n <= 5 else min(35, 10 + 5 * (n - 5)))


def _session_active(last_activity, now=None):
    """Сессия открыта: с последней активности прошло не больше SESSION_TIMEOUT.
    last_activity=0 (записи нет) — сессии нет. Чистая функция (юнит-тесты)."""
    if not last_activity:
        return False
    now = time.time() if now is None else now
    return (now - last_activity) <= _SESSION_TIMEOUT


_SOURCE_LABELS = {
    'typing': 'typing в ЛС',
    'message': 'сообщение в ЛС',
    'read': 'галочки (ваши прочитаны)',
    'update-profile': 'профиль',
    'update-story': 'сторис',
    'update-reaction': 'реакция в ЛС',
    'update-call': 'звонок',
    'update-edit': 'правка в ЛС',
    'reaction-group': 'реакция в группе',
    'story-channel': 'сторис канала',
}


def _parse_onwatch_args(text):
    '''Разбор «!onwatch [-m] <цель> <текст>». Флаг -m можно ставить и до цели,
    и сразу после неё — на разбор ID/username это не влияет. Возвращает
    (markdown, цель, текст) или None, если аргументов не хватает.'''
    body = (text or '').strip()
    if body[:8].lower() != '!onwatch':
        return None
    body = body[8:].strip()
    if not body:
        return None
    md = False
    if body.lower().startswith('-m ') or body.lower() == '-m':
        md = True
        body = body[2:].strip()
        if not body:
            return None
    parts = body.split(None, 1)
    target = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ''
    if rest.lower() == '-m':
        # флаг после цели, но без текста — это не команда
        return None
    if rest[:3].lower() == '-m ':
        md = True
        rest = rest[3:].strip()
    if not rest or not re.fullmatch(r'@?[\w\d_]+', target):
        return None
    return md, target, rest


def _log_source_tag(source):
    """Метка канала строки в !onwatchlog: реальный вход (status) не помечаем,
    пропущенную сессию и каналы активности — помечаем с расшифровкой."""
    if not source or source == 'status':
        return ''
    if source == 'flicker':
        return ' · **flicker** (was_online)'
    return ' · ' + _SOURCE_LABELS.get(source, source)


def _raw_activity(update):
    '''(target_id, source) для «боковых» сигналов активности, иначе None.

    Только ЛС и собственный профиль цели: реакции/сторис/правки в группах и
    каналах не считаются. Входящий звонок — самый сильный сигнал (человек был
    в сети секунду назад), берём admin_id из PhoneCallRequested.'''
    if isinstance(update, UpdateUserName):
        return update.user_id, 'update-profile'
    if isinstance(update, UpdateStory):
        return ((update.peer.user_id, 'update-story')
                if isinstance(update.peer, PeerUser) else None)
    if isinstance(update, UpdateMessageReactions):
        return ((update.peer.user_id, 'update-reaction')
                if isinstance(update.peer, PeerUser) else None)
    if isinstance(update, UpdateEditMessage):
        msg = update.message
        if not isinstance(msg, Message):
            return None
        peer, frm = getattr(msg, 'peer_id', None), getattr(msg, 'from_id', None)
        # правит своё сообщение в ЛС: правка наших сообщений не в счёт
        if (isinstance(peer, PeerUser) and isinstance(frm, PeerUser)
                and peer.user_id == frm.user_id):
            return peer.user_id, 'update-edit'
        return None
    if isinstance(update, UpdatePhoneCall):
        call = update.phone_call
        # PhoneCallRequested = входящий звонок, admin_id — кто звонит
        return ((call.admin_id, 'update-call')
                if isinstance(call, PhoneCallRequested) else None)
    return None


def _raw_log_candidates(update):
    '''Активность для истории БЕЗ уведомления: реакция в общем чате
    (кто реагировал — в recent_reactions) и сторис канала, где цель — автор.
    Возвращает список (target_id, source); fire по ним не делаем (PM-only).'''
    out = []
    if (isinstance(update, UpdateMessageReactions)
            and not isinstance(update.peer, PeerUser)):
        for r in (getattr(update.reactions, 'recent_reactions', None) or []):
            peer = getattr(r, 'peer_id', None)
            if isinstance(peer, PeerUser):
                out.append((peer.user_id, 'reaction-group'))
    elif (isinstance(update, UpdateStory)
            and not isinstance(update.peer, PeerUser)):
        frm = getattr(update.story, 'from_id', None)
        if isinstance(frm, PeerUser):
            out.append((frm.user_id, 'story-channel'))
    return out


@client.on(events.Raw(types=_RAW_ACTIVITY_TYPES))
async def onwatch_raw_activity(update):
    '''Смена профиля, новая сторис, реакция, звонок, правка сообщения в ЛС:
    каждый сигнал = цель активна прямо сейчас. Уходит ли уведомление, решает
    сессионная модель в _onwatch_fire (один fire на непрерывную сессию).'''
    try:
        hit = _raw_activity(update)
        if not hit:
            # групповые сигналы видим только в истории, без уведомления
            for tid, source in _raw_log_candidates(update):
                if not tid or tid == OWNER_ID:
                    continue
                if not db.get_online_watch_by_target(tid):
                    continue
                db.append_online_watch_log(tid, 'online', int(time.time()),
                                           source=source)
                logger.debug(f"[ONWATCH] {tid}: {source} — только в историю")
            return
        tid, source = hit
        if not tid or tid == OWNER_ID:
            return
        w = db.get_online_watch_by_target(tid)
        if not w:
            return
        await _onwatch_fire(w, source)
    except Exception as ex:
        logger.warning(f"[ONWATCH] raw-activity err: {ex}")


async def online_watcher():
    global _watch_count_logged, _warmed, _last_prune, _conn_up, _refresh_fails
    global _next_refresh_try
    logger.info("[ONWATCH] поллер запущен: события + страховочный опрос "
                "(адаптивный интервал per-target)")
    interval_logged = None
    while True:
        try:
            # восстановление соединения: разрыв мог «проглотить» вход цели,
            # difference-апдейты приходят с задержкой — сверяем всё сразу
            try:
                conn = client.is_connected()
            except Exception:
                conn = True
            if conn and not _conn_up:
                _mark_reconnect()
                logger.info("[ONWATCH] соединение восстановлено — "
                            "внеплановый опрос (поллер)")
            elif not conn:
                _conn_up = False
            watches = db.get_online_watches()
            if _watch_count_logged != len(watches):
                _watch_count_logged = len(watches)
                logger.info(f"[ONWATCH] активных треков: {len(watches)}")
            if not watches:
                await asyncio.sleep(40)
                continue
            n = len(watches)
            # база: до 2 целей — 5с, до 5 — 10с, дальше растём против FloodWait;
            # сверху — адаптив per-target через _poll_gap
            base = _poll_base(n)
            if interval_logged != base:
                interval_logged = base
                logger.info(f"[ONWATCH] базовый интервал: {base}с (целей: {n})")
            # прогрев после старта/реконнекта: один get_users по 200 вместо
            # N get_entity, сразу с расписанием по фактическим статусам
            # поколение снимаем ДО прогрева: если реконнект случится, пока
            # идёт батч, расписание из его статусов писать нельзя
            gen = _schedule_gen
            if _wants_refresh(_warmed, _next_refresh_try):
                pause = _refresh_delay(_refresh_reason, _last_refresh)
                if pause:
                    # флаппинг связи: не чаще одного батча за _REFRESH_MIN_GAP
                    logger.debug(f"[ONWATCH] пауза {pause:.0f}с перед прогревом")
                    await asyncio.sleep(pause)
                statuses = await _force_refresh(_refresh_reason)
                if statuses:
                    if not _apply_schedule(watches, statuses, base, gen):
                        # реконнект посреди батча: статусы уже могли устареть —
                        # следующий цикл начнётся с нового прогрева
                        logger.debug("[ONWATCH] расписание сброшено во время "
                                     "прогрева — повторяем")
                        continue
                    _warmed = True
                    _refresh_fails = 0
                    _next_refresh_try = 0.0
                else:
                    # прогрев без статусов (сеть/сессия): _warmed НЕ ставим —
                    # иначе расписание останется пустым и пойдёт поштучный
                    # шторм get_entity; повторяем с паузой, после N неудач —
                    # откат на обычный поштучный опрос
                    _refresh_fails += 1
                    if _refresh_fails >= _REFRESH_FALLBACK_AFTER:
                        logger.warning("[ONWATCH] прогрев не удался "
                                       f"{_refresh_fails} раза — поштучный "
                                       "опрос, повтор батча через "
                                       f"{_REFRESH_FALLBACK_RETRY}с")
                        _warmed = True
                        _refresh_fails = 0
                        # деградация временная: через N секунд снова батч
                        _next_refresh_try = (time.time()
                                             + _REFRESH_FALLBACK_RETRY)
                    else:
                        await asyncio.sleep(_REFRESH_RETRY_DELAY)
                        continue
            # ретеншн истории — периодической задачей, не в пути отправки
            if time.time() - _last_prune > 6 * 3600:
                _last_prune = time.time()
                try:
                    removed = db.prune_online_watch_log()
                    if removed:
                        logger.info(f"[ONWATCH] история: удалено {removed} "
                                    "строк старше 90 дней")
                except Exception as ex:
                    logger.debug(f"[ONWATCH] prune: {ex}")
            nowm = time.monotonic()
            soonest = None
            for w0 in watches:
                if _schedule_gen != gen:
                    # реконнект сбросил расписание посреди цикла: не опрашиваем
                    # треки поштучно — следующий цикл начнётся с прогрева
                    break
                tid = w0['target_id']
                if tid not in _next_due:
                    # новый трек (старт после рестарта, добавление): первый
                    # опрос размазан по окну ~0.5с на трек, не выше 60с
                    _next_due[tid] = nowm + random.uniform(
                        0, min(_START_JITTER_MAX, 0.5 * n))
                due = _next_due.get(tid, 0)
                if due > nowm:
                    # ещё не время этой цели — запоминаем ближайший срок
                    if soonest is None or due < soonest:
                        soonest = due
                    continue
                try:
                    # (1) первичное чтение: если событие уже записало свежий
                    # статус после нашего последнего опроса — GetUsers не нужен
                    w = db.get_online_watch_by_target(tid) or w0
                    st_since = w.get('status_since') or 0
                    if _skip_due_to_event(st_since, _last_poll.get(tid, 0)):
                        _last_poll[tid] = time.time()
                        _next_due[tid] = time.monotonic() + _poll_gap(base, w.get('last_status') or 'unknown')
                        logger.debug(f"[ONWATCH] {tid}: статус обновлён событием — опрос пропущен")
                        continue
                    # (2) сначала сеть, потом свежее чтение БД: между чтением
                    # и записью нет await — событие не может «перетереть» статус
                    ent = await client.get_entity(tid)
                    w = db.get_online_watch_by_target(tid) or w0
                    cur = _status_name(getattr(ent, 'status', None))
                    await _apply_status(w, getattr(ent, 'status', None),
                                        'poll', base)
                    _last_poll[tid] = time.time()
                    _next_due[tid] = time.monotonic() + _poll_gap(base, cur)
                except FloodWaitError as fe:
                    logger.warning(f"[ONWATCH] FloodWait {fe.seconds}s — пауза")
                    await asyncio.sleep(fe.seconds + 5)
                    break
                except Exception as ex:
                    if tid not in _watch_err_logged:
                        _watch_err_logged.add(tid)
                        logger.warning(f"[ONWATCH] не удалось проверить {tid}: {ex}")
                    else:
                        logger.debug(f"[ONWATCH] skip {tid}: {ex}")
                    # _last_poll НЕ трогаем: ошибка (в т.ч. разрыв связи) —
                    # это не успешный опрос, иначе трек выглядел бы «только
                    # что проверенным» и после реконнекта выпал бы из сверки
                    _next_due[tid] = time.monotonic() + _poll_gap(base, 'unknown')
                    continue
            # спим до ближайшего срока, но не дольше 35с: свежий трек
            # и «мигающие» цели не ждут чужого долгого цикла
            if soonest is None:
                await asyncio.sleep(base)
            else:
                await asyncio.sleep(max(0.5, min(soonest - time.monotonic(), 35)))
        except Exception as ex:
            logger.warning(f"[ONWATCH] loop err: {ex}")
            await asyncio.sleep(10)


@client.on(events.UserUpdate)
async def onwatch_status_event(e):
    """Мгновенная реакция: смена статуса (updateUserStatus) ИЛИ активность
    (typing-апдейты) — ловит появление в сети между опросами.

    ВАЖНО: у целей со скрытым last-seen («был(а) недавно» — плейсхолдер
    приватности) сервер может не вернуть online. Для них работают: переход
    «недавно» → «не в сети» (fire-recently), свежий was_online пропущенной
    сессии (fire-flicker) и каналы активности — typing и входящие сообщения
    (см. onwatch_activity_msg)."""
    try:
        # Защита от расширения UserUpdate в новых версиях Telethon: профиль,
        # фото и прочее обрабатывает raw-канал, здесь они дали бы второй fire.
        # Синтетические события в тестах без original_update проходят.
        raw = getattr(e, 'original_update', None)
        if raw is not None and not isinstance(
                raw, (UpdateUserStatus, UpdateUserTyping,
                      UpdateChatUserTyping, UpdateChannelUserTyping)):
            return
        tid = e.user_id
        if not tid or tid == OWNER_ID:
            return
        if e.status is None and e.typing is None:
            return  # прочие user-update без статуса и без активности
        w = db.get_online_watch_by_target(tid)
        if not w:
            return
        if e.status is not None:
            # статус пишется до await: поллер увидит свежее состояние,
            # повторные online-продления не пересылают уведомление
            await _apply_status(w, e.status, 'update')
            return
        # typing из групп/каналов не считаем: UpdateChatUserTyping даёт
        # отрицательный chat_id, караул работает только по ЛС
        cid = getattr(e, 'chat_id', None)
        if cid is not None and cid <= 0:
            return
        # typing/choose_sticker/speak = пользователь реально в сети;
        # кулдаун от последней отправки решает, уходить ли уведомлению
        await _onwatch_fire(w, 'typing')
    except Exception as ex:
        logger.warning(f"[ONWATCH] status-event err: {ex}")


@client.on(events.NewMessage(incoming=True))
async def onwatch_activity_msg(e):
    """Цель написала сообщение В ЛС = она в сети. Групповые сообщения не
    считаем: from_id там атрибутируется, но по дизайну бот в группах не
    работает. Работает даже когда online-статус скрыт приватностью."""
    try:
        sid = e.sender_id
        if not sid or sid == OWNER_ID:
            return
        # караул только по ЛС: в общей группе from_id атрибутируется, но
        # по дизайну бот в группах не работает
        if not getattr(e, 'is_private', True):
            return
        w = db.get_online_watch_by_target(sid)
        if not w:
            return
        # кулдаун от последней отправки — внутри _onwatch_fire
        await _onwatch_fire(w, 'message')
    except Exception as ex:
        logger.warning(f"[ONWATCH] activity-msg err: {ex}")


@client.on(events.MessageRead)
async def onwatch_read_receipt(e):
    """Цель прочитала мои сообщения = она в сети. Telethon по умолчанию
    (inbox=False) доставляет только UpdateReadHistoryOutbox — ваши исходящие,
    прочитанные целью; UpdateReadHistoryInbox (вы прочитали её) отфильтрован.
    Двойные галочки приходят НЕЗАВИСИМО от скрытого last-seen — основной
    канал, когда online-статус цели скрыт приватностью. Ограничение Telegram:
    если у цели отключены read-receipts, событие не приходит."""
    try:
        tid = e.chat_id
        if not tid or tid <= 0 or tid == OWNER_ID:
            return   # только ЛС (chat_id положителен и равен id юзера)
        w = db.get_online_watch_by_target(tid)
        if not w:
            return
        # кулдаун от последней отправки — внутри _onwatch_fire
        await _onwatch_fire(w, 'read')
    except Exception as ex:
        logger.warning(f"[ONWATCH] read-receipt err: {ex}")
