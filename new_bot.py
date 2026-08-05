# -*- coding: utf-8 -*-
"""
new_bot.py — Telegram-бот жалоб (Render Web Service, gunicorn, webhook).

ГЛАВНАЯ ПРИЧИНА, ПО КОТОРОЙ КНОПКА «сообщения чата» НЕ РАБОТАЛА:

    setWebhook -> {'ok': False, 'error_code': 400,
                   'description': 'Bad Request: secret token contains
                                   unallowed characters'}

Telegram разрешает в secret_token только символы [A-Za-z0-9_-] (1..256).
Секрет, сгенерированный через `openssl rand -base64 32`, содержит '+', '/'
и '=' — setWebhook падает, вебхук НЕ устанавливается, и бот не получает от
Telegram вообще ничего: ни callback_query от кнопок, ни команды /diag, /dump.
При этом жалобы продолжают приходить, потому что ВДС пушит их напрямую в
POST /relay/report, минуя Telegram-апдейты. Отсюда обманчивая картина:
«сообщения идут, а кнопки мёртвые».

Что сделано:
  * Сырой WEBHOOK_SECRET больше не уходит в Telegram. Из него выводится
    валидный токен (sha256-hex). Переменную на Render менять НЕ нужно.
  * Если Telegram всё же ругается на secret_token — вебхук ставится без него
    (защита остаётся за счёт секретного пути в URL).
  * Путь /webhook/<...> принимает и производный токен, и старый сырой секрет,
    чтобы уже настроенный вебхук не отвалился.
  * Если вебхук не встал — админу приходит сообщение с причиной, а не тишина.
  * Тяжёлая выгрузка с ВДС ушла в фоновый поток: раньше два запроса по 45 сек
    внутри вебхука перебивали gunicorn timeout, воркер убивался, файл не уходил.
  * Ошибки не глотаются: неверный секрет ВДС, отсутствующий chat_id, недоступный
    сервер — всё приходит админу текстом.
  * БД теперь не обязательна: пул открывается лениво и в фоне, недоступная база
    больше не тормозит старт на 10 секунд и не мешает кнопкам.

Диагностика:
  /diag           — конфиг + getWebhookInfo + пинг ВДС
  /dump 29        — выгрузить переписку вручную (просто число, без <> и слова id)
  /last           — последние жалобы (нужна рабочая БД)

Запуск на Render:
  Build:  pip install -r requirements.txt
  Start:  gunicorn new_bot:app -c gunicorn_bot.py

Переменные окружения:
  BOT_TOKEN, ADMIN_TELEGRAM_ID, WEBHOOK_SECRET, PUBLIC_URL
  REPORT_RELAY_SECRET   — тот же, что на ВДС
  VDS_BASE_URL          — https://vuntserverrr.site
  VDS_ADMIN_SECRET      — тот же, что ADMIN_DUMP_SECRET на ВДС
  DATABASE_URL          — опционально
"""

import os
import io
import re
import json
import hashlib
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

try:
    from psycopg_pool import ConnectionPool
except Exception:  # без БД бот обязан работать
    ConnectionPool = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [report-bot] %(message)s'
)
log = logging.getLogger(__name__)

BOT_TOKEN = os.environ.get('BOT_TOKEN', '').strip()
ADMIN_TELEGRAM_ID = str(os.environ.get('ADMIN_TELEGRAM_ID', '5574610358')).strip()
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', 'change-me').strip()
DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
PUBLIC_URL = os.environ.get('PUBLIC_URL', '').strip().rstrip('/')

RELAY_SECRET = (os.environ.get('REPORT_RELAY_SECRET')
                or os.environ.get('RELAY_SECRET') or '').strip()

VDS_BASE_URL = os.environ.get('VDS_BASE_URL', 'https://vuntserverrr.site').strip().rstrip('/')
VDS_ADMIN_SECRET = os.environ.get('VDS_ADMIN_SECRET', '').strip()

TELEGRAM_API = f'https://api.telegram.org/bot{BOT_TOKEN}'

app = Flask(__name__)


# ------------------------------------------------- токен вебхука (тот самый баг)

_TOKEN_OK_RE = re.compile(r'^[A-Za-z0-9_-]{1,256}$')


def _derive_token(raw: str) -> str:
    """Валидный для Telegram и для URL токен из произвольного секрета."""
    if _TOKEN_OK_RE.match(raw or ''):
        return raw
    digest = hashlib.sha256((raw or '').encode('utf-8')).hexdigest()
    log.warning('WEBHOOK_SECRET содержит символы, недопустимые для Telegram '
                '(разрешены только A-Za-z0-9_-). Использую производный токен '
                'sha256. Менять переменную на Render не нужно.')
    return digest


WEBHOOK_TOKEN = _derive_token(WEBHOOK_SECRET)

# Путь принимаем и по производному токену, и по сырому секрету — чтобы
# уже настроенный ранее вебхук со старым URL продолжал работать.
_VALID_WEBHOOK_PATHS = {WEBHOOK_TOKEN}
if WEBHOOK_SECRET:
    _VALID_WEBHOOK_PATHS.add(WEBHOOK_SECRET)

_VALID_HEADER_TOKENS = {WEBHOOK_TOKEN}
if WEBHOOK_SECRET and _TOKEN_OK_RE.match(WEBHOOK_SECRET):
    _VALID_HEADER_TOKENS.add(WEBHOOK_SECRET)


# ------------------------------------------------------------------- БД (опц.)

# Пул открываем ЛЕНИВО и в фоне. Раньше pool.open() на старте висел 10 секунд
# ('couldn't get a connection after 10.00 sec') и задерживал инициализацию,
# включая установку вебхука. БД нужна только для /last и журнала действий —
# chat_id для выгрузки приходит прямо в callback_data кнопки.
pool = None
_pool_state = 'disabled' if not DATABASE_URL else 'pending'
_pool_error = ''
_pool_lock = threading.Lock()


def _open_pool_bg():
    global pool, _pool_state, _pool_error
    if not DATABASE_URL or ConnectionPool is None:
        _pool_state = 'disabled'
        return
    try:
        p = ConnectionPool(DATABASE_URL, min_size=1, max_size=3, timeout=8,
                           kwargs={'autocommit': True}, open=False)
        p.open(wait=True, timeout=15)
        with p.connection() as conn:
            conn.execute(SCHEMA_SQL)
        with _pool_lock:
            pool = p
            _pool_state = 'ok'
        log.info('PostgreSQL: пул открыт, схема проверена')
    except Exception as e:
        _pool_state = 'error'
        _pool_error = f'{type(e).__name__}: {str(e)[:200]}'
        log.error('PostgreSQL недоступна (%s). Бот продолжит работать без БД: '
                  'кнопки и выгрузка чата от неё не зависят.', _pool_error)


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS report_actions (
    id            BIGSERIAL PRIMARY KEY,
    report_id     BIGINT,
    admin_tg_id   TEXT        NOT NULL,
    action        TEXT        NOT NULL,
    result        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_report_actions_report ON report_actions (report_id);
"""

_init_lock = threading.Lock()
_initialized = False

# Дедупликация: Telegram ретраит апдейт при таймауте, а нам не нужен второй дамп
_seen_updates = set()
_seen_lock = threading.Lock()


# ------------------------------------------------------------ Telegram API

def tg(method: str, **payload):
    if not BOT_TOKEN:
        log.error('BOT_TOKEN не задан')
        return None
    try:
        r = requests.post(f'{TELEGRAM_API}/{method}', data=payload, timeout=15)
        body = r.json() if r.content else {}
        if not body.get('ok'):
            log.error('%s -> %s', method, body)
        return body
    except Exception as e:
        log.error('%s исключение: %s: %s', method, type(e).__name__, str(e)[:200])
        return None


def answer_callback(callback_id: str, text: str, alert: bool = True):
    tg('answerCallbackQuery', callback_query_id=callback_id, text=text[:190],
       show_alert='true' if alert else 'false')


def send(chat_id, text: str, html: bool = False):
    kw = {'chat_id': chat_id, 'text': text[:4000], 'disable_web_page_preview': 'true'}
    if html:
        kw['parse_mode'] = 'HTML'
    tg('sendMessage', **kw)


def send_keyboard(chat_id, text: str, keyboard: dict):
    return tg('sendMessage', chat_id=chat_id, text=text[:4000],
              parse_mode='HTML', disable_web_page_preview='true',
              reply_markup=json.dumps(keyboard, ensure_ascii=False))


def edit_keyboard(chat_id, message_id, text: str, keyboard: dict):
    return tg('editMessageText', chat_id=chat_id, message_id=message_id,
              text=text[:4000], parse_mode='HTML',
              disable_web_page_preview='true',
              reply_markup=json.dumps(keyboard, ensure_ascii=False))


def delete_message(chat_id, message_id):
    return tg('deleteMessage', chat_id=chat_id, message_id=message_id)


def send_document(chat_id, filename: str, content: bytes,
                  caption: str = '', mime: str = 'text/html'):
    if not BOT_TOKEN:
        return None
    try:
        r = requests.post(
            f'{TELEGRAM_API}/sendDocument',
            data={'chat_id': chat_id, 'caption': caption[:1000], 'parse_mode': 'HTML'},
            files={'document': (filename, io.BytesIO(content), mime)},
            timeout=180,
        )
        body = r.json() if r.content else {}
        if not body.get('ok'):
            log.error('sendDocument -> %s', body)
        return body
    except Exception as e:
        log.error('sendDocument исключение: %s: %s', type(e).__name__, str(e)[:200])
        return None


def log_action(report_id, admin_id, action, result='pending'):
    if not pool:
        return
    try:
        with pool.connection() as conn:
            conn.execute(
                """INSERT INTO report_actions (report_id, admin_tg_id, action, result)
                   VALUES (%s, %s, %s, %s)""",
                (report_id or None, str(admin_id), action, result)
            )
    except Exception as e:
        log.error('log_action: %s', e)


def get_report(report_id):
    """Фолбэк, если в кнопке нет chat_id. Требует, чтобы DATABASE_URL бота
    смотрел в ту же базу, куда пишет ВДС."""
    if not pool or not report_id:
        return None
    try:
        with pool.connection() as conn:
            row = conn.execute(
                """SELECT id, reporter_id, reported_id, reported_nickname,
                          reason, comment, chat_id, status, created_at
                     FROM user_reports WHERE id = %s""",
                (report_id,)
            ).fetchone()
            if not row:
                return None
            keys = ['id', 'reporter_id', 'reported_id', 'reported_nickname',
                    'reason', 'comment', 'chat_id', 'status', 'created_at']
            return dict(zip(keys, row))
    except Exception as e:
        log.error('get_report: %s', e)
        return None


# --------------------------------------------------------- выгрузка с ВДС

def fetch_chat_dump(chat_id):
    """
    (html_bytes, stats) при успехе или (None, текст_ошибки).
    Сообщения в БД зашифрованы Fernet, ключ есть только на ВДС — расшифровку
    делает он, бот лишь пересылает готовый HTML.
    """
    if not VDS_ADMIN_SECRET:
        return None, ('VDS_ADMIN_SECRET не задан в переменных Render.\n'
                      'Он должен совпадать с ADMIN_DUMP_SECRET в .env на ВДС.')
    if not VDS_BASE_URL:
        return None, 'VDS_BASE_URL не задан в переменных Render'

    url = f'{VDS_BASE_URL}/admin/chat_dump'
    headers = {'X-Admin-Secret': VDS_ADMIN_SECRET}

    try:
        rj = requests.get(url, params={'chat_id': chat_id, 'format': 'json'},
                          headers=headers, timeout=30)
        if rj.status_code == 403:
            return None, ('ВДС отклонил секрет (403).\n'
                          'Сверь ADMIN_DUMP_SECRET на ВДС и VDS_ADMIN_SECRET на Render.')
        if rj.status_code == 400:
            return None, f'ВДС: некорректный chat_id «{chat_id}» (ожидается число)'
        if rj.status_code == 404:
            return None, (f'Чат {chat_id} не найден в БД.\n'
                          'Скорее всего он удалён «у всех» — строки стёрты безвозвратно.')
        if not rj.ok:
            return None, f'ВДС ответил {rj.status_code}: {rj.text[:200]}'

        payload = (rj.json() or {}).get('chat') or {}
        msgs = payload.get('messages') or []
        stats = {
            'total': len(msgs),
            'deleted': sum(1 for m in msgs if m.get('deleted_by')),
            'images': sum(1 for m in msgs if m.get('is_image')),
            'participants': payload.get('participants') or [],
            'json': payload,
        }

        # html теперь тяжелее: ВДС качает фото с Cloudinary и вшивает их в файл
        rh = requests.get(url, params={'chat_id': chat_id},
                          headers=headers, timeout=180)
        if not rh.ok:
            return None, f'ВДС ответил {rh.status_code} на HTML-выгрузку'
        return rh.content, stats
    except requests.Timeout:
        return None, ('ВДС не ответил вовремя. Если в чате много фото, сборка отчёта\n'
                      'может быть долгой — попробуй ещё раз.')
    except requests.ConnectionError as e:
        return None, (f'Не достучался до {VDS_BASE_URL}: {str(e)[:180]}\n'
                      'Проверь домен, HTTPS-сертификат и что сервер запущен.')
    except Exception as e:
        return None, f'{type(e).__name__}: {str(e)[:200]}'


def dump_to_text(payload) -> bytes:
    """Плоская .txt-копия: мобильный Telegram HTML-файл не превьюит, только
    скачивает, а .txt открывает прямо в приложении."""
    lines = [f'Отчёт по чату #{payload.get("chat_id")}',
             f'Создан: {payload.get("created_at")}',
             'Участники:']
    for p in payload.get('participants') or []:
        lines.append(f'  - {p.get("user_id")} {p.get("nickname") or ""}')
    for d in payload.get('chat_deletions') or []:
        lines.append(f'  ! чат скрыт у {d.get("nickname") or d.get("user_id")} ({d.get("at")})')
    lines.append('')
    lines.append('-' * 48)
    for m in payload.get('messages') or []:
        who = m.get('sender_nickname') or m.get('sender_id')
        flags = []
        if m.get('is_image'):
            flags.append('изображение')
        if m.get('is_edited'):
            flags.append('изменено')
        if not m.get('is_read'):
            flags.append('не прочитано')
        for d in m.get('deleted_by') or []:
            flags.append(f'удалено у {d.get("nickname") or d.get("user_id")}')
        tail = f'   [{", ".join(flags)}]' if flags else ''
        body = m.get('text') or ''
        if m.get('caption'):
            body = f'{body}\n    подпись: {m["caption"]}'
        lines.append(f'#{m.get("id")} {m.get("sent_at")} {who} ({m.get("sender_id")}):{tail}')
        lines.append(f'    {body}')
        lines.append('')
    return '\n'.join(lines).encode('utf-8')


def deliver_dump(chat_id, report_id=None, admin_id=ADMIN_TELEGRAM_ID):
    """Тянет дамп и отправляет админу. Вызывается ИЗ ФОНОВОГО ПОТОКА."""
    content, info = fetch_chat_dump(chat_id)

    if content is None:
        log.error('dump чата %s не получен: %s', chat_id, info)
        log_action(report_id, admin_id, 'view_messages', f'error: {info}'[:200])
        send(ADMIN_TELEGRAM_ID, f'❌ Не удалось собрать отчёт по чату {chat_id}\n\n{info}')
        return False

    caption = (
        f'📄 <b>Отчёт по чату {chat_id}</b>\n'
        + (f'Жалоба #{report_id}\n' if report_id else '')
        + f'\nВсего сообщений: <b>{info["total"]}</b>\n'
          f'Удалено у кого-то: <b>{info["deleted"]}</b>\n'
          f'Изображений: <b>{info["images"]}</b>\n\n'
          '.html — фото уже внутри файла, открывать в браузере.\n'
          '.txt — быстрый просмотр текста прямо в Telegram.'
    )

    res = send_document(ADMIN_TELEGRAM_ID, f'report_{chat_id}.html', content, caption)
    ok = bool(res and res.get('ok'))

    try:
        txt = dump_to_text(info.get('json') or {})
        send_document(ADMIN_TELEGRAM_ID, f'report_{chat_id}.txt', txt, '', mime='text/plain')
    except Exception as e:
        log.error('txt-копия не собралась: %s', e)

    if not ok:
        send(ADMIN_TELEGRAM_ID,
             f'⚠️ Отчёт по чату {chat_id} получен с ВДС, но Telegram не принял файл.\n'
             f'Ответ: {str(res)[:300]}')
    log_action(report_id, admin_id, 'view_messages', 'done' if ok else 'error: sendDocument')
    return ok


def run_bg(fn, *args, **kwargs):
    """Вебхук обязан ответить 200 за пару секунд, иначе gunicorn убьёт воркер
    по timeout, а Telegram начнёт ретраить апдейт по кругу."""
    threading.Thread(target=_bg_wrap, args=(fn, args, kwargs), daemon=True).start()


def _bg_wrap(fn, args, kwargs):
    try:
        fn(*args, **kwargs)
    except Exception as e:
        log.exception('фоновая задача упала: %s', e)
        send(ADMIN_TELEGRAM_ID,
             f'❌ Внутренняя ошибка бота: {type(e).__name__}: {str(e)[:300]}')


# ------------------------------------------------------- обработчики кнопок

def handle_messages_button(report_id, admin_id, callback_id, chat_id=''):
    chat_id = re.sub(r'\D', '', str(chat_id or ''))

    if not chat_id:
        report = get_report(report_id)
        chat_id = re.sub(r'\D', '', str((report or {}).get('chat_id') or ''))

    if not chat_id:
        answer_callback(callback_id, 'В жалобе нет id чата — выгружать нечего')
        log_action(report_id, admin_id, 'view_messages', 'error: no chat_id')
        send(ADMIN_TELEGRAM_ID,
             f'❌ У жалобы #{report_id or "—"} нет chat_id.\n\n'
             'Причины: жалоба отправлена не из чата, фронт не передал chat_id '
             'в POST /report, или кнопка создана до патча.\n\n'
             'Выгрузить вручную: /dump 29')
        return

    answer_callback(callback_id, 'Собираю переписку, файл придёт через пару секунд…',
                    alert=False)
    run_bg(deliver_dump, chat_id, report_id, admin_id)


# ------------------------------------------------- меню бана (пока визуал)
#
# Сообщение с жалобой НЕ дублируется новым: экраны меняют текст и клавиатуру
# того же сообщения через editMessageText. Сценарий:
#   жалоба -> «Кому из пользователей отправить?» -> «На какое время забанить
#   пользователя "ник"?» (часы -> дни -> месяцы) -> заглушка выбора.
#
# «отмена» возвращает исходный текст жалобы с исходными кнопками, поэтому
# оригинал сообщения хранится в _BAN_STATE.
#
# callback_data (лимит Telegram — 64 байта, поэтому только индексы и коды):
#   ban:<rid>:u   — экран выбора пользователя
#   ban:<rid>:h   — экран «часы»
#   ban:<rid>:d   — экран «дни»
#   ban:<rid>:m   — экран «месяцы»
#   ban:<rid>:x   — отмена, вернуть жалобу
#   bu:<rid>:<idx>            — выбран пользователь по индексу в _BAN_STATE
#   bs:<rid>:<h1|d3|m2|perm>  — выбран срок (заглушка)

USER_PROMPT = '👤 <b>Кому из пользователей отправить?</b>'

# Состояние по сообщению: оригинал жалобы + участники + выбранная цель.
# Память процесса: после рестарта Render меню на старом сообщении не сможет
# восстановить текст жалобы — тогда покажем короткую заглушку вместо неё.
_BAN_STATE = {}
_ban_lock = threading.Lock()


def _mkey(chat_id, message_id):
    return f'{chat_id}:{message_id}'


def ban_state_put(chat_id, message_id, **fields):
    key = _mkey(chat_id, message_id)
    with _ban_lock:
        st = _BAN_STATE.setdefault(key, {})
        st.update(fields)
        if len(_BAN_STATE) > 300:
            for k in list(_BAN_STATE)[:100]:
                _BAN_STATE.pop(k, None)
        return st


def ban_state_get(chat_id, message_id):
    with _ban_lock:
        return dict(_BAN_STATE.get(_mkey(chat_id, message_id)) or {})


def _btn(text, data):
    return {'text': text, 'callback_data': data}


def report_kb(rid, chat_ref=''):
    """Исходная клавиатура под жалобой — нужна и для relay, и для отмены."""
    msgs_cb = f'rep:{rid}:msgs:{chat_ref}' if chat_ref else f'rep:{rid}:msgs'
    return {'inline_keyboard': [[
        _btn('📄 отчёт', msgs_cb),
        _btn('🚫 отправить БАН', f'rep:{rid}:ban'),
    ]]}


def ban_kb_users(rid, users):
    rows = [[_btn(u['nick'], f'bu:{rid}:{i}')] for i, u in enumerate(users)]
    rows.append([_btn('✖️ отмена', f'ban:{rid}:x')])
    return {'inline_keyboard': rows}


def ban_kb_hours(rid):
    return {'inline_keyboard': [
        [_btn('1 час', f'bs:{rid}:h1'), _btn('3 часа', f'bs:{rid}:h3'),
         _btn('5 часов', f'bs:{rid}:h5')],
        [_btn('7 часов', f'bs:{rid}:h7'), _btn('10 часов', f'bs:{rid}:h10'),
         _btn('12 часов', f'bs:{rid}:h12')],
        [_btn('✖️ отмена', f'ban:{rid}:x'), _btn('дни ▶️', f'ban:{rid}:d')],
    ]}


def ban_kb_days(rid):
    return {'inline_keyboard': [
        [_btn('1 день', f'bs:{rid}:d1'), _btn('3 дня', f'bs:{rid}:d3'),
         _btn('5 дней', f'bs:{rid}:d5')],
        [_btn('7 дней', f'bs:{rid}:d7'), _btn('10 дней', f'bs:{rid}:d10')],
        [_btn('◀️ назад', f'ban:{rid}:h'), _btn('месяцы ▶️', f'ban:{rid}:m')],
    ]}


def ban_kb_months(rid):
    return {'inline_keyboard': [
        [_btn('1 месяц', f'bs:{rid}:m1'), _btn('2 месяца', f'bs:{rid}:m2'),
         _btn('3 месяца', f'bs:{rid}:m3')],
        [_btn('◀️ назад', f'ban:{rid}:d'), _btn('♾ НАВСЕГДА', f'bs:{rid}:perm')],
    ]}


BAN_SCREENS = {'h': ban_kb_hours, 'd': ban_kb_days, 'm': ban_kb_months}

BAN_LABELS = {
    'h1': '1 час', 'h3': '3 часа', 'h5': '5 часов', 'h7': '7 часов',
    'h10': '10 часов', 'h12': '12 часов',
    'd1': '1 день', 'd3': '3 дня', 'd5': '5 дней', 'd7': '7 дней',
    'd10': '10 дней',
    'm1': '1 месяц', 'm2': '2 месяца', 'm3': '3 месяца',
    'perm': 'НАВСЕГДА',
}


def _esc(s):
    return (str(s or '').replace('&', '&amp;')
            .replace('<', '&lt;').replace('>', '&gt;'))


def time_prompt(rid, nick):
    head = f'⏳ <b>На какое время забанить пользователя "{_esc(nick)}"?</b>'
    return head + (f'\nЖалоба #{rid}' if rid else '')


def resolve_users(rid, chat_ref, why=None):
    """Участники чата: сначала жалоба в БД, потом выгрузка с ВДС.
    Вызывать только из фонового потока — оба источника сетевые.
    why — список, куда пишем причины неудачи для сообщения админу."""
    why = why if why is not None else []
    users = []
    rep = get_report(rid) if rid else None
    if not rep:
        why.append(f'• жалоба #{rid or "—"} не читается из БД бота '
                   f'(состояние: {_pool_state}{": " + _pool_error if _pool_error else ""})')
    if rep:
        for uid, nick in ((rep.get('reported_id'), rep.get('reported_nickname')),
                          (rep.get('reporter_id'), None)):
            if uid:
                users.append({'id': str(uid), 'nick': nick or f'id {uid}'})
        if not chat_ref:
            chat_ref = re.sub(r'\D', '', str(rep.get('chat_id') or ''))

    if not users and not chat_ref:
        why.append('• в жалобе нет chat_id, поэтому участников не спросить у ВДС')

    if not users and chat_ref:
        content, info = fetch_chat_dump(chat_ref)
        if content is not None:
            for p in (info.get('participants') or []):
                users.append({'id': str(p.get('user_id')),
                              'nick': p.get('nickname') or f'id {p.get("user_id")}'})
            if not users:
                why.append(f'• ВДС отдал чат {chat_ref} без участников')
        else:
            why.append(f'• ВДС не дал участников чата {chat_ref}: {info}')
            log.warning('resolve_users: ВДС не дал участников чата %s: %s',
                        chat_ref, info)

    seen, out = set(), []
    for u in users:
        if u['id'] and u['id'] not in seen:
            seen.add(u['id'])
            out.append(u)
    return out


def open_user_screen(rid, admin_id, chat_id, message_id, orig_text, chat_ref,
                     users=None):
    """Меняет текст жалобы на выбор пользователя. Фоновый поток: если участники
    не пришли вместе с жалобой, их приходится тянуть из БД или с ВДС."""
    why = []
    users = users or resolve_users(rid, chat_ref, why)
    if not users:
        edit_keyboard(chat_id, message_id, orig_text, report_kb(rid, chat_ref))
        send(ADMIN_TELEGRAM_ID,
             f'❌ Не удалось определить участников жалобы #{rid or "—"}.\n\n'
             + ('\n'.join(why) if why else '• источники не вернули данных')
             + '\n\nСамое надёжное — чтобы ВДС передавал в POST /relay/report '
               'поля reported_id, reported_nickname, reporter_id, '
               'reporter_nickname и participants: тогда БД и ВДС не нужны.')
        return
    ban_state_put(chat_id, message_id, rid=rid, chat_ref=chat_ref,
                  text=orig_text, users=users, target=None)
    log_action(rid or None, admin_id, 'ban', 'user_screen')
    edit_keyboard(chat_id, message_id, USER_PROMPT, ban_kb_users(rid, users))


def handle_ban_button(report_id, admin_id, callback_id, cq_chat_id,
                      cq_msg_id, cq_msg=None, chat_ref=''):
    """Нажат «🚫 отправить БАН» под жалобой — сообщение жалобы превращается
    в экран выбора пользователя. Ничего нового не отправляется."""
    rid = report_id or 0
    st = ban_state_get(cq_chat_id, cq_msg_id)
    orig = st.get('text') or (cq_msg or {}).get('text') or 'Жалоба'
    chat_ref = chat_ref or st.get('chat_ref') or ''
    answer_callback(callback_id, '', alert=False)
    run_bg(open_user_screen, rid, admin_id, cq_chat_id, cq_msg_id, orig, chat_ref,
           st.get('users') or None)


def handle_ban_user_pick(rid, idx, admin_id, callback_id, cq_chat_id, cq_msg_id):
    """Пользователь выбран — тот же экран становится выбором срока."""
    st = ban_state_get(cq_chat_id, cq_msg_id)
    users = st.get('users') or []
    if idx < 0 or idx >= len(users):
        answer_callback(callback_id, 'Меню устарело — нажми «отправить БАН» заново')
        return
    target = users[idx]
    ban_state_put(cq_chat_id, cq_msg_id, target=target)
    log_action(rid or None, admin_id, f'ban_target:{target["id"]}', 'time_screen')
    answer_callback(callback_id, '', alert=False)
    edit_keyboard(cq_chat_id, cq_msg_id, time_prompt(rid, target['nick']),
                  ban_kb_hours(rid))


def handle_ban_nav(rid, screen, callback_id, cq_chat_id, cq_msg_id):
    """Отмена и переключение часы/дни/месяцы — всё в одном сообщении."""
    st = ban_state_get(cq_chat_id, cq_msg_id)

    if screen == 'x':
        answer_callback(callback_id, 'Отменено', alert=False)
        edit_keyboard(cq_chat_id, cq_msg_id,
                      st.get('text') or f'Жалоба #{rid or "—"}',
                      report_kb(rid, st.get('chat_ref') or ''))
        return

    if screen == 'u':
        users = st.get('users') or []
        if not users:
            answer_callback(callback_id, 'Меню устарело — нажми «отправить БАН» заново')
            return
        answer_callback(callback_id, '', alert=False)
        edit_keyboard(cq_chat_id, cq_msg_id, USER_PROMPT, ban_kb_users(rid, users))
        return

    kb = BAN_SCREENS.get(screen)
    if not kb:
        answer_callback(callback_id, f'Неизвестный экран: {screen}')
        return
    nick = (st.get('target') or {}).get('nick') or '—'
    answer_callback(callback_id, '', alert=False)
    edit_keyboard(cq_chat_id, cq_msg_id, time_prompt(rid, nick), kb(rid))


def vds_post(path, payload):
    """POST на ВДС с админским секретом. (ok: bool, данные_или_текст_ошибки)."""
    if not VDS_ADMIN_SECRET:
        return False, ('VDS_ADMIN_SECRET не задан в переменных Render.\n'
                       'Он должен совпадать с ADMIN_DUMP_SECRET в .env на ВДС.')
    if not VDS_BASE_URL:
        return False, 'VDS_BASE_URL не задан в переменных Render'
    try:
        r = requests.post(f'{VDS_BASE_URL}{path}',
                          json=payload,
                          headers={'X-Admin-Secret': VDS_ADMIN_SECRET},
                          timeout=20)
        try:
            body = r.json()
        except Exception:
            body = {}
        if r.status_code == 200 and body.get('success'):
            return True, body
        return False, (f'ВДС ответил {r.status_code}: '
                       f'{body.get("error") or r.text[:200]}')
    except Exception as e:
        return False, f'{type(e).__name__}: {str(e)[:200]}'


def vds_get(path, params):
    if not VDS_ADMIN_SECRET or not VDS_BASE_URL:
        return False, 'VDS_ADMIN_SECRET / VDS_BASE_URL не заданы на Render'
    try:
        r = requests.get(f'{VDS_BASE_URL}{path}', params=params,
                         headers={'X-Admin-Secret': VDS_ADMIN_SECRET}, timeout=20)
        body = r.json() if r.content else {}
        if r.status_code == 200 and body.get('success'):
            return True, body
        return False, f'ВДС ответил {r.status_code}: {body.get("error") or r.text[:200]}'
    except Exception as e:
        return False, f'{type(e).__name__}: {str(e)[:200]}'


def _fmt_until(iso_str):
    """'2026-08-07T14:32:11+00:00' -> '07.08.2026 14:32 UTC'."""
    if not iso_str:
        return 'навсегда'
    try:
        dt = datetime.fromisoformat(str(iso_str).replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
    except Exception:
        return str(iso_str)


def unban_kb(rid, user_id):
    """Кнопка отмены под подтверждением бана — на случай, если бан отправлен
    случайно. Снятие доходит до пользователя в реальном времени."""
    return {'inline_keyboard': [[
        _btn('♻️ ОТМЕНИТЬ БАН', f'ub:{rid}:{user_id}'),
    ]]}


def _do_ban(rid, code, admin_id, cq_chat_id, cq_msg_id, target):
    """Фоновый поток: POST /admin/ban на ВДС и правка того же сообщения."""
    label = BAN_LABELS.get(code, code)
    nick = target.get('nick') or f'id {target.get("id")}'
    ok, res = vds_post('/admin/ban', {
        'user_id': target['id'],
        'duration': code,
        'reason': f'Жалоба #{rid}' if rid else 'Решение модератора',
        'report_id': rid or None,
        'admin_id': str(admin_id),
    })

    if not ok:
        log_action(rid or None, admin_id, f'ban_pick:{code}', f'error: {res}'[:200])
        restore_report_view(
            rid, cq_chat_id, cq_msg_id,
            note=(f'❌ <b>Бан НЕ выдан</b>\nПользователь: {_esc(nick)} — '
                  f'{_esc(label)}\n{_esc(res)}'))
        return

    log_action(rid or None, admin_id, f'ban_pick:{code}', f'banned:{target["id"]}')
    permanent = bool(res.get('is_permanent'))
    lines = [
        '🚫 <b>БАН ВЫДАН</b>',
        '',
        f'Пользователь: <b>{_esc(nick)}</b> (id {_esc(target["id"])})',
        f'Срок: <b>{_esc(label)}</b>',
    ]
    if not permanent:
        lines.append(f'Истекает: {_esc(_fmt_until(res.get("expires_at")))}')
    else:
        lines.append('Истекает: <b>никогда</b>')
    if rid:
        lines.append(f'Жалоба #{rid}')
    lines += ['', 'Пользователь уже видит экран блокировки — страницы платформы '
                  'заблокированы, из аккаунта его не выкидывало.']
    edit_keyboard(cq_chat_id, cq_msg_id, '\n'.join(lines),
                  unban_kb(rid, target['id']))


def handle_ban_pick(rid, code, admin_id, callback_id, cq_chat_id, cq_msg_id):
    """Срок выбран — реально отправляем бан на ВДС."""
    st = ban_state_get(cq_chat_id, cq_msg_id)
    target = st.get('target') or {}
    if not target.get('id'):
        answer_callback(callback_id, 'Меню устарело — нажми «отправить БАН» заново')
        return
    answer_callback(callback_id, '', alert=False)
    edit_keyboard(cq_chat_id, cq_msg_id,
                  f'⏳ Отправляю бан {_esc(target.get("nick") or target["id"])} — '
                  f'{_esc(BAN_LABELS.get(code, code))}…', {'inline_keyboard': []})
    run_bg(_do_ban, rid, code, admin_id, cq_chat_id, cq_msg_id, target)


def restore_report_view(rid, cq_chat_id, cq_msg_id, note='', fallback_text=''):
    """Возвращает сообщение к исходному тексту жалобы с кнопками «отчёт» и
    «отправить БАН» — чтобы сразу можно было выдать другой срок.
    Оригинал жалобы лежит в _BAN_STATE (положен при доставке через relay),
    поэтому HTML в нём валидный. Если состояние потерялось (рестарт процесса),
    берём fallback_text."""
    st = ban_state_get(cq_chat_id, cq_msg_id)
    body = st.get('text') or fallback_text or f'Жалоба #{rid or "—"}'
    text = (note + '\n\n' + body) if note else body
    # цель сбрасываем: следующий «отправить БАН» снова начнётся с выбора юзера
    ban_state_put(cq_chat_id, cq_msg_id, target=None)
    edit_keyboard(cq_chat_id, cq_msg_id, text,
                  report_kb(rid, st.get('chat_ref') or ''))


def _do_unban(rid, user_id, admin_id, cq_chat_id, cq_msg_id, orig_text):
    ok, res = vds_post('/admin/unban', {'user_id': str(user_id),
                                        'admin_id': str(admin_id)})
    if not ok:
        log_action(rid or None, admin_id, f'unban:{user_id}', f'error: {res}'[:200])
        edit_keyboard(cq_chat_id, cq_msg_id,
                      orig_text + f'\n\n❌ Снять бан не удалось: {_esc(res)}',
                      unban_kb(rid, user_id))
        return
    log_action(rid or None, admin_id, f'unban:{user_id}', 'lifted')

    note = (f'♻️ <b>Бан снят</b> — id {_esc(user_id)} снова пользуется платформой '
            f'(блокировка исчезла у него сразу, без перезагрузки).\n'
            f'Ниже снова жалоба: можно выдать другой срок.')

    st = ban_state_get(cq_chat_id, cq_msg_id)
    if st.get('text'):
        restore_report_view(rid, cq_chat_id, cq_msg_id, note=note)
    else:
        # жалобы под этим сообщением нет (например, бан из /ban) — тогда просто
        # подтверждение, но с кнопкой, чтобы забанить заново.
        edit_keyboard(cq_chat_id, cq_msg_id, note,
                      {'inline_keyboard': [[
                          _btn('🚫 отправить БАН', f'rep:{rid or 0}:ban'),
                      ]]})


def handle_unban_button(rid, user_id, admin_id, callback_id, cq_chat_id, cq_msg_id,
                        cq_msg=None):
    """Нажата «♻️ ОТМЕНИТЬ БАН» под подтверждением бана."""
    orig = (cq_msg or {}).get('text') or 'Бан'
    answer_callback(callback_id, 'Снимаю бан…', alert=False)
    run_bg(_do_unban, rid, user_id, admin_id, cq_chat_id, cq_msg_id, _esc(orig))


# ------------------------------------------------ текстовые команды бана

def cmd_ban(chat_id, user_id, code, admin_id):
    """Бан вручную, без жалобы: /ban 123 d3"""
    if code not in BAN_LABELS:
        send(chat_id, f'Неизвестный код срока: {code}\n\nДоступно: '
                      + ', '.join(BAN_LABELS.keys()))
        return
    ok, res = vds_post('/admin/ban', {
        'user_id': str(user_id), 'duration': code,
        'reason': 'Решение модератора', 'admin_id': str(admin_id),
    })
    if not ok:
        send(chat_id, f'❌ Бан не выдан: {res}')
        return
    log_action(None, admin_id, f'ban_cmd:{code}', f'banned:{user_id}')
    tail = ('навсегда' if res.get('is_permanent')
            else f'до {_fmt_until(res.get("expires_at"))}')
    send_keyboard(chat_id,
                  f'🚫 <b>БАН ВЫДАН</b>\n\nПользователь id {_esc(user_id)}\n'
                  f'Срок: <b>{_esc(BAN_LABELS.get(code, code))}</b> ({_esc(tail)})',
                  unban_kb(0, user_id))


def cmd_unban(chat_id, user_id, admin_id):
    ok, res = vds_post('/admin/unban', {'user_id': str(user_id),
                                        'admin_id': str(admin_id)})
    if not ok:
        send(chat_id, f'❌ Снять бан не удалось: {res}')
        return
    log_action(None, admin_id, f'unban_cmd:{user_id}', 'lifted')
    send(chat_id, f'♻️ Бан снят: id {user_id}\nБлокировка у пользователя исчезла '
                  f'сразу, перезагружать страницу ему не нужно.')


def cmd_baninfo(chat_id, user_id):
    ok, res = vds_get('/admin/ban_status', {'user_id': str(user_id)})
    if not ok:
        send(chat_id, f'❌ {res}')
        return
    if not res.get('is_banned'):
        send(chat_id, f'✅ id {user_id} не забанен')
        return
    send_keyboard(chat_id,
                  f'🚫 id {_esc(user_id)} <b>забанен</b>\n'
                  f'Выдан: {_esc(_fmt_until(res.get("banned_at")))}\n'
                  f'Истекает: {_esc(_fmt_until(res.get("expires_at")))}\n'
                  f'Причина: {_esc(res.get("reason") or "—")}',
                  unban_kb(0, user_id))


# --------------------------------------------------------------- вебхук

def ensure_webhook():
    """Ставит вебхук при старте. Без него callback_query от кнопок и команды
    не приходят вообще, хотя жалобы через /relay/report доходят нормально."""
    if not BOT_TOKEN:
        return
    base = PUBLIC_URL
    if not base:
        log.warning('PUBLIC_URL не задан — вебхук не выставлен, кнопки работать '
                    'не будут. Открой /set_webhook?secret=<WEBHOOK_SECRET>.')
        return
    if not base.startswith('https://'):
        base = 'https://' + base.split('://', 1)[-1]

    url = f'{base}/webhook/{WEBHOOK_TOKEN}'
    try:
        info = requests.get(f'{TELEGRAM_API}/getWebhookInfo', timeout=10).json()
        current = ((info.get('result') or {}).get('url') or '')
        if current == url:
            log.info('вебхук уже стоит: %s', url)
            return
        log.info('текущий вебхук %r -> ставлю %r', current, url)
    except Exception as e:
        log.error('getWebhookInfo: %s', e)

    res = tg('setWebhook', url=url, secret_token=WEBHOOK_TOKEN,
             allowed_updates='["message","callback_query"]',
             drop_pending_updates='true')
    if res and res.get('ok'):
        log.info('вебхук установлен: %s', url)
        return

    log.error('setWebhook НЕ УДАЛСЯ: %s', res)
    desc = str((res or {}).get('description') or '')

    # Фолбэк: если Telegram всё же ругается на secret_token — ставим без него.
    # Защита остаётся за счёт секретного пути в URL.
    if 'secret token' in desc.lower():
        log.warning('повторяю setWebhook без secret_token')
        res2 = tg('setWebhook', url=url,
                  allowed_updates='["message","callback_query"]',
                  drop_pending_updates='true')
        if res2 and res2.get('ok'):
            log.info('вебхук установлен без secret_token: %s', url)
            return
        log.error('повтор тоже не удался: %s', res2)

    send(ADMIN_TELEGRAM_ID,
         '⚠️ Вебхук не установился — кнопки и команды работать не будут.\n\n'
         f'Telegram: {desc or res}\n\nURL: {url}')


@app.before_request
def _ensure_init():
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            _initialized = True
            # Сначала вебхук — он важнее и быстрее. БД поднимается в фоне,
            # чтобы недоступная база не задерживала старт.
            try:
                ensure_webhook()
            except Exception as e:
                log.error('ensure_webhook: %s', e)
            threading.Thread(target=_open_pool_bg, daemon=True).start()


@app.route('/health', methods=['GET'])
@app.route('/', methods=['GET'])
def health():
    return jsonify({
        'status': 'ok',
        'service': 'new_bot',
        'bot_token': bool(BOT_TOKEN),
        'relay_secret_set': bool(RELAY_SECRET),
        'vds_base_url': VDS_BASE_URL,
        'vds_admin_secret_set': bool(VDS_ADMIN_SECRET),
        'public_url_set': bool(PUBLIC_URL),
        'webhook_secret_sanitized': WEBHOOK_TOKEN != WEBHOOK_SECRET,
        'db': _pool_state,
        'time': datetime.now(timezone.utc).isoformat(),
    })


@app.route('/webhook/<secret>', methods=['POST'])
def webhook(secret):
    if secret not in _VALID_WEBHOOK_PATHS:
        log.warning('вебхук: неверный путь с %s', request.remote_addr)
        return jsonify({'ok': False}), 403

    header_token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
    if header_token and header_token not in _VALID_HEADER_TOKENS:
        log.warning('вебхук: неверный secret-token в заголовке')
        return jsonify({'ok': False}), 403

    update = request.get_json(silent=True) or {}
    uid = update.get('update_id')
    if uid is not None:
        with _seen_lock:
            if uid in _seen_updates:
                return jsonify({'ok': True})
            _seen_updates.add(uid)
            if len(_seen_updates) > 500:
                _seen_updates.clear()

    try:
        cq = update.get('callback_query')
        if cq:
            callback_id = cq.get('id')
            from_id = str((cq.get('from') or {}).get('id', ''))
            data = cq.get('data') or ''
            log.info('callback от %s: %s', from_id, data)

            if from_id != ADMIN_TELEGRAM_ID:
                answer_callback(callback_id, 'Нет доступа')
                return jsonify({'ok': True})

            cq_msg = cq.get('message') or {}
            cq_chat_id = (cq_msg.get('chat') or {}).get('id') or ADMIN_TELEGRAM_ID
            cq_msg_id = cq_msg.get('message_id')

            # Формат: rep:<report_id>:<action>[:<chat_id>]
            parts = data.split(':')
            if len(parts) == 3 and parts[0] == 'ban':
                rid = int(parts[1]) if parts[1].isdigit() else 0
                handle_ban_nav(rid, parts[2], callback_id, cq_chat_id, cq_msg_id)
                return jsonify({'ok': True})
            if len(parts) == 3 and parts[0] == 'bs':
                rid = int(parts[1]) if parts[1].isdigit() else 0
                handle_ban_pick(rid, parts[2], from_id, callback_id,
                                cq_chat_id, cq_msg_id)
                return jsonify({'ok': True})
            if len(parts) == 3 and parts[0] == 'ub':
                rid = int(parts[1]) if parts[1].isdigit() else 0
                handle_unban_button(rid, parts[2], from_id, callback_id,
                                    cq_chat_id, cq_msg_id, cq_msg)
                return jsonify({'ok': True})
            if len(parts) == 3 and parts[0] == 'bu':
                rid = int(parts[1]) if parts[1].isdigit() else 0
                idx = int(parts[2]) if parts[2].isdigit() else -1
                handle_ban_user_pick(rid, idx, from_id, callback_id,
                                     cq_chat_id, cq_msg_id)
                return jsonify({'ok': True})
            if len(parts) >= 3 and parts[0] == 'rep':
                report_id = int(parts[1]) if parts[1].isdigit() else None
                action = parts[2]
                cb_chat_id = parts[3] if len(parts) > 3 else ''
                if action == 'msgs':
                    handle_messages_button(report_id, from_id, callback_id, cb_chat_id)
                elif action == 'ban':
                    handle_ban_button(report_id, from_id, callback_id, cq_chat_id,
                                      cq_msg_id, cq_msg, cb_chat_id)
                else:
                    answer_callback(callback_id, f'Неизвестное действие: {action}')
            else:
                answer_callback(callback_id, f'Неизвестная кнопка: {data[:40]}')
            return jsonify({'ok': True})

        msg = update.get('message') or update.get('edited_message')
        if msg:
            chat_id = (msg.get('chat') or {}).get('id')
            from_id = str((msg.get('from') or {}).get('id', ''))
            text = (msg.get('text') or '').strip()
            log.info('message от %s: %s', from_id, text[:60])

            if text.startswith('/id'):
                send(chat_id, f'Твой Telegram ID: {from_id}')
            elif from_id != ADMIN_TELEGRAM_ID:
                send(chat_id, 'Этот бот служебный.')
            elif text.startswith('/start') or text.startswith('/help'):
                send(chat_id, 'Бот жалоб на связи.\n\n'
                              '/diag — самопроверка\n'
                              '/dump 29 — выгрузить переписку чата 29\n'
                              '/last — последние жалобы\n'
                              '/ban 123 d3 — забанить (коды: h1 h3 h5 h7 h10 h12, '
                              'd1 d3 d5 d7 d10, m1 m2 m3, perm)\n'
                              '/unban 123 — снять бан\n'
                              '/baninfo 123 — проверить бан')
            elif text.startswith('/unban'):
                arg = re.sub(r'\D', '', text[6:])
                if not arg:
                    send(chat_id, 'Формат: /unban 123 — id пользователя на платформе')
                else:
                    run_bg(cmd_unban, chat_id, arg, from_id)
            elif text.startswith('/baninfo'):
                arg = re.sub(r'\D', '', text[8:])
                if not arg:
                    send(chat_id, 'Формат: /baninfo 123')
                else:
                    run_bg(cmd_baninfo, chat_id, arg)
            elif text.startswith('/ban'):
                arg_parts = text.split()
                m_ok = len(arg_parts) == 3 and arg_parts[1].isdigit()
                if not m_ok:
                    send(chat_id, 'Формат: /ban 123 d3\n\nКоды срока: h1 h3 h5 h7 '
                                  'h10 h12, d1 d3 d5 d7 d10, m1 m2 m3, perm')
                else:
                    run_bg(cmd_ban, chat_id, arg_parts[1], arg_parts[2], from_id)
            elif text.startswith('/last'):
                run_bg(lambda: send(chat_id, format_last_reports()))
            elif text.startswith('/diag'):
                run_bg(send_diag, chat_id)
            elif text.startswith('/dump'):
                # Терпим любой ввод: /dump 29, /dump <29>, /dump <id 29>
                arg = re.sub(r'\D', '', text[5:])
                if not arg:
                    send(chat_id, 'Формат: /dump 29 — только число, без <> и слова id')
                else:
                    send(chat_id, f'Тяну чат {arg}…')
                    run_bg(deliver_dump, arg, None, from_id)
            else:
                send(chat_id, 'Не понял. Команды: /diag, /dump 29, /last')
        return jsonify({'ok': True})
    except Exception as e:
        log.exception('webhook: %s', e)
        # Telegram всегда получает 200, иначе будет ретраить апдейт бесконечно
        return jsonify({'ok': True})


def send_diag(chat_id):
    lines = ['🔧 <b>Самопроверка</b>', '']
    lines.append(f'BOT_TOKEN: {"есть" if BOT_TOKEN else "НЕТ"}')
    lines.append(f'PUBLIC_URL: {PUBLIC_URL or "НЕ ЗАДАН"}')
    lines.append(f'VDS_BASE_URL: {VDS_BASE_URL or "НЕ ЗАДАН"}')
    lines.append(f'VDS_ADMIN_SECRET: {"есть" if VDS_ADMIN_SECRET else "НЕТ"}')
    lines.append(f'RELAY_SECRET: {"есть" if RELAY_SECRET else "НЕТ"}')
    if WEBHOOK_TOKEN != WEBHOOK_SECRET:
        lines.append('WEBHOOK_SECRET: содержал недопустимые символы, '
                     'используется производный токен')
    db_line = {'ok': 'подключена', 'pending': 'подключается…',
               'disabled': 'не настроена (DATABASE_URL пуст)',
               'error': f'ОШИБКА — {_pool_error}'}.get(_pool_state, _pool_state)
    lines.append(f'БД бота: {db_line}')
    lines.append('')

    try:
        wi = requests.get(f'{TELEGRAM_API}/getWebhookInfo', timeout=10).json()
        r = wi.get('result') or {}
        lines.append(f'Вебхук URL: {r.get("url") or "НЕ УСТАНОВЛЕН ← кнопки не работают"}')
        lines.append(f'Ожидает апдейтов: {r.get("pending_update_count")}')
        if r.get('last_error_message'):
            lines.append(f'Последняя ошибка Telegram: {r["last_error_message"]}')
    except Exception as e:
        lines.append(f'getWebhookInfo упал: {e}')

    lines.append('')
    try:
        r = requests.get(f'{VDS_BASE_URL}/admin/dump_selftest',
                         headers={'X-Admin-Secret': VDS_ADMIN_SECRET}, timeout=20)
        lines.append(f'ВДС /admin/dump_selftest: {r.status_code}')
        lines.append(f'<code>{(r.text or "")[:600]}</code>')
    except Exception as e:
        lines.append(f'ВДС недоступен: {type(e).__name__}: {str(e)[:200]}')

    send(chat_id, '\n'.join(lines), html=True)


def format_last_reports(limit: int = 5) -> str:
    if not pool:
        return (f'БД недоступна ({_pool_state}: {_pool_error or "—"}).\n'
                'Список жалоб не покажу, но выгрузка чата от БД не зависит: /dump 29')
    try:
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT id, reporter_id, reason, chat_id, created_at
                     FROM user_reports ORDER BY id DESC LIMIT %s""",
                (limit,)
            ).fetchall()
        if not rows:
            return 'Жалоб пока нет.'
        return '\n'.join(
            f'#{r[0]} от {r[1]} — {r[2]} · чат {r[3] or "—"} ({r[4]:%d.%m %H:%M})'
            for r in rows
        )
    except Exception as e:
        return f'Ошибка БД: {e}'


@app.route('/relay/report', methods=['POST'])
def relay_report():
    """ВДС из РФ не видит api.telegram.org, поэтому пушит жалобу сюда."""
    if RELAY_SECRET:
        if request.headers.get('X-Relay-Secret', '') != RELAY_SECRET:
            log.warning('relay: неверный секрет с %s', request.remote_addr)
            return jsonify({'ok': False, 'error': 'forbidden'}), 403

    if not BOT_TOKEN:
        return jsonify({'ok': False, 'error': 'no_bot_token'}), 500

    data = request.get_json(silent=True) or {}
    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'ok': False, 'error': 'empty_text'}), 400

    try:
        rid = int(data.get('report_id') or 0)
    except (TypeError, ValueError):
        rid = 0

    chat_ref = re.sub(r'\D', '', str(data.get('chat_id') or ''))[:20]
    if not chat_ref:
        log.warning('relay: жалоба #%s пришла без chat_id — кнопка выгрузки '
                    'сможет опереться только на БД', rid)

    # Участники прямо из жалобы: тогда экран «кому отправить» открывается
    # мгновенно, без запроса в БД и на ВДС.
    users = []
    for uid_key, nick_key in (('reported_id', 'reported_nickname'),
                              ('reporter_id', 'reporter_nickname')):
        uid = str(data.get(uid_key) or '').strip()
        if uid:
            users.append({'id': uid,
                          'nick': str(data.get(nick_key) or '').strip() or f'id {uid}'})
    for p in (data.get('participants') or []):
        uid = str((p or {}).get('user_id') or (p or {}).get('id') or '').strip()
        if uid and uid not in {u['id'] for u in users}:
            users.append({'id': uid,
                          'nick': str((p or {}).get('nickname') or '').strip() or f'id {uid}'})

    keyboard = report_kb(rid, chat_ref)

    body = tg(
        'sendMessage',
        chat_id=ADMIN_TELEGRAM_ID,
        text=text,
        parse_mode='HTML',
        disable_web_page_preview='true',
        reply_markup=json.dumps(keyboard, ensure_ascii=False),
    )

    if not body or not body.get('ok'):
        log.error('relay: sendMessage не ок: %s', body)
        return jsonify({'ok': False, 'error': 'telegram_failed', 'telegram': body}), 502

    message_id = body['result']['message_id']
    # Оригинал жалобы нужен, чтобы «отмена» вернула сообщение как было —
    # меню бана редактирует это же сообщение, а не отправляет новое.
    ban_state_put(int(ADMIN_TELEGRAM_ID) if ADMIN_TELEGRAM_ID.isdigit()
                  else ADMIN_TELEGRAM_ID, message_id,
                  rid=rid, chat_ref=chat_ref, text=text,
                  users=users, target=None)
    log.info('relay: жалоба #%s доставлена (message_id=%s, chat_id=%s)',
             rid, message_id, chat_ref or '—')
    return jsonify({'ok': True, 'message_id': message_id})


@app.route('/set_webhook', methods=['GET', 'POST'])
def set_webhook():
    """/set_webhook?secret=WEBHOOK_SECRET — принимает и сырой секрет, и токен."""
    given = request.args.get('secret', '')
    if given not in _VALID_WEBHOOK_PATHS:
        return jsonify({'ok': False, 'error': 'forbidden'}), 403

    base = PUBLIC_URL or request.url_root.rstrip('/').replace('http://', 'https://')
    url = f'{base}/webhook/{WEBHOOK_TOKEN}'
    res = tg('setWebhook', url=url, secret_token=WEBHOOK_TOKEN,
             allowed_updates='["message","callback_query"]',
             drop_pending_updates='true')
    if not (res and res.get('ok')):
        res_no_token = tg('setWebhook', url=url,
                          allowed_updates='["message","callback_query"]',
                          drop_pending_updates='true')
    else:
        res_no_token = None
    try:
        info = requests.get(f'{TELEGRAM_API}/getWebhookInfo', timeout=10).json()
    except Exception as e:
        info = {'error': str(e)}
    return jsonify({
        'requested_url': url,
        'secret_was_sanitized': WEBHOOK_TOKEN != WEBHOOK_SECRET,
        'telegram': res,
        'telegram_retry_without_secret_token': res_no_token,
        'webhook_info': info,
    })


if __name__ == '__main__':
    ensure_webhook()
    threading.Thread(target=_open_pool_bg, daemon=True).start()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5002)), debug=False)
