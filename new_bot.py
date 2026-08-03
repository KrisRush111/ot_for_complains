# -*- coding: utf-8 -*-
"""
new_bot.py — Telegram-бот жалоб (Render Web Service, gunicorn, webhook).

ЧТО ИСПРАВЛЕНО В ЭТОЙ ВЕРСИИ (почему кнопка «сообщения чата» молчала):

1. ВЕБХУК МОГ БЫТЬ НЕ УСТАНОВЛЕН.
   Жалобы приходят админу через /relay/report (ВДС сам пушит их HTTP-запросом),
   поэтому сообщения в Telegram появляются даже если вебхук не настроен.
   А вот НАЖАТИЕ кнопки прилетает только через вебхук. Нет вебхука ->
   callback_query никуда не приходит -> кнопка крутится и ничего не делает.
   Теперь вебхук ставится автоматически при старте (ensure_webhook), плюс есть
   /diag, который показывает getWebhookInfo.

2. ЗАПРОС К ВДС ВЫПОЛНЯЛСЯ ВНУТРИ ВЕБХУКА (до 45+45 сек).
   gunicorn по умолчанию убивает воркер через 30 сек -> ответ Telegram не
   уходит, файл не отправляется, в логах только SIGKILL. Плюс Telegram
   ретраит апдейт и всё повторяется по кругу.
   Теперь тяжёлая работа уходит в фоновый поток, вебхук отвечает 200 сразу.

3. ОШИБКИ ГЛОТАЛИСЬ.
   Если VDS_ADMIN_SECRET не задан / не совпал, или в жалобе нет chat_id —
   админ не видел причины. Теперь любая ошибка приходит админу текстом
   с конкретным кодом и подсказкой, что править.

Дополнительно:
  * /diag           — состояние конфига + пинг ВДС + getWebhookInfo
  * /dump <chat_id> — выгрузить переписку вручную, не дожидаясь жалобы
  * chat_id больше не обязан быть числом до отправки — валидирует ВДС
  * .html дублируется .txt-версией (Telegram на телефоне HTML не превьюит)

Запуск на Render:
  Build:  pip install -r requirements.txt
  Start:  gunicorn new_bot:app -c gunicorn_bot.py
  ВАЖНО: в gunicorn_bot.py -> timeout = 120, threads = 4

Переменные окружения (Render):
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
import logging
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify

from psycopg_pool import ConnectionPool

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

pool = None
if DATABASE_URL:
    pool = ConnectionPool(
        DATABASE_URL, min_size=1, max_size=3, timeout=10,
        kwargs={'autocommit': True}, open=False
    )
    try:
        pool.open()
        log.info('PostgreSQL pool открыт')
    except Exception as e:
        log.error('Пул к БД не открылся: %s', e)


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

# Чтобы один и тот же апдейт (Telegram ретраит при таймауте) не выгружался дважды
_seen_updates = set()
_seen_lock = threading.Lock()


def init_db():
    if not pool:
        return
    try:
        with pool.connection() as conn:
            conn.execute(SCHEMA_SQL)
        log.info('Схема report_actions проверена/создана')
    except Exception as e:
        log.error('init_db: %s', e)


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
        log.error('%s исключение: %s', method, e)
        return None


def answer_callback(callback_id: str, text: str, alert: bool = True):
    tg('answerCallbackQuery', callback_query_id=callback_id, text=text[:190],
       show_alert='true' if alert else 'false')


def send(chat_id, text: str, html: bool = False):
    kw = {'chat_id': chat_id, 'text': text[:4000], 'disable_web_page_preview': 'true'}
    if html:
        kw['parse_mode'] = 'HTML'
    tg('sendMessage', **kw)


def send_document(chat_id, filename: str, content: bytes,
                  caption: str = '', mime: str = 'text/html'):
    if not BOT_TOKEN:
        return None
    try:
        r = requests.post(
            f'{TELEGRAM_API}/sendDocument',
            data={'chat_id': chat_id, 'caption': caption[:1000], 'parse_mode': 'HTML'},
            files={'document': (filename, io.BytesIO(content), mime)},
            timeout=90,
        )
        body = r.json() if r.content else {}
        if not body.get('ok'):
            log.error('sendDocument -> %s', body)
        return body
    except Exception as e:
        log.error('sendDocument исключение: %s', e)
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
    """Фолбэк: если в кнопке нет chat_id, пробуем взять его из БД.
    Работает только если DATABASE_URL бота смотрит в ту же базу, что и ВДС."""
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
    Сообщения в БД зашифрованы Fernet, ключ есть только на ВДС — поэтому
    расшифровку делает он, бот только пересылает готовый HTML.
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
                          'Скорее всего чат удалён «у всех» — строки стёрты безвозвратно.')
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

        rh = requests.get(url, params={'chat_id': chat_id},
                          headers=headers, timeout=30)
        if not rh.ok:
            return None, f'ВДС ответил {rh.status_code} на HTML-выгрузку'
        return rh.content, stats
    except requests.Timeout:
        return None, 'ВДС не ответил за 30 секунд (сервер/БД тормозит или лежит)'
    except requests.ConnectionError as e:
        return None, (f'Не достучался до {VDS_BASE_URL}: {str(e)[:180]}\n'
                      'Проверь домен, HTTPS-сертификат и что сервер запущен.')
    except Exception as e:
        return None, f'{type(e).__name__}: {str(e)[:200]}'


def dump_to_text(payload) -> bytes:
    """Плоская .txt-копия: Telegram на телефоне HTML-файл не показывает превью,
    а txt открывает прямо в приложении."""
    lines = [f'Переписка чата #{payload.get("chat_id")}',
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
        send(ADMIN_TELEGRAM_ID,
             f'❌ Не удалось выгрузить чат {chat_id}\n\n{info}')
        return False

    caption = (
        f'💬 <b>Переписка чата {chat_id}</b>\n'
        + (f'Жалоба #{report_id}\n' if report_id else '')
        + f'\nВсего сообщений: <b>{info["total"]}</b>\n'
          f'Удалено у кого-то: <b>{info["deleted"]}</b>\n'
          f'Изображений: <b>{info["images"]}</b>\n\n'
          '.html — открывается в браузере, .txt — прямо в Telegram.'
    )

    res = send_document(ADMIN_TELEGRAM_ID, f'chat_{chat_id}.html', content, caption)
    ok = bool(res and res.get('ok'))

    # txt-копия: HTML в мобильном Telegram не превьюится
    try:
        txt = dump_to_text(info.get('json') or {})
        send_document(ADMIN_TELEGRAM_ID, f'chat_{chat_id}.txt', txt,
                      '', mime='text/plain')
    except Exception as e:
        log.error('txt-копия не собралась: %s', e)

    if not ok:
        send(ADMIN_TELEGRAM_ID,
             f'⚠️ Дамп чата {chat_id} получен с ВДС, но Telegram не принял файл. '
             f'Ответ: {str(res)[:300]}')
    log_action(report_id, admin_id, 'view_messages',
               'done' if ok else 'error: sendDocument')
    return ok


def run_bg(fn, *args, **kwargs):
    """Фон: вебхук обязан ответить 200 за пару секунд, иначе gunicorn убьёт
    воркер по timeout, а Telegram начнёт ретраить апдейт."""
    t = threading.Thread(target=_bg_wrap, args=(fn, args, kwargs), daemon=True)
    t.start()


def _bg_wrap(fn, args, kwargs):
    try:
        fn(*args, **kwargs)
    except Exception as e:
        log.exception('фоновая задача упала: %s', e)
        send(ADMIN_TELEGRAM_ID, f'❌ Внутренняя ошибка бота: {type(e).__name__}: {str(e)[:300]}')


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
             'Причины: жалоба отправлена не из чата, или фронт не передал '
             'chat_id в POST /report, или кнопка старая (создана до патча).\n\n'
             'Можно выгрузить вручную: /dump <id чата>')
        return

    # Отвечаем Telegram сразу, дальше работаем в фоне
    answer_callback(callback_id, 'Собираю переписку, файл придёт через пару секунд…',
                    alert=False)
    run_bg(deliver_dump, chat_id, report_id, admin_id)


def handle_ban_button(report_id, admin_id, callback_id):
    """TODO: POST /admin/ban на ВДС с X-Admin-Secret и reported_id из жалобы."""
    log_action(report_id, admin_id, 'ban', 'pending')
    answer_callback(callback_id, 'Бан: функция пока не подключена')


# --------------------------------------------------------------- вебхук

def ensure_webhook():
    """Ставит вебхук при старте. Без него callback_query от кнопок не приходит,
    и кнопка выглядит «мёртвой», хотя жалобы через relay доходят нормально."""
    if not BOT_TOKEN:
        return
    base = PUBLIC_URL
    if not base:
        log.warning('PUBLIC_URL не задан — вебхук не выставлен, кнопки работать не будут. '
                    'Открой /set_webhook?secret=<WEBHOOK_SECRET> вручную.')
        return
    url = f'{base}/webhook/{WEBHOOK_SECRET}'
    try:
        info = requests.get(f'{TELEGRAM_API}/getWebhookInfo', timeout=10).json()
        current = ((info.get('result') or {}).get('url') or '')
        if current == url:
            log.info('вебхук уже стоит: %s', url)
            return
    except Exception as e:
        log.error('getWebhookInfo: %s', e)
    res = tg('setWebhook', url=url, secret_token=WEBHOOK_SECRET,
             allowed_updates='["message","callback_query"]',
             drop_pending_updates='true')
    log.info('setWebhook -> %s', res)


@app.before_request
def _ensure_init():
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            init_db()
            ensure_webhook()
            _initialized = True


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
        'time': datetime.now(timezone.utc).isoformat(),
    })


@app.route('/webhook/<secret>', methods=['POST'])
def webhook(secret):
    if secret != WEBHOOK_SECRET:
        return jsonify({'ok': False}), 403

    header_token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
    if header_token and header_token != WEBHOOK_SECRET:
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

            # Формат: rep:<report_id>:<action>[:<chat_id>]
            parts = data.split(':')
            if len(parts) >= 3 and parts[0] == 'rep':
                report_id = int(parts[1]) if parts[1].isdigit() else None
                action = parts[2]
                cb_chat_id = parts[3] if len(parts) > 3 else ''
                if action == 'msgs':
                    handle_messages_button(report_id, from_id, callback_id, cb_chat_id)
                elif action == 'ban':
                    handle_ban_button(report_id, from_id, callback_id)
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

            if text.startswith('/id'):
                send(chat_id, f'Твой Telegram ID: {from_id}')
            elif from_id != ADMIN_TELEGRAM_ID:
                send(chat_id, 'Этот бот служебный.')
            elif text.startswith('/start'):
                send(chat_id, 'Бот жалоб на связи.\n\n'
                              '/diag — самопроверка\n'
                              '/dump <id чата> — выгрузить переписку\n'
                              '/last — последние жалобы')
            elif text.startswith('/last'):
                send(chat_id, format_last_reports())
            elif text.startswith('/diag'):
                run_bg(send_diag, chat_id)
            elif text.startswith('/dump'):
                arg = re.sub(r'\D', '', text[5:])
                if not arg:
                    send(chat_id, 'Формат: /dump 123')
                else:
                    send(chat_id, f'Тяну чат {arg}…')
                    run_bg(deliver_dump, arg, None, from_id)
        return jsonify({'ok': True})
    except Exception as e:
        log.exception('webhook: %s', e)
        # Telegram всегда получает 200, иначе ретраит апдейт бесконечно
        return jsonify({'ok': True})


def send_diag(chat_id):
    lines = ['🔧 <b>Самопроверка</b>', '']
    lines.append(f'BOT_TOKEN: {"есть" if BOT_TOKEN else "НЕТ"}')
    lines.append(f'PUBLIC_URL: {PUBLIC_URL or "НЕ ЗАДАН"}')
    lines.append(f'VDS_BASE_URL: {VDS_BASE_URL or "НЕ ЗАДАН"}')
    lines.append(f'VDS_ADMIN_SECRET: {"есть" if VDS_ADMIN_SECRET else "НЕТ"}')
    lines.append(f'RELAY_SECRET: {"есть" if RELAY_SECRET else "НЕТ"}')
    lines.append(f'БД бота: {"подключена" if pool else "не подключена"}')
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
        return 'БД не подключена (DATABASE_URL пуст) — список жалоб недоступен.'
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
    msgs_cb = f'rep:{rid}:msgs:{chat_ref}' if chat_ref else f'rep:{rid}:msgs'
    if not chat_ref:
        log.warning('relay: жалоба #%s пришла без chat_id — кнопка выгрузки '
                    'сможет опереться только на БД', rid)

    keyboard = {
        'inline_keyboard': [[
            {'text': '💬 сообщения чата', 'callback_data': msgs_cb},
            {'text': '🚫 отправить БАН', 'callback_data': f'rep:{rid}:ban'},
        ]]
    }

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
    log.info('relay: жалоба #%s доставлена (message_id=%s, chat_id=%s)',
             rid, message_id, chat_ref or '—')
    return jsonify({'ok': True, 'message_id': message_id})


@app.route('/set_webhook', methods=['GET', 'POST'])
def set_webhook():
    """/set_webhook?secret=WEBHOOK_SECRET"""
    if request.args.get('secret', '') != WEBHOOK_SECRET:
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    base = PUBLIC_URL or request.url_root.rstrip('/').replace('http://', 'https://')
    url = f'{base}/webhook/{WEBHOOK_SECRET}'
    res = tg('setWebhook', url=url, secret_token=WEBHOOK_SECRET,
             allowed_updates='["message","callback_query"]',
             drop_pending_updates='true')
    try:
        info = requests.get(f'{TELEGRAM_API}/getWebhookInfo', timeout=10).json()
    except Exception as e:
        info = {'error': str(e)}
    return jsonify({'requested_url': url, 'telegram': res, 'webhook_info': info})


if __name__ == '__main__':
    init_db()
    ensure_webhook()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5002)), debug=False)
