# -*- coding: utf-8 -*-
"""
new_bot.py — НОВЫЙ Telegram-бот жалоб (Render Web Service, gunicorn, webhook).

Почему webhook, а не polling: на Render Web Service должен слушать порт,
long-polling там засыпает/дублируется. Webhook + gunicorn = стабильно.

Что делает:
  * POST /webhook/<WEBHOOK_SECRET> — принимает апдейты Telegram
  * /start, /id — служебные ответы админу
  * Обрабатывает нажатия inline-кнопок «сообщения чата» и «отправить БАН».
    ПОКА эти функции не работают: бот просто отвечает всплывашкой
    «Функция пока не подключена» и логирует нажатие в report_actions.
    Логика на будущее уже размечена — см. handle_messages_button / handle_ban_button.

Запуск на Render:
  Build Command: pip install -r requirements.txt
  Start Command: gunicorn new_bot:app -c gunicorn_bot.py

Переменные окружения:
  BOT_TOKEN         — тот же токен, что у new_server.py
  ADMIN_TELEGRAM_ID — 5574610358
  WEBHOOK_SECRET    — любая длинная случайная строка (часть URL вебхука)
  DATABASE_URL      — та же БД, что у new_server.py (необязательно, но желательно)
  PUBLIC_URL        — https://new-bot-xxxx.onrender.com (для авто-установки вебхука)
"""

import os
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
    action        TEXT        NOT NULL,   -- 'view_messages' | 'ban'
    result        TEXT,                   -- 'pending' | 'done' | 'error: ...'
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_report_actions_report ON report_actions (report_id);
"""

_init_lock = threading.Lock()
_initialized = False


def init_db():
    if not pool:
        return
    try:
        with pool.connection() as conn:
            conn.execute(SCHEMA_SQL)
        log.info('Схема report_actions проверена/создана')
    except Exception as e:
        log.error('init_db: %s', e)


@app.before_request
def _ensure_init():
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            init_db()
            _initialized = True


# ------------------------------------------------------------ Telegram API

def tg(method: str, **payload):
    if not BOT_TOKEN:
        log.error('BOT_TOKEN не задан')
        return None
    try:
        r = requests.post(f'{TELEGRAM_API}/{method}', data=payload, timeout=12)
        body = r.json() if r.content else {}
        if not body.get('ok'):
            log.error('%s -> %s', method, body)
        return body
    except Exception as e:
        log.error('%s исключение: %s', method, e)
        return None


def answer_callback(callback_id: str, text: str, alert: bool = True):
    tg('answerCallbackQuery', callback_query_id=callback_id, text=text, show_alert=alert)


def send(chat_id, text: str):
    tg('sendMessage', chat_id=chat_id, text=text, disable_web_page_preview=True)


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
    """Пригодится, когда кнопки заработают."""
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


# ------------------------------------------------------- обработчики кнопок

def handle_messages_button(report_id, admin_id, callback_id):
    """
    TODO (когда решишь включить): вытащить сообщения чата report['chat_id']
    из основной БД / через служебный эндпоинт server-for-vvv и прислать сюда.
    Сейчас — заглушка.
    """
    log_action(report_id, admin_id, 'view_messages', 'pending')
    answer_callback(callback_id, 'Сообщения чата: функция пока не подключена')


def handle_ban_button(report_id, admin_id, callback_id):
    """
    TODO: дернуть служебный эндпоинт основного сервера (например
    POST /admin/ban с админ-секретом) и забанить report['reported_id'].
    Сейчас — заглушка.
    """
    log_action(report_id, admin_id, 'ban', 'pending')
    answer_callback(callback_id, 'Бан: функция пока не подключена')


# ------------------------------------------------------------------ роуты

@app.route('/health', methods=['GET'])
@app.route('/', methods=['GET'])
def health():
    return jsonify({
        'status': 'ok',
        'service': 'new_bot',
        'bot_token': bool(BOT_TOKEN),
        'time': datetime.now(timezone.utc).isoformat(),
    })


@app.route('/webhook/<secret>', methods=['POST'])
def webhook(secret):
    if secret != WEBHOOK_SECRET:
        return jsonify({'ok': False}), 403

    # доп. проверка от Telegram (устанавливается вместе с вебхуком)
    header_token = request.headers.get('X-Telegram-Bot-Api-Secret-Token')
    if header_token and header_token != WEBHOOK_SECRET:
        return jsonify({'ok': False}), 403

    update = request.get_json(silent=True) or {}

    try:
        # --- нажатие inline-кнопки под жалобой ---
        cq = update.get('callback_query')
        if cq:
            callback_id = cq.get('id')
            from_id = str((cq.get('from') or {}).get('id', ''))
            data = cq.get('data') or ''

            if from_id != ADMIN_TELEGRAM_ID:
                answer_callback(callback_id, 'Нет доступа')
                return jsonify({'ok': True})

            parts = data.split(':')
            if len(parts) == 3 and parts[0] == 'rep':
                report_id = int(parts[1]) if parts[1].isdigit() else None
                action = parts[2]
                if action == 'msgs':
                    handle_messages_button(report_id, from_id, callback_id)
                elif action == 'ban':
                    handle_ban_button(report_id, from_id, callback_id)
                else:
                    answer_callback(callback_id, 'Неизвестное действие')
            else:
                answer_callback(callback_id, 'Неизвестная кнопка')
            return jsonify({'ok': True})

        # --- обычные сообщения ---
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
                send(chat_id, 'Бот жалоб на связи. Жалобы приходят сюда автоматически.')
            elif text.startswith('/last'):
                send(chat_id, format_last_reports())
        return jsonify({'ok': True})
    except Exception as e:
        log.error('webhook: %s', e)
        # Telegram всегда получает 200, иначе будет ретраить апдейт бесконечно
        return jsonify({'ok': True})


def format_last_reports(limit: int = 5) -> str:
    if not pool:
        return 'БД не подключена.'
    try:
        with pool.connection() as conn:
            rows = conn.execute(
                """SELECT id, reporter_id, reason, created_at
                     FROM user_reports ORDER BY id DESC LIMIT %s""",
                (limit,)
            ).fetchall()
        if not rows:
            return 'Жалоб пока нет.'
        return '\n'.join(
            f'#{r[0]} от {r[1]} — {r[2]} ({r[3]:%d.%m %H:%M})' for r in rows
        )
    except Exception as e:
        return f'Ошибка БД: {e}'


@app.route('/set_webhook', methods=['GET', 'POST'])
def set_webhook():
    """Открой один раз в браузере: /set_webhook?secret=WEBHOOK_SECRET"""
    if request.args.get('secret', '') != WEBHOOK_SECRET:
        return jsonify({'ok': False, 'error': 'forbidden'}), 403
    base = PUBLIC_URL or request.url_root.rstrip('/').replace('http://', 'https://')
    url = f'{base}/webhook/{WEBHOOK_SECRET}'
    res = tg(
        'setWebhook',
        url=url,
        secret_token=WEBHOOK_SECRET,
        allowed_updates='["message","callback_query"]',
        drop_pending_updates='true',
    )
    return jsonify({'requested_url': url, 'telegram': res})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5002)), debug=False)
