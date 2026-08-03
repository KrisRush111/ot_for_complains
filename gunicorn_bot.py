# gunicorn-конфиг для new_bot.py (Render)
# Start Command: gunicorn new_bot:app -c gunicorn_bot.py
#
# Боту тем более нужен ровно ОДИН воркер: он обрабатывает вебхук Telegram,
# нагрузка — единицы запросов в минуту. Больше процессов = только память
# и лишние соединения к БД.

import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

workers = 1
worker_class = 'gthread'
# Потоков минимум 4: вебхук должен отвечать 200 сразу, пока фоновый поток
# тянет выгрузку чата с ВДС. На одном потоке кнопка «сообщения чата»
# блокировала бы приём следующих апдейтов.
threads = int(os.environ.get('GUNICORN_THREADS', '4'))

# Telegram ждёт ответ на вебхук недолго и ретраит апдейт при таймауте,
# поэтому сами обработчики отвечают быстро — тяжёлую работу (запрос к
# /admin/chat_dump + sendDocument) new_bot.py уносит в фоновый поток.
#
# Но 30 сек здесь всё равно мало: под этот же лимит попадают ручные
# /dump и /diag, а также запуск воркера с открытием пула к БД. Держим 120 —
# иначе gunicorn убивает воркер по SIGKILL посреди выгрузки, и админ не
# получает ни файла, ни ошибки (это и была одна из причин «мёртвой» кнопки).
timeout = 120
graceful_timeout = 30

keepalive = 5

max_requests = 2000
max_requests_jitter = 200

accesslog = '-'
errorlog = '-'
loglevel = os.environ.get('LOG_LEVEL', 'info')

# preload_app=False обязателен: при preload воркер наследует уже открытый
# пул psycopg и вебхук ставится в мастер-процессе — соединения ломаются.
preload_app = False
