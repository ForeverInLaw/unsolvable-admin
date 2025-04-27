# -*- coding: utf-8 -*-
# Позволяет использовать русские символы в коде (например, в именах переменных)

import logging
import os
import time
import psycopg2 # Для работы с PostgreSQL
from psycopg2 import pool # Для пула соединений
from datetime import timedelta, datetime # Импортируем datetime
from collections import defaultdict
import asyncio # Важно для асинхронности и run_in_executor

from telegram import Update, ChatPermissions
from telegram.constants import ParseMode, ChatType
from telegram.ext import (
    Application, # Замена Updater
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes, # Замена CallbackContext
    ChatMemberHandler, # Оставляем для возможного использования
    filters # Новое имя для Filters
)
from telegram.helpers import escape_markdown # Для экранирования Markdown V2
from telegram.error import BadRequest, Forbidden # Добавим Forbidden
from dotenv import load_dotenv

# --- Настройки ---
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Проверка обязательных переменных
if not all([BOT_TOKEN, DB_NAME, DB_USER, DB_PASSWORD]):
    print("!!! ОШИБКА: Не установлены все необходимые переменные окружения: "
          "BOT_TOKEN, DB_NAME, DB_USER, DB_PASSWORD !!!")
    exit()

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Настройка пула соединений PostgreSQL ---
db_pool = None
try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        minconn=1, maxconn=10, dbname=DB_NAME, user=DB_USER,
        password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    logger.info("Пул соединений PostgreSQL успешно создан.")
except psycopg2.OperationalError as e:
    logger.error(f"Не удалось подключиться к базе данных PostgreSQL: {e}")
except Exception as e:
    logger.error(f"Неожиданная ошибка при создании пула соединений: {e}")

# --- Функции для работы с БД (Синхронные хелперы) ---

def _sync_init_db():
    """Инициализирует/обновляет структуру БД (таблицы users и user_chat_stats)."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            # 1. Таблица Users
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,          -- Может быть NULL
                    first_name TEXT NOT NULL,
                    last_name TEXT,         -- Может быть NULL
                    last_seen TIMESTAMPTZ NOT NULL
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_username_lower
                ON users (lower(username))
                WHERE username IS NOT NULL;
            """)
            logger.info("Таблица 'users' и индекс проверены/созданы.")
            conn.commit()

            # 2. Таблица user_chat_stats
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_chat_stats (
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    message_count INTEGER DEFAULT 0 NOT NULL,
                    last_message_timestamp TIMESTAMPTZ,
                    PRIMARY KEY (chat_id, user_id)
                );
            """)
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'user_chat_stats'
                  AND column_name = 'last_message_timestamp';
            """)
            if not cur.fetchone():
                logger.info("Добавляем колонку 'last_message_timestamp' в user_chat_stats...")
                cur.execute("ALTER TABLE user_chat_stats ADD COLUMN last_message_timestamp TIMESTAMPTZ;")
                logger.info("Колонка 'last_message_timestamp' добавлена.")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_chat_stats_chat_id_count
                ON user_chat_stats (chat_id, message_count DESC);
            """)
            logger.info("Таблица 'user_chat_stats' и индексы проверены/обновлены.")
            conn.commit()

    except psycopg2.Error as e:
        logger.error(f"Ошибка при инициализации/обновлении структуры БД: {e}")
        if conn: conn.rollback()
        raise
    finally:
        if conn: db_pool.putconn(conn)

def _sync_update_user_info_db(user_id: int, username: str | None, first_name: str, last_name: str | None, current_time: datetime):
    """Синхронно обновляет информацию о пользователе в таблице users."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None
    sql = """
        INSERT INTO users (user_id, username, first_name, last_name, last_seen)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            last_seen = EXCLUDED.last_seen;
    """
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, username, first_name, last_name, current_time))
            conn.commit()
    except psycopg2.Error as e:
        logger.error(f"DB Error updating user info for {user_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn: db_pool.putconn(conn)

def _sync_find_user_by_username_db(username: str) -> int | None:
    """Синхронно ищет user_id по username (регистронезависимо)."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None; user_id = None
    sql = "SELECT user_id FROM users WHERE lower(username) = lower(%s) LIMIT 1;"
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(sql, (username,)); result = cur.fetchone()
            if result: user_id = result[0]; logger.info(f"Found {user_id} for @{username} in DB.")
            else: logger.info(f"@{username} not found in DB.")
    except psycopg2.Error as e: logger.error(f"DB Error finding @{username}: {e}"); conn.rollback(); raise
    finally: db_pool.putconn(conn)
    return user_id

def _sync_track_message_db(chat_id: int, user_id: int, message_time: datetime):
    """Синхронно записывает статистику сообщения в user_chat_stats."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None
    sql = """
        INSERT INTO user_chat_stats (chat_id, user_id, message_count, last_message_timestamp)
        VALUES (%s, %s, 1, %s)
        ON CONFLICT (chat_id, user_id) DO UPDATE
        SET message_count = user_chat_stats.message_count + 1,
            last_message_timestamp = EXCLUDED.last_message_timestamp;
    """
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur: cur.execute(sql, (chat_id, user_id, message_time)); conn.commit()
    except psycopg2.Error as e:
        if e.pgcode == '23503': logger.warning(f"DB FK violation tracking msg {user_id}/{chat_id}.")
        else: logger.error(f"DB Error tracking msg stats {user_id}/{chat_id}: {e}")
        if conn: conn.rollback()
    finally: db_pool.putconn(conn)

def _sync_get_user_stats_db(chat_id: int, user_id: int) -> int:
    """Синхронно получает статистику пользователя."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None; count = 0
    sql = "SELECT message_count FROM user_chat_stats WHERE chat_id = %s AND user_id = %s;"
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur: cur.execute(sql, (chat_id, user_id)); result = cur.fetchone(); count = result[0] if result else 0
    except psycopg2.Error as e: logger.error(f"DB Error getting user stats {user_id}/{chat_id}: {e}"); conn.rollback(); raise
    finally: db_pool.putconn(conn)
    return count

def _sync_get_chat_stats_db(chat_id: int) -> tuple[int, int, list[tuple[int, int]]]:
    """Синхронно получает общую статистику чата и топа."""
    if not db_pool: raise ConnectionError("Пул соединений БД не инициализирован.")
    conn = None; total_messages, active_users_count, top_users = 0, 0, []
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            sql_total = "SELECT SUM(message_count), COUNT(user_id) FROM user_chat_stats WHERE chat_id = %s;"
            cur.execute(sql_total, (chat_id,)); total_result = cur.fetchone()
            total_messages = int(total_result[0] or 0); active_users_count = int(total_result[1] or 0)
            sql_top = "SELECT user_id, message_count FROM user_chat_stats WHERE chat_id = %s ORDER BY message_count DESC LIMIT 5;"
            cur.execute(sql_top, (chat_id,)); top_users = cur.fetchall()
    except psycopg2.Error as e: logger.error(f"DB Error getting chat stats {chat_id}: {e}"); conn.rollback(); raise
    finally: db_pool.putconn(conn)
    return total_messages, active_users_count, top_users

# --- Вспомогательные асинхронные функции ---

async def is_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Асинхронно проверяет, является ли пользователь администратором чата."""
    if not update.effective_chat or not update.effective_user or update.effective_chat.type == ChatType.PRIVATE: return False
    chat_id, user_id = update.effective_chat.id, update.effective_user.id
    cache_key, cache_time_key = f"admin_list_{chat_id}", f"admin_list_{chat_id}_time"
    admin_ids = context.chat_data.get(cache_key)
    last_update_time = context.chat_data.get(cache_time_key, 0)

    if not admin_ids or time.time() - last_update_time > 300:
        try:
            logger.debug(f"Updating admin list for chat {chat_id}")
            chat_admins = await context.bot.get_chat_administrators(chat_id)
            admin_ids = {admin.user.id for admin in chat_admins}
            context.chat_data[cache_key], context.chat_data[cache_time_key] = admin_ids, time.time()
        except Forbidden: logger.warning(f"No rights to get admins in {chat_id}"); context.chat_data[cache_key], context.chat_data[cache_time_key] = set(), time.time(); return False
        except BadRequest as e: logger.error(f"BadRequest getting admins {chat_id}: {e}"); return False
        except Exception as e: logger.error(f"Error getting admins {chat_id}: {e}"); return False
    else: logger.debug(f"Using cached admin list for {chat_id}")
    return user_id in admin_ids

async def extract_target_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> tuple[int | None, list[str]]:
    """Извлекает ID целевого пользователя (Reply, ID, @username из DB, text_mention)."""
    target_user_id = None
    args = context.args or []
    loop = asyncio.get_running_loop()

    # 1. Ответ на сообщение
    if update.message and update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
        logger.info(f"Target found via reply: {target_user_id}")
        return target_user_id, args # Возвращаем ВСЕ аргументы команды, если они были

    # 2. Аргументы
    if args:
        first_arg = args[0]
        remaining_args = args[1:] # Аргументы ПОСЛЕ первого

        # 2a. Числовой ID
        if first_arg.isdigit():
            try: target_user_id = int(first_arg); logger.info(f"Target found via ID: {target_user_id}"); return target_user_id, remaining_args
            except ValueError: pass

        # 2b. @username (поиск в БД)
        if first_arg.startswith('@'):
            username_to_find = first_arg.lstrip('@')
            if username_to_find:
                try:
                    found_id = await loop.run_in_executor(None, _sync_find_user_by_username_db, username_to_find)
                    if found_id:
                        return found_id, remaining_args # Нашли! Возвращаем ID и остальные аргс
                    else:
                        logger.info(f"Username '{username_to_find}' not found in DB, checking text_mention.")
                        # Если не нашли в БД, продолжаем проверку на text_mention, так как
                        # первый аргумент все еще может им быть.
                except ConnectionError: logger.error("DB Connection error during username lookup.")
                except Exception as e: logger.error(f"Error during username DB lookup for '{username_to_find}': {e}")
            else: logger.info("Empty username after @.")

        # *** ИСПРАВЛЕНИЕ ЗДЕСЬ ***
        # 2c. text_mention (проверяем, если не нашли по ID или @username в БД)
        # Важно: Проверяем, что text_mention соответствует ПЕРВОМУ аргументу
        if update.message and update.message.entities:
            for entity in update.message.entities:
                 # Ищем только text_mention, так как @mention уже проверили через БД
                 if entity.type == "text_mention" and entity.user:
                     # Извлекаем текст упоминания из сообщения
                     mention_text = update.message.text[entity.offset : entity.offset + entity.length]
                     # Сравниваем извлеченный текст с ПЕРВЫМ аргументом команды
                     if mention_text == first_arg:
                         target_user_id = entity.user.id
                         logger.info(f"Target found via text_mention matching first argument: {target_user_id}")
                         # Нашли пользователя по text_mention, который был первым аргументом.
                         # Возвращаем ID и ОСТАЛЬНЫЕ аргументы (args[1:])
                         return target_user_id, remaining_args
                     else:
                         # Нашли text_mention, но он не совпадает с первым аргументом.
                         # Это может быть упоминание в середине команды, игнорируем его для поиска цели.
                         logger.debug(f"Found text_mention '{mention_text}' but it doesn't match first arg '{first_arg}'.")
                         # Не прерываем цикл, вдруг дальше будет нужный text_mention (маловероятно для команд)

        # Если дошли сюда и target_user_id все еще None, значит не нашли
        if not target_user_id:
            logger.info(f"First argument '{first_arg}' is not ID and not recognized as known @username or matching text_mention.")
        # Если цель не найдена через ID/@username/text_mention, возвращаем None и все аргументы команды
        return None, args # Означает "цель не найдена", mute/kick/etc. должны обработать это

    logger.info("Target not found via reply or arguments.")
    return None, []

def parse_duration(duration_str: str) -> int | None:
    """Парсит строку времени (5m, 1h, 2d) и возвращает время в секундах."""
    if not duration_str: return None
    s = duration_str.lower(); v = s[:-1]; u = s[-1]
    if len(s) < 2 or not v.isdigit(): return None
    try: val = int(v)
    except ValueError: return None
    if u == 'm': d = timedelta(minutes=val)
    elif u == 'h': d = timedelta(hours=val)
    elif u == 'd': d = timedelta(days=val)
    else: return None
    return int(d.total_seconds())

# --- Обработчики команд (асинхронные) ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text('Привет\! Я бот для администрирования чата\. '
                                    'Используйте /help, чтобы увидеть доступные команды\.',
                                    parse_mode=ParseMode.MARKDOWN_V2)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показывает список команд (только для админов)."""
    if not await is_user_admin(update, context): return
    help_text = """
*Доступные команды администратора:*

`/kick` \[в ответ / user\_id / @username\] \- Кикнуть пользователя\.
`/mute` \[в ответ / user\_id / @username\] \[время \(5m, 1h, 2d\)\] \- Замутить пользователя\. Без времени \- навсегда\.
`/unmute` \[в ответ / user\_id / @username\] \- Размутить пользователя\.
`/stat` \[опционально: user\_id / @username\] \- Показать статистику сообщений\.

*Как использовать:*
\- Ответьте на сообщение пользователя нужной командой\.
\- Напишите команду с ID пользователя \(например, `/mute 123456789 2d`\)\.
\- Используйте @username \(бот должен был видеть сообщения от этого пользователя ранее\)\.

_Примечание: Команды не работают против других администраторов\._
    """
    await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN_V2)

async def kick_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Асинхронно кикает пользователя."""
    if not await is_user_admin(update, context): return
    target_user_id, _ = await extract_target_user_id(update, context) # _ содержит оставшиеся аргументы, не используем их здесь
    chat_id = update.effective_chat.id

    if not target_user_id:
        await update.message.reply_text("Не могу определить пользователя\. "
                                        "Используйте команду в ответ, укажите ID или известный боту @username\.",
                                        parse_mode=ParseMode.MARKDOWN_V2); return
    if target_user_id == context.bot.id: await update.message.reply_text("Я не могу кикнуть сам себя\."); return

    try: # Check admin
        chat_admins = await context.bot.get_chat_administrators(chat_id)
        if target_user_id in {admin.user.id for admin in chat_admins}: await update.message.reply_text("Я не могу кикнуть другого администратора\."); return
    except Forbidden: logger.warning(f"No rights check admin {target_user_id} in {chat_id} before kick.")
    except Exception as e: logger.warning(f"Failed check admin {target_user_id} before kick: {e}")

    try: # Kick
        await context.bot.ban_chat_member(chat_id, target_user_id)
        await context.bot.unban_chat_member(chat_id, target_user_id, only_if_banned=True)
        await update.message.reply_text(f"Пользователь `{target_user_id}` был кикнут\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {update.effective_user.id} kicked {target_user_id} in {chat_id}")
    except Forbidden: await update.message.reply_text("Нет прав на блокировку участников\.", parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        error_message = str(e).lower(); reply = f"Ошибка кика: {escape_markdown(str(e), version=2)}"
        if "user not found" in error_message: reply = f"Пользователь `{target_user_id}` не найден в чате\."
        elif "rights" in error_message or "owner" in error_message or "administrator" in error_message: reply = f"Не могу кикнуть этого пользователя \(админ/владелец\?\)\."
        logger.error(f"Kick BadRequest {target_user_id}/{chat_id}: {e}"); await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e: logger.error(f"Kick Error {target_user_id}/{chat_id}: {e}"); await update.message.reply_text("Непредвиденная ошибка кика\.")

async def mute_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Асинхронно мутит (ограничивает) пользователя."""
    if not await is_user_admin(update, context): return
    # args здесь - это аргументы ПОСЛЕ идентификатора пользователя (если он был найден)
    target_user_id, args = await extract_target_user_id(update, context)
    chat_id = update.effective_chat.id

    if not target_user_id:
        await update.message.reply_text("Не могу определить пользователя\. "
                                        "Используйте команду в ответ, укажите ID или известный боту @username\.",
                                        parse_mode=ParseMode.MARKDOWN_V2); return
    if target_user_id == context.bot.id: await update.message.reply_text("Я не могу замутить сам себя\."); return

    try: # Check admin
        chat_admins = await context.bot.get_chat_administrators(chat_id)
        if target_user_id in {admin.user.id for admin in chat_admins}: await update.message.reply_text("Я не могу замутить другого администратора\."); return
    except Forbidden: logger.warning(f"No rights check admin {target_user_id} in {chat_id} before mute.")
    except Exception as e: logger.warning(f"Failed check admin {target_user_id} before mute: {e}")

    duration_text, until_date_ts = "навсегда", 0
    # Парсим ПЕРВЫЙ из ОСТАВШИХСЯ аргументов как время
    if args:
        duration_seconds = parse_duration(args[0])
        if duration_seconds: duration_text, until_date_ts = f"на `{args[0]}`", int(time.time() + duration_seconds)
        else: logger.info(f"Arg '{args[0]}' after target user not parsed as duration for mute.")

    permissions = ChatPermissions(can_send_messages=False)
    try: # Mute
        await context.bot.restrict_chat_member(chat_id, target_user_id, permissions, until_date=until_date_ts)
        await update.message.reply_text(f"Пользователь `{target_user_id}` замучен {duration_text}\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {update.effective_user.id} muted {target_user_id} in {chat_id} {duration_text}")
    except Forbidden: await update.message.reply_text("Нет прав на ограничение участников\.", parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        error_message = str(e).lower(); reply = f"Ошибка мута: {escape_markdown(str(e), version=2)}"
        if "user not found" in error_message: reply = f"Пользователь `{target_user_id}` не найден в чате\."
        elif "rights" in error_message or "restrict self" in error_message: reply = "Не могу ограничить этого пользователя \(админ/я\?\)\."
        elif "administrator" in error_message or "owner" in error_message: reply = "Нельзя ограничить администратора/владельца\."
        logger.error(f"Mute BadRequest {target_user_id}/{chat_id}: {e}"); await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e: logger.error(f"Mute Error {target_user_id}/{chat_id}: {e}"); await update.message.reply_text("Непредвиденная ошибка мута\.")

async def unmute_user(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Асинхронно размучивает (снимает ограничения) пользователя."""
    if not await is_user_admin(update, context): return
    target_user_id, _ = await extract_target_user_id(update, context) # Аргументы после цели не нужны
    chat_id = update.effective_chat.id

    if not target_user_id:
        await update.message.reply_text("Не могу определить пользователя\. "
                                        "Используйте команду в ответ, укажите ID или известный боту @username\.",
                                        parse_mode=ParseMode.MARKDOWN_V2); return

    permissions = ChatPermissions(can_send_messages=True)
    try: # Unmute
        await context.bot.restrict_chat_member(chat_id, target_user_id, permissions)
        await update.message.reply_text(f"Пользователь `{target_user_id}` размучен\.", parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {update.effective_user.id} unmuted {target_user_id} in {chat_id}")
    except Forbidden: await update.message.reply_text("Нет прав на изменение ограничений\.", parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e:
        error_message = str(e).lower(); reply = f"Ошибка размута: {escape_markdown(str(e), version=2)}"
        if "user not found" in error_message: reply = f"Пользователь `{target_user_id}` не найден в чате\."
        elif "rights" in error_message: reply = "Недостаточно прав\."
        elif "creator" in error_message: reply = "Нельзя изменить права создателя\."
        elif "user_not_participant" in error_message: reply = f"Пользователь `{target_user_id}` не участник чата\."
        elif "administrator" in error_message: reply = f"Пользователь `{target_user_id}` — администратор\."
        logger.error(f"Unmute BadRequest {target_user_id}/{chat_id}: {e}"); await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e: logger.error(f"Unmute Error {target_user_id}/{chat_id}: {e}"); await update.message.reply_text("Непредвиденная ошибка размута\.")

# --- Статистика ---

async def track_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Асинхронно отслеживает сообщения и обновляет инфо пользователя в БД."""
    if not db_pool or not update.message or not update.effective_chat or not update.effective_user or update.effective_user.is_bot: return
    user, chat_id, msg_time = update.effective_user, update.effective_chat.id, update.message.date
    loop = asyncio.get_running_loop()
    try: await loop.run_in_executor(None, _sync_update_user_info_db, user.id, user.username, user.first_name, user.last_name, msg_time)
    except Exception as e: logger.error(f"Error updating user info {user.id}: {e}")
    try: await loop.run_in_executor(None, _sync_track_message_db, chat_id, user.id, msg_time)
    except Exception as e: logger.error(f"Error tracking msg stats {user.id}/{chat_id}: {e}")

async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Асинхронно показывает статистику чата или пользователя из БД."""
    if not await is_user_admin(update, context): return
    if not db_pool: await update.message.reply_text("Ошибка БД статистики\.", parse_mode=ParseMode.MARKDOWN_V2); return

    chat_id = update.effective_chat.id
    args_provided = bool(context.args)
    # args содержит аргументы ПОСЛЕ найденного пользователя (если он найден)
    target_user_id, args_after_target = await extract_target_user_id(update, context)
    loop = asyncio.get_running_loop()

    try:
        if target_user_id:
            # --- Статистика по КОНКРЕТНОМУ пользователю ---
            count = await loop.run_in_executor(None, _sync_get_user_stats_db, chat_id, target_user_id)
            mention = f"<code>{target_user_id}</code>"
            try: member = await context.bot.get_chat_member(chat_id, target_user_id); mention = member.user.mention_html()
            except Exception as e: logger.warning(f"Error getting member info {target_user_id}/{chat_id} for stats: {e}")
            reply = f"📊 Статистика для {mention}:\n— Сообщений: <b>{count}</b>"
            if count == 0: reply += "\n\n<i>(Нет сообщений с начала учета.)</i>"
            await update.message.reply_text(reply, parse_mode=ParseMode.HTML)

        elif not target_user_id and args_provided:
             # --- Пользователь что-то ввел, но цель не распознана ---
             failed_input = escape_markdown(" ".join(context.args), version=2) # context.args содержит ВСЕ аргументы
             logger.info(f"Stat request unrecognized arg: '{failed_input}' from {update.effective_user.id}")
             reply = (
                 f"Не удалось определить пользователя по '{failed_input}'\.\n\n"
                 "Нужно:\n— Ответить на сообщение `/stat`\.\n"
                 "— Указать ID: `/stat 123…`\.\n"
                 "— Указать `@username` \(если бот его видел\)\.\n\n"
                 "Для общей статистики чата: `/stat`\."
             )
             await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN_V2)

        else:
            # --- Общая статистика по чату ---
            total, active, top = await loop.run_in_executor(None, _sync_get_chat_stats_db, chat_id)
            if total == 0: await update.message.reply_text("📊 В чате нет сообщений\.", parse_mode=ParseMode.MARKDOWN_V2); return
            reply = f"📊 *Общая статистика чата:*\n— Всего сообщений: *{total}*\n— Активных пользователей: *{active}*\n\n"
            if top:
                 reply += "*Топ пользователей:*\n"
                 tasks = [get_user_info_for_stats(context, chat_id, uid, c) for uid, c in top]
                 reply += "".join(await asyncio.gather(*tasks))
                 reply += "\n_\(Показаны пользователи, написавшие хотя бы одно сообщение\.\)_"
            else: reply += "_Нет данных для топа\._"
            await update.message.reply_text(reply, parse_mode=ParseMode.MARKDOWN_V2)

    except ConnectionError: logger.error("DB Connection Error showing stats."); await update.message.reply_text("Ошибка БД статистики\.", parse_mode=ParseMode.MARKDOWN_V2)
    except BadRequest as e: logger.error(f"BadRequest sending stats: {e}", exc_info=True); await update.message.reply_text(f"Ошибка форматирования: {escape_markdown(str(e), version=2)}", parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e: logger.error(f"Unexpected error showing stats: {e}", exc_info=True); await update.message.reply_text("Непредвиденная ошибка статистики\.", parse_mode=ParseMode.MARKDOWN_V2)

async def get_user_info_for_stats(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int, msg_count: int) -> str:
    """Асинхронно получает имя пользователя и форматирует строку для топа статистики (Markdown V2)."""
    user_name_md = f"`{user_id}`"
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        try: name = member.user.full_name or str(user_id); link_text = escape_markdown(name, 2); user_name_md = member.user.mention_markdown_v2() if member.user.first_name else f"[{link_text}](tg://user?id={user_id})"
        except ValueError: name = member.user.full_name or str(user_id); link_text = escape_markdown(name, 2); user_name_md = f"[{link_text}](tg://user?id={user_id})"
    except Forbidden: logger.warning(f"No rights get user info {user_id}/{chat_id}"); user_name_md = f"ID `{user_id}`"
    except BadRequest: logger.warning(f"User {user_id} not found in {chat_id}"); user_name_md = f"Покинул чат \\(`{user_id}`\\)"
    except Exception as e: logger.warning(f"Error get user info {user_id}: {e}"); user_name_md = f"ID `{user_id}`"
    return f"— {user_name_md}: *{msg_count}*\n"

# --- Обработчик ошибок ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логирует ошибки."""
    if isinstance(update, Update): logger.error(f"Update {update.update_id} caused error:", exc_info=context.error)
    else: logger.error("Error outside Update context:", exc_info=context.error)
    if isinstance(context.error, Forbidden): logger.warning(f"Forbidden: {context.error}")
    elif isinstance(context.error, BadRequest) and "parse entities" not in str(context.error): logger.warning(f"BadRequest: {context.error}")

# --- Основная функция ---
async def post_init(application: Application) -> None:
    """Инициализация БД и получение информации о боте после старта."""
    logger.info("Запуск post_init...")
    if db_pool:
        loop = asyncio.get_running_loop()
        try: await loop.run_in_executor(None, _sync_init_db)
        except Exception as e: logger.error(f"DB init error in post_init: {e}")
    else: logger.warning("DB pool not created, skipping DB init.")
    try: bot_user = await application.bot.get_me(); logger.info(f"Bot @{bot_user.username} ready!")
    except Exception as e: logger.error(f"Failed get bot info: {e}")

def main() -> None:
    """Запускает бота."""
    if not db_pool: logger.warning("!!! DB pool not created. Stats & username lookup WILL NOT WORK !!!")
    application = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()
    handlers = [
        CommandHandler("start", start), CommandHandler("help", help_command),
        CommandHandler("kick", kick_user), CommandHandler("mute", mute_user),
        CommandHandler("unmute", unmute_user), CommandHandler("stat", show_stats),
        MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.GROUPS, track_message)
    ]
    application.add_handlers(handlers); application.add_error_handler(error_handler)
    logger.info("Starting bot..."); application.run_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Stopping bot...")
    if db_pool: db_pool.closeall(); logger.info("PostgreSQL connection pool closed.")
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()