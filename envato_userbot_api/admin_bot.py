import logging
import os
import json
import aiofiles
import datetime
import pytz 
from typing import Dict, Optional
import asyncio # Добавлено для subprocess

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    filters,
)
from telegram.constants import ParseMode
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    ApiIdInvalidError, 
    AuthKeyError,      
    UserDeactivatedError, 
    SessionExpiredError,   
    FloodWaitError        
)
from telethon.sessions import StringSession
import re

# --- Конфигурация ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN")
API_ID_STR = os.getenv("API_ID") 
API_HASH = os.getenv("API_HASH")
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "") 
LOG_FILE_FROM_MAIN_APP = os.getenv("LOG_FILE_FOR_DOWNLOAD", "main_fastapi_app.log") 

SESSIONS_FILE = os.getenv("SESSIONS_FILE_PATH", "sessions.json") 
STATS_FILE = os.getenv("STATS_FILE_PATH", "usage_stats.json")     
MOSCOW_TZ_STR = os.getenv("MOSCOW_TZ", "Europe/Moscow")
WEBHOOK_TASKS_FILE = os.getenv("WEBHOOK_DB_FILE", "webhook_tasks.json")
FASTAPI_SERVICE_NAME = os.getenv("FASTAPI_SERVICE_NAME", "envato-bot") # Имя systemd сервиса


# --- Валидация конфигурации ---
if not ADMIN_BOT_TOKEN:
    logger.critical("ADMIN_BOT_TOKEN не найден в .env! Выход.")
    exit(1)
if not API_ID_STR or not API_HASH:
    logger.critical("API_ID или API_HASH не найдены в .env для admin_bot! Выход.")
    exit(1)

try:
    API_ID = int(API_ID_STR)
except ValueError:
    logger.critical(f"Некорректное значение API_ID в .env: '{API_ID_STR}'. Должно быть числом. Выход.")
    exit(1)

ADMIN_IDS = []
if ADMIN_IDS_STR:
    try:
        ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip()]
    except ValueError:
        logger.error("Ошибка в ADMIN_IDS в .env. Убедитесь, что это список чисел, разделенных запятыми.")
if not ADMIN_IDS:
    logger.warning("ADMIN_IDS не сконфигурированы или указаны некорректно.")

try:
    MOSCOW_TZ = pytz.timezone(MOSCOW_TZ_STR)
except pytz.exceptions.UnknownTimeZoneError:
    logger.error(f"Неизвестная временная зона в .env: {MOSCOW_TZ_STR}. Используется Europe/Moscow.")
    MOSCOW_TZ = pytz.timezone("Europe/Moscow")


# --- Состояния ---
ASK_PHONE, ASK_CODE, ASK_PASSWORD = range(3)

# --- Вспомогательные функции ---
async def load_json_from_file(filepath: str) -> Dict:
    if not os.path.exists(filepath):
        return {}
    try:
        async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content) if content else {}
    except json.JSONDecodeError:
        logger.error(f"Ошибка декодирования JSON из файла: {filepath}")
        return {}
    except Exception as e:
        logger.error(f"Ошибка чтения файла {filepath}: {e}")
        return {}

async def save_json_to_file(filepath: str, data: Dict):
    try:
        async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False)) 
    except Exception as e:
        logger.error(f"Ошибка сохранения файла {filepath}: {e}")


async def load_sessions() -> Dict:
    return await load_json_from_file(SESSIONS_FILE)

async def save_sessions(sessions: Dict):
    await save_json_to_file(SESSIONS_FILE, sessions)

async def load_stats() -> Dict: 
    return await load_json_from_file(STATS_FILE)

async def save_stats(stats: Dict):
    await save_json_to_file(STATS_FILE, stats)

async def load_webhook_tasks() -> Dict:
    return await load_json_from_file(WEBHOOK_TASKS_FILE)

async def save_webhook_tasks(tasks: Dict):
    await save_json_to_file(WEBHOOK_TASKS_FILE, tasks)


# --- Декоратор и команды ---
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not ADMIN_IDS:
            if update.message:
                await update.message.reply_text("⛔️ Список администраторов не настроен.")
            elif update.callback_query and update.callback_query.message:
                 await update.callback_query.message.reply_text("⛔️ Список администраторов не настроен.")
            return
        if update.effective_user.id not in ADMIN_IDS:
            if update.message:
                await update.message.reply_text("⛔️ Доступ запрещен.")
            elif update.callback_query and update.callback_query.message:
                await update.callback_query.message.reply_text("⛔️ Доступ запрещен.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

@admin_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    keyboard = [
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")],
        [InlineKeyboardButton("❌ Удалить сессию", callback_data="delete_session")],
        [InlineKeyboardButton("📁 Скачать логи FastAPI", callback_data="download_main_logs")],
        [InlineKeyboardButton("🗑️ Очистить задачи вебхуков", callback_data="clear_webhook_tasks_confirm")],
        [InlineKeyboardButton(f"🚀 Перезагрузить FastAPI ({FASTAPI_SERVICE_NAME})", callback_data="restart_fastapi_confirm")] # Новая кнопка
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🤖 *Управление FastAPI воркером и сессиями Telegram*\n\nВыберите действие:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "add_account":
        await query.message.reply_text("Введите номер телефона в международном формате (например, +1234567890):")
        return ASK_PHONE
    elif data == "show_stats":
        await show_statistics(update, context)
    elif data == "delete_session":
        await show_delete_options(update, context)
    elif data.startswith("delete_phone_"): 
        phone_to_delete = data.replace("delete_phone_", "")
        await delete_session_by_phone(update, context, phone_to_delete)
    elif data == "download_main_logs":
        await download_main_app_logs(update, context)
    elif data == "clear_webhook_tasks_confirm": 
        await confirm_clear_webhook_tasks(update, context)
    elif data == "clear_webhook_tasks_execute": 
        await execute_clear_webhook_tasks(update, context)
    elif data == "cancel_clear_webhook_tasks": 
        await query.edit_message_text("Очистка списка задач вебхуков отменена.")
    elif data == "restart_fastapi_confirm": # Кнопка подтверждения перезагрузки
        await confirm_restart_fastapi(update, context)
    elif data == "restart_fastapi_execute": # Кнопка выполнения перезагрузки
        await execute_restart_fastapi(update, context)
    elif data == "cancel_restart_fastapi": # Кнопка отмены перезагрузки
        await query.edit_message_text(f"Перезагрузка сервиса {FASTAPI_SERVICE_NAME} отменена.")
    elif data == "cancel_add_account":
        await query.message.reply_text("Добавление аккаунта отменено.")
        if "client" in context.user_data:
            client_to_disconnect: TelegramClient = context.user_data["client"]
            if client_to_disconnect.is_connected():
                await client_to_disconnect.disconnect()
            del context.user_data["client"]
        return ConversationHandler.END # Только для диалога добавления аккаунта
    
    # Если это не команда для ConversationHandler, завершаем его, если он был активен
    # Это предотвращает зависание в состояниях, если пользователь нажимает другие кнопки
    if context.user_data.get("_current_state") is not None and data != "add_account":
         return ConversationHandler.END
    return None 

# --- Функции для очистки задач вебхуков ---
@admin_only
async def confirm_clear_webhook_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton("🔴 Да, очистить ВСЕ задачи", callback_data="clear_webhook_tasks_execute")],
        [InlineKeyboardButton("🟢 Нет, отмена", callback_data="cancel_clear_webhook_tasks")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        "⚠️ *Вы уверены, что хотите очистить ВЕСЬ список задач вебхуков?*\n"
        "Это действие необратимо и удалит все ожидающие и завершенные задачи из файла "
        f"`{os.path.basename(WEBHOOK_TASKS_FILE)}`.\n"
        "Задачи, которые уже выполняются в FastAPI, могут завершиться, но новые не будут возобновлены после перезапуска.",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def execute_clear_webhook_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await save_webhook_tasks({}) 
        logger.info(f"Администратор {update.effective_user.id} очистил файл задач вебхуков: {WEBHOOK_TASKS_FILE}")
        await query.edit_message_text(f"✅ Файл задач вебхуков (`{os.path.basename(WEBHOOK_TASKS_FILE)}`) успешно очищен.")
    except Exception as e:
        logger.error(f"Ошибка при очистке файла задач вебхуков ({WEBHOOK_TASKS_FILE}): {e}", exc_info=True)
        await query.edit_message_text(f"❌ Не удалось очистить файл задач вебхуков: {e}")

# --- Новые функции для перезагрузки FastAPI сервиса ---
@admin_only
async def confirm_restart_fastapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрашивает подтверждение на перезагрузку FastAPI сервиса."""
    query = update.callback_query
    keyboard = [
        [InlineKeyboardButton(f"🔴 Да, перезагрузить {FASTAPI_SERVICE_NAME}", callback_data="restart_fastapi_execute")],
        [InlineKeyboardButton("🟢 Нет, отмена", callback_data="cancel_restart_fastapi")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"⚠️ *Вы уверены, что хотите перезагрузить сервис `{FASTAPI_SERVICE_NAME}`?*\n"
        "Это приведет к временной недоступности API и перезапуску всех текущих обработок.",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def execute_restart_fastapi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполняет перезагрузку FastAPI сервиса."""
    query = update.callback_query
    await query.edit_message_text(f"🚀 Попытка перезагрузки сервиса `{FASTAPI_SERVICE_NAME}`...")
    
    command = f"sudo systemctl restart {FASTAPI_SERVICE_NAME}"
    try:
        # Выполняем команду в shell
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Администратор {update.effective_user.id} успешно перезагрузил сервис {FASTAPI_SERVICE_NAME}.")
            await query.message.reply_text(f"✅ Сервис `{FASTAPI_SERVICE_NAME}` успешно отправлен на перезагрузку.\n"
                                           f"Вывод: `{(stdout.decode() if stdout else 'Нет вывода')}`")
        else:
            logger.error(f"Ошибка при перезагрузке сервиса {FASTAPI_SERVICE_NAME} администратором {update.effective_user.id}. Код: {process.returncode}")
            error_message = stderr.decode() if stderr else "Нет информации об ошибке."
            await query.message.reply_text(f"❌ Ошибка при перезагрузке сервиса `{FASTAPI_SERVICE_NAME}`.\n"
                                           f"Код возврата: `{process.returncode}`\n"
                                           f"Ошибка: `{error_message}`")
    except Exception as e:
        logger.error(f"Исключение при попытке перезагрузки сервиса {FASTAPI_SERVICE_NAME}: {e}", exc_info=True)
        await query.message.reply_text(f"❌ Исключение при попытке перезагрузки сервиса: {e}")


@admin_only
async def ask_phone_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    phone = update.message.text.strip()
    if not re.match(r"^\+\d{10,15}$", phone): 
        await update.message.reply_text("❌ Неверный формат номера. Пример: +1234567890. Попробуйте снова или /cancel.")
        return ASK_PHONE

    context.user_data["phone"] = phone
    client = TelegramClient(StringSession(), API_ID, API_HASH, request_retries=2, connection_retries=2, retry_delay=3)
    context.user_data["client"] = client

    try:
        await update.message.reply_text("🔄 Подключаюсь к Telegram...")
        await client.connect()
        if not client.is_connected(): 
            await update.message.reply_text("❌ Не удалось подключиться к Telegram. Попробуйте позже или /cancel.")
            return ConversationHandler.END

        sent_code = await client.send_code_request(phone)
        context.user_data["phone_code_hash"] = sent_code.phone_code_hash
        await update.message.reply_text("📱 Код подтверждения отправлен. Введите его или /cancel:")
        return ASK_CODE
    except PhoneNumberInvalidError:
        await update.message.reply_text("❌ Указанный номер телефона недействителен. Проверьте его и попробуйте снова или /cancel.")
        if client.is_connected(): await client.disconnect()
        return ASK_PHONE 
    except FloodWaitError as e:
        logger.warning(f"FloodWait при запросе кода для {phone}: {e.seconds} сек")
        await update.message.reply_text(f"⏳ Слишком много попыток. Пожалуйста, подождите {e.seconds // 60} мин. {e.seconds % 60} сек. и попробуйте снова или /cancel.")
        if client.is_connected(): await client.disconnect()
        return ConversationHandler.END
    except (ApiIdInvalidError, AuthKeyError) as e:
        logger.error(f"Критическая ошибка конфигурации Telegram (ApiId/Hash) при запросе кода: {e}")
        await update.message.reply_text("❌ Ошибка конфигурации Telegram API (ID/Hash). Обратитесь к администратору. /cancel")
        if client.is_connected(): await client.disconnect()
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Неизвестная ошибка при запросе кода для {phone}: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла неизвестная ошибка при запросе кода. Попробуйте снова или /cancel.")
        if client.is_connected(): await client.disconnect()
        return ConversationHandler.END

@admin_only
async def handle_code_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    code = update.message.text.strip()
    client: TelegramClient = context.user_data["client"]
    phone: str = context.user_data["phone"]
    phone_code_hash: str = context.user_data["phone_code_hash"]
    context.user_data["_current_state"] = ASK_CODE # Для finally

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        session_str = client.session.save()
        sessions = await load_sessions()
        sessions[phone] = session_str 
        await save_sessions(sessions)

        stats = await load_stats()
        if phone not in stats:
            me = await client.get_me()
            account_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or me.username or f"ID:{me.id}"
            stats[phone] = {
                "name": account_name,
                "session_string_ref": session_str,
                "status_from_worker": "ok", 
                "total_uses": 0,
                "daily_usage": {},
                "last_active": datetime.datetime.now(pytz.utc).isoformat(),
                "notified_error": False
            }
            await save_stats(stats)
        else: 
            stats[phone]["session_string_ref"] = session_str
            stats[phone]["status_from_worker"] = "ok" 
            stats[phone]["notified_error"] = False
            await save_stats(stats)


        await update.message.reply_text("✅ Аккаунт успешно добавлен/обновлен!")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    except SessionPasswordNeededError:
        await update.message.reply_text("🔐 Аккаунт защищен двухфакторной аутентификацией. Введите пароль (2FA) или /cancel:")
        context.user_data["_current_state"] = ASK_PASSWORD
        return ASK_PASSWORD
    except PhoneCodeInvalidError:
        await update.message.reply_text("❌ Введен неверный код подтверждения. Попробуйте снова или /cancel.")
        return ASK_CODE 
    except FloodWaitError as e:
        logger.warning(f"FloodWait при вводе кода для {phone}: {e.seconds} сек")
        await update.message.reply_text(f"⏳ Слишком много попыток. Пожалуйста, подождите {e.seconds // 60} мин. {e.seconds % 60} сек. и попробуйте снова или /cancel.")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    except (UserDeactivatedError, SessionExpiredError, AuthKeyError) as e:
        logger.error(f"Ошибка сессии/аккаунта при вводе кода для {phone}: {e}")
        await update.message.reply_text(f"❌ Ошибка аккаунта или сессии: {type(e).__name__}. Не удалось войти. /cancel")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Неизвестная ошибка при вводе кода для {phone}: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла неизвестная ошибка при проверке кода. Попробуйте снова или /cancel.")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    finally:
        if client.is_connected() and context.user_data.get("_current_state") == ConversationHandler.END: 
            await client.disconnect()
            if "client" in context.user_data: del context.user_data["client"]


@admin_only
async def handle_2fa_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    password = update.message.text.strip()
    client: TelegramClient = context.user_data["client"]
    phone: str = context.user_data["phone"]
    context.user_data["_current_state"] = ASK_PASSWORD

    try:
        await client.sign_in(password=password)
        session_str = client.session.save()
        sessions = await load_sessions()
        sessions[phone] = session_str
        await save_sessions(sessions)

        stats = await load_stats()
        if phone not in stats:
            me = await client.get_me()
            account_name = f"{me.first_name or ''} {me.last_name or ''}".strip() or me.username or f"ID:{me.id}"
            stats[phone] = {
                "name": account_name,
                "session_string_ref": session_str,
                "status_from_worker": "ok",
                "total_uses": 0,
                "daily_usage": {},
                "last_active": datetime.datetime.now(pytz.utc).isoformat(),
                "notified_error": False
            }
            await save_stats(stats)
        else:
            stats[phone]["session_string_ref"] = session_str
            stats[phone]["status_from_worker"] = "ok"
            stats[phone]["notified_error"] = False
            await save_stats(stats)

        await update.message.reply_text("✅ Аккаунт успешно добавлен/обновлен с 2FA паролем!")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    except (UserDeactivatedError, SessionExpiredError, AuthKeyError) as e: 
        logger.error(f"Ошибка сессии/аккаунта при вводе 2FA для {phone}: {e}")
        await update.message.reply_text(f"❌ Ошибка: {type(e).__name__}. Возможно, неверный пароль или проблема с сессией. Попробуйте снова или /cancel.")
        return ASK_PASSWORD 
    except FloodWaitError as e:
        logger.warning(f"FloodWait при вводе 2FA для {phone}: {e.seconds} сек")
        await update.message.reply_text(f"⏳ Слишком много попыток. Пожалуйста, подождите {e.seconds // 60} мин. {e.seconds % 60} сек. и попробуйте снова или /cancel.")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Неизвестная ошибка при вводе 2FA для {phone}: {e}", exc_info=True)
        await update.message.reply_text("❌ Произошла неизвестная ошибка при проверке 2FA пароля. Попробуйте снова или /cancel.")
        context.user_data["_current_state"] = ConversationHandler.END
        return ConversationHandler.END
    finally:
        if client.is_connected(): # Всегда отключаем клиента после попытки 2FA
            await client.disconnect()
            if "client" in context.user_data: del context.user_data["client"]

@admin_only
async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats_data = await load_stats() 
    sessions_data = await load_sessions() 

    if not stats_data:
        # Если callback_query существует, отвечаем на него, иначе на сообщение
        reply_target = update.callback_query.message if update.callback_query else update.message
        await reply_target.reply_text("📊 Статистика пока пуста.")
        return

    text = "*📊 Статистика аккаунтов:*\n"
    active_sessions_phones = set(sessions_data.keys())

    for phone, data in stats_data.items():
        name = data.get("name", f"Аккаунт_{phone.replace('+', '')[-4:]}")
        status_worker = data.get("status_from_worker", "неизвестно")
        total_uses = data.get("total_uses", 0)
        last_active_utc_str = data.get("last_active")
        session_string_ref = data.get("session_string_ref", "N/A") 

        is_active_session = phone in active_sessions_phones and sessions_data.get(phone) == session_string_ref

        last_active_display = "Никогда"
        if last_active_utc_str:
            try:
                last_active_dt_utc = datetime.datetime.fromisoformat(last_active_utc_str.replace("Z", "+00:00"))
                last_active_dt_msk = last_active_dt_utc.astimezone(MOSCOW_TZ)
                last_active_display = last_active_dt_msk.strftime('%Y-%m-%d %H:%M:%S МСК')
            except ValueError:
                last_active_display = last_active_utc_str 

        text += f"\n📱 *{name}* ({phone})\n"
        text += f"   Статус сессии: {'✅ Активна' if is_active_session else '❌ Неактивна/Удалена'}\n"
        text += f"   Статус воркера: `{status_worker}`\n"
        text += f"   Всего использований: {total_uses}\n"
        text += f"   Последняя активность: {last_active_display}\n"

        daily_usage = data.get("daily_usage", {})
        if daily_usage:
            text += "   Использования по дням (UTC):\n"
            sorted_days = sorted(daily_usage.keys(), reverse=True)[:5]
            for day in sorted_days:
                text += f"     `{day}`: {daily_usage[day]} раз\n"
        else:
            text += "   Дневная статистика отсутствует.\n"
    
    reply_target = update.callback_query.message if update.callback_query else update.message
    if len(text) > 4096: 
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await reply_target.reply_text(part, parse_mode=ParseMode.MARKDOWN)
    else:
        await reply_target.reply_text(text, parse_mode=ParseMode.MARKDOWN)

@admin_only
async def show_delete_options(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sessions = await load_sessions() 
    stats = await load_stats()      

    reply_target = update.callback_query.message if update.callback_query else update.message
    if not sessions:
        await reply_target.reply_text("❌ Нет активных сессий для удаления.")
        return

    keyboard = []
    for phone in sessions.keys():
        acc_name = stats.get(phone, {}).get("name", f"Аккаунт_{phone.replace('+', '')[-4:]}")
        keyboard.append([InlineKeyboardButton(f"❌ {acc_name} ({phone})", callback_data=f"delete_phone_{phone}")])

    if not keyboard: 
        await reply_target.reply_text("Не удалось сформировать список для удаления.")
        return

    reply_markup = InlineKeyboardMarkup(keyboard)
    await reply_target.reply_text(
        "Выберите аккаунт для удаления сессии (статистика также будет удалена):",
        reply_markup=reply_markup
    )

@admin_only
async def delete_session_by_phone(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    sessions = await load_sessions()
    stats = await load_stats()
    query = update.callback_query 

    session_deleted = False
    if phone in sessions:
        del sessions[phone]
        await save_sessions(sessions)
        session_deleted = True

    stats_deleted = False
    if phone in stats:
        del stats[phone]
        await save_stats(stats)
        stats_deleted = True
    
    reply_target = query.message if query else update.message
    if session_deleted or stats_deleted:
        await reply_target.reply_text(f"✅ Данные для аккаунта {phone} удалены (сессия: {'да' if session_deleted else 'нет'}, статистика: {'да' if stats_deleted else 'нет'}).")
    else:
        await reply_target.reply_text(f"❌ Аккаунт {phone} не найден ни в сессиях, ни в статистике.")

@admin_only
async def download_main_app_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query 
    message_to_reply = query.message if query else update.message 

    if not os.path.exists(LOG_FILE_FROM_MAIN_APP):
        await message_to_reply.reply_text(f"❌ Файл логов FastAPI ('{LOG_FILE_FROM_MAIN_APP}') не найден.")
        return

    try:
        with open(LOG_FILE_FROM_MAIN_APP, 'rb') as log_file_obj:
            current_time_str = datetime.datetime.now(pytz.utc).strftime("%Y%m%d_%H%M%S_UTC")
            download_filename = f"fastapi_app_logs_{current_time_str}.log"
            await message_to_reply.reply_document(
                document=InputFile(log_file_obj, filename=download_filename),
                caption="📄 Вот актуальные логи FastAPI воркера."
            )
    except Exception as e:
        logger.error(f"Ошибка при отправке файла логов: {e}", exc_info=True)
        await message_to_reply.reply_text(f"❌ Не удалось отправить файл логов: {e}")

@admin_only
async def cancel_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    reply_target = update.message if update.message else (update.callback_query.message if update.callback_query else None)
    if reply_target:
        await reply_target.reply_text("Операция отменена.")
    
    if "client" in context.user_data:
        client_to_disconnect: TelegramClient = context.user_data["client"]
        if client_to_disconnect.is_connected():
            try:
                await client_to_disconnect.disconnect()
            except Exception as e:
                logger.warning(f"Ошибка при отключении клиента в cancel_handler: {e}")
        del context.user_data["client"]
    context.user_data.clear()
    return ConversationHandler.END


def main():
    if not ADMIN_IDS:
        logger.critical("Список ADMIN_IDS пуст. Бот не будет работать корректно. Завершение.")
        return

    application = Application.builder().token(ADMIN_BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^add_account$")],
        states={
            ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_phone_handler)],
            ASK_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_code_handler)],
            ASK_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_2fa_handler)],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_handler),
            CallbackQueryHandler(button_handler, pattern="^cancel_add_account$") 
        ],
        per_message=False 
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("logs", download_main_app_logs)) 
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_handler)) 

    logger.info("Админ-бот запускается...")
    application.run_polling()
    logger.info("Админ-бот остановлен.")

if __name__ == "__main__":
    main()