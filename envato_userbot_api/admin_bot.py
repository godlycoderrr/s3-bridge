import logging
import os
import json
import aiofiles
import datetime
from typing import Dict, Optional
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
)
from telethon.sessions import StringSession

# --- Конфигурация ---
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()

ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN")
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
ADMIN_ID = 6808477367  # ID администратора

SESSIONS_FILE = "sessions.json"
STATS_FILE = "usage_stats.json"

# --- Состояния ---
ASK_PHONE, ASK_CODE, ASK_PASSWORD = range(3)

def admin_only(func):
    """Декоратор для проверки прав администратора"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            await update.message.reply_text("⛔️ Доступ запрещен")
            return
        return await func(update, context)
    return wrapper

async def load_sessions() -> Dict:
    """Загружает сессии из файла"""
    if not os.path.exists(SESSIONS_FILE):
        return {}
    async with aiofiles.open(SESSIONS_FILE, 'r') as f:
        return json.loads(await f.read())

async def save_sessions(sessions: Dict):
    """Сохраняет сессии в файл"""
    async with aiofiles.open(SESSIONS_FILE, 'w') as f:
        await f.write(json.dumps(sessions, indent=4))

async def load_stats() -> Dict:
    """Загружает статистику из файла"""
    if not os.path.exists(STATS_FILE):
        return {}
    async with aiofiles.open(STATS_FILE, 'r') as f:
        return json.loads(await f.read())

async def save_stats(stats: Dict):
    """Сохраняет статистику в файл"""
    async with aiofiles.open(STATS_FILE, 'w') as f:
        await f.write(json.dumps(stats, indent=4))

async def update_session_stats(phone: str):
    """Обновляет статистику использования сессии"""
    stats = await load_stats()
    stats[phone] = {
        "last_active": datetime.datetime.now().isoformat(),
        "total_uses": stats.get(phone, {}).get("total_uses", 0) + 1
    }
    await save_stats(stats)

@admin_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отображает главное меню"""
    keyboard = [
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton("📊 Статистика", callback_data="show_stats")],
        [InlineKeyboardButton("❌ Удалить сессию", callback_data="delete_session")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🤖 *Управление сессиями Telegram*\n\nВыберите действие:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

@admin_only
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Обработчик нажатий на кнопки"""
    query = update.callback_query
    await query.answer()

    if query.data == "add_account":
        await query.message.reply_text("Введите номер телефона в формате +1234567890:")
        return ASK_PHONE
    elif query.data == "show_stats":
        await show_statistics(update, context)
    elif query.data == "delete_session":
        await show_delete_options(update, context)
    elif query.data.startswith("delete_"):
        phone = query.data.replace("delete_", "")
        await delete_session(update, context, phone)

async def ask_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запрашивает код подтверждения"""
    phone = update.message.text.strip()
    if not phone.startswith("+") or not phone[1:].isdigit():
        await update.message.reply_text("❌ Неверный формат номера. Попробуйте снова.")
        return ASK_PHONE

    context.user_data["phone"] = phone
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    context.user_data["client"] = client

    try:
        await client.connect()
        sent_code = await client.send_code_request(phone)
        context.user_data["phone_code_hash"] = sent_code.phone_code_hash
        await update.message.reply_text("📱 Введите код подтверждения:")
        return ASK_CODE
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END

async def handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает введенный код подтверждения"""
    code = update.message.text.strip()
    client = context.user_data["client"]
    phone = context.user_data["phone"]
    phone_code_hash = context.user_data["phone_code_hash"]

    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        session_str = client.session.save()
        
        # Сохраняем сессию
        sessions = await load_sessions()
        sessions[phone] = session_str
        await save_sessions(sessions)
        
        # Обновляем статистику
        await update_session_stats(phone)
        
        await update.message.reply_text("✅ Сессия успешно создана и сохранена!")
        return ConversationHandler.END
    except SessionPasswordNeededError:
        await update.message.reply_text("🔐 Введите пароль 2FA:")
        return ASK_PASSWORD
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Произошла ошибка. Попробуйте снова.")
        return ConversationHandler.END

async def handle_2fa(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает пароль 2FA"""
    password = update.message.text.strip()
    client = context.user_data["client"]
    phone = context.user_data["phone"]

    try:
        await client.sign_in(password=password)
        session_str = client.session.save()
        
        sessions = await load_sessions()
        sessions[phone] = session_str
        await save_sessions(sessions)
        await update_session_stats(phone)
        
        await update.message.reply_text("✅ Сессия успешно создана и сохранена!")
        return ConversationHandler.END
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text("❌ Неверный пароль. Попробуйте снова.")
        return ASK_PASSWORD

@admin_only
async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику использования сессий"""
    stats = await load_stats()
    sessions = await load_sessions()
    
    if not sessions:
        await update.callback_query.message.reply_text("📊 Нет активных сессий")
        return

    text = "*📊 Статистика сессий:*\n\n"
    for phone, _ in sessions.items():
        session_stats = stats.get(phone, {})
        last_active = session_stats.get("last_active", "Никогда")
        total_uses = session_stats.get("total_uses", 0)
        text += f"📱 *{phone}*\n"
        text += f"└ Последняя активность: {last_active}\n"
        text += f"└ Всего использований: {total_uses}\n\n"

    await update.callback_query.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def show_delete_options(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список сессий для удаления"""
    sessions = await load_sessions()
    if not sessions:
        await update.callback_query.message.reply_text("❌ Нет доступных сессий для удаления")
        return

    keyboard = []
    for phone in sessions.keys():
        keyboard.append([InlineKeyboardButton(f"❌ {phone}", callback_data=f"delete_{phone}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.message.reply_text(
        "Выберите сессию для удаления:",
        reply_markup=reply_markup
    )

async def delete_session(update: Update, context: ContextTypes.DEFAULT_TYPE, phone: str):
    """Удаляет выбранную сессию"""
    sessions = await load_sessions()
    stats = await load_stats()
    
    if phone in sessions:
        del sessions[phone]
        await save_sessions(sessions)
        if phone in stats:
            del stats[phone]
            await save_stats(stats)
        await update.callback_query.message.reply_text(f"✅ Сессия {phone} удалена")
    else:
        await update.callback_query.message.reply_text("❌ Сессия не найдена")

def main():
    """Запуск бота"""
    application = Application.builder().token(ADMIN_BOT_TOKEN).build()

    # Обработчик диалога создания сессии
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^add_account$")],
        states={
            ASK_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_code)],
            ASK_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_code)],
            ASK_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_2fa)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_handler))

    application.run_polling()

if __name__ == "__main__":
    main()