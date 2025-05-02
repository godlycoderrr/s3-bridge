import asyncio
import os
import logging
import random
import re
import uuid
import json
import datetime
import pytz # Для временных меток статистики
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Tuple

import aiofiles
import httpx
from aioboto3 import Session as AioBoto3Session
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError, TimeoutError, AuthKeyError, UserDeactivatedError, SessionExpiredError
from telethon.sessions import StringSession
from telethon.tl.types import Message


# --- Конфигурация ---
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("telethon").setLevel(logging.WARNING) # Уменьшаем спам от Telethon
logging.getLogger("aioboto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Telegram API
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
try:
    API_ID = int(API_ID) if API_ID else None
except (ValueError, TypeError):
    logger.error("Неверный формат API_ID в .env")
    API_ID = None

if not API_ID or not API_HASH:
    logger.critical("API_ID или API_HASH не найдены в .env! Приложение не может стартовать.")
    exit(1)

# Хранилища
SESSIONS_STORAGE_FILE = os.getenv("SESSIONS_STORAGE_FILE", "sessions.json")
STATS_STORAGE_FILE = os.getenv("STATS_STORAGE_FILE", "usage_stats.json")

# Настройки взаимодействия
TARGET_BOT_USERNAME = os.getenv("TARGET_BOT_USERNAME", "@sp_envato_bot")
TARGET_BUTTON_TEXT = os.getenv("TARGET_BUTTON_TEXT", "С лицензией")
TELEGRAM_RESPONSE_TIMEOUT = int(os.getenv("TELEGRAM_RESPONSE_TIMEOUT", 180))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", 600))

# Настройки S3 (Beget Cloud)
# Настройки S3
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_UPLOAD_TIMEOUT = 360  # 6 минут
TELEGRAM_CONNECT_TIMEOUT = 45  # 45 секунд
# --- Глобальные переменные и блокировки ---
clients: Dict[str, TelegramClient] = {} # Словарь: {session_string: client_instance}
client_status: Dict[str, str] = {} # Словарь: {session_string: "ok" | "error" | "initializing"}
client_locks: Dict[str, asyncio.Lock] = {} # Блокировки для каждого клиента по session_string
current_client_indices: Dict[str, int] = {"index": 0} # Для Round Robin
app_state = {"s3_session": None, "clients_initialized": False} # Глобальное состояние приложения
sessions_file_lock = asyncio.Lock() # Лок для чтения файла сессий
stats_file_lock = asyncio.Lock()    # Лок для чтения/записи статистики

# --- Функции для работы с файлами --- (Адаптированные из admin_bot.py)
async def load_json_data(filepath: str, lock: asyncio.Lock) -> Dict:
    async with lock:
        try:
            async with aiofiles.open(filepath, mode='r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content) if content else {}
        except FileNotFoundError: return {}
        except json.JSONDecodeError: logger.error(f"JSONDecodeError в {filepath}"); return {}
        except Exception as e: logger.error(f"Ошибка загрузки {filepath}: {e}"); return {}

async def save_json_data(filepath: str, data: Dict, lock: asyncio.Lock):
    async with lock:
        try:
            async with aiofiles.open(filepath, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=4, ensure_ascii=False))
        except Exception as e: logger.error(f"Ошибка сохранения {filepath}: {e}")

async def load_sessions_from_file() -> Dict[str, str]:
    """Загружает словарь {имя_аккаунта: строка_сессии}."""
    return await load_json_data(SESSIONS_STORAGE_FILE, sessions_file_lock)

async def load_stats() -> Dict[str, Dict]:
    """Загружает словарь статистики {строка_сессии: {данные}}."""
    return await load_json_data(STATS_STORAGE_FILE, stats_file_lock)

async def save_stats(stats: Dict[str, Dict]):
    """Сохраняет словарь статистики."""
    await save_json_data(STATS_STORAGE_FILE, stats, stats_file_lock)

async def update_stats(session_string: str, account_name: Optional[str] = None):
    """Обновляет статистику для сессии: увеличивает счетчик и ставит временную метку."""
    async with stats_file_lock:
        try:
            stats = await load_json_data(STATS_STORAGE_FILE, asyncio.Lock()) # Внутренний лок не нужен, т.к. внешний уже взят
            now_utc = datetime.datetime.now(pytz.utc)
            stat_entry = stats.get(session_string, {"requests": 0})
            stat_entry["requests"] = stat_entry.get("requests", 0) + 1
            stat_entry["last_active"] = now_utc.isoformat()
            if account_name and "name" not in stat_entry: # Добавляем имя, если его нет
                stat_entry["name"] = account_name
            stats[session_string] = stat_entry
            # Сохраняем без лока, т.к. внешний лок взят
            async with aiofiles.open(STATS_STORAGE_FILE, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps(stats, indent=4, ensure_ascii=False))
            logger.info(f"Статистика обновлена для сессии (...' {session_string[-5:]}). Запросов: {stat_entry['requests']}")
        except Exception as e:
            logger.error(f"Ошибка обновления статистики для сессии (... {session_string[-5:]}): {e}", exc_info=True)


# --- Функции S3 ---
async def get_s3_client():
    """Возвращает S3 клиент."""
    if not all([S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_ACCESS_KEY, S3_SECRET_KEY]):
        logger.error("S3 не настроен. Проверьте переменные окружения.")
        return None

    if app_state.get("s3_session") is None:
        app_state["s3_session"] = AioBoto3Session(
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
    
    return app_state["s3_session"].client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL
    )

async def upload_file_to_s3(file_path: str, s3_key: str) -> Optional[str]:
    """Загружает файл в S3 и возвращает публичную ссылку."""
    s3_client = await get_s3_client()
    if not s3_client:
        return None

    try:
        async with s3_client as s3:
            async with aiofiles.open(file_path, 'rb') as f:
                await s3.upload_fileobj(
                    f,
                    S3_BUCKET_NAME,
                    s3_key
                )
            
            # Формируем URL для Beget S3
            s3_url = f"{S3_ENDPOINT_URL}/{S3_BUCKET_NAME}/{s3_key}"
            logger.info(f"Файл загружен в S3. URL: {s3_url}")
            return s3_url
    except Exception as e:
        logger.error(f"Ошибка загрузки файла {file_path} в S3: {e}", exc_info=True)
        return None
async def download_file(url: str, local_path: str):
    """Скачивает файл по URL асинхронно."""
    try:
        # Увеличим таймауты для больших файлов
        timeout = httpx.Timeout(DOWNLOAD_TIMEOUT, connect=60)
        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, limits=limits) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                # Проверим размер файла (если доступен), чтобы не скачать слишком большой
                content_length = response.headers.get("Content-Length")
                if content_length:
                    logger.info(f"Размер скачиваемого файла: {int(content_length) / 1024 / 1024:.2f} MB")
                else:
                    logger.info("Размер скачиваемого файла неизвестен.")

                async with aiofiles.open(local_path, 'wb') as f:
                    bytes_downloaded = 0
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)
                        bytes_downloaded += len(chunk)
                        # Можно добавить логгирование прогресса, если нужно
        logger.info(f"Файл успешно скачан с {url} в {local_path} ({bytes_downloaded} байт)")
    except httpx.RequestError as e:
        logger.error(f"Ошибка сети при скачивании {url}: {e}")
        raise ValueError(f"Ошибка сети при скачивании: {e.request.url}") from e
    except httpx.HTTPStatusError as e:
        logger.error(f"Ошибка HTTP {e.response.status_code} при скачивании {url}")
        raise ValueError(f"Ошибка HTTP {e.response.status_code} от сервера {e.request.url}") from e
    except Exception as e:
        logger.error(f"Неизвестная ошибка при скачивании {url}: {e}", exc_info=True)
        raise ValueError(f"Ошибка при скачивании файла: {e}") from e

# --- Функции Telegram ---
async def initialize_telegram_clients():
    """Инициализирует и подключает Telegram клиенты из файла сессий."""
    if app_state.get("clients_initialized"):
        logger.info("Клиенты Telegram уже инициализированы.")
        return

    logger.info("Инициализация Telegram клиентов...")
    sessions_data = await load_sessions_from_file() # Получаем dict {имя: строка_сессии}

    if not sessions_data:
        logger.warning(f"Файл сессий {SESSIONS_STORAGE_FILE} пуст или не найден. Нет клиентов для запуска.")
        app_state["clients_initialized"] = True
        return

    stats_data = await load_stats() # Загружаем статистику для получения имен

    init_tasks = []
    session_strings_map = {session_str: name for name, session_str in sessions_data.items()}

    for session_str, account_name in session_strings_map.items():
        if not session_str:
            logger.warning(f"Пустая строка сессии для аккаунта '{account_name}', пропуск.")
            continue

        # Добавляем запись о статусе и лок сразу
        client_status[session_str] = "initializing"
        if session_str not in client_locks:
             client_locks[session_str] = asyncio.Lock()

        init_tasks.append(connect_single_client(session_str, account_name))

    if init_tasks:
        await asyncio.gather(*init_tasks)
    else:
        logger.warning("Не найдено валидных строк сессий для инициализации.")

    app_state["clients_initialized"] = True
    active_clients = [s for s, status in client_status.items() if status == "ok"]
    logger.info(f"Инициализация Telegram клиентов завершена. Активно: {len(active_clients)} из {len(sessions_data)}.")

async def connect_single_client(session_str: str, account_name: str):
    """Подключает одного клиента Telethon."""
    logger.info(f"Попытка подключения клиента '{account_name}'...")
    client = TelegramClient(StringSession(session_str), API_ID, API_HASH,
                             # Увеличим таймауты и лимиты для стабильности
                             request_retries=5, connection_retries=5,
                             retry_delay=5, auto_reconnect=True)
    try:
        # Используем client.start() - он сам подключится и проверит авторизацию
        await client.start()

        me = await client.get_me()
        logger.info(f"Клиент '{account_name}' (ID: {me.id}) успешно подключен и авторизован.")
        clients[session_str] = client
        client_status[session_str] = "ok"

    except (AuthKeyError, SessionExpiredError, UserDeactivatedError) as e:
        logger.error(f"Критическая ошибка авторизации клиента '{account_name}': {type(e).__name__}. Сессия невалидна. Удалите и добавьте аккаунт заново через админ-бота.")
        client_status[session_str] = "error"
        if client.is_connected(): await client.disconnect()
        # Можно добавить логику удаления невалидной сессии из файла
        # await remove_invalid_session(session_str)
    except Exception as e:
        logger.error(f"Ошибка при подключении клиента '{account_name}': {e}", exc_info=True)
        client_status[session_str] = "error"
        if client.is_connected(): await client.disconnect()

async def cleanup_telegram_clients():
    """Отключает все Telegram клиенты."""
    logger.info("Отключение Telegram клиентов...")
    disconnect_tasks = []
    for session_str, client in clients.items():
        if client.is_connected():
            account_name = "(имя неизвестно)"
            # Попробуем получить имя из статистики
            stats = await load_stats()
            if stats.get(session_str) and stats[session_str].get("name"):
                account_name = stats[session_str]["name"]
            logger.info(f"Отключение клиента '{account_name}'...")
            disconnect_tasks.append(client.disconnect())

    if disconnect_tasks:
        await asyncio.gather(*disconnect_tasks, return_exceptions=True) # Собираем ошибки отключения, если есть
    logger.info("Все Telegram клиенты отключены.")
    clients.clear()
    client_status.clear()
    client_locks.clear()


def select_client() -> Optional[Tuple[str, TelegramClient, asyncio.Lock, str]]:
    """Выбирает следующего рабочего клиента по кругу (Round Robin)."""
    global current_client_indices
    active_clients_sessions = [s for s, status in client_status.items() if status == "ok"]

    if not active_clients_sessions:
        logger.warning("Нет доступных рабочих Telegram клиентов.")
        return None

    num_clients = len(active_clients_sessions)
    start_index = current_client_indices["index"]

    # Пробуем найти свободного клиента, начиная со следующего
    for i in range(num_clients):
        current_index = (start_index + i) % num_clients
        session_str = active_clients_sessions[current_index]
        lock = client_locks[session_str]

        if not lock.locked(): # Если клиент не занят другим запросом
            client = clients[session_str]
            # Получаем имя аккаунта из статистики
            stats = app_state.get("current_stats", {}) # Берем кешированную статистику
            account_name = stats.get(session_str, {}).get("name", f"Аккаунт_{session_str[:5]}")

            current_client_indices["index"] = (current_index + 1) % num_clients # Обновляем индекс для следующего раза
            logger.info(f"Выбран клиент '{account_name}' для обработки запроса.")
            return session_str, client, lock, account_name # Возвращаем строку сессии, клиент, лок и имя

    # Если все клиенты заняты
    logger.warning("Все активные клиенты заняты. Повторите запрос позже.")
    return None # Или можно выбрать случайного и ждать лока


async def process_link_with_telegram(client: TelegramClient, url: str, account_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Обрабатывает ссылку через целевого Telegram бота.
    Возвращает (путь_к_файлу, None) или (None, ссылка_на_скачивание).
    Может выбросить исключение при ошибке.
    """
    temp_file_path = None
    start_time = datetime.datetime.now()

    try:
        logger.info(f"[{account_name}] Получение информации о боте {TARGET_BOT_USERNAME}...")
        target_entity = await client.get_entity(TARGET_BOT_USERNAME)

        # Используем диалог
        async with client.conversation(target_entity, timeout=TELEGRAM_RESPONSE_TIMEOUT) as conv:
            # 1. Отправляем ссылку
            logger.info(f"[{account_name}] Отправка URL боту: {url}")
            await conv.send_message(url)

            # 2. Ждем ответ с кнопками
            logger.info(f"[{account_name}] Ожидание ответа с кнопками...")
            response_buttons = await conv.get_response()
            logger.info(f"[{account_name}] Получено сообщение ID: {response_buttons.id}")

            if not response_buttons or not response_buttons.buttons:
                logger.warning(f"[{account_name}] Сообщение {response_buttons.id} без кнопок. Ждем еще...")
                response_buttons = await conv.get_response() # Попробуем еще раз
                if not response_buttons or not response_buttons.buttons:
                     logger.error(f"[{account_name}] Бот не прислал кнопки.")
                     raise TimeoutError("Бот не прислал сообщение с кнопками.")

            # 3. Ищем и нажимаем кнопку
            button_found = False
            for row in response_buttons.buttons:
                for button in row:
                    # Сравниваем текст кнопки, убрав лишние пробелы и регистр
                    if button.text.strip().lower() == TARGET_BUTTON_TEXT.strip().lower():
                        logger.info(f"[{account_name}] Найдена кнопка '{button.text}'. Нажимаем...")
                        # Используем click() с нужным текстом
                        await response_buttons.click(text=button.text)
                        logger.info(f"[{account_name}] Кнопка нажата.")
                        button_found = True
                        break
                if button_found: break

            if not button_found:
                logger.error(f"[{account_name}] Кнопка '{TARGET_BUTTON_TEXT}' не найдена.")
                raise ValueError(f"Кнопка '{TARGET_BUTTON_TEXT}' не найдена в ответе бота")

            # 4. Ждем финальный ответ (файл или ссылка)
            logger.info(f"[{account_name}] Ожидание финального ответа от бота...")
            final_response = await conv.get_response()
            logger.info(f"[{account_name}] Получен финальный ответ ID: {final_response.id}")

            # 5. Обрабатываем ответ
            if final_response.media and hasattr(final_response.media, 'document'):
                doc = final_response.media.document
                logger.info(f"[{account_name}] Получен документ: {getattr(doc.attributes[0], 'file_name', 'имя неизвестно')} ({doc.size / 1024 / 1024:.2f} MB)")

                # Генерируем временное имя файла
                original_filename = "unknown_file"
                for attr in doc.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        original_filename = attr.file_name
                        break
                # Очистим имя файла от недопустимых символов (простая версия)
                safe_filename = re.sub(r'[\\/*?:"<>|]', "_", original_filename)
                temp_filename = f"temp_{uuid.uuid4().hex[:8]}_{safe_filename}"
                # Ограничим длину имени файла
                temp_filename = temp_filename[:200] # Ограничение на всякий случай
                temp_file_path = os.path.join("temp_downloads", temp_filename)
                os.makedirs("temp_downloads", exist_ok=True)

                logger.info(f"[{account_name}] Скачивание файла в {temp_file_path}...")
                download_start_time = datetime.datetime.now()
                # Используем download_media самого сообщения
                await final_response.download_media(file=temp_file_path)
                download_duration = datetime.datetime.now() - download_start_time
                logger.info(f"[{account_name}] Файл скачан за {download_duration.total_seconds():.2f} сек.")
                return temp_file_path, None # Возвращаем путь к скачанному файлу

            elif final_response.text:
                logger.info(f"[{account_name}] Получено текстовое сообщение. Поиск ссылки...")
                # Ищем первую HTTP/HTTPS ссылку
                match = re.search(r'https?://\S+', final_response.text)
                if match:
                    download_url = match.group(0)
                    logger.info(f"[{account_name}] Найдена ссылка для скачивания: {download_url}")
                    return None, download_url # Возвращаем ссылку
                else:
                    logger.error(f"[{account_name}] Ссылка не найдена в тексте: {final_response.text[:200]}...")
                    raise ValueError("Ссылка на скачивание не найдена в тексте ответа бота")
            else:
                logger.error(f"[{account_name}] Неожиданный формат ответа от бота (не текст и не документ).")
                raise ValueError("Неожиданный формат ответа от бота")

    except TimeoutError as e:
        logger.error(f"[{account_name}] Таймаут ожидания ответа от {TARGET_BOT_USERNAME}. {e}")
        raise TimeoutError(f"Бот {TARGET_BOT_USERNAME} не ответил вовремя ({TELEGRAM_RESPONSE_TIMEOUT} сек).")
    except (ValueError, TypeError) as e: # Ловим ошибки парсинга и логики
        logger.error(f"[{account_name}] Ошибка обработки ответа бота: {e}", exc_info=True)
        raise # Передаем ошибку дальше как есть
    except FloodWaitError as e:
         logger.error(f"[{account_name}] FloodWaitError: {e.seconds} сек. Попробуйте позже.")
         raise HTTPException(status_code=429, detail=f"Telegram ограничил действия аккаунта. Попробуйте через {e.seconds} сек.")
    except Exception as e:
        logger.error(f"[{account_name}] Неожиданная ошибка при взаимодействии с Telegram: {type(e).__name__}: {e}", exc_info=True)
        # Проверяем на критические ошибки сессии
        if isinstance(e, (AuthKeyError, SessionExpiredError, UserDeactivatedError)):
             logger.critical(f"[{account_name}] Критическая ошибка сессии! Помечаем клиент как нерабочий.")
             client_status[client.session.save()] = "error" # Помечаем сессию как плохую
        raise HTTPException(status_code=502, detail=f"Ошибка взаимодействия с Telegram: {type(e).__name__}") # Bad Gateway
    finally:
        duration = datetime.datetime.now() - start_time
        logger.info(f"[{account_name}] Завершение process_link_with_telegram за {duration.total_seconds():.2f} сек.")
        # Временный файл удаляется позже, после загрузки в S3


# --- FastAPI приложение ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Код при старте приложения
    logger.info("Запуск FastAPI приложения...")
    os.makedirs("temp_downloads", exist_ok=True) # Создаем папку для временных файлов
    logger.info("Загрузка статистики...")
    app_state["current_stats"] = await load_stats() # Кешируем статистику при старте
    await initialize_telegram_clients()
    if S3_BUCKET_NAME: # Пробуем инициализировать S3 клиент сразу
        await get_s3_client()
    yield
    # Код при остановке приложения
    logger.info("Остановка FastAPI приложения...")
    await cleanup_telegram_clients()
    # Очистка временной папки (опционально)
    # import shutil
    # if os.path.exists("temp_downloads"):
    #     shutil.rmtree("temp_downloads")
    #     logger.info("Временная папка temp_downloads удалена.")

app = FastAPI(title="Userbot API", version="1.0.0", lifespan=lifespan)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = datetime.datetime.now()
    response = await call_next(request)
    process_time = (datetime.datetime.now() - start_time).total_seconds()
    logger.info(f"Request: {request.method} {request.url.path}?{request.query_params} - Status: {response.status_code} - Time: {process_time:.4f}s")
    return response

@app.get("/files/get-link", response_model=Dict[str, str])
async def get_file_link(url: str = Query(..., description="URL ссылки для обработки (например, с Envato Elements)")):
    """
    Обрабатывает URL через Telegram бота, скачивает результат и загружает в S3.
    Возвращает JSON с ключом 'url', содержащим ссылку на файл в S3.
    """
    if not app_state.get("clients_initialized"):
        raise HTTPException(status_code=503, detail="Сервис инициализируется, попробуйте позже.")

    client_info = select_client()
    if client_info is None:
        active_clients_count = len([s for s, status in client_status.items() if status == "ok"])
        if active_clients_count == 0:
             raise HTTPException(status_code=503, detail="Нет доступных рабочих аккаунтов Telegram.")
        else:
             raise HTTPException(status_code=429, detail="Все аккаунты Telegram заняты, попробуйте позже.") # Too Many Requests

    session_str, client, lock, account_name = client_info
    temp_downloaded_path: Optional[str] = None
    request_id = uuid.uuid4().hex[:8] # Для логов

    logger.info(f"[{account_name}][{request_id}] Начало обработки запроса для URL: {url[:50]}...")

    # Используем лок для конкретного клиента
    async with lock:
        logger.info(f"[{account_name}][{request_id}] Клиент разблокирован, начинаем работу с Telegram.")
        try:
            # 1. Взаимодействие с Telegram ботом
            local_path, download_url = await process_link_with_telegram(client, url, account_name)

            # 2. Скачивание файла (если бот вернул ссылку)
            if download_url:
                logger.info(f"[{account_name}][{request_id}] Бот вернул ссылку, скачиваем файл...")
                # Генерируем временное имя файла
                original_filename = "downloaded_file"
                try:
                    parsed_url_path = httpx.URL(download_url).path
                    potential_filename = os.path.basename(parsed_url_path)
                    if potential_filename:
                         original_filename = re.sub(r'[\\/*?:"<>|]', "_", potential_filename)[:100] # Очищаем и обрезаем
                except Exception: pass

                temp_filename = f"temp_{request_id}_{original_filename}"
                temp_downloaded_path = os.path.join("temp_downloads", temp_filename)
                os.makedirs("temp_downloads", exist_ok=True)

                await download_file(download_url, temp_downloaded_path)
            elif local_path:
                logger.info(f"[{account_name}][{request_id}] Файл скачан клиентом Telethon: {local_path}")
                temp_downloaded_path = local_path # Используем путь от Telethon
            else:
                 # Это не должно произойти, если process_link_with_telegram отработал без ошибок
                 raise ValueError("Не получен ни путь к файлу, ни ссылка от Telegram-обработчика.")

            # 3. Загрузка в S3
            if not S3_BUCKET_NAME:
                 logger.error(f"[{account_name}][{request_id}] S3 бакет не настроен.")
                 raise HTTPException(status_code=501, detail="S3 хранилище не настроено на сервере.")

            # Извлекаем оригинальное имя файла для S3 ключа
            # В функции get_file_link меняем:
            s3_filename = os.path.basename(temp_downloaded_path).split('_', 2)[-1]
            s3_key = f"{request_id}_{s3_filename}"  # Теперь без папки
            logger.info(f"[{account_name}][{request_id}] Загрузка {temp_downloaded_path} в S3 как {s3_key}...")

            s3_upload_start = datetime.datetime.now()
            s3_url = await upload_file_to_s3(temp_downloaded_path, s3_key)
            s3_upload_duration = (datetime.datetime.now() - s3_upload_start).total_seconds()

            if not s3_url:
                logger.error(f"[{account_name}][{request_id}] Не удалось загрузить файл в S3.")
                raise HTTPException(status_code=502, detail="Ошибка при загрузке файла в S3 хранилище.")
            logger.info(f"[{account_name}][{request_id}] Файл загружен в S3 за {s3_upload_duration:.2f} сек. URL: {s3_url}")

            # 4. Обновление статистики (после успешной загрузки)
            await update_stats(session_str, account_name)

            # 5. Отправка ответа
            logger.info(f"[{account_name}][{request_id}] Успешно обработан URL {url[:50]}.")
            return JSONResponse(content={"url": s3_url})

        except HTTPException as e:
            # Если это ошибка HTTP, которую мы сами создали (например, 429, 502, 503)
            logger.error(f"[{account_name}][{request_id}] Ошибка обработки {url[:50]}: HTTP {e.status_code} - {e.detail}")
            raise e # Просто передаем ее дальше
        except TimeoutError as e:
            logger.error(f"[{account_name}][{request_id}] Таймаут при обработке {url[:50]}: {e}")
            raise HTTPException(status_code=504, detail=f"Таймаут операции: {e}") # Gateway Timeout
        except ValueError as e: # Ошибки нашей логики (не найдена кнопка, ссылка, ошибка скачивания)
             logger.error(f"[{account_name}][{request_id}] Ошибка значения при обработке {url[:50]}: {e}")
             raise HTTPException(status_code=422, detail=f"Ошибка обработки данных: {e}") # Unprocessable Entity
        except Exception as e:
            logger.error(f"[{account_name}][{request_id}] Неожиданная ошибка при обработке {url[:50]}: {type(e).__name__}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {type(e).__name__}")
        finally:
            # 6. Очистка временного файла
            if temp_downloaded_path and os.path.exists(temp_downloaded_path):
                try:
                    os.remove(temp_downloaded_path)
                    logger.info(f"[{account_name}][{request_id}] Временный файл {temp_downloaded_path} удален.")
                except OSError as e:
                    logger.error(f"[{account_name}][{request_id}] Не удалось удалить временный файл {temp_downloaded_path}: {e}")
            logger.info(f"[{account_name}][{request_id}] Блокировка клиента снята.")


@app.get("/", include_in_schema=False)
async def root():
    return {"message": "Envato Userbot API. Используйте /docs для документации."}

@app.get("/health", include_in_schema=False)
async def health_check():
    # Простая проверка работоспособности
    active_clients_count = len([s for s, status in client_status.items() if status == "ok"])
    total_clients_count = len(client_status)
    s3_ok = bool(app_state.get("s3_session")) and bool(S3_BUCKET_NAME)
    return {
        "status": "ok" if app_state.get("clients_initialized") else "initializing",
        "telegram_clients": {
            "active": active_clients_count,
            "total": total_clients_count,
            "initialized": app_state.get("clients_initialized", False)
        },
        "s3_configured": s3_ok
    }


# --- Запуск ---
if __name__ == "__main__":
    import uvicorn
    logger.info("Запуск Uvicorn сервера...")
    # Рекомендуется запускать через командную строку для production:
    # uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
    # workers=1 ВАЖНО, т.к. Telethon клиенты и их состояние не потокобезопасны между воркерами
    uvicorn.run(app, host="0.0.0.0", port=8000, workers=1)