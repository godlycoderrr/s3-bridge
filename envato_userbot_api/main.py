import asyncio
import os
import logging
import re
import uuid
import json
import datetime
import mimetypes
import pytz
import random
from contextlib import asynccontextmanager
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import unquote as url_unquote, urlparse, quote as url_quote

import aiofiles
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Body, status, Security, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse, FileResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, HttpUrl # type: ignore
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, TimeoutError, AuthKeyError, UserDeactivatedError, SessionExpiredError,
    PhoneNumberInvalidError, PhoneCodeInvalidError, ApiIdInvalidError, UserAlreadyParticipantError
)
from telethon.sessions import StringSession

# --- Версия приложения ---
APP_VERSION = "1.3.8" # Совпадает с версией, где улучшено возобновление задач

# --- Загрузка конфигурации из .env ---
load_dotenv()

LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO").upper()
API_ID_STR = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSIONS_FILE_PATH = os.getenv("SESSIONS_FILE_PATH", "sessions.json")
STATS_FILE_PATH = os.getenv("STATS_FILE_PATH", "usage_stats.json")
MOSCOW_TZ_STR = os.getenv("MOSCOW_TZ", "Europe/Moscow")
UPLOAD_API_ENDPOINT = os.getenv("UPLOAD_API_ENDPOINT")
UPLOAD_API_KEY = os.getenv("UPLOAD_API_KEY")
UPLOAD_TIMEOUT_STR = os.getenv("UPLOAD_TIMEOUT", "600")
TARGET_BOT_USERNAME = os.getenv("TARGET_BOT_USERNAME", "@sp_envato_bot")
TARGET_BUTTON_TEXT = os.getenv("TARGET_BUTTON_TEXT", "С лицензией")
TELEGRAM_RESPONSE_TIMEOUT_STR = os.getenv("TELEGRAM_RESPONSE_TIMEOUT", "1800")
DOWNLOAD_TIMEOUT_STR = os.getenv("DOWNLOAD_TIMEOUT", "600")
TELEGRAM_CONNECT_TIMEOUT_STR = os.getenv("TELEGRAM_CONNECT_TIMEOUT", "45")
TEMP_DOWNLOAD_DIR = os.getenv("TEMP_DOWNLOAD_DIR", "temp_downloads")
MAX_UPLOAD_RETRIES_STR = os.getenv("MAX_UPLOAD_RETRIES", "2")
UPLOAD_RETRY_DELAY_STR = os.getenv("UPLOAD_RETRY_DELAY", "5")
MAIN_FILE_KEYWORD = os.getenv("MAIN_FILE_KEYWORD", "получены")
LICENSE_KEYWORD = os.getenv("LICENSE_KEYWORD", "скачана")
LINK_KEYWORD = os.getenv("LINK_KEYWORD", "ссылка")
FASTAPI_HOST = os.getenv("FASTAPI_HOST", "0.0.0.0")
FASTAPI_PORT_STR = os.getenv("FASTAPI_PORT", "8000")
RELOAD_FASTAPI_STR = os.getenv("RELOAD_FASTAPI", "False")
WEBHOOK_DB_FILE = os.getenv("WEBHOOK_DB_FILE", "webhook_tasks.json")
WEBHOOK_SEND_TIMEOUT_STR = os.getenv("WEBHOOK_SEND_TIMEOUT", "30")
WEBHOOK_MAX_RETRIES_STR = os.getenv("WEBHOOK_MAX_RETRIES", "6")
WEBHOOK_RETRY_DELAYS_SECONDS_STR = os.getenv("WEBHOOK_RETRY_DELAYS_SECONDS", "60,300,900,1800,3600,10800")
SESSION_REQUEST_DELAY_MIN_STR = os.getenv("SESSION_REQUEST_DELAY_MIN", "10")
SESSION_REQUEST_DELAY_MAX_STR = os.getenv("SESSION_REQUEST_DELAY_MAX", "20")
FASTAPI_CLIENT_API_KEY_EXPECTED = os.getenv("FASTAPI_CLIENT_API_KEY")
API_KEY_NAME_HEADER = "X-API-Key"
MAX_CLIENT_ACQUIRE_RETRIES_STR = os.getenv("MAX_CLIENT_ACQUIRE_RETRIES", "3") 
CLIENT_ACQUIRE_RETRY_DELAY_STR = os.getenv("CLIENT_ACQUIRE_RETRY_DELAY", "60") 
LOG_FILE_PATH_FOR_DOWNLOAD = os.getenv("LOG_FILE_FOR_DOWNLOAD", "main_fastapi_app.log")
DEFAULT_USER_AGENT = f"EnvatoFileProcessor/{APP_VERSION} (FastAPI; +https://your-service-domain.com/about)"

# --- Настройка логирования ---
logging.basicConfig(
    level=LOG_LEVEL_STR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE_PATH_FOR_DOWNLOAD, mode='a', encoding='utf-8')]
)

class RequestIdAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        request_id = self.extra.get('request_id', 'N/A')
        return '[%s] %s' % (request_id, msg), kwargs

base_logger = logging.getLogger(__name__)
logger = RequestIdAdapter(base_logger, {'request_id': 'SYSTEM'})

logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").propagate = False
logging.getLogger("uvicorn.access").propagate = False
logging.getLogger("uvicorn").propagate = False

# --- Валидация и преобразование конфигурации ---
if not API_ID_STR or not API_HASH:
    logger.critical("API_ID или API_HASH не найдены в .env! Выход.")
    exit(1)

API_ID = 0
try:
    API_ID = int(API_ID_STR)
except ValueError:
    logger.critical(f"Некорректное значение API_ID в .env: '{API_ID_STR}'. Должно быть числом. Выход.")
    exit(1)

if not FASTAPI_CLIENT_API_KEY_EXPECTED:
    logger.warning(f"FASTAPI_CLIENT_API_KEY не задан в .env. Эндпоинты FastAPI будут доступны без авторизации.")

try:
    MOSCOW_TZ = pytz.timezone(MOSCOW_TZ_STR)
except pytz.exceptions.UnknownTimeZoneError:
    logger.error(f"Неизвестная временная зона: {MOSCOW_TZ_STR}. Используется Europe/Moscow.")
    MOSCOW_TZ = pytz.timezone("Europe/Moscow")

UPLOAD_TIMEOUT = 600; TELEGRAM_RESPONSE_TIMEOUT = 1800; DOWNLOAD_TIMEOUT = 600; TELEGRAM_CONNECT_TIMEOUT = 45
MAX_UPLOAD_RETRIES = 2; UPLOAD_RETRY_DELAY = 5; FASTAPI_PORT = 8000
WEBHOOK_SEND_TIMEOUT = 30; WEBHOOK_MAX_RETRIES = 6
SESSION_REQUEST_DELAY_MIN = 10; SESSION_REQUEST_DELAY_MAX = 20
MAX_CLIENT_ACQUIRE_RETRIES = 3; CLIENT_ACQUIRE_RETRY_DELAY = 60 
WEBHOOK_RETRY_DELAYS_LIST = [60, 300, 900, 1800, 3600, 10800]

try:
    UPLOAD_TIMEOUT = int(UPLOAD_TIMEOUT_STR)
    TELEGRAM_RESPONSE_TIMEOUT = int(TELEGRAM_RESPONSE_TIMEOUT_STR)
    DOWNLOAD_TIMEOUT = int(DOWNLOAD_TIMEOUT_STR)
    TELEGRAM_CONNECT_TIMEOUT = int(TELEGRAM_CONNECT_TIMEOUT_STR)
    MAX_UPLOAD_RETRIES = int(MAX_UPLOAD_RETRIES_STR)
    UPLOAD_RETRY_DELAY = int(UPLOAD_RETRY_DELAY_STR)
    FASTAPI_PORT = int(FASTAPI_PORT_STR)
    WEBHOOK_SEND_TIMEOUT = int(WEBHOOK_SEND_TIMEOUT_STR)
    WEBHOOK_MAX_RETRIES = int(WEBHOOK_MAX_RETRIES_STR)
    SESSION_REQUEST_DELAY_MIN = int(SESSION_REQUEST_DELAY_MIN_STR)
    SESSION_REQUEST_DELAY_MAX = int(SESSION_REQUEST_DELAY_MAX_STR)
    MAX_CLIENT_ACQUIRE_RETRIES = int(MAX_CLIENT_ACQUIRE_RETRIES_STR) 
    CLIENT_ACQUIRE_RETRY_DELAY = int(CLIENT_ACQUIRE_RETRY_DELAY_STR) 

    if SESSION_REQUEST_DELAY_MIN < 0 or SESSION_REQUEST_DELAY_MAX < 0:
        raise ValueError("Задержки сессии не могут быть отрицательными.")
    if SESSION_REQUEST_DELAY_MIN > SESSION_REQUEST_DELAY_MAX:
        logger.warning(f"SESSION_REQUEST_DELAY_MIN ({SESSION_REQUEST_DELAY_MIN}) > SESSION_REQUEST_DELAY_MAX ({SESSION_REQUEST_DELAY_MAX}). Используется MAX как MIN.")
        SESSION_REQUEST_DELAY_MIN = SESSION_REQUEST_DELAY_MAX
    
    temp_delays_list = [int(d.strip()) for d in WEBHOOK_RETRY_DELAYS_SECONDS_STR.split(',') if d.strip()]
    if not temp_delays_list:
        raise ValueError("Список задержек WEBHOOK_RETRY_DELAYS_SECONDS пуст или содержит некорректные значения.")
    WEBHOOK_RETRY_DELAYS_LIST = temp_delays_list

    if len(WEBHOOK_RETRY_DELAYS_LIST) != WEBHOOK_MAX_RETRIES and WEBHOOK_MAX_RETRIES > 0 :
        logger.warning(
            f"Количество задержек в WEBHOOK_RETRY_DELAYS_SECONDS ({len(WEBHOOK_RETRY_DELAYS_LIST)}) "
            f"не совпадает с WEBHOOK_MAX_RETRIES ({WEBHOOK_MAX_RETRIES}). "
            f"Будут использованы доступные задержки, затем последняя из списка для оставшихся ретраев."
        )
except ValueError as e:
    logger.error(f"Ошибка преобразования числовых параметров из .env: {e}. Проверьте .env. Используются значения по умолчанию где возможно.")

RELOAD_FASTAPI = RELOAD_FASTAPI_STR.lower() == 'true'
logger.info(f"Конфигурация получения клиента: MAX_CLIENT_ACQUIRE_RETRIES={MAX_CLIENT_ACQUIRE_RETRIES}, CLIENT_ACQUIRE_RETRY_DELAY={CLIENT_ACQUIRE_RETRY_DELAY} сек.")

# --- Глобальные состояния и локи ---
clients: Dict[str, TelegramClient] = {}
client_status: Dict[str, str] = {} 
client_locks: Dict[str, asyncio.Lock] = {} 
client_details: Dict[str, Dict[str, str]] = {} 
current_client_indices: Dict[str, int] = {"index": 0} 

app_state = {
    "clients_initialized": False,
    "current_stats": {}, 
    "session_to_phone_map": {} 
}
client_cooldown_end_times: Dict[str, float] = {} 

sessions_file_lock_fastapi = asyncio.Lock()
stats_file_lock_fastapi = asyncio.Lock()
select_client_lock = asyncio.Lock() 
webhook_db_lock = asyncio.Lock()

# --- Защита API ключом ---
api_key_header_auth_scheme = APIKeyHeader(name=API_KEY_NAME_HEADER, auto_error=False)

async def verify_api_key(api_key_header: Optional[str] = Security(api_key_header_auth_scheme)):
    if not FASTAPI_CLIENT_API_KEY_EXPECTED: 
        return True
    if api_key_header == FASTAPI_CLIENT_API_KEY_EXPECTED:
        return True
    
    if api_key_header is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Не аутентифицирован: отсутствует API ключ в заголовке '{API_KEY_NAME_HEADER}'"
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Запрещено: неверный API ключ"
        )

# --- Вспомогательные функции ---
async def load_json_data_fastapi(filepath: str, lock: asyncio.Lock) -> Dict:
    local_logger = logging.getLogger(f"{__name__}.load_json_fastapi")
    async with lock:
        data_dict = {}
        try:
            async with aiofiles.open(filepath, 'r', encoding='utf-8') as f:
                data = await f.read()
                data_dict = json.loads(data) if data else {}
        except FileNotFoundError:
            local_logger.warning(f"Файл не найден: {filepath}.")
        except json.JSONDecodeError:
            local_logger.error(f"Ошибка декодирования JSON: {filepath}. Файл может быть поврежден или пуст.")
            data_dict = {} 
        except Exception as e:
            local_logger.error(f"Ошибка чтения JSON {filepath}: {e}", exc_info=True)
            data_dict = {}
        return data_dict

async def save_json_data_fastapi(filepath: str, data: Dict, lock: asyncio.Lock):
    local_logger = logging.getLogger(f"{__name__}.save_json_fastapi")
    async with lock:
        try:
            async with aiofiles.open(filepath, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            local_logger.error(f"Ошибка сохранения JSON {filepath}: {e}", exc_info=True)

async def load_sessions_from_file_fastapi() -> Dict[str, str]:
    return await load_json_data_fastapi(SESSIONS_FILE_PATH, sessions_file_lock_fastapi)

async def update_stats_on_request(phone_number: str, account_name: Optional[str] = None, request_id: str = 'N/A'):
    req_logger = RequestIdAdapter(base_logger, {'request_id': request_id})
    try:
        async with stats_file_lock_fastapi:
            stats = app_state.get("current_stats", {})
            if not stats: 
                try:
                    async with aiofiles.open(STATS_FILE_PATH, 'r', encoding='utf-8') as f_stats_read:
                        data = await f_stats_read.read()
                        stats = json.loads(data) if data else {}
                except FileNotFoundError: stats = {}
                except json.JSONDecodeError: stats = {}
                except Exception: stats = {}

            today_utc_str = datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d')
            entry = stats.get(phone_number, {})
            entry["total_uses"] = entry.get("total_uses", 0) + 1
            entry["last_active"] = datetime.datetime.now(pytz.utc).isoformat()
            if account_name: entry["name"] = account_name
            elif "name" not in entry: entry["name"] = f"Аккаунт_{phone_number.replace('+', '')[-4:]}"
            
            daily_usage_data = entry.get("daily_usage", {})
            daily_usage_data[today_utc_str] = daily_usage_data.get(today_utc_str, 0) + 1
            entry["daily_usage"] = daily_usage_data
            stats[phone_number] = entry
            
            try:
                async with aiofiles.open(STATS_FILE_PATH, 'w', encoding='utf-8') as f_stats_write:
                    await f_stats_write.write(json.dumps(stats, indent=2, ensure_ascii=False))
                req_logger.info(f"Статистика (счетчики) обновлена для '{entry.get('name', phone_number)}' (Запросов сегодня UTC: {daily_usage_data.get(today_utc_str,0)})")
                app_state["current_stats"] = stats 
            except Exception as e_save:
                req_logger.error(f"Ошибка сохранения статистики (счетчики) {STATS_FILE_PATH}: {e_save}", exc_info=True)
    except Exception as e_outer:
        req_logger.error(f"Критическая ошибка обновления статистики (счетчики) для {phone_number}: {e_outer}", exc_info=True)

async def update_session_worker_status_in_stats(
    phone_number: str, session_string: str, new_status: str,
    account_name_from_worker: Optional[str] = None, request_id: str = 'N/A'
):
    req_logger = RequestIdAdapter(base_logger, {'request_id': request_id or 'STATUS_UPDATE'})
    try:
        async with stats_file_lock_fastapi:
            stats = app_state.get("current_stats", {})
            if not stats: 
                try:
                    async with aiofiles.open(STATS_FILE_PATH, 'r', encoding='utf-8') as f_stats_read:
                        data = await f_stats_read.read(); stats = json.loads(data) if data else {}
                except FileNotFoundError: stats = {}
                except json.JSONDecodeError: stats = {}
                except Exception: stats = {}

            entry = stats.get(phone_number, {})
            changed_fields = False
            if entry.get("status_from_worker") != new_status:
                entry["status_from_worker"] = new_status; changed_fields = True
                if new_status == "ok" and entry.get("notified_error") is True: 
                    entry["notified_error"] = False
                req_logger.info(f"Статус воркера для {phone_number} (SID: ...{session_string[-6:]}) изменен на '{new_status}'.")

            if account_name_from_worker and entry.get("name") != account_name_from_worker:
                entry["name"] = account_name_from_worker; changed_fields = True
            
            if "last_active" not in entry: entry["last_active"] = datetime.datetime.now(pytz.utc).isoformat(); changed_fields = True
            if "total_uses" not in entry: entry["total_uses"] = 0; changed_fields = True
            if "daily_usage" not in entry: entry["daily_usage"] = {}; changed_fields = True
            if "name" not in entry: entry["name"] = account_name_from_worker or f"Аккаунт_{phone_number.replace('+', '')[-4:]}"; changed_fields = True
            if "notified_error" not in entry: entry["notified_error"] = False; changed_fields = True 
            if "session_string_ref" not in entry or entry["session_string_ref"] != session_string:
                 entry["session_string_ref"] = session_string; changed_fields = True
            
            today_utc_str = datetime.datetime.now(pytz.utc).strftime('%Y-%m-%d')
            notified_limit_key = f"notified_daily_limit_{today_utc_str}" 
            if notified_limit_key not in entry : entry[notified_limit_key] = False; changed_fields = True


            if changed_fields or phone_number not in stats: 
                stats[phone_number] = entry
                try:
                    async with aiofiles.open(STATS_FILE_PATH, 'w', encoding='utf-8') as f_stats_write:
                        await f_stats_write.write(json.dumps(stats, indent=2, ensure_ascii=False))
                    req_logger.info(f"Данные сессии {phone_number} (статус: {new_status}) сохранены в {STATS_FILE_PATH}.")
                    app_state["current_stats"] = stats 
                except Exception as e_save:
                    req_logger.error(f"Ошибка сохранения данных сессии в {STATS_FILE_PATH}: {e_save}", exc_info=True)
    except Exception as e_outer:
        req_logger.error(f"Критическая ошибка обновления данных сессии для {phone_number}: {e_outer}", exc_info=True)

# --- Функции взаимодействия с внешними API и Telegram ---
async def upload_file_via_api(
    file_path: str, request_id: str, s3_filename: str, original_human_readable_filename: str
) -> Optional[str]:
    req_logger = RequestIdAdapter(base_logger, {'request_id': request_id})
    if not UPLOAD_API_ENDPOINT or not UPLOAD_API_KEY:
        req_logger.error("❌ API endpoint/key для загрузки не настроены!")
        return None
    if not os.path.exists(file_path):
        req_logger.error(f"❌ Файл для загрузки не существует: {file_path}")
        return None

    req_logger.info(f"🚀 Подготовка к загрузке: '{original_human_readable_filename}' (S3: '{s3_filename}') из '{file_path}'")
    file_size = os.path.getsize(file_path)
    req_logger.info(f"📦 Размер: {file_size} байт")
    mime_type, _ = mimetypes.guess_type(original_human_readable_filename)
    content_type = mime_type or "application/octet-stream"
    req_logger.info(f"🔍 Content-Type: {content_type}")

    try:
        original_human_readable_filename.encode('ascii')
        content_disposition_header = f'attachment; filename="{original_human_readable_filename}"'
    except UnicodeEncodeError:
        encoded_filename = url_quote(original_human_readable_filename, encoding='utf-8')
        content_disposition_header = f"attachment; filename*=UTF-8''{encoded_filename}"

    upload_target_url = UPLOAD_API_ENDPOINT
    http_headers = {
        "Authorization": UPLOAD_API_KEY, "Content-Type": content_type,
        "Content-Disposition": content_disposition_header, "User-Agent": DEFAULT_USER_AGENT
    }
    upload_url_result = None; last_error = None

    try:
        async with aiofiles.open(file_path, 'rb') as f_content_reader:
            file_content_bytes = await f_content_reader.read() 

        for attempt in range(MAX_UPLOAD_RETRIES + 1):
            req_logger.info(f"⏫ Попытка загрузки #{attempt + 1}/{MAX_UPLOAD_RETRIES + 1} файла '{s3_filename}' (ориг. '{original_human_readable_filename}')...")
            response = None
            try:
                async with httpx.AsyncClient(timeout=UPLOAD_TIMEOUT) as client_http:
                    response = await client_http.post(upload_target_url, headers=http_headers, content=file_content_bytes)
                req_logger.info(f"📥 Ответ API [Попытка {attempt + 1}]: Статус {response.status_code}")

                if response.status_code == 200 or response.status_code == 201:
                    api_result_data = {}
                    try:
                        api_result_data = response.json()
                    except json.JSONDecodeError:
                        req_logger.warning(f"Ответ API не JSON, статус {response.status_code}. Текст: {response.text[:200]}")
                    except Exception:
                        req_logger.exception("Ошибка при обработке JSON ответа")
                    
                    url_from_response = api_result_data.get('url') or api_result_data.get('filePath') or api_result_data.get('link')
                    if not url_from_response and response.headers.get("Location"): url_from_response = response.headers.get("Location")

                    if url_from_response:
                        req_logger.info(f"✅ Успех [Попытка {attempt + 1}]: URL получен: {url_from_response}")
                        upload_url_result = url_from_response
                    else:
                        req_logger.info(f"✅ Успех [Попытка {attempt + 1}]: Загрузка успешна (статус {response.status_code}), URL не получен. Используем s3_filename: {s3_filename}")
                        upload_url_result = s3_filename 
                    last_error = None; break
                elif 400 <= response.status_code < 500:
                    error_text = response.text
                    try: error_json = response.json(); error_text = json.dumps(error_json)
                    except: pass
                    last_error = f"HTTP ошибка клиента {response.status_code}: {error_text}"; req_logger.error(f"❌ {last_error} (повтор не для 4xx)"); break
                else: 
                    last_error = f"HTTP ошибка {response.status_code}: {response.text}"; req_logger.warning(f"⚠️ {last_error}")
            except httpx.TimeoutException: last_error = f"Таймаут API [Попытка {attempt + 1}]"; req_logger.warning(f"⌛ {last_error}: {upload_target_url}")
            except httpx.RequestError as e_req: last_error = f"Ошибка HTTP запроса API [Попытка {attempt + 1}]"; req_logger.warning(f"❌ {last_error}: {e_req}")
            except Exception as e_inner: last_error = f"Ошибка загрузки [Попытка {attempt + 1}]"; req_logger.error(f"🚨 {last_error}: {str(e_inner)}", exc_info=True); break 

            if upload_url_result: break 
            if attempt < MAX_UPLOAD_RETRIES and not (response and 400 <= response.status_code < 500): 
                req_logger.info(f"Ожидание {UPLOAD_RETRY_DELAY} сек..."); await asyncio.sleep(UPLOAD_RETRY_DELAY)
    except Exception as e_outer:
        last_error = f"Крит. ошибка ДО загрузки '{s3_filename}': {str(e_outer)}"; req_logger.error(f"🚨 {last_error}", exc_info=True)

    if not upload_url_result and last_error:
        req_logger.error(f"⛔ Загрузка '{original_human_readable_filename}' (S3: '{s3_filename}') не удалась. Ошибка: {last_error}")
    return upload_url_result

def extract_url(text: str, request_id: str = 'N/A') -> Optional[str]:
    req_logger = RequestIdAdapter(base_logger, {'request_id': request_id})
    if not text:
        req_logger.debug("Пустой текст для извлечения URL.")
        return None
    pattern = r'https?://(?:[a-zA-Z0-9_.\-~:/?#\[\]@!$&\'()*+,;=]|%[0-9a-fA-F]{2})+'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        url = match.group(0)
        while url.endswith(('.', ',', ';', ':', '!', '?')):
            url = url[:-1]
        if url.endswith(')') and text.rfind('(', 0, match.start()) != -1 and url.count('(') < url.count(')'):
            url = url[:-1]
        req_logger.info(f"🔗 Найден URL: {url}")
        return url
    req_logger.warning(f"URL не найден в тексте: '{text[:200]}...'")
    return None

async def process_link_with_telegram(client: TelegramClient, url: str, account_name: str, phone_number: str, request_id: str) -> dict:
    proc_logger = RequestIdAdapter(base_logger, {'request_id': request_id})
    proc_logger.info(f"🚀 Обработка ссылки: '{url[:100]}...' аккаунтом '{account_name}' ({phone_number})")
    result = {'main_url': None, 'license_url': None, 'error': None, 'telegram_error_type': None}
    conversation_timed_out = False
    session_str_saved = client.session.save() if client and client.session else None

    try:
        entity = await client.get_entity(TARGET_BOT_USERNAME)
        total_conversation_timeout = TELEGRAM_RESPONSE_TIMEOUT + 120 

        async with client.conversation(entity, timeout=total_conversation_timeout) as conv:
            proc_logger.info(f"📨 Отправка URL '{url[:100]}...' боту {TARGET_BOT_USERNAME}")
            await conv.send_message(url)
            
            response_buttons = None
            try:
                proc_logger.info(f"⏳ Ожидание кнопки '{TARGET_BUTTON_TEXT}' (таймаут {TELEGRAM_RESPONSE_TIMEOUT} сек)")
                response_buttons = await conv.get_response(timeout=TELEGRAM_RESPONSE_TIMEOUT)
            except asyncio.TimeoutError:
                conversation_timed_out = True
                err_msg = f"Бот {TARGET_BOT_USERNAME} не ответил с кнопками за {TELEGRAM_RESPONSE_TIMEOUT} сек."
                result['telegram_error_type'] = "ButtonTimeoutError"
                raise TimeoutError(err_msg)

            button_found = False
            if response_buttons and response_buttons.buttons:
                for r_idx, row in enumerate(response_buttons.buttons):
                    for b_idx, btn in enumerate(row):
                        if btn.text.strip().lower() == TARGET_BUTTON_TEXT.lower():
                            proc_logger.info(f"🔘 Кнопка '{btn.text}' найдена (row {r_idx}, col {b_idx}). Нажимаем...")
                            await asyncio.sleep(random.uniform(0.5,1.5)) 
                            await response_buttons.click(text=btn.text)
                            button_found = True; break
                    if button_found: break
            
            if not button_found:
                err_msg = f"❌ Кнопка '{TARGET_BUTTON_TEXT}' не найдена. Ответ: '{response_buttons.text[:200] if response_buttons else 'Нет ответа'}'"
                result['telegram_error_type'] = "ButtonNotFoundError"
                raise ValueError(err_msg)

            try: 
                intermediate_response = await conv.get_response(timeout=30) 
                proc_logger.info(f"💬 Промежуточный ответ: '{intermediate_response.text[:150]}...'")
            except asyncio.TimeoutError:
                proc_logger.warning("⏳ Промежуточного сообщения не было (таймаут 30 сек).")

            proc_logger.info(f"⏳ Ожидание файлов/ссылок от {TARGET_BOT_USERNAME}...")
            main_file_message, license_message = None, None
            wait_start_time = asyncio.get_event_loop().time()

            while not (result.get('main_url') and result.get('license_url')): 
                if main_file_message and license_message and not result.get('main_url') and not result.get('license_url'):
                    proc_logger.info("Сообщения для основного файла и лицензии получены, но URL не извлеклись. Завершаем ожидание.")
                    break 

                remaining_time = total_conversation_timeout - (asyncio.get_event_loop().time() - wait_start_time)
                if remaining_time <= 1: 
                    proc_logger.error("⌛ Истекло общее время ожидания файлов/ссылок.")
                    if not result.get('main_url'):
                        result['telegram_error_type'] = "MainFileTimeoutError"
                        raise TimeoutError("Истекло время, основной файл/ссылка не получены.")
                    proc_logger.warning("Таймаут ожидания лицензии (основной файл есть). Завершаем ожидание.")
                    break 

                current_resp = None
                try:
                    current_get_timeout = min(remaining_time, float(TELEGRAM_RESPONSE_TIMEOUT)) 
                    current_resp = await conv.get_response(timeout=current_get_timeout)
                    current_text = (current_resp.text or "").lower()

                    if not result.get('main_url') and MAIN_FILE_KEYWORD in current_text and LINK_KEYWORD in current_text:
                        proc_logger.info(f"✅ Сообщение основного файла получено!")
                        main_file_message = current_resp
                        extracted_main_url = extract_url(main_file_message.text, request_id)
                        if extracted_main_url:
                            result['main_url'] = extracted_main_url
                            proc_logger.info(f"🔗 URL основного файла: {result['main_url']}")
                        else:
                            proc_logger.warning(f"⚠️ Не извлечен URL из сообщения основного файла: {main_file_message.text[:100]}")
                    elif not result.get('license_url') and LICENSE_KEYWORD in current_text and LINK_KEYWORD in current_text:
                        proc_logger.info(f"✅ Сообщение лицензии получено!")
                        license_message = current_resp
                        extracted_license_url = extract_url(license_message.text, request_id)
                        if extracted_license_url:
                            result['license_url'] = extracted_license_url
                            proc_logger.info(f"🔗 URL лицензии: {result['license_url']}")
                        else:
                            proc_logger.warning(f"⚠️ Не извлечен URL из сообщения лицензии: {license_message.text[:100]}")
                    elif "ошибка" in current_text or "не найден" in current_text or "лимит" in current_text:
                        error_bot = f"Бот сообщил ошибку: {current_text[:100]}"
                        proc_logger.error(f"❌ {error_bot} (Текст: '{current_text[:200]}...')")
                        result['telegram_error_type'] = "BotReportedError"
                        raise ValueError(error_bot)

                except asyncio.TimeoutError:
                    proc_logger.error(f"⌛ Таймаут ({current_get_timeout:.1f} сек) ожидания очередного сообщения от бота.")
                    conversation_timed_out = True
                    if not result.get('main_url'):
                        result['telegram_error_type'] = "MainFileTimeoutError"
                        raise TimeoutError(f"Бот не прислал основной файл/ссылку (таймаут в цикле ожидания).")
                    else: 
                        proc_logger.warning("Таймаут ожидания лицензии (основной файл есть). Завершаем ожидание.")
                        break 
            
            if not result.get('main_url'):
                result['telegram_error_type'] = "MainUrlMissingAfterLoop"
                raise ValueError("Сообщение с основным файлом/ссылкой не получено или URL не извлечен после завершения цикла ожидания.")
            if not result.get('license_url'):
                proc_logger.warning("⚠️ URL лицензии не получен/извлечен по итогу диалога.")
            
            proc_logger.info(f"🏁 Завершение диалога с ботом для '{account_name}'.")
            return result

    except (FloodWaitError, TimeoutError, UserDeactivatedError, AuthKeyError, SessionExpiredError, ValueError,
            PhoneNumberInvalidError, PhoneCodeInvalidError, ApiIdInvalidError, UserAlreadyParticipantError) as e:
        err_type_name = type(e).__name__
        err_msg = f"{err_type_name}: {str(e)}"
        proc_logger.error(f"Ошибка TG ({err_type_name}) для '{account_name}' ({phone_number}): {str(e)}", exc_info=isinstance(e, (FloodWaitError, TimeoutError)))
        result['error'] = err_msg
        result['telegram_error_type'] = err_type_name

        if session_str_saved and phone_number: 
            worker_new_status = "error" 
            if isinstance(e, FloodWaitError):
                flood_end_ts = int(datetime.datetime.now().timestamp() + e.seconds + 5) 
                client_status[session_str_saved] = f"flood_wait_{flood_end_ts}"
                proc_logger.warning(f"Аккаунт '{account_name}' ({phone_number}) FloodWait на {e.seconds} сек (до {datetime.datetime.fromtimestamp(flood_end_ts).isoformat()}).")
                return result 
            elif isinstance(e, TimeoutError) and conversation_timed_out:
                proc_logger.warning(f"Таймаут ответа от {TARGET_BOT_USERNAME} для '{account_name}' ({phone_number}). Статус клиента не меняется из-за этого типа таймаута.")
            elif isinstance(e, (UserDeactivatedError, AuthKeyError, SessionExpiredError, PhoneNumberInvalidError, ApiIdInvalidError)):
                worker_new_status = "auth_error"
                if isinstance(e, UserDeactivatedError): worker_new_status = "deactivated"
                elif isinstance(e, SessionExpiredError): worker_new_status = "expired"
                client_status[session_str_saved] = worker_new_status
                proc_logger.error(f"Ошибка авторизации/сессии ({err_type_name}) для '{account_name}' ({phone_number}). Статус: '{worker_new_status}'.")
                await update_session_worker_status_in_stats(phone_number, session_str_saved, worker_new_status, account_name, request_id)
            else: 
                client_status[session_str_saved] = "error"
                proc_logger.error(f"Ошибка ({err_type_name}) с '{account_name}' ({phone_number}). Статус: 'error'.")
                await update_session_worker_status_in_stats(phone_number, session_str_saved, "error", account_name, request_id)
        return result
    except Exception as e:
        err_type_name = type(e).__name__
        err_msg = f"🚨 Крит. неперехваченная ошибка в диалоге TG: {err_type_name} - {str(e)}"
        proc_logger.error(err_msg, exc_info=True)
        result['error'] = err_msg
        result['telegram_error_type'] = "UnhandledExceptionInTelegramLogic"
        if session_str_saved and phone_number:
            client_status[session_str_saved] = "error"
            proc_logger.warning(f"Аккаунт '{account_name}' ({phone_number}) помечен 'error' из-за крит. ошибки в логике TG.")
            await update_session_worker_status_in_stats(phone_number, session_str_saved, "error", account_name, request_id)
        return result

async def download_file(url: str, request_id: str, prefix: str) -> Optional[Tuple[str, str, str]]:
    req_logger = RequestIdAdapter(base_logger, {'request_id': request_id})
    try:
        req_logger.info(f"⬇️ Скачивание '{prefix}' URL: {url}")
        http_headers = {"User-Agent": DEFAULT_USER_AGENT}
        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True, headers=http_headers) as client_http:
            async with client_http.stream("GET", url) as response:
                response.raise_for_status() 
                original_human_readable_filename = None
                
                content_disposition = response.headers.get("Content-Disposition")
                if content_disposition:
                    match_simple = re.search(r'filename="([^"]+)"', content_disposition, re.IGNORECASE)
                    if match_simple:
                        original_human_readable_filename = url_unquote(match_simple.group(1))
                    else:
                        match_rfc = re.search(r"filename\*=([^']+)'([^'])?'(.+)", content_disposition, re.IGNORECASE)
                        if match_rfc:
                            encoding = match_rfc.group(1) or 'utf-8'
                            encoded_name = match_rfc.group(3)
                            try:
                                original_human_readable_filename = url_unquote(encoded_name, encoding=encoding)
                            except Exception as e_dec:
                                req_logger.warning(f"Ошибка декодирования filename* ('{encoded_name}') с кодировкой '{encoding}': {e_dec}, fallback.")
                
                if not original_human_readable_filename: 
                    path_from_url = urlparse(url).path
                    if path_from_url:
                        original_human_readable_filename = url_unquote(os.path.basename(path_from_url))

                file_extension = ""
                if original_human_readable_filename:
                    original_human_readable_filename = re.sub(r'[^\w\s.-]', '', original_human_readable_filename) 
                    original_human_readable_filename = re.sub(r'\s+', '_', original_human_readable_filename).strip('.-_') 
                    original_human_readable_filename = original_human_readable_filename[:200] 
                    _, file_extension = os.path.splitext(original_human_readable_filename)
                
                if not original_human_readable_filename: 
                    content_type_header = response.headers.get("Content-Type", "").split(";")[0].strip()
                    file_extension = mimetypes.guess_extension(content_type_header) or ".dat"
                    original_human_readable_filename = f"downloaded_file{file_extension}"
                elif not file_extension: 
                    content_type_header = response.headers.get("Content-Type", "").split(";")[0].strip()
                    guessed_extension = mimetypes.guess_extension(content_type_header)
                    if guessed_extension:
                        original_human_readable_filename += guessed_extension
                        file_extension = guessed_extension
                
                unique_s3_filename = f"{prefix}_{uuid.uuid4().hex}{file_extension}" 
                temp_file_name_local = f"{uuid.uuid4().hex[:12]}.tmp" 
                temp_file_path = os.path.join(TEMP_DOWNLOAD_DIR, temp_file_name_local)
                os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)

                req_logger.info(f"📥 Сохранение '{prefix}' (ориг. '{original_human_readable_filename}', S3: '{unique_s3_filename}') в: {temp_file_path}")
                bytes_downloaded = 0
                async with aiofiles.open(temp_file_path, 'wb') as f_write:
                    async for chunk in response.aiter_bytes():
                        await f_write.write(chunk)
                        bytes_downloaded += len(chunk)
                
                req_logger.info(f"✅ Скачан '{prefix}': '{original_human_readable_filename}' ({bytes_downloaded} байт). Путь: {temp_file_path}. S3: {unique_s3_filename}")
                return (temp_file_path, unique_s3_filename, original_human_readable_filename)

    except httpx.HTTPStatusError as e_http:
        req_logger.error(f"🚨 HTTP ошибка {e_http.response.status_code} скачивания '{prefix}' URL {url}: {e_http.response.text[:200]}")
        return None
    except Exception as e:
        req_logger.error(f"🚨 Общая ошибка скачивания '{prefix}' URL {url}: {str(e)}", exc_info=True)
        return None

async def initialize_telegram_clients():
    sys_logger = RequestIdAdapter(base_logger, {'request_id': 'INIT'})
    if app_state["clients_initialized"]:
        sys_logger.info("Клиенты TG уже инициализированы.")
        return

    sys_logger.info("🔌 Инициализация Telegram клиентов...")
    client_cooldown_end_times.clear()
    client_details.clear() 

    sessions_data = await load_sessions_from_file_fastapi()
    if not sessions_data:
        sys_logger.warning(f"⚠️ Файл сессий '{SESSIONS_FILE_PATH}' пуст или не найден. Инициализация без клиентов.")
        app_state["clients_initialized"] = True
        return

    app_state["session_to_phone_map"] = {v: k for k, v in sessions_data.items() if isinstance(k, str) and isinstance(v, str)}

    try: 
        async with stats_file_lock_fastapi: 
            stats_content = {};
            try:
                async with aiofiles.open(STATS_FILE_PATH, 'r', encoding='utf-8') as f_stats:
                    data = await f_stats.read()
                    stats_content = json.loads(data) if data else {}
                sys_logger.info(f"📊 Статистика из {STATS_FILE_PATH} загружена")
            except FileNotFoundError:
                sys_logger.warning(f"Файл статистики {STATS_FILE_PATH} не найден. Будет создан новый.")
            except json.JSONDecodeError:
                sys_logger.error(f"Ошибка декодирования JSON статистики {STATS_FILE_PATH}. Файл может быть поврежден.")
            app_state["current_stats"] = stats_content
    except Exception as e:
        sys_logger.error(f"Не удалось обработать файл статистики {STATS_FILE_PATH}: {e}", exc_info=True)
        app_state["current_stats"] = {} 

    init_tasks = []
    for phone, session_str in sessions_data.items():
        if session_str and isinstance(session_str, str):
            if session_str not in client_locks: 
                client_locks[session_str] = asyncio.Lock()
            init_tasks.append(connect_single_client(session_str, phone))
        else:
            sys_logger.warning(f"Пропущена некорректная сессия для телефона '{phone}' в {SESSIONS_FILE_PATH}.")

    if not init_tasks:
        sys_logger.warning("⚠️ Нет валидных сессий для инициализации.")
        app_state["clients_initialized"] = True
        return

    await asyncio.gather(*init_tasks)
    app_state["clients_initialized"] = True
    
    active = len([s for s, v in client_status.items() if v == "ok"])
    errors = len([s for s, v in client_status.items() if v != "ok" and not v.startswith("flood_wait")])
    flood = len([s for s, v in client_status.items() if v.startswith("flood_wait")])
    sys_logger.info(f"✅ Инициализация клиентов завершена. Всего сессий: {len(sessions_data)}. Активных: {active}, ошибок: {errors}, flood_wait: {flood}")

async def connect_single_client(session_str: str, phone_hint: str):
    req_id_phone_part = phone_hint.replace("+", "")[:10] if phone_hint else "UnkPh"
    conn_logger = RequestIdAdapter(base_logger, {'request_id': f'CONN-{req_id_phone_part}'.strip('-')})
    
    conn_logger.info(f"🔗 Подключение клиента (тел. {phone_hint}) SID: ...{session_str[-5:]}")

    if session_str in clients and clients[session_str].is_connected():
        conn_logger.info(f"Клиент для SID ...{session_str[-5:]} (тел. {phone_hint}) уже подключен.")
        if client_status.get(session_str) != "ok": client_status[session_str] = "ok" 
        if session_str not in client_details: 
            try:
                me = await clients[session_str].get_me()
                name = f"{me.first_name or ''} {me.last_name or ''}".strip() or me.username or f"ID:{me.id}" if me else phone_hint
                client_details[session_str] = {"phone": phone_hint, "name": name, "original_phone_hint": phone_hint}
            except Exception as e:
                conn_logger.warning(f"Не удалось выполнить get_me для уже подключенного клиента SID ...{session_str[-5:]}: {e}")
        return

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH,
                            request_retries=3, connection_retries=3, retry_delay=5)
    name_from_tg = phone_hint 

    try:
        await asyncio.wait_for(client.connect(), timeout=float(TELEGRAM_CONNECT_TIMEOUT))
        if not await client.is_user_authorized():
            conn_logger.error(f"❌ Ошибка авторизации для '{phone_hint}'. Сессия невалидна или отозвана.")
            client_status[session_str] = "auth_error"
            await update_session_worker_status_in_stats(phone_hint, session_str, "auth_error", phone_hint, conn_logger.extra['request_id'])
            await client.disconnect()
            return
        
        me = await client.get_me()
        if me:
            name_from_tg = f"{me.first_name or ''} {me.last_name or ''}".strip() or me.username or f"ID:{me.id}"
            conn_logger.info(f"✅ Подключен: '{name_from_tg}' (тел. {phone_hint})")
        else:
            conn_logger.warning(f"✅ Подключен для '{phone_hint}', но не удалось получить информацию о пользователе (get_me). Используется имя: '{name_from_tg}'.")
        
        client_status[session_str] = "ok"
        client_details[session_str] = {"phone": phone_hint, "name": name_from_tg, "original_phone_hint": phone_hint}
        await update_session_worker_status_in_stats(phone_hint, session_str, "ok", name_from_tg, conn_logger.extra['request_id'])
        clients[session_str] = client

    except asyncio.TimeoutError:
        conn_logger.error(f"⌛ Таймаут подключения ({TELEGRAM_CONNECT_TIMEOUT} сек) для '{phone_hint}'.")
        client_status[session_str] = "timeout"
        await update_session_worker_status_in_stats(phone_hint, session_str, "timeout", name_from_tg, conn_logger.extra['request_id'])
    except (AuthKeyError, SessionPasswordNeededError) as e: 
        conn_logger.error(f"🔑 Ошибка ключа авторизации или 2FA ({type(e).__name__}) для '{phone_hint}'. Сессия может быть недействительной.")
        client_status[session_str] = "auth_key_error" 
        await update_session_worker_status_in_stats(phone_hint, session_str, "auth_key_error", name_from_tg, conn_logger.extra['request_id'])
    except UserDeactivatedError:
        conn_logger.error(f"🚫 Аккаунт '{phone_hint}' деактивирован (забанен).")
        client_status[session_str] = "deactivated"
        await update_session_worker_status_in_stats(phone_hint, session_str, "deactivated", name_from_tg, conn_logger.extra['request_id'])
    except SessionExpiredError:
        conn_logger.error(f"🕒 Сессия истекла для '{phone_hint}'. Требуется новая авторизация.")
        client_status[session_str] = "expired"
        await update_session_worker_status_in_stats(phone_hint, session_str, "expired", name_from_tg, conn_logger.extra['request_id'])
    except Exception as e:
        conn_logger.error(f"🚨 Общая ошибка подключения для '{phone_hint}': {type(e).__name__} - {str(e)}", exc_info=True)
        client_status[session_str] = "error"
        await update_session_worker_status_in_stats(phone_hint, session_str, "error", name_from_tg, conn_logger.extra['request_id'])
    finally:
        if client_status.get(session_str) != "ok":
            if client.is_connected():
                try: await client.disconnect()
                except Exception: pass 
            if session_str in clients: del clients[session_str]

async def select_client_with_lock() -> Optional[Tuple[str, TelegramClient, asyncio.Lock, str, str]]:
    async with select_client_lock: 
        sys_logger = RequestIdAdapter(base_logger, {'request_id': 'SELECT_CLIENT'})
        now = datetime.datetime.now().timestamp()
        available_clients_s_str: List[str] = []
        
        current_statuses = dict(client_status) 
        stats_cache = app_state.get("current_stats", {})
        s2p_map = app_state.get("session_to_phone_map", {})

        skipped_reasons = {"cooldown":0,"missing_client_obj":0,"not_connected":0,"flood_wait_active":0,"error_status":0,"no_phone_mapping":0}
        
        for s_str, status_val in current_statuses.items():
            cooldown_end_ts = client_cooldown_end_times.get(s_str)
            if cooldown_end_ts and now < cooldown_end_ts:
                skipped_reasons["cooldown"] +=1
                continue

            phone_num_for_client = s2p_map.get(s_str)
            client_name_for_client = f"SID...{s_str[-6:]}" 
            if phone_num_for_client:
                detail = client_details.get(s_str)
                if detail and detail.get("name"):
                    client_name_for_client = detail["name"]
                else: 
                    client_name_for_client = stats_cache.get(phone_num_for_client, {}).get("name", f"Акк_{phone_num_for_client.replace('+','_')[-4:]}")
            else: 
                if status_val == "ok": 
                     sys_logger.error(f"Критично: Сессия ...{s_str[-5:]} имеет статус 'ok', но отсутствует в session_to_phone_map. Помечается 'error'.")
                     client_status[s_str] = "error" 
                skipped_reasons["no_phone_mapping"] +=1
                continue
            
            if status_val == "ok":
                if s_str in clients and clients[s_str].is_connected():
                    available_clients_s_str.append(s_str)
                else: 
                    sys_logger.warning(f"Клиент '{client_name_for_client}' (SID ...{s_str[-5:]}) статус 'ok', но объект отсутствует или не подключен. Помечается 'error'.")
                    client_status[s_str] = "error"
                    asyncio.create_task(update_session_worker_status_in_stats(phone_num_for_client, s_str, "error", client_name_for_client, 'SELECT_STALE_OK'))
                    skipped_reasons["not_connected" if s_str not in clients else "missing_client_obj"] +=1
            elif status_val.startswith("flood_wait_"):
                try:
                    flood_end_ts_from_status = int(status_val.split("_")[-1])
                    if now >= flood_end_ts_from_status: 
                        sys_logger.info(f"⏳ Flood wait для '{client_name_for_client}' (SID ...{s_str[-5:]}) закончился. Попытка восстановить статус 'ok'.")
                        client_status[s_str] = "ok" 
                        if s_str in clients and clients[s_str].is_connected():
                            available_clients_s_str.append(s_str)
                            asyncio.create_task(update_session_worker_status_in_stats(phone_num_for_client, s_str, "ok", client_name_for_client, 'SELECT_FLOOD_END_OK'))
                        else: 
                            sys_logger.warning(f"Flood wait для '{client_name_for_client}' (SID ...{s_str[-5:]}) закончился, но клиент не подключен/отсутствует. Помечается 'error'.")
                            client_status[s_str] = "error"
                            asyncio.create_task(update_session_worker_status_in_stats(phone_num_for_client, s_str, "error", client_name_for_client, 'SELECT_FLOOD_END_ERR'))
                            skipped_reasons["not_connected" if s_str not in clients else "missing_client_obj"] +=1
                    else: 
                        skipped_reasons["flood_wait_active"] +=1
                except ValueError: 
                    sys_logger.error(f"Ошибка парсинга flood_wait timestamp для '{client_name_for_client}' (SID ...{s_str[-5:]}): '{status_val}'. Помечается 'error'.")
                    client_status[s_str] = "error"
                    asyncio.create_task(update_session_worker_status_in_stats(phone_num_for_client, s_str, "error", client_name_for_client, 'SELECT_FLOOD_PARSE_ERR'))
                    skipped_reasons["error_status"] +=1
            else: 
                skipped_reasons["error_status"] +=1
        
        if not available_clients_s_str:
            sys_logger.warning(f"⚠️ Нет доступных клиентов для выбора. Всего сконфигурировано: {len(current_statuses)}. Пропущено по причинам: {skipped_reasons}.")
            return None

        current_idx = current_client_indices["index"]
        selected_session_str = available_clients_s_str[current_idx % len(available_clients_s_str)]
        current_client_indices["index"] = current_idx + 1
        
        selected_client_obj = clients[selected_session_str]
        selected_client_lock_obj = client_locks[selected_session_str] 
        
        selected_client_details = client_details.get(selected_session_str)
        if not selected_client_details: 
            sys_logger.error(f"КРИТИЧНО: Детали для выбранного клиента SID ...{selected_session_str[-5:]} не найдены, хотя он 'ok'! Помечается 'error'.")
            client_status[selected_session_str] = "error"
            phone_for_err_update = s2p_map.get(selected_session_str, "UnknownPhone")
            asyncio.create_task(update_session_worker_status_in_stats(phone_for_err_update, selected_session_str, "error", "UnknownNameDueToError", 'SELECT_CRIT_NO_DETAILS'))
            return None 

        selected_client_name = selected_client_details["name"]
        selected_client_phone = selected_client_details["phone"]
        
        sys_logger.info(f"Выбран клиент: '{selected_client_name}' ({selected_client_phone}, SID: ...{selected_session_str[-5:]})")
        return (selected_session_str, selected_client_obj, selected_client_lock_obj, selected_client_name, selected_client_phone)

async def cleanup_telegram_clients():
    sys_logger = RequestIdAdapter(base_logger, {'request_id': 'CLEANUP'})
    sys_logger.info("🧹 Очистка и отключение Telegram клиентов...")
    
    tasks = []
    client_keys = list(clients.keys()) 
    for s_str in client_keys:
        client_obj = clients.get(s_str)
        if client_obj and client_obj.is_connected():
            sys_logger.info(f"Отключение клиента SID ...{s_str[-5:]}")
            tasks.append(client_obj.disconnect())
        elif client_obj: 
             sys_logger.info(f"Клиент SID ...{s_str[-5:]} уже был отключен или не подключился.")
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                sys_logger.error(f"Ошибка при отключении клиента (индекс {i}): {res}")
    
    clients.clear()
    client_status.clear()
    client_cooldown_end_times.clear()
    client_details.clear() 
    current_client_indices["index"] = 0
    app_state["session_to_phone_map"].clear()
    app_state["clients_initialized"] = False 
    sys_logger.info("✅ Очистка клиентов завершена.")

@asynccontextmanager
async def lifespan(app: FastAPI):
    startup_logger = RequestIdAdapter(base_logger, {'request_id': 'STARTUP'})
    startup_logger.info(f"🚀 Запуск FastAPI приложения v{APP_VERSION}...")
    try:
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
        startup_logger.info(f"📂 Временная директория для скачиваний: '{os.path.abspath(TEMP_DOWNLOAD_DIR)}'")
    except Exception as e:
        startup_logger.critical(f"Не удалось создать временную директорию '{TEMP_DOWNLOAD_DIR}': {e}. Выход.", exc_info=True)
        exit(1)
    
    await initialize_telegram_clients()
    startup_logger.info("Инициализация Telegram клиентов на старте приложения завершена.")

    # --- Улучшенное возобновление задач ---
    tasks_db = await load_webhook_tasks_db()
    startup_logger.info(f"Загружено {len(tasks_db)} задач из {WEBHOOK_DB_FILE} для проверки возобновления.")
    
    pending_resumption_count = 0
    skipped_already_terminal_count = 0
    skipped_duplicate_idempotency_count = 0
    
    resumed_idempotency_keys = set() 
    tasks_to_update_db_on_startup = {} 

    terminal_statuses = [
        "completed", "failed", 
        "skipped_duplicate_on_restart", 
        "completed_no_webhook", "failed_no_webhook"
    ]

    for task_id, task_info in tasks_db.items():
        current_task_status = task_info.get("status", "unknown")
        original_url = task_info.get("original_url")
        metadata = task_info.get("metadata")
        client_request_id_from_meta = str(metadata.get("requestId")) if metadata and metadata.get("requestId") is not None else None

        if current_task_status in terminal_statuses:
            startup_logger.info(f"Задача {task_id} (URL: {original_url}) уже в терминальном статусе '{current_task_status}'. Пропуск возобновления.")
            skipped_already_terminal_count += 1
            continue

        can_resume_by_status = current_task_status in ["pending", "processing"] or \
                               current_task_status.startswith("waiting_for_client")
        
        if not can_resume_by_status:
            startup_logger.info(f"Задача {task_id} (URL: {original_url}) имеет статус '{current_task_status}', не подлежащий возобновлению. Пропуск.")
            continue

        idempotency_key_for_active_resume = (client_request_id_from_meta, original_url) \
            if client_request_id_from_meta and original_url else None

        if idempotency_key_for_active_resume and idempotency_key_for_active_resume in resumed_idempotency_keys:
            startup_logger.warning(
                f"Задача {task_id} (ClientReqId: {client_request_id_from_meta}, URL: {original_url}) "
                f"является дубликатом уже активно возобновляемой задачи с тем же ключом идемпотентности. "
                f"Помечается как 'skipped_duplicate_on_restart'."
            )
            task_info["status"] = "skipped_duplicate_on_restart"
            task_info["status_updated_at"] = datetime.datetime.now(pytz.utc).isoformat()
            task_info["error_details"] = "Пропущена при рестарте из-за дублирования с другой активной задачей по ключу (ClientReqId, URL)."
            tasks_to_update_db_on_startup[task_id] = task_info
            skipped_duplicate_idempotency_count += 1
            continue
        elif idempotency_key_for_active_resume:
            resumed_idempotency_keys.add(idempotency_key_for_active_resume)

        startup_logger.info(f"Возобновление задачи {task_id} (URL: {original_url}, текущий статус: '{current_task_status}')...")
        
        client_acquire_attempt = 1 
        if current_task_status.startswith("waiting_for_client"):
            match = re.search(r"attempt (\d+)", current_task_status)
            if match:
                try:
                    client_acquire_attempt = int(match.group(1))
                    startup_logger.info(f"Задача {task_id} была в ожидании клиента, возобновляем с попытки #{client_acquire_attempt}.")
                except ValueError:
                    startup_logger.warning(f"Не удалось извлечь номер попытки из статуса '{current_task_status}' для задачи {task_id}. Возобновляем с попытки #1.")
            else: 
                 startup_logger.warning(f"Статус задачи {task_id} '{current_task_status}' не содержит номера попытки. Возобновляем с попытки #1.")


        asyncio.create_task(process_file_and_send_webhook(
            original_url, 
            task_id, 
            metadata, 
            _client_acquire_attempt=client_acquire_attempt
        ))
        pending_resumption_count += 1
    
    if tasks_to_update_db_on_startup:
        tasks_db.update(tasks_to_update_db_on_startup) 
        await save_webhook_tasks_db(tasks_db) 
        startup_logger.info(f"{len(tasks_to_update_db_on_startup)} задач были обновлены в БД при старте (например, 'skipped_duplicate_on_restart').")

    startup_logger.info(
        f"Проверка возобновления задач завершена. "
        f"Активно возобновлено: {pending_resumption_count}. "
        f"Пропущено (уже в терминальном статусе): {skipped_already_terminal_count}. "
        f"Пропущено (дубликат по ключу идемпотентности): {skipped_duplicate_idempotency_count}."
    )
    # --- Конец улучшенного возобновления задач ---
    
    yield 

    shutdown_logger = RequestIdAdapter(base_logger, {'request_id': 'SHUTDOWN'})
    shutdown_logger.info("🛑 Остановка FastAPI приложения...")
    await cleanup_telegram_clients()
    
    try:
        shutdown_logger.info(f"🧹 Очистка временной директории '{TEMP_DOWNLOAD_DIR}'...")
        for item in os.listdir(TEMP_DOWNLOAD_DIR):
            item_path = os.path.join(TEMP_DOWNLOAD_DIR, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
            except Exception as e_remove:
                shutdown_logger.error(f"Ошибка удаления элемента '{item_path}': {e_remove}")
        shutdown_logger.info(f"Очистка '{TEMP_DOWNLOAD_DIR}' завершена.")
    except Exception as e_cleanup_dir:
        shutdown_logger.error(f"Ошибка при очистке директории '{TEMP_DOWNLOAD_DIR}': {e_cleanup_dir}", exc_info=True)
    
    shutdown_logger.info("✅ Приложение остановлено.")

app = FastAPI(lifespan=lifespan, title="Envato File Processor API", version=APP_VERSION)

class ProcessLinkWebhookRequest(BaseModel):
    url: HttpUrl
    webhook_url: HttpUrl
    metadata: Optional[Dict[str, Any]] = None

async def load_webhook_tasks_db() -> Dict:
    return await load_json_data_fastapi(WEBHOOK_DB_FILE, webhook_db_lock)

async def save_webhook_tasks_db(data: Dict):
    await save_json_data_fastapi(WEBHOOK_DB_FILE, data, webhook_db_lock)

async def _send_intermediate_webhook(task_id: str, webhook_url: str, payload: Dict, task_logger: RequestIdAdapter):
    task_logger.info(f"Задача {task_id}: Отправка промежуточного вебхука на {webhook_url}. Payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
    headers = {"User-Agent": DEFAULT_USER_AGENT, "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=WEBHOOK_SEND_TIMEOUT, headers=headers) as client:
            resp = await client.post(webhook_url, json=payload)
            if resp.is_success:
                task_logger.info(f"Задача {task_id}: Промежуточный вебхук успешно отправлен, статус: {resp.status_code}")
            else:
                task_logger.error(f"Задача {task_id}: Ошибка статуса ({resp.status_code}) при отправке промежуточного вебхука: {resp.text[:200]}")
    except Exception as e:
        task_logger.error(f"Задача {task_id}: Исключение при отправке промежуточного вебхука: {e}", exc_info=True)

async def process_file_and_send_webhook(
    original_url: str,
    task_id: str,
    client_metadata: Optional[Dict[str, Any]],
    _client_acquire_attempt: int = 1 
):
    task_logger = RequestIdAdapter(base_logger, {'request_id': task_id})
    if _client_acquire_attempt == 1:
        task_logger.info(f"🏁 ФОНОВАЯ ЗАДАЧА СТАРТ (Попытка получения клиента #{_client_acquire_attempt}/{MAX_CLIENT_ACQUIRE_RETRIES}): URL='{original_url}', Meta='{client_metadata}'")
    else:
        task_logger.info(f"🔄 Повторная попытка получения клиента #{_client_acquire_attempt}/{MAX_CLIENT_ACQUIRE_RETRIES} для задачи {task_id}, URL='{original_url}'")

    main_s3_url, main_orig_name, lic_s3_url, lic_orig_name = None, None, None, None
    err_msg_hook, tg_err_details = None, None 
    
    session_str, phone_num, acc_name = None, None, None
    lock_task: Optional[asyncio.Lock] = None 
    lock_acquired = False
    temp_files_to_clean: List[str] = []
    selected_client_info: Optional[Tuple[str, TelegramClient, asyncio.Lock, str, str]] = None

    try:
        selected_client_info = await select_client_with_lock()
        
        if not selected_client_info:
            if _client_acquire_attempt < MAX_CLIENT_ACQUIRE_RETRIES:
                task_logger.warning(f"Клиент не найден (попытка {_client_acquire_attempt} из {MAX_CLIENT_ACQUIRE_RETRIES}). Задача {task_id} будет повторена через {CLIENT_ACQUIRE_RETRY_DELAY} сек.")
                
                db_tasks_for_retry = await load_webhook_tasks_db()
                task_info_for_retry = db_tasks_for_retry.get(task_id)
                if task_info_for_retry:
                    task_info_for_retry["status"] = f"waiting_for_client (attempt {_client_acquire_attempt + 1})"
                    task_info_for_retry["status_updated_at"] = datetime.datetime.now(pytz.utc).isoformat()
                    
                    if task_info_for_retry.get("webhook_url"):
                        payload_for_retry_webhook = {
                            "task_id": task_id,
                            "original_url": task_info_for_retry.get("original_url", original_url),
                            "timestamp": datetime.datetime.now(pytz.utc).isoformat(),
                            "status": "retrying_no_client",
                            "message": f"Нет доступного Telegram аккаунта. Текущая попытка {_client_acquire_attempt} из {MAX_CLIENT_ACQUIRE_RETRIES}.",
                            "details": {
                                "current_attempt": _client_acquire_attempt,
                                "max_attempts": MAX_CLIENT_ACQUIRE_RETRIES,
                                "next_attempt_in_seconds": CLIENT_ACQUIRE_RETRY_DELAY
                            }
                        }
                        if "metadata" in task_info_for_retry: payload_for_retry_webhook["metadata"] = task_info_for_retry["metadata"]
                        await _send_intermediate_webhook(task_id, task_info_for_retry["webhook_url"], payload_for_retry_webhook, task_logger)
                    
                    db_tasks_for_retry[task_id] = task_info_for_retry
                    await save_webhook_tasks_db(db_tasks_for_retry)

                await asyncio.sleep(CLIENT_ACQUIRE_RETRY_DELAY)
                asyncio.create_task(process_file_and_send_webhook(original_url, task_id, client_metadata, _client_acquire_attempt + 1))
                return 
            else: 
                err_msg_hook = f"Не удалось найти доступный Telegram аккаунт для задачи {task_id} после {MAX_CLIENT_ACQUIRE_RETRIES} попыток."
                task_logger.error(err_msg_hook)
                tg_err_details = "NoClientAvailableAfterRetries"
                
                db_tasks_for_fail = await load_webhook_tasks_db()
                task_info_for_fail = db_tasks_for_fail.get(task_id)
                if task_info_for_fail:
                    task_info_for_fail["status"] = "failed"
                    task_info_for_fail["error_details"] = err_msg_hook
                    task_info_for_fail["error_type"] = tg_err_details
                    task_info_for_fail["completed_at"] = datetime.datetime.now(pytz.utc).isoformat()
                    db_tasks_for_fail[task_id] = task_info_for_fail
                    await save_webhook_tasks_db(db_tasks_for_fail)
                    task_logger.info(f"Задача {task_id} помечена 'failed' в БД (нет доступных клиентов).")
                raise Exception(err_msg_hook) 

        session_str, client_obj, lock_task, acc_name, phone_num = selected_client_info
        
        task_logger.info(f"Задача {task_id}: Выбран клиент '{acc_name}' ({phone_num}, SID: ...{session_str[-5:]}) для попытки #{_client_acquire_attempt}")

        db_tasks_for_processing = await load_webhook_tasks_db()
        task_info_for_processing = db_tasks_for_processing.get(task_id)
        if task_info_for_processing and task_info_for_processing.get("status") != "processing":
            task_info_for_processing["status"] = "processing"
            task_info_for_processing["processing_started_at"] = datetime.datetime.now(pytz.utc).isoformat()
            task_info_for_processing["processed_by_account"] = f"{acc_name} ({phone_num})"
            db_tasks_for_processing[task_id] = task_info_for_processing
            await save_webhook_tasks_db(db_tasks_for_processing)

        async with lock_task: 
            lock_acquired = True
            task_logger.info(f"Задача {task_id}: Блокировка для клиента '{acc_name}' получена.")

            current_client_s_status = client_status.get(session_str)
            if current_client_s_status != "ok":
                err_msg_hook = f"Клиент '{acc_name}' ({phone_num}) стал недоступен (статус: {current_client_s_status}) перед началом обработки."
                task_logger.warning(f"Задача {task_id}: {err_msg_hook}")
                tg_err_details = "ClientBecameUnavailable"
                raise Exception(err_msg_hook) 

            tg_result = await process_link_with_telegram(client_obj, original_url, acc_name, phone_num, task_id)

            if tg_result.get('error'):
                tg_err_details = tg_result.get('telegram_error_type', 'UnknownTGError')
                err_msg_hook = f"Ошибка Telegram ({tg_err_details}) при обработке ссылки для '{acc_name}': {tg_result['error']}"
                raise Exception(err_msg_hook) 

            main_url_from_bot = tg_result.get('main_url')
            lic_url_from_bot = tg_result.get('license_url')

            if not main_url_from_bot:
                err_msg_hook = "Не получен URL основного файла от Telegram-бота."
                tg_err_details = "MainUrlNotReceivedFromBot"
                raise Exception(err_msg_hook)

            main_download_result = await download_file(main_url_from_bot, task_id, "main")
            if main_download_result:
                temp_main_path, main_s3_name_val, main_orig_name_val = main_download_result
                temp_files_to_clean.append(temp_main_path)
                main_orig_name = main_orig_name_val 
                main_s3_url = await upload_file_via_api(temp_main_path, task_id, main_s3_name_val, main_orig_name_val)
            else:
                err_msg_hook = f"Не удалось скачать основной файл с URL: {main_url_from_bot}"
                tg_err_details = "MainFileDownloadFailed"
                raise Exception(err_msg_hook)
            
            if not main_s3_url: 
                err_msg_hook = f"Не удалось загрузить основной файл '{main_orig_name}' (скачанный с {main_url_from_bot}) на API."
                if not tg_err_details: tg_err_details = "MainFileUploadFailed"
                raise Exception(err_msg_hook)

            if lic_url_from_bot:
                lic_download_result = await download_file(lic_url_from_bot, task_id, "license")
                if lic_download_result:
                    temp_lic_path, lic_s3_name_val, lic_orig_name_val = lic_download_result
                    temp_files_to_clean.append(temp_lic_path)
                    lic_orig_name = lic_orig_name_val 
                    lic_s3_url = await upload_file_via_api(temp_lic_path, task_id, lic_s3_name_val, lic_orig_name_val)
                    if not lic_s3_url:
                        task_logger.warning(f"Задача {task_id}: Не удалось загрузить файл лицензии '{lic_orig_name}' на API (скачанный с {lic_url_from_bot}). Продолжаем без него.")
                else:
                    task_logger.warning(f"Задача {task_id}: Не удалось скачать файл лицензии (URL: {lic_url_from_bot}).")
            
            if session_str and phone_num and acc_name: 
                await update_stats_on_request(phone_num, acc_name, task_id)
            
            task_logger.info(f"Задача {task_id}: Успешная обработка файла для URL: {original_url}")

    except Exception as e:
        task_logger.critical(f"Задача {task_id}: Критическая ошибка в фоновой задаче (попытка клиента #{_client_acquire_attempt}/{MAX_CLIENT_ACQUIRE_RETRIES}): {type(e).__name__} - {str(e)}", exc_info=True)
        if not err_msg_hook: 
            err_msg_hook = f"Внутренняя ошибка сервера при обработке задачи: {str(e)}"
        if not tg_err_details: 
            tg_err_details = "ProcessingBackgroundTaskError"
        
        if session_str and phone_num and client_status.get(session_str) == "ok":
            non_client_fault_error_types = [
                "ProcessingBackgroundTaskError", "MainFileDownloadFailed", "MainFileUploadFailed",
                "NoClientAvailableAfterRetries", "ClientBecameUnavailable"
            ]
            telegram_handled_error_types = [
                "ButtonTimeoutError", "ButtonNotFoundError", "BotReportedError",
                "MainUrlMissingAfterLoop", "MainFileTimeoutError", "FloodWaitError",
                "AuthKeyError", "UserDeactivatedError", "SessionExpiredError",
                "PhoneNumberInvalidError", "PhoneCodeInvalidError", "ApiIdInvalidError",
                "UserAlreadyParticipantError", "UnhandledExceptionInTelegramLogic"
            ]
            if tg_err_details not in non_client_fault_error_types and tg_err_details not in telegram_handled_error_types:
                client_status[session_str] = "error"
                name_for_log = client_details.get(session_str, {}).get("name", f"SID...{session_str[-5:]}")
                await update_session_worker_status_in_stats(phone_num, session_str, "error", name_for_log, task_id)
                task_logger.warning(f"Задача {task_id}: Клиент '{acc_name or 'N/A'}' ({phone_num}) помечен 'error' из-за непредвиденной ошибки ({tg_err_details}).")
    finally:
        if lock_acquired and session_str and session_str in client_status: 
            async with select_client_lock: 
                cooldown_delay_seconds = random.randint(SESSION_REQUEST_DELAY_MIN, SESSION_REQUEST_DELAY_MAX)
                cooldown_ends_at_ts = datetime.datetime.now().timestamp() + cooldown_delay_seconds
                client_cooldown_end_times[session_str] = cooldown_ends_at_ts
                task_logger.info(f"Клиент '{acc_name or 'N/A'}' (SID: ...{session_str[-5:]}) установлен на кулдаун {cooldown_delay_seconds} сек (до {datetime.datetime.fromtimestamp(cooldown_ends_at_ts).isoformat()}). Защищено select_client_lock.")
        
        for f_path in temp_files_to_clean:
            try:
                os.remove(f_path)
                task_logger.info(f"Задача {task_id}: Удален временный файл: {f_path}")
            except Exception as e_clean:
                task_logger.error(f"Задача {task_id}: Ошибка удаления временного файла '{f_path}': {e_clean}")

        should_send_webhook = bool(err_msg_hook) or bool(main_s3_url) 
        if should_send_webhook:
            db_tasks_for_webhook = await load_webhook_tasks_db()
            task_info_from_db = db_tasks_for_webhook.get(task_id)

            if task_info_from_db and task_info_from_db.get("webhook_url"):
                target_webhook_url = task_info_from_db["webhook_url"]
                webhook_payload = {
                    "task_id": task_id,
                    "original_url": task_info_from_db.get("original_url", original_url),
                    "timestamp": datetime.datetime.now(pytz.utc).isoformat()
                }
                if "metadata" in task_info_from_db: webhook_payload["metadata"] = task_info_from_db["metadata"]

                if err_msg_hook: 
                    webhook_payload["status"] = "error"
                    webhook_payload["error_message"] = err_msg_hook
                    task_info_from_db["status"] = "failed" 
                    task_info_from_db["error_details"] = err_msg_hook
                else: 
                    webhook_payload["status"] = "success"
                    webhook_payload["main_file_url"] = main_s3_url
                    task_info_from_db["status"] = "completed" 
                
                if tg_err_details and err_msg_hook: 
                    webhook_payload["error_type"] = tg_err_details
                    task_info_from_db["error_type"] = tg_err_details
                
                if main_orig_name and not err_msg_hook: webhook_payload["main_file_original_name"] = main_orig_name
                if lic_s3_url and not err_msg_hook: webhook_payload["license_file_url"] = lic_s3_url
                if lic_orig_name and not err_msg_hook: webhook_payload["license_file_original_name"] = lic_orig_name
                
                task_info_from_db["completed_at"] = webhook_payload["timestamp"] 
                db_tasks_for_webhook[task_id] = task_info_from_db 
                await save_webhook_tasks_db(db_tasks_for_webhook) 

                task_logger.info(f"Задача {task_id}: Подготовка к отправке финального вебхука на {target_webhook_url}. Payload: {json.dumps(webhook_payload, ensure_ascii=False, indent=2)}")
                
                webhook_http_headers = {"User-Agent": DEFAULT_USER_AGENT, "Content-Type": "application/json"}
                for attempt in range(WEBHOOK_MAX_RETRIES + 1):
                    try:
                        async with httpx.AsyncClient(timeout=WEBHOOK_SEND_TIMEOUT, headers=webhook_http_headers) as client_wh:
                            response_wh = await client_wh.post(target_webhook_url, json=webhook_payload)
                        response_wh.raise_for_status() 
                        task_logger.info(f"Задача {task_id}: Финальный вебхук успешно отправлен (попытка {attempt+1}/{WEBHOOK_MAX_RETRIES+1}), статус: {response_wh.status_code}")
                        task_info_from_db["webhook_status"] = "sent"
                        break 
                    except httpx.HTTPStatusError as e_wh_http:
                        task_logger.error(f"Задача {task_id}: Ошибка статуса ({e_wh_http.response.status_code}) при отправке финального вебхука (попытка {attempt+1}/{WEBHOOK_MAX_RETRIES+1}): {e_wh_http.response.text[:200]}")
                        task_info_from_db["webhook_error"] = f"HTTP Status: {e_wh_http.response.status_code}, Response: {e_wh_http.response.text[:100]}"
                    except httpx.RequestError as e_wh_req: 
                        task_logger.error(f"Задача {task_id}: Ошибка сети при отправке финального вебхука (попытка {attempt+1}/{WEBHOOK_MAX_RETRIES+1}): {e_wh_req}")
                        task_info_from_db["webhook_error"] = f"Request Error: {str(e_wh_req)}"
                    except Exception as e_wh_gen: 
                        task_logger.error(f"Задача {task_id}: Общая ошибка при отправке финального вебхука (попытка {attempt+1}/{WEBHOOK_MAX_RETRIES+1}): {e_wh_gen}", exc_info=True)
                        task_info_from_db["webhook_error"] = f"Generic Error: {str(e_wh_gen)}"
                    
                    if attempt == WEBHOOK_MAX_RETRIES: 
                        task_info_from_db["webhook_status"] = "failed_after_retries"
                        task_logger.error(f"Задача {task_id}: Финальный вебхук не удалось отправить после {WEBHOOK_MAX_RETRIES+1} попыток.")
                    
                    if attempt < WEBHOOK_MAX_RETRIES:
                        delay_index = attempt 
                        sleep_duration = WEBHOOK_RETRY_DELAYS_LIST[min(delay_index, len(WEBHOOK_RETRY_DELAYS_LIST)-1)] if WEBHOOK_RETRY_DELAYS_LIST else 60
                        task_logger.info(f"Задача {task_id}: Ожидание {sleep_duration} сек перед повторной отправкой финального вебхука...")
                        await asyncio.sleep(sleep_duration)
                
                if task_info_from_db: 
                    task_info_from_db["webhook_last_attempt_at"] = datetime.datetime.now(pytz.utc).isoformat()
                    await save_webhook_tasks_db(db_tasks_for_webhook)

            elif task_info_from_db: 
                task_logger.info(f"Задача {task_id}: Webhook URL не указан в информации о задаче. Финальный вебхук не будет отправлен.")
                task_info_from_db["status"] = "completed_no_webhook" if not err_msg_hook else "failed_no_webhook"
                task_info_from_db["completed_at"] = datetime.datetime.now(pytz.utc).isoformat()
                await save_webhook_tasks_db(db_tasks_for_webhook)
            else: 
                task_logger.error(f"Задача {task_id}: Информация о задаче не найдена в БД ({WEBHOOK_DB_FILE}). Финальный вебхук не может быть отправлен.")
        
        task_logger.info(f"🏁 ФОНОВАЯ ЗАДАЧА ЗАВЕРШЕНА (Попытка клиента #{_client_acquire_attempt}/{MAX_CLIENT_ACQUIRE_RETRIES}): URL='{original_url}'. Результат: {'Успех' if not err_msg_hook else 'Ошибка: ' + err_msg_hook}")

@app.post("/files/v2/process-link", status_code=status.HTTP_202_ACCEPTED,
          summary="Асинхронная обработка ссылки с вебхуком",
          response_description="Запрос принят, вебхук будет отправлен по завершении.")
async def process_link_with_webhook_v2(
    req_data: ProcessLinkWebhookRequest,
    bg_tasks: BackgroundTasks,
    authorized: bool = Depends(verify_api_key) 
):
    client_request_id = str(req_data.metadata["requestId"]) if req_data.metadata and "requestId" in req_data.metadata else None
    api_log_id = f"req-{client_request_id}" if client_request_id else f"api-{uuid.uuid4().hex[:8]}"
    logger_ep = RequestIdAdapter(base_logger, {'request_id': api_log_id})

    logger_ep.info(f"Входящий запрос на /files/v2/process-link: URL='{req_data.url}', Webhook='{req_data.webhook_url}', Meta='{req_data.metadata}' (ClientReqId: {client_request_id or 'N/A'})")

    if not app_state["clients_initialized"]:
        logger_ep.warning("Сервис еще не полностью инициализирован (Telegram клиенты).")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Сервис инициализируется, попробуйте позже.")

    tasks_db = await load_webhook_tasks_db()

    if client_request_id:
        for existing_task_id, task_info in tasks_db.items():
            meta_db = task_info.get("metadata")
            url_db = task_info.get("original_url")
            if meta_db and str(meta_db.get("requestId")) == client_request_id and url_db == str(req_data.url):
                status_db = task_info.get("status", "unknown")
                webhook_status_db = task_info.get("webhook_status", "unknown")
                logger_ep.info(f"Найдена существующая задача {existing_task_id} для ClientReqId '{client_request_id}' и URL '{req_data.url}'. Статус: '{status_db}', статус вебхука: '{webhook_status_db}'.")
                
                if status_db == "completed" and webhook_status_db == "sent":
                    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Запрос уже был успешно обработан, и вебхук был отправлен.", "task_id": existing_task_id, "status": status_db})
                elif status_db == "completed_no_webhook":
                     return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Запрос уже был успешно обработан (вебхук не требовался или не был отправлен).", "task_id": existing_task_id, "status": status_db})
                elif status_db in ["pending", "processing"] or status_db.startswith("waiting_for_client"):
                    return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content={"message": "Запрос уже находится в обработке.", "task_id": existing_task_id})
                elif status_db == "failed" or status_db == "failed_no_webhook" or webhook_status_db == "failed_after_retries":
                    logger_ep.warning(f"Предыдущая задача {existing_task_id} для ClientReqId '{client_request_id}' и URL '{req_data.url}' завершилась с ошибкой. Будет создана новая задача.")
                    break 
                else: 
                    logger_ep.info(f"Задача {existing_task_id} для ClientReqId '{client_request_id}' и URL '{req_data.url}' имеет статус '{status_db}'. Будет создана новая задача.")
                    break
    
    app_internal_task_id = uuid.uuid4().hex[:12] 
    task_entry_data = {
        "original_url": str(req_data.url),
        "webhook_url": str(req_data.webhook_url),
        "status": "pending", 
        "added_at": datetime.datetime.now(pytz.utc).isoformat(),
        "webhook_status": "pending_send" 
    }
    if req_data.metadata:
        task_entry_data["metadata"] = req_data.metadata
    
    tasks_db[app_internal_task_id] = task_entry_data
    await save_webhook_tasks_db(tasks_db)
    
    logger_ep.info(f"Задача {app_internal_task_id} (ClientReqId: {client_request_id or 'N/A'}) добавлена в очередь. Webhook будет отправлен на: '{req_data.webhook_url}'.")
    
    bg_tasks.add_task(process_file_and_send_webhook, str(req_data.url), app_internal_task_id, req_data.metadata)
    
    return {"message": "Запрос принят в обработку. По завершении будет отправлен вебхук.", "task_id": app_internal_task_id}

@app.get("/files/get-link", deprecated=True,
         summary="Синхронная обработка ссылки (устаревший метод)",
         response_description="Прямой ответ с URL файлов или ошибка.")
async def get_file_link_synchronous(
    url: str = Query(..., description="URL для отправки Telegram боту"),
    redirect: bool = Query(False, description="Если True, выполнит редирект на URL основного файла вместо JSON ответа"),
    authorized: bool = Depends(verify_api_key)
):
    req_id_sync = f"sync-{uuid.uuid4().hex[:8]}"
    logger_sync = RequestIdAdapter(base_logger, {'request_id': req_id_sync})
    logger_sync.info(f"🌐 СИНХРОННЫЙ запрос /files/get-link: URL='{url[:100]}...', Redirect={redirect}")

    temp_files_registry_sync: List[str] = []
    session_str_sync, phone_sync, acc_name_sync = None, None, None
    lock_acquired_sync = False
    selected_client_info_sync: Optional[Tuple[str, TelegramClient, asyncio.Lock, str, str]] = None

    try:
        if not app_state["clients_initialized"]:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="🔧 Сервис инициализируется, попробуйте позже.")

        selected_client_info_sync = await select_client_with_lock()
        if not selected_client_info_sync:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="⚠️ Нет доступных Telegram аккаунтов в данный момент.")
        
        session_str_sync, client_obj_sync, lock_obj_sync, acc_name_sync, phone_sync = selected_client_info_sync
        
        logger_sync.info(f"Попытка захвата блокировки для клиента '{acc_name_sync}' ({phone_sync})")
        async with lock_obj_sync:
            lock_acquired_sync = True
            logger_sync.info(f"🔒 Блокировка для клиента '{acc_name_sync}' ({phone_sync}) получена.")

            current_status_sync = client_status.get(session_str_sync)
            if current_status_sync != "ok":
                logger_sync.warning(f"Статус клиента '{acc_name_sync}' ({phone_sync}) изменился на '{current_status_sync}' после захвата блокировки!")
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"Выбранный клиент '{acc_name_sync}' стал недоступен (статус: {current_status_sync}).")

            tg_result_sync = await process_link_with_telegram(client_obj_sync, url, acc_name_sync, phone_sync, req_id_sync)

            if tg_result_sync.get('error'):
                tg_error_type_sync = tg_result_sync.get('telegram_error_type', 'UnknownTGError')
                raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"❌ Ошибка Telegram ({tg_error_type_sync}): {tg_result_sync['error']}")

            main_url_from_bot_sync = tg_result_sync.get('main_url')
            lic_url_from_bot_sync = tg_result_sync.get('license_url')

            if not main_url_from_bot_sync:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="❌ Не получен URL основного файла от Telegram-бота.")

            main_download_info_sync, lic_download_info_sync = None, None

            main_dl_sync_tuple = await download_file(main_url_from_bot_sync, req_id_sync, "main_sync")
            if main_dl_sync_tuple:
                temp_files_registry_sync.append(main_dl_sync_tuple[0])
                main_download_info_sync = main_dl_sync_tuple 
            else:
                raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=f"❌ Не удалось скачать основной файл с {main_url_from_bot_sync}.")

            if lic_url_from_bot_sync:
                lic_dl_sync_tuple = await download_file(lic_url_from_bot_sync, req_id_sync, "license_sync")
                if lic_dl_sync_tuple:
                    temp_files_registry_sync.append(lic_dl_sync_tuple[0])
                    lic_download_info_sync = lic_dl_sync_tuple
                else:
                    logger_sync.warning(f"⚠️ Не удалось скачать файл лицензии (URL: {lic_url_from_bot_sync}).")
            
            response_data_sync = {}
            if not main_download_info_sync: 
                 raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка: информация об основном файле отсутствует после скачивания.")

            main_s3_url_sync = await upload_file_via_api(main_download_info_sync[0], req_id_sync, main_download_info_sync[1], main_download_info_sync[2])
            if not main_s3_url_sync:
                raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"❌ Не удалось загрузить основной файл '{main_download_info_sync[2]}' на API.")
            response_data_sync['main_file_url'] = main_s3_url_sync
            response_data_sync['main_file_original_name'] = main_download_info_sync[2]

            if lic_download_info_sync:
                lic_s3_url_sync = await upload_file_via_api(lic_download_info_sync[0], req_id_sync, lic_download_info_sync[1], lic_download_info_sync[2])
                if lic_s3_url_sync:
                    response_data_sync['license_file_url'] = lic_s3_url_sync
                    response_data_sync['license_file_original_name'] = lic_download_info_sync[2]
                else:
                    logger_sync.warning(f"⚠️ Не удалось загрузить файл лицензии '{lic_download_info_sync[2]}' на API.")
            
            if phone_sync and acc_name_sync: 
                await update_stats_on_request(phone_sync, acc_name_sync, req_id_sync)

            logger_sync.info(f"✅ Синхронный запрос успешно обработан. Результат: {response_data_sync}")
            
            if redirect and 'main_file_url' in response_data_sync:
                return RedirectResponse(response_data_sync['main_file_url'])
            return JSONResponse(content=response_data_sync)

    except HTTPException as he_sync:
        logger_sync.error(f"HTTP ошибка в синхронном запросе {he_sync.status_code}: {he_sync.detail}")
        raise he_sync 
    except Exception as e_sync:
        logger_sync.critical(f"🚨 Необработанная ошибка в синхронном запросе: {type(e_sync).__name__} - {str(e_sync)}", exc_info=True)
        if session_str_sync and phone_sync and client_status.get(session_str_sync) == "ok":
            client_status[session_str_sync] = "error"
            name_for_log_sync = client_details.get(session_str_sync, {}).get("name", f"SID...{session_str_sync[-5:]}")
            asyncio.create_task(update_session_worker_status_in_stats(phone_sync, session_str_sync, "error", name_for_log_sync, req_id_sync))
            logger_sync.warning(f"Клиент '{acc_name_sync}' ({phone_sync}) помечен 'error' из-за необработанной ошибки в синхронном запросе.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="🚨 Внутренняя ошибка сервера при обработке вашего запроса.")
    finally:
        logger_sync.info(f"🧹 Блок finally для синхронного запроса {req_id_sync}...")
        if lock_acquired_sync and session_str_sync and acc_name_sync: 
            async with select_client_lock: 
                delay_sync = random.randint(SESSION_REQUEST_DELAY_MIN, SESSION_REQUEST_DELAY_MAX)
                cooldown_sync_ts = datetime.datetime.now().timestamp() + delay_sync
                client_cooldown_end_times[session_str_sync] = cooldown_sync_ts
                logger_sync.info(f"Клиент '{acc_name_sync}' (SID: ...{session_str_sync[-5:]}) установлен на кулдаун {delay_sync} сек (до {datetime.datetime.fromtimestamp(cooldown_sync_ts).isoformat()}). Защищено select_client_lock.")
        
        for f_path_sync in temp_files_registry_sync:
            try:
                os.remove(f_path_sync)
                logger_sync.info(f"Удален временный файл: {f_path_sync}")
            except Exception as e_clean_sync:
                logger_sync.error(f"❌ Ошибка удаления временного файла '{f_path_sync}': {e_clean_sync}")
        logger_sync.info(f"🏁 Синхронный запрос {req_id_sync} завершен.")


@app.get("/health", summary="Проверка состояния сервиса и Telegram-клиентов")
async def health_check(authorized: bool = Depends(verify_api_key)):
    logger_health = RequestIdAdapter(base_logger, {'request_id': 'HEALTH_CHECK'})
    now = datetime.datetime.now().timestamp()
    
    active_clients_count = 0
    cooldown_clients_info: List[Dict] = []
    flood_wait_clients_info: List[Dict] = []
    error_clients_info: List[Dict] = []
    
    current_client_statuses_copy = dict(client_status)
    s2p_map_cache_health = app_state.get("session_to_phone_map", {})
    
    tasks_db_for_health = await load_webhook_tasks_db()
    waiting_for_client_tasks = {
        task_id: task_data for task_id, task_data in tasks_db_for_health.items()
        if isinstance(task_data.get("status"), str) and task_data["status"].startswith("waiting_for_client")
    }

    detailed_client_statuses_report: Dict[str, str] = {}

    for s_str, status_val in current_client_statuses_copy.items():
        client_detail_entry = client_details.get(s_str)
        phone_val_health = "N/A"
        acc_name_val_health = f"SID...{s_str[-6:]}" 

        if client_detail_entry:
            phone_val_health = client_detail_entry.get("phone", "N/A")
            acc_name_val_health = client_detail_entry.get("name", acc_name_val_health)
        elif s_str in s2p_map_cache_health: 
            phone_val_health = s2p_map_cache_health[s_str]
            stats_entry_health = app_state.get("current_stats", {}).get(phone_val_health, {})
            acc_name_val_health = stats_entry_health.get("name", f"Акк_{phone_val_health.replace('+','_')[-4:]}")
        
        display_name_for_report = f"{acc_name_val_health} (тел: {phone_val_health}, SID: ...{s_str[-6:]})"
        current_display_status = status_val

        cooldown_end_ts_health = client_cooldown_end_times.get(s_str)
        if cooldown_end_ts_health and now < cooldown_end_ts_health:
            remaining_cooldown_sec = int(cooldown_end_ts_health - now)
            cooldown_clients_info.append({"name": display_name_for_report, "remaining_sec": remaining_cooldown_sec})
            current_display_status = f"cooldown (~{remaining_cooldown_sec} сек)"
            detailed_client_statuses_report[display_name_for_report] = current_display_status
            continue 

        if status_val == "ok":
            if s_str in clients and clients[s_str].is_connected():
                active_clients_count += 1
            else: 
                error_clients_info.append({"name": display_name_for_report, "status": "error (stale 'ok' - not connected or missing)"})
                current_display_status = "error (stale 'ok')"
                if client_status.get(s_str) == "ok": 
                    client_status[s_str] = "error"
                    asyncio.create_task(update_session_worker_status_in_stats(phone_val_health, s_str, "error", acc_name_val_health, 'HEALTH_STALE_OK_CORRECTION'))
        elif status_val.startswith("flood_wait_"):
            try:
                flood_end_ts_from_status_health = int(status_val.split("_")[-1])
                remaining_flood_sec = flood_end_ts_from_status_health - now
                if remaining_flood_sec > 0:
                    flood_wait_clients_info.append({"name": display_name_for_report, "remaining_sec": int(remaining_flood_sec)})
                    current_display_status = f"flood_wait (~{int(remaining_flood_sec)} сек)"
                else: 
                    current_display_status = "ok (flood_wait истек)"
                    if s_str in clients and clients[s_str].is_connected():
                        active_clients_count += 1
                        if client_status.get(s_str, "").startswith("flood_wait"): 
                            client_status[s_str] = "ok"
                            asyncio.create_task(update_session_worker_status_in_stats(phone_val_health, s_str, "ok", acc_name_val_health, 'HEALTH_FLOOD_END_CORRECTION_OK'))
                    else: 
                        error_clients_info.append({"name": display_name_for_report, "status": "error (stale flood_wait - not connected)"})
                        current_display_status = "error (stale flood_wait)"
                        if client_status.get(s_str, "").startswith("flood_wait"): 
                            client_status[s_str] = "error"
                            asyncio.create_task(update_session_worker_status_in_stats(phone_val_health, s_str, "error", acc_name_val_health, 'HEALTH_FLOOD_END_CORRECTION_ERR'))
            except ValueError: 
                error_clients_info.append({"name": display_name_for_report, "status": f"error (ошибка парсинга flood_wait: '{status_val}')"})
                current_display_status = "error (flood parse error)"
                if client_status.get(s_str, "").startswith("flood_wait"): 
                    client_status[s_str] = "error"
                    asyncio.create_task(update_session_worker_status_in_stats(phone_val_health, s_str, "error", acc_name_val_health, 'HEALTH_FLOOD_PARSE_ERR_CORRECTION'))
        else: 
            error_clients_info.append({"name": display_name_for_report, "status": status_val})
        
        detailed_client_statuses_report[display_name_for_report] = current_display_status

    total_configured_clients = len(client_status) 
    service_overall_status = "ok"
    service_message = "Сервис работает нормально."

    if active_clients_count == 0:
        if total_configured_clients > 0:
            service_overall_status = "warning"
            service_message = "Внимание: Нет активных Telegram аккаунтов, но аккаунты сконфигурированы."
        else:
            service_overall_status = "error"
            service_message = "Критично: Нет сконфигурированных Telegram аккаунтов."
    elif len(error_clients_info) > 0 :
        service_overall_status = "warning"
        service_message = f"Сервис работает, но есть {len(error_clients_info)} аккаунт(ов) с ошибками."


    status_response_payload = {
        "app_version": APP_VERSION,
        "service_status": service_overall_status,
        "message": service_message,
        "active_clients": active_clients_count,
        "cooldown_clients_count": len(cooldown_clients_info),
        "flood_wait_clients_count": len(flood_wait_clients_info),
        "error_clients_count": len(error_clients_info),
        "tasks_waiting_for_client": len(waiting_for_client_tasks),
        "total_configured_clients": total_configured_clients,
        "client_statuses_detailed": detailed_client_statuses_report,
        "config_files": {
            "sessions_file": SESSIONS_FILE_PATH,
            "stats_file": STATS_FILE_PATH,
            "webhook_tasks_file": WEBHOOK_DB_FILE,
        },
        "upload_api_configured": bool(UPLOAD_API_ENDPOINT and UPLOAD_API_KEY)
    }
    if waiting_for_client_tasks:
        status_response_payload["waiting_tasks_details"] = {
            tid: {"original_url": t_info.get("original_url"), "status": t_info.get("status")}
            for tid, t_info in waiting_for_client_tasks.items()
        }

    logger_health.info(f"🩺 Health Check: v{APP_VERSION}, Status={service_overall_status}, Active={active_clients_count}, Cooldown={len(cooldown_clients_info)}, Flood={len(flood_wait_clients_info)}, Errors={len(error_clients_info)}, WaitingTasks={len(waiting_for_client_tasks)}")
    return status_response_payload

@app.get("/logs/download", summary="Скачать файл логов приложения", response_class=FileResponse)
async def download_logs(authorized: bool = Depends(verify_api_key)):
    log_file_to_serve = LOG_FILE_PATH_FOR_DOWNLOAD
    if not os.path.exists(log_file_to_serve):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Файл логов '{log_file_to_serve}' не найден.")
    
    download_log_filename = f"fastapi_app_logs_{os.path.basename(LOG_FILE_PATH_FOR_DOWNLOAD)}"
    return FileResponse(path=log_file_to_serve, filename=download_log_filename, media_type='text/plain')

if __name__ == "__main__":
    import uvicorn
    logger.info(f"Запуск Uvicorn сервера на {FASTAPI_HOST}:{FASTAPI_PORT} (reload: {RELOAD_FASTAPI})...")
    
    app_module_name = os.path.splitext(os.path.basename(__file__))[0]
    
    uvicorn.run(
        f"{app_module_name}:app", 
        host=FASTAPI_HOST,
        port=FASTAPI_PORT,
        log_config=None, 
        reload=RELOAD_FASTAPI
    )