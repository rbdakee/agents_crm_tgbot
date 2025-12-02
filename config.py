import os
from typing import List
from dotenv import load_dotenv
load_dotenv()


# Конфигурация бота
BOT_TOKEN = os.getenv('BOT_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')

# Проверяем обязательные переменные
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

if not BOT_USERNAME:
    raise ValueError("BOT_USERNAME не установлен в переменных окружения")

# Внешние сервисы (n8n не используется в текущем коде) — удалено
API_BASE_URL = os.getenv('API_BASE_URL', 'https://dm.jurta.kz/open-api')
DEVICE_UUID = os.getenv('DEVICE_UUID')

# Авторизация (Keycloak)
AUTH_TOKEN_URL = os.getenv('AUTH_TOKEN_URL', 'https://idp.jurta.kz/auth/realms/htc/protocol/openid-connect/token')
AUTH_CLIENT_ID = os.getenv('AUTH_CLIENT_ID', 'htc')
PROFILE_URL = os.getenv('PROFILE_URL', 'https://um.jurta.kz/api/profile')

# Google Sheets API
SHEET_ID = os.getenv('SHEET_ID')  # ID Google Spreadsheet
FIRST_SHEET_GID = os.getenv('FIRST_SHEET_GID')  # GID первого листа (SHEET_DEALS - только чтение)
SECOND_SHEET_GID = os.getenv('SECOND_SHEET_GID')  # GID второго листа (SHEET_PROGRESS - можно изменять)
THIRD_SHEET_GID = os.getenv('THIRD_SHEET_GID')  # GID третьего листа ("Лист8")

# Лист с телефонами агентов (A: ФИО, B: Контакты)
AGENTS_PHONES_SHEET_GID = os.getenv('AGENTS_PHONES_SHEET_GID')

# Лист для отчета по холодным звонкам (общая и покомпонентная статистика)
AGENTS_COOL_CALLS_GID = os.getenv('AGENTS_COOL_CALLS_GID')

# PostgreSQL Database
DATABASE_URL = os.getenv('DATABASE_URL')  # URL подключения к PostgreSQL
if not DATABASE_URL:
    # Формируем URL из отдельных параметров
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    if DB_HOST and DB_PORT and DB_NAME and DB_USER:
        DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Google Sheets Credentials
# Учетные данные загружаются из файла credentials.json

# Настройки приложения
CONTRACTS_PER_PAGE = int(os.getenv('CONTRACTS_PER_PAGE', '10'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Webhook configuration
USE_WEBHOOK = os.getenv('USE_WEBHOOK', 'false').lower() == 'true'
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')
WEBAPP_HOST = os.getenv('WEBAPP_HOST', '0.0.0.0')
WEBAPP_PORT = 8080 if os.getenv('WEBAPP_PORT') == '' else int(os.getenv('WEBAPP_PORT', '8080'))
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', f"/{BOT_TOKEN}")

# Health check
HEALTH_CHECK_PORT = int(os.getenv('HEALTH_CHECK_PORT', '8081'))

# Настройки синхронизации
SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', '5'))  # Интервал синхронизации в минутах
SYNC_ENABLED = os.getenv('SYNC_ENABLED', 'true').lower() == 'true'  # Включить/выключить синхронизацию
CRM_API_ENRICHMENT = os.getenv('CRM_API_ENRICHMENT', 'true').lower() == 'true'  # Включить/выключить обогащение данными из CRM API

# Роли и доступы
# Комма-разделенный список 10-значных телефонов (без +7/8), которым назначается роль VIEW (только просмотр)
_ADMIN_VIEW_PHONES_ENV = os.getenv('ADMIN_VIEW_PHONES', '').strip()
ADMIN_VIEW_PHONES = set(
    p.strip() for p in _ADMIN_VIEW_PHONES_ENV.split(',') if p.strip()
)

# Авторизованный пользователь для административных команд
_AUTHORIZED_USER_ID_ENV = os.getenv('AUTHORIZED_USER_ID', '').strip()
if not _AUTHORIZED_USER_ID_ENV:
    raise ValueError("AUTHORIZED_USER_ID не установлен в переменных окружения")
try:
    AUTHORIZED_USER_ID = int(_AUTHORIZED_USER_ID_ENV)
except ValueError:
    raise ValueError(f"AUTHORIZED_USER_ID должен быть числом, получено: '{_AUTHORIZED_USER_ID_ENV}'")

# Настройки пагинации
PARSED_OBJECTS_PER_PAGE = int(os.getenv('PARSED_OBJECTS_PER_PAGE', '10'))
MOPS_PER_PAGE = int(os.getenv('MOPS_PER_PAGE', '10'))
ROPS_PER_PAGE = int(os.getenv('ROPS_PER_PAGE', '10'))
BULK_ASSIGN_COUNT = int(os.getenv('BULK_ASSIGN_COUNT', '10'))

# Настройки RBD парсера
RBD_MAX_DUPLICATES = int(os.getenv('RBD_MAX_DUPLICATES', '30'))
RBD_START_PAGE = int(os.getenv('RBD_START_PAGE', '1'))
RBD_EMAIL = os.getenv('EMAIL_RBD')
RBD_PASSWORD = os.getenv('PASSWORD_RBD')

# Настройки архивации
ARCHIVE_CONCURRENCY = int(os.getenv('ARCHIVE_CONCURRENCY', '10'))
ARCHIVE_TIMEOUT = float(os.getenv('ARCHIVE_TIMEOUT', '15.0'))
ARCHIVE_HTTP_TIMEOUT = float(os.getenv('ARCHIVE_HTTP_TIMEOUT', '20.0'))

# Настройки уведомлений о перезвоне
RECALL_CHECK_INTERVAL_SECONDS = int(os.getenv('RECALL_CHECK_INTERVAL_SECONDS', '60'))
RECALL_MAX_AGE_HOURS = int(os.getenv('RECALL_MAX_AGE_HOURS', '24'))  # Максимальный возраст просроченного уведомления

# Настройки батчинга для БД
DB_BATCH_SIZE = int(os.getenv('DB_BATCH_SIZE', '150'))
DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '10'))
DB_MAX_OVERFLOW = int(os.getenv('DB_MAX_OVERFLOW', '20'))
DB_POOL_RECYCLE = int(os.getenv('DB_POOL_RECYCLE', '3600'))

# Настройки таймаутов HTTP
HTTP_TIMEOUT = float(os.getenv('HTTP_TIMEOUT', '30.0'))

# RBD.kz настройки
RBD_BASE_URL = os.getenv('RBD_BASE_URL', 'https://rbd.kz')
RBD_API_BASE_URL = os.getenv('RBD_API_BASE_URL', f'{RBD_BASE_URL}/backend/app/rest')
RBD_SUPPLY_SEARCH_URL = f'{RBD_API_BASE_URL}/supply/search/list'
RBD_LOGIN_URL = f'{RBD_API_BASE_URL}/auth/login'
RBD_USER_AGENT = os.getenv('RBD_USER_AGENT', (
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/142.0.0.0 Mobile Safari/537.36"
))

# Krisha.kz настройки
KRISHA_BASE_URL = os.getenv('KRISHA_BASE_URL', 'https://m.krisha.kz')
KRISHA_URL_TEMPLATE = f'{KRISHA_BASE_URL}/a/show/{{krisha_id}}'
KRISHA_USER_AGENT = os.getenv('KRISHA_USER_AGENT', RBD_USER_AGENT)

# RBD RAW_DATA для запросов (можно вынести в отдельный файл при необходимости)
RBD_RAW_DATA_JSON = os.getenv('RBD_RAW_DATA_JSON', (
    '{"type":"databox","value":{"filter":{"type":"databox","value":{"address":{"type":"list","value":[]},'
    '"complexOrBCenterAddress":{"type":"list","value":[]},"district":{"type":"list","value":[]},'
    '"tags":{"type":"list","value":[]},"demand":{"type":"databox","value":{"agency":{"type":"number","value":57625},'
    '"usr":{"type":"number","value":69577},"searchMine":{"type":"boolean","value":false},'
    '"searchArchive":{"type":"boolean","value":false},"searchAgency":{"type":"boolean","value":false},'
    '"searchGlobal":{"type":"boolean","value":true},"searchOther":{"type":"boolean","value":false},'
    '"operationType":{"type":"number","value":0},"objectType":{"type":"number","value":1},'
    '"supplySource":{"type":"number","value":2},"areaLandMeasure":{"type":"number","value":2},'
    '"city":{"type":"number","value":1},"sell":{"type":"boolean","value":true},'
    '"sellCurrency":{"type":"number","value":1},"leaseCurrency":{"type":"number","value":1},'
    '"hash":{"type":"string","value":"438EF7CDE67CF0566D1B282D58460F70"},"sortType":{"type":"number","value":1},'
    '"viewType":{"type":"number","value":3},"clientType":{"type":"number","value":1},'
    '"dtSearch":{"type":"string","value":"2025-11-25T12:03:57.000+05:00"},"saved":{"type":"boolean","value":false},'
    '"sharedWithMe":{"type":"boolean","value":false},"usr__text":{"type":"string","value":""},'
    '"objectType__text":{"type":"string","value":"Квартира"},"supplySource__text":{"type":"string","value":"От собственника"},'
    '"areaLandMeasure__text":{"type":"string","value":"Сотка"},"city__text":{"type":"string","value":"Астана"},'
    '"sellCurrency__text":{"type":"string","value":"тенге"},"leaseCurrency__text":{"type":"string","value":"тенге"},'
    '"sortType__text":{"type":"string","value":"По дате изменения (сначала новые)"},"viewType__text":{"type":"string","value":"Таблица"},'
    '"clientType__text":{"type":"string","value":"Холодный"}}},"discussion":{"type":"databox","value":{}}}},'
    '"pageNum":{"type":"number","value":1},"filterChanged":{"type":"boolean","value":false},"external":{"type":"number","value":1}}}'
))

# ДД пользователи (имя -> телефон 10 цифр)
DD_ALLOWED = {
    'Мирасхан': '7055471077',
    'Рустам': '7752152555',
    'Айжан': '7058155000',
    'Айнамкоз': '7477777719',
    'Бегзат': '7757511212',
}

# Обратное отображение: телефон (10 цифр) -> имя ДД
PHONE_TO_DD_NAME = {v: k for k, v in DD_ALLOWED.items()}

# Поддержка
SUPPORT_USERNAME = os.getenv('SUPPORT_USERNAME', '')
SUPPORT_URL = f"https://t.me/{SUPPORT_USERNAME}"

# Настройки автоматических задач (время в формате HH:MM, часовой пояс Asia/Almaty)
AUTO_TASKS_TIME = os.getenv('AUTO_TASKS_TIME', '02:00')  # Время запуска автоматических задач (get_new_objects и archive)

# Список классов недвижимости (динамически загружается из БД)
PROPERTY_CLASSES: List[str] = []

async def refresh_property_classes():
    """Обновляет список классов недвижимости из БД"""
    global PROPERTY_CLASSES
    try:
        # Импортируем здесь, чтобы избежать циклических зависимостей
        from database_postgres import get_db_manager
        import logging
        
        db_manager = await get_db_manager()
        classes = await db_manager.get_distinct_property_classes()
        PROPERTY_CLASSES = classes
        logging.info(f"Загружены классы недвижимости: {PROPERTY_CLASSES}")
    except Exception as e:
        import logging
        logging.error(f"Ошибка загрузки классов недвижимости: {e}")
        # Fallback на дефолтный список
        PROPERTY_CLASSES = ['Эконом', 'Комфорт lite', 'Комфорт+', 'Бизнес', 'Бизнес+', 'Премиум', 'Элит']