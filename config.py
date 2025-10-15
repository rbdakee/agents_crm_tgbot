import os
from dotenv import load_dotenv
load_dotenv()


# Конфигурация бота
BOT_TOKEN = os.getenv('BOT_TOKEN')
BOT_USERNAME = os.getenv('BOT_USERNAME')
AGENTS_FILE = 'data/agents.csv'

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

# PostgreSQL Database
DATABASE_URL = os.getenv('DATABASE_URL')  # URL подключения к PostgreSQL
if not DATABASE_URL:
    # Формируем URL из отдельных параметров
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'agents_crm')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    # Используем SQLite для быстрого тестирования
    # DATABASE_URL = "sqlite+aiosqlite:///./agents_crm.db"
    # Для PostgreSQL раскомментируйте строку ниже:
    DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Google Sheets Credentials
# Учетные данные загружаются из файла credentials.json

# Файлы и пути
# Единый источник AGENTS_FILE (без дублирования)
AGENTS_FILE = os.getenv('AGENTS_FILE', AGENTS_FILE)

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

# Таймауты и лимиты (не используются напрямую) — можно вернуть при необходимости

# Настройки синхронизации
SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', '5'))  # Интервал синхронизации в минутах
SYNC_ENABLED = os.getenv('SYNC_ENABLED', 'true').lower() == 'true'  # Включить/выключить синхронизацию