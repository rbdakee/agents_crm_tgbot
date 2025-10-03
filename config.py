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

# Внешние сервисы
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://dm.jurta.kz/open-api')
DEVICE_UUID = os.getenv('DEVICE_UUID')

# Файлы и пути
AGENTS_FILE = os.getenv('AGENTS_FILE', 'data/agents.csv')

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

# Таймауты и лимиты
REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))