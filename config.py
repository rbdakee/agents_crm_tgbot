import os
from dotenv import load_dotenv
load_dotenv()


# Конфигурация бота
BOT_TOKEN = os.getenv('BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')
BOT_USERNAME = os.getenv('BOT_USERNAME', 'YOUR_BOT_USERNAME_HERE')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL', 'https://n8n.ivitrina.kz/webhook/sheet')
AGENTS_FILE = 'data/agents.csv'

# Настройки пагинации
CONTRACTS_PER_PAGE = 10

# Настройки логирования
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# Webhook configuration
USE_WEBHOOK = os.getenv('USE_WEBHOOK', 'false').lower() == 'true'
WEBHOOK_URL = os.getenv('WEBHOOK_URL', '')  # e.g., https://your.domain.tld/bot
WEBAPP_HOST = os.getenv('WEBAPP_HOST', '0.0.0.0')
WEBAPP_PORT = 8080 if os.getenv('WEBAPP_PORT') == '' else int(os.getenv('WEBAPP_PORT', '8080'))
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', f"/{BOT_TOKEN}")