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
