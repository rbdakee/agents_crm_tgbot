import logging
import sys
import signal
from telegram.ext import Application

from config import BOT_TOKEN, USE_WEBHOOK, WEBHOOK_URL, WEBAPP_HOST, WEBAPP_PORT, WEBHOOK_PATH, LOG_LEVEL
from handlers import setup_handlers
from health import start_health_server

# Настройка логирования для продакшена
def setup_logging():
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Настройка уровня логирования
    numeric_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    
    logging.basicConfig(
        format=log_format,
        level=numeric_level,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('bot.log', encoding='utf-8')
        ]
    )
    
    # Уменьшаем уровень логирования для внешних библиотек
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

# Graceful shutdown
def signal_handler(signum, frame):
    logging.info(f"Получен сигнал {signum}, завершаем работу...")
    sys.exit(0)

def main():
    setup_logging()
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("Запуск Telegram бота...")

    # Запускаем health check сервер
    health_server = start_health_server()

    try:
        application = Application.builder().token(BOT_TOKEN).build()
        setup_handlers(application)

        if USE_WEBHOOK and WEBHOOK_URL:
            logging.info(f"Бот запущен в режиме вебхука на {WEBAPP_HOST}:{WEBAPP_PORT}")
            application.run_webhook(
                listen=WEBAPP_HOST,
                port=WEBAPP_PORT,
                webhook_url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
                url_path=WEBHOOK_PATH,
                secret_token=None,
            )
        else:
            logging.info("Бот запущен в режиме polling...")
            application.run_polling()
            
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске бота: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

