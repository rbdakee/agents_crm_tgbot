import logging
import sys
import signal
import asyncio
import os
from telegram.ext import Application, CommandHandler

from config import (
    BOT_TOKEN, USE_WEBHOOK, WEBHOOK_URL, WEBAPP_HOST, WEBAPP_PORT, WEBHOOK_PATH, LOG_LEVEL,
    DATABASE_URL, SYNC_ENABLED, SYNC_INTERVAL_MINUTES
)
from handlers import setup_handlers, db_stats, manual_sync
from health import start_health_server
from database_postgres import init_db_manager, get_db_manager
from sheets_sync import init_sync_manager, get_sync_manager

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

async def main():
    setup_logging()
    
    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.info("Запуск Telegram бота с PostgreSQL и синхронизацией...")

    # Инициализируем подключения к БД и Google Sheets
    try:
        # Инициализируем менеджер БД
        await init_db_manager(DATABASE_URL)
        logging.info("Менеджер PostgreSQL инициализирован")
        
        # Инициализируем менеджер синхронизации
        if SYNC_ENABLED:
            config = {
                'SHEET_ID': os.getenv('SHEET_ID'),
                'FIRST_SHEET_GID': os.getenv('FIRST_SHEET_GID'),
                'SECOND_SHEET_GID': os.getenv('SECOND_SHEET_GID'),
                'DATABASE_URL': DATABASE_URL,
                'SYNC_INTERVAL_MINUTES': SYNC_INTERVAL_MINUTES
            }
            await init_sync_manager(config)
            logging.info("Менеджер синхронизации инициализирован")
        
    except Exception as e:
        logging.error(f"Ошибка инициализации: {e}")
        sys.exit(1)

    # Запускаем health check сервер
    health_server = start_health_server()

    try:
        application = Application.builder().token(BOT_TOKEN).build()
        setup_handlers(application)
        
        # Добавляем команду для статистики БД (только для разработки)
        application.add_handler(CommandHandler("db_stats", db_stats))
        
        # Добавляем команду для ручной синхронизации
        if SYNC_ENABLED:
            application.add_handler(CommandHandler("sync", manual_sync))

        # Запускаем фоновую синхронизацию
        sync_task = None
        if SYNC_ENABLED:
            sync_manager = await get_sync_manager()

            # На старте выполняем быструю синхронизацию (только insert/delete по CRM ID)
            try:
                logging.info("Выполняется быстрая синхронизация при старте...")
                fast_stats = await sync_manager.sync_from_sheets_fast()
                logging.info(f"Стартовая быстрая синхронизация завершена: {fast_stats}")
                # После стартовой быстрой синхронизации сразу выгружаем DB -> Sheets(2)
                try:
                    to_sheets_stats = await sync_manager.sync_to_sheets()
                    logging.info(f"Стартовая выгрузка (DB->Sheets(2)) завершена: {to_sheets_stats}")
                except Exception as e:
                    logging.error(f"Ошибка стартовой выгрузки DB->Sheets(2): {e}")
            except Exception as e:
                logging.error(f"Ошибка стартовой быстрой синхронизации: {e}")
            
            sync_task = asyncio.create_task(sync_manager.run_background_sync())
            logging.info(f"Запущена фоновая синхронизация каждые {SYNC_INTERVAL_MINUTES} минут")

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
            # Запускаем polling в отдельной задаче
            await application.initialize()
            await application.start()
            await application.updater.start_polling()
            
            # Ждем сигнал остановки
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                logging.info("Получен сигнал остановки")
            finally:
                await application.updater.stop()
                await application.stop()
                await application.shutdown()
            
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске бота: {e}")
        sys.exit(1)
    finally:
        # Очистка ресурсов
        if sync_task:
            sync_task.cancel()
        try:
            db_manager = await get_db_manager()
            await db_manager.close()
        except:
            pass
        try:
            if SYNC_ENABLED:
                sync_manager = await get_sync_manager()
                await sync_manager.close()
        except:
            pass


if __name__ == '__main__':
    asyncio.run(main())

