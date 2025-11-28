import logging
import sys
import signal
import asyncio
import os
from datetime import datetime, time
from zoneinfo import ZoneInfo
from telegram.ext import Application, CommandHandler

from config import (
    BOT_TOKEN, USE_WEBHOOK, WEBHOOK_URL, WEBAPP_HOST, WEBAPP_PORT, WEBHOOK_PATH, LOG_LEVEL,
    DATABASE_URL, SYNC_ENABLED, SYNC_INTERVAL_MINUTES, AUTO_TASKS_TIME
)
from handlers import setup_handlers, db_stats, manual_sync, manual_sync_with_cats, run_recall_notifications_task
from health import start_health_server
from database_postgres import init_db_manager, get_db_manager
from sheets_sync import init_sync_manager, get_sync_manager
from services.rbd_service import fetch_new_objects
from services.archive_service import archive_missing_objects

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

async def run_auto_tasks_scheduler(application: Application):
    """Фоновая задача для автоматического запуска get_new_objects и archive в заданное время"""
    almaty_tz = ZoneInfo("Asia/Almaty")
    
    # Парсим время из конфига (формат HH:MM)
    try:
        hour, minute = map(int, AUTO_TASKS_TIME.split(':'))
        target_time = time(hour, minute)
    except (ValueError, AttributeError) as e:
        logging.error(f"Неверный формат времени AUTO_TASKS_TIME: {AUTO_TASKS_TIME}. Используется дефолт 02:00")
        target_time = time(2, 0)
    
    last_run_date = None
    is_running = False
    
    while True:
        try:
            now_almaty = datetime.now(almaty_tz)
            current_time = now_almaty.time()
            current_date = now_almaty.date()
            
            # Проверяем, наступило ли время запуска и не запускали ли мы уже сегодня
            if (current_time.hour == target_time.hour and 
                current_time.minute == target_time.minute and
                last_run_date != current_date and
                not is_running):
                
                logging.info(f"Запуск автоматических задач в {AUTO_TASKS_TIME}")
                last_run_date = current_date
                is_running = True

                try:
                    # Последовательно выполняем автопарсинг и автоархивирование
                    logging.info("Запуск автоматического get_new_objects...")
                    stats = await fetch_new_objects()
                    logging.info(f"Автоматический get_new_objects завершен: {stats}")

                    logging.info("Запуск автоматического archive...")
                    archive_stats = await archive_missing_objects()
                    logging.info(f"Автоматический archive завершен: {archive_stats}")

                except Exception as e:
                    logging.error(f"Ошибка при выполнении автоматических задач: {e}", exc_info=True)
                finally:
                    is_running = False
            
            # Проверяем каждую минуту
            await asyncio.sleep(60)
            
        except asyncio.CancelledError:
            logging.info("Автоматические задачи остановлены")
            break
        except Exception as e:
            logging.error(f"Ошибка в планировщике автоматических задач: {e}", exc_info=True)
            await asyncio.sleep(60)  # Продолжаем работу даже при ошибке

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
        # Гарантируем миграцию со снятием бэкапа, если требуется
        try:
            db_manager = await get_db_manager()
            await db_manager.ensure_schema_with_backup()
            await db_manager.ensure_parsed_properties_schema()
            # Применяем оптимизации индексов
            try:
                await db_manager.apply_database_optimizations()
            except Exception as e:
                logging.warning(f"Не удалось применить оптимизации БД (возможно, уже применены): {e}")
        except Exception as e:
            logging.error(f"Ошибка авто-миграции схемы с бэкапом: {e}")
            raise
        
        # Инициализируем менеджер синхронизации
        if SYNC_ENABLED:
            config = {
                'SHEET_ID': os.getenv('SHEET_ID'),
                'FIRST_SHEET_GID': os.getenv('FIRST_SHEET_GID'),
                'SECOND_SHEET_GID': os.getenv('SECOND_SHEET_GID'),
                'THIRD_SHEET_GID': os.getenv('THIRD_SHEET_GID'),
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
        
        # Добавляем команды для ручной синхронизации
        if SYNC_ENABLED:
            application.add_handler(CommandHandler("sync", manual_sync))
            application.add_handler(CommandHandler("sync_with_cats", manual_sync_with_cats))
        # Команды автоматического обновления категорий удалены (перенесено в full sync)

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

        # Запускаем фоновую задачу для проверки уведомлений о перезвоне
        recall_notifications_task = asyncio.create_task(run_recall_notifications_task(application))
        logging.info("Запущена фоновая задача проверки уведомлений о перезвоне")
        
        # Запускаем фоновую задачу для автоматических задач (get_new_objects и archive)
        auto_tasks_task = asyncio.create_task(run_auto_tasks_scheduler(application))
        logging.info(f"Запущена фоновая задача автоматических задач (время запуска: {AUTO_TASKS_TIME})")

        if USE_WEBHOOK and WEBHOOK_URL:
            logging.info(f"Бот запущен в режиме вебхука на {WEBAPP_HOST}:{WEBAPP_PORT}")
            # Единый async-путь запуска (без blocking run_webhook). Используем updater.start_webhook
            await application.initialize()
            await application.updater.start_webhook(
                listen=WEBAPP_HOST,
                port=WEBAPP_PORT,
                url_path=WEBHOOK_PATH,
                webhook_url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
                secret_token=None,
            )
            await application.start()
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
        if 'recall_notifications_task' in locals():
            recall_notifications_task.cancel()
        if 'auto_tasks_task' in locals():
            auto_tasks_task.cancel()
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

