import logging
from telegram.ext import Application

from config import BOT_TOKEN, USE_WEBHOOK, WEBHOOK_URL, WEBAPP_HOST, WEBAPP_PORT, WEBHOOK_PATH
from handlers import setup_handlers


def main():
    logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

    application = Application.builder().token(BOT_TOKEN).build()
    setup_handlers(application)

    if USE_WEBHOOK and WEBHOOK_URL:
        print(f"Бот запущен в режиме вебхука на {WEBAPP_HOST}:{WEBAPP_PORT}")
        application.run_webhook(
            listen=WEBAPP_HOST,
            port=WEBAPP_PORT,
            webhook_url=f"{WEBHOOK_URL}{WEBHOOK_PATH}",
            secret_token=None,
        )
    else:
        print("Бот запущен в режиме polling...")
        application.run_polling()


if __name__ == '__main__':
    main()

