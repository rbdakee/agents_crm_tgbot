import logging
import asyncio
import os
import uuid
import re
import html
from typing import Dict, List

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from config import BOT_USERNAME, CONTRACTS_PER_PAGE
from database import crm
from collage import CollageInput, render_collage_to_image
from api_client import get_collage_data_from_api

logger = logging.getLogger(__name__)

# User-scoped state and cache structures
user_states: Dict[int, str] = {}
user_contracts: Dict[int, List[Dict]] = {}
user_current_page: Dict[int, int] = {}
user_search_results: Dict[int, List[Dict]] = {}
user_current_search_page: Dict[int, int] = {}
user_last_messages: Dict[int, object] = {}

# Temporary storage of collage building state per user
user_collage_inputs: Dict[int, CollageInput] = {}
user_pending_downloads: Dict[int, int] = {}


# Utilities
PHONE_CLEAN_RE = re.compile(r"[\d\+\-\(\)\s]+")
# Регулярное выражение для очистки имени клиента - оставляем только буквы, пробелы, дефисы и апострофы
NAME_CLEAN_RE = re.compile(r"[^а-яёА-ЯЁa-zA-Z\s\-\']+", re.UNICODE)

async def show_loading(query) -> None:
    try:
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
    except Exception:
        pass


def clean_client_name(client_info: str) -> str:
    """Очищает имя клиента, оставляя только буквы, пробелы, дефисы и апострофы"""
    if not client_info:
        return ""
    
    # Сначала убираем номера телефонов
    cleaned = PHONE_CLEAN_RE.sub(" ", client_info)
    
    # Затем убираем все символы кроме букв, пробелов, дефисов и апострофов
    cleaned = NAME_CLEAN_RE.sub("", cleaned)
    
    # Убираем лишние пробелы и приводим к нормальному виду
    cleaned = " ".join(cleaned.split())
    
    # Убираем лишние дефисы и апострофы в начале/конце
    cleaned = cleaned.strip(" -'")
    
    return cleaned.strip()


def get_status_value(contract: Dict) -> str:
    value = contract.get('Статус объекта')
    if isinstance(value, str):
        value = value.strip()
    if not value:
        alt = contract.get('Статус')
        if isinstance(alt, str):
            alt = alt.strip()
        value = alt or 'Размещено'
    return value


def build_pending_tasks(contract: Dict, status_value: str, analytics_mode_active: bool) -> List[str]:
    pending: List[str] = []
    # Базовые задачи
    if not contract.get('Коллаж'):
        pending.append("❌ Коллаж")
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        pending.append("❌ Проф Коллаж")

    # Проверка наличия базовых ссылок первого этапа
    def is_filled(value) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        return bool(value)

    base_links_fields = [
        ("Крыша", 'Загрузка на крышу'),
        ("Инстаграм", 'Инстаграм'),
        ("Тикток", 'Тик ток'),
        ("Рассылка", 'Рассылка'),
        ("Стрим", 'Стрим'),
    ]
    missing_base_links = [label for (label, field) in base_links_fields if not is_filled(contract.get(field))]
    if missing_base_links:
        pending.append("❌ Добавить ссылки: " + ", ".join(missing_base_links))

    # Задачи по режимам/статусам
    if status_value == 'Реализовано':
        return pending

    if analytics_mode_active:
        if not contract.get('Аналитика'):
            pending.append("❌ Аналитика")
        elif not contract.get('Предоставление Аналитики через 5 дней'):
            pending.append("❌ Аналитика через 5 дней")
        if contract.get('Предоставление Аналитики через 5 дней') and not contract.get('Дожим на новую цену'):
            pending.append("❌ Дожим")
    elif status_value == 'Корректировка цены':
        if not contract.get('Дожим на новую цену'):
            pending.append("❌ Дожим")
        # Добавляем задачу на обновление цены, только если поле пустое
        if not str(contract.get('Корректировка цены', '')).strip():
            pending.append("❌ Обновление цены")
        # После корректировки цены — нужно добавить обновленные ссылки
        updated_links_fields = [
            ("Крыша", 'Обновление цены на крыше'),
            ("Инстаграм", 'Обновление цены в инстаграм'),
            ("Тикток", 'Обновление цены в Тик ток'),
            ("Рассылка", 'Обновление цены в рассылка'),
            ("Стрим", 'Обновление цены в Стрим'),
        ]
        missing_updated_links = [label for (label, field) in updated_links_fields if not is_filled(contract.get(field))]
        if missing_updated_links:
            pending.append("❌ Добавить обновленные ссылки: " + ", ".join(missing_updated_links))

    # Если задач нет, и объект еще не реализован — подсказать сменить статус
    if not pending and status_value != 'Реализовано':
        pending.append("❌ Для следующего этапа смените Статус объекта")

    return pending


def get_agent_phone_by_name(agent_name: str) -> str:
    phone = crm.get_phone_by_agent(agent_name)
    return phone if phone else "N/A"


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
        [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
        [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")],
    ])


# Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if context.args and context.args[0].startswith('crm_'):
        crm_id = context.args[0].replace('crm_', '')

        if user_states.get(user_id) == 'authenticated' and context.user_data.get('agent_name'):
            agent_name = context.user_data.get('agent_name')

            try:
                await update.message.delete()
            except:
                pass

            if user_id in user_last_messages:
                try:
                    await user_last_messages[user_id].delete()
                    del user_last_messages[user_id]
                except:
                    pass

            if user_id in user_search_results:
                del user_search_results[user_id]
            if user_id in user_current_search_page:
                del user_current_search_page[user_id]

            contract = await crm.search_contract_by_crm_id(crm_id, agent_name)

            if contract:
                await show_contract_detail_by_contract(update, context, contract)
            else:
                reply_markup = build_main_menu_keyboard()
                agent_phone = get_agent_phone_by_name(agent_name)
                await update.message.reply_text(
                    f"Агент: {agent_name}\n"
                    f"Номер: {agent_phone}\n\n"
                    "Выберите действие:",
                    reply_markup=reply_markup,
                )
        else:
            context.user_data['pending_crm_id'] = crm_id
            user_states[user_id] = 'waiting_phone'

            await update.message.reply_text(
                "Добро пожаловать!\n\n"
                "Для входа в систему введите ваш номер телефона:"
            )
        return

    if user_states.get(user_id) == 'authenticated' and context.user_data.get('agent_name'):
        agent_name = context.user_data.get('agent_name')
        reply_markup = build_main_menu_keyboard()
        agent_phone = get_agent_phone_by_name(agent_name)
        await update.message.reply_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
        )
    else:
        user_states[user_id] = 'waiting_phone'
        await update.message.reply_text(
            "Добро пожаловать!\n\n"
            "Для входа в систему введите ваш номер телефона:"
        )


async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_states[user_id] = 'waiting_phone'
    context.user_data.clear()
    await update.message.reply_text(
        "Вы вышли из системы.\n\n"
        "Для входа введите команду /start"
    )


async def my_contracts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = update.effective_user.id
    agent_name = context.user_data.get('agent_name')

    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return

    await show_loading(query)

    contracts = await crm.get_contracts_by_agent(agent_name)
    user_contracts[user_id] = contracts
    user_current_page[user_id] = 0

    if not contracts:
        await query.edit_message_text("У вас нет активных объектов")
        return

    await show_contracts_page(query, contracts, 0)


async def show_contracts_page(query, contracts: List[Dict], page: int):
    contracts_per_page = CONTRACTS_PER_PAGE
    start_idx = page * contracts_per_page
    end_idx = start_idx + contracts_per_page
    page_contracts = contracts[start_idx:end_idx]

    message = "Ваши объекты:\n\n"

    keyboard = []
    for contract in page_contracts:
        crm_id = contract.get('CRM ID', 'N/A')
        client_info = contract.get('Имя клиента и номер', 'N/A')
        client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')

        message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message += f"Клиент: {client_name}\n"
        message += f"Адрес: {address}\n"
        message += f"Истекает: {expires}\n"
        message += "-"*30 + "\n\n"

        # Добавляем кнопку для быстрого перехода к карточке контракта
        keyboard.append([InlineKeyboardButton(f"CRM iD: {crm_id}", callback_data=f"contract_{crm_id}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"prev_page_{page}"))
    if end_idx < len(contracts):
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"next_page_{page}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    edited_message = await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    user_id = query.from_user.id
    user_last_messages[user_id] = edited_message


async def show_search_results_page(message_or_query, contracts: List[Dict], page: int, client_name: str):
    contracts_per_page = CONTRACTS_PER_PAGE
    start_idx = page * contracts_per_page
    end_idx = start_idx + contracts_per_page
    page_contracts = contracts[start_idx:end_idx]

    message_text = f"Найдено {len(contracts)} контрактов для клиента '{client_name}':\n\n"

    keyboard = []
    for contract in page_contracts:
        crm_id = contract.get('CRM ID', 'N/A')
        client_info = contract.get('Имя клиента и номер', 'N/A')
        client_name_clean = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')

        message_text += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message_text += f"Клиент: {client_name_clean}\n"
        message_text += f"Адрес: {address}\n"
        message_text += f"Истекает: {expires}\n"
        message_text += "-"*30 + "\n\n"

        # Кнопка для показа карточки контракта из результатов поиска
        keyboard.append([InlineKeyboardButton(f"CRM iD: {crm_id}", callback_data=f"contract_{crm_id}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"search_prev_page_{page}"))
    if end_idx < len(contracts):
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"search_next_page_{page}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if hasattr(message_or_query, 'edit_message_text'):
        edited_message = await message_or_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        user_id = message_or_query.from_user.id
        user_last_messages[user_id] = edited_message
    else:
        edited_message = await message_or_query.edit_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        user_id = message_or_query.from_user.id
        user_last_messages[user_id] = edited_message


async def show_contract_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    query = update.callback_query
    await query.answer()

    agent_name = context.user_data.get('agent_name')
    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return

    contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
    if not contract:
        await query.edit_message_text("Контракт не найден среди ваших сделок")
        return

    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в договоре', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"📌 Статус: {get_status_value(contract)}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"

    # Добавляем блок со ссылками, если есть
    link_fields = [
        ("Инстаграм", 'Инстаграм'),
        ("Тикток", 'Тик ток'),
        ("Крыша", 'Загрузка на крышу'),
        ("Рассылка", 'Рассылка'),
        ("Стрим", 'Стрим'),
    ]
    available_links = []
    for label, field in link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

    # Добавляем блок с обновленными ссылками (после корректировки цены), если есть
    updated_link_fields = [
        ("Инстаграм", 'Обновление цены в инстаграм'),
        ("Тикток", 'Обновление цены в Тик ток'),
        ("Крыша", 'Обновление цены на крыше'),
        ("Рассылка", 'Обновление цены в рассылка'),
        ("Стрим", 'Обновление цены в Стрим'),
    ]
    available_updated_links = []
    for label, field in updated_link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_updated_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_updated_links:
        message += f"🔗 Обновленные ссылки: {', '.join(available_updated_links)}\n\n"

    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    if contract.get('Дожим на новую цену'):
        message += "✅ Дожим\n"

    # Рендер кнопок в зависимости от статуса
    status_value = get_status_value(contract)
    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)

    # Чек-лист невыполненных задач
    pending = build_pending_tasks(contract, status_value, analytics_mode_active)
    if pending:
        message += "\n📝 Необходимо сделать:\n" + "\n".join(pending) + "\n"

    # Если реализовано — кнопок нет
    if status_value == 'Реализовано':
        keyboard = [
            [InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
        return

    keyboard = []
    # Общие правила на коллаж/проф/показ
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Создать коллаж", callback_data=f"collage_build_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    if status_value == 'Корректировка цены':
        # Кнопки для статуса "Корректировка цены"
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        if not str(contract.get('Корректировка цены', '')).strip():
            keyboard.append([InlineKeyboardButton("Обновление цены", callback_data=f"price_adjust_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    elif analytics_mode_active:
        # Кнопки для режима предоставления аналитики
        if not contract.get('Аналитика'):
            keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_done_{crm_id}")])
        if contract.get('Аналитика') and not contract.get('Предоставление Аналитики через 5 дней'):
            keyboard.append([InlineKeyboardButton("Аналитика через 5 дней", callback_data=f"analytics_provided_{crm_id}")])
        if contract.get('Предоставление Аналитики через 5 дней') and not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    else:
        # Базовые кнопки по умолчанию
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    if data == "my_contracts":
        await my_contracts(update, context)

    elif data.startswith("contract_"):
        crm_id = data.replace("contract_", "")
        user_id = update.effective_user.id
        user_states[user_id] = 'authenticated'
        await show_loading(query)
        await show_contract_detail(update, context, crm_id)

    elif data.startswith("prev_page_"):
        page = int(data.replace("prev_page_", ""))
        user_id = update.effective_user.id
        contracts = user_contracts.get(user_id, [])
        await show_loading(query)
        await show_contracts_page(query, contracts, page - 1)
        user_current_page[user_id] = page - 1

    elif data.startswith("next_page_"):
        page = int(data.replace("next_page_", ""))
        user_id = update.effective_user.id
        contracts = user_contracts.get(user_id, [])
        await show_loading(query)
        await show_contracts_page(query, contracts, page + 1)
        user_current_page[user_id] = page + 1

    elif data.startswith("search_prev_page_"):
        page = int(data.replace("search_prev_page_", ""))
        user_id = update.effective_user.id
        contracts = user_search_results.get(user_id, [])
        client_name = context.user_data.get('last_search_client', '')
        await show_loading(query)
        await show_search_results_page(query, contracts, page - 1, client_name)
        user_current_search_page[user_id] = page - 1

    elif data.startswith("search_next_page_"):
        page = int(data.replace("search_next_page_", ""))
        user_id = update.effective_user.id
        contracts = user_search_results.get(user_id, [])
        client_name = context.user_data.get('last_search_client', '')
        await show_loading(query)
        await show_search_results_page(query, contracts, page + 1, client_name)
        user_current_search_page[user_id] = page + 1

    elif data.startswith("collage_build_"):
        crm_id = data.replace("collage_build_", "")
        user_id = update.effective_user.id
        await show_loading(query)
        await query.edit_message_text("Получаю данные из CRM...")
        
        try:
            # Получаем данные из API
            collage_input = await get_collage_data_from_api(crm_id)
            if not collage_input:
                await query.edit_message_text("❌ Не удалось получить данные из CRM. Проверьте CRM ID.")
                return
            
            # Получаем имя клиента из кеша n8n
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract and contract.get('Имя клиента и номер'):
                    client_info = contract['Имя клиента и номер']
                    # Извлекаем только имя клиента (до двоеточия) и очищаем от лишних символов
                    raw_client_name = client_info.split(':')[0].strip()
                    client_name = clean_client_name(raw_client_name)
                    collage_input.client_name = client_name
            
            # Сохраняем данные для пользователя
            user_collage_inputs[user_id] = collage_input
            
            # Показываем данные коллажа с кнопками редактирования
            await show_collage_data_with_edit_buttons(query, collage_input, crm_id)
        except Exception as e:
            logger.error(f"Error getting collage data from API: {e}")
            await query.edit_message_text("❌ Ошибка при получении данных из CRM. Попробуйте позже.")

    elif data.startswith("collage_proceed_"):
        crm_id = data.replace("collage_proceed_", "")
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_collage_photos_{crm_id}'
        
        await query.edit_message_text(
            f"📸 Теперь отправьте фотографии для коллажа (1-5 штук).\n"
            f"После отправки всех фото напишите 'Готово'.\n\n"
            f"Вы можете ввести 'отмена' чтобы прервать."
        )

    elif data.startswith("edit_collage_"):
        # Обработка редактирования полей коллажа
        parts = data.replace("edit_collage_", "").split("_")
        field = parts[0]
        crm_id = parts[1]
        user_id = update.effective_user.id
        
        field_names = {
            'client': 'имя клиента',
            'complex': 'название ЖК',
            'address': 'адрес',
            'area': 'площадь',
            'rooms': 'количество комнат',
            'floor': 'этаж',
            'price': 'цену',
            'class': 'класс жилья',
            'rop': 'имя РОП',
            'phone': 'номер телефона агента',
            'benefits': 'достоинства'
        }
        
        field_name = field_names.get(field, field)
        user_states[user_id] = f'editing_collage_{field}_{crm_id}'
        
        if field == 'benefits':
            ci = user_collage_inputs.get(user_id)
            if ci and ci.benefits:
                benefits_text = "\n".join([f"{i+1}. {benefit}" for i, benefit in enumerate(ci.benefits)])
                await query.edit_message_text(
                    f"📋 Текущие достоинства:\n{benefits_text}\n\n"
                    f"Введите новые достоинства (каждое с новой строки) или 'отмена' для возврата:"
                )
            else:
                await query.edit_message_text(
                    f"📋 Достоинства не заданы.\n\n"
                    f"Введите достоинства (каждое с новой строки) или 'отмена' для возврата:"
                )
        else:
            await query.edit_message_text(
                f"✏️ Введите новое значение для поля '{field_name}' или 'отмена' для возврата:"
            )

    elif data.startswith("collage_save_"):
        crm_id = data.replace("collage_save_", "")
        
        # Редактируем caption фотографии
        try:
            await query.edit_message_caption(caption="Коллаж сохранен!")
        except Exception:
            await query.answer("Коллаж сохранен!")
        
        success = await crm.update_contract(crm_id, {"collage": True})
        if success:
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                await crm.refresh_agent_cache(agent_name)
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    # Отправляем новое сообщение с деталями объекта напрямую
                    await send_contract_detail_directly(update.effective_chat, context, contract)
                else:
                    await update.effective_chat.send_message("Контракт не найден")
            else:
                await update.effective_chat.send_message("Ошибка: агент не найден")
        else:
            await query.answer("Ошибка при сохранении", show_alert=True)

    elif data.startswith("collage_redo_"):
        crm_id = data.replace("collage_redo_", "")
        user_id = update.effective_user.id
        
        # Редактируем caption фотографии
        try:
            await query.edit_message_caption(caption="Коллаж переделывается...")
        except Exception:
            await query.answer("Коллаж переделывается...")
        
        try:
            # Получаем данные из API заново
            collage_input = await get_collage_data_from_api(crm_id)
            if not collage_input:
                await update.effective_chat.send_message("❌ Не удалось получить данные из CRM. Проверьте CRM ID.")
                return
            
            # Получаем имя клиента из кеша n8n
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract and contract.get('Имя клиента и номер'):
                    client_info = contract['Имя клиента и номер']
                    # Извлекаем только имя клиента (до двоеточия) и очищаем от лишних символов
                    raw_client_name = client_info.split(':')[0].strip()
                    client_name = clean_client_name(raw_client_name)
                    collage_input.client_name = client_name
            
            # Сохраняем данные для пользователя
            user_collage_inputs[user_id] = collage_input
            
            # Создаем фиктивный query объект для отправки нового сообщения
            class FakeQuery:
                async def edit_message_text(self, text, reply_markup=None):
                    await update.effective_chat.send_message(text, reply_markup=reply_markup)
            
            fake_query = FakeQuery()
            # Показываем данные коллажа с кнопками редактирования
            await show_collage_data_with_edit_buttons(fake_query, collage_input, crm_id)
        except Exception as e:
            logger.error(f"Error getting collage data from API: {e}")
            await update.effective_chat.send_message("❌ Ошибка при получении данных из CRM. Попробуйте позже.")

    elif data.startswith("collage_cancel_"):
        crm_id = data.replace("collage_cancel_", "")
        user_id = update.effective_user.id
        
        # Очищаем данные коллажа пользователя
        user_collage_inputs.pop(user_id, None)
        user_states[user_id] = 'authenticated'
        
        # Редактируем caption фотографии
        try:
            await query.edit_message_caption(caption="Создание отменено.")
        except Exception:
            await query.answer("Создание отменено.")
        
        # Возвращаемся к деталям объекта
        agent_name = context.user_data.get('agent_name')
        if agent_name:
            contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                # Отправляем новое сообщение с деталями объекта напрямую
                await send_contract_detail_directly(update.effective_chat, context, contract)
            else:
                await update.effective_chat.send_message("Контракт не найден")
        else:
            await update.effective_chat.send_message("Ошибка: агент не найден")

    elif data.startswith("action_pro_collage_"):
        crm_id = data.replace("action_pro_collage_", "")
        await show_loading(query)
        success = await crm.update_contract(crm_id, {"updatedCollage": True})
        if success:
            await query.answer("Проф коллаж отмечен как выполненный")
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                await crm.refresh_agent_cache(agent_name)
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.edit_message_text("Контракт не найден")
            else:
                await query.edit_message_text("Ошибка: агент не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("action_show_"):
        crm_id = data.replace("action_show_", "")
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await query.answer("Ошибка: агент не найден", show_alert=True)
            return
        await show_loading(query)
        contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await query.answer("Контракт не найден", show_alert=True)
            return
        current_show = int(contract.get('Показ', 0))
        success = await crm.update_contract(crm_id, {"show": current_show + 1})
        if success:
            await query.answer(f"Показ увеличен до {current_show + 1}")
            await crm.refresh_agent_cache(agent_name)
            contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                await show_contract_detail_by_contract(update, context, contract)
            else:
                await query.edit_message_text("Контракт не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("analytics_done_"):
        crm_id = data.replace("analytics_done_", "")
        await show_loading(query)
        success = await crm.update_contract(crm_id, {"analytics": True})
        if success:
            await query.answer("Аналитика отмечена как сделанная")
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                await crm.refresh_agent_cache(agent_name)
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.edit_message_text("Контракт не найден")
            else:
                await query.edit_message_text("Ошибка: агент не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("analytics_provided_"):
        crm_id = data.replace("analytics_provided_", "")
        await show_loading(query)
        success = await crm.update_contract(crm_id, {"analyticsIn5Days": True})
        if success:
            await query.answer("Аналитика отмечена как предоставленная")
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                await crm.refresh_agent_cache(agent_name)
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.edit_message_text("Контракт не найден")
            else:
                await query.edit_message_text("Ошибка: агент не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("status_menu_"):
        crm_id = data.replace("status_menu_", "")
        keyboard = [
            [InlineKeyboardButton("Предоставление Аналитики", callback_data=f"status_set_{crm_id}_analytics_mode")],
            [InlineKeyboardButton("Корректировка цены", callback_data=f"status_set_{crm_id}_price_adjust")],
            [InlineKeyboardButton("На задатке", callback_data=f"status_set_{crm_id}_deposit")],
            [InlineKeyboardButton("Реализовано", callback_data=f"status_set_{crm_id}_completed")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await show_loading(query)
        await query.edit_message_text("Смена статуса объекта:", reply_markup=reply_markup)

    elif data.startswith("status_set_"):
        remaining = data.replace("status_set_", "")
        parts = remaining.split("_")
        crm_id = parts[0]
        status_key = parts[1]
        status_map = {
            'analyticsmode': 'Размещено',
        }
        if status_key == 'analytics' or status_key == 'analyticsmode':
            # Включаем режим аналитики (кнопки в деталях) без смены статуса
            context.user_data['analytics_mode'] = crm_id
            await show_loading(query)
            await show_contract_detail(update, context, crm_id)
        else:
            updates = {}
            if status_key == 'price':
                updates['status'] = 'Корректировка цены'
            elif status_key == 'priceadjust':
                updates['status'] = 'Корректировка цены'
            elif status_key == 'deposit':
                updates['status'] = 'Задаток/Сделка'
            elif status_key == 'completed':
                updates['status'] = 'Реализовано'
            else:
                updates['status'] = 'Размещено'
            await show_loading(query)
            success = await crm.update_contract(crm_id, updates)
            if success:
                await query.answer("Статус обновлен")
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    await crm.refresh_agent_cache(agent_name)
                    contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                    if contract:
                        await show_contract_detail_by_contract(update, context, contract)
                    else:
                        await query.edit_message_text("Контракт не найден")
                else:
                    await query.edit_message_text("Ошибка: агент не найден")
            else:
                await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("push_"):
        crm_id = data.replace("push_", "")
        await show_loading(query)
        success = await crm.update_contract(crm_id, {"pricePush": True})
        if success:
            # Если был режим аналитики и дожим сделан, меняем статус на "Корректировка цены"
            updates = {"status": 'Корректировка цены'}
            await crm.update_contract(crm_id, updates)
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                await crm.refresh_agent_cache(agent_name)
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.edit_message_text("Контракт не найден")
            else:
                await query.edit_message_text("Ошибка: агент не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)

    elif data.startswith("analytics_"):
        crm_id = data.replace("analytics_", "")
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await query.answer("Ошибка: агент не найден", show_alert=True)
            return
        contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await query.answer("Контракт не найден", show_alert=True)
            return
        analytics_done = contract.get('Аналитика', False)
        analytics_provided = contract.get('Предоставление Аналитики через 5 дней', False)
        keyboard = []
        if not analytics_done:
            keyboard.append([InlineKeyboardButton("Сделано", callback_data=f"analytics_done_{crm_id}")])
        if not analytics_provided:
            keyboard.append([InlineKeyboardButton("Предоставлено (после 5 дней)", callback_data=f"analytics_provided_{crm_id}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await show_loading(query)
        await query.edit_message_text("Выберите действие по аналитике:", reply_markup=reply_markup)

    # Удален пункт меню links_

    elif data.startswith("price_adjust_"):
        crm_id = data.replace("price_adjust_", "")
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_price_{crm_id}'
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ])
        await show_loading(query)
        await query.edit_message_text(f"Введите новую цену для CRM ID {crm_id}:", reply_markup=back_keyboard)

    # Удален просмотр ссылок по кнопке view_links_

    elif data.startswith("add_link_"):
        crm_id = data.replace("add_link_", "")
        await show_loading(query)
        await show_add_link_menu(update, context, crm_id)

    elif data.startswith("link_type_"):
        remaining = data.replace("link_type_", "")
        parts = remaining.split("_")
        crm_id = parts[0]
        link_type = "_".join(parts[1:])
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_link_{crm_id}_{link_type}'
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ])
        await show_loading(query)
        await query.edit_message_text(f"Введите ссылку для {link_type}:", reply_markup=back_keyboard)

    elif data == "search_client":
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_client_search'
        await show_loading(query)
        await query.edit_message_text("Введите имя клиента для поиска:")

    elif data == "main_menu":
        user_id = update.effective_user.id
        user_states[user_id] = 'authenticated'
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await query.edit_message_text("Ошибка: агент не найден")
            return
        reply_markup = build_main_menu_keyboard()
        agent_phone = get_agent_phone_by_name(agent_name)
        await show_loading(query)
        await query.edit_message_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
        )

    elif data == "logout_confirm":
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_phone'
        context.user_data.clear()
        if user_id in user_last_messages:
            del user_last_messages[user_id]
        await show_loading(query)
        await query.edit_message_text(
            "Вы вышли из системы.\n\n"
            "Для входа введите команду /start"
        )


async def show_links_view(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    # Отдельный просмотр ссылок больше не используется
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("Просмотр ссылок перенесен в детали контракта")


async def show_add_link_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    query = update.callback_query
    await query.answer()

    agent_name = context.user_data.get('agent_name')
    contract = None
    if agent_name:
        await crm.refresh_agent_cache(agent_name)
        contract = await crm.search_contract_by_crm_id(crm_id, agent_name)

    def create_button(text, callback_data, has_link=False):
        button_text = f"✅ {text}" if has_link else text
        return InlineKeyboardButton(button_text, callback_data=callback_data)

    def is_field_filled(value):
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        return bool(value)

    links_status = {
        'krisha': contract.get('Загрузка на крышу', '') if contract else '',
        'krisha_update': contract.get('Обновление цены на крыше', '') if contract else '',
        'instagram': contract.get('Инстаграм', '') if contract else '',
        'instagram_update': contract.get('Обновление цены в инстаграм', '') if contract else '',
        'tiktok': contract.get('Тик ток', '') if contract else '',
        'tiktok_update': contract.get('Обновление цены в Тик ток', '') if contract else '',
        'mailing': contract.get('Рассылка', '') if contract else '',
        'mailing_update': contract.get('Обновление цены в рассылка', '') if contract else '',
        'stream': contract.get('Стрим', '') if contract else '',
        'stream_update': contract.get('Обновление цены в Стрим', '') if contract else ''
    }

    keyboard = [
        [create_button("Крыша", f"link_type_{crm_id}_krisha", is_field_filled(links_status['krisha']))],
        [create_button("Инстаграм", f"link_type_{crm_id}_instagram", is_field_filled(links_status['instagram']))],
        [create_button("Тикток", f"link_type_{crm_id}_tiktok", is_field_filled(links_status['tiktok']))],
        [create_button("Рассылка", f"link_type_{crm_id}_mailing", is_field_filled(links_status['mailing']))],
        [create_button("Стрим", f"link_type_{crm_id}_stream", is_field_filled(links_status['stream']))],
        [create_button("Крыша (обновление)", f"link_type_{crm_id}_krisha_update", is_field_filled(links_status['krisha_update']))],
        [create_button("Инстаграм (обновление)", f"link_type_{crm_id}_instagram_update", is_field_filled(links_status['instagram_update']))],
        [create_button("Тикток (обновление)", f"link_type_{crm_id}_tiktok_update", is_field_filled(links_status['tiktok_update']))],
        [create_button("Рассылка (обновление)", f"link_type_{crm_id}_mailing_update", is_field_filled(links_status['mailing_update']))],
        [create_button("Стрим (обновление)", f"link_type_{crm_id}_stream_update", is_field_filled(links_status['stream_update']))],
        [InlineKeyboardButton("🔙 Назад к контракту", callback_data=f"contract_{crm_id}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите тип ссылки для добавления:", reply_markup=reply_markup)


async def show_links_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    # Старое меню ссылок больше не используется; перенаправляем на добавление
    await show_add_link_menu(update, context, crm_id)


async def handle_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')

    if state.startswith('waiting_price_'):
        crm_id = state.replace('waiting_price_', '')
        new_price = update.message.text.strip()
        loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
        success = await crm.update_contract(crm_id, {"priceAdjustment": new_price})
        if success:
            await loading_msg.edit_text(f"Цена успешно обновлена на: {new_price}")
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
            user_states[user_id] = 'authenticated'
        else:
            await loading_msg.edit_text("Ошибка при обновлении цены")

    elif state.startswith('waiting_link_'):
        remaining = state.replace('waiting_link_', '')
        parts = remaining.split('_')
        crm_id = parts[0]
        link_type = '_'.join(parts[1:])
        link_url = update.message.text.strip()
        loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")

        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await loading_msg.edit_text("Ошибка: агент не найден")
            return
        contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await loading_msg.edit_text("Контракт не найден")
            return

        field_mapping = {
            'krisha': 'krishaUpload',
            'krisha_update': 'priceUpdateKrisha',
            'instagram': 'instagram',
            'instagram_update': 'priceUpdateInstagram',
            'tiktok': 'tikTok',
            'tiktok_update': 'priceUpdateTikTok',
            'mailing': 'mailing',
            'mailing_update': 'priceUpdateMailing',
            'stream': 'stream',
            'stream_update': 'priceUpdateStream',
        }
        field_name = field_mapping.get(link_type)
        if not field_name:
            await loading_msg.edit_text("Неизвестный тип ссылки")
            return

        display_names = {
            'krisha': 'Крыша',
            'krisha_update': 'Крыша (обновление)',
            'instagram': 'Инстаграм',
            'instagram_update': 'Инстаграм (обновление)',
            'tiktok': 'Тикток',
            'tiktok_update': 'Тикток (обновление)',
            'mailing': 'Рассылка',
            'mailing_update': 'Рассылка (обновление)',
            'stream': 'Стрим',
            'stream_update': 'Стрим (обновление)',
        }
        field_display_name = display_names.get(link_type, link_type)

        success = await crm.update_contract(crm_id, {field_name: link_url})
        if success:
            await loading_msg.edit_text(f"Ссылка {field_display_name} успешно добавлена")
            await crm.refresh_agent_cache(agent_name)
            contract = await crm.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                await show_contract_detail_by_contract(update, context, contract)
        else:
            await loading_msg.edit_text("Ошибка при добавлении ссылки")

        user_states[user_id] = 'authenticated'

    elif state.startswith('waiting_collage_photos_'):
        text = update.message.text.strip()
        if text.lower() == 'отмена':
            user_states[user_id] = 'authenticated'
            user_collage_inputs.pop(user_id, None)
            await update.message.reply_text('Создание коллажа отменено.')
            return
        
        # User should send 'Готово' to finish
        if text.lower() in ('готово', 'готово.', 'готов'):            
            crm_id = state.replace('waiting_collage_photos_', '')
            ci = user_collage_inputs.get(user_id)
            if not ci or not ci.photos:
                await update.message.reply_text('Не получено фото. Пожалуйста отправьте хотя бы одно фото или введите Отмена.')
                return
            status_msg = await update.message.reply_text('Создаю коллаж, подождите...')
            
            def _cleanup_files():
                try:
                    # Удаляем итоговый PNG и временный HTML
                    png_path = os.path.join('data', f"collage_{ci.crm_id}.png")
                    html_path = os.path.join('data', f"collage_{ci.crm_id}.html")
                    for p in [png_path, html_path]:
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except Exception:
                            logger.exception('Failed to remove temp file %s', p)
                    # Удаляем загруженные пользователем фото
                    for p in list(ci.photos or []):
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except Exception:
                            logger.exception('Failed to remove user photo %s', p)
                except Exception:
                    logger.exception('Cleanup error')

            async def _render_and_send():
                image_path = await render_collage_to_image(ci)
                
                # Создаем кнопки для действий с коллажем
                keyboard = [
                    [InlineKeyboardButton("✅ Сохранить Коллаж", callback_data=f"collage_save_{crm_id}")],
                    [InlineKeyboardButton("🔄 Переделать", callback_data=f"collage_redo_{crm_id}")],
                    [InlineKeyboardButton("❌ Отменить создание", callback_data=f"collage_cancel_{crm_id}")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Попытки отправки с повторами при сетевых ошибках
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        with open(image_path, 'rb') as f:
                            await update.message.reply_photo(
                                photo=f, 
                                caption='Коллаж готов! Выберите действие:',
                                reply_markup=reply_markup,
                                read_timeout=60,  # Увеличиваем тайм-аут для загрузки
                                write_timeout=60
                            )
                        break  # Успешно отправлено
                    except Exception as e:
                        if attempt == max_retries - 1:
                            # Последняя попытка не удалась
                            logger.error(f"Failed to send collage after {max_retries} attempts: {e}")
                            await update.message.reply_text(
                                f"❌ Коллаж создан, но не удалось отправить из-за проблем с сетью.\n"
                                f"Попробуйте создать коллаж еще раз."
                            )
                            raise
                        else:
                            logger.warning(f"Attempt {attempt + 1} failed, retrying: {e}")
                            await asyncio.sleep(2)  # Пауза перед повтором
                
                try:
                    await status_msg.delete()
                except Exception:
                    pass
            
            try:
                await asyncio.wait_for(_render_and_send(), timeout=180)  # Увеличиваем общий тайм-аут
            except asyncio.TimeoutError:
                try:
                    await status_msg.edit_text('⏰ Не удалось создать коллаж за 3 минуты. Попробуйте ещё раз.')
                except Exception:
                    pass
            except Exception as e:
                logger.exception('Collage render/send failed')
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                
                # Определяем тип ошибки для более точного сообщения
                error_msg = "❌ Ошибка при создании коллажа."
                if "TimedOut" in str(e) or "ReadTimeout" in str(e) or "SSLWantReadError" in str(e):
                    error_msg = "🌐 Проблемы с сетью при отправке коллажа. Проверьте интернет-соединение и попробуйте еще раз."
                elif "SSL" in str(e):
                    error_msg = "🔒 Проблемы с SSL-соединением. Попробуйте еще раз через несколько минут."
                
                await update.message.reply_text(f"{error_msg}\n\nПопробуйте еще раз.")
            finally:
                _cleanup_files()
            
            # cleanup
            user_states[user_id] = 'authenticated'
            user_collage_inputs.pop(user_id, None)
            return


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    if state == 'waiting_phone':
        await handle_phone(update, context)
    elif state.startswith('waiting_price_') or state.startswith('waiting_link_') or state.startswith('waiting_collage_photos_'):
        await handle_price_input(update, context)
    elif state.startswith('editing_collage_'):
        await handle_collage_edit(update, context)
    elif state == 'waiting_client_search':
        await handle_client_search(update, context)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    if not state.startswith('waiting_collage_photos_'):
        return
    ci = user_collage_inputs.get(user_id)
    if not ci:
        return
    try:
        user_pending_downloads[user_id] = user_pending_downloads.get(user_id, 0) + 1
        photo_sizes = update.message.photo
        if not photo_sizes:
            return
        # get best resolution
        file_id = photo_sizes[-1].file_id
        file = await context.bot.get_file(file_id)
        photos_dir = os.path.join('data')
        os.makedirs(photos_dir, exist_ok=True)
        local_path = os.path.join(photos_dir, f"collage_{uuid.uuid4().hex}.jpg")
        await file.download_to_drive(local_path)
        if not ci.photos:
            ci.photos = []
        if len(ci.photos) < 5:
            ci.photos.append(local_path)
    except Exception as e:
        await update.message.reply_text(f"Не удалось сохранить фото: {e}")
    finally:
        user_pending_downloads[user_id] = max(0, user_pending_downloads.get(user_id, 1) - 1)


async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_states.get(user_id) != 'waiting_phone':
        return
    
    phone = update.message.text.strip()
    
    # Проверяем валидность номера телефона
    if not crm.is_valid_phone(phone):
        await update.message.reply_text(
            "❌ Неверный формат номера телефона.\n\n"
            "Пожалуйста, введите номер в одном из форматов:\n"
            "• 87777777777\n"
            "• +77777777777\n"
            "• 7777777777\n"
            "• 8777777777\n\n"
            "Номер должен содержать 10-11 цифр и начинаться с 8 или 7."
        )
        return
    
    # Нормализуем номер для поиска
    normalized_phone = crm.normalize_phone(phone)
    agent_name = crm.get_agent_by_phone(normalized_phone)
    
    if agent_name:
        user_states[user_id] = 'authenticated'
        context.user_data['agent_name'] = agent_name
        context.user_data['phone'] = normalized_phone
        reply_markup = build_main_menu_keyboard()
        pending_crm_id = context.user_data.get('pending_crm_id')
        if pending_crm_id:
            del context.user_data['pending_crm_id']
            loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
            contract = await crm.search_contract_by_crm_id(pending_crm_id, agent_name)
            if contract:
                await loading_msg.delete()
                await show_contract_detail_by_contract(update, context, contract)
            else:
                await loading_msg.edit_text(f"Контракт с CRM ID {pending_crm_id} не найден среди ваших сделок")
                agent_phone = get_agent_phone_by_name(agent_name)
                await update.message.reply_text(
                    f"Агент: {agent_name}\n"
                    f"Номер: {agent_phone}\n\n"
                    "Выберите действие:",
                    reply_markup=reply_markup,
                )
        else:
            agent_phone = get_agent_phone_by_name(agent_name)
            await update.message.reply_text(
                f"Агент: {agent_name}\n"
                f"Номер: {agent_phone}\n\n"
                "Выберите действие:",
                reply_markup=reply_markup,
            )
    else:
        await update.message.reply_text(
            "❌ Номер телефона не найден в системе.\n\n"
            "Пожалуйста, проверьте правильность ввода или обратитесь к администратору.\n\n"
            f"Введенный номер: {phone}\n"
            f"Нормализованный: {normalized_phone}"
        )


async def handle_client_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    client_name = update.message.text.strip()
    agent_name = context.user_data.get('agent_name')
    if not agent_name:
        await update.message.reply_text("Ошибка: агент не найден")
        user_states[user_id] = 'authenticated'
        return
    loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
    contracts = await crm.search_contracts_by_client_name(client_name, agent_name)
    if contracts:
        if len(contracts) == 1:
            await show_contract_detail_by_contract(update, context, contracts[0])
        else:
            user_search_results[user_id] = contracts
            user_current_search_page[user_id] = 0
            context.user_data['last_search_client'] = client_name
            await show_search_results_page(loading_msg, contracts, 0, client_name)
    else:
        await loading_msg.edit_text(f"Контракты для клиента '{client_name}' не найдены среди ваших сделок")
        reply_markup = build_main_menu_keyboard()
        agent_phone = get_agent_phone_by_name(agent_name)
        await update.message.reply_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup,
        )
    user_states[user_id] = 'authenticated'


async def send_contract_detail_directly(chat, context: ContextTypes.DEFAULT_TYPE, contract: Dict):
    """Отправляет новое сообщение с деталями контракта напрямую в чат"""
    crm_id = contract.get('CRM ID', 'N/A')
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в договоре', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"📌 Статус: {get_status_value(contract)}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"

    # Добавляем блок со ссылками, если есть
    link_fields = [
        ("Инстаграм", 'Инстаграм'),
        ("Тикток", 'Тик ток'),
        ("Крыша", 'Загрузка на крышу'),
        ("Рассылка", 'Рассылка'),
        ("Стрим", 'Стрим'),
    ]
    available_links = []
    for label, field in link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

    # Добавляем блок с обновленными ссылками (после корректировки цены), если есть
    updated_link_fields = [
        ("Инстаграм", 'Обновление цены в инстаграм'),
        ("Тикток", 'Обновление цены в Тик ток'),
        ("Крыша", 'Обновление цены на крыше'),
        ("Рассылка", 'Обновление цены в рассылка'),
        ("Стрим", 'Обновление цены в Стрим'),
    ]
    available_updated_links = []
    for label, field in updated_link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_updated_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_updated_links:
        message += f"🔗 Обновленные ссылки: {', '.join(available_updated_links)}\n\n"

    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    if contract.get('Дожим на новую цену'):
        message += "✅ Дожим\n"

    status_value = get_status_value(contract)
    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)

    # Чек-лист невыполненных задач
    pending = build_pending_tasks(contract, status_value, analytics_mode_active)
    if pending:
        message += "\n📝 Необходимо сделать:\n" + "\n".join(pending) + "\n"

    # Создаем кнопки
    keyboard = []
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Создать коллаж", callback_data=f"collage_build_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    if status_value == 'Корректировка цены':
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        if not str(contract.get('Корректировка цены', '')).strip():
            keyboard.append([InlineKeyboardButton("Обновление цены", callback_data=f"price_adjust_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    elif analytics_mode_active:
        if not contract.get('Аналитика'):
            keyboard.append([InlineKeyboardButton("Аналитика сделано", callback_data=f"analytics_done_{crm_id}")])
        if not contract.get('Предоставление Аналитики через 5 дней'):
            keyboard.append([InlineKeyboardButton("Аналитика предоставлено", callback_data=f"analytics_provided_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Выйти из режима аналитики", callback_data=f"exit_analytics_{crm_id}")])
    else:
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await chat.send_message(message, reply_markup=reply_markup, parse_mode='HTML')


async def send_contract_detail_message(update: Update, context: ContextTypes.DEFAULT_TYPE, contract: Dict):
    """Отправляет новое сообщение с деталями контракта"""
    crm_id = contract.get('CRM ID', 'N/A')
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в договоре', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"📌 Статус: {get_status_value(contract)}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"

    # Добавляем блок со ссылками, если есть
    link_fields = [
        ("Инстаграм", 'Инстаграм'),
        ("Тикток", 'Тик ток'),
        ("Крыша", 'Загрузка на крышу'),
        ("Рассылка", 'Рассылка'),
        ("Стрим", 'Стрим'),
    ]
    available_links = []
    for label, field in link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

    # Добавляем блок с обновленными ссылками (после корректировки цены), если есть
    updated_link_fields = [
        ("Инстаграм", 'Обновление цены в инстаграм'),
        ("Тикток", 'Обновление цены в Тик ток'),
        ("Крыша", 'Обновление цены на крыше'),
        ("Рассылка", 'Обновление цены в рассылка'),
        ("Стрим", 'Обновление цены в Стрим'),
    ]
    available_updated_links = []
    for label, field in updated_link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_updated_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_updated_links:
        message += f"🔗 Обновленные ссылки: {', '.join(available_updated_links)}\n\n"

    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    if contract.get('Дожим на новую цену'):
        message += "✅ Дожим\n"

    status_value = get_status_value(contract)
    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)

    # Чек-лист невыполненных задач
    pending = build_pending_tasks(contract, status_value, analytics_mode_active)
    if pending:
        message += "\n📝 Необходимо сделать:\n" + "\n".join(pending) + "\n"

    # Создаем кнопки
    keyboard = []
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Создать коллаж", callback_data=f"collage_build_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    if status_value == 'Корректировка цены':
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        if not str(contract.get('Корректировка цены', '')).strip():
            keyboard.append([InlineKeyboardButton("Обновление цены", callback_data=f"price_adjust_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    elif analytics_mode_active:
        if not contract.get('Аналитика'):
            keyboard.append([InlineKeyboardButton("Аналитика сделано", callback_data=f"analytics_done_{crm_id}")])
        if not contract.get('Предоставление Аналитики через 5 дней'):
            keyboard.append([InlineKeyboardButton("Аналитика предоставлено", callback_data=f"analytics_provided_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Выйти из режима аналитики", callback_data=f"exit_analytics_{crm_id}")])
    else:
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_chat.send_message(message, reply_markup=reply_markup, parse_mode='HTML')


async def show_contract_detail_by_contract(update: Update, context: ContextTypes.DEFAULT_TYPE, contract: Dict):
    crm_id = contract.get('CRM ID', 'N/A')
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в договоре', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"📌 Статус: {get_status_value(contract)}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"

    # Добавляем блок со ссылками, если есть
    link_fields = [
        ("Инстаграм", 'Инстаграм'),
        ("Тикток", 'Тик ток'),
        ("Крыша", 'Загрузка на крышу'),
        ("Рассылка", 'Рассылка'),
        ("Стрим", 'Стрим'),
    ]
    available_links = []
    for label, field in link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

    # Добавляем блок с обновленными ссылками (после корректировки цены), если есть
    updated_link_fields = [
        ("Инстаграм", 'Обновление цены в инстаграм'),
        ("Тикток", 'Обновление цены в Тик ток'),
        ("Крыша", 'Обновление цены на крыше'),
        ("Рассылка", 'Обновление цены в рассылка'),
        ("Стрим", 'Обновление цены в Стрим'),
    ]
    available_updated_links = []
    for label, field in updated_link_fields:
        value = contract.get(field)
        url = value.strip() if isinstance(value, str) else ''
        if url:
            safe_url = html.escape(url, quote=True)
            available_updated_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_updated_links:
        message += f"🔗 Обновленные ссылки: {', '.join(available_updated_links)}\n\n"

    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    if contract.get('Дожим на новую цену'):
        message += "✅ Дожим\n"

    status_value = get_status_value(contract)
    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)

    # Чек-лист невыполненных задач
    pending = build_pending_tasks(contract, status_value, analytics_mode_active)
    if pending:
        message += "\n📝 Необходимо сделать:\n" + "\n".join(pending) + "\n"

    if status_value == 'Реализовано':
        keyboard = [
            [InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML')
        return

    keyboard = []
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Создать коллаж", callback_data=f"collage_build_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)
    if status_value == 'Корректировка цены':
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        if not str(contract.get('Корректировка цены', '')).strip():
            keyboard.append([InlineKeyboardButton("Обновление цены", callback_data=f"price_adjust_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    elif analytics_mode_active:
        if not contract.get('Аналитика'):
            keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_done_{crm_id}")])
        if contract.get('Аналитика') and not contract.get('Предоставление Аналитики через 5 дней'):
            keyboard.append([InlineKeyboardButton("Аналитика через 5 дней", callback_data=f"analytics_provided_{crm_id}")])
        if contract.get('Предоставление Аналитики через 5 дней') and not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
    else:
        keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
    else:
        sent_message = await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML')
        user_id = update.effective_user.id
        user_last_messages[user_id] = sent_message


async def handle_collage_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает редактирование полей коллажа"""
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    text = update.message.text.strip()
    
    if text.lower() == 'отмена':
        # Возвращаемся к показу данных коллажа
        parts = state.replace('editing_collage_', '').split('_')
        crm_id = parts[1]
        ci = user_collage_inputs.get(user_id)
        if ci:
            user_states[user_id] = 'authenticated'
            # Создаем фиктивный query объект для показа данных
            class FakeQuery:
                async def edit_message_text(self, text, reply_markup=None):
                    await update.message.reply_text(text, reply_markup=reply_markup)
            
            fake_query = FakeQuery()
            await show_collage_data_with_edit_buttons(fake_query, ci, crm_id)
        return
    
    # Извлекаем поле и crm_id из состояния
    parts = state.replace('editing_collage_', '').split('_')
    field = parts[0]
    crm_id = parts[1]
    
    ci = user_collage_inputs.get(user_id)
    if not ci:
        await update.message.reply_text("❌ Данные коллажа не найдены. Начните заново.")
        user_states[user_id] = 'authenticated'
        return
    
    # Обновляем соответствующее поле
    if field == 'client':
        ci.client_name = text
    elif field == 'complex':
        ci.complex_name = text
    elif field == 'address':
        ci.address = text
    elif field == 'area':
        ci.area_sqm = text
    elif field == 'rooms':
        ci.rooms = text
    elif field == 'floor':
        ci.floor = text
    elif field == 'price':
        ci.price = text
    elif field == 'class':
        ci.housing_class = text
    elif field == 'rop':
        ci.rop = text
    elif field == 'phone':
        ci.agent_phone = text
    elif field == 'benefits':
        # Разбиваем текст на строки и очищаем от пустых
        benefits = [line.strip() for line in text.split('\n') if line.strip()]
        ci.benefits = benefits
    
    # Сохраняем обновленные данные
    user_collage_inputs[user_id] = ci
    user_states[user_id] = 'authenticated'
    
    # Показываем обновленные данные
    class FakeQuery:
        async def edit_message_text(self, text, reply_markup=None):
            await update.message.reply_text(text, reply_markup=reply_markup)
    
    fake_query = FakeQuery()
    await show_collage_data_with_edit_buttons(fake_query, ci, crm_id)


async def show_collage_data_with_edit_buttons(query, collage_input: CollageInput, crm_id: str):
    """Показывает данные коллажа с кнопками для редактирования"""
    
    # Формируем сообщение с данными
    message = f"✅ Данные для коллажа:\n\n"
    message += f"👤 Клиент: {collage_input.client_name or 'Не указан'}\n"
    message += f"🏢 ЖК: {collage_input.complex_name}\n"
    message += f"📍 Адрес: {collage_input.address}\n"
    message += f"📐 Площадь: {collage_input.area_sqm} м²\n"
    message += f"🏠 Комнат: {collage_input.rooms}\n"
    message += f"🏗️ Этаж: {collage_input.floor}\n"
    message += f"💰 Цена: {collage_input.price}\n"
    message += f"🏗️ Класс жилья: {collage_input.housing_class}\n"
    message += f"👤 РОП: {collage_input.rop}\n"
    message += f"📞 Телефон агента: {collage_input.agent_phone or 'Не указан'}\n\n"
    
    # Достоинства
    if collage_input.benefits:
        message += f"📋 Достоинства ({len(collage_input.benefits)} шт.):\n"
        for i, benefit in enumerate(collage_input.benefits, 1):
            message += f"   {i}. {benefit}\n"
        message += "\n"
    
    # Создаем кнопки для редактирования
    keyboard = [
        [
            InlineKeyboardButton("👤 Клиент", callback_data=f"edit_collage_client_{crm_id}"),
            InlineKeyboardButton("🏢 ЖК", callback_data=f"edit_collage_complex_{crm_id}")
        ],
        [
            InlineKeyboardButton("📍 Адрес", callback_data=f"edit_collage_address_{crm_id}"),
            InlineKeyboardButton("📐 Площадь", callback_data=f"edit_collage_area_{crm_id}")
        ],
        [
            InlineKeyboardButton("🏠 Комнаты", callback_data=f"edit_collage_rooms_{crm_id}"),
            InlineKeyboardButton("🏗️ Этаж", callback_data=f"edit_collage_floor_{crm_id}")
        ],
        [
            InlineKeyboardButton("💰 Цена", callback_data=f"edit_collage_price_{crm_id}"),
            InlineKeyboardButton("🏗️ Класс", callback_data=f"edit_collage_class_{crm_id}")
        ],
        [
            InlineKeyboardButton("👤 РОП", callback_data=f"edit_collage_rop_{crm_id}"),
            InlineKeyboardButton("📞 Телефон", callback_data=f"edit_collage_phone_{crm_id}")
        ],
        [
            InlineKeyboardButton("📋 Достоинства", callback_data=f"edit_collage_benefits_{crm_id}")
        ],
        [
            InlineKeyboardButton("✅ Продолжить с фото", callback_data=f"collage_proceed_{crm_id}"),
        ],
        [
            InlineKeyboardButton("❌ Отмена", callback_data=f"contract_{crm_id}")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup)


def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


