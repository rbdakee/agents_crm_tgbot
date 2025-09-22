import logging
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

logger = logging.getLogger(__name__)

# User-scoped state and cache structures
user_states: Dict[int, str] = {}
user_contracts: Dict[int, List[Dict]] = {}
user_current_page: Dict[int, int] = {}
user_search_results: Dict[int, List[Dict]] = {}
user_current_search_page: Dict[int, int] = {}
user_last_messages: Dict[int, object] = {}


# Utilities
PHONE_CLEAN_RE = re.compile(r"[\d\+\-\(\)\s]+")

async def show_loading(query) -> None:
    try:
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
    except Exception:
        pass


def clean_client_name(client_info: str) -> str:
    cleaned = PHONE_CLEAN_RE.sub(" ", client_info)
    cleaned = " ".join(cleaned.split())
    return cleaned.strip()


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
                "Добро пожаловать в CRM систему!\n\n"
                "Для входа в систему введите ваш номер телефона в формате:\n"
                "87777777777"
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
            "Добро пожаловать в CRM систему!\n\n"
            "Для входа в систему введите ваш номер телефона в формате:\n"
            "87777777777"
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
    message += f"📌 Статус: {contract.get('Статус объекта', 'Размещено')}\n"
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
        url = (contract.get(field) or '').strip()
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

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
    status_value = contract.get('Статус объекта', 'Размещено')
    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)

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
        keyboard.append([InlineKeyboardButton("Коллаж", callback_data=f"action_collage_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    if status_value == 'Корректировка цены':
        # Кнопки для статуса "Корректировка цены"
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
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

    elif data.startswith("action_collage_"):
        crm_id = data.replace("action_collage_", "")
        await show_loading(query)
        success = await crm.update_contract(crm_id, {"collage": True})
        if success:
            await query.answer("Коллаж отмечен как выполненный")
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


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    if state == 'waiting_phone':
        await handle_phone(update, context)
    elif state.startswith('waiting_price_') or state.startswith('waiting_link_'):
        await handle_price_input(update, context)
    elif state == 'waiting_client_search':
        await handle_client_search(update, context)


async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_states.get(user_id) != 'waiting_phone':
        return
    phone = update.message.text.strip()
    agent_name = crm.get_agent_by_phone(phone)
    if agent_name:
        user_states[user_id] = 'authenticated'
        context.user_data['agent_name'] = agent_name
        context.user_data['phone'] = phone
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
            "Номер телефона не найден в системе. "
            "Пожалуйста, проверьте правильность ввода или обратитесь к администратору."
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
    message += f"📌 Статус: {contract.get('Статус объекта', 'Размещено')}\n"
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
        url = (contract.get(field) or '').strip()
        if url:
            safe_url = html.escape(url, quote=True)
            available_links.append(f"<a href=\"{safe_url}\">{label}</a>")
    if available_links:
        message += f"🔗 Ссылки: {', '.join(available_links)}\n\n"

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

    status_value = contract.get('Статус объекта', 'Размещено')

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
        keyboard.append([InlineKeyboardButton("Коллаж", callback_data=f"action_collage_{crm_id}")])
    if contract.get('Коллаж') and not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

    analytics_mode_active = context.user_data.get('analytics_mode') == str(crm_id)
    if status_value == 'Корректировка цены':
        if not contract.get('Дожим на новую цену'):
            keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
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


def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


