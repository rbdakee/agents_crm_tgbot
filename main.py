import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
from typing import Dict, List, Optional

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Импорт конфигурации и базы данных
from config import BOT_TOKEN, BOT_USERNAME, CONTRACTS_PER_PAGE
from database import crm

# Состояния пользователей
user_states = {}
user_contracts = {}
user_current_page = {}
user_search_results = {}  # Результаты поиска по клиенту
user_current_search_page = {}  # Текущая страница результатов поиска
user_last_messages = {}  # Храним последние сообщения для удаления

def clean_client_name(client_info: str) -> str:
    """Убирает номера телефонов из строки с именем клиента"""
    import re
    # Убираем все цифры и символы +, -, (, ), пробелы
    cleaned = re.sub(r'[\d\+\-\(\)\s]+', ' ', client_info)
    # Убираем лишние пробелы
    cleaned = ' '.join(cleaned.split())
    return cleaned.strip()

def get_agent_phone_by_name(agent_name: str) -> str:
    """Получает номер телефона агента по имени"""
    for phone, name in crm.agents.items():
        if name == agent_name:
            return phone
    return "N/A"


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = update.effective_user.id
    
    # Проверяем параметры deep-link
    if context.args and context.args[0].startswith('crm_'):
        crm_id = context.args[0].replace('crm_', '')
        
        # Проверяем, авторизован ли пользователь
        if user_states.get(user_id) == 'authenticated' and context.user_data.get('agent_name'):
            agent_name = context.user_data.get('agent_name')
            
            # Удаляем сообщение пользователя
            try:
                await update.message.delete()
            except:
                pass  # Игнорируем ошибки удаления
            
            # Удаляем предыдущее сообщение со списком объектов, если оно есть
            if user_id in user_last_messages:
                try:
                    await user_last_messages[user_id].delete()
                    del user_last_messages[user_id]
                except:
                    pass  # Игнорируем ошибки удаления
            
            # Очищаем данные поиска, если они есть
            if user_id in user_search_results:
                del user_search_results[user_id]
            if user_id in user_current_search_page:
                del user_current_search_page[user_id]
            
            # Ищем контракт
            contract = crm.search_contract_by_crm_id(crm_id, agent_name)
            
            if contract:
                # Показываем детали контракта
                await show_contract_detail_by_contract(update, context, contract)
            else:
                # Показываем главное меню
                keyboard = [
                    [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
                    [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
                    [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                agent_phone = get_agent_phone_by_name(agent_name)
                await update.message.reply_text(
                    f"Агент: {agent_name}\n"
                    f"Номер: {agent_phone}\n\n"
                    "Выберите действие:",
                    reply_markup=reply_markup
                )
        else:
            # Пользователь не авторизован, сохраняем CRM ID для после авторизации
            context.user_data['pending_crm_id'] = crm_id
            user_states[user_id] = 'waiting_phone'
            
            await update.message.reply_text(
                "Добро пожаловать в CRM систему!\n\n"
                "Для входа в систему введите ваш номер телефона в формате:\n"
                "87777777777"
            )
        return
    
    # Проверяем, авторизован ли пользователь
    if user_states.get(user_id) == 'authenticated' and context.user_data.get('agent_name'):
        # Пользователь уже авторизован, показываем главное меню
        agent_name = context.user_data.get('agent_name')
        
        keyboard = [
            [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
            [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
            [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        agent_phone = get_agent_phone_by_name(agent_name)
        await update.message.reply_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
    else:
        # Пользователь не авторизован, просим логин
        user_states[user_id] = 'waiting_phone'
        
        await update.message.reply_text(
            "Добро пожаловать в CRM систему!\n\n"
            "Для входа в систему введите ваш номер телефона в формате:\n"
            "87777777777"
        )

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /logout"""
    user_id = update.effective_user.id
    
    # Очищаем данные пользователя
    user_states[user_id] = 'waiting_phone'
    context.user_data.clear()
    
    await update.message.reply_text(
        "Вы вышли из системы.\n\n"
        "Для входа введите команду /start"
    )

async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода номера телефона"""
    user_id = update.effective_user.id
    
    if user_states.get(user_id) != 'waiting_phone':
        return
    
    phone = update.message.text.strip()
    agent_name = crm.get_agent_by_phone(phone)
    
    if agent_name:
        user_states[user_id] = 'authenticated'
        context.user_data['agent_name'] = agent_name
        context.user_data['phone'] = phone
        
        keyboard = [
            [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
            [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
            [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Проверяем, есть ли отложенный CRM ID
        pending_crm_id = context.user_data.get('pending_crm_id')
        if pending_crm_id:
            # Очищаем отложенный CRM ID
            del context.user_data['pending_crm_id']
            
            # Показываем сообщение о загрузке
            loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
            
            # Ищем контракт
            contract = crm.search_contract_by_crm_id(pending_crm_id, agent_name)
            
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
                    reply_markup=reply_markup
                )
        else:
            agent_phone = get_agent_phone_by_name(agent_name)
            await update.message.reply_text(
                f"Агент: {agent_name}\n"
                f"Номер: {agent_phone}\n\n"
                "Выберите действие:",
                reply_markup=reply_markup
            )
    else:
        await update.message.reply_text(
            "Номер телефона не найден в системе. "
            "Пожалуйста, проверьте правильность ввода или обратитесь к администратору."
        )

async def my_contracts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает сделки агента с пагинацией"""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    agent_name = context.user_data.get('agent_name')
    
    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return
    
    # Показываем сообщение о загрузке
    await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
    
    # Получаем контракты агента
    contracts = crm.get_contracts_by_agent(agent_name)
    user_contracts[user_id] = contracts
    user_current_page[user_id] = 0
    
    if not contracts:
        await query.edit_message_text("У вас нет активных объектов")
        return
    
    # Показываем первую страницу
    await show_contracts_page(query, contracts, 0)

async def show_contracts_page(query, contracts: List[Dict], page: int):
    """Показывает страницу с контрактами"""
    contracts_per_page = CONTRACTS_PER_PAGE
    start_idx = page * contracts_per_page
    end_idx = start_idx + contracts_per_page
    page_contracts = contracts[start_idx:end_idx]
    
    message = "Ваши объекты:\n\n"
    
    keyboard = []
    for i, contract in enumerate(page_contracts):
        crm_id = contract.get('CRM ID', 'N/A')
        client_info = contract.get('Имя клиента и номер', 'N/A')
        client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')
        
        message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message += f"   Клиент: {client_name}\n"
        message += f"   Адрес: {address}\n"
        message += f"   Истекает: {expires}\n\n"
    
    # Добавляем кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"prev_page_{page}"))
    if end_idx < len(contracts):
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"next_page_{page}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Добавляем кнопку главного меню
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    edited_message = await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')
    
    # Сохраняем ссылку на сообщение для возможного удаления
    user_id = query.from_user.id
    user_last_messages[user_id] = edited_message

async def show_search_results_page(message_or_query, contracts: List[Dict], page: int, client_name: str):
    """Показывает страницу с результатами поиска"""
    contracts_per_page = CONTRACTS_PER_PAGE
    start_idx = page * contracts_per_page
    end_idx = start_idx + contracts_per_page
    page_contracts = contracts[start_idx:end_idx]
    
    message_text = f"Найдено {len(contracts)} контрактов для клиента '{client_name}':\n\n"
    
    keyboard = []
    for i, contract in enumerate(page_contracts):
        crm_id = contract.get('CRM ID', 'N/A')
        client_info = contract.get('Имя клиента и номер', 'N/A')
        client_name_clean = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')
        
        message_text += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message_text += f"   Клиент: {client_name_clean}\n"
        message_text += f"   Адрес: {address}\n"
        message_text += f"   Истекает: {expires}\n\n"
    
    # Добавляем кнопки навигации
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"search_prev_page_{page}"))
    if end_idx < len(contracts):
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"search_next_page_{page}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Добавляем кнопку главного меню
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Проверяем тип объекта и используем соответствующий метод
    if hasattr(message_or_query, 'edit_message_text'):
        # Это CallbackQuery
        edited_message = await message_or_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        # Сохраняем ссылку на сообщение для возможного удаления
        user_id = message_or_query.from_user.id
        user_last_messages[user_id] = edited_message
    else:
        # Это Message
        edited_message = await message_or_query.edit_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        # Сохраняем ссылку на сообщение для возможного удаления
        user_id = message_or_query.from_user.id
        user_last_messages[user_id] = edited_message

async def show_contract_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает детали контракта"""
    query = update.callback_query
    await query.answer()
    
    # Получаем имя агента из контекста
    agent_name = context.user_data.get('agent_name')
    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return
    
    # Используем кешированные данные агента для поиска контракта
    contract = crm.search_contract_by_crm_id(crm_id, agent_name)
    
    if not contract:
        await query.edit_message_text("Контракт не найден среди ваших сделок")
        return
    
    # Формируем сообщение с деталями
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в объекте', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"
    
    # Добавляем статусы если они True
    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    
    # Создаем кнопки
    keyboard = []
    
    # Кнопки для действий (показываем только если False)
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Коллаж", callback_data=f"action_collage_{crm_id}")])
    
    if not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Ссылки", callback_data=f"links_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Корректировка цены", callback_data=f"price_adjust_{crm_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback запросов"""
    query = update.callback_query
    data = query.data
    
    if data == "my_contracts":
        await my_contracts(update, context)
    
    elif data.startswith("contract_"):
        crm_id = data.replace("contract_", "")
        await show_contract_detail(update, context, crm_id)
    
    elif data.startswith("prev_page_"):
        page = int(data.replace("prev_page_", ""))
        user_id = update.effective_user.id
        contracts = user_contracts.get(user_id, [])
        await show_contracts_page(query, contracts, page - 1)
        user_current_page[user_id] = page - 1
    
    elif data.startswith("next_page_"):
        page = int(data.replace("next_page_", ""))
        user_id = update.effective_user.id
        contracts = user_contracts.get(user_id, [])
        await show_contracts_page(query, contracts, page + 1)
        user_current_page[user_id] = page + 1
    
    elif data.startswith("search_prev_page_"):
        page = int(data.replace("search_prev_page_", ""))
        user_id = update.effective_user.id
        contracts = user_search_results.get(user_id, [])
        client_name = context.user_data.get('last_search_client', '')
        await show_search_results_page(query, contracts, page - 1, client_name)
        user_current_search_page[user_id] = page - 1
    
    elif data.startswith("search_next_page_"):
        page = int(data.replace("search_next_page_", ""))
        user_id = update.effective_user.id
        contracts = user_search_results.get(user_id, [])
        client_name = context.user_data.get('last_search_client', '')
        await show_search_results_page(query, contracts, page + 1, client_name)
        user_current_search_page[user_id] = page + 1
    
    elif data.startswith("action_collage_"):
        crm_id = data.replace("action_collage_", "")
        # Показываем сообщение о загрузке
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
        
        success = crm.update_contract(crm_id, {"collage": True})
        if success:
            await query.answer("Коллаж отмечен как выполненный")
            # Обновляем кеш агента и показываем обновленный контракт
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                crm.refresh_agent_cache(agent_name)
                contract = crm.search_contract_by_crm_id(crm_id, agent_name)
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
        # Показываем сообщение о загрузке
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
        
        success = crm.update_contract(crm_id, {"updatedCollage": True})
        if success:
            await query.answer("Проф коллаж отмечен как выполненный")
            # Обновляем кеш агента и показываем обновленный контракт
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                crm.refresh_agent_cache(agent_name)
                contract = crm.search_contract_by_crm_id(crm_id, agent_name)
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
        
        # Показываем сообщение о загрузке
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
        
        # Получаем текущее значение показа из кешированных данных
        contract = crm.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await query.answer("Контракт не найден", show_alert=True)
            return
        
        current_show = int(contract.get('Показ', 0))
        
        success = crm.update_contract(crm_id, {"show": current_show + 1})
        if success:
            await query.answer(f"Показ увеличен до {current_show + 1}")
            # Обновляем кеш агента и показываем обновленный контракт
            crm.refresh_agent_cache(agent_name)
            contract = crm.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                await show_contract_detail_by_contract(update, context, contract)
            else:
                await query.edit_message_text("Контракт не найден")
        else:
            await query.answer("Ошибка при обновлении", show_alert=True)
    
    elif data.startswith("analytics_done_"):
        crm_id = data.replace("analytics_done_", "")
        # Показываем сообщение о загрузке
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
        
        success = crm.update_contract(crm_id, {"analytics": True})
        
        if success:
            await query.answer("Аналитика отмечена как сделанная")
            # Обновляем кеш агента и показываем обновленный контракт
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                crm.refresh_agent_cache(agent_name)
                contract = crm.search_contract_by_crm_id(crm_id, agent_name)
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
        # Показываем сообщение о загрузке
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
        
        success = crm.update_contract(crm_id, {"analyticsIn5Days": True})
        
        if success:
            await query.answer("Аналитика отмечена как предоставленная")
            # Обновляем кеш агента и показываем обновленный контракт
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                crm.refresh_agent_cache(agent_name)
                contract = crm.search_contract_by_crm_id(crm_id, agent_name)
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
        
        # Получаем контракт из кешированных данных
        contract = crm.search_contract_by_crm_id(crm_id, agent_name)
        
        if not contract:
            await query.answer("Контракт не найден", show_alert=True)
            return
        
        # Получаем текущие значения аналитики
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
        await query.edit_message_text("Выберите действие по аналитике:", reply_markup=reply_markup)
    
    
    elif data.startswith("links_"):
        crm_id = data.replace("links_", "")
        await show_links_menu(update, context, crm_id)
    
    elif data.startswith("price_adjust_"):
        crm_id = data.replace("price_adjust_", "")
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_price_{crm_id}'
        await query.edit_message_text(f"Введите новую цену для CRM ID {crm_id}:")
    
    elif data.startswith("view_links_"):
        crm_id = data.replace("view_links_", "")
        await show_links_view(update, context, crm_id)
    
    elif data.startswith("add_link_"):
        crm_id = data.replace("add_link_", "")
        await show_add_link_menu(update, context, crm_id)
    
    elif data.startswith("link_type_"):
        # Убираем "link_type_" и разбираем оставшуюся часть
        remaining = data.replace("link_type_", "")
        parts = remaining.split("_")
        crm_id = parts[0]
        # Объединяем все части после crm_id обратно в link_type
        link_type = "_".join(parts[1:])
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_link_{crm_id}_{link_type}'
        
        await query.edit_message_text(f"Введите ссылку для {link_type}:")
    
    elif data == "search_client":
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_client_search'
        await query.edit_message_text("Введите имя клиента для поиска:")
    
    elif data == "main_menu":
        user_id = update.effective_user.id
        agent_name = context.user_data.get('agent_name')
        
        if not agent_name:
            await query.edit_message_text("Ошибка: агент не найден")
            return
        
        keyboard = [
            [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
            [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
            [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        agent_phone = get_agent_phone_by_name(agent_name)
        await query.edit_message_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
    
    elif data == "logout_confirm":
        user_id = update.effective_user.id
        
        # Очищаем данные пользователя
        user_states[user_id] = 'waiting_phone'
        context.user_data.clear()
        
        # Очищаем сохраненные сообщения
        if user_id in user_last_messages:
            del user_last_messages[user_id]
        
        await query.edit_message_text(
            "Вы вышли из системы.\n\n"
            "Для входа введите команду /start"
        )

async def show_links_view(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает все ссылки контракта"""
    query = update.callback_query
    await query.answer()
    
    # Получаем контракт
    all_contracts = crm.get_all_contracts()
    contract = None
    
    for c in all_contracts:
        if str(c.get('CRM ID', '')) == str(crm_id):
            contract = c
            break
    
    if not contract:
        await query.edit_message_text("Контракт не найден")
        return
    
    message = f"🔗 Ссылки для CRM ID {crm_id}:\n\n"
    
    links = {
        "Крыша": (contract.get('Загрузка на крышу', ''), contract.get('Обновление цены на крыше', '')),
        "Инстаграм": (contract.get('Инстаграм', ''), contract.get('Обновление цены в инстаграм', '')),
        "Тикток": (contract.get('Тик ток', ''), contract.get('Обновление цены в Тик ток', '')),
        "Рассылка": (contract.get('Рассылка', ''), contract.get('Обновление цены в рассылка', '')),
        "Стрим": (contract.get('Стрим', ''), contract.get('Обновление цены в Стрим', ''))
    }
    
    # Сначала показываем основные ссылки
    for platform, (primary_link, secondary_link) in links.items():
        if primary_link and primary_link.strip():
            message += f"📱 {platform}: {primary_link}\n"
        else:
            message += f"📱 {platform}: N/A\n"
    
    message += "\n"  # Добавляем пустую строку для разделения
    
    # Потом показываем ссылки после обновления
    for platform, (primary_link, secondary_link) in links.items():
        if secondary_link and secondary_link.strip():
            message += f"📱 {platform} (обновление): {secondary_link}\n"
        else:
            message += f"📱 {platform} (обновление): N/A\n"
    
    keyboard = [
        [InlineKeyboardButton("🔙 Назад к ссылкам", callback_data=f"links_{crm_id}")],
        [InlineKeyboardButton("🔙 К контракту", callback_data=f"contract_{crm_id}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message, reply_markup=reply_markup)

async def show_add_link_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает меню добавления ссылки"""
    query = update.callback_query
    await query.answer()
    
    # Получаем текущий контракт для проверки заполненных полей
    agent_name = context.user_data.get('agent_name')
    contract = None
    if agent_name:
        # Принудительно обновляем кеш агента для актуальных данных
        crm.refresh_agent_cache(agent_name)
        # Получаем контракт из обновленного кеша
        contract = crm.search_contract_by_crm_id(crm_id, agent_name)
    
    # Функция для создания кнопки с галочкой
    def create_button(text, callback_data, has_link=False):
        button_text = f"✅ {text}" if has_link else text
        return InlineKeyboardButton(button_text, callback_data=callback_data)
    
    # Функция для проверки заполненности поля
    def is_field_filled(value):
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        return bool(value)
    
    # Проверяем, какие поля заполнены
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
        # Основные ссылки
        [create_button("Крыша", f"link_type_{crm_id}_krisha", is_field_filled(links_status['krisha']))],
        [create_button("Инстаграм", f"link_type_{crm_id}_instagram", is_field_filled(links_status['instagram']))],
        [create_button("Тикток", f"link_type_{crm_id}_tiktok", is_field_filled(links_status['tiktok']))],
        [create_button("Рассылка", f"link_type_{crm_id}_mailing", is_field_filled(links_status['mailing']))],
        [create_button("Стрим", f"link_type_{crm_id}_stream", is_field_filled(links_status['stream']))],
        
        # Ссылки после обновления
        [create_button("Крыша (обновление)", f"link_type_{crm_id}_krisha_update", is_field_filled(links_status['krisha_update']))],
        [create_button("Инстаграм (обновление)", f"link_type_{crm_id}_instagram_update", is_field_filled(links_status['instagram_update']))],
        [create_button("Тикток (обновление)", f"link_type_{crm_id}_tiktok_update", is_field_filled(links_status['tiktok_update']))],
        [create_button("Рассылка (обновление)", f"link_type_{crm_id}_mailing_update", is_field_filled(links_status['mailing_update']))],
        [create_button("Стрим (обновление)", f"link_type_{crm_id}_stream_update", is_field_filled(links_status['stream_update']))],
        
        # Навигационные кнопки
        [InlineKeyboardButton("🔙 Назад к ссылкам", callback_data=f"links_{crm_id}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите тип ссылки для добавления:", reply_markup=reply_markup)

async def show_links_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает меню управления ссылками"""
    query = update.callback_query
    await query.answer()
    
    keyboard = [
        [InlineKeyboardButton("Посмотреть ссылки", callback_data=f"view_links_{crm_id}")],
        [InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Управление ссылками:", reply_markup=reply_markup)

async def handle_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода новой цены и ссылок"""
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    
    if state.startswith('waiting_price_'):
        crm_id = state.replace('waiting_price_', '')
        new_price = update.message.text.strip()
        
        # Показываем сообщение о загрузке
        loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
        
        success = crm.update_contract(crm_id, {"priceAdjustment": new_price})
        
        if success:
            await loading_msg.edit_text(f"Цена успешно обновлена на: {new_price}")
            # Показываем обновленную информацию о контракте
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = crm.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
            user_states[user_id] = 'authenticated'
        else:
            await loading_msg.edit_text("Ошибка при обновлении цены")
    
    elif state.startswith('waiting_link_'):
        # Обработка ввода ссылки
        remaining = state.replace('waiting_link_', '')
        parts = remaining.split('_')
        crm_id = parts[0]
        link_type = '_'.join(parts[1:])
        link_url = update.message.text.strip()
        
        # Показываем сообщение о загрузке
        loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
        
        # Получаем текущий контракт
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await loading_msg.edit_text("Ошибка: агент не найден")
            return
            
        contract = crm.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await loading_msg.edit_text("Контракт не найден")
            return
        
        # Определяем, в какое поле записывать ссылку
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
            'stream_update': 'priceUpdateStream'
        }
        
        field_name = field_mapping.get(link_type)
        if not field_name:
            await loading_msg.edit_text("Неизвестный тип ссылки")
            return
        
        # Определяем название для отображения
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
            'stream_update': 'Стрим (обновление)'
        }
        
        field_display_name = display_names.get(link_type, link_type)
        
        success = crm.update_contract(crm_id, {field_name: link_url})
        if success:
            await loading_msg.edit_text(f"Ссылка {field_display_name} успешно добавлена")
            # Принудительно обновляем кеш агента
            crm.refresh_agent_cache(agent_name)
            # Получаем обновленный контракт
            contract = crm.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                await show_contract_detail_by_contract(update, context, contract)
        else:
            await loading_msg.edit_text("Ошибка при добавлении ссылки")
        
        user_states[user_id] = 'authenticated'

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    
    
    if state == 'waiting_phone':
        await handle_phone(update, context)
    elif state.startswith('waiting_price_') or state.startswith('waiting_link_'):
        await handle_price_input(update, context)
    elif state == 'waiting_client_search':
        await handle_client_search(update, context)


async def handle_client_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик поиска по имени клиента"""
    user_id = update.effective_user.id
    client_name = update.message.text.strip()
    agent_name = context.user_data.get('agent_name')
    
    if not agent_name:
        await update.message.reply_text("Ошибка: агент не найден")
        user_states[user_id] = 'authenticated'
        return
    
    # Показываем сообщение о загрузке
    loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
    
    contracts = crm.search_contracts_by_client_name(client_name, agent_name)
    
    if contracts:
        if len(contracts) == 1:
            await show_contract_detail_by_contract(update, context, contracts[0])
        else:
            # Сохраняем результаты поиска для пагинации
            user_search_results[user_id] = contracts
            user_current_search_page[user_id] = 0
            context.user_data['last_search_client'] = client_name
            
            # Показываем первую страницу результатов
            await show_search_results_page(loading_msg, contracts, 0, client_name)
    else:
        await loading_msg.edit_text(f"Контракты для клиента '{client_name}' не найдены среди ваших сделок")
        
        # Показываем главное меню
        keyboard = [
            [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
            [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
            [InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        agent_phone = get_agent_phone_by_name(agent_name)
        await update.message.reply_text(
            f"Агент: {agent_name}\n"
            f"Номер: {agent_phone}\n\n"
            "Выберите действие:",
            reply_markup=reply_markup
        )
    
    user_states[user_id] = 'authenticated'

async def show_contract_detail_by_contract_edit(message, context: ContextTypes.DEFAULT_TYPE, contract: Dict):
    """Показывает детали контракта, редактируя существующее сообщение"""
    crm_id = contract.get('CRM ID', 'N/A')
    
    # Формируем сообщение с деталями
    message_text = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message_text += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message_text += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message_text += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message_text += f"📞 Клиент: {client_name}\n"
    message_text += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message_text += f"💰 Цена: {contract.get('Цена указанная в объекте', 'N/A')}\n"
    message_text += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message_text += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message_text += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"
    
    # Добавляем статусы если они True
    if contract.get('Коллаж'):
        message_text += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message_text += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message_text += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message_text += "✅ Аналитика-предоставлено\n"
    
    # Создаем кнопки
    keyboard = []
    
    # Кнопки для действий (показываем только если False)
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Коллаж", callback_data=f"action_collage_{crm_id}")])
    
    if not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Ссылки", callback_data=f"links_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Корректировка цены", callback_data=f"price_adjust_{crm_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await message.edit_text(message_text, reply_markup=reply_markup)

async def show_contract_detail_by_contract(update: Update, context: ContextTypes.DEFAULT_TYPE, contract: Dict):
    """Показывает детали контракта по объекту контракта"""
    crm_id = contract.get('CRM ID', 'N/A')
    
    # Формируем сообщение с деталями
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {contract.get('Дата подписания', 'N/A')}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name = clean_client_name(client_info) if client_info != 'N/A' else 'N/A'
    message += f"📞 Клиент: {client_name}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в объекте', 'N/A')}\n"
    message += f"⏰ Истекает: {contract.get('Истекает', 'N/A')}\n"
    message += f"📊 Корректировка цены: {contract.get('Корректировка цены', 'N/A')}\n"
    message += f"👁️ Показы: {contract.get('Показ', 0)}\n\n"
    
    # Добавляем статусы если они True
    if contract.get('Коллаж'):
        message += "✅ Коллаж\n"
    if contract.get('Обновленный колаж'):
        message += "✅ Проф Коллаж\n"
    if contract.get('Аналитика'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('Предоставление Аналитики через 5 дней'):
        message += "✅ Аналитика-предоставлено\n"
    
    # Создаем кнопки
    keyboard = []
    
    # Кнопки для действий (показываем только если False)
    if not contract.get('Коллаж'):
        keyboard.append([InlineKeyboardButton("Коллаж", callback_data=f"action_collage_{crm_id}")])
    
    if not contract.get('Обновленный колаж'):
        keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
    
    keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Ссылки", callback_data=f"links_{crm_id}")])
    keyboard.append([InlineKeyboardButton("Корректировка цены", callback_data=f"price_adjust_{crm_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data="my_contracts")])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Проверяем, есть ли callback_query (для inline кнопок) или message (для обычных сообщений)
    if update.callback_query:
        # Для callback query редактируем существующее сообщение
        await update.callback_query.edit_message_text(message, reply_markup=reply_markup)
    else:
        # Для обычных сообщений отправляем новое
        sent_message = await update.message.reply_text(message, reply_markup=reply_markup)
        # Сохраняем ссылку на новое сообщение для возможного удаления
        user_id = update.effective_user.id
        user_last_messages[user_id] = sent_message


def main():
    """Основная функция запуска бота"""
    if BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("Пожалуйста, установите BOT_TOKEN в переменных окружения")
        return
    
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    # Запускаем бота
    print("Бот запущен...")
    application.run_polling()

if __name__ == '__main__':
    main()
