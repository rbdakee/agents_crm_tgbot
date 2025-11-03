import logging
import asyncio
import os
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
from database_postgres import get_db_manager
from api_client import get_collage_data_from_api, CollageInput, APIClient
from collage import render_collage_to_image
from sqlalchemy import text

logger = logging.getLogger(__name__)

"""
Удалена устаревшая версия clean_client_name; используется расширенная ниже.
"""

# User-scoped state structures
user_states: Dict[int, str] = {}
user_collage_inputs: Dict[int, CollageInput] = {}
user_contracts: Dict[int, List[Dict]] = {}
user_current_page: Dict[int, int] = {}
user_search_results: Dict[int, List[Dict]] = {}
user_current_search_page: Dict[int, int] = {}
user_last_messages: Dict[int, object] = {}
user_pending_downloads: Dict[int, int] = {}


# Role constants and helpers
ROLE_MOP = 'МОП'
ROLE_ROP = 'РОП'
ROLE_DD = 'ДД'

def set_user_role(context: ContextTypes.DEFAULT_TYPE, role: str) -> None:
    context.user_data['role'] = role

def get_user_role(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.user_data.get('role')

DD_ALLOWED: Dict[str, str] = {
    # Имя: телефон (10 цифр)
    'Мирасхан': '7055471077',
    'Рустам': '7752152555',
    'Айжан': '7058155000',
    'Айнамкоз': '7477777719',
    'Бекзат': '7002706800',
}

# Обратное отображение: телефон (10 цифр) -> имя ДД
PHONE_TO_DD_NAME: Dict[str, str] = {v: k for k, v in DD_ALLOWED.items()}

def _normalize_to_10_digits(phone: str) -> str:
    digits = ''.join(c for c in (phone or '') if c.isdigit())
    if len(digits) == 11 and digits.startswith('7'):
        return digits[1:]
    if len(digits) == 10:
        return digits
    return digits[-10:] if len(digits) > 10 else digits

def is_dd_allowed(context: ContextTypes.DEFAULT_TYPE) -> bool:
    phone = context.user_data.get('phone')
    if not phone:
        return False
    norm = _normalize_to_10_digits(phone)
    return norm in set(DD_ALLOWED.values())

def build_role_select_keyboard(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton(ROLE_MOP, callback_data=f"select_role_{ROLE_MOP}")],
        [InlineKeyboardButton(ROLE_ROP, callback_data=f"select_role_{ROLE_ROP}")],
    ]
    if is_dd_allowed(context):
        keyboard.append([InlineKeyboardButton(ROLE_DD, callback_data=f"select_role_{ROLE_DD}")])
    return InlineKeyboardMarkup(keyboard)

def build_main_menu_keyboard_by_role(context: ContextTypes.DEFAULT_TYPE) -> InlineKeyboardMarkup:
    role = get_user_role(context)
    keyboard = []
    if role == ROLE_ROP:
        keyboard.append([InlineKeyboardButton("Мои МОП-ы", callback_data="my_mops")])
    if role == ROLE_DD:
        keyboard.append([InlineKeyboardButton("Мои РОП-ы", callback_data="my_rops")])
        keyboard.append([InlineKeyboardButton("Мои МОП-ы", callback_data="my_mops")])
    keyboard.append([InlineKeyboardButton("Мои объекты", callback_data="my_contracts")])
    if role in {ROLE_ROP, ROLE_DD}:
        keyboard.append([InlineKeyboardButton("Поиск", callback_data="search")])
    else:
        keyboard.append([InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")])
    keyboard.append([InlineKeyboardButton("Поменять роль", callback_data="change_role")])
    keyboard.append([InlineKeyboardButton("🚪 Выйти", callback_data="logout_confirm")])
    return InlineKeyboardMarkup(keyboard)


# Utilities
PHONE_CLEAN_RE = re.compile(r"[\d\+\-\(\)\s]+")
# Регулярное выражение для очистки имени клиента - оставляем только буквы, пробелы, дефисы и апострофы
NAME_CLEAN_RE = re.compile(r"[^а-яёА-ЯЁa-zA-Z\s\-\']+", re.UNICODE)

async def show_loading(query) -> None:
    try:
        await query.edit_message_text("Идет загрузка. Пожалуйста подождите...")
    except Exception:
        pass


def value_is_filled(value) -> bool:
    """Возвращает True, если значение реально заполнено (учитывает None из SQL)."""
    if value is None:
        return False
    if isinstance(value, str):
        s = value.strip()
        # Строковое 'None' тоже считаем пустым
        return bool(s) and s.lower() != 'none'
    return bool(value)


def format_date_ddmmyyyy(value) -> str:
    """Форматирует значение даты в dd/mm/yyyy. При невозможности — возвращает исходное."""
    if value is None:
        return "N/A"
    try:
        from datetime import date, datetime
        if isinstance(value, datetime):
            d = value.date()
            return d.strftime('%d/%m/%Y')
        if isinstance(value, date):
            return value.strftime('%d/%m/%Y')
        # Строковые форматы
        s = str(value).strip()
        if not s or s.lower() == 'none':
            return "N/A"
        # Попробуем несколько распространенных форматов
        from datetime import datetime as dt
        for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S'):
            try:
                return dt.strptime(s, fmt).strftime('%d/%m/%Y')
            except ValueError:
                continue
        return s
    except Exception:
        return str(value)


async def send_photo_with_retry(bot, chat_id: int, photo_path: str, caption: str, reply_markup=None, attempts: int = 3, delay: float = 2.0):
    """Отправляет фото с повторными попытками при временных ошибках."""
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            with open(photo_path, 'rb') as photo:
                await bot.send_photo(
                    chat_id=chat_id,
                    photo=photo,
                    caption=caption,
                    reply_markup=reply_markup,
                )
            return True
        except Exception as e:
            last_err = e
            try:
                await asyncio.sleep(delay)
            except Exception:
                pass
    logger.error(f"Не удалось отправить фото после {attempts} попыток: {last_err}")
    return False


async def cleanup_collage_files(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Очищает временные файлы коллажа"""
    try:
        # Удаляем временный файл коллажа
        if 'collage_temp_path' in context.user_data:
            temp_path = context.user_data['collage_temp_path']
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    logger.info(f"Удален временный файл коллажа: {temp_path}")
            except Exception as e:
                logger.warning(f"Не удалось удалить временный файл {temp_path}: {e}")
            finally:
                del context.user_data['collage_temp_path']
        
        # Удаляем загруженные фотографии
        if user_id in user_collage_inputs:
            collage_input = user_collage_inputs[user_id]
            if hasattr(collage_input, 'photo_paths') and collage_input.photo_paths:
                for photo_path in collage_input.photo_paths:
                    try:
                        if os.path.exists(photo_path):
                            os.remove(photo_path)
                            logger.info(f"Удалена временная фотография: {photo_path}")
                    except Exception as e:
                        logger.warning(f"Не удалось удалить фотографию {photo_path}: {e}")
                collage_input.photo_paths = []
            
            # Удаляем объект коллажа из памяти
            del user_collage_inputs[user_id]
            
    except Exception as e:
        logger.error(f"Ошибка при очистке файлов коллажа: {e}")


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
    value = contract.get('status')
    if isinstance(value, str):
        value = value.strip()
    if not value:
        value = 'Размещено'
    return value


def build_pending_tasks(contract: Dict, status_value: str, analytics_mode_active: bool) -> List[str]:
    pending: List[str] = []
    # Базовые задачи
    if not contract.get('collage'):
        pending.append("❌ Коллаж")
    if contract.get('collage') and not contract.get('prof_collage'):
        pending.append("❌ Проф Коллаж")

    # Проверка наличия базовых ссылок первого этапа
    def is_filled(value) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        return bool(value)

    base_links_fields = [
        ("Крыша", 'krisha'),
        ("Инстаграм", 'instagram'),
        ("Тикток", 'tiktok'),
        ("Рассылка", 'mailing'),
        ("Стрим", 'stream'),
    ]
    missing_base_links = [label for (label, field) in base_links_fields if not is_filled(contract.get(field))]
    if missing_base_links:
        pending.append("❌ Добавить ссылки: " + ", ".join(missing_base_links))

    # Задачи по режимам/статусам
    if status_value == 'Реализовано':
        return pending

    if analytics_mode_active:
        if not contract.get('analytics'):
            pending.append("❌ Аналитика")
        elif not contract.get('provide_analytics'):
            pending.append("❌ Аналитика через 5 дней")
        if contract.get('provide_analytics') and not contract.get('push_for_price'):
            pending.append("❌ Дожим")
    elif status_value == 'Корректировка цены':
        if not contract.get('push_for_price'):
            pending.append("❌ Дожим")
        # Добавляем задачу на обновление цены, только если поле пустое
        if not str(contract.get('price_update', '')).strip():
            pending.append("❌ Обновление цены")
        # После корректировки цены — нужно добавить обновленные ссылки
        updated_links_fields = [
            ("Крыша", 'krisha'),
            ("Инстаграм", 'instagram'),
            ("Тикток", 'tiktok'),
            ("Рассылка", 'mailing'),
            ("Стрим", 'stream'),
        ]
        missing_updated_links = [label for (label, field) in updated_links_fields if not is_filled(contract.get(field))]
        if missing_updated_links:
            pending.append("❌ Добавить обновленные ссылки: " + ", ".join(missing_updated_links))

    # Если задач нет, и объект еще не реализован — подсказать сменить статус
    if not pending and status_value != 'Реализовано':
        pending.append("❌ Для следующего этапа смените Статус объекта")

    return pending


async def get_agent_phone_by_name(agent_name: str) -> str:
    """Получает номер телефона агента по имени"""
    try:
        db_manager = await get_db_manager()
        phone = await db_manager.get_phone_by_agent(agent_name)
        return phone if phone else "N/A"
    except Exception as e:
        logger.error(f"Ошибка получения телефона агента {agent_name}: {e}")
        return "N/A"


def build_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Мои объекты", callback_data="my_contracts")],
        [InlineKeyboardButton("Поиск по имени клиента", callback_data="search_client")],
        [InlineKeyboardButton("Поменять роль", callback_data="change_role")],
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

            db_manager = await get_db_manager()
            contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)

            if contract:
                await show_contract_detail_by_contract(update, context, contract)
            else:
                reply_markup = build_main_menu_keyboard()
                agent_phone = context.user_data.get('phone')
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
                "Введите номер телефона:"
            )
        return

    if user_states.get(user_id) == 'authenticated' and context.user_data.get('agent_name'):
        agent_name = context.user_data.get('agent_name')
        agent_phone = context.user_data.get('phone')
        role = get_user_role(context)
        if not role:
            await update.message.reply_text(
                "Выберите свою роль:",
                reply_markup=build_role_select_keyboard(context)
            )
            return
        header = f"{role}: {agent_name}"
        reply_markup = build_main_menu_keyboard_by_role(context)
        await update.message.reply_text(
            f"{header}\nНомер: {agent_phone}\n\nВыберите действие:",
            reply_markup=reply_markup,
        )
    else:
        user_states[user_id] = 'waiting_phone'
        await update.message.reply_text(
            "Добро пожаловать!\n\n"
            "Введите номер телефона:"
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
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Failed to answer callback query: {e}")

    user_id = update.effective_user.id
    agent_name = context.user_data.get('agent_name')

    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return

    role = get_user_role(context)
    
    # Для РОП и ДД сначала показываем статистику с кнопками фильтрации
    if role in {ROLE_ROP, ROLE_DD}:
        await show_contracts_stats_menu(query, context, agent_name)
    else:
        # Для МОП сразу показываем список объектов
        await show_loading(query)
        db_manager = await get_db_manager()
        name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
        contracts, total_count = await db_manager.get_agent_contracts_page(name_for_query, 1, CONTRACTS_PER_PAGE, role)
        user_contracts[user_id] = contracts
        user_current_page[user_id] = 0

        if not contracts:
            await query.edit_message_text("У вас нет активных объектов")
            return

        await show_contracts_page_lazy(query, contracts, 1, total_count, agent_name)


async def show_contracts_stats_menu(query, context: ContextTypes.DEFAULT_TYPE, agent_name: str):
    """Показывает меню статистики объектов для РОП и ДД"""
    role = get_user_role(context)
    db_manager = await get_db_manager()
    name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
    totals = await db_manager.get_role_totals(name_for_query, role)
    
    message = "Мои объекты:\n"
    keyboard = [
        [InlineKeyboardButton(f"Все объекты ({totals.get('total', 0)})", callback_data="contracts_filter_all")],
        [InlineKeyboardButton(f"Категория А ({totals.get('cat_A', 0)})", callback_data="contracts_filter_A")],
        [InlineKeyboardButton(f"Категория В ({totals.get('cat_B', 0)})", callback_data="contracts_filter_B")],
        [InlineKeyboardButton(f"Категория С ({totals.get('cat_C', 0)})", callback_data="contracts_filter_C")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
    ]
    
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_contracts_page_lazy(query, contracts: List[Dict], page: int, total_count: int, agent_name: str):
    contracts_per_page = CONTRACTS_PER_PAGE

    message = "Ваши объекты:\n\n"

    keyboard = []
    for contract in contracts:
        crm_id = contract.get('CRM ID', 'N/A')
        client_name_raw = contract.get('Имя клиента и номер', 'N/A')
        # Отображаем только имя клиента без номера
        client_name = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')

        message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message += f"Клиент: {client_name}\n"
        message += f"Адрес: {address}\n"
        message += f"Истекает: {format_date_ddmmyyyy(expires)}\n"
        message += "-"*30 + "\n\n"

        # Добавляем кнопку для быстрого перехода к карточке контракта
        keyboard.append([InlineKeyboardButton(f"CRM ID: {crm_id}", callback_data=f"contract_{crm_id}")])

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"page_contracts_{page-1}"))
    # Показываем кнопку "Следующие", если ещё есть записи после текущей страницы
    if page * contracts_per_page < total_count:
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"page_contracts_{page+1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    edited_message = await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    user_id = query.from_user.id
    user_last_messages[user_id] = edited_message


async def show_search_results_page_lazy(message_or_query, contracts: List[Dict], page: int, total_count: int, client_name: str, agent_name: str):
    contracts_per_page = CONTRACTS_PER_PAGE

    message_text = f"Найдено {total_count} контрактов для клиента '{client_name}':\n\n"

    keyboard = []
    for contract in contracts:
        crm_id = contract.get('CRM ID', 'N/A')
        client_name_raw = contract.get('Имя клиента и номер', 'N/A')
        client_name_clean = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
        address = contract.get('Адрес', 'N/A')
        expires = contract.get('Истекает', 'N/A')

        message_text += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
        message_text += f"Клиент: {client_name_clean}\n"
        message_text += f"Адрес: {address}\n"
        message_text += f"Истекает: {expires}\n"
        message_text += "-"*30 + "\n\n"

        # Кнопка для показа карточки контракта из результатов поиска
        keyboard.append([InlineKeyboardButton(f"CRM ID: {crm_id}", callback_data=f"contract_{crm_id}")])

    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"page_search_{page-1}"))
    if page * contracts_per_page < total_count:
        nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"page_search_{page+1}"))

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


async def update_agent_name_from_phone(context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Обновляет имя агента в контексте из телефона, если оно устарело"""
    try:
        user_phone = context.user_data.get('phone')
        logger.info(f"update_agent_name_from_phone: user_phone from context: {user_phone}")
        
        if not user_phone:
            logger.warning("update_agent_name_from_phone: No phone in context")
            return False
            
        db_manager = await get_db_manager()
        updated_agent_name = await db_manager.get_agent_by_phone(user_phone)
        logger.info(f"update_agent_name_from_phone: Found agent_name by phone: {updated_agent_name}")
        
        if updated_agent_name:
            current_agent_name = context.user_data.get('agent_name')
            logger.info(f"update_agent_name_from_phone: Current agent_name: {current_agent_name}")
            
            if updated_agent_name != current_agent_name:
                context.user_data['agent_name'] = updated_agent_name
                logger.info(f"update_agent_name_from_phone: Updated agent_name from '{current_agent_name}' to '{updated_agent_name}'")
                return True
            else:
                logger.info("update_agent_name_from_phone: Agent name is already up to date")
        else:
            logger.warning(f"update_agent_name_from_phone: No agent found for phone {user_phone}")
        return False
    except Exception as e:
        logger.error(f"Ошибка обновления имени агента: {e}")
        return False


async def show_contract_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logger.warning(f"Failed to answer callback query: {e}")

    agent_name = context.user_data.get('agent_name')
    if not agent_name:
        await query.edit_message_text("Ошибка: агент не найден")
        return

    db_manager = await get_db_manager()
    role = get_user_role(context)
    name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
    contract = await db_manager.search_contract_by_crm_id(crm_id, name_for_query, role)
    if not contract:
        # Если контракт не найден, попробуем обновить имя агента из телефона
        if await update_agent_name_from_phone(context):
            agent_name = context.user_data.get('agent_name')
            contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
        
        if not contract:
            await query.edit_message_text("Контракт не найден среди ваших сделок")
            return

    await show_contract_detail_by_contract(update, context, contract)


async def show_contract_detail_by_contract(update: Update, context: ContextTypes.DEFAULT_TYPE, contract: Dict, force_new_message: bool = False):
    crm_id = contract.get('CRM ID', 'N/A')
    logger.info(f"show_contract_detail_by_contract: CRM ID from contract: {crm_id}")
    message = f"📋 Детали объекта CRM ID: {crm_id}\n\n"
    message += f"📅 Дата подписания: {format_date_ddmmyyyy(contract.get('Дата подписания'))}\n"
    message += f"👤 МОП: {contract.get('МОП', 'N/A')}\n"
    message += f"👤 РОП: {contract.get('РОП', 'N/A')}\n"
    message += f"👤 ДД: {contract.get('ДД', 'N/A')}\n"
    client_info = contract.get('Имя клиента и номер', 'N/A')
    client_name_only = clean_client_name(str(client_info).split(':')[0].strip()) if isinstance(client_info, str) else str(client_info)
    message += f"📞 Клиент: {client_name_only}\n"
    message += f"🏠 Адрес: {contract.get('Адрес', 'N/A')}\n"
    message += f"🏢 ЖК: {contract.get('ЖК', 'N/A')}\n"
    message += f"💰 Цена: {contract.get('Цена указанная в договоре', 'N/A')}\n"
    message += f"⏰ Истекает: {format_date_ddmmyyyy(contract.get('Истекает'))}\n"
    message += f"📊 Корректировка цены: {contract.get('price_update', 'N/A')}\n"
    message += f"📌 Статус: {get_status_value(contract)}\n"
    category_val = contract.get('category', 'N/A')
    message += f"📂 Категория: {category_val}\n"
    message += f"👁️ Показы: {contract.get('shows', 0)}\n\n"

    # Добавляем блок со ссылками, если есть
    link_fields = [
        ("Инстаграм", 'instagram'),
        ("Тикток", 'tiktok'),
        ("Крыша", 'krisha'),
        ("Рассылка", 'mailing'),
        ("Стрим", 'stream'),
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

    if contract.get('collage'):
        message += "✅ Коллаж\n"
    if contract.get('prof_collage'):
        message += "✅ Проф Коллаж\n"
    if contract.get('analytics'):
        message += "✅ Аналитика-сделано\n"
    if contract.get('provide_analytics'):
        message += "✅ Аналитика-предоставлено\n"
    if contract.get('push_for_price'):
        message += "✅ Дожим\n"

    # Рендер кнопок в зависимости от статуса
    status_value = get_status_value(contract)
    # Режим аналитики теперь трактуем как выбранный статус "Аналитика"
    analytics_mode_active = (status_value == 'Аналитика')

    # Чек-лист невыполненных задач
    pending = build_pending_tasks(contract, status_value, analytics_mode_active)
    if pending:
        message += "\n📝 Необходимо сделать:\n" + "\n".join(pending) + "\n"

    # Определяем callback_data для кнопки "Назад к списку"
    back_to_list_callback = "my_contracts"
    
    # Проверяем, есть ли информация о возврате к списку МОП-а
    back_info_mop = context.user_data.get('back_to_mop_list')
    if back_info_mop:
        rop_idx = back_info_mop.get('rop_idx')
        mop_idx = back_info_mop.get('mop_idx')
        category = back_info_mop.get('category')
        page = back_info_mop.get('page', 1)
        if rop_idx:
            # МОП из РОП-а
            if page > 1:
                back_to_list_callback = f"mop_category_rop_{rop_idx}_{mop_idx}_page_{page}_{category}"
            else:
                back_to_list_callback = f"mop_category_rop_{rop_idx}_{mop_idx}_{category}"
        else:
            # Обычный МОП
            if page > 1:
                back_to_list_callback = f"mop_category_{mop_idx}_page_{page}_{category}"
            else:
                back_to_list_callback = f"mop_category_{mop_idx}_{category}"
    # Проверяем, есть ли информация о возврате к списку РОП-а
    elif context.user_data.get('back_to_rop_list'):
        back_info = context.user_data.get('back_to_rop_list')
        rop_idx = back_info.get('rop_idx')
        category = back_info.get('category', 'all')
        page = back_info.get('page', 1)
        if page > 1:
            back_to_list_callback = f"rop_category_{rop_idx}_page_{page}_{category}"
        else:
            back_to_list_callback = f"rop_category_{rop_idx}_{category}"
    # Проверяем, есть ли информация о возврате к списку объектов РОП/ДД
    elif context.user_data.get('back_to_contracts_list'):
        back_info = context.user_data.get('back_to_contracts_list')
        category_str = back_info.get('category', 'all')
        page = back_info.get('page', 1)
        if page > 1:
            back_to_list_callback = f"contracts_filter_{category_str}_page_{page}"
        else:
            back_to_list_callback = f"contracts_filter_{category_str}"
    
    # Получаем роль пользователя для определения доступных действий
    user_role = get_user_role(context)
    is_rop = (user_role == ROLE_ROP)
    is_mop = (user_role == ROLE_MOP)
    
    # Если реализовано — кнопок нет
    if status_value == 'Реализовано':
        keyboard = [
            [InlineKeyboardButton("🔙 Назад к списку", callback_data=back_to_list_callback)],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        else:
            await update.message.reply_text(
                message,
                reply_markup=reply_markup,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
        return

    keyboard = []
    
    # Для РОП-а доступна только смена категории
    if is_rop:
        keyboard.append([InlineKeyboardButton("Изменить категорию", callback_data=f"change_category_menu_{crm_id}")])
    # Для МОП-а доступны все действия кроме смены категории
    elif is_mop:
        # Общие правила на коллаж/проф/показ
        if not contract.get('collage'):
            keyboard.append([InlineKeyboardButton("Создать коллаж", callback_data=f"collage_build_{crm_id}")])
        if contract.get('collage') and not contract.get('prof_collage'):
            keyboard.append([InlineKeyboardButton("Проф коллаж", callback_data=f"action_pro_collage_{crm_id}")])
        keyboard.append([InlineKeyboardButton("Показ +1", callback_data=f"action_show_{crm_id}")])

        if status_value == 'Корректировка цены':
            # Кнопки для статуса "Корректировка цены"
            if not value_is_filled(contract.get('push_for_price')):
                keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
            price_update_val = contract.get('price_update')
            if not value_is_filled(price_update_val):
                keyboard.append([InlineKeyboardButton("Обновление цены", callback_data=f"price_adjust_{crm_id}")])
            keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
            keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
        elif analytics_mode_active:
            # Кнопки для режима предоставления аналитики
            if not value_is_filled(contract.get('analytics')):
                keyboard.append([InlineKeyboardButton("Аналитика", callback_data=f"analytics_done_{crm_id}")])
            if value_is_filled(contract.get('analytics')) and not value_is_filled(contract.get('provide_analytics')):
                keyboard.append([InlineKeyboardButton("Аналитика через 5 дней", callback_data=f"analytics_provided_{crm_id}")])
            if value_is_filled(contract.get('provide_analytics')) and not value_is_filled(contract.get('push_for_price')):
                keyboard.append([InlineKeyboardButton("Дожим", callback_data=f"push_{crm_id}")])
            keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
            keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])
        else:
            # Базовые кнопки по умолчанию
            keyboard.append([InlineKeyboardButton("Добавить ссылку", callback_data=f"add_link_{crm_id}")])
            keyboard.append([InlineKeyboardButton("Смена статуса объекта", callback_data=f"status_menu_{crm_id}")])

    keyboard.append([InlineKeyboardButton("🔙 Назад к списку", callback_data=back_to_list_callback)])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query and not force_new_message:
        try:
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML', disable_web_page_preview=True)
        except Exception:
            # Если не удается отредактировать (например, сообщение с фотографией), отправляем новое
            sent_message = await update.callback_query.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML', disable_web_page_preview=True)
            user_id = update.effective_user.id
            user_last_messages[user_id] = sent_message
    else:
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id:
            sent_message = await context.bot.send_message(chat_id=chat_id, text=message, reply_markup=reply_markup, parse_mode='HTML', disable_web_page_preview=True)
        else:
            sent_message = await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML', disable_web_page_preview=True)
        user_id = update.effective_user.id
        user_last_messages[user_id] = sent_message


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data

    # Отвечаем на callback query сразу
    await query.answer()

    if data == "my_contracts":
        # Очищаем информацию о возврате к списку МОП-а при переходе к общему списку
        context.user_data.pop('back_to_mop_list', None)
        await my_contracts(update, context)

    elif data == "change_role":
        await query.edit_message_text(
            "Выберите свою роль:",
            reply_markup=build_role_select_keyboard(context)
        )

    elif data.startswith("select_role_"):
        role = data.replace("select_role_", "")
        if role in {ROLE_MOP, ROLE_ROP, ROLE_DD}:
            set_user_role(context, role)
            if role == ROLE_DD:
                # Сохраняем имя для поиска в колонке dd по номеру телефона
                phone = context.user_data.get('phone')
                norm = _normalize_to_10_digits(phone or '')
                dd_name = PHONE_TO_DD_NAME.get(norm)
                if dd_name:
                    context.user_data['dd_query_name'] = dd_name
            agent_name = context.user_data.get('agent_name', 'Агент')
            agent_phone = context.user_data.get('phone') or await get_agent_phone_by_name(agent_name)
            header = f"{role}: {agent_name}"
            reply_markup = build_main_menu_keyboard_by_role(context)
            await query.edit_message_text(
                f"{header}\nНомер: {agent_phone}\n\nВыберите действие:",
                reply_markup=reply_markup
            )
        else:
            await query.edit_message_text("❌ Неизвестная роль")

    elif data.startswith("contract_"):
        # Обработка клика по CRM ID
        data_parts = data.replace("contract_", "")
        parts = data_parts.split("_")
        crm_id = parts[0]
        
        # Проверяем различные форматы callback_data для возврата
        if len(parts) >= 4 and parts[1] == "mop":
            # Формат: contract_{crm_id}_mop_{idx}_{category} или contract_{crm_id}_mop_{idx}_page_{page}_{category}
            # Или: contract_{crm_id}_mop_rop_{rop_idx}_{mop_idx}_{category} или contract_{crm_id}_mop_rop_{rop_idx}_{mop_idx}_page_{page}_{category}
            if len(parts) >= 6 and parts[2] == "rop":
                # МОП из РОП-а
                rop_idx = parts[3]
                mop_idx = parts[4]
                if len(parts) >= 8 and parts[5] == "page":
                    page = int(parts[6])
                    category = parts[7]
                    context.user_data['back_to_mop_list'] = {'rop_idx': rop_idx, 'mop_idx': mop_idx, 'category': category, 'page': page}
                else:
                    category = parts[5]
                    context.user_data['back_to_mop_list'] = {'rop_idx': rop_idx, 'mop_idx': mop_idx, 'category': category, 'page': 1}
            else:
                # Обычный МОП
                mop_idx = parts[2]
                if len(parts) >= 6 and parts[3] == "page":
                    page = int(parts[4])
                    category = parts[5]
                    context.user_data['back_to_mop_list'] = {'mop_idx': mop_idx, 'category': category, 'page': page}
                else:
                    category = parts[3]
                    context.user_data['back_to_mop_list'] = {'mop_idx': mop_idx, 'category': category, 'page': 1}
        elif len(parts) >= 3 and parts[1] == "rop":
            # Формат: contract_{crm_id}_rop_{idx}_{category} или contract_{crm_id}_rop_{idx}_page_{page}_{category}
            rop_idx = parts[2]
            if len(parts) >= 6 and parts[3] == "page":
                page = int(parts[4])
                category = parts[5]
                context.user_data['back_to_rop_list'] = {'rop_idx': rop_idx, 'category': category, 'page': page}
            else:
                category = parts[3]
                context.user_data['back_to_rop_list'] = {'rop_idx': rop_idx, 'category': category, 'page': 1}
        elif len(parts) >= 3 and parts[1] == "filter":
            # Формат: contract_{crm_id}_filter_{category} или contract_{crm_id}_filter_{category}_page_{page}
            category_str = parts[2]
            if len(parts) >= 5 and parts[3] == "page":
                page = int(parts[4])
                context.user_data['back_to_contracts_list'] = {'category': category_str, 'page': page}
            else:
                context.user_data['back_to_contracts_list'] = {'category': category_str, 'page': 1}
        
        user_id = update.effective_user.id
        user_states[user_id] = 'authenticated'
        await show_loading(query)
        await show_contract_detail(update, context, crm_id)

    elif data.startswith("page_"):
        # Обработка пагинации
        page_data = data.replace("page_", "")
        if "_" in page_data:
            page_type, page_num = page_data.split("_", 1)
            page_num = int(page_num)
            
            if page_type == "contracts":
                # Загружаем контракты для страницы
                user_id = update.effective_user.id
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    db_manager = await get_db_manager()
                    role = get_user_role(context)
                    name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
                    contracts, total_count = await db_manager.get_agent_contracts_page(name_for_query, page_num, CONTRACTS_PER_PAGE, role)
                    await show_contracts_page_lazy(query, contracts, page_num, total_count, agent_name)
            elif page_type == "search":
                search_query = context.user_data.get('last_search_query', '')
                if search_query:
                    user_id = update.effective_user.id
                    agent_name = context.user_data.get('agent_name')
                    if agent_name:
                        db_manager = await get_db_manager()
                        role = get_user_role(context)
                        name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
                        contracts, total_count = await db_manager.search_contracts_by_client_name_lazy(search_query, name_for_query, page_num, CONTRACTS_PER_PAGE, role)
                        await show_search_results_page_lazy(query, contracts, page_num, total_count, search_query, agent_name)

    elif data == "back_to_main" or data == "main_menu":
        # Возврат в главное меню
        user_id = update.effective_user.id
        if user_states.get(user_id) == 'authenticated':
            reply_markup = build_main_menu_keyboard_by_role(context) if get_user_role(context) else build_main_menu_keyboard()
            agent_name = context.user_data.get('agent_name', 'Агент')
            agent_phone = context.user_data.get('phone') or await get_agent_phone_by_name(agent_name)
            role = get_user_role(context)
            header = f"{role}: {agent_name}" if role else f"Агент: {agent_name}"
            await query.edit_message_text(
                f"{header}\nНомер: {agent_phone}\n\nВыберите действие:",
                reply_markup=reply_markup
            )

    elif data == "search":
        # Меню выбора вида поиска
        role = get_user_role(context)
        keyboard = []
        
        if role == ROLE_DD:
            keyboard.append([InlineKeyboardButton("Найти РОП-а по имени", callback_data="search_rop")])
            keyboard.append([InlineKeyboardButton("Найти МОП-а по имени", callback_data="search_mop")])
            keyboard.append([InlineKeyboardButton("Найти объект по имени клиента", callback_data="search_client")])
            message = "Выберите вид поиска:"
        elif role == ROLE_ROP:
            keyboard.append([InlineKeyboardButton("Найти МОП-а по имени", callback_data="search_mop")])
            keyboard.append([InlineKeyboardButton("Найти объект по имени клиента", callback_data="search_client")])
            message = "Выберите вид поиска:"
        else:
            # Для МОП и других ролей - сразу поиск по имени клиента
            user_id = update.effective_user.id
            user_states[user_id] = 'waiting_client_search'
            await query.edit_message_text(
                "🔍 Введите имя клиента для поиска:"
            )
            return
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "search_client":
        # Поиск по имени клиента
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_client_search'
        await query.edit_message_text(
            "🔍 Введите имя клиента для поиска:"
        )
    
    elif data == "search_rop":
        # Поиск РОП-а по имени
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_rop_search'
        await query.edit_message_text(
            "🔍 Введите имя РОП-а для поиска:"
        )
    
    elif data == "search_mop":
        # Поиск МОП-а по имени
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_mop_search'
        await query.edit_message_text(
            "🔍 Введите имя МОП-а для поиска:"
        )

    elif data.startswith("rop_search_result_"):
        # Обработка клика по найденному РОП-у
        try:
            idx = int(data.replace("rop_search_result_", ""))
        except:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        
        rop_search_results = context.user_data.get('rop_search_results') or []
        if idx < 0 or idx >= len(rop_search_results):
            await query.edit_message_text("❌ РОП не найден")
            return
        
        rop = rop_search_results[idx]
        rop_name = rop.get('name')
        
        # Находим индекс в основном списке РОП-ов или используем результаты поиска
        owner_name = context.user_data.get('dd_query_name')
        db_manager = await get_db_manager()
        all_rops = await db_manager.get_subordinates(owner_name, ROLE_DD, subordinate_role=ROLE_ROP)
        
        # Создаем полный список РОП-ов для контекста
        if not context.user_data.get('rops_menu') or len(context.user_data.get('rops_menu', [])) != len(all_rops):
            all_rops_menu = []
            for item in all_rops:
                full_name = (item.get('name') or 'Не указан').strip()
                cnt = item.get('count', 0)
                name_parts = full_name.split()
                display_name = ' '.join(name_parts[:2]) if name_parts else full_name
                pending = await db_manager.count_pending_tasks_for_rop(full_name)
                all_rops_menu.append({
                    'name': full_name,
                    'count': cnt,
                    'pending': pending,
                    'display': display_name,
                })
            context.user_data['rops_menu'] = all_rops_menu
        
        # Ищем индекс РОП-а в списке
        rop_idx = None
        rops_menu = context.user_data.get('rops_menu') or []
        for i, item in enumerate(rops_menu):
            if item.get('name') == rop_name:
                rop_idx = i
                break
        
        if rop_idx is not None:
            # Используем ту же логику, что и в rop_filter_
            rops_menu = context.user_data.get('rops_menu') or []
            if rop_idx < 0 or rop_idx >= len(rops_menu):
                await query.edit_message_text("❌ РОП не найден")
                return
            rop_name_from_menu = rops_menu[rop_idx]['name']
            message = f"Посмотреть объекты/МОП-ы данного РОП-а:\n{rop_name_from_menu}\n"
            keyboard = [
                [InlineKeyboardButton("Объекты", callback_data=f"rop_objects_{rop_idx}")],
                [InlineKeyboardButton("МОП-ы", callback_data=f"rop_mops_{rop_idx}")],
                [InlineKeyboardButton("🔙 Назад", callback_data="my_rops_page_1")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
            ]
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text("❌ РОП не найден в списке")
    
    elif data.startswith("mop_search_result_"):
        # Обработка клика по найденному МОП-у
        try:
            idx = int(data.replace("mop_search_result_", ""))
        except:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        
        mop_search_results = context.user_data.get('mop_search_results') or []
        if idx < 0 or idx >= len(mop_search_results):
            await query.edit_message_text("❌ МОП не найден")
            return
        
        mop = mop_search_results[idx]
        mop_name = mop.get('name')
        
        # Находим МОП-а в основном списке
        role = get_user_role(context)
        owner_name = context.user_data.get('dd_query_name') if role == ROLE_DD else context.user_data.get('agent_name')
        owner_role = ROLE_DD if role == ROLE_DD else ROLE_ROP
        
        db_manager = await get_db_manager()
        all_mops = await db_manager.get_subordinates(owner_name, owner_role, subordinate_role=ROLE_MOP)
        
        # Создаем полный список МОП-ов для контекста
        if not context.user_data.get('mops_menu') or len(context.user_data.get('mops_menu', [])) != len(all_mops):
            all_mops_menu = []
            for item in all_mops:
                full_name = (item.get('name') or 'Не указан').strip()
                cnt = item.get('count', 0)
                name_parts = full_name.split()
                display_name = ' '.join(name_parts[:2]) if name_parts else full_name
                pending = await db_manager.count_pending_tasks_for_mop(full_name)
                all_mops_menu.append({
                    'name': full_name,
                    'count': cnt,
                    'pending': pending,
                    'display': display_name,
                })
            context.user_data['mops_menu'] = all_mops_menu
        
        # Ищем индекс МОП-а в списке
        mop_idx = None
        mops_menu = context.user_data.get('mops_menu') or []
        for i, item in enumerate(mops_menu):
            if item.get('name') == mop_name:
                mop_idx = i
                break
        
        if mop_idx is not None:
            # Используем ту же логику, что и в mop_filter_ для обычного МОП-а
            mops_menu = context.user_data.get('mops_menu') or []
            if mop_idx < 0 or mop_idx >= len(mops_menu):
                await query.edit_message_text("❌ МОП не найден")
                return
            mop_name_from_menu = mops_menu[mop_idx]['name']
            db_manager = await get_db_manager()
            totals = await db_manager.get_mop_category_stats(mop_name_from_menu)
            message = f"Объекты МОП: {mop_name_from_menu}\n"
            keyboard = [
                [InlineKeyboardButton(f"Все объекты ({totals['total']})", callback_data=f"mop_category_{mop_idx}_all")],
                [InlineKeyboardButton(f"Объекты категории А ({totals['cat_A']})", callback_data=f"mop_category_{mop_idx}_A")],
                [InlineKeyboardButton(f"Объекты категории В ({totals['cat_B']})", callback_data=f"mop_category_{mop_idx}_B")],
                [InlineKeyboardButton(f"Объекты категории С ({totals['cat_C']})", callback_data=f"mop_category_{mop_idx}_C")],
                [InlineKeyboardButton("🔙 Назад", callback_data="my_mops_page_1")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
            ]
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await query.edit_message_text("❌ МОП не найден в списке")

    elif data == "logout_confirm":
        # Подтверждение выхода
        await query.edit_message_text(
            "🚪 Вы уверены, что хотите выйти?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Да, выйти", callback_data="logout_yes")],
                [InlineKeyboardButton("Отмена", callback_data="main_menu")]
            ])
        )

    elif data == "logout_yes":
        # Выход из системы
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_phone'
        context.user_data.clear()
        await query.edit_message_text(
            "👋 Вы вышли из системы.\n\nДля входа введите номер телефона:"
        )

    elif data == "my_mops" or data.startswith("my_mops_page_"):
        # Экран с МОП-ами для РОП и ДД с пагинацией
        page = 1
        if data.startswith("my_mops_page_"):
            try:
                page = int(data.replace("my_mops_page_", ""))
            except:
                page = 1
        
        agent_name = context.user_data.get('agent_name')
        role = get_user_role(context)
        if not agent_name or role not in {ROLE_ROP, ROLE_DD}:
            await query.edit_message_text("❌ Недоступно для вашей роли")
            return
        db_manager = await get_db_manager()
        owner_role = ROLE_ROP if role == ROLE_ROP else ROLE_DD
        owner_name = (context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name)
        totals = await db_manager.get_role_totals(owner_name, owner_role)
        mops = await db_manager.get_subordinates(owner_name, owner_role, subordinate_role=ROLE_MOP)
        
        message = (
            "Мои МОП-ы:\n"
            f"Всего объектов: {totals.get('total', 0)}\n"
            f"Объектов с категорией А: {totals.get('cat_A', 0)}\n"
            f"Объектов с категорией В: {totals.get('cat_B', 0)}\n"
            f"Объектов с категорией С: {totals.get('cat_C', 0)}\n"
        )
        pending_total = await db_manager.count_pending_tasks_for_owner(owner_name, owner_role)
        message += f"Невыполненных заданий всего: {pending_total}\n\n"
        
        # Пагинация: по 10 МОП-ов на страницу
        mops_per_page = 10
        total_count = len(mops)
        start_idx = (page - 1) * mops_per_page
        end_idx = start_idx + mops_per_page
        mops_page = mops[start_idx:end_idx]
        
        keyboard = []
        # Сохраняем сопоставление индексов к МОП-ам, чтобы не превышать лимит callback_data
        # Если список еще не сохранен или был очищен, создаем полный список всех МОП-ов
        mops_menu = context.user_data.get('mops_menu') or []
        if not mops_menu or len(mops_menu) != len(mops):
            # Создаем полный список всех МОП-ов для правильной работы индексов
            all_mops_menu = []
            for item in mops:
                full_name = (item.get('name') or 'Не указан').strip()
                cnt = item.get('count', 0)
                name_parts = full_name.split()
                display_name = ' '.join(name_parts[:2]) if name_parts else full_name
                pending = await db_manager.count_pending_tasks_for_mop(full_name)
                all_mops_menu.append({
                    'name': full_name,
                    'count': cnt,
                    'pending': pending,
                    'display': display_name,
                })
            context.user_data['mops_menu'] = all_mops_menu
            mops_menu = all_mops_menu
        
        # Показываем только МОП-ов текущей страницы
        for i, item in enumerate(mops_page):
            full_name = (item.get('name') or 'Не указан').strip()
            # Ищем индекс в полном списке
            index = start_idx + i
            keyboard.append([InlineKeyboardButton(
                f"{mops_menu[index]['display']} (📋{mops_menu[index]['count']}/🚩{mops_menu[index]['pending']})",
                callback_data=f"mop_filter_{index}"
            )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"my_mops_page_{page-1}"))
        if end_idx < total_count:
            nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"my_mops_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "my_rops" or data.startswith("my_rops_page_"):
        # Экран с РОП-ами для ДД с пагинацией
        page = 1
        if data.startswith("my_rops_page_"):
            try:
                page = int(data.replace("my_rops_page_", ""))
            except:
                page = 1
        
        agent_name = context.user_data.get('agent_name')
        role = get_user_role(context)
        if not agent_name or role != ROLE_DD:
            await query.edit_message_text("❌ Недоступно для вашей роли")
            return
        db_manager = await get_db_manager()
        owner_name = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
        totals = await db_manager.get_role_totals(owner_name, ROLE_DD)
        rops = await db_manager.get_subordinates(owner_name, ROLE_DD, subordinate_role=ROLE_ROP)
        
        message = (
            "Мои РОП-ы:\n"
            f"Всего объектов: {totals.get('total', 0)}\n"
            f"Объектов с категорией А: {totals.get('cat_A', 0)}\n"
            f"Объектов с категорией В: {totals.get('cat_B', 0)}\n"
            f"Объектов с категорией С: {totals.get('cat_C', 0)}\n"
        )
        pending_total = await db_manager.count_pending_tasks_for_owner(owner_name, ROLE_DD)
        message += f"Невыполненных заданий всего: {pending_total}\n\n"
        
        # Пагинация: по 10 РОП-ов на страницу
        rops_per_page = 10
        total_count = len(rops)
        start_idx = (page - 1) * rops_per_page
        end_idx = start_idx + rops_per_page
        rops_page = rops[start_idx:end_idx]
        
        keyboard = []
        # Сохраняем сопоставление индексов к РОП-ам
        rops_menu = context.user_data.get('rops_menu') or []
        if not rops_menu or len(rops_menu) != len(rops):
            # Создаем полный список всех РОП-ов для правильной работы индексов
            all_rops_menu = []
            for item in rops:
                full_name = (item.get('name') or 'Не указан').strip()
                cnt = item.get('count', 0)
                name_parts = full_name.split()
                display_name = ' '.join(name_parts[:2]) if name_parts else full_name
                pending = await db_manager.count_pending_tasks_for_rop(full_name)
                all_rops_menu.append({
                    'name': full_name,
                    'count': cnt,
                    'pending': pending,
                    'display': display_name,
                })
            context.user_data['rops_menu'] = all_rops_menu
            rops_menu = all_rops_menu
        
        # Показываем только РОП-ов текущей страницы
        for i, item in enumerate(rops_page):
            full_name = (item.get('name') or 'Не указан').strip()
            # Ищем индекс в полном списке
            index = start_idx + i
            keyboard.append([InlineKeyboardButton(
                f"{rops_menu[index]['display']} (📋{rops_menu[index]['count']}/🚩{rops_menu[index]['pending']})",
                callback_data=f"rop_filter_{index}"
            )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"my_rops_page_{page-1}"))
        if end_idx < total_count:
            nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"my_rops_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("rop_filter_"):
        # Обработка клика по РОП-у - показываем меню выбора действия
        try:
            idx = int(data.replace("rop_filter_", "").strip())
        except Exception:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        rops_menu = context.user_data.get('rops_menu') or []
        if idx < 0 or idx >= len(rops_menu):
            await query.edit_message_text("❌ РОП не найден")
            return
        rop_name = rops_menu[idx]['name']
        message = f"Посмотреть объекты/МОП-ы данного РОП-а:\n{rop_name}\n"
        keyboard = [
            [InlineKeyboardButton("Объекты", callback_data=f"rop_objects_{idx}")],
            [InlineKeyboardButton("МОП-ы", callback_data=f"rop_mops_{idx}")],
            [InlineKeyboardButton("🔙 Назад", callback_data="my_rops_page_1")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("rop_objects_"):
        # Показываем меню статистики объектов РОП-а
        try:
            idx = int(data.replace("rop_objects_", "").strip())
        except Exception:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        rops_menu = context.user_data.get('rops_menu') or []
        if idx < 0 or idx >= len(rops_menu):
            await query.edit_message_text("❌ РОП не найден")
            return
        rop_name = rops_menu[idx]['name']
        db_manager = await get_db_manager()
        # Получаем статистику по категориям для этого РОП-а без загрузки всех объектов
        totals = await db_manager.get_rop_category_stats(rop_name)
        message = f"Объекты РОП-а: {rop_name}\n"
        keyboard = [
            [InlineKeyboardButton(f"Все объекты ({totals['total']})", callback_data=f"rop_category_{idx}_all")],
            [InlineKeyboardButton(f"Объекты категории А ({totals['cat_A']})", callback_data=f"rop_category_{idx}_A")],
            [InlineKeyboardButton(f"Объекты категории В ({totals['cat_B']})", callback_data=f"rop_category_{idx}_B")],
            [InlineKeyboardButton(f"Объекты категории С ({totals['cat_C']})", callback_data=f"rop_category_{idx}_C")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"rop_filter_{idx}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ]
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("rop_category_"):
        # Обработка клика по категории РОП-а - показываем список объектов с пагинацией
        data_parts = data.replace("rop_category_", "")
        
        # Проверяем, есть ли номер страницы
        page = 1
        if "_page_" in data_parts:
            parts = data_parts.split("_page_", 1)
            if len(parts) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            idx_str = parts[0]
            rest = parts[1]
            page_category = rest.rsplit("_", 1)
            if len(page_category) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            try:
                page = int(page_category[0])
                category = page_category[1]
            except:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
        else:
            parts = data_parts.rsplit("_", 1)
            if len(parts) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            idx_str, category = parts
        
        try:
            idx = int(idx_str)
        except Exception:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        
        rops_menu = context.user_data.get('rops_menu') or []
        if idx < 0 or idx >= len(rops_menu):
            await query.edit_message_text("❌ РОП не найден")
            return
        rop_name = rops_menu[idx]['name']
        category_filter = None if category == "all" else category
        
        # Сохраняем информацию для кнопки "Назад" в деталях контракта
        context.user_data['back_to_rop_list'] = {'rop_idx': str(idx), 'category': category, 'page': page}
        
        await show_loading(query)
        db_manager = await get_db_manager()
        contracts = await db_manager.get_rop_contracts_by_category(rop_name, category_filter)
        
        if not contracts:
            category_label = "Все объекты" if category == "all" else f"Объекты категории {category}"
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"rop_objects_{idx}")]]
            await query.edit_message_text(f"{category_label}:\n\nОбъекты не найдены", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Пагинация: по 10 объектов на страницу
        contracts_per_page = 10
        total_count = len(contracts)
        start_idx = (page - 1) * contracts_per_page
        end_idx = start_idx + contracts_per_page
        contracts_page = contracts[start_idx:end_idx]
        
        category_label = "Все объекты:" if category == "all" else f"Объекты категории {category}:"
        message = f"{category_label}\n\n"
        keyboard = []
        
        for contract in contracts_page:
            crm_id = contract.get('CRM ID', 'N/A')
            client_name_raw = contract.get('Имя клиента и номер', 'N/A')
            client_name = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
            address = contract.get('Адрес', 'N/A')
            expires = contract.get('Истекает', 'N/A')
            category_val = contract.get('category', 'N/A')
            
            message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
            message += f"Клиент: {client_name}\n"
            message += f"Адрес: {address}\n"
            message += f"Истекает: {format_date_ddmmyyyy(expires)}\n"
            message += f"Категория: {category_val}\n"
            message += "-"*30 + "\n\n"
            
            # Сохраняем информацию о РОП-е и категории в callback_data для правильного возврата
            if page > 1:
                keyboard.append([InlineKeyboardButton(
                    f"CRM ID: {crm_id}", 
                    callback_data=f"contract_{crm_id}_rop_{idx}_page_{page}_{category}"
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"CRM ID: {crm_id}", 
                    callback_data=f"contract_{crm_id}_rop_{idx}_{category}"
                )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"rop_category_{idx}_page_{page-1}_{category}"))
        if end_idx < total_count:
            nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"rop_category_{idx}_page_{page+1}_{category}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопка "Назад" к меню РОП-а
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"rop_objects_{idx}")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif data.startswith("rop_mops_"):
        # Показываем список МОП-ов выбранного РОП-а с пагинацией
        data_parts = data.replace("rop_mops_", "")
        
        # Проверяем, есть ли номер страницы
        page = 1
        if "_page_" in data_parts:
            parts = data_parts.split("_page_", 1)
            if len(parts) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            idx_str = parts[0]
            try:
                idx = int(idx_str)
                page = int(parts[1])
            except:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
        else:
            try:
                idx = int(data_parts)
            except:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
        
        rops_menu = context.user_data.get('rops_menu') or []
        if idx < 0 or idx >= len(rops_menu):
            await query.edit_message_text("❌ РОП не найден")
            return
        rop_name = rops_menu[idx]['name']
        
        # Получаем имя ДД из контекста, если пользователь - ДД
        role = get_user_role(context)
        dd_name = None
        if role == ROLE_DD:
            dd_name = context.user_data.get('dd_query_name')
        
        await show_loading(query)
        db_manager = await get_db_manager()
        # Фильтруем по ДД и РОП одновременно
        mops = await db_manager.get_mops_by_rop(rop_name, dd_name=dd_name)
        
        if not mops:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"rop_filter_{idx}")]]
            await query.edit_message_text(f"МОП-ы РОП-а: {rop_name}\n\nМОП-ы не найдены", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Получаем статистику для всех МОП-ов этого РОП-а
        rop_totals = await db_manager.get_role_totals(rop_name, ROLE_ROP)
        message = (
            f"МОП-ы РОП-а: {rop_name}\n"
            f"Всего объектов: {rop_totals.get('total', 0)}\n"
            f"Объектов с категорией А: {rop_totals.get('cat_A', 0)}\n"
            f"Объектов с категорией В: {rop_totals.get('cat_B', 0)}\n"
            f"Объектов с категорией С: {rop_totals.get('cat_C', 0)}\n"
        )
        pending_total = await db_manager.count_pending_tasks_for_owner(rop_name, ROLE_ROP)
        message += f"Невыполненных заданий всего: {pending_total}\n\n"
        
        # Пагинация: по 10 МОП-ов на страницу
        mops_per_page = 10
        total_count = len(mops)
        start_idx = (page - 1) * mops_per_page
        end_idx = start_idx + mops_per_page
        mops_page = mops[start_idx:end_idx]
        
        keyboard = []
        # Сохраняем список МОП-ов этого РОП-а
        rop_mops_menu = context.user_data.get(f'rop_{idx}_mops_menu') or []
        if not rop_mops_menu or len(rop_mops_menu) != len(mops):
            all_mops_menu = []
            for item in mops:
                full_name = (item.get('name') or 'Не указан').strip()
                cnt = item.get('count', 0)
                name_parts = full_name.split()
                display_name = ' '.join(name_parts[:2]) if name_parts else full_name
                pending = await db_manager.count_pending_tasks_for_mop(full_name)
                all_mops_menu.append({
                    'name': full_name,
                    'count': cnt,
                    'pending': pending,
                    'display': display_name,
                })
            context.user_data[f'rop_{idx}_mops_menu'] = all_mops_menu
            rop_mops_menu = all_mops_menu
        
        # Показываем только МОП-ов текущей страницы
        for i, item in enumerate(mops_page):
            full_name = (item.get('name') or 'Не указан').strip()
            index = start_idx + i
            keyboard.append([InlineKeyboardButton(
                f"{rop_mops_menu[index]['display']} (📋{rop_mops_menu[index]['count']}/🚩{rop_mops_menu[index]['pending']})",
                callback_data=f"mop_filter_rop_{idx}_{index}"
            )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"rop_mops_{idx}_page_{page-1}"))
        if end_idx < total_count:
            nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"rop_mops_{idx}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"rop_filter_{idx}")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("mop_filter_"):
        # Обработка клика по МОП-у - показываем меню фильтрации
        data_parts = data.replace("mop_filter_", "")
        
        # Проверяем, это МОП из списка РОП-а или обычный МОП
        if data_parts.startswith("rop_"):
            # Формат: mop_filter_rop_{rop_idx}_{mop_idx}
            parts = data_parts.replace("rop_", "").split("_")
            if len(parts) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            try:
                rop_idx = int(parts[0])
                mop_idx = int(parts[1])
            except:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            
            rops_menu = context.user_data.get('rops_menu') or []
            if rop_idx < 0 or rop_idx >= len(rops_menu):
                await query.edit_message_text("❌ РОП не найден")
                return
            
            rop_mops_menu = context.user_data.get(f'rop_{rop_idx}_mops_menu') or []
            if mop_idx < 0 or mop_idx >= len(rop_mops_menu):
                await query.edit_message_text("❌ МОП не найден")
                return
            
            mop_name = rop_mops_menu[mop_idx]['name']
            rop_name = rops_menu[rop_idx]['name']
            
            # Получаем имя ДД из контекста, если пользователь - ДД
            role = get_user_role(context)
            dd_name = None
            if role == ROLE_DD:
                dd_name = context.user_data.get('dd_query_name')
            
            db_manager = await get_db_manager()
            # Фильтруем по ДД, РОП и МОП одновременно
            totals = await db_manager.get_mop_category_stats(mop_name, rop_name=rop_name, dd_name=dd_name)
            message = f"Объекты МОП: {mop_name}\n"
            keyboard = [
                [InlineKeyboardButton(f"Все объекты ({totals['total']})", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_all")],
                [InlineKeyboardButton(f"Объекты категории А ({totals['cat_A']})", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_A")],
                [InlineKeyboardButton(f"Объекты категории В ({totals['cat_B']})", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_B")],
                [InlineKeyboardButton(f"Объекты категории С ({totals['cat_C']})", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_C")],
                [InlineKeyboardButton("🔙 Назад", callback_data=f"rop_mops_{rop_idx}_page_1")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
            ]
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            # Обычный МОП из списка "Мои МОП-ы"
            try:
                idx = int(data_parts.strip())
            except Exception:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            mops_menu = context.user_data.get('mops_menu') or []
            if idx < 0 or idx >= len(mops_menu):
                await query.edit_message_text("❌ МОП не найден")
                return
            mop_name = mops_menu[idx]['name']
            db_manager = await get_db_manager()
            # Получаем статистику по категориям для этого МОП-а без загрузки всех объектов
            totals = await db_manager.get_mop_category_stats(mop_name)
            message = f"Объекты МОП: {mop_name}\n"
            keyboard = [
                [InlineKeyboardButton(f"Все объекты ({totals['total']})", callback_data=f"mop_category_{idx}_all")],
                [InlineKeyboardButton(f"Объекты категории А ({totals['cat_A']})", callback_data=f"mop_category_{idx}_A")],
                [InlineKeyboardButton(f"Объекты категории В ({totals['cat_B']})", callback_data=f"mop_category_{idx}_B")],
                [InlineKeyboardButton(f"Объекты категории С ({totals['cat_C']})", callback_data=f"mop_category_{idx}_C")],
                [InlineKeyboardButton("🔙 Назад", callback_data="my_mops_page_1")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
            ]
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("mop_category_"):
        # Обработка клика по категории МОП-а - показываем список объектов с пагинацией
        # Формат: mop_category_{idx}_page_{page}_{category} или mop_category_{idx}_{category}
        # Или: mop_category_rop_{rop_idx}_{mop_idx}_page_{page}_{category} или mop_category_rop_{rop_idx}_{mop_idx}_{category}
        data_parts = data.replace("mop_category_", "")
        
        # Проверяем, это МОП из списка РОП-а или обычный МОП
        is_rop_mop = data_parts.startswith("rop_")
        
        if is_rop_mop:
            # Формат: rop_{rop_idx}_{mop_idx}_page_{page}_{category} или rop_{rop_idx}_{mop_idx}_{category}
            data_parts = data_parts.replace("rop_", "")
            page = 1
            
            if "_page_" in data_parts:
                parts = data_parts.split("_page_", 1)
                if len(parts) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                idxs_str = parts[0]
                rest = parts[1]
                page_category = rest.rsplit("_", 1)
                if len(page_category) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                try:
                    page = int(page_category[0])
                    category = page_category[1]
                except:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                
                idxs_parts = idxs_str.rsplit("_", 1)
                if len(idxs_parts) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                try:
                    rop_idx = int(idxs_parts[0])
                    mop_idx = int(idxs_parts[1])
                except:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
            else:
                # Формат: {rop_idx}_{mop_idx}_{category}
                # Разбиваем по последним двум подчеркиваниям, чтобы правильно извлечь rop_idx, mop_idx и category
                parts = data_parts.rsplit("_", 2)
                if len(parts) != 3:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                try:
                    rop_idx = int(parts[0])
                    mop_idx = int(parts[1])
                    category = parts[2]
                except:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
            
            rops_menu = context.user_data.get('rops_menu') or []
            if rop_idx < 0 or rop_idx >= len(rops_menu):
                await query.edit_message_text("❌ РОП не найден")
                return
            
            rop_mops_menu = context.user_data.get(f'rop_{rop_idx}_mops_menu') or []
            if mop_idx < 0 or mop_idx >= len(rop_mops_menu):
                await query.edit_message_text("❌ МОП не найден")
                return
            
            mop_name = rop_mops_menu[mop_idx]['name']
            rop_name = rops_menu[rop_idx]['name']
            category_filter = None if category == "all" else category
            
            # Получаем имя ДД из контекста, если пользователь - ДД
            role = get_user_role(context)
            dd_name = None
            if role == ROLE_DD:
                dd_name = context.user_data.get('dd_query_name')
            
            # Сохраняем информацию для кнопки "Назад" в деталях контракта
            context.user_data['back_to_mop_list'] = {'rop_idx': str(rop_idx), 'mop_idx': str(mop_idx), 'category': category, 'page': page}
            
            await show_loading(query)
            db_manager = await get_db_manager()
            # Фильтруем по ДД, РОП и МОП одновременно
            contracts = await db_manager.get_mop_contracts_by_category(mop_name, category_filter, rop_name=rop_name, dd_name=dd_name)
            
            if not contracts:
                category_label = "Все объекты" if category == "all" else f"Объекты категории {category}"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"mop_filter_rop_{rop_idx}_{mop_idx}")]]
                await query.edit_message_text(f"{category_label}:\n\nОбъекты не найдены", reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # Пагинация: по 10 объектов на страницу
            contracts_per_page = 10
            total_count = len(contracts)
            start_idx = (page - 1) * contracts_per_page
            end_idx = start_idx + contracts_per_page
            contracts_page = contracts[start_idx:end_idx]
            
            category_label = "Все объекты:" if category == "all" else f"Объекты категории {category}:"
            message = f"{category_label}\n\n"
            keyboard = []
            
            for contract in contracts_page:
                crm_id = contract.get('CRM ID', 'N/A')
                client_name_raw = contract.get('Имя клиента и номер', 'N/A')
                client_name = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
                address = contract.get('Адрес', 'N/A')
                expires = contract.get('Истекает', 'N/A')
                category_val = contract.get('category', 'N/A')
                
                message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
                message += f"Клиент: {client_name}\n"
                message += f"Адрес: {address}\n"
                message += f"Истекает: {format_date_ddmmyyyy(expires)}\n"
                message += f"Категория: {category_val}\n"
                message += "-"*30 + "\n\n"
                
                # Сохраняем информацию о РОП-е, МОП-е и категории в callback_data для правильного возврата
                if page > 1:
                    keyboard.append([InlineKeyboardButton(
                        f"CRM ID: {crm_id}", 
                        callback_data=f"contract_{crm_id}_mop_rop_{rop_idx}_{mop_idx}_page_{page}_{category}"
                    )])
                else:
                    keyboard.append([InlineKeyboardButton(
                        f"CRM ID: {crm_id}", 
                        callback_data=f"contract_{crm_id}_mop_rop_{rop_idx}_{mop_idx}_{category}"
                    )])
            
            # Кнопки пагинации
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_page_{page-1}_{category}"))
            if end_idx < total_count:
                nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"mop_category_rop_{rop_idx}_{mop_idx}_page_{page+1}_{category}"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            
            # Кнопка "Назад" к меню МОП-а
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"mop_filter_rop_{rop_idx}_{mop_idx}")])
            keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        else:
            # Обычный МОП из списка "Мои МОП-ы"
            # Формат: {idx}_page_{page}_{category} или {idx}_{category}
            page = 1
            if "_page_" in data_parts:
                parts = data_parts.split("_page_", 1)
                if len(parts) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                idx_str = parts[0]
                rest = parts[1]
                page_category = rest.rsplit("_", 1)
                if len(page_category) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                try:
                    page = int(page_category[0])
                    category = page_category[1]
                except:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
            else:
                parts = data_parts.rsplit("_", 1)
                if len(parts) != 2:
                    await query.edit_message_text("❌ Ошибка формата данных")
                    return
                idx_str, category = parts
            
            try:
                idx = int(idx_str)
            except Exception:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            
            mops_menu = context.user_data.get('mops_menu') or []
            if idx < 0 or idx >= len(mops_menu):
                await query.edit_message_text("❌ МОП не найден")
                return
            mop_name = mops_menu[idx]['name']
            category_filter = None if category == "all" else category
            
            # Сохраняем информацию для кнопки "Назад" в деталях контракта
            context.user_data['back_to_mop_list'] = {'mop_idx': str(idx), 'category': category, 'page': page}
            
            await show_loading(query)
            db_manager = await get_db_manager()
            contracts = await db_manager.get_mop_contracts_by_category(mop_name, category_filter)
            
            if not contracts:
                category_label = "Все объекты" if category == "all" else f"Объекты категории {category}"
                keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"mop_filter_{idx}")]]
                await query.edit_message_text(f"{category_label}:\n\nОбъекты не найдены", reply_markup=InlineKeyboardMarkup(keyboard))
                return
            
            # Пагинация: по 10 объектов на страницу
            contracts_per_page = 10
            total_count = len(contracts)
            start_idx = (page - 1) * contracts_per_page
            end_idx = start_idx + contracts_per_page
            contracts_page = contracts[start_idx:end_idx]
            
            category_label = "Все объекты:" if category == "all" else f"Объекты категории {category}:"
            message = f"{category_label}\n\n"
            keyboard = []
            
            for contract in contracts_page:
                crm_id = contract.get('CRM ID', 'N/A')
                client_name_raw = contract.get('Имя клиента и номер', 'N/A')
                client_name = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
                address = contract.get('Адрес', 'N/A')
                expires = contract.get('Истекает', 'N/A')
                category_val = contract.get('category', 'N/A')
                
                message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
                message += f"Клиент: {client_name}\n"
                message += f"Адрес: {address}\n"
                message += f"Истекает: {format_date_ddmmyyyy(expires)}\n"
                message += f"Категория: {category_val}\n"
                message += "-"*30 + "\n\n"
                
                # Сохраняем информацию о МОП-е и категории в callback_data для правильного возврата
                if page > 1:
                    keyboard.append([InlineKeyboardButton(
                        f"CRM ID: {crm_id}", 
                        callback_data=f"contract_{crm_id}_mop_{idx}_page_{page}_{category}"
                    )])
                else:
                    keyboard.append([InlineKeyboardButton(
                        f"CRM ID: {crm_id}", 
                        callback_data=f"contract_{crm_id}_mop_{idx}_{category}"
                    )])
            
            # Кнопки пагинации
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"mop_category_{idx}_page_{page-1}_{category}"))
            if end_idx < total_count:
                nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"mop_category_{idx}_page_{page+1}_{category}"))
            
            if nav_buttons:
                keyboard.append(nav_buttons)
            
            # Кнопка "Назад" к меню МОП-а
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"mop_filter_{idx}")])
            keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
            await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    elif data.startswith("contracts_filter_"):
        # Обработка фильтрации объектов РОП/ДД по категориям с пагинацией
        # Формат: contracts_filter_{category} или contracts_filter_{category}_page_{page}
        data_parts = data.replace("contracts_filter_", "")
        
        # Проверяем, есть ли номер страницы
        page = 1
        if "_page_" in data_parts:
            # Формат: {category}_page_{page}
            parts = data_parts.split("_page_", 1)
            if len(parts) != 2:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
            category_filter = parts[0]
            try:
                page = int(parts[1])
            except:
                await query.edit_message_text("❌ Ошибка формата данных")
                return
        else:
            # Формат: {category}
            category_filter = data_parts
        
        if category_filter == "all":
            category_filter = None
        
        role = get_user_role(context)
        agent_name = context.user_data.get('agent_name')
        name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
        
        await show_loading(query)
        db_manager = await get_db_manager()
        contracts_filtered = await db_manager.get_contracts_by_category(name_for_query, role, category_filter)
        
        category_label = "Все объекты:" if category_filter is None else f"Объекты категории {category_filter}:"
        
        if not contracts_filtered:
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="my_contracts")]]
            await query.edit_message_text(f"{category_label}\n\nОбъекты не найдены", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        
        # Пагинация: по 10 объектов на страницу
        contracts_per_page = 10
        total_count = len(contracts_filtered)
        start_idx = (page - 1) * contracts_per_page
        end_idx = start_idx + contracts_per_page
        contracts_page = contracts_filtered[start_idx:end_idx]
        
        message = f"{category_label}\n\n"
        keyboard = []
        
        # Определяем строку категории для callback_data
        category_str = "all" if category_filter is None else category_filter
        
        # Сохраняем информацию о фильтре и странице для кнопки "Назад" в деталях контракта
        context.user_data['back_to_contracts_list'] = {
            'category': category_str,
            'page': page
        }
        
        for contract in contracts_page:
            crm_id = contract.get('CRM ID', 'N/A')
            client_name_raw = contract.get('Имя клиента и номер', 'N/A')
            client_name = clean_client_name(str(client_name_raw).split(':')[0].strip()) if isinstance(client_name_raw, str) else str(client_name_raw)
            address = contract.get('Адрес', 'N/A')
            expires = contract.get('Истекает', 'N/A')
            category_val = contract.get('category', 'N/A')
            
            message += f"[CRM ID: {crm_id}](https://t.me/{BOT_USERNAME}?start=crm_{crm_id})\n"
            message += f"Клиент: {client_name}\n"
            message += f"Адрес: {address}\n"
            message += f"Истекает: {format_date_ddmmyyyy(expires)}\n"
            message += f"Категория: {category_val}\n"
            message += "-"*30 + "\n\n"
            
            # Сохраняем информацию о фильтре и странице в callback_data для правильного возврата
            if page > 1:
                keyboard.append([InlineKeyboardButton(
                    f"CRM ID: {crm_id}", 
                    callback_data=f"contract_{crm_id}_filter_{category_str}_page_{page}"
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"CRM ID: {crm_id}", 
                    callback_data=f"contract_{crm_id}_filter_{category_str}"
                )])
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Предыдущие", callback_data=f"contracts_filter_{category_str}_page_{page-1}"))
        if end_idx < total_count:
            nav_buttons.append(InlineKeyboardButton("Следующие ▶️", callback_data=f"contracts_filter_{category_str}_page_{page+1}"))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        # Кнопка "Назад" к меню статистики
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="my_contracts")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')


    elif data.startswith("change_category_menu_"):
        # Показываем меню выбора категории для РОП-а
        crm_id = data.replace("change_category_menu_", "")
        
        # Определяем callback_data для кнопки "Назад" с учетом контекста МОП-а
        back_callback = f"contract_{crm_id}"
        back_info = context.user_data.get('back_to_mop_list')
        if back_info:
            mop_idx = back_info.get('mop_idx')
            category = back_info.get('category')
            back_callback = f"contract_{crm_id}_mop_{mop_idx}_{category}"
        
        keyboard = [
            [InlineKeyboardButton("Категория А", callback_data=f"set_category_{crm_id}_A")],
            [InlineKeyboardButton("Категория В", callback_data=f"set_category_{crm_id}_B")],
            [InlineKeyboardButton("Категория С", callback_data=f"set_category_{crm_id}_C")],
            [InlineKeyboardButton("🔙 Назад", callback_data=back_callback)],
        ]
        await query.edit_message_text(
            "Выберите категорию:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("set_category_"):
        # Устанавливаем категорию для контракта
        parts = data.replace("set_category_", "").rsplit("_", 1)
        if len(parts) != 2:
            await query.edit_message_text("❌ Ошибка формата данных")
            return
        crm_id, category = parts
        category = category.upper()
        
        if category not in {'A', 'B', 'C'}:
            await query.edit_message_text("❌ Недопустимая категория")
            return
        
        await show_loading(query)
        db_manager = await get_db_manager()
        success = await db_manager.update_contract_category(crm_id, category)
        
        if success:
            await query.answer(f"✅ Категория изменена на {category}")
            
            # Обновляем отображение контракта
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                role = get_user_role(context)
                name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
                contract = await db_manager.search_contract_by_crm_id(crm_id, name_for_query, role)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.edit_message_text("❌ Контракт не найден")
            else:
                await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
        else:
            await query.edit_message_text("❌ Ошибка при обновлении категории")

    elif data.startswith("update_status_"):
        # Обновление статуса контракта
        crm_id = data.replace("update_status_", "")
        await update_contract_status(update, context, crm_id)
    elif data.startswith("collage_build_"):
        crm_id = data.replace("collage_build_", "")
        user_id = update.effective_user.id
        await show_loading(query)
        await query.edit_message_text("Получаю данные из CRM...")
        
        try:
            # Получаем данные контракта из базы данных
            agent_name = context.user_data.get('agent_name')
            db_contract = None
            if agent_name:
                db_manager = await get_db_manager()
                db_contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
            
            # Получаем данные из API с данными контракта
            collage_input = await get_collage_data_from_api(crm_id, db_contract)
            if not collage_input:
                await query.edit_message_text("❌ Не удалось получить данные из CRM. Проверьте CRM ID.")
                return
            
            # Получаем имя клиента из базы данных
            if db_contract and db_contract.get('Имя клиента и номер'):
                client_info = db_contract['Имя клиента и номер']
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
            # Очищаем временные файлы при ошибке
            await cleanup_collage_files(context, user_id)

    elif data.startswith("action_pro_collage_"):
        crm_id = data.replace("action_pro_collage_", "")
        await show_loading(query)
        
        try:
            # Обновляем статус проф коллажа в базе данных
            db_manager = await get_db_manager()
            success = await db_manager.update_contract(crm_id, {"prof_collage": True})
            
            if success:
                await query.answer("✅ Проф коллаж отмечен как выполненный")
                
                # Обновляем отображение контракта
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                    if contract:
                        await show_contract_detail_by_contract(update, context, contract)
                    else:
                        await query.edit_message_text("❌ Контракт не найден")
                else:
                    await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            else:
                await query.edit_message_text("❌ Ошибка при обновлении статуса проф коллажа")
                
        except Exception as e:
            logger.error(f"Ошибка обновления проф коллажа: {e}")
            await query.edit_message_text("❌ Ошибка при обновлении проф коллажа")

    elif data.startswith("action_show_"):
        crm_id = data.replace("action_show_", "")
        await update_show_count(update, context, crm_id)

    elif data.startswith("push_"):
        crm_id = data.replace("push_", "")
        await show_loading(query)
        
        try:
            # Обновляем статус дожима в базе данных
            db_manager = await get_db_manager()
            success = await db_manager.update_contract(crm_id, {"push_for_price": True})
            
            if success:
                # Если был режим аналитики и дожим сделан, меняем статус на "Корректировка цены"
                await db_manager.update_contract(crm_id, {"status": "Корректировка цены"})
                
                await query.answer("✅ Дожим отмечен как выполненный")
                
                # Обновляем отображение контракта
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                    if contract:
                        await show_contract_detail_by_contract(update, context, contract)
                    else:
                        await query.edit_message_text("❌ Контракт не найден")
                else:
                    await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            else:
                await query.edit_message_text("❌ Ошибка при обновлении статуса дожима")
                
        except Exception as e:
            logger.error(f"Ошибка обновления дожима: {e}")
            await query.edit_message_text("❌ Ошибка при обновлении дожима")

    elif data.startswith("price_adjust_"):
        crm_id = data.replace("price_adjust_", "")
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_price_{crm_id}'
        
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
        ])
        
        await show_loading(query)
        await query.edit_message_text(
            f"💰 Введите новую цену для контракта {crm_id}:\n\n"
            f"Пример: 25000000 или 25 000 000",
            reply_markup=back_keyboard
        )

    elif data.startswith("add_link_type_"):
        # Обработка выбора типа ссылки
        link_data = data.replace("add_link_type_", "")
        logger.info(f"add_link_type_ handler: callback_data='{data}', link_data='{link_data}'")
        if "_" in link_data:
            # Разделяем с конца, чтобы правильно обработать CRM ID с подчеркиваниями
            parts = link_data.rsplit("_", 1)
            if len(parts) == 2:
                crm_id, link_type = parts
                logger.info(f"add_link_type_ handler: parsed crm_id='{crm_id}', link_type='{link_type}'")
                await handle_link_type_selection(update, context, crm_id, link_type)

    elif data.startswith("add_link_"):
        crm_id = data.replace("add_link_", "")
        logger.info(f"add_link_ handler: callback_data='{data}', extracted crm_id='{crm_id}'")
        await show_add_link_menu(update, context, crm_id)

    elif data.startswith("status_menu_"):
        crm_id = data.replace("status_menu_", "")
        await show_status_menu(update, context, crm_id)

    elif data.startswith("analytics_done_"):
        crm_id = data.replace("analytics_done_", "")
        await show_loading(query)
        
        try:
            # Обновляем статус аналитики в базе данных
            db_manager = await get_db_manager()
            success = await db_manager.update_contract(crm_id, {"analytics": True})
            
            if success:
                await query.answer("✅ Аналитика отмечена как выполненная")
                
                # Обновляем отображение контракта
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                    if contract:
                        await show_contract_detail_by_contract(update, context, contract)
                    else:
                        await query.edit_message_text("❌ Контракт не найден")
                else:
                    await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            else:
                await query.edit_message_text("❌ Ошибка при обновлении статуса аналитики")
                
        except Exception as e:
            logger.error(f"Ошибка обновления аналитики: {e}")
            await query.edit_message_text("❌ Ошибка при обновлении аналитики")

    elif data.startswith("analytics_provided_"):
        crm_id = data.replace("analytics_provided_", "")
        await show_loading(query)
        
        try:
            # Обновляем статус предоставления аналитики в базе данных
            db_manager = await get_db_manager()
            success = await db_manager.update_contract(crm_id, {"provide_analytics": True})
            
            if success:
                await query.answer("✅ Аналитика запланирована через 5 дней")
                
                # Обновляем отображение контракта
                agent_name = context.user_data.get('agent_name')
                if agent_name:
                    contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                    if contract:
                        await show_contract_detail_by_contract(update, context, contract)
                    else:
                        await query.edit_message_text("❌ Контракт не найден")
                else:
                    await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            else:
                await query.edit_message_text("❌ Ошибка при обновлении статуса аналитики")
                
        except Exception as e:
            logger.error(f"Ошибка обновления аналитики: {e}")
            await query.edit_message_text("❌ Ошибка при обновлении аналитики")

    elif data.startswith("set_status_"):
        # Установка статуса контракта
        status_data = data.replace("set_status_", "")
        if "_" in status_data:
            # Разделяем с конца, чтобы правильно обработать CRM ID с подчеркиваниями
            parts = status_data.rsplit("_", 1)
            if len(parts) == 2:
                crm_id, new_status = parts
                await set_contract_status(update, context, crm_id, new_status)

    elif data.startswith("collage_proceed_"):
        crm_id = data.replace("collage_proceed_", "")
        user_id = update.effective_user.id
        user_states[user_id] = f'waiting_collage_photos_{crm_id}'
        
        # Сбрасываем список фото в вводе коллажа
        ci = user_collage_inputs.get(user_id)
        if ci:
            ci.photo_paths = []
            user_collage_inputs[user_id] = ci

        # Первичное сообщение-инструкция с кнопкой "Отмена"
        progress_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Отмена", callback_data=f"collage_cancel_{crm_id}")]
        ])

        progress_text = (
            "📸 Теперь отправьте фотографии для коллажа (4 штуки)\n"
            "Первое фото как основное фото (фото ЖК)\n"
            "2-3-4 Это фото внутри квартиры\n\n"
            "0/4"
        )

        edited_msg = await query.edit_message_text(progress_text, reply_markup=progress_keyboard)

        # Сохраняем данные прогресса для последующего редактирования
        context.user_data['collage_progress'] = {
            'crm_id': crm_id,
            'chat_id': edited_msg.chat.id,
            'message_id': edited_msg.message_id,
            'count': 0
        }
        
    elif data.startswith("edit_collage_"):
        # Обработка редактирования полей коллажа
        parts = data.replace("edit_collage_", "").split("_")
        field = parts[0]
        crm_id = parts[1]
        user_id = update.effective_user.id
        
        field_names = {
            'complex': 'название ЖК',
            'address': 'адрес',
            'area': 'площадь',
            'rooms': 'количество комнат',
            'floor': 'этаж',
            'price': 'цену',
            'class': 'класс жилья',
            'rop': 'имя РОП',
            'mop': 'имя МОП',
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

    elif data.startswith("collage_cancel_") and not data.startswith("collage_cancel_creation_"):
        # Отмена процесса загрузки фотографий для коллажа
        crm_id = data.replace("collage_cancel_", "")
        user_id = update.effective_user.id
        user_states[user_id] = 'authenticated'
        
        # Очищаем прогресс и временные файлы
        if 'collage_progress' in context.user_data:
            del context.user_data['collage_progress']
        await cleanup_collage_files(context, user_id)
        
        # Возвращаемся к карточке контракта
        try:
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                db_manager = await get_db_manager()
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await query.answer("❌ Контракт не найден")
            else:
                await query.answer("❌ Ошибка: агент не найден в сессии")
        except Exception as e:
            logger.error(f"Ошибка отмены коллажа: {e}")
            await query.answer("❌ Ошибка отмены процесса")

    elif data.startswith("collage_save_"):
        # Сохранение результата коллажа: отметим в БД и вернем карточку
        crm_id = data.replace("collage_save_", "")
        user_id = update.effective_user.id
        try:
            db_manager = await get_db_manager()
            await db_manager.update_contract(crm_id, {'collage': True})

            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    # Редактируем сообщение с коллажем, убираем кнопки и оставляем только "готов!"
                    try:
                        await update.callback_query.edit_message_caption(
                            caption=f"✅ Коллаж для контракта {crm_id} готов!",
                            reply_markup=None
                        )
                    except Exception:
                        # Если не удается отредактировать, отправляем новое сообщение
                        await update.callback_query.answer("✅ Коллаж сохранен!")
                    # продолжим
                else:
                    await update.callback_query.answer("❌ Контракт не найден")
                # В любом случае после сохранения возвращаем карточку объекта
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
            else:
                await update.callback_query.answer("❌ Ошибка: агент не найден в сессии")
            
            # Очищаем временные файлы
            await cleanup_collage_files(context, user_id)
            
        except Exception as e:
            logger.error(f"Ошибка сохранения коллажа: {e}")
            await update.callback_query.answer("❌ Ошибка сохранения коллажа")
            # Очищаем временные файлы даже при ошибке
            await cleanup_collage_files(context, user_id)

    elif data.startswith("collage_redo_"):
        # Переделать коллаж — возвращаемся на этап получения данных из CRM
        crm_id = data.replace("collage_redo_", "")
        user_id = update.effective_user.id
        try:
            # Сначала отредактируем подпись текущего сообщения с коллажем: уберем кнопки и текст "Выберите действие"
            try:
                await update.callback_query.edit_message_caption(
                    caption=f"✅ Коллаж для контракта {crm_id} готов!",
                    reply_markup=None
                )
            except Exception:
                pass

            # Очищаем предыдущие данные и временные файлы
            await cleanup_collage_files(context, user_id)
            if 'collage_progress' in context.user_data:
                del context.user_data['collage_progress']
            
            # Перейдем заново к действию collage_build_
            await update.callback_query.answer("🔄 Переделываю коллаж...")
            
            # Получаем данные контракта из базы данных
            agent_name = context.user_data.get('agent_name')
            db_contract = None
            if agent_name:
                db_manager = await get_db_manager()
                db_contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
            
            collage_input = await get_collage_data_from_api(crm_id, db_contract)
            if not collage_input:
                await update.callback_query.answer("❌ Не удалось получить данные из CRM. Проверьте CRM ID.")
                return

            # Получаем имя клиента из базы данных для корректного имени
            if db_contract and db_contract.get('Имя клиента и номер'):
                client_info = db_contract['Имя клиента и номер']
                raw_client_name = client_info.split(':')[0].strip()
                client_name = clean_client_name(raw_client_name)
                collage_input.client_name = client_name

            user_collage_inputs[user_id] = collage_input
            await show_collage_data_with_edit_buttons(update.callback_query, collage_input, crm_id)
        except Exception as e:
            logger.error(f"Ошибка перезапуска коллажа: {e}")
            await update.callback_query.answer("❌ Ошибка при перезапуске коллажа")
            # Очищаем временные файлы при ошибке
            await cleanup_collage_files(context, user_id)

    elif data.startswith("collage_cancel_creation_"):
        # Отмена создания — просто возвращаем карточку без изменения коллажа
        crm_id = data.replace("collage_cancel_creation_", "")
        user_id = update.effective_user.id
        try:
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                db_manager = await get_db_manager()
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    # Редактируем сообщение с коллажем: оставляем "готов!" и убираем кнопки
                    try:
                        await update.callback_query.edit_message_caption(
                            caption=f"✅ Коллаж для контракта {crm_id} готов!",
                            reply_markup=None
                        )
                    except Exception:
                        pass
                    # Возвращаемся к карточке объекта
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await update.callback_query.answer("❌ Контракт не найден")
            else:
                await update.callback_query.answer("❌ Ошибка: агент не найден в сессии")
            
            # Очищаем временные файлы
            await cleanup_collage_files(context, user_id)
            
        except Exception as e:
            logger.error(f"Ошибка отмены создания коллажа: {e}")
            await update.callback_query.answer("❌ Ошибка отмены создания коллажа")
            # Очищаем временные файлы даже при ошибке
            await cleanup_collage_files(context, user_id)

    elif data.startswith("collage_finish_"):
        # Завершение и создание коллажа после 4 фото
        crm_id = data.replace("collage_finish_", "")
        user_id = update.effective_user.id
        # Начинаем создание коллажа напрямую
        try:
            collage_input = user_collage_inputs.get(user_id)
            if not collage_input:
                await update.callback_query.edit_message_text("❌ Данные коллажа не найдены")
                user_states[user_id] = 'authenticated'
                return

            # Обновляем прогресс-сообщение
            cp = context.user_data.get('collage_progress')
            if cp and cp.get('message_id') and cp.get('chat_id'):
                try:
                    await context.bot.edit_message_text(
                        chat_id=cp['chat_id'],
                        message_id=cp['message_id'],
                        text="🎨 Создаю коллаж..."
                    )
                except Exception:
                    pass

            # Подготовка фото
            if hasattr(collage_input, 'photo_paths') and collage_input.photo_paths:
                collage_input.photos = collage_input.photo_paths

            # Генерация коллажа
            collage_path, collage_html = await render_collage_to_image(collage_input)

            if collage_path:
                # Отправляем итоговый коллаж в тот же чат с кнопками действий
                try:
                    target_chat_id = cp['chat_id'] if cp and cp.get('chat_id') else update.effective_chat.id
                    action_keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton("💾 Сохранить коллаж", callback_data=f"collage_save_{crm_id}")],
                        [InlineKeyboardButton("🔁 Переделать коллаж", callback_data=f"collage_redo_{crm_id}")],
                        [InlineKeyboardButton("❌ Отменить создание", callback_data=f"collage_cancel_creation_{crm_id}")],
                    ])
                    sent_ok = await send_photo_with_retry(
                        context.bot,
                        target_chat_id,
                        collage_path,
                        caption=f"✅ Коллаж для контракта {crm_id} готов!\n\nВыберите действие:",
                        reply_markup=action_keyboard,
                        attempts=3,
                        delay=2.5
                    )
                    if not sent_ok:
                        raise RuntimeError("send_photo retry failed")

                    # Сразу удаляем временные файлы (png + html)
                    try:
                        if os.path.exists(collage_path):
                            os.remove(collage_path)
                    except Exception:
                        pass
                    try:
                        if os.path.exists(collage_html):
                            os.remove(collage_html)
                    except Exception:
                        pass

                except Exception as send_err:
                    logger.error(f"Ошибка отправки коллажа: {send_err}")
                    await update.callback_query.edit_message_text("❌ Ошибка отправки коллажа")
                    # Удаляем временные файлы при ошибке
                    try:
                        if os.path.exists(collage_path):
                            os.remove(collage_path)
                    except Exception:
                        pass
                    try:
                        if os.path.exists(collage_html):
                            os.remove(collage_html)
                    except Exception:
                        pass

                # Не обновляем БД и состояния до выбора действия
                user_states[user_id] = 'authenticated'
                if 'collage_progress' in context.user_data:
                    del context.user_data['collage_progress']
            else:
                await update.callback_query.edit_message_text("❌ Ошибка при создании коллажа")
                # Очищаем временные файлы при ошибке
                await cleanup_collage_files(context, user_id)
        except Exception as e:
            logger.error(f"Ошибка при завершении коллажа: {e}")
            await update.callback_query.edit_message_text("❌ Ошибка при создании коллажа")
            # Очищаем временные файлы при ошибке
            await cleanup_collage_files(context, user_id)

    else:
        # Неизвестный callback
        await query.edit_message_text("❌ Неизвестная команда")


async def update_show_count(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Увеличение счетчика показов"""
    try:
        query = update.callback_query
        db_manager = await get_db_manager()
        
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            return
            
        contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            # Если контракт не найден, попробуем обновить имя агента из телефона
            if await update_agent_name_from_phone(context):
                agent_name = context.user_data.get('agent_name')
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
            
            if not contract:
                await query.edit_message_text("❌ Контракт не найден")
                return
        
        current_shows = contract.get('shows', 0)
        new_shows = current_shows + 1
        
        await db_manager.update_contract(crm_id, {'shows': new_shows})
        
        await query.edit_message_text(f"✅ Счетчик показов увеличен до {new_shows}")

        # После подтверждения возвращаем карточку объекта со всеми кнопками
        try:
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                # Небольшая пауза, чтобы пользователь увидел подтверждение
                await asyncio.sleep(0.8)
                updated_contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                if updated_contract:
                    await show_contract_detail_by_contract(update, context, updated_contract)
        except Exception as inner_e:
            logger.warning(f"Не удалось вернуть карточку после увеличения показов: {inner_e}")
        
    except Exception as e:
        logger.error(f"Ошибка обновления счетчика показов: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка обновления счетчика показов")


async def show_status_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает меню смены статуса"""
    try:
        query = update.callback_query
        
        # Новый список статусов по требованиям:
        # Корректировка цены / Аналитика / Задаток/сделка / Реализовано / Размещено
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Корректировка цены", callback_data=f"set_status_{crm_id}_Корректировка цены")],
            [InlineKeyboardButton("Аналитика", callback_data=f"set_status_{crm_id}_Аналитика")],
            [InlineKeyboardButton("Задаток/сделка", callback_data=f"set_status_{crm_id}_Задаток/сделка")],
            [InlineKeyboardButton("Реализовано", callback_data=f"set_status_{crm_id}_Реализовано")],
            [InlineKeyboardButton("Размещено", callback_data=f"set_status_{crm_id}_Размещено")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")]
        ])
        
        await query.edit_message_text(
            f"📋 Выберите новый статус для контракта {crm_id}:",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка показа меню статуса: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка показа меню статуса")


async def set_contract_status(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str, new_status: str):
    """Устанавливает новый статус контракта"""
    try:
        query = update.callback_query
        db_manager = await get_db_manager()
        
        # Обновляем статус в БД
        await db_manager.update_contract(crm_id, {'status': new_status})
        
        await query.edit_message_text(f"✅ Статус контракта {crm_id} изменен на: {new_status}")
        try:
            await asyncio.sleep(0.6)
        except Exception:
            pass
        agent_name_ctx = context.user_data.get('agent_name')
        updated = await db_manager.search_contract_by_crm_id(crm_id, agent_name_ctx) if agent_name_ctx else None
        if updated:
            await show_contract_detail_by_contract(update, context, updated)
        
    except Exception as e:
        logger.error(f"Ошибка установки статуса: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка установки статуса")


async def show_add_link_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Показывает меню добавления ссылок"""
    try:
        query = update.callback_query
        db_manager = await get_db_manager()
        
        # Получаем имя агента из контекста
        agent_name = context.user_data.get('agent_name')
        logger.info(f"show_add_link_menu: CRM ID {crm_id}, agent_name from context: {agent_name}")
        
        if not agent_name:
            logger.warning(f"show_add_link_menu: No agent_name in context for CRM ID {crm_id}")
            await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            return
        
        # Получаем текущий контракт
        contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
        logger.info(f"show_add_link_menu: Contract found with agent_name '{agent_name}': {contract is not None}")
        
        if not contract:
            # Если контракт не найден, попробуем обновить имя агента из телефона
            logger.info(f"show_add_link_menu: Contract not found, trying to update agent_name from phone")
            if await update_agent_name_from_phone(context):
                agent_name = context.user_data.get('agent_name')
                logger.info(f"show_add_link_menu: Updated agent_name to: {agent_name}")
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                logger.info(f"show_add_link_menu: Contract found after update: {contract is not None}")
            
            if not contract:
                logger.error(f"show_add_link_menu: Contract {crm_id} not found for agent {agent_name}")
                await query.edit_message_text("❌ Контракт не найден")
                return
        
        # Создаем кнопки для каждого типа ссылки
        keyboard = []
        
        # Функция для проверки заполненности поля
        def is_field_filled(value):
            if value is None:
                return False
            if isinstance(value, str):
                return bool(value.strip())
            return bool(value)
        
        # Крыша
        krisha_value = contract.get('krisha', '')
        krisha_status = "✅" if is_field_filled(krisha_value) else "❌"
        keyboard.append([InlineKeyboardButton(f"{krisha_status} Крыша", callback_data=f"add_link_type_{crm_id}_krisha")])
        
        # Инстаграм
        instagram_value = contract.get('instagram', '')
        instagram_status = "✅" if is_field_filled(instagram_value) else "❌"
        keyboard.append([InlineKeyboardButton(f"{instagram_status} Инстаграм", callback_data=f"add_link_type_{crm_id}_instagram")])
        
        # Тикток
        tiktok_value = contract.get('tiktok', '')
        tiktok_status = "✅" if is_field_filled(tiktok_value) else "❌"
        keyboard.append([InlineKeyboardButton(f"{tiktok_status} Тикток", callback_data=f"add_link_type_{crm_id}_tiktok")])
        
        # Рассылка
        mailing_value = contract.get('mailing', '')
        mailing_status = "✅" if is_field_filled(mailing_value) else "❌"
        keyboard.append([InlineKeyboardButton(f"{mailing_status} Рассылка", callback_data=f"add_link_type_{crm_id}_mailing")])
        
        # Стрим
        stream_value = contract.get('stream', '')
        stream_status = "✅" if is_field_filled(stream_value) else "❌"
        keyboard.append([InlineKeyboardButton(f"{stream_status} Стрим", callback_data=f"add_link_type_{crm_id}_stream")])
        
        # Кнопка назад
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"contract_{crm_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"🔗 Выберите тип ссылки для контракта {crm_id}:\n\n"
            f"✅ - ссылка уже добавлена\n"
            f"❌ - ссылка не добавлена",
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Ошибка показа меню ссылок: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка показа меню ссылок")


async def handle_link_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str, link_type: str):
    """Обрабатывает выбор типа ссылки"""
    try:
        query = update.callback_query
        logger.info(f"handle_link_type_selection: CRM ID: {crm_id}, link_type: {link_type}")
        
        # Маппинг типов ссылок на их названия
        link_names = {
            'krisha': 'Крыша',
            'instagram': 'Инстаграм',
            'tiktok': 'Тикток',
            'mailing': 'Рассылка',
            'stream': 'Стрим'
        }
        
        link_name = link_names.get(link_type, link_type)
        
        # Сохраняем данные в контексте для обработки ввода
        context.user_data['waiting_for_link'] = {
            'crm_id': crm_id,
            'link_type': link_type,
            'link_name': link_name
        }
        
        # Устанавливаем состояние ожидания ввода ссылки
        user_id = update.effective_user.id
        user_states[user_id] = 'waiting_link_input'
        
        # Создаем клавиатуру с кнопкой "Назад"
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data=f"add_link_{crm_id}")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
        ])
        
        await query.edit_message_text(
            f"🔗 Введите ссылку для {link_name}:\n\n"
            f"Контракт: {crm_id}\n"
            f"Тип: {link_name}\n\n"
            f"Просто отправьте ссылку в следующем сообщении.",
            reply_markup=back_keyboard
        )
        
    except Exception as e:
        logger.error(f"Ошибка обработки выбора типа ссылки: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка обработки выбора типа ссылки")


async def handle_link_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод ссылки"""
    try:
        user_id = update.effective_user.id
        link_data = context.user_data.get('waiting_for_link')
        
        if not link_data:
            await update.message.reply_text("❌ Ошибка: данные ссылки не найдены")
            return
            
        crm_id = link_data['crm_id']
        link_type = link_data['link_type']
        link_name = link_data['link_name']
        link_url = update.message.text.strip()
        
        # Простая валидация URL
        if not (link_url.startswith('http://') or link_url.startswith('https://')):
            await update.message.reply_text(
                "❌ Неверный формат ссылки.\n\n"
                "Ссылка должна начинаться с http:// или https://\n"
                "Попробуйте еще раз:"
            )
            return

        # Маппинг типов ссылок на названия полей в базе данных
        field_mapping = {
            'krisha': 'krisha',
            'instagram': 'instagram',
            'tiktok': 'tiktok',
            'mailing': 'mailing',
            'stream': 'stream'
        }
        
        field_name = field_mapping.get(link_type)
        if not field_name:
            await update.message.reply_text("❌ Неизвестный тип ссылки")
            return
        
        # Обновляем ссылку в базе данных
        db_manager = await get_db_manager()
        update_data = {field_name: link_url}
        
        success = await db_manager.update_contract(crm_id, update_data)
        
        if success:
            # Очищаем состояние ожидания
            user_states[user_id] = 'authenticated'
            del context.user_data['waiting_for_link']
            
            # Получаем обновленный контракт и показываем его детали
            agent_name = context.user_data.get('agent_name')
            contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
            if contract:
                await show_contract_detail_by_contract(update, context, contract)
            else:
                await update.message.reply_text(
                    f"✅ Ссылка для {link_name} успешно добавлена!\n\n"
                    f"Контракт: {crm_id}\n"
                    f"Тип: {link_name}\n"
                    f"Ссылка: {link_url}"
                )
        else:
            await update.message.reply_text("❌ Ошибка при сохранении ссылки")
        
    except Exception as e:
        logger.error(f"Ошибка обработки ввода ссылки: {e}")
        await update.message.reply_text("❌ Ошибка при обработке ссылки")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    
    if state == 'waiting_phone':
        await handle_phone(update, context)
    elif state == 'waiting_client_search':
        await handle_client_search(update, context)
    elif state == 'waiting_rop_search':
        await handle_rop_search(update, context)
    elif state == 'waiting_mop_search':
        await handle_mop_search(update, context)
    elif state == 'waiting_password':
        await handle_password(update, context)
    elif state == 'waiting_link_input':
        await handle_link_input(update, context)
    elif state.startswith('editing_collage_'):
        # Обработка редактирования полей коллажа
        text = update.message.text
        await handle_collage_field_edit(update, context, text, state)
    # Удален текстовый поток waiting_collage_photos_ (используется callback-поток)
    elif state.startswith('waiting_price_'):
        # Обработка ввода новой цены
        text = update.message.text
        await handle_price_input(update, context, text, state)
    else:
        # Игнорируем неизвестные сообщения
        pass


async def handle_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_states.get(user_id) != 'waiting_phone':
        return
    
    phone_input = update.message.text.strip()
    digits = ''.join(c for c in phone_input if c.isdigit())
    if len(digits) == 11 and (digits.startswith('7') or digits.startswith('8')):
        digits = digits[1:]
    if len(digits) != 10:
        await update.message.reply_text("❌ Ошибка при валидации номера телефона")
        return
    context.user_data['login_username'] = digits
    user_states[user_id] = 'waiting_password'
    await update.message.reply_text("Введите пароль:")


async def handle_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_states.get(user_id) != 'waiting_password':
        return
    password = update.message.text.strip()
    username = context.user_data.get('login_username')
    if not username:
        user_states[user_id] = 'waiting_phone'
        await update.message.reply_text("Введите номер телефона:")
        return
    loading_msg = await update.message.reply_text("Идет авторизация...")
    async with APIClient() as api:
        profile = await api.login_and_get_profile(username, password)
    if not profile:
        await loading_msg.edit_text("❌ Неверный логин или пароль. Попробуйте снова.\nВведите номер телефона:")
        user_states[user_id] = 'waiting_phone'
        return
    agent_name = f"{(profile.get('surname') or '').strip()} {(profile.get('name') or '').strip()}".strip()
    context.user_data['agent_name'] = agent_name
    context.user_data['phone'] = profile.get('phone')
    context.user_data['auth_token'] = profile.get('token')
    user_states[user_id] = 'authenticated'

    # После логина — выбор роли
    pending_crm_id = context.user_data.get('pending_crm_id')
    if pending_crm_id:
        del context.user_data['pending_crm_id']
        db_manager = await get_db_manager()
        contract = await db_manager.search_contract_by_crm_id(pending_crm_id, agent_name)
        if contract:
            await loading_msg.delete()
            await show_contract_detail_by_contract(update, context, contract)
            return
        await loading_msg.edit_text(f"Контракт с CRM ID {pending_crm_id} не найден среди ваших сделок")
    else:
        await loading_msg.delete()
    await update.message.reply_text(
        "Выберите свою роль:",
        reply_markup=build_role_select_keyboard(context),
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
    
    db_manager = await get_db_manager()
    role = get_user_role(context)
    name_for_query = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
    contracts, total_count = await db_manager.search_contracts_by_client_name_lazy(client_name, name_for_query, 1, CONTRACTS_PER_PAGE, role)
    if contracts:
        if len(contracts) == 1:
            await show_contract_detail_by_contract(update, context, contracts[0])
        else:
            user_search_results[user_id] = contracts
            user_current_search_page[user_id] = 0
            context.user_data['last_search_query'] = client_name
            await show_search_results_page_lazy(loading_msg, contracts, 1, total_count, client_name, agent_name)
    else:
        await loading_msg.edit_text(f"Контракты для клиента '{client_name}' не найдены среди ваших сделок")
        reply_markup = build_main_menu_keyboard_by_role(context) if get_user_role(context) else build_main_menu_keyboard()
        agent_phone = context.user_data.get('phone')
        await update.message.reply_text(
            (f"{get_user_role(context)}: {agent_name}\n" if get_user_role(context) else f"Агент: {agent_name}\n") +
            f"Номер: {agent_phone}\n\nВыберите действие:",
            reply_markup=reply_markup,
        )
    user_states[user_id] = 'authenticated'


async def handle_rop_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка поиска РОП-а по имени"""
    user_id = update.effective_user.id
    rop_name = update.message.text.strip()
    agent_name = context.user_data.get('agent_name')
    role = get_user_role(context)
    
    if not agent_name or role != ROLE_DD:
        await update.message.reply_text("❌ Недоступно для вашей роли")
        user_states[user_id] = 'authenticated'
        return
    
    loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
    
    db_manager = await get_db_manager()
    dd_name = context.user_data.get('dd_query_name')
    rops = await db_manager.search_rops_by_name(rop_name, dd_name)
    
    if not rops:
        await loading_msg.edit_text(f"РОП-ы с именем '{rop_name}' не найдены")
        user_states[user_id] = 'authenticated'
        return
    
    # Показываем список найденных РОП-ов с кнопками
    message = f"Найдено РОП-ов: {len(rops)}\n\n"
    keyboard = []
    
    for i, rop in enumerate(rops):
        rop_full_name = rop.get('name', 'Не указан')
        rops_count = rop.get('count', 0)
        name_parts = rop_full_name.split()
        display_name = ' '.join(name_parts[:2]) if name_parts else rop_full_name
        message += f"{i+1}. {display_name} ({rops_count} объектов)\n"
        keyboard.append([InlineKeyboardButton(
            f"{display_name} ({rops_count})",
            callback_data=f"rop_search_result_{i}"
        )])
    
    # Сохраняем результаты поиска
    context.user_data['rop_search_results'] = rops
    
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    await loading_msg.edit_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
    user_states[user_id] = 'authenticated'


async def handle_mop_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка поиска МОП-а по имени"""
    user_id = update.effective_user.id
    mop_name = update.message.text.strip()
    agent_name = context.user_data.get('agent_name')
    role = get_user_role(context)
    
    if not agent_name or role not in {ROLE_ROP, ROLE_DD}:
        await update.message.reply_text("❌ Недоступно для вашей роли")
        user_states[user_id] = 'authenticated'
        return
    
    loading_msg = await update.message.reply_text("Идет загрузка. Пожалуйста подождите...")
    
    db_manager = await get_db_manager()
    owner_name = context.user_data.get('dd_query_name') if role == ROLE_DD else agent_name
    owner_role = ROLE_DD if role == ROLE_DD else ROLE_ROP
    
    mops = await db_manager.search_mops_by_name(mop_name, owner_name, owner_role)
    
    if not mops:
        await loading_msg.edit_text(f"МОП-ы с именем '{mop_name}' не найдены")
        user_states[user_id] = 'authenticated'
        return
    
    # Показываем список найденных МОП-ов с кнопками
    message = f"Найдено МОП-ов: {len(mops)}\n\n"
    keyboard = []
    
    for i, mop in enumerate(mops):
        mop_full_name = mop.get('name', 'Не указан')
        mops_count = mop.get('count', 0)
        name_parts = mop_full_name.split()
        display_name = ' '.join(name_parts[:2]) if name_parts else mop_full_name
        message += f"{i+1}. {display_name} ({mops_count} объектов)\n"
        keyboard.append([InlineKeyboardButton(
            f"{display_name} ({mops_count})",
            callback_data=f"mop_search_result_{i}"
        )])
    
    # Сохраняем результаты поиска
    context.user_data['mop_search_results'] = mops
    
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    await loading_msg.edit_text(message, reply_markup=InlineKeyboardMarkup(keyboard))
    user_states[user_id] = 'authenticated'


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка фотографий"""
    user_id = update.effective_user.id
    state = user_states.get(user_id, '')
    
    if state.startswith('waiting_collage_photos_'):
        # Обработка фотографий для коллажа с прогрессом 1/4..4/4
        try:
            # Получаем фотографию
            photo = update.message.photo[-1]
            file = await context.bot.get_file(photo.file_id)

            photos_dir = "data"
            os.makedirs(photos_dir, exist_ok=True)

            import uuid
            filename = f"{uuid.uuid4()}.jpg"
            file_path = os.path.join(photos_dir, filename)

            await file.download_to_drive(file_path)

            collage_input = user_collage_inputs.get(user_id)
            if not collage_input:
                await update.message.reply_text("❌ Данные коллажа не найдены")
                # Очищаем временные файлы
                await cleanup_collage_files(context, user_id)
                return

            if not hasattr(collage_input, 'photo_paths'):
                collage_input.photo_paths = []

            # Не добавляем больше 4 фотографий
            if len(collage_input.photo_paths) >= 4:
                # Игнорируем дополнительные фото
                return

            collage_input.photo_paths.append(file_path)
            user_collage_inputs[user_id] = collage_input

            # Обновляем прогресс в закрепленном сообщении
            cp = context.user_data.get('collage_progress', {})
            crm_id = state.replace('waiting_collage_photos_', '')
            count = len(collage_input.photo_paths)
            progress_text = (
                "📸 Теперь отправьте фотографии для коллажа (4 штуки)\n"
                "Первое фото как основное фото (фото ЖК)\n"
                "2-3-4 Это фото внутри квартиры\n\n"
                f"{count}/4"
            )

            # Кнопки: до 3/4 только "Отмена", на 4/4 добавить "Готово"
            if count < 4:
                progress_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data=f"collage_cancel_{crm_id}")]
                ])
            else:
                progress_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("❌ Отмена", callback_data=f"collage_cancel_{crm_id}")],
                    [InlineKeyboardButton("✅ Готово", callback_data=f"collage_finish_{crm_id}")]
                ])

            try:
                if cp and cp.get('message_id') and cp.get('chat_id'):
                    await context.bot.edit_message_text(
                        chat_id=cp['chat_id'],
                        message_id=cp['message_id'],
                        text=progress_text,
                        reply_markup=progress_keyboard
                    )
                else:
                    # Если по какой-то причине нет прогресса, ответим текстом
                    await update.message.reply_text(progress_text, reply_markup=progress_keyboard)
            except Exception as e:
                logger.warning(f"Не удалось обновить прогресс по коллажу: {e}")
                await update.message.reply_text(progress_text, reply_markup=progress_keyboard)

        except Exception as e:
            logger.error(f"Ошибка сохранения фотографии: {e}")
            await update.message.reply_text("❌ Ошибка при сохранении фотографии")
            # Очищаем временные файлы при ошибке
            await cleanup_collage_files(context, user_id)


async def db_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику базы данных"""
    try:
        db_manager = await get_db_manager()
        stats = await db_manager.get_cache_stats()
        
        message = f"📊 Статистика базы данных:\n\n"
        message += f"📁 Всего записей: {stats['total_records']}\n"
        message += f"📅 Последнее обновление: {stats['last_updated']}\n"
        message += f"💾 Размер БД: {stats['db_size']}\n"
        
        await update.message.reply_text(message)
    except Exception as e:
        logger.error(f"Ошибка получения статистики БД: {e}")
        await update.message.reply_text("❌ Ошибка получения статистики базы данных")


async def update_contract_status(update: Update, context: ContextTypes.DEFAULT_TYPE, crm_id: str):
    """Обновление статуса контракта"""
    try:
        query = update.callback_query
        db_manager = await get_db_manager()
        
        agent_name = context.user_data.get('agent_name')
        if not agent_name:
            await query.edit_message_text("❌ Ошибка: агент не найден в сессии")
            return
        
        contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
        if not contract:
            await query.edit_message_text("❌ Контракт не найден")
            return

        current_status = contract.get('status', 'Размещено')
        # Тоггл теперь: Размещено <-> Реализовано
        new_status = 'Реализовано' if current_status == 'Размещено' else 'Размещено'
        
        await db_manager.update_contract(crm_id, {'status': new_status})
        
        await query.edit_message_text(f"✅ Статус контракта {crm_id} изменен на: {new_status}")
        
    except Exception as e:
        logger.error(f"Ошибка обновления статуса: {e}")
        await update.callback_query.edit_message_text("❌ Ошибка обновления статуса")


"""
Удалена заглушка update_contract_field.
"""


async def manual_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная синхронизация данных из Google Sheets БЕЗ обновления категорий (только для @rbdakee)"""
    try:
        from sheets_sync import get_sync_manager
        
        # Проверяем, что команду вызвал авторизованный пользователь @rbdakee
        authorized_user_id = 893220231  # User ID для @rbdakee
        
        if update.effective_user.id != authorized_user_id:
            await update.message.reply_text("❌ У вас нет прав для выполнения полной синхронизации")
            logger.warning(f"Пользователь {update.effective_user.username} (ID: {update.effective_user.id}) попытался выполнить полную синхронизацию")
            return
        
        await update.message.reply_text("🔄 Начинаю полную синхронизацию (без обновления категорий)...")
        
        sync_manager = await get_sync_manager()
        # Синхронизация БЕЗ обновления категорий
        sync_stats = await sync_manager.sync_from_sheets(update_categories=False)
        # После импорта из Sheets(1) сразу выгружаем в Sheets(2)
        to_sheets_stats = await sync_manager.sync_to_sheets()
        
        message = f"✅ Полная синхронизация завершена!\n\n"
        message += f"📥 Sheets(1) → DB:\n"
        message += f"• Создано: {sync_stats.get('created', 0)}\n"
        message += f"• Обновлено: {sync_stats.get('updated', 0)}\n"
        message += f"• Ошибок: {sync_stats.get('errors', 0)}\n\n"
        message += f"📤 DB → Sheets(2):\n"
        message += f"• Выгружено строк: {to_sheets_stats.get('updated', 0)}\n"
        message += f"• Ошибок: {to_sheets_stats.get('errors', 0)}\n"
        
        await update.message.reply_text(message)
        logger.info(f"Полная синхронизация (без категорий) выполнена пользователем {update.effective_user.username} (ID: {update.effective_user.id})")
    except Exception as e:
        logger.error(f"Ошибка ручной синхронизации: {e}")
        await update.message.reply_text(f"❌ Ошибка синхронизации: {str(e)}")


async def manual_sync_with_cats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная синхронизация данных из Google Sheets С обновлением категорий (только для @rbdakee)"""
    try:
        from sheets_sync import get_sync_manager
        
        # Проверяем, что команду вызвал авторизованный пользователь @rbdakee
        authorized_user_id = 893220231  # User ID для @rbdakee
        
        if update.effective_user.id != authorized_user_id:
            await update.message.reply_text("❌ У вас нет прав для выполнения полной синхронизации")
            logger.warning(f"Пользователь {update.effective_user.username} (ID: {update.effective_user.id}) попытался выполнить полную синхронизацию с категориями")
            return
        
        await update.message.reply_text("🔄 Начинаю полную синхронизацию с обновлением категорий...")
        
        sync_manager = await get_sync_manager()
        # Синхронизация С обновлением категорий
        sync_stats = await sync_manager.sync_from_sheets(update_categories=True)
        # После импорта из Sheets(1) сразу выгружаем в Sheets(2)
        to_sheets_stats = await sync_manager.sync_to_sheets()
        
        message = f"✅ Полная синхронизация с категориями завершена!\n\n"
        message += f"📥 Sheets(1) → DB:\n"
        message += f"• Создано: {sync_stats.get('created', 0)}\n"
        message += f"• Обновлено: {sync_stats.get('updated', 0)}\n"
        message += f"• Ошибок: {sync_stats.get('errors', 0)}\n\n"
        message += f"📤 DB → Sheets(2):\n"
        message += f"• Выгружено строк: {to_sheets_stats.get('updated', 0)}\n"
        message += f"• Ошибок: {to_sheets_stats.get('errors', 0)}\n"
        
        await update.message.reply_text(message)
        logger.info(f"Полная синхронизация (с категориями) выполнена пользователем {update.effective_user.username} (ID: {update.effective_user.id})")
    except Exception as e:
        logger.error(f"Ошибка ручной синхронизации с категориями: {e}")
        await update.message.reply_text(f"❌ Ошибка синхронизации: {str(e)}")


async def show_collage_data_with_edit_buttons(query, collage_input: CollageInput, crm_id: str):
    """Показывает данные коллажа с кнопками для редактирования"""
    
    # Формируем сообщение с данными
    message = f"✅ Данные для коллажа:\n\n"
    message += f"🏢 ЖК: {collage_input.complex_name}\n"
    message += f"📍 Адрес: {collage_input.address}\n"
    message += f"📐 Площадь: {collage_input.area_sqm} м²\n"
    message += f"🏠 Комнат: {collage_input.rooms}\n"
    message += f"🏗️ Этаж: {collage_input.floor}\n"
    message += f"💰 Цена: {collage_input.price}\n"
    message += f"🏗️ Класс жилья: {collage_input.housing_class}\n"
    message += f"👤 РОП: {collage_input.rop}\n"
    message += f"👤 МОП: {collage_input.mop or 'Не указан'}\n\n"
    
    # Достоинства
    if collage_input.benefits:
        message += f"📋 Достоинства ({len(collage_input.benefits)} шт.):\n"
        for i, benefit in enumerate(collage_input.benefits, 1):
            message += f"   {i}. {benefit}\n"
        message += "\n"
    
    # Создаем кнопки для редактирования
    keyboard = [
        [
            InlineKeyboardButton("🏢 ЖК", callback_data=f"edit_collage_complex_{crm_id}"),
            InlineKeyboardButton("📍 Адрес", callback_data=f"edit_collage_address_{crm_id}")
        ],
        [
            InlineKeyboardButton("📐 Площадь", callback_data=f"edit_collage_area_{crm_id}"),
            InlineKeyboardButton("🏠 Комнаты", callback_data=f"edit_collage_rooms_{crm_id}")
        ],
        [
            InlineKeyboardButton("🏗️ Этаж", callback_data=f"edit_collage_floor_{crm_id}"),
            InlineKeyboardButton("🏗️ Класс", callback_data=f"edit_collage_class_{crm_id}")
            
        ],
        [
            InlineKeyboardButton("👤 МОП", callback_data=f"edit_collage_mop_{crm_id}"),
            InlineKeyboardButton("👤 РОП", callback_data=f"edit_collage_rop_{crm_id}")
        ],
        [
            
            InlineKeyboardButton("💰 Цена", callback_data=f"edit_collage_price_{crm_id}"),
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
    
    # Проверяем, является ли query callback_query или message
    if hasattr(query, 'edit_message_text'):
        # Это callback_query, пытаемся отредактировать
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception:
            # Если не удается отредактировать (например, сообщение с фотографией), отправляем новое
            await query.message.reply_text(message, reply_markup=reply_markup)
    else:
        # Это обычное сообщение, отправляем новое
        await query.reply_text(message, reply_markup=reply_markup)


async def handle_collage_field_edit(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, state: str):
    """Обработка редактирования полей коллажа"""
    user_id = update.effective_user.id
    
    if text.lower() == 'отмена':
        user_states[user_id] = 'authenticated'
        await update.message.reply_text("❌ Редактирование отменено")
        # Очищаем временные файлы при отмене
        await cleanup_collage_files(context, user_id)
        return
    
    # Извлекаем информацию из состояния
    parts = state.split('_')
    field = parts[2]
    crm_id = parts[3]
    
    # Получаем объект коллажа
    collage_input = user_collage_inputs.get(user_id)
    if not collage_input:
        await update.message.reply_text("❌ Данные коллажа не найдены. Начните заново.")
        user_states[user_id] = 'authenticated'
        # Очищаем временные файлы
        await cleanup_collage_files(context, user_id)
        return
    
    # Обновляем поле
    try:
        if field == 'complex':
            collage_input.complex_name = text
        elif field == 'address':
            collage_input.address = text
        elif field == 'area':
            collage_input.area_sqm = text
        elif field == 'rooms':
            collage_input.rooms = text
        elif field == 'floor':
            collage_input.floor = text
        elif field == 'price':
            collage_input.price = text
        elif field == 'class':
            collage_input.housing_class = text
        elif field == 'rop':
            collage_input.rop = text
        elif field == 'mop':
            collage_input.mop = text
        elif field == 'benefits':
            # Разбиваем по строкам и очищаем
            benefits = [line.strip() for line in text.split('\n') if line.strip()]
            collage_input.benefits = benefits
        
        # Сохраняем обновленный объект
        user_collage_inputs[user_id] = collage_input
        
        # Показываем обновленные данные
        await show_collage_data_with_edit_buttons(update.message, collage_input, crm_id)
        
    except Exception as e:
        logger.error(f"Ошибка редактирования поля коллажа: {e}")
        await update.message.reply_text("❌ Ошибка при редактировании поля")
        # Очищаем временные файлы при ошибке
        await cleanup_collage_files(context, user_id)


"""
Удален устаревший текстовый обработчик загрузки фотографий для коллажа.
"""


async def handle_price_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, state: str):
    """Обработка ввода новой цены"""
    user_id = update.effective_user.id
    
    # Извлекаем CRM ID из состояния
    crm_id = state.replace('waiting_price_', '')
    
    try:
        # Очищаем цену от пробелов и форматируем
        price_clean = text.replace(' ', '').replace(',', '').replace('.', '')
        
        # Проверяем, что это число
        if not price_clean.isdigit():
            await update.message.reply_text(
                "❌ Неверный формат цены. Введите только цифры.\n\n"
                "Пример: 25000000 или 25 000 000"
            )
            return
        
        # Обновляем цену в базе данных
        db_manager = await get_db_manager()
        success = await db_manager.update_contract(crm_id, {'price_update': text})
        
        if success:
            await update.message.reply_text(f"✅ Цена для контракта {crm_id} обновлена: {text}")
            
            # Возвращаемся к деталям контракта
            agent_name = context.user_data.get('agent_name')
            if agent_name:
                contract = await db_manager.search_contract_by_crm_id(crm_id, agent_name)
                if contract:
                    await show_contract_detail_by_contract(update, context, contract)
                else:
                    await update.message.reply_text("❌ Контракт не найден")
            else:
                await update.message.reply_text("❌ Ошибка: агент не найден в сессии")
        else:
            await update.message.reply_text("❌ Ошибка при обновлении цены")
            
    except Exception as e:
        logger.error(f"Ошибка обновления цены: {e}")
        await update.message.reply_text("❌ Ошибка при обновлении цены")


def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("logout", logout))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))


async def automate_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Автоматически пересчитывает и обновляет категории для всех объектов.
    Доступно только пользователю с authorized_user_id = 893220231 и по команде /automate_categories.
    Берёт значения roof (B), score (C), window (D) из третьего листа ("Лист8"),
    площадь (area) из API, contract_price из SQL, затем рассчитывает category и обновляет SQL.
    """
    try:
        authorized_user_id = 893220231
        if update.effective_user.id != authorized_user_id:
            await update.message.reply_text("❌ У вас нет прав для автоматического обновления категорий")
            logger.warning(f"Пользователь {update.effective_user.username} (ID: {update.effective_user.id}) попытался выполнить automate_categories")
            return

        await update.message.reply_text("🔄 Запускаю автоматическое обновление категорий... Это может занять время")

        await update.message.reply_text("Команда отключена: расчёт категорий выполняется в full sync")

        # Автокатегоризация перенесена в full sync
    except Exception as e:
        logger.error(f"Ошибка automate_categories: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def automate_categories_2(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пересчитывает категории только для объектов с пустой category.
    Доступно только пользователю с authorized_user_id = 893220231 и по команде /automate_categories_2.
    """
    try:
        authorized_user_id = 893220231
        if update.effective_user.id != authorized_user_id:
            await update.message.reply_text("❌ У вас нет прав для автоматического обновления категорий")
            logger.warning(f"Пользователь {update.effective_user.username} (ID: {update.effective_user.id}) попытался выполнить automate_categories_2")
            return

        await update.message.reply_text("🔄 Запускаю обновление категорий только для пустых значений... Это может занять время")

        await update.message.reply_text("Команда отключена: расчёт категорий выполняется в full sync")

        # Автокатегоризация перенесена в full sync
    except Exception as e:
        logger.error(f"Ошибка automate_categories_2: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def automate_categories_c(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заполняет категорией 'С' все пустые значения category.
    Доступно только пользователю с authorized_user_id = 893220231 и по команде /automate_categories_c.
    """
    try:
        authorized_user_id = 893220231
        if update.effective_user.id != authorized_user_id:
            await update.message.reply_text("❌ У вас нет прав для массового заполнения категорий")
            logger.warning(f"Пользователь {update.effective_user.username} (ID: {update.effective_user.id}) попытался выполнить automate_categories_c")
            return

        await update.message.reply_text("🔄 Запускаю заполнение категории 'С' для пустых значений...")
        await update.message.reply_text("Команда отключена: расчёт категорий выполняется в full sync")
    except Exception as e:
        logger.error(f"Ошибка automate_categories_c: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


# Тестовый хендлер удалён по требованию