"""
Сервис для работы с историей изменения цен ЖК из Google Sheets
"""

import logging
import os
import asyncio
import time
from typing import Dict, Optional, Any
from io import BytesIO
import gspread
from gspread.exceptions import APIError
from google.oauth2.service_account import Credentials
import matplotlib
matplotlib.use('Agg')  # Используем backend без GUI
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from config import PRICE_HISTORY_SHEET_ID, PRICE_HISTORY_SHEET_GID
from sheets_sync import SheetsSyncManager

logger = logging.getLogger(__name__)

# Глобальный экземпляр клиента для работы с таблицей истории цен
_price_history_gc: Optional[gspread.Client] = None
_price_history_sheet: Optional[gspread.Worksheet] = None

# Кэш данных таблицы
_cached_values: Optional[list] = None
_cache_timestamp: float = 0
_cache_ttl: float = 300  # Кэш на 5 минут


def _init_price_history_client():
    """Инициализация подключения к Google Sheets для истории цен"""
    global _price_history_gc, _price_history_sheet
    
    if _price_history_gc is not None:
        return
    
    try:
        credentials_file = 'credentials.json'
        if not os.path.exists(credentials_file):
            raise ValueError(f"Файл {credentials_file} не найден")
        
        credentials = Credentials.from_service_account_file(
            credentials_file,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        
        _price_history_gc = gspread.authorize(credentials)
        
        if not PRICE_HISTORY_SHEET_ID:
            logger.warning("PRICE_HISTORY_SHEET_ID не установлен, история цен недоступна")
            return
        
        spreadsheet = _price_history_gc.open_by_key(PRICE_HISTORY_SHEET_ID)
        
        if PRICE_HISTORY_SHEET_GID:
            _price_history_sheet = spreadsheet.get_worksheet_by_id(int(PRICE_HISTORY_SHEET_GID))
        else:
            # Если GID не указан, берем первый лист
            _price_history_sheet = spreadsheet.sheet1
        
        logger.info(f"Подключение к таблице истории цен установлено")
        
    except Exception as e:
        logger.error(f"Ошибка инициализации подключения к таблице истории цен: {e}", exc_info=True)
        raise


async def _get_all_values_with_retry(max_attempts: int = 3, base_delay: float = 2.0) -> list:
    """
    Читает все данные из листа с повторными попытками при ошибках API
    
    Args:
        max_attempts: Максимальное количество попыток
        base_delay: Базовая задержка между попытками (секунды)
    
    Returns:
        list: Все значения из листа
    
    Raises:
        APIError: Если все попытки исчерпаны
    """
    global _price_history_sheet
    
    if _price_history_sheet is None:
        raise ValueError("Лист истории цен не инициализирован")
    
    last_error = None
    
    for attempt in range(1, max_attempts + 1):
        try:
            values = await asyncio.to_thread(_price_history_sheet.get_all_values)
            return values
            
        except APIError as e:
            last_error = e
            error_code = getattr(e, 'response', {}).get('status', 0) if hasattr(e, 'response') else 0
            
            # Обрабатываем только временные ошибки (503, 500, 429)
            if error_code in [503, 500, 429]:
                if attempt < max_attempts:
                    # Экспоненциальная задержка: 2s, 4s, 8s
                    delay = base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        f"Ошибка API {error_code} при чтении таблицы (попытка {attempt}/{max_attempts}). "
                        f"Повтор через {delay:.1f} сек..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Все {max_attempts} попыток исчерпаны. Последняя ошибка: {e}")
                    raise
            else:
                # Для других ошибок не делаем retry
                logger.error(f"Критическая ошибка API {error_code}: {e}")
                raise
                
        except Exception as e:
            # Для других исключений не делаем retry
            logger.error(f"Неожиданная ошибка при чтении таблицы: {e}", exc_info=True)
            raise
    
    # Если дошли сюда, значит все попытки исчерпаны
    if last_error:
        raise last_error
    raise RuntimeError("Не удалось прочитать данные из таблицы")


def _norm_complex(x: str) -> str:
    """Нормализует название ЖК (копия из sheets_sync.py)"""
    import re
    s = (x or '').lower()
    for token in ['жк', 'жилой комплекс', 'residence', 'residential', 'complex']:
        s = s.replace(token, ' ')
    for ch in ['"', '\'', '«', '»', '.', ',', ';', ':', '(', ')', '[', ']', '{', '}', '/', '\\', '-', '–', '_']:
        s = s.replace(ch, ' ')
    s = re.sub(r"\bблок\s+[a-zа-я0-9]+\b", " ", s)
    s = re.sub(r"\bочередь\b", " ", s)
    s = re.sub(r"\b(\d+)\s*\-\s*\d+\b", r"\1", s)
    s = ' '.join(s.split())
    synonyms = {
        'buqar': 'бухар', 'bukhar': 'бухар', 'buqarjyrau': 'бухаржырау', 'jyrau': 'жырау',
        'qalashyq': 'калашык', 'qalashy': 'калашык', 'exclusive': 'эксклюзив',
        'dauletti': 'даулетти', 'qalashyk': 'калашык'
    }
    tokens = s.split()
    norm_tokens = []
    for t in tokens:
        norm_tokens.append(synonyms.get(t, t))
    return ' '.join(norm_tokens)


async def get_price_history_for_complex(complex_name: str) -> Dict[str, Any]:
    """
    Получает историю цен для ЖК из Google Sheets
    
    Args:
        complex_name: Название ЖК из контракта
        
    Returns:
        Dict с ключами:
            - found: bool - найдена ли запись
            - complex_name: str - оригинальное название из таблицы
            - prices: Dict[str, Optional[float]] - цены по годам (2020-2025)
    """
    global _cached_values, _cache_timestamp
    
    if not PRICE_HISTORY_SHEET_ID or not PRICE_HISTORY_SHEET_GID:
        logger.warning("PRICE_HISTORY_SHEET_ID или PRICE_HISTORY_SHEET_GID не установлены")
        return {'found': False, 'complex_name': '', 'prices': {}}
    
    try:
        # Инициализируем клиент, если еще не инициализирован
        if _price_history_gc is None:
            _init_price_history_client()
        
        if _price_history_sheet is None:
            logger.error("Не удалось инициализировать лист истории цен")
            return {'found': False, 'complex_name': '', 'prices': {}}
        
        # Приводим к нижнему регистру для поиска
        search_lower = complex_name.lower().strip()
        
        # Проверяем кэш
        current_time = time.time()
        if _cached_values is None or (current_time - _cache_timestamp) > _cache_ttl:
            # Читаем все данные из листа с retry
            values = await _get_all_values_with_retry(max_attempts=3, base_delay=2.0)
            _cached_values = values
            _cache_timestamp = current_time
            logger.info(f"Данные истории цен загружены из таблицы ({len(values)} строк)")
        else:
            values = _cached_values
        
        if not values:
            logger.warning("Таблица истории цен пуста")
            return {'found': False, 'complex_name': '', 'prices': {}}
        
        # Структура таблицы:
        # Столбец B (индекс 1) - ЖК
        # Столбцы H-M (индексы 7-12) - 2020, 2021, 2022, 2023, 2024, 2025
        
        years = ['2020', '2021', '2022', '2023', '2024', '2025']
        year_columns = [7, 8, 9, 10, 11, 12]  # H, I, J, K, L, M
        
        # Ищем строку с нужным ЖК
        found_row = None
        found_complex_name = None
        
        # Сначала пытаемся найти точное совпадение
        for row_idx, row in enumerate(values):
            if len(row) <= 1:
                continue
            
            # Берем название ЖК из столбца B (индекс 1)
            row_complex = (row[1] if len(row) > 1 else '').strip()
            if not row_complex:
                continue
            
            # Приводим к нижнему регистру для сравнения
            row_lower = row_complex.lower().strip()
            
            # Точное совпадение
            if row_lower == search_lower:
                found_row = row
                found_complex_name = row_complex
                break
        
        # Если точное совпадение не найдено, ищем по подстроке
        if found_row is None:
            import re
            
            # Разбиваем поисковую строку на слова
            search_words = set(re.findall(r'\b\w+\b', search_lower))
            
            best_match = None
            best_score = 0
            best_row_idx = None
            best_original_name = None
            
            for row_idx, row in enumerate(values):
                if len(row) <= 1:
                    continue
                
                row_complex = (row[1] if len(row) > 1 else '').strip()
                if not row_complex:
                    continue
                
                row_lower = row_complex.lower().strip()
                
                # Проверяем, содержит ли строка из таблицы искомую строку или наоборот
                if search_lower in row_lower or row_lower in search_lower:
                    # Вычисляем "качество" совпадения
                    row_words = set(re.findall(r'\b\w+\b', row_lower))
                    
                    # Количество совпадающих слов
                    matching_words = len(search_words & row_words)
                    total_search_words = len(search_words)
                    
                    # Процент совпадения слов
                    word_match_ratio = matching_words / total_search_words if total_search_words > 0 else 0
                    
                    # Бонус за близость длины (чем ближе длина, тем лучше)
                    length_diff = abs(len(row_lower) - len(search_lower))
                    length_bonus = 1.0 / (1.0 + length_diff / 10.0)  # Нормализуем разницу длины
                    
                    # Итоговый score: приоритет совпадению слов, затем близости длины
                    score = word_match_ratio * 0.7 + length_bonus * 0.3
                    
                    if score > best_score:
                        best_score = score
                        best_match = row_lower
                        best_row_idx = row_idx
                        best_original_name = row_complex
            
            if best_match:
                found_row = values[best_row_idx]
                found_complex_name = best_original_name
        
        if found_row is None:
            logger.warning(f"ЖК '{complex_name}' не найден в таблице истории цен")
            return {'found': False, 'complex_name': '', 'prices': {}}
        
        # Извлекаем цены из столбцов H-M (2020-2025)
        # Столбец H (индекс 7) = 2020, I (8) = 2021, J (9) = 2022, K (10) = 2023, L (11) = 2024, M (12) = 2025
        prices = {}
        for year, col_idx in zip(years, year_columns):
            if len(found_row) > col_idx:
                price_str = found_row[col_idx].strip() if col_idx < len(found_row) else ''
                if price_str:
                    try:
                        # Убираем пробелы и заменяем запятую на точку
                        price_str = price_str.replace(' ', '').replace(',', '.').replace('\u00A0', '')  # Убираем неразрывные пробелы
                        if price_str:  # Проверяем, что после очистки осталось что-то
                            prices[year] = float(price_str)
                        else:
                            prices[year] = None
                    except (ValueError, TypeError):
                        prices[year] = None
                else:
                    prices[year] = None
            else:
                prices[year] = None
        
        logger.info(f"Найдена история цен для ЖК '{found_complex_name}'")
        
        return {
            'found': True,
            'complex_name': found_complex_name,
            'prices': prices
        }
        
    except APIError as e:
        error_code = getattr(e, 'response', {}).get('status', 0) if hasattr(e, 'response') else 0
        logger.error(
            f"Ошибка API {error_code} при получении истории цен для ЖК '{complex_name}': {e}",
            exc_info=True
        )
        return {'found': False, 'complex_name': '', 'prices': {}}
    except Exception as e:
        logger.error(f"Ошибка получения истории цен для ЖК '{complex_name}': {e}", exc_info=True)
        return {'found': False, 'complex_name': '', 'prices': {}}


def generate_price_chart(price_data: Dict[str, Any]) -> bytes:
    """
    Генерирует график изменения цены
    
    Args:
        price_data: Результат get_price_history_for_complex
        
    Returns:
        bytes: PNG изображение графика
    """
    if not price_data.get('found'):
        raise ValueError("Данные не найдены")
    
    prices = price_data.get('prices', {})
    complex_name = price_data.get('complex_name', 'Неизвестный ЖК')
    
    # Фильтруем пустые значения и сортируем по годам
    years_str = []  # Строковые представления годов для подписей
    years_int = []  # Числовые значения годов для построения графика
    values = []
    
    for year in ['2020', '2021', '2022', '2023', '2024', '2025']:
        price = prices.get(year)
        if price is not None:
            years_str.append(year)
            years_int.append(int(year))  # Преобразуем в число для построения графика
            values.append(price)
    
    if len(years_str) < 2:
        raise ValueError("Недостаточно данных для построения графика (нужно минимум 2 точки)")
    
    # Подавляем предупреждения matplotlib о категориальных единицах
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')
        
        # Создаем график
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Определяем цвета для сегментов (зеленый для роста, красный для спада)
        colors = []
        for i in range(len(values)):
            if i == 0:
                colors.append('#3498db')  # Синий для первой точки
            elif values[i] > values[i-1]:
                colors.append('#2ecc71')  # Зеленый для роста
            elif values[i] < values[i-1]:
                colors.append('#e74c3c')  # Красный для спада
            else:
                colors.append('#95a5a6')  # Серый для без изменений
        
        # Строим график с цветными сегментами (используем числовые значения годов)
        for i in range(len(years_int) - 1):
            ax.plot(
                [years_int[i], years_int[i+1]],
                [values[i], values[i+1]],
                color=colors[i+1],
                linewidth=2.5,
                marker='o',
                markersize=8
            )
        
        # Добавляем точки на график (используем числовые значения годов)
        for i, (year_int, year_str, value) in enumerate(zip(years_int, years_str, values)):
            ax.plot(year_int, value, 'o', color=colors[i], markersize=10, zorder=5)
            # Добавляем подписи значений
            ax.annotate(
                f'{int(value):,}'.replace(',', ' '),
                (year_int, value),
                textcoords="offset points",
                xytext=(0, 15),
                ha='center',
                fontsize=9,
                fontweight='bold'
            )
        
        # Настройка графика
        ax.set_xlabel('Год', fontsize=12, fontweight='bold')
        ax.set_ylabel('Цена (тенге)', fontsize=12, fontweight='bold')
        ax.set_title(f'График изменения цены: {complex_name}', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--')
        # Используем числовые значения для позиций и строковые для подписей
        ax.set_xticks(years_int)
        ax.set_xticklabels(years_str, rotation=0)
    
    # Устанавливаем отступы по оси Y, чтобы точки не выходили за границы
    y_min, y_max = min(values), max(values)
    y_range = y_max - y_min
    if y_range > 0:
        # Добавляем 10% отступа сверху и снизу
        ax.set_ylim(y_min - y_range * 0.1, y_max + y_range * 0.1)
    else:
        # Если все значения одинаковые, добавляем небольшой отступ
        ax.set_ylim(y_min - y_min * 0.05, y_max + y_max * 0.05)
    
    # Форматируем ось Y с разделителями тысяч
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'.replace(',', ' ')))
    
    # Добавляем легенду
    legend_elements = [
        mpatches.Patch(color='#2ecc71', label='Рост цены'),
        mpatches.Patch(color='#e74c3c', label='Спад цены'),
        mpatches.Patch(color='#3498db', label='Начальная точка')
    ]
    ax.legend(handles=legend_elements, loc='best', fontsize=10)
    
    # Улучшаем внешний вид
    plt.tight_layout()
    
    # Сохраняем в BytesIO
    buf = BytesIO()
    plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)
    
    return buf.getvalue()

