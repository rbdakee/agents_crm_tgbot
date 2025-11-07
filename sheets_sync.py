"""
Модуль синхронизации с Google Sheets для CRM агентов по недвижимости
Объединяет данные из двух таблиц: SHEET_DEALS и SHEET_PROGRESS
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, date, time as dtime
from typing import Dict, List, Optional, Any
import gspread
from google.oauth2.service_account import Credentials
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import json
from api_client import APIClient
from config import CRM_API_ENRICHMENT

logger = logging.getLogger(__name__)

# Константы синхронизации
EXPECTED_DEALS_HEADERS: List[str] = [
    'CRM ID',
    'Дата подписания',
    'Номер договора',
    'МОП',
    'РОП',
    'ДД',
    'Имя клиента и номер',
]

# Размер батча для коммитов при полной синхронизации
BATCH_SIZE: int = 100

class SheetsSyncManager:
    """Менеджер синхронизации с Google Sheets и PostgreSQL"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Настройки Google Sheets
        self.sheet_id = config['SHEET_ID']
        self.deals_sheet_gid = config['FIRST_SHEET_GID']  # SHEET_DEALS
        self.progress_sheet_gid = config['SECOND_SHEET_GID']  # SHEET_PROGRESS
        self.third_sheet_gid = config.get('THIRD_SHEET_GID')  # SHEET_THIRD (Лист8)
        self.sync_interval_minutes = int(config.get('SYNC_INTERVAL_MINUTES', 30))
        
        # Настройки PostgreSQL
        self.db_url = config['DATABASE_URL']
        
        # Инициализация подключений
        self._init_google_sheets()
        self._init_database()
        
        # Состояние синхронизации
        self.last_sync_time = None
        self.sync_in_progress = False

    async def _to_thread(self, func, *args, **kwargs):
        import asyncio
        return await asyncio.to_thread(func, *args, **kwargs)
        
    def _init_google_sheets(self):
        """Инициализация подключения к Google Sheets"""
        try:
            # Загружаем учетные данные из файла credentials.json
            credentials_file = 'credentials.json'
            if not os.path.exists(credentials_file):
                raise ValueError(f"Файл {credentials_file} не найден")
            
            credentials = Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            
            self.gc = gspread.authorize(credentials)
            self.spreadsheet = self.gc.open_by_key(self.sheet_id)
            
            # Получаем листы
            self.deals_sheet = self.spreadsheet.get_worksheet_by_id(int(self.deals_sheet_gid))
            self.progress_sheet = self.spreadsheet.get_worksheet_by_id(int(self.progress_sheet_gid))
            self.third_sheet = None
            try:
                if self.third_sheet_gid:
                    self.third_sheet = self.spreadsheet.get_worksheet_by_id(int(self.third_sheet_gid))
            except Exception:
                self.third_sheet = None
            
            logger.info("Подключение к Google Sheets установлено")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации Google Sheets: {e}")
            raise
    
    def _init_database(self):
        """Инициализация подключения к PostgreSQL"""
        try:
            # Создаем асинхронный движок
            self.engine = create_async_engine(
                self.db_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Создаем фабрику сессий
            self.async_session = sessionmaker(
                self.engine, 
                class_=AsyncSession, 
                expire_on_commit=False
            )
            
            logger.info("Подключение к PostgreSQL установлено")
            
        except Exception as e:
            logger.error(f"Ошибка инициализации PostgreSQL: {e}")
            raise
    
    async def init_db(self):
        """Создание таблицы properties в PostgreSQL из database_schema.sql (idempotent).

        ВАЖНО: Не трогаем существующие данные. Если таблица уже есть — выходим.
        """
        try:
            async with self.engine.begin() as conn:
                # Проверяем, существует ли таблица
                exists_result = await conn.execute(text(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = 'properties'
                    """
                ))
                exists = exists_result.first() is not None
                if exists:
                    logger.info("init_db: таблица properties уже существует — пропускаем применение схемы")
                    return

                # Читаем актуальную схему из единого файла
                schema_path = 'database_schema.sql'
                with open(schema_path, 'r', encoding='utf-8') as f:
                    schema_sql = f.read()

                # Выполним пооператорно
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                for stmt in statements:
                    await conn.execute(text(stmt))

            logger.info("Таблица properties создана по database_schema.sql")
            
        except Exception as e:
            logger.error(f"Ошибка создания таблицы: {e}")
            raise
    
    async def sync_from_sheets(self, update_categories: bool = False) -> Dict[str, int]:
        """Полная синхронизация из Google Sheets (1) в PostgreSQL.
        Источник истины — первая таблица (SHEET_DEALS). Удаляет записи из БД,
        если их CRM ID отсутствует в SHEET_DEALS.
        
        Args:
            update_categories: Если True, то категории будут пересчитываться и обновляться.
                              Если False, категории не трогаются (только для новых записей устанавливается 'С')."""
        if self.sync_in_progress:
            logger.warning("Синхронизация уже выполняется, пропускаем")
            return {"skipped": 1}
        
        self.sync_in_progress = True
        stats = {"created": 0, "updated": 0, "errors": 0}
        
        try:
            logger.info("Начинаем синхронизацию из Google Sheets (SHEET_DEALS -> DB)")
            
            # Загружаем данные только из SHEET_DEALS (источник истины)
            deals_data = await self._load_deals_sheet()
            deals_crm_ids = {row['crm_id'] for row in deals_data}
            
            
            # Обогащаем данные из CRM API (если включено)
            if CRM_API_ENRICHMENT:
                await self._enrich_with_crm_data(deals_data)
            else:
                logger.info("Обогащение данными из CRM API отключено")

            # Грузим карту третьего листа для расчёта категории
            third_map = {}
            if self.third_sheet is not None:
                try:
                    third_map = await self._build_third_sheet_map()
                except Exception as e:
                    logger.warning(f"Не удалось загрузить Лист8: {e}")
            
            # Синхронизируем с базой данных: upsert сделок и удаление отсутствующих
            logger.info(f"Начинаем запись {len(deals_data)} записей в базу данных...")
            async with self.async_session() as session:
                # Upsert для всех записей из SHEET_DEALS
                batch_size = BATCH_SIZE
                for i, deal in enumerate(deals_data):
                    crm_id = deal['crm_id']
                    # заполняем техн. поля
                    property_data = deal.copy()
                    # Запись score и производных цен восстановлена (если area уже есть)
                    try:
                        complex_name = property_data.get('complex') or ''
                        area_val = property_data.get('area')
                        area_f = float(area_val) if area_val is not None else None
                        if complex_name and third_map:
                            key = self._find_by_variants(complex_name, third_map)
                            if key:
                                params = third_map.get(key) or {}
                                roof = params.get('roof')
                                window = params.get('window')
                                property_data['score'] = params.get('score')
                                if area_f is not None:
                                    property_data['krisha_price'] = int(roof * area_f) if (roof is not None) else None
                                    property_data['vitrina_price'] = int(window * area_f) if (window is not None) else None
                                # Вычисляем категорию при необходимости
                                if update_categories:
                                    category_value = self._compute_category(property_data, third_map)
                                    property_data['category'] = category_value
                    except Exception:
                        # Если произошла ошибка при вычислении категории
                        if update_categories:
                            property_data['category'] = 'С'

                    # Гарантируем категорию 'С' при режиме обновления категорий,
                    # если ни одно из условий не сработало и категория так и не выставлена
                    if update_categories and not property_data.get('category'):
                        property_data['category'] = 'С'
                    # На первой синхронизации мы НЕ должны перетирать прогресс-поля.
                    # В _upsert_property ниже реализована логика "обновлять только изменившиеся поля",
                    # а здесь фиксируем источник изменений только для новых записей.
                    property_data['last_modified_by'] = 'SHEET'
                    try:
                        result = await self._upsert_property(session, crm_id, property_data)
                        if result == "created":
                            stats["created"] += 1
                        elif result == "updated":
                            stats["updated"] += 1
                    except Exception as e:
                        logger.error(f"Ошибка синхронизации записи {crm_id}: {e}")
                        stats["errors"] += 1
                    
                    # Периодические коммиты, чтобы не держать большую транзакцию
                    if (i + 1) % batch_size == 0:
                        try:
                            logger.info(f"Коммитим батч на {i + 1} записи...")
                            await session.commit()
                        except Exception as commit_err:
                            logger.error(f"Ошибка при коммите батча: {commit_err}")
                            stats["errors"] += 1
                            # Попробуем продолжить с новой транзакцией
                            await session.rollback()
                
                logger.info(f"Завершена обработка всех записей. Начинаем коммит транзакции...")
                # Удаляем записи, которых больше нет в SHEET_DEALS
                try:
                    await self._delete_missing_records(session, deals_crm_ids)
                except Exception as e:
                    logger.error(f"Ошибка удаления отсутствующих записей: {e}")
                    stats["errors"] += 1
                
                logger.info("Коммитим транзакцию...")
                await session.commit()
                logger.info("Транзакция успешно закоммичена")
            
            self.last_sync_time = datetime.now()
            logger.info(f"Синхронизация из SHEET_DEALS завершена: {stats}")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации из Google Sheets: {e}", exc_info=True)
            stats["errors"] += 1
        finally:
            self.sync_in_progress = False
        
        return stats

    async def sync_from_sheets_fast(self) -> Dict[str, int]:
        """Быстрая синхронизация: только создание новых и удаление отсутствующих записей.

        Важно: существующие записи НЕ обновляются вообще. Проверяем только наличие CRM ID.
        Для новых записей получаем данные из CRM API.
        """
        if self.sync_in_progress:
            logger.warning("Синхронизация уже выполняется, пропускаем")
            return {"skipped": 1}
        
        self.sync_in_progress = True
        stats = {"created": 0, "deleted": 0, "errors": 0}
        
        try:
            logger.info("Начинаем быструю синхронизацию из Google Sheets (только insert/delete)")
            deals_data = await self._load_deals_sheet()
            # Защита: если загрузка пустая или сломалась — не удаляем ничего
            if not deals_data:
                logger.warning("Быстрая синхронизация: данные из Sheets пустые/ошибка. Пропускаем цикл, без удалений.")
                return {"created": 0, "deleted": 0, "errors": 0, "skipped": 1}
            deals_by_crm: Dict[str, Dict] = {row['crm_id']: row for row in deals_data}
            deals_crm_ids = set(deals_by_crm.keys())

            # Подготовим карту из третьего листа для расчёта категории (если доступен)
            third_map = {}
            if self.third_sheet is not None:
                try:
                    third_map = await self._build_third_sheet_map()
                except Exception as e:
                    logger.warning(f"Не удалось загрузить Лист8 для категорий: {e}")

            async with self.async_session() as session:
                # Получаем текущие CRM ID из БД одним запросом
                result = await session.execute(text("SELECT crm_id FROM properties"))
                db_ids = {row.crm_id for row in result.fetchall()}

                # Новые к вставке
                new_ids = list(deals_crm_ids - db_ids)
                # Лишние к удалению
                to_delete = list(db_ids - deals_crm_ids)

                # Получаем данные из CRM API только для новых записей
                new_crm_data = {}
                if new_ids and CRM_API_ENRICHMENT:
                    new_crm_data = await self._enrich_new_records_with_crm_data(new_ids)

                # Вставляем новые записи батчево
                if new_ids:
                    logger.info(f"Новых записей для вставки: {len(new_ids)}")
                    now = datetime.now()
                    for crm_id in new_ids:
                        deal = deals_by_crm.get(crm_id)
                        if not deal:
                            continue
                        try:
                            property_data = deal.copy()
                            
                            # Обогащаем данными из CRM API если доступны
                            if crm_id in new_crm_data:
                                api_data = new_crm_data[crm_id]
                                if api_data.get('address'):
                                    property_data['address'] = api_data['address']
                                if api_data.get('complex'):
                                    property_data['complex'] = api_data['complex']
                                if api_data.get('price') is not None:
                                    property_data['contract_price'] = api_data['price']
                                if api_data.get('area') is not None:
                                    try:
                                        property_data['area'] = float(api_data['area'])
                                    except Exception:
                                        pass
                            
                            # Сохраняем score и производные цены для новых записей
                            try:
                                complex_name = property_data.get('complex') or ''
                                area_val = property_data.get('area')
                                area_f = float(area_val) if area_val is not None else None
                                if complex_name and third_map:
                                    key = self._find_by_variants(complex_name, third_map)
                                    if key:
                                        params = third_map.get(key) or {}
                                        roof = params.get('roof')
                                        window = params.get('window')
                                        property_data['score'] = params.get('score')
                                        if area_f is not None:
                                            property_data['krisha_price'] = int(roof * area_f) if (roof is not None) else None
                                            property_data['vitrina_price'] = int(window * area_f) if (window is not None) else None
                            except Exception:
                                pass

                            # Расчёт категории для новой записи (после установки всех необходимых данных)
                            category_value = 'С'
                            try:
                                computed_category = self._compute_category_for_insert(property_data, third_map, new_crm_data.get(crm_id, {}))
                                if computed_category and computed_category.strip():
                                    category_value = computed_category
                            except Exception:
                                category_value = 'С'
                            # Гарантируем, что категория всегда установлена
                            property_data['category'] = category_value if category_value and category_value.strip() else 'С'

                            property_data['created_at'] = now
                            property_data['last_modified_at'] = now
                            property_data['last_modified_by'] = 'SHEET'
                            columns = ", ".join(property_data.keys())
                            placeholders = ", ".join([f":{k}" for k in property_data.keys()])
                            query = f"INSERT INTO properties ({columns}) VALUES ({placeholders})"
                            await session.execute(text(query), property_data)
                            stats['created'] += 1
                        except Exception as e:
                            logger.error(f"Ошибка вставки новой записи {crm_id}: {e}")
                            stats['errors'] += 1

                # Удаляем отсутствующие записи (с предохранителем)
                if to_delete:
                    # Предохранитель: если доля удаляемых > 50%, не удаляем в fast-режиме
                    try:
                        total_db = len(db_ids)
                        if total_db > 0 and (len(to_delete) / total_db) > 0.5:
                            logger.warning(f"Быстрая синхронизация: попытка удалить {len(to_delete)} из {total_db} (>50%). Отменяем удаление в fast-режиме.")
                            to_delete = []
                    except Exception:
                        pass
                
                if to_delete:
                    try:
                        logger.info(f"Удаляем {len(to_delete)} отсутствующих записей")
                        await session.execute(
                            text("DELETE FROM properties WHERE crm_id = ANY(:ids)"),
                            {"ids": to_delete}
                        )
                        stats['deleted'] = len(to_delete)
                    except Exception as e:
                        logger.error(f"Ошибка удаления отсутствующих записей: {e}")
                        stats['errors'] += 1

                await session.commit()

            self.last_sync_time = datetime.now()
            logger.info(f"Быстрая синхронизация завершена: {stats}")
        except Exception as e:
            logger.error(f"Ошибка быстрой синхронизации: {e}", exc_info=True)
            stats['errors'] += 1
        finally:
            self.sync_in_progress = False

        return stats
    
    def _norm_complex(self, x: str) -> str:
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

    async def _build_third_sheet_map(self) -> Dict[str, Dict[str, float]]:
        values = await self._to_thread(self.third_sheet.get_all_values)
        if not values:
            return {}
        header_idx = 0
        for i, r in enumerate(values):
            line = ' '.join(r).lower()
            if ('жк' in line) or ('крыша' in line) or ('витрина' in line) or ('общий балл' in line):
                header_idx = i
                break
        header = values[header_idx] if values else []
        # A=ЖК, B=Крыша, C=Общий балл, D=Витрина
        def to_float_safe(v):
            try:
                s = str(v).replace(' ', '').replace('\u00A0', '').replace(',', '.')
                return float(s) if s.strip() else None
            except Exception:
                return None
        mp = {}
        import re
        for i, row in enumerate(values):
            if i <= header_idx:
                continue
            complex_name = (row[0] if len(row) > 0 else '').strip()
            if not complex_name:
                continue
            key = self._norm_complex(complex_name)
            roof = to_float_safe(row[1] if len(row) > 1 else '')
            score = to_float_safe(row[2] if len(row) > 2 else '')
            window = to_float_safe(row[3] if len(row) > 3 else '')
            mp[key] = {'roof': roof, 'score': score, 'window': window}
            key_variant = re.sub(r"\b(\d+)\s*\-\s*\d+\b", r"\1", key)
            if key_variant != key and key_variant not in mp:
                mp[key_variant] = mp[key]
        return mp

    def _find_by_variants(self, raw_name: str, mp: Dict[str, Dict]) -> Optional[str]:
        base = self._norm_complex(raw_name)
        parts = base.split()
        if not parts:
            return None
        def all_parts_in_key(parts_list: List[str], key: str) -> bool:
            for p in parts_list:
                if p and p not in key:
                    return False
            return True
        for cut in range(0, len(parts)):
            variant_parts = [p for p in parts[:len(parts)-cut] if not p.isdigit()]
            if not variant_parts:
                continue
            variant_str = ' '.join(variant_parts)
            if variant_str in mp:
                return variant_str
            for key in mp.keys():
                if all_parts_in_key(variant_parts, key):
                    return key
        return None

    def _assign_category(self, contract_price, window_price, roof_price, score) -> str:
        def is_num(x):
            return isinstance(x, (int, float)) and x is not None
        score_is_num = is_num(score)
        if score_is_num:
            if all(is_num(x) for x in [contract_price, window_price, roof_price]):
                if (window_price <= contract_price <= roof_price) and (score > 8):
                    return 'A'
                elif ((contract_price < window_price) or (contract_price > roof_price)) or (5 <= score <= 8):
                    return 'B'
                elif (contract_price > roof_price) and (score < 5):
                    return 'C'
        else:
            if all(is_num(x) for x in [contract_price, window_price, roof_price]):
                if (window_price <= contract_price <= roof_price):
                    return 'B'
            if (window_price is None) or (roof_price is None):
                if is_num(score) and (score > 8):
                    return 'A'
                elif is_num(score) and (5 <= score <= 8):
                    return 'B'
        return 'C'

    def _compute_category(self, property_data: Dict[str, Any], third_map: Dict[str, Dict]) -> str:
        """Вычисляет категорию для записи на основе property_data и third_map.
        
        Используется как для новых записей, так и для обновления существующих.
        Берёт area из property_data (может быть из CRM API или из БД).
        """
        # Берём complex только из property_data (SQL/Sheets). Если его нет — сразу 'С'
        complex_name = property_data.get('complex') or ''
        if not complex_name or not third_map:
            return 'С'
        key = self._find_by_variants(complex_name, third_map)
        if not key:
            return 'С'
        params = third_map.get(key) or {}
        roof = params.get('roof')
        window = params.get('window')
        score = params.get('score')
        # Площадь берём из property_data (может быть из CRM API или из БД)
        area = property_data.get('area')
        try:
            area = float(area) if area is not None else None
        except Exception:
            area = None
        contract_price = property_data.get('contract_price')
        window_price = (window * area) if (window is not None and area is not None) else None
        roof_price = (roof * area) if (roof is not None and area is not None) else None
        return self._assign_category(contract_price, window_price, roof_price, score)
    
    def _compute_category_for_insert(self, property_data: Dict[str, Any], third_map: Dict[str, Dict], api_data: Dict[str, Any]) -> str:
        # Берём complex только из property_data (SQL/Sheets). Если его нет — сразу 'С'
        complex_name = property_data.get('complex') or ''
        if not complex_name or not third_map:
            return 'С'
        key = self._find_by_variants(complex_name, third_map)
        if not key:
            return 'С'
        params = third_map.get(key) or {}
        roof = params.get('roof')
        window = params.get('window')
        score = params.get('score')
        area = api_data.get('area')  # площадь — только для расчёта
        try:
            area = float(area) if area is not None else None
        except Exception:
            area = None
        contract_price = property_data.get('contract_price')
        window_price = (window * area) if (window is not None and area is not None) else None
        roof_price = (roof * area) if (roof is not None and area is not None) else None
        return self._assign_category(contract_price, window_price, roof_price, score)

    # Тестовый метод удалён по требованию
    
    async def _load_deals_sheet(self) -> List[Dict]:
        """Загружает данные из таблицы SHEET_DEALS (только поля из Sheets)"""
        try:
            # Читаем значения напрямую и берём только столбцы A..G (7 столбцов)
            values = await self._to_thread(self.deals_sheet.get_all_values)
            if not values:
                raise RuntimeError("Лист пуст или недоступен")
            # Первая строка — заголовки, игнорируем их содержимое и жёстко маппим A..G на EXPECTED_DEALS_HEADERS
            rows = values[1:]
            records = []
            for row in rows:
                # берём первые 7 ячеек (A..G)
                cols = (row[:7] if len(row) >= 7 else row + [''] * (7 - len(row)))
                # пропускаем полностью пустые строки
                if not any((c.strip() for c in cols)):
                    continue
                rec = {EXPECTED_DEALS_HEADERS[i]: cols[i].strip() for i in range(7)}
                records.append(rec)
            
            # Преобразуем в нужный формат
            deals_data = []
            def _to_str(v: Any) -> str:
                return '' if v is None else str(v)

            for record in records:
                crm_val = str(record.get('CRM ID', '')).strip()
                if crm_val:  # Пропускаем пустые строки и пробелы
                    date_signed = self._parse_date(record.get('Дата подписания'))
                    expires_date = self._calculate_expires_date(date_signed)
                    
                    deals_data.append({
                        'crm_id': crm_val,
                        'date_signed': date_signed,
                        'contract_number': _to_str(record.get('Номер договора', '')),
                        'mop': _to_str(record.get('МОП', '')),
                        'rop': _to_str(record.get('РОП', '')),
                        'dd': _to_str(record.get('ДД', '')),
                        'client_name': _to_str(record.get('Имя клиента и номер', '')),
                        'address': '',  # Будет заполнено из CRM API
                        'complex': '',  # Будет заполнено из CRM API
                        'contract_price': None,  # Будет заполнено из CRM API
                        'expires': expires_date  # Вычисляется автоматически
                    })
            
            logger.info(f"Загружено {len(deals_data)} записей из SHEET_DEALS")
            return deals_data
            
        except Exception as e:
            logger.error(f"Ошибка загрузки SHEET_DEALS: {e}")
            return []

    async def _enrich_with_crm_data(self, deals_data: List[Dict]):
        """Обогащает данные из Google Sheets данными из CRM API для полей address, complex, contract_price, area"""
        if not deals_data:
            return
        
        # Собираем все CRM ID
        crm_ids = [deal['crm_id'] for deal in deals_data if deal.get('crm_id')]
        if not crm_ids:
            logger.warning("Нет CRM ID для обогащения данными из API")
            return
        
        logger.info(f"Обогащение {len(crm_ids)} записей данными из CRM API")
        
        try:
            # Получаем данные из CRM API батчами
            async with APIClient() as api_client:
                crm_data = await api_client.get_crm_data_batch(crm_ids, batch_size=200)
                
                # Обновляем данные
                updated_count = 0
                for deal in deals_data:
                    crm_id = deal.get('crm_id')
                    if crm_id and crm_id in crm_data:
                        api_data = crm_data[crm_id]
                        
                        # Заменяем данные из Sheets на данные из API
                        updated = False
                        if api_data.get('address'):
                            deal['address'] = api_data['address']
                            updated = True
                        if api_data.get('complex'):
                            deal['complex'] = api_data['complex']
                            updated = True
                        if api_data.get('price') is not None:
                            deal['contract_price'] = api_data['price']
                            updated = True
                        if api_data.get('area') is not None:
                            try:
                                deal['area'] = float(api_data['area'])
                                updated = True
                            except Exception:
                                pass
                        
                        if updated:
                            updated_count += 1
                            logger.debug(f"Обновлены данные для CRM ID {crm_id}: address={deal['address'][:50]}..., complex={deal['complex']}, price={deal['contract_price']}")
                    else:
                        logger.debug(f"Не удалось получить данные из API для CRM ID {crm_id}")
                
                logger.info(f"Обогащение завершено: {updated_count} из {len(deals_data)} записей обновлено данными из CRM API")
                
        except Exception as e:
            logger.error(f"Ошибка обогащения данными из CRM API: {e}")
            # Продолжаем работу с исходными данными из Sheets

    async def _enrich_new_records_with_crm_data(self, new_crm_ids: List[str]) -> Dict[str, Dict]:
        """Получает данные из CRM API только для новых CRM ID"""
        if not new_crm_ids:
            return {}
        
        logger.info(f"Получение данных из CRM API для {len(new_crm_ids)} новых записей")
        
        try:
            async with APIClient() as api_client:
                crm_data = await api_client.get_crm_data_batch(new_crm_ids, batch_size=200)
                logger.info(f"Получены данные из CRM API для {len(crm_data)} новых записей")
                return crm_data
                
        except Exception as e:
            logger.error(f"Ошибка получения данных из CRM API для новых записей: {e}")
            return {}
    
    async def _load_progress_sheet(self) -> List[Dict]:
        """Загружает данные из таблицы SHEET_PROGRESS (можно изменять)"""
        try:
            # Читаем значения напрямую
            values = await self._to_thread(self.progress_sheet.get_all_values)
            if not values:
                return []

            headers = [h.strip() for h in values[0]] if values else []
            header_index: Dict[str, int] = {name: idx for idx, name in enumerate(headers) if name}

            # Поля, которые ожидаем прочитать
            expected_fields = [
                'CRM ID','category','collage','prof_collage','krisha','instagram','tiktok',
                'mailing','stream','shows','analytics','price_update','provide_analytics',
                'push_for_price','status'
            ]

            def _get(row: List[str], key: str) -> Any:
                idx = header_index.get(key)
                return row[idx].strip() if idx is not None and idx < len(row) else ''

            # Преобразуем в нужный формат
            progress_data = []
            def _to_str(v: Any) -> str:
                return '' if v is None else str(v)

            for row in values[1:]:
                crm_val = _get(row, 'CRM ID')
                if not crm_val:
                    continue
                progress_data.append({
                    'crm_id': crm_val,
                    'category': _to_str(_get(row, 'category')),
                    'collage': self._parse_boolean(_get(row, 'collage')),
                    'prof_collage': self._parse_boolean(_get(row, 'prof_collage')),
                    'krisha': _to_str(_get(row, 'krisha')),
                    'instagram': _to_str(_get(row, 'instagram')),
                    'tiktok': _to_str(_get(row, 'tiktok')),
                    'mailing': _to_str(_get(row, 'mailing')),
                    'stream': _to_str(_get(row, 'stream')),
                    'shows': self._parse_int(_get(row, 'shows')),
                    'analytics': self._parse_boolean(_get(row, 'analytics')),
                    'price_update': _to_str(_get(row, 'price_update')),
                    'provide_analytics': self._parse_boolean(_get(row, 'provide_analytics')),
                    'push_for_price': self._parse_boolean(_get(row, 'push_for_price')),
                    'status': _to_str(_get(row, 'status') or 'Размещено')
                })
            
            logger.info(f"Загружено {len(progress_data)} записей из SHEET_PROGRESS")
            return progress_data
            
        except Exception as e:
            logger.error(f"Ошибка загрузки SHEET_PROGRESS: {e}")
            return []
    
    async def _delete_missing_records(self, session: AsyncSession, valid_crm_ids: set):
        """Удаляет из БД записи, чьи CRM ID отсутствуют в первой таблице."""
        # Получаем все CRM ID из БД
        result = await session.execute(text("SELECT crm_id FROM properties"))
        db_ids = {row.crm_id for row in result.fetchall()}
        to_delete = list(db_ids - valid_crm_ids)
        if not to_delete:
            logger.info("Нет записей для удаления")
            return
        # Удаляем партиями
        logger.info(f"Удаляем {len(to_delete)} записей, отсутствующих в SHEET_DEALS")
        await session.execute(
            text("DELETE FROM properties WHERE crm_id = ANY(:ids)"),
            {"ids": to_delete}
        )
        logger.info(f"Успешно удалено {len(to_delete)} записей")
    
    async def _upsert_property(self, session: AsyncSession, crm_id: str, property_data: Dict) -> str:
        """Вставляет или обновляет запись в базе данных"""
        try:
            # Проверяем, существует ли запись
            result = await session.execute(
                text("SELECT crm_id FROM properties WHERE crm_id = :crm_id"),
                {"crm_id": crm_id}
            )
            exists = result.fetchone() is not None
            
            if exists:
                # Обновляем существующую запись: базовые поля из deals + вычисленные поля из API/Лист3
                deals_readonly_fields = {
                    'date_signed','contract_number','mop','rop','dd',
                    'client_name','address','complex','contract_price','expires',
                    # добавляем вычисляемые поля, чтобы не было рассинхронизации
                    'area','krisha_price','vitrina_price','score'
                }
                # Добавляем category только если она присутствует в property_data (т.е. была вычислена)
                if 'category' in property_data:
                    deals_readonly_fields.add('category')
                update_data = {k: v for k, v in property_data.items() if k in deals_readonly_fields and v is not None}
                update_data['last_modified_at'] = datetime.now()
                
                set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys()])
                query = f"UPDATE properties SET {set_clause} WHERE crm_id = :crm_id"
                
                params = update_data.copy()
                params['crm_id'] = crm_id
                
                await session.execute(text(query), params)
                return "updated"
            else:
                # Создаем новую запись
                property_data['created_at'] = datetime.now()
                property_data['last_modified_at'] = datetime.now()
                
                columns = ", ".join(property_data.keys())
                placeholders = ", ".join([f":{k}" for k in property_data.keys()])
                query = f"INSERT INTO properties ({columns}) VALUES ({placeholders})"
                
                await session.execute(text(query), property_data)
                return "created"
                
        except Exception as e:
            logger.error(f"Ошибка upsert для {crm_id}: {e}")
            logger.error(f"Данные записи: {property_data}")
            raise
    
    async def sync_to_sheets(self) -> Dict[str, int]:
        """Полная выгрузка БД в Google Sheets (2). Полная копия SQL, кроме
        last_modified_by, last_modified_at, created_at. Полностью перезаписывает лист."""
        if self.sync_in_progress:
            logger.warning("Синхронизация уже выполняется, пропускаем")
            return {"skipped": 1}
        
        self.sync_in_progress = True
        stats = {"updated": 0, "errors": 0}
        
        try:
            logger.info("Начинаем полную выгрузку БД в SHEET_PROGRESS (лист 2)")
            # Читаем все из БД
            async with self.async_session() as session:
                result = await session.execute(text("SELECT * FROM properties ORDER BY last_modified_at DESC"))
                rows = [dict(r._mapping) for r in result.fetchall()]
            
            # Готовим заголовки и значения
            if rows:
                # Исключаем мета-колонки и формируем упорядоченный список заголовков
                excluded = {"last_modified_by", "last_modified_at", "created_at"}
                present_keys = [k for k in rows[0].keys() if k not in excluded]
                desired_order = [
                    'crm_id','date_signed','contract_number','mop','rop','dd','client_name','address','complex',
                    'area','contract_price','expires','krisha_price','vitrina_price','score','category',
                    'collage','prof_collage','krisha','instagram','tiktok','mailing','stream','shows','analytics',
                    'price_update','provide_analytics','push_for_price','status'
                ]
                # Сначала желаемый порядок, затем оставшиеся поля, чтобы ничего не потерять
                headers = [h for h in desired_order if h in present_keys] + [k for k in present_keys if k not in set(desired_order)]
                def _to_cell_value(v: Any) -> Any:
                    # Приводим типы к сериализуемым для Google Sheets
                    if v is None:
                        return ""
                    # Булевы — нативный bool
                    if isinstance(v, bool):
                        return v
                    # Дата/время — ISO строка (только дата для date)
                    try:
                        from datetime import date, datetime
                        if isinstance(v, date) and not isinstance(v, datetime):
                            return v.strftime('%Y-%m-%d')
                        if isinstance(v, datetime):
                            return v.strftime('%Y-%m-%d %H:%M:%S')
                    except Exception:
                        pass
                    # Decimal -> float для числовых колонок
                    try:
                        from decimal import Decimal
                        if isinstance(v, Decimal):
                            return float(v)
                    except Exception:
                        pass
                    return v
                values = [[_to_cell_value(row.get(k)) for k in headers] for row in rows]
            else:
                headers = [
                    'crm_id','date_signed','contract_number','mop','rop','dd','client_name','address','complex',
                    'area','contract_price','expires','krisha_price','vitrina_price','score','category','collage','prof_collage','krisha','instagram','tiktok',
                    'mailing','stream','shows','analytics','price_update','provide_analytics','push_for_price','status'
                ]
                values = []
            
            # Очищаем и записываем
            logger.info(f"Очищаем лист SHEET_PROGRESS и записываем {len(values)} строк")
            await self._to_thread(self.progress_sheet.clear)
            # Пишем заголовок и данные разом
            logger.info(f"Записываем заголовки: {headers}")
            await self._to_thread(self.progress_sheet.update, 'A1', [headers], value_input_option='USER_ENTERED')
            if values:
                logger.info(f"Записываем данные: {len(values)} строк")
                await self._to_thread(self.progress_sheet.update, 'A2', values, value_input_option='USER_ENTERED')
                
                # Установим формат DATE для колонок дат, если они присутствуют
                try:
                    def col_index(name: str) -> int:
                        return headers.index(name) if name in headers else -1
                    date_cols = [col_index('date_signed'), col_index('expires')]
                    date_cols = [c for c in date_cols if c >= 0]
                    if date_cols:
                        # Применяем формат ко всем строкам с данными (со 2-й строки)
                        sheet_id = self.progress_sheet.id
                        requests = []
                        for c in date_cols:
                            requests.append({
                                'repeatCell': {
                                    'range': {
                                        'sheetId': sheet_id,
                                        'startRowIndex': 1,  # со второй строки
                                        'endRowIndex': 1 + len(values),
                                        'startColumnIndex': c,
                                        'endColumnIndex': c + 1
                                    },
                                    'cell': {
                                        'userEnteredFormat': {
                                            'numberFormat': {
                                                'type': 'DATE',
                                                'pattern': 'yyyy-mm-dd'
                                            }
                                        }
                                    },
                                    'fields': 'userEnteredFormat.numberFormat'
                                }
                            })
                        # Устанавливаем чекбоксы для булевых колонок
                        bool_cols_names = ['collage','prof_collage','analytics','provide_analytics','push_for_price']
                        bool_cols = [col_index(n) for n in bool_cols_names]
                        bool_cols = [c for c in bool_cols if c >= 0]
                        for c in bool_cols:
                            requests.append({
                                'setDataValidation': {
                                    'range': {
                                        'sheetId': sheet_id,
                                        'startRowIndex': 1,
                                        'endRowIndex': 1 + len(values),
                                        'startColumnIndex': c,
                                        'endColumnIndex': c + 1
                                    },
                                    'rule': {
                                        'condition': { 'type': 'BOOLEAN' },
                                        'showCustomUi': True
                                    }
                                }
                            })
                        # Выполняем пакетное обновление непосредственно через клиент
                        await self._to_thread(self.spreadsheet.batch_update, {'requests': requests})
                except Exception as fmt_err:
                    logger.warning(f"Не удалось применить формат даты: {fmt_err}")
            
            stats["updated"] = len(values)
            logger.info(f"Выгружено {stats['updated']} строк в SHEET_PROGRESS")
        except Exception as e:
            logger.error(f"Ошибка выгрузки в Google Sheets (2): {e}", exc_info=True)
            stats["errors"] += 1
        finally:
            self.sync_in_progress = False
        return stats
    
    # Удален неиспользуемый метод _update_progress_sheet
    
    def _get_column_letter(self, column_name: str) -> Optional[str]:
        """Возвращает букву колонки по имени"""
        column_mapping = {
            'category': 'B',
            'collage': 'C',
            'prof_collage': 'D',
            'krisha': 'E',
            'instagram': 'F',
            'tiktok': 'G',
            'mailing': 'H',
            'stream': 'I',
            'shows': 'J',
            'analytics': 'K',
            'price_update': 'L',
            'provide_analytics': 'M',
            'push_for_price': 'N',
            'status': 'O'
        }
        return column_mapping.get(column_name)
    
    # Удален неиспользуемый метод merge_records
    
    async def run_background_sync(self):
        """Фоновая задача синхронизации каждые SYNC_INTERVAL_MINUTES минут"""
        logger.info("Запущена фоновая синхронизация")
        interval_sec = max(1, int(self.sync_interval_minutes)) * 60
        while True:
            try:
                await asyncio.sleep(interval_sec)
                logger.info("Выполняется фоновая синхронизация")
                # Выполняем только быструю синхронизацию
                # Полная синхронизация теперь доступна только через команду /sync для авторизованного пользователя
                fast_stats = await self.sync_from_sheets_fast()
                logger.info(f"Быстрая синхронизация Sheets(1)->DB: {fast_stats}")
                # После быстрой — выгружаем DB -> Sheets(2)
                to_sheets_stats = await self.sync_to_sheets()
                logger.info(f"Синхронизация DB->Sheets(2): {to_sheets_stats}")
            except Exception as e:
                logger.error(f"Ошибка фоновой синхронизации: {e}", exc_info=True)
                await asyncio.sleep(60)  # Пауза при ошибке
    
    # Вспомогательные методы для парсинга данных
    
    def _calculate_expires_date(self, date_signed: Optional[datetime]) -> Optional[datetime]:
        """Вычисляет дату истечения: дата подписания + 2 месяца"""
        if not date_signed:
            return None
        
        try:
            # Используем более надежный способ добавления месяцев
            from dateutil.relativedelta import relativedelta
            
            # Добавляем 2 месяца к дате подписания
            expires_date = date_signed + relativedelta(months=2)
            
            return expires_date
                
        except Exception as e:
            logger.error(f"Ошибка вычисления даты истечения для {date_signed}: {e}")
            return None
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Парсит дату из строки"""
        if not date_str:
            return None
        
        try:
            # Пробуем разные форматы
            for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt).date()
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _parse_price(self, price_str: str) -> Optional[int]:
        """Парсит цену из строки и возвращает целое число"""
        if not price_str:
            return None
        
        try:
            # Если уже число, возвращаем как целое
            if isinstance(price_str, (int, float)):
                return int(price_str)
            
            price_str = str(price_str).strip()
            
            # Обрабатываем случаи с "млн", "тыс" и т.д.
            price_str_lower = price_str.lower()
            multiplier = 1
            
            if 'млн' in price_str_lower or 'million' in price_str_lower:
                multiplier = 1000000
            elif 'тыс' in price_str_lower or 'thousand' in price_str_lower:
                multiplier = 1000
            
            # Убираем все кроме цифр, точек и запятых
            cleaned = ''.join(c for c in price_str if c.isdigit() or c in '.,')
            if not cleaned:
                return None
            
            # Заменяем запятую на точку
            cleaned = cleaned.replace(',', '.')
            base_value = float(cleaned)
            
            # Возвращаем целое число
            return int(base_value * multiplier)
        except Exception:
            return None
    
    def _parse_boolean(self, value: Any) -> bool:
        """Парсит булево значение"""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            value = value.strip().upper()
            return value in ['TRUE', '1', 'ДА', 'YES', 'Y', '✓', '☑', 'CHECKED']
        return False
    
    def _parse_int(self, value: Any) -> int:
        """Парсит целое число"""
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                return 0
        return 0
    
    async def close(self):
        """Закрывает все подключения"""
        try:
            await self.engine.dispose()
            logger.info("Подключения закрыты")
        except Exception as e:
            logger.error(f"Ошибка при закрытии подключений: {e}")


# Глобальный экземпляр менеджера синхронизации
sync_manager: Optional[SheetsSyncManager] = None

async def init_sync_manager(config: Dict[str, Any]) -> SheetsSyncManager:
    """Инициализирует глобальный менеджер синхронизации"""
    global sync_manager
    sync_manager = SheetsSyncManager(config)
    await sync_manager.init_db()
    return sync_manager

async def get_sync_manager() -> SheetsSyncManager:
    """Возвращает глобальный менеджер синхронизации"""
    if sync_manager is None:
        raise RuntimeError("Менеджер синхронизации не инициализирован")
    return sync_manager
