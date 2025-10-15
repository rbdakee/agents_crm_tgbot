"""
Модуль для работы с PostgreSQL базой данных
Заменяет старую систему кеширования на прямую работу с БД
"""

import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

logger = logging.getLogger(__name__)

class PostgreSQLManager:
    """Менеджер для работы с PostgreSQL базой данных"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._init_database()
    
    def _init_database(self):
        """Инициализация подключения к PostgreSQL"""
        try:
            # Создаем асинхронный движок
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600,
                pool_size=10,
                max_overflow=20
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
    
    async def get_agent_contracts_page(self, agent_name: str, page: int = 1, page_size: int = 10) -> Tuple[List[Dict], int]:
        """Получает страницу контрактов агента с пагинацией"""
        try:
            offset = (page - 1) * page_size
            
            async with self.async_session() as session:
                # Разбиваем ФИО (ожидаем Фамилия Имя, отчество опционально); требуем наличие и фамилии, и имени
                fio_parts = [p for p in str(agent_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"

                where_clause = (
                    "((LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like) "
                    "OR (LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like) "
                    "OR (LOWER(dd)  LIKE :surname_like AND LOWER(dd)  LIKE :name_like))"
                )

                # Получаем общее количество контрактов агента (совпадение по фамилии и имени)
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM properties WHERE {where_clause}"),
                    {"surname_like": surname_like, "name_like": name_like}
                )
                total_count = count_result.scalar()
                
                # Получаем страницу контрактов
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE {where_clause} ORDER BY last_modified_at DESC LIMIT :limit OFFSET :offset"),
                    {"surname_like": surname_like, "name_like": name_like, "limit": page_size, "offset": offset}
                )
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    # Преобразуем в формат, совместимый со старым API
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                logger.info(f"Загружено {len(contracts)} контрактов для агента {agent_name} (страница {page})")
                return contracts, total_count
                
        except Exception as e:
            logger.error(f"Ошибка получения контрактов агента {agent_name}: {e}")
            return [], 0
    
    async def search_contract_by_crm_id(self, crm_id: str, agent_name: str) -> Optional[Dict]:
        """Ищет контракт по CRM ID для конкретного агента"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(agent_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                where_clause = (
                    "((LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like) "
                    "OR (LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like) "
                    "OR (LOWER(dd)  LIKE :surname_like AND LOWER(dd)  LIKE :name_like))"
                )
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE crm_id = :crm_id AND {where_clause}"),
                    {"crm_id": crm_id, "surname_like": surname_like, "name_like": name_like}
                )
                
                row = result.fetchone()
                if row:
                    contract_dict = dict(row._mapping)
                    return self._convert_to_legacy_format(contract_dict)
                
                return None
                
        except Exception as e:
            logger.error(f"Ошибка поиска контракта {crm_id} для агента {agent_name}: {e}")
            return None
    
    async def search_contracts_by_client_name_lazy(self, client_name: str, agent_name: str, page: int = 1, page_size: int = 10) -> Tuple[List[Dict], int]:
        """Ищет контракты по имени клиента с пагинацией"""
        try:
            offset = (page - 1) * page_size
            
            async with self.async_session() as session:
                fio_parts = [p for p in str(agent_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                where_clause = (
                    "((LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like) "
                    "OR (LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like) "
                    "OR (LOWER(dd)  LIKE :surname_like AND LOWER(dd)  LIKE :name_like))"
                )
                # Получаем общее количество
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM properties WHERE LOWER(client_name) LIKE LOWER(:client_name) AND {where_clause}"),
                    {"client_name": f"%{client_name}%", "surname_like": surname_like, "name_like": name_like}
                )
                total_count = count_result.scalar()
                
                # Получаем страницу результатов
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE LOWER(client_name) LIKE LOWER(:client_name) AND {where_clause} ORDER BY last_modified_at DESC LIMIT :limit OFFSET :offset"),
                    {"client_name": f"%{client_name}%", "surname_like": surname_like, "name_like": name_like, "limit": page_size, "offset": offset}
                )
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                logger.info(f"Найдено {len(contracts)} контрактов для клиента '{client_name}' агента {agent_name}")
                return contracts, total_count
                
        except Exception as e:
            logger.error(f"Ошибка поиска контрактов по клиенту {client_name}: {e}")
            return [], 0
    
    async def update_contract(self, crm_id: str, updates: Dict[str, Any]) -> bool:
        """Обновляет контракт в базе данных"""
        try:
            async with self.async_session() as session:
                # Проверяем, существует ли контракт
                result = await session.execute(
                    text("SELECT crm_id FROM properties WHERE crm_id = :crm_id"),
                    {"crm_id": crm_id}
                )
                
                if not result.fetchone():
                    logger.warning(f"Контракт {crm_id} не найден для обновления")
                    return False
                
                # Подготавливаем данные для обновления
                update_data = {}
                for key, value in updates.items():
                    # Преобразуем ключи в формат базы данных
                    db_key = self._convert_key_to_db_format(key)
                    update_data[db_key] = value
                
                # Добавляем метаданные
                update_data['last_modified_by'] = 'BOT'
                update_data['last_modified_at'] = datetime.now()
                
                # Выполняем обновление
                set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys()])
                query = f"UPDATE properties SET {set_clause} WHERE crm_id = :crm_id"
                
                params = update_data.copy()
                params['crm_id'] = crm_id
                
                await session.execute(text(query), params)
                
                await session.commit()
                logger.info(f"Контракт {crm_id} обновлен: {list(updates.keys())}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка обновления контракта {crm_id}: {e}")
            return False
    
    async def get_agent_by_phone(self, phone: str) -> Optional[str]:
        """Получает имя агента по номеру телефона"""
        try:
            # Нормализуем номер телефона
            normalized_phone = self.normalize_phone(phone)
            logger.info(f"Поиск агента: введенный номер {phone}, нормализованный {normalized_phone}")
            
            # Загружаем файл агентов
            import pandas as pd
            agents_df = pd.read_csv('data/agents.csv', header=None, names=['name', 'phone'])
            
            # Ищем агента по номеру
            for _, row in agents_df.iterrows():
                agent_phone_raw = str(row.get('phone', ''))
                agent_phone = self.normalize_phone(agent_phone_raw)
                if agent_phone == normalized_phone:
                    logger.info(f"Найден агент: {row.get('name', '')} с номером {agent_phone_raw}")
                    return str(row.get('name', ''))
            
            # Если не найден, попробуем найти по частичному совпадению
            logger.info(f"Попытка частичного поиска для номера {normalized_phone}")
            for _, row in agents_df.iterrows():
                agent_phone_raw = str(row.get('phone', ''))
                agent_phone = self.normalize_phone(agent_phone_raw)
                # Проверяем последние 10 цифр
                if len(normalized_phone) >= 10 and len(agent_phone) >= 10:
                    if normalized_phone[-10:] == agent_phone[-10:]:
                        logger.info(f"Найден агент по частичному совпадению: {row.get('name', '')} с номером {agent_phone_raw}")
                        return str(row.get('name', ''))
            
            logger.warning(f"Агент с номером {normalized_phone} не найден")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска агента по телефону {phone}: {e}")
            return None
    
    async def get_phone_by_agent(self, agent_name: str) -> Optional[str]:
        """Получает номер телефона агента по имени"""
        try:
            # Загружаем файл агентов
            import pandas as pd
            agents_df = pd.read_csv('data/agents.csv', header=None, names=['name', 'phone'])
            
            # Ищем агента по имени
            for _, row in agents_df.iterrows():
                if str(row.get('name', '')).strip() == agent_name.strip():
                    return str(row.get('phone', ''))
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска телефона агента {agent_name}: {e}")
            return None
    
    def is_valid_phone(self, phone: str) -> bool:
        """Проверяет валидность номера телефона"""
        if not phone:
            return False
        
        # Убираем все символы кроме цифр
        digits_only = ''.join(c for c in phone if c.isdigit())
        
        # Проверяем длину (10-11 цифр)
        if len(digits_only) < 10 or len(digits_only) > 11:
            return False
        
        # Проверяем, что начинается с 7 или 8
        if not (digits_only.startswith('7') or digits_only.startswith('8')):
            return False
        
        return True
    
    def normalize_phone(self, phone: str) -> str:
        """Нормализует номер телефона"""
        if not phone:
            return ""
        
        # Убираем все символы кроме цифр
        digits_only = ''.join(c for c in phone if c.isdigit())
        
        # Если номер начинается с 8, заменяем на 7
        if digits_only.startswith('8') and len(digits_only) == 11:
            digits_only = '7' + digits_only[1:]
        
        # Если номер начинается с 7 и имеет 11 цифр, оставляем как есть
        if digits_only.startswith('7') and len(digits_only) == 11:
            return digits_only
        
        # Если номер имеет 10 цифр, добавляем 7 в начало
        if len(digits_only) == 10:
            return '7' + digits_only
        
        # Если номер имеет 9 цифр, добавляем 77 в начало (для казахстанских номеров)
        if len(digits_only) == 9:
            return '77' + digits_only
        
        return digits_only
    
    def _convert_to_legacy_format(self, db_record: Dict) -> Dict:
        """Преобразует запись из БД в формат, совместимый со старым API"""
        return {
            'CRM ID': db_record.get('crm_id', ''),
            'Дата подписания': db_record.get('date_signed', ''),
            'Номер договора': db_record.get('contract_number', ''),
            'МОП': db_record.get('mop', ''),
            'РОП': db_record.get('rop', ''),
            'ДД': db_record.get('dd', ''),
            'Имя клиента и номер': db_record.get('client_name', ''),
            'Адрес': db_record.get('address', ''),
            'ЖК': db_record.get('complex', ''),
            'Цена указанная в договоре': db_record.get('contract_price', ''),
            'Истекает': db_record.get('expires', ''),
            'category': db_record.get('category', ''),
            'collage': db_record.get('collage', False),
            'prof_collage': db_record.get('prof_collage', False),
            'krisha': db_record.get('krisha', ''),
            'instagram': db_record.get('instagram', ''),
            'tiktok': db_record.get('tiktok', ''),
            'mailing': db_record.get('mailing', ''),
            'stream': db_record.get('stream', ''),
            'shows': db_record.get('shows', 0),
            'analytics': db_record.get('analytics', False),
            'price_update': db_record.get('price_update', ''),
            'provide_analytics': db_record.get('provide_analytics', False),
            'push_for_price': db_record.get('push_for_price', False),
            'status': db_record.get('status', 'Размещено'),
            'last_modified_by': db_record.get('last_modified_by', 'SHEET'),
            'last_modified_at': db_record.get('last_modified_at', ''),
            'created_at': db_record.get('created_at', '')
        }
    
    def _convert_key_to_db_format(self, key: str) -> str:
        """Преобразует ключ из старого формата в формат БД"""
        key_mapping = {
            'CRM ID': 'crm_id',
            'Дата подписания': 'date_signed',
            'Номер договора': 'contract_number',
            'МОП': 'mop',
            'РОП': 'rop',
            'ДД': 'dd',
            'Имя клиента и номер': 'client_name',
            'Адрес': 'address',
            'ЖК': 'complex',
            'Цена указанная в договоре': 'contract_price',
            'Истекает': 'expires',
            'category': 'category',
            'collage': 'collage',
            'prof_collage': 'prof_collage',
            'krisha': 'krisha',
            'instagram': 'instagram',
            'tiktok': 'tiktok',
            'mailing': 'mailing',
            'stream': 'stream',
            'shows': 'shows',
            'analytics': 'analytics',
            'price_update': 'price_update',
            'provide_analytics': 'provide_analytics',
            'push_for_price': 'push_for_price',
            'status': 'status'
        }
        return key_mapping.get(key, key)
    
    async def get_cache_stats(self) -> Dict:
        """Возвращает статистику базы данных (для совместимости)"""
        try:
            async with self.async_session() as session:
                # Общее количество записей
                total_result = await session.execute(
                    text("SELECT COUNT(*) FROM properties")
                )
                total_records = total_result.scalar()
                
                # Количество записей по агентам
                agents_result = await session.execute(
                    text("SELECT mop, COUNT(*) as count FROM properties GROUP BY mop")
                )
                agents_stats = {row.mop: row.count for row in agents_result.fetchall()}
                
                return {
                    'total_records': total_records,
                    'agents_stats': agents_stats,
                    'database_type': 'PostgreSQL',
                    'sync_enabled': True
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики БД: {e}")
            return {'error': str(e)}
    
    async def preload_popular_contracts_fire_and_forget(self, agent_name: str, limit: int = 10):
        """Предзагружает популярные контракты (для совместимости)"""
        # В новой системе предзагрузка не нужна, так как данные уже в БД
        logger.info(f"Предзагрузка для агента {agent_name} не требуется (данные в БД)")
    
    async def close(self):
        """Закрывает подключение к базе данных"""
        try:
            await self.engine.dispose()
            logger.info("Подключение к PostgreSQL закрыто")
        except Exception as e:
            logger.error(f"Ошибка при закрытии подключения: {e}")


# Глобальный экземпляр менеджера БД
db_manager: Optional[PostgreSQLManager] = None

async def init_db_manager(database_url: str) -> PostgreSQLManager:
    """Инициализирует глобальный менеджер БД"""
    global db_manager
    db_manager = PostgreSQLManager(database_url)
    return db_manager

async def get_db_manager() -> PostgreSQLManager:
    """Возвращает глобальный менеджер БД"""
    if db_manager is None:
        raise RuntimeError("Менеджер БД не инициализирован")
    return db_manager
