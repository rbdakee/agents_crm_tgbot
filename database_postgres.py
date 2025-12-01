import logging, gspread, os
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from sqlalchemy import text, func, and_, or_, select, update, Table, Column, String, Integer, BigInteger, Boolean, Date, DateTime, MetaData, Float, Text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from google.oauth2.service_account import Credentials
from config import SHEET_ID, THIRD_SHEET_GID, DB_BATCH_SIZE, DB_POOL_SIZE, DB_MAX_OVERFLOW, DB_POOL_RECYCLE
from api_client import APIClient

logger = logging.getLogger(__name__)

def chunk_list(items: List[Any], size: int) -> List[List[Any]]:
    for idx in range(0, len(items), size):
        yield items[idx:idx + size]

# SQLAlchemy Core table definition (safe subset covering used columns)
metadata = MetaData()
properties = Table(
    "properties", metadata,
    Column("crm_id", String(50), primary_key=True),
    Column("date_signed", Date),
    Column("contract_number", String(100)),
    Column("mop", String(100)),
    Column("rop", String(100)),
    Column("dd", String(100)),
    Column("client_name", String),
    Column("address", String),
    Column("complex", String(200)),
    Column("contract_price", BigInteger),
    Column("expires", Date),
    Column("category", String(100)),
    Column("area", Float),
    Column("rooms_count", Integer),
    Column("krisha_price", BigInteger),
    Column("vitrina_price", BigInteger),
    Column("score", Float),
    Column("collage", Boolean),
    Column("prof_collage", Boolean),
    Column("krisha", String),
    Column("instagram", String),
    Column("tiktok", String),
    Column("mailing", String),
    Column("stream", String),
    Column("shows", Integer),
    Column("analytics", Boolean),
    Column("price_update", String),
    Column("provide_analytics", Boolean),
    Column("push_for_price", Boolean),
    Column("status", String(100)),
    Column("last_modified_by", String(10)),
    Column("last_modified_at", DateTime),
    Column("created_at", DateTime),
)

parsed_properties_table = Table(
    "parsed_properties", metadata,
    Column("vitrina_id", BigInteger, primary_key=True, autoincrement=True),
    Column("rbd_id", BigInteger, unique=True, nullable=False),
    Column("krisha_id", String(64)),
    Column("krisha_date", DateTime(timezone=True)),
    Column("object_type", String(255)),
    Column("address", Text),
    Column("complex", String(255)),
    Column("builder", String(255)),
    Column("flat_type", String(255)),
    Column("property_class", String(255)),
    Column("condition", String(255)),
    Column("sell_price", Float),
    Column("sell_price_per_m2", Float),
    Column("address_type", String(255)),
    Column("house_num", String(255)),
    Column("floor_num", Integer),
    Column("floor_count", Integer),
    Column("room_count", Integer),
    Column("phones", String(255)),
    Column("description", Text),
    Column("ceiling_height", Float),
    Column("area", Float),
    Column("year_built", Integer),
    Column("wall_type", String(255)),
    Column("stats_agent_given", String(255)),
    Column("stats_time_given", DateTime(timezone=True)),
    Column("stats_object_status", String(255)),
    Column("stats_recall_time", DateTime(timezone=True)),
    Column("stats_description", Text),
    Column("stats_object_category", String(10)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("updated_at", DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()),
)

ACTIVE_STATUS_FILTER = or_(
    properties.c.status.is_(None),
    func.lower(properties.c.status) != 'реализовано'
)

class PostgreSQLManager:
    """Менеджер для работы с PostgreSQL базой данных"""
    
    def __init__(self, database_url: str):
        self.database_url = database_url
        self._init_database()
        self._third_map_cache: Optional[Dict[str, Dict[str, Optional[float]]]] = None
        self._third_map_cache_time: Optional[datetime] = None
        self._third_map_cache_ttl = 3600  # Кеш на 1 час
    
    def _init_database(self):
        """Инициализация подключения к PostgreSQL"""
        try:
            # Создаем асинхронный движок
            self.engine = create_async_engine(
                self.database_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=DB_POOL_RECYCLE,
                pool_size=DB_POOL_SIZE,
                max_overflow=DB_MAX_OVERFLOW
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

    async def apply_database_optimizations(self) -> None:
        """Применяет оптимизации индексов для улучшения производительности БД"""
        try:
            async with self.async_session() as session:
                # Читаем SQL файл с оптимизациями
                optimization_file = os.path.join(os.path.dirname(__file__), 'database_optimization.sql')
                
                if os.path.exists(optimization_file):
                    with open(optimization_file, 'r', encoding='utf-8') as f:
                        sql_content = f.read()
                    
                    # Выполняем SQL команды (разделяем по ;)
                    statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
                    
                    for statement in statements:
                        if statement:
                            try:
                                await session.execute(text(statement))
                            except Exception as e:
                                # Игнорируем ошибки "already exists" для индексов
                                if 'already exists' not in str(e).lower():
                                    logger.warning(f"Ошибка при применении оптимизации: {e}")
                    
                    await session.commit()
                    logger.info("Оптимизации БД успешно применены")
                else:
                    logger.warning(f"Файл оптимизации не найден: {optimization_file}")
        except Exception as e:
            logger.error(f"Ошибка применения оптимизаций БД: {e}", exc_info=True)

    async def ensure_schema_with_backup(self) -> None:
        """Проверяет наличие новых колонок (area, krisha_price, vitrina_price, score, rooms_count).
        Если отсутствуют — создаёт резервную копию таблицы properties и добавляет недостающие колонки.
        Операция идемпотентная и безопасная: данные не удаляются, ALTER выполняются с IF NOT EXISTS.
        """
        try:
            async with self.async_session() as session:
                # Проверяем наличие нужных столбцов
                col_check = await session.execute(text(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'properties'
                      AND column_name IN ('area','krisha_price','vitrina_price','score','rooms_count')
                    """
                ))
                existing = {row.column_name for row in col_check.fetchall()}
                required = {'area','krisha_price','vitrina_price','score','rooms_count'}
                missing = required - existing
                if not missing:
                    logger.info("Схема таблицы properties уже содержит все новые колонки")
                    return

                logger.warning(f"Обнаружены отсутствующие колонки в properties: {missing}. Создаю резервную копию и применяю ALTER...")

                # Создаём резервную копию, если её нет
                tbl_check = await session.execute(text(
                    """
                    SELECT COUNT(*) FROM information_schema.tables
                    WHERE table_name = 'properties_backup'
                    """
                ))
                has_backup = (tbl_check.scalar() or 0) > 0
                if not has_backup:
                    # Создаём snapshot с данными
                    await session.execute(text("CREATE TABLE properties_backup AS TABLE properties WITH DATA"))
                    logger.info("Таблица properties_backup создана и заполнена")
                else:
                    logger.info("Таблица properties_backup уже существует — пропускаю создание")

                # Добавляем недостающие колонки
                alter_stmts = []
                if 'area' in missing:
                    alter_stmts.append("ALTER TABLE properties ADD COLUMN IF NOT EXISTS area DOUBLE PRECISION")
                if 'krisha_price' in missing:
                    alter_stmts.append("ALTER TABLE properties ADD COLUMN IF NOT EXISTS krisha_price BIGINT")
                if 'vitrina_price' in missing:
                    alter_stmts.append("ALTER TABLE properties ADD COLUMN IF NOT EXISTS vitrina_price BIGINT")
                if 'score' in missing:
                    alter_stmts.append("ALTER TABLE properties ADD COLUMN IF NOT EXISTS score DOUBLE PRECISION")
                if 'rooms_count' in missing:
                    alter_stmts.append("ALTER TABLE properties ADD COLUMN IF NOT EXISTS rooms_count INTEGER")
                for stmt in alter_stmts:
                    await session.execute(text(stmt))
                await session.commit()
                logger.info("ALTER TABLE выполнены, схема обновлена")
        except Exception as e:
            logger.error(f"Ошибка ensure_schema_with_backup: {e}")
    
    async def ensure_parsed_properties_schema(self) -> None:
        """Гарантирует наличие таблицы parsed_properties и колонки stats_object_category."""
        try:
            async with self.async_session() as session:
                # Проверяем наличие таблицы parsed_properties
                table_check = await session.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = 'parsed_properties'
                    )
                """))
                table_exists = table_check.scalar()

                if not table_exists:
                    logger.info("Таблица parsed_properties не найдена — создаю...")
                    await session.execute(text("""
                        CREATE TABLE IF NOT EXISTS parsed_properties (
                            vitrina_id BIGSERIAL PRIMARY KEY,
                            rbd_id BIGINT UNIQUE NOT NULL,
                            krisha_id VARCHAR(64),
                            krisha_date TIMESTAMPTZ,
                            object_type VARCHAR(255),
                            address TEXT,
                            complex VARCHAR(255),
                            builder VARCHAR(255),
                            flat_type VARCHAR(255),
                            property_class VARCHAR(255),
                            condition VARCHAR(255),
                            sell_price DOUBLE PRECISION,
                            sell_price_per_m2 DOUBLE PRECISION,
                            address_type VARCHAR(255),
                            house_num VARCHAR(255),
                            floor_num INTEGER,
                            floor_count INTEGER,
                            room_count INTEGER,
                            phones VARCHAR(255),
                            description TEXT,
                            ceiling_height DOUBLE PRECISION,
                            area DOUBLE PRECISION,
                            year_built INTEGER,
                            wall_type VARCHAR(255),
                            stats_agent_given VARCHAR(255),
                            stats_time_given TIMESTAMPTZ,
                            stats_object_status VARCHAR(255),
                            stats_recall_time TIMESTAMPTZ,
                            stats_description TEXT,
                            stats_object_category VARCHAR(10),
                            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                        )
                    """))

                    # Создаем основные индексы
                    index_statements = [
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_created ON parsed_properties(created_at)",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_krisha_id ON parsed_properties(krisha_id) WHERE krisha_id IS NOT NULL AND krisha_id != ''",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_agent_given ON parsed_properties(stats_agent_given) WHERE stats_agent_given IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_status ON parsed_properties(stats_object_status) WHERE stats_object_status IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_time ON parsed_properties(stats_recall_time) WHERE stats_recall_time IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_recall_notification ON parsed_properties(stats_object_status, stats_recall_time, stats_agent_given) WHERE stats_object_status = 'Перезвонить' AND stats_recall_time IS NOT NULL AND stats_agent_given IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_latest ON parsed_properties(krisha_id, stats_agent_given, krisha_date DESC) WHERE krisha_id IS NOT NULL AND krisha_id != ''",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_time_given ON parsed_properties(stats_time_given DESC NULLS LAST)",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_my_objects ON parsed_properties(stats_agent_given, stats_time_given DESC NULLS LAST, vitrina_id DESC) WHERE stats_agent_given IS NOT NULL",
                        "CREATE INDEX IF NOT EXISTS idx_parsed_properties_archive ON parsed_properties(stats_object_status, krisha_id) WHERE krisha_id IS NOT NULL AND krisha_id != '' AND (stats_object_status IS NULL OR stats_object_status != 'Архив')"
                    ]
                    for stmt in index_statements:
                        await session.execute(text(stmt))

                    await session.commit()
                    logger.info("Таблица parsed_properties создана вместе с индексами")

                # Проверяем наличие колонки stats_object_category
                col_check = await session.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'parsed_properties'
                      AND column_name = 'stats_object_category'
                """))
                column_exists = col_check.fetchone() is not None

                if not column_exists:
                    logger.info("Добавляю колонку stats_object_category в parsed_properties...")
                    await session.execute(text("""
                        ALTER TABLE parsed_properties 
                        ADD COLUMN IF NOT EXISTS stats_object_category VARCHAR(10)
                    """))
                    await session.commit()
                    logger.info("Колонка stats_object_category успешно добавлена в parsed_properties")
                else:
                    logger.info("Колонка stats_object_category уже существует в parsed_properties")

        except Exception as e:
            logger.error(f"Ошибка ensure_parsed_properties_schema: {e}", exc_info=True)
    
    async def get_agent_contracts_page(self, agent_name: str, page: int = 1, page_size: int = 10, role: Optional[str] = None) -> Tuple[List[Dict], int]:
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
                
                logger.debug(f"Поиск контрактов для агента: '{agent_name}' -> фамилия: '{surname}', имя: '{name}'")

                # WHERE через SQLAlchemy Core
                def _role_condition(role):
                    if role == 'МОП':
                        return and_(func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like))
                    if role == 'РОП':
                        return and_(func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like))
                    if role == 'ДД':
                        return and_(func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like))
                    return or_(
                        and_(func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like)),
                        and_(func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like)),
                        and_(func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like)),
                    )
                
                def _where(role):
                    return and_(_role_condition(role), ACTIVE_STATUS_FILTER)

                # count
                stmt_count = select(func.count()).select_from(properties).where(_where(role))
                total_count = (await session.execute(stmt_count)).scalar() or 0

                # page
                stmt_page = (
                    select(properties)
                    .where(_where(role))
                    .order_by(properties.c.last_modified_at.desc())
                    .limit(page_size)
                    .offset(offset)
                )
                result = await session.execute(stmt_page)
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    # Преобразуем в формат, совместимый со старым API
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                logger.info(f"Загружено {len(contracts)} контрактов для агента {agent_name} (страница {page})")
                return contracts, total_count
                
        except Exception as e:
            logger.error(f"Ошибка получения контрактов агента {agent_name}: {e}", exc_info=True)
            return [], 0
    
    async def search_contract_by_crm_id(self, crm_id: str, agent_name: str, role: Optional[str] = None) -> Optional[Dict]:
        """Ищет контракт по CRM ID для конкретного агента"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(agent_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                # WHERE через Core
                def _where(role):
                    base = (properties.c.crm_id == crm_id)
                    if role == 'МОП':
                        return and_(base, func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like))
                    if role == 'РОП':
                        return and_(base, func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like))
                    if role == 'ДД':
                        return and_(base, func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like))
                    return and_(
                        base,
                        or_(
                            and_(func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like)),
                            and_(func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like)),
                            and_(func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like)),
                        )
                    )
                stmt = select(properties).where(_where(role))
                result = await session.execute(stmt)
                
                row = result.fetchone()
                if row:
                    contract_dict = dict(row._mapping)
                    return self._convert_to_legacy_format(contract_dict)
                
                return None
                
        except Exception as e:
            logger.error(f"Ошибка поиска контракта {crm_id} для агента {agent_name}: {e}", exc_info=True)
            return None
    
    async def search_contracts_by_client_name_lazy(self, client_name: str, agent_name: str, page: int = 1, page_size: int = 10, role: Optional[str] = None) -> Tuple[List[Dict], int]:
        """Ищет контракты по имени клиента с пагинацией.

        Поведение:
        - Для ролей МОП/РОП/ДД — фильтрация по соответствующим полям (mop/rop/dd) и имени владельца.
        - Для роли ADMIN_VIEW — глобальный поиск по всей базе без ограничения по владельцу (только ACTIVE_STATUS_FILTER).
        - Для остальных случаев (role is None) — поиск по всем ролям, но в рамках указанного agent_name.
        """
        try:
            offset = (page - 1) * page_size
            
            async with self.async_session() as session:
                client_like = f"%{client_name}%"
                def _where(role_value: Optional[str]):
                    base = and_(
                        func.lower(properties.c.client_name).like(func.lower(client_like)),
                        ACTIVE_STATUS_FILTER,
                    )

                    # ADMIN_VIEW — глобальный поиск без ограничения по владельцу
                    if role_value == 'ADMIN_VIEW':
                        return base

                    # Для остальных ролей используем ФИО владельца, если оно есть
                    fio_parts = [p for p in str(agent_name).strip().split() if p]
                    surname = fio_parts[0] if fio_parts else ''
                    name = fio_parts[1] if len(fio_parts) > 1 else ''
                    surname_like = f"%{surname.lower()}%"
                    name_like = f"%{name.lower()}%"

                    if role_value == 'МОП':
                        return and_(base, func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like))
                    if role_value == 'РОП':
                        return and_(base, func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like))
                    if role_value == 'ДД':
                        return and_(base, func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like))
                    return and_(
                        base,
                        or_(
                            and_(func.lower(properties.c.mop).like(surname_like), func.lower(properties.c.mop).like(name_like)),
                            and_(func.lower(properties.c.rop).like(surname_like), func.lower(properties.c.rop).like(name_like)),
                            and_(func.lower(properties.c.dd).like(surname_like), func.lower(properties.c.dd).like(name_like)),
                        )
                    )

                stmt_count = select(func.count()).select_from(properties).where(_where(role))
                total_count = (await session.execute(stmt_count)).scalar() or 0
                stmt_page = (
                    select(properties)
                    .where(_where(role))
                    .order_by(properties.c.last_modified_at.desc())
                    .limit(page_size).offset(offset)
                )
                result = await session.execute(stmt_page)
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                logger.info(f"Найдено {len(contracts)} контрактов для клиента '{client_name}' агента {agent_name}")
                return contracts, total_count
                
        except Exception as e:
            logger.error(f"Ошибка поиска контрактов по клиенту {client_name}: {e}", exc_info=True)
            return [], 0
    
    async def update_contract_category(self, crm_id: str, category: str) -> bool:
        """Обновляет категорию контракта"""
        try:
            async with self.async_session() as session:
                # Проверяем существование контракта
                check_query = text("SELECT crm_id FROM properties WHERE crm_id = :crm_id")
                result = await session.execute(check_query, {"crm_id": crm_id})
                
                if not result.fetchone():
                    logger.warning(f"Контракт {crm_id} не найден для обновления категории")
                    return False
                
                # Обновляем категорию (поддерживаем как латиницу, так и кириллицу)
                category_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                category_cyr = category_mapping.get(category.upper(), category.upper())
                
                update_query = text("""
                    UPDATE properties 
                    SET category = :category, 
                        last_modified_by = 'BOT',
                        last_modified_at = :now
                    WHERE crm_id = :crm_id
                """)
                
                await session.execute(
                    update_query,
                    {
                        "category": category_cyr,
                        "crm_id": crm_id,
                        "now": datetime.now()
                    }
                )
                
                await session.commit()
                logger.info(f"Категория контракта {crm_id} изменена на {category_cyr}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка обновления категории контракта {crm_id}: {e}")
            return False

    async def update_contract(self, crm_id: str, updates: Dict[str, Any]) -> bool:
        """Обновляет контракт в базе данных"""
        try:
            async with self.async_session() as session:
                # Проверяем, существует ли контракт
                result = await session.execute(select(properties.c.crm_id).where(properties.c.crm_id == crm_id))
                
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
                
                # Выполняем обновление через Core
                upd = (
                    update(properties)
                    .where(properties.c.crm_id == crm_id)
                    .values(**update_data)
                )
                await session.execute(upd)
                
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
            
            # Ищем агента в базе данных по номеру телефона
            async with self.async_session() as session:
                # Ищем в полях mop, rop, dd по номеру телефона
                result = await session.execute(
                    text("SELECT DISTINCT mop, rop, dd FROM properties WHERE "
                         "LOWER(mop) LIKE LOWER(:phone) OR "
                         "LOWER(rop) LIKE LOWER(:phone) OR "
                         "LOWER(dd) LIKE LOWER(:phone)"),
                    {"phone": f"%{normalized_phone}%"}
                )
                
                for row in result.fetchall():
                    # Проверяем каждое поле на совпадение номера
                    for field in [row.mop, row.rop, row.dd]:
                        if field and normalized_phone in field:
                            logger.info(f"Найден агент: {field} с номером {normalized_phone}")
                            return field
            
            logger.warning(f"Агент с номером {normalized_phone} не найден")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска агента по телефону {phone}: {e}", exc_info=True)
            return None
    
    async def get_phone_by_agent(self, agent_name: str) -> Optional[str]:
        """Получает номер телефона агента по имени"""
        try:
            # Ищем агента в базе данных по имени
            async with self.async_session() as session:
                # Ищем в полях mop, rop, dd по имени агента
                result = await session.execute(
                    text("SELECT DISTINCT mop, rop, dd FROM properties WHERE "
                         "LOWER(mop) LIKE LOWER(:agent_name) OR "
                         "LOWER(rop) LIKE LOWER(:agent_name) OR "
                         "LOWER(dd) LIKE LOWER(:agent_name)"),
                    {"agent_name": f"%{agent_name.strip()}%"}
                )
                
                for row in result.fetchall():
                    # Проверяем каждое поле на совпадение имени
                    for field in [row.mop, row.rop, row.dd]:
                        if field and agent_name.strip().lower() in field.lower():
                            # Извлекаем номер телефона из поля (предполагаем, что номер в конце)
                            import re
                            phone_match = re.search(r'\b[78]\d{10}\b', field)
                            if phone_match:
                                return phone_match.group()
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска телефона агента {agent_name}: {e}", exc_info=True)
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
            'area': db_record.get('area'),
            'rooms_count': db_record.get('rooms_count'),
            'krisha_price': db_record.get('krisha_price'),
            'vitrina_price': db_record.get('vitrina_price'),
            'score': db_record.get('score'),
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

    async def automate_categories(self) -> Dict[str, int]:
        """Массово пересчитывает категории на основе данных третьего листа ("Лист8") и API.

        Возвращает статистику: updated/skipped/errors.
        """
        # 1) Инициализируем Google Sheets
        credentials_file = 'credentials.json'
        if not os.path.exists(credentials_file):
            raise ValueError(f"Файл {credentials_file} не найден")
        if not SHEET_ID or not THIRD_SHEET_GID:
            raise ValueError("Переменные окружения SHEET_ID или THIRD_SHEET_GID не установлены")

        credentials = Credentials.from_service_account_file(
            credentials_file,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(SHEET_ID)
        third_ws = spreadsheet.get_worksheet_by_id(int(THIRD_SHEET_GID))

        # 2) Читаем и находим индексы нужных колонок (ЖК=A, Крыша=B, Общий балл=C, Витрина=D)
        rows = third_ws.get_all_values()
        if not rows:
            return {"updated": 0, "skipped": 0, "errors": 0}

        # Поиск строки заголовков: ищем строку, где есть «ЖК» и/или нужные названия
        header_row_idx = 0
        for i, r in enumerate(rows):
            line = ' '.join(r).lower()
            if ('жк' in line) or ('крыша' in line) or ('витрина' in line) or ('общий балл' in line):
                header_row_idx = i
                break

        def idx_by_name(header: List[str], names: List[str], default: Optional[int] = None) -> Optional[int]:
            header_low = [h.strip().lower() for h in header]
            for name in names:
                if name.lower() in header_low:
                    return header_low.index(name.lower())
            return default

        header = rows[header_row_idx] if rows else []
        # Жёсткие индексы по условию: A=ЖК, B=Крыша, C=Общий балл, D=Витрина — с fallback по названиям
        complex_col = 0 if len(header) >= 1 else idx_by_name(header, ['жк', 'комплекс'])
        roof_col = 1 if len(header) >= 2 else idx_by_name(header, ['крыша', 'roof'])
        score_col = 2 if len(header) >= 3 else idx_by_name(header, ['общий балл', 'балл', 'score'])
        window_col = 3 if len(header) >= 4 else idx_by_name(header, ['витрина', 'window'])

        # Если индексы не определены — считаем A/B/C/D по фиксированным позициям
        if complex_col is None:
            complex_col = 0
        if roof_col is None:
            roof_col = 1
        if score_col is None:
            score_col = 2
        if window_col is None:
            window_col = 3

        def to_float_safe(v):
            try:
                s = str(v).replace(' ', '').replace('\u00A0', '')
                s = s.replace(',', '.')
                if s.strip() == '':
                    return None
                return float(s)
            except Exception:
                return None

        # Строим словарь по названию ЖК (complex)
        complex_to_params: Dict[str, Dict[str, Optional[float]]] = {}
        def norm_complex(x: str) -> str:
            import re
            s = (x or '').lower()
            # Базовые заменители и удаление служебных слов
            for token in ['жк', 'жилой комплекс', 'residence', 'residential', 'complex']:
                s = s.replace(token, ' ')
            # Заменяем разделители на пробел
            for ch in ['"', '\'', '«', '»', '.', ',', ';', ':', '(', ')', '[', ']', '{', '}', '/', '\\', '-', '–', '_']:
                s = s.replace(ch, ' ')
            # Удаляем конструкции вида "блок X" и слова-артефакты
            s = re.sub(r"\bблок\s+[a-zа-я0-9]+\b", " ", s)
            s = re.sub(r"\bочередь\b", " ", s)
            # Схлопываем числовые хвосты вида "2-1" -> "2"
            s = re.sub(r"\b(\d+)\s*\-\s*\d+\b", r"\1", s)
            # Нормализуем множественные пробелы
            s = ' '.join(s.split())
            # Токен-уровневая нормализация (транслитерации и синонимы)
            synonyms = {
                'buqar': 'бухар', 'bukhar': 'бухар', 'buqarjyrau': 'бухаржырау', 'jyrau': 'жырау',
                'qalashyq': 'калашык', 'qalashy': 'калашык', 'qurylys': 'курылыс', 'exclusive': 'эксклюзив',
                'bukhar': 'бухар', 'jyray': 'жырау', 'dauletti': 'даулетти', 'qalashyk': 'калашык',
                'city': 'city', 'sat': 'sat'
            }
            tokens = s.split()
            norm_tokens = []
            for t in tokens:
                t_clean = t
                if t in synonyms:
                    t_clean = synonyms[t]
                norm_tokens.append(t_clean)
            return ' '.join(norm_tokens)

        def norm_tokens_set(s: str) -> set:
            return set(norm_complex(s).split())
        for i, r in enumerate(rows):
            if i <= header_row_idx:
                continue
            complex_name = (r[complex_col] if complex_col < len(r) else '').strip()
            if not complex_name:
                continue
            roof_raw = r[roof_col] if roof_col < len(r) else ''
            score_raw = r[score_col] if score_col < len(r) else ''
            window_raw = r[window_col] if window_col < len(r) else ''
            complex_to_params[norm_complex(complex_name)] = {
                'roof': to_float_safe(roof_raw),
                'score': to_float_safe(score_raw),
                'window': to_float_safe(window_raw),
            }

        # 3) Берём все crm_id, contract_price и complex из БД
        async with self.async_session() as session:
            res = await session.execute(text("SELECT crm_id, contract_price, complex FROM properties"))
            db_rows = [dict(r._mapping) for r in res.fetchall()]

        # 4) Подготавливаем функцию assign_category (максимально близко к предоставленной логике)
        def is_num(x) -> bool:
            return isinstance(x, (int, float)) and x is not None

        def assign_category(contract_price: Optional[float], window_price: Optional[float], roof_price: Optional[float], score: Optional[float]) -> str:
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
                # Если L или M пустые
                if (window_price is None) or (roof_price is None):
                    if is_num(score) and (score > 8):
                        return 'A'
                    elif is_num(score) and (5 <= score <= 8):
                        return 'B'
            return 'C'

        # 5) Подбор ближайшего совпадения по токенам (если прямого ключа нет)
        updated, skipped, errors = 0, 0, 0
        # Вспомогательная: подобрать ближайшее совпадение по токенам, если прямого ключа нет
        def find_best_match(norm_name: str) -> Optional[str]:
            name_set = set(norm_name.split())
            if not name_set:
                return None
            best_key, best_score = None, 0.0
            for k in complex_to_params.keys():
                k_set = set(k.split())
                if not k_set:
                    continue
                inter = len(name_set & k_set)
                union = len(name_set | k_set)
                score = inter / union if union else 0.0
                # Правило подмножества: если меньшее множество полностью входит в большее — считаем полноценным совпадением
                smaller, bigger = (name_set, k_set) if len(name_set) <= len(k_set) else (k_set, name_set)
                if smaller and smaller.issubset(bigger):
                    score = max(score, 0.999)
                if score > best_score:
                    best_score, best_key = score, k
            return best_key if best_score >= 0.45 else None

        def find_by_variants(raw_name: str) -> Optional[str]:
            import re
            base = norm_complex(raw_name)
            parts = base.split()
            if not parts:
                return None
            # Функция проверки: все части должны встречаться как подстроки в ключе
            def all_parts_in_key(parts_list: List[str], key: str) -> bool:
                for p in parts_list:
                    if p and p not in key:
                        return False
                return True
            # Генерируем варианты, постепенно укорачивая хвост и убирая числовые токены
            for cut in range(0, len(parts)):
                variant_parts = parts[:len(parts) - cut]
                # Убираем чисто числовые части и формы вида 1-2 (остаётся первая часть — уже нормализовано)
                variant_parts = [p for p in variant_parts if not p.isdigit()]
                if not variant_parts:
                    continue
                variant_str = ' '.join(variant_parts)
                # 1) Прямое попадание ключа
                if variant_str in complex_to_params:
                    return variant_str
                # 2) Поиск по включению всех частей
                for key in complex_to_params.keys():
                    if all_parts_in_key(variant_parts, key):
                        return key
            return None

        # 6) Готовим карту площадей через пакетные запросы к API
        # Сначала отберём записи, у которых есть параметры из листа (после нормализации/подбора)
        rows_prepared = []
        not_found_logged = 0
        unmatched_rows: List[Dict[str, Any]] = []
        for row in db_rows:
            complex_name_db = str(row.get('complex') or '')
            complex_key = norm_complex(complex_name_db)
            sheet_params = complex_to_params.get(complex_key)
            if not sheet_params:
                # сначала быстрый матч по вариантам укорачивания
                best = find_by_variants(complex_name_db)
                if not best:
                    best = find_best_match(complex_key)
                    if best:
                            sheet_params = complex_to_params.get(best)
            if not sheet_params:
                if not_found_logged < 15:
                    sample_keys = list(complex_to_params.keys())[:5]
                    logger.info("Не найдено соответствие ЖК: '%s' (норм: '%s'). Примеры ключей листа: %s",
                                complex_name_db, complex_key, sample_keys)
                    not_found_logged += 1
                unmatched_rows.append(row)
                continue
            rows_prepared.append((row, sheet_params))

        area_by_crm: Dict[str, Optional[float]] = {}
        try:
            async with APIClient() as api_client:
                crm_ids = [str(r[0].get('crm_id')) for r in rows_prepared if r[0].get('crm_id')]
                if crm_ids:
                    crm_data = await api_client.get_crm_data_batch(crm_ids, batch_size=DB_BATCH_SIZE)
                    for cid, data in crm_data.items():
                        area_val = data.get('area')
                        try:
                            area_by_crm[cid] = float(area_val) if area_val is not None else None
                        except Exception:
                            area_by_crm[cid] = None
                # Второй проход: для несопоставленных возьмём complex из API и попробуем матчинг
                unmatched_ids = [str(r.get('crm_id')) for r in unmatched_rows if r.get('crm_id')]
                if unmatched_ids:
                    api_batch = await api_client.get_crm_data_batch(unmatched_ids, batch_size=150)
                    api_unmatched_names_logged = set()
                    for cid, data in api_batch.items():
                        api_complex = str((data or {}).get('complex') or '')
                        api_key = norm_complex(api_complex)
                        sp = complex_to_params.get(api_key)
                        if not sp:
                            best2 = find_best_match(api_key)
                            if best2:
                                sp = complex_to_params.get(best2)
                                logger.info("ЖК (API) сопоставлен по похожести: '%s' -> '%s'", api_complex, best2)
                        if sp:
                            # добавляем в подготовленные
                            for row in unmatched_rows:
                                if str(row.get('crm_id')) == cid:
                                    rows_prepared.append((row, sp))
                                    break
                        else:
                            if len(api_unmatched_names_logged) < 15 and api_key not in api_unmatched_names_logged:
                                logger.info("Не найдено соответствие ЖК (по API complex): '%s'", api_complex)
                                api_unmatched_names_logged.add(api_key)
        except Exception as e:
            logger.error(f"Ошибка пакетного получения площадей: {e}")

        # Подсчитываем пропуски после двух проходов
        prepared_ids = {str(r[0].get('crm_id')) for r in rows_prepared if r[0].get('crm_id')}
        all_ids = {str(r.get('crm_id')) for r in db_rows if r.get('crm_id')}
        skipped = len(all_ids - prepared_ids)

        # 7) Проходим по подготовленным строкам и обновляем
        sample_logged = 0
        for row, sheet_params in rows_prepared:
            try:
                crm_id = str(row.get('crm_id') or '').strip()
                complex_name_db = str(row.get('complex') or '')
                contract_price = row.get('contract_price')
                roof = sheet_params.get('roof')
                window = sheet_params.get('window')
                score = sheet_params.get('score')

                # Площадь из предварительно загруженной карты
                area = area_by_crm.get(crm_id)

                window_price = (window * area) if (window is not None and area is not None) else None
                roof_price = (roof * area) if (roof is not None and area is not None) else None

                category = assign_category(contract_price, window_price, roof_price, score)

                    # Убрано подробное логирование параметров
                ok = await self.update_contract_category(crm_id, category)
                if ok:
                    updated += 1
                else:
                    errors += 1
            except Exception as e:
                logger.error(f"Ошибка automate_categories для {row}: {e}")
                errors += 1

        return {"updated": updated, "skipped": skipped, "errors": errors}

    async def automate_categories_missing_only(self) -> Dict[str, int]:
        """Пересчитывает категории только для объектов с пустой category в SQL.

        Логика идентична automate_categories, но набор записей ограничен записями,
        у которых category IS NULL или пустая строка.
        """
        # 1) Инициализация Google Sheets
        credentials_file = 'credentials.json'
        if not os.path.exists(credentials_file):
            raise ValueError(f"Файл {credentials_file} не найден")
        if not SHEET_ID or not THIRD_SHEET_GID:
            raise ValueError("Переменные окружения SHEET_ID или THIRD_SHEET_GID не установлены")

        credentials = Credentials.from_service_account_file(
            credentials_file,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(SHEET_ID)
        third_ws = spreadsheet.get_worksheet_by_id(int(THIRD_SHEET_GID))

        rows = third_ws.get_all_values()
        if not rows:
            return {"updated": 0, "skipped": 0, "errors": 0}

        header_row_idx = 0
        for i, r in enumerate(rows):
            line = ' '.join(r).lower()
            if ('жк' in line) or ('крыша' in line) or ('витрина' in line) or ('общий балл' in line):
                header_row_idx = i
                break

        def idx_by_name(header: List[str], names: List[str], default: Optional[int] = None) -> Optional[int]:
            header_low = [h.strip().lower() for h in header]
            for name in names:
                if name.lower() in header_low:
                    return header_low.index(name.lower())
            return default

        header = rows[header_row_idx] if rows else []
        complex_col = 0 if len(header) >= 1 else idx_by_name(header, ['жк', 'комплекс'])
        roof_col = 1 if len(header) >= 2 else idx_by_name(header, ['крыша', 'roof'])
        score_col = 2 if len(header) >= 3 else idx_by_name(header, ['общий балл', 'балл', 'score'])
        window_col = 3 if len(header) >= 4 else idx_by_name(header, ['витрина', 'window'])

        if complex_col is None:
            complex_col = 0
        if roof_col is None:
            roof_col = 1
        if score_col is None:
            score_col = 2
        if window_col is None:
            window_col = 3

        def to_float_safe(v):
            try:
                s = str(v).replace(' ', '').replace('\u00A0', '')
                s = s.replace(',', '.')
                if s.strip() == '':
                    return None
                return float(s)
            except Exception:
                return None

        def norm_complex(x: str) -> str:
            s = (x or '').lower()
            for token in ['жк', 'жилой комплекс', 'residence', 'residential', 'complex']:
                s = s.replace(token, ' ')
            for ch in ['"', '\'', '«', '»', '.', ',', ';', ':', '(', ')', '[', ']', '{', '}', '/', '\\', '-']:
                s = s.replace(ch, ' ')
            s = ' '.join(s.split())
            return s

        complex_to_params: Dict[str, Dict[str, Optional[float]]] = {}
        for i, r in enumerate(rows):
            if i <= header_row_idx:
                continue
            complex_name = (r[complex_col] if complex_col < len(r) else '').strip()
            if not complex_name:
                continue
            roof_raw = r[roof_col] if roof_col < len(r) else ''
            score_raw = r[score_col] if score_col < len(r) else ''
            window_raw = r[window_col] if window_col < len(r) else ''
            key_main = norm_complex(complex_name)
            complex_to_params[key_main] = {
                'roof': to_float_safe(roof_raw),
                'score': to_float_safe(score_raw),
                'window': to_float_safe(window_raw),
            }
            # Добавляем облегчённый ключ без хвостов вида 2-1
            import re
            key_variant = re.sub(r"\b(\d+)\s*\-\s*\d+\b", r"\1", key_main)
            if key_variant != key_main and key_variant not in complex_to_params:
                complex_to_params[key_variant] = complex_to_params[key_main]

        # Грузим только записи без категории
        async with self.async_session() as session:
            res = await session.execute(text(
                "SELECT crm_id, contract_price, complex FROM properties WHERE category IS NULL OR TRIM(category) = ''"
            ))
            db_rows = [dict(r._mapping) for r in res.fetchall()]

        def is_num(x) -> bool:
            return isinstance(x, (int, float)) and x is not None

        def assign_category(contract_price: Optional[float], window_price: Optional[float], roof_price: Optional[float], score: Optional[float]) -> str:
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

        def find_best_match(norm_name: str) -> Optional[str]:
            name_set = set(norm_name.split())
            if not name_set:
                return None
            best_key, best_score = None, 0.0
            for k in complex_to_params.keys():
                k_set = set(k.split())
                if not k_set:
                    continue
                inter = len(name_set & k_set)
                union = len(name_set | k_set)
                score = inter / union if union else 0.0
                if score > best_score:
                    best_score, best_key = score, k
            return best_key if best_score >= 0.45 else None

        def find_by_variants(raw_name: str) -> Optional[str]:
            import re
            base = norm_complex(raw_name)
            parts = base.split()
            if not parts:
                return None
            def all_parts_in_key(parts_list: List[str], key: str) -> bool:
                for p in parts_list:
                    if p and p not in key:
                        return False
                return True
            for cut in range(0, len(parts)):
                variant_parts = parts[:len(parts) - cut]
                variant_parts = [p for p in variant_parts if not p.isdigit()]
                if not variant_parts:
                    continue
                variant_str = ' '.join(variant_parts)
                if variant_str in complex_to_params:
                    return variant_str
                for key in complex_to_params.keys():
                    if all_parts_in_key(variant_parts, key):
                        return key
            return None

        # Первый проход сопоставления
        rows_prepared: List[Tuple[Dict[str, Any], Dict[str, Optional[float]]]] = []
        unmatched_rows: List[Dict[str, Any]] = []
        not_found_logged = 0
        for row in db_rows:
            complex_name_db = str(row.get('complex') or '')
            complex_key = norm_complex(complex_name_db)
            sheet_params = complex_to_params.get(complex_key)
            if not sheet_params:
                best = find_by_variants(complex_name_db)
                if not best:
                    best = find_best_match(complex_key)
                if best:
                    sheet_params = complex_to_params.get(best)
                    logger.info("ЖК сопоставлен по похожести: '%s' -> '%s'", complex_name_db, best)
            if not sheet_params:
                if not_found_logged < 15:
                    sample_keys = list(complex_to_params.keys())[:5]
                    logger.info("Не найдено соответствие ЖК: '%s' (норм: '%s'). Примеры ключей листа: %s",
                                complex_name_db, complex_key, sample_keys)
                    not_found_logged += 1
                unmatched_rows.append(row)
                continue
            rows_prepared.append((row, sheet_params))

        area_by_crm: Dict[str, Optional[float]] = {}
        try:
            async with APIClient() as api_client:
                # Площади для уже сопоставленных
                crm_ids = [str(r[0].get('crm_id')) for r in rows_prepared if r[0].get('crm_id')]
                if crm_ids:
                    crm_data = await api_client.get_crm_data_batch(crm_ids, batch_size=DB_BATCH_SIZE)
                    for cid, data in crm_data.items():
                        area_val = (data or {}).get('area')
                        try:
                            area_by_crm[cid] = float(area_val) if area_val is not None else None
                        except Exception:
                            area_by_crm[cid] = None

                # Второй проход по API отключен: берём имя ЖК только из SQL
        except Exception as e:
            logger.error(f"Ошибка пакетного получения площадей: {e}")

        # Подсчитываем пропуски
        prepared_ids = {str(r[0].get('crm_id')) for r in rows_prepared if r[0].get('crm_id')}
        all_ids = {str(r.get('crm_id')) for r in db_rows if r.get('crm_id')}
        skipped = len(all_ids - prepared_ids)

        # Обновляем категории
        updated, errors = 0, 0
        sample_logged = 0
        for row, sheet_params in rows_prepared:
            try:
                crm_id = str(row.get('crm_id') or '').strip()
                complex_name_db = str(row.get('complex') or '')
                contract_price = row.get('contract_price')
                roof = sheet_params.get('roof')
                window = sheet_params.get('window')
                score = sheet_params.get('score')
                area = area_by_crm.get(crm_id)
                window_price = (window * area) if (window is not None and area is not None) else None
                roof_price = (roof * area) if (roof is not None and area is not None) else None
                category = assign_category(contract_price, window_price, roof_price, score)
                if sample_logged < 15:
                    logger.info(
                        "assign_category_2 params | crm_id=%s | complex='%s' | contract_price=%s | area=%s | roof=%s | window=%s | score=%s | window_price=%s | roof_price=%s | result=%s",
                        crm_id, complex_name_db, contract_price, area, roof, window, score, window_price, roof_price, category
                    )
                    sample_logged += 1
                ok = await self.update_contract_category(crm_id, category)
                if ok:
                    updated += 1
                else:
                    errors += 1
            except Exception as e:
                logger.error(f"Ошибка automate_categories_missing_only для {row}: {e}")
                errors += 1

        return {"updated": updated, "skipped": skipped, "errors": errors}

    async def get_role_totals(self, owner_name: str, owner_role: str) -> Dict[str, int]:
        """Сводные показатели по объектам для владельца роли (РОП/ДД)."""
        role_col = 'rop' if owner_role == 'РОП' else 'dd'
        fio_parts = [p for p in str(owner_name).strip().split() if p]
        surname = fio_parts[0] if fio_parts else ''
        name = fio_parts[1] if len(fio_parts) > 1 else ''
        surname_like = f"%{surname.lower()}%"
        name_like = f"%{name.lower()}%"
        try:
            async with self.async_session() as session:
                col = properties.c.rop if owner_role == 'РОП' else properties.c.dd
                cond = and_(
                    func.lower(col).like(surname_like),
                    func.lower(col).like(name_like),
                    ACTIVE_STATUS_FILTER
                )
                total = (await session.execute(select(func.count()).select_from(properties).where(cond))).scalar() or 0
                cat_rows = (await session.execute(
                    select(properties.c.category, func.count().label('cnt')).where(cond).group_by(properties.c.category)
                )).fetchall()
                cats = { (row.category or '').strip().upper(): row.cnt for row in cat_rows }
                return {
                    'total': total,
                    'cat_A': cats.get('А', 0) + cats.get('A', 0),
                    'cat_B': cats.get('В', 0) + cats.get('B', 0),
                    'cat_C': cats.get('С', 0) + cats.get('C', 0),
                }
        except Exception as e:
            logger.error(f"Ошибка get_role_totals({owner_name}, {owner_role}): {e}")
            return {'total': 0, 'cat_A': 0, 'cat_B': 0, 'cat_C': 0}

    async def get_dds_with_counts(self) -> List[Dict[str, Any]]:
        """Возвращает всех ДД (ФИО + количество объектов). Используется в ADMIN_VIEW."""
        try:
            async with self.async_session() as session:
                res = await session.execute(text(
                    "SELECT dd AS name, COUNT(*) AS cnt "
                    "FROM properties "
                    "WHERE dd IS NOT NULL AND dd <> '' "
                    "GROUP BY dd "
                    "ORDER BY cnt DESC NULLS LAST"
                ))
                items: List[Dict[str, Any]] = []
                for row in res.fetchall():
                    items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка get_dds_with_counts(): {e}")
            return []

    async def get_dd_contracts_by_category(self, dd_name: str, category: Optional[str] = None) -> List[Dict]:
        """Получает все объекты конкретного ДД с фильтрацией по категории (для ADMIN_VIEW и меню ДД)."""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(dd_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"

                where_clause = "(LOWER(dd) LIKE :surname_like AND LOWER(dd) LIKE :name_like)"
                params = {"surname_like": surname_like, "name_like": name_like}

                if category:
                    cat_upper = category.upper()
                    cat_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                    cat_cyr = cat_mapping.get(cat_upper, cat_upper)
                    where_clause += " AND (UPPER(category) = :cat OR UPPER(category) = :cat_cyr)"
                    params['cat'] = cat_upper
                    params['cat_cyr'] = cat_cyr

                status_filter_sql = " AND (status IS NULL OR LOWER(status) != 'реализовано')"
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE {where_clause}{status_filter_sql} ORDER BY last_modified_at DESC"),
                    params
                )

                contracts: List[Dict] = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                return contracts
        except Exception as e:
            logger.error(f"Ошибка get_dd_contracts_by_category({dd_name}, {category}): {e}")
            return []

    async def get_all_mops_with_counts(self) -> List[Dict[str, Any]]:
        """Возвращает всех МОП-ов (ФИО + количество объектов) по всей базе. Используется в ADMIN_VIEW."""
        try:
            async with self.async_session() as session:
                res = await session.execute(text(
                    "SELECT mop AS name, COUNT(*) AS cnt "
                    "FROM properties "
                    "WHERE mop IS NOT NULL AND mop <> '' "
                    "GROUP BY mop "
                    "ORDER BY cnt DESC NULLS LAST"
                ))
                items: List[Dict[str, Any]] = []
                for row in res.fetchall():
                    items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка get_all_mops_with_counts(): {e}")
            return []

    async def get_global_totals(self) -> Dict[str, int]:
        """Глобальная статистика по объектам (все ДД/РОП/МОП) для ADMIN_VIEW."""
        try:
            async with self.async_session() as session:
                cond = ACTIVE_STATUS_FILTER
                total = (await session.execute(
                    select(func.count()).select_from(properties).where(cond)
                )).scalar() or 0
                cat_rows = (await session.execute(
                    select(properties.c.category, func.count().label('cnt')).where(cond).group_by(properties.c.category)
                )).fetchall()
                cats = { (row.category or '').strip().upper(): row.cnt for row in cat_rows }
                return {
                    'total': total,
                    'cat_A': cats.get('А', 0) + cats.get('A', 0),
                    'cat_B': cats.get('В', 0) + cats.get('B', 0),
                    'cat_C': cats.get('С', 0) + cats.get('C', 0),
                }
        except Exception as e:
            logger.error(f"Ошибка get_global_totals(): {e}")
            return {'total': 0, 'cat_A': 0, 'cat_B': 0, 'cat_C': 0}

    async def get_global_contracts_by_category(self, category: Optional[str] = None) -> List[Dict]:
        """Возвращает объекты по всей базе для ADMIN_VIEW, с опциональной фильтрацией по категории."""
        try:
            async with self.async_session() as session:
                where_clause = "1=1"
                params: Dict[str, Any] = {}
                if category:
                    cat_upper = category.upper()
                    cat_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                    cat_cyr = cat_mapping.get(cat_upper, cat_upper)
                    where_clause += " AND (UPPER(category) = :cat OR UPPER(category) = :cat_cyr)"
                    params['cat'] = cat_upper
                    params['cat_cyr'] = cat_cyr

                # Применяем фильтр по активным статусам
                status_filter_sql = " AND (status IS NULL OR LOWER(status) != 'реализовано')"
                query = text(
                    f"SELECT * FROM properties WHERE {where_clause}{status_filter_sql} ORDER BY last_modified_at DESC"
                )
                result = await session.execute(query, params)

                contracts: List[Dict] = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                return contracts
        except Exception as e:
            logger.error(f"Ошибка get_global_contracts_by_category({category}): {e}")
            return []

    async def get_subordinates(self, owner_name: str, owner_role: str, subordinate_role: str) -> List[Dict[str, Any]]:
        """Возвращает подчинённых (имя + количество объектов) для владельца роли.
        owner_role: 'РОП'|'ДД', subordinate_role: 'МОП'|'РОП'
        """
        owner_col = 'rop' if owner_role == 'РОП' else 'dd'
        sub_col = 'mop' if subordinate_role == 'МОП' else 'rop'
        fio_parts = [p for p in str(owner_name).strip().split() if p]
        surname = fio_parts[0] if fio_parts else ''
        name = fio_parts[1] if len(fio_parts) > 1 else ''
        surname_like = f"%{surname.lower()}%"
        name_like = f"%{name.lower()}%"
        try:
            async with self.async_session() as session:
                res = await session.execute(text(
                    f"SELECT {sub_col} AS name, COUNT(*) AS cnt FROM properties "
                    f"WHERE LOWER({owner_col}) LIKE :surname_like AND LOWER({owner_col}) LIKE :name_like "
                    f"GROUP BY {sub_col} ORDER BY cnt DESC NULLS LAST"
                ), {"surname_like": surname_like, "name_like": name_like})
                items = []
                for row in res.fetchall():
                    items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка get_subordinates({owner_name}, {owner_role}, {subordinate_role}): {e}")
            return []

    async def count_pending_tasks_for_owner(self, owner_name: str, owner_role: str) -> int:
        """Подсчитывает невыполненные задачи по всем объектам владельца роли.
        Для простоты грузим партии и считаем на Python, используя близкую логику UI.
        """
        try:
            # Грузим все записи постранично, чтобы не держать всё в памяти
            page = 1
            page_size = 200
            total_pending = 0
            while True:
                if owner_role == 'РОП':
                    where_role = 'РОП'
                else:
                    where_role = 'ДД'
                contracts, total_count = await self.get_agent_contracts_page(owner_name, page, page_size, where_role)
                if not contracts:
                    break
                from handlers import get_status_value, build_pending_tasks  # локальный импорт, чтобы избежать циклов при старте
                for c in contracts:
                    status_value = get_status_value(c)
                    analytics_mode_active = (status_value == 'Аналитика')
                    pending = build_pending_tasks(c, status_value, analytics_mode_active)
                    total_pending += len(pending)
                if page * page_size >= total_count:
                    break
                page += 1
            return total_pending
        except Exception as e:
            logger.error(f"Ошибка count_pending_tasks_for_owner({owner_name}, {owner_role}): {e}")
            return 0

    async def count_pending_tasks_for_mop(self, mop_name: str) -> int:
        """Подсчитывает невыполненные задачи у конкретного МОП-а через SQL
        Считает количество задач точно как build_pending_tasks:
        - Каждая отдельная задача считается отдельно
        - "Добавить ссылки" - это одна задача, даже если несколько ссылок отсутствует
        """
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(mop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                # SQL запрос для подсчета количества задач по логике build_pending_tasks
                query = text("""
                    WITH mop_contracts AS (
                        SELECT 
                            crm_id,
                            COALESCE(collage, false) as has_collage,
                            COALESCE(prof_collage, false) as has_prof_collage,
                            COALESCE(NULLIF(krisha, ''), '') as krisha,
                            COALESCE(NULLIF(instagram, ''), '') as instagram,
                            COALESCE(NULLIF(tiktok, ''), '') as tiktok,
                            COALESCE(NULLIF(mailing, ''), '') as mailing,
                            COALESCE(NULLIF(stream, ''), '') as stream,
                            COALESCE(status, 'Размещено') as status,
                            COALESCE(analytics, false) as has_analytics,
                            COALESCE(provide_analytics, false) as has_provide_analytics,
                            COALESCE(push_for_price, false) as has_push_for_price,
                            COALESCE(NULLIF(NULLIF(price_update, ''), 'None'), '') as price_update
                        FROM properties
                        WHERE LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like
                    )
                    SELECT 
                        -- Базовые задачи (только для статуса != 'Реализовано')
                        SUM(CASE WHEN status != 'Реализовано' AND NOT has_collage THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status != 'Реализовано' AND has_collage AND NOT has_prof_collage THEN 1 ELSE 0 END) +
                        -- "Добавить ссылки" - одна задача если хотя бы одна ссылка отсутствует
                        SUM(CASE WHEN status != 'Реализовано' AND (krisha = '' OR instagram = '' OR tiktok = '' OR mailing = '' OR stream = '') THEN 1 ELSE 0 END) +
                        -- Задачи для статуса "Аналитика"
                        SUM(CASE WHEN status = 'Аналитика' AND NOT has_analytics THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Аналитика' AND has_analytics AND NOT has_provide_analytics THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Аналитика' AND has_provide_analytics AND NOT has_push_for_price THEN 1 ELSE 0 END) +
                        -- Задачи для статуса "Корректировка цены"
                        SUM(CASE WHEN status = 'Корректировка цены' AND NOT has_push_for_price THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Корректировка цены' AND price_update = '' THEN 1 ELSE 0 END) +
                        -- "Добавить обновленные ссылки" - одна задача если хотя бы одна ссылка отсутствует
                        SUM(CASE WHEN status = 'Корректировка цены' AND (krisha = '' OR instagram = '' OR tiktok = '' OR mailing = '' OR stream = '') THEN 1 ELSE 0 END) +
                        -- Задача на смену статуса (если все базовые задачи выполнены, но статус не финальный)
                        SUM(CASE WHEN status NOT IN ('Реализовано', 'Аналитика', 'Корректировка цены', 'Задаток/сделка')
                                 AND has_collage AND has_prof_collage
                                 AND krisha != '' AND instagram != '' AND tiktok != '' AND mailing != '' AND stream != '' THEN 1 ELSE 0 END) as total_tasks
                    FROM mop_contracts
                """)
                
                result = await session.execute(query, {"surname_like": surname_like, "name_like": name_like})
                count = result.scalar() or 0
                
                return count
        except Exception as e:
            logger.error(f"Ошибка count_pending_tasks_for_mop({mop_name}): {e}")
            return 0

    async def count_pending_tasks_for_rop(self, rop_name: str) -> int:
        """Подсчитывает невыполненные задачи у конкретного РОП-а через SQL"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(rop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                # SQL запрос для подсчета количества задач по логике build_pending_tasks
                query = text("""
                    WITH rop_contracts AS (
                        SELECT 
                            crm_id,
                            COALESCE(collage, false) as has_collage,
                            COALESCE(prof_collage, false) as has_prof_collage,
                            COALESCE(NULLIF(krisha, ''), '') as krisha,
                            COALESCE(NULLIF(instagram, ''), '') as instagram,
                            COALESCE(NULLIF(tiktok, ''), '') as tiktok,
                            COALESCE(NULLIF(mailing, ''), '') as mailing,
                            COALESCE(NULLIF(stream, ''), '') as stream,
                            COALESCE(status, 'Размещено') as status,
                            COALESCE(analytics, false) as has_analytics,
                            COALESCE(provide_analytics, false) as has_provide_analytics,
                            COALESCE(push_for_price, false) as has_push_for_price,
                            COALESCE(NULLIF(NULLIF(price_update, ''), 'None'), '') as price_update
                        FROM properties
                        WHERE LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like
                    )
                    SELECT 
                        -- Базовые задачи (только для статуса != 'Реализовано')
                        SUM(CASE WHEN status != 'Реализовано' AND NOT has_collage THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status != 'Реализовано' AND has_collage AND NOT has_prof_collage THEN 1 ELSE 0 END) +
                        -- "Добавить ссылки" - одна задача если хотя бы одна ссылка отсутствует
                        SUM(CASE WHEN status != 'Реализовано' AND (krisha = '' OR instagram = '' OR tiktok = '' OR mailing = '' OR stream = '') THEN 1 ELSE 0 END) +
                        -- Задачи для статуса "Аналитика"
                        SUM(CASE WHEN status = 'Аналитика' AND NOT has_analytics THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Аналитика' AND has_analytics AND NOT has_provide_analytics THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Аналитика' AND has_provide_analytics AND NOT has_push_for_price THEN 1 ELSE 0 END) +
                        -- Задачи для статуса "Корректировка цены"
                        SUM(CASE WHEN status = 'Корректировка цены' AND NOT has_push_for_price THEN 1 ELSE 0 END) +
                        SUM(CASE WHEN status = 'Корректировка цены' AND price_update = '' THEN 1 ELSE 0 END) +
                        -- "Добавить обновленные ссылки" - одна задача если хотя бы одна ссылка отсутствует
                        SUM(CASE WHEN status = 'Корректировка цены' AND (krisha = '' OR instagram = '' OR tiktok = '' OR mailing = '' OR stream = '') THEN 1 ELSE 0 END) +
                        -- Задача на смену статуса (если все базовые задачи выполнены, но статус не финальный)
                        SUM(CASE WHEN status NOT IN ('Реализовано', 'Аналитика', 'Корректировка цены', 'Задаток/сделка')
                                 AND has_collage AND has_prof_collage
                                 AND krisha != '' AND instagram != '' AND tiktok != '' AND mailing != '' AND stream != '' THEN 1 ELSE 0 END) as total_tasks
                    FROM rop_contracts
                """)
                
                result = await session.execute(query, {"surname_like": surname_like, "name_like": name_like})
                count = result.scalar() or 0
                
                return count
        except Exception as e:
            logger.error(f"Ошибка count_pending_tasks_for_rop({rop_name}): {e}")
            return 0

    async def get_rop_category_stats(self, rop_name: str) -> Dict[str, int]:
        """Получает статистику по категориям для конкретного РОП-а без загрузки всех объектов"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(rop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                # Используем отдельные запросы для каждой категории (как в get_role_totals)
                # total
                total_res = await session.execute(text(
                    "SELECT COUNT(*) FROM properties WHERE LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like"
                ), {"surname_like": surname_like, "name_like": name_like})
                total = total_res.scalar() or 0
                
                # categories - используем GROUP BY как в get_role_totals
                cat_res = await session.execute(text(
                    "SELECT category, COUNT(*) cnt FROM properties "
                    "WHERE LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like "
                    "GROUP BY category"
                ), {"surname_like": surname_like, "name_like": name_like})
                cats = { (row.category or '').strip().upper(): row.cnt for row in cat_res.fetchall() }
                
                return {
                    'total': total,
                    'cat_A': cats.get('А', 0) + cats.get('A', 0),
                    'cat_B': cats.get('В', 0) + cats.get('B', 0),
                    'cat_C': cats.get('С', 0) + cats.get('C', 0),
                }
        except Exception as e:
            logger.error(f"Ошибка get_rop_category_stats({rop_name}): {e}")
            return {'total': 0, 'cat_A': 0, 'cat_B': 0, 'cat_C': 0}

    async def get_mop_category_stats(self, mop_name: str, rop_name: Optional[str] = None, dd_name: Optional[str] = None) -> Dict[str, int]:
        """Получает статистику по категориям для конкретного МОП-а без загрузки всех объектов, опционально фильтрует по РОП-у и ДД"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(mop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                where_clause = "(LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like)"
                params = {"surname_like": surname_like, "name_like": name_like}
                
                # Добавляем фильтр по РОП-у, если указан
                if rop_name:
                    rop_fio_parts = [p for p in str(rop_name).strip().split() if p]
                    rop_surname = rop_fio_parts[0] if rop_fio_parts else ''
                    rop_name_part = rop_fio_parts[1] if len(rop_fio_parts) > 1 else ''
                    rop_surname_like = f"%{rop_surname.lower()}%"
                    rop_name_like = f"%{rop_name_part.lower()}%"
                    where_clause += " AND (LOWER(rop) LIKE :rop_surname_like AND LOWER(rop) LIKE :rop_name_like)"
                    params['rop_surname_like'] = rop_surname_like
                    params['rop_name_like'] = rop_name_like
                
                # Добавляем фильтр по ДД, если указан
                if dd_name:
                    dd_fio_parts = [p for p in str(dd_name).strip().split() if p]
                    dd_surname = dd_fio_parts[0] if dd_fio_parts else ''
                    dd_name_part = dd_fio_parts[1] if len(dd_fio_parts) > 1 else ''
                    dd_surname_like = f"%{dd_surname.lower()}%"
                    dd_name_like = f"%{dd_name_part.lower()}%"
                    where_clause += " AND (LOWER(dd) LIKE :dd_surname_like AND LOWER(dd) LIKE :dd_name_like)"
                    params['dd_surname_like'] = dd_surname_like
                    params['dd_name_like'] = dd_name_like
                
                # Используем отдельные запросы для каждой категории (как в get_role_totals)
                # total
                total_res = await session.execute(text(
                    f"SELECT COUNT(*) FROM properties WHERE {where_clause}"
                ), params)
                total = total_res.scalar() or 0
                
                # categories - используем GROUP BY как в get_role_totals
                cat_res = await session.execute(text(
                    f"SELECT category, COUNT(*) cnt FROM properties "
                    f"WHERE {where_clause} "
                    f"GROUP BY category"
                ), params)
                cats = { (row.category or '').strip().upper(): row.cnt for row in cat_res.fetchall() }
                
                return {
                    'total': total,
                    'cat_A': cats.get('А', 0) + cats.get('A', 0),
                    'cat_B': cats.get('В', 0) + cats.get('B', 0),
                    'cat_C': cats.get('С', 0) + cats.get('C', 0),
                }
        except Exception as e:
            logger.error(f"Ошибка get_mop_category_stats({mop_name}, rop_name={rop_name}, dd_name={dd_name}): {e}")
            return {'total': 0, 'cat_A': 0, 'cat_B': 0, 'cat_C': 0}

    async def get_mop_contracts_by_category(self, mop_name: str, category: Optional[str] = None, rop_name: Optional[str] = None, dd_name: Optional[str] = None) -> List[Dict]:
        """Получает все объекты МОП-а с фильтрацией по категории, опционально фильтрует по РОП-у и ДД"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(mop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                where_clause = "(LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like)"
                params = {"surname_like": surname_like, "name_like": name_like}
                
                # Добавляем фильтр по РОП-у, если указан
                if rop_name:
                    rop_fio_parts = [p for p in str(rop_name).strip().split() if p]
                    rop_surname = rop_fio_parts[0] if rop_fio_parts else ''
                    rop_name_part = rop_fio_parts[1] if len(rop_fio_parts) > 1 else ''
                    rop_surname_like = f"%{rop_surname.lower()}%"
                    rop_name_like = f"%{rop_name_part.lower()}%"
                    where_clause += " AND (LOWER(rop) LIKE :rop_surname_like AND LOWER(rop) LIKE :rop_name_like)"
                    params['rop_surname_like'] = rop_surname_like
                    params['rop_name_like'] = rop_name_like
                
                # Добавляем фильтр по ДД, если указан
                if dd_name:
                    dd_fio_parts = [p for p in str(dd_name).strip().split() if p]
                    dd_surname = dd_fio_parts[0] if dd_fio_parts else ''
                    dd_name_part = dd_fio_parts[1] if len(dd_fio_parts) > 1 else ''
                    dd_surname_like = f"%{dd_surname.lower()}%"
                    dd_name_like = f"%{dd_name_part.lower()}%"
                    where_clause += " AND (LOWER(dd) LIKE :dd_surname_like AND LOWER(dd) LIKE :dd_name_like)"
                    params['dd_surname_like'] = dd_surname_like
                    params['dd_name_like'] = dd_name_like
                
                if category:
                    # Фильтруем по категории (А, В, С)
                    cat_upper = category.upper()
                    # Поддерживаем как латиницу, так и кириллицу
                    cat_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                    cat_cyr = cat_mapping.get(cat_upper, cat_upper)
                    where_clause += " AND (UPPER(category) = :cat OR UPPER(category) = :cat_cyr)"
                    params['cat'] = cat_upper
                    params['cat_cyr'] = cat_cyr
                
                status_filter_sql = " AND (status IS NULL OR LOWER(status) != 'реализовано')"
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE {where_clause}{status_filter_sql} ORDER BY last_modified_at DESC"),
                    params
                )
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                return contracts
        except Exception as e:
            logger.error(f"Ошибка get_mop_contracts_by_category({mop_name}, {category}, rop_name={rop_name}, dd_name={dd_name}): {e}")
            return []

    async def get_rop_contracts_by_category(self, rop_name: str, category: Optional[str] = None) -> List[Dict]:
        """Получает все объекты РОП-а с фильтрацией по категории"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(rop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                where_clause = "(LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like)"
                params = {"surname_like": surname_like, "name_like": name_like}
                
                if category:
                    # Фильтруем по категории (А, В, С)
                    cat_upper = category.upper()
                    cat_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                    cat_cyr = cat_mapping.get(cat_upper, cat_upper)
                    where_clause += " AND (UPPER(category) = :cat OR UPPER(category) = :cat_cyr)"
                    params['cat'] = cat_upper
                    params['cat_cyr'] = cat_cyr
                
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE {where_clause} ORDER BY last_modified_at DESC"),
                    params
                )
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                return contracts
        except Exception as e:
            logger.error(f"Ошибка get_rop_contracts_by_category({rop_name}, {category}): {e}")
            return []

    async def search_rops_by_name(self, search_name: str, dd_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Ищет РОП-ов по имени, опционально фильтрует по ДД"""
        try:
            async with self.async_session() as session:
                # Разбиваем поисковый запрос на части
                search_parts = [p for p in str(search_name).strip().split() if p]
                search_like = f"%{search_name.lower()}%"
                
                where_clause = "LOWER(rop) LIKE :search_like"
                params = {"search_like": search_like}
                
                # Добавляем фильтр по ДД, если указан
                if dd_name:
                    dd_fio_parts = [p for p in str(dd_name).strip().split() if p]
                    dd_surname = dd_fio_parts[0] if dd_fio_parts else ''
                    dd_name_part = dd_fio_parts[1] if len(dd_fio_parts) > 1 else ''
                    dd_surname_like = f"%{dd_surname.lower()}%"
                    dd_name_like = f"%{dd_name_part.lower()}%"
                    where_clause += " AND (LOWER(dd) LIKE :dd_surname_like AND LOWER(dd) LIKE :dd_name_like)"
                    params['dd_surname_like'] = dd_surname_like
                    params['dd_name_like'] = dd_name_like
                
                res = await session.execute(text(
                    f"SELECT DISTINCT rop AS name, COUNT(*) AS cnt FROM properties "
                    f"WHERE {where_clause} AND rop IS NOT NULL "
                    f"GROUP BY rop ORDER BY cnt DESC"
                ), params)
                items = []
                for row in res.fetchall():
                    if row.name:
                        items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка search_rops_by_name({search_name}, dd_name={dd_name}): {e}")
            return []

    async def search_mops_by_name(self, search_name: str, owner_name: str, owner_role: str) -> List[Dict[str, Any]]:
        """Ищет МОП-ов по имени для конкретного владельца (РОП или ДД)"""
        try:
            async with self.async_session() as session:
                search_like = f"%{search_name.lower()}%"
                
                owner_fio_parts = [p for p in str(owner_name).strip().split() if p]
                owner_surname = owner_fio_parts[0] if owner_fio_parts else ''
                owner_name_part = owner_fio_parts[1] if len(owner_fio_parts) > 1 else ''
                owner_surname_like = f"%{owner_surname.lower()}%"
                owner_name_like = f"%{owner_name_part.lower()}%"
                
                if owner_role == 'ДД':
                    where_clause = "(LOWER(dd) LIKE :owner_surname_like AND LOWER(dd) LIKE :owner_name_like)"
                elif owner_role == 'РОП':
                    where_clause = "(LOWER(rop) LIKE :owner_surname_like AND LOWER(rop) LIKE :owner_name_like)"
                else:
                    return []
                
                params = {
                    "search_like": search_like,
                    "owner_surname_like": owner_surname_like,
                    "owner_name_like": owner_name_like
                }
                
                res = await session.execute(text(
                    f"SELECT DISTINCT mop AS name, COUNT(*) AS cnt FROM properties "
                    f"WHERE {where_clause} AND LOWER(mop) LIKE :search_like AND mop IS NOT NULL "
                    f"GROUP BY mop ORDER BY cnt DESC"
                ), params)
                items = []
                for row in res.fetchall():
                    if row.name:
                        items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка search_mops_by_name({search_name}, {owner_name}, {owner_role}): {e}")
            return []

    async def get_mops_by_rop(self, rop_name: str, dd_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Получает список МОП-ов для конкретного РОП-а, опционально фильтрует по ДД"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(rop_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                where_clause = "(LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like)"
                params = {"surname_like": surname_like, "name_like": name_like}
                
                # Добавляем фильтр по ДД, если указан
                if dd_name:
                    dd_fio_parts = [p for p in str(dd_name).strip().split() if p]
                    dd_surname = dd_fio_parts[0] if dd_fio_parts else ''
                    dd_name_part = dd_fio_parts[1] if len(dd_fio_parts) > 1 else ''
                    dd_surname_like = f"%{dd_surname.lower()}%"
                    dd_name_like = f"%{dd_name_part.lower()}%"
                    where_clause += " AND (LOWER(dd) LIKE :dd_surname_like AND LOWER(dd) LIKE :dd_name_like)"
                    params['dd_surname_like'] = dd_surname_like
                    params['dd_name_like'] = dd_name_like
                
                res = await session.execute(text(
                    f"SELECT mop AS name, COUNT(*) AS cnt FROM properties "
                    f"WHERE {where_clause} "
                    f"GROUP BY mop ORDER BY cnt DESC NULLS LAST"
                ), params)
                items = []
                for row in res.fetchall():
                    if row.name:  # Игнорируем NULL значения
                        items.append({'name': row.name, 'count': row.cnt})
                return items
        except Exception as e:
            logger.error(f"Ошибка get_mops_by_rop({rop_name}, dd_name={dd_name}): {e}")
            return []

    async def get_contracts_by_category(self, agent_name: str, role: str, category: Optional[str] = None) -> List[Dict]:
        """Получает все объекты агента с фильтрацией по категории для любой роли"""
        try:
            async with self.async_session() as session:
                fio_parts = [p for p in str(agent_name).strip().split() if p]
                surname = fio_parts[0] if fio_parts else ''
                name = fio_parts[1] if len(fio_parts) > 1 else ''
                surname_like = f"%{surname.lower()}%"
                name_like = f"%{name.lower()}%"
                
                if role == 'МОП':
                    where_clause = "(LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like)"
                elif role == 'РОП':
                    where_clause = "(LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like)"
                elif role == 'ДД':
                    where_clause = "(LOWER(dd) LIKE :surname_like AND LOWER(dd) LIKE :name_like)"
                else:
                    where_clause = (
                        "((LOWER(mop) LIKE :surname_like AND LOWER(mop) LIKE :name_like) "
                        "OR (LOWER(rop) LIKE :surname_like AND LOWER(rop) LIKE :name_like) "
                        "OR (LOWER(dd)  LIKE :surname_like AND LOWER(dd)  LIKE :name_like))"
                    )
                
                params = {"surname_like": surname_like, "name_like": name_like}
                
                if category:
                    # Фильтруем по категории (А, В, С)
                    cat_upper = category.upper()
                    cat_mapping = {'A': 'А', 'B': 'В', 'C': 'С'}
                    cat_cyr = cat_mapping.get(cat_upper, cat_upper)
                    where_clause += " AND (UPPER(category) = :cat OR UPPER(category) = :cat_cyr)"
                    params['cat'] = cat_upper
                    params['cat_cyr'] = cat_cyr
                
                result = await session.execute(
                    text(f"SELECT * FROM properties WHERE {where_clause} ORDER BY last_modified_at DESC"),
                    params
                )
                
                contracts = []
                for row in result.fetchall():
                    contract_dict = dict(row._mapping)
                    contracts.append(self._convert_to_legacy_format(contract_dict))
                
                return contracts
        except Exception as e:
            logger.error(f"Ошибка get_contracts_by_category({agent_name}, {role}, {category}): {e}")
            return []
    
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
    
    async def set_category_c_for_missing(self) -> Dict[str, int]:
        """Заполняет категорией 'С' все записи, где category пустой или NULL.

        Возвращает статистику {updated, skipped, errors}.
        """
        try:
            async with self.async_session() as session:
                res_count = await session.execute(text(
                    "SELECT COUNT(*) FROM properties WHERE category IS NULL OR TRIM(category) = ''"
                ))
                to_update = res_count.scalar() or 0
                if to_update == 0:
                    return {"updated": 0, "skipped": 0, "errors": 0}
                await session.execute(text(
                    """
                    UPDATE properties
                    SET category = 'С',
                        last_modified_by = 'BOT',
                        last_modified_at = NOW()
                    WHERE category IS NULL OR TRIM(category) = ''
                    """
                ))
                await session.commit()
                logger.info(f"Категория 'С' проставлена для {to_update} записей с пустым значением")
                return {"updated": to_update, "skipped": 0, "errors": 0}
        except Exception as e:
            logger.error(f"Ошибка массового заполнения категории 'С': {e}")
            return {"updated": 0, "skipped": 0, "errors": 1}

    async def get_parsed_properties_count(self) -> int:
        """Получает общее количество объектов в таблице parsed_properties"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT COUNT(*) FROM parsed_properties
                """))
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества объектов в parsed_properties: {e}")
            return 0

    async def get_new_objects_count_by_phone(self) -> int:
        """Получает количество новых объектов(где stats_agent_given IS NULL и krisha_id IS NOT NULL)"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT COUNT(*) FROM parsed_properties
                    WHERE stats_agent_given IS NULL
                    AND krisha_id IS NOT NULL
                    AND krisha_id != ''
                """))
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества новых объектов: {e}")
            return 0

    async def get_agent_objects_count_by_phone(self, phone: str) -> int:
        """Получает количество объектов конкретного агента по номеру телефона (где stats_agent_given = phone и krisha_id IS NOT NULL)"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT COUNT(*) FROM parsed_properties
                    WHERE stats_agent_given = :phone
                    AND krisha_id IS NOT NULL
                    AND krisha_id != ''
                """), {"phone": phone})
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества объектов агента: {e}")
            return 0

    async def get_recall_objects_count_by_phone(self, phone: str) -> int:
        """Получает количество объектов с stats_recall_time для агента по номеру телефона"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT COUNT(*) FROM parsed_properties
                    WHERE stats_recall_time IS NOT NULL
                    AND krisha_id IS NOT NULL
                    AND krisha_id != ''
                """))
                return result.scalar() or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества объектов для перезвона: {e}")
            return 0

    async def get_latest_parsed_properties(self, page: int = 1, page_size: int = 10) -> Tuple[List[Dict], int]:
        """Получает последние объекты из parsed_properties, отсортированные по krisha_date (только с krisha_id)"""
        try:
            offset = (page - 1) * page_size
            async with self.async_session() as session:
                # Подсчет общего количества (только не взятые объекты)
                count_result = await session.execute(text("""
                    SELECT COUNT(*) FROM parsed_properties
                    WHERE krisha_id IS NOT NULL AND krisha_id != ''
                    AND stats_agent_given IS NULL
                """))
                total_count = count_result.scalar() or 0

                # Получение страницы (только не взятые объекты)
                result = await session.execute(text("""
                    SELECT vitrina_id, rbd_id, krisha_id, krisha_date, object_type, address,
                           complex, builder, flat_type, property_class, condition, sell_price,
                           sell_price_per_m2, house_num, floor_num, floor_count, room_count,
                           phones, description, ceiling_height, area, year_built, wall_type,
                           stats_agent_given, stats_time_given, stats_object_status, stats_recall_time, stats_description
                    FROM parsed_properties
                    WHERE krisha_id IS NOT NULL AND krisha_id != ''
                    AND stats_agent_given IS NULL
                    ORDER BY krisha_date DESC NULLS LAST, vitrina_id DESC
                    LIMIT :limit OFFSET :offset
                """), {"limit": page_size, "offset": offset})
                
                objects = []
                for row in result.fetchall():
                    obj = {
                        "vitrina_id": row.vitrina_id,
                        "rbd_id": row.rbd_id,
                        "krisha_id": row.krisha_id,
                        "krisha_date": row.krisha_date,
                        "object_type": row.object_type,
                        "address": row.address,
                        "complex": row.complex,
                        "builder": row.builder,
                        "flat_type": row.flat_type,
                        "property_class": row.property_class,
                        "condition": row.condition,
                        "sell_price": row.sell_price,
                        "sell_price_per_m2": row.sell_price_per_m2,
                        "house_num": row.house_num,
                        "floor_num": row.floor_num,
                        "floor_count": row.floor_count,
                        "room_count": row.room_count,
                        "phones": row.phones,
                        "description": row.description,
                        "ceiling_height": row.ceiling_height,
                        "area": row.area,
                        "year_built": row.year_built,
                        "wall_type": row.wall_type,
                        "stats_agent_given": row.stats_agent_given,
                        "stats_time_given": row.stats_time_given,
                        "stats_object_status": row.stats_object_status,
                        "stats_recall_time": row.stats_recall_time,
                        "stats_description": row.stats_description,
                    }
                    objects.append(obj)
                
                return objects, total_count
        except Exception as e:
            logger.error(f"Ошибка получения последних объектов: {e}", exc_info=True)
            return [], 0

    async def get_parsed_property_by_vitrina_id(self, vitrina_id: int) -> Optional[Dict]:
        """Получает объект по vitrina_id"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT vitrina_id, rbd_id, krisha_id, krisha_date, object_type, address,
                           complex, builder, flat_type, property_class, condition, sell_price,
                           sell_price_per_m2, house_num, floor_num, floor_count, room_count,
                           phones, description, ceiling_height, area, year_built, wall_type,
                           stats_agent_given, stats_time_given, stats_object_status, stats_recall_time, 
                           stats_description, stats_object_category
                    FROM parsed_properties
                    WHERE vitrina_id = :vitrina_id
                """), {"vitrina_id": vitrina_id})
                
                row = result.fetchone()
                if not row:
                    return None
                
                return {
                    "vitrina_id": row.vitrina_id,
                    "rbd_id": row.rbd_id,
                    "krisha_id": row.krisha_id,
                    "krisha_date": row.krisha_date,
                    "object_type": row.object_type,
                    "address": row.address,
                    "complex": row.complex,
                    "builder": row.builder,
                    "flat_type": row.flat_type,
                    "property_class": row.property_class,
                    "condition": row.condition,
                    "sell_price": row.sell_price,
                    "sell_price_per_m2": row.sell_price_per_m2,
                    "house_num": row.house_num,
                    "floor_num": row.floor_num,
                    "floor_count": row.floor_count,
                    "room_count": row.room_count,
                    "phones": row.phones,
                    "description": row.description,
                    "ceiling_height": row.ceiling_height,
                    "area": row.area,
                    "year_built": row.year_built,
                    "wall_type": row.wall_type,
                    "stats_agent_given": row.stats_agent_given,
                    "stats_time_given": row.stats_time_given,
                    "stats_object_status": row.stats_object_status,
                    "stats_recall_time": row.stats_recall_time,
                    "stats_description": row.stats_description,
                    "stats_object_category": row.stats_object_category,
                }
        except Exception as e:
            logger.error(f"Ошибка получения объекта по vitrina_id {vitrina_id}: {e}", exc_info=True)
            return None

    async def take_parsed_property(self, vitrina_id: int, agent_phone: str) -> bool:
        """Берет объект: устанавливает stats_agent_given, stats_time_given, stats_object_status"""
        try:
            async with self.async_session() as session:
                # Проверяем, не взят ли уже объект
                check_result = await session.execute(text("""
                    SELECT stats_agent_given FROM parsed_properties
                    WHERE vitrina_id = :vitrina_id
                """), {"vitrina_id": vitrina_id})
                row = check_result.fetchone()
                if row and row.stats_agent_given:
                    return False  # Уже взят
                
                # Обновляем объект
                await session.execute(text("""
                    UPDATE parsed_properties
                    SET stats_agent_given = :phone,
                        stats_time_given = NOW() AT TIME ZONE 'Asia/Almaty',
                        stats_object_status = 'Не позвонили',
                        updated_at = NOW()
                    WHERE vitrina_id = :vitrina_id
                """), {"phone": agent_phone, "vitrina_id": vitrina_id})
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка взятия объекта {vitrina_id}: {e}", exc_info=True)
            return False

    async def assign_latest_parsed_properties(self, agent_phone: str, limit: int = 10) -> Tuple[int, List[int], Dict[str, List[int]]]:
        """Назначает агенту несколько последних свободных объектов с распределением по категориям.
        
        Возвращает: (количество добавленных, список всех ID, словарь {категория: [id, ...]})
        Распределение: 3-A, 3-B, 4-C. Если не хватает A, увеличивается B. Если не хватает B, увеличивается C.
        """
        try:
            # Целевое распределение: 3-A, 3-B, 4-C
            target_a = 3
            target_b = 3
            target_c = 4
            
            async with self.async_session() as session:
                # Получаем объекты категории A
                result_a = await session.execute(text("""
                    SELECT vitrina_id, stats_object_category
                    FROM parsed_properties
                    WHERE krisha_id IS NOT NULL AND krisha_id != ''
                      AND stats_agent_given IS NULL
                      AND stats_object_category = 'A'
                    ORDER BY krisha_date DESC NULLS LAST, vitrina_id DESC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                """), {"limit": target_a})
                rows_a = result_a.fetchall()
                ids_a = [row.vitrina_id for row in rows_a]
                
                # Вычисляем сколько нужно B (увеличиваем, если не хватило A, но не больше общего лимита)
                missing_a = target_a - len(ids_a)
                needed_b = min(limit - len(ids_a), target_b + missing_a)
                needed_b = max(0, needed_b)  # Не может быть отрицательным
                
                # Получаем объекты категории B
                result_b = await session.execute(text("""
                    SELECT vitrina_id, stats_object_category
                    FROM parsed_properties
                    WHERE krisha_id IS NOT NULL AND krisha_id != ''
                      AND stats_agent_given IS NULL
                      AND stats_object_category = 'B'
                    ORDER BY krisha_date DESC NULLS LAST, vitrina_id DESC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                """), {"limit": needed_b})
                rows_b = result_b.fetchall()
                ids_b = [row.vitrina_id for row in rows_b]
                
                # Вычисляем сколько нужно C (остальное до общего лимита)
                needed_c = limit - len(ids_a) - len(ids_b)
                needed_c = max(0, needed_c)  # Не может быть отрицательным
                
                # Получаем объекты категории C
                result_c = await session.execute(text("""
                    SELECT vitrina_id, stats_object_category
                    FROM parsed_properties
                    WHERE krisha_id IS NOT NULL AND krisha_id != ''
                      AND stats_agent_given IS NULL
                      AND (stats_object_category = 'C' OR stats_object_category IS NULL)
                    ORDER BY krisha_date DESC NULLS LAST, vitrina_id DESC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED
                """), {"limit": needed_c})
                rows_c = result_c.fetchall()
                ids_c = [row.vitrina_id for row in rows_c]
                
                # Объединяем все ID
                all_ids = ids_a + ids_b + ids_c
                
                if not all_ids:
                    return 0, [], {}
                
                # Обновляем объекты
                for vitrina_id in all_ids:
                    await session.execute(text("""
                        UPDATE parsed_properties
                        SET stats_agent_given = :phone,
                            stats_time_given = NOW() AT TIME ZONE 'Asia/Almaty',
                            stats_object_status = 'Не позвонили',
                            updated_at = NOW()
                        WHERE vitrina_id = :vitrina_id
                    """), {"phone": agent_phone, "vitrina_id": vitrina_id})
                
                await session.commit()
                
                # Формируем словарь по категориям
                categories_dict = {
                    'A': ids_a,
                    'B': ids_b,
                    'C': ids_c,
                }
                
                return len(all_ids), all_ids, categories_dict
        except Exception as e:
            logger.error(f"Ошибка пакетного назначения объектов агенту {agent_phone}: {e}", exc_info=True)
            return 0, [], {}

    async def get_my_objects_status_stats(self, agent_phone: str) -> Dict[str, int]:
        """Получает статистику по статусам объектов агента"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Не позвонили') as not_called,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Перезвонить') as recall,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Встреча') as meeting,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Договор') as deal,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Отказ') as rejected,
                        COUNT(*) FILTER (WHERE stats_object_status = 'Архив') as archived
                    FROM parsed_properties
                    WHERE stats_agent_given = :phone
                    AND krisha_id IS NOT NULL AND krisha_id != ''
                """), {"phone": agent_phone})
                
                row = result.fetchone()
                if row:
                    return {
                        "total": row.total or 0,
                        "not_called": row.not_called or 0,
                        "recall": row.recall or 0,
                        "meeting": row.meeting or 0,
                        "deal": row.deal or 0,
                        "rejected": row.rejected or 0,
                        "archived": row.archived or 0,
                    }
                return {"total": 0, "not_called": 0, "recall": 0, "meeting": 0, "deal": 0, "rejected": 0, "archived": 0}
        except Exception as e:
            logger.error(f"Ошибка получения статистики по статусам: {e}", exc_info=True)
            return {"total": 0, "not_called": 0, "recall": 0, "meeting": 0, "deal": 0, "rejected": 0, "archived": 0}

    async def get_my_new_parsed_properties(
        self, 
        agent_phone: str, 
        page: int = 1, 
        page_size: int = 10,
        status_filter: Optional[str] = None
    ) -> Tuple[List[Dict], int]:
        """Получает объекты агента по номеру телефона с опциональной фильтрацией по статусу"""
        try:
            offset = (page - 1) * page_size
            async with self.async_session() as session:
                # Базовое условие WHERE
                where_clause = "WHERE stats_agent_given = :phone AND krisha_id IS NOT NULL AND krisha_id != ''"
                params = {"phone": agent_phone}
                
                # Добавляем фильтр по статусу
                if status_filter:
                    if isinstance(status_filter, list):
                        placeholders = []
                        for idx, status_val in enumerate(status_filter):
                            key = f"status_{idx}"
                            placeholders.append(f":{key}")
                            params[key] = status_val
                        where_clause += f" AND stats_object_status IN ({', '.join(placeholders)})"
                    else:
                        where_clause += " AND stats_object_status = :status"
                        params["status"] = status_filter
                
                # Подсчет общего количества
                count_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM parsed_properties {where_clause}"),
                    params
                )
                total_count = count_result.scalar() or 0

                # Получение страницы
                result = await session.execute(text(f"""
                    SELECT vitrina_id, rbd_id, krisha_id, krisha_date, object_type, address,
                           complex, builder, flat_type, property_class, condition, sell_price,
                           sell_price_per_m2, house_num, floor_num, floor_count, room_count,
                           phones, description, ceiling_height, area, year_built, wall_type,
                           stats_agent_given, stats_time_given, stats_object_status, stats_recall_time, stats_description
                    FROM parsed_properties
                    {where_clause}
                    ORDER BY stats_time_given DESC NULLS LAST, vitrina_id DESC
                    LIMIT :limit OFFSET :offset
                """), {**params, "limit": page_size, "offset": offset})
                
                objects = []
                for row in result.fetchall():
                    obj = {
                        "vitrina_id": row.vitrina_id,
                        "rbd_id": row.rbd_id,
                        "krisha_id": row.krisha_id,
                        "krisha_date": row.krisha_date,
                        "object_type": row.object_type,
                        "address": row.address,
                        "complex": row.complex,
                        "builder": row.builder,
                        "flat_type": row.flat_type,
                        "property_class": row.property_class,
                        "condition": row.condition,
                        "sell_price": row.sell_price,
                        "sell_price_per_m2": row.sell_price_per_m2,
                        "house_num": row.house_num,
                        "floor_num": row.floor_num,
                        "floor_count": row.floor_count,
                        "room_count": row.room_count,
                        "phones": row.phones,
                        "description": row.description,
                        "ceiling_height": row.ceiling_height,
                        "area": row.area,
                        "year_built": row.year_built,
                        "wall_type": row.wall_type,
                        "stats_agent_given": row.stats_agent_given,
                        "stats_time_given": row.stats_time_given,
                        "stats_object_status": row.stats_object_status,
                        "stats_recall_time": row.stats_recall_time,
                        "stats_description": row.stats_description,
                    }
                    objects.append(obj)
                
                return objects, total_count
        except Exception as e:
            logger.error(f"Ошибка получения объектов агента: {e}", exc_info=True)
            return [], 0

    async def _load_third_map(self) -> Dict[str, Dict[str, Optional[float]]]:
        """Загружает third_map из Google Sheets с кешированием"""
        from datetime import datetime, timedelta
        
        # Проверяем кеш
        if (self._third_map_cache is not None and 
            self._third_map_cache_time is not None and
            (datetime.now() - self._third_map_cache_time).total_seconds() < self._third_map_cache_ttl):
            return self._third_map_cache
        
        # Загружаем из Google Sheets
        credentials_file = 'credentials.json'
        if not os.path.exists(credentials_file):
            logger.warning(f"Файл {credentials_file} не найден, категоризация будет упрощенной")
            return {}
        
        if not SHEET_ID or not THIRD_SHEET_GID:
            logger.warning("SHEET_ID или THIRD_SHEET_GID не установлены, категоризация будет упрощенной")
            return {}
        
        try:
            credentials = Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(SHEET_ID)
            third_ws = spreadsheet.get_worksheet_by_id(int(THIRD_SHEET_GID))
            
            rows = third_ws.get_all_values()
            if not rows:
                return {}
            
            # Поиск строки заголовков
            header_row_idx = 0
            for i, r in enumerate(rows):
                line = ' '.join(r).lower()
                if ('жк' in line) or ('крыша' in line) or ('витрина' in line) or ('общий балл' in line):
                    header_row_idx = i
                    break
            
            def to_float_safe(v):
                try:
                    s = str(v).replace(' ', '').replace('\u00A0', '')
                    s = s.replace(',', '.')
                    if s.strip() == '':
                        return None
                    return float(s)
                except Exception:
                    return None
            
            def norm_complex(x: str) -> str:
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
                    'qalashyq': 'калашык', 'qalashy': 'калашык', 'qurylys': 'курылыс', 'exclusive': 'эксклюзив',
                    'bukhar': 'бухар', 'jyray': 'жырау', 'dauletti': 'даулетти', 'qalashyk': 'калашык',
                    'city': 'city', 'sat': 'sat'
                }
                tokens = s.split()
                norm_tokens = []
                for t in tokens:
                    t_clean = synonyms.get(t, t)
                    norm_tokens.append(t_clean)
                return ' '.join(norm_tokens)
            
            complex_to_params: Dict[str, Dict[str, Optional[float]]] = {}
            for i, r in enumerate(rows):
                if i <= header_row_idx:
                    continue
                complex_name = (r[0] if len(r) > 0 else '').strip()
                if not complex_name:
                    continue
                roof_raw = r[1] if len(r) > 1 else ''
                score_raw = r[2] if len(r) > 2 else ''
                window_raw = r[3] if len(r) > 3 else ''
                complex_to_params[norm_complex(complex_name)] = {
                    'roof': to_float_safe(roof_raw),
                    'score': to_float_safe(score_raw),
                    'window': to_float_safe(window_raw),
                }
            
            # Сохраняем в кеш
            self._third_map_cache = complex_to_params
            self._third_map_cache_time = datetime.now()
            logger.info(f"Загружен third_map: {len(complex_to_params)} записей")
            return complex_to_params
            
        except Exception as e:
            logger.error(f"Ошибка загрузки third_map: {e}", exc_info=True)
            return {}
    
    def _find_complex_in_map(self, complex_name: str, third_map: Dict[str, Dict[str, Optional[float]]]) -> Optional[Dict[str, Optional[float]]]:
        """Находит параметры для ЖК в third_map с нормализацией и поиском по вариантам"""
        if not complex_name or not third_map:
            return None
        
        def norm_complex(x: str) -> str:
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
                'qalashyq': 'калашык', 'qalashy': 'калашык', 'qurylys': 'курылыс', 'exclusive': 'эксклюзив',
                'bukhar': 'бухар', 'jyray': 'жырау', 'dauletti': 'даулетти', 'qalashyk': 'калашык',
                'city': 'city', 'sat': 'sat'
            }
            tokens = s.split()
            norm_tokens = []
            for t in tokens:
                t_clean = synonyms.get(t, t)
                norm_tokens.append(t_clean)
            return ' '.join(norm_tokens)
        
        def find_best_match(norm_name: str) -> Optional[str]:
            name_set = set(norm_name.split())
            if not name_set:
                return None
            best_key, best_score = None, 0.0
            for k in third_map.keys():
                k_set = set(k.split())
                if not k_set:
                    continue
                inter = len(name_set & k_set)
                union = len(name_set | k_set)
                score = inter / union if union else 0.0
                smaller, bigger = (name_set, k_set) if len(name_set) <= len(k_set) else (k_set, name_set)
                if smaller and smaller.issubset(bigger):
                    score = max(score, 0.999)
                if score > best_score:
                    best_score, best_key = score, k
            return best_key if best_score >= 0.45 else None
        
        # Прямое совпадение
        norm_key = norm_complex(complex_name)
        if norm_key in third_map:
            return third_map[norm_key]
        
        # Поиск по вариантам
        best = find_best_match(norm_key)
        if best and best in third_map:
            return third_map[best]
        
        return None

    async def _calculate_category_for_parsed(self, item: Dict[str, Any]) -> str:
        """Вычисляет категорию для parsed_property используя полную формулу из automate_categories.
        
        Использует sell_price как contract_price, area для расчета window_price и roof_price.
        Загружает third_map из Google Sheets для получения roof, window, score.
        Если данных недостаточно, возвращает 'C'.
        """
        def is_num(x) -> bool:
            return isinstance(x, (int, float)) and x is not None
        
        def assign_category(contract_price: Optional[float], window_price: Optional[float], 
                           roof_price: Optional[float], score: Optional[float]) -> str:
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
        
        sell_price = item.get('sell_price')
        area = item.get('area')
        complex_name = item.get('complex')
        
        # Загружаем third_map
        third_map = await self._load_third_map()
        
        if not third_map or not complex_name:
            return 'C'
        
        # Ищем параметры для ЖК
        params = self._find_complex_in_map(complex_name, third_map)
        if not params:
            return 'C'
        
        roof = params.get('roof')
        window = params.get('window')
        score = params.get('score')
        
        # Вычисляем window_price и roof_price
        window_price = (window * area) if (window is not None and area is not None) else None
        roof_price = (roof * area) if (roof is not None and area is not None) else None
        
        # Используем sell_price как contract_price
        contract_price = sell_price
        
        # Применяем формулу категоризации
        return assign_category(contract_price, window_price, roof_price, score)
    
    async def upsert_parsed_properties(self, items: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Добавляет или обновляет объекты parsed_properties с автокатегоризацией"""
        if not items:
            return 0, 0
        inserted = 0
        updated = 0
        try:
            # Вычисляем категорию для каждого элемента перед вставкой
            for item in items:
                if 'stats_object_category' not in item or not item.get('stats_object_category'):
                    item['stats_object_category'] = await self._calculate_category_for_parsed(item)
            
            async with self.async_session() as session:
                for batch in chunk_list(items, 200):
                    if not batch:
                        continue
                    stmt = pg_insert(parsed_properties_table).values(batch)
                    updatable_fields = {
                        "krisha_id": stmt.excluded.krisha_id,
                        "krisha_date": stmt.excluded.krisha_date,
                        "object_type": stmt.excluded.object_type,
                        "address": stmt.excluded.address,
                        "complex": stmt.excluded.complex,
                        "builder": stmt.excluded.builder,
                        "flat_type": stmt.excluded.flat_type,
                        "property_class": stmt.excluded.property_class,
                        "condition": stmt.excluded.condition,
                        "sell_price": stmt.excluded.sell_price,
                        "sell_price_per_m2": stmt.excluded.sell_price_per_m2,
                        "address_type": stmt.excluded.address_type,
                        "house_num": stmt.excluded.house_num,
                        "floor_num": stmt.excluded.floor_num,
                        "floor_count": stmt.excluded.floor_count,
                        "room_count": stmt.excluded.room_count,
                        "phones": stmt.excluded.phones,
                        "description": stmt.excluded.description,
                        "ceiling_height": stmt.excluded.ceiling_height,
                        "area": stmt.excluded.area,
                        "year_built": stmt.excluded.year_built,
                        "wall_type": stmt.excluded.wall_type,
                        "stats_agent_given": stmt.excluded.stats_agent_given,
                        "stats_time_given": stmt.excluded.stats_time_given,
                        "stats_object_status": stmt.excluded.stats_object_status,
                        "stats_recall_time": stmt.excluded.stats_recall_time,
                        "stats_description": stmt.excluded.stats_description,
                        "stats_object_category": stmt.excluded.stats_object_category,
                        "updated_at": func.now(),
                    }
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[parsed_properties_table.c.rbd_id],
                        set_=updatable_fields,
                    )
                    result = await session.execute(stmt)
                    rowcount = result.rowcount or 0
                    inserted_count = len(batch)
                    updated_count = max(rowcount - inserted_count, 0)
                    inserted += inserted_count
                    updated += updated_count
                await session.commit()
            return inserted, updated
        except Exception as e:
            logger.error(f"Ошибка upsert parsed_properties: {e}", exc_info=True)
            return inserted, updated

    async def get_existing_rbd_ids(self, rbd_ids: List[int]) -> set:
        if not rbd_ids:
            return set()
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT rbd_id FROM parsed_properties WHERE rbd_id = ANY(:ids)
                """), {"ids": rbd_ids})
                return {row.rbd_id for row in result.fetchall()}
        except Exception as e:
            logger.error(f"Ошибка count_existing_rbd_ids: {e}")
            return set()

    async def fetch_parsed_properties_for_archive(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            sql = """
                SELECT vitrina_id, krisha_id, stats_object_status
                FROM parsed_properties
                WHERE krisha_id IS NOT NULL AND krisha_id != ''
                  AND (stats_object_status IS NULL OR stats_object_status != 'Архив')
            """
            if limit:
                sql += " LIMIT :limit"
            async with self.async_session() as session:
                result = await session.execute(text(sql), {"limit": limit} if limit else {})
                return [
                    {"vitrina_id": row.vitrina_id, "krisha_id": row.krisha_id, "stats_object_status": row.stats_object_status}
                    for row in result.fetchall()
                ]
        except Exception as e:
            logger.error(f"Ошибка fetch_parsed_properties_for_archive: {e}")
            return []

    async def mark_parsed_property_archived(self, vitrina_id: int) -> None:
        try:
            async with self.async_session() as session:
                await session.execute(text("""
                    UPDATE parsed_properties
                    SET stats_object_status = 'Архив',
                        updated_at = NOW()
                    WHERE vitrina_id = :vitrina_id
                """), {"vitrina_id": vitrina_id})
                await session.commit()
        except Exception as e:
            logger.error(f"Ошибка mark_parsed_property_archived: {e}")

    async def update_parsed_property_status(
        self, 
        vitrina_id: int, 
        status: str, 
        recall_time: Optional[datetime] = None
    ) -> bool:
        """Обновляет статус объекта и опционально время перезвона"""
        try:
            async with self.async_session() as session:
                if recall_time is not None:
                    # Обновляем статус и время перезвона
                    await session.execute(text("""
                        UPDATE parsed_properties
                        SET stats_object_status = :status,
                            stats_recall_time = :recall_time,
                            updated_at = NOW()
                        WHERE vitrina_id = :vitrina_id
                    """), {
                        "status": status,
                        "recall_time": recall_time,
                        "vitrina_id": vitrina_id
                    })
                else:
                    # Обновляем только статус, очищаем время перезвона если статус не "Перезвонить"
                    if status != "Перезвонить":
                        await session.execute(text("""
                            UPDATE parsed_properties
                            SET stats_object_status = :status,
                                stats_recall_time = NULL,
                                updated_at = NOW()
                            WHERE vitrina_id = :vitrina_id
                        """), {
                            "status": status,
                            "vitrina_id": vitrina_id
                        })
                    else:
                        await session.execute(text("""
                            UPDATE parsed_properties
                            SET stats_object_status = :status,
                                updated_at = NOW()
                            WHERE vitrina_id = :vitrina_id
                        """), {
                            "status": status,
                            "vitrina_id": vitrina_id
                        })
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка update_parsed_property_status для {vitrina_id}: {e}", exc_info=True)
            return False

    async def add_parsed_property_comment(self, vitrina_id: int, comment: str) -> bool:
        """Добавляет комментарий к объекту. Комментарии разделяются через ';' с датой/временем."""
        try:
            from datetime import datetime
            from zoneinfo import ZoneInfo
            
            # Проверяем, что в комментарии нет ';'
            if ';' in comment:
                return False
            
            async with self.async_session() as session:
                # Получаем текущий комментарий
                result = await session.execute(text("""
                    SELECT stats_description FROM parsed_properties WHERE vitrina_id = :vitrina_id
                """), {"vitrina_id": vitrina_id})
                row = result.fetchone()
                current_comment = row.stats_description if row else None
                
                # Формируем новую запись с датой/временем
                almaty_tz = ZoneInfo("Asia/Almaty")
                now = datetime.now(almaty_tz)
                new_entry = f"{now.strftime('%d.%m.%Y %H:%M')} - {comment};"
                
                # Добавляем к существующему комментарию
                if current_comment:
                    updated_comment = current_comment + " " + new_entry
                else:
                    updated_comment = new_entry
                
                # Обновляем в БД
                await session.execute(text("""
                    UPDATE parsed_properties
                    SET stats_description = :comment,
                        updated_at = NOW()
                    WHERE vitrina_id = :vitrina_id
                """), {
                    "comment": updated_comment,
                    "vitrina_id": vitrina_id
                })
                await session.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка add_parsed_property_comment для {vitrina_id}: {e}", exc_info=True)
            return False

    async def get_parsed_properties_for_recall_notification(self) -> List[Dict[str, Any]]:
        """Получает объекты, которым нужно отправить уведомление о перезвоне"""
        try:
            async with self.async_session() as session:
                result = await session.execute(text("""
                    SELECT vitrina_id, stats_agent_given, stats_recall_time, address, krisha_id
                    FROM parsed_properties
                    WHERE stats_object_status = 'Перезвонить'
                      AND stats_recall_time IS NOT NULL
                      AND stats_recall_time <= NOW() AT TIME ZONE 'Asia/Almaty'
                      AND stats_agent_given IS NOT NULL
                """))
                return [
                    {
                        "vitrina_id": row.vitrina_id,
                        "agent_phone": row.stats_agent_given,
                        "recall_time": row.stats_recall_time,
                        "address": row.address,
                        "krisha_id": row.krisha_id
                    }
                    for row in result.fetchall()
                ]
        except Exception as e:
            logger.error(f"Ошибка get_parsed_properties_for_recall_notification: {e}", exc_info=True)
            return []

    async def mark_recall_notification_sent(self, vitrina_id: int) -> None:
        """Помечает, что уведомление о перезвоне было отправлено (очищает stats_recall_time)"""
        try:
            async with self.async_session() as session:
                await session.execute(text("""
                    UPDATE parsed_properties
                    SET stats_recall_time = NULL,
                        updated_at = NOW()
                    WHERE vitrina_id = :vitrina_id
                """), {"vitrina_id": vitrina_id})
                await session.commit()
        except Exception as e:
            logger.error(f"Ошибка mark_recall_notification_sent для {vitrina_id}: {e}", exc_info=True)

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
