import json, logging, httpx
from typing import Any, Dict, List, Optional
from dateutil import parser as date_parser
from config import (
    RBD_MAX_DUPLICATES,
    RBD_EMAIL,
    RBD_PASSWORD,
    HTTP_TIMEOUT,
    RBD_SUPPLY_SEARCH_URL,
    RBD_LOGIN_URL,
    RBD_USER_AGENT,
    RBD_BASE_URL,
    RBD_RAW_DATA_JSON,
    refresh_property_classes,
)
from database_postgres import get_db_manager

logger = logging.getLogger(__name__)

# Используем константы из config.py
BASE_URL = RBD_SUPPLY_SEARCH_URL
LOGIN_URL = RBD_LOGIN_URL
USER_AGENT = RBD_USER_AGENT
RAW_DATA = json.loads(RBD_RAW_DATA_JSON)


def build_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.7",
        "Origin": RBD_BASE_URL,
        "Referer": f"{RBD_BASE_URL}/app/demand/start?pageNum=1&sortType=1&flatType=&viewType=3",
        "X-Requested-With": "XMLHttpRequest",
        "mb-ajax": "true",
    }


def build_login_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.7",
        "Origin": RBD_BASE_URL,
        "Referer": f"{RBD_BASE_URL}/login",
    }


def build_login_payload(email: str, password: str, remember: bool = True) -> Dict[str, Any]:
    return {
        "type": "databox",
        "value": {
            "email": {"type": "string", "value": email},
            "passwd": {"type": "string", "value": password},
            "remember": {"type": "boolean", "value": remember},
        },
    }


def to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_datetime(value: Any):
    if not value:
        return None
    try:
        return date_parser.parse(str(value))
    except (ValueError, TypeError):
        return None


def build_address(item: Dict[str, Any]) -> Optional[str]:
    parts = [
        item.get("city__text"),
        item.get("district__text"),
        item.get("addressType__text"),
        item.get("addressName"),
    ]
    filtered = [str(part).strip() for part in parts if part]
    return ", ".join(filtered) if filtered else None


def clean_description(description: Any) -> Optional[str]:
    """Очищает описание от служебного текста 'Перевести Перевод может быть неточным Показать оригинал'"""
    if not description:
        return None
    
    desc_str = str(description).strip()
    if not desc_str:
        return None
    
    # Удаляем фразу в конце, если она есть
    unwanted_text = "Перевести Перевод может быть неточным Показать оригинал"
    if desc_str.endswith(unwanted_text):
        desc_str = desc_str[:-len(unwanted_text)].strip()
    
    return desc_str if desc_str else None


def item_to_row(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rbd_id = to_int(item.get("id"))
    if rbd_id is None:
        return None

    return {
        "rbd_id": rbd_id,
        "krisha_id": str(item["krishaId"]) if item.get("krishaId") else None,
        "krisha_date": parse_datetime(item.get("krishaDate")),
        "object_type": item.get("objectType__text"),
        "address": build_address(item),
        "complex": item.get("complex__text") or item.get("complexName"),
        "builder": item.get("builder__text") or item.get("builderName"),
        "flat_type": item.get("flatType__text"),
        "property_class": item.get("propertyClass__text"),
        "condition": item.get("condition__text"),
        "sell_price": to_float(item.get("sourceSellPrice") or item.get("sellPriceFull")),
        "sell_price_per_m2": to_float(item.get("sellPriceMeter")),
        "address_type": item.get("addressType__text"),
        "house_num": item.get("houseNum"),
        "floor_num": to_int(item.get("floorNum")),
        "floor_count": to_int(item.get("floorCount")),
        "room_count": to_int(item.get("roomCount")),
        "phones": item.get("phones"),
        "description": clean_description(item.get("memoPublic")),
        "ceiling_height": to_float(item.get("ceilingHeight")),
        "area": to_float(item.get("area")),
        "year_built": to_int(item.get("yearBuilt")),
        "wall_type": item.get("wallType__text"),
        "stats_agent_given": item.get("statsAgentGiven"),
        "stats_time_given": parse_datetime(item.get("statsTimeGiven")),
        "stats_object_status": item.get("statsObjectStatus"),
        "stats_recall_time": parse_datetime(item.get("statsRecallTime")),
        "stats_description": item.get("statsDescription"),
    }


class RBDAsyncFetcher:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.client = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers={"User-Agent": USER_AGENT})

    async def __aenter__(self):
        await self.login()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.client.aclose()

    async def login(self):
        await self.client.get(RBD_BASE_URL, headers={"User-Agent": USER_AGENT})
        payload = build_login_payload(self.email, self.password)
        files = {"data": (None, json.dumps(payload, ensure_ascii=False))}
        response = await self.client.post(
            LOGIN_URL,
            headers=build_login_headers(),
            files=files,
        )
        if response.status_code != 200:
            raise RuntimeError("Не удалось авторизоваться в rbd.kz")

    async def fetch_page(self, page_num: int) -> Optional[Dict[str, Any]]:
        payload = json.loads(json.dumps(RAW_DATA))
        payload["value"]["pageNum"]["value"] = page_num
        payload["value"]["filterChanged"]["value"] = page_num == 1
        files = {"data": (None, json.dumps(payload, ensure_ascii=False))}
        response = await self.client.post(
            BASE_URL,
            headers=build_headers(),
            files=files,
        )
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        if isinstance(data, dict) and data.get("errorMessage") == "Need recreate session":
            await self.login()
            return await self.fetch_page(page_num)
        return data


async def fetch_new_objects(max_duplicates: Optional[int] = None) -> Dict[str, Any]:
    email = RBD_EMAIL
    password = RBD_PASSWORD
    if not email or not password:
        raise RuntimeError("EMAIL_RBD и PASSWORD_RBD должны быть заданы в .env")

    db_manager = await get_db_manager()
    
    # Проверяем количество объектов в таблице
    # Если объектов 0, то парсинг без лимита дубликатов
    parsed_count = await db_manager.get_parsed_properties_count()
    if parsed_count == 0:
        max_duplicates = None  # Без лимита дубликатов
    else:
        max_duplicates = max_duplicates or RBD_MAX_DUPLICATES

    stats = {
        "pages": 0,
        "inserted": 0,
        "duplicates": 0,
        "stopped": False,
    }

    async with RBDAsyncFetcher(email, password) as fetcher:
        page = 1
        while True:
            data = await fetcher.fetch_page(page)
            if not data:
                break
            store = data.get("store") or []
            if not store:
                break

            rows: List[Dict[str, Any]] = []
            for item in store:
                row = item_to_row(item)
                if row:
                    rows.append(row)
            if not rows:
                page += 1
                continue

            rbd_ids = [row["rbd_id"] for row in rows if row.get("rbd_id") is not None]
            existing_ids = await db_manager.get_existing_rbd_ids(rbd_ids)
            stats["duplicates"] += len(existing_ids)
            new_rows = [row for row in rows if row["rbd_id"] not in existing_ids]

            if new_rows:
                inserted, _ = await db_manager.upsert_parsed_properties(new_rows)
                stats["inserted"] += inserted

            stats["pages"] += 1

            # Проверяем лимит дубликатов только если он установлен
            if max_duplicates is not None and stats["duplicates"] >= max_duplicates:
                stats["stopped"] = True
                break

            page += 1

    # После загрузки новых данных обновляем классы недвижимости из БД,
    # чтобы PROPERTY_CLASSES содержал актуальные значения.
    try:
        await refresh_property_classes()
    except Exception as e:
        logger.warning(f"Не удалось обновить классы недвижимости после парсинга: {e}")

    return stats

