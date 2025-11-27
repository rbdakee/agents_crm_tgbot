import asyncio
import logging
from typing import Optional

import httpx

from config import (
    ARCHIVE_CONCURRENCY, ARCHIVE_TIMEOUT, ARCHIVE_HTTP_TIMEOUT,
    KRISHA_URL_TEMPLATE, KRISHA_USER_AGENT
)
from database_postgres import get_db_manager

logger = logging.getLogger(__name__)

KRISHA_HEADERS = {
    "User-Agent": KRISHA_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.8",
    "Cache-Control": "no-cache",
}


async def _check_single(client: httpx.AsyncClient, krisha_id: str, semaphore: asyncio.Semaphore) -> Optional[int]:
    url = KRISHA_URL_TEMPLATE.format(krisha_id=krisha_id)
    async with semaphore:
        try:
            response = await client.get(url, headers=KRISHA_HEADERS, timeout=ARCHIVE_TIMEOUT, follow_redirects=False)
            return response.status_code
        except httpx.RequestError as exc:
            logger.warning("Ошибка запроса к Krisha %s: %s", krisha_id, exc)
            return None


async def archive_missing_objects(limit: Optional[int] = None) -> dict:
    db_manager = await get_db_manager()
    targets = await db_manager.fetch_parsed_properties_for_archive(limit=limit)
    if not targets:
        return {"checked": 0, "archived": 0}

    semaphore = asyncio.Semaphore(ARCHIVE_CONCURRENCY)
    archived = 0

    async with httpx.AsyncClient(timeout=ARCHIVE_HTTP_TIMEOUT) as client:
        tasks = [
            _check_single(client, item["krisha_id"], semaphore)
            for item in targets
        ]
        for idx, status_code in enumerate(await asyncio.gather(*tasks)):
            item = targets[idx]
            if status_code == 200:
                continue
            if status_code in (404, 410):
                await db_manager.mark_parsed_property_archived(item["vitrina_id"])
                archived += 1
            else:
                logger.info("Krisha %s вернула статус %s — пропускаем", item["krisha_id"], status_code)

    return {"checked": len(targets), "archived": archived}

