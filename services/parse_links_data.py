"""
Сервис для парсинга аналитики по ссылкам Krisha, Instagram, TikTok
"""
import os
import re
import json
import logging
import asyncio
import sys
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from contextlib import contextmanager

import httpx
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Подавляем логи от Apify
logging.getLogger('apify').setLevel(logging.ERROR)
logging.getLogger('apify_client').setLevel(logging.ERROR)
logging.getLogger('ApifyClient').setLevel(logging.ERROR)

# Устанавливаем переменную окружения для подавления логов Apify
os.environ.setdefault('APIFY_LOG_LEVEL', 'ERROR')


@contextmanager
def suppress_stdout_stderr():
    """Контекстный менеджер для подавления stdout и stderr"""
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            sys.stdout = devnull
            sys.stderr = devnull
            yield
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr


def extract_krisha_id(krisha_url: str) -> Optional[str]:
    """
    Извлекает ID объявления из ссылки Krisha.kz
    
    Примеры:
    - https://krisha.kz/a/show/1007207741 -> 1007207741
    - https://krisha.kz/a/show/123456 -> 123456
    """
    if not krisha_url or not isinstance(krisha_url, str):
        return None
    
    # Паттерн для извлечения ID из URL
    pattern = r'krisha\.kz/a/show/(\d+)'
    match = re.search(pattern, krisha_url)
    if match:
        return match.group(1)
    return None


async def parse_krisha_views(krisha_url: str) -> Optional[int]:
    """
    Парсит количество просмотров объявления на Krisha.kz
    
    Args:
        krisha_url: Ссылка на объявление Krisha.kz
        
    Returns:
        Количество просмотров (nb_views) или None в случае ошибки
    """
    try:
        krisha_id = extract_krisha_id(krisha_url)
        if not krisha_id:
            logger.warning(f"Не удалось извлечь ID из ссылки Krisha: {krisha_url}")
            return None
        
        # Формируем URL для API
        api_url = f"https://krisha.kz/ms/views/krisha/live/{krisha_id}/"
        
        # Делаем GET запрос
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(api_url)
            response.raise_for_status()
            
            data = response.json()
            
            # Проверяем структуру ответа: {"status":"ok","data":{"{id}":{"nb_phone_views":0,"nb_views":1640}}}
            if data.get("status") == "ok" and "data" in data:
                data_obj = data["data"]
                # ID может быть ключом в data
                if krisha_id in data_obj:
                    views_data = data_obj[krisha_id]
                    nb_views = views_data.get("nb_views")
                    if nb_views is not None:
                        return int(nb_views)
                # Если структура другая, пробуем найти первый ключ
                elif data_obj:
                    first_key = list(data_obj.keys())[0]
                    views_data = data_obj[first_key]
                    nb_views = views_data.get("nb_views")
                    if nb_views is not None:
                        return int(nb_views)
            
            logger.warning(f"Неожиданная структура ответа от Krisha API: {data}")
            return None
            
    except httpx.HTTPError as e:
        logger.error(f"Ошибка HTTP при парсинге Krisha {krisha_url}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка парсинга JSON от Krisha API: {e}")
        return None
    except Exception as e:
        logger.error(f"Неожиданная ошибка при парсинге Krisha {krisha_url}: {e}", exc_info=True)
        return None


def _tiktok_input_from_url(url: str) -> Optional[Dict]:
    """Определяет тип входа для TikTok-актора (пост или профиль)."""
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    host = parsed.netloc.lower()
    
    # Короткие ссылки (редирект на видео)
    if "vm.tiktok.com" in host or "vt.tiktok.com" in host:
        return {"postURLs": [url]}
    
    # Пример видео: /@user/video/1234567890
    parts = path.split("/")
    if len(parts) >= 3 and parts[1] == "video":
        return {"postURLs": [url]}
    
    # Пример профиля: /@user
    if len(parts) == 1 and parts[0].startswith("@"):
        return {"profiles": [parts[0]]}
    
    # Если домен TikTok, но форма не распознана — шлем как пост
    if "tiktok.com" in host and path:
        return {"postURLs": [url]}
    
    return None


async def parse_tiktok_stats(tiktok_url: str) -> Optional[Dict[str, int]]:
    """
    Парсит статистику TikTok поста через Apify
    
    Args:
        tiktok_url: Ссылка на TikTok пост
        
    Returns:
        Словарь с ключами: diggCount, playCount, commentCount, collectCount
        или None в случае ошибки
    """
    try:
        token = os.getenv("APIFY_API_TOKEN")
        if not token:
            logger.error("Переменная окружения APIFY_API_TOKEN не задана")
            return None
        
        actor_input = _tiktok_input_from_url(tiktok_url)
        if actor_input is None:
            logger.warning(f"TikTok ссылка не распознана: {tiktok_url}")
            return None
        
        # Запускаем актор в отдельном потоке (ApifyClient синхронный)
        # Используем asyncio.to_thread для запуска синхронного кода
        def run_tiktok_scraper():
            with suppress_stdout_stderr():
                client = ApifyClient(token)
                run = client.actor("clockworks/free-tiktok-scraper").call(run_input=actor_input)
                dataset = client.dataset(run["defaultDatasetId"])
                items = dataset.list_items().items
                return items
        
        items = await asyncio.to_thread(run_tiktok_scraper)
        
        if not items:
            logger.warning(f"Не получены данные от TikTok актора для {tiktok_url}")
            return None
        
        # Берем первый элемент (для одного поста должен быть один результат)
        item = items[0]
        
        stats = {
            "diggCount": item.get("diggCount", 0),
            "playCount": item.get("playCount", 0),
            "commentCount": item.get("commentCount", 0),
            "collectCount": item.get("collectCount", 0),
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге TikTok {tiktok_url}: {e}", exc_info=True)
        return None


async def parse_instagram_stats(instagram_url: str) -> Optional[Dict[str, int]]:
    """
    Парсит статистику Instagram поста через Apify
    
    Args:
        instagram_url: Ссылка на Instagram пост
        
    Returns:
        Словарь с ключами: commentsCount, likesCount, videoPlayCount
        или None в случае ошибки
    """
    try:
        token = os.getenv("APIFY_API_TOKEN")
        if not token:
            logger.error("Переменная окружения APIFY_API_TOKEN не задана")
            return None
        
        actor_name = os.getenv("APIFY_INSTAGRAM_ACTOR", "apify/instagram-scraper")
        actor_input = {"directUrls": [instagram_url]}
        
        # Запускаем актор в отдельном потоке (ApifyClient синхронный)
        # Используем asyncio.to_thread для запуска синхронного кода
        def run_instagram_scraper():
            with suppress_stdout_stderr():
                client = ApifyClient(token)
                run = client.actor(actor_name).call(run_input=actor_input)
                dataset = client.dataset(run["defaultDatasetId"])
                items = dataset.list_items().items
                return items
        
        items = await asyncio.to_thread(run_instagram_scraper)
        
        if not items:
            logger.warning(f"Не получены данные от Instagram актора для {instagram_url}")
            return None
        
        # Берем первый элемент (для одного поста должен быть один результат)
        item = items[0]
        
        stats = {
            "commentsCount": item.get("commentsCount", 0),
            "likesCount": item.get("likesCount", 0),
            "videoPlayCount": item.get("videoPlayCount", 0),
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"Ошибка при парсинге Instagram {instagram_url}: {e}", exc_info=True)
        return None


async def parse_all_links_analytics(
    krisha_links: List[str],
    instagram_links: List[str],
    tiktok_links: List[str]
) -> Dict[str, Any]:
    """
    Парсит аналитику для всех переданных ссылок
    
    Args:
        krisha_links: Список ссылок Krisha.kz
        instagram_links: Список ссылок Instagram
        tiktok_links: Список ссылок TikTok
        
    Returns:
        Словарь с результатами парсинга:
        {
            "krisha": {"views": int, "urls_processed": int},
            "instagram": {"comments": int, "likes": int, "views": int, "urls_processed": int},
            "tiktok": {"likes": int, "views": int, "comments": int, "saves": int, "urls_processed": int}
        }
    """
    result = {
        "krisha": {"views": 0, "urls_processed": 0},
        "instagram": {"comments": 0, "likes": 0, "views": 0, "urls_processed": 0},
        "tiktok": {"likes": 0, "views": 0, "comments": 0, "saves": 0, "urls_processed": 0}
    }
    
    # Парсим Krisha ссылки
    if krisha_links:
        krisha_tasks = [parse_krisha_views(url) for url in krisha_links]
        krisha_results = await asyncio.gather(*krisha_tasks, return_exceptions=True)
        
        for views in krisha_results:
            if isinstance(views, Exception):
                logger.warning(f"Ошибка при парсинге Krisha: {views}")
                continue
            if views is not None:
                result["krisha"]["views"] += views
                result["krisha"]["urls_processed"] += 1
    
    # Парсим Instagram ссылки
    if instagram_links:
        instagram_tasks = [parse_instagram_stats(url) for url in instagram_links]
        instagram_results = await asyncio.gather(*instagram_tasks, return_exceptions=True)
        
        for stats in instagram_results:
            if isinstance(stats, Exception):
                logger.warning(f"Ошибка при парсинге Instagram: {stats}")
                continue
            if stats:
                result["instagram"]["comments"] += stats.get("commentsCount", 0)
                result["instagram"]["likes"] += stats.get("likesCount", 0)
                result["instagram"]["views"] += stats.get("videoPlayCount", 0)
                result["instagram"]["urls_processed"] += 1
    
    # Парсим TikTok ссылки
    if tiktok_links:
        tiktok_tasks = [parse_tiktok_stats(url) for url in tiktok_links]
        tiktok_results = await asyncio.gather(*tiktok_tasks, return_exceptions=True)
        
        for stats in tiktok_results:
            if isinstance(stats, Exception):
                logger.warning(f"Ошибка при парсинге TikTok: {stats}")
                continue
            if stats:
                result["tiktok"]["likes"] += stats.get("diggCount", 0)
                result["tiktok"]["views"] += stats.get("playCount", 0)
                result["tiktok"]["comments"] += stats.get("commentCount", 0)
                result["tiktok"]["saves"] += stats.get("collectCount", 0)
                result["tiktok"]["urls_processed"] += 1
    
    return result


def format_analytics_text(analytics_data: Dict[str, Any]) -> str:
    """
    Форматирует данные аналитики в структурированный текст для Telegram
    
    Args:
        analytics_data: Результат функции parse_all_links_analytics
        
    Returns:
        Отформатированный текст с эмодзи
    """
    text_parts = []
    
    # Анализ по Krisha KZ
    krisha_data = analytics_data.get("krisha", {})
    if krisha_data.get("urls_processed", 0) > 0:
        text_parts.append("📊 Анализ по Krisha KZ:")
        text_parts.append(f"👁️ Просмотры: {krisha_data.get('views', 0):,}")
        text_parts.append("")
    
    # Анализ по Instagram
    instagram_data = analytics_data.get("instagram", {})
    if instagram_data.get("urls_processed", 0) > 0:
        text_parts.append("📸 Анализ по Instagram:")
        text_parts.append(f"👁️ Просмотры: {instagram_data.get('views', 0):,}")
        text_parts.append(f"❤️ Лайки: {instagram_data.get('likes', 0):,}")
        text_parts.append(f"💬 Комментарии: {instagram_data.get('comments', 0):,}")
        text_parts.append("")
    
    # Анализ по TikTok
    tiktok_data = analytics_data.get("tiktok", {})
    if tiktok_data.get("urls_processed", 0) > 0:
        text_parts.append("🎵 Анализ по TikTok:")
        text_parts.append(f"👁️ Просмотры: {tiktok_data.get('views', 0):,}")
        text_parts.append(f"❤️ Лайки: {tiktok_data.get('likes', 0):,}")
        text_parts.append(f"💬 Комментарии: {tiktok_data.get('comments', 0):,}")
        saves = tiktok_data.get("saves", 0)
        if saves > 0:
            text_parts.append(f"⭐ Сохранено в избранное: {saves:,}")
        text_parts.append("")
    
    if not text_parts:
        return "❌ Нет данных для анализа"
    
    return "\n".join(text_parts).strip()

