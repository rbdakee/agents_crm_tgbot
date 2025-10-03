import csv
import logging
import re
import asyncio
from typing import Dict, List, Optional, Tuple
import httpx
from config import N8N_WEBHOOK_URL, AGENTS_FILE, REQUEST_TIMEOUT, MAX_RETRIES
from cache import cache_manager

logger = logging.getLogger(__name__)

class CRMData:
    def __init__(self):
        self.agents: Dict[str, str] = self.load_agents()  # phone -> name
        # Обратная мапа для быстрого поиска телефона по имени
        self.agent_name_to_phone: Dict[str, str] = {name: phone for phone, name in self.agents.items() if name}
        self._client: Optional[httpx.AsyncClient] = None
        self._request_timeout_seconds: float = REQUEST_TIMEOUT
    
    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Нормализует номер телефона к единому формату 7XXXXXXXXXX
        
        Обрабатывает различные форматы:
        - 87777777777 -> 7777777777
        - +77777777777 -> 7777777777  
        - 7777777777 -> 7777777777
        - 8777777777 -> 7777777777 (если 10 цифр)
        """
        if not phone:
            return ""
        
        # Убираем все символы кроме цифр и +
        cleaned = re.sub(r'[^\d+]', '', phone.strip())
        
        # Убираем + если есть
        if cleaned.startswith('+'):
            cleaned = cleaned[1:]
        
        # Если номер начинается с 8, заменяем на 7
        if cleaned.startswith('8'):
            cleaned = '7' + cleaned[1:]
        
        # Если номер начинается с 7 и имеет 11 цифр, оставляем как есть
        if cleaned.startswith('7') and len(cleaned) == 11:
            return cleaned
        
        # Если номер начинается с 7 и имеет 10 цифр, добавляем 7 в начало
        if cleaned.startswith('7') and len(cleaned) == 10:
            return '7' + cleaned
        
        # Если номер не начинается с 7, но имеет 10 цифр, добавляем 7
        if len(cleaned) == 10 and not cleaned.startswith('7'):
            return '7' + cleaned
        
        # Если номер не начинается с 7, но имеет 11 цифр, заменяем первую цифру на 7
        if len(cleaned) == 11 and not cleaned.startswith('7'):
            return '7' + cleaned[1:]
        
        # Если ничего не подошло, возвращаем как есть
        return cleaned
    
    @staticmethod
    def is_valid_phone(phone: str) -> bool:
        """
        Проверяет, является ли номер телефона валидным казахстанским номером
        """
        normalized = CRMData.normalize_phone(phone)
        
        # Проверяем, что номер начинается с 7 и имеет 11 цифр
        if not normalized.startswith('7') or len(normalized) != 11:
            return False
        
        # Проверяем, что все символы - цифры
        if not normalized.isdigit():
            return False
        
        # Проверяем код страны Казахстана (7)
        if not normalized.startswith('77'):
            return False
        
        return True
    
    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._request_timeout_seconds)
        return self._client
    
    def load_agents(self) -> Dict[str, str]:
        """Загружает список агентов из CSV файла с нормализацией номеров телефонов"""
        agents = {}
        invalid_phones = []
        try:
            with open(AGENTS_FILE, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for row_num, row in enumerate(reader, 1):
                    if len(row) >= 2 and row[1].strip():
                        # Убираем лишние пробелы из имени
                        name = ' '.join(row[0].strip().split())
                        raw_phone = row[1].strip()
                        
                        # Нормализуем номер телефона
                        normalized_phone = self.normalize_phone(raw_phone)
                        
                        # Проверяем валидность номера
                        if self.is_valid_phone(normalized_phone):
                            agents[normalized_phone] = name
                        else:
                            invalid_phones.append(f"Строка {row_num}: {name} - {raw_phone}")
                            logger.warning(f"Невалидный номер телефона в строке {row_num}: {raw_phone} для агента {name}")
        
        except FileNotFoundError:
            logger.error(f"Файл {AGENTS_FILE} не найден")
        
        if invalid_phones:
            logger.warning(f"Найдено {len(invalid_phones)} невалидных номеров телефонов:")
            for invalid in invalid_phones:
                logger.warning(f"  {invalid}")
        
        logger.info(f"Загружено {len(agents)} агентов с валидными номерами телефонов")
        return agents
    
    def get_agent_by_phone(self, phone: str) -> Optional[str]:
        """Возвращает имя агента по номеру телефона с нормализацией"""
        if not phone:
            return None
        
        # Нормализуем введенный номер
        normalized_phone = self.normalize_phone(phone)
        
        # Ищем агента по нормализованному номеру
        return self.agents.get(normalized_phone)

    def get_phone_by_agent(self, agent_name: str) -> Optional[str]:
        """Возвращает нормализованный номер телефона по имени агента"""
        phone = self.agent_name_to_phone.get(agent_name)
        if phone:
            return self.normalize_phone(phone)
        return phone
    
    async def get_all_contracts(self) -> List[Dict]:
        """Получает все контракты из n8n (async) с retry логикой"""
        for attempt in range(MAX_RETRIES):
            try:
                client = await self._get_client()
                response = await client.get(N8N_WEBHOOK_URL)
                if response.status_code == 200:
                    return response.json()
                logger.warning(f"Попытка {attempt + 1}/{MAX_RETRIES}: Ошибка получения контрактов: {response.status_code}")
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1}/{MAX_RETRIES}: Ошибка при запросе к n8n: {e}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Все {MAX_RETRIES} попытки исчерпаны")
                    return []
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return []
    
    async def get_contracts_by_agent(self, agent_name: str) -> List[Dict]:
        """Получает контракты конкретного агента"""
        # Нормализуем имя агента (убираем лишние пробелы)
        normalized_agent_name = ' '.join(agent_name.strip().split())
        
        # Проверяем кеш
        cached_contracts = cache_manager.get_agent_contracts(normalized_agent_name)
        if cached_contracts is not None:
            return cached_contracts
        
        # Если кеша нет, загружаем из API
        all_contracts = await self.get_all_contracts()
        agent_contracts = []
        
        for contract in all_contracts:
            mop = contract.get('МОП', '').strip()
            rop = contract.get('РОП', '').strip()
            
            # Нормализуем имена в контракте
            normalized_mop = ' '.join(mop.split()) if mop else ''
            normalized_rop = ' '.join(rop.split()) if rop else ''
            
            if normalized_agent_name == normalized_mop or normalized_agent_name == normalized_rop:
                agent_contracts.append(contract)
        
        # Сохраняем в кеш
        cache_manager.set_agent_contracts(normalized_agent_name, agent_contracts)
        
        return agent_contracts
    
    async def search_contract_by_crm_id(self, crm_id: str, agent_name: str) -> Optional[Dict]:
        """Поиск контракта по CRM ID (только для агента)"""
        # Нормализуем имя агента
        normalized_agent_name = ' '.join(agent_name.strip().split())
        
        # Сначала проверяем кеш
        cached_contract = cache_manager.get_contract_by_crm_id(crm_id, normalized_agent_name)
        if cached_contract is not None:
            return cached_contract
        
        # Если в кеше нет, ищем в загруженных контрактах агента
        agent_contracts = await self.get_contracts_by_agent(agent_name)
        
        for contract in agent_contracts:
            if str(contract.get('CRM ID', '')) == str(crm_id):
                return contract
        
        return None
    
    async def search_contracts_by_client_name(self, client_name: str, agent_name: str) -> List[Dict]:
        """Поиск контрактов по имени клиента (только для агента)"""
        agent_contracts = await self.get_contracts_by_agent(agent_name)
        matching_contracts = []
        
        for contract in agent_contracts:
            client_info = contract.get('Имя клиента и номер', '').lower()
            if client_name.lower() in client_info:
                matching_contracts.append(contract)
        
        return matching_contracts
    
    async def update_contract(self, crm_id: str, updates: Dict) -> bool:
        """Обновляет контракт через n8n (async), стараясь не делать лишний GET"""
        try:
            # 1) Пытаемся получить контракт из кеша (любой агент)
            current_contract: Optional[Dict] = cache_manager.find_contract_globally(crm_id)
            
            # 2) Если в кеше не нашли — делаем один запрос к API
            if current_contract is None:
                all_contracts = await self.get_all_contracts()
                for contract in all_contracts:
                    if str(contract.get('CRM ID', '')) == str(crm_id):
                        current_contract = contract
                        break
            
            if not current_contract:
                logger.error(f"Контракт с CRM ID {crm_id} не найден")
                return False
            
            # Подготавливаем данные для обновления
            update_data = {
                # "signingDate": current_contract.get('Дата подписания', ''),
                # "contractNumber": current_contract.get('Номер договора', ''),
                # "mop": current_contract.get('МОП', ''),
                # "rop": current_contract.get('РОП', ''),
                "crmId": str(crm_id),
                # "clientNameAndPhone": current_contract.get('Имя клиента и номер', ''),
                # "address": current_contract.get('Адрес', ''),
                # "contractPrice": current_contract.get('Цена указанная в договоре', ''),
                # "expires": current_contract.get('Истекает', ''),
                "collage": current_contract.get('Коллаж', False),
                # "photoSession": current_contract.get('Фотосессия', False),
                # "photo": current_contract.get('Фото', False),
                # "video": current_contract.get('Видео', False),
                # "link": current_contract.get('Ссылка', ''),
                "updatedCollage": current_contract.get('Обновленный колаж', False),
                "krishaUpload": current_contract.get('Загрузка на крышу', ''),
                "instagram": current_contract.get('Инстаграм', ''),
                "tikTok": current_contract.get('Тик ток', ''),
                "mailing": current_contract.get('Рассылка', ''),
                "stream": current_contract.get('Стрим', ''),
                "show": current_contract.get('Показ', 0),
                "analytics": current_contract.get('Аналитика', False),
                "priceAdjustment": current_contract.get('Корректировка цены', ''),
                "analyticsIn5Days": current_contract.get('Предоставление Аналитики через 5 дней', False),
                "pricePush": current_contract.get('Дожим на новую цену', False),
                "priceUpdateKrisha": current_contract.get('Обновление цены на крыше', ''),
                "priceUpdateInstagram": current_contract.get('Обновление цены в инстаграм', ''),
                "priceUpdateTikTok": current_contract.get('Обновление цены в Тик ток', ''),
                "priceUpdateMailing": current_contract.get('Обновление цены в рассылка', ''),
                "priceUpdateStream": current_contract.get('Обновление цены в Стрим', ''),
                # объединенный статус (нормализация пустого в 'Размещено')
                "status": (
                    (current_contract.get('Статус объекта') or current_contract.get('Статус') or 'Размещено').strip()
                    if isinstance((current_contract.get('Статус объекта') or current_contract.get('Статус') or 'Размещено'), str)
                    else (current_contract.get('Статус объекта') or current_contract.get('Статус') or 'Размещено')
                )
            }
            
            # Применяем обновления
            update_data.update(updates)
            
            # Отправляем обновление
            client = await self._get_client()
            response = await client.post(N8N_WEBHOOK_URL, json=update_data)
            if response.status_code == 200:
                # Обновляем кеш
                normalized_agent_name = ' '.join(current_contract.get('МОП', '').strip().split())
                if not normalized_agent_name:
                    normalized_agent_name = ' '.join(current_contract.get('РОП', '').strip().split())
                
                if normalized_agent_name:
                    cache_manager.update_contract_in_cache(crm_id, normalized_agent_name, updates)
                
                return True
            return False
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении контракта: {e}")
            return False
    
    async def refresh_agent_cache(self, agent_name: str):
        """Принудительно обновляет кеш для конкретного агента (async)"""
        try:
            # Получаем свежие данные из API
            all_contracts = await self.get_all_contracts()
            
            # Фильтруем контракты для агента
            agent_contracts = []
            for contract in all_contracts:
                mop_name = ' '.join(contract.get('МОП', '').strip().split())
                rop_name = ' '.join(contract.get('РОП', '').strip().split())
                
                if mop_name == agent_name or rop_name == agent_name:
                    agent_contracts.append(contract)
            
            # Обновляем кеш
            cache_manager.set_agent_contracts(agent_name, agent_contracts)
            logger.info(f"Кеш принудительно обновлен для агента {agent_name}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении кеша агента {agent_name}: {e}")
            return False

    async def close(self):
        if self._client:
            await self._client.aclose()

# Глобальный экземпляр CRM
crm = CRMData()
