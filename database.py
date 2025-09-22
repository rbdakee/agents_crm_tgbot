import csv
import logging
from typing import Dict, List, Optional, Tuple
import httpx
from config import N8N_WEBHOOK_URL, AGENTS_FILE
from cache import cache_manager

logger = logging.getLogger(__name__)

class CRMData:
    def __init__(self):
        self.agents: Dict[str, str] = self.load_agents()  # phone -> name
        # Обратная мапа для быстрого поиска телефона по имени
        self.agent_name_to_phone: Dict[str, str] = {name: phone for phone, name in self.agents.items() if name}
        self._client: Optional[httpx.AsyncClient] = None
        self._request_timeout_seconds: float = 15.0
    
    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._request_timeout_seconds)
        return self._client
    
    def load_agents(self) -> Dict[str, str]:
        """Загружает список агентов из CSV файла"""
        agents = {}
        try:
            with open(AGENTS_FILE, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                for row in reader:
                    if len(row) >= 2 and row[1].strip():
                        # Убираем лишние пробелы из имени
                        name = ' '.join(row[0].strip().split())
                        phone = row[1].strip()
                        agents[phone] = name
        except FileNotFoundError:
            logger.error(f"Файл {AGENTS_FILE} не найден")
        return agents
    
    def get_agent_by_phone(self, phone: str) -> Optional[str]:
        """Возвращает имя агента по номеру телефона"""
        return self.agents.get(phone)

    def get_phone_by_agent(self, agent_name: str) -> Optional[str]:
        """Возвращает номер телефона по имени агента"""
        return self.agent_name_to_phone.get(agent_name)
    
    async def get_all_contracts(self) -> List[Dict]:
        """Получает все контракты из n8n (async)"""
        try:
            client = await self._get_client()
            response = await client.get(N8N_WEBHOOK_URL)
            if response.status_code == 200:
                return response.json()
            logger.error(f"Ошибка получения контрактов: {response.status_code}")
            return []
        except Exception as e:
            logger.error(f"Ошибка при запросе к n8n: {e}")
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
                "signingDate": current_contract.get('Дата подписания', ''),
                "contractNumber": current_contract.get('Номер договора', ''),
                "mop": current_contract.get('МОП', ''),
                "rop": current_contract.get('РОП', ''),
                "crmId": str(crm_id),
                "clientNameAndPhone": current_contract.get('Имя клиента и номер', ''),
                "address": current_contract.get('Адрес', ''),
                "contractPrice": current_contract.get('Цена указанная в договоре', ''),
                "expires": current_contract.get('Истекает', ''),
                "collage": current_contract.get('Коллаж', False),
                "photoSession": current_contract.get('Фотосессия', False),
                "photo": current_contract.get('Фото', False),
                "video": current_contract.get('Видео', False),
                "link": current_contract.get('Ссылка', ''),
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
                # объединенный статус
                "status": current_contract.get('Статус объекта', current_contract.get('Статус', 'Размещено'))
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
