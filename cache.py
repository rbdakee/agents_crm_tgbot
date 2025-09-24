import json
import time
import os
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, cache_file: str = "data/cache.json"):
        self.cache_file = cache_file
        self.cache_ttl = 30 * 60  # 30 минут в секундах
    
    def _load_cache(self) -> Dict:
        """Загружает кеш из файла"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Ошибка загрузки кеша: {e}")
        return {}
    
    def _save_cache(self, cache_data: Dict):
        """Сохраняет кеш в файл"""
        try:
            os.makedirs(os.path.dirname(self.cache_file) or '.', exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения кеша: {e}")
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """Проверяет, действителен ли кеш"""
        return time.time() - timestamp < self.cache_ttl
    
    def get_agent_contracts(self, agent_name: str) -> Optional[List[Dict]]:
        """Получает кешированные контракты агента"""
        cache = self._load_cache()
        
        if agent_name in cache:
            data = cache[agent_name]
            if self._is_cache_valid(data['timestamp']):
                logger.info(f"Используем кеш для агента {agent_name}")
                return data['contracts']
            else:
                # Удаляем устаревший кеш
                del cache[agent_name]
                self._save_cache(cache)
                logger.info(f"Кеш для агента {agent_name} устарел, удален")
        
        return None
    
    def set_agent_contracts(self, agent_name: str, contracts: List[Dict]):
        """Сохраняет контракты агента в кеш"""
        cache = self._load_cache()
        # Индексируем по CRM ID для мгновенного доступа
        by_crm_id = {}
        for contract in contracts:
            crm_id_value = contract.get('CRM ID', '')
            if crm_id_value is not None:
                by_crm_id[str(crm_id_value)] = contract
        cache[agent_name] = {
            'contracts': contracts,
            'by_crm_id': by_crm_id,
            'timestamp': time.time()
        }
        self._save_cache(cache)
        logger.info(f"Кеш обновлен для агента {agent_name}")
    
    def get_contract_by_crm_id(self, crm_id: str, agent_name: str) -> Optional[Dict]:
        """Получает контракт по CRM ID из кеша"""
        cache = self._load_cache()
        data = cache.get(agent_name)
        if data and self._is_cache_valid(data.get('timestamp', 0)):
            by_crm_id = data.get('by_crm_id')
            if isinstance(by_crm_id, dict):
                contract = by_crm_id.get(str(crm_id))
                if contract:
                    return contract
            # Фолбэк на линейный поиск, если индекса нет
            for contract in data.get('contracts', []):
                if str(contract.get('CRM ID', '')) == str(crm_id):
                    return contract
        return None
    
    def update_contract_in_cache(self, crm_id: str, agent_name: str, updates: Dict):
        """Обновляет контракт в кеше"""
        cache = self._load_cache()
        data = cache.get(agent_name)
        if not data or not self._is_cache_valid(data.get('timestamp', 0)):
            return
        contracts = data.get('contracts', [])
        by_crm_id = data.get('by_crm_id', {})
        contract = by_crm_id.get(str(crm_id))
        if not contract:
            for c in contracts:
                if str(c.get('CRM ID', '')) == str(crm_id):
                    contract = c
                    break
        if not contract:
            return
        # Обновляем поля в кеше
        for key, value in updates.items():
            field_mapping = {
                'collage': 'Коллаж',
                'updatedCollage': 'Обновленный колаж',
                'show': 'Показ',
                'analytics': 'Аналитика',
                'analyticsIn5Days': 'Предоставление Аналитики через 5 дней',
                'priceAdjustment': 'Корректировка цены',
                'status': 'Статус объекта',
                'pricePush': 'Дожим на новую цену',
                'krishaUpload': 'Загрузка на крышу',
                'instagram': 'Инстаграм',
                'tikTok': 'Тик ток',
                'mailing': 'Рассылка',
                'stream': 'Стрим',
                'priceUpdateKrisha': 'Обновление цены на крыше',
                'priceUpdateInstagram': 'Обновление цены в инстаграм',
                'priceUpdateTikTok': 'Обновление цены в Тик ток',
                'priceUpdateMailing': 'Обновление цены в рассылка',
                'priceUpdateStream': 'Обновление цены в Стрим'
            }
            if key in field_mapping:
                contract[field_mapping[key]] = value
        # Обновляем индекс
        by_crm_id[str(crm_id)] = contract
        data['contracts'] = contracts
        data['by_crm_id'] = by_crm_id
        cache[agent_name] = data
        self._save_cache(cache)
        logger.info(f"Кеш обновлен для контракта {crm_id}")
    
    def clear_expired_cache(self):
        """Очищает устаревший кеш"""
        cache = self._load_cache()
        current_time = time.time()
        expired_agents = []
        
        for agent_name, data in cache.items():
            if current_time - data['timestamp'] > self.cache_ttl:
                expired_agents.append(agent_name)
        
        for agent_name in expired_agents:
            del cache[agent_name]
            logger.info(f"Удален устаревший кеш для агента {agent_name}")
        
        if expired_agents:
            self._save_cache(cache)

    def find_contract_globally(self, crm_id: str) -> Optional[Dict]:
        """Ищет контракт по CRM ID во всём кеше (любой агент)"""
        cache = self._load_cache()
        for agent_name, data in cache.items():
            timestamp = data.get('timestamp', 0)
            if not self._is_cache_valid(timestamp):
                continue
            by_crm_id = data.get('by_crm_id', {})
            if isinstance(by_crm_id, dict) and str(crm_id) in by_crm_id:
                return by_crm_id[str(crm_id)]
            # Фолбэк линейного поиска
            for contract in data.get('contracts', []):
                if str(contract.get('CRM ID', '')) == str(crm_id):
                    return contract
        return None

# Глобальный экземпляр кеш-менеджера
cache_manager = CacheManager()
