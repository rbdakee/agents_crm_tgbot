import json
import time
import os
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class CacheManager:
    def __init__(self, cache_file: str = "cache.json"):
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
        cache[agent_name] = {
            'contracts': contracts,
            'timestamp': time.time()
        }
        self._save_cache(cache)
        logger.info(f"Кеш обновлен для агента {agent_name}")
    
    def get_contract_by_crm_id(self, crm_id: str, agent_name: str) -> Optional[Dict]:
        """Получает контракт по CRM ID из кеша"""
        contracts = self.get_agent_contracts(agent_name)
        if contracts:
            for contract in contracts:
                if str(contract.get('CRM ID', '')) == str(crm_id):
                    return contract
        return None
    
    def update_contract_in_cache(self, crm_id: str, agent_name: str, updates: Dict):
        """Обновляет контракт в кеше"""
        contracts = self.get_agent_contracts(agent_name)
        if contracts:
            for contract in contracts:
                if str(contract.get('CRM ID', '')) == str(crm_id):
                    # Обновляем поля в кеше
                    for key, value in updates.items():
                        # Маппинг полей из API в поля из базы
                        field_mapping = {
                            'collage': 'Коллаж',
                            'updatedCollage': 'Обновленный колаж',
                            'show': 'Показ',
                            'analytics': 'Аналитика',
                            'analyticsIn5Days': 'Предоставление Аналитики через 5 дней',
                            'priceAdjustment': 'Корректировка цены',
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
                    
                    # Сохраняем обновленный кеш
                    self.set_agent_contracts(agent_name, contracts)
                    logger.info(f"Кеш обновлен для контракта {crm_id}")
                    break
    
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

# Глобальный экземпляр кеш-менеджера
cache_manager = CacheManager()
