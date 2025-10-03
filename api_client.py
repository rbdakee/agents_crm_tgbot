import asyncio
import httpx
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

from config import API_BASE_URL, DEVICE_UUID
from collage import CollageInput

logger = logging.getLogger(__name__)


@dataclass
class ApplicationData:
    """Структура данных заявки из API"""
    crm_id: str
    price: int
    complex_name: str
    address: str
    area_sqm: int
    floor: int
    rooms: int
    housing_class: str
    agent_name: str
    agent_surname: str
    agent_phone: str
    client_name: str
    benefits: List[str]
    photo_ids: List[str]


class APIClient:
    """Клиент для работы с API недвижимости"""
    
    def __init__(self):
        self.base_url = API_BASE_URL
        self.device_uuid = DEVICE_UUID
        self.client: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()
    
    async def get_application_data(self, crm_id: str) -> Optional[ApplicationData]:
        """Получает данные заявки по CRM ID"""
        url = f"{self.base_url}/applications-client/{crm_id}/{self.device_uuid}/"
        
        try:
            response = await self.client.get(url)
            if response.status_code == 200:
                data = response.json()
                return self._parse_application_data(data)
            else:
                logger.error(f"API request failed with status {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching application data: {e}")
            return None
    
    def _parse_application_data(self, json_data: Dict) -> ApplicationData:
        """Парсит JSON данные в структуру ApplicationData"""
        
        # Основные данные
        crm_id = str(json_data['data']['id'])
        price = json_data['data']['sellDataDto']['objectPrice']
        
        # Данные недвижимости
        real_property = json_data['data']['realPropertyDto']
        complex_data = real_property['residentialComplexDto']
        address_data = real_property['addressDto']
        
        complex_name = complex_data['houseName']
        address = f"{address_data['street']['nameRu']} дом {address_data['building']}, кв {real_property['apartmentNumber']}"
        area_sqm = real_property['totalArea']
        floor = real_property['floor']
        rooms = real_property['numberOfRooms']
        housing_class = complex_data['housingClass'] or 'Комфорт'
        
        # Агент
        agent = json_data['data']['agentDto']
        agent_name = agent['name']
        agent_surname = agent['surname']
        agent_phone = self.format_phone(agent.get('phone', ''))
        
        # Клиент (пока пустой, нужно будет получать отдельно)
        client_name = ""
        
        # Достоинства
        benefits = self._extract_benefits(complex_data, real_property)
        
        # Фотографии
        photo_ids = real_property['photoIdList']
        
        return ApplicationData(
            crm_id=crm_id,
            price=price,
            complex_name=complex_name,
            address=address,
            area_sqm=area_sqm,
            floor=floor,
            rooms=rooms,
            housing_class=housing_class,
            agent_name=agent_name,
            agent_surname=agent_surname,
            agent_phone=agent_phone,
            client_name=client_name,
            benefits=benefits,
            photo_ids=photo_ids
        )
    
    def _extract_benefits(self, complex_data: Dict, real_property_data: Dict) -> List[str]:
        """Извлекает достоинства из данных комплекса и недвижимости"""
        benefits = []
        
        # Состояние/ремонт квартиры из generalCharacteristicsDto
        general_chars = real_property_data.get('generalCharacteristicsDto', {})
        if general_chars.get('houseCondition'):
            condition_name = general_chars['houseCondition'].get('nameRu')
            if condition_name:
                benefits.append(condition_name)
        
        # Застройщик
        if complex_data.get('propertyDeveloper'):
            benefits.append(f"Застройщик: {complex_data['propertyDeveloper']['nameRu']}")
        
        # Материал
        if complex_data.get('materialOfConstruction'):
            benefits.append(f"Материал: {complex_data['materialOfConstruction']['nameRu']}")
        
        # Год постройки
        if complex_data.get('yearOfConstruction'):
            benefits.append(f"Год постройки: {complex_data['yearOfConstruction']}")
        
        # Парковка
        if complex_data.get('parkingTypeList'):
            parking_types = [p['nameRu'] for p in complex_data['parkingTypeList']]
            benefits.append(f"Парковка: {', '.join(parking_types)}")
        
        # Двор
        if complex_data.get('yardType'):
            benefits.append(f"Двор: {complex_data['yardType']['nameRu']}")
        
        # Лифты
        if complex_data.get('typeOfElevatorList'):
            elevators = [elev['nameRu'] for elev in complex_data['typeOfElevatorList']]
            benefits.append(f"Лифты: {', '.join(elevators)}")
        
        # Детская площадка
        if complex_data.get('playground'):
            benefits.append("Детская площадка")
        
        # Доступность
        if complex_data.get('wheelchair'):
            benefits.append("Доступность для инвалидов")
        
        # Рейтинг
        if complex_data.get('rating'):
            benefits.append(f"Рейтинг ЖК: {complex_data['rating']}")
        
        # Количество квартир
        if complex_data.get('numberOfApartments'):
            benefits.append(f"Квартир в доме: {complex_data['numberOfApartments']}")
        
        # Количество подъездов
        if complex_data.get('numberOfEntrances'):
            benefits.append(f"Подъездов: {complex_data['numberOfEntrances']}")
        
        return benefits
    
    def format_price(self, price) -> str:
        """Форматирует цену для отображения"""
        # Если price уже строка, проверяем есть ли "тг" в конце
        if isinstance(price, str):
            price_str = price.strip()
            if price_str.lower().endswith('тг'):
                return price_str
            # Если это строка с числом, конвертируем в int
            try:
                price = int(float(price_str))
            except (ValueError, TypeError):
                return price_str
        
        # Форматируем число: убираем дробную часть и добавляем пробелы
        if isinstance(price, (int, float)):
            # Убираем дробную часть если она равна 0
            price_int = int(price)
            formatted = f"{price_int:,}".replace(',', ' ')
            return f"{formatted} тг"
        
        # Если не можем обработать, возвращаем как есть
        return str(price)
    
    def format_phone(self, phone: str) -> str:
        """Форматирует номер телефона для отображения"""
        if not phone:
            return ""
        
        # Убираем все символы кроме цифр
        digits_only = ''.join(filter(str.isdigit, phone))
        
        # Если номер начинается с 7 и имеет 11 цифр (российский/казахстанский формат)
        if len(digits_only) == 11 and digits_only.startswith('7'):
            # Форматируем как +7 (XXX) XXX-XX-XX
            return f"+7 ({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:9]}-{digits_only[9:11]}"
        
        # Если номер имеет 10 цифр, добавляем +7
        elif len(digits_only) == 10:
            return f"+7 ({digits_only[0:3]}) {digits_only[3:6]}-{digits_only[6:8]}-{digits_only[8:10]}"
        
        # Для других форматов возвращаем как есть, но с + в начале если его нет
        if not phone.startswith('+'):
            return f"+{phone}"
        
        return phone
    
    def create_collage_input(self, app_data: ApplicationData, photos: List[str] = None) -> CollageInput:
        """Создает CollageInput из данных API"""
        return CollageInput(
            crm_id=app_data.crm_id,
            complex_name=app_data.complex_name,
            address=app_data.address,
            area_sqm=str(app_data.area_sqm),
            floor=str(app_data.floor),
            housing_class=app_data.housing_class,
            price=self.format_price(app_data.price),
            rooms=str(app_data.rooms),
            benefits=app_data.benefits,
            photos=photos or [],
            client_name=app_data.client_name,
            rop=f"{app_data.agent_surname} {app_data.agent_name}",
            agent_phone=app_data.agent_phone,
            action_banner=""
        )


async def get_collage_data_from_api(crm_id: str) -> Optional[CollageInput]:
    """Получает данные для коллажа из API и создает CollageInput"""
    async with APIClient() as client:
        app_data = await client.get_application_data(crm_id)
        if app_data:
            return client.create_collage_input(app_data)
        return None
