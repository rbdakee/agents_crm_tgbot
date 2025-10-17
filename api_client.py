import asyncio
import httpx
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

from config import API_BASE_URL, DEVICE_UUID, AUTH_TOKEN_URL, AUTH_CLIENT_ID, PROFILE_URL
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
        self.auth_token_url = AUTH_TOKEN_URL
        self.auth_client_id = AUTH_CLIENT_ID
        self.profile_url = PROFILE_URL
    
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

    async def get_crm_data_batch(self, crm_ids: List[str], batch_size: int = 200) -> Dict[str, Dict]:
        """Получает данные из CRM API батчами с параллельными запросами
        
        Args:
            crm_ids: Список CRM ID для получения данных
            batch_size: Размер батча (по умолчанию 200)
            
        Returns:
            Словарь {crm_id: {address, complex, price}} с данными из API
        """
        result = {}
        
        # Разбиваем на батчи
        total_batches = (len(crm_ids) + batch_size - 1) // batch_size
        logger.info(f"Получение данных из CRM API: {len(crm_ids)} записей, {total_batches} батчей по {batch_size}")
        
        for batch_index in range(total_batches):
            start_idx = batch_index * batch_size
            end_idx = min(start_idx + batch_size, len(crm_ids))
            batch_crm_ids = crm_ids[start_idx:end_idx]
            
            logger.info(f"Обрабатываю батч {batch_index + 1}/{total_batches} ({len(batch_crm_ids)} записей)")
            
            # Создаем задачи для параллельных запросов
            tasks = []
            for crm_id in batch_crm_ids:
                task = self._fetch_single_crm_data(crm_id)
                tasks.append(task)
            
            # Выполняем все запросы параллельно
            try:
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Обрабатываем результаты
                for i, result_data in enumerate(batch_results):
                    crm_id = batch_crm_ids[i]
                    if isinstance(result_data, Exception):
                        logger.error(f"Ошибка получения данных для {crm_id}: {result_data}")
                        result[crm_id] = {"address": "", "complex": "", "price": None}
                    elif result_data:
                        result[crm_id] = result_data
                    else:
                        result[crm_id] = {"address": "", "complex": "", "price": None}
                
                logger.info(f"Батч {batch_index + 1} завершен: {len(batch_results)} записей")
                
                # Небольшая пауза между батчами, чтобы не перегружать API
                if batch_index < total_batches - 1:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке батча {batch_index + 1}: {e}")
                # Заполняем пустыми данными для этого батча
                for crm_id in batch_crm_ids:
                    result[crm_id] = {"address": "", "complex": "", "price": None}
        
        logger.info(f"Получение данных из CRM API завершено: {len(result)} записей")
        return result

    async def _fetch_single_crm_data(self, crm_id: str) -> Optional[Dict]:
        """Получает данные для одного CRM ID и возвращает только нужные поля"""
        url = f"{self.base_url}/applications-client/{crm_id}/{self.device_uuid}/"
        
        try:
            response = await self.client.get(url)
            if response.status_code == 200:
                try:
                    data = response.json()
                    return self._extract_crm_fields(data)
                except Exception as json_error:
                    logger.warning(f"Ошибка парсинга JSON для {crm_id}: {json_error}")
                    return {"address": "", "complex": "", "price": None}
            elif response.status_code == 404:
                logger.debug(f"CRM ID {crm_id} не найден (404)")
                return {"address": "", "complex": "", "price": None}
            else:
                logger.warning(f"API request failed for {crm_id} with status {response.status_code}")
                return {"address": "", "complex": "", "price": None}
        except Exception as e:
            logger.warning(f"Error fetching data for {crm_id}: {e}")
            return {"address": "", "complex": "", "price": None}

    def _extract_crm_fields(self, json_data: Dict) -> Dict:
        """Извлекает только нужные поля (address, complex, price) из JSON ответа API"""
        try:
            # Проверяем базовую структуру
            if not json_data:
                logger.debug("API ответ пустой")
                return {"address": "", "complex": "", "price": None}
            
            # Проверяем наличие поля success
            if not json_data.get('success', False):
                logger.debug(f"API вернул success=False: {json_data}")
                return {"address": "", "complex": "", "price": None}
            
            # Проверяем наличие поля data
            if 'data' not in json_data:
                logger.debug(f"API ответ не содержит поле 'data': {json_data}")
                return {"address": "", "complex": "", "price": None}
            
            data = json_data['data']
            if not data:
                logger.debug("Поле 'data' пустое")
                return {"address": "", "complex": "", "price": None}
            
            # Цена из sellDataDto
            sell_data = data.get('sellDataDto')
            price = None
            if sell_data and isinstance(sell_data, dict):
                price = sell_data.get('objectPrice')
            
            # Данные недвижимости
            real_property = data.get('realPropertyDto')
            complex_name = ""
            address = ""
            
            if real_property and isinstance(real_property, dict):
                complex_data = real_property.get('residentialComplexDto')
                address_data = real_property.get('addressDto')
                
                # Название ЖК
                if complex_data and isinstance(complex_data, dict):
                    complex_name = complex_data.get('houseName', '')
                
                # Формируем адрес
                if address_data and isinstance(address_data, dict):
                    address_parts = []
                    
                    city = address_data.get('city')
                    if city and isinstance(city, dict):
                        city_name = city.get('nameRu')
                        if city_name:
                            address_parts.append(city_name)
                    
                    district = address_data.get('district')
                    if district and isinstance(district, dict):
                        district_name = district.get('nameRu')
                        if district_name:
                            address_parts.append(district_name)
                    
                    street = address_data.get('street')
                    if street and isinstance(street, dict):
                        street_name = street.get('nameRu')
                        if street_name:
                            building = address_data.get('building', '')
                            if building:
                                street_name += f" {building}"
                            address_parts.append(street_name)
                    
                    apartment = real_property.get('apartmentNumber')
                    if apartment:
                        address_parts.append(f"кв {apartment}")
                    
                    address = ", ".join(address_parts)
            
            return {
                "address": address,
                "complex": complex_name,
                "price": price
            }
            
        except Exception as e:
            logger.error(f"Ошибка извлечения полей из API ответа: {e}")
            logger.debug(f"Проблемный JSON: {json_data}")
            return {"address": "", "complex": "", "price": None}

    async def login_and_get_profile(self, username: str, password: str) -> Optional[Dict]:
        """Получает токен по логину/паролю и затем профиль пользователя.

        Требования:
        - username: 10-значный номер (без 8/7/+7 в начале)
        - grant_type=password, client_id=htc
        - Тело: x-www-form-urlencoded
        - Далее GET профиль с заголовком Authorization: Bearer <token>
        """
        try:
            # Нормализуем логин (оставляем 10 цифр без префикса страны)
            digits = ''.join(ch for ch in username if ch.isdigit())
            if len(digits) == 11 and digits.startswith('7'):
                digits = digits[1:]
            elif len(digits) == 11 and digits.startswith('8'):
                digits = digits[1:]
            # ожидаем 10 цифр
            if len(digits) != 10:
                raise ValueError("username должен быть 10-значным номером телефона")

            # 1) Получаем токен
            token_resp = await self.client.post(
                self.auth_token_url,
                data={
                    'username': digits,
                    'password': password,
                    'grant_type': 'password',
                    'client_id': self.auth_client_id,
                },
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            )
            if token_resp.status_code != 200:
                logger.error(f"Auth failed: {token_resp.status_code} {token_resp.text}")
                return None
            token_json = token_resp.json()
            access_token = token_json.get('access_token')
            if not access_token:
                logger.error("Auth failed: access_token not found")
                return None

            # 2) Запрашиваем профиль
            profile_resp = await self.client.get(
                self.profile_url,
                headers={'Authorization': f'Bearer {access_token}'}
            )
            if profile_resp.status_code != 200:
                logger.error(f"Profile request failed: {profile_resp.status_code} {profile_resp.text}")
                return None

            profile = profile_resp.json()
            # Возвращаем только нужные поля и сам токен на будущее
            return {
                'token': access_token,
                'phone': profile.get('phone'),
                'name': profile.get('name'),
                'surname': profile.get('surname'),
                'userId': profile.get('userId'),
                'raw': profile,
            }
        except Exception as e:
            logger.error(f"Login/profile error: {e}")
            return None
    
    def _parse_application_data(self, json_data: Dict) -> ApplicationData:
        """Парсит JSON данные в структуру ApplicationData"""
        
        # Проверяем структуру данных
        if not json_data or 'data' not in json_data:
            raise ValueError("Invalid JSON structure: missing 'data' field")
        
        data = json_data['data']
        if not data:
            raise ValueError("Invalid JSON structure: 'data' field is empty")
        
        # Основные данные
        crm_id = str(data.get('id', ''))
        if not crm_id:
            raise ValueError("Missing CRM ID in data")
        
        sell_data = data.get('sellDataDto')
        if not sell_data:
            raise ValueError("Missing sellDataDto in data")
        
        price = sell_data.get('objectPrice', 0)
        
        # Данные недвижимости
        real_property = data.get('realPropertyDto')
        if not real_property:
            raise ValueError("Missing realPropertyDto in data")
        
        complex_data = real_property.get('residentialComplexDto', {})
        address_data = real_property.get('addressDto', {})
        
        complex_name = complex_data.get('houseName', 'Неизвестный ЖК')
        
        # Формируем адрес с проверками
        street_name = address_data.get('street', {}).get('nameRu', 'Неизвестная улица')
        building = address_data.get('building', '')
        apartment = real_property.get('apartmentNumber', '')
        address = f"{street_name} дом {building}, кв {apartment}" if building and apartment else street_name
        
        area_sqm = real_property.get('totalArea', 0)
        floor = real_property.get('floor', 0)
        rooms = real_property.get('numberOfRooms', 0)
        housing_class = complex_data.get('housingClass') or 'Комфорт'
        
        # Агент
        agent = data.get('agentDto', {})
        agent_name = agent.get('name', '')
        agent_surname = agent.get('surname', '')
        agent_phone = self.format_phone(agent.get('phone', ''))
        
        # Клиент (пока пустой, нужно будет получать отдельно)
        client_name = ""
        
        # Достоинства
        benefits = self._extract_benefits(complex_data, real_property)
        
        # Фотографии
        photo_ids = real_property.get('photoIdList', [])
        
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
