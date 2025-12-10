import re
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime

from src.utils.geo import extract_locations_from_text, extract_price_from_text, get_coordinates, get_coordinates_async
from src.config import ORDER_KEYWORDS, REGIONS, CLOSED_MARKERS, KNOWN_CITIES, CITY_ALIASES, CITY_DECLENSIONS

logger = logging.getLogger(__name__)

NOT_CITIES = {
    'мин', 'час', 'чел', 'человек', 'человека', 'пассажир', 'пассажира', 'пассажиров',
    'руб', 'рубль', 'рублей', 'тыс', 'место', 'места', 'багаж', 'багажа',
    'сегодня', 'завтра', 'вчера', 'утро', 'день', 'вечер', 'ночь', 'утром', 'вечером', 'днём', 'ночью',
    'январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь',
    'пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс', 'понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье',
    'срочно', 'свободно', 'занято', 'закрыт', 'закрыто', 'открыт', 'открыто',
    'такси', 'водитель', 'машина', 'авто', 'автомобиль', 'междугороднее', 'межгород',
    'цена', 'стоимость', 'торг', 'договорная', 'договор',
    'комфорт', 'эконом', 'бизнес', 'премиум',
    'детское', 'кресло', 'животное', 'питомец',
    'заказ', 'заявка', 'бронь', 'бронирование',
    'туда', 'обратно', 'попутно',
    'нал', 'наличные', 'карта', 'перевод', 'оплата',
    'трансфер', 'нсфер', 'сфер', 'границ', 'без-границ', 'безграниц',
    'империя', 'премьер', 'минивен', 'отель', 'санкт', 'петербург',
    'россия', 'область', 'край', 'республика', 'округ',
    'сибирское',
    'жд', 'аэропорт', 'вокзал', 'станция', 'остановка',
    'стоимость', 'цена', 'услуга', 'услуги', 'сервис',
}

def is_valid_city_name(name: str) -> bool:
    if not name:
        return False
    
    name_lower = name.lower().strip()
    
    if len(name_lower) < 3:
        return False
    
    if name_lower in NOT_CITIES:
        return False
    
    if name_lower.isdigit():
        return False
    
    if re.match(r'^\d', name_lower):
        return False
    
    if re.match(r'^[+-]?\d+([.,]\d+)?$', name_lower):
        return False
    
    known_cities_lower = {c.lower() for c in KNOWN_CITIES}
    if name_lower in known_cities_lower:
        return True
    
    if name_lower in CITY_ALIASES:
        return True
    
    if name_lower in CITY_DECLENSIONS:
        return True
    
    coords = get_coordinates(name)
    if coords:
        return True
    
    logger.debug(f"Invalid city name rejected: {name}")
    return False

@dataclass
class ParsedOrder:
    point_a: str
    point_b: str
    price: Optional[int]
    original_text: str
    source_group: str
    source_link: str
    region: Optional[str]
    point_a_coords: Optional[tuple]
    point_b_coords: Optional[tuple]
    timestamp: str
    group_title: Optional[str] = None
    source_group_id: Optional[int] = None
    message_id: Optional[int] = None
    author_id: Optional[int] = None
    author_username: Optional[str] = None
    author_first_name: Optional[str] = None

def is_closed_order(text: str) -> bool:
    text_lower = text.lower()
    for marker in CLOSED_MARKERS:
        if marker.lower() in text_lower:
            return True
    return False

def is_order_message(text: str) -> bool:
    if not text:
        return False
    
    if is_closed_order(text):
        return False
    
    text_lower = text.lower()
    
    for keyword in ORDER_KEYWORDS:
        if keyword.lower() in text_lower:
            return True
    
    location_patterns = [
        r'[А-Яа-яЁё]+\s*[-–—→>]\s*[А-Яа-яЁё]+',
        r'(?:из|от|с)\s+[А-Яа-яЁё]+\s+(?:в|до|на)\s+[А-Яа-яЁё]+',
    ]
    
    for pattern in location_patterns:
        if re.search(pattern, text):
            return True
    
    return False

def detect_region(text: str, point_a: Optional[str] = None, point_b: Optional[str] = None) -> Optional[str]:
    search_text = f"{text} {point_a or ''} {point_b or ''}"
    
    for region_key, region_names in REGIONS.items():
        for name in region_names:
            if name.lower() in search_text.lower():
                return region_key
    
    return None

def _make_telegram_link(group_id: str, message_id: int, group_username: Optional[str] = None) -> str:
    if group_username:
        return f"https://t.me/{group_username}/{message_id}"
    group_id_int = abs(int(group_id))
    if group_id_int > 1000000000000:
        channel_id = group_id_int - 1000000000000
    else:
        channel_id = group_id_int
    return f"https://t.me/c/{channel_id}/{message_id}"

def parse_order(text: str, source_group: str, message_id: int, group_username: Optional[str] = None) -> Optional[ParsedOrder]:
    if not is_order_message(text):
        return None
    
    point_a, point_b = extract_locations_from_text(text)
    
    if not point_a or not point_b:
        return None
    
    price = extract_price_from_text(text)
    
    region = detect_region(text, point_a, point_b)
    
    point_a_coords = get_coordinates(point_a)
    point_b_coords = get_coordinates(point_b)
    
    source_link = _make_telegram_link(source_group, message_id, group_username)
    
    return ParsedOrder(
        point_a=point_a,
        point_b=point_b,
        price=price,
        original_text=text,
        source_group=source_group,
        source_link=source_link,
        region=region,
        point_a_coords=point_a_coords,
        point_b_coords=point_b_coords,
        timestamp=datetime.now().isoformat()
    )

async def parse_order_async(text: str, source_group: str, message_id: int, group_username: Optional[str] = None, group_title: Optional[str] = None, author_id: Optional[int] = None, author_username: Optional[str] = None, author_first_name: Optional[str] = None) -> Optional[ParsedOrder]:
    if not is_order_message(text):
        return None
    
    point_a, point_b = extract_locations_from_text(text)
    price = extract_price_from_text(text)
    
    if not point_a or not point_b:
        try:
            from src.parser.ai_parser import extract_order_with_ai, is_ai_available
            if is_ai_available():
                logger.info(f"Using AI fallback for: {text[:50]}...")
                ai_point_a, ai_point_b, ai_price = extract_order_with_ai(text)
                if ai_point_a and ai_point_b:
                    if is_valid_city_name(ai_point_a) and is_valid_city_name(ai_point_b):
                        point_a = ai_point_a
                        point_b = ai_point_b
                        if ai_price and not price:
                            price = ai_price
                        logger.info(f"AI extracted valid cities: {point_a} -> {point_b}")
                    else:
                        logger.info(f"AI extracted invalid cities: {ai_point_a} -> {ai_point_b} - rejected")
        except Exception as e:
            logger.error(f"AI parsing failed: {e}")
    
    if not point_a or not point_b:
        logger.debug(f"Order rejected - no valid cities found in: {text[:100]}...")
        return None
    
    if not is_valid_city_name(point_a) or not is_valid_city_name(point_b):
        logger.info(f"Order rejected - invalid city names: {point_a} -> {point_b}")
        return None
    
    region = detect_region(text, point_a, point_b)
    
    point_a_coords = await get_coordinates_async(point_a)
    point_b_coords = await get_coordinates_async(point_b)
    
    source_link = _make_telegram_link(source_group, message_id, group_username)
    
    return ParsedOrder(
        point_a=point_a,
        point_b=point_b,
        price=price,
        original_text=text,
        source_group=source_group,
        source_link=source_link,
        region=region,
        point_a_coords=point_a_coords,
        point_b_coords=point_b_coords,
        timestamp=datetime.now().isoformat(),
        group_title=group_title,
        message_id=message_id,
        author_id=author_id,
        author_username=author_username,
        author_first_name=author_first_name
    )

def format_order_message(order: ParsedOrder, distance_km: float = 0, group_title: str = None, group_links: list = None, group_id: int = None, is_favorite: bool = False) -> str:
    import urllib.parse
    from src.utils.database import is_service_group
    
    point_a_url = f"https://yandex.ru/maps/?text={urllib.parse.quote(order.point_a)}"
    
    group_name = group_title or "Источник"
    
    author_section = ""
    if order.author_id:
        if order.author_username:
            author_link = f"https://t.me/{order.author_username}"
            author_section = f"Заказ выложил:\n<a href=\"{author_link}\">@{order.author_username}</a>\n"
        else:
            author_name = order.author_first_name or "Автор"
            author_link = f"tg://user?id={order.author_id}"
            author_section = f"Заказ выложил:\n<a href=\"{author_link}\">{author_name}</a>\n"
    
    if group_links and len(group_links) > 0:
        groups_section = "Заказ выложен в группах:\n"
        has_service_group = False
        for link in group_links:
            title = link.get('group_title', 'Группа') if isinstance(link, dict) else getattr(link, 'group_title', 'Группа')
            url = link.get('source_link', '#') if isinstance(link, dict) else getattr(link, 'source_link', '#')
            link_group_id = link.get('group_id') if isinstance(link, dict) else getattr(link, 'group_id', None)
            is_our = is_service_group(link_group_id) if link_group_id else False
            if is_our:
                has_service_group = True
            badge = " ✅" if is_our else ""
            groups_section += f"➡️ <a href=\"{url}\">{title}</a>{badge}\n"
        if has_service_group:
            groups_section += "✅ = наша группа\n"
    else:
        is_our = is_service_group(group_id) if group_id else False
        badge = " ✅" if is_our else ""
        groups_section = f"Заказ выложен тут:\n➡️ <a href=\"{order.source_link}\">{group_name}</a>{badge}\n"
        if is_our:
            groups_section += "✅ = наша группа\n"
    
    favorite_badge = "⭐ " if is_favorite else ""
    
    message = f"""{favorite_badge}🔊 {order.point_a} - {order.point_b}

{order.original_text}

• <a href="{point_a_url}">Маршрут до точки "А"</a>
──────────────────
{author_section}{groups_section}"""
    return message.strip()
