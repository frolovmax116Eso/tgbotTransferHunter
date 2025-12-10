import asyncio
from concurrent.futures import ThreadPoolExecutor
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.location import Location
from typing import Optional, Tuple, List, cast
import re
from difflib import SequenceMatcher

geolocator = Nominatim(user_agent="taxi_order_bot")
_executor = ThreadPoolExecutor(max_workers=3)

_geocode_cache: dict = {}

KNOWN_COORDINATES = {
    'солнечная долина': (55.0344, 60.0878),
    'завьялиха': (55.0267, 59.9567),
    'банное': (53.5983, 58.6317),
    'абзаково': (53.8000, 58.6167),
    'металлург-магнитогорск': (53.8033, 58.6200),
    'аджигардак': (54.9500, 58.7833),
    'гора белая': (57.6500, 59.5667),
    'гора ежовая': (57.3000, 59.7000),
    'уктус': (56.7833, 60.6167),
    'роза хутор': (43.6572, 40.2971),
    'красная поляна': (43.6833, 40.2000),
    'газпром': (43.6889, 40.2667),
    'горки город': (43.6600, 40.2700),
    'альпика': (43.6850, 40.2750),
    'шерегеш': (52.9333, 87.9833),
    'гладенькая': (53.3000, 91.5000),
    'бобровый лог': (55.9667, 92.7667),
    'манжерок': (51.8167, 85.7833),
    'белокуриха': (51.9833, 84.9833),
    'архыз': (43.5500, 41.2833),
    'теберда': (43.4500, 41.7333),
    'домбай': (43.2903, 41.6506),
    'эльбрус': (43.4167, 42.5000),
    'аэропорт челябинска': (55.3000, 61.5000),
    'баландино': (55.3000, 61.5000),
    'кольцово': (56.7500, 60.8000),
    'аэропорт екатеринбурга': (56.7500, 60.8000),
    'богучар': (49.9324, 40.5545),
    'россошь': (50.1983, 39.5706),
    'валуйки': (50.2136, 38.0989),
    'шебекино': (50.4067, 36.8933),
    'острогожск': (50.8617, 39.0600),
    'марковка': (49.4511, 39.6083),
    'бугаевка': (49.7833, 39.8500),
    'энем': (44.9231, 38.9071),
    'раевская': (44.8354, 37.5493),
    'бесскорбная': (45.1333, 41.0833),
    'ростов на дону': (47.2222, 39.7198),
    'обнинск': (55.0968, 36.6106),
    'смоленск': (54.7826, 32.0453),
    'минск': (53.9006, 27.5590),
    
    'бердянск': (46.7558, 36.7989),
    'мелитополь': (46.8489, 35.3653),
    'энергодар': (47.4989, 34.6567),
    'токмак': (47.2553, 35.7058),
    'пологи': (47.4833, 36.2500),
    'херсон': (46.6354, 32.6169),
    'новая каховка': (46.7556, 33.3478),
    'каховка': (46.8167, 33.4833),
    'геническ': (46.1750, 34.8167),
    'скадовск': (46.1167, 32.9167),
    
    'санаторий танып': (55.9667, 56.8333),
    'танып': (55.9667, 56.8333),
    
    'тула': (54.1961, 37.6182),
    'курск': (51.7373, 36.1874),
    
    'набережные челны': (55.7167, 52.4167),
    'челны': (55.7167, 52.4167),
    'н.челны': (55.7167, 52.4167),
    'нч': (55.7167, 52.4167),
    
    'казань': (55.7887, 49.1221),
    'уфа': (54.7431, 55.9678),
    'самара': (53.1959, 50.1002),
    'пермь': (58.0105, 56.2502),
    'оренбург': (51.7727, 55.0988),
    'ижевск': (56.8527, 53.2114),
    'саратов': (51.5336, 46.0343),
    'ульяновск': (54.3142, 48.4031),
    'пенза': (53.1959, 45.0183),
}

def get_coordinates(location_name: str) -> Optional[Tuple[float, float]]:
    if location_name in _geocode_cache:
        return _geocode_cache[location_name]
    
    location_lower = location_name.lower().strip()
    if location_lower in KNOWN_COORDINATES:
        coords = KNOWN_COORDINATES[location_lower]
        _geocode_cache[location_name] = coords
        return coords
    
    try:
        search_query = f"{location_name}, Россия"
        result = geolocator.geocode(search_query)
        if result:
            location = cast(Location, result)
            coords = (location.latitude, location.longitude)
            _geocode_cache[location_name] = coords
            return coords
        _geocode_cache[location_name] = None
        return None
    except Exception:
        return None

async def get_coordinates_async(location_name: str) -> Optional[Tuple[float, float]]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, get_coordinates, location_name)


def get_city_by_coordinates(latitude: float, longitude: float) -> Optional[str]:
    """Reverse geocoding - get city name from coordinates"""
    try:
        location = geolocator.reverse((latitude, longitude), language='ru', exactly_one=True)
        if location and location.raw:
            address = location.raw.get('address', {})
            city = address.get('city') or address.get('town') or address.get('village') or address.get('municipality')
            if city:
                return city
            state = address.get('state')
            if state:
                return state
        return None
    except Exception:
        return None


async def get_city_by_coordinates_async(latitude: float, longitude: float) -> Optional[str]:
    """Async reverse geocoding"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, get_city_by_coordinates, latitude, longitude)

def calculate_distance(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    return geodesic(coord1, coord2).kilometers

def is_within_radius(driver_coords: Tuple[float, float], 
                     order_coords: Tuple[float, float], 
                     radius_km: float) -> bool:
    distance = calculate_distance(driver_coords, order_coords)
    return distance <= radius_km

def extract_price_from_text(text: str) -> Optional[int]:
    text_clean = re.sub(r'\d{1,2}[\.\/]\d{1,2}[\.\/]\d{2,4}', '', text)
    text_clean = re.sub(r'\d{1,2}\s*:\s*\d{2}', '', text_clean)
    text_clean = re.sub(r'(?:8|7|\+7)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}', '', text_clean)
    
    comma_match = re.search(r'(\d{1,3})[,](\d{3})\s*(?:руб|₽|р\.?|рублей)?', text_clean, re.IGNORECASE)
    if comma_match:
        try:
            price = int(comma_match.group(1) + comma_match.group(2))
            if 500 <= price <= 500000:
                return price
        except ValueError:
            pass
    
    exact_match = re.search(r'(\d{3,5})\s*(?:руб|₽|р\.?\b)', text_clean, re.IGNORECASE)
    if exact_match:
        try:
            price = int(exact_match.group(1))
            if 500 <= price <= 500000:
                return price
        except ValueError:
            pass
    
    thousands_match = re.search(r'(?<!\d)(\d{1,2})\s*(?:к|тыс|т)\.?\s*(?:руб|₽|р\.?|на руки)?(?!\d)', text_clean, re.IGNORECASE)
    if thousands_match:
        try:
            price = int(thousands_match.group(1)) * 1000
            if 500 <= price <= 500000:
                return price
        except ValueError:
            pass
    
    standalone_match = re.search(r'(?:^|\s)(\d{4,5})(?:\s|$)', text_clean)
    if standalone_match:
        try:
            price = int(standalone_match.group(1))
            if 500 <= price <= 500000:
                return price
        except ValueError:
            pass
    
    return None

def _normalize_city_name(name: str) -> str:
    replacements = {
        'ё': 'е',
        'й': 'и',
    }
    result = name.lower().strip()
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result

def _fuzzy_match_city(text: str, cities: List[str], threshold: float = 0.85) -> Optional[str]:
    text_norm = _normalize_city_name(text)
    
    for city in cities:
        city_norm = _normalize_city_name(city)
        if city_norm == text_norm:
            return city
    
    for city in cities:
        city_norm = _normalize_city_name(city)
        ratio = SequenceMatcher(None, text_norm, city_norm).ratio()
        if ratio >= threshold:
            return city
    
    return None

def _find_city_in_text(text: str, cities: List[str], city_aliases: dict) -> Optional[str]:
    text_lower = text.lower()
    
    for alias, city_name in city_aliases.items():
        if alias.lower() in text_lower:
            return city_name
    
    for city in sorted(cities, key=len, reverse=True):
        if city.lower() in text_lower:
            return city
    
    return None

def _extract_with_ab_pattern(text: str, cities: List[str]) -> Tuple[Optional[str], Optional[str]]:
    ab_patterns = [
        r'(?:откуда|из|от)\s*[:.\-]?\s*([^\n]+?)(?:\n|$).*?(?:куда|в|до)\s*[:.\-]?\s*([^\n]+?)(?:\n|$)',
        r'(?:^|\n)\s*[🚩🏁]?\s*[AАaа]\s*[:.\-]\s*([^\n]+?)(?:\n|$).*?(?:^|\n)\s*[🚩🏁]?\s*[BБбb]\s*[:.\-]\s*([^\n]+?)(?:\n|$)',
        r'(?:точка\s+)?[AАaа]\s*[:.\-]\s*([^/\n]+?)\s*/?\s*(?:точка\s+)?[BБбb]\s*[:.\-]\s*([^\n]+)',
        r'[🚩]\s*[AАaа]\s*[:.\-]?\s*([^\n🏁]+?)\s*[🏁]\s*[BБбb]\s*[:.\-]?\s*([^\n]+)',
    ]
    
    for ab_pattern in ab_patterns:
        match = re.search(ab_pattern, text, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if match:
            point_a_raw = match.group(1).strip().rstrip(',').rstrip('/')
            point_b_raw = match.group(2).strip().rstrip(',').rstrip('/')
            
            point_a = None
            point_b = None
            
            for city in sorted(cities, key=len, reverse=True):
                if city.lower() in point_a_raw.lower():
                    point_a = city
                    break
            
            for city in sorted(cities, key=len, reverse=True):
                if city.lower() in point_b_raw.lower():
                    point_b = city
                    break
            
            if not point_a and len(point_a_raw) >= 3:
                point_a = _fuzzy_match_city(point_a_raw.split()[0] if point_a_raw.split() else point_a_raw, cities, 0.8)
                if not point_a:
                    point_a = point_a_raw.split()[0] if point_a_raw.split() else point_a_raw
            
            if not point_b and len(point_b_raw) >= 3:
                point_b = _fuzzy_match_city(point_b_raw.split()[0] if point_b_raw.split() else point_b_raw, cities, 0.8)
                if not point_b:
                    point_b = point_b_raw.split()[0] if point_b_raw.split() else point_b_raw
            
            if point_a and point_b and len(str(point_a)) >= 3 and len(str(point_b)) >= 3:
                return (point_a, point_b)
    
    return (None, None)

def _extract_with_dash_pattern(text: str, cities: List[str]) -> Tuple[Optional[str], Optional[str]]:
    patterns = [
        r'([А-Яа-яЁё][А-Яа-яЁё\-]+(?:\s+[А-Яа-яЁё\-]+)?)\s*[-–—→>]+\s*([А-Яа-яЁё][А-Яа-яЁё\-]+(?:\s+[А-Яа-яЁё\-]+)?)',
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            point_a_raw = match[0].strip()
            point_b_raw = match[1].strip()
            
            point_a = _find_city_in_text(point_a_raw, cities, {})
            point_b = _find_city_in_text(point_b_raw, cities, {})
            
            if not point_a:
                point_a = _fuzzy_match_city(point_a_raw, cities, 0.85)
            if not point_b:
                point_b = _fuzzy_match_city(point_b_raw, cities, 0.85)
            
            if point_a and point_b:
                return (point_a, point_b)
            
            if len(point_a_raw) >= 3 and len(point_b_raw) >= 3:
                skip_words = ['улица', 'ул', 'проспект', 'пр', 'переулок', 'пер', 'бульвар', 'бул', 
                              'площадь', 'пл', 'шоссе', 'ш', 'набережная', 'наб', 'аллея', 'дом', 'д',
                              'квартира', 'кв', 'корпус', 'корп', 'строение', 'стр', 'офис', 'оф']
                
                def is_valid_location(loc: str) -> bool:
                    loc_lower = loc.lower()
                    for word in skip_words:
                        if loc_lower.startswith(word + ' ') or loc_lower.startswith(word + '.'):
                            return False
                    if re.match(r'^\d+', loc):
                        return False
                    return True
                
                if is_valid_location(point_a_raw) and is_valid_location(point_b_raw):
                    return (point_a_raw if not point_a else point_a, 
                            point_b_raw if not point_b else point_b)
    
    return (None, None)

def _normalize_declension(word: str, declensions: dict) -> str:
    word_lower = word.lower().strip()
    if word_lower in declensions:
        return declensions[word_lower]
    return word

def _extract_with_preposition_pattern(text: str, cities: List[str]) -> Tuple[Optional[str], Optional[str]]:
    from src.config import CITY_DECLENSIONS, CITY_ALIASES
    
    all_mappings = {**CITY_DECLENSIONS, **CITY_ALIASES}
    
    patterns = [
        r'(?:из|от|с)\s+([А-Яа-яЁё][А-Яа-яЁё\-]*(?:\s+[А-Яа-яЁё\-]+)?)\s+(?:в|до|на|к)\s+([А-Яа-яЁё][А-Яа-яЁё\-]*(?:\s+[А-Яа-яЁё\-]+)?)',
        r'(?:еду|выезжаю|поеду|направляюсь)\s+(?:из|от|с)\s+([А-Яа-яЁё][А-Яа-яЁё\-]*)\s+(?:в|до|на)\s+([А-Яа-яЁё][А-Яа-яЁё\-]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            point_a_raw = match.group(1).strip().rstrip(',')
            point_b_raw = match.group(2).strip().rstrip(',')
            
            point_a_normalized = _normalize_declension(point_a_raw, all_mappings)
            point_b_normalized = _normalize_declension(point_b_raw, all_mappings)
            
            point_a = _find_city_in_text(point_a_normalized, cities, {})
            point_b = _find_city_in_text(point_b_normalized, cities, {})
            
            if not point_a:
                point_a = _fuzzy_match_city(point_a_normalized, cities, 0.85)
            if not point_b:
                point_b = _fuzzy_match_city(point_b_normalized, cities, 0.85)
            
            if point_a and point_b:
                return (point_a, point_b)
    
    return (None, None)

def _extract_known_cities_by_position(text: str, cities: List[str], city_aliases: dict) -> Tuple[Optional[str], Optional[str]]:
    found_cities: List[Tuple[int, str]] = []
    text_lower = text.lower()
    
    for alias, city_name in city_aliases.items():
        pos = text_lower.find(alias.lower())
        if pos != -1:
            if city_name not in [c[1] for c in found_cities]:
                found_cities.append((pos, city_name))
    
    for city in sorted(cities, key=len, reverse=True):
        city_lower = city.lower()
        pos = text_lower.find(city_lower)
        if pos != -1:
            if city not in [c[1] for c in found_cities]:
                end_pos = pos + len(city_lower)
                if end_pos < len(text_lower) and text_lower[end_pos].isalpha():
                    continue
                if pos > 0 and text_lower[pos-1].isalpha():
                    continue
                found_cities.append((pos, city))
    
    found_cities.sort(key=lambda x: x[0])
    
    unique_cities: List[Tuple[int, str]] = []
    for pos, city in found_cities:
        is_duplicate = False
        for existing_pos, existing_city in unique_cities:
            if city.lower() == existing_city.lower():
                is_duplicate = True
                break
            if abs(pos - existing_pos) < max(len(city), len(existing_city)):
                is_duplicate = True
                break
        if not is_duplicate:
            unique_cities.append((pos, city))
    
    if len(unique_cities) >= 2:
        return (unique_cities[0][1], unique_cities[1][1])
    
    return (None, None)

def _normalize_city_result(city: Optional[str], aliases: dict, declensions: dict) -> Optional[str]:
    if not city:
        return None
    city_lower = city.lower().strip()
    if city_lower in aliases:
        return aliases[city_lower]
    if city_lower in declensions:
        return declensions[city_lower]
    return city

def extract_locations_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    from src.config import KNOWN_CITIES, CITY_ALIASES, CITY_DECLENSIONS
    
    result = _extract_with_ab_pattern(text, KNOWN_CITIES)
    if result[0] and result[1]:
        point_a = _normalize_city_result(result[0], CITY_ALIASES, CITY_DECLENSIONS)
        point_b = _normalize_city_result(result[1], CITY_ALIASES, CITY_DECLENSIONS)
        return (point_a, point_b)
    
    result = _extract_with_preposition_pattern(text, KNOWN_CITIES)
    if result[0] and result[1]:
        return result
    
    all_aliases = {**CITY_ALIASES, **CITY_DECLENSIONS}
    result = _extract_known_cities_by_position(text, KNOWN_CITIES, all_aliases)
    if result[0] and result[1]:
        return result
    
    result = _extract_with_dash_pattern(text, KNOWN_CITIES)
    if result[0] and result[1]:
        point_a = _normalize_city_result(result[0], CITY_ALIASES, CITY_DECLENSIONS)
        point_b = _normalize_city_result(result[1], CITY_ALIASES, CITY_DECLENSIONS)
        return (point_a, point_b)
    
    return (None, None)

async def validate_location_with_geocoder(location: str) -> bool:
    coords = await get_coordinates_async(location)
    return coords is not None
