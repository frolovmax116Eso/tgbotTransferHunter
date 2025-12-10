import asyncio
import json
import re
from collections import Counter
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from telethon import TelegramClient
from telethon.sessions import StringSession
from src.utils.database import get_session, UserSession
from src.config import TELEGRAM_API_ID, TELEGRAM_API_HASH, KNOWN_CITIES, CITY_ALIASES
from src.utils.geo import get_coordinates, KNOWN_COORDINATES

GROUP_ID = -1002290679743
MESSAGES_LIMIT = 500

async def analyze_group():
    print(f"=" * 60)
    print(f"Анализ группы {GROUP_ID}")
    print(f"=" * 60)
    
    db_session = get_session()
    user_sessions = db_session.query(UserSession).filter(
        UserSession.is_authorized == True,
        UserSession.session_string.isnot(None)
    ).all()
    
    if not user_sessions:
        print("Нет авторизованных сессий. Используйте /auth в боте.")
        return
    
    user_session = None
    for us in user_sessions:
        print(f"Пробуем сессию пользователя {us.user_id}...")
        client = TelegramClient(
            StringSession(us.session_string),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH
        )
        await client.connect()
        
        if not await client.is_user_authorized():
            print(f"  Сессия {us.user_id} не авторизована")
            await client.disconnect()
            continue
        
        try:
            entity = await client.get_entity(GROUP_ID)
            print(f"  ✅ Сессия {us.user_id} имеет доступ к группе: {entity.title}")
            user_session = us
            break
        except Exception as e:
            print(f"  ❌ Нет доступа: {e}")
            await client.disconnect()
            continue
    
    if not user_session:
        print("\nНи одна сессия не имеет доступа к группе.")
        print("Убедитесь, что хотя бы один авторизованный пользователь состоит в этой группе.")
        return
    
    print(f"\nСобираем последние {MESSAGES_LIMIT} сообщений...")
    
    messages = []
    async for message in client.iter_messages(entity, limit=MESSAGES_LIMIT):
        if message.text:
            messages.append({
                'id': message.id,
                'date': message.date.isoformat(),
                'text': message.text,
                'sender_id': message.sender_id
            })
    
    print(f"Получено {len(messages)} сообщений с текстом")
    
    cities_found = Counter()
    unknown_locations = Counter()
    price_patterns = Counter()
    order_examples = []
    keywords_found = Counter()
    
    known_cities_lower = {c.lower(): c for c in KNOWN_CITIES}
    aliases_lower = {a.lower(): v for a, v in CITY_ALIASES.items()}
    known_coords_lower = set(KNOWN_COORDINATES.keys())
    
    route_patterns = [
        r'([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)\s*[-–—→>]+\s*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
        r'(?:откуда|из|от|с)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)[\s,\-–—]+(?:куда|в|до|на)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
        r'(?:А|а)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)[\s,\-–—]+(?:Б|б)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
    ]
    
    price_regex = [
        (r'(\d{1,3})[,](\d{3})\s*(?:руб|₽|р\.?)?', 'comma'),
        (r'(\d{3,5})\s*(?:руб|₽|р\.?\b)', 'direct'),
        (r'(\d{1,2})\s*(?:к|тыс|т)\.?', 'thousands'),
    ]
    
    order_keywords = ['заказ', 'пассажир', 'чел', 'человек', 'поездка', 'трансфер', 
                      'межгород', 'такси', 'водитель', 'минивен', 'седан', 'комфорт',
                      'бизнес', 'эконом', 'багаж', 'чемодан', 'детское кресло']
    
    for msg in messages:
        text = msg['text']
        text_lower = text.lower()
        
        for kw in order_keywords:
            if kw in text_lower:
                keywords_found[kw] += 1
        
        for pattern in route_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                point_a = match[0].strip()
                point_b = match[1].strip()
                
                for point in [point_a, point_b]:
                    point_clean = re.sub(r'[^\w\s\-]', '', point).strip()
                    point_lower = point_clean.lower()
                    
                    if len(point_clean) < 3:
                        continue
                    
                    if point_lower in known_cities_lower:
                        cities_found[known_cities_lower[point_lower]] += 1
                    elif point_lower in aliases_lower:
                        cities_found[aliases_lower[point_lower]] += 1
                    elif point_lower in known_coords_lower:
                        cities_found[point_clean] += 1
                    else:
                        if len(point_clean) >= 4 and point_clean[0].isupper():
                            unknown_locations[point_clean] += 1
                
                if point_a and point_b and len(order_examples) < 50:
                    order_examples.append({
                        'text': text[:300],
                        'point_a': point_a,
                        'point_b': point_b
                    })
        
        for regex, pattern_type in price_regex:
            if re.search(regex, text, re.IGNORECASE):
                price_patterns[pattern_type] += 1
    
    print("\n" + "=" * 60)
    print("РЕЗУЛЬТАТЫ АНАЛИЗА")
    print("=" * 60)
    
    print(f"\n📊 СТАТИСТИКА:")
    print(f"   Всего сообщений: {len(messages)}")
    print(f"   Найдено маршрутов: {sum(cities_found.values()) // 2}")
    
    print(f"\n🏙️ ИЗВЕСТНЫЕ ГОРОДА (топ-20):")
    for city, count in cities_found.most_common(20):
        print(f"   {city}: {count}")
    
    print(f"\n❓ НЕИЗВЕСТНЫЕ ЛОКАЦИИ (топ-30):")
    for loc, count in unknown_locations.most_common(30):
        coords = get_coordinates(loc)
        status = "✅" if coords else "❌"
        print(f"   {status} {loc}: {count} (coords: {coords})")
    
    print(f"\n💰 ПАТТЕРНЫ ЦЕН:")
    for pattern, count in price_patterns.most_common():
        print(f"   {pattern}: {count}")
    
    print(f"\n🔑 КЛЮЧЕВЫЕ СЛОВА:")
    for kw, count in keywords_found.most_common():
        print(f"   {kw}: {count}")
    
    results = {
        'group_id': GROUP_ID,
        'analyzed_at': datetime.now().isoformat(),
        'total_messages': len(messages),
        'known_cities': dict(cities_found.most_common(50)),
        'unknown_locations': dict(unknown_locations.most_common(50)),
        'price_patterns': dict(price_patterns),
        'keywords': dict(keywords_found),
        'order_examples': order_examples[:20]
    }
    
    with open('scripts/group_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Результаты сохранены в scripts/group_analysis.json")
    
    if unknown_locations:
        print(f"\n📝 РЕКОМЕНДАЦИИ ПО ДОБАВЛЕНИЮ:")
        needs_coords = []
        for loc, count in unknown_locations.most_common(20):
            if count >= 2:
                coords = get_coordinates(loc)
                if coords:
                    needs_coords.append(f"    '{loc.lower()}': {coords},")
        
        if needs_coords:
            print("\nДобавить в KNOWN_COORDINATES (src/utils/geo.py):")
            for line in needs_coords:
                print(line)
    
    await client.disconnect()
    db_session.close()

if __name__ == "__main__":
    asyncio.run(analyze_group())
