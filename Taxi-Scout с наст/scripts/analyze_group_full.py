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
DAYS_BACK = 14

async def analyze_group_full():
    print(f"=" * 70)
    print(f"ПОЛНЫЙ АНАЛИЗ ГРУППЫ {GROUP_ID}")
    print(f"Анализ за последние {DAYS_BACK} дней")
    print(f"=" * 70)
    
    db_session = get_session()
    user_sessions = db_session.query(UserSession).filter(
        UserSession.is_authorized == True,
        UserSession.session_string.isnot(None)
    ).all()
    
    if not user_sessions:
        print("Нет авторизованных сессий.")
        return
    
    client = None
    entity = None
    
    for us in user_sessions:
        print(f"Пробуем сессию пользователя {us.user_id}...")
        client = TelegramClient(
            StringSession(us.session_string),
            TELEGRAM_API_ID,
            TELEGRAM_API_HASH
        )
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.disconnect()
            continue
        
        try:
            entity = await client.get_entity(GROUP_ID)
            print(f"✅ Доступ к группе: {entity.title}")
            break
        except Exception as e:
            print(f"❌ Нет доступа: {e}")
            await client.disconnect()
            continue
    
    if not entity:
        print("\nНи одна сессия не имеет доступа к группе.")
        return
    
    cutoff_date = datetime.now() - timedelta(days=DAYS_BACK)
    print(f"\nСобираем сообщения за последние {DAYS_BACK} дней (с {cutoff_date.strftime('%Y-%m-%d')})...")
    
    messages = []
    count = 0
    async for message in client.iter_messages(entity, offset_date=datetime.now(), reverse=False):
        if message.date.replace(tzinfo=None) < cutoff_date:
            break
        if message.text:
            messages.append({
                'id': message.id,
                'date': message.date.isoformat(),
                'text': message.text
            })
        count += 1
        if count % 2000 == 0:
            print(f"  Обработано {count} сообщений...")
    
    print(f"✅ Получено {len(messages)} сообщений с текстом")
    
    departures = Counter()
    arrivals = Counter()
    routes = Counter()
    routes_by_day = {}
    price_stats = []
    
    known_cities_lower = {c.lower(): c for c in KNOWN_CITIES}
    aliases_lower = {a.lower(): v for a, v in CITY_ALIASES.items()}
    known_coords_lower = set(KNOWN_COORDINATES.keys())
    
    route_patterns = [
        r'([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)\s*[-–—→>]+\s*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
        r'(?:откуда|из|от|с)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)[\s,\-–—]+(?:куда|в|до|на)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
        r'(?:А|а)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+?)[\s,\-–—]+(?:Б|б)[:\s]*([А-Яа-яЁё][А-Яа-яЁё\s\-]+)',
    ]
    
    price_patterns = [
        r'(\d{1,3})[,](\d{3})\s*(?:руб|₽|р\.?)?',
        r'(\d{3,5})\s*(?:руб|₽|р\.?\b)',
        r'(\d{1,2})\s*(?:к|тыс|т)\.?',
    ]
    
    def normalize_city(name):
        name_clean = re.sub(r'[^\w\s\-]', '', name).strip()
        name_lower = name_clean.lower()
        
        if len(name_clean) < 3:
            return None
        
        skip_words = ['трансфер', 'империя', 'премьер', 'междугороднее', 
                      'заказ', 'минивен', 'такси', 'срочно', 'сегодня',
                      'завтра', 'платная', 'бесплатная', 'отель', 'санкт']
        if name_lower in skip_words:
            return None
        
        if name_lower in aliases_lower:
            return aliases_lower[name_lower]
        if name_lower in known_cities_lower:
            return known_cities_lower[name_lower]
        if name_lower in known_coords_lower:
            return name_clean.title()
        
        if name_clean[0].isupper() and len(name_clean) >= 4:
            return name_clean
        
        return None
    
    for msg in messages:
        text = msg['text']
        msg_date = msg['date'][:10]
        
        for pattern in route_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                point_a = normalize_city(match[0])
                point_b = normalize_city(match[1])
                
                if point_a and point_b and point_a != point_b:
                    departures[point_a] += 1
                    arrivals[point_b] += 1
                    route_key = f"{point_a} → {point_b}"
                    routes[route_key] += 1
                    
                    if msg_date not in routes_by_day:
                        routes_by_day[msg_date] = 0
                    routes_by_day[msg_date] += 1
        
        for pattern in price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    if ',' in match.group(0):
                        price = int(match.group(1) + match.group(2))
                    elif 'к' in match.group(0).lower() or 'тыс' in match.group(0).lower():
                        price = int(match.group(1)) * 1000
                    else:
                        price = int(match.group(1))
                    if 500 <= price <= 100000:
                        price_stats.append(price)
                except:
                    pass
    
    print("\n")
    print("=" * 70)
    print("📊 ПОЛНАЯ СТАТИСТИКА ЗА МЕСЯЦ")
    print("=" * 70)
    
    print(f"\n📈 ОБЩАЯ ИНФОРМАЦИЯ:")
    print(f"   Всего сообщений: {len(messages)}")
    print(f"   Найдено маршрутов: {sum(routes.values())}")
    print(f"   Уникальных маршрутов: {len(routes)}")
    print(f"   Период: {DAYS_BACK} дней")
    
    print(f"\n🚀 ОТКУДА ЧАЩЕ ВСЕГО УЕЗЖАЮТ (топ-20):")
    print("-" * 50)
    for i, (city, count) in enumerate(departures.most_common(20), 1):
        bar = "█" * min(count // 2, 30)
        print(f"   {i:2}. {city:25} {count:4} {bar}")
    
    print(f"\n🏁 КУДА ЧАЩЕ ВСЕГО ПРИЕЗЖАЮТ (топ-20):")
    print("-" * 50)
    for i, (city, count) in enumerate(arrivals.most_common(20), 1):
        bar = "█" * min(count // 2, 30)
        print(f"   {i:2}. {city:25} {count:4} {bar}")
    
    print(f"\n🛤️ САМЫЕ ПОПУЛЯРНЫЕ МАРШРУТЫ (топ-30):")
    print("-" * 60)
    for i, (route, count) in enumerate(routes.most_common(30), 1):
        bar = "█" * min(count, 20)
        print(f"   {i:2}. {route:40} {count:3} {bar}")
    
    if price_stats:
        avg_price = sum(price_stats) / len(price_stats)
        min_price = min(price_stats)
        max_price = max(price_stats)
        median_price = sorted(price_stats)[len(price_stats) // 2]
        print(f"\n💰 СТАТИСТИКА ЦЕН:")
        print("-" * 50)
        print(f"   Средняя цена:    {avg_price:,.0f} ₽")
        print(f"   Медианная цена:  {median_price:,.0f} ₽")
        print(f"   Минимальная:     {min_price:,.0f} ₽")
        print(f"   Максимальная:    {max_price:,.0f} ₽")
        print(f"   Всего цен:       {len(price_stats)}")
    
    print(f"\n📅 АКТИВНОСТЬ ПО ДНЯМ:")
    print("-" * 50)
    sorted_days = sorted(routes_by_day.items(), key=lambda x: x[1], reverse=True)[:10]
    for day, count in sorted_days:
        bar = "█" * min(count // 2, 40)
        print(f"   {day}: {count:4} заказов {bar}")
    
    results = {
        'group_id': GROUP_ID,
        'analyzed_at': datetime.now().isoformat(),
        'period_days': DAYS_BACK,
        'total_messages': len(messages),
        'total_routes': sum(routes.values()),
        'unique_routes': len(routes),
        'departures_top50': dict(departures.most_common(50)),
        'arrivals_top50': dict(arrivals.most_common(50)),
        'routes_top100': dict(routes.most_common(100)),
        'price_stats': {
            'average': round(avg_price, 0) if price_stats else 0,
            'median': median_price if price_stats else 0,
            'min': min_price if price_stats else 0,
            'max': max_price if price_stats else 0,
            'count': len(price_stats)
        },
        'routes_by_day': routes_by_day
    }
    
    with open('scripts/group_full_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n✅ Полные результаты сохранены в scripts/group_full_analysis.json")
    
    await client.disconnect()
    db_session.close()

if __name__ == "__main__":
    asyncio.run(analyze_group_full())
