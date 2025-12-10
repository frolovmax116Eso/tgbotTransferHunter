# Telegram Taxi Order Bot

## Overview
Telegram-бот с парсером групп для мониторинга заказов межгород такси и автоматической рассылки водителям по геолокации. Многопользовательская система - каждый водитель авторизуется через свой Telegram-аккаунт и выбирает группы для мониторинга.

## Project Structure
```
├── main.py                    # Main entry point
├── src/
│   ├── config.py              # Configuration and environment variables
│   ├── matcher.py             # Order-driver matching by geolocation
│   ├── auth/
│   │   ├── __init__.py
│   │   └── telethon_auth.py   # Telethon authorization manager
│   ├── bot/
│   │   ├── __init__.py
│   │   └── driver_bot.py      # Telegram bot for drivers (Bot API)
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── order_parser.py    # Order text parsing
│   │   ├── ai_parser.py       # AI-powered parsing fallback (OpenAI)
│   │   ├── group_monitor.py   # Single-user group monitoring (legacy)
│   │   └── multi_user_monitor.py  # Multi-user group monitoring
│   └── utils/
│       ├── __init__.py
│       ├── database.py        # PostgreSQL database operations
│       ├── geo.py             # Geolocation utilities
│       └── storage.py         # JSON data storage (legacy)
└── session/                   # Telethon session files
```

## Architecture
- **PostgreSQL**: Хранение данных пользователей, сессий, групп и заказов
- **Telethon (User API)**: Парсинг сообщений из Telegram-групп для каждого пользователя
- **python-telegram-bot (Bot API)**: Бот для регистрации водителей и отправки уведомлений
- **geopy**: Расчет расстояний между координатами
- **Multi-User Monitor**: Каждый авторизованный пользователь имеет свой Telethon клиент

## Required Secrets
- `BOT_TOKEN` - Bot token from @BotFather
- `TELEGRAM_API_ID` - From my.telegram.org
- `TELEGRAM_API_HASH` - From my.telegram.org
- `DATABASE_URL` - PostgreSQL connection string

## Bot Commands (с меню)
- `/start` - Регистрация / перерегистрация
- `/auth` - Подключить Telegram-аккаунт
- `/groups` - Выбрать группы для парсинга
- `/status` - Мой статус и настройки
- `/update_location` - Изменить местоположение
- `/stop` - Вкл/выкл уведомления
- `/help` - Справка по командам
- `/admin` - Админ-панель (только для администраторов)

## Driver Registration Options
Водитель может указать местоположение тремя способами:
1. Отправить геолокацию с телефона
2. Написать название города (например: Екатеринбург)
3. Ввести координаты (например: 56.8389 60.6057)

## Order Notification Format
```
🔊 Челябинск - Екатеринбург

🚩A: Челябинск / 🏁Б: Екатеринбург
3500₽
1 чел

• Маршрут до точки "А"
──────────────────
Заказ выложил:
@username (ссылка на автора)
Заказ выложен тут:
➡️ Название группы (ссылка на пост)
```

## Monitored Groups
- сайт (-1001945539589)
- Тест группа (-1002770911077)

## Database Tables
- `users` - telegram_id, username, first_name, phone, latitude, longitude, city_name, radius_km, min_price, active, is_authorized, is_admin
- `user_sessions` - user_id, session_data, session_string, is_authorized, auth_phone
- `user_groups` - user_id, group_id, group_title, group_username, is_active, is_premium
- `subscriptions` - user_id, plan_type, starts_at, expires_at, is_active
- `orders` - point_a, point_b, price, source_group_id, source_link, coordinates
- `premium_groups` - group_id, group_title, group_username, is_active
- `order_notifications` - order_id, user_id, sent_at, message_id, route_key (статистика отправок)
- `order_responses` - notification_id, user_id, response_type, responded_at (отклики на заказы)
- `order_group_links` - route_key, user_id, group_id, group_title, source_link, message_id, author_id, author_username, author_first_name (ссылки на группы для объединения дубликатов)

## Duplicate Order Merging
Если один и тот же маршрут (A -> B) выкладывается в нескольких группах за 2 часа, система объединяет уведомления:
- Первый заказ - отправляется новое сообщение водителю
- Повторный заказ того же маршрута - редактируется существующее сообщение
- В сообщении показывается список всех групп, где выложен заказ

Техническая реализация:
- `order_notifications.route_key` - ключ маршрута для определения дубликатов
- `order_notifications.message_id` - ID сообщения для редактирования
- `order_group_links` - таблица со всеми ссылками на группы для одного маршрута
- Окно дедупликации: 2 часа

## Recent Changes
- 2025-12-08: Added "Наши группы" (Service Groups) management in admin panel - mark groups as "our" groups
- 2025-12-08: Orders from "Наши группы" now show "✅ Наша группа" badge in notifications
- 2025-12-07: Unified bot - removed separate run modes, now single bot handles both commands and group parsing
- 2025-12-07: Added reverse geocoding - city auto-detected from coordinates during registration
- 2025-12-07: Fixed admin search - now handles @username format (strips @ symbol)
- 2025-12-07: Improved "Мои группы" menu - now shows group list with hyperlinks first, then "Изменить выбор" button
- 2025-12-07: Added hyperlinks to driver's group list (after /groups selection)
- 2025-12-06: Author persistence - if author is visible in any group, it's preserved across all merged notifications
- 2025-12-06: Added duplicate order merging - same route from multiple groups combined into one message
- 2025-12-06: Added admin panel (/admin) with driver management and statistics
- 2025-12-06: Added author hyperlink with username in order notifications
- 2025-12-06: Added "Take Order" button that auto-sends "я" reply to group post via Telethon
- 2025-12-06: Added AI parsing fallback (OpenAI GPT-4o-mini) for complex order texts
- 2025-12-06: Improved price extraction patterns (3300, 5к, 6 тыс)
- 2025-12-06: Added Кировоград and other Sverdlovsk region cities
- 2025-12-06: Replaced SMS code auth with QR-code authorization (solves Telegram anti-phishing block)
- 2025-12-06: Implemented multi-user group monitoring (each user has own Telethon client)
- 2025-12-06: Added PostgreSQL database for users, sessions, groups, orders
- 2025-12-06: Added Telethon authorization flow (/auth command)
- 2025-12-06: Added group selection with inline buttons (/groups command)
- 2025-12-06: Updated matcher to filter drivers by group subscription
- 2025-12-06: Added bot commands menu for easy access
- 2025-12-06: Added manual coordinates input for drivers
- 2025-12-06: Added city name input for driver location

## AI Parsing (OpenAI)
- Uses Replit AI Integrations (no API key needed)
- Model: GPT-4o-mini (~$0.0001 per request)
- AI is used as fallback when pattern matching fails
- Charges billed to Replit credits

## Admin Panel
Админ-панель доступна через команду `/admin` для пользователей с правами администратора.

### Как назначить администратора
1. Через переменную окружения `ADMIN_TELEGRAM_ID` (автоматически)
2. Через базу данных: `UPDATE users SET is_admin = TRUE WHERE telegram_id = <your_id>`

### Функционал админ-панели
- **Список водителей** с пагинацией (10 на страницу)
- **Детали водителя**: группы, локация, статистика откликов
- **Все группы**: список всех уникальных групп со всех сессий (с гиперссылками)
- **Наши группы**: управление списком "наших" групп (заказы из них помечаются как ✅ Наша группа)
- **Синхронизация групп**: кнопка для автоматического добавления всех групп себе
- **Назначение/снятие админов**
- **Общая статистика**: пользователи, заказы, топ групп
- Все заказы приходят админам с пометкой [ADMIN]
- Новые группы автоматически добавляются всем админам

## Current State: WORKING
Бот полностью функционален:
- Многопользовательский парсер групп
- AI-улучшенный парсинг заказов
- Объединение дубликатов заказов из разных групп
- Каждый водитель авторизуется через /auth
- Выбор групп через /groups
- Заказы отправляются только подписчикам группы
- Фильтрация по геолокации и минимальной цене
- Админ-панель для управления водителями и статистики
