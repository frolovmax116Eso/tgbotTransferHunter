import logging
import asyncio
import io
import re
from typing import Optional
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application, 
    CommandHandler, 
    MessageHandler, 
    ConversationHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes
)

from src.config import BOT_TOKEN, ADMIN_TELEGRAM_ID
from src.utils.database import (
    get_user_by_telegram_id, 
    create_or_update_user, 
    get_active_users,
    get_user_session,
    delete_user_session,
    get_user_groups,
    add_user_group,
    toggle_user_group,
    init_db,
    get_admin_users,
    set_user_admin,
    get_all_users,
    get_user_stats,
    get_system_stats,
    get_user_by_id,
    get_all_unique_groups,
    sync_all_groups_to_admin,
    get_notification_by_message_id,
    get_order_group_links,
    search_users,
    get_service_groups,
    add_service_group,
    remove_service_group,
    toggle_service_group,
    is_service_group,
    search_service_groups,
    search_all_groups,
    get_driver_settings,
    update_driver_settings,
    is_user_in_quiet_hours,
    is_user_busy,
    set_user_busy,
    clear_user_busy,
    get_favorite_routes,
    add_favorite_route,
    remove_favorite_route,
    get_blacklist,
    add_to_blacklist,
    remove_from_blacklist,
    is_blacklisted,
    get_driver_profile,
    update_driver_profile,
    get_quick_replies,
    add_quick_reply,
    remove_quick_reply,
    toggle_quick_reply
)
from src.utils.geo import is_within_radius, get_coordinates_async, get_city_by_coordinates_async
from src.auth.telethon_auth import auth_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

LOCATION, RADIUS, MIN_PRICE = range(3)
AUTH_2FA = 10

MENU_STATUS = "📊 Мой статус"
MENU_GROUPS = "📢 Мои группы"  
MENU_AUTH = "🔐 Авторизация"
MENU_LOCATION = "📍 Локация"
MENU_NOTIFICATIONS = "🔔 Уведомления"
MENU_SETTINGS = "⚙️ Настройки"
MENU_HELP = "❓ Помощь"
MENU_ADMIN = "👑 Админ"

class DriverBot:
    def __init__(self):
        if not BOT_TOKEN:
            raise ValueError("BOT_TOKEN must be set")
        
        init_db()
        
        self.pending_2fa = {}
        self.admin_search_mode = {}
        self.admin_group_search_mode = {}
        self.favorite_route_input = {}
        self.blacklist_input = {}
        self.profile_input = {}
        self.quick_reply_input = {}
        
        self.application = Application.builder().token(BOT_TOKEN).build()
        self._setup_handlers()
    
    def _setup_handlers(self):
        conv_handler = ConversationHandler(
            entry_points=[CommandHandler('start', self.start_command)],
            states={
                LOCATION: [
                    MessageHandler(filters.LOCATION, self.receive_location),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_city_text)
                ],
                RADIUS: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_radius)],
                MIN_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_min_price)],
            },
            fallbacks=[CommandHandler('cancel', self.cancel_command)],
        )
        
        auth_conv_handler = ConversationHandler(
            entry_points=[CommandHandler('auth', self.auth_command)],
            states={
                AUTH_2FA: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.receive_2fa_password)],
            },
            fallbacks=[CommandHandler('cancel', self.cancel_auth_command)],
        )
        
        self.application.add_handler(conv_handler)
        self.application.add_handler(auth_conv_handler)
        self.application.add_handler(CommandHandler('status', self.status_command))
        self.application.add_handler(CommandHandler('update_location', self.update_location_command))
        self.application.add_handler(CommandHandler('settings', self.settings_command))
        self.application.add_handler(CommandHandler('stop', self.stop_command))
        self.application.add_handler(CommandHandler('help', self.help_command))
        self.application.add_handler(CommandHandler('groups', self.groups_command))
        self.application.add_handler(CommandHandler('admin', self.admin_command))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_group_toggle, pattern=r'^toggle_group:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_groups_done, pattern=r'^groups_done$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_groups_refresh, pattern=r'^groups_refresh$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_groups_page, pattern=r'^groups_page:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_selected_page, pattern=r'^selected_page:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_change_groups, pattern=r'^change_groups$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_start_groups_selection, pattern=r'^start_groups_selection$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_refresh_qr, pattern=r'^refresh_qr$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_logout_session, pattern=r'^logout_session$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_take_order, pattern=r'^take_order:'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_main, pattern=r'^admin:main$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_users, pattern=r'^admin:users:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_user_detail, pattern=r'^admin:user:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_stats, pattern=r'^admin:stats$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_all_groups, pattern=r'^admin:all_groups:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_sync_groups, pattern=r'^admin:sync_groups$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_search, pattern=r'^admin:search$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_search_cancel, pattern=r'^admin:search_cancel$'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_groups, pattern=r'^admin:service_groups:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_group_toggle, pattern=r'^admin:sg_toggle:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_group_remove, pattern=r'^admin:sg_remove:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_group_add, pattern=r'^admin:sg_add$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_group_search, pattern=r'^admin:sg_search$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_service_group_add_confirm, pattern=r'^admin:sg_add_confirm:'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_settings_main, pattern=r'^settings:main$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_menu, pattern=r'^settings:quiet_hours$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_toggle, pattern=r'^settings:quiet_toggle$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_start, pattern=r'^settings:quiet_start$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_end, pattern=r'^settings:quiet_end$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_start_set, pattern=r'^settings:quiet_start_set:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quiet_hours_end_set, pattern=r'^settings:quiet_end_set:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_busy_mode_menu, pattern=r'^settings:busy_mode$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_busy_mode_set, pattern=r'^settings:busy_set:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_busy_mode_clear, pattern=r'^settings:busy_clear$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_favorite_routes_menu, pattern=r'^settings:favorite_routes$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_favorite_route_add, pattern=r'^settings:fav_add$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_favorite_route_remove, pattern=r'^settings:fav_remove:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_favorite_route_toggle, pattern=r'^settings:fav_toggle:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_favorite_route_cancel, pattern=r'^settings:fav_cancel$'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_blacklist_menu, pattern=r'^settings:blacklist$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_blacklist_add_author, pattern=r'^settings:bl_add_author$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_blacklist_add_group, pattern=r'^settings:bl_add_group$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_blacklist_remove, pattern=r'^settings:bl_remove:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_blacklist_cancel, pattern=r'^settings:bl_cancel$'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_menu, pattern=r'^settings:profile$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_name, pattern=r'^settings:profile_name$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_car, pattern=r'^settings:profile_car$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_license, pattern=r'^settings:profile_license$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_sts, pattern=r'^settings:profile_sts$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_child_seat, pattern=r'^settings:profile_child_seat$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_profile_cancel, pattern=r'^settings:profile_cancel$'))
        
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_replies_menu, pattern=r'^settings:quick_replies$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_reply_add, pattern=r'^settings:qr_add$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_reply_remove, pattern=r'^settings:qr_remove:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_reply_toggle, pattern=r'^settings:qr_toggle:'))
        self.application.add_handler(CallbackQueryHandler(self.handle_quick_reply_cancel, pattern=r'^settings:qr_cancel$'))
        
        self.application.add_handler(MessageHandler(filters.PHOTO, self.handle_photo_upload))
        self.application.add_handler(MessageHandler(filters.LOCATION, self.quick_location_update))
        menu_pattern = f'^({re.escape(MENU_STATUS)}|{re.escape(MENU_GROUPS)}|{re.escape(MENU_AUTH)}|{re.escape(MENU_LOCATION)}|{re.escape(MENU_NOTIFICATIONS)}|{re.escape(MENU_SETTINGS)}|{re.escape(MENU_HELP)}|{re.escape(MENU_ADMIN)})$'
        self.application.add_handler(MessageHandler(filters.Regex(menu_pattern), self.handle_menu_button))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_2fa_text))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if driver:
            await update.message.reply_text(
                f"С возвращением, {user.first_name}!\n\n"
                "Вы уже зарегистрированы как водитель.\n"
                "Используйте /status для проверки статуса\n"
                "или /update_location для обновления местоположения.\n\n"
                "Хотите пройти регистрацию заново?\n"
                "• Отправьте геолокацию\n"
                "• Напишите город или координаты",
                reply_markup=self._location_keyboard()
            )
        else:
            await update.message.reply_text(
                f"Добро пожаловать, {user.first_name}!\n\n"
                "Я помогу вам получать заказы межгород такси "
                "в вашем районе.\n\n"
                "Укажите ваше местоположение:\n"
                "• Отправьте геолокацию\n"
                "• Напишите название города (например: Екатеринбург)\n"
                "• Или введите координаты (например: 56.8389 60.6057)",
                reply_markup=self._location_keyboard()
            )
        
        return LOCATION
    
    async def receive_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        location = update.message.location
        context.user_data['latitude'] = location.latitude
        context.user_data['longitude'] = location.longitude
        
        city_name = await get_city_by_coordinates_async(location.latitude, location.longitude)
        context.user_data['city_name'] = city_name
        
        location_info = f"Город: {city_name}" if city_name else f"Координаты: {location.latitude:.4f}, {location.longitude:.4f}"
        
        await update.message.reply_text(
            f"Отлично! Геолокация получена.\n"
            f"{location_info}\n\n"
            "Теперь укажите радиус поиска заказов в километрах "
            "(например: 50):",
            reply_markup=ReplyKeyboardRemove()
        )
        
        return RADIUS
    
    def _parse_coordinates(self, text: str):
        text = text.replace(',', ' ').replace(';', ' ')
        parts = text.split()
        
        numbers = []
        for part in parts:
            try:
                num = float(part)
                numbers.append(num)
            except ValueError:
                continue
        
        if len(numbers) >= 2:
            lat, lon = numbers[0], numbers[1]
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return (lat, lon)
        return None
    
    async def receive_city_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        text = update.message.text.strip()
        
        coords = self._parse_coordinates(text)
        if coords:
            context.user_data['latitude'] = coords[0]
            context.user_data['longitude'] = coords[1]
            
            city_name = await get_city_by_coordinates_async(coords[0], coords[1])
            context.user_data['city_name'] = city_name
            
            location_info = f"Город: {city_name}" if city_name else f"Координаты: {coords[0]:.4f}, {coords[1]:.4f}"
            
            await update.message.reply_text(
                f"Координаты установлены: {coords[0]:.4f}, {coords[1]:.4f}\n"
                f"{location_info}\n\n"
                "Теперь укажите радиус поиска заказов в километрах "
                "(например: 50):",
                reply_markup=ReplyKeyboardRemove()
            )
            return RADIUS
        
        await update.message.reply_text(
            f"Ищу координаты для: {text}...",
            reply_markup=ReplyKeyboardRemove()
        )
        
        coords = await get_coordinates_async(text)
        
        if not coords:
            await update.message.reply_text(
                f"Не удалось найти '{text}'.\n\n"
                "Попробуйте:\n"
                "• Название города (например: Екатеринбург)\n"
                "• Координаты (например: 56.8389 60.6057)\n"
                "• Или отправьте геолокацию",
                reply_markup=self._location_keyboard()
            )
            return LOCATION
        
        context.user_data['latitude'] = coords[0]
        context.user_data['longitude'] = coords[1]
        context.user_data['city_name'] = text
        
        await update.message.reply_text(
            f"Найдено: {text}\n"
            f"Координаты: {coords[0]:.4f}, {coords[1]:.4f}\n\n"
            "Теперь укажите радиус поиска заказов в километрах "
            "(например: 50):"
        )
        
        return RADIUS
    
    async def receive_radius(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            radius = int(update.message.text.strip())
            if radius < 1 or radius > 500:
                await update.message.reply_text(
                    "Укажите радиус от 1 до 500 км:"
                )
                return RADIUS
            
            context.user_data['radius_km'] = radius
            
            await update.message.reply_text(
                f"Радиус: {radius} км.\n\n"
                "Укажите минимальную сумму заказа в рублях "
                "(например: 1000, или 0 если без ограничений):"
            )
            
            return MIN_PRICE
            
        except ValueError:
            await update.message.reply_text(
                "Пожалуйста, введите число (радиус в км):"
            )
            return RADIUS
    
    async def receive_min_price(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        try:
            min_price = int(update.message.text.strip())
            if min_price < 0:
                min_price = 0
            
            user = update.effective_user
            city_name = context.user_data.get('city_name')
            
            create_or_update_user(
                telegram_id=user.id,
                username=user.username,
                first_name=user.first_name,
                latitude=context.user_data['latitude'],
                longitude=context.user_data['longitude'],
                radius_km=context.user_data['radius_km'],
                min_price=min_price,
                city_name=city_name,
                active=True
            )
            
            location_display = city_name if city_name else "по геолокации"
            
            driver = get_user_by_telegram_id(user.id)
            is_admin = driver.is_admin if driver else False
            
            await update.message.reply_text(
                f"Регистрация завершена!\n\n"
                f"Ваши настройки:\n"
                f"- Местоположение: {location_display}\n"
                f"- Радиус поиска: {context.user_data['radius_km']} км\n"
                f"- Мин. сумма: {min_price} руб.\n\n"
                f"Вы будете получать уведомления о заказах в вашем районе.\n\n"
                f"Следующий шаг: подключите Telegram через кнопку «🔐 Авторизация»",
                reply_markup=self._main_menu_keyboard(is_admin)
            )
            
            return ConversationHandler.END
            
        except ValueError:
            await update.message.reply_text(
                "Пожалуйста, введите число (минимальная сумма в рублях):"
            )
            return MIN_PRICE
    
    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user = update.effective_user
        await update.message.reply_text(
            "Регистрация отменена.",
            reply_markup=self._get_menu_for_user(user.id)
        )
        return ConversationHandler.END
    
    async def auth_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Сначала зарегистрируйтесь через /start"
            )
            return ConversationHandler.END
        
        user_session = get_user_session(driver.id)
        if user_session and user_session.is_authorized:
            keyboard = [[InlineKeyboardButton("🚪 Выйти из авторизации", callback_data="logout_session")]]
            await update.message.reply_text(
                "✅ Вы уже авторизованы в Telegram.\n"
                "Ваш аккаунт подключен к боту.\n\n"
                "Используйте /groups для выбора групп.\n\n"
                "Если хотите сменить аккаунт или возникли проблемы — выйдите и авторизуйтесь заново:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return ConversationHandler.END
        
        await update.message.reply_text(
            "Генерирую QR-код для авторизации..."
        )
        
        success, url_or_error, qr_image = await auth_manager.start_qr_login(driver.id)
        
        if not success:
            await update.message.reply_text(
                f"{url_or_error}\n\n"
                "Используйте /auth для повторной попытки."
            )
            return ConversationHandler.END
        
        if qr_image:
            qr_file = InputFile(io.BytesIO(qr_image), filename="qr_auth.png")
            await update.message.reply_photo(
                photo=qr_file,
                caption=(
                    "Отсканируйте QR-код в приложении Telegram:\n\n"
                    "1. Откройте Telegram на другом устройстве\n"
                    "2. Настройки → Устройства → Подключить устройство\n"
                    "3. Отсканируйте этот QR-код\n\n"
                    "Ожидаю подтверждение (60 сек)...\n"
                    "Для отмены отправьте /cancel"
                )
            )
        
        asyncio.create_task(self._wait_for_qr_auth(update, context, driver.id, user.id))
        
        return ConversationHandler.END
    
    async def _wait_for_qr_auth(self, update: Update, context: ContextTypes.DEFAULT_TYPE, driver_db_id: int, telegram_id: int):
        try:
            success, message, session_string = await auth_manager.wait_for_qr_confirm(driver_db_id, timeout=60)
            
            if success:
                create_or_update_user(telegram_id=telegram_id, is_authorized=True)
                await update.message.reply_text(
                    "Авторизация успешна!\n\n"
                    "Теперь вы можете выбрать группы для парсинга.\n"
                    "Используйте /groups для выбора групп."
                )
            elif "пароль" in message.lower() or "2fa" in message.lower():
                self.pending_2fa[telegram_id] = driver_db_id
                await update.message.reply_text(
                    "Требуется пароль двухфакторной аутентификации.\n\n"
                    "Введите ваш облачный пароль:"
                )
            else:
                keyboard = [[InlineKeyboardButton("Обновить QR-код", callback_data="refresh_qr")]]
                await update.message.reply_text(
                    f"{message}",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        except Exception as e:
            logger.error(f"Error waiting for QR auth: {e}")
            await update.message.reply_text(
                "Ошибка авторизации.\n"
                "Используйте /auth для повторной попытки."
            )
    
    async def handle_2fa_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        telegram_id = user.id
        
        if telegram_id in self.admin_search_mode:
            del self.admin_search_mode[telegram_id]
            search_query = update.message.text.strip()
            await self.handle_admin_search_query(telegram_id, search_query, update.message)
            return
        
        if telegram_id in self.admin_group_search_mode:
            del self.admin_group_search_mode[telegram_id]
            search_query = update.message.text.strip()
            await self.handle_admin_group_search_query(telegram_id, search_query, update.message)
            return
        
        if telegram_id in self.favorite_route_input:
            handled = await self.handle_favorite_route_input(telegram_id, update.message.text, update.message)
            if handled:
                return
        
        if telegram_id in self.blacklist_input:
            handled = await self.handle_blacklist_input(telegram_id, update.message.text, update.message)
            if handled:
                return
        
        if telegram_id in self.profile_input:
            handled = await self.handle_profile_input(telegram_id, update.message.text, update.message)
            if handled:
                return
        
        if telegram_id in self.quick_reply_input:
            handled = await self.handle_quick_reply_input(telegram_id, update.message.text, update.message)
            if handled:
                return
        
        if telegram_id not in self.pending_2fa:
            return
        
        password = update.message.text.strip()
        driver = get_user_by_telegram_id(telegram_id)
        
        if not driver:
            await update.message.reply_text("Ошибка. Попробуйте /start")
            del self.pending_2fa[telegram_id]
            return
        
        driver_db_id = self.pending_2fa[telegram_id]
        
        await update.message.reply_text("Проверяю пароль...")
        
        success, message, session_string = await auth_manager.verify_2fa(driver_db_id, password)
        
        if success:
            del self.pending_2fa[telegram_id]
            create_or_update_user(telegram_id=telegram_id, is_authorized=True)
            await update.message.reply_text(
                "Авторизация успешна!\n\n"
                "Теперь вы можете выбрать группы для парсинга.\n"
                "Используйте /groups для выбора групп."
            )
        else:
            await update.message.reply_text(
                f"{message}\n\n"
                "Попробуйте ещё раз или /cancel для отмены"
            )
    
    async def handle_refresh_qr(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Сначала зарегистрируйтесь через /start")
            return
        
        await query.edit_message_text("Генерирую новый QR-код...")
        
        success, url_or_error, qr_image = await auth_manager.start_qr_login(driver.id)
        
        if not success:
            await query.message.reply_text(
                f"{url_or_error}\n\n"
                "Используйте /auth для повторной попытки."
            )
            return
        
        if qr_image:
            qr_file = InputFile(io.BytesIO(qr_image), filename="qr_auth.png")
            await query.message.reply_photo(
                photo=qr_file,
                caption=(
                    "Отсканируйте QR-код в приложении Telegram:\n\n"
                    "1. Откройте Telegram на другом устройстве\n"
                    "2. Настройки → Устройства → Подключить устройство\n"
                    "3. Отсканируйте этот QR-код\n\n"
                    "Ожидаю подтверждение (60 сек)..."
                )
            )
        
        asyncio.create_task(self._wait_for_qr_auth(update, context, driver.id, user.id))
    
    async def handle_logout_session(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Вы не зарегистрированы.")
            return
        
        delete_user_session(driver.id)
        create_or_update_user(telegram_id=user.id, is_authorized=False)
        
        await query.edit_message_text(
            "✅ Вы вышли из авторизации.\n\n"
            "Сессия удалена. Теперь вы можете подключить другой аккаунт.\n\n"
            "Нажмите «🔐 Авторизация» для нового входа."
        )
    
    async def receive_2fa_password(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        return ConversationHandler.END
    
    async def cancel_auth_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        user = update.effective_user
        telegram_id = user.id
        driver = get_user_by_telegram_id(telegram_id)
        
        if telegram_id in self.pending_2fa:
            del self.pending_2fa[telegram_id]
        
        if driver:
            await auth_manager.cancel_auth(driver.id)
        
        await update.message.reply_text(
            "Авторизация отменена.",
            reply_markup=self._get_menu_for_user(user.id)
        )
        return ConversationHandler.END
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Вы не зарегистрированы.\n"
                "Используйте /start для регистрации."
            )
            return
        
        status_emoji = "✅" if driver.active else "⏸"
        status_text = "Активен" if driver.active else "Приостановлен"
        
        city_name = driver.city_name
        if city_name:
            location_display = city_name
        elif driver.latitude and driver.longitude:
            location_display = f"{driver.latitude:.4f}, {driver.longitude:.4f}"
        else:
            location_display = "не указано"
        
        user_session = get_user_session(driver.id)
        auth_status = "Подключён" if (user_session and user_session.is_authorized) else "Не подключён"
        
        active_groups = get_user_groups(driver.id, active_only=True)
        groups_count = len(active_groups) if active_groups else 0
        
        await update.message.reply_text(
            f"Ваш статус: <u>{status_emoji} {status_text}</u>\n"
            f"📍 Местоположение: <u>{location_display}</u>\n"
            f"📏 Радиус поиска: <u>{driver.radius_km or 'не указан'} км</u>\n"
            f"💰 Мин. сумма: <u>{driver.min_price or 0} руб.</u>\n"
            f"📱 Telegram-аккаунт: <u>{auth_status}</u>\n"
            f"👥 Групп подключено: <u>{groups_count}</u>",
            reply_markup=self._get_menu_for_user(user.id),
            parse_mode='HTML'
        )
    
    async def update_location_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Укажите новое местоположение:\n"
            "• Отправьте геолокацию\n"
            "• Напишите название города\n"
            "• Или введите координаты (например: 56.8389 60.6057)",
            reply_markup=self._location_keyboard()
        )
    
    async def quick_location_update(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Вы не зарегистрированы. Используйте /start"
            )
            return
        
        location = update.message.location
        city_name = await get_city_by_coordinates_async(location.latitude, location.longitude)
        
        create_or_update_user(
            telegram_id=user.id,
            latitude=location.latitude,
            longitude=location.longitude,
            city_name=city_name
        )
        
        location_info = f"Город: {city_name}" if city_name else f"Координаты: {location.latitude:.4f}, {location.longitude:.4f}"
        
        await update.message.reply_text(
            f"✅ Геолокация обновлена!\n📍 {location_info}",
            reply_markup=self._get_menu_for_user(user.id)
        )
    
    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Вы не зарегистрированы. Используйте /start"
            )
            return
        
        settings = get_driver_settings(driver.id)
        keyboard = self._build_settings_keyboard(settings)
        
        await update.message.reply_text(
            self._format_settings_text(settings, driver.id),
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    
    def _format_settings_text(self, settings, driver_id: int = None) -> str:
        quiet_status = "Включены" if settings and settings.quiet_hours_enabled else "Выключены"
        quiet_start = settings.quiet_hours_start if settings else "23:00"
        quiet_end = settings.quiet_hours_end if settings else "07:00"
        
        busy_status = "Нет"
        if settings and settings.busy_until:
            from datetime import datetime
            import pytz
            now = datetime.now(pytz.timezone('Europe/Moscow'))
            busy_utc = settings.busy_until
            if busy_utc.tzinfo is None:
                busy_utc = pytz.UTC.localize(busy_utc)
            busy_msk = busy_utc.astimezone(pytz.timezone('Europe/Moscow'))
            if busy_msk > now:
                busy_status = f"До {busy_msk.strftime('%H:%M')}"
            else:
                busy_status = "Нет"
        
        fav_count = 0
        bl_count = 0
        if driver_id:
            routes = get_favorite_routes(driver_id)
            fav_count = len(routes)
            blacklist = get_blacklist(driver_id)
            bl_count = len(blacklist)
        
        return (
            "<b>⚙️ Настройки</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>🌙 Тихие часы:</b> {quiet_status}\n"
            f"   Время: {quiet_start} - {quiet_end}\n\n"
            f"<b>⏳ Режим занят:</b> {busy_status}\n\n"
            f"<b>⭐ Любимые направления:</b> {fav_count}\n\n"
            f"<b>🚫 Чёрный список:</b> {bl_count}\n\n"
            "Выберите настройку для изменения:"
        )
    
    def _build_settings_keyboard(self, settings, driver_id: int = None) -> InlineKeyboardMarkup:
        quiet_enabled = settings and settings.quiet_hours_enabled
        quiet_icon = "🌙" if quiet_enabled else "🔕"
        quiet_text = f"{quiet_icon} Тихие часы: {'ВКЛ' if quiet_enabled else 'ВЫКЛ'}"
        
        keyboard = [
            [InlineKeyboardButton("👤 Мой профиль", callback_data="settings:profile")],
            [InlineKeyboardButton("💬 Быстрые ответы", callback_data="settings:quick_replies")],
            [InlineKeyboardButton(quiet_text, callback_data="settings:quiet_hours")],
            [InlineKeyboardButton("⏳ Режим занят", callback_data="settings:busy_mode")],
            [InlineKeyboardButton("⭐ Любимые направления", callback_data="settings:favorite_routes")],
            [InlineKeyboardButton("🚫 Чёрный список", callback_data="settings:blacklist")],
        ]
        return InlineKeyboardMarkup(keyboard)
    
    async def handle_settings_main(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        settings = get_driver_settings(driver.id)
        keyboard = self._build_settings_keyboard(settings)
        
        await query.edit_message_text(
            self._format_settings_text(settings, driver.id),
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    
    async def handle_quiet_hours_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        settings = get_driver_settings(driver.id)
        quiet_enabled = settings and settings.quiet_hours_enabled
        quiet_start = settings.quiet_hours_start if settings else "23:00"
        quiet_end = settings.quiet_hours_end if settings else "07:00"
        
        toggle_text = "🔕 Выключить" if quiet_enabled else "🌙 Включить"
        
        keyboard = [
            [InlineKeyboardButton(toggle_text, callback_data="settings:quiet_toggle")],
            [
                InlineKeyboardButton(f"Начало: {quiet_start}", callback_data="settings:quiet_start"),
                InlineKeyboardButton(f"Конец: {quiet_end}", callback_data="settings:quiet_end")
            ],
            [InlineKeyboardButton("« Назад", callback_data="settings:main")]
        ]
        
        status_text = "включены" if quiet_enabled else "выключены"
        
        await query.edit_message_text(
            f"<b>🌙 Тихие часы</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Статус: <b>{status_text}</b>\n"
            f"Время: {quiet_start} - {quiet_end}\n\n"
            f"В тихие часы уведомления о заказах не приходят.\n"
            f"Часовой пояс: Москва (МСК)",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_quiet_hours_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        settings = get_driver_settings(driver.id)
        new_value = not (settings and settings.quiet_hours_enabled)
        update_driver_settings(driver.id, quiet_hours_enabled=new_value)
        
        status = "включены" if new_value else "выключены"
        await query.answer(f"Тихие часы {status}")
        
        await self.handle_quiet_hours_menu(update, context)
    
    async def handle_quiet_hours_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            return
        
        settings = get_driver_settings(driver.id)
        current_start = settings.quiet_hours_start if settings else "23:00"
        
        hours = ["20:00", "21:00", "22:00", "23:00", "00:00", "01:00"]
        keyboard = []
        row = []
        for hour in hours:
            icon = "✓ " if hour == current_start else ""
            row.append(InlineKeyboardButton(f"{icon}{hour}", callback_data=f"settings:quiet_start_set:{hour}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:quiet_hours")])
        
        await query.edit_message_text(
            "<b>🌙 Начало тихих часов</b>\n\n"
            f"Текущее: {current_start}\n\n"
            "Выберите время начала:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_quiet_hours_end(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            return
        
        settings = get_driver_settings(driver.id)
        current_end = settings.quiet_hours_end if settings else "07:00"
        
        hours = ["05:00", "06:00", "07:00", "08:00", "09:00", "10:00"]
        keyboard = []
        row = []
        for hour in hours:
            icon = "✓ " if hour == current_end else ""
            row.append(InlineKeyboardButton(f"{icon}{hour}", callback_data=f"settings:quiet_end_set:{hour}"))
            if len(row) == 3:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:quiet_hours")])
        
        await query.edit_message_text(
            "<b>🌙 Конец тихих часов</b>\n\n"
            f"Текущее: {current_end}\n\n"
            "Выберите время окончания:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_quiet_hours_start_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            new_time = query.data.split(":")[3]
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        update_driver_settings(driver.id, quiet_hours_start=new_time)
        await query.answer(f"Начало тихих часов: {new_time}")
        
        await self.handle_quiet_hours_menu(update, context)
    
    async def handle_quiet_hours_end_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            new_time = query.data.split(":")[3]
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        update_driver_settings(driver.id, quiet_hours_end=new_time)
        await query.answer(f"Конец тихих часов: {new_time}")
        
        await self.handle_quiet_hours_menu(update, context)
    
    async def handle_busy_mode_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        settings = get_driver_settings(driver.id)
        
        busy_status = "Не активен"
        busy_until_text = ""
        if settings and settings.busy_until:
            from datetime import datetime
            import pytz
            now = datetime.now(pytz.timezone('Europe/Moscow'))
            busy_utc = settings.busy_until
            if busy_utc.tzinfo is None:
                busy_utc = pytz.UTC.localize(busy_utc)
            busy_msk = busy_utc.astimezone(pytz.timezone('Europe/Moscow'))
            if busy_msk > now:
                busy_status = "Активен"
                busy_until_text = f"\nДо: {busy_msk.strftime('%H:%M')} МСК"
        
        keyboard = [
            [InlineKeyboardButton("1 час", callback_data="settings:busy_set:1")],
            [InlineKeyboardButton("2 часа", callback_data="settings:busy_set:2")],
            [InlineKeyboardButton("До утра (до 08:00)", callback_data="settings:busy_set:morning")],
            [InlineKeyboardButton("🔔 Снять режим занят", callback_data="settings:busy_clear")],
            [InlineKeyboardButton("« Назад", callback_data="settings:main")]
        ]
        
        await query.edit_message_text(
            f"<b>⏳ Режим занят</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Статус: <b>{busy_status}</b>{busy_until_text}\n\n"
            f"В режиме «занят» уведомления о заказах не приходят.\n"
            f"Выберите на сколько установить:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_busy_mode_set(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            duration = query.data.split(":")[2]
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        from datetime import datetime, timedelta
        import pytz
        
        now = datetime.now(pytz.timezone('Europe/Moscow'))
        
        if duration == "morning":
            morning_today = now.replace(hour=8, minute=0, second=0, microsecond=0)
            if now >= morning_today:
                until = morning_today + timedelta(days=1)
            else:
                until = morning_today
            duration_text = "до утра (08:00)"
        else:
            hours = int(duration)
            until = now + timedelta(hours=hours)
            duration_text = f"на {hours} ч."
        
        until_utc = until.astimezone(pytz.UTC).replace(tzinfo=None)
        set_user_busy(driver.id, until_utc)
        
        await query.answer(f"Режим занят установлен {duration_text}")
        
        await self.handle_busy_mode_menu(update, context)
    
    async def handle_busy_mode_clear(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        clear_user_busy(driver.id)
        await query.answer("Режим занят снят")
        
        await self.handle_busy_mode_menu(update, context)
    
    async def handle_favorite_routes_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        routes = get_favorite_routes(driver.id)
        
        keyboard = []
        
        if routes:
            for route in routes:
                priority_icon = "⭐" if route.priority_notify else "☆"
                route_text = f"{priority_icon} {route.point_a} → {route.point_b}"
                keyboard.append([
                    InlineKeyboardButton(route_text, callback_data=f"settings:fav_toggle:{route.id}"),
                    InlineKeyboardButton("❌", callback_data=f"settings:fav_remove:{route.id}")
                ])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить маршрут", callback_data="settings:fav_add")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:main")])
        
        routes_text = ""
        if routes:
            routes_text = "Ваши любимые направления:\n\n"
            for route in routes:
                priority = "⭐ приоритет" if route.priority_notify else "☆ обычный"
                routes_text += f"• {route.point_a} → {route.point_b} ({priority})\n"
            routes_text += "\nНажмите на маршрут чтобы переключить приоритет.\n❌ — удалить маршрут"
        else:
            routes_text = "У вас пока нет любимых направлений.\n\nДобавьте маршруты, по которым ездите чаще всего — заказы по ним будут отмечены ⭐"
        
        await query.edit_message_text(
            f"<b>⭐ Любимые направления</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{routes_text}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_favorite_route_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.favorite_route_input[user.id] = {'stage': 'point_a', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:fav_cancel")]]
        
        await query.edit_message_text(
            "<b>➕ Добавить маршрут</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите <b>точку отправления (А)</b>:\n\n"
            "Например: Екатеринбург, Челябинск, Тюмень",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_favorite_route_input(self, telegram_id: int, text: str, message):
        if telegram_id not in self.favorite_route_input:
            return False
        
        data = self.favorite_route_input[telegram_id]
        stage = data.get('stage')
        driver_id = data.get('driver_id')
        
        if stage == 'point_a':
            data['point_a'] = text.strip()
            data['stage'] = 'point_b'
            
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:fav_cancel")]]
            
            await message.reply_text(
                f"<b>➕ Добавить маршрут</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Точка А: <b>{data['point_a']}</b>\n\n"
                f"Теперь введите <b>точку назначения (Б)</b>:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return True
        
        elif stage == 'point_b':
            point_a = data['point_a']
            point_b = text.strip()
            
            del self.favorite_route_input[telegram_id]
            
            route = add_favorite_route(driver_id, point_a, point_b)
            
            if route:
                driver = get_user_by_telegram_id(telegram_id)
                routes = get_favorite_routes(driver.id)
                
                keyboard = []
                for r in routes:
                    priority_icon = "⭐" if r.priority_notify else "☆"
                    route_text = f"{priority_icon} {r.point_a} → {r.point_b}"
                    keyboard.append([
                        InlineKeyboardButton(route_text, callback_data=f"settings:fav_toggle:{r.id}"),
                        InlineKeyboardButton("❌", callback_data=f"settings:fav_remove:{r.id}")
                    ])
                keyboard.append([InlineKeyboardButton("➕ Добавить маршрут", callback_data="settings:fav_add")])
                keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:main")])
                
                await message.reply_text(
                    f"<b>⭐ Любимые направления</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ Маршрут <b>{point_a} → {point_b}</b> добавлен!\n\n"
                    f"Заказы по этому маршруту будут отмечены ⭐",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode='HTML'
                )
            else:
                await message.reply_text(
                    "Ошибка при добавлении маршрута. Попробуйте снова.",
                    parse_mode='HTML'
                )
            return True
        
        return False
    
    async def handle_favorite_route_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            route_id = int(query.data.split(":")[2])
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        success = remove_favorite_route(route_id, driver.id)
        
        if success:
            await query.answer("Маршрут удалён")
        else:
            await query.answer("Ошибка удаления", show_alert=True)
        
        await self.handle_favorite_routes_menu(update, context)
    
    async def handle_favorite_route_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            route_id = int(query.data.split(":")[2])
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        from src.utils.database import get_session, FavoriteRoute
        session = get_session()
        if session:
            try:
                route = session.query(FavoriteRoute).filter(
                    FavoriteRoute.id == route_id,
                    FavoriteRoute.user_id == driver.id
                ).first()
                if route:
                    route.priority_notify = not route.priority_notify
                    session.commit()
                    status = "включен" if route.priority_notify else "выключен"
                    await query.answer(f"Приоритет {status}")
                else:
                    await query.answer("Маршрут не найден", show_alert=True)
            finally:
                session.close()
        
        await self.handle_favorite_routes_menu(update, context)
    
    async def handle_favorite_route_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        
        if user.id in self.favorite_route_input:
            del self.favorite_route_input[user.id]
        
        await self.handle_favorite_routes_menu(update, context)
    
    async def handle_blacklist_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        blacklist = get_blacklist(driver.id)
        
        keyboard = []
        
        if blacklist:
            for entry in blacklist:
                if entry.block_type == 'author':
                    icon = "👤"
                    name = entry.blocked_name or entry.blocked_username or f"ID: {entry.blocked_id}"
                else:
                    icon = "📢"
                    name = entry.blocked_name or f"ID: {entry.blocked_id}"
                entry_text = f"{icon} {name}"
                keyboard.append([
                    InlineKeyboardButton(entry_text, callback_data=f"settings:bl_info:{entry.id}"),
                    InlineKeyboardButton("❌", callback_data=f"settings:bl_remove:{entry.id}")
                ])
        
        keyboard.append([
            InlineKeyboardButton("👤 Заблокировать автора", callback_data="settings:bl_add_author"),
            InlineKeyboardButton("📢 Группу", callback_data="settings:bl_add_group")
        ])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:main")])
        
        if blacklist:
            authors = [e for e in blacklist if e.block_type == 'author']
            groups = [e for e in blacklist if e.block_type == 'group']
            list_text = f"В чёрном списке:\n• Авторов: {len(authors)}\n• Групп: {len(groups)}\n\n❌ — удалить из списка"
        else:
            list_text = "Чёрный список пуст.\n\nДобавьте авторов или группы, заказы от которых вы не хотите получать."
        
        await query.edit_message_text(
            f"<b>🚫 Чёрный список</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{list_text}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_blacklist_add_author(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.blacklist_input[user.id] = {'type': 'author', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:bl_cancel")]]
        
        await query.edit_message_text(
            "<b>👤 Заблокировать автора</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите <b>@username</b> или <b>ID</b> автора:\n\n"
            "Пример: @username или 123456789",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_blacklist_add_group(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.blacklist_input[user.id] = {'type': 'group', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:bl_cancel")]]
        
        await query.edit_message_text(
            "<b>📢 Заблокировать группу</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите <b>название группы</b> или <b>ID</b>:\n\n"
            "Пример: Межгород Екб или -1001234567890",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_blacklist_input(self, telegram_id: int, text: str, message):
        if telegram_id not in self.blacklist_input:
            return False
        
        data = self.blacklist_input[telegram_id]
        block_type = data.get('type')
        driver_id = data.get('driver_id')
        
        del self.blacklist_input[telegram_id]
        
        text = text.strip()
        blocked_id = None
        blocked_username = None
        blocked_name = None
        
        if block_type == 'author':
            if text.startswith('@'):
                blocked_username = text[1:]
                blocked_name = text
            else:
                try:
                    blocked_id = int(text)
                    blocked_name = f"ID: {blocked_id}"
                except ValueError:
                    blocked_username = text.replace('@', '')
                    blocked_name = text
        else:
            try:
                blocked_id = int(text)
                blocked_name = f"Группа ID: {blocked_id}"
            except ValueError:
                blocked_name = text
        
        entry = add_to_blacklist(
            user_id=driver_id,
            block_type=block_type,
            blocked_id=blocked_id,
            blocked_username=blocked_username,
            blocked_name=blocked_name
        )
        
        if entry:
            driver = get_user_by_telegram_id(telegram_id)
            blacklist = get_blacklist(driver.id)
            
            keyboard = []
            for e in blacklist:
                if e.block_type == 'author':
                    icon = "👤"
                    name = e.blocked_name or e.blocked_username or f"ID: {e.blocked_id}"
                else:
                    icon = "📢"
                    name = e.blocked_name or f"ID: {e.blocked_id}"
                entry_text = f"{icon} {name}"
                keyboard.append([
                    InlineKeyboardButton(entry_text, callback_data=f"settings:bl_info:{e.id}"),
                    InlineKeyboardButton("❌", callback_data=f"settings:bl_remove:{e.id}")
                ])
            keyboard.append([
                InlineKeyboardButton("👤 Заблокировать автора", callback_data="settings:bl_add_author"),
                InlineKeyboardButton("📢 Группу", callback_data="settings:bl_add_group")
            ])
            keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:main")])
            
            type_text = "Автор" if block_type == 'author' else "Группа"
            
            await message.reply_text(
                f"<b>🚫 Чёрный список</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"✅ {type_text} <b>{blocked_name}</b> добавлен в чёрный список!\n\n"
                f"Заказы от этого источника больше не будут приходить.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        else:
            await message.reply_text(
                "Ошибка при добавлении в чёрный список. Попробуйте снова.",
                parse_mode='HTML'
            )
        return True
    
    async def handle_blacklist_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            entry_id = int(query.data.split(":")[2])
        except (IndexError, ValueError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        success = remove_from_blacklist(entry_id, driver.id)
        
        if success:
            await query.answer("Удалено из чёрного списка")
        else:
            await query.answer("Ошибка удаления", show_alert=True)
        
        await self.handle_blacklist_menu(update, context)
    
    async def handle_blacklist_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        
        if user.id in self.blacklist_input:
            del self.blacklist_input[user.id]
        
        await self.handle_blacklist_menu(update, context)
    
    async def handle_profile_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        profile = get_driver_profile(driver.id)
        
        full_name = profile.full_name if profile and profile.full_name else "Не указано"
        car_info = "Не указано"
        if profile and profile.car_brand:
            car_info = f"{profile.car_brand}"
            if profile.car_model:
                car_info += f" {profile.car_model}"
            if profile.car_year:
                car_info += f" ({profile.car_year})"
            if profile.car_capacity:
                car_info += f", {profile.car_capacity} мест"
        
        child_seat = "Да" if profile and profile.has_child_seat else "Нет"
        license_status = "✅ Загружено" if profile and profile.license_front_file_id else "❌ Нет"
        sts_status = "✅ Загружено" if profile and profile.sts_front_file_id else "❌ Нет"
        
        keyboard = [
            [InlineKeyboardButton("✏️ ФИО", callback_data="settings:profile_name")],
            [InlineKeyboardButton("🚗 Авто", callback_data="settings:profile_car")],
            [InlineKeyboardButton(f"🪪 Права: {license_status}", callback_data="settings:profile_license")],
            [InlineKeyboardButton(f"📄 СТС: {sts_status}", callback_data="settings:profile_sts")],
            [InlineKeyboardButton(f"👶 Детское кресло: {child_seat}", callback_data="settings:profile_child_seat")],
            [InlineKeyboardButton("« Назад", callback_data="settings:main")]
        ]
        
        await query.edit_message_text(
            f"<b>👤 Мой профиль</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>ФИО:</b> {full_name}\n"
            f"<b>Авто:</b> {car_info}\n"
            f"<b>Детское кресло:</b> {child_seat}\n"
            f"<b>Права:</b> {license_status}\n"
            f"<b>СТС:</b> {sts_status}\n\n"
            f"Нажмите на кнопку для редактирования:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_profile_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.profile_input[user.id] = {'type': 'name', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:profile_cancel")]]
        
        await query.edit_message_text(
            "<b>✏️ Редактирование ФИО</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите ваше полное ФИО:\n"
            "<i>Например: Иванов Иван Иванович</i>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_profile_car(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.profile_input[user.id] = {'type': 'car', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:profile_cancel")]]
        
        await query.edit_message_text(
            "<b>🚗 Редактирование авто</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите информацию об автомобиле в формате:\n"
            "<code>Марка Модель Год Мест</code>\n\n"
            "<i>Примеры:</i>\n"
            "<code>Kia K5 2022 4</code>\n"
            "<code>Toyota Camry 2020 4</code>\n"
            "<code>Mercedes E200 2021 4</code>",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_profile_license(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        profile = get_driver_profile(driver.id)
        has_front = profile and profile.license_front_file_id
        has_back = profile and profile.license_back_file_id
        
        self.profile_input[user.id] = {'type': 'license_front', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:profile_cancel")]]
        
        status_text = ""
        if has_front and has_back:
            status_text = "✅ Обе стороны загружены\n\n"
        elif has_front:
            status_text = "✅ Лицевая сторона загружена\n❌ Обратная сторона не загружена\n\n"
        elif has_back:
            status_text = "❌ Лицевая сторона не загружена\n✅ Обратная сторона загружена\n\n"
        
        await query.edit_message_text(
            "<b>🪪 Водительское удостоверение</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{status_text}"
            "Отправьте фото <b>лицевой стороны</b> ВУ.\n"
            "После этого будет запрошена обратная сторона.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_profile_sts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        profile = get_driver_profile(driver.id)
        has_front = profile and profile.sts_front_file_id
        has_back = profile and profile.sts_back_file_id
        
        self.profile_input[user.id] = {'type': 'sts_front', 'driver_id': driver.id}
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:profile_cancel")]]
        
        status_text = ""
        if has_front and has_back:
            status_text = "✅ Обе стороны загружены\n\n"
        elif has_front:
            status_text = "✅ Лицевая сторона загружена\n❌ Обратная сторона не загружена\n\n"
        elif has_back:
            status_text = "❌ Лицевая сторона не загружена\n✅ Обратная сторона загружена\n\n"
        
        await query.edit_message_text(
            "<b>📄 Свидетельство о регистрации ТС</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{status_text}"
            "Отправьте фото <b>лицевой стороны</b> СТС.\n"
            "После этого будет запрошена обратная сторона.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_profile_child_seat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        profile = get_driver_profile(driver.id)
        current = profile.has_child_seat if profile else False
        
        update_driver_profile(driver.id, has_child_seat=not current)
        
        new_status = "включено" if not current else "выключено"
        await query.answer(f"Детское кресло: {new_status}")
        
        await self.handle_profile_menu(update, context)
    
    async def handle_profile_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        
        if user.id in self.profile_input:
            del self.profile_input[user.id]
        
        await self.handle_profile_menu(update, context)
    
    async def handle_profile_input(self, telegram_id: int, text: str, message) -> bool:
        if telegram_id not in self.profile_input:
            return False
        
        input_data = self.profile_input[telegram_id]
        input_type = input_data.get('type')
        driver_id = input_data.get('driver_id')
        
        if input_type == 'name':
            del self.profile_input[telegram_id]
            
            update_driver_profile(driver_id, full_name=text.strip())
            
            driver = get_user_by_telegram_id(telegram_id)
            settings = get_driver_settings(driver.id) if driver else None
            keyboard = self._build_settings_keyboard(settings, driver.id if driver else None)
            
            await message.reply_text(
                f"✅ ФИО обновлено: {text.strip()}\n\n"
                "Возвращаемся в профиль...",
                reply_markup=keyboard
            )
            return True
        
        elif input_type == 'car':
            del self.profile_input[telegram_id]
            
            parts = text.strip().split()
            brand = parts[0] if len(parts) > 0 else None
            model = parts[1] if len(parts) > 1 else None
            year = None
            capacity = 4
            
            for part in parts[2:]:
                if part.isdigit():
                    num = int(part)
                    if num > 1900 and num < 2100:
                        year = num
                    elif num >= 1 and num <= 9:
                        capacity = num
            
            update_driver_profile(
                driver_id,
                car_brand=brand,
                car_model=model,
                car_year=year,
                car_capacity=capacity
            )
            
            driver = get_user_by_telegram_id(telegram_id)
            settings = get_driver_settings(driver.id) if driver else None
            keyboard = self._build_settings_keyboard(settings, driver.id if driver else None)
            
            car_text = f"{brand}"
            if model:
                car_text += f" {model}"
            if year:
                car_text += f" ({year})"
            car_text += f", {capacity} мест"
            
            await message.reply_text(
                f"✅ Авто обновлено: {car_text}\n\n"
                "Возвращаемся в профиль...",
                reply_markup=keyboard
            )
            return True
        
        return False
    
    async def handle_photo_upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        telegram_id = user.id
        
        if telegram_id not in self.profile_input:
            return
        
        input_data = self.profile_input[telegram_id]
        input_type = input_data.get('type')
        driver_id = input_data.get('driver_id')
        
        photo = update.message.photo[-1]
        file_id = photo.file_id
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:profile_cancel")]]
        
        if input_type == 'license_front':
            update_driver_profile(driver_id, license_front_file_id=file_id)
            self.profile_input[telegram_id]['type'] = 'license_back'
            
            await update.message.reply_text(
                "✅ Лицевая сторона ВУ сохранена!\n\n"
                "Теперь отправьте фото <b>обратной стороны</b> ВУ.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        
        elif input_type == 'license_back':
            update_driver_profile(driver_id, license_back_file_id=file_id)
            del self.profile_input[telegram_id]
            
            driver = get_user_by_telegram_id(telegram_id)
            settings = get_driver_settings(driver.id) if driver else None
            keyboard = self._build_settings_keyboard(settings, driver.id if driver else None)
            
            await update.message.reply_text(
                "✅ Обратная сторона ВУ сохранена!\n"
                "Водительское удостоверение полностью загружено.",
                reply_markup=keyboard
            )
        
        elif input_type == 'sts_front':
            update_driver_profile(driver_id, sts_front_file_id=file_id)
            self.profile_input[telegram_id]['type'] = 'sts_back'
            
            await update.message.reply_text(
                "✅ Лицевая сторона СТС сохранена!\n\n"
                "Теперь отправьте фото <b>обратной стороны</b> СТС.",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
        
        elif input_type == 'sts_back':
            update_driver_profile(driver_id, sts_back_file_id=file_id)
            del self.profile_input[telegram_id]
            
            driver = get_user_by_telegram_id(telegram_id)
            settings = get_driver_settings(driver.id) if driver else None
            keyboard = self._build_settings_keyboard(settings, driver.id if driver else None)
            
            await update.message.reply_text(
                "✅ Обратная сторона СТС сохранена!\n"
                "Свидетельство о регистрации ТС полностью загружено.",
                reply_markup=keyboard
            )
    
    async def handle_quick_replies_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        quick_replies = get_quick_replies(driver.id)
        
        text = "<b>💬 Быстрые ответы</b>\n"
        text += "━━━━━━━━━━━━━━━━━━━━\n\n"
        text += "Настройте кнопки для быстрого отклика на заказы.\n"
        text += "Эти кнопки появятся под каждым уведомлением о заказе.\n\n"
        
        keyboard = []
        
        if quick_replies:
            text += "<b>Ваши кнопки:</b>\n"
            for qr in quick_replies:
                status = "✅" if qr.is_active else "❌"
                text += f"{status} [{qr.button_text}] → \"{qr.reply_text}\"\n"
                
                keyboard.append([
                    InlineKeyboardButton(
                        f"{'✅' if qr.is_active else '❌'} {qr.button_text}",
                        callback_data=f"settings:qr_toggle:{qr.id}"
                    ),
                    InlineKeyboardButton(
                        "🗑",
                        callback_data=f"settings:qr_remove:{qr.id}"
                    )
                ])
        else:
            text += "<i>Кнопки не настроены. По умолчанию: \"я\" и \"не себе\"</i>\n"
        
        if len(quick_replies) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="settings:qr_add")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="settings:main")])
        
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_quick_reply_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        self.quick_reply_input[user.id] = {
            'driver_id': driver.id,
            'step': 'button_text'
        }
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:qr_cancel")]]
        
        await query.edit_message_text(
            "<b>➕ Новая кнопка быстрого ответа</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Шаг 1/2: Введите <b>текст кнопки</b>\n"
            "(короткий текст, до 20 символов)\n\n"
            "Примеры: \"Беру\", \"Еду\", \"Звоню\"",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_quick_reply_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            qr_id = int(query.data.split(":")[-1])
        except (ValueError, IndexError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        remove_quick_reply(qr_id, driver.id)
        await query.answer("Кнопка удалена")
        
        await self.handle_quick_replies_menu(update, context)
    
    async def handle_quick_reply_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        try:
            qr_id = int(query.data.split(":")[-1])
        except (ValueError, IndexError):
            await query.answer("Ошибка", show_alert=True)
            return
        
        new_state = toggle_quick_reply(qr_id, driver.id)
        status = "включена" if new_state else "выключена"
        await query.answer(f"Кнопка {status}")
        
        await self.handle_quick_replies_menu(update, context)
    
    async def handle_quick_reply_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = query.from_user
        
        if user.id in self.quick_reply_input:
            del self.quick_reply_input[user.id]
        
        await self.handle_quick_replies_menu(update, context)
    
    async def handle_quick_reply_input(self, telegram_id: int, text: str, message) -> bool:
        if telegram_id not in self.quick_reply_input:
            return False
        
        input_data = self.quick_reply_input[telegram_id]
        driver_id = input_data.get('driver_id')
        step = input_data.get('step')
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="settings:qr_cancel")]]
        
        if step == 'button_text':
            button_text = text.strip()[:20]
            
            self.quick_reply_input[telegram_id]['button_text'] = button_text
            self.quick_reply_input[telegram_id]['step'] = 'reply_text'
            
            await message.reply_text(
                "<b>➕ Новая кнопка быстрого ответа</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Кнопка: <b>{button_text}</b>\n\n"
                "Шаг 2/2: Введите <b>текст ответа</b>\n"
                "(текст, который отправится в группу)\n\n"
                "Примеры: \"я\", \"беру заказ\", \"еду от вокзала\"",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode='HTML'
            )
            return True
        
        elif step == 'reply_text':
            button_text = input_data.get('button_text', 'Ответ')
            reply_text = text.strip()[:200]
            
            del self.quick_reply_input[telegram_id]
            
            quick_replies = get_quick_replies(driver_id)
            sort_order = len(quick_replies)
            
            add_quick_reply(driver_id, button_text, reply_text, sort_order)
            
            driver = get_user_by_telegram_id(telegram_id)
            settings = get_driver_settings(driver.id) if driver else None
            keyboard = self._build_settings_keyboard(settings, driver.id if driver else None)
            
            await message.reply_text(
                f"✅ Кнопка добавлена!\n\n"
                f"[{button_text}] → \"{reply_text}\"\n\n"
                "Кнопка будет отображаться под уведомлениями о заказах.",
                reply_markup=keyboard
            )
            return True
        
        return False
    
    async def stop_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Вы не зарегистрированы. Используйте /start",
                reply_markup=self._get_menu_for_user(user.id)
            )
            return
        
        new_active = not driver.active
        create_or_update_user(telegram_id=user.id, active=new_active)
        
        if new_active:
            await update.message.reply_text(
                "🔔 Уведомления возобновлены!",
                reply_markup=self._get_menu_for_user(user.id)
            )
        else:
            await update.message.reply_text(
                "🔕 Уведомления приостановлены.\n"
                "Нажмите 🔔 Уведомления для возобновления.",
                reply_markup=self._get_menu_for_user(user.id)
            )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await update.message.reply_text(
            "Используйте кнопки меню:\n\n"
            "📊 Мой статус — проверить настройки\n"
            "📢 Мои группы — выбрать группы\n"
            "🔐 Авторизация — подключить аккаунт\n"
            "📍 Локация — изменить местоположение\n"
            "🔔 Уведомления — вкл/выкл",
            reply_markup=self._get_menu_for_user(user.id)
        )
    
    async def groups_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Сначала зарегистрируйтесь через /start"
            )
            return
        
        user_session = get_user_session(driver.id)
        if not user_session or not user_session.is_authorized:
            await update.message.reply_text(
                "Для выбора групп нужно подключить Telegram-аккаунт.\n\n"
                "Используйте /auth для авторизации."
            )
            return
        
        await update.message.reply_text("Загружаю список ваших групп...")
        
        telegram_groups = await auth_manager.get_user_groups(driver.id)
        
        if not telegram_groups:
            await update.message.reply_text(
                "Не удалось получить список групп.\n"
                "Возможно, сессия устарела.\n\n"
                "Попробуйте /auth для переподключения."
            )
            return
        
        context.user_data['available_groups'] = telegram_groups
        context.user_data['groups_page'] = 0
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        saved_groups_map = {g.group_id: g.is_active for g in saved_groups}
        
        keyboard = self._build_groups_keyboard(telegram_groups, saved_groups_map, page=0)
        
        selected_count = sum(1 for g in telegram_groups if saved_groups_map.get(g['id'], False))
        total_pages = (len(telegram_groups) + 9) // 10
        
        await update.message.reply_text(
            f"Выберите группы для парсинга заказов:\n\n"
            f"Найдено групп: {len(telegram_groups)}\n"
            f"Выбрано: {selected_count}\n"
            f"Страница: 1/{total_pages}\n\n"
            f"Нажмите на группу чтобы выбрать/убрать",
            reply_markup=keyboard
        )
    
    def _build_groups_keyboard(self, groups: list, saved_map: dict, page: int = 0) -> InlineKeyboardMarkup:
        keyboard = []
        per_page = 10
        start = page * per_page
        end = start + per_page
        page_groups = groups[start:end]
        total_pages = (len(groups) + per_page - 1) // per_page
        
        for group in page_groups:
            group_id = group['id']
            title = group['title']
            is_selected = saved_map.get(group_id, False)
            
            if len(title) > 30:
                title = title[:27] + "..."
            
            check = "✅" if is_selected else "⬜"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"{check} {title}",
                    callback_data=f"toggle_group:{group_id}"
                )
            ])
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"groups_page:{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"groups_page:{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([
            InlineKeyboardButton("🔄 Обновить", callback_data="groups_refresh"),
            InlineKeyboardButton("✅ Готово", callback_data="groups_done")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    async def handle_group_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        callback_data = query.data
        group_id = int(callback_data.split(":")[1])
        
        available_groups = context.user_data.get('available_groups', [])
        
        if not available_groups:
            telegram_groups = await auth_manager.get_user_groups(driver.id)
            if telegram_groups:
                context.user_data['available_groups'] = telegram_groups
                available_groups = telegram_groups
            else:
                await query.edit_message_text(
                    "Сессия устарела. Используйте /groups для обновления."
                )
                return
        
        group_info = next((g for g in available_groups if g['id'] == group_id), None)
        
        if not group_info:
            await query.answer("Группа не найдена", show_alert=True)
            return
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        existing = next((g for g in saved_groups if g.group_id == group_id), None)
        
        if existing:
            toggle_user_group(driver.id, group_id)
        else:
            add_user_group(
                user_id=driver.id,
                group_id=group_id,
                group_title=group_info['title'],
                group_username=group_info.get('username')
            )
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        saved_groups_map = {g.group_id: g.is_active for g in saved_groups}
        
        page = context.user_data.get('groups_page', 0)
        keyboard = self._build_groups_keyboard(available_groups, saved_groups_map, page)
        
        selected_count = sum(1 for g in available_groups if saved_groups_map.get(g['id'], False))
        total_pages = (len(available_groups) + 9) // 10
        
        await query.edit_message_text(
            f"Выберите группы для парсинга заказов:\n\n"
            f"Найдено групп: {len(available_groups)}\n"
            f"Выбрано: {selected_count}\n"
            f"Страница: {page + 1}/{total_pages}\n\n"
            f"Нажмите на группу чтобы выбрать/убрать",
            reply_markup=keyboard
        )
    
    async def handle_groups_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        page = int(query.data.split(":")[1])
        context.user_data['groups_page'] = page
        
        available_groups = context.user_data.get('available_groups', [])
        if not available_groups:
            await query.edit_message_text("Используйте /groups для обновления.")
            return
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        saved_groups_map = {g.group_id: g.is_active for g in saved_groups}
        
        keyboard = self._build_groups_keyboard(available_groups, saved_groups_map, page)
        
        selected_count = sum(1 for g in available_groups if saved_groups_map.get(g['id'], False))
        total_pages = (len(available_groups) + 9) // 10
        
        await query.edit_message_text(
            f"Выберите группы для парсинга заказов:\n\n"
            f"Найдено групп: {len(available_groups)}\n"
            f"Выбрано: {selected_count}\n"
            f"Страница: {page + 1}/{total_pages}\n\n"
            f"Нажмите на группу чтобы выбрать/убрать",
            reply_markup=keyboard
        )
    
    async def handle_groups_done(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        active_groups = get_user_groups(driver.id, active_only=True)
        
        if not active_groups:
            await query.edit_message_text(
                "Вы не выбрали ни одной группы.\n\n"
                "Используйте /groups чтобы выбрать группы для парсинга."
            )
            return
        
        context.user_data.pop('available_groups', None)
        context.user_data['selected_groups'] = [g.group_title for g in active_groups]
        context.user_data['selected_page'] = 0
        
        keyboard = self._build_selected_keyboard(active_groups, page=0)
        groups_list = self._format_selected_list(active_groups, page=0)
        total_pages = (len(active_groups) + 14) // 15
        
        await query.edit_message_text(
            f"Выбрано групп: {len(active_groups)}\n"
            f"Страница: 1/{total_pages}\n\n"
            f"{groups_list}\n\n"
            "Вы будете получать заказы из этих групп.",
            reply_markup=keyboard,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    def _format_selected_list(self, groups, page: int = 0) -> str:
        per_page = 15
        start = page * per_page
        end = start + per_page
        page_groups = groups[start:end]
        
        def make_group_link(g):
            title = g.group_title if hasattr(g, 'group_title') else str(g)
            if hasattr(g, 'group_username') and g.group_username:
                return f'<a href="https://t.me/{g.group_username}">{title}</a>'
            elif hasattr(g, 'group_id') and g.group_id:
                chat_id = str(g.group_id).replace("-100", "")
                return f'<a href="https://t.me/c/{chat_id}">{title}</a>'
            else:
                return title
        
        return "\n".join([f"• {make_group_link(g)}" for g in page_groups])
    
    def _build_selected_keyboard(self, groups, page: int = 0) -> InlineKeyboardMarkup:
        keyboard = []
        per_page = 15
        total_pages = (len(groups) + per_page - 1) // per_page
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"selected_page:{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"selected_page:{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("Изменить выбор", callback_data="change_groups")])
        
        return InlineKeyboardMarkup(keyboard)
    
    async def handle_selected_page(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        page = int(query.data.split(":")[1])
        
        active_groups = get_user_groups(driver.id, active_only=True)
        if not active_groups:
            await query.edit_message_text("Нет выбранных групп.")
            return
        
        keyboard = self._build_selected_keyboard(active_groups, page)
        groups_list = self._format_selected_list(active_groups, page)
        total_pages = (len(active_groups) + 14) // 15
        
        await query.edit_message_text(
            f"Выбрано групп: {len(active_groups)}\n"
            f"Страница: {page + 1}/{total_pages}\n\n"
            f"{groups_list}\n\n"
            "Вы будете получать заказы из этих групп.",
            reply_markup=keyboard,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    async def handle_change_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик кнопки 'Изменить выбор' - запускает выбор групп"""
        query = update.callback_query
        await query.answer()
        await self._start_groups_selection_callback(query, context)
    
    async def handle_start_groups_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик кнопки 'Выбрать группы' - запускает выбор групп"""
        query = update.callback_query
        await query.answer()
        await self._start_groups_selection_callback(query, context)
    
    async def _start_groups_selection_callback(self, query, context: ContextTypes.DEFAULT_TYPE):
        """Общая логика запуска выбора групп через callback"""
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        user_session = get_user_session(driver.id)
        if not user_session or not user_session.is_authorized:
            await query.edit_message_text(
                "Для выбора групп нужно подключить Telegram-аккаунт.\n\n"
                "Используйте /auth для авторизации."
            )
            return
        
        await query.edit_message_text("Загружаю список ваших групп...")
        
        telegram_groups = await auth_manager.get_user_groups(driver.id)
        
        if not telegram_groups:
            await query.edit_message_text(
                "Не удалось получить список групп.\n"
                "Возможно, сессия устарела.\n\n"
                "Попробуйте /auth для переподключения."
            )
            return
        
        context.user_data['available_groups'] = telegram_groups
        context.user_data['groups_page'] = 0
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        saved_groups_map = {g.group_id: g.is_active for g in saved_groups}
        
        keyboard = self._build_groups_keyboard(telegram_groups, saved_groups_map, page=0)
        
        selected_count = sum(1 for g in telegram_groups if saved_groups_map.get(g['id'], False))
        total_pages = (len(telegram_groups) + 9) // 10
        
        await query.edit_message_text(
            f"Выберите группы для парсинга заказов:\n\n"
            f"Найдено групп: {len(telegram_groups)}\n"
            f"Выбрано: {selected_count}\n"
            f"Страница: 1/{total_pages}\n\n"
            f"Нажмите на группу чтобы выбрать/убрать",
            reply_markup=keyboard
        )
    
    async def my_groups_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает список выбранных групп с гиперссылками"""
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await update.message.reply_text(
                "Сначала зарегистрируйтесь через /start"
            )
            return
        
        user_session = get_user_session(driver.id)
        if not user_session or not user_session.is_authorized:
            await update.message.reply_text(
                "Для просмотра групп нужно подключить Telegram-аккаунт.\n\n"
                "Используйте /auth для авторизации."
            )
            return
        
        active_groups = get_user_groups(driver.id, active_only=True)
        
        if not active_groups:
            keyboard = [[InlineKeyboardButton("Выбрать группы", callback_data="start_groups_selection")]]
            await update.message.reply_text(
                "У вас пока не выбрано ни одной группы.\n\n"
                "Нажмите кнопку ниже, чтобы выбрать группы для мониторинга заказов.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        groups_list = self._format_selected_list(active_groups, page=0)
        total_pages = (len(active_groups) + 14) // 15
        keyboard = self._build_selected_keyboard(active_groups, page=0)
        
        await update.message.reply_text(
            f"Ваши группы ({len(active_groups)}):\n"
            f"Страница: 1/{total_pages}\n\n"
            f"{groups_list}\n\n"
            "Вы получаете заказы из этих групп.",
            reply_markup=keyboard,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    async def handle_menu_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        text = update.message.text
        
        if text == MENU_STATUS:
            await self.status_command(update, context)
        elif text == MENU_GROUPS:
            await self.my_groups_command(update, context)
        elif text == MENU_AUTH:
            await self.auth_command(update, context)
        elif text == MENU_LOCATION:
            await self.update_location_command(update, context)
        elif text == MENU_NOTIFICATIONS:
            await self.stop_command(update, context)
        elif text == MENU_SETTINGS:
            await self.settings_command(update, context)
        elif text == MENU_HELP:
            await self.help_command(update, context)
        elif text == MENU_ADMIN:
            await self.admin_command(update, context)
    
    async def handle_take_order(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        
        user = query.from_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.answer("Вы не зарегистрированы", show_alert=True)
            return
        
        user_session = get_user_session(driver.id)
        if not user_session or not user_session.is_authorized or not user_session.session_string:
            await query.answer("Подключите Telegram через /auth", show_alert=True)
            return
        
        try:
            parts = query.data.split(":")
            group_id = int(parts[1])
            message_id = int(parts[2])
            reply_text = parts[3] if len(parts) > 3 else "я"
        except (IndexError, ValueError):
            await query.answer("Ошибка данных заказа", show_alert=True)
            return
        
        await query.answer(f"Отправляю '{reply_text}' в группу...")
        
        success, error_msg = await self._send_reply_via_telethon(
            user_session.session_string, 
            group_id, 
            message_id, 
            reply_text
        )
        
        if not success and "admin privileges" in error_msg.lower():
            bot_message_id = query.message.message_id
            notification = get_notification_by_message_id(driver.id, bot_message_id)
            
            if notification and notification.route_key:
                group_links = get_order_group_links(notification.route_key, driver.id)
                
                for link in group_links:
                    if link.group_id != group_id and link.message_id:
                        success, error_msg = await self._send_reply_via_telethon(
                            user_session.session_string,
                            link.group_id,
                            link.message_id,
                            reply_text
                        )
                        if success:
                            break
        
        if success:
            keyboard = query.message.reply_markup
            new_keyboard = []
            if keyboard:
                for row in keyboard.inline_keyboard:
                    new_row = []
                    for button in row:
                        if not button.callback_data or not button.callback_data.startswith("take_order:"):
                            new_row.append(button)
                    if new_row:
                        new_keyboard.append(new_row)
            
            original_html = query.message.text_html or query.message.text
            new_text = original_html + "\n\n✅ Вы откликнулись на заказ!"
            await query.edit_message_text(
                text=new_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(new_keyboard) if new_keyboard else None
            )
        else:
            await query.message.reply_text(f"Не удалось отправить: {error_msg}")
    
    async def _send_reply_via_telethon(self, session_string: str, group_id: int, message_id: int, text: str):
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        from src.config import TELEGRAM_API_ID, TELEGRAM_API_HASH
        
        client = None
        try:
            client = TelegramClient(
                StringSession(session_string),
                TELEGRAM_API_ID,
                TELEGRAM_API_HASH
            )
            await client.connect()
            
            if not await client.is_user_authorized():
                return False, "Сессия устарела. Используйте /auth"
            
            entity = await client.get_entity(group_id)
            
            await client.send_message(
                entity,
                text,
                reply_to=message_id
            )
            
            return True, "Сообщение отправлено"
            
        except Exception as e:
            logger.error(f"Telethon send error: {e}")
            return False, str(e)
        finally:
            if client:
                await client.disconnect()
    
    async def handle_groups_refresh(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer("Обновляю список...")
        
        user = update.effective_user
        driver = get_user_by_telegram_id(user.id)
        
        if not driver:
            await query.edit_message_text("Ошибка. Используйте /start")
            return
        
        telegram_groups = await auth_manager.get_user_groups(driver.id)
        
        if not telegram_groups:
            await query.edit_message_text(
                "Не удалось получить список групп.\n"
                "Возможно, сессия устарела.\n\n"
                "Попробуйте /auth для переподключения."
            )
            return
        
        context.user_data['available_groups'] = telegram_groups
        context.user_data['groups_page'] = 0
        
        saved_groups = get_user_groups(driver.id, active_only=False)
        saved_groups_map = {g.group_id: g.is_active for g in saved_groups}
        
        keyboard = self._build_groups_keyboard(telegram_groups, saved_groups_map, page=0)
        
        selected_count = sum(1 for g in telegram_groups if saved_groups_map.get(g['id'], False))
        total_pages = (len(telegram_groups) + 9) // 10
        
        await query.edit_message_text(
            f"Выберите группы для парсинга заказов:\n\n"
            f"Найдено групп: {len(telegram_groups)}\n"
            f"Выбрано: {selected_count}\n"
            f"Страница: 1/{total_pages}\n\n"
            f"Нажмите на группу чтобы выбрать/убрать",
            reply_markup=keyboard
        )
    
    def _is_admin(self, telegram_id: int) -> bool:
        if ADMIN_TELEGRAM_ID and telegram_id == ADMIN_TELEGRAM_ID:
            return True
        user = get_user_by_telegram_id(telegram_id)
        return user and user.is_admin
    
    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        
        if not self._is_admin(user.id):
            await update.message.reply_text("⛔ У вас нет доступа к этой команде.")
            return
        
        keyboard = [
            [InlineKeyboardButton("👥 Водители", callback_data="admin:users:page:0")],
            [InlineKeyboardButton("📢 Все группы", callback_data="admin:all_groups:page:0")],
            [InlineKeyboardButton("📋 Наши группы", callback_data="admin:service_groups:page:0")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        ]
        
        await update.message.reply_text(
            "👑 <b>Админ-панель</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Выберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_admin_main(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("⛔ У вас нет доступа.")
            return
        
        keyboard = [
            [InlineKeyboardButton("👥 Водители", callback_data="admin:users:page:0")],
            [InlineKeyboardButton("📢 Все группы", callback_data="admin:all_groups:page:0")],
            [InlineKeyboardButton("📋 Наши группы", callback_data="admin:service_groups:page:0")],
            [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        ]
        
        await query.edit_message_text(
            "👑 <b>Админ-панель</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Выберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_admin_users(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        data = query.data
        page = 0
        if ":page:" in data:
            try:
                page = int(data.split(":page:")[1])
            except (ValueError, IndexError):
                page = 0
        
        per_page = 10
        offset = page * per_page
        users, total = get_all_users(limit=per_page, offset=offset)
        total_pages = (total + per_page - 1) // per_page
        
        keyboard = []
        for u in users:
            status_emoji = "🟢" if u.active else "🔴"
            auth_emoji = "🔑" if u.is_authorized else ""
            admin_emoji = "👑" if u.is_admin else ""
            name = u.first_name or "Без имени"
            username_part = f" (@{u.username})" if u.username else ""
            display_name = f"{name}{username_part}"
            if len(display_name) > 30:
                display_name = display_name[:27] + "..."
            keyboard.append([InlineKeyboardButton(
                f"{status_emoji}{auth_emoji}{admin_emoji} {display_name}",
                callback_data=f"admin:user:{u.id}:info"
            )])
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("« Назад", callback_data=f"admin:users:page:{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперёд »", callback_data=f"admin:users:page:{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("🔍 Поиск", callback_data="admin:search")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin:main")])
        
        await query.edit_message_text(
            f"👥 <b>Водители</b> ({total})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📄 Страница {page+1}/{total_pages}\n\n"
            f"🟢 активен  🔴 неактивен\n"
            f"🔑 авторизован  👑 админ",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_admin_user_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        data = query.data
        parts = data.split(":")
        
        try:
            user_id = int(parts[2])
        except (ValueError, IndexError):
            await query.edit_message_text("Ошибка: неверный ID пользователя")
            return
        
        action = parts[3] if len(parts) > 3 else "info"
        
        user = get_user_by_id(user_id)
        if not user:
            await query.edit_message_text("Пользователь не найден")
            return
        
        if action == "toggle_admin":
            new_admin_status = not user.is_admin
            set_user_admin(user.telegram_id, new_admin_status)
            user = get_user_by_id(user_id)
        
        user_groups = get_user_groups(user_id, active_only=False)
        
        if action == "groups":
            if not user_groups:
                text = f"У пользователя @{user.username or user.first_name} нет подключённых групп."
            else:
                active_groups = [g for g in user_groups if g.is_active]
                inactive_groups = [g for g in user_groups if not g.is_active]
                
                def make_group_link(g):
                    if g.group_username:
                        return f'<a href="https://t.me/{g.group_username}">{g.group_title}</a>'
                    else:
                        chat_id = str(g.group_id).replace("-100", "")
                        return f'<a href="https://t.me/c/{chat_id}">{g.group_title}</a>'
                
                text = f"Группы пользователя @{user.username or user.first_name}:\n\n"
                
                if active_groups:
                    text += "🟢 Активные:\n"
                    for g in active_groups:
                        text += f"  • {make_group_link(g)}\n"
                
                if inactive_groups:
                    text += "\n🔴 Неактивные:\n"
                    for g in inactive_groups:
                        text += f"  • {make_group_link(g)}\n"
                
                text += f"\nВсего: {len(user_groups)}, активных: {len(active_groups)}"
            
            keyboard = [
                [InlineKeyboardButton("« К профилю", callback_data=f"admin:user:{user_id}:info")],
                [InlineKeyboardButton("« Главное меню", callback_data="admin:main")]
            ]
            
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML', disable_web_page_preview=True)
            return
        
        stats = get_user_stats(user_id)
        
        status_text = "🟢 Активен" if user.active else "🔴 Неактивен"
        auth_text = "🔑 Авторизован" if user.is_authorized else "❌ Не авторизован"
        admin_text = "👑 Админ" if user.is_admin else "Обычный пользователь"
        
        location = user.city_name or (f"{user.latitude:.4f}, {user.longitude:.4f}" if user.latitude else "не указана")
        
        groups_text = ""
        if user_groups:
            active_groups = [g for g in user_groups if g.is_active]
            groups_text = f"\nГрупп подключено: {len(active_groups)}"
        
        text = (
            f"Водитель: {user.first_name or 'без имени'}\n"
            f"Username: @{user.username or 'нет'}\n"
            f"Telegram ID: {user.telegram_id}\n\n"
            f"Статус: {status_text}\n"
            f"Авторизация: {auth_text}\n"
            f"Роль: {admin_text}\n\n"
            f"Локация: {location}\n"
            f"Радиус: {user.radius_km or 50} км\n"
            f"Мин. цена: {user.min_price or 0} руб.{groups_text}\n\n"
            f"Статистика:\n"
            f"- Уведомлений получено: {stats.get('notifications_total', 0)}\n"
            f"- Откликов всего: {stats.get('responses_total', 0)}\n"
            f"- Откликов за день: {stats.get('responses_day', 0)}\n"
            f"- Откликов за неделю: {stats.get('responses_week', 0)}\n"
            f"- Откликов за месяц: {stats.get('responses_month', 0)}"
        )
        
        keyboard = []
        
        admin_btn_text = "❌ Снять админа" if user.is_admin else "👑 Сделать админом"
        keyboard.append([InlineKeyboardButton(admin_btn_text, callback_data=f"admin:user:{user_id}:toggle_admin")])
        
        if user_groups:
            keyboard.append([InlineKeyboardButton("Группы пользователя", callback_data=f"admin:user:{user_id}:groups")])
        
        keyboard.append([InlineKeyboardButton("« К списку", callback_data="admin:users:page:0")])
        keyboard.append([InlineKeyboardButton("« Главное меню", callback_data="admin:main")])
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def handle_admin_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        stats = get_system_stats()
        
        top_groups_text = ""
        if stats.get('top_groups'):
            top_groups_text = "\n\n🏆 <b>Топ групп:</b>\n"
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
            for i, (name, count) in enumerate(stats['top_groups'][:5], 0):
                medal = medals[i] if i < 5 else f"{i+1}."
                top_groups_text += f"{medal} {name}: {count}\n"
        
        text = (
            "📊 <b>Статистика системы</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👥 <b>Пользователи:</b>\n"
            f"   📋 Всего: {stats.get('total_users', 0)}\n"
            f"   🟢 Активных: {stats.get('active_users', 0)}\n"
            f"   🔑 Авторизованных: {stats.get('authorized_users', 0)}\n\n"
            f"🚕 <b>Заказы:</b>\n"
            f"   📋 Всего: {stats.get('orders_total', 0)}\n"
            f"   📅 За день: {stats.get('orders_day', 0)}\n"
            f"   📆 За неделю: {stats.get('orders_week', 0)}\n"
            f"   🗓 За месяц: {stats.get('orders_month', 0)}\n\n"
            f"✋ <b>Отклики:</b>\n"
            f"   📋 Всего: {stats.get('responses_total', 0)}\n"
            f"   📅 За день: {stats.get('responses_day', 0)}"
            f"{top_groups_text}"
        )
        
        keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin:main")]]
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    async def handle_admin_all_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        data = query.data
        page = 0
        if ":page:" in data:
            try:
                page = int(data.split(":page:")[1])
            except (ValueError, IndexError):
                page = 0
        
        all_groups = get_all_unique_groups()
        
        per_page = 15
        total = len(all_groups)
        total_pages = max(1, (total + per_page - 1) // per_page)
        start = page * per_page
        end = start + per_page
        page_groups = all_groups[start:end]
        
        def make_group_link(group_id, group_title, group_username):
            if group_username:
                return f'<a href="https://t.me/{group_username}">{group_title}</a>'
            else:
                chat_id = str(group_id).replace("-100", "")
                return f'<a href="https://t.me/c/{chat_id}">{group_title}</a>'
        
        if not all_groups:
            text = "📢 <b>Группы</b>\n━━━━━━━━━━━━━━━━━━━━\n\n❌ Нет групп в системе."
        else:
            text = f"📢 <b>Все группы</b> ({total})\n━━━━━━━━━━━━━━━━━━━━\n📄 Страница {page+1}/{total_pages}\n\n"
            for g in page_groups:
                link = make_group_link(g.group_id, g.group_title, g.group_username)
                driver_count = getattr(g, 'driver_count', 0) or 0
                admin_count = getattr(g, 'admin_count', 0) or 0
                text += f"• {link}\n   🚗{driver_count} 👑{admin_count}\n"
            text += "\n🚗 водители  👑 админы"
        
        keyboard = []
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("« Назад", callback_data=f"admin:all_groups:page:{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперёд »", callback_data=f"admin:all_groups:page:{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("🔄 Синхронизировать всё себе", callback_data="admin:sync_groups")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin:main")])
        
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML', disable_web_page_preview=True)
    
    async def handle_admin_sync_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer("Синхронизация...")
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        user = get_user_by_telegram_id(query.from_user.id)
        if user:
            sync_all_groups_to_admin(user.id)
            await query.edit_message_text(
                "✅ Все группы синхронизированы!\n\n"
                "Теперь вы получаете заказы из всех групп, добавленных водителями.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("« К списку групп", callback_data="admin:all_groups:page:0")],
                    [InlineKeyboardButton("« Главное меню", callback_data="admin:main")]
                ])
            )
        else:
            await query.edit_message_text("Ошибка: пользователь не найден")
    
    async def handle_admin_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("У вас нет доступа.")
            return
        
        self.admin_search_mode[query.from_user.id] = True
        
        await query.edit_message_text(
            "🔍 Поиск пользователей\n\n"
            "Введите имя, никнейм или город для поиска:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="admin:search_cancel")]
            ])
        )
    
    async def handle_admin_search_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        
        user_id = query.from_user.id
        if user_id in self.admin_search_mode:
            del self.admin_search_mode[user_id]
        
        keyboard = [
            [InlineKeyboardButton("Водители", callback_data="admin:users:page:0")],
            [InlineKeyboardButton("Все группы", callback_data="admin:all_groups:page:0")],
            [InlineKeyboardButton("Статистика системы", callback_data="admin:stats")],
        ]
        
        await query.edit_message_text(
            "Админ-панель\n\n"
            "Выберите раздел:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def handle_admin_search_query(self, user_id: int, search_query: str, message):
        users = search_users(search_query)
        
        if not users:
            keyboard = [
                [InlineKeyboardButton("🔍 Искать ещё", callback_data="admin:search")],
                [InlineKeyboardButton("« К списку водителей", callback_data="admin:users:page:0")],
                [InlineKeyboardButton("« Главное меню", callback_data="admin:main")]
            ]
            await message.reply_text(
                f"По запросу \"{search_query}\" ничего не найдено.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = []
        for u in users:
            status_emoji = "🟢" if u.active else "🔴"
            auth_emoji = "🔑" if u.is_authorized else ""
            admin_emoji = "👑" if u.is_admin else ""
            name = u.first_name or "Без имени"
            username_part = f" (@{u.username})" if u.username else ""
            display_name = f"{name}{username_part}"
            if len(display_name) > 30:
                display_name = display_name[:27] + "..."
            keyboard.append([InlineKeyboardButton(
                f"{status_emoji}{auth_emoji}{admin_emoji} {display_name}",
                callback_data=f"admin:user:{u.id}:info"
            )])
        
        keyboard.append([InlineKeyboardButton("🔍 Искать ещё", callback_data="admin:search")])
        keyboard.append([InlineKeyboardButton("« К списку водителей", callback_data="admin:users:page:0")])
        keyboard.append([InlineKeyboardButton("« Главное меню", callback_data="admin:main")])
        
        await message.reply_text(
            f"🔍 Результаты поиска \"{search_query}\":\n"
            f"Найдено: {len(users)}\n\n"
            f"🟢 = активен | 🔴 = неактивен\n"
            f"🔑 = авторизован | 👑 = админ",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def handle_admin_service_groups(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle service groups (our groups) list"""
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("⛔ У вас нет доступа.")
            return
        
        data = query.data
        page = 0
        if ":page:" in data:
            try:
                page = int(data.split(":page:")[1])
            except (ValueError, IndexError):
                page = 0
        
        per_page = 10
        offset = page * per_page
        groups, total = get_service_groups(active_only=False, limit=per_page, offset=offset)
        total_pages = max(1, (total + per_page - 1) // per_page)
        
        def make_group_link(group):
            if group.group_username:
                return f'<a href="https://t.me/{group.group_username}">{group.group_title}</a>'
            else:
                chat_id = str(group.group_id).replace("-100", "")
                return f'<a href="https://t.me/c/{chat_id}">{group.group_title}</a>'
        
        if not groups:
            text = (
                "📋 <b>Наши группы</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "❌ Нет добавленных групп.\n\n"
                "Нажмите «➕ Добавить» чтобы добавить группу из списка."
            )
        else:
            text = (
                f"📋 <b>Наши группы</b> ({total})\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 Страница {page+1}/{total_pages}\n\n"
            )
            for g in groups:
                status = "🟢" if g.is_active else "🔴"
                link = make_group_link(g)
                text += f"{status} {link}\n"
            text += "\n🟢 = активна | 🔴 = неактивна"
        
        keyboard = []
        
        for g in groups:
            status = "🟢" if g.is_active else "🔴"
            name = g.group_title[:25] + "..." if len(g.group_title) > 25 else g.group_title
            keyboard.append([
                InlineKeyboardButton(f"{status} {name}", callback_data=f"admin:sg_toggle:{g.group_id}"),
                InlineKeyboardButton("❌", callback_data=f"admin:sg_remove:{g.group_id}")
            ])
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("« Назад", callback_data=f"admin:service_groups:page:{page-1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Вперёд »", callback_data=f"admin:service_groups:page:{page+1}"))
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([
            InlineKeyboardButton("➕ Добавить", callback_data="admin:sg_add"),
            InlineKeyboardButton("🔍 Поиск", callback_data="admin:sg_search")
        ])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin:main")])
        
        await query.edit_message_text(
            text, 
            reply_markup=InlineKeyboardMarkup(keyboard), 
            parse_mode='HTML',
            disable_web_page_preview=True
        )
    
    async def handle_admin_service_group_toggle(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Toggle service group active status"""
        query = update.callback_query
        
        if not self._is_admin(query.from_user.id):
            await query.answer("⛔ У вас нет доступа.")
            return
        
        try:
            group_id = int(query.data.split(":")[2])
        except (ValueError, IndexError):
            await query.answer("Ошибка")
            return
        
        result = toggle_service_group(group_id)
        if result:
            status = "активирована" if result.is_active else "деактивирована"
            await query.answer(f"Группа {status}")
        else:
            await query.answer("Ошибка при изменении статуса")
        
        query.data = "admin:service_groups:page:0"
        await self.handle_admin_service_groups(update, context)
    
    async def handle_admin_service_group_remove(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Remove group from service groups"""
        query = update.callback_query
        
        if not self._is_admin(query.from_user.id):
            await query.answer("⛔ У вас нет доступа.")
            return
        
        try:
            group_id = int(query.data.split(":")[2])
        except (ValueError, IndexError):
            await query.answer("Ошибка")
            return
        
        if remove_service_group(group_id):
            await query.answer("✅ Группа удалена из списка")
        else:
            await query.answer("Ошибка при удалении")
        
        query.data = "admin:service_groups:page:0"
        await self.handle_admin_service_groups(update, context)
    
    async def handle_admin_service_group_add(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show list of available groups to add"""
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("⛔ У вас нет доступа.")
            return
        
        all_groups = get_all_unique_groups()
        service_groups, _ = get_service_groups(active_only=False)
        service_group_ids = {g.group_id for g in service_groups}
        
        available_groups = [g for g in all_groups if g.group_id not in service_group_ids]
        
        if not available_groups:
            await query.edit_message_text(
                "📋 <b>Добавить группу</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                "❌ Нет доступных групп для добавления.\n\n"
                "Все группы из системы уже добавлены в «Наши группы».",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("« Назад", callback_data="admin:service_groups:page:0")]
                ]),
                parse_mode='HTML'
            )
            return
        
        keyboard = []
        for g in available_groups[:15]:
            name = g.group_title[:30] + "..." if len(g.group_title) > 30 else g.group_title
            keyboard.append([InlineKeyboardButton(
                f"➕ {name}",
                callback_data=f"admin:sg_add_confirm:{g.group_id}"
            )])
        
        if len(available_groups) > 15:
            keyboard.append([InlineKeyboardButton("🔍 Поиск по группам", callback_data="admin:sg_search")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin:service_groups:page:0")])
        
        await query.edit_message_text(
            f"📋 <b>Добавить группу</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Доступно групп: {len(available_groups)}\n"
            f"(показаны первые 15)\n\n"
            f"Выберите группу для добавления:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    async def handle_admin_service_group_add_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Confirm adding group to service groups"""
        query = update.callback_query
        
        if not self._is_admin(query.from_user.id):
            await query.answer("⛔ У вас нет доступа.")
            return
        
        try:
            group_id = int(query.data.split(":")[2])
        except (ValueError, IndexError):
            await query.answer("Ошибка")
            return
        
        all_groups = get_all_unique_groups()
        group_info = next((g for g in all_groups if g.group_id == group_id), None)
        
        if not group_info:
            await query.answer("Группа не найдена")
            return
        
        result = add_service_group(
            group_id=group_id,
            group_title=group_info.group_title,
            group_username=group_info.group_username
        )
        
        if result:
            await query.answer(f"✅ Группа «{group_info.group_title}» добавлена!")
        else:
            await query.answer("Ошибка при добавлении")
        
        query.data = "admin:service_groups:page:0"
        await self.handle_admin_service_groups(update, context)
    
    async def handle_admin_service_group_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start group search mode"""
        query = update.callback_query
        await query.answer()
        
        if not self._is_admin(query.from_user.id):
            await query.edit_message_text("⛔ У вас нет доступа.")
            return
        
        self.admin_group_search_mode[query.from_user.id] = True
        
        await query.edit_message_text(
            "🔍 <b>Поиск группы</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "Введите название группы для поиска:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Отмена", callback_data="admin:service_groups:page:0")]
            ]),
            parse_mode='HTML'
        )
    
    async def handle_admin_group_search_query(self, user_id: int, search_query: str, message):
        """Handle group search query"""
        all_groups = search_all_groups(search_query)
        service_groups, _ = get_service_groups(active_only=False)
        service_group_ids = {g.group_id for g in service_groups}
        
        if not all_groups:
            keyboard = [
                [InlineKeyboardButton("🔍 Искать ещё", callback_data="admin:sg_search")],
                [InlineKeyboardButton("« К нашим группам", callback_data="admin:service_groups:page:0")]
            ]
            await message.reply_text(
                f"По запросу «{search_query}» ничего не найдено.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return
        
        keyboard = []
        for g in all_groups:
            is_added = g.group_id in service_group_ids
            prefix = "✅" if is_added else "➕"
            name = g.group_title[:25] + "..." if len(g.group_title) > 25 else g.group_title
            
            if not is_added:
                keyboard.append([InlineKeyboardButton(
                    f"{prefix} {name}",
                    callback_data=f"admin:sg_add_confirm:{g.group_id}"
                )])
            else:
                keyboard.append([InlineKeyboardButton(
                    f"{prefix} {name} (уже добавлена)",
                    callback_data="admin:service_groups:page:0"
                )])
        
        keyboard.append([InlineKeyboardButton("🔍 Искать ещё", callback_data="admin:sg_search")])
        keyboard.append([InlineKeyboardButton("« К нашим группам", callback_data="admin:service_groups:page:0")])
        
        await message.reply_text(
            f"🔍 <b>Результаты поиска</b> «{search_query}»\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Найдено: {len(all_groups)}\n"
            f"✅ = уже добавлена | ➕ = добавить",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    
    def _location_keyboard(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            [[KeyboardButton("Отправить геолокацию", request_location=True)]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    
    def _main_menu_keyboard(self, is_admin: bool = False) -> ReplyKeyboardMarkup:
        rows = [
            [KeyboardButton(MENU_STATUS), KeyboardButton(MENU_GROUPS)],
            [KeyboardButton(MENU_AUTH), KeyboardButton(MENU_LOCATION)],
            [KeyboardButton(MENU_NOTIFICATIONS), KeyboardButton(MENU_SETTINGS)],
            [KeyboardButton(MENU_HELP)]
        ]
        if is_admin:
            rows.append([KeyboardButton(MENU_ADMIN)])
        return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)
    
    def _get_menu_for_user(self, telegram_id: int) -> ReplyKeyboardMarkup:
        driver = get_user_by_telegram_id(telegram_id)
        is_admin = driver.is_admin if driver else False
        return self._main_menu_keyboard(is_admin)
    
    def _build_order_keyboard(self, order_link: str, group_id: int = None, message_id: int = None, driver_db_id: int = None):
        """Build keyboard for order notification with custom quick replies"""
        keyboard = []
        
        if group_id and message_id:
            quick_replies = []
            if driver_db_id:
                quick_replies = get_quick_replies(driver_db_id, active_only=True)
            
            if quick_replies:
                row = []
                for qr in quick_replies[:4]:
                    row.append(InlineKeyboardButton(
                        qr.button_text,
                        callback_data=f"take_order:{group_id}:{message_id}:{qr.reply_text}"
                    ))
                    if len(row) == 2:
                        keyboard.append(row)
                        row = []
                if row:
                    keyboard.append(row)
            else:
                keyboard.append([
                    InlineKeyboardButton(
                        "Взять себе", 
                        callback_data=f"take_order:{group_id}:{message_id}:я"
                    ),
                    InlineKeyboardButton(
                        "Не себе", 
                        callback_data=f"take_order:{group_id}:{message_id}:не себе"
                    )
                ])
        
        if order_link:
            keyboard.append([
                InlineKeyboardButton("Открыть пост", url=order_link)
            ])
        
        return InlineKeyboardMarkup(keyboard) if keyboard else None
    
    async def send_order_notification(self, driver_id: int, order_message: str, order_link: str, 
                                       group_id: int = None, message_id: int = None) -> int:
        """Send order notification and return sent message_id"""
        try:
            driver = get_user_by_telegram_id(driver_id)
            driver_db_id = driver.id if driver else None
            reply_markup = self._build_order_keyboard(order_link, group_id, message_id, driver_db_id)
            
            sent_message = await self.application.bot.send_message(
                chat_id=driver_id,
                text=order_message,
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
            logger.info(f"Notification sent to driver {driver_id}, msg_id: {sent_message.message_id}")
            return sent_message.message_id
        except Exception as e:
            logger.error(f"Failed to send notification to {driver_id}: {e}")
            return None
    
    async def edit_order_notification(self, driver_id: int, message_id: int, order_message: str, 
                                       order_link: str, group_id: int = None, source_message_id: int = None):
        """Edit existing order notification with updated groups list"""
        try:
            driver = get_user_by_telegram_id(driver_id)
            driver_db_id = driver.id if driver else None
            reply_markup = self._build_order_keyboard(order_link, group_id, source_message_id, driver_db_id)
            
            await self.application.bot.edit_message_text(
                chat_id=driver_id,
                message_id=message_id,
                text=order_message,
                parse_mode='HTML',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
            logger.info(f"Notification edited for driver {driver_id}, msg_id: {message_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to edit notification for {driver_id}: {e}")
            return False
    
    def run(self):
        logger.info("Starting driver bot...")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)
    
    async def _clear_commands_menu(self):
        await self.application.bot.delete_my_commands()
        logger.info("Bot commands menu cleared")
    
    async def start_async(self):
        await self.application.initialize()
        await self.application.start()
        await self._clear_commands_menu()
        await self.application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("Driver bot started in async mode")
    
    async def start_without_polling(self):
        await self.application.initialize()
        await self.application.start()
        logger.info("Driver bot initialized without polling (parser only mode)")
    
    async def stop_async(self):
        if self.application.updater and self.application.updater.running:
            await self.application.updater.stop()
        await self.application.stop()
        await self.application.shutdown()


def get_matching_drivers(order_coords: tuple, order_price: Optional[int] = None) -> list:
    matching = []
    drivers = get_active_users()
    
    for driver in drivers:
        driver_coords = (driver.latitude, driver.longitude)
        radius = driver.radius_km or 50
        min_price = driver.min_price or 0
        
        if not driver_coords[0] or not driver_coords[1]:
            continue
        
        if not is_within_radius(driver_coords, order_coords, radius):
            continue
        
        if order_price and min_price > 0 and order_price < min_price:
            continue
        
        driver_info = {
            'telegram_id': driver.telegram_id,
            'username': driver.username,
            'first_name': driver.first_name,
            'latitude': driver.latitude,
            'longitude': driver.longitude,
            'radius_km': driver.radius_km,
            'min_price': driver.min_price,
            'active': driver.active
        }
        matching.append(driver_info)
    
    return matching
