import logging
from typing import List, Optional
from dataclasses import asdict

from src.parser.order_parser import ParsedOrder, format_order_message
from src.utils.database import (
    get_active_users, save_order, get_users_subscribed_to_group, 
    get_admin_users, get_user_groups, normalize_route_key,
    get_existing_notification, add_order_group_link, get_order_group_links,
    save_order_notification, update_notification_message_id, get_user_by_telegram_id,
    is_user_in_quiet_hours, is_user_busy, is_favorite_route, is_blacklisted
)
from src.utils.geo import is_within_radius, calculate_distance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_matching_drivers(order: ParsedOrder, filter_by_group: bool = True) -> List[dict]:
    if not order.point_a_coords:
        logger.warning(f"No coordinates for point A: {order.point_a}")
        return []
    
    logger.info(f"Order coords: A={order.point_a} {order.point_a_coords}, B={order.point_b} {order.point_b_coords}")
    
    if filter_by_group and order.source_group_id:
        drivers = get_users_subscribed_to_group(order.source_group_id)
        logger.info(f"Filtering by group {order.source_group_id}, found {len(drivers)} subscribers")
    elif filter_by_group and order.source_group:
        try:
            group_id = int(order.source_group)
            drivers = get_users_subscribed_to_group(group_id)
            logger.info(f"Filtering by group {group_id}, found {len(drivers)} subscribers")
        except (ValueError, TypeError):
            drivers = get_active_users()
    else:
        drivers = get_active_users()
    
    matching = []
    
    for driver in drivers:
        driver_coords = (driver.latitude, driver.longitude)
        
        if not driver_coords[0] or not driver_coords[1]:
            logger.debug(f"Driver {driver.telegram_id} has no coordinates, skipping")
            continue
        
        radius = driver.radius_km or 50
        min_price = driver.min_price or 0
        
        distance = calculate_distance(driver_coords, order.point_a_coords)
        in_radius = is_within_radius(driver_coords, order.point_a_coords, radius)
        
        logger.debug(f"Driver {driver.first_name or driver.telegram_id}: coords={driver_coords}, "
                    f"radius={radius}km, distance={round(distance, 1)}km, in_radius={in_radius}")
        
        if not in_radius:
            logger.info(f"Driver {driver.first_name or driver.telegram_id} NOT in radius: "
                       f"{round(distance, 1)}km > {radius}km")
            continue
        
        if order.price and min_price > 0 and order.price < min_price:
            logger.info(f"Driver {driver.first_name or driver.telegram_id} filtered by price: "
                       f"{order.price} < {min_price}")
            continue
        
        logger.info(f"Driver {driver.first_name or driver.telegram_id} MATCHES: "
                   f"{round(distance, 1)}km <= {radius}km")
        
        driver_info = {
            'user_id': driver.telegram_id,
            'telegram_id': driver.telegram_id,
            'db_user_id': driver.id,
            'username': driver.username,
            'first_name': driver.first_name,
            'latitude': driver.latitude,
            'longitude': driver.longitude,
            'radius_km': driver.radius_km,
            'min_price': driver.min_price,
            'distance_to_order': round(distance, 1)
        }
        matching.append(driver_info)
    
    matching.sort(key=lambda x: x.get('distance_to_order', 999))
    
    return matching


def create_order_id(order: ParsedOrder) -> str:
    group_id = order.source_group_id if order.source_group_id else order.source_group
    return f"{group_id}_{order.source_link.split('/')[-1]}"


def save_order_to_db(order: ParsedOrder) -> str:
    order_id = create_order_id(order)
    group_id = order.source_group_id if order.source_group_id else None
    if group_id is None and order.source_group:
        try:
            group_id = int(order.source_group)
        except (ValueError, TypeError):
            group_id = None
    result = save_order(
        order_id=order_id,
        point_a=order.point_a,
        point_b=order.point_b,
        price=order.price,
        source_group_id=group_id,
        source_group_title=order.group_title,
        source_link=order.source_link,
        point_a_coords=order.point_a_coords,
        point_b_coords=order.point_b_coords
    )
    return result.id if result else None


def format_driver_notification(order: ParsedOrder, distance: float, group_links: list = None, group_id: int = None, user_id: int = None) -> str:
    source_group_id = group_id or order.source_group_id
    if not source_group_id and order.source_group:
        try:
            source_group_id = int(order.source_group)
        except (ValueError, TypeError):
            source_group_id = None
    
    is_fav = False
    if user_id:
        is_fav = is_favorite_route(user_id, order.point_a, order.point_b)
    
    return format_order_message(order, distance_km=distance, group_title=order.group_title, group_links=group_links, group_id=source_group_id, is_favorite=is_fav)


def check_driver_matches_order(driver, order: ParsedOrder) -> tuple:
    """Check if driver matches order by location and price filters.
    Returns (matches: bool, distance: float or None)"""
    if not order.point_a_coords:
        return False, None
    
    if not driver.latitude or not driver.longitude:
        return False, None
    
    driver_coords = (driver.latitude, driver.longitude)
    radius = driver.radius_km or 50
    min_price = driver.min_price or 0
    
    if not is_within_radius(driver_coords, order.point_a_coords, radius):
        return False, None
    
    if order.price and min_price > 0 and order.price < min_price:
        return False, None
    
    distance = calculate_distance(driver_coords, order.point_a_coords)
    return True, round(distance, 1)


def is_user_subscribed_to_group(user_id: int, group_id: int) -> bool:
    """Check if user is subscribed to a specific group"""
    user_groups = get_user_groups(user_id, active_only=True)
    group_ids = [g.group_id for g in user_groups]
    group_id_abs = abs(group_id) if group_id else 0
    return group_id in group_ids or -group_id_abs in group_ids or group_id_abs in group_ids


class OrderMatcher:
    def __init__(self, bot_send_func=None, bot_edit_func=None):
        self.bot_send_func = bot_send_func
        self.bot_edit_func = bot_edit_func
    
    def _get_author_from_links(self, group_links) -> dict:
        """Get author info from any link that has it"""
        for link in group_links:
            author_id = getattr(link, 'author_id', None)
            if author_id:
                return {
                    'id': author_id,
                    'username': getattr(link, 'author_username', None),
                    'first_name': getattr(link, 'author_first_name', None)
                }
        return {}
    
    async def process_order(self, order: ParsedOrder):
        logger.info(f"Processing order: {order.point_a} -> {order.point_b}")
        
        order_db = save_order_to_db(order)
        order_db_id = order_db if order_db else 0
        logger.info(f"Order saved with DB ID: {order_db_id}")
        
        route_key = normalize_route_key(order.point_a, order.point_b)
        group_id = order.source_group_id if order.source_group_id else None
        if group_id is None and order.source_group:
            try:
                group_id = int(order.source_group)
            except (ValueError, TypeError):
                group_id = None
        
        matching_drivers = find_matching_drivers(order)
        logger.info(f"Found {len(matching_drivers)} matching drivers")
        
        admins = get_admin_users()
        admin_ids = set(admin.telegram_id for admin in admins)
        logger.info(f"Found {len(admins)} admins to notify")
        
        notified_ids = set()
        
        for driver in matching_drivers:
            driver_id = driver.get('telegram_id')
            db_user_id = driver.get('db_user_id')
            distance = driver.get('distance_to_order', 0)
            
            await self._notify_driver(
                driver_id=driver_id,
                db_user_id=db_user_id,
                order=order,
                order_db_id=order_db_id,
                route_key=route_key,
                group_id=group_id,
                distance=distance,
                is_admin_extra=False
            )
            notified_ids.add(driver_id)
        
        for admin in admins:
            admin_id = admin.telegram_id
            if admin_id in notified_ids:
                continue
            
            matches, distance = check_driver_matches_order(admin, order)
            if not matches:
                continue
            
            is_in_group = is_user_subscribed_to_group(admin.id, order.source_group_id)
            
            await self._notify_driver(
                driver_id=admin_id,
                db_user_id=admin.id,
                order=order,
                order_db_id=order_db_id,
                route_key=route_key,
                group_id=group_id,
                distance=distance,
                is_admin_extra=not is_in_group
            )
    
    async def _notify_driver(self, driver_id: int, db_user_id: int, order: ParsedOrder, 
                              order_db_id: int, route_key: str, group_id: int,
                              distance: float, is_admin_extra: bool = False):
        """Send or update notification for a driver"""
        
        if is_user_in_quiet_hours(db_user_id):
            logger.info(f"Driver {driver_id} is in quiet hours, skipping notification")
            return
        
        if is_user_busy(db_user_id):
            logger.info(f"Driver {driver_id} is busy, skipping notification")
            return
        
        if is_blacklisted(db_user_id, author_id=order.author_id, group_id=group_id):
            logger.info(f"Driver {driver_id} has blacklisted author/group, skipping notification")
            return
        
        existing = get_existing_notification(db_user_id, route_key, hours=2)
        
        existing_links = get_order_group_links(route_key, db_user_id)
        already_has_link = any(
            link.source_link == order.source_link 
            for link in existing_links
        )
        
        if existing and existing.message_id and self.bot_edit_func:
            if not already_has_link:
                add_order_group_link(
                    route_key=route_key,
                    user_id=db_user_id,
                    group_id=group_id,
                    group_title=order.group_title or "Группа",
                    source_link=order.source_link,
                    message_id=order.message_id,
                    author_id=order.author_id,
                    author_username=order.author_username,
                    author_first_name=order.author_first_name
                )
                group_links = get_order_group_links(route_key, db_user_id)
            else:
                group_links = existing_links
            
            author_from_links = self._get_author_from_links(group_links)
            if author_from_links:
                order.author_id = author_from_links.get('id') or order.author_id
                order.author_username = author_from_links.get('username') or order.author_username
                order.author_first_name = author_from_links.get('first_name') or order.author_first_name
            
            notification = format_driver_notification(order, distance, group_links=group_links, user_id=db_user_id)
            if is_admin_extra:
                notification = f"[ADMIN] {notification}"
            
            try:
                await self.bot_edit_func(
                    driver_id=driver_id,
                    message_id=existing.message_id,
                    order_message=notification,
                    order_link=order.source_link,
                    group_id=group_id,
                    source_message_id=order.message_id
                )
                logger.info(f"Updated existing notification for driver {driver_id} (msg_id: {existing.message_id})")
                return
            except Exception as e:
                logger.warning(f"Failed to edit message for driver {driver_id}: {e}, will send new")
        
        if self.bot_send_func:
            add_order_group_link(
                route_key=route_key,
                user_id=db_user_id,
                group_id=group_id,
                group_title=order.group_title or "Группа",
                source_link=order.source_link,
                message_id=order.message_id,
                author_id=order.author_id,
                author_username=order.author_username,
                author_first_name=order.author_first_name
            )
            group_links = get_order_group_links(route_key, db_user_id)
            
            author_from_links = self._get_author_from_links(group_links)
            if author_from_links:
                order.author_id = author_from_links.get('id') or order.author_id
                order.author_username = author_from_links.get('username') or order.author_username
                order.author_first_name = author_from_links.get('first_name') or order.author_first_name
            
            notification = format_driver_notification(order, distance, group_links=group_links, user_id=db_user_id)
            if is_admin_extra:
                notification = f"[ADMIN] {notification}"
            
            try:
                sent_message_id = await self.bot_send_func(
                    driver_id=driver_id,
                    order_message=notification,
                    order_link=order.source_link,
                    group_id=group_id,
                    message_id=order.message_id
                )
                
                if sent_message_id:
                    save_order_notification(
                        order_id=order_db_id,
                        user_id=db_user_id,
                        message_id=sent_message_id,
                        route_key=route_key
                    )
                
                logger.info(f"Notification sent to driver {driver_id} (msg_id: {sent_message_id})")
            except Exception as e:
                logger.error(f"Failed to notify driver {driver_id}: {e}")
