import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, BigInteger, String, Float, Boolean, Text, DateTime, ForeignKey, LargeBinary
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.pool import QueuePool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')

if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True
    )
else:
    engine = None
    logger.warning("DATABASE_URL not set, database features disabled")

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    username = Column(String(255))
    first_name = Column(String(255))
    phone = Column(String(20))
    latitude = Column(Float)
    longitude = Column(Float)
    city_name = Column(String(255))
    radius_km = Column(Integer, default=50)
    min_price = Column(Integer, default=0)
    active = Column(Boolean, default=True)
    is_authorized = Column(Boolean, default=False)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    session = relationship("UserSession", back_populates="user", uselist=False)
    groups = relationship("UserGroup", back_populates="user")
    subscription = relationship("Subscription", back_populates="user", uselist=False)
    notifications = relationship("OrderNotification", back_populates="user")
    responses = relationship("OrderResponse", back_populates="user")


class UserSession(Base):
    __tablename__ = 'user_sessions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    session_data = Column(LargeBinary)
    session_string = Column(Text)
    is_authorized = Column(Boolean, default=False)
    auth_phone = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="session")


class UserGroup(Base):
    __tablename__ = 'user_groups'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    group_id = Column(BigInteger, nullable=False)
    group_title = Column(String(255))
    group_username = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_premium = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="groups")


class Subscription(Base):
    __tablename__ = 'subscriptions'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    plan_type = Column(String(50), default='free')
    starts_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", back_populates="subscription")


class Order(Base):
    __tablename__ = 'orders'
    
    id = Column(Integer, primary_key=True)
    point_a = Column(String(255))
    point_b = Column(String(255))
    price = Column(Integer)
    source_group_id = Column(BigInteger)
    source_group_title = Column(String(255))
    source_link = Column(String(500))
    point_a_lat = Column(Float)
    point_a_lon = Column(Float)
    point_b_lat = Column(Float)
    point_b_lon = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


class PremiumGroup(Base):
    __tablename__ = 'premium_groups'
    
    id = Column(Integer, primary_key=True)
    group_id = Column(BigInteger, unique=True, nullable=False)
    group_title = Column(String(255))
    group_username = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class OrderNotification(Base):
    __tablename__ = 'order_notifications'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    sent_at = Column(DateTime, default=datetime.utcnow)
    message_id = Column(BigInteger)
    route_key = Column(String(500), index=True)
    
    order = relationship("Order", backref="notifications")
    user = relationship("User", back_populates="notifications")


class OrderGroupLink(Base):
    __tablename__ = 'order_group_links'
    
    id = Column(Integer, primary_key=True)
    route_key = Column(String(500), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    group_id = Column(BigInteger)
    group_title = Column(String(255))
    source_link = Column(String(500))
    message_id = Column(BigInteger)
    author_id = Column(BigInteger)
    author_username = Column(String(255))
    author_first_name = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User")


class OrderResponse(Base):
    __tablename__ = 'order_responses'
    
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    response_type = Column(String(50))
    responded_at = Column(DateTime, default=datetime.utcnow)
    
    order = relationship("Order", backref="responses")
    user = relationship("User", back_populates="responses")


class DriverSettings(Base):
    __tablename__ = 'driver_settings'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False, unique=True)
    quiet_hours_enabled = Column(Boolean, default=False)
    quiet_hours_start = Column(String(5), default='23:00')
    quiet_hours_end = Column(String(5), default='07:00')
    busy_until = Column(DateTime, nullable=True)
    stats_enabled = Column(Boolean, default=False)
    reminder_delay_minutes = Column(Integer, default=60)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", backref="settings")


class DriverProfile(Base):
    __tablename__ = 'driver_profiles'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False, unique=True)
    full_name = Column(String(255))
    birth_date = Column(String(10))
    license_front_file_id = Column(String(255))
    license_back_file_id = Column(String(255))
    car_brand = Column(String(100))
    car_model = Column(String(100))
    car_year = Column(Integer)
    car_capacity = Column(Integer, default=4)
    has_child_seat = Column(Boolean, default=False)
    sts_front_file_id = Column(String(255))
    sts_back_file_id = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", backref="profile")


class FavoriteRoute(Base):
    __tablename__ = 'favorite_routes'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    point_a = Column(String(255), nullable=False)
    point_b = Column(String(255), nullable=False)
    priority_notify = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", backref="favorite_routes")


class Blacklist(Base):
    __tablename__ = 'blacklist'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    block_type = Column(String(20), nullable=False)
    blocked_id = Column(BigInteger)
    blocked_username = Column(String(255))
    blocked_name = Column(String(255))
    reason = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", backref="blacklist")


class QuickReply(Base):
    __tablename__ = 'quick_replies'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    button_text = Column(String(50), nullable=False)
    reply_text = Column(String(500), nullable=False)
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", backref="quick_replies")


class DriverStats(Base):
    __tablename__ = 'driver_stats'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    status = Column(String(20), default='pending')
    completed_at = Column(DateTime)
    price = Column(Integer)
    point_a = Column(String(255))
    point_b = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", backref="stats")
    order = relationship("Order", backref="driver_stats")


class OrderReminder(Base):
    __tablename__ = 'order_reminders'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    notification_id = Column(Integer, ForeignKey('order_notifications.id'))
    reminder_time = Column(DateTime, nullable=False)
    status = Column(String(20), default='pending')
    created_at = Column(DateTime, default=datetime.utcnow)
    
    user = relationship("User", backref="reminders")
    order = relationship("Order", backref="reminders")


def init_db():
    if engine:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
    else:
        logger.error("Cannot init database: no engine")


def get_session():
    if engine:
        Session = sessionmaker(bind=engine)
        return Session()
    return None


def get_user_by_telegram_id(telegram_id: int):
    session = get_session()
    if not session:
        return None
    try:
        user = session.query(User).filter(User.telegram_id == telegram_id).first()
        return user
    finally:
        session.close()


def create_or_update_user(telegram_id: int, **kwargs):
    session = get_session()
    if not session:
        return None
    try:
        user = session.query(User).filter(User.telegram_id == telegram_id).first()
        if user:
            for key, value in kwargs.items():
                if hasattr(user, key):
                    setattr(user, key, value)
            user.updated_at = datetime.utcnow()
        else:
            user = User(telegram_id=telegram_id, **kwargs)
            session.add(user)
        session.commit()
        session.refresh(user)
        return user
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating/updating user: {e}")
        return None
    finally:
        session.close()


def get_active_users():
    session = get_session()
    if not session:
        return []
    try:
        users = session.query(User).filter(User.active == True, User.latitude != None).all()
        return users
    finally:
        session.close()


def save_user_session(user_id: int, session_string: str, phone: str = None):
    session = get_session()
    if not session:
        return None
    try:
        user_session = session.query(UserSession).filter(UserSession.user_id == user_id).first()
        if user_session:
            user_session.session_string = session_string
            user_session.is_authorized = True
            if phone:
                user_session.auth_phone = phone
            user_session.updated_at = datetime.utcnow()
        else:
            user_session = UserSession(
                user_id=user_id,
                session_string=session_string,
                is_authorized=True,
                auth_phone=phone
            )
            session.add(user_session)
        session.commit()
        return user_session
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving user session: {e}")
        return None
    finally:
        session.close()


def get_user_session(user_id: int):
    session = get_session()
    if not session:
        return None
    try:
        user_session = session.query(UserSession).filter(UserSession.user_id == user_id).first()
        return user_session
    finally:
        session.close()


def delete_user_session(user_id: int) -> bool:
    session = get_session()
    if not session:
        return False
    try:
        user_session = session.query(UserSession).filter(UserSession.user_id == user_id).first()
        if user_session:
            session.delete(user_session)
            session.commit()
            logger.info(f"Deleted session for user {user_id}")
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting user session: {e}")
        return False
    finally:
        session.close()


def add_user_group(user_id: int, group_id: int, group_title: str, group_username: str = None, is_premium: bool = False, sync_to_admins: bool = True):
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(UserGroup).filter(
            UserGroup.user_id == user_id,
            UserGroup.group_id == group_id
        ).first()
        
        is_new_group = False
        if existing:
            existing.is_active = True
            existing.group_title = group_title
            if group_username:
                existing.group_username = group_username
        else:
            user_group = UserGroup(
                user_id=user_id,
                group_id=group_id,
                group_title=group_title,
                group_username=group_username,
                is_premium=is_premium
            )
            session.add(user_group)
            is_new_group = True
        session.commit()
        
        if sync_to_admins and is_new_group:
            sync_group_to_admins(group_id, group_title, group_username)
        
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding user group: {e}")
        return False
    finally:
        session.close()


def get_user_groups(user_id: int, active_only: bool = True):
    session = get_session()
    if not session:
        return []
    try:
        query = session.query(UserGroup).filter(UserGroup.user_id == user_id)
        if active_only:
            query = query.filter(UserGroup.is_active == True)
        return query.all()
    finally:
        session.close()


def toggle_user_group(user_id: int, group_id: int):
    session = get_session()
    if not session:
        return None
    try:
        user_group = session.query(UserGroup).filter(
            UserGroup.user_id == user_id,
            UserGroup.group_id == group_id
        ).first()
        if user_group:
            user_group.is_active = not user_group.is_active
            session.commit()
            return user_group.is_active
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error toggling user group: {e}")
        return None
    finally:
        session.close()


def save_order(order_id: str, point_a: str, point_b: str, price: int = None,
               source_group_id: int = None, source_group_title: str = None,
               source_link: str = None, point_a_coords: tuple = None, 
               point_b_coords: tuple = None):
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(Order).filter(Order.source_link == source_link).first()
        if existing:
            logger.info(f"Order already exists: {source_link}")
            return existing
        
        order = Order(
            point_a=point_a,
            point_b=point_b,
            price=price,
            source_group_id=source_group_id,
            source_group_title=source_group_title,
            source_link=source_link,
            point_a_lat=point_a_coords[0] if point_a_coords else None,
            point_a_lon=point_a_coords[1] if point_a_coords else None,
            point_b_lat=point_b_coords[0] if point_b_coords else None,
            point_b_lon=point_b_coords[1] if point_b_coords else None
        )
        session.add(order)
        session.commit()
        session.refresh(order)
        logger.info(f"Order saved: {point_a} -> {point_b}, ID: {order.id}")
        return order
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving order: {e}")
        return None
    finally:
        session.close()


def get_order(order_id: int):
    session = get_session()
    if not session:
        return None
    try:
        order = session.query(Order).filter(Order.id == order_id).first()
        return order
    finally:
        session.close()


def get_authorized_users():
    session = get_session()
    if not session:
        return []
    try:
        users = session.query(User).join(UserSession).filter(
            User.is_authorized == True,
            UserSession.is_authorized == True,
            UserSession.session_string != None
        ).all()
        return users
    finally:
        session.close()


def get_authorized_users_with_sessions():
    session = get_session()
    if not session:
        return []
    try:
        results = session.query(User, UserSession).join(UserSession).filter(
            User.is_authorized == True,
            UserSession.is_authorized == True,
            UserSession.session_string != None
        ).all()
        return results
    finally:
        session.close()


def get_group_subscribers(group_id: int, active_only: bool = True):
    session = get_session()
    if not session:
        return []
    try:
        query = session.query(User).join(UserGroup).filter(
            UserGroup.group_id == group_id
        )
        if active_only:
            query = query.filter(
                UserGroup.is_active == True,
                User.active == True
            )
        return query.all()
    finally:
        session.close()


def get_all_active_groups():
    session = get_session()
    if not session:
        return []
    try:
        groups = session.query(UserGroup).filter(
            UserGroup.is_active == True
        ).distinct(UserGroup.group_id).all()
        return groups
    finally:
        session.close()


def get_all_unique_groups():
    """Get all unique groups from all user sessions with driver/admin counts"""
    session = get_session()
    if not session:
        return []
    try:
        from sqlalchemy import func, case
        
        groups = session.query(
            UserGroup.group_id,
            func.max(UserGroup.group_title).label('group_title'),
            func.max(UserGroup.group_username).label('group_username'),
            func.count(UserGroup.user_id.distinct()).label('user_count'),
            func.sum(case((User.is_admin == True, 1), else_=0)).label('admin_count'),
            func.sum(case((User.is_admin == False, 1), else_=0)).label('driver_count')
        ).join(
            User, UserGroup.user_id == User.id
        ).filter(
            UserGroup.is_active == True
        ).group_by(UserGroup.group_id).order_by(func.max(UserGroup.group_title)).all()
        
        return groups
    finally:
        session.close()


def sync_group_to_admins(group_id: int, group_title: str, group_username: str = None):
    """Add group to all admin users automatically"""
    session = get_session()
    if not session:
        return False
    try:
        admins = session.query(User).filter(User.is_admin == True).all()
        
        for admin in admins:
            existing = session.query(UserGroup).filter(
                UserGroup.user_id == admin.id,
                UserGroup.group_id == group_id
            ).first()
            
            if not existing:
                user_group = UserGroup(
                    user_id=admin.id,
                    group_id=group_id,
                    group_title=group_title,
                    group_username=group_username,
                    is_active=True
                )
                session.add(user_group)
                logger.info(f"Added group {group_title} to admin {admin.telegram_id}")
        
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error syncing group to admins: {e}")
        return False
    finally:
        session.close()


def sync_all_groups_to_admin(admin_user_id: int):
    """Sync all existing groups to a specific admin user"""
    session = get_session()
    if not session:
        return False
    try:
        all_groups = session.query(
            UserGroup.group_id,
            UserGroup.group_title,
            UserGroup.group_username
        ).filter(UserGroup.is_active == True).distinct(UserGroup.group_id).all()
        
        for group in all_groups:
            existing = session.query(UserGroup).filter(
                UserGroup.user_id == admin_user_id,
                UserGroup.group_id == group.group_id
            ).first()
            
            if not existing:
                user_group = UserGroup(
                    user_id=admin_user_id,
                    group_id=group.group_id,
                    group_title=group.group_title,
                    group_username=group.group_username,
                    is_active=True
                )
                session.add(user_group)
        
        session.commit()
        logger.info(f"Synced {len(all_groups)} groups to admin user {admin_user_id}")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Error syncing all groups to admin: {e}")
        return False
    finally:
        session.close()


def get_users_subscribed_to_group(group_id: int):
    session = get_session()
    if not session:
        return []
    try:
        from sqlalchemy import or_
        group_id_variants = [group_id, abs(group_id), -abs(group_id)]
        users = session.query(User).join(UserGroup).filter(
            or_(*[UserGroup.group_id == gid for gid in group_id_variants]),
            UserGroup.is_active == True,
            User.active == True,
            User.latitude != None
        ).all()
        return users
    finally:
        session.close()


def get_admin_users():
    session = get_session()
    if not session:
        return []
    try:
        users = session.query(User).filter(User.is_admin == True).all()
        return users
    finally:
        session.close()


def set_user_admin(telegram_id: int, is_admin: bool = True):
    session = get_session()
    if not session:
        return False
    try:
        user = session.query(User).filter(User.telegram_id == telegram_id).first()
        if user:
            user.is_admin = is_admin
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error setting user admin: {e}")
        return False
    finally:
        session.close()


def save_order_notification(order_id: int, user_id: int, message_id: int = None, route_key: str = None):
    session = get_session()
    if not session:
        return None
    try:
        notification = OrderNotification(
            order_id=order_id, 
            user_id=user_id,
            message_id=message_id,
            route_key=route_key
        )
        session.add(notification)
        session.commit()
        session.refresh(notification)
        return notification
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving notification: {e}")
        return None
    finally:
        session.close()


def normalize_route_key(point_a: str, point_b: str) -> str:
    """Create normalized route key for duplicate detection"""
    a = point_a.strip().lower() if point_a else ""
    b = point_b.strip().lower() if point_b else ""
    return f"{a}:{b}"


def get_existing_notification(user_id: int, route_key: str, hours: int = 2):
    """Find existing notification for the same route within last N hours"""
    session = get_session()
    if not session:
        return None
    try:
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        notification = session.query(OrderNotification).filter(
            OrderNotification.user_id == user_id,
            OrderNotification.route_key == route_key,
            OrderNotification.message_id != None,
            OrderNotification.sent_at >= cutoff
        ).order_by(OrderNotification.sent_at.desc()).first()
        
        return notification
    finally:
        session.close()


def update_notification_message_id(notification_id: int, message_id: int):
    """Update message_id for existing notification"""
    session = get_session()
    if not session:
        return False
    try:
        notification = session.query(OrderNotification).filter(
            OrderNotification.id == notification_id
        ).first()
        if notification:
            notification.message_id = message_id
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating notification message_id: {e}")
        return False
    finally:
        session.close()


def add_order_group_link(route_key: str, user_id: int, group_id: int, group_title: str, 
                         source_link: str, message_id: int = None,
                         author_id: int = None, author_username: str = None, 
                         author_first_name: str = None):
    """Add a group link to the order for a specific user"""
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(OrderGroupLink).filter(
            OrderGroupLink.route_key == route_key,
            OrderGroupLink.user_id == user_id,
            OrderGroupLink.source_link == source_link
        ).first()
        
        if existing:
            return existing
        
        link = OrderGroupLink(
            route_key=route_key,
            user_id=user_id,
            group_id=group_id,
            group_title=group_title,
            source_link=source_link,
            message_id=message_id,
            author_id=author_id,
            author_username=author_username,
            author_first_name=author_first_name
        )
        session.add(link)
        session.commit()
        session.refresh(link)
        return link
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding order group link: {e}")
        return None
    finally:
        session.close()


def get_order_group_links(route_key: str, user_id: int):
    """Get all group links for a route and user"""
    session = get_session()
    if not session:
        return []
    try:
        links = session.query(OrderGroupLink).filter(
            OrderGroupLink.route_key == route_key,
            OrderGroupLink.user_id == user_id
        ).order_by(OrderGroupLink.created_at).all()
        return links
    finally:
        session.close()


def get_notification_by_message_id(user_id: int, bot_message_id: int):
    """Get notification by bot message ID"""
    session = get_session()
    if not session:
        return None
    try:
        notification = session.query(OrderNotification).filter(
            OrderNotification.user_id == user_id,
            OrderNotification.message_id == bot_message_id
        ).first()
        return notification
    finally:
        session.close()


def save_order_response(order_id: int, user_id: int, response_type: str = "take"):
    session = get_session()
    if not session:
        return None
    try:
        response = OrderResponse(order_id=order_id, user_id=user_id, response_type=response_type)
        session.add(response)
        session.commit()
        return response
    except Exception as e:
        session.rollback()
        logger.error(f"Error saving response: {e}")
        return None
    finally:
        session.close()


def get_all_users(limit: int = 100, offset: int = 0):
    session = get_session()
    if not session:
        return [], 0
    try:
        total = session.query(User).count()
        users = session.query(User).order_by(User.created_at.desc()).offset(offset).limit(limit).all()
        return users, total
    finally:
        session.close()


def search_users(query: str, limit: int = 20):
    session = get_session()
    if not session:
        return []
    try:
        clean_query = query.strip().lstrip('@')
        search_term = f"%{clean_query.lower()}%"
        users = session.query(User).filter(
            (User.username.ilike(search_term)) |
            (User.first_name.ilike(search_term)) |
            (User.city_name.ilike(search_term))
        ).order_by(User.created_at.desc()).limit(limit).all()
        return users
    finally:
        session.close()


def get_user_stats(user_id: int):
    session = get_session()
    if not session:
        return {}
    try:
        from sqlalchemy import func
        from datetime import timedelta
        
        now = datetime.utcnow()
        day_ago = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        
        notifications_total = session.query(func.count(OrderNotification.id)).filter(
            OrderNotification.user_id == user_id
        ).scalar() or 0
        
        responses_total = session.query(func.count(OrderResponse.id)).filter(
            OrderResponse.user_id == user_id
        ).scalar() or 0
        
        responses_day = session.query(func.count(OrderResponse.id)).filter(
            OrderResponse.user_id == user_id,
            OrderResponse.responded_at >= day_ago
        ).scalar() or 0
        
        responses_week = session.query(func.count(OrderResponse.id)).filter(
            OrderResponse.user_id == user_id,
            OrderResponse.responded_at >= week_ago
        ).scalar() or 0
        
        responses_month = session.query(func.count(OrderResponse.id)).filter(
            OrderResponse.user_id == user_id,
            OrderResponse.responded_at >= month_ago
        ).scalar() or 0
        
        return {
            'notifications_total': notifications_total,
            'responses_total': responses_total,
            'responses_day': responses_day,
            'responses_week': responses_week,
            'responses_month': responses_month
        }
    finally:
        session.close()


def get_system_stats():
    session = get_session()
    if not session:
        return {}
    try:
        from sqlalchemy import func
        from datetime import timedelta
        
        now = datetime.utcnow()
        day_ago = now - timedelta(days=1)
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        
        total_users = session.query(func.count(User.id)).scalar() or 0
        active_users = session.query(func.count(User.id)).filter(User.active == True).scalar() or 0
        authorized_users = session.query(func.count(User.id)).filter(User.is_authorized == True).scalar() or 0
        
        orders_total = session.query(func.count(Order.id)).scalar() or 0
        orders_day = session.query(func.count(Order.id)).filter(Order.created_at >= day_ago).scalar() or 0
        orders_week = session.query(func.count(Order.id)).filter(Order.created_at >= week_ago).scalar() or 0
        orders_month = session.query(func.count(Order.id)).filter(Order.created_at >= month_ago).scalar() or 0
        
        responses_total = session.query(func.count(OrderResponse.id)).scalar() or 0
        responses_day = session.query(func.count(OrderResponse.id)).filter(OrderResponse.responded_at >= day_ago).scalar() or 0
        
        top_groups = session.query(
            Order.source_group_title,
            func.count(Order.id).label('count')
        ).filter(
            Order.source_group_title != None
        ).group_by(Order.source_group_title).order_by(func.count(Order.id).desc()).limit(5).all()
        
        return {
            'total_users': total_users,
            'active_users': active_users,
            'authorized_users': authorized_users,
            'orders_total': orders_total,
            'orders_day': orders_day,
            'orders_week': orders_week,
            'orders_month': orders_month,
            'responses_total': responses_total,
            'responses_day': responses_day,
            'top_groups': [(g[0], g[1]) for g in top_groups]
        }
    finally:
        session.close()


def get_user_by_id(user_id: int):
    session = get_session()
    if not session:
        return None
    try:
        user = session.query(User).filter(User.id == user_id).first()
        return user
    finally:
        session.close()


def get_service_groups(active_only: bool = True, limit: int = None, offset: int = 0):
    """Get all service groups (our groups)"""
    session = get_session()
    if not session:
        return [], 0
    try:
        query = session.query(PremiumGroup)
        if active_only:
            query = query.filter(PremiumGroup.is_active == True)
        total = query.count()
        query = query.order_by(PremiumGroup.created_at.desc())
        if limit:
            query = query.offset(offset).limit(limit)
        groups = query.all()
        return groups, total
    finally:
        session.close()


def add_service_group(group_id: int, group_title: str, group_username: str = None):
    """Add a group to service groups list"""
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(PremiumGroup).filter(PremiumGroup.group_id == group_id).first()
        if existing:
            existing.group_title = group_title
            existing.group_username = group_username
            existing.is_active = True
            session.commit()
            session.refresh(existing)
            return existing
        
        group = PremiumGroup(
            group_id=group_id,
            group_title=group_title,
            group_username=group_username,
            is_active=True
        )
        session.add(group)
        session.commit()
        session.refresh(group)
        return group
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding service group: {e}")
        return None
    finally:
        session.close()


def remove_service_group(group_id: int):
    """Remove group from service groups list"""
    session = get_session()
    if not session:
        return False
    try:
        group = session.query(PremiumGroup).filter(PremiumGroup.group_id == group_id).first()
        if group:
            session.delete(group)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error removing service group: {e}")
        return False
    finally:
        session.close()


def toggle_service_group(group_id: int):
    """Toggle service group active status"""
    session = get_session()
    if not session:
        return None
    try:
        group = session.query(PremiumGroup).filter(PremiumGroup.group_id == group_id).first()
        if group:
            group.is_active = not group.is_active
            session.commit()
            session.refresh(group)
            return group
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error toggling service group: {e}")
        return None
    finally:
        session.close()


def is_service_group(group_id: int) -> bool:
    """Check if group is in service groups list"""
    session = get_session()
    if not session:
        return False
    try:
        group = session.query(PremiumGroup).filter(
            PremiumGroup.group_id == group_id,
            PremiumGroup.is_active == True
        ).first()
        return group is not None
    finally:
        session.close()


def search_service_groups(query: str):
    """Search service groups by title or username"""
    session = get_session()
    if not session:
        return []
    try:
        search_pattern = f"%{query}%"
        groups = session.query(PremiumGroup).filter(
            (PremiumGroup.group_title.ilike(search_pattern)) |
            (PremiumGroup.group_username.ilike(search_pattern))
        ).order_by(PremiumGroup.created_at.desc()).limit(20).all()
        return groups
    finally:
        session.close()


def search_all_groups(query: str):
    """Search across all user groups by title"""
    session = get_session()
    if not session:
        return []
    try:
        search_pattern = f"%{query}%"
        from sqlalchemy import distinct, func
        
        groups = session.query(
            UserGroup.group_id,
            UserGroup.group_title,
            UserGroup.group_username
        ).filter(
            UserGroup.group_title.ilike(search_pattern)
        ).group_by(
            UserGroup.group_id,
            UserGroup.group_title,
            UserGroup.group_username
        ).limit(20).all()
        
        return groups
    finally:
        session.close()


def get_driver_settings(user_id: int):
    """Get driver settings or create default"""
    session = get_session()
    if not session:
        return None
    try:
        settings = session.query(DriverSettings).filter(DriverSettings.user_id == user_id).first()
        if not settings:
            settings = DriverSettings(user_id=user_id)
            session.add(settings)
            session.commit()
            session.refresh(settings)
        return settings
    except Exception as e:
        session.rollback()
        logger.error(f"Error getting driver settings: {e}")
        return None
    finally:
        session.close()


def update_driver_settings(user_id: int, **kwargs):
    """Update driver settings"""
    session = get_session()
    if not session:
        return None
    try:
        settings = session.query(DriverSettings).filter(DriverSettings.user_id == user_id).first()
        if not settings:
            settings = DriverSettings(user_id=user_id, **kwargs)
            session.add(settings)
        else:
            for key, value in kwargs.items():
                if hasattr(settings, key):
                    setattr(settings, key, value)
            settings.updated_at = datetime.utcnow()
        session.commit()
        session.refresh(settings)
        return settings
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating driver settings: {e}")
        return None
    finally:
        session.close()


def is_user_in_quiet_hours(user_id: int) -> bool:
    """Check if user is currently in quiet hours"""
    session = get_session()
    if not session:
        return False
    try:
        settings = session.query(DriverSettings).filter(DriverSettings.user_id == user_id).first()
        if not settings or not settings.quiet_hours_enabled:
            return False
        
        from datetime import datetime as dt
        import pytz
        
        now = dt.now(pytz.timezone('Europe/Moscow'))
        current_time = now.strftime('%H:%M')
        
        start = settings.quiet_hours_start
        end = settings.quiet_hours_end
        
        if start <= end:
            return start <= current_time <= end
        else:
            return current_time >= start or current_time <= end
    finally:
        session.close()


def is_user_busy(user_id: int) -> bool:
    """Check if user is currently busy. Auto-clears expired busy status."""
    session = get_session()
    if not session:
        return False
    try:
        settings = session.query(DriverSettings).filter(DriverSettings.user_id == user_id).first()
        if not settings or not settings.busy_until:
            return False
        
        if settings.busy_until > datetime.utcnow():
            return True
        else:
            settings.busy_until = None
            session.commit()
            return False
    finally:
        session.close()


def set_user_busy(user_id: int, until: datetime):
    """Set user as busy until specified time"""
    return update_driver_settings(user_id, busy_until=until)


def clear_user_busy(user_id: int):
    """Clear user busy status"""
    return update_driver_settings(user_id, busy_until=None)


def get_driver_profile(user_id: int):
    """Get driver profile"""
    session = get_session()
    if not session:
        return None
    try:
        profile = session.query(DriverProfile).filter(DriverProfile.user_id == user_id).first()
        return profile
    finally:
        session.close()


def update_driver_profile(user_id: int, **kwargs):
    """Update driver profile"""
    session = get_session()
    if not session:
        return None
    try:
        profile = session.query(DriverProfile).filter(DriverProfile.user_id == user_id).first()
        if not profile:
            profile = DriverProfile(user_id=user_id, **kwargs)
            session.add(profile)
        else:
            for key, value in kwargs.items():
                if hasattr(profile, key):
                    setattr(profile, key, value)
            profile.updated_at = datetime.utcnow()
        session.commit()
        session.refresh(profile)
        return profile
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating driver profile: {e}")
        return None
    finally:
        session.close()


def get_favorite_routes(user_id: int, active_only: bool = True):
    """Get user's favorite routes"""
    session = get_session()
    if not session:
        return []
    try:
        query = session.query(FavoriteRoute).filter(FavoriteRoute.user_id == user_id)
        if active_only:
            query = query.filter(FavoriteRoute.is_active == True)
        return query.all()
    finally:
        session.close()


def add_favorite_route(user_id: int, point_a: str, point_b: str, priority_notify: bool = True):
    """Add favorite route"""
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(FavoriteRoute).filter(
            FavoriteRoute.user_id == user_id,
            FavoriteRoute.point_a.ilike(point_a),
            FavoriteRoute.point_b.ilike(point_b)
        ).first()
        
        if existing:
            existing.is_active = True
            existing.priority_notify = priority_notify
            session.commit()
            return existing
        
        route = FavoriteRoute(
            user_id=user_id,
            point_a=point_a,
            point_b=point_b,
            priority_notify=priority_notify
        )
        session.add(route)
        session.commit()
        session.refresh(route)
        return route
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding favorite route: {e}")
        return None
    finally:
        session.close()


def remove_favorite_route(route_id: int, user_id: int):
    """Remove favorite route"""
    session = get_session()
    if not session:
        return False
    try:
        route = session.query(FavoriteRoute).filter(
            FavoriteRoute.id == route_id,
            FavoriteRoute.user_id == user_id
        ).first()
        if route:
            session.delete(route)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error removing favorite route: {e}")
        return False
    finally:
        session.close()


def is_favorite_route(user_id: int, point_a: str, point_b: str) -> bool:
    """Check if route is in user's favorites"""
    session = get_session()
    if not session:
        return False
    try:
        route = session.query(FavoriteRoute).filter(
            FavoriteRoute.user_id == user_id,
            FavoriteRoute.point_a.ilike(f"%{point_a}%"),
            FavoriteRoute.point_b.ilike(f"%{point_b}%"),
            FavoriteRoute.is_active == True
        ).first()
        return route is not None
    finally:
        session.close()


def get_blacklist(user_id: int):
    """Get user's blacklist"""
    session = get_session()
    if not session:
        return []
    try:
        return session.query(Blacklist).filter(Blacklist.user_id == user_id).all()
    finally:
        session.close()


def add_to_blacklist(user_id: int, block_type: str, blocked_id: int = None, 
                     blocked_username: str = None, blocked_name: str = None, reason: str = None):
    """Add to blacklist (type: 'author' or 'group')"""
    session = get_session()
    if not session:
        return None
    try:
        existing = session.query(Blacklist).filter(
            Blacklist.user_id == user_id,
            Blacklist.block_type == block_type,
            ((Blacklist.blocked_id == blocked_id) if blocked_id else True)
        ).first()
        
        if existing:
            return existing
        
        entry = Blacklist(
            user_id=user_id,
            block_type=block_type,
            blocked_id=blocked_id,
            blocked_username=blocked_username,
            blocked_name=blocked_name,
            reason=reason
        )
        session.add(entry)
        session.commit()
        session.refresh(entry)
        return entry
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding to blacklist: {e}")
        return None
    finally:
        session.close()


def remove_from_blacklist(entry_id: int, user_id: int):
    """Remove from blacklist"""
    session = get_session()
    if not session:
        return False
    try:
        entry = session.query(Blacklist).filter(
            Blacklist.id == entry_id,
            Blacklist.user_id == user_id
        ).first()
        if entry:
            session.delete(entry)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error removing from blacklist: {e}")
        return False
    finally:
        session.close()


def is_blacklisted(user_id: int, author_id: int = None, group_id: int = None) -> bool:
    """Check if author or group is blacklisted"""
    session = get_session()
    if not session:
        return False
    try:
        query = session.query(Blacklist).filter(Blacklist.user_id == user_id)
        
        if author_id:
            result = query.filter(
                Blacklist.block_type == 'author',
                Blacklist.blocked_id == author_id
            ).first()
            if result:
                return True
        
        if group_id:
            result = query.filter(
                Blacklist.block_type == 'group',
                Blacklist.blocked_id == group_id
            ).first()
            if result:
                return True
        
        return False
    finally:
        session.close()


def get_quick_replies(user_id: int, active_only: bool = True):
    """Get user's quick replies"""
    session = get_session()
    if not session:
        return []
    try:
        query = session.query(QuickReply).filter(QuickReply.user_id == user_id)
        if active_only:
            query = query.filter(QuickReply.is_active == True)
        return query.order_by(QuickReply.sort_order).all()
    finally:
        session.close()


def add_quick_reply(user_id: int, button_text: str, reply_text: str, sort_order: int = 0):
    """Add quick reply"""
    session = get_session()
    if not session:
        return None
    try:
        reply = QuickReply(
            user_id=user_id,
            button_text=button_text,
            reply_text=reply_text,
            sort_order=sort_order
        )
        session.add(reply)
        session.commit()
        session.refresh(reply)
        return reply
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding quick reply: {e}")
        return None
    finally:
        session.close()


def update_quick_reply(reply_id: int, user_id: int, **kwargs):
    """Update quick reply"""
    session = get_session()
    if not session:
        return None
    try:
        reply = session.query(QuickReply).filter(
            QuickReply.id == reply_id,
            QuickReply.user_id == user_id
        ).first()
        if reply:
            for key, value in kwargs.items():
                if hasattr(reply, key):
                    setattr(reply, key, value)
            session.commit()
            session.refresh(reply)
            return reply
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating quick reply: {e}")
        return None
    finally:
        session.close()


def remove_quick_reply(reply_id: int, user_id: int):
    """Remove quick reply"""
    session = get_session()
    if not session:
        return False
    try:
        reply = session.query(QuickReply).filter(
            QuickReply.id == reply_id,
            QuickReply.user_id == user_id
        ).first()
        if reply:
            session.delete(reply)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error removing quick reply: {e}")
        return False
    finally:
        session.close()


def toggle_quick_reply(reply_id: int, user_id: int):
    """Toggle quick reply active status"""
    session = get_session()
    if not session:
        return None
    try:
        reply = session.query(QuickReply).filter(
            QuickReply.id == reply_id,
            QuickReply.user_id == user_id
        ).first()
        if reply:
            reply.is_active = not reply.is_active
            session.commit()
            return reply.is_active
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error toggling quick reply: {e}")
        return None
    finally:
        session.close()


def add_driver_stat(user_id: int, order_id: int, point_a: str, point_b: str, price: int = None):
    """Add order to driver stats"""
    session = get_session()
    if not session:
        return None
    try:
        stat = DriverStats(
            user_id=user_id,
            order_id=order_id,
            point_a=point_a,
            point_b=point_b,
            price=price,
            status='pending'
        )
        session.add(stat)
        session.commit()
        session.refresh(stat)
        return stat
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding driver stat: {e}")
        return None
    finally:
        session.close()


def update_driver_stat(stat_id: int, user_id: int, status: str):
    """Update driver stat status (completed, skipped)"""
    session = get_session()
    if not session:
        return None
    try:
        stat = session.query(DriverStats).filter(
            DriverStats.id == stat_id,
            DriverStats.user_id == user_id
        ).first()
        if stat:
            stat.status = status
            if status == 'completed':
                stat.completed_at = datetime.utcnow()
            session.commit()
            session.refresh(stat)
            return stat
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating driver stat: {e}")
        return None
    finally:
        session.close()


def get_driver_stats_summary(user_id: int):
    """Get driver stats summary"""
    session = get_session()
    if not session:
        return {}
    try:
        from sqlalchemy import func
        from datetime import timedelta
        
        now = datetime.utcnow()
        week_ago = now - timedelta(days=7)
        month_ago = now - timedelta(days=30)
        
        total = session.query(func.count(DriverStats.id)).filter(
            DriverStats.user_id == user_id,
            DriverStats.status == 'completed'
        ).scalar() or 0
        
        week = session.query(func.count(DriverStats.id)).filter(
            DriverStats.user_id == user_id,
            DriverStats.status == 'completed',
            DriverStats.completed_at >= week_ago
        ).scalar() or 0
        
        month = session.query(func.count(DriverStats.id)).filter(
            DriverStats.user_id == user_id,
            DriverStats.status == 'completed',
            DriverStats.completed_at >= month_ago
        ).scalar() or 0
        
        total_earnings = session.query(func.sum(DriverStats.price)).filter(
            DriverStats.user_id == user_id,
            DriverStats.status == 'completed',
            DriverStats.price != None
        ).scalar() or 0
        
        return {
            'total': total,
            'week': week,
            'month': month,
            'total_earnings': total_earnings
        }
    finally:
        session.close()


def add_order_reminder(user_id: int, order_id: int, notification_id: int, reminder_time: datetime):
    """Add order reminder"""
    session = get_session()
    if not session:
        return None
    try:
        reminder = OrderReminder(
            user_id=user_id,
            order_id=order_id,
            notification_id=notification_id,
            reminder_time=reminder_time,
            status='pending'
        )
        session.add(reminder)
        session.commit()
        session.refresh(reminder)
        return reminder
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding order reminder: {e}")
        return None
    finally:
        session.close()


def get_pending_reminders():
    """Get all pending reminders that are due"""
    session = get_session()
    if not session:
        return []
    try:
        return session.query(OrderReminder).filter(
            OrderReminder.status == 'pending',
            OrderReminder.reminder_time <= datetime.utcnow()
        ).all()
    finally:
        session.close()


def update_reminder_status(reminder_id: int, status: str):
    """Update reminder status"""
    session = get_session()
    if not session:
        return False
    try:
        reminder = session.query(OrderReminder).filter(OrderReminder.id == reminder_id).first()
        if reminder:
            reminder.status = status
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating reminder: {e}")
        return False
    finally:
        session.close()


def get_driver_profile(user_id: int):
    """Get driver profile"""
    session = get_session()
    if not session:
        return None
    try:
        return session.query(DriverProfile).filter(DriverProfile.user_id == user_id).first()
    finally:
        session.close()


def update_driver_profile(user_id: int, **kwargs):
    """Update or create driver profile"""
    session = get_session()
    if not session:
        return None
    try:
        profile = session.query(DriverProfile).filter(DriverProfile.user_id == user_id).first()
        if profile:
            for key, value in kwargs.items():
                if hasattr(profile, key):
                    setattr(profile, key, value)
            profile.updated_at = datetime.utcnow()
        else:
            profile = DriverProfile(user_id=user_id, **kwargs)
            session.add(profile)
        session.commit()
        session.refresh(profile)
        return profile
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating driver profile: {e}")
        return None
    finally:
        session.close()


if __name__ == '__main__':
    init_db()
    print("Database initialized!")
