import asyncio
import logging
from typing import Dict, Optional, Callable, Set
from telethon import TelegramClient, events, utils
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat

from src.config import TELEGRAM_API_ID, TELEGRAM_API_HASH
from src.parser.order_parser import parse_order_async, ParsedOrder
from src.utils.database import (
    get_authorized_users_with_sessions,
    get_user_groups,
    get_users_subscribed_to_group
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UserMonitor:
    def __init__(self, user_id: int, telegram_id: int, session_string: str,
                 on_order_callback: Optional[Callable] = None):
        self.user_id = user_id
        self.telegram_id = telegram_id
        self.session_string = session_string
        self.on_order_callback = on_order_callback
        self.client: Optional[TelegramClient] = None
        self.monitored_groups: Dict[int, dict] = {}
        self.all_dialogs: Dict[int, dict] = {}
        self.running = False
        self._handlers_set = False
    
    async def start(self) -> bool:
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
            logger.error("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
            return False
        
        try:
            self.client = TelegramClient(
                StringSession(self.session_string),
                int(TELEGRAM_API_ID),
                TELEGRAM_API_HASH
            )
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                logger.warning(f"User {self.user_id} session is no longer valid")
                await self.client.disconnect()
                return False
            
            await self._cache_all_dialogs()
            await self._load_groups()
            
            self._setup_handlers()
            self.running = True
            
            if self.monitored_groups:
                logger.info(f"User {self.user_id} monitoring {len(self.monitored_groups)} groups")
            else:
                logger.info(f"User {self.user_id} started, waiting for group subscriptions")
            
            return True
            
        except Exception as e:
            logger.error(f"Error starting monitor for user {self.user_id}: {e}")
            if self.client:
                try:
                    await self.client.disconnect()
                except:
                    pass
            return False
    
    async def _cache_all_dialogs(self):
        try:
            self.all_dialogs.clear()
            async for dialog in self.client.iter_dialogs():
                entity = dialog.entity
                if isinstance(entity, (Channel, Chat)):
                    dialog_id = dialog.id
                    dialog_data = {
                        'entity': entity,
                        'title': getattr(entity, 'title', str(dialog_id)),
                        'username': getattr(entity, 'username', None)
                    }
                    self.all_dialogs[dialog_id] = dialog_data
                    self.all_dialogs[abs(dialog_id)] = dialog_data
                    self.all_dialogs[-abs(dialog_id)] = dialog_data
                    
                    if isinstance(entity, Channel):
                        peer_id = utils.get_peer_id(entity)
                        self.all_dialogs[peer_id] = dialog_data
                        self.all_dialogs[abs(peer_id)] = dialog_data
                        channel_id = entity.id
                        self.all_dialogs[channel_id] = dialog_data
                        self.all_dialogs[-channel_id] = dialog_data
                        supergroup_id = -1000000000000 - channel_id
                        self.all_dialogs[supergroup_id] = dialog_data
                        self.all_dialogs[abs(supergroup_id)] = dialog_data
                        marked_id = int(f"-100{channel_id}")
                        self.all_dialogs[marked_id] = dialog_data
                        self.all_dialogs[abs(marked_id)] = dialog_data
                    
            logger.info(f"User {self.user_id} cached dialogs with {len(self.all_dialogs)} ID mappings")
        except Exception as e:
            logger.error(f"Error caching dialogs for user {self.user_id}: {e}")
    
    async def _load_groups(self):
        user_groups = get_user_groups(self.user_id, active_only=True)
        
        if not user_groups:
            logger.info(f"User {self.user_id} has no active groups selected")
            self.monitored_groups.clear()
            return
        
        new_groups = {}
        missing_groups = []
        for g in user_groups:
            group_id = int(g.group_id)
            
            dialog_data = None
            if group_id in self.all_dialogs:
                dialog_data = self.all_dialogs[group_id]
            elif abs(group_id) in self.all_dialogs:
                dialog_data = self.all_dialogs[abs(group_id)]
            elif -group_id in self.all_dialogs:
                dialog_data = self.all_dialogs[-group_id]
            
            if dialog_data:
                peer_id = utils.get_peer_id(dialog_data['entity'])
                new_groups[peer_id] = dialog_data
                new_groups[abs(peer_id)] = dialog_data
                new_groups[group_id] = dialog_data
                new_groups[abs(group_id)] = dialog_data
            else:
                missing_groups.append(f"{g.group_title or group_id}")
        
        if missing_groups:
            logger.warning(f"User {self.user_id} missing dialogs for groups: {', '.join(missing_groups)}")
        
        self.monitored_groups = new_groups
        logger.info(f"User {self.user_id} resolved {len(new_groups)} group mappings for {len(user_groups)} subscriptions")
    
    def _setup_handlers(self):
        if self._handlers_set:
            return
        
        @self.client.on(events.NewMessage())
        async def handle_new_message(event):
            chat = await event.get_chat()
            if isinstance(chat, (Channel, Chat)):
                peer_id = utils.get_peer_id(chat)
                if peer_id not in self.monitored_groups and abs(peer_id) not in self.monitored_groups:
                    return
                await self._process_message(event, peer_id)
        
        self._handlers_set = True
    
    async def refresh_groups(self):
        await self._cache_all_dialogs()
        await self._load_groups()
        logger.info(f"User {self.user_id} refreshed groups, now monitoring {len(self.monitored_groups)}")
        return len(self.monitored_groups)
    
    async def _process_message(self, event, peer_id: int):
        try:
            message = event.message
            text = message.text or message.message or ""
            
            if not text:
                return
            
            chat = await event.get_chat()
            chat_title = getattr(chat, 'title', 'Unknown')
            chat_username = getattr(chat, 'username', None)
            
            author_id = None
            author_username = None
            author_first_name = None
            try:
                sender = await event.get_sender()
                if sender:
                    sender_id = getattr(sender, 'id', None)
                    is_channel = hasattr(sender, 'broadcast') or hasattr(sender, 'megagroup')
                    
                    if not is_channel and sender_id != abs(peer_id) and sender_id != abs(peer_id) - 1000000000000:
                        author_id = sender_id
                        author_username = getattr(sender, 'username', None)
                        author_first_name = getattr(sender, 'first_name', None)
                        logger.info(f"[User {self.user_id}] Author: id={author_id}, username={author_username}, name={author_first_name}")
                    else:
                        if hasattr(message, 'post_author') and message.post_author:
                            author_first_name = message.post_author
                            logger.info(f"[User {self.user_id}] Post author signature: {author_first_name}")
            except Exception as e:
                logger.debug(f"Could not get sender info: {e}")
            
            logger.info(f"[User {self.user_id}] New message in '{chat_title}' (peer_id={peer_id}): {text[:50]}...")
            
            order = await parse_order_async(
                text=text,
                source_group=str(peer_id),
                message_id=message.id,
                group_username=chat_username,
                group_title=chat_title,
                author_id=author_id,
                author_username=author_username,
                author_first_name=author_first_name
            )
            
            if order:
                logger.info(f"[User {self.user_id}] Found order: {order.point_a} -> {order.point_b}")
                order.source_group_id = peer_id
                if self.on_order_callback:
                    await self._notify_order(order)
            
        except Exception as e:
            logger.error(f"Error processing message for user {self.user_id}: {e}", exc_info=True)
    
    async def _notify_order(self, order: ParsedOrder):
        if self.on_order_callback:
            if asyncio.iscoroutinefunction(self.on_order_callback):
                await self.on_order_callback(order)
            else:
                self.on_order_callback(order)
    
    async def stop(self):
        self.running = False
        if self.client and self.client.is_connected():
            try:
                await self.client.disconnect()
            except:
                pass
        logger.info(f"User {self.user_id} monitor stopped")
    
    async def run_until_disconnected(self):
        if self.client:
            await self.client.run_until_disconnected()


class MultiUserMonitor:
    def __init__(self, on_order_callback: Optional[Callable] = None):
        self.on_order_callback = on_order_callback
        self.user_monitors: Dict[int, UserMonitor] = {}
        self.running = False
        self.processed_orders: Set[str] = set()
    
    async def start(self):
        logger.info("Starting multi-user monitor...")
        self.running = True
        
        await self._load_user_monitors()
        
        tasks = []
        for user_id, monitor in self.user_monitors.items():
            task = asyncio.create_task(self._run_monitor(monitor))
            tasks.append(task)
        
        check_task = asyncio.create_task(self._periodic_check())
        tasks.append(check_task)
        
        if not self.user_monitors:
            logger.warning("No authorized users found, waiting for new users...")
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _load_user_monitors(self):
        users_with_sessions = get_authorized_users_with_sessions()
        
        logger.info(f"Found {len(users_with_sessions)} authorized users")
        
        for user, session in users_with_sessions:
            if user.id in self.user_monitors:
                continue
            
            monitor = UserMonitor(
                user_id=user.id,
                telegram_id=user.telegram_id,
                session_string=session.session_string,
                on_order_callback=self._handle_order
            )
            
            success = await monitor.start()
            if success:
                self.user_monitors[user.id] = monitor
                logger.info(f"Started monitor for user {user.id}")
            else:
                logger.warning(f"Failed to start monitor for user {user.id}")
    
    async def _run_monitor(self, monitor: UserMonitor):
        try:
            await monitor.run_until_disconnected()
        except Exception as e:
            logger.error(f"Monitor error for user {monitor.user_id}: {e}")
        finally:
            monitor.running = False
    
    async def _periodic_check(self):
        while self.running:
            await asyncio.sleep(300)
            await self._check_new_users()
    
    async def _check_new_users(self):
        try:
            users_with_sessions = get_authorized_users_with_sessions()
            
            for user, session in users_with_sessions:
                if user.id in self.user_monitors:
                    monitor = self.user_monitors[user.id]
                    if monitor.running:
                        await monitor.refresh_groups()
                    continue
                
                monitor = UserMonitor(
                    user_id=user.id,
                    telegram_id=user.telegram_id,
                    session_string=session.session_string,
                    on_order_callback=self._handle_order
                )
                
                success = await monitor.start()
                if success:
                    self.user_monitors[user.id] = monitor
                    asyncio.create_task(self._run_monitor(monitor))
                    logger.info(f"Added new monitor for user {user.id}")
        
        except Exception as e:
            logger.error(f"Error checking new users: {e}")
    
    async def _handle_order(self, order: ParsedOrder):
        group_id = order.source_group_id if order.source_group_id else order.source_group
        msg_id = order.message_id if order.message_id else order.source_link.split('/')[-1] if order.source_link else "unknown"
        order_key = f"{group_id}_{msg_id}"
        
        if order_key in self.processed_orders:
            logger.debug(f"Order already processed: {order_key}")
            return
        
        self.processed_orders.add(order_key)
        
        if len(self.processed_orders) > 10000:
            old_orders = list(self.processed_orders)[:5000]
            for o in old_orders:
                self.processed_orders.discard(o)
        
        if self.on_order_callback:
            if asyncio.iscoroutinefunction(self.on_order_callback):
                await self.on_order_callback(order)
            else:
                self.on_order_callback(order)
    
    async def stop(self):
        logger.info("Stopping multi-user monitor...")
        self.running = False
        
        for user_id, monitor in self.user_monitors.items():
            await monitor.stop()
        
        self.user_monitors.clear()
        logger.info("Multi-user monitor stopped")


async def run_multi_user_monitor(on_order_callback: Callable):
    monitor = MultiUserMonitor(on_order_callback=on_order_callback)
    await monitor.start()
