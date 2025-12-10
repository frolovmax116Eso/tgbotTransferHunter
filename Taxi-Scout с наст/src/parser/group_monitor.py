import asyncio
import logging
from typing import List, Optional, Callable
from telethon import TelegramClient, events
from telethon.tl.types import Channel, Chat

from src.config import TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE, SOURCE_GROUPS
from src.parser.order_parser import parse_order_async, ParsedOrder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GroupMonitor:
    def __init__(self, on_order_callback: Optional[Callable[[ParsedOrder], None]] = None):
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
            raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        
        logger.info(f"API_ID length: {len(str(TELEGRAM_API_ID))}, type: {type(TELEGRAM_API_ID)}")
        logger.info(f"API_HASH length: {len(str(TELEGRAM_API_HASH))}, first 4 chars: {str(TELEGRAM_API_HASH)[:4]}...")
        logger.info(f"PHONE: {TELEGRAM_PHONE}")
        
        self.client = TelegramClient(
            'session/user_session',
            int(TELEGRAM_API_ID),
            TELEGRAM_API_HASH
        )
        self.on_order_callback = on_order_callback
        self.monitored_groups: List[str] = SOURCE_GROUPS
        self.group_entities = {}
    
    async def start(self):
        logger.info("Starting group monitor...")
        
        await self.client.start(phone=TELEGRAM_PHONE)
        
        logger.info("Connected to Telegram")
        
        await self._resolve_groups()
        
        self._setup_handlers()
        
        logger.info(f"Monitoring {len(self.group_entities)} groups")
        
        await self.client.run_until_disconnected()
    
    async def _resolve_groups(self):
        logger.info("Loading dialogs to find target groups...")
        
        all_dialogs = {}
        async for dialog in self.client.iter_dialogs():
            entity = dialog.entity
            if isinstance(entity, (Channel, Chat)):
                all_dialogs[str(dialog.id)] = entity
        
        logger.info(f"Found {len(all_dialogs)} groups/channels in account")
        
        for group_id in self.monitored_groups:
            if not group_id:
                continue
            
            if group_id in all_dialogs:
                entity = all_dialogs[group_id]
                self.group_entities[group_id] = entity
                name = getattr(entity, 'title', group_id)
                logger.info(f"Resolved group: {name} (ID: {group_id})")
            else:
                logger.error(f"Group {group_id} not found in account dialogs")
    
    def _setup_handlers(self):
        @self.client.on(events.NewMessage(chats=list(self.group_entities.values())))
        async def handle_new_message(event):
            await self._process_message(event)
    
    async def _process_message(self, event):
        try:
            message = event.message
            text = message.text or message.message or ""
            
            if not text:
                return
            
            chat = await event.get_chat()
            chat_id = str(chat.id)
            chat_title = getattr(chat, 'title', 'Unknown')
            chat_username = getattr(chat, 'username', None)
            
            logger.info(f"New message in '{chat_title}' (ID: {chat_id}): {text[:50]}...")
            
            order = await parse_order_async(
                text=text,
                source_group=chat_id,
                message_id=message.id,
                group_username=chat_username,
                group_title=chat_title
            )
            
            if order:
                logger.info(f"Found order: {order.point_a} -> {order.point_b}, price: {order.price}")
                if self.on_order_callback:
                    await self._notify_order(order)
            else:
                logger.debug(f"Message not recognized as order")
        
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def _notify_order(self, order: ParsedOrder):
        if self.on_order_callback:
            if asyncio.iscoroutinefunction(self.on_order_callback):
                await self.on_order_callback(order)
            else:
                self.on_order_callback(order)
    
    async def stop(self):
        logger.info("Stopping group monitor...")
        if self.client.is_connected():
            await self.client.disconnect()
        logger.info("Group monitor stopped")
    
    def add_group(self, group_id: str):
        if group_id not in self.monitored_groups:
            self.monitored_groups.append(group_id)
    
    def remove_group(self, group_id: str):
        if group_id in self.monitored_groups:
            self.monitored_groups.remove(group_id)

async def run_monitor(on_order_callback: Callable[[ParsedOrder], None]):
    monitor = GroupMonitor(on_order_callback=on_order_callback)
    await monitor.start()
