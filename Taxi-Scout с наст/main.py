import asyncio
import logging
import signal
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from src.config import BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH
from src.bot.driver_bot import DriverBot
from src.parser.multi_user_monitor import MultiUserMonitor
from src.matcher import OrderMatcher


class TaxiOrderSystem:
    def __init__(self):
        self.driver_bot = None
        self.multi_user_monitor = None
        self.order_matcher = None
        self.running = False
    
    async def start(self):
        logger.info("=" * 50)
        logger.info("Starting Taxi Order Bot System")
        logger.info("Bot commands + Group parsing enabled")
        logger.info("=" * 50)
        
        self._check_config()
        
        self.driver_bot = DriverBot()
        await self.driver_bot.start_async()
        logger.info("Driver bot started")
        
        self.order_matcher = OrderMatcher(
            bot_send_func=self.driver_bot.send_order_notification,
            bot_edit_func=self.driver_bot.edit_order_notification
        )
        logger.info("Order matcher initialized")
        
        self.multi_user_monitor = MultiUserMonitor(
            on_order_callback=self.order_matcher.process_order
        )
        logger.info("Multi-user monitor initialized")
        
        self.running = True
        
        logger.info("Starting multi-user group monitoring...")
        
        try:
            await self.multi_user_monitor.start()
        except Exception as e:
            logger.error(f"Error in multi-user monitor: {e}")
            raise
    
    def _check_config(self):
        missing = []
        
        if not BOT_TOKEN:
            missing.append("BOT_TOKEN")
        if not TELEGRAM_API_ID:
            missing.append("TELEGRAM_API_ID")
        if not TELEGRAM_API_HASH:
            missing.append("TELEGRAM_API_HASH")
        
        if missing:
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
            logger.info("")
            logger.info("Please set the following secrets:")
            logger.info("  BOT_TOKEN - Token from @BotFather")
            logger.info("  TELEGRAM_API_ID - From my.telegram.org")
            logger.info("  TELEGRAM_API_HASH - From my.telegram.org")
            logger.info("")
            sys.exit(1)
    
    async def stop(self):
        logger.info("Stopping system...")
        self.running = False
        
        if self.multi_user_monitor:
            await self.multi_user_monitor.stop()
        
        if self.driver_bot:
            await self.driver_bot.stop_async()
        
        logger.info("System stopped")


async def run_full_system():
    system = TaxiOrderSystem()
    
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        asyncio.create_task(system.stop())
    
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            pass
    
    await system.start()


if __name__ == "__main__":
    try:
        asyncio.run(run_full_system())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
