import logging
import asyncio
import time
import qrcode
import io
import base64
from typing import Optional, Tuple
from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError

from src.config import TELEGRAM_API_ID, TELEGRAM_API_HASH
from src.utils.database import save_user_session, get_user_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TelethonAuthManager:
    def __init__(self):
        if not TELEGRAM_API_ID or not TELEGRAM_API_HASH:
            raise ValueError("TELEGRAM_API_ID and TELEGRAM_API_HASH must be set")
        
        self.api_id = int(TELEGRAM_API_ID)
        self.api_hash = TELEGRAM_API_HASH
        self.pending_qr = {}
    
    async def start_qr_login(self, user_db_id: int) -> Tuple[bool, str, Optional[bytes]]:
        try:
            if user_db_id in self.pending_qr:
                old_data = self.pending_qr[user_db_id]
                old_client = old_data.get('client')
                if old_client:
                    try:
                        await old_client.disconnect()
                    except:
                        pass
                del self.pending_qr[user_db_id]
            
            client = TelegramClient(
                StringSession(),
                self.api_id,
                self.api_hash,
                device_model="Taxi Bot",
                system_version="1.0",
                app_version="1.0",
                lang_code="ru",
                system_lang_code="ru"
            )
            await client.connect()
            
            if await client.is_user_authorized():
                session_string = client.session.save()
                save_user_session(user_db_id, session_string, "qr_login")
                await client.disconnect()
                return False, "Вы уже авторизованы!", None
            
            qr_login = await client.qr_login()
            
            login_url = qr_login.url
            logger.info(f"QR login started for user {user_db_id}, url: {login_url[:50]}...")
            
            qr = qrcode.QRCode(version=1, box_size=10, border=2)
            qr.add_data(login_url)
            qr.make(fit=True)
            
            img = qr.make_image(fill_color="black", back_color="white")
            img_buffer = io.BytesIO()
            img.save(img_buffer, format='PNG')
            img_bytes = img_buffer.getvalue()
            
            self.pending_qr[user_db_id] = {
                'client': client,
                'qr_login': qr_login,
                'timestamp': time.time(),
                'url': login_url
            }
            
            return True, login_url, img_bytes
            
        except Exception as e:
            logger.error(f"Error starting QR login: {type(e).__name__}: {e}")
            return False, f"Ошибка: {str(e)}", None
    
    async def wait_for_qr_confirm(self, user_db_id: int, timeout: int = 60) -> Tuple[bool, str, Optional[str]]:
        if user_db_id not in self.pending_qr:
            return False, "Сначала запросите QR-код через /auth", None
        
        pending = self.pending_qr[user_db_id]
        client = pending['client']
        qr_login = pending['qr_login']
        
        try:
            logger.info(f"Waiting for QR confirmation for user {user_db_id}...")
            
            try:
                await asyncio.wait_for(qr_login.wait(timeout), timeout=timeout)
            except asyncio.TimeoutError:
                logger.info(f"QR login timeout for user {user_db_id}")
                return False, "Время ожидания истекло. Попробуйте /auth снова", None
            
            session_string = client.session.save()
            save_user_session(user_db_id, session_string, "qr_login")
            
            del self.pending_qr[user_db_id]
            
            logger.info(f"User {user_db_id} authorized via QR successfully")
            return True, "Авторизация успешна!", session_string
            
        except SessionPasswordNeededError:
            pending['needs_2fa'] = True
            logger.info(f"2FA required for user {user_db_id}")
            return False, "Требуется пароль двухфакторной аутентификации.\nВведите ваш облачный пароль:", None
        except Exception as e:
            logger.error(f"Error waiting for QR confirm: {type(e).__name__}: {e}")
            if user_db_id in self.pending_qr:
                try:
                    await client.disconnect()
                except:
                    pass
                del self.pending_qr[user_db_id]
            return False, f"Ошибка: {str(e)}", None
    
    async def verify_2fa(self, user_db_id: int, password: str) -> Tuple[bool, str, Optional[str]]:
        if user_db_id not in self.pending_qr:
            return False, "Сессия истекла. Начните заново с /auth", None
        
        pending = self.pending_qr[user_db_id]
        if not pending.get('needs_2fa'):
            return False, "2FA не требуется", None
        
        client = pending['client']
        
        try:
            if not client.is_connected():
                await client.connect()
            
            await client.sign_in(password=password)
            
            session_string = client.session.save()
            save_user_session(user_db_id, session_string, "qr_login_2fa")
            
            del self.pending_qr[user_db_id]
            
            logger.info(f"User {user_db_id} authorized with 2FA successfully")
            return True, "Авторизация успешна!", session_string
            
        except Exception as e:
            logger.error(f"2FA error for user {user_db_id}: {e}")
            return False, f"Неверный пароль: {str(e)}", None
    
    async def cancel_auth(self, user_db_id: int):
        if user_db_id in self.pending_qr:
            pending = self.pending_qr[user_db_id]
            client = pending.get('client')
            if client:
                try:
                    await client.disconnect()
                except:
                    pass
            del self.pending_qr[user_db_id]
    
    async def get_user_client(self, user_db_id: int) -> Optional[TelegramClient]:
        user_session = get_user_session(user_db_id)
        if not user_session or not user_session.session_string:
            return None
        
        try:
            client = TelegramClient(
                StringSession(user_session.session_string),
                self.api_id,
                self.api_hash
            )
            await client.connect()
            
            if await client.is_user_authorized():
                return client
            else:
                logger.warning(f"User {user_db_id} session is no longer valid")
                return None
        except Exception as e:
            logger.error(f"Error getting user client: {e}")
            return None
    
    async def get_user_groups(self, user_db_id: int) -> list:
        client = await self.get_user_client(user_db_id)
        if not client:
            return []
        
        try:
            groups = []
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if hasattr(entity, 'megagroup') and entity.megagroup:
                    peer_id = utils.get_peer_id(entity)
                    groups.append({
                        'id': peer_id,
                        'title': dialog.title,
                        'username': getattr(entity, 'username', None)
                    })
                elif hasattr(entity, 'broadcast') and not entity.broadcast:
                    peer_id = utils.get_peer_id(entity)
                    groups.append({
                        'id': peer_id,
                        'title': dialog.title,
                        'username': getattr(entity, 'username', None)
                    })
            
            await client.disconnect()
            return groups
        except Exception as e:
            logger.error(f"Error getting user groups: {e}")
            try:
                await client.disconnect()
            except:
                pass
            return []


auth_manager = TelethonAuthManager()
