import os
import json
import logging
from typing import Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from openai import OpenAI

logger = logging.getLogger(__name__)

AI_INTEGRATIONS_OPENAI_API_KEY = os.environ.get("AI_INTEGRATIONS_OPENAI_API_KEY")
AI_INTEGRATIONS_OPENAI_BASE_URL = os.environ.get("AI_INTEGRATIONS_OPENAI_BASE_URL")

openai_client = None
if AI_INTEGRATIONS_OPENAI_API_KEY and AI_INTEGRATIONS_OPENAI_BASE_URL:
    openai_client = OpenAI(
        api_key=AI_INTEGRATIONS_OPENAI_API_KEY,
        base_url=AI_INTEGRATIONS_OPENAI_BASE_URL
    )

def is_rate_limit_error(exception: BaseException) -> bool:
    error_msg = str(exception)
    if "429" in error_msg or "RATELIMIT_EXCEEDED" in error_msg:
        return True
    if "quota" in error_msg.lower() or "rate limit" in error_msg.lower():
        return True
    if hasattr(exception, "status_code"):
        status = getattr(exception, "status_code", None)
        if status == 429:
            return True
    return False

EXTRACTION_PROMPT = """Ты - эксперт по анализу заказов такси межгород в России.

Проанализируй текст сообщения и извлеки информацию о заказе:
1. Город отправления (точка А)
2. Город назначения (точка Б)
3. Цена в рублях (если указана)

Правила:
- Ищи названия городов России
- Игнорируй адреса улиц, только города
- Цена должна быть числом в рублях
- Если город указан в сокращении (СПб, Екб, МСК), преобразуй в полное название
- Если город в косвенном падеже (из Уфы, в Казань), преобразуй в именительный падеж

Ответь ТОЛЬКО в формате JSON:
{"point_a": "Город1", "point_b": "Город2", "price": 3500}

Если не можешь определить город, поставь null.
Если цена не указана, поставь null.

Текст сообщения:
"""

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception(is_rate_limit_error),
    reraise=True
)
def extract_order_with_ai(text: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not openai_client:
        logger.warning("OpenAI client not configured")
        return (None, None, None)
    
    try:
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты извлекаешь информацию о заказах такси. Отвечай только JSON."},
                {"role": "user", "content": EXTRACTION_PROMPT + text}
            ],
            response_format={"type": "json_object"},
            max_completion_tokens=200
        )
        
        content = response.choices[0].message.content
        if not content:
            return (None, None, None)
        
        data = json.loads(content)
        
        point_a = data.get("point_a")
        point_b = data.get("point_b")
        price = data.get("price")
        
        if price is not None:
            try:
                price = int(price)
            except (ValueError, TypeError):
                price = None
        
        logger.info(f"AI extracted: {point_a} -> {point_b}, price: {price}")
        return (point_a, point_b, price)
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse AI response: {e}")
        return (None, None, None)
    except Exception as e:
        logger.error(f"AI extraction error: {e}")
        raise

def is_ai_available() -> bool:
    return openai_client is not None
