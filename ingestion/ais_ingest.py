"""
ais_ingest.py
--------------
يتصل هذا السكريبت بخدمة aisstream.io عبر WebSocket ليستقبل بيانات مواقع السفن
الحية (خط الطول، خط العرض، السرعة، الاتجاه، اسم السفينة...) داخل صندوق جغرافي
(Bounding Box) نحدده حول موانئ الإمارات، ثم يرسل كل رسالة كما هي إلى Kafka
كطبقة Bronze خام قبل أي معالجة.

المتطلبات:
    pip install websockets kafka-python python-dotenv

قبل التشغيل:
    1) اعمل حساب مجاني على https://aisstream.io واحصل على API Key
    2) ضع الـ API Key في ملف .env بجانب هذا الملف كالتالي:
       AISSTREAM_API_KEY=ضع_المفتاح_هنا
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import websockets
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ais_ingest")

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ais-raw-messages")

# صندوق جغرافي يغطي موانئ الإمارات الرئيسية (جبل علي، ميناء زايد، ميناء خليفة، الفجيرة)
# الصيغة المطلوبة من aisstream.io: [[[lat_min, lon_min], [lat_max, lon_max]]]
UAE_BOUNDING_BOX = [[[24.5, 51.0], [26.5, 57.0]]]


def build_kafka_producer() -> KafkaProducer:
    """ينشئ Kafka Producer مع إعادة المحاولة التلقائية عند الفشل."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        linger_ms=200,
    )


async def stream_ais_data(producer: KafkaProducer) -> None:
    """يفتح اتصال WebSocket مع aisstream.io ويرسل كل رسالة واردة إلى Kafka."""
    if not AISSTREAM_API_KEY:
        raise RuntimeError(
            "لم يتم العثور على AISSTREAM_API_KEY. تأكد من وجود ملف .env يحتوي عليه."
        )

    uri = "wss://stream.aisstream.io/v0/stream"

    async with websockets.connect(uri) as websocket:
        subscribe_message = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": UAE_BOUNDING_BOX,
            "FilterMessageTypes": ["PositionReport"],
        }
        await websocket.send(json.dumps(subscribe_message))
        logger.info("تم الاشتراك في تدفق بيانات AIS لمنطقة الإمارات")

        async for raw_message in websocket:
            try:
                message = json.loads(raw_message)
                enriched = {
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "raw": message,
                }
                producer.send(KAFKA_TOPIC, value=enriched)
                mmsi = (
                    message.get("MetaData", {}).get("MMSI")
                    if isinstance(message, dict)
                    else None
                )
                logger.info("تم استلام رسالة موقع سفينة | MMSI=%s", mmsi)
            except (json.JSONDecodeError, KafkaError) as exc:
                logger.warning("تم تجاهل رسالة بسبب خطأ: %s", exc)


async def run_forever_with_reconnect() -> None:
    """يعيد المحاولة تلقائيًا عند انقطاع الاتصال بدل توقف السكريبت بالكامل."""
    producer = build_kafka_producer()
    backoff_seconds = 5

    while True:
        try:
            await stream_ais_data(producer)
        except Exception as exc:  # noqa: BLE001
            logger.error("انقطع الاتصال، إعادة المحاولة خلال %ss | السبب: %s", backoff_seconds, exc)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 60)
        else:
            backoff_seconds = 5


if __name__ == "__main__":
    asyncio.run(run_forever_with_reconnect())
