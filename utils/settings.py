import os
from dotenv import load_dotenv

load_dotenv()

SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt").split(",") if s.strip()]
TOPIC = os.getenv("TOPIC", "trades.crypto")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MOVING_AVG_WINDOW = int(os.getenv("MOVING_AVG_WINDOW", "50"))
ALERT_PCT = float(os.getenv("ALERT_PCT", "0.75"))
