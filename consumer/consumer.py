import json
import time
from collections import defaultdict, deque
from statistics import mean
from kafka import KafkaConsumer
from utils.settings import TOPIC, KAFKA_BOOTSTRAP, MOVING_AVG_WINDOW, ALERT_PCT

# per-symbol sliding window of last N prices
windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=MOVING_AVG_WINDOW))

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    group_id="crypto-analytics",
    enable_auto_commit=True,
    auto_offset_reset="latest",
)

def maybe_alert(symbol: str, price: float, ma: float) -> str | None:
    # example alert: trade price deviates X% below/above moving average
    if ma <= 0:
        return None
    delta = (price - ma) / ma
    if abs(delta) >= ALERT_PCT / 100:  # ALERT_PCT is %, e.g. 0.75
        direction = "↑" if delta > 0 else "↓"
        return f"[ALERT] {symbol.upper()} {direction} price={price:.2f} MA({MOVING_AVG_WINDOW})={ma:.2f} Δ={delta*100:.2f}%"
    return None

if __name__ == "__main__":
    print(f"Consuming from '{TOPIC}' @ {KAFKA_BOOTSTRAP}. Window={MOVING_AVG_WINDOW}, Alert%={ALERT_PCT}%")
    try:
        for msg in consumer:
            event = msg.value
            symbol = (event.get("symbol") or msg.key or "unknown").lower()
            price = float(event["price"])
            dq = windows[symbol]
            dq.append(price)
            ma = mean(dq) if len(dq) > 0 else 0.0

            # simple stdout “dashboard”
            print(f"{time.strftime('%H:%M:%S')} | {symbol.upper():7} | price={price:,.2f} | MA{MOVING_AVG_WINDOW}={ma:,.2f} | n={len(dq)}")

            alert = maybe_alert(symbol, price, ma)
            if alert:
                print(alert)
    except KeyboardInterrupt:
        print("consumer stopped")
