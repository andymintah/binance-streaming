import json
from collections import defaultdict, deque
from statistics import mean
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer
from threading import Thread
import time
from utils.settings import TOPIC, KAFKA_BOOTSTRAP, MOVING_AVG_WINDOW, ALERT_PCT

app = FastAPI(title="Crypto Dashboard")

windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=MOVING_AVG_WINDOW))
latest_price: dict[str, float] = {}
alerts: list[dict] = []

def consume_trades():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="crypto-dashboard",
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    for msg in consumer:
        event = msg.value
        symbol = (event.get("symbol") or msg.key or "unknown").lower()
        price = float(event["price"])
        dq = windows[symbol]
        dq.append(price)
        latest_price[symbol] = price
        ma = mean(dq) if len(dq) > 0 else 0.0

        delta = (price - ma) / ma if ma else 0
        if abs(delta) >= ALERT_PCT / 100:
            alert = {
                "symbol": symbol,
                "price": price,
                "ma": ma,
                "delta_pct": round(delta*100, 2),
                "ts": int(time.time())
            }
            alerts.append(alert)
            # keep last 50 alerts
            if len(alerts) > 50:
                alerts.pop(0)

Thread(target=consume_trades, daemon=True).start()

@app.get("/prices")
def get_prices():
    result = []
    for symbol, dq in windows.items():
        ma = mean(dq) if len(dq) > 0 else 0
        result.append({
            "symbol": symbol,
            "latest_price": latest_price.get(symbol, 0.0),
            "moving_average": ma,
            "window_size": len(dq)
        })
    return JSONResponse(result)

@app.get("/alerts")
def get_alerts():
    return JSONResponse(alerts)
