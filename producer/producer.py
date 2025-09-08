import json
import time
import threading
from websocket import WebSocketApp
from kafka import KafkaProducer
from utils.settings import SYMBOLS, TOPIC, KAFKA_BOOTSTRAP

BASE_WS = "wss://stream.binance.com:9443/stream?streams=" + "/".join([f"{s}@trade" for s in SYMBOLS])

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    linger_ms=5,
    retries=5,
)

def on_message(ws, message):
    try:
        data = json.loads(message)
        evt = data.get("data", {})
        # normalize payload
        out = {
            "exchange": "binance",
            "symbol": evt.get("s", "").lower(),
            "trade_id": evt.get("t"),
            "price": float(evt.get("p")),
            "qty": float(evt.get("q")),
            "is_buyer_maker": bool(evt.get("m")),
            "ts_event": int(evt.get("E")),
            "ts_trade": int(evt.get("T")),
            "ingest_ts": int(time.time() * 1000),
        }
        key = out["symbol"]
        producer.send(TOPIC, key=key, value=out)
    except Exception as e:
        print("producer error:", e)

def on_error(ws, error):
    print("ws error:", error)

def on_close(ws, *_):
    print("ws closed — retrying in 3s…")
    time.sleep(3)
    start_ws()  # simple auto-reconnect

def on_open(ws):
    print("connected to:", BASE_WS)

def start_ws():
    ws = WebSocketApp(BASE_WS, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open)
    # run forever on thread so SIGINT is nicer
    t = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 20, "ping_timeout": 10}, daemon=True)
    t.start()
    return ws, t

if __name__ == "__main__":
    print(f"Producing trades for {SYMBOLS} to topic '{TOPIC}' via {KAFKA_BOOTSTRAP}")
    ws, t = start_ws()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("shutting down producer…")
        producer.flush(5)
        producer.close(5)
