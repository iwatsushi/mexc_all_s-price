#!/usr/bin/env python3
"""
MEXC WebSocket 詳細デバッグ用最小実装
ワイヤレベルのログと自動再接続機能付き
"""

import asyncio, json, time, traceback, logging, sys
import websockets

URL = "wss://contract.mexc.com/edge"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

PING_INTERVAL_SEC = 15
STALL_WARN_SEC = 5          # 受信が5s止まったら警告
STALL_RESET_SEC = 10        # 受信が10s止まったら再接続

def j(obj, maxlen=300):
    try:
        s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        return s if len(s) <= maxlen else s[:maxlen] + f"...({len(s)} bytes)"
    except Exception:
        return f"<non-json {type(obj)}>"

async def mexc_client():
    attempt = 0
    while True:
        attempt += 1
        try:
            logging.info(f"CONNECT attempt={attempt} -> {URL}")
            async with websockets.connect(
                URL,
                ping_interval=None,     # WSコントロールPingは無効（JSON pingを送る）
                max_size=None,          # 大きなpush.tickersに備えて上限解除
                open_timeout=20,
                close_timeout=5,
            ) as ws:
                last_recv = time.monotonic()
                msg_count = 0
                last_tickers_ts = None

                # 送信: sub.tickers（非圧縮）
                sub = {"method":"sub.tickers", "param":{}, "gzip": False}
                await ws.send(json.dumps(sub))
                logging.info("SEND " + j(sub))

                # JSON pingタスク（RTT計測）
                async def keepalive():
                    while True:
                        await asyncio.sleep(PING_INTERVAL_SEC)
                        t0 = time.time()
                        ping = {"method":"ping"}
                        await ws.send(json.dumps(ping))
                        logging.info(f"SEND {j(ping)} rtt=?")
                ka_task = asyncio.create_task(keepalive())

                # 受信ループ＋スタール監視
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        # 受信スタール監視
                        since = time.monotonic() - last_recv
                        if since > STALL_WARN_SEC:
                            logging.warning(f"STALL {since:.1f}s without messages")
                        if since > STALL_RESET_SEC:
                            raise RuntimeError(f"No messages for {since:.1f}s -> reconnect")
                        continue

                    now = time.monotonic()
                    last_recv = now
                    msg_count += 1

                    # テキスト/バイナリで分岐
                    if isinstance(msg, (bytes, bytearray)):
                        logging.info(f"RECV BINARY len={len(msg)} bytes (unexpected with gzip:false)")
                        continue

                    try:
                        data = json.loads(msg)
                    except Exception:
                        logging.error("RECV TEXT (non-JSON) " + (msg[:200] + "..."))
                        continue

                    ch = data.get("channel")
                    # 重要イベントごとに短く要約
                    if ch == "push.tickers":
                        n = len(data.get("data", []))
                        ts = data.get("ts")
                        last_tickers_ts = ts
                        logging.info(f"RECV push.tickers items={n} ts={ts} total_msgs={msg_count}")
                    elif ch in ("pong", "rs.login", "rs.error",
                                "rs.sub.tickers", "rs.unsub.tickers"):
                        logging.info("RECV " + j(data))
                    else:
                        # その他のpublic pushなど
                        logging.debug("RECV " + j(data))

        except Exception as e:
            logging.error("EXCEPTION: " + repr(e))
            logging.error(traceback.format_exc())
            # エクスポネンシャルバックオフ（最大20s）
            backoff = min(20, 2 ** min(attempt, 5))
            logging.info(f"RECONNECT in {backoff}s")
            await asyncio.sleep(backoff)
            continue

if __name__ == "__main__":
    asyncio.run(mexc_client())