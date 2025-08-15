#!/usr/bin/env python3
"""
MEXC Futures WebSocket直接テスト - sub.tickersチャネル確認用
"""
import asyncio
import json
import sys
from datetime import datetime

import websockets


async def test_mexc_futures_websocket():
    """MEXC Futures WebSocketを直接テストしてsub.tickersの動作を確認"""

    # MEXC Futures WebSocket URL
    ws_url = "wss://contract.mexc.com/edge"
    print(f"Connecting to MEXC Futures WebSocket: {ws_url}")

    try:
        async with websockets.connect(ws_url) as websocket:
            print("Connected successfully!")

            # sub.tickers チャネルを購読
            print("Subscribing to sub.tickers channel...")
            subscribe_msg = {"method": "sub.tickers", "param": {}}
            message = json.dumps(subscribe_msg)
            print(f"   -> Sending: {message}")
            await websocket.send(message)

            print("Listening for messages (60 seconds)...")
            message_count = 0
            ticker_count = 0
            unique_symbols = set()

            # 60秒間メッセージを受信
            start_time = datetime.now()
            timeout_duration = 60

            while (datetime.now() - start_time).seconds < timeout_duration:
                try:
                    # 1秒タイムアウトでメッセージ受信
                    message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    message_count += 1

                    try:
                        data = json.loads(message)

                        if message_count <= 3:
                            print(
                                f"[{message_count:3d}] Raw message: {len(message)} chars"
                            )
                            print(f"     Full data: {data}")
                        elif message_count % 50 == 0:
                            print(
                                f"[{message_count:3d}] Processed {message_count} messages..."
                            )

                        # 購読確認メッセージ
                        if data.get("channel") == "sub.tickers":
                            print(f"     Subscription confirmed: {data}")
                            continue

                        # ティッカーデータをチェック
                        if "data" in data and isinstance(data["data"], list):
                            for ticker in data["data"]:
                                if isinstance(ticker, dict):
                                    symbol = ticker.get("symbol", "")
                                    price = ticker.get("lastPrice", "")
                                    if symbol and price:
                                        ticker_count += 1
                                        unique_symbols.add(symbol)

                                        # 最初の5個のティッカーデータを表示
                                        if ticker_count <= 5:
                                            print(f"     TICKER: {symbol} = {price}")

                        # 単一ティッカーデータの場合
                        elif "symbol" in data and "lastPrice" in data:
                            symbol = data.get("symbol", "")
                            price = data.get("lastPrice", "")
                            ticker_count += 1
                            unique_symbols.add(symbol)

                            if ticker_count <= 5:
                                print(f"     TICKER: {symbol} = {price}")

                    except json.JSONDecodeError:
                        print(f"     Non-JSON message: {message[:100]}...")

                except asyncio.TimeoutError:
                    if message_count == 0:
                        print("No message received in 1 second")
                    continue
                except Exception as e:
                    print(f"Error receiving message: {e}")
                    break

            print(f"\nTest Results:")
            print(f"   - Duration: {timeout_duration} seconds")
            print(f"   - Total messages: {message_count}")
            print(f"   - Ticker messages: {ticker_count}")
            print(f"   - Unique symbols: {len(unique_symbols)}")
            print(f"   - Messages per second: {message_count / timeout_duration:.2f}")
            print(f"   - Tickers per second: {ticker_count / timeout_duration:.2f}")

            if len(unique_symbols) > 0:
                print(f"   - Sample symbols: {list(unique_symbols)[:10]}")

            if ticker_count > 0:
                print("MEXC Futures sub.tickers is working!")
                return True
            else:
                print("No ticker data received")
                return False

    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    print("MEXC Futures WebSocket Test Starting...")
    result = asyncio.run(test_mexc_futures_websocket())

    if result:
        print("\nTest PASSED: MEXC Futures sub.tickers is working")
        sys.exit(0)
    else:
        print("\nTest FAILED: MEXC Futures sub.tickers is not working")
        sys.exit(1)
