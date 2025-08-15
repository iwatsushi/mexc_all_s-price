#!/usr/bin/env python3
"""
MEXC WebSocket直接テスト - v3 API確認用
"""
import asyncio
import json
import sys
from datetime import datetime

import websockets


async def test_mexc_websocket():
    """MEXC WebSocketを直接テストしてv3 APIの動作を確認"""

    # セキュアWebSocket URLを試行
    ws_url = "wss://wbs-api.mexc.com/ws"
    print(f"Connecting to MEXC WebSocket: {ws_url}")

    try:
        async with websockets.connect(ws_url) as websocket:
            print("Connected successfully!")

            # 正しいv3 API形式で購読（ティッカーデータ）
            major_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

            print(
                f"Subscribing to {len(major_symbols)} symbols using correct v3 API..."
            )
            for symbol in major_symbols:
                # 正しい購読形式: spot@public.deals.v3.api@{symbol}
                subscribe_msg = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.deals.v3.api@{symbol}"],
                }
                message = json.dumps(subscribe_msg)
                print(f"   -> Sending: {message}")
                await websocket.send(message)
                await asyncio.sleep(0.2)  # 待機

            print("Listening for messages (30 seconds)...")
            message_count = 0
            ticker_count = 0

            # 30秒間メッセージを受信
            start_time = datetime.now()
            timeout_duration = 30

            while (datetime.now() - start_time).seconds < timeout_duration:
                try:
                    # 1秒タイムアウトでメッセージ受信
                    message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    message_count += 1

                    try:
                        data = json.loads(message)
                        print(f"[{message_count:3d}] Raw message: {len(message)} chars")

                        # メッセージタイプを分析
                        if "method" in data:
                            print(f"     Method: {data.get('method')}")

                        if "result" in data:
                            print(f"     Result: {data.get('result')}")

                        # v3 APIティッカーデータをチェック
                        if "c" in data and "s" in data:
                            symbol = data.get("s", "")
                            price = data.get("c", "")
                            volume = data.get("v", "")
                            ticker_count += 1
                            print(
                                f"     TICKER DATA: {symbol} = {price} (volume: {volume})"
                            )

                        # 全データを表示（最初の5件のみ）
                        if message_count <= 5:
                            print(f"     Full data: {data}")

                    except json.JSONDecodeError:
                        print(f"     Non-JSON message: {message[:100]}...")

                except asyncio.TimeoutError:
                    print("No message received in 1 second")
                    continue
                except Exception as e:
                    print(f"Error receiving message: {e}")
                    break

            print(f"\nTest Results:")
            print(f"   - Duration: {timeout_duration} seconds")
            print(f"   - Total messages: {message_count}")
            print(f"   - Ticker messages: {ticker_count}")
            print(f"   - Messages per second: {message_count / timeout_duration:.2f}")

            if ticker_count > 0:
                print("MEXC v3 API is working - ticker data received!")
            else:
                print("No ticker data received - API may not be working")

    except Exception as e:
        print(f"Connection failed: {e}")
        return False

    return ticker_count > 0


if __name__ == "__main__":
    print("MEXC WebSocket Direct Test Starting...")
    result = asyncio.run(test_mexc_websocket())

    if result:
        print("\nTest PASSED: MEXC v3 API is working")
        sys.exit(0)
    else:
        print("\nTest FAILED: MEXC v3 API is not working")
        sys.exit(1)
