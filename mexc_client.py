"""
MEXC取引所クライアント（WebSocket + REST API統合）
"""

import asyncio
import hashlib
import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests
import websockets

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """ティックデータ"""

    symbol: str
    price: float
    timestamp: datetime
    volume: float = 0.0


@dataclass
class PositionData:
    """ポジションデータ"""

    symbol: str
    side: str  # "LONG" or "SHORT"
    size: float
    entry_price: float
    mark_price: float
    pnl: float
    margin_type: str  # "isolated" or "cross"


@dataclass
class OrderResult:
    """注文結果"""

    success: bool
    order_id: str = ""
    message: str = ""


class MEXCWebSocketClient:
    """MEXC Futures WebSocket全銘柄価格購読クライアント"""

    def __init__(self, config: Config):
        self.config = config
        # MEXC Futures WebSocket URL
        self.ws_url = "wss://contract.mexc.com/edge"
        self.running = False
        self.shutdown_event = threading.Event()

        # データコールバック
        self.tick_callback: Optional[Callable[[TickData], None]] = None

        # WebSocket関連
        self._ws_task = None
        self._websocket = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5

    async def connect(self) -> bool:
        """WebSocket接続開始"""
        try:
            logger.info(f"Starting MEXC WebSocket connection: {self.ws_url}")

            self.running = True
            # WebSocketタスクを開始
            self._ws_task = asyncio.create_task(self._websocket_loop())
            logger.info("WebSocket task started")
            return True

        except Exception as e:
            logger.error(f"Failed to start MEXC WebSocket: {e}")
            return False

    async def disconnect(self):
        """WebSocket接続停止"""
        logger.info("Stopping MEXC WebSocket...")

        self.running = False
        self.shutdown_event.set()

        if self._websocket:
            await self._websocket.close()

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        logger.info("MEXC WebSocket stopped")

    async def start(self) -> bool:
        """WebSocket接続開始（エイリアス）"""
        return await self.connect()

    async def stop(self):
        """WebSocket接続停止（エイリアス）"""
        await self.disconnect()

    async def subscribe_all_tickers(self) -> bool:
        """全銘柄ティッカー購読開始"""
        if self.running:
            logger.info("MEXC WebSocket already running and subscribed")
            return True
        else:
            logger.warning("MEXC WebSocket not started")
            return False

    def set_tick_callback(self, callback: Callable[[TickData], None]):
        """価格データコールバックを設定"""
        self.tick_callback = callback

    async def _websocket_loop(self):
        """WebSocketメインループ（再接続対応）"""
        logger.info("🔄 MEXC WebSocket loop started")

        while self.running and not self.shutdown_event.is_set():
            try:
                await self._websocket_connection()
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")

                if (
                    self.running
                    and self._reconnect_attempts < self._max_reconnect_attempts
                ):
                    self._reconnect_attempts += 1
                    wait_time = min(
                        2**self._reconnect_attempts, 30
                    )  # 指数バックオフ、最大30秒
                    logger.info(
                        f"Reconnecting in {wait_time} seconds (attempt {self._reconnect_attempts})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max reconnection attempts reached")
                    break

        logger.info("MEXC WebSocket loop ended")

    async def _websocket_connection(self):
        """WebSocket接続処理"""

        async with websockets.connect(
            self.ws_url, 
            ping_interval=None,      # 手動pingを使用
            max_size=None,          # フレームサイズ制限を解除
            open_timeout=20,
            close_timeout=5
        ) as websocket:
            self._websocket = websocket
            self._reconnect_attempts = 0  # 成功したらリセット

            logger.info("WebSocket connected, subscribing to tickers...")

            # sub.tickers チャネルを購読（全銘柄、非圧縮指定）
            subscribe_msg = {"method": "sub.tickers", "param": {}, "gzip": False}
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to sub.tickers channel (non-compressed)")
            
            # sub.tickersのみに集中（シンプル化）
            logger.info("Focusing on sub.tickers only for continuous data")
            
            # ping定期送信用タスクを開始
            ping_task = asyncio.create_task(self._send_periodic_ping(websocket))

            # メッセージ受信ループ（デバッグスクリプトと同じタイムアウト方式を採用）
            last_recv = time.monotonic()  # デバッグスクリプトと同じ単調時間を使用
            message_count = 0
            
            while not self.shutdown_event.is_set():
                try:
                    # タイムアウト付きでメッセージを受信（デバッグスクリプトと同じ方式）
                    message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    last_recv = time.monotonic()  # デバッグスクリプトと同じ方式
                    message_count += 1
                    logger.info(f"💬 Raw message #{message_count} received: {message[:200]}...")
                    data = json.loads(message)

                    # 購読確認メッセージ
                    if data.get("channel") == "rs.sub.tickers":
                        logger.info(f"Subscription confirmed: {data.get('data')}")
                        continue
                    
                    # pongメッセージの処理
                    if data.get("channel") == "pong":
                        logger.debug("💓 Received pong from server")
                        continue

                    # ティッカーデータ処理（全銘柄）
                    if data.get("channel") == "push.tickers" and "data" in data:
                        tickers = data["data"]
                        if isinstance(tickers, list):
                            logger.info(
                                f"📊 MEXC WebSocket received {len(tickers)} tickers (全銘柄)"
                            )
                            # 🔧 デバッグ: コールバック処理を完全に無効化してメッセージ受信のみテスト
                            logger.info(f"🔧 DEBUG: Skipping callback processing for {len(tickers)} tickers to test pure message reception")
                            # self._process_ticker_data(tickers)  # 一時的に無効化
                    
                    
                    # 未処理のメッセージをログ出力（デバッグ用）
                    else:
                        logger.info(f"🔍 Unhandled message: {json.dumps(data)[:200]}...")

                except asyncio.TimeoutError:
                    # タイムアウト（1秒間メッセージなし）- スタール検出（デバッグスクリプトと同じ方式）
                    since = time.monotonic() - last_recv
                    if since > 10:  # 10秒でスタール警告
                        logger.warning(f"⚠️ MEXC WebSocket STALL: {since:.1f}秒間メッセージを受信していません")
                        # 再接続をトリガーするために例外を発生
                        raise websockets.exceptions.ConnectionClosed(None, None)
                    continue  # タイムアウト時は継続
                    
                except json.JSONDecodeError:
                    logger.warning(f"Non-JSON message received: {message[:100]}...")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")
            
            # pingタスクをキャンセル
            if 'ping_task' in locals():
                ping_task.cancel()

    def _process_ticker_data(self, tickers):
        """ティッカーデータを処理"""
        if not self.tick_callback:
            logger.warning("No tick callback set!")
            return
            
        logger.info(f"🔧 Processing {len(tickers)} tickers with callback")
        processed_count = 0
        
        for ticker in tickers:
            if isinstance(ticker, dict):
                symbol = ticker.get("symbol", "")
                price = float(ticker.get("lastPrice", 0))
                volume = float(ticker.get("volume24", 0))

                if symbol and price > 0:
                    tick = TickData(
                        symbol=symbol,
                        price=price,
                        timestamp=datetime.now(),
                        volume=volume,
                    )

                    try:
                        self.tick_callback(tick)
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"Error in tick callback for {symbol}: {e}")

        if processed_count > 0:
            logger.info(f"✅ Processed {processed_count} ticks via callback")

    async def _send_periodic_ping(self, websocket):
        """定期的にpingメッセージを送信してWebSocket接続を維持"""
        try:
            while not self.shutdown_event.is_set():
                await asyncio.sleep(15)  # 15秒間隔でping送信
                try:
                    ping_msg = {"method": "ping"}
                    await websocket.send(json.dumps(ping_msg))
                    logger.debug("💓 Sent ping to maintain WebSocket connection")
                except Exception as e:
                    logger.warning(f"Failed to send ping: {e}")
                    break
        except asyncio.CancelledError:
            logger.debug("Ping task cancelled")


# MEXCClientとしてWebSocket版を使用
MEXCClient = MEXCWebSocketClient
