"""
MEXC取引所クライアント（WebSocket + REST API統合）
"""

import asyncio
import gzip
import hashlib
import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from collections import deque

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
        self.batch_callback: Optional[Callable[[list], None]] = (
            None  # パターンB'用バッチコールバック
        )

        # WebSocket関連
        self._ws_task = None
        self._websocket = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5
        
        # マルチプロセスキュー（main.pyから提供される）
        self._data_queue = None

    async def connect(self) -> bool:
        """WebSocket接続開始"""
        try:
            logger.info(f"Starting MEXC WebSocket connection: {self.ws_url}")

            self.running = True
            # WebSocketタスクを開始
            self._ws_task = asyncio.create_task(self._websocket_loop())
            logger.info("🔄 WebSocket task started")
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

    def set_batch_callback(self, callback: Callable[[list], None]):
        """バッチデータコールバックを設定（パターンB'用）"""
        self.batch_callback = callback

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
            ping_interval=None,  # 手動pingを使用
            max_size=None,  # フレームサイズ制限を解除
            open_timeout=20,
            close_timeout=5,
        ) as websocket:
            self._websocket = websocket
            self._reconnect_attempts = 0  # 成功したらリセット

            logger.info("WebSocket connected, subscribing to tickers...")

            # sub.tickers チャネルを購読（全銘柄、gzip圧縮有効）
            subscribe_msg = {"method": "sub.tickers", "param": {}, "gzip": True}
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to sub.tickers channel (gzip compressed)")

            # sub.tickersのみに集中（シンプル化）
            logger.info("Focusing on sub.tickers only for continuous data")

            # ping定期送信用タスクを開始
            ping_task = asyncio.create_task(self._send_periodic_ping(websocket))

            # メッセージ受信ループ（デバッグスクリプトと同じタイムアウト方式を採用）
            last_recv = time.monotonic()  # デバッグスクリプトと同じ単調時間を使用
            message_count = 0
            
            # 📊 受信間隔測定用（ChatGPT5提案）
            last_ticker_time = None
            ticker_intervals = []

            logger.info("🔄 Starting WebSocket message receive loop...")

            while not self.shutdown_event.is_set():
                try:
                    # タイムアウト付きでメッセージを受信（デバッグスクリプトと同じ方式）
                    logger.debug("📥 Waiting for WebSocket message...")
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    rx_time = time.monotonic()  # 📊 受信直後の時刻（ChatGPT5提案）
                    last_recv = rx_time
                    message_count += 1

                    # 🚀 ChatGPT5提案: 受信直後は生データをキューに投入のみ
                    logger.debug(f"💬 Raw message #{message_count} received: {len(raw_message)} chars")
                    
                    # 📊 受信間隔測定（受信直後）
                    if last_ticker_time is not None:
                        interval = rx_time - last_ticker_time
                        ticker_intervals.append(interval)
                        
                        # 統計ログ（10回毎）
                        if len(ticker_intervals) % 10 == 0:
                            recent_intervals = ticker_intervals[-10:]
                            avg_interval = sum(recent_intervals) / len(recent_intervals)
                            min_interval = min(recent_intervals)
                            max_interval = max(recent_intervals)
                            logger.info(
                                f"📊 Arrival interval stats (last 10): avg={avg_interval:.3f}s, "
                                f"min={min_interval:.3f}s, max={max_interval:.3f}s"
                            )
                    last_ticker_time = rx_time
                    
                    # 🚀 超軽量処理：生データをそのままマルチプロセスキューに投入（解凍は後段で）
                    if self._data_queue:
                        try:
                            self._data_queue.put_nowait({
                                "raw_message": raw_message,
                                "rx_time": rx_time,
                                "message_count": message_count
                            })
                            logger.debug(f"✅ Raw message #{message_count} sent to multiprocess queue")
                        except:
                            logger.debug(f"⚠️ Multiprocess queue full, dropping message #{message_count}")
                    else:
                        logger.debug(f"⚠️ Data queue not configured, dropping message #{message_count}")

                except asyncio.TimeoutError:
                    # タイムアウト（1秒間メッセージなし）- スタール検出（デバッグスクリプトと同じ方式）
                    since = time.monotonic() - last_recv
                    if since > 15:  # 15秒でスタール警告（少し延長）
                        logger.warning(
                            f"⚠️ MEXC WebSocket STALL: {since:.1f}秒間メッセージを受信していません"
                        )
                        # 再接続をトリガーするために例外を発生
                        raise websockets.exceptions.ConnectionClosed(None, None)
                    elif since > 2:  # 2秒以上でDEBUGログ
                        logger.info(
                            f"⏰ WebSocket timeout check: {since:.1f}s since last message (total_messages: {message_count})"
                        )
                    
                    # 🔄 タイムアウト後のヘルスチェック追加（完全に安全なアクセス）
                    try:
                        # WebSocket状態を安全にチェック（closed属性を使わない）
                        try:
                            # ノンブロッキングpingで接続状態を確認
                            ping_task = asyncio.create_task(websocket.ping())
                            try:
                                await asyncio.wait_for(ping_task, timeout=0.5)
                                logger.debug("💓 WebSocket ping successful during timeout check")
                            except asyncio.TimeoutError:
                                ping_task.cancel()
                                logger.debug("💓 WebSocket ping timeout (continuing)")
                        except (AttributeError, websockets.exceptions.ConnectionClosed) as ping_error:
                            logger.warning(f"⚠️ WebSocket ping failed: {ping_error}")
                            # ping失敗 = 接続問題の可能性
                            raise websockets.exceptions.ConnectionClosed(None, None)
                        
                    except Exception as health_error:
                        logger.error(f"🚨 WebSocket health check failed: {health_error}")
                        raise websockets.exceptions.ConnectionClosed(None, None)
                    
                    continue  # タイムアウト時は継続

                except json.JSONDecodeError as e:
                    logger.warning(f"Non-JSON message received: {e}")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")

            # pingタスクをキャンセル
            if "ping_task" in locals():
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    def _process_ticker_batch_safe(self, tickers):
        """WebSocket受信を保護する超高速バッチティッカーデータ処理（ChatGPT5最適化）"""
        if not self.batch_callback:
            logger.warning("No batch callback set!")
            return

        try:
            # 🚀 最小限の前処理：受信ループでは最低限のみ（ChatGPT5提案）
            valid_count = 0
            for ticker in tickers:
                if (
                    isinstance(ticker, dict)
                    and ticker.get("symbol")
                    and ticker.get("lastPrice")
                ):
                    valid_count += 1

            if valid_count > 0:
                logger.debug(f"🎯 Calling batch callback with {valid_count}/{len(tickers)} tickers")
                # 🚀 重要：生データをそのまま渡して処理は後段で（受信ループ保護）
                self.batch_callback(tickers)
                logger.debug(f"✅ Batch callback completed")

        except Exception as e:
            # エラーログは出すが、WebSocket受信は継続
            logger.error(f"Error in batch callback: {e}")

    def _process_ticker_data_safe(self, tickers):
        """WebSocket受信を保護する高速ティッカーデータ処理（互換性維持）"""
        if not self.tick_callback:
            logger.warning("No tick callback set!")
            return

        # 📊 統計のみ（瞬時）
        logger.debug(f"🚀 Fast processing {len(tickers)} tickers (legacy mode)")
        processed_count = 0

        # 🎯 最小限の処理：TickDataオブジェクト作成と非同期キューイング
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
                        # 🚀 重要：同期コールバックで即座に処理（最小限）
                        # 重い処理（戦略分析・DB保存）は内部で非同期化される
                        self.tick_callback(tick)
                        processed_count += 1
                    except Exception as e:
                        # エラーログは出すが、WebSocket受信は継続
                        logger.error(f"Error in tick callback for {symbol}: {e}")

        if processed_count > 0:
            logger.debug(f"✅ Fast processed {processed_count} ticks (legacy mode)")

    def _process_ticker_data(self, tickers):
        """従来のティッカーデータ処理（互換性維持）"""
        # 新しい安全な処理に移譲
        self._process_ticker_data_safe(tickers)

    def set_data_queue(self, data_queue):
        """マルチプロセスキューを設定"""
        self._data_queue = data_queue
        logger.info("✅ Multiprocess data queue configured")
    
    async def _send_periodic_ping(self, websocket):
        """定期的にpingメッセージを送信してWebSocket接続を維持（ノンブロッキング版）"""
        try:
            while not self.shutdown_event.is_set():
                # ノンブロッキング待機（0.1秒刻みで中断可能）
                for _ in range(200):  # 20秒間を0.1秒刻みで分割
                    if self.shutdown_event.is_set():
                        break
                    await asyncio.sleep(0.1)
                
                if self.shutdown_event.is_set():
                    break
                    
                try:
                    # ノンブロッキングping送信
                    ping_msg = {"method": "ping"}
                    # send_nowaitを使用してノンブロッキング送信
                    if hasattr(websocket, 'send_nowait'):
                        websocket.send_nowait(json.dumps(ping_msg))
                        logger.debug("💓 Sent ping to maintain WebSocket connection (non-blocking)")
                    else:
                        # フォールバック：短いタイムアウトで送信
                        await asyncio.wait_for(
                            websocket.send(json.dumps(ping_msg)), timeout=0.5
                        )
                        logger.debug("💓 Sent ping to maintain WebSocket connection (fallback)")
                except (AttributeError, websockets.exceptions.ConnectionClosed) as e:
                    logger.warning(f"⚠️ Non-blocking ping failed: {e}")
                    break
                except asyncio.TimeoutError:
                    logger.debug("💓 Ping timeout (continuing)")
                except Exception as e:
                    logger.warning(f"Failed to send ping: {e}")
                    break
        except asyncio.CancelledError:
            logger.debug("Ping task cancelled")


# MEXCClientとしてWebSocket版を使用
MEXCClient = MEXCWebSocketClient
