"""
MEXCデータ収集クライアント（WebSocket専用）
"""

import asyncio
import gzip
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional

import websockets

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """ティックデータ"""

    symbol: str
    price: float
    timestamp: int  # ナノ秒単位のUNIXタイムスタンプ（数値型に統一）
    volume: float = 0.0


# PositionData, OrderResult classes removed - データ収集専用


class MEXCWebSocketClient:
    """MEXC WebSocket価格データ収集クライアント（データ収集専用）"""

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

        # デバッグフラグ（CPU負荷軽減のため無効化）
        self._debug_interval_stats = False

        # ping管理（受信ループ内で実行）
        self._last_ping_time = 0
        self._ping_interval = config.mexc_ping_interval  # 設定ファイルから読み込み

        # pong重複検出
        self._last_pong_timestamp = None

    async def connect(self) -> bool:
        """WebSocket接続開始"""
        try:
            logger.info(f"🔗 MEXC WebSocket接続開始: {self.ws_url}")

            self.running = True
            # WebSocketタスクを開始
            self._ws_task = asyncio.create_task(self._websocket_loop())
            logger.info("🔄 WebSocketタスク開始")
            return True

        except Exception as e:
            logger.error(f"MEXC WebSocket開始失敗: {e}")
            return False

    async def disconnect(self):
        """WebSocket接続停止"""
        logger.info("🛑 MEXC WebSocket停止中...")

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
            ping_interval=None,  # WebSocketレベルのpingは無効化
            max_size=None,  # フレームサイズ制限を解除
            open_timeout=10,  # 接続タイムアウトを短縮
            close_timeout=2,  # クローズタイムアウトを短縮
            # 高速化のための追加設定
            compression=None,  # 圧縮を無効化（レイテンシー優先）
            max_queue=1000,  # 受信キューを拡大
        ) as websocket:
            self._websocket = websocket
            self._reconnect_attempts = 0  # 成功したらリセット

            logger.info("WebSocket connected, subscribing to tickers...")

            # 🚀 高速化: バルク購読のみ（個別購読はMEXC先物でサポートされていない）
            subscribe_msg = {"method": "sub.tickers", "param": {}, "gzip": True}
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("✅ Subscribed to sub.tickers channel (gzip compressed)")
            logger.info("🚀 Fast mode: bulk subscription optimized for high throughput")

            # ping初期化（受信ループ内で管理）
            self._last_ping_time = time.monotonic()
            logger.info(
                f"💓 MEXC ping initialized ({self._ping_interval}s interval, unified)"
            )

            # メッセージ受信ループ（デバッグスクリプトと同じタイムアウト方式を採用）
            last_recv = time.monotonic()  # デバッグスクリプトと同じ単調時間を使用
            message_count = 0

            # 📊 受信間隔測定用（CPU負荷軽減のため無効化）

            logger.info("🔄 Starting WebSocket message receive loop...")

            logger.debug(
                f"🔍 DEBUG: shutdown_event.is_set() = {self.shutdown_event.is_set()}"
            )

            while not self.shutdown_event.is_set():
                logger.debug("🔄 Entered main receive loop")
                try:
                    # タイムアウト付きでメッセージを受信（高速化のため短縮）
                    raw_message = await asyncio.wait_for(websocket.recv(), timeout=0.1)
                    rx_time_wall = time.time()  # 📊 受信直後の時刻（wall clock time）
                    rx_time_mono = time.monotonic()  # monotonic time（統計用）
                    last_recv = rx_time_mono
                    message_count += 1

                    # 🚀 ChatGPT5提案: 受信直後は生データをキューに投入のみ

                    # 📊 受信間隔測定（CPU負荷軽減のため無効化）

                    # 🚀 超軽量処理：生データを直接解凍してコールバック呼び出し
                    if self.batch_callback:
                        self._process_ticker_batch_safe(raw_message, rx_time_wall)
                    # バッチコールバックがない場合は静かにドロップ

                    # 💓 ping処理は_process_ticker_batch_safe内で統一実行

                except asyncio.TimeoutError:
                    # タイムアウト（1秒間メッセージなし）- スタール検出（デバッグスクリプトと同じ方式）
                    since = time.monotonic() - last_recv
                    if since > 15:  # 15秒でスタール警告（少し延長）
                        logger.warning(
                            f"⚠️ MEXC WebSocket STALL: {since:.1f}秒間メッセージを受信していません"
                        )
                        # 再接続をトリガーするために例外を発生
                        raise websockets.exceptions.ConnectionClosed(None, None)
                    # 4秒以上のログはCPU負荷軽減のため削除

                    # ping専用プロセスが接続維持を担当するため、ここでは何もしない

                    continue  # タイムアウト時は継続

                except json.JSONDecodeError as e:
                    logger.warning(f"Non-JSON message received: {e}")
                except websockets.exceptions.ConnectionClosed as e:
                    logger.warning(f"WebSocket connection closed: {e}")
                    # 接続切断は正常な再接続処理で対処
                    break
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")

            # inline ping管理のため特別なクリーンアップ不要
            logger.info("💓 MEXC inline ping stopped")

    async def send_ping(self):
        """外部からping送信を要求するためのメソッド"""
        try:
            if hasattr(self, "_websocket") and self._websocket:
                ping_msg = {"method": "ping"}
                ping_json = json.dumps(ping_msg)
                await self._websocket.send(ping_json)
                logger.debug(f"💓 MEXC ping sent (external request): {ping_json}")
                return True
            else:
                logger.warning("💓 WebSocket not connected, cannot send ping")
                return False
        except Exception as e:
            logger.warning(f"💓 Failed to send external ping: {e}")
            return False

    def _process_ticker_batch_safe(self, raw_message, receive_time=None):
        """WebSocket受信を保護する超高速バッチティッカーデータ処理（生データ解凍統合版）"""
        if not self.batch_callback:
            logger.warning("No batch callback set!")
            return

        # 💓 統一ping送信チェック（全モード対応）
        current_time = time.monotonic()
        time_since_last_ping = current_time - self._last_ping_time

        if time_since_last_ping >= self._ping_interval:
            try:
                # WebSocket参照が利用できる場合のみping送信
                if hasattr(self, "_websocket") and self._websocket:
                    ping_msg = {"method": "ping"}
                    ping_json = json.dumps(ping_msg)
                    # 非同期でping送信（ブロッキングを避けるため）
                    asyncio.create_task(self._websocket.send(ping_json))
                    self._last_ping_time = current_time
                    logger.info(
                        f"💓 MEXC ping sent (unified, after {time_since_last_ping:.1f}s): {ping_json}"
                    )
                else:
                    logger.debug(
                        f"💓 WebSocket reference not available for ping (after {time_since_last_ping:.1f}s)"
                    )
            except Exception as e:
                logger.warning(f"💓 Failed to send ping: {e}")

        try:
            # 🚀 生データ解凍処理
            try:
                if isinstance(raw_message, (bytes, bytearray)):
                    # gzip圧縮されたデータを解凍
                    decompressed = gzip.decompress(raw_message)
                    data = json.loads(decompressed)
                    logger.debug(
                        f"📦 Decompressed {len(raw_message)} → {len(decompressed)} bytes"
                    )
                else:
                    # 非圧縮データ
                    data = json.loads(raw_message)
            except (gzip.BadGzipFile, json.JSONDecodeError) as e:
                logger.warning(f"Failed to decode message: {e}")
                return

            # チャンネル判定：ティッカーデータのみ処理
            if data.get("channel") == "push.tickers" and "data" in data:
                tickers = data["data"]
            elif data.get("channel") == "push.ticker" and "data" in data:
                # 個別銘柄チャンネル（高頻度更新）
                single_ticker = data["data"]
                tickers = [single_ticker]  # 単一要素のリストとして処理
                logger.debug(
                    f"⚡ High-freq update: {single_ticker.get('symbol', 'unknown')}"
                )
            elif data.get("channel") == "rs.sub.tickers":
                return  # サブスクリプション確認
            elif data.get("channel") == "rs.sub.ticker":
                return  # 個別サブスクリプション確認
            elif data.get("channel") == "pong":
                self._last_pong_timestamp = data.get("data")
                return  # pong処理
            else:
                return  # 未処理チャンネル

            # 🚀 ティッカーデータのバッチ処理
            if isinstance(tickers, list) and len(tickers) > 0:
                self.batch_callback(tickers, receive_time)

        except Exception as e:
            # エラーログは出すが、WebSocket受信は継続
            logger.error(f"Error in batch callback: {e}")

    def _process_ticker_data_safe(self, tickers):
        """WebSocket受信を保護する高速ティッカーデータ処理（互換性維持）"""
        if not self.tick_callback:
            logger.warning("No tick callback set!")
            return

        # 📊 統計のみ（瞬時）
        processed_count = 0

        # バッチ受信時刻をフォールバック用に取得（ナノ秒単位）
        batch_receive_time_ns = int(time.time() * 1_000_000_000)

        # 🎯 最小限の処理：TickDataオブジェクト作成と非同期キューイング
        for ticker in tickers:
            if isinstance(ticker, dict):
                symbol = ticker.get("symbol", "")
                price = float(ticker.get("lastPrice", 0))
                volume = float(ticker.get("volume24", 0))

                if symbol and price > 0:
                    # MEXCの実際のタイムスタンプを使用（ミリ秒単位）
                    mexc_timestamp = ticker.get("timestamp")
                    if mexc_timestamp is not None and isinstance(
                        mexc_timestamp, (int, float)
                    ):
                        try:
                            # 型安全性を強化：必ずfloatに変換してから計算
                            timestamp_ms = float(mexc_timestamp)
                            tick_timestamp_ns = int(
                                timestamp_ms * 1_000_000
                            )  # ミリ秒→ナノ秒
                        except (ValueError, OverflowError, OSError, TypeError) as e:
                            logger.warning(
                                f"Invalid timestamp for {symbol}: {mexc_timestamp} (type: {type(mexc_timestamp)}) - {e}"
                            )
                            # フォールバック：バッチ受信時刻を使用
                            tick_timestamp_ns = batch_receive_time_ns
                    else:
                        # フォールバック：バッチ受信時刻を使用（datetime.now()の代わり）
                        tick_timestamp_ns = batch_receive_time_ns

                    tick = TickData(
                        symbol=symbol,
                        price=price,
                        timestamp=tick_timestamp_ns,  # ナノ秒単位の数値タイムスタンプ
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

        # 処理完了（ログはパフォーマンス優先のため削除）

    def _process_ticker_data(self, tickers):
        """従来のティッカーデータ処理（互換性維持）"""
        # 新しい安全な処理に移譲
        self._process_ticker_data_safe(tickers)


# MEXCClientとしてWebSocket版を使用
MEXCClient = MEXCWebSocketClient
