"""
MEXC WebSocket専用プロセス - WebSocket受信とPing送信を独立して処理
"""

import asyncio
import gzip
import json
import logging
import multiprocessing
import signal
import sys
import time
from typing import Optional

import websockets
from loguru import logger

from config import Config


class MEXCWebSocketProcess:
    """MEXC WebSocket専用プロセス - WebSocket接続維持とPing送信を担当"""

    def __init__(
        self,
        config: Config,
        data_queue: multiprocessing.Queue,
        control_queue: multiprocessing.Queue,
    ):
        """
        初期化

        Args:
            config: 設定オブジェクト
            data_queue: データ送信用キュー（メインプロセスへ）
            control_queue: 制御信号受信用キュー（メインプロセスから）
        """
        self.config = config
        self.data_queue = data_queue
        self.control_queue = control_queue

        # WebSocket接続管理
        self._websocket: Optional[websockets.WebSocketServerProtocol] = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = config.mexc_max_reconnect_attempts
        self._reconnect_interval = config.mexc_reconnect_interval

        # Ping管理
        self._last_ping_time = 0
        self._ping_interval = config.mexc_ping_interval

        # Pong重複検出
        self._last_pong_timestamp = None

        # プロセス状態管理
        self._shutdown_event = asyncio.Event()
        self._running = False

        # 統計
        self.stats = {
            "messages_received": 0,
            "pings_sent": 0,
            "pongs_received": 0,
            "reconnections": 0,
            "start_time": None,
        }

    def run(self):
        """プロセスメインループ"""
        logger.info("🚀 MEXC WebSocketプロセス開始...")

        # シグナルハンドラー設定
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        try:
            # asyncio イベントループ開始
            asyncio.run(self._main_loop())
        except KeyboardInterrupt:
            logger.info("🛑 MEXC WebSocket Process interrupted by user")
        except Exception as e:
            logger.error(f"💥 MEXC WebSocket Process error: {e}")
        finally:
            logger.info("🔚 MEXC WebSocket Process terminated")

    def _signal_handler(self, signum, frame):
        """シグナルハンドラー"""
        logger.info(f"📡 MEXC WebSocket Process received signal {signum}")
        asyncio.create_task(self._shutdown())

    async def _main_loop(self):
        """メインイベントループ"""
        self.stats["start_time"] = time.time()
        self._running = True

        logger.info("💓 MEXC WebSocketプロセス初期化完了")

        # WebSocket接続とコントロール監視を並行実行
        tasks = []

        # WebSocket接続タスク
        websocket_task = asyncio.create_task(self._websocket_loop())
        tasks.append(websocket_task)

        # コントロール監視タスク
        control_task = asyncio.create_task(self._control_monitor())
        tasks.append(control_task)

        try:
            # いずれかのタスクが完了するまで待機
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            # 残りのタスクをキャンセル
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except Exception as e:
            logger.error(f"💥 Main loop error: {e}")
        finally:
            await self._shutdown()

    async def _control_monitor(self):
        """制御信号監視"""
        while self._running and not self._shutdown_event.is_set():
            try:
                # 非ブロッキングでコントロールキューをチェック
                if not self.control_queue.empty():
                    command = self.control_queue.get_nowait()
                    logger.info(f"📨 Control command received: {command}")

                    if command == "shutdown":
                        await self._shutdown()
                        break
                    elif command == "stats":
                        await self._send_stats()

                await asyncio.sleep(0.1)  # CPU使用率制御

            except Exception as e:
                logger.warning(f"⚠️ Control monitor error: {e}")
                await asyncio.sleep(1.0)

    async def _websocket_loop(self):
        """WebSocket接続メインループ"""
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._connect_and_run()
            except Exception as e:
                logger.error(f"💥 WebSocket loop error: {e}")

                if self._reconnect_attempts < self._max_reconnect_attempts:
                    self._reconnect_attempts += 1
                    self.stats["reconnections"] += 1
                    logger.info(
                        f"🔄 Reconnecting... (attempt {self._reconnect_attempts})"
                    )
                    await asyncio.sleep(self._reconnect_interval)
                else:
                    logger.error("❌ Max reconnection attempts reached")
                    await self._shutdown()
                    break

    async def _connect_and_run(self):
        """WebSocket接続と受信処理"""
        ws_url = self.config.mexc_ws_url
        logger.info(f"🔗 MEXC WebSocketに接続中: {ws_url}")

        try:
            async with websockets.connect(ws_url) as websocket:
                self._websocket = websocket
                self._reconnect_attempts = 0

                logger.info("✅ MEXC WebSocket接続成功")

                # チャンネル購読
                await self._subscribe_channels()

                # Ping初期化
                self._last_ping_time = time.monotonic()
                logger.info(f"💓 MEXC ping初期化完了 ({self._ping_interval}秒間隔)")

                # メッセージ受信ループ
                await self._message_loop(websocket)

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"🔌 WebSocket connection closed: {e}")
            raise
        except Exception as e:
            logger.error(f"💥 WebSocket connection error: {e}")
            raise
        finally:
            self._websocket = None

    async def _subscribe_channels(self):
        """チャンネル購読"""
        if not self._websocket:
            return

        # シンプルなティッカー購読（従来の方式）
        subscribe_msg = {"method": "sub.tickers"}
        subscribe_json = json.dumps(subscribe_msg)

        try:
            await self._websocket.send(subscribe_json)
            logger.info(f"📡 MEXCティッカーチャンネル購読: {subscribe_json}")
        except Exception as e:
            logger.error(f"💥 Failed to subscribe: {e}")
            raise

    async def _message_loop(self, websocket):
        """メッセージ受信ループ"""
        message_count = 0
        last_recv = time.monotonic()

        logger.info("🔄 WebSocketメッセージ受信ループ開始...")

        while self._running and not self._shutdown_event.is_set():
            try:
                # メッセージ受信（タイムアウト付き）
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                rx_time = time.monotonic()
                last_recv = rx_time
                message_count += 1
                self.stats["messages_received"] += 1

                # メッセージ処理
                await self._process_message(raw_message)

                # Ping送信チェック
                await self._check_ping(rx_time)

            except asyncio.TimeoutError:
                # タイムアウト処理
                since = time.monotonic() - last_recv

                # Ping送信チェック（タイムアウト時も実行） - 先に実行
                await self._check_ping(time.monotonic())

                if since > 30:  # 30秒でスタール警告（pingを考慮して延長）
                    logger.warning(
                        f"⚠️ MEXC WebSocket STALL: {since:.1f}s since last message"
                    )
                    # 再接続をトリガー
                    raise websockets.exceptions.ConnectionClosed(None, None)

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(
                    f"🔌 WebSocket connection closed during message loop: {e}"
                )
                raise
            except Exception as e:
                logger.error(f"💥 Message processing error: {e}")
                # エラーが発生してもループを継続
                await asyncio.sleep(0.1)

    async def _process_message(self, raw_message):
        """メッセージ処理"""
        try:
            # データ解凍・デコード
            if isinstance(raw_message, (bytes, bytearray)):
                decompressed = gzip.decompress(raw_message)
                data = json.loads(decompressed)
            else:
                data = json.loads(raw_message)

            channel = data.get("channel", "unknown")

            # 📊 受信メッセージの詳細をログ出力（デバッグ用）
            logger.debug(f"🔍 Received message - Channel: {channel}, Data size: {len(str(data))}")

            if channel == "pong":
                await self._handle_pong(data)
            elif channel == "push.tickers":
                await self._handle_tickers(data)
            elif channel == "rs.sub.tickers":
                logger.info(f"✅ 購読確認: {data.get('data')}")
            else:
                logger.info(f"🔍 Unhandled channel: {channel}, Full data: {data}")

        except Exception as e:
            logger.warning(f"⚠️ Failed to process message: {e}")

    async def _handle_pong(self, data):
        """Pong処理"""
        pong_data = data.get("data", "unknown")

        # 重複pongチェック
        if self._last_pong_timestamp == pong_data:
            logger.debug(f"💓 Duplicate pong ignored: {pong_data}")
            return

        self._last_pong_timestamp = pong_data
        self.stats["pongs_received"] += 1
        logger.info(f"💓 サーバーからpong受信: {pong_data}")

    async def _handle_tickers(self, data):
        """ティッカーデータ処理"""
        tickers = data.get("data", [])
        if isinstance(tickers, list) and len(tickers) > 0:
            # データをメインプロセスに送信
            try:
                # 非ブロッキングでキューに追加
                self.data_queue.put_nowait(
                    {"type": "tickers", "data": tickers, "timestamp": time.time()}
                )
                logger.debug(f"📤 Sent {len(tickers)} tickers to main process")
            except Exception as e:
                logger.warning(f"⚠️ Failed to send data to main process: {e}")

    async def _check_ping(self, current_time):
        """Ping送信チェック"""
        time_since_last_ping = current_time - self._last_ping_time

        if time_since_last_ping >= self._ping_interval:
            await self._send_ping()
            self._last_ping_time = current_time

    async def _send_ping(self):
        """Ping送信"""
        if not self._websocket:
            return

        try:
            # MEXCの標準Ping形式
            ping_msg = {"method": "ping"}
            ping_json = json.dumps(ping_msg)
            await self._websocket.send(ping_json)

            self.stats["pings_sent"] += 1
            logger.info(f"💓 MEXC ping sent: {ping_json}")

        except Exception as e:
            logger.warning(f"💓 Failed to send ping: {e}")

    async def _send_stats(self):
        """統計情報送信"""
        try:
            uptime = (
                time.time() - self.stats["start_time"]
                if self.stats["start_time"]
                else 0
            )
            stats_data = {
                "type": "stats",
                "data": {
                    **self.stats,
                    "uptime": uptime,
                    "process_id": multiprocessing.current_process().pid,
                },
                "timestamp": time.time(),
            }
            self.data_queue.put_nowait(stats_data)
            logger.debug("📊 Stats sent to main process")
        except Exception as e:
            logger.warning(f"⚠️ Failed to send stats: {e}")

    async def _shutdown(self):
        """シャットダウン処理"""
        if not self._running:
            return

        logger.info("🛑 MEXC WebSocket Process shutting down...")
        self._running = False
        self._shutdown_event.set()

        # WebSocket接続クローズ
        if self._websocket:
            try:
                await self._websocket.close()
            except Exception as e:
                logger.warning(f"⚠️ Error closing WebSocket: {e}")

        # 最終統計送信
        await self._send_stats()

        logger.info("✅ MEXC WebSocket Process shutdown completed")


def mexc_websocket_worker(
    config_dict: dict,
    data_queue: multiprocessing.Queue,
    control_queue: multiprocessing.Queue,
):
    """WebSocketプロセスのワーカー関数"""
    # 設定オブジェクト再構築
    config = Config()
    config._config = config_dict

    # WebSocketプロセス実行
    process = MEXCWebSocketProcess(config, data_queue, control_queue)
    process.run()
