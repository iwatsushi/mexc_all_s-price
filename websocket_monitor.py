"""
WebSocket受信監視モード（受信頻度確認専用）
データ処理やQuestDB保存は行わず、純粋にWebSocket受信とping送信のみを実行
"""

import asyncio
import gzip
import json
import logging
import time
from datetime import datetime
from typing import Optional

import websockets

from config import Config

logger = logging.getLogger(__name__)


class WebSocketMonitor:
    """MEXC WebSocket受信監視（受信頻度測定専用）"""

    def __init__(self, config: Config):
        self.config = config
        self.ws_url = "wss://contract.mexc.com/edge"
        self.running = False

        # 統計データ
        self.stats = {
            "total_messages": 0,
            "ticker_messages": 0,
            "subscription_messages": 0,
            "pong_messages": 0,
            "ping_sent": 0,
            "start_time": None,
            "last_message_time": None,
            "message_intervals": [],
            "connection_attempts": 0,
            "reconnections": 0,
        }

        # ping管理
        self._last_ping_time = 0
        self._ping_interval = 15  # 15秒間隔

        # 受信間隔測定
        self._last_recv_time = None
        self._min_interval = float("inf")
        self._max_interval = 0.0
        self._interval_sum = 0.0

        # 再接続制御
        self._max_reconnect_attempts = 10
        self._reconnect_attempts = 0

    async def start_monitoring(self):
        """WebSocket監視開始"""
        logger.info("🔍 Starting WebSocket monitoring mode...")
        self.running = True
        self.stats["start_time"] = datetime.now()

        while self.running and self._reconnect_attempts < self._max_reconnect_attempts:
            try:
                await self._connect_and_monitor()
            except Exception as e:
                self._reconnect_attempts += 1
                self.stats["reconnections"] += 1
                logger.error(
                    f"WebSocket connection failed (attempt {self._reconnect_attempts}): {e}"
                )

                if self._reconnect_attempts < self._max_reconnect_attempts:
                    wait_time = min(2**self._reconnect_attempts, 30)  # 指数バックオフ
                    logger.info(f"Reconnecting in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max reconnection attempts reached")
                    break

        logger.info("WebSocket monitoring stopped")
        self._print_final_stats()

    async def _connect_and_monitor(self):
        """WebSocket接続と監視"""
        self.stats["connection_attempts"] += 1
        logger.info(f"🔗 Connecting to MEXC WebSocket: {self.ws_url}")

        async with websockets.connect(
            self.ws_url,
            ping_interval=None,  # 自前でping管理
            max_size=None,
            open_timeout=20,
            close_timeout=5,
        ) as websocket:
            logger.info("✅ WebSocket connected successfully")
            self._reconnect_attempts = 0  # 成功したらリセット

            # sub.tickersを購読
            subscribe_msg = {"method": "sub.tickers", "param": {}, "gzip": True}
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("📡 Subscribed to sub.tickers channel")

            # ping初期化
            self._last_ping_time = time.monotonic()
            logger.info("💓 Ping manager initialized (15s interval)")

            # 受信ループ開始
            await self._message_receive_loop(websocket)

    async def _message_receive_loop(self, websocket):
        """メッセージ受信ループ"""
        logger.info("🔄 Starting message receive loop...")
        last_stats_time = time.monotonic()
        stats_interval = 10.0  # 10秒ごとに統計表示

        while self.running:
            try:
                # メッセージ受信（1秒タイムアウト）
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                current_time = time.monotonic()

                # 受信統計更新
                self._update_receive_stats(current_time)

                # メッセージ処理（超軽量）
                self._process_message_minimal(raw_message)

                # ping送信チェック
                await self._check_and_send_ping(websocket, current_time)

                # 定期統計表示
                if current_time - last_stats_time >= stats_interval:
                    self._print_current_stats()
                    last_stats_time = current_time

            except asyncio.TimeoutError:
                # タイムアウト処理
                current_time = time.monotonic()

                # ping送信チェック
                await self._check_and_send_ping(websocket, current_time)

                # スタール検出
                if self.stats["last_message_time"]:
                    since_last = current_time - time.mktime(
                        self.stats["last_message_time"].timetuple()
                    )
                    if since_last > 30:  # 30秒でスタール警告
                        logger.warning(
                            f"⚠️ Message stall detected: {since_last:.1f}s since last message"
                        )
                        if since_last > 60:  # 60秒で再接続
                            raise websockets.exceptions.ConnectionClosed(None, None)

                continue

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                break
            except Exception as e:
                logger.error(f"Error in message receive loop: {e}")

    def _update_receive_stats(self, current_time: float):
        """受信統計更新"""
        self.stats["total_messages"] += 1
        self.stats["last_message_time"] = datetime.now()

        # 受信間隔計算
        if self._last_recv_time is not None:
            interval = current_time - self._last_recv_time
            self.stats["message_intervals"].append(interval)

            # 間隔統計更新
            self._min_interval = min(self._min_interval, interval)
            self._max_interval = max(self._max_interval, interval)
            self._interval_sum += interval

            # 直近100件のみ保持（メモリ効率）
            if len(self.stats["message_intervals"]) > 100:
                self.stats["message_intervals"].pop(0)

        self._last_recv_time = current_time

    def _process_message_minimal(self, raw_message):
        """最小限のメッセージ処理（統計のみ）"""
        try:
            # データ解凍
            if isinstance(raw_message, (bytes, bytearray)):
                decompressed = gzip.decompress(raw_message)
                data = json.loads(decompressed)
            else:
                data = json.loads(raw_message)

            # チャンネル別統計
            channel = data.get("channel", "unknown")
            if channel == "push.tickers":
                self.stats["ticker_messages"] += 1
                # ティッカー数をカウント（処理は行わない）
                tickers = data.get("data", [])
                if isinstance(tickers, list):
                    logger.debug(f"📈 Received {len(tickers)} tickers")
            elif channel == "rs.sub.tickers":
                self.stats["subscription_messages"] += 1
                logger.info(f"✅ Subscription confirmed: {data.get('data')}")
            elif channel == "pong":
                self.stats["pong_messages"] += 1
                logger.debug(f"💓 Pong received: {data.get('data')}")
            else:
                logger.debug(f"🔍 Unknown channel: {channel}")

        except Exception as e:
            logger.warning(f"Failed to process message: {e}")

    async def _check_and_send_ping(self, websocket, current_time: float):
        """ping送信チェック"""
        if current_time - self._last_ping_time >= self._ping_interval:
            try:
                ping_msg = {"method": "ping"}
                await websocket.send(json.dumps(ping_msg))
                self._last_ping_time = current_time
                self.stats["ping_sent"] += 1
                logger.debug("💓 Ping sent")
            except Exception as e:
                logger.warning(f"💓 Failed to send ping: {e}")

    def _print_current_stats(self):
        """現在の統計表示"""
        uptime = (datetime.now() - self.stats["start_time"]).total_seconds()

        # 受信レート計算
        message_rate = self.stats["total_messages"] / uptime if uptime > 0 else 0
        ticker_rate = self.stats["ticker_messages"] / uptime if uptime > 0 else 0

        # 受信間隔統計
        avg_interval = 0
        if len(self.stats["message_intervals"]) > 0:
            avg_interval = sum(self.stats["message_intervals"]) / len(
                self.stats["message_intervals"]
            )

        logger.info("📊 WebSocket Monitoring Stats:")
        logger.info(f"   ⏱️  Uptime: {uptime:.1f}s")
        logger.info(
            f"   📨 Total messages: {self.stats['total_messages']} ({message_rate:.2f}/s)"
        )
        logger.info(
            f"   📈 Ticker messages: {self.stats['ticker_messages']} ({ticker_rate:.2f}/s)"
        )
        logger.info(f"   💓 Pings sent: {self.stats['ping_sent']}")
        logger.info(f"   🔄 Reconnections: {self.stats['reconnections']}")

        if self.stats["message_intervals"]:
            logger.info(
                f"   📊 Message intervals: avg={avg_interval:.3f}s, min={self._min_interval:.3f}s, max={self._max_interval:.3f}s"
            )

    def _print_final_stats(self):
        """最終統計表示"""
        if not self.stats["start_time"]:
            return

        uptime = (datetime.now() - self.stats["start_time"]).total_seconds()

        logger.info("🏁 Final WebSocket Monitoring Statistics:")
        logger.info(f"   ⏱️  Total uptime: {uptime:.1f}s")
        logger.info(f"   📨 Total messages: {self.stats['total_messages']}")
        logger.info(f"   📈 Ticker messages: {self.stats['ticker_messages']}")
        logger.info(
            f"   ✅ Subscription messages: {self.stats['subscription_messages']}"
        )
        logger.info(f"   💓 Pong messages: {self.stats['pong_messages']}")
        logger.info(f"   💓 Pings sent: {self.stats['ping_sent']}")
        logger.info(f"   🔗 Connection attempts: {self.stats['connection_attempts']}")
        logger.info(f"   🔄 Reconnections: {self.stats['reconnections']}")

        if uptime > 0:
            logger.info(f"   📊 Average rates:")
            logger.info(
                f"      Total: {self.stats['total_messages'] / uptime:.2f} messages/s"
            )
            logger.info(
                f"      Tickers: {self.stats['ticker_messages'] / uptime:.2f} messages/s"
            )

        if self.stats["message_intervals"]:
            avg_interval = sum(self.stats["message_intervals"]) / len(
                self.stats["message_intervals"]
            )
            logger.info(
                f"   ⏱️  Message intervals: avg={avg_interval:.3f}s, min={self._min_interval:.3f}s, max={self._max_interval:.3f}s"
            )

    async def stop_monitoring(self):
        """監視停止"""
        logger.info("🛑 Stopping WebSocket monitoring...")
        self.running = False


async def run_websocket_monitor():
    """WebSocket監視実行"""
    config = Config()
    monitor = WebSocketMonitor(config)

    try:
        await monitor.start_monitoring()
    except KeyboardInterrupt:
        logger.info("Monitoring interrupted by user")
    finally:
        await monitor.stop_monitoring()


if __name__ == "__main__":
    # ログ設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    print("🔍 MEXC WebSocket Monitor - 受信頻度確認モード")
    print("Press Ctrl+C to stop monitoring")

    asyncio.run(run_websocket_monitor())
