"""
MEXC Data Collector - MEXCからの価格データをQuestDBに記録
"""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict

# ログ設定
from loguru import logger as loguru_logger

# グローバルロガー
logger = loguru_logger

# 自作モジュール
from config import Config
from mexc_client import MEXCClient, TickData
from questdb_client import QuestDBClient


class MEXCDataCollector:
    """MEXCデータ収集アプリケーション"""

    def __init__(self, config_path: str = "config.yml"):
        """
        初期化

        Args:
            config_path: 設定ファイルパス
        """
        # 設定読み込み
        self.config = Config(config_path)

        # ログ設定
        self._setup_logging()

        # コンポーネント
        self.mexc_client = None
        self.questdb_client = None

        # 実行制御
        self.running = False
        self.shutdown_event = asyncio.Event()

        # 統計
        self.stats = {
            "start_time": datetime.now(),
            "ticks_processed": 0,
            "ticks_saved": 0,
            "batches_received": 0,
        }

        logger.info("🎆 MEXC Data Collector初期化完了")

    def _setup_logging(self):
        """ログ設定"""
        # 既存のログハンドラーを削除
        loguru_logger.remove()

        # コンソール出力
        loguru_logger.add(
            sys.stderr,
            level=self.config.log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>",
        )

        # ファイル出力
        loguru_logger.add(
            self.config.log_file,
            level=self.config.log_level,
            rotation=f"{self.config.get('logging.max_size_mb', 10)} MB",
            retention=self.config.get("logging.backup_count", 5),
            encoding="utf-8",
        )

        # 標準loggingモジュールをloguru にリダイレクト
        logging.basicConfig(handlers=[], level=logging.INFO)
        logging.getLogger().handlers.clear()

        class InterceptHandler(logging.Handler):
            def emit(self, record):
                try:
                    level = loguru_logger.level(record.levelname).name
                except ValueError:
                    level = record.levelno

                frame, depth = logging.currentframe(), 2
                while frame.f_code.co_filename == logging.__file__:
                    frame = frame.f_back
                    depth += 1

                loguru_logger.opt(depth=depth, exception=record.exc_info).log(
                    level, record.getMessage()
                )

        logging.getLogger().addHandler(InterceptHandler())

    async def initialize(self):
        """コンポーネント初期化"""
        logger.info("🔧 コンポーネント初期化中...")

        try:
            # MEXCクライアント
            self.mexc_client = MEXCClient(self.config)
            logger.info("MEXCクライアント作成完了")

            # QuestDB クライアント
            self.questdb_client = QuestDBClient(self.config)
            logger.info("QuestDBクライアント作成完了")

            # MEXC WebSocket 接続
            if not await self.mexc_client.start():
                raise Exception("MEXC WebSocket接続に失敗しました")

            # ティッカーバッチコールバック設定
            self.mexc_client.set_batch_callback(self._on_ticker_batch_received)

            # 全銘柄購読
            if not await self.mexc_client.subscribe_all_tickers():
                raise Exception("Failed to subscribe to all tickers")

            logger.info("✅ 全コンポーネントの初期化成功")

        except Exception as e:
            logger.error(f"❌ コンポーネント初期化失敗: {e}")
            await self.shutdown()
            raise

    def _on_ticker_batch_received(self, tickers: list, ws_receive_time=None):
        """WebSocket受信コールバック"""
        try:
            self.stats["batches_received"] += 1
            current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            # logger.debug(tickers)

            # 🕒 レイテンシ計算（最新のティッカータイムスタンプと比較）
            latency_info = ""
            if tickers and len(tickers) > 0:
                # 🚀 最新のタイムスタンプを検索（最も新しいデータ時刻）
                latest_timestamp = 0
                valid_timestamps = 0
                for ticker in tickers:
                    if isinstance(ticker, dict):
                        ts = ticker.get("timestamp")
                        if ts:
                            latest_timestamp = max(latest_timestamp, int(ts))
                            valid_timestamps += 1

                if latest_timestamp > 0 and valid_timestamps > 0:
                    if ws_receive_time is not None:
                        # 🚀 WebSocket受信直後のwall clock timeを直接使用
                        receive_time_ms = int(ws_receive_time * 1000)
                        time_source = "direct"
                    else:
                        # フォールバック：現在時刻を使用
                        receive_time_ms = int(time.time() * 1000)
                        time_source = "callback"

                    # 🔍 デバッグ: タイムスタンプの値を確認
                    logger.debug(f"🔍 Latest MEXC timestamp: {latest_timestamp}")
                    logger.debug(
                        f"🔍 Receive time: {ws_receive_time} -> {receive_time_ms}ms"
                    )
                    logger.debug(
                        f"🔍 Valid timestamps in batch: {valid_timestamps}/{len(tickers)}"
                    )

                    latency_ms = receive_time_ms - latest_timestamp
                    latency_info = (
                        f" | ⏱️ Latency: {latency_ms}ms ({time_source}, latest)"
                    )

            logger.info(
                f"📨 [{current_time}] Batch #{self.stats['batches_received']}: {len(tickers)} tickers received{latency_info}"
            )

            # 🚀 高速化: 非同期でデータ処理（並列処理）
            asyncio.create_task(
                self._process_ticker_batch_fast(tickers, self.stats["batches_received"])
            )

        except Exception as e:
            logger.error(f"Error in reception callback: {e}")

    async def _process_ticker_batch_fast(self, tickers: list, batch_id: int):
        """高速バッチ処理（QuestDBのみ）"""
        try:
            start_time = time.time()

            # 🚀 即座に統計更新（レスポンス優先）
            self.stats["ticks_processed"] += len(tickers)

            # QuestDB保存のみ（data_managerは不要のため削除）
            saved_count = await self._save_to_questdb_fast(tickers, start_time)

            # 統計更新
            self.stats["ticks_saved"] += saved_count

            duration = time.time() - start_time

            # ログ頻度を大幅削減（パフォーマンス優先）
            if batch_id % 20 == 0:  # 20回に1回のみログ
                logger.info(
                    f"⚡ Fast batch #{batch_id}: {saved_count} saved in {duration:.3f}s"
                )

        except Exception as e:
            logger.error(f"Error in fast ticker batch: {e}")

    # 古い関数群削除：_process_ticker_batch()、_save_to_questdb_batch()
    # → _process_ticker_batch_fast()、_save_to_questdb_fast() に統一

    async def _save_to_questdb_fast(self, tickers: list, batch_timestamp: float) -> int:
        """高速QuestDB保存"""
        try:
            ilp_lines = []
            batch_ts_ns = int(batch_timestamp * 1_000_000_000)

            for ticker_data in tickers:
                if not isinstance(ticker_data, dict):
                    continue

                symbol = ticker_data.get("symbol", "")
                price = ticker_data.get("lastPrice")

                if symbol and price:
                    try:
                        price_f = float(price)
                        volume_f = float(ticker_data.get("volume24", "0"))

                        # 🕒 MEXCのタイムスタンプを使用（ミリ秒→ナノ秒に変換）
                        mexc_timestamp = ticker_data.get("timestamp")
                        if mexc_timestamp:
                            timestamp_ns = (
                                int(mexc_timestamp) * 1_000_000
                            )  # ミリ秒→ナノ秒
                        else:
                            timestamp_ns = batch_ts_ns  # フォールバック

                        line = f"tick_data,symbol={symbol} price={price_f},volume={volume_f} {timestamp_ns}"
                        ilp_lines.append(line)
                    except (ValueError, TypeError):
                        continue

            if ilp_lines:
                saved_count = self.questdb_client.save_ilp_lines(ilp_lines)
                return saved_count

            return 0
        except Exception as e:
            logger.error(f"Error in fast QuestDB save: {e}")
            return 0

    async def run(self):
        """メインループ実行"""
        logger.info("🚀 MEXC Data Collector開始...")

        try:
            # 初期化
            await self.initialize()

            # シグナルハンドラー設定
            self._setup_signal_handlers()

            self.running = True
            logger.info(
                "✅ MEXC Data Collector稼働中。停止は Ctrl+C を押してください。"
            )

            # 統計表示タイマー開始（頻度削減）
            asyncio.create_task(self._stats_timer())

            # メインループ（効率的な待機） - シャットダウンイベントを待機
            await self.shutdown_event.wait()

        except Exception as e:
            logger.error(f"Critical error: {e}")
        finally:
            await self.shutdown()

    async def _stats_timer(self):
        """統計表示タイマー"""
        while self.running:
            try:
                await asyncio.sleep(60)  # 60秒間隔に変更（CPU負荷軽減）
                if self.running:
                    self._show_stats()
            except Exception as e:
                logger.error(f"Error in stats timer: {e}")

    def _show_stats(self):
        """統計情報表示"""
        try:
            uptime = (datetime.now() - self.stats["start_time"]).total_seconds()

            logger.info("📊 === MEXC Data Collector Statistics ===")
            logger.info(f"⏱️  稼働時間: {uptime:.1f}秒 ({uptime/60:.1f}分)")
            logger.info(f"📨 受信バッチ数: {self.stats['batches_received']}")
            logger.info(f"📈 処理ティック数: {self.stats['ticks_processed']}")
            logger.info(f"💾 QuestDB保存数: {self.stats['ticks_saved']}")

            # 処理レート
            if uptime > 0:
                batch_rate = self.stats["batches_received"] / uptime
                tick_rate = self.stats["ticks_processed"] / uptime
                logger.info(
                    f"📊 処理レート: {batch_rate:.2f} batches/s, {tick_rate:.2f} ticks/s"
                )

            logger.info("=" * 50)

        except Exception as e:
            logger.error(f"Error showing stats: {e}")

    def _setup_signal_handlers(self):
        """シグナルハンドラー設定"""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.running = False
            # シャットダウンイベントをセット（非同期なのでloop経由）
            asyncio.create_task(self._set_shutdown_event())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def _set_shutdown_event(self):
        """シャットダウンイベントをセット"""
        self.shutdown_event.set()

    async def shutdown(self):
        """シャットダウン処理"""
        logger.info("Shutting down MEXC Data Collector...")

        self.running = False

        try:
            # 最終統計表示
            self._show_stats()

            # 各コンポーネントのシャットダウン
            if self.mexc_client:
                logger.info("Shutting down MEXC client...")
                await self.mexc_client.stop()

            if self.questdb_client:
                logger.info("Shutting down QuestDB client...")
                self.questdb_client.shutdown()


            logger.info("MEXC Data Collector shutdown completed")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

    def get_status(self) -> Dict[str, Any]:
        """現在の状態を取得"""
        try:
            uptime = (datetime.now() - self.stats["start_time"]).total_seconds()

            return {
                "running": self.running,
                "uptime_hours": uptime / 3600,
                "stats": self.stats,
                "questdb": (
                    self.questdb_client.get_stats() if self.questdb_client else {}
                ),
            }
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}


async def main():
    """メイン関数"""
    import sys

    # ヘルプ表示
    if "--help" in sys.argv or "-h" in sys.argv:
        print("🚀 MEXC Data Collector - MEXCからの価格データをQuestDBに記録")
        print("")
        print("Usage:")
        print("  python main.py                    データ収集開始")
        print("  python main.py --help             このヘルプを表示（-h）")
        return

    try:
        # MEXC Data Collector インスタンス作成
        app = MEXCDataCollector()

        # 実行
        await app.run()

    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # イベントループで実行
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Application interrupted")
    except Exception as e:
        print(f"Application failed: {e}")
        sys.exit(1)
