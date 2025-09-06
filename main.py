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
        """WebSocket受信コールバック（超軽量版）"""
        try:
            self.stats["batches_received"] += 1
            
            # 🚀 ログを大幅削減（100回に1回のみ）
            if self.stats["batches_received"] % 100 == 0:
                logger.info(f"📨 Batch #{self.stats['batches_received']}: {len(tickers)} tickers")

            # 🚀 高速化: 非同期でデータ処理（並列処理）
            asyncio.create_task(
                self._process_ticker_batch_fast(tickers, self.stats["batches_received"])
            )

        except Exception as e:
            logger.error(f"Error in reception callback: {e}")

    async def _process_ticker_batch_fast(self, tickers: list, batch_id: int):
        """高速バッチ処理（超軽量版）"""
        try:
            # 🚀 時間計算を削除（CPU負荷軽減）
            self.stats["ticks_processed"] += len(tickers)

            # QuestDB保存のみ
            saved_count = await self._save_to_questdb_fast(tickers)
            self.stats["ticks_saved"] += saved_count

            # ログを完全削除（CPU負荷最小化）

        except Exception as e:
            logger.error(f"Batch error: {e}")

    # 古い関数群削除：_process_ticker_batch()、_save_to_questdb_batch()
    # → _process_ticker_batch_fast()、_save_to_questdb_fast() に統一

    async def _save_to_questdb_fast(self, tickers: list) -> int:
        """高速QuestDB保存（超軽量版）"""
        try:
            ilp_lines = []
            current_time_ns = int(time.time() * 1_000_000_000)

            for ticker_data in tickers:
                if not isinstance(ticker_data, dict):
                    continue

                symbol = ticker_data.get("symbol")
                price = ticker_data.get("lastPrice")

                if symbol and price:
                    try:
                        price_f = float(price)
                        volume_f = float(ticker_data.get("volume24", "0"))

                        # 🚀 タイムスタンプ処理を簡素化
                        mexc_timestamp = ticker_data.get("timestamp")
                        timestamp_ns = int(mexc_timestamp) * 1_000_000 if mexc_timestamp else current_time_ns

                        ilp_lines.append(f"tick_data,symbol={symbol} price={price_f},volume={volume_f} {timestamp_ns}")
                    except (ValueError, TypeError):
                        continue

            return self.questdb_client.save_ilp_lines(ilp_lines) if ilp_lines else 0
            
        except Exception as e:
            logger.error(f"QuestDB save error: {e}")
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
                await asyncio.sleep(120)  # 120秒間隔に変更（CPU負荷最小化）
                if self.running:
                    self._show_stats()
            except Exception as e:
                logger.error(f"Error in stats timer: {e}")

    def _show_stats(self):
        """統計情報表示（軽量版）"""
        try:
            uptime = int((datetime.now() - self.stats["start_time"]).total_seconds())
            
            logger.info(f"📊 Stats: {uptime}s | Batches: {self.stats['batches_received']} | Saved: {self.stats['ticks_saved']}")
            
        except Exception as e:
            logger.error(f"Stats error: {e}")

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
