"""
Trade Mini - メインアプリケーション
"""

import asyncio
import logging
import signal
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict

# ログ設定
from loguru import logger as loguru_logger

# グローバルロガー
logger = loguru_logger

# 自作モジュール
from config import Config
from data_manager import DataManager
from mexc_client import MEXCClient, TickData
from bybit_client import BybitClient
from symbol_mapper import SymbolMapper
from position_manager import PositionManager
from questdb_client import QuestDBClient, QuestDBTradeRecordManager
from strategy import SignalType, TradingStrategy


class TradeMini:
    """Trade Mini メインアプリケーション"""

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
        self.bybit_client = None
        self.symbol_mapper = None
        self.data_manager = None
        self.strategy = None
        self.position_manager = None
        self.questdb_client = None
        self.trade_record_manager = None

        # 実行制御
        self.running = False
        self.shutdown_event = threading.Event()

        # 統計
        self.stats = {
            "start_time": datetime.now(),
            "ticks_processed": 0,
            "signals_generated": 0,
            "trades_executed": 0,
            "uptime": 0.0,
        }

        # 統計表示タイマー
        self.stats_timer = None

        logger.info("Trade Mini initialized")

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
            retention=self.config.get('logging.backup_count', 5),
            encoding="utf-8",
        )

        # 標準loggingモジュールをloguru にリダイレクト
        logging.basicConfig(handlers=[], level=logging.DEBUG)
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
        logger.info("Initializing components...")

        try:
            # MEXC クライアント（ティックデータ取得用）
            self.mexc_client = MEXCClient(self.config)
            logger.info("MEXC client created")

            # Bybit クライアント（注文・決済用）
            self.bybit_client = BybitClient(
                self.config.bybit_api_key,
                self.config.bybit_api_secret,
                self.config.bybit_testnet,
                self.config.bybit_api_url
            )
            logger.info("Bybit client created")

            # 銘柄マッピング管理
            self.symbol_mapper = SymbolMapper(self.bybit_client)
            logger.info("Symbol mapper created")

            # データ管理
            self.data_manager = DataManager(self.config)
            logger.info("Data manager created")

            # 取引戦略
            self.strategy = TradingStrategy(self.config, self.data_manager)
            logger.info("Trading strategy created")

            # ポジション管理
            self.position_manager = PositionManager(
                self.config, self.mexc_client, self.bybit_client, self.symbol_mapper
            )
            logger.info("Position manager created")

            # QuestDB クライアント
            self.questdb_client = QuestDBClient(self.config)
            self.trade_record_manager = QuestDBTradeRecordManager(self.questdb_client)
            logger.info("QuestDB client created")

            # MEXC WebSocket 接続
            if not await self.mexc_client.start():
                raise Exception("Failed to connect to MEXC WebSocket")

            # ティックデータコールバック設定
            self.mexc_client.set_tick_callback(self._on_tick_received)

            # 全銘柄購読
            if not await self.mexc_client.subscribe_all_tickers():
                raise Exception("Failed to subscribe to all tickers")

            # 統計表示タイマー開始
            self._start_stats_timer()

            logger.info("All components initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            await self.shutdown()
            raise

    def _on_tick_received(self, tick: TickData):
        """ティックデータ受信時のコールバック（超高速処理優先）"""
        try:
            # 統計更新
            self.stats["ticks_processed"] += 1

            # ⚡ 最優先：即座にトレーディング分析
            trading_exchange = self.config.get("trading.exchange", "bybit")
            
            if trading_exchange == "bybit":
                # Bybitで取引可能な銘柄のみ戦略分析
                if self.symbol_mapper.is_tradeable_on_bybit(tick.symbol):
                    signal = self.strategy.analyze_tick(tick)
                else:
                    signal = None
            else:
                # MEXC取引の場合は全銘柄で戦略分析
                signal = self.strategy.analyze_tick(tick)

            # ⚡ シグナル処理（最優先）
            if signal and signal.signal_type != SignalType.NONE:
                self.stats["signals_generated"] += 1
                logger.info(
                    f"🚨 SIGNAL: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f} - {signal.reason}"
                )

                # シグナル処理を最優先で実行
                asyncio.create_task(self._process_signal(signal))

            # ⚡ ポジション PnL 更新（既存ポジションがある場合のみ）
            if tick.symbol in self.position_manager.get_position_symbols():
                self.position_manager.update_position_pnl(tick.symbol, tick.price)

            # 🔄 データ管理・保存（最低優先度）- 非同期で実行してメインスレッドをブロックしない
            # トレーディング判断に影響しないよう、バックグラウンドで処理
            asyncio.create_task(self._background_data_processing(tick))

        except Exception as e:
            logger.error(f"Error processing tick data for {tick.symbol}: {e}")

    async def _background_data_processing(self, tick: TickData):
        """バックグラウンドでのデータ管理・保存処理（トレーディングをブロックしない）"""
        try:
            # データ管理に追加（ロック処理があるため非同期で実行）
            await asyncio.get_event_loop().run_in_executor(
                None, self.data_manager.add_tick, tick
            )
            
            # QuestDB保存（非同期キューに追加のみ - ブロックしない）
            self.questdb_client.save_tick_data(tick)
            
        except Exception as e:
            logger.error(f"Error in background data processing for {tick.symbol}: {e}")

    async def _process_signal(self, signal):
        """取引シグナル処理"""
        try:
            if (
                signal.signal_type == SignalType.LONG
                or signal.signal_type == SignalType.SHORT
            ):
                await self._process_entry_signal(signal)
            elif signal.signal_type == SignalType.CLOSE:
                await self._process_exit_signal(signal)

        except Exception as e:
            logger.error(f"Error processing signal: {e}")

    async def _process_entry_signal(self, signal):
        """エントリーシグナル処理"""
        symbol = signal.symbol
        side = signal.signal_type.value
        entry_price = signal.price

        # ポジション開設可能性チェック
        can_open, reason = self.position_manager.can_open_position(symbol)
        if not can_open:
            logger.info(f"Cannot open position for {symbol}: {reason}")
            return

        # ポジション開設
        success, message, position = self.position_manager.open_position(
            symbol, side, entry_price, signal.timestamp
        )

        if success and position:
            logger.info(f"Position opened successfully: {symbol} {side}")
            self.stats["trades_executed"] += 1

            # 戦略にポジションを登録
            self.strategy.add_position(
                symbol, side, entry_price, position.size, signal.timestamp
            )

            # 取引記録
            trade_id = self.trade_record_manager.record_trade_open(position)
            logger.info(f"Trade recorded with ID: {trade_id}")

        else:
            logger.error(f"Failed to open position: {message}")

    async def _process_exit_signal(self, signal):
        """決済シグナル処理"""
        symbol = signal.symbol

        # ポジション決済
        success, message, position = self.position_manager.close_position(
            symbol, signal.reason
        )

        if success and position:
            logger.info(f"Position closed successfully: {symbol} {position.side}")

            # 戦略からポジションを削除
            tracker = self.strategy.remove_position(symbol)

            # PnL計算
            realized_pnl = position.unrealized_pnl

            # 取引記録を更新（簡略化）
            logger.info(f"Trade closed: PnL = {realized_pnl:.2f} USDT")

        else:
            logger.error(f"Failed to close position: {message}")

    def _start_stats_timer(self):
        """統計表示タイマー開始"""

        def show_stats():
            if self.running:
                self._log_statistics()
                # 次のタイマーをスケジュール
                self.stats_timer = threading.Timer(60.0, show_stats)  # 1分間隔
                self.stats_timer.daemon = True
                self.stats_timer.start()

        self.stats_timer = threading.Timer(60.0, show_stats)
        self.stats_timer.daemon = True
        self.stats_timer.start()

    def _log_statistics(self):
        """統計情報をログ出力"""
        try:
            # アップタイム計算
            uptime = (datetime.now() - self.stats["start_time"]).total_seconds()
            self.stats["uptime"] = uptime

            # 各コンポーネントの統計取得
            data_stats = self.data_manager.get_stats() if self.data_manager else {}
            strategy_stats = self.strategy.get_stats() if self.strategy else {}
            position_stats = (
                self.position_manager.get_stats() if self.position_manager else {}
            )
            questdb_stats = (
                self.questdb_client.get_stats() if self.questdb_client else {}
            )
            symbol_stats = (
                self.symbol_mapper.get_mapping_stats() if self.symbol_mapper else {}
            )

            # ポートフォリオ要約
            portfolio = (
                self.position_manager.get_portfolio_summary()
                if self.position_manager
                else {}
            )

            logger.info("=== TRADE MINI STATISTICS ===")
            logger.info(f"Uptime: {uptime/3600:.2f} hours")
            logger.info(f"Ticks processed: {self.stats['ticks_processed']}")
            logger.info(f"Signals generated: {self.stats['signals_generated']}")
            logger.info(f"Trades executed: {self.stats['trades_executed']}")

            logger.info(f"Active symbols: {data_stats.get('active_symbols', 0)}")
            logger.info(f"Open positions: {position_stats.get('current_positions', 0)}")
            logger.info(
                f"Account balance: {portfolio.get('account_balance', 0):.2f} USDT"
            )
            logger.info(
                f"Total unrealized PnL: {portfolio.get('total_unrealized_pnl', 0):.2f} USDT"
            )

            logger.info(f"QuestDB ticks saved: {questdb_stats.get('ticks_saved', 0)}")
            logger.info(f"Tradeable symbols on Bybit: {symbol_stats.get('total_tradeable_symbols', 0)}")
            logger.info("=============================")

        except Exception as e:
            logger.error(f"Error logging statistics: {e}")

    async def run(self):
        """メインループ実行"""
        logger.info("Starting Trade Mini...")

        try:
            # 初期化
            await self.initialize()

            # シグナルハンドラー設定
            self._setup_signal_handlers()

            self.running = True
            logger.info("Trade Mini is running. Press Ctrl+C to stop.")

            # メインループ
            while self.running and not self.shutdown_event.is_set():
                try:
                    await asyncio.sleep(1.0)

                    # 定期的なクリーンアップ
                    if int(time.time()) % 300 == 0:  # 5分毎
                        self.position_manager.cleanup_closed_positions()

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    await asyncio.sleep(1.0)

            logger.info("Main loop ended")

        except Exception as e:
            logger.error(f"Critical error: {e}")
        finally:
            await self.shutdown()

    def _setup_signal_handlers(self):
        """シグナルハンドラー設定"""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.running = False
            self.shutdown_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def shutdown(self):
        """シャットダウン処理"""
        logger.info("Shutting down Trade Mini...")

        self.running = False
        self.shutdown_event.set()

        try:
            # 統計タイマー停止
            if self.stats_timer:
                self.stats_timer.cancel()

            # 最終統計表示
            self._log_statistics()

            # 各コンポーネントのシャットダウン
            if self.mexc_client:
                logger.info("Shutting down MEXC client...")
                await self.mexc_client.stop()

            if self.position_manager:
                logger.info("Shutting down position manager...")
                self.position_manager.shutdown()

            if self.questdb_client:
                logger.info("Shutting down QuestDB client...")
                self.questdb_client.shutdown()

            if self.data_manager:
                logger.info("Shutting down data manager...")
                self.data_manager.shutdown()

            logger.info("Trade Mini shutdown completed")

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
                "data_manager": (
                    self.data_manager.get_stats() if self.data_manager else {}
                ),
                "strategy": self.strategy.get_stats() if self.strategy else {},
                "positions": (
                    self.position_manager.get_stats() if self.position_manager else {}
                ),
                "questdb": (
                    self.questdb_client.get_stats() if self.questdb_client else {}
                ),
                "portfolio": (
                    self.position_manager.get_portfolio_summary()
                    if self.position_manager
                    else {}
                ),
            }
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {"error": str(e)}


async def main():
    """メイン関数"""
    try:
        # Trade Mini インスタンス作成
        app = TradeMini()

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
