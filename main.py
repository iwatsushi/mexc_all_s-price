"""
Trade Mini - メインアプリケーション
"""

import asyncio
import gzip
import json
import logging
import multiprocessing
import signal
import socket
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Dict

# ログ設定
from loguru import logger as loguru_logger

# グローバルロガー
logger = loguru_logger

from bybit_client import BybitClient

# 自作モジュール
from config import Config
from data_manager import DataManager
from mexc_client import MEXCClient, TickData
from mexc_websocket_process import mexc_websocket_worker
from position_manager import PositionManager
from questdb_client import QuestDBClient, QuestDBTradeRecordManager
from strategy import SignalType, TradingStrategy
from symbol_mapper import SymbolMapper


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
        # self.bybit_client = None  # 削除：マルチプロセス内でのみ使用
        self.symbol_mapper = None
        self.data_manager = None
        self.strategy = None
        self.position_manager = None
        self.questdb_client = None
        self.trade_record_manager = None

        # 実行制御
        self.running = False
        self.shutdown_event = threading.Event()
        
        # マルチプロセス管理
        self.websocket_process = None
        self.websocket_data_queue = None
        self.websocket_control_queue = None
        self.use_dedicated_websocket_process = False

        # 統計
        self.stats = {
            "start_time": datetime.now(),
            "ticks_processed": 0,
            "signals_generated": 0,
            "trades_executed": 0,
            "uptime": 0.0,
        }

        # 変動率統計（非同期収集）
        self.price_changes = {
            "max_change": 0.0,
            "max_change_symbol": "",
            "max_change_direction": "",
            "last_report_time": datetime.now(),
            "changes_since_last_report": 0,
        }

        # 統計表示タイマー
        self.stats_timer = None

        # 🛡️ 真のマルチプロセス分離設計
        self.data_queue = multiprocessing.Queue(maxsize=10)  # プロセス間通信キュー
        self.processing_active = multiprocessing.Value(
            "b", True
        )  # プロセス間共有フラグ
        self.worker_heartbeat = multiprocessing.Value(
            "d", time.time()
        )  # ワーカーハートビート
        self.data_processor = None  # データ処理プロセス

        # 📊 価格履歴管理（10秒前比較用） - symbol -> deque([(timestamp_sec, price), ...])
        self.price_history = defaultdict(lambda: deque(maxlen=15))  # 約15秒分のバッファ

        # 📈 統計カウンタ（WebSocket受信とデータ処理で分離）
        self.reception_stats = {"batches_received": 0, "tickers_received": 0}
        self.processing_stats = {"batches_processed": 0, "tickers_processed": 0}

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
            retention=self.config.get("logging.backup_count", 5),
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

    def _init_multiprocess_websocket(self):
        """専用WebSocketプロセス初期化"""
        try:
            # プロセス間通信キュー作成
            self.websocket_data_queue = multiprocessing.Queue(maxsize=1000)
            self.websocket_control_queue = multiprocessing.Queue(maxsize=10)
            
            # WebSocketプロセス作成
            self.websocket_process = multiprocessing.Process(
                target=mexc_websocket_worker,
                args=(
                    self.config._config,  # 設定辞書を渡す
                    self.websocket_data_queue,
                    self.websocket_control_queue
                ),
                name="MEXCWebSocketProcess"
            )
            
            logger.info("🚀 MEXC WebSocket Process initialized")
            self.use_dedicated_websocket_process = True
            
        except Exception as e:
            logger.error(f"💥 Failed to initialize WebSocket process: {e}")
            raise

    def _start_websocket_process(self):
        """WebSocketプロセス開始"""
        if self.websocket_process and not self.websocket_process.is_alive():
            try:
                self.websocket_process.start()
                logger.info(f"✅ MEXC WebSocket Process started (PID: {self.websocket_process.pid})")
            except Exception as e:
                logger.error(f"💥 Failed to start WebSocket process: {e}")
                raise

    def _stop_websocket_process(self):
        """WebSocketプロセス停止"""
        if self.websocket_process and self.websocket_process.is_alive():
            try:
                # 停止シグナル送信
                self.websocket_control_queue.put("shutdown")
                
                # プロセス終了を待つ（タイムアウト付き）
                self.websocket_process.join(timeout=10)
                
                if self.websocket_process.is_alive():
                    logger.warning("⚠️ WebSocket process did not shutdown gracefully, terminating...")
                    self.websocket_process.terminate()
                    self.websocket_process.join(timeout=5)
                    
                    if self.websocket_process.is_alive():
                        logger.error("💥 Force killing WebSocket process...")
                        self.websocket_process.kill()
                        self.websocket_process.join()
                
                logger.info("✅ MEXC WebSocket Process stopped")
                
            except Exception as e:
                logger.error(f"💥 Error stopping WebSocket process: {e}")

    async def _process_websocket_data(self):
        """WebSocketプロセスからのデータ処理"""
        while self.running:
            try:
                # 非ブロッキングでデータ取得
                if not self.websocket_data_queue.empty():
                    data_packet = self.websocket_data_queue.get_nowait()
                    
                    packet_type = data_packet.get('type')
                    if packet_type == 'tickers':
                        # ティッカーデータを既存のコールバックに転送
                        tickers = data_packet.get('data', [])
                        if tickers:
                            self._on_ticker_batch_received(tickers)
                    elif packet_type == 'stats':
                        # WebSocketプロセス統計情報を処理
                        ws_stats = data_packet.get('data', {})
                        logger.debug(f"📊 WebSocket Process Stats: {ws_stats}")
                
                await asyncio.sleep(0.01)  # CPU使用率制御
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing WebSocket data: {e}")
                await asyncio.sleep(0.1)

    async def initialize(self):
        """コンポーネント初期化"""
        logger.info("Initializing components...")

        try:
            # WebSocket処理方式の判定
            use_dedicated_process = self.config.get('bybit.environment') != 'websocket-ping_only'
            
            if use_dedicated_process:
                # 専用WebSocketプロセス使用
                logger.info("🚀 Using dedicated WebSocket process for MEXC connection")
                self._init_multiprocess_websocket()
            else:
                # 従来のインラインWebSocket使用
                logger.info("🔍 Using inline WebSocket for MEXC connection")
                self.mexc_client = MEXCClient(self.config)
                logger.info("MEXC client created")

            # Bybit クライアント（統計表示用にメインプロセスでも初期化）
            from bybit_client import BybitClient
            self.bybit_client = BybitClient(
                self.config.bybit_api_key,
                self.config.bybit_api_secret,
                self.config.bybit_environment,
                self.config.bybit_api_url,
            )
            logger.info("Bybit client initialized for main process")

            # 銘柄マッピング管理
            from symbol_mapper import SymbolMapper
            self.symbol_mapper = SymbolMapper(self.bybit_client)
            logger.info("Symbol mapper created")

            # データ管理
            self.data_manager = DataManager(self.config)
            logger.info("Data manager created")

            # QuestDB クライアント
            self.questdb_client = QuestDBClient(self.config)
            self.trade_record_manager = QuestDBTradeRecordManager(self.questdb_client)
            logger.info("QuestDB client created")

            # ポジション管理
            from position_manager import PositionManager
            self.position_manager = PositionManager(
                self.config, self.mexc_client, self.bybit_client, self.symbol_mapper
            )
            logger.info("Position manager created")

            # 取引戦略（統計表示用のコンポーネント参照を含む）
            self.strategy = TradingStrategy(
                self.config, self.data_manager,
                position_manager=self.position_manager,
                questdb_client=self.questdb_client,
                symbol_mapper=self.symbol_mapper,
                main_stats=self.stats
            )
            logger.info("Trading strategy created")

            # MEXC WebSocket 接続
            if self.use_dedicated_websocket_process:
                # 専用プロセスでWebSocket処理
                self._start_websocket_process()
                logger.info("✅ Dedicated WebSocket process started")
            else:
                # インラインでWebSocket処理
                if not await self.mexc_client.start():
                    raise Exception("Failed to connect to MEXC WebSocket")

                # ティッカーバッチコールバック設定（パターンB'）
                self.mexc_client.set_batch_callback(self._on_ticker_batch_received)

                # 全銘柄購読
                if not await self.mexc_client.subscribe_all_tickers():
                    raise Exception("Failed to subscribe to all tickers")

            # 統計表示タイマー開始
            logger.info("🔧 Starting statistics timer...")
            self._start_stats_timer()
            logger.info("✅ Statistics timer started")

            # WebSocket+pingモード以外でマルチプロセス開始
            if self.config.get('bybit.environment') != 'websocket-ping_only':
                # 🚀 真のマルチプロセスデータ処理ワーカー開始（GIL完全回避）
                self._start_multiprocess_data_worker()
            else:
                logger.info("🔍 WebSocket+ping mode: Multiprocess worker disabled")

            logger.info("All components initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            await self.shutdown()
            raise

    def _on_ticker_batch_received(self, tickers: list):
        """WebSocket受信コールバック（真のマルチプロセス分離）"""
        try:
            # ping処理テストログは無効化
            
            # 🚀 受信証明のみ（極限の軽量化 < 0.001ms）
            self.reception_stats["batches_received"] += 1
            current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            # WebSocket+pingモードの場合は詳細統計のみ（データ処理スキップ）
            if self.config.get('bybit.environment') == 'websocket-ping_only':
                self._handle_websocket_monitor_batch(tickers, current_time)
                return

            # 💓 ping送信はMEXCClient内で統一処理

            # 📨 受信証明ログのみ
            logger.info(
                f"🔥 [{current_time}] WebSocket ALIVE! Batch #{self.reception_stats['batches_received']}: {len(tickers)} tickers → Multi-Process Queue"
            )

            # 🎯 マルチプロセスキューに瞬間投入（ノンブロッキング）
            try:
                # 生データをそのまま送信（変換処理なし）
                self.data_queue.put_nowait(
                    {
                        "tickers": tickers,
                        "timestamp": time.time(),
                        "batch_id": self.reception_stats["batches_received"],
                    }
                )
            except:
                # キューが満杯でも受信は継続（データ処理より受信を優先）
                logger.debug(
                    f"Multi-process queue full, skipping batch #{self.reception_stats['batches_received']}"
                )

        except Exception as e:
            # エラーが発生してもWebSocket受信は絶対に停止しない
            logger.error(f"Error in reception callback: {e}")

    def _handle_websocket_monitor_batch(self, tickers: list, current_time: str):
        """WebSocket監視モード用バッチ処理"""
        # 受信間隔測定
        if not hasattr(self, '_last_monitor_time'):
            self._last_monitor_time = time.time()
            self._monitor_intervals = []
            self._monitor_min_interval = float('inf')
            self._monitor_max_interval = 0.0
            self._monitor_start_time = time.time()
            # ping送信管理はMEXCClient内で統一処理
        
        current_timestamp = time.time()
        if self._last_monitor_time:
            interval = current_timestamp - self._last_monitor_time
            self._monitor_intervals.append(interval)
            self._monitor_min_interval = min(self._monitor_min_interval, interval)
            self._monitor_max_interval = max(self._monitor_max_interval, interval)
            
            # 直近100件のみ保持
            if len(self._monitor_intervals) > 100:
                self._monitor_intervals.pop(0)
        
        self._last_monitor_time = current_timestamp
        
        # 統計更新（ping処理より前に実行）
        self.reception_stats["batches_received"] += 1
        self.reception_stats["tickers_received"] += len(tickers)
        
        # 💓 ping送信はMEXCClient内で統一処理（監視モードでも共通）
        
        # 詳細ログ（受信統計） - 重複削除
        # logger.info(
        #     f"📊 [{current_time}] WebSocket Monitor: Batch #{self.reception_stats['batches_received']}: "
        #     f"{len(tickers)} tickers (total: {self.reception_stats['tickers_received']})"
        # )
    
    

    def _print_websocket_monitor_stats(self):
        """WebSocket監視モード統計表示"""
        uptime = time.time() - self._monitor_start_time
        
        # 受信レート計算
        message_rate = self.reception_stats["batches_received"] / uptime if uptime > 0 else 0
        ticker_rate = self.reception_stats["tickers_received"] / uptime if uptime > 0 else 0
        
        # 受信間隔統計
        avg_interval = 0
        if hasattr(self, '_monitor_intervals') and self._monitor_intervals:
            avg_interval = sum(self._monitor_intervals) / len(self._monitor_intervals)
        
        logger.info("📊 WebSocket Monitor Stats (Main Process):")
        logger.info(f"   ⏱️  Uptime: {uptime:.1f}s")
        logger.info(f"   📨 Total batches: {self.reception_stats['batches_received']} ({message_rate:.2f}/s)")
        logger.info(f"   📈 Total tickers: {self.reception_stats['tickers_received']} ({ticker_rate:.2f}/s)")
        
        if hasattr(self, '_monitor_intervals') and self._monitor_intervals:
            logger.info(
                f"   📊 Batch intervals: avg={avg_interval:.3f}s, "
                f"min={self._monitor_min_interval:.3f}s, max={self._monitor_max_interval:.3f}s"
            )

    def _start_multiprocess_data_worker(self):
        """マルチプロセスデータ処理ワーカーを開始"""
        logger.info("🚀 Starting multi-process data worker (true process separation)")

        # 独立プロセスでデータ処理を実行
        self.data_processor = multiprocessing.Process(
            target=self._multiprocess_data_worker,
            args=(self.data_queue, self.processing_active, self.worker_heartbeat),
            daemon=True,
        )
        self.data_processor.start()
        logger.info(
            f"✅ Multi-process data worker started with PID: {self.data_processor.pid}"
        )

    @staticmethod
    def _multiprocess_data_worker(
        data_queue: multiprocessing.Queue,
        processing_active: multiprocessing.Value,
        worker_heartbeat: multiprocessing.Value,
    ):
        """独立プロセスでのデータ処理（GIL完全回避）"""
        import time
        from datetime import datetime, timedelta

        # プロセス独立ログ設定
        from loguru import logger

        logger.add("multiprocess_worker.log", rotation="1 MB")

        logger.info(
            f"🔄 Multi-process data worker started in PID: {multiprocessing.current_process().pid}"
        )

        last_heartbeat = time.time()

        while processing_active.value:
            try:
                # 🩸 ハートビート更新（5秒毎）
                current_time = time.time()
                if current_time - last_heartbeat >= 5.0:
                    worker_heartbeat.value = current_time
                    last_heartbeat = current_time
                    logger.debug(
                        f"💓 Worker heartbeat: {datetime.fromtimestamp(current_time).strftime('%H:%M:%S')}"
                    )

                # キューからデータを取得（タイムアウト付き）
                try:
                    batch_data = data_queue.get(timeout=1.0)
                except:
                    continue  # タイムアウト時は次の循環へ

                # 既存フォーマットに戻す
                tickers = batch_data["tickers"]
                batch_timestamp = batch_data["timestamp"]
                batch_id = batch_data["batch_id"]

                # 🚀 高速処理（JSONからQuestDB形式への直接変換）
                TradeMini._process_batch_lightning_fast(
                    tickers, batch_timestamp, batch_id
                )

                # 処理後にもハートビート更新
                worker_heartbeat.value = time.time()

            except Exception as e:
                logger.error(f"Error in multi-process data worker: {e}")
                time.sleep(0.1)  # エラー時は短時間待機

        logger.info("Multi-process data worker shutdown completed")

    def _check_multiprocess_health(self):
        """マルチプロセスワーカーのヘルスチェック"""
        try:
            current_time = time.time()

            # ワーカープロセスの生存確認
            if self.data_processor and not self.data_processor.is_alive():
                logger.error(
                    "🚨 Multi-process data worker is dead! Attempting restart..."
                )
                self._restart_multiprocess_worker()
                return

            # ハートビートチェック
            last_heartbeat = self.worker_heartbeat.value
            heartbeat_age = current_time - last_heartbeat

            if heartbeat_age > 30.0:  # 30秒以上ハートビートがない
                logger.warning(f"⚠️ Worker heartbeat stale: {heartbeat_age:.1f}s ago")
                if heartbeat_age > 60.0:  # 1分以上なら強制再起動
                    logger.error("🚨 Worker heartbeat timeout! Restarting worker...")
                    self._restart_multiprocess_worker()
                    return

            # キューサイズ監視
            queue_size = self.data_queue.qsize()
            if queue_size >= 8:  # キューが詰まっている
                logger.warning(f"⚠️ Data queue congestion: {queue_size}/10 items")

            # 正常時のヘルスレポート
            worker_pid = self.data_processor.pid if self.data_processor else "None"
            logger.debug(
                f"💪 Health check OK - Worker PID: {worker_pid}, Queue: {queue_size}/10, Heartbeat: {heartbeat_age:.1f}s ago"
            )

        except Exception as e:
            logger.error(f"Error in health check: {e}")

    def _restart_multiprocess_worker(self):
        """マルチプロセスワーカーを再起動"""
        try:
            logger.info("🔄 Restarting multi-process data worker...")

            # 古いプロセスを停止
            if self.data_processor:
                self.processing_active.value = False
                self.data_processor.terminate()
                self.data_processor.join(timeout=5)
                if self.data_processor.is_alive():
                    logger.warning("Force killing stuck worker process")
                    self.data_processor.kill()

            # 新しいプロセスを開始
            self.processing_active.value = True
            self.worker_heartbeat.value = time.time()
            self._start_multiprocess_data_worker()

            logger.info("✅ Multi-process worker restart completed")

        except Exception as e:
            logger.error(f"Failed to restart multi-process worker: {e}")

    # マルチプロセス用グローバル変数（プロセス開始時に一度だけ初期化）
    _mp_config = None
    _mp_bybit_client = None
    _mp_data_manager = None
    _mp_strategy = None
    _mp_position_manager = None
    _mp_symbol_mapper = None

    @staticmethod
    def _init_multiprocess_components():
        """マルチプロセス開始時に一度だけ実行される初期化"""
        try:
            print("🔧 Starting multi-process component initialization...", flush=True)
            logger.info("🔧 Starting multi-process component initialization...")

            TradeMini._mp_config = Config()
            print("✅ Config initialized", flush=True)
            logger.info("✅ Config initialized")

            TradeMini._mp_data_manager = DataManager(TradeMini._mp_config)
            print("✅ DataManager initialized", flush=True)
            logger.info("✅ DataManager initialized")

            # TradingStrategy初期化（PositionManagerを後で再設定）
            TradeMini._mp_strategy = TradingStrategy(
                TradeMini._mp_config, TradeMini._mp_data_manager
            )
            print("✅ TradingStrategy (initial) initialized", flush=True)
            logger.info("✅ TradingStrategy (initial) initialized")

            # マルチプロセス用のMEXCClient初期化（PositionManager用）
            from mexc_client import MEXCWebSocketClient
            TradeMini._mp_mexc_client = MEXCWebSocketClient(TradeMini._mp_config)
            print("✅ MEXCClient initialized for multiprocess", flush=True)
            logger.info("✅ MEXCClient initialized for multiprocess")

            # マルチプロセス用のBybitClient初期化（各プロセスで必要なため独立したインスタンスを作成）
            from bybit_client import BybitClient
            from symbol_mapper import SymbolMapper
            from position_manager import PositionManager
            
            # Bybitクライアントを作成（マルチプロセス環境のため独立したインスタンスが必要）
            TradeMini._mp_bybit_client = BybitClient(
                TradeMini._mp_config.bybit_api_key,
                TradeMini._mp_config.bybit_api_secret,
                TradeMini._mp_config.bybit_environment,
                TradeMini._mp_config.bybit_api_url,
            )
            print("✅ Bybit client initialized for multiprocess", flush=True)
            logger.info("✅ Bybit client initialized for multiprocess")
            
            # SymbolMapperを初期化
            TradeMini._mp_symbol_mapper = SymbolMapper(TradeMini._mp_bybit_client)
            print("✅ SymbolMapper initialized for multiprocess", flush=True)
            logger.info("✅ SymbolMapper initialized for multiprocess")

            # PositionManagerを初期化（config, mexc_client, bybit_client, symbol_mapperの順序）
            TradeMini._mp_position_manager = PositionManager(
                TradeMini._mp_config, 
                TradeMini._mp_mexc_client,  # MEXCクライアントを追加
                TradeMini._mp_bybit_client, 
                TradeMini._mp_symbol_mapper
            )
            print("✅ PositionManager initialized for multiprocess", flush=True)
            logger.info("✅ PositionManager initialized for multiprocess")

            # PositionManagerが初期化されたのでstrategyに参照を設定
            TradeMini._mp_strategy.position_manager = TradeMini._mp_position_manager
            print("✅ Strategy configured with PositionManager", flush=True)
            logger.info("✅ Strategy configured with PositionManager")

            print(
                "✅ Multi-process components initialization completed successfully",
                flush=True,
            )
            logger.info(
                "✅ Multi-process components initialization completed successfully"
            )

        except Exception as e:
            print(f"❌ Failed to initialize multi-process components: {e}", flush=True)
            logger.error(f"❌ Failed to initialize multi-process components: {e}")
            import traceback

            print(f"Traceback: {traceback.format_exc()}", flush=True)
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @staticmethod
    def _process_batch_lightning_fast(
        tickers: list, batch_timestamp: float, batch_id: int
    ):
        """
        バッチ処理（QuestDB保存 + 戦略分析）
        
        タイムスタンプ統一方針：
        - 基本：MEXCのAPIタイムスタンプ (datetime.fromtimestamp(mexc_timestamp / 1000))
        - フォールバック：バッチ受信時刻 (datetime.fromtimestamp(batch_timestamp))
        - 廃止：datetime.now() の使用（データ一貫性のため）
        """
        # 強制的なログ出力（マルチプロセス内でのデバッグ）
        print(
            f"🔥 BATCH FUNCTION CALLED: batch_id={batch_id}, tickers={len(tickers)}",
            flush=True,
        )

        # 初期化チェック（プロセス開始時に一度だけ）
        if TradeMini._mp_config is None:
            print("🔧 INITIALIZING MULTIPROCESS COMPONENTS...", flush=True)
            TradeMini._init_multiprocess_components()

        start_time = time.time()
        processed_count = 0
        questdb_lines = []
        signals_count = 0

        try:
            # 🚀 JSONから直接QuestDB ILP形式に変換
            batch_ts_ns = int(batch_timestamp * 1_000_000_000)
            
            # バッチ受信時刻をナノ秒タイムスタンプで統一（QuestDBと同じ形式）

            # サンプルティッカーデータの構造をログ出力（最初のバッチのみ）
            if batch_id == 1 and len(tickers) > 0:
                sample_ticker = tickers[0]
                print(f"🔍 Sample ticker data structure: {sample_ticker}")
                print(
                    f"🔍 Available fields: {list(sample_ticker.keys()) if isinstance(sample_ticker, dict) else 'Not a dict'}"
                )

                # MEXCタイムスタンプフィールドの確認（存在するフィールドのみ）
                mexc_ts = sample_ticker.get("timestamp")
                print(f"🕒 MEXC TIMESTAMP CHECK:")
                print(f"🕒   timestamp={mexc_ts} (type: {type(mexc_ts)})")

            # タイムスタンプデバッグ用カウンター（バッチ毎にリセット）
            timestamp_debug_count = 0

            for ticker_data in tickers:
                if not isinstance(ticker_data, dict):
                    continue

                symbol = ticker_data.get("symbol", "")
                price = ticker_data.get("lastPrice")
                volume = ticker_data.get("volume24", "0")

                # MEXCのタイムスタンプフィールドのみ取得（存在しないフィールドは不要）
                mexc_timestamp = ticker_data.get("timestamp")

                if symbol and price:
                    try:
                        price_f = float(price)
                        volume_f = float(volume)

                        # MEXCタイムスタンプを使用（ミリ秒→ナノ秒変換）
                        if mexc_timestamp is not None and isinstance(mexc_timestamp, (int, float)):
                            try:
                                # 型安全性を強化：必ずfloatに変換してから計算
                                timestamp_ms = float(mexc_timestamp)
                                timestamp_ns = int(timestamp_ms * 1_000_000)  # ミリ秒→ナノ秒
                            except (ValueError, TypeError) as e:
                                print(f"⚠️ Timestamp conversion error for {symbol}: {mexc_timestamp} - {e}")
                                timestamp_ns = batch_ts_ns  # フォールバック
                        else:
                            timestamp_ns = batch_ts_ns  # フォールバック

                        # QuestDB ILP形式で直接生成
                        line = f"tick_data,symbol={symbol} price={price_f},volume={volume_f} {timestamp_ns}"
                        questdb_lines.append(line)
                        processed_count += 1

                        # 最初の20銘柄を確実に出力してMEXCの銘柄形式を確認
                        if processed_count <= 20:
                            logger.info(
                                f"🔍 Sample symbol #{processed_count}: {symbol}"
                            )

                        # 🔄 全銘柄を戦略分析対象に変更（制限削除）
                        signal = None

                        # 全銘柄に対してデータ分析を実行
                        if processed_count <= 100:  # 最初の100銘柄で詳細分析をテスト
                            try:
                                print(
                                    f"🔄 全銘柄分析: {symbol} (processed_count={processed_count})"
                                )

                                # TickDataオブジェクトの作成（数値タイムスタンプで統一）
                                # QuestDBと同じtimestamp_nsを使用してdatetime型との混在を回避
                                tick_timestamp_ns = timestamp_ns  # 既に計算済みのナノ秒タイムスタンプを使用

                                tick = TickData(
                                    symbol=symbol,
                                    price=price_f,
                                    timestamp=tick_timestamp_ns,  # ナノ秒単位の数値タイムスタンプ
                                    volume=volume_f,
                                )

                                # データ追加
                                start_time = time.time()
                                TradeMini._mp_data_manager.add_tick(tick)
                                elapsed = time.time() - start_time

                                print(
                                    f"✅ Data added successfully in {elapsed:.3f}s for {symbol}"
                                )

                                # データ件数とタイムレンジの確認
                                symbol_data = (
                                    TradeMini._mp_data_manager.get_symbol_data(symbol)
                                )
                                if symbol_data:
                                    data_count = symbol_data.get_data_count()
                                    time_range = symbol_data.get_time_range()
                                    print(
                                        f"📊 {symbol}: data_count={data_count}, time_range={time_range}"
                                    )

                                    # 戦略エンジンに処理を委任
                                    if TradeMini._mp_strategy is not None:
                                        try:
                                            # ティックデータを作成してstrategyに渡す
                                            tick_data = TickData(
                                                symbol=symbol,
                                                price=price_f,
                                                timestamp=tick_timestamp_ns,
                                                volume=0.0  # デフォルト値
                                            )
                                            
                                            # 戦略エンジンでティック処理と取引実行
                                            trade_executed = TradeMini._mp_strategy.process_tick_and_execute_trades(tick_data)
                                            
                                            if trade_executed:
                                                print(f"🎯 Trade executed for {symbol}")
                                                
                                        except Exception as e:
                                            print(f"❌ Strategy processing error for {symbol}: {e}")
                                    else:
                                        print(f"⚠️ Strategy engine not available for {symbol}")

                            except Exception as data_error:
                                print(f"❌ 全銘柄分析失敗 for {symbol}: {data_error}")
                                import traceback

                                print(f"Error traceback: {traceback.format_exc()}")

                        # 🧪 強制テストシグナル（特定銘柄で確実にシグナル生成をテスト）
                        if symbol == "CSKY_USDT" and processed_count == 1:
                            signals_count += 1
                            logger.info(
                                f"🧪 FORCED TEST SIGNAL: {symbol} @ {price_f} (Testing signal generation)"
                            )

                        if signal and signal.signal_type != SignalType.NONE:
                            signals_count += 1
                            logger.info(
                                f"🚨 SIGNAL DETECTED: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f} ({signal.reason})"
                            )

                            # 実際の注文処理を実行
                            try:
                                if signal.signal_type in [
                                    SignalType.LONG,
                                    SignalType.SHORT,
                                ]:
                                    # 新規オープン注文
                                    side = (
                                        "LONG"
                                        if signal.signal_type == SignalType.LONG
                                        else "SHORT"
                                    )
                                    if TradeMini._mp_position_manager is not None:
                                        success, message, position = (
                                            TradeMini._mp_position_manager.open_position(
                                                symbol, side, signal.price, signal.timestamp
                                            )
                                        )
                                    else:
                                        success, message, position = False, "Position manager disabled", None

                                    if success and position:
                                        logger.info(
                                            f"✅ POSITION OPENED: {symbol} {side} @ {signal.price:.6f}"
                                        )
                                    else:
                                        logger.error(
                                            f"❌ POSITION OPEN FAILED: {symbol} {side} - {message}"
                                        )

                                elif signal.signal_type == SignalType.CLOSE:
                                    # ポジションクローズ注文
                                    if TradeMini._mp_position_manager is not None:
                                        success, message, position = (
                                            TradeMini._mp_position_manager.close_position(
                                                symbol, signal.reason
                                            )
                                        )
                                    else:
                                        success, message, position = False, "Position manager disabled", None

                                    if success and position:
                                        logger.info(
                                            f"✅ POSITION CLOSED: {symbol} @ {signal.price:.6f} - {signal.reason}"
                                        )
                                    else:
                                        logger.error(
                                            f"❌ POSITION CLOSE FAILED: {symbol} - {message}"
                                        )

                            except Exception as order_error:
                                logger.error(
                                    f"❌ ORDER PROCESSING ERROR: {symbol} {signal.signal_type.value} - {order_error}"
                                )
                                import traceback

                                logger.error(
                                    f"Order error traceback: {traceback.format_exc()}"
                                )

                    except (ValueError, TypeError):
                        continue

            # 🚀 QuestDB一括書き込み
            questdb_saved = 0
            if questdb_lines:
                questdb_saved = TradeMini._send_to_questdb_lightning(questdb_lines)

            duration = time.time() - start_time
            logger.info(
                f"⚡ Lightning batch #{batch_id}: {processed_count}/{len(tickers)} processed, {questdb_saved} saved to QuestDB, {signals_count} signals in {duration:.3f}s"
            )

        except Exception as e:
            import traceback
            logger.error(f"Error in lightning processing: {e}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            # 型エラーの詳細を特定するため、変数の型情報を出力
            print(f"DEBUG: Error occurred with exception type: {type(e)}")
            print(f"DEBUG: Exception message: {str(e)}")
            traceback.print_exc()

    @staticmethod
    def _send_to_questdb_lightning(ilp_lines: list) -> int:
        """QuestDBに超高速で一括送信（マルチプロセス用）"""
        try:
            import socket  # マルチプロセス内で明示的にインポート

            # QuestDB ILP接続
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect(("questdb", 9009))

            # 全行を一括送信
            ilp_data = "\n".join(ilp_lines) + "\n"
            sock.sendall(ilp_data.encode("utf-8"))
            sock.close()

            from loguru import logger

            logger.debug(f"✅ QuestDB ILP: {len(ilp_lines)} records sent successfully")
            return len(ilp_lines)

        except Exception as e:
            from loguru import logger

            logger.warning(f"QuestDB write error: {e}")
            return 0

    async def _process_single_batch_efficiently(
        self, tickers: list, batch_timestamp: float, batch_id: int
    ):
        """1つのタスクで全銘柄を効率的に処理（GIL制約考慮）"""
        try:
            start_time = time.time()
            batch_ts_sec = int(batch_timestamp)
            trading_exchange = self.config.get("trading.exchange", "bybit")

            # 処理統計更新
            self.processing_stats["batches_processed"] += 1
            self.processing_stats["tickers_processed"] += len(tickers)

            logger.info(
                f"🔄 Processing batch #{batch_id}: {len(tickers)} tickers (全銘柄分析 - エントリー機会を逃さない)"
            )

            # 📊 効率的な一括処理（全銘柄対応 - 軽量化）
            signals_count = 0
            significant_changes = 0
            processed_count = 0
            tradeable_count = 0

            # 🚀 QuestDB一括書き込み用のリスト
            batch_ticks_for_questdb = []

            # 全銘柄を順次処理（1つのタスク内で完結 - 軽量版）
            for ticker_data in tickers:
                # 🚀 処理数の制限で早期終了（WebSocket受信を保護）
                if processed_count >= 500:  # 最大500銘柄まで処理
                    break
                if not isinstance(ticker_data, dict):
                    continue

                symbol = ticker_data.get("symbol", "")
                price = float(ticker_data.get("lastPrice", 0))

                if not symbol or price <= 0:
                    continue

                # 📈 価格履歴更新（高速）
                self.price_history[symbol].append((batch_ts_sec, price))
                price_change_percent = self._update_price_history_and_get_change(
                    symbol, price, batch_ts_sec
                )

                # TickData作成
                tick = TickData(
                    symbol=symbol,
                    price=price,
                    timestamp=datetime.now(),
                    volume=float(ticker_data.get("volume24", 0)),
                )

                # データ管理
                self.data_manager.add_tick(tick)

                # 🎯 戦略分析（全銘柄対応 - エントリー機会を逃さない）
                if trading_exchange == "bybit":
                    if self.symbol_mapper.is_tradeable_on_bybit(symbol):
                        tradeable_count += 1

                        # 戦略分析を軽量化（処理時間を短縮）
                        if tradeable_count <= 50:  # 最初の50銘柄のみ詳細分析
                            signal = self.strategy.analyze_tick(tick)

                            # 既存ポジションの価格更新（損切り・利確判定）
                            self.position_manager.update_position_pnl(
                                symbol, tick.price
                            )

                            if signal and signal.signal_type != SignalType.NONE:
                                signals_count += 1
                                logger.info(
                                    f"🚨 SIGNAL: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f}"
                                )

                                # 実際の注文処理を実行
                                try:
                                    if signal.signal_type in [
                                        SignalType.LONG,
                                        SignalType.SHORT,
                                    ]:
                                        # 新規オープン注文
                                        side = (
                                            "LONG"
                                            if signal.signal_type == SignalType.LONG
                                            else "SHORT"
                                        )
                                        success, message, position = (
                                            self.position_manager.open_position(
                                                symbol,
                                                side,
                                                signal.price,
                                                signal.timestamp,
                                            )
                                        )

                                        if success and position:
                                            logger.info(
                                                f"✅ POSITION OPENED: {symbol} {side} @ {signal.price:.6f}"
                                            )
                                        else:
                                            logger.error(
                                                f"❌ POSITION OPEN FAILED: {symbol} {side} - {message}"
                                            )

                                    elif signal.signal_type == SignalType.CLOSE:
                                        # ポジションクローズ注文
                                        success, message, position = (
                                            self.position_manager.close_position(
                                                symbol, signal.reason
                                            )
                                        )

                                        if success and position:
                                            logger.info(
                                                f"✅ POSITION CLOSED: {symbol} @ {signal.price:.6f} - {signal.reason}"
                                            )
                                        else:
                                            logger.error(
                                                f"❌ POSITION CLOSE FAILED: {symbol} - {message}"
                                            )

                                except Exception as order_error:
                                    logger.error(
                                        f"❌ ORDER PROCESSING ERROR: {symbol} {signal.signal_type.value} - {order_error}"
                                    )

                                # 🚀 シグナル処理を非同期で実行（WebSocket受信をブロックしない）
                                asyncio.create_task(self._process_signal(signal))

                # 📊 統計収集（全銘柄）
                if abs(price_change_percent) > 1.0:
                    significant_changes += 1

                # 💾 QuestDB保存用リストに追加（一括書き込み用）
                if (
                    processed_count < 100 or abs(price_change_percent) > 1.0
                ):  # 重要な銘柄のみ保存
                    batch_ticks_for_questdb.append(tick)

                processed_count += 1

                # 🚀 定期的にイベントループを譲る（WebSocket受信をブロックしない）
                if processed_count % 25 == 0:
                    await asyncio.sleep(0.001)  # 1ms待機でイベントループを譲る

            # 🚀 QuestDB一括書き込み（大幅なパフォーマンス向上）
            if batch_ticks_for_questdb:
                try:
                    self.questdb_client.save_batch_tick_data(batch_ticks_for_questdb)
                    logger.info(
                        f"💾 QuestDB batch write: {len(batch_ticks_for_questdb)} ticks saved efficiently"
                    )
                except Exception as e:
                    logger.error(f"Error in QuestDB batch write: {e}")

            # ⏱️ 処理時間計測
            duration = time.time() - start_time
            current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            logger.info(
                f"✅ [{current_time}] Batch #{batch_id} completed: {processed_count}/{len(tickers)} processed in {duration:.3f}s, tradeable: {tradeable_count}, signals: {signals_count}, significant_changes: {significant_changes}, questdb_saved: {len(batch_ticks_for_questdb)}"
            )

        except Exception as e:
            logger.error(f"Error processing batch #{batch_id}: {e}")

    def _minimal_sync_processing(self, tickers: list):
        """同期フォールバック処理（最小限のデータ保存のみ）"""
        try:
            logger.info(f"🔧 Minimal sync processing for {len(tickers)} tickers")
            processed_count = 0

            for ticker_data in tickers[:50]:  # 最初の50銘柄のみ処理（負荷軽減）
                if not isinstance(ticker_data, dict):
                    continue

                symbol = ticker_data.get("symbol", "")
                price = float(ticker_data.get("lastPrice", 0))

                if not symbol or price <= 0:
                    continue

                # TickData作成とQuestDB保存のみ
                tick = TickData(
                    symbol=symbol,
                    price=price,
                    timestamp=datetime.now(),
                    volume=float(ticker_data.get("volume24", 0)),
                )

                # データ管理に追加
                self.data_manager.add_tick(tick)

                # QuestDB保存（同期）
                self.questdb_client.save_tick_data(tick)
                processed_count += 1

            logger.info(
                f"✅ Minimal sync processing completed: {processed_count} tickers"
            )

        except Exception as e:
            logger.error(f"Error in minimal sync processing: {e}")

    async def _process_ticker_batch_controlled(self, tickers: list):
        """Semaphore制御付きバッチ処理（WebSocket受信保護）"""
        logger.info(f"🎯 Entering batch processing control for {len(tickers)} tickers")

        # 🔍 監視：待機中のバッチ処理数をチェック
        waiting_batches = 2 - self.batch_processing_semaphore._value
        if waiting_batches > 0:
            logger.info(f"⏳ {waiting_batches}/2 batch tasks waiting")

        logger.info(f"🔒 Acquiring batch processing semaphore...")
        async with self.batch_processing_semaphore:
            logger.info(f"✅ Semaphore acquired, starting batch processing")
            self._batch_processing = True
            try:
                await self._process_single_batch_efficiently(
                    tickers, time.time(), self.processing_stats["batches_processed"]
                )
            finally:
                self._batch_processing = False
                logger.info(f"🔓 Batch processing completed, releasing semaphore")

    async def _background_questdb_save(self, tick: TickData):
        """バックグラウンドでのQuestDB保存処理（トレーディングをブロックしない）"""
        try:
            # QuestDB保存（非同期キューに追加のみ - ブロックしない）
            self.questdb_client.save_tick_data(tick)

        except Exception as e:
            logger.error(f"Error in background QuestDB save for {tick.symbol}: {e}")

    def _update_price_history_and_get_change(
        self, symbol: str, price: float, timestamp_sec: int
    ) -> float:
        """価格履歴を更新し10秒前との変動率を計算（バッチ処理用高速版）"""
        try:
            # 🚀 履歴更新（deque操作は高速）
            self.price_history[symbol].append((timestamp_sec, price))

            # 🔍 10秒前の価格を検索（後ろから前へ効率的に検索）
            target_sec = timestamp_sec - 10
            prev_price = None

            # dequeを後ろから検索して target_sec 以下の最新価格を取得
            for ts, px in reversed(self.price_history[symbol]):
                if ts <= target_sec:
                    prev_price = px
                    break

            # 📊 変動率計算
            if prev_price and prev_price > 0:
                change_percent = ((price - prev_price) / prev_price) * 100
                return change_percent

            return 0.0

        except Exception:
            return 0.0

    def _get_price_change_from_strategy(self, symbol: str) -> float:
        """戦略から価格変動率を取得（互換性維持）"""
        try:
            # 戦略から最新の価格変動率を取得
            if hasattr(self.strategy, "get_price_change_percent"):
                return self.strategy.get_price_change_percent(symbol)
            return 0.0
        except Exception:
            return 0.0

    async def _update_price_change_stats(self, symbol: str, change_percent: float):
        """変動率統計を非同期で更新"""
        try:
            abs_change = abs(change_percent)

            # 最大変動率の更新
            if abs_change > abs(self.price_changes["max_change"]):
                self.price_changes["max_change"] = change_percent
                self.price_changes["max_change_symbol"] = symbol
                self.price_changes["max_change_direction"] = (
                    "上昇" if change_percent > 0 else "下落"
                )

            self.price_changes["changes_since_last_report"] += 1

            # 15秒ごとに最大変動率をレポート（デバッグ用に短縮）
            now = datetime.now()
            if (now - self.price_changes["last_report_time"]).total_seconds() >= 15:
                if self.price_changes["changes_since_last_report"] > 0:
                    logger.info(
                        f"📈 最大変動率: {self.price_changes['max_change_symbol']} "
                        f"{self.price_changes['max_change']:.3f}% ({self.price_changes['max_change_direction']}) "
                        f"- {self.price_changes['changes_since_last_report']}銘柄分析済み"
                    )

                # 統計リセット
                self.price_changes["max_change"] = 0.0
                self.price_changes["max_change_symbol"] = ""
                self.price_changes["max_change_direction"] = ""
                self.price_changes["last_report_time"] = now
                self.price_changes["changes_since_last_report"] = 0

        except Exception as e:
            logger.error(f"Error updating price change stats: {e}")

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

        logger.info(f"🔄 ENTRY処理開始: {symbol} {side} @ {entry_price:.6f}")

        # ポジション開設可能性チェック
        can_open, reason = self.position_manager.can_open_position(symbol)
        if not can_open:
            logger.warning(f"❌ ENTRY拒否: {symbol} {side} - 理由: {reason}")
            return

        # ポジション開設
        success, message, position = self.position_manager.open_position(
            symbol, side, entry_price, signal.timestamp
        )

        if success and position:
            logger.info(
                f"✅ ENTRY成功: {symbol} {side} @ {entry_price:.6f} "
                f"サイズ: {position.size:.4f} レバレッジ: {position.max_leverage:.1f}x"
            )
            self.stats["trades_executed"] += 1

            # 戦略にポジションを登録
            self.strategy.add_position(
                symbol, side, entry_price, position.size, signal.timestamp
            )

            # 取引記録
            trade_id = self.trade_record_manager.record_trade_open(position)
            logger.info(f"📝 取引記録作成: ID={trade_id}")

        else:
            logger.error(f"❌ ENTRY失敗: {symbol} {side} - {message}")

    async def _process_exit_signal(self, signal):
        """決済シグナル処理"""
        symbol = signal.symbol

        logger.info(
            f"🔄 EXIT処理開始: {symbol} @ {signal.price:.6f} - 理由: {signal.reason}"
        )

        # ポジション決済
        success, message, position = self.position_manager.close_position(
            symbol, signal.reason
        )

        if success and position:
            # PnL計算
            realized_pnl = position.unrealized_pnl
            pnl_percent = (realized_pnl / (position.entry_price * position.size)) * 100

            logger.info(
                f"✅ EXIT成功: {symbol} {position.side} @ {signal.price:.6f} "
                f"PnL: {realized_pnl:.2f} USDT ({pnl_percent:.2f}%)"
            )

            # 戦略からポジションを削除
            tracker = self.strategy.remove_position(symbol)

            # 取引記録を更新（簡略化）
            logger.info(f"📝 取引完了記録: {symbol} 総利益 {realized_pnl:.2f} USDT")

        else:
            logger.error(f"❌ EXIT失敗: {symbol} - {message}")

    def _start_stats_timer(self):
        """統計表示タイマー開始"""

        def show_stats():
            try:
                logger.info("🔔 Statistics timer triggered")
                if self.running and self.strategy:
                    logger.info("📊 Calling strategy.log_comprehensive_statistics...")
                    # 統計表示をstrategyに委任
                    self.strategy.log_comprehensive_statistics(self.stats["start_time"], self.stats)
                    logger.info("✅ Statistics display completed")
                else:
                    logger.warning(f"⚠️ Statistics skipped: running={self.running}, strategy={self.strategy is not None}")
            except Exception as e:
                logger.error(f"Error in stats display: {e}")
                import traceback
                logger.debug(f"Stats display error traceback: {traceback.format_exc()}")
            finally:
                # 次のタイマーをスケジュール（エラーがあっても継続）
                if self.running:
                    logger.info("⏰ Scheduling next statistics display in 10 seconds")
                    self.stats_timer = threading.Timer(10.0, show_stats)  # 10秒間隔（テスト用）
                    self.stats_timer.daemon = True
                    self.stats_timer.start()

        logger.info("⏰ Initial statistics timer started (10 second interval)")
        self.stats_timer = threading.Timer(10.0, show_stats)
        self.stats_timer.daemon = True
        self.stats_timer.start()



    async def run(self):
        """メインループ実行"""
        # config.ymlでWebSocket+pingモードが設定されているかチェック
        websocket_ping_mode = self.config.get('bybit.environment') == 'websocket-ping_only'
        
        if websocket_ping_mode:
            logger.info("🔍 WebSocket+Ping Only Mode (configured in config.yml)")
            logger.info("   - Data processing: DISABLED")
            logger.info("   - Multiprocess worker: DISABLED")
            logger.info("   - Trading: DISABLED")
            logger.info("   - QuestDB: DISABLED")
            logger.info("   - Only WebSocket receive + ping monitoring")
            logger.info("=" * 60)
        
            
        logger.info("Starting Trade Mini...")

        try:
            # 初期化
            await self.initialize()

            # シグナルハンドラー設定
            self._setup_signal_handlers()

            self.running = True
            logger.info("Trade Mini is running. Press Ctrl+C to stop.")

            # メインループタスク作成
            main_tasks = []
            
            # WebSocketデータ処理タスク（専用プロセス使用時）
            if self.use_dedicated_websocket_process:
                websocket_data_task = asyncio.create_task(self._process_websocket_data())
                main_tasks.append(websocket_data_task)
                logger.info("🔄 WebSocket data processing task started")
            
            # メインループタスク
            main_loop_task = asyncio.create_task(self._main_loop())
            main_tasks.append(main_loop_task)
            
            try:
                # 全タスクの完了を待つ
                await asyncio.gather(*main_tasks, return_exceptions=True)
            except Exception as e:
                logger.error(f"💥 Main task error: {e}")
            finally:
                # 残りのタスクをキャンセル
                for task in main_tasks:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass

        except Exception as e:
            logger.error(f"Critical error: {e}")
        finally:
            await self.shutdown()

    async def _main_loop(self):
        """メインループ処理"""
        last_health_check = time.time()
        
        while self.running and not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(1.0)

                # 🩺 プロセスヘルスチェック（30秒毎）- WebSocket監視モードでは無効
                if self.config.get('bybit.environment') != 'websocket-ping_only':
                    current_time = time.time()
                    if current_time - last_health_check >= 30.0:
                        self._check_multiprocess_health()
                        last_health_check = current_time

                # 定期的なクリーンアップ
                if int(time.time()) % 300 == 0 and self.position_manager:  # 5分毎
                    self.position_manager.cleanup_closed_positions()

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1.0)
        
        logger.info("Main loop ended")

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

        # WebSocketプロセス停止
        if self.use_dedicated_websocket_process:
            logger.info("Stopping WebSocket process...")
            self._stop_websocket_process()
            logger.info("WebSocket process stopped")

        # マルチプロセスワーカー停止
        if hasattr(self, "processing_active"):
            self.processing_active.value = False

        if hasattr(self, "data_processor") and self.data_processor:
            logger.info("Terminating multi-process data worker...")
            self.data_processor.terminate()
            self.data_processor.join(timeout=5)
            logger.info("Multi-process data worker terminated")

        try:
            # 統計タイマー停止
            if self.stats_timer:
                self.stats_timer.cancel()

            # 最終統計表示
            # 最終統計をstrategyから表示
            if self.strategy:
                self.strategy.log_comprehensive_statistics(self.stats["start_time"], self.stats)

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
    import sys
    
    # ヘルプ表示
    if "--help" in sys.argv or "-h" in sys.argv:
        print("🚀 Trade Mini - MEXC/Bybit自動取引システム")
        print("")
        print("Usage:")
        print("  python main.py                    通常のトレーディングモード")
        print("  python main.py --help             このヘルプを表示（-h）")
        print("")
        print("WebSocket+Ping監視モード:")
        print("  config.yml の bybit.environment を 'websocket-ping_only' に設定")
        print("  - 本編のMEXCクライアントを使用")
        print("  - データ処理、戦略分析、取引実行は一切スキップ")
        print("  - WebSocket受信頻度とping送信のみ確認")
        print("  - 受信統計を10秒ごとに表示")
        print("  - マルチプロセスは起動せず軽量動作")
        return
    
    try:
        # マルチプロセス開始方法を設定（Dockerコンテナ対応）
        multiprocessing.set_start_method("fork", force=True)

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
