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
                self.config.bybit_environment,
                self.config.bybit_api_url,
            )
            logger.info("Bybit client created")

            # 銘柄マッピング管理
            # self.symbol_mapper = SymbolMapper(self.bybit_client)  # 一時的に無効化
            self.symbol_mapper = None
            logger.info("Symbol mapper created")

            # データ管理
            self.data_manager = DataManager(self.config)
            logger.info("Data manager created")

            # 取引戦略
            self.strategy = TradingStrategy(self.config, self.data_manager)
            logger.info("Trading strategy created")

            # ポジション管理
            # self.position_manager = PositionManager(
            #     self.config, self.mexc_client, self.bybit_client, self.symbol_mapper
            # ) # 一時的に無効化（SymbolMapperがNoneのため）
            self.position_manager = None
            logger.info("Position manager created")

            # QuestDB クライアント
            self.questdb_client = QuestDBClient(self.config)
            self.trade_record_manager = QuestDBTradeRecordManager(self.questdb_client)
            logger.info("QuestDB client created")

            # MEXC WebSocket 接続
            if not await self.mexc_client.start():
                raise Exception("Failed to connect to MEXC WebSocket")

            # ティッカーバッチコールバック設定（パターンB'）
            self.mexc_client.set_batch_callback(self._on_ticker_batch_received)

            # 全銘柄購読
            if not await self.mexc_client.subscribe_all_tickers():
                raise Exception("Failed to subscribe to all tickers")

            # 統計表示タイマー開始
            self._start_stats_timer()

            # 🚀 真のマルチプロセスデータ処理ワーカー開始（GIL完全回避）
            self._start_multiprocess_data_worker()

            logger.info("All components initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            await self.shutdown()
            raise

    def _on_ticker_batch_received(self, tickers: list):
        """WebSocket受信コールバック（真のマルチプロセス分離）"""
        try:
            # 🚀 受信証明のみ（極限の軽量化 < 0.001ms）
            self.reception_stats["batches_received"] += 1
            current_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]

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
        from datetime import datetime

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
    _mp_data_manager = None
    _mp_strategy = None
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
            
            TradeMini._mp_strategy = TradingStrategy(TradeMini._mp_config, TradeMini._mp_data_manager)
            print("✅ TradingStrategy initialized", flush=True)
            logger.info("✅ TradingStrategy initialized")
            
            # SymbolMapperはマルチプロセスで問題があるため、完全に無効化
            TradeMini._mp_symbol_mapper = None
            print("✅ Multi-process components initialization completed successfully", flush=True)
            logger.info("✅ Multi-process components initialization completed successfully")
            
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
        """バッチ処理（QuestDB保存 + 戦略分析）"""
        # 強制的なログ出力（マルチプロセス内でのデバッグ）
        print(f"🔥 BATCH FUNCTION CALLED: batch_id={batch_id}, tickers={len(tickers)}", flush=True)
        
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

            # サンプルティッカーデータの構造をログ出力（最初のバッチのみ）
            if batch_id == 1 and len(tickers) > 0:
                sample_ticker = tickers[0]
                print(f"🔍 Sample ticker data structure: {sample_ticker}")
                print(f"🔍 Available fields: {list(sample_ticker.keys()) if isinstance(sample_ticker, dict) else 'Not a dict'}")
                
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
                        if mexc_timestamp and isinstance(mexc_timestamp, (int, float)):
                            timestamp_ns = int(mexc_timestamp * 1_000_000)  # ミリ秒→ナノ秒
                        else:
                            timestamp_ns = batch_ts_ns  # フォールバック

                        # QuestDB ILP形式で直接生成
                        line = f"tick_data,symbol={symbol} price={price_f},volume={volume_f} {timestamp_ns}"
                        questdb_lines.append(line)
                        processed_count += 1
                        
                        # 最初の20銘柄を確実に出力してMEXCの銘柄形式を確認
                        if processed_count <= 20:
                            logger.info(f"🔍 Sample symbol #{processed_count}: {symbol}")
                        
                        # 🎯 戦略分析：MEXCの実際の銘柄形式が確認できたため、対象銘柄を設定
                        # QuestDBに確認したところ、BTC_USDT形式とBTCUSDT形式の両方が存在
                        # 高変動銘柄を追加（CSKY_USDT: 21.97%変動、ALU_USDT: 10.76%変動）
                        major_symbols = {
                            # メジャー銘柄（両形式をサポート）
                            "BTCUSDT", "BTC_USDT", "ETHUSDT", "ETH_USDT", 
                            "ADAUSDT", "ADA_USDT", "SOLUSDT", "SOL_USDT", 
                            "DOGEUSDT", "DOGE_USDT",
                            # より変動しやすい銘柄を追加
                            "PEPEUSDT", "PEPE_USDT", "SHIBUSDT", "SHIB_USDT", 
                            "FLOKIUSDT", "FLOKI_USDT", "BONKUSDT", "BONK_USDT", 
                            "1000RATSUSDT", "1000RATS_USDT",
                            # 追加の変動銘柄
                            "JUPUSDT", "JUP_USDT",
                            # 高変動テスト銘柄（QuestDBで21%+の変動を確認）
                            "CSKY_USDT", "ALU_USDT", "BOSS_USDT", "CLANKER_USDT", 
                            "MEMEFI_USDT", "ASR_USDT", "MYX_USDT", "BIO_USDT"
                        }
                        
                        # 🔍 戦略分析開始の確認（デバッグ用）
                        if processed_count == 1:  # 最初の銘柄で必ずログ出力
                            logger.info(f"🔍 First symbol processed: {symbol} (checking if in major_symbols)")
                        
                        # 🔄 Phase 2: 制御された戦略分析機能の復元
                        signal = None
                        
                        # Phase 2: 制御されたデータ追加と価格変動率チェック（複数の主要銘柄でテスト）
                        test_symbols = ["SOL_USDT", "BTC_USDT", "ETH_USDT", "KAITO_USDT", "UNI_USDT"]
                        if symbol in test_symbols:
                            print(f"🔍 Phase 2: Found test symbol {symbol}, processed_count={processed_count}")
                        if symbol in test_symbols and processed_count <= 50:  # 各銘柄50回まで（テストのため）
                            try:
                                print(f"🔄 Phase 2: Data analysis test for {symbol} (attempt {processed_count})")
                                
                                # TickDataオブジェクトの作成（MEXCの実際のタイムスタンプを使用）
                                mexc_timestamp = ticker_data.get("timestamp")
                                if mexc_timestamp is not None and isinstance(mexc_timestamp, (int, float)):
                                    try:
                                        # MEXCはミリ秒単位のUNIXタイムスタンプを提供
                                        tick_timestamp = datetime.fromtimestamp(mexc_timestamp / 1000)
                                    except (ValueError, OverflowError, OSError) as e:
                                        print(f"⚠️ Invalid timestamp for {symbol}: {mexc_timestamp} - {e}")
                                        tick_timestamp = datetime.now()
                                else:
                                    # フォールバック（通常は不要）
                                    tick_timestamp = datetime.now()
                                
                                tick = TickData(
                                    symbol=symbol,
                                    price=price_f,
                                    timestamp=tick_timestamp,
                                    volume=volume_f
                                )
                                
                                # データ追加
                                start_time = datetime.now()
                                TradeMini._mp_data_manager.add_tick(tick)
                                elapsed = (datetime.now() - start_time).total_seconds()
                                
                                print(f"✅ Data added successfully in {elapsed:.3f}s for {symbol}")
                                
                                # データ件数とタイムレンジの確認
                                symbol_data = TradeMini._mp_data_manager.get_symbol_data(symbol)
                                if symbol_data:
                                    data_count = symbol_data.get_data_count()
                                    time_range = symbol_data.get_time_range()
                                    print(f"📊 {symbol}: data_count={data_count}, time_range={time_range}")
                                    
                                    # 設定された時間分のデータが蓄積されているかチェック
                                    config_seconds = TradeMini._mp_config.price_comparison_seconds
                                    if time_range[0] and time_range[1]:
                                        try:
                                            # datetime型であることを確認してから計算
                                            if isinstance(time_range[0], datetime) and isinstance(time_range[1], datetime):
                                                time_span = (time_range[1] - time_range[0]).total_seconds()
                                                has_sufficient_data = time_span >= config_seconds
                                            else:
                                                print(f"⚠️ Invalid time_range types for {symbol}: {type(time_range[0])}, {type(time_range[1])}")
                                                has_sufficient_data = False
                                        except Exception as time_error:
                                            print(f"⚠️ Time calculation error for {symbol}: {time_error}")
                                            has_sufficient_data = False
                                    else:
                                        has_sufficient_data = False
                                    
                                    if has_sufficient_data and data_count >= 2:
                                        price_change = symbol_data.get_price_change_percent(config_seconds)
                                        print(f"📈 {symbol}: price_change={price_change}% over {config_seconds}s")
                                        
                                        # 設定値による閾値チェック
                                        long_threshold = TradeMini._mp_config.long_threshold_percent
                                        short_threshold = TradeMini._mp_config.short_threshold_percent
                                        
                                        if price_change is not None:
                                            if price_change >= long_threshold:
                                                print(f"🔥 LONG THRESHOLD REACHED: {symbol} change={price_change}% >= {long_threshold}%")
                                                
                                                # 実際にロングポジションを開く処理
                                                try:
                                                    success, message, position = TradeMini._mp_position_manager.open_position(
                                                        symbol, "LONG", price_f, tick_timestamp
                                                    )
                                                    if success:
                                                        print(f"✅ LONG POSITION OPENED: {symbol} @ {price_f}")
                                                    else:
                                                        print(f"❌ LONG POSITION FAILED: {symbol} - {message}")
                                                except Exception as e:
                                                    print(f"❌ LONG POSITION ERROR: {symbol} - {e}")
                                            
                                            elif price_change <= -short_threshold:
                                                print(f"🔥 SHORT THRESHOLD REACHED: {symbol} change={price_change}% <= -{short_threshold}%")
                                                
                                                # 実際にショートポジションを開く処理
                                                try:
                                                    success, message, position = TradeMini._mp_position_manager.open_position(
                                                        symbol, "SHORT", price_f, tick_timestamp
                                                    )
                                                    if success:
                                                        print(f"✅ SHORT POSITION OPENED: {symbol} @ {price_f}")
                                                    else:
                                                        print(f"❌ SHORT POSITION FAILED: {symbol} - {message}")
                                                except Exception as e:
                                                    print(f"❌ SHORT POSITION ERROR: {symbol} - {e}")
                                
                            except Exception as data_error:
                                print(f"❌ Phase 2: Data analysis failed for {symbol}: {data_error}")
                                import traceback
                                print(f"Error traceback: {traceback.format_exc()}")
                            
                            # 🧪 強制テストシグナル（特定銘柄で確実にシグナル生成をテスト）
                            if symbol == "CSKY_USDT" and processed_count == 1:
                                signals_count += 1
                                logger.info(f"🧪 FORCED TEST SIGNAL: {symbol} @ {price_f} (Testing signal generation)")
                            
                            if signal and signal.signal_type != SignalType.NONE:
                                signals_count += 1
                                logger.info(
                                    f"🚨 SIGNAL DETECTED: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f} ({signal.reason})"
                                )
                                
                                # 実際の注文処理を実行
                                try:
                                    if signal.signal_type in [SignalType.LONG, SignalType.SHORT]:
                                        # 新規オープン注文
                                        side = "LONG" if signal.signal_type == SignalType.LONG else "SHORT"
                                        success, message, position = TradeMini._mp_position_manager.open_position(
                                            symbol, side, signal.price, signal.timestamp
                                        )
                                        
                                        if success and position:
                                            logger.info(f"✅ POSITION OPENED: {symbol} {side} @ {signal.price:.6f}")
                                        else:
                                            logger.error(f"❌ POSITION OPEN FAILED: {symbol} {side} - {message}")
                                    
                                    elif signal.signal_type == SignalType.CLOSE:
                                        # ポジションクローズ注文
                                        success, message, position = TradeMini._mp_position_manager.close_position(
                                            symbol, signal.reason
                                        )
                                        
                                        if success and position:
                                            logger.info(f"✅ POSITION CLOSED: {symbol} @ {signal.price:.6f} - {signal.reason}")
                                        else:
                                            logger.error(f"❌ POSITION CLOSE FAILED: {symbol} - {message}")
                                
                                except Exception as order_error:
                                    logger.error(f"❌ ORDER PROCESSING ERROR: {symbol} {signal.signal_type.value} - {order_error}")
                                    import traceback
                                    logger.error(f"Order error traceback: {traceback.format_exc()}")

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
            logger.error(f"Error in lightning processing: {e}")

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
                            self.position_manager.update_position_pnl(symbol, tick.price)

                            if signal and signal.signal_type != SignalType.NONE:
                                signals_count += 1
                                logger.info(
                                    f"🚨 SIGNAL: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f}"
                                )
                                
                                # 実際の注文処理を実行
                                try:
                                    if signal.signal_type in [SignalType.LONG, SignalType.SHORT]:
                                        # 新規オープン注文
                                        side = "LONG" if signal.signal_type == SignalType.LONG else "SHORT"
                                        success, message, position = self.position_manager.open_position(
                                            symbol, side, signal.price, signal.timestamp
                                        )
                                        
                                        if success and position:
                                            logger.info(f"✅ POSITION OPENED: {symbol} {side} @ {signal.price:.6f}")
                                        else:
                                            logger.error(f"❌ POSITION OPEN FAILED: {symbol} {side} - {message}")
                                    
                                    elif signal.signal_type == SignalType.CLOSE:
                                        # ポジションクローズ注文
                                        success, message, position = self.position_manager.close_position(
                                            symbol, signal.reason
                                        )
                                        
                                        if success and position:
                                            logger.info(f"✅ POSITION CLOSED: {symbol} @ {signal.price:.6f} - {signal.reason}")
                                        else:
                                            logger.error(f"❌ POSITION CLOSE FAILED: {symbol} - {message}")
                                
                                except Exception as order_error:
                                    logger.error(f"❌ ORDER PROCESSING ERROR: {symbol} {signal.signal_type.value} - {order_error}")

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
            logger.info(
                f"Tradeable symbols on Bybit: {symbol_stats.get('total_tradeable_symbols', 0)}"
            )
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
            last_health_check = time.time()
            while self.running and not self.shutdown_event.is_set():
                try:
                    await asyncio.sleep(1.0)

                    # 🩺 プロセスヘルスチェック（30秒毎）
                    current_time = time.time()
                    if current_time - last_health_check >= 30.0:
                        self._check_multiprocess_health()
                        last_health_check = current_time

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
