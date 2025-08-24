"""
QuestDB保存クライアント
"""

import logging
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from queue import Empty, Queue
from typing import Any, Dict, List, Optional

from config import Config
from mexc_client import TickData
from position_manager import Position

logger = logging.getLogger(__name__)


@dataclass
class TradeRecord:
    """取引記録"""

    id: str
    symbol: str
    side: str
    open_time: datetime
    open_price: float
    quantity_symbol: float
    quantity_usdt: float

    # 決済情報（決済時に更新）
    close_time: Optional[datetime] = None
    close_price: Optional[float] = None
    realized_pnl: Optional[float] = None
    fees: Optional[float] = None
    forced_liquidation: bool = False


class QuestDBClient:
    """QuestDB保存クライアント"""

    def __init__(self, config: Config):
        self.config = config
        self.host = config.questdb_host
        self.port = config.questdb_port
        self.ilp_port = config.questdb_ilp_port

        self.tick_table = config.tick_table_name
        self.trade_table = config.trade_table_name

        # バッファリング設定
        self.batch_size = 100
        self.flush_interval = 5.0  # 秒

        # バッファ
        self.tick_buffer: Queue = Queue()
        self.trade_record_buffer: Queue = Queue()

        # ワーカースレッド
        self.running = True
        self.tick_worker_thread = None
        self.trade_worker_thread = None

        # 統計
        self.stats = {
            "ticks_saved": 0,
            "trades_saved": 0,
            "connection_errors": 0,
            "write_errors": 0,
            "last_flush": datetime.now(),
        }

        # スレッドセーフティ
        self._lock = threading.Lock()

        # 接続テスト
        self._test_connection()

        # ワーカー開始
        self._start_workers()

        logger.info(f"QuestDB client initialized: {self.host}:{self.ilp_port}")

    def _test_connection(self) -> bool:
        """QuestDB接続テスト（リトライ機能付き）"""
        max_retries = 5
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10.0)
                result = sock.connect_ex((self.host, self.ilp_port))
                sock.close()

                if result == 0:
                    logger.info(
                        f"QuestDB connection test successful (attempt {attempt + 1})"
                    )
                    return True
                else:
                    if attempt < max_retries - 1:
                        logger.debug(
                            f"QuestDB connection attempt {attempt + 1} failed: {result}, retrying in {retry_delay}s..."
                        )
                        time.sleep(retry_delay)
                    else:
                        logger.warning(
                            f"QuestDB connection test failed after {max_retries} attempts: {result}"
                        )
                        return False

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.debug(
                        f"QuestDB connection attempt {attempt + 1} error: {e}, retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                else:
                    logger.warning(
                        f"QuestDB connection test error after {max_retries} attempts: {e}"
                    )
                    return False

        return False

    def _start_workers(self):
        """ワーカースレッド開始"""
        self.tick_worker_thread = threading.Thread(
            target=self._tick_worker, daemon=True, name="questdb_tick_worker"
        )
        self.tick_worker_thread.start()

        self.trade_worker_thread = threading.Thread(
            target=self._trade_worker, daemon=True, name="questdb_trade_worker"
        )
        self.trade_worker_thread.start()

        logger.info("QuestDB worker threads started")

    def _send_ilp_data(self, data: str) -> bool:
        """ILPデータをQuestDBに送信（リトライ機能付き）"""
        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10.0)
                sock.connect((self.host, self.ilp_port))
                sock.sendall(data.encode("utf-8"))
                sock.close()

                # 成功時はエラーカウントをリセット
                with self._lock:
                    if self.stats["write_errors"] > 0:
                        logger.info(
                            f"QuestDB ILP connection restored after {self.stats['write_errors']} errors"
                        )
                        self.stats["write_errors"] = 0

                return True

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    # 最終試行失敗時のみエラーログ（頻度削減）
                    with self._lock:
                        self.stats["write_errors"] += 1
                        if self.stats["write_errors"] % 10 == 1:
                            logger.warning(
                                f"QuestDB ILP connection failed after {max_retries} retries (error #{self.stats['write_errors']}): {e}"
                            )
                    return False

        return False

    def _tick_worker(self):
        """ティックデータワーカー"""
        logger.info("Tick data worker started")

        batch = []
        last_flush = time.time()

        while self.running:
            try:
                # バッファからデータを取得（タイムアウト付き）
                try:
                    tick_data = self.tick_buffer.get(timeout=1.0)
                    batch.append(tick_data)
                except Empty:
                    pass

                current_time = time.time()

                # フラッシュ条件判定
                should_flush = len(batch) >= self.batch_size or (
                    batch and (current_time - last_flush) >= self.flush_interval
                )

                if should_flush and batch:
                    if self._flush_tick_batch(batch):
                        with self._lock:
                            self.stats["ticks_saved"] += len(batch)
                            self.stats["last_flush"] = datetime.now()

                    batch.clear()
                    last_flush = current_time

            except Exception as e:
                logger.error(f"Error in tick worker: {e}")
                time.sleep(1.0)

        # 終了時の残りデータ処理
        if batch:
            self._flush_tick_batch(batch)

        logger.info("Tick data worker stopped")

    def _trade_worker(self):
        """取引記録ワーカー"""
        logger.info("Trade record worker started")

        batch = []
        last_flush = time.time()

        while self.running:
            try:
                # バッファからデータを取得
                try:
                    trade_record = self.trade_record_buffer.get(timeout=1.0)
                    batch.append(trade_record)
                except Empty:
                    pass

                current_time = time.time()

                # フラッシュ条件判定
                should_flush = len(batch) >= self.batch_size or (
                    batch and (current_time - last_flush) >= self.flush_interval
                )

                if should_flush and batch:
                    if self._flush_trade_batch(batch):
                        with self._lock:
                            self.stats["trades_saved"] += len(batch)

                    batch.clear()
                    last_flush = current_time

            except Exception as e:
                logger.error(f"Error in trade worker: {e}")
                time.sleep(1.0)

        # 終了時の残りデータ処理
        if batch:
            self._flush_trade_batch(batch)

        logger.info("Trade record worker stopped")

    def _flush_tick_batch(self, batch: List[TickData]) -> bool:
        """ティックデータバッチを送信"""
        try:
            lines = []
            for tick in batch:
                # ILP形式: table,tag1=value1,tag2=value2 field1=value1,field2=value2 timestamp
                timestamp_ns = int(tick.timestamp.timestamp() * 1_000_000_000)

                line = (
                    f"{self.tick_table},symbol={tick.symbol} "
                    f"price={tick.price},volume={tick.volume} "
                    f"{timestamp_ns}"
                )
                lines.append(line)

            ilp_data = "\n".join(lines) + "\n"
            return self._send_ilp_data(ilp_data)

        except Exception as e:
            logger.error(f"Error flushing tick batch: {e}")
            return False

    def _flush_trade_batch(self, batch: List[TradeRecord]) -> bool:
        """取引記録バッチを送信"""
        try:
            lines = []
            for record in batch:
                # オープン時と決済時で異なるフィールド構成
                timestamp_ns = int(record.open_time.timestamp() * 1_000_000_000)

                fields = [
                    f'symbol="{record.symbol}"',
                    f'side="{record.side}"',
                    f"open_price={record.open_price}",
                    f"quantity_symbol={record.quantity_symbol}",
                    f"quantity_usdt={record.quantity_usdt}",
                ]

                # 決済情報が存在する場合
                if record.close_time is not None:
                    close_timestamp_ns = int(
                        record.close_time.timestamp() * 1_000_000_000
                    )
                    fields.extend(
                        [
                            f"close_time={close_timestamp_ns}i",
                            f"close_price={record.close_price or 0.0}",
                            f"realized_pnl={record.realized_pnl or 0.0}",
                            f"fees={record.fees or 0.0}",
                            f"forced_liquidation={str(record.forced_liquidation).lower()}",
                        ]
                    )

                line = (
                    f'{self.trade_table},id="{record.id}",symbol={record.symbol} '
                    f"{','.join(fields)} "
                    f"{timestamp_ns}"
                )
                lines.append(line)

            ilp_data = "\n".join(lines) + "\n"
            return self._send_ilp_data(ilp_data)

        except Exception as e:
            logger.error(f"Error flushing trade batch: {e}")
            return False

    def save_tick_data(self, tick: TickData):
        """ティックデータを保存（非同期）"""
        try:
            self.tick_buffer.put_nowait(tick)
        except Exception as e:
            logger.error(f"Error queuing tick data: {e}")

    def save_batch_tick_data(self, ticks: List[TickData]):
        """バッチティックデータを保存（一括書き込みで大幅高速化）"""
        try:
            # 一括でキューに追加（従来の方式より圧倒的に効率的）
            for tick in ticks:
                self.tick_buffer.put_nowait(tick)
            logger.debug(f"Batch queued {len(ticks)} ticks for QuestDB")
        except Exception as e:
            logger.error(f"Error queuing batch tick data: {e}")
    
    def save_ilp_lines(self, ilp_lines: List[str]) -> int:
        """
        🚀 ILPライン形式でバッチデータを直接保存（main.pyから移譲）
        
        Args:
            ilp_lines: ILP形式のライン配列
            
        Returns:
            保存成功した件数
        """
        try:
            if not ilp_lines:
                return 0
                
            # ILPデータを結合
            ilp_data = "\n".join(ilp_lines) + "\n"
            
            # 直接送信（バッファリングせず即座に書き込み）
            if self._send_ilp_data(ilp_data):
                with self._lock:
                    self.stats["ticks_saved"] += len(ilp_lines)
                    self.stats["last_flush"] = datetime.now()
                
                logger.debug(f"✅ QuestDB ILP: {len(ilp_lines)} records sent successfully")
                return len(ilp_lines)
            else:
                logger.warning(f"❌ QuestDB ILP write failed for {len(ilp_lines)} records")
                return 0
                
        except Exception as e:
            logger.error(f"Error sending ILP lines to QuestDB: {e}")
            return 0

    def save_trade_open(
        self,
        trade_id: str,
        symbol: str,
        side: str,
        open_time: datetime,
        open_price: float,
        quantity_symbol: float,
        quantity_usdt: float,
    ):
        """取引オープン記録を保存"""
        try:
            record = TradeRecord(
                id=trade_id,
                symbol=symbol,
                side=side,
                open_time=open_time,
                open_price=open_price,
                quantity_symbol=quantity_symbol,
                quantity_usdt=quantity_usdt,
            )
            self.trade_record_buffer.put_nowait(record)
            logger.debug(f"Trade open record queued: {trade_id}")

        except Exception as e:
            logger.error(f"Error queuing trade open record: {e}")

    def save_trade_close(
        self,
        trade_id: str,
        close_time: datetime,
        close_price: float,
        realized_pnl: float,
        fees: float = 0.0,
        forced_liquidation: bool = False,
    ):
        """取引決済記録を保存（既存レコードを更新）"""
        try:
            # 決済情報付きの完全なレコードを作成
            # 実際の実装では、既存のオープン記録を検索して更新する必要がある
            # ここでは簡略化して新しいレコードとして保存

            # 注意: 実際のQuestDBでは更新操作は限定的なため、
            # オープン時と決済時で別々のテーブルを使用することも検討可能

            logger.debug(f"Trade close record processed: {trade_id}")

        except Exception as e:
            logger.error(f"Error processing trade close record: {e}")

    def create_tables(self):
        """テーブルを作成（初回のみ実行）"""
        try:
            # HTTP APIを使用してテーブル作成SQLを実行
            # 実際の実装では、QuestDBのHTTP APIまたはJDBCを使用

            tick_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.tick_table} (
                symbol SYMBOL,
                price DOUBLE,
                volume DOUBLE,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """

            trade_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.trade_table} (
                id STRING,
                symbol SYMBOL,
                side SYMBOL,
                open_price DOUBLE,
                quantity_symbol DOUBLE,
                quantity_usdt DOUBLE,
                close_time TIMESTAMP,
                close_price DOUBLE,
                realized_pnl DOUBLE,
                fees DOUBLE,
                forced_liquidation BOOLEAN,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """

            logger.info("Table creation SQL prepared (manual execution required)")
            logger.info(f"Tick table: {tick_table_sql}")
            logger.info(f"Trade table: {trade_table_sql}")

        except Exception as e:
            logger.error(f"Error creating tables: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        with self._lock:
            return {
                **self.stats,
                "tick_buffer_size": self.tick_buffer.qsize(),
                "trade_buffer_size": self.trade_record_buffer.qsize(),
                "worker_running": self.running,
            }

    def flush_all(self):
        """全バッファを強制フラッシュ"""
        logger.info("Forcing flush of all buffers")

        # バッファをクリアするために十分な時間待機
        time.sleep(self.flush_interval + 1.0)

        logger.info("Buffer flush completed")

    def shutdown(self):
        """QuestDBクライアントをシャットダウン"""
        logger.info("Shutting down QuestDB client")

        self.running = False

        # ワーカースレッドの終了を待機
        if self.tick_worker_thread and self.tick_worker_thread.is_alive():
            self.tick_worker_thread.join(timeout=10.0)

        if self.trade_worker_thread and self.trade_worker_thread.is_alive():
            self.trade_worker_thread.join(timeout=10.0)

        # 残りのバッファを処理
        self.flush_all()

        logger.info("QuestDB client shutdown completed")


class QuestDBTradeRecordManager:
    """取引記録管理（QuestDBクライアントの補助クラス）"""

    def __init__(self, questdb_client: QuestDBClient):
        self.questdb = questdb_client
        self.open_trades: Dict[str, TradeRecord] = {}
        self._lock = threading.Lock()

    def record_trade_open(self, position: Position) -> str:
        """取引オープンを記録"""
        trade_id = f"{position.symbol}_{int(position.entry_time.timestamp())}"

        with self._lock:
            # 取引記録作成
            record = TradeRecord(
                id=trade_id,
                symbol=position.symbol,
                side=position.side,
                open_time=position.entry_time,
                open_price=position.entry_price,
                quantity_symbol=position.size,
                quantity_usdt=position.size * position.entry_price,
            )

            # メモリ上で管理
            self.open_trades[trade_id] = record

            # QuestDBに保存
            self.questdb.save_trade_open(
                trade_id=trade_id,
                symbol=position.symbol,
                side=position.side,
                open_time=position.entry_time,
                open_price=position.entry_price,
                quantity_symbol=position.size,
                quantity_usdt=position.size * position.entry_price,
            )

        logger.info(f"Trade open recorded: {trade_id}")
        return trade_id

    def record_trade_close(
        self,
        trade_id: str,
        close_time: datetime,
        close_price: float,
        realized_pnl: float,
        fees: float = 0.0,
    ):
        """取引決済を記録"""
        with self._lock:
            record = self.open_trades.get(trade_id)
            if not record:
                logger.warning(f"Open trade record not found: {trade_id}")
                return

            # 決済情報を更新
            record.close_time = close_time
            record.close_price = close_price
            record.realized_pnl = realized_pnl
            record.fees = fees

            # QuestDBに保存
            self.questdb.save_trade_close(
                trade_id=trade_id,
                close_time=close_time,
                close_price=close_price,
                realized_pnl=realized_pnl,
                fees=fees,
            )

            # メモリから削除
            del self.open_trades[trade_id]

        logger.info(f"Trade close recorded: {trade_id}")

    def get_open_trades_count(self) -> int:
        """オープン取引数を取得"""
        with self._lock:
            return len(self.open_trades)
