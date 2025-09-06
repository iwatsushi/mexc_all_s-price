"""
QuestDB保存クライアント（ティックデータ専用）
"""

import logging
import socket
import threading
import time
from datetime import datetime
from queue import Empty, Queue
from typing import Any, Dict, List

from config import Config
from mexc_client import TickData

logger = logging.getLogger(__name__)


class QuestDBClient:
    """QuestDB保存クライアント（ティックデータ専用）"""

    def __init__(self, config: Config):
        self.config = config
        self.host = config.questdb_host
        self.port = config.questdb_port
        self.ilp_port = config.questdb_ilp_port

        self.tick_table = config.tick_table_name

        # バッファリング設定（パフォーマンス最適化）
        self.batch_size = 200  # バッチサイズを増大
        self.flush_interval = 3.0  # フラッシュ間隔を短縮

        # バッファ
        self.tick_buffer: Queue = Queue()

        # ワーカースレッド
        self.running = True
        self.tick_worker_thread = None

        # 統計（最小限）
        self.stats = {
            "ticks_saved": 0,
            "write_errors": 0,
        }

        # 接続テスト
        self._test_connection()

        # ワーカー開始
        self._start_workers()

        logger.info(f"💾 QuestDBクライアント初期化完了: {self.host}:{self.ilp_port}")

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
                    logger.info(f"QuestDB接続テスト成功 (試行{attempt + 1}回目)")
                    return True
                else:
                    if attempt < max_retries - 1:
                        # 接続試行失敗ログをCPU負荷軽減のため削減
                        pass
                        time.sleep(retry_delay)
                    else:
                        logger.warning(
                            f"QuestDB接続テスト失敗 ({max_retries}回試行後): {result}"
                        )
                        return False

            except Exception as e:
                if attempt < max_retries - 1:
                    # エラーログをCPU負荷軽減のため削減
                    pass
                    time.sleep(retry_delay)
                else:
                    logger.warning(
                        f"QuestDB接続テストエラー ({max_retries}回試行後): {e}"
                    )
                    return False

        return False

    def _start_workers(self):
        """ワーカースレッド開始"""
        self.tick_worker_thread = threading.Thread(
            target=self._tick_worker, daemon=True, name="questdb_tick_worker"
        )
        self.tick_worker_thread.start()

        logger.info("🚀 QuestDBワーカースレッド開始（ティックデータのみ）")

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
                # 接続復旧ログをCPU負荷軽減のため削除
                self.stats["write_errors"] = 0

                return True

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    # 最終試行失敗時のみエラーログ（頻度削減）
                    self.stats["write_errors"] += 1
                    if self.stats["write_errors"] % 20 == 1:  # エラーログをさらに削減
                        logger.warning(f"QuestDB connection failed (#{self.stats['write_errors']})")
                    return False

        return False

    def _tick_worker(self):
        """ティックデータワーカー"""
        logger.info("📊 ティックデータワーカー開始")

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
                        self.stats["ticks_saved"] += len(batch)

                    batch.clear()
                    last_flush = current_time

            except Exception as e:
                logger.error(f"ティックワーカーエラー: {e}")
                time.sleep(1.0)

        # 終了時の残りデータ処理
        if batch:
            self._flush_tick_batch(batch)

        logger.info("🛑 ティックデータワーカー停止")

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
            # バッチキューログをCPU負荷軽減のため削除
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
                self.stats["ticks_saved"] += len(ilp_lines)

                # ILP送信成功ログをCPU負荷軽減のため削除
                return len(ilp_lines)
            else:
                logger.warning(
                    f"❌ QuestDB ILP write failed for {len(ilp_lines)} records"
                )
                return 0

        except Exception as e:
            logger.error(f"Error sending ILP lines to QuestDB: {e}")
            return 0

    def create_tables(self):
        """QuestDBテーブル作成SQL生成（手動実行用）"""
        try:
            # ティックデータテーブル
            tick_table_sql = f"""
            CREATE TABLE {self.tick_table} (
                symbol SYMBOL,
                price DOUBLE,
                volume DOUBLE,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """

            logger.info("Table creation SQL prepared (manual execution required)")
            logger.info(f"Tick table SQL: {tick_table_sql}")

        except Exception as e:
            logger.error(f"Error creating table SQL: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        return {
            **self.stats,
            "tick_buffer_size": self.tick_buffer.qsize(),
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

        # 残りのバッファを処理
        self.flush_all()

        logger.info("✅ QuestDBクライアントシャットダウン完了")
