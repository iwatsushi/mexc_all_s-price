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
        self.symbol_table = config.get("symbol_table_name", "symbols")

        # バッファリング設定（パフォーマンス最適化）
        self.batch_size = 200  # バッチサイズを増大
        self.flush_interval = 3.0  # フラッシュ間隔を短縮

        # バッファ
        self.tick_buffer: Queue = Queue()
        self.ilp_write_queue: Queue = Queue()  # 専用ILPワーカー用キュー

        # 持続接続用ソケット（ILPワーカー専用）
        self._ilp_connection = None
        self._connection_lock = threading.Lock()

        # ワーカースレッド
        self.running = True
        self.tick_worker_thread = None
        self.ilp_worker_thread = None  # 専用ILPワーカー

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
        # ティックデータワーカー
        self.tick_worker_thread = threading.Thread(
            target=self._tick_worker, daemon=True, name="questdb_tick_worker"
        )
        self.tick_worker_thread.start()

        # 専用ILPワーカー
        self.ilp_worker_thread = threading.Thread(
            target=self._ilp_worker, daemon=True, name="questdb_ilp_worker"
        )
        self.ilp_worker_thread.start()

        logger.info("🚀 QuestDBワーカースレッド開始（ティック＋専用ILP）")

    def _get_connection(self) -> socket.socket:
        """持続接続を取得または作成（スレッドセーフ）"""
        with self._connection_lock:
            current_time = time.time()

            # 既存の接続が有効かチェック
            if (self._connection_socket is not None and
                current_time - self._last_connection_time < self._connection_timeout):
                try:
                    # 接続の生存確認
                    self._connection_socket.settimeout(1.0)
                    return self._connection_socket
                except (socket.error, OSError):
                    # 接続が無効になっている場合
                    self._connection_socket = None

            # 古い接続を閉じる
            if self._connection_socket is not None:
                try:
                    self._connection_socket.close()
                except:
                    pass
                self._connection_socket = None

            # 新しい接続を作成（複数回リトライ）
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5.0)
                    sock.connect((self.host, self.ilp_port))

                    self._connection_socket = sock
                    self._last_connection_time = current_time

                    return sock

                except (ConnectionRefusedError, OSError) as e:
                    if attempt < max_retries - 1:
                        time.sleep(0.1 * (2 ** attempt))  # 指数バックオフ
                        continue
                    else:
                        # 最終的に失敗した場合、例外を再発生
                        raise e

            # ここには到達しないはずだが、安全のため
            raise ConnectionError("Failed to establish persistent connection")

    def _ilp_worker(self):
        """専用ILPワーカー - 一つの永続接続で全ILP送信を処理"""
        logger.info("📡 専用ILPワーカー開始 - 永続接続によるデータ送信")

        connection = None
        reconnect_attempts = 0
        max_reconnect_attempts = 10

        while self.running:
            try:
                # 接続確立
                if connection is None:
                    try:
                        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        connection.settimeout(10.0)
                        connection.connect((self.host, self.ilp_port))
                        reconnect_attempts = 0
                        logger.info("🔗 ILP永続接続確立成功")
                    except Exception as e:
                        reconnect_attempts += 1
                        if reconnect_attempts <= max_reconnect_attempts:
                            wait_time = min(0.5 * (2 ** reconnect_attempts), 10.0)
                            logger.warning(f"ILP接続失敗 (試行{reconnect_attempts}/{max_reconnect_attempts}): {wait_time:.1f}秒後リトライ")
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"ILP接続失敗が続きます。60秒後に再試行します。")
                            time.sleep(60.0)
                            reconnect_attempts = 0
                            continue

                # キューからデータを取得
                try:
                    ilp_data = self.ilp_write_queue.get(timeout=1.0)

                    # データ送信
                    connection.sendall(ilp_data.encode('utf-8'))
                    self.stats["write_errors"] = 0  # 成功時はエラーカウントリセット

                except Empty:
                    continue
                except (socket.error, OSError, ConnectionError) as e:
                    logger.warning(f"ILP送信エラー、接続をリセット: {type(e).__name__}")
                    if connection:
                        try:
                            connection.close()
                        except:
                            pass
                        connection = None
                    # データを再キューに戻す
                    try:
                        self.ilp_write_queue.put_nowait(ilp_data)
                    except:
                        pass
                    continue

            except Exception as e:
                logger.error(f"ILPワーカー予期しないエラー: {e}")
                time.sleep(1.0)

        # 終了時の接続クリーンアップ
        if connection:
            try:
                connection.close()
            except:
                pass

        logger.info("🛑 専用ILPワーカー停止")

    def _send_ilp_data(self, data: str) -> bool:
        """ILPデータを専用ワーカーキューに送信"""
        try:
            self.ilp_write_queue.put_nowait(data)
            return True
        except Exception as e:
            logger.error(f"ILPキューへの送信失敗: {e}")
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
            logger.error(f"Error sending ILP lines to QuestDB: {type(e).__name__}: {e}")
            return 0

    def save_symbol_info(self, symbols: Dict[str, Any]) -> int:
        """
        🏷️ 銘柄情報をQuestDBに保存
        
        Args:
            symbols: 銘柄情報の辞書
        
        Returns:
            保存成功した件数
        """
        try:
            if not symbols:
                return 0
            
            ilp_lines = []
            current_time_ns = int(time.time() * 1_000_000_000)
            
            for symbol, info in symbols.items():
                # ILP形式: table,tag1=value1 field1=value1,field2=value2 timestamp
                line = (
                    f"{self.symbol_table},symbol={symbol} "
                    f"mexc_available={str(info.mexc_available).lower()},"
                    f"bybit_available={str(info.bybit_available).lower()} "
                    f"{current_time_ns}"
                )
                ilp_lines.append(line)
            
            # 直接送信
            if self._send_ilp_data("\n".join(ilp_lines) + "\n"):
                logger.info(f"🏷️ QuestDB: {len(symbols)}銘柄情報を保存")
                return len(symbols)
            else:
                logger.warning(f"❌ QuestDB: 銘柄情報保存失敗")
                return 0

        except Exception as e:
            logger.error(f"Error saving symbol info to QuestDB: {type(e).__name__}: {e}")
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
            
            # 銘柄管理テーブル
            symbol_table_sql = f"""
            CREATE TABLE {self.symbol_table} (
                symbol SYMBOL,
                mexc_available BOOLEAN,
                bybit_available BOOLEAN,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
            """

            logger.info("Table creation SQL prepared (manual execution required)")
            logger.info(f"Tick table SQL: {tick_table_sql}")
            logger.info(f"Symbol table SQL: {symbol_table_sql}")

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

        if self.ilp_worker_thread and self.ilp_worker_thread.is_alive():
            self.ilp_worker_thread.join(timeout=10.0)

        # 持続接続を閉じる
        with self._connection_lock:
            if self._ilp_connection is not None:
                try:
                    self._ilp_connection.close()
                except:
                    pass
                self._ilp_connection = None

        # 残りのバッファを処理
        self.flush_all()

        logger.info("✅ QuestDBクライアントシャットダウン完了")
