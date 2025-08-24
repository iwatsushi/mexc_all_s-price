"""
ティックデータ管理クラス
"""

import logging
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, Optional

from config import Config
from mexc_client import TickData

logger = logging.getLogger(__name__)


class SymbolTickData:
    """個別銘柄のティックデータ管理"""

    def __init__(self, symbol: str, retention_hours: int = 6):
        self.symbol = symbol
        self.retention_hours = retention_hours
        self.retention_seconds = retention_hours * 3600

        # ティックデータを時系列で保持（最新が右端）
        self.tick_data: Deque[TickData] = deque()

        # 高速アクセス用インデックス（タイムスタンプ -> TickData）
        self.timestamp_index: Dict[int, TickData] = {}

        # 最新データのキャッシュ
        self.latest_tick: Optional[TickData] = None

        # 🚀 クリーンアップ頻度制御
        self.last_cleanup_time = 0
        self.cleanup_interval = 300  # 5分に1回のみ

        # 統計情報
        self.stats = {
            "total_ticks": 0,
            "oldest_tick": None,
            "newest_tick": None,
            "price_updates": 0,
        }

        # スレッドセーフティ用ロック
        self._lock = threading.Lock()

    def add_tick(self, tick: TickData):
        """ティックデータを追加（高速化版）"""
        with self._lock:
            # 重複データのチェック（高速化）
            if tick.timestamp in self.timestamp_index:
                # 価格が更新された場合のみ処理
                existing_tick = self.timestamp_index[tick.timestamp]
                if existing_tick.price != tick.price:
                    # 既存データを更新
                    existing_tick.price = tick.price
                    existing_tick.volume = tick.volume
                    self.stats["price_updates"] += 1
                return

            # 新しいティックデータを追加（高速化）
            self.tick_data.append(tick)
            self.timestamp_index[tick.timestamp] = tick
            self.latest_tick = tick
            self.stats["total_ticks"] += 1

            # 統計更新（高速化）
            if (
                not self.stats["oldest_tick"]
                or tick.timestamp < self.stats["oldest_tick"]
            ):
                self.stats["oldest_tick"] = tick.timestamp
            if (
                not self.stats["newest_tick"]
                or tick.timestamp > self.stats["newest_tick"]
            ):
                self.stats["newest_tick"] = tick.timestamp

            # 🚀 クリーンアップ頻度制御（5分に1回のみ）
            import time
            current_time = time.time()
            if current_time - self.last_cleanup_time > self.cleanup_interval:
                self._cleanup_old_data()
                self.last_cleanup_time = current_time

    def _cleanup_old_data(self):
        """古いデータを削除（retention_hours を超えたデータ）"""
        if not self.tick_data:
            return

        cutoff_time_ns = int((time.time() - self.retention_seconds) * 1_000_000_000)

        # dequeの左端から古いデータを削除
        while self.tick_data and self.tick_data[0].timestamp < cutoff_time_ns:
            old_tick = self.tick_data.popleft()
            # インデックスからも削除
            self.timestamp_index.pop(old_tick.timestamp, None)

        # 統計の最古時刻を更新
        if self.tick_data:
            self.stats["oldest_tick"] = self.tick_data[0].timestamp

    def get_price_n_seconds_ago(self, n_seconds: int) -> Optional[float]:
        """🚀 N秒前の価格を高速取得（早期終了版）"""
        with self._lock:
            if not self.latest_tick or len(self.tick_data) < 2:
                return None

            if not isinstance(self.latest_tick.timestamp, int):
                return None

            try:
                target_time_ns = self.latest_tick.timestamp - (
                    n_seconds * 1_000_000_000
                )
            except (TypeError, AttributeError):
                return None

            # 🚀 逆順検索 + 早期終了で高速化
            closest_tick = None
            min_time_diff_ns = float("inf")

            for tick in reversed(self.tick_data):
                if not isinstance(tick.timestamp, int):
                    continue

                time_diff_ns = abs(tick.timestamp - target_time_ns)

                # より良い候補が見つかったら更新
                if time_diff_ns < min_time_diff_ns:
                    min_time_diff_ns = time_diff_ns
                    closest_tick = tick

                    # 🚀 十分に近い値が見つかったら早期終了
                    if time_diff_ns < 100_000_000:  # 0.1秒以内なら十分
                        break

                # 目標時刻を大幅に過ぎたら検索終了
                if tick.timestamp < target_time_ns - 5_000_000_000:  # 5秒以上古い
                    break

            return closest_tick.price if closest_tick else None

    def get_latest_price(self) -> Optional[float]:
        """最新価格を取得"""
        with self._lock:
            return self.latest_tick.price if self.latest_tick else None

    def get_price_change_percent(self, n_seconds: int) -> Optional[float]:
        """N秒前からの価格変動率（%）を計算"""
        with self._lock:
            if not self.latest_tick:
                return None

            past_price = self.get_price_n_seconds_ago(n_seconds)
            if past_price is None or past_price == 0:
                return None

            current_price = self.latest_tick.price
            change_percent = ((current_price - past_price) / past_price) * 100.0

            return change_percent

    def get_data_count(self) -> int:
        """保持しているデータ数を取得"""
        with self._lock:
            return len(self.tick_data)

    def get_time_range(self) -> tuple[Optional[int], Optional[int]]:
        """データの時間範囲を取得（ナノ秒単位のタイムスタンプ）"""
        with self._lock:
            if not self.tick_data:
                return None, None
            return self.tick_data[0].timestamp, self.tick_data[-1].timestamp

    def get_all_ticks(self, limit: Optional[int] = None) -> List[TickData]:
        """全ティックデータを取得（最新からlimit件）"""
        with self._lock:
            if limit is None:
                return list(self.tick_data)
            else:
                return list(self.tick_data)[-limit:] if limit > 0 else []


class DataManager:
    """全銘柄のティックデータ管理"""

    def __init__(self, config: Config):
        self.config = config
        self.retention_hours = config.tick_data_retention_hours

        # 銘柄別データ管理
        self.symbol_data: Dict[str, SymbolTickData] = {}

        # 統計情報
        self.stats = {
            "active_symbols": 0,
            "total_ticks": 0,
            "start_time": time.time(),  # UNIX秒単位
            "last_cleanup": time.time(),  # UNIX秒単位
        }

        # スレッドセーフティ用ロック
        self._lock = threading.Lock()

        # クリーンアップタイマー
        self._cleanup_timer = None
        self._setup_cleanup_timer()

    def _setup_cleanup_timer(self):
        """定期クリーンアップタイマーを設定"""

        def cleanup_task():
            self._periodic_cleanup()
            # 次のクリーンアップをスケジュール
            self._cleanup_timer = threading.Timer(300, cleanup_task)  # 5分間隔
            self._cleanup_timer.daemon = True
            self._cleanup_timer.start()

        self._cleanup_timer = threading.Timer(300, cleanup_task)
        self._cleanup_timer.daemon = True
        self._cleanup_timer.start()

    def add_tick(self, tick: TickData):
        """ティックデータを追加"""
        with self._lock:
            # 銘柄データが存在しない場合は作成
            if tick.symbol not in self.symbol_data:
                self.symbol_data[tick.symbol] = SymbolTickData(
                    tick.symbol, self.retention_hours
                )
                self.stats["active_symbols"] = len(self.symbol_data)

            # ティックデータを追加
            self.symbol_data[tick.symbol].add_tick(tick)
            self.stats["total_ticks"] += 1

    def get_symbol_data(self, symbol: str) -> Optional[SymbolTickData]:
        """特定銘柄のデータ管理オブジェクトを取得"""
        with self._lock:
            return self.symbol_data.get(symbol)

    def get_price_change_percent(self, symbol: str, n_seconds: int) -> Optional[float]:
        """指定銘柄のN秒前からの価格変動率を取得"""
        symbol_data = self.get_symbol_data(symbol)
        return symbol_data.get_price_change_percent(n_seconds) if symbol_data else None

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """指定銘柄の最新価格を取得"""
        symbol_data = self.get_symbol_data(symbol)
        return symbol_data.get_latest_price() if symbol_data else None

    def get_active_symbols(self) -> List[str]:
        """アクティブな銘柄一覧を取得"""
        with self._lock:
            return list(self.symbol_data.keys())

    def get_symbols_with_significant_change(
        self, n_seconds: int, long_threshold: float, short_threshold: float
    ) -> Dict[str, float]:
        """
        大きな価格変動のある銘柄を取得

        Args:
            n_seconds: 比較対象の秒数
            long_threshold: ロング判定閾値（%）
            short_threshold: ショート判定閾値（%、負の値）

        Returns:
            {symbol: change_percent} の辞書
        """
        significant_changes = {}

        with self._lock:
            for symbol, symbol_data in self.symbol_data.items():
                change_percent = symbol_data.get_price_change_percent(n_seconds)

                if change_percent is not None:
                    # ロング条件
                    if change_percent >= long_threshold:
                        significant_changes[symbol] = change_percent
                    # ショート条件
                    elif change_percent <= -short_threshold:
                        significant_changes[symbol] = change_percent

        return significant_changes

    def get_all_price_changes_batch(self, n_seconds: int) -> Dict[str, float]:
        """
        🚀 全銘柄の価格変化率を一括計算（2秒周期最適化）

        個別計算の代わりに一括処理でパフォーマンス向上
        """
        changes = {}

        with self._lock:
            for symbol, symbol_data in self.symbol_data.items():
                change_percent = symbol_data.get_price_change_percent(n_seconds)
                if change_percent is not None:
                    changes[symbol] = change_percent

        return changes

    def _periodic_cleanup(self):
        """定期的なクリーンアップ処理"""
        logger.debug("Performing periodic data cleanup")

        with self._lock:
            # 各銘柄のデータをクリーンアップ
            empty_symbols = []

            for symbol, symbol_data in self.symbol_data.items():
                symbol_data._cleanup_old_data()

                # データが空の銘柄をマーク
                if symbol_data.get_data_count() == 0:
                    empty_symbols.append(symbol)

            # 空の銘柄データを削除
            for symbol in empty_symbols:
                del self.symbol_data[symbol]

            # 統計更新
            self.stats["active_symbols"] = len(self.symbol_data)
            self.stats["last_cleanup"] = time.time()

            logger.debug(
                f"Cleanup completed. Active symbols: {self.stats['active_symbols']}"
            )

    def get_stats(self) -> Dict[str, any]:
        """統計情報を取得"""
        with self._lock:
            stats = self.stats.copy()

            # 各銘柄の詳細統計
            symbol_stats = {}
            for symbol, symbol_data in self.symbol_data.items():
                symbol_stats[symbol] = {
                    "data_count": symbol_data.get_data_count(),
                    "latest_price": symbol_data.get_latest_price(),
                    "time_range": symbol_data.get_time_range(),
                }

            stats["symbols"] = symbol_stats
            stats["runtime"] = time.time() - stats["start_time"]

            return stats

    def shutdown(self):
        """データマネージャーをシャットダウン"""
        logger.info("🛑 データマネージャシャットダウン")

        if self._cleanup_timer:
            self._cleanup_timer.cancel()

        with self._lock:
            self.symbol_data.clear()
            self.stats["active_symbols"] = 0

        logger.info("✅ データマネージャシャットダウン完了")
