"""
取引戦略クラス
"""

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple

from config import Config
from data_manager import DataManager
from mexc_client import TickData

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """シグナルタイプ"""

    LONG = "LONG"
    SHORT = "SHORT"
    CLOSE = "CLOSE"
    NONE = "NONE"


class ExitReason(Enum):
    """決済理由"""

    REVERSAL = "REVERSAL"  # 反発による決済
    STOP_LOSS = "STOP_LOSS"  # 損切り
    BREAKEVEN_STOP = "BREAKEVEN_STOP"  # 建値決済


@dataclass
class TradingSignal:
    """取引シグナル"""

    symbol: str
    signal_type: SignalType
    price: float
    timestamp: datetime
    reason: str = ""
    confidence: float = 1.0  # シグナルの信頼度（0-1）


@dataclass
class PositionTracker:
    """ポジション追跡データ"""

    symbol: str
    side: str  # "LONG" or "SHORT"
    entry_price: float
    entry_time: datetime
    size: float

    # 価格追跡
    highest_price_after_entry: float = 0.0
    lowest_price_after_entry: float = float("inf")

    # 利益管理
    max_profit_percent: float = 0.0
    min_profit_threshold_reached: bool = False
    breakeven_stop_set: bool = False

    # 統計
    price_updates: int = 0
    last_update: datetime = None


class TradingStrategy:
    """取引戦略クラス"""

    def __init__(self, config: Config, data_manager: DataManager):
        self.config = config
        self.data_manager = data_manager

        # 戦略パラメータ
        self.price_comparison_seconds = config.price_comparison_seconds
        self.long_threshold = config.long_threshold_percent
        self.short_threshold = config.short_threshold_percent
        self.reversal_threshold = config.reversal_threshold_percent
        self.min_profit_percent = config.min_profit_percent

        # ポジション追跡
        self.position_trackers: Dict[str, PositionTracker] = {}

        # 統計情報
        self.stats = {
            "signals_generated": 0,
            "long_signals": 0,
            "short_signals": 0,
            "close_signals": 0,
            "active_positions": 0,
            "total_positions_tracked": 0,
        }

        # スレッドセーフティ
        self._lock = threading.Lock()

        logger.info(f"Trading strategy initialized with parameters:")
        logger.info(f"  - Price comparison: {self.price_comparison_seconds}s")
        logger.info(f"  - Long threshold: {self.long_threshold}%")
        logger.info(f"  - Short threshold: {self.short_threshold}%")
        logger.info(f"  - Reversal threshold: {self.reversal_threshold}%")
        logger.info(f"  - Min profit threshold: {self.min_profit_percent}%")

    def analyze_tick(self, tick: TickData) -> TradingSignal:
        """
        ティックデータを分析してシグナルを生成

        Args:
            tick: 最新ティックデータ

        Returns:
            取引シグナル
        """
        with self._lock:
            # 既存ポジションの更新
            if tick.symbol in self.position_trackers:
                self._update_position_tracker(tick)

                # 決済シグナルをチェック
                close_signal = self._check_close_signal(
                    tick.symbol, tick.price, tick.timestamp
                )
                if close_signal.signal_type != SignalType.NONE:
                    return close_signal

            # 新規エントリーシグナルをチェック
            return self._check_entry_signal(tick)

    def _check_entry_signal(self, tick: TickData) -> TradingSignal:
        """新規エントリーシグナルをチェック"""
        # 既にポジションがある場合は新規エントリーしない
        if tick.symbol in self.position_trackers:
            return TradingSignal(
                symbol=tick.symbol,
                signal_type=SignalType.NONE,
                price=tick.price,
                timestamp=tick.timestamp,
            )

        # 価格変動率を取得
        change_percent = self.data_manager.get_price_change_percent(
            tick.symbol, self.price_comparison_seconds
        )

        if change_percent is None:
            return TradingSignal(
                symbol=tick.symbol,
                signal_type=SignalType.NONE,
                price=tick.price,
                timestamp=tick.timestamp,
            )

        signal_type = SignalType.NONE
        reason = ""

        # ロングシグナル判定
        if change_percent >= self.long_threshold:
            signal_type = SignalType.LONG
            reason = f"Price increased {change_percent:.2f}% in {self.price_comparison_seconds}s"
            self.stats["long_signals"] += 1

        # ショートシグナル判定
        elif change_percent <= -self.short_threshold:
            signal_type = SignalType.SHORT
            reason = f"Price decreased {change_percent:.2f}% in {self.price_comparison_seconds}s"
            self.stats["short_signals"] += 1

        if signal_type != SignalType.NONE:
            self.stats["signals_generated"] += 1

        return TradingSignal(
            symbol=tick.symbol,
            signal_type=signal_type,
            price=tick.price,
            timestamp=tick.timestamp,
            reason=reason,
            confidence=min(
                abs(change_percent) / max(self.long_threshold, self.short_threshold),
                2.0,
            ),
        )

    def _check_close_signal(
        self, symbol: str, current_price: float, timestamp: datetime
    ) -> TradingSignal:
        """決済シグナルをチェック"""
        tracker = self.position_trackers.get(symbol)
        if not tracker:
            return TradingSignal(
                symbol=symbol,
                signal_type=SignalType.NONE,
                price=current_price,
                timestamp=timestamp,
            )

        # 現在の利益率を計算
        if tracker.side == "LONG":
            profit_percent = (
                (current_price - tracker.entry_price) / tracker.entry_price
            ) * 100.0
        else:  # SHORT
            profit_percent = (
                (tracker.entry_price - current_price) / tracker.entry_price
            ) * 100.0

        # 利益閾値を超えたかチェック
        if profit_percent > tracker.max_profit_percent:
            tracker.max_profit_percent = profit_percent

            # 最小利益閾値を超えた場合
            if (
                not tracker.min_profit_threshold_reached
                and profit_percent >= self.min_profit_percent
            ):
                tracker.min_profit_threshold_reached = True
                tracker.breakeven_stop_set = True
                logger.info(
                    f"{symbol}: Min profit threshold reached. Breakeven stop set."
                )

        # 反発による決済判定
        reversal_signal = self._check_reversal_exit(tracker, current_price)
        if reversal_signal:
            return TradingSignal(
                symbol=symbol,
                signal_type=SignalType.CLOSE,
                price=current_price,
                timestamp=timestamp,
                reason=f"Reversal detected: {reversal_signal}",
                confidence=1.0,
            )

        return TradingSignal(
            symbol=symbol,
            signal_type=SignalType.NONE,
            price=current_price,
            timestamp=timestamp,
        )

    def _check_reversal_exit(
        self, tracker: PositionTracker, current_price: float
    ) -> Optional[str]:
        """反発による決済をチェック"""
        # 最小利益を一度も達成していない場合は反発判定しない
        if not tracker.min_profit_threshold_reached:
            return None

        reversal_percent = 0.0

        if tracker.side == "LONG":
            # ロングポジション：最高値からの下落率
            if tracker.highest_price_after_entry > 0:
                reversal_percent = (
                    (tracker.highest_price_after_entry - current_price)
                    / tracker.highest_price_after_entry
                ) * 100.0
        else:
            # ショートポジション：最安値からの上昇率
            if tracker.lowest_price_after_entry < float("inf"):
                reversal_percent = (
                    (current_price - tracker.lowest_price_after_entry)
                    / tracker.lowest_price_after_entry
                ) * 100.0

        # 反発閾値を超えているかチェック
        if reversal_percent >= self.reversal_threshold:
            return f"{reversal_percent:.2f}% reversal from {'high' if tracker.side == 'LONG' else 'low'}"

        return None

    def _update_position_tracker(self, tick: TickData):
        """ポジション追跡データを更新"""
        tracker = self.position_trackers.get(tick.symbol)
        if not tracker:
            return

        # 価格の最高値・最安値を更新
        if tracker.side == "LONG":
            if tick.price > tracker.highest_price_after_entry:
                tracker.highest_price_after_entry = tick.price
        else:  # SHORT
            if tick.price < tracker.lowest_price_after_entry:
                tracker.lowest_price_after_entry = tick.price

        # 統計更新
        tracker.price_updates += 1
        tracker.last_update = tick.timestamp

    def add_position(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        size: float,
        entry_time: datetime,
    ):
        """
        ポジション追跡を開始

        Args:
            symbol: 銘柄
            side: "LONG" or "SHORT"
            entry_price: エントリー価格
            size: ポジションサイズ
            entry_time: エントリー時刻
        """
        with self._lock:
            tracker = PositionTracker(
                symbol=symbol,
                side=side,
                entry_price=entry_price,
                entry_time=entry_time,
                size=size,
                highest_price_after_entry=entry_price,
                lowest_price_after_entry=entry_price,
                last_update=entry_time,
            )

            self.position_trackers[symbol] = tracker
            self.stats["active_positions"] = len(self.position_trackers)
            self.stats["total_positions_tracked"] += 1

            logger.info(f"Position tracking started: {symbol} {side} @ {entry_price}")

    def remove_position(self, symbol: str) -> Optional[PositionTracker]:
        """
        ポジション追跡を終了

        Args:
            symbol: 銘柄

        Returns:
            削除されたポジション追跡データ
        """
        with self._lock:
            tracker = self.position_trackers.pop(symbol, None)
            if tracker:
                self.stats["active_positions"] = len(self.position_trackers)
                logger.info(f"Position tracking ended: {symbol}")
            return tracker

    def get_position_tracker(self, symbol: str) -> Optional[PositionTracker]:
        """指定銘柄のポジション追跡データを取得"""
        with self._lock:
            return self.position_trackers.get(symbol)

    def get_active_positions(self) -> Dict[str, PositionTracker]:
        """アクティブなポジション追跡データを取得"""
        with self._lock:
            return self.position_trackers.copy()

    def get_signals_summary(self, symbol: str = None) -> Dict[str, any]:
        """指定した期間のシグナル要約を取得"""
        summary = {
            "total_signals": self.stats["signals_generated"],
            "long_signals": self.stats["long_signals"],
            "short_signals": self.stats["short_signals"],
            "close_signals": self.stats["close_signals"],
            "active_positions": self.stats["active_positions"],
        }

        if symbol and symbol in self.position_trackers:
            tracker = self.position_trackers[symbol]
            summary["position_info"] = {
                "side": tracker.side,
                "entry_price": tracker.entry_price,
                "max_profit_percent": tracker.max_profit_percent,
                "min_threshold_reached": tracker.min_profit_threshold_reached,
                "price_updates": tracker.price_updates,
            }

        return summary

    def get_stats(self) -> Dict[str, any]:
        """戦略統計を取得"""
        with self._lock:
            return {
                **self.stats,
                "strategy_params": {
                    "price_comparison_seconds": self.price_comparison_seconds,
                    "long_threshold": self.long_threshold,
                    "short_threshold": self.short_threshold,
                    "reversal_threshold": self.reversal_threshold,
                    "min_profit_percent": self.min_profit_percent,
                },
                "active_position_details": {
                    symbol: {
                        "side": tracker.side,
                        "entry_price": tracker.entry_price,
                        "max_profit": tracker.max_profit_percent,
                        "updates": tracker.price_updates,
                    }
                    for symbol, tracker in self.position_trackers.items()
                },
            }
