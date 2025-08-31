"""
取引戦略クラス
"""

import logging
import threading
import time
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

    def __init__(
        self,
        config: Config,
        data_manager: DataManager,
        position_manager=None,
        questdb_client=None,
        symbol_mapper=None,
        main_stats=None,
    ):
        self.config = config
        self.data_manager = data_manager

        # 統計表示用のコンポーネント参照
        self.position_manager = position_manager
        self.questdb_client = questdb_client
        self.symbol_mapper = symbol_mapper
        self.main_stats = main_stats

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

        # 価格変動率キャッシュ（最新分析結果）
        self.price_changes: Dict[str, float] = {}

        # シングルスレッド環境のためロック不要
        # self._lock = threading.Lock()  # 削除：不要

        logger.info(f"💹 トレーディング戦略初期化完了:")
        logger.info(f"  - 価格比較期間: {self.price_comparison_seconds}秒")
        logger.info(f"  - ロング闾値: {self.long_threshold}%")
        logger.info(f"  - ショート闾値: {self.short_threshold}%")
        logger.info(f"  - 反発闾値: {self.reversal_threshold}%")
        logger.info(f"  - 最小利益闾値: {self.min_profit_percent}%")

    def analyze_tick(self, tick: TickData) -> TradingSignal:
        """
        ティックデータを分析してシグナルを生成

        Args:
            tick: 最新ティックデータ

        Returns:
            取引シグナル
        """
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

        # デバッグ用ログ（主要銘柄のみ）
        major_debug_symbols = ["BTCUSDT", "BTC_USDT", "ETHUSDT", "ETH_USDT"]
        if tick.symbol in major_debug_symbols:
            logger.info(f"{tick.symbol}: 変動率={change_percent}, 価格={tick.price}")

        # 価格変動率をキャッシュ（メイン処理から取得可能に）
        if change_percent is not None:
            self.price_changes[tick.symbol] = change_percent

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
                logger.info(f"{symbol}: 最小利益闾値達成。建値ストップ設定")

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

        logger.info(f"ポジション追跡開始: {symbol} {side} @ {entry_price}")

    def remove_position(self, symbol: str) -> Optional[PositionTracker]:
        """
        ポジション追跡を終了

        Args:
            symbol: 銘柄

        Returns:
            削除されたポジション追跡データ
        """
        tracker = self.position_trackers.pop(symbol, None)
        if tracker:
            self.stats["active_positions"] = len(self.position_trackers)
            logger.info(f"ポジション追跡終了: {symbol}")
        return tracker

    def get_position_tracker(self, symbol: str) -> Optional[PositionTracker]:
        """指定銘柄のポジション追跡データを取得"""
        return self.position_trackers.get(symbol)

    def get_active_positions(self) -> Dict[str, PositionTracker]:
        """アクティブなポジション追跡データを取得"""
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

    def get_price_change_percent(self, symbol: str) -> float:
        print(f"Retrieving price change percent for {symbol}", flush=True)
        """指定銘柄の最新価格変動率を取得"""
        print(
            f"Price change percent for {symbol}: {self.price_changes.get(symbol, 0.0)}",
            flush=True,
        )
        return self.price_changes.get(symbol, 0.0)

    def log_comprehensive_statistics(self, start_time: datetime, main_stats: dict):
        """包括的な統計情報をログ出力"""
        try:
            # アップタイム計算
            uptime = (datetime.now() - start_time).total_seconds()

            # 各コンポーネントの統計取得（安全な取得）
            try:
                data_stats = self.data_manager.get_stats() if self.data_manager else {}
            except Exception as e:
                logger.debug(f"Failed to get data_manager stats: {e}")
                data_stats = {}

            strategy_stats = self.get_stats()

            try:
                position_stats = (
                    self.position_manager.get_stats() if self.position_manager else {}
                )
            except Exception as e:
                logger.debug(f"Failed to get position_manager stats: {e}")
                position_stats = {}

            try:
                questdb_stats = (
                    self.questdb_client.get_stats() if self.questdb_client else {}
                )
            except Exception as e:
                logger.debug(f"Failed to get questdb_client stats: {e}")
                questdb_stats = {}

            try:
                symbol_stats = (
                    self.symbol_mapper.get_mapping_stats() if self.symbol_mapper else {}
                )
            except Exception as e:
                logger.debug(f"Failed to get symbol_mapper stats: {e}")
                symbol_stats = {}

            # ポートフォリオ要約
            try:
                portfolio = (
                    self.position_manager.get_portfolio_summary()
                    if self.position_manager
                    else {}
                )
            except Exception as e:
                logger.debug(f"Failed to get portfolio summary: {e}")
                portfolio = {}

            logger.info(
                "\n=== TRADE MINI 統計情報 ===\n"
                f"⏰ 稼働時間: {uptime/3600:.2f}時間\n"
                f"📊 ティック処理数: {main_stats.get('ticks_processed', 0):,}\n"
                f"📡 シグナル生成数: {strategy_stats.get('signals_generated', 0)}\n"
                f"💼 実行取引数: {main_stats.get('trades_executed', 0)}\n"
                f"💎 アクティブ銘柄数: {data_stats.get('active_symbols', 0)}\n"
                f"🔥 オープンポジション: {position_stats.get('current_positions', 0)}\n"
                f"🏦 アカウント残高: {portfolio.get('account_balance', 0):.2f} USDT\n"
                f"📈 未実現損益: {portfolio.get('total_unrealized_pnl', 0):.2f} USDT\n"
                f"💾 QuestDB保存ティック数: {questdb_stats.get('ticks_saved', 0):,}\n"
                f"🔄 Bybit取引可能銘柄数: {symbol_stats.get('total_tradeable_symbols', 0)}\n"
                "============================="
            )

        except Exception as e:
            logger.error(f"統計情報ログ出力エラー: {e}")
            import traceback

            logger.debug(f"統計情報エラートレース: {traceback.format_exc()}")

    def analyze_tick_optimized(self, tick: TickData) -> TradingSignal:
        """
        🚀 最適化されたティック分析 (全銘柄対応高速版)

        主な最適化:
        - ロック時間の最小化
        - 不要な計算のスキップ
        - 軽量ポジショントラッキング
        """
        # print(
        #     f"🔍 Analyzing tick optimized for {tick.symbol} at {tick.timestamp}",
        #     flush=True,
        # )
        # 🚀 ロック外での事前チェック（ロック競合回避）
        has_position = tick.symbol in self.position_trackers

        if has_position:
            # 既存ポジションがある場合の軽量処理
            print(f"🔄 Existing position detected for {tick.symbol}", flush=True)
            return self._analyze_existing_position(tick)
        else:
            # 新規エントリー候補の超高速分析
            # print(
            #     f"✨ No existing position for {tick.symbol}, analyzing for entry",
            #     flush=True,
            # )
            return self._analyze_new_entry_fast(tick)

    def _analyze_existing_position(self, tick: TickData) -> TradingSignal:
        """既存ポジションの軽量分析"""
        tracker = self.position_trackers.get(tick.symbol)
        if not tracker:
            # ポジションが消失した場合
            return self._create_no_signal(tick)

        self._update_position_tracker(tick)
        return self._check_close_signal(tick.symbol, tick.price, tick.timestamp)

    def _analyze_new_entry_fast(self, tick: TickData) -> TradingSignal:
        """新規エントリーの超高速分析 (キャッシュ最適化)"""
        # print(f"🚀 Fast entry analysis for {tick.symbol}", flush=True)
        # 価格変動率を高速取得（キャッシュ利用）
        # print("価格変動率を高速取得（キャッシュ利用）", flush=True)
        # print(self.data_manager, flush=True)
        change_percent = self.data_manager.get_price_change_percent(
            tick.symbol, self.price_comparison_seconds
        )
        # print(
        #     f"📈 Price change for {tick.symbol}: {change_percent}%",
        #     flush=True,
        # )

        if change_percent is None:
            # print(f"❌ No price change data for {tick.symbol}", flush=True)
            return self._create_no_signal(tick)

        # 🚀 閾値チェックを最適化（早期リターン）
        if change_percent >= self.long_threshold:
            # print(f"📊 Long signal generated for {tick.symbol}", flush=True)
            return self._create_signal(tick, SignalType.LONG, change_percent)
        elif change_percent <= -self.short_threshold:
            # print(f"📊 Short signal generated for {tick.symbol}", flush=True)
            return self._create_signal(tick, SignalType.SHORT, change_percent)
        else:
            # print(f"ℹ️ No entry signal for {tick.symbol}", flush=True)
            return self._create_no_signal(tick)

    def _create_no_signal(self, tick: TickData) -> TradingSignal:
        """NONEシグナルの高速生成"""
        return TradingSignal(
            symbol=tick.symbol,
            signal_type=SignalType.NONE,
            price=tick.price,
            timestamp=tick.timestamp,
        )

    def _create_signal(
        self, tick: TickData, signal_type: SignalType, change_percent: float
    ) -> TradingSignal:
        """取引シグナルの高速生成"""
        if signal_type == SignalType.LONG:
            reason = f"Price +{change_percent:.2f}%"
            self.stats["long_signals"] += 1
        else:
            reason = f"Price {change_percent:.2f}%"
            self.stats["short_signals"] += 1

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

    def process_tick_and_execute_trades(self, tick: TickData) -> bool:
        """
        ティックデータを処理して取引を実行 (全銘柄高効率版)

        Args:
            tick: ティックデータ

        Returns:
            取引が実行されたかどうか
        """
        # print(
        #     f"🔍 Processing tick for {tick.symbol} at {tick.timestamp} with price {tick.price}",
        #     flush=True,
        # )
        try:
            # 🚀 高速化: 軽量チェック（全銘柄処理）
            if not self._should_process_tick(tick):
                print(f"❌ Skipping tick for {tick.symbol}", flush=True)
                return False

            # 🚀 高効率戦略分析（キャッシュ最適化済み）
            signal = self.analyze_tick_optimized(tick)

            if signal.signal_type == SignalType.NONE:
                # print(f"ℹ️ No trade signal for {tick.symbol}", flush=True)
                return False

            # シグナルに基づいて取引実行
            # print(
            #     f"🚀 Trade signal for {tick.symbol}: {signal.signal_type}", flush=True
            # )
            return self._execute_trade_from_signal(signal)

        except Exception as e:
            logger.error(f"{tick.symbol}のティック処理エラー: {e}")
            return False

    def _should_process_tick(self, tick: TickData) -> bool:
        """
        🚀 軽量事前チェック: 全銘柄処理のための効率化
        """
        # 全銘柄を処理するが、軽量チェックで無駄な処理を回避
        return True  # 全銘柄処理

    def _execute_trade_from_signal(self, signal: TradingSignal) -> bool:
        """
        シグナルに基づいて取引を実行

        Args:
            signal: 取引シグナル

        Returns:
            取引が実行されたかどうか
        """
        try:
            if signal.signal_type == SignalType.LONG:
                return self._execute_long_position(signal)
            elif signal.signal_type == SignalType.SHORT:
                return self._execute_short_position(signal)
            elif signal.signal_type == SignalType.CLOSE:
                return self._execute_close_position(signal)
            else:
                return False

        except Exception as e:
            logger.error(f"{signal.symbol}のシグナルからの取引実行エラー: {e}")
            return False

    def _execute_long_position(self, signal: TradingSignal) -> bool:
        """ロングポジションを開く"""
        try:
            logger.info(f"⬆️ ロング闾値達成: {signal.symbol} 変動={signal.reason}")

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ ポジションマネージャ無効: {signal.symbol} LONGシグナルを無視"
                )
                return False

            # datetime型のタイムスタンプに変換（ナノ秒から）
            entry_time = (
                signal.timestamp
                if isinstance(signal.timestamp, datetime)
                else datetime.now()
            )

            success, message, position = self.position_manager.open_position(
                signal.symbol, "LONG", signal.price, entry_time
            )

            if success:
                logger.info(
                    f"✅ ロングポジションオープン: {signal.symbol} @ {signal.price}"
                )
                # 戦略側でもポジション追跡開始
                self.add_position(signal.symbol, "LONG", signal.price, 1.0, entry_time)
                return True
            else:
                logger.error(f"❌ ロングポジション失敗: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ ロングポジションエラー: {signal.symbol} - {e}")
            return False

    def _execute_short_position(self, signal: TradingSignal) -> bool:
        """ショートポジションを開く"""
        try:
            logger.info(f"⬇️ ショート闾値達成: {signal.symbol} 変動={signal.reason}")

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ ポジションマネージャ無効: {signal.symbol} SHORTシグナルを無視"
                )
                return False

            # datetime型のタイムスタンプに変換（ナノ秒から）
            entry_time = (
                signal.timestamp
                if isinstance(signal.timestamp, datetime)
                else datetime.now()
            )

            success, message, position = self.position_manager.open_position(
                signal.symbol, "SHORT", signal.price, entry_time
            )

            if success:
                logger.info(
                    f"✅ ショートポジションオープン: {signal.symbol} @ {signal.price}"
                )
                # 戦略側でもポジション追跡開始
                self.add_position(signal.symbol, "SHORT", signal.price, 1.0, entry_time)
                return True
            else:
                logger.error(f"❌ ショートポジション失敗: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ ショートポジションエラー: {signal.symbol} - {e}")
            return False

    def _execute_close_position(self, signal: TradingSignal) -> bool:
        """ポジションを決済"""
        try:
            logger.info(f"🔥 クローズシグナル: {signal.symbol} 理由={signal.reason}")

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ ポジションマネージャ無効: {signal.symbol} CLOSEシグナルを無視"
                )
                return False

            success, message, position = self.position_manager.close_position(
                signal.symbol, signal.reason
            )

            if success:
                logger.info(f"✅ ポジションクローズ: {signal.symbol} @ {signal.price}")
                # 戦略側でもポジション追跡終了
                self.remove_position(signal.symbol)
                return True
            else:
                logger.error(f"❌ ポジションクローズ失敗: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ ポジションクローズエラー: {signal.symbol} - {e}")
            return False

    def process_ticker_batch(
        self,
        tickers: list,
        batch_timestamp: float,
        batch_id: int,
        worker_heartbeat=None,
    ) -> Dict[str, int]:
        """
        🚀 ティッカーバッチを処理（戦略責務）

        main.pyから移譲された処理：
        - 各銘柄の変動率確認
        - 戦略分析とシグナル生成
        - オープン・クローズ発注
        - ポジション管理

        Args:
            tickers: ティッカーデータリスト
            batch_timestamp: バッチタイムスタンプ
            batch_id: バッチID

        Returns:
            処理統計 {"processed_count": int, "signals_count": int, "trades_executed": int}
        """
        # print(
        #     f"🔍 ENTERED process_ticker_batch: batch_id={batch_id}, tickers_count={len(tickers)}",
        #     flush=True,
        # )
        logger.info(f"🚀 戦略バッチ#{batch_id}処理開始: {len(tickers)}ティッカー")
        # print("🔍 Logger.info completed", flush=True)
        start_time = time.time()
        processed_count = 0
        signals_count = 0
        trades_executed = 0

        # バッチ受信時刻をナノ秒タイムスタンプで統一
        batch_ts_ns = int(batch_timestamp * 1_000_000_000)

        # 詳細ステップ時間計測
        data_processing_time = 0
        analysis_time = 0
        trading_time = 0

        # 🚀 全銘柄処理ループ（制限解除）
        # logger.info(f"🔄 バッチ#{batch_id}: {len(tickers)}銘柄の処理ループ開始")

        # 即座にハートビート更新
        if worker_heartbeat is not None:
            try:
                # print("💓 ハートビート更新")
                worker_heartbeat.value = time.time()
            except Exception as e:
                logger.warning(f"❌ ハートビート更新失敗: {e}")

        # 🚀 全銘柄を処理（制限なし）
        # logger.info("🚀 全銘柄を処理（制限なし）")
        for i, ticker_data in enumerate(tickers):
            # print(f"🔍 START ticker {i+1}/{len(tickers)}", flush=True)
            try:
                symbol = (
                    ticker_data.get("symbol", "N/A")
                    # if isinstance(ticker_data, dict)
                    # else "NOT_DICT"
                )
                # print(f"🔍 Symbol: {symbol}", flush=True)
                # logger.info(f"Processing ticker {i+1}/{len(tickers)}: {symbol}")
                # print(
                #     f"🔍 ticker_data keys: {list(ticker_data.keys()) if isinstance(ticker_data, dict) else 'NOT_DICT'}",
                #     flush=True,
                # )
            except Exception as e:
                print(f"🔍 ERROR getting symbol: {e}", flush=True)
                continue
            try:
                # データ処理開始時間
                data_start = time.time()

                # if not isinstance(ticker_data, dict):
                #     print(f"❌ 無効なティッカーデータ形式: {ticker_data}")
                #     continue

                symbol = ticker_data.get("symbol", "")
                # if not symbol:
                #     print(f"❌ 無効なシンボル: {ticker_data}")
                #     continue

                # 本来のTickData作成（実際の価格データを使用）
                try:
                    # print(f"🔍 About to get price for {symbol}...", flush=True)
                    price = float(ticker_data.get("lastPrice", 0))
                    if price <= 0:
                        price = float(ticker_data.get("indexPrice", 0))
                        if price <= 0:
                            price = float(ticker_data.get("fairPrice", 0))
                            if price <= 0:
                                print(f"❌ 0な価格データ: {symbol} {ticker_data}")
                                continue
                    # print(f"🔍 Price for {symbol}: {price}", flush=True)

                    # print(f"🔍 About to get volume for {symbol}...", flush=True)
                    volume = float(ticker_data.get("volume", 0))
                    # print(f"🔍 Volume for {symbol}: {volume}", flush=True)

                    # print(
                    #     f"🔍 Price/Volume check passed for {symbol}: price={price}, volume={volume}",
                    #     flush=True,
                    # )
                    # print(f"🔍 Creating TickData for {symbol}...", flush=True)
                    tick = TickData(
                        symbol=symbol,
                        price=price,
                        volume=volume,
                        timestamp=batch_ts_ns,
                    )
                    # print(f"🔍 TickData created: {tick}", flush=True)

                    # DataManager.add_tick を呼び出し
                    # print(
                    #     f"🔍 About to check data_manager: {self.data_manager is not None}",
                    #     flush=True,
                    # )
                    # if self.data_manager is not None:
                    # print("🔍 Adding tick to DataManager...", flush=True)
                    # print(
                    #     f"🔍 DataManager type: {type(self.data_manager)}",
                    #     flush=True,
                    # )
                    try:
                        self.data_manager.add_tick(tick)
                        # print("🔍 DataManager.add_tick completed", flush=True)
                    except Exception as e:
                        logger.error(f"🔍 DataManager.add_tick ERROR: {e}")
                        raise
                    # else:
                    #     print("🔍 DataManager is None!", flush=True)

                    # データ処理時間測定
                    data_processing_time += time.time() - data_start

                    # 🚀 戦略分析と取引実行
                    analysis_start = time.time()
                    # print("🚀 戦略分析と取引実行")
                    trade_executed = self.process_tick_and_execute_trades(tick)
                    analysis_time += time.time() - analysis_start

                    processed_count += 1

                    if trade_executed:
                        trades_executed += 1

                    # 100銘柄毎にハートビート更新でタイムアウト防止
                    if processed_count % 100 == 0:  # and worker_heartbeat is not None:
                        # try:
                        worker_heartbeat.value = time.time()
                        # except Exception:
                        # pass

                except (ValueError, TypeError) as e:
                    logger.error(f"価格データ変換エラー {symbol}: {e}")
                    continue

            except Exception as e:
                logger.error(f"❌ 銘柄#{i}処理エラー: {e}")
                continue

        # 定期的なハートビート更新（処理中）
        # if worker_heartbeat is not None:
        #     try:
        #         worker_heartbeat.value = time.time()
        #     except Exception:
        #         pass

        duration = time.time() - start_time

        # 💓 処理完了時のハートビート更新 - 一時的に無効化
        # if False and worker_heartbeat is not None:
        worker_heartbeat.value = time.time()

        # 📊 詳細タイミングログ
        avg_time_per_ticker = duration / len(tickers) * 1000 if len(tickers) > 0 else 0
        logger.info(
            f"✅ 戦略バッチ#{batch_id}処理完了: {processed_count}/{len(tickers)}処理済み, "
            f"{signals_count}シグナル, {trades_executed}取引実行 ({duration:.3f}秒, "
            f"平均{avg_time_per_ticker:.2f}ms/銘柄)"
        )

        # 🔍 詳細パフォーマンス分析（ワーカー=1での計測用）
        tickers_per_second = len(tickers) / duration if duration > 0 else 0
        # logger.info(
        #     f"🚀 PERFORMANCE METRICS - Batch #{batch_id}: "
        #     f"処理速度={tickers_per_second:.1f}銘柄/秒, "
        #     f"総時間={duration:.3f}s, 平均={avg_time_per_ticker:.2f}ms/銘柄"
        # )

        # 🔬 詳細ステップ別時間分析
        logger.info(
            f"🔬 DETAILED BREAKDOWN - Batch #{batch_id}: "
            f"データ処理={data_processing_time:.3f}s, "
            f"戦略分析={analysis_time:.3f}s, "
            f"取引処理={trading_time:.3f}s"
        )

        # 🐌 パフォーマンス警告（30秒以上の場合）
        if duration > 30.0:
            logger.warning(
                f"⚠️ DETAILED TIMING - SLOW BATCH: #{batch_id} took {duration:.1f}s "
                f"({len(tickers)} tickers, {avg_time_per_ticker:.2f}ms/ticker)"
            )

        return {
            "processed_count": processed_count,
            "signals_count": signals_count,
            "trades_executed": trades_executed,
            "duration": duration,
        }

    def get_all_price_changes(self, n_seconds: int) -> Dict[str, float]:
        """
        🚀 全銘柄の価格変動率を取得（戦略責務）

        main.pyから移譲された処理
        """
        if self.data_manager is None:
            return {}

        return self.data_manager.get_all_price_changes_batch(n_seconds)
