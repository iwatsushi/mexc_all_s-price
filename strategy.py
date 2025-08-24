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

        # デバッグ用ログ（主要銘柄のみ）
        major_debug_symbols = ["BTCUSDT", "BTC_USDT", "ETHUSDT", "ETH_USDT"]
        if tick.symbol in major_debug_symbols:
            logger.info(
                f"{tick.symbol}: change_percent={change_percent}, price={tick.price}"
            )

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

    def get_price_change_percent(self, symbol: str) -> float:
        """指定銘柄の最新価格変動率を取得"""
        with self._lock:
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

            logger.info("=== TRADE MINI STATISTICS ===")
            logger.info(f"Uptime: {uptime/3600:.2f} hours")
            logger.info(f"Ticks processed: {main_stats.get('ticks_processed', 0)}")
            logger.info(
                f"Signals generated: {strategy_stats.get('signals_generated', 0)}"
            )
            logger.info(f"Trades executed: {main_stats.get('trades_executed', 0)}")

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
            logger.error(f"Error logging comprehensive statistics: {e}")
            import traceback

            logger.debug(f"Statistics error traceback: {traceback.format_exc()}")

    def analyze_tick_optimized(self, tick: TickData) -> TradingSignal:
        """
        🚀 最適化されたティック分析 (全銘柄対応高速版)

        主な最適化:
        - ロック時間の最小化
        - 不要な計算のスキップ
        - 軽量ポジショントラッキング
        """
        # 🚀 ロック外での事前チェック（ロック競合回避）
        has_position = tick.symbol in self.position_trackers

        if has_position:
            # 既存ポジションがある場合の軽量処理
            return self._analyze_existing_position(tick)
        else:
            # 新規エントリー候補の超高速分析
            return self._analyze_new_entry_fast(tick)

    def _analyze_existing_position(self, tick: TickData) -> TradingSignal:
        """既存ポジションの軽量分析"""
        with self._lock:
            tracker = self.position_trackers.get(tick.symbol)
            if not tracker:
                # ポジションが消失した場合
                return self._create_no_signal(tick)

            self._update_position_tracker(tick)
            return self._check_close_signal(tick.symbol, tick.price, tick.timestamp)

    def _analyze_new_entry_fast(self, tick: TickData) -> TradingSignal:
        """新規エントリーの超高速分析 (キャッシュ最適化)"""
        # 価格変動率を高速取得（キャッシュ利用）
        change_percent = self.data_manager.get_price_change_percent(
            tick.symbol, self.price_comparison_seconds
        )

        if change_percent is None:
            return self._create_no_signal(tick)

        # 🚀 閾値チェックを最適化（早期リターン）
        if change_percent >= self.long_threshold:
            return self._create_signal(tick, SignalType.LONG, change_percent)
        elif change_percent <= -self.short_threshold:
            return self._create_signal(tick, SignalType.SHORT, change_percent)
        else:
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
        try:
            # 🚀 高速化: 軽量チェック（全銘柄処理）
            if not self._should_process_tick(tick):
                return False

            # 🚀 高効率戦略分析（キャッシュ最適化済み）
            signal = self.analyze_tick_optimized(tick)

            if signal.signal_type == SignalType.NONE:
                return False

            # シグナルに基づいて取引実行
            return self._execute_trade_from_signal(signal)

        except Exception as e:
            logger.error(f"Error processing tick for {tick.symbol}: {e}")
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
            logger.error(f"Error executing trade from signal for {signal.symbol}: {e}")
            return False

    def _execute_long_position(self, signal: TradingSignal) -> bool:
        """ロングポジションを開く"""
        try:
            logger.info(
                f"🔥 LONG THRESHOLD REACHED: {signal.symbol} change={signal.reason}"
            )

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ POSITION MANAGER DISABLED: {signal.symbol} LONG signal ignored"
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
                    f"✅ LONG POSITION OPENED: {signal.symbol} @ {signal.price}"
                )
                # 戦略側でもポジション追跡開始
                self.add_position(signal.symbol, "LONG", signal.price, 1.0, entry_time)
                return True
            else:
                logger.error(f"❌ LONG POSITION FAILED: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ LONG POSITION ERROR: {signal.symbol} - {e}")
            return False

    def _execute_short_position(self, signal: TradingSignal) -> bool:
        """ショートポジションを開く"""
        try:
            logger.info(
                f"🔥 SHORT THRESHOLD REACHED: {signal.symbol} change={signal.reason}"
            )

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ POSITION MANAGER DISABLED: {signal.symbol} SHORT signal ignored"
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
                    f"✅ SHORT POSITION OPENED: {signal.symbol} @ {signal.price}"
                )
                # 戦略側でもポジション追跡開始
                self.add_position(signal.symbol, "SHORT", signal.price, 1.0, entry_time)
                return True
            else:
                logger.error(f"❌ SHORT POSITION FAILED: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ SHORT POSITION ERROR: {signal.symbol} - {e}")
            return False

    def _execute_close_position(self, signal: TradingSignal) -> bool:
        """ポジションを決済"""
        try:
            logger.info(f"🔥 CLOSE SIGNAL: {signal.symbol} reason={signal.reason}")

            if self.position_manager is None:
                logger.warning(
                    f"⚠️ POSITION MANAGER DISABLED: {signal.symbol} CLOSE signal ignored"
                )
                return False

            success, message, position = self.position_manager.close_position(
                signal.symbol, signal.reason
            )

            if success:
                logger.info(f"✅ POSITION CLOSED: {signal.symbol} @ {signal.price}")
                # 戦略側でもポジション追跡終了
                self.remove_position(signal.symbol)
                return True
            else:
                logger.error(f"❌ CLOSE POSITION FAILED: {signal.symbol} - {message}")
                return False

        except Exception as e:
            logger.error(f"❌ CLOSE POSITION ERROR: {signal.symbol} - {e}")
            return False

    def process_ticker_batch(self, tickers: list, batch_timestamp: float, batch_id: int) -> Dict[str, int]:
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
        logger.info(f"🚀 Strategy processing batch #{batch_id}: {len(tickers)} tickers")
        
        start_time = time.time()
        processed_count = 0
        signals_count = 0
        trades_executed = 0
        
        # バッチ受信時刻をナノ秒タイムスタンプで統一
        batch_ts_ns = int(batch_timestamp * 1_000_000_000)
        
        # メインのティッカー処理ループ
        for ticker_data in tickers:
            if not isinstance(ticker_data, dict):
                continue
                
            symbol = ticker_data.get("symbol", "")
            price = ticker_data.get("lastPrice")
            volume = ticker_data.get("volume24", "0")
            mexc_timestamp = ticker_data.get("timestamp")
            
            if not symbol or not price:
                continue
                
            try:
                price_f = float(price)
                volume_f = float(volume)
                
                # MEXCタイムスタンプを使用（ミリ秒→ナノ秒変換）
                if mexc_timestamp is not None and isinstance(mexc_timestamp, (int, float)):
                    try:
                        timestamp_ms = float(mexc_timestamp)
                        timestamp_ns = int(timestamp_ms * 1_000_000)  # ミリ秒→ナノ秒
                    except (ValueError, TypeError):
                        timestamp_ns = batch_ts_ns  # フォールバック
                else:
                    timestamp_ns = batch_ts_ns  # フォールバック
                
                # TickData作成
                tick = TickData(
                    symbol=symbol,
                    price=price_f,
                    volume=volume_f,
                    timestamp=timestamp_ns,
                )
                
                # 🚀 戦略責務：データマネージャーに追加
                if self.data_manager is not None:
                    self.data_manager.add_tick(tick)
                
                processed_count += 1
                
                # 🚀 戦略責務：変動率確認と戦略分析
                signal = self.analyze_tick_optimized(tick)
                
                if signal.signal_type != SignalType.NONE:
                    signals_count += 1
                    logger.info(
                        f"🚨 SIGNAL DETECTED: {signal.symbol} {signal.signal_type.value} @ {signal.price:.6f} ({signal.reason})"
                    )
                    
                    # 🚀 戦略責務：取引実行
                    if self._execute_trade_from_signal(signal):
                        trades_executed += 1
                        
                    # ハートビート更新（5銘柄毎）
                    if processed_count % 5 == 0:
                        # worker_heartbeat更新は main.py のマルチプロセスワーカーで処理
                        pass
                        
            except (ValueError, TypeError) as e:
                logger.warning(f"Error processing ticker {symbol}: {e}")
                continue
                
        duration = time.time() - start_time
        
        logger.info(
            f"✅ Strategy batch #{batch_id} completed: {processed_count}/{len(tickers)} processed, "
            f"{signals_count} signals, {trades_executed} trades in {duration:.3f}s"
        )
        
        return {
            "processed_count": processed_count,
            "signals_count": signals_count,
            "trades_executed": trades_executed,
            "duration": duration
        }
        
    def get_all_price_changes(self, n_seconds: int) -> Dict[str, float]:
        """
        🚀 全銘柄の価格変動率を取得（戦略責務）
        
        main.pyから移譲された処理
        """
        if self.data_manager is None:
            return {}
            
        return self.data_manager.get_all_price_changes_batch(n_seconds)
