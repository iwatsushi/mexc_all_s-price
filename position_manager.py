"""
ポジション管理クラス
"""

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

from bybit_client import BybitClient, BybitOrderResult
from config import Config
from mexc_client import MEXCClient, OrderResult
from symbol_mapper import SymbolMapper

logger = logging.getLogger(__name__)


class MarginMode(Enum):
    """マージンモード"""

    ISOLATED = "isolated"
    CROSS = "cross"


class PositionStatus(Enum):
    """ポジション状態"""

    OPENING = "opening"
    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    ERROR = "error"


@dataclass
class Position:
    """ポジション情報"""

    symbol: str
    side: str  # "LONG" or "SHORT"
    size: float
    entry_price: float
    entry_time: datetime
    status: PositionStatus

    # 注文情報
    entry_order_id: str = ""
    close_order_id: str = ""

    # 利益管理
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0

    # リスク管理
    max_leverage: float = 0.0
    margin_mode: MarginMode = MarginMode.ISOLATED
    margin_used: float = 0.0

    # 統計
    price_updates: int = 0
    last_update: datetime = None


class PositionManager:
    """ポジション管理クラス"""

    def __init__(
        self,
        config: Config,
        mexc_client: MEXCClient,
        bybit_client: BybitClient,
        symbol_mapper: SymbolMapper,
    ):
        self.config = config
        self.mexc_client = mexc_client
        self.bybit_client = bybit_client
        self.symbol_mapper = symbol_mapper

        # 取引所設定（将来のMEXC対応のため）
        self.trading_exchange = config.get(
            "trading.exchange", "bybit"
        )  # "mexc" or "bybit"

        # 設定パラメータ
        self.max_concurrent_positions = config.max_concurrent_positions
        self.capital_usage_percent = config.capital_usage_percent
        self.cross_margin_threshold = config.cross_margin_threshold
        self.max_loss_on_2x_reversal = config.max_loss_on_2x_reversal
        self.min_order_size = config.min_order_size

        # ポジション管理
        self.positions: Dict[str, Position] = {}
        self.account_balance: float = 0.0
        self.available_balance: float = 0.0
        self.total_margin_used: float = 0.0

        # 統計
        self.stats = {
            "total_positions_opened": 0,
            "total_positions_closed": 0,
            "successful_orders": 0,
            "failed_orders": 0,
            "total_realized_pnl": 0.0,
            "total_unrealized_pnl": 0.0,
            "margin_mode_switches": 0,
        }

        # スレッドセーフティ
        # self._lock = threading.Lock()  # 削除：マルチプロセス環境では不要

        # 初期化
        self._update_account_info()

        logger.info(f"💼 ポジションマネージャ初期化完了:")
        logger.info(f"  - 最大同時ポジション数: {self.max_concurrent_positions}")
        logger.info(f"  - 資本使用率: {self.capital_usage_percent}%")
        logger.info(f"  - クロスマージン闾値: {self.cross_margin_threshold} USDT")

    def _update_account_info(self):
        """アカウント情報を更新"""
        try:
            if self.trading_exchange == "bybit":
                balance_response = self.bybit_client.get_wallet_balance()

                if balance_response.get("retCode") == 0:
                    result = balance_response.get("result", {})
                    accounts = result.get("list", [])

                    for account in accounts:
                        coins = account.get("coin", [])
                        for coin in coins:
                            if coin.get("coin") == "USDT":
                                # 安全な数値変換（空文字列対応）
                                wallet_balance = coin.get("walletBalance", "0")
                                available_balance = coin.get("availableToWithdraw", "0")

                                # 空文字列または None の場合は 0 として扱う
                                if not wallet_balance or wallet_balance == "":
                                    wallet_balance = "0"
                                if not available_balance or available_balance == "":
                                    available_balance = "0"

                                self.account_balance = float(wallet_balance)
                                self.available_balance = float(available_balance)
                                break
                        if self.account_balance > 0:
                            break

                    # logger.info(
                    #     f"🏦 Bybitアカウント残高更新: {self.account_balance} USDT"
                    # )
                else:
                    logger.error(f"Bybit残高取得失敗: {balance_response.get('retMsg')}")

            else:  # MEXC
                balance_response = self.mexc_client.get_balance()

                if balance_response.get("success"):
                    assets = balance_response.get("data", [])
                    for asset in assets:
                        if asset.get("currency") == "USDT":
                            # 安全な数値変換（空文字列対応）
                            available_balance = asset.get("availableBalance", "0")

                            # 空文字列または None の場合は 0 として扱う
                            if not available_balance or available_balance == "":
                                available_balance = "0"

                            self.account_balance = float(available_balance)
                            self.available_balance = float(available_balance)
                            break

                    logger.info(
                        f"🏦 MEXCアカウント残高更新: {self.account_balance} USDT"
                    )
                else:
                    logger.error(f"MEXC残高取得失敗: {balance_response.get('message')}")

        except Exception as e:
            logger.error(f"アカウント情報更新エラー: {e}")

    def _determine_margin_mode(self) -> MarginMode:
        """マージンモードを決定"""
        if self.account_balance >= self.cross_margin_threshold:
            return MarginMode.CROSS
        else:
            return MarginMode.ISOLATED

    def _calculate_position_size(
        self, symbol: str, side: str, entry_price: float, price_change_percent: float
    ) -> Tuple[float, float, float]:
        """
        ポジションサイズを計算

        Args:
            symbol: 銘柄
            side: "LONG" or "SHORT"
            entry_price: エントリー価格
            price_change_percent: 価格変動率（%）

        Returns:
            (position_size, leverage, margin_required)
        """
        # 銘柄情報を取得してレバレッジ上限を確認
        max_leverage = 100  # デフォルト値

        if self.trading_exchange == "bybit":
            # Bybitの場合は、変換された銘柄でシンボル情報を取得
            bybit_symbol = self.symbol_mapper.get_bybit_symbol(symbol)
            if bybit_symbol:
                symbol_info = self.bybit_client.get_symbol_info(bybit_symbol)
                if symbol_info.get("success"):
                    max_leverage = float(
                        symbol_info.get("data", {}).get("maxLeverage", 100)
                    )
        else:
            # MEXCの場合
            symbol_info = self.mexc_client.get_symbol_info(symbol)
            if symbol_info.get("success"):
                max_leverage = float(
                    symbol_info.get("data", {}).get("maxLeverage", 100)
                )

        # 使用可能資金を計算
        usable_capital = self.account_balance * (self.capital_usage_percent / 100.0)
        print(
            f"💰使用可能資金: {usable_capital}/{self.account_balance} ({self.capital_usage_percent}%)",
            flush=True,
        )
        amount = self.account_balance / entry_price
        print(f"🎳数量: {amount} {symbol}", flush=True)

        # 現在のポジション数を考慮して資金を分配
        active_positions = len(self.positions)
        remaining_slots = max(1, self.max_concurrent_positions - active_positions)
        print(
            f"📊アクティブポジション数: {active_positions}(残：{remaining_slots}/{self.max_concurrent_positions})",
            flush=True,
        )
        capital_per_position = usable_capital  # / remaining_slots

        # 価格差に基づいたレバレッジを計算
        # レバレッジ = usable_capital / (2 * 価格差)
        # 価格差は価格変動率の絶対値を使用
        price_diff_percent = abs(price_change_percent)

        logger.info(
            f"🔍 レバレッジ計算開始: 価格変動={price_change_percent:.3f}%, 絶対値={price_diff_percent:.3f}%"
        )

        if price_diff_percent > 0:
            # 価格差に基づくレバレッジ計算
            calculated_leverage = usable_capital / (
                2 * ((entry_price * price_diff_percent) / (100 + price_diff_percent))
            )
            # 最大レバレッジと比較して安全な値を選択
            safe_leverage = min(calculated_leverage, max_leverage)
            logger.info(
                f"📈 価格差: {price_diff_percent:.3f}% → レバレッジ: {calculated_leverage:.2f}x (制限後: {safe_leverage:.2f}x)"
            )
        else:
            # 価格差が0の場合はデフォルト値を使用
            safe_leverage = min(10.0, max_leverage)  # 控えめなデフォルト値
            logger.info(f"⚠️ 価格差が0% → デフォルトレバレッジ: {safe_leverage:.2f}x")

        # ポジションサイズ計算
        # position_value = capital_per_position * leverage
        # position_size = position_value / entry_price
        position_value = capital_per_position * safe_leverage
        position_size = position_value / entry_price
        margin_required = position_value / safe_leverage

        # 最小注文サイズチェック
        min_position_value = self.min_order_size
        if position_value < min_position_value:
            logger.warning(
                f"Position value {position_value} below minimum {min_position_value}"
            )
            return 0.0, 0.0, 0.0

        logger.info(f"Position size calculation for {symbol}:")
        logger.info(f"  - Capital per position: {capital_per_position:.2f} USDT")
        logger.info(f"  - Safe leverage: {safe_leverage:.2f}x")
        logger.info(f"  - Position size: {position_size:.6f}")
        logger.info(f"  - Margin required: {margin_required:.2f} USDT")

        return position_size, safe_leverage, margin_required

    def can_open_position(self, symbol: str) -> Tuple[bool, str]:
        """
        新しいポジションを開けるかチェック

        Args:
            symbol: 銘柄

        Returns:
            (可能かどうか, 理由)
        """
        # 同じ銘柄で既にポジションがある場合
        if symbol in self.positions:
            return False, f"Position already exists for {symbol}"

        # Bybitで取引可能な銘柄かチェック（Bybit取引の場合）
        if self.trading_exchange == "bybit":
            if not self.symbol_mapper.is_tradeable_on_bybit(symbol):
                return False, f"Symbol {symbol} is not tradeable on Bybit"

        # 最大ポジション数チェック
        if len(self.positions) >= self.max_concurrent_positions:
            return (
                False,
                f"最大ポジション({self.max_concurrent_positions})に到達",
            )

        # 残高チェック
        self._update_account_info()
        usable_capital = self.account_balance * (self.capital_usage_percent / 100.0)

        if usable_capital < self.min_order_size:
            return (
                False,
                f"Insufficient balance. Available: {usable_capital:.2f} USDT",
            )

        return True, "OK"

    def open_position(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        price_change_percent: float,
        timestamp: datetime = None,
    ) -> Tuple[bool, str, Optional[Position]]:
        """
        ポジションを開く

        Args:
            symbol: 銘柄
            side: "LONG" or "SHORT"
            entry_price: エントリー価格
            price_change_percent: 価格変動率（%）
            timestamp: エントリー時刻

        Returns:
            (成功/失敗, メッセージ, ポジション情報)
        """
        if timestamp is None:
            timestamp = datetime.now()

        # ポジション開設可能性をチェック
        can_open, reason = self.can_open_position(symbol)
        if not can_open:
            return False, reason, None

        # ポジションサイズ計算
        position_size, leverage, margin_required = self._calculate_position_size(
            symbol, side, entry_price, price_change_percent
        )

        if position_size <= 0:
            return False, "Invalid position size calculated", None

        # マージンモード設定とレバレッジ設定
        margin_mode = self._determine_margin_mode()

        if self.trading_exchange == "bybit":
            # MEXC銘柄をBybit銘柄に変換
            bybit_symbol = self.symbol_mapper.get_bybit_symbol(symbol)
            if not bybit_symbol:
                return False, f"Cannot convert {symbol} to Bybit format", None

            # Bybitでレバレッジ設定
            leverage_success = self.bybit_client.set_leverage(bybit_symbol, leverage)
            if leverage_success:
                self.stats["margin_mode_switches"] += 1
                logger.info(
                    f"Set leverage for {bybit_symbol} ({symbol}) to {leverage}x on Bybit"
                )

            # 注文実行（Bybit）
            order_result = self.bybit_client.place_market_order(
                bybit_symbol, side, position_size
            )
            order_success = order_result.success
            order_id = order_result.order_id
            order_message = order_result.message
        else:
            # MEXC
            margin_success = self.mexc_client.set_margin_mode(symbol, margin_mode.value)
            if margin_success:
                self.stats["margin_mode_switches"] += 1
                logger.info(f"Set margin mode for {symbol} to {margin_mode.value}")

            # 注文実行（MEXC）
            mexc_order_result = self.mexc_client.place_market_order(
                symbol, side, position_size
            )
            order_success = mexc_order_result.success
            order_id = mexc_order_result.order_id
            order_message = mexc_order_result.message

        if order_success:
            # ポジション作成
            position = Position(
                symbol=symbol,
                side=side,
                size=position_size,
                entry_price=entry_price,
                entry_time=timestamp,
                status=PositionStatus.OPEN,
                entry_order_id=order_id,
                max_leverage=leverage,
                margin_mode=margin_mode,
                margin_used=margin_required,
                last_update=timestamp,
            )

            self.positions[symbol] = position
            self.total_margin_used += margin_required
            self.stats["total_positions_opened"] += 1
            self.stats["successful_orders"] += 1

            logger.info(
                f"Position opened on {self.trading_exchange.upper()}: {symbol} {side} size={position_size:.6f}"
            )
            return True, f"Position opened successfully", position

        else:
            self.stats["failed_orders"] += 1
            logger.error(f"Failed to open position for {symbol}: {order_message}")
            return False, f"Order failed: {order_message}", None

    def close_position(
        self, symbol: str, reason: str = ""
    ) -> Tuple[bool, str, Optional[Position]]:
        """
        ポジションを閉じる

        Args:
            symbol: 銘柄
            reason: 閉じる理由

        Returns:
            (成功/失敗, メッセージ, ポジション情報)
        """
        position = self.positions.get(symbol)
        if not position:
            return False, f"No position found for {symbol}", None

        if position.status != PositionStatus.OPEN:
            return False, f"Position {symbol} is not in OPEN status", position

        # 決済注文実行
        position.status = PositionStatus.CLOSING

        if self.trading_exchange == "bybit":
            order_result = self.bybit_client.close_position(symbol, position.side)
            order_success = order_result.success
            order_id = order_result.order_id
            order_message = order_result.message
        else:
            mexc_order_result = self.mexc_client.close_position(symbol, position.side)
            order_success = mexc_order_result.success
            order_id = mexc_order_result.order_id
            order_message = mexc_order_result.message

        if order_success:
            position.status = PositionStatus.CLOSED
            position.close_order_id = order_id
            position.last_update = datetime.now()

            # 統計更新
            self.total_margin_used -= position.margin_used
            self.stats["total_positions_closed"] += 1
            self.stats["successful_orders"] += 1
            self.stats["total_realized_pnl"] += position.unrealized_pnl

            # ポジション削除
            closed_position = self.positions.pop(symbol)

            logger.info(
                f"Position closed on {self.trading_exchange.upper()}: {symbol} {position.side} reason='{reason}'"
            )
            return True, "Position closed successfully", closed_position

        else:
            position.status = PositionStatus.ERROR
            self.stats["failed_orders"] += 1
            logger.error(f"Failed to close position for {symbol}: {order_message}")
            return False, f"Close order failed: {order_message}", position

    def update_position_pnl(self, symbol: str, current_price: float):
        """ポジションのPnLを更新"""
        position = self.positions.get(symbol)
        if not position or position.status != PositionStatus.OPEN:
            return

        # 未実現損益計算
        if position.side == "LONG":
            pnl = (current_price - position.entry_price) * position.size
        else:  # SHORT
            pnl = (position.entry_price - current_price) * position.size

        position.unrealized_pnl = pnl
        position.price_updates += 1
        position.last_update = datetime.now()

        # 統計更新
        total_unrealized = sum(
            pos.unrealized_pnl
            for pos in self.positions.values()
            if pos.status == PositionStatus.OPEN
        )
        self.stats["total_unrealized_pnl"] = total_unrealized

    def get_position(self, symbol: str) -> Optional[Position]:
        """特定銘柄のポジション取得"""
        return self.positions.get(symbol)

    def get_all_positions(self) -> Dict[str, Position]:
        """全ポジション取得"""
        return self.positions.copy()

    def get_open_positions(self) -> Dict[str, Position]:
        """オープン中のポジション取得"""
        return {
            symbol: pos
            for symbol, pos in self.positions.items()
            if pos.status == PositionStatus.OPEN
        }

    def get_position_symbols(self) -> List[str]:
        """ポジションを持つ銘柄一覧を取得"""
        return list(self.positions.keys())

    def get_available_slots(self) -> int:
        """利用可能なポジション枠数を取得"""
        return max(0, self.max_concurrent_positions - len(self.positions))

    def get_portfolio_summary(self) -> Dict[str, any]:
        """ポートフォリオ要約を取得"""
        self._update_account_info()

        open_positions = self.get_open_positions()
        total_unrealized_pnl = sum(
            pos.unrealized_pnl for pos in open_positions.values()
        )

        return {
            "account_balance": self.account_balance,
            "available_balance": self.available_balance,
            "total_margin_used": self.total_margin_used,
            "total_unrealized_pnl": total_unrealized_pnl,
            "open_positions_count": len(open_positions),
            "available_slots": self.get_available_slots(),
            "capital_usage_percent": self.capital_usage_percent,
            "margin_mode": self._determine_margin_mode().value,
            "positions": {
                symbol: {
                    "side": pos.side,
                    "size": pos.size,
                    "entry_price": pos.entry_price,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "margin_used": pos.margin_used,
                    "leverage": pos.max_leverage,
                }
                for symbol, pos in open_positions.items()
            },
        }

    def get_stats(self) -> Dict[str, any]:
        """統計情報を取得"""
        return {
            **self.stats,
            "current_positions": len(self.positions),
            "available_slots": self.get_available_slots(),
            "total_margin_used": self.total_margin_used,
            "account_balance": self.account_balance,
            "portfolio_value": self.account_balance
            + self.stats["total_unrealized_pnl"],
        }

    def cleanup_closed_positions(self):
        """クローズ済みポジションをクリーンアップ"""
        closed_symbols = [
            symbol
            for symbol, pos in self.positions.items()
            if pos.status == PositionStatus.CLOSED
        ]

        for symbol in closed_symbols:
            del self.positions[symbol]
            logger.debug(f"Cleaned up closed position: {symbol}")

    def shutdown(self):
        """ポジション管理をシャットダウン"""
        logger.info("Shutting down PositionManager")

        # すべてのオープンポジションを強制決済
        open_positions = self.get_open_positions()
        for symbol in open_positions.keys():
            logger.warning(f"Force closing position: {symbol}")
            self.close_position(symbol, "Shutdown")

        logger.info("PositionManager shutdown completed")
