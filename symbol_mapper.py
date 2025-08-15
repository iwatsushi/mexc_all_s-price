"""
銘柄マッピング管理クラス
MEXCとBybitの銘柄対応関係を管理
"""

import logging
import threading
import time
from typing import Dict, List, Set

from bybit_client import BybitClient

logger = logging.getLogger(__name__)


class SymbolMapper:
    """MEXCとBybitの銘柄マッピング管理"""

    def __init__(self, bybit_client: BybitClient):
        """
        初期化

        Args:
            bybit_client: Bybitクライアント
        """
        self.bybit_client = bybit_client

        # 銘柄マッピング（MEXC形式 -> Bybit形式）
        self.mexc_to_bybit: Dict[str, str] = {}

        # Bybitで取引可能な銘柄セット（MEXC形式）
        self.tradeable_mexc_symbols: Set[str] = set()

        # 更新制御
        self._lock = threading.Lock()
        self._last_update = 0
        self._update_interval = 3600  # 1時間ごとに更新

        # 初期化時に銘柄リストを取得
        self.update_symbol_mapping()

    def update_symbol_mapping(self) -> bool:
        """銘柄マッピングを更新"""
        current_time = time.time()

        with self._lock:
            # 更新間隔チェック
            if current_time - self._last_update < self._update_interval:
                logger.debug("Symbol mapping update skipped (too soon)")
                return True

            try:
                logger.info("Updating symbol mapping from Bybit...")

                # Bybitから取引可能銘柄一覧を取得
                available_symbols = self.bybit_client.get_available_symbols()

                if not available_symbols:
                    logger.warning("No symbols received from Bybit")
                    return False

                # マッピング更新
                new_mapping = {}
                new_tradeable = set()

                for mexc_symbol in available_symbols:
                    bybit_symbol = self._convert_mexc_to_bybit(mexc_symbol)
                    new_mapping[mexc_symbol] = bybit_symbol
                    new_tradeable.add(mexc_symbol)

                self.mexc_to_bybit = new_mapping
                self.tradeable_mexc_symbols = new_tradeable
                self._last_update = current_time

                logger.info(
                    f"Symbol mapping updated: {len(self.tradeable_mexc_symbols)} tradeable symbols"
                )
                logger.debug(
                    f"Sample symbols: {list(self.tradeable_mexc_symbols)[:10]}"
                )

                return True

            except Exception as e:
                logger.error(f"Error updating symbol mapping: {e}")
                return False

    def _convert_mexc_to_bybit(self, mexc_symbol: str) -> str:
        """MEXCシンボルをBybitシンボルに変換"""
        if mexc_symbol.endswith("_USDT"):
            return mexc_symbol.replace("_USDT", "USDT")
        return mexc_symbol

    def _convert_bybit_to_mexc(self, bybit_symbol: str) -> str:
        """BybitシンボルをMEXCシンボルに変換"""
        if bybit_symbol.endswith("USDT"):
            base = bybit_symbol[:-4]
            return f"{base}_USDT"
        return bybit_symbol

    def is_tradeable_on_bybit(self, mexc_symbol: str) -> bool:
        """
        指定されたMEXC銘柄がBybitで取引可能かチェック

        Args:
            mexc_symbol: MEXCシンボル（例：BTC_USDT）

        Returns:
            取引可能かどうか
        """
        with self._lock:
            # 古いデータの場合は更新を試行
            if time.time() - self._last_update > self._update_interval:
                threading.Thread(target=self.update_symbol_mapping, daemon=True).start()

            return mexc_symbol in self.tradeable_mexc_symbols

    def get_bybit_symbol(self, mexc_symbol: str) -> str:
        """
        MEXCシンボルに対応するBybitシンボルを取得

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            対応するBybitシンボル（存在しない場合は空文字）
        """
        with self._lock:
            return self.mexc_to_bybit.get(mexc_symbol, "")

    def get_tradeable_symbols(self) -> List[str]:
        """Bybitで取引可能な銘柄一覧を取得（MEXC形式）"""
        with self._lock:
            return sorted(list(self.tradeable_mexc_symbols))

    def get_symbol_count(self) -> int:
        """取引可能銘柄数を取得"""
        with self._lock:
            return len(self.tradeable_mexc_symbols)

    def filter_tradeable_symbols(self, mexc_symbols: List[str]) -> List[str]:
        """
        MEXCシンボルリストからBybitで取引可能なもののみを抽出

        Args:
            mexc_symbols: MEXCシンボルリスト

        Returns:
            Bybitで取引可能なシンボルのみのリスト
        """
        with self._lock:
            return [
                symbol
                for symbol in mexc_symbols
                if symbol in self.tradeable_mexc_symbols
            ]

    def get_mapping_stats(self) -> Dict[str, any]:
        """マッピング統計情報を取得"""
        with self._lock:
            return {
                "total_tradeable_symbols": len(self.tradeable_mexc_symbols),
                "last_update": self._last_update,
                "update_interval": self._update_interval,
                "time_since_last_update": time.time() - self._last_update,
                "sample_symbols": list(self.tradeable_mexc_symbols)[:10],
            }

    def force_update(self) -> bool:
        """強制的に銘柄マッピングを更新"""
        with self._lock:
            self._last_update = 0  # 強制更新のため最終更新時刻をリセット

        return self.update_symbol_mapping()

    def check_symbol_realtime(self, mexc_symbol: str) -> bool:
        """
        リアルタイムで銘柄の取引可能性をチェック
        （キャッシュされていない場合は直接Bybitに問い合わせ）

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            取引可能かどうか
        """
        # まずキャッシュをチェック
        if self.is_tradeable_on_bybit(mexc_symbol):
            return True

        # キャッシュにない場合は直接チェック
        try:
            is_available = self.bybit_client.check_symbol_availability(mexc_symbol)

            # 利用可能な場合はキャッシュに追加
            if is_available:
                with self._lock:
                    bybit_symbol = self._convert_mexc_to_bybit(mexc_symbol)
                    self.mexc_to_bybit[mexc_symbol] = bybit_symbol
                    self.tradeable_mexc_symbols.add(mexc_symbol)
                    logger.info(f"Added new tradeable symbol to cache: {mexc_symbol}")

            return is_available

        except Exception as e:
            logger.error(f"Error checking symbol availability for {mexc_symbol}: {e}")
            return False
