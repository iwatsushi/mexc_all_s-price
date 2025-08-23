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

    # 特殊マッピングケース（実際のBybit銘柄に基づく修正版）
    MEXC_TO_BYBIT_SPECIAL_CASES = {
        # 1000倍表記の特殊ケース（実際のBybit銘柄名で修正）
        "SHIB_USDT": "SHIB1000USDT",  # 実際のBybitはSHIB1000USDT
        "PEPE_USDT": "1000PEPEUSDT",
        "BONK_USDT": "1000BONKUSDT",  # 両者とも1000BONKなので正しい
        "FLOKI_USDT": "1000FLOKIUSDT",
        "LUNC_USDT": "1000LUNCUSDT",
        "XEC_USDT": "1000XECUSDT",
        "TURBO_USDT": "1000TURBOUSDT",
        "BTT_USDT": "1000BTTUSDT",
        "WIN_USDT": "1000WINUSDT",
        "BABYDOGE_USDT": "1000BABYDOGEUSDT",
        "LADYS_USDT": "1000LADYSUSDT",
        "RATS_USDT": "1000RATSUSDT",
        "SATS_USDT": "1000SATSUSDT",
        "COQ_USDT": "10000COQUSDT",
        "WHY_USDT": "10000WHYUSDT",
        "WEN_USDT": "10000WENUSDT",
        # 名称変更・リブランディング（実際のBybit銘柄名で確認済み）
        "FILECOIN_USDT": "FILUSDT",
        "RAY_USDT": "RAYDIUMUSDT",
        "LUNA_USDT": "LUNA2USDT",  # 実際のBybitはLUNA2USDT
        "TERRA_USDT": "LUNA2USDT",
        "POLYGON_USDT": "POLUSDT",
        "MATIC_USDT": "POLUSDT",
        # 特殊命名パターン
        "AVAX_USD": "AVAXUSDT",
        "BTC_USD": "BTCUSDT",
        "ETH_USD": "ETHUSDT",
        "SOL_USD": "SOLUSDT",
        "DOGE_USD": "DOGEUSDT",
        "ADA_USD": "ADAUSDT",
        "NEAR_USD": "NEARUSDT",
        # プレフィックス付きケース
        "BNB_USDC": "BNBUSDT",
        "ETH_USDC": "ETHUSDT",
        "BTC_USDC": "BTCUSDT",
        "SOL_USDC": "SOLUSDT",
    }

    # 近似マッピング（完全一致しない場合）
    MEXC_TO_BYBIT_APPROXIMATE = {
        "DOGE2_USDT": "DOGEUSDT",
        "BITCOIN_USDT": "BTCUSDT",
        "ETHEREUM_USDT": "ETHUSDT",
        "BINANCE_USDT": "BNBUSDT",
        "CARDANO_USDT": "ADAUSDT",
        "POLKADOT_USDT": "DOTUSDT",
        "CHAINLINK_USDT": "LINKUSDT",
        "LITECOIN_USDT": "LTCUSDT",
        "STELLAR_USDT": "XLMUSDT",
        "VECHAIN_USDT": "VETUSDT",
    }

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

        # マッピング統計
        self.mapping_stats = {
            "special_cases_used": 0,
            "approximate_matches": 0,
            "basic_conversions": 0,
            "failed_mappings": 0,
        }

        # 銘柄状態管理（後から上場対応）
        self.symbol_status = {
            "failed_mapping": {},  # マッピング失敗銘柄: {symbol: last_check_time}
            "mexc_exclusive": {},  # MEXC専用銘柄: {symbol: last_check_time}
            "pending_recheck": set(),  # 再チェック待ち銘柄
        }

        # 更新制御
        self._lock = threading.Lock()
        self._last_update = 0
        self._update_interval = 3600  # 1時間ごとに更新
        self._recheck_interval = 86400  # 24時間ごとに失敗銘柄を再チェック

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
        """
        MEXCシンボルをBybitシンボルに変換（改良版）

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            対応するBybitシンボル
        """
        # 1. 特殊ケースをチェック（最優先）
        if mexc_symbol in self.MEXC_TO_BYBIT_SPECIAL_CASES:
            self.mapping_stats["special_cases_used"] += 1
            converted = self.MEXC_TO_BYBIT_SPECIAL_CASES[mexc_symbol]
            logger.debug(f"Special case mapping: {mexc_symbol} -> {converted}")
            return converted

        # 2. 近似マッピングをチェック
        if mexc_symbol in self.MEXC_TO_BYBIT_APPROXIMATE:
            self.mapping_stats["approximate_matches"] += 1
            converted = self.MEXC_TO_BYBIT_APPROXIMATE[mexc_symbol]
            logger.debug(f"Approximate mapping: {mexc_symbol} -> {converted}")
            return converted

        # 3. 基本変換：アンダースコア除去
        if mexc_symbol.endswith("_USDT"):
            self.mapping_stats["basic_conversions"] += 1
            converted = mexc_symbol.replace("_USDT", "USDT")
            logger.debug(f"Basic conversion: {mexc_symbol} -> {converted}")
            return converted

        # 4. USD -> USDT変換
        if mexc_symbol.endswith("_USD"):
            self.mapping_stats["basic_conversions"] += 1
            base = mexc_symbol.replace("_USD", "")
            converted = f"{base}USDT"
            logger.debug(f"USD to USDT conversion: {mexc_symbol} -> {converted}")
            return converted

        # 5. USDC -> USDT変換
        if mexc_symbol.endswith("_USDC"):
            self.mapping_stats["basic_conversions"] += 1
            base = mexc_symbol.replace("_USDC", "")
            converted = f"{base}USDT"
            logger.debug(f"USDC to USDT conversion: {mexc_symbol} -> {converted}")
            return converted

        # 6. そのまま返す（変換不能）
        self.mapping_stats["failed_mappings"] += 1
        logger.debug(f"No conversion available for: {mexc_symbol}")
        return mexc_symbol

    def _convert_bybit_to_mexc(self, bybit_symbol: str) -> str:
        """BybitシンボルをMEXCシンボルに変換"""
        # 特殊ケースの逆マッピング（実際のBybit銘柄名で修正）
        special_cases_reverse = {
            "SHIB1000USDT": "SHIB_USDT",  # 修正: 実際のBybitはSHIB1000USDT
            "1000PEPEUSDT": "PEPE_USDT",
            "1000BONKUSDT": "BONK_USDT",
            "1000FLOKIUSDT": "FLOKI_USDT",
            "1000LUNCUSDT": "LUNC_USDT",
            "1000XECUSDT": "XEC_USDT",
            "1000TURBOUSDT": "TURBO_USDT",
            "FILUSDT": "FILECOIN_USDT",
            "RAYDIUMUSDT": "RAY_USDT",
            "LUNA2USDT": "LUNA_USDT",  # 確認済み: BybitはLUNA2USDT
        }

        # 特殊ケースをチェック
        if bybit_symbol in special_cases_reverse:
            return special_cases_reverse[bybit_symbol]

        # 基本変換
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

        current_time = time.time()

        # 過去にMEXC専用と判定されたが、再チェック期間が経過した場合は再試行
        if mexc_symbol in self.symbol_status["mexc_exclusive"]:
            last_check = self.symbol_status["mexc_exclusive"][mexc_symbol]
            if current_time - last_check < self._recheck_interval:
                logger.debug(f"Symbol {mexc_symbol} recently checked as MEXC-exclusive")
                return False
            else:
                logger.info(
                    f"🔄 Re-checking previously MEXC-exclusive symbol: {mexc_symbol}"
                )

        # 過去にマッピング失敗したが、再チェック期間が経過した場合は再試行
        if mexc_symbol in self.symbol_status["failed_mapping"]:
            last_check = self.symbol_status["failed_mapping"][mexc_symbol]
            if current_time - last_check < self._recheck_interval:
                logger.debug(f"Symbol {mexc_symbol} recently failed mapping check")
                return False
            else:
                logger.info(f"🔄 Re-checking previously failed symbol: {mexc_symbol}")

        # MEXC専用銘柄チェック（厳格なパターンのみ事前除外）
        if self._is_strictly_mexc_exclusive(mexc_symbol):
            logger.debug(
                f"Symbol {mexc_symbol} is strictly MEXC-exclusive, skipping Bybit check"
            )
            with self._lock:
                self.symbol_status["mexc_exclusive"][mexc_symbol] = current_time
            return False

        # キャッシュにない場合は直接チェック
        try:
            # 複数の変換パターンを試す（新銘柄対応）
            conversion_candidates = self._generate_conversion_candidates(mexc_symbol)

            for bybit_candidate in conversion_candidates:
                is_available = self.bybit_client.check_symbol_availability(
                    bybit_candidate
                )

                if is_available:
                    with self._lock:
                        self.mexc_to_bybit[mexc_symbol] = bybit_candidate
                        self.tradeable_mexc_symbols.add(mexc_symbol)

                        # 成功時は失敗記録から削除
                        self.symbol_status["failed_mapping"].pop(mexc_symbol, None)
                        self.symbol_status["mexc_exclusive"].pop(mexc_symbol, None)

                        # 新しいマッピングパターンを学習
                        self._learn_new_mapping_pattern(mexc_symbol, bybit_candidate)

                        logger.info(
                            f"🆕 New symbol mapping discovered: {mexc_symbol} -> {bybit_candidate}"
                        )

                    return True

            # どの変換候補も見つからない場合
            with self._lock:
                if self.is_mexc_exclusive_symbol(mexc_symbol):
                    self.symbol_status["mexc_exclusive"][mexc_symbol] = current_time
                    logger.info(f"❌ Symbol {mexc_symbol} confirmed as MEXC exclusive")
                else:
                    self.symbol_status["failed_mapping"][mexc_symbol] = current_time
                    logger.info(
                        f"❌ Symbol {mexc_symbol} mapping failed, will retry in 24h"
                    )

            return False

        except Exception as e:
            logger.error(f"Error checking symbol availability for {mexc_symbol}: {e}")
            with self._lock:
                self.symbol_status["failed_mapping"][mexc_symbol] = current_time
            return False

    def _generate_conversion_candidates(self, mexc_symbol: str) -> List[str]:
        """
        新銘柄のための変換候補を生成

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            可能性のあるBybit銘柄名のリスト
        """
        candidates = []

        # 1. 基本変換（アンダースコア除去）
        basic_conversion = self._convert_mexc_to_bybit(mexc_symbol)
        candidates.append(basic_conversion)

        if mexc_symbol.endswith("_USDT"):
            base_symbol = mexc_symbol.replace("_USDT", "")

            # 2. 1000倍表記のパターン
            candidates.extend(
                [
                    f"1000{base_symbol}USDT",
                    f"{base_symbol}1000USDT",
                    f"10000{base_symbol}USDT",
                    f"{base_symbol}10000USDT",
                ]
            )

            # 3. 略称パターン（最初の3-4文字）
            if len(base_symbol) > 4:
                short_name = base_symbol[:3]
                candidates.append(f"{short_name}USDT")
                short_name = base_symbol[:4]
                candidates.append(f"{short_name}USDT")

            # 4. 数字サフィックスパターン
            candidates.extend([f"{base_symbol}2USDT", f"{base_symbol}3USDT"])

        # 重複を除去して返す
        return list(set(candidates))

    def _learn_new_mapping_pattern(self, mexc_symbol: str, bybit_symbol: str):
        """
        新しいマッピングパターンを学習して将来の予測に活用

        Args:
            mexc_symbol: MEXCシンボル
            bybit_symbol: Bybitシンボル
        """
        # パターン分析
        if mexc_symbol.endswith("_USDT") and bybit_symbol.endswith("USDT"):
            mexc_base = mexc_symbol.replace("_USDT", "")
            bybit_base = bybit_symbol.replace("USDT", "")

            # 1000倍表記パターンの検出
            if bybit_base.startswith("1000") and bybit_base[4:] == mexc_base:
                logger.info(f"📈 Learned 1000x pattern: {mexc_base} -> 1000{mexc_base}")
            elif bybit_base.endswith("1000") and bybit_base[:-4] == mexc_base:
                logger.info(
                    f"📈 Learned reverse 1000x pattern: {mexc_base} -> {mexc_base}1000"
                )

            # 略称パターンの検出
            elif len(mexc_base) > len(bybit_base) and mexc_base.startswith(bybit_base):
                logger.info(
                    f"📈 Learned abbreviation pattern: {mexc_base} -> {bybit_base}"
                )

    def is_mexc_exclusive_symbol(self, mexc_symbol: str) -> bool:
        """
        MEXCにのみ存在する銘柄かどうかを判定（拡張版）

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            MEXC専用銘柄かどうか
        """
        # 既知のMEXC専用銘柄パターン
        mexc_exclusive_patterns = [
            # 株式トークン
            "STOCK_USDT",
            "AAPLSTOCK_USDT",
            "CVNASTOCK_USDT",
            # 政治・ミーム系トークン
            "TRUMP_USDT",
            "BIDEN_USDT",
            "FARTBOY_USDT",
            "ELON_USDT",
            "GROK_USDT",
            # その他のMEXC専用
            "TST_USDT",
            "BOSS_USDT",
            "HOUSE_USDT",
            "TOKEN_USDT",
            "TEST_USDT",
            # 一般的なテスト用トークン
            "TESTNET_USDT",
            "DEMO_USDT",
        ]

        # パターンマッチング
        for pattern in mexc_exclusive_patterns:
            if pattern in mexc_symbol or mexc_symbol in pattern:
                return True

        # 銘柄名による推定
        if mexc_symbol.endswith("_USDT"):
            base = mexc_symbol.replace("_USDT", "")

            # 株式銘柄パターン
            if "STOCK" in base or base in ["AAPL", "TSLA", "GOOGL", "MSFT", "AMZN"]:
                return True

            # 政治系パターン
            if base in ["TRUMP", "BIDEN", "MAGA", "USA", "POLITICAL"]:
                return True

            # ミーム系の特殊パターン
            if base in ["FARTBOY", "URANUS", "SCAM", "RUG"]:
                return True

        return False

    def get_mexc_exclusive_symbols(self) -> List[str]:
        """MEXC専用銘柄のリストを取得"""
        exclusive_symbols = []

        # 既知の専用銘柄
        known_exclusive = [
            "TRUMP_USDT",
            "FARTBOY_USDT",
            "ELON_USDT",
            "GROK_USDT",
            "AAPLSTOCK_USDT",
            "CVNASTOCK_USDT",
            "ICGSTOCK_USDT",
            "TST_USDT",
            "BOSS_USDT",
            "HOUSE_USDT",
            "TOKEN_USDT",
        ]

        exclusive_symbols.extend(known_exclusive)

        # キャッシュから判定可能なものを追加
        with self._lock:
            for symbol in self.mexc_to_bybit.keys():
                if self.is_mexc_exclusive_symbol(symbol):
                    exclusive_symbols.append(symbol)

        return list(set(exclusive_symbols))

    def auto_discover_new_symbols(self, mexc_symbols: List[str]) -> Dict[str, str]:
        """
        新しいMEXC銘柄を自動発見してマッピングを試行

        Args:
            mexc_symbols: MEXCで見つかった新銘柄リスト

        Returns:
            発見された新しいマッピング
        """
        new_mappings = {}

        for mexc_symbol in mexc_symbols:
            # まだマッピングされていない銘柄のみ処理
            if not self.is_tradeable_on_bybit(mexc_symbol):
                logger.info(f"🔍 Discovering new symbol: {mexc_symbol}")

                if self.check_symbol_realtime(mexc_symbol):
                    bybit_symbol = self.get_bybit_symbol(mexc_symbol)
                    new_mappings[mexc_symbol] = bybit_symbol
                    logger.info(f"✅ New mapping: {mexc_symbol} -> {bybit_symbol}")
                else:
                    logger.info(f"❌ Symbol {mexc_symbol} not available on Bybit")

        return new_mappings

    def _is_strictly_mexc_exclusive(self, mexc_symbol: str) -> bool:
        """
        厳格にMEXC専用と判定できる銘柄（後から他の取引所で上場する可能性が低い）

        Args:
            mexc_symbol: MEXCシンボル

        Returns:
            厳格にMEXC専用かどうか
        """
        if mexc_symbol.endswith("_USDT"):
            base = mexc_symbol.replace("_USDT", "")

            # 株式トークンは基本的にMEXC専用
            if "STOCK" in base or base in [
                "AAPL",
                "TSLA",
                "GOOGL",
                "MSFT",
                "AMZN",
                "NVDA",
            ]:
                return True

            # 政治系トークンも基本的にMEXC専用
            if base in ["TRUMP", "BIDEN", "MAGA"]:
                return True

            # 明らかなジョークトークン
            if base in ["FARTBOY", "SCAM", "RUG", "PONZI"]:
                return True

        return False

    def periodic_recheck_failed_symbols(self) -> Dict[str, str]:
        """
        定期的な失敗銘柄の再チェック

        Returns:
            新たにマッピングできた銘柄の辞書
        """
        current_time = time.time()
        new_mappings = {}
        recheck_candidates = []

        with self._lock:
            # 再チェック対象の収集
            for symbol, last_check in self.symbol_status["failed_mapping"].items():
                if current_time - last_check >= self._recheck_interval:
                    recheck_candidates.append(symbol)

            for symbol, last_check in self.symbol_status["mexc_exclusive"].items():
                if current_time - last_check >= self._recheck_interval:
                    if not self._is_strictly_mexc_exclusive(symbol):
                        recheck_candidates.append(symbol)

        if not recheck_candidates:
            logger.debug("No symbols need rechecking")
            return new_mappings

        logger.info(
            f"🔄 Rechecking {len(recheck_candidates)} previously failed symbols"
        )

        for mexc_symbol in recheck_candidates:
            logger.info(f"🔍 Rechecking symbol: {mexc_symbol}")

            # 再チェック実行
            if self.check_symbol_realtime(mexc_symbol):
                bybit_symbol = self.get_bybit_symbol(mexc_symbol)
                new_mappings[mexc_symbol] = bybit_symbol
                logger.info(
                    f"✅ Previously failed symbol now available: {mexc_symbol} -> {bybit_symbol}"
                )

            # API制限を避けるため少し待機
            time.sleep(0.1)

        if new_mappings:
            logger.info(f"🎉 Recheck discovered {len(new_mappings)} new mappings!")
        else:
            logger.info("No new mappings discovered during recheck")

        return new_mappings

    def schedule_symbol_for_recheck(self, mexc_symbol: str):
        """
        銘柄を再チェック対象に追加

        Args:
            mexc_symbol: 再チェックする銘柄
        """
        with self._lock:
            self.symbol_status["pending_recheck"].add(mexc_symbol)
            logger.info(f"📅 Scheduled {mexc_symbol} for next recheck cycle")

    def get_failed_symbols_stats(self) -> Dict[str, any]:
        """
        失敗した銘柄の統計情報を取得

        Returns:
            統計情報の辞書
        """
        current_time = time.time()

        with self._lock:
            failed_count = len(self.symbol_status["failed_mapping"])
            mexc_exclusive_count = len(self.symbol_status["mexc_exclusive"])
            pending_recheck_count = len(self.symbol_status["pending_recheck"])

            # 再チェック期間が過ぎた銘柄数
            failed_ready_for_recheck = sum(
                1
                for last_check in self.symbol_status["failed_mapping"].values()
                if current_time - last_check >= self._recheck_interval
            )

            mexc_ready_for_recheck = sum(
                1
                for symbol, last_check in self.symbol_status["mexc_exclusive"].items()
                if (
                    current_time - last_check >= self._recheck_interval
                    and not self._is_strictly_mexc_exclusive(symbol)
                )
            )

            return {
                "failed_mapping_count": failed_count,
                "mexc_exclusive_count": mexc_exclusive_count,
                "pending_recheck_count": pending_recheck_count,
                "failed_ready_for_recheck": failed_ready_for_recheck,
                "mexc_ready_for_recheck": mexc_ready_for_recheck,
                "total_ready_for_recheck": failed_ready_for_recheck
                + mexc_ready_for_recheck,
                "recheck_interval_hours": self._recheck_interval / 3600,
                "sample_failed_symbols": list(
                    self.symbol_status["failed_mapping"].keys()
                )[:5],
                "sample_mexc_exclusive": list(
                    self.symbol_status["mexc_exclusive"].keys()
                )[:5],
            }

    def manual_recheck_symbol(self, mexc_symbol: str) -> bool:
        """
        指定された銘柄を手動で再チェック

        Args:
            mexc_symbol: 再チェックする銘柄

        Returns:
            再チェック結果（成功/失敗）
        """
        logger.info(f"🔍 Manual recheck requested for: {mexc_symbol}")

        # 既存の記録をクリア
        with self._lock:
            self.symbol_status["failed_mapping"].pop(mexc_symbol, None)
            self.symbol_status["mexc_exclusive"].pop(mexc_symbol, None)
            self.symbol_status["pending_recheck"].discard(mexc_symbol)

        # 再チェック実行
        result = self.check_symbol_realtime(mexc_symbol)

        if result:
            bybit_symbol = self.get_bybit_symbol(mexc_symbol)
            logger.info(
                f"✅ Manual recheck successful: {mexc_symbol} -> {bybit_symbol}"
            )
        else:
            logger.info(f"❌ Manual recheck failed: {mexc_symbol}")

        return result
