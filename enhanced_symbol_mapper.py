#!/usr/bin/env python3
"""
Enhanced Symbol Mapper for MEXC <-> Bybit Trading Pairs

This module provides comprehensive mapping between MEXC and Bybit symbol formats,
including special cases, 1000x notation, and rebranding scenarios.
"""

import json
import logging
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class EnhancedSymbolMapper:
    """Enhanced symbol mapper with comprehensive mapping rules"""

    def __init__(self):
        # 基本的な変換ルール
        self.basic_mexc_to_bybit = self._convert_mexc_to_bybit_basic
        self.basic_bybit_to_mexc = self._convert_bybit_to_mexc_basic

        # 特殊ケースの完全マッピングテーブル（手動で精査）
        self.special_cases_mexc_to_bybit = {
            # 1000倍表記の特殊ケース（MEXCには通常名、Bybitには1000倍）
            "SHIB_USDT": "1000SHIBUSDT",  # MEXC: SHIB, Bybit: 1000SHIB
            "PEPE_USDT": "1000PEPEUSDT",  # MEXC: PEPE, Bybit: 1000PEPE
            "BONK_USDT": "1000BONKUSDT",  # MEXC: BONK, Bybit: 1000BONK
            "FLOKI_USDT": "1000FLOKIUSDT",  # MEXC: FLOKI, Bybit: 1000FLOKI
            "LUNC_USDT": "1000LUNCUSDT",  # MEXC: LUNC, Bybit: 1000LUNC
            "XEC_USDT": "1000XECUSDT",  # MEXC: XEC, Bybit: 1000XEC
            "TURBO_USDT": "1000TURBOUSDT",  # MEXC: TURBO, Bybit: 1000TURBO
            # 名称変更・リブランディング
            "FILECOIN_USDT": "FILUSDT",  # MEXC: FILECOIN, Bybit: FIL
            "RAY_USDT": "RAYDIUMUSDT",  # MEXC: RAY, Bybit: RAYDIUM
            # その他の特殊ケース
            "LUNA_USDT": "LUNA2USDT",  # MEXC: LUNA, Bybit: LUNA2
            # 存在しない可能性が高い自動マッチングを除外
            # （類似名検出で誤検出されたもの）
        }

        # 逆マッピング（自動生成）
        self.special_cases_bybit_to_mexc = {
            v: k for k, v in self.special_cases_mexc_to_bybit.items()
        }

        # マッピング不可能なMEXC専用銘柄（Bybitには存在しない）
        self.mexc_exclusive_symbols = {
            # トークンエアドロップ・ミーム系
            "TRUMP_USDT",
            "FARTBOY_USDT",
            "ELON_USDT",
            "GROK_USDT",
            # 株式トークン
            "AAPLSTOCK_USDT",
            "CVNASTOCK_USDT",
            "ICGSTOCK_USDT",
            "BMNRSTOCK_USDT",
            "ETHWSTOCK_USDT",
            # その他のMEXC専用
            "TST_USDT",
            "BOSS_USDT",
            "HOUSE_USDT",
            "TOKEN_USDT",
            "SPEC_USDT",
            "MORE_USDT",
            "THE_USDT",
        }

        # マッピング不可能なBybit専用銘柄
        self.bybit_exclusive_symbols = {
            # Bybit独自の表記
            "SHIB1000USDT",
            "1000000CHEEMSUSDT",
            "10000SATSUSDT",
            "HPOS10IUSDT",
            "ETHBTCUSDT",
            "10000QUBICUSDT",
            "10000ELONUSDT",
            # Bybit専用銘柄
            "SOLAYERUSDT",
            "KUSDT",
            "RONINUSDT",
            "FUELUSDT",
            "DEXEUSDT",
            "BEAMUSDT",
            "ARCUSDT",
        }

        # 実際のAPI結果から読み込み（もしあれば）
        self._load_api_results()

    def _load_api_results(self):
        """API調査結果があれば読み込み"""
        try:
            with open("symbol_analysis.json", "r", encoding="utf-8") as f:
                data = json.load(f)

                # 基本マッピングを確認用として保存
                self.api_basic_matches = {
                    item["mexc"]: item["bybit"]
                    for item in data.get("basic_conversion_matches", [])
                }

                logger.info(
                    f"Loaded {len(self.api_basic_matches)} basic matches from API results"
                )

        except FileNotFoundError:
            logger.info("No API results file found, using predefined mappings only")
        except Exception as e:
            logger.warning(f"Error loading API results: {e}")

    def _convert_mexc_to_bybit_basic(self, mexc_symbol: str) -> str:
        """基本的なMEXC -> Bybit変換（_USDT -> USDT）"""
        if mexc_symbol.endswith("_USDT"):
            return mexc_symbol.replace("_USDT", "USDT")
        return mexc_symbol

    def _convert_bybit_to_mexc_basic(self, bybit_symbol: str) -> str:
        """基本的なBybit -> MEXC変換（USDT -> _USDT）"""
        if bybit_symbol.endswith("USDT") and not bybit_symbol.endswith("_USDT"):
            base = bybit_symbol[:-4]
            return f"{base}_USDT"
        return bybit_symbol

    def mexc_to_bybit(self, mexc_symbol: str) -> Optional[str]:
        """
        MEXC銘柄をBybit銘柄に変換

        Args:
            mexc_symbol: MEXCの銘柄名（例: BTC_USDT）

        Returns:
            対応するBybit銘柄名、存在しない場合はNone
        """
        # MEXC専用銘柄チェック
        if mexc_symbol in self.mexc_exclusive_symbols:
            return None

        # 特殊ケースチェック
        if mexc_symbol in self.special_cases_mexc_to_bybit:
            return self.special_cases_mexc_to_bybit[mexc_symbol]

        # 基本変換
        return self.basic_mexc_to_bybit(mexc_symbol)

    def bybit_to_mexc(self, bybit_symbol: str) -> Optional[str]:
        """
        Bybit銘柄をMEXC銘柄に変換

        Args:
            bybit_symbol: Bybitの銘柄名（例: BTCUSDT）

        Returns:
            対応するMEXC銘柄名、存在しない場合はNone
        """
        # Bybit専用銘柄チェック
        if bybit_symbol in self.bybit_exclusive_symbols:
            return None

        # 特殊ケースチェック
        if bybit_symbol in self.special_cases_bybit_to_mexc:
            return self.special_cases_bybit_to_mexc[bybit_symbol]

        # 基本変換
        return self.basic_bybit_to_mexc(bybit_symbol)

    def get_comprehensive_mapping(self) -> Dict[str, str]:
        """完全なマッピングテーブルを生成"""
        mapping = {}

        # API結果の基本マッチング
        if hasattr(self, "api_basic_matches"):
            # API結果から特殊ケース以外を追加
            for mexc, bybit in self.api_basic_matches.items():
                if mexc not in self.special_cases_mexc_to_bybit:
                    mapping[mexc] = bybit

        # 特殊ケースを追加
        mapping.update(self.special_cases_mexc_to_bybit)

        return mapping

    def validate_mapping(
        self, mexc_symbols: Set[str], bybit_symbols: Set[str]
    ) -> Dict[str, any]:
        """マッピングを検証"""
        validation_result = {
            "total_mexc_symbols": len(mexc_symbols),
            "total_bybit_symbols": len(bybit_symbols),
            "successful_mappings": 0,
            "failed_mappings": 0,
            "mexc_exclusive_count": 0,
            "bybit_exclusive_count": 0,
            "mapping_details": [],
        }

        for mexc_symbol in mexc_symbols:
            bybit_mapped = self.mexc_to_bybit(mexc_symbol)

            if bybit_mapped is None:
                validation_result["mexc_exclusive_count"] += 1
                validation_result["mapping_details"].append(
                    {"mexc": mexc_symbol, "status": "mexc_exclusive"}
                )
            elif bybit_mapped in bybit_symbols:
                validation_result["successful_mappings"] += 1
                validation_result["mapping_details"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_mapped, "status": "success"}
                )
            else:
                validation_result["failed_mappings"] += 1
                validation_result["mapping_details"].append(
                    {
                        "mexc": mexc_symbol,
                        "bybit_expected": bybit_mapped,
                        "status": "failed",
                    }
                )

        return validation_result

    def get_trading_pair_recommendations(self) -> Dict[str, any]:
        """取引ペア推奨リストを生成"""
        mapping = self.get_comprehensive_mapping()

        # カテゴリ別に分類
        categories = {
            "major_coins": [],  # 主要コイン
            "defi_tokens": [],  # DeFiトークン
            "meme_coins": [],  # ミームコイン
            "gaming_metaverse": [],  # ゲーム・メタバース
            "layer1_layer2": [],  # レイヤー1・2
            "others": [],  # その他
        }

        # 主要コインの判定（例）
        major_coins = {
            "BTC",
            "ETH",
            "BNB",
            "ADA",
            "SOL",
            "DOT",
            "AVAX",
            "MATIC",
            "LINK",
            "UNI",
        }
        meme_coins = {"SHIB", "DOGE", "PEPE", "FLOKI", "BONK", "WIF"}
        defi_tokens = {"AAVE", "COMP", "SUSHI", "CAKE", "UNI", "INCH", "CRV"}

        for mexc_symbol, bybit_symbol in mapping.items():
            base_coin = mexc_symbol.replace("_USDT", "")

            if base_coin in major_coins:
                categories["major_coins"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_symbol}
                )
            elif base_coin in meme_coins or "1000" in bybit_symbol:
                categories["meme_coins"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_symbol}
                )
            elif base_coin in defi_tokens:
                categories["defi_tokens"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_symbol}
                )
            else:
                categories["others"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_symbol}
                )

        return {
            "categories": categories,
            "summary": {
                "total_tradeable_pairs": len(mapping),
                "major_coins": len(categories["major_coins"]),
                "defi_tokens": len(categories["defi_tokens"]),
                "meme_coins": len(categories["meme_coins"]),
                "gaming_metaverse": len(categories["gaming_metaverse"]),
                "layer1_layer2": len(categories["layer1_layer2"]),
                "others": len(categories["others"]),
            },
        }

    def export_mapping_as_code(self, format: str = "python") -> str:
        """マッピングをコード形式でエクスポート"""
        mapping = self.get_comprehensive_mapping()

        if format == "python":
            lines = ["# MEXC to Bybit Symbol Mapping", "MEXC_TO_BYBIT_MAPPING = {"]
            for mexc, bybit in sorted(mapping.items()):
                lines.append(f'    "{mexc}": "{bybit}",')
            lines.append("}")

            # 逆マッピングも生成
            lines.extend(
                ["", "# Bybit to MEXC Symbol Mapping", "BYBIT_TO_MEXC_MAPPING = {"]
            )
            for mexc, bybit in sorted(mapping.items()):
                lines.append(f'    "{bybit}": "{mexc}",')
            lines.append("}")

            return "\n".join(lines)

        elif format == "json":
            return json.dumps(
                {
                    "mexc_to_bybit": mapping,
                    "bybit_to_mexc": {v: k for k, v in mapping.items()},
                },
                indent=2,
                ensure_ascii=False,
            )

        else:
            raise ValueError(f"Unsupported format: {format}")


def main():
    """Test the enhanced symbol mapper"""
    mapper = EnhancedSymbolMapper()

    # テスト用の銘柄
    test_symbols = [
        "BTC_USDT",
        "ETH_USDT",
        "SHIB_USDT",
        "PEPE_USDT",
        "FILECOIN_USDT",
        "RAY_USDT",
        "TRUMP_USDT",
        "AAPLSTOCK_USDT",
    ]

    print("Enhanced Symbol Mapper Test Results")
    print("=" * 50)

    for mexc_symbol in test_symbols:
        bybit_result = mapper.mexc_to_bybit(mexc_symbol)
        if bybit_result:
            # 逆変換テスト
            reverse_result = mapper.bybit_to_mexc(bybit_result)
            status = "OK" if reverse_result == mexc_symbol else "WARN"
        else:
            status = "FAIL"

        print(f"[{status}] {mexc_symbol} -> {bybit_result}")

    # マッピングテーブルをエクスポート
    print("\n" + "=" * 50)
    print("Exporting comprehensive mapping...")

    mapping_code = mapper.export_mapping_as_code("python")
    with open("comprehensive_symbol_mapping.py", "w", encoding="utf-8") as f:
        f.write(mapping_code)

    print("Mapping exported to comprehensive_symbol_mapping.py")

    # 推奨リスト生成
    recommendations = mapper.get_trading_pair_recommendations()
    print(f"\nTrading Pair Recommendations:")
    for category, count in recommendations["summary"].items():
        if count > 0:
            print(f"  {category}: {count} pairs")


if __name__ == "__main__":
    main()
