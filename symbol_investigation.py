#!/usr/bin/env python3
"""
MEXCとBybitの暗号通貨取引ペア銘柄名の違いを調査するスクリプト

このスクリプトは以下を実行します：
1. MEXCから取引可能な銘柄一覧を取得
2. Bybitから取引可能な銘柄一覧を取得
3. 不一致がある銘柄を特定
4. マッピングテーブルを生成

調査対象：
- プレフィックス・サフィックスの違い (BTC_USDT vs BTCUSDT)
- 特別な銘柄名の違い (1000SHIB vs 1000SHIBUSDT)
- 新旧名称の違い (MATIC vs POL)
"""

import json
import logging
import re
import time
from collections import defaultdict
from typing import Dict, List, Set, Tuple

import requests

# ログ設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SymbolInvestigator:
    """銘柄名調査クラス"""

    def __init__(self):
        self.mexc_symbols: Set[str] = set()
        self.bybit_symbols: Set[str] = set()
        self.mapping_issues: Dict[str, any] = {}

        # 既知の特殊ケース
        self.known_special_cases = {
            # 1000倍の表記
            "1000SHIB_USDT": "1000SHIBUSDT",
            "1000PEPE_USDT": "1000PEPEUSDT",
            "1000BONK_USDT": "1000BONKUSDT",
            "1000FLOKI_USDT": "1000FLOKIUSDT",
            "1000LUNC_USDT": "1000LUNCUSDT",
            "1000XEC_USDT": "1000XECUSDT",
            "1000RATS_USDT": "1000RATSUSDT",
            # 特殊な名称の違い
            "MATIC_USDT": "MATICUSDT",  # または POLUSDT に変わっている可能性
            "FTT_USDT": "FTTUSDT",
            # 新旧名称（推定）
            "LUNA_USDT": "LUNAUSDT",  # または LUNCUSDT
            "UST_USDT": "USTUSDT",  # または削除されている可能性
        }

    def get_mexc_symbols(self) -> Set[str]:
        """MEXCから取引可能なUSDTペア銘柄を取得"""
        logger.info("Getting MEXC symbols...")

        try:
            # MEXC API: Get all contracts
            url = "https://contract.mexc.com/api/v1/contract/detail"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            if not data.get("success", False):
                logger.error(f"MEXC API error: {data}")
                return set()

            symbols = set()
            contracts = data.get("data", [])

            for contract in contracts:
                symbol = contract.get("symbol", "")
                state = contract.get("state", 0)  # 0=正常取引, 1=停止

                # USDTペアで取引中のもののみ
                if symbol.endswith("_USDT") and state == 0:
                    symbols.add(symbol)

            logger.info(f"Found {len(symbols)} MEXC USDT symbols")
            self.mexc_symbols = symbols
            return symbols

        except Exception as e:
            logger.error(f"Error getting MEXC symbols: {e}")
            return set()

    def get_bybit_symbols(self) -> Set[str]:
        """Bybitから取引可能なUSDTペア銘柄を取得"""
        logger.info("Getting Bybit symbols...")

        try:
            # Bybit API: Get instruments info
            url = "https://api.bybit.com/v5/market/instruments-info"
            params = {"category": "linear"}
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get("retCode") != 0:
                logger.error(f"Bybit API error: {data}")
                return set()

            symbols = set()
            instruments = data.get("result", {}).get("list", [])

            for instrument in instruments:
                symbol = instrument.get("symbol", "")
                status = instrument.get("status", "")
                quote_coin = instrument.get("quoteCoin", "")

                # USDTペアで取引中のもののみ
                if (
                    symbol.endswith("USDT")
                    and status == "Trading"
                    and quote_coin == "USDT"
                ):
                    symbols.add(symbol)

            logger.info(f"Found {len(symbols)} Bybit USDT symbols")
            self.bybit_symbols = symbols
            return symbols

        except Exception as e:
            logger.error(f"Error getting Bybit symbols: {e}")
            return set()

    def convert_mexc_to_bybit_basic(self, mexc_symbol: str) -> str:
        """基本的なMEXCからBybitへの変換"""
        if mexc_symbol.endswith("_USDT"):
            return mexc_symbol.replace("_USDT", "USDT")
        return mexc_symbol

    def convert_bybit_to_mexc_basic(self, bybit_symbol: str) -> str:
        """基本的なBybitからMEXCへの変換"""
        if bybit_symbol.endswith("USDT"):
            base = bybit_symbol[:-4]
            return f"{base}_USDT"
        return bybit_symbol

    def analyze_symbol_differences(self) -> Dict[str, any]:
        """銘柄の違いを分析"""
        logger.info("Analyzing symbol differences...")

        if not self.mexc_symbols or not self.bybit_symbols:
            logger.warning("Symbol lists are empty, fetching first...")
            self.get_mexc_symbols()
            self.get_bybit_symbols()

        analysis = {
            "mexc_total": len(self.mexc_symbols),
            "bybit_total": len(self.bybit_symbols),
            "perfect_matches": [],
            "basic_conversion_matches": [],
            "mexc_only": [],
            "bybit_only": [],
            "potential_special_cases": [],
            "suggested_mapping": {},
        }

        # 基本変換でマッチするもの
        basic_matches = {}
        for mexc_symbol in self.mexc_symbols:
            bybit_equivalent = self.convert_mexc_to_bybit_basic(mexc_symbol)
            if bybit_equivalent in self.bybit_symbols:
                basic_matches[mexc_symbol] = bybit_equivalent
                analysis["basic_conversion_matches"].append(
                    {"mexc": mexc_symbol, "bybit": bybit_equivalent}
                )

        # MEXC特有のシンボル（Bybitにない）
        mexc_only = []
        for mexc_symbol in self.mexc_symbols:
            bybit_equivalent = self.convert_mexc_to_bybit_basic(mexc_symbol)
            if bybit_equivalent not in self.bybit_symbols:
                mexc_only.append(mexc_symbol)

        analysis["mexc_only"] = mexc_only

        # Bybit特有のシンボル（MEXCにない）
        bybit_only = []
        for bybit_symbol in self.bybit_symbols:
            mexc_equivalent = self.convert_bybit_to_mexc_basic(bybit_symbol)
            if mexc_equivalent not in self.mexc_symbols:
                bybit_only.append(bybit_symbol)

        analysis["bybit_only"] = bybit_only

        # 特殊ケースの推定
        special_cases = self.find_special_cases(mexc_only, bybit_only)
        analysis["potential_special_cases"] = special_cases

        # マッピングテーブル生成
        mapping_table = self.generate_mapping_table(basic_matches, special_cases)
        analysis["suggested_mapping"] = mapping_table

        logger.info(
            f"Analysis complete: {len(basic_matches)} basic matches, {len(special_cases)} special cases"
        )

        return analysis

    def find_special_cases(
        self, mexc_only: List[str], bybit_only: List[str]
    ) -> List[Dict[str, str]]:
        """特殊ケースを推定"""
        special_cases = []

        # 既知の特殊ケースをチェック
        for mexc_symbol, expected_bybit in self.known_special_cases.items():
            if mexc_symbol in mexc_only and expected_bybit in bybit_only:
                special_cases.append(
                    {
                        "mexc": mexc_symbol,
                        "bybit": expected_bybit,
                        "type": "known_special_case",
                    }
                )

        # 1000倍表記の推定
        for mexc_symbol in mexc_only:
            if mexc_symbol.startswith("1000") and mexc_symbol.endswith("_USDT"):
                base = mexc_symbol[4:-5]  # "1000" と "_USDT" を除去
                potential_bybit = f"1000{base}USDT"
                if potential_bybit in bybit_only:
                    special_cases.append(
                        {
                            "mexc": mexc_symbol,
                            "bybit": potential_bybit,
                            "type": "1000x_notation",
                        }
                    )

        # 類似名の推定（編集距離ベース）
        for mexc_symbol in mexc_only[:20]:  # パフォーマンス上限定
            mexc_base = mexc_symbol.replace("_USDT", "")
            best_match = None
            best_score = float("inf")

            for bybit_symbol in bybit_only:
                bybit_base = bybit_symbol.replace("USDT", "")
                score = self.levenshtein_distance(mexc_base, bybit_base)
                if score < best_score and score <= 2:  # 編集距離2以下
                    best_score = score
                    best_match = bybit_symbol

            if best_match:
                special_cases.append(
                    {
                        "mexc": mexc_symbol,
                        "bybit": best_match,
                        "type": "similar_name",
                        "distance": best_score,
                    }
                )

        return special_cases

    def levenshtein_distance(self, s1: str, s2: str) -> int:
        """レーベンシュタイン距離を計算"""
        if len(s1) < len(s2):
            return self.levenshtein_distance(s2, s1)

        if len(s2) == 0:
            return len(s1)

        previous_row = list(range(len(s2) + 1))
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def generate_mapping_table(
        self, basic_matches: Dict[str, str], special_cases: List[Dict[str, str]]
    ) -> Dict[str, str]:
        """最終的なマッピングテーブルを生成"""
        mapping = {}

        # 基本変換
        mapping.update(basic_matches)

        # 特殊ケース
        for case in special_cases:
            mapping[case["mexc"]] = case["bybit"]

        return mapping

    def save_analysis_to_file(
        self, analysis: Dict[str, any], filename: str = "symbol_analysis.json"
    ):
        """分析結果をファイルに保存"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(analysis, f, indent=2, ensure_ascii=False)
            logger.info(f"Analysis saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")

    def print_summary(self, analysis: Dict[str, any]):
        """分析結果のサマリーを出力"""
        print("\n" + "=" * 80)
        print("MEXC vs Bybit Symbol Differences Analysis Results")
        print("=" * 80)

        print(f"\n[STATISTICS]")
        print(f"  MEXC total symbols: {analysis['mexc_total']}")
        print(f"  Bybit total symbols: {analysis['bybit_total']}")
        print(
            f"  Basic conversion matches: {len(analysis['basic_conversion_matches'])}"
        )
        print(f"  MEXC only: {len(analysis['mexc_only'])}")
        print(f"  Bybit only: {len(analysis['bybit_only'])}")
        print(f"  Special cases: {len(analysis['potential_special_cases'])}")

        print(f"\n[BASIC CONVERSION EXAMPLES] (First 10):")
        for i, match in enumerate(analysis["basic_conversion_matches"][:10]):
            print(f"  {match['mexc']} -> {match['bybit']}")

        if analysis["potential_special_cases"]:
            print(f"\n[SPECIAL CASES]:")
            for case in analysis["potential_special_cases"][:10]:
                case_type = case.get("type", "unknown")
                print(f"  {case['mexc']} -> {case['bybit']} ({case_type})")

        print(f"\n[MEXC ONLY SYMBOLS] (First 20):")
        for symbol in analysis["mexc_only"][:20]:
            print(f"  {symbol}")

        print(f"\n[BYBIT ONLY SYMBOLS] (First 20):")
        for symbol in analysis["bybit_only"][:20]:
            print(f"  {symbol}")

        print(f"\n[GENERATED MAPPING TABLE] (Python Dict format):")
        print("MEXC_TO_BYBIT_MAPPING = {")
        mapping_items = list(analysis["suggested_mapping"].items())[:20]
        for mexc, bybit in mapping_items:
            print(f'    "{mexc}": "{bybit}",')
        if len(analysis["suggested_mapping"]) > 20:
            print(f"    # ... and {len(analysis['suggested_mapping']) - 20} more")
        print("}")


def main():
    """Main execution function"""
    print("Starting MEXC vs Bybit cryptocurrency symbol differences investigation...")

    investigator = SymbolInvestigator()

    # Fetch symbol lists
    print("\n1. Getting MEXC symbol list...")
    investigator.get_mexc_symbols()

    print("\n2. Getting Bybit symbol list...")
    investigator.get_bybit_symbols()

    # Perform analysis
    print("\n3. Analyzing symbol differences...")
    analysis = investigator.analyze_symbol_differences()

    # Print results
    investigator.print_summary(analysis)

    # Save to file
    investigator.save_analysis_to_file(analysis)

    print(f"\nInvestigation complete! Check symbol_analysis.json for detailed results.")


if __name__ == "__main__":
    main()
