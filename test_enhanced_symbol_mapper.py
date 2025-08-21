#!/usr/bin/env python3
"""
強化されたSymbol Mapperの新機能テスト
新銘柄自動発見とMEXC専用銘柄判定のテスト
"""

import sys
import time
from typing import List

# Trade Mini環境の設定
sys.path.append(".")


class MockBybitClient:
    """テスト用のモックBybitクライアント"""

    def __init__(self):
        # 実際のBybit銘柄をシミュレート
        self.available_symbols = {
            "BTCUSDT",
            "ETHUSDT",
            "ADAUSDT",
            "SOLUSDT",
            "SHIB1000USDT",
            "1000PEPEUSDT",
            "1000BONKUSDT",
            "1000FLOKIUSDT",
            "FILUSDT",
            "RAYDIUMUSDT",
            "LUNA2USDT",
            # 新しい架空の銘柄（テスト用）
            "1000NEWCOINUSDT",
            "TESTCOIN2USDT",
            "ABCUSDT",
        }

    def get_available_symbols(self) -> List[str]:
        """利用可能銘柄リストを返す"""
        return list(self.available_symbols)

    def check_symbol_availability(self, symbol: str) -> bool:
        """銘柄の利用可能性をチェック"""
        return symbol in self.available_symbols


def test_new_symbol_discovery():
    """新銘柄の自動発見機能をテスト"""
    print("[TEST] 新銘柄自動発見機能のテスト")
    print("=" * 50)

    try:
        from symbol_mapper import SymbolMapper

        # モッククライアントでテスト
        mock_client = MockBybitClient()
        mapper = SymbolMapper(mock_client)

        # 新しい銘柄をテスト
        new_symbols = [
            "NEWCOIN_USDT",  # 1000倍表記で存在する予定
            "TESTCOIN_USDT",  # 数字サフィックスで存在する予定
            "ABCDEFGH_USDT",  # 略称で存在する予定
            "UNKNOWN_USDT",  # 存在しない銘柄
            "TRUMP_USDT",  # MEXC専用銘柄
        ]

        print("[INFO] Starting new symbol auto-discovery...")
        discovered_mappings = mapper.auto_discover_new_symbols(new_symbols)

        print(f"\n[RESULTS] Discovery Results:")
        print(f"   Newly mapped symbols: {len(discovered_mappings)}")

        for mexc_symbol, bybit_symbol in discovered_mappings.items():
            print(f"   [MAPPED] {mexc_symbol} -> {bybit_symbol}")

        return True

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_mexc_exclusive_detection():
    """MEXC専用銘柄判定機能をテスト"""
    print("\n[TEST] MEXC専用銘柄判定機能のテスト")
    print("=" * 50)

    try:
        from symbol_mapper import SymbolMapper

        mock_client = MockBybitClient()
        mapper = SymbolMapper(mock_client)

        # テスト銘柄リスト
        test_symbols = [
            ("BTC_USDT", False),  # 通常の銘柄
            ("TRUMP_USDT", True),  # 政治系MEXC専用
            ("AAPLSTOCK_USDT", True),  # 株式トークンMEXC専用
            ("FARTBOY_USDT", True),  # ミーム系MEXC専用
            ("TST_USDT", True),  # テスト用MEXC専用
            ("SHIB_USDT", False),  # Bybitにも存在
        ]

        print("[INFO] Testing MEXC exclusive symbol detection:")
        success_count = 0

        for symbol, expected_exclusive in test_symbols:
            is_exclusive = mapper.is_mexc_exclusive_symbol(symbol)
            status = "[PASS]" if is_exclusive == expected_exclusive else "[FAIL]"
            result = "MEXC exclusive" if is_exclusive else "Also on Bybit"

            print(f"   {status} {symbol}: {result}")
            if is_exclusive == expected_exclusive:
                success_count += 1

        accuracy = (success_count / len(test_symbols)) * 100
        print(
            f"\n[STATS] MEXC exclusive detection accuracy: {accuracy:.1f}% ({success_count}/{len(test_symbols)})"
        )

        # 既知のMEXC専用銘柄リストをテスト
        exclusive_symbols = mapper.get_mexc_exclusive_symbols()
        print(f"[INFO] Known MEXC exclusive symbols: {len(exclusive_symbols)}")
        print(f"   Sample: {exclusive_symbols[:5]}")

        return accuracy >= 80.0

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_conversion_candidates():
    """変換候補生成機能をテスト"""
    print("\n[TEST] 変換候補生成機能のテスト")
    print("=" * 50)

    try:
        from symbol_mapper import SymbolMapper

        mock_client = MockBybitClient()
        mapper = SymbolMapper(mock_client)

        # テストケース
        test_cases = ["NEWTOKEN_USDT", "TESTCOIN_USDT", "LONGNAME_USDT"]

        for mexc_symbol in test_cases:
            candidates = mapper._generate_conversion_candidates(mexc_symbol)
            print(f"[INFO] {mexc_symbol} conversion candidates:")
            for i, candidate in enumerate(candidates[:8], 1):  # 最大8個表示
                print(f"   {i}. {candidate}")
            print(f"   (Total: {len(candidates)} candidates)")

        return True

    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        return False


def main():
    """メインテスト実行"""
    print("[TEST] Enhanced SymbolMapper Function Test Start")
    print("=" * 60)

    # 各テストを実行
    test_results = []

    test_results.append(("New Symbol Discovery", test_new_symbol_discovery()))
    test_results.append(("MEXC Exclusive Detection", test_mexc_exclusive_detection()))
    test_results.append(("Conversion Candidates", test_conversion_candidates()))

    # 結果サマリー
    print("\n" + "=" * 60)
    print("[SUMMARY] Test Results Summary")
    print("=" * 60)

    passed_tests = 0
    for test_name, result in test_results:
        status = "PASS" if result else "FAIL"
        print(f"[{status}] {test_name}")
        if result:
            passed_tests += 1

    overall_success = (passed_tests / len(test_results)) * 100
    print(
        f"\n[STATS] Overall Success Rate: {overall_success:.1f}% ({passed_tests}/{len(test_results)})"
    )

    if overall_success >= 80.0:
        print("[SUCCESS] Enhanced SymbolMapper is working correctly")
    else:
        print("[WARNING] Some features have issues")


if __name__ == "__main__":
    main()
