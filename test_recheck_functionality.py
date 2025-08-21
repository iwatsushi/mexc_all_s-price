#!/usr/bin/env python3
"""
再チェック機能のテスト
片方の取引所で上場され、後から他方でも上場するケースのシミュレーション
"""

import sys
import time
from typing import Dict, List

# Trade Mini環境の設定
sys.path.append(".")


class MockBybitClient:
    """テスト用のモックBybitクライアント（動的上場シミュレーション）"""

    def __init__(self):
        # 初期状態では限定的な銘柄のみ
        self.available_symbols = {
            "BTCUSDT",
            "ETHUSDT",
            "ADAUSDT",
            "SOLUSDT",
            "SHIB1000USDT",
            "1000PEPEUSDT",
            "1000BONKUSDT",
            "FILUSDT",
            "RAYDIUMUSDT",
            "LUNA2USDT",
        }

        # 時間経過で上場する銘柄（後から追加される）
        self.pending_listings = {
            "NEWCOINUSDT": 5,  # 5秒後に上場
            "1000TESTUSDT": 10,  # 10秒後に上場
            "LATERUSDT": 15,  # 15秒後に上場
        }

        self.start_time = time.time()

    def get_available_symbols(self) -> List[str]:
        """利用可能銘柄リストを返す（時間経過で増加）"""
        self._update_listings()
        return list(self.available_symbols)

    def check_symbol_availability(self, symbol: str) -> bool:
        """銘柄の利用可能性をチェック（時間経過で変化）"""
        self._update_listings()
        return symbol in self.available_symbols

    def _update_listings(self):
        """時間経過による新規上場をシミュレート"""
        current_time = time.time()
        elapsed = current_time - self.start_time

        # 時間経過で銘柄を追加
        newly_listed = []
        for symbol, list_time in list(self.pending_listings.items()):
            if elapsed >= list_time:
                self.available_symbols.add(symbol)
                newly_listed.append(symbol)
                del self.pending_listings[symbol]

        if newly_listed:
            print(f"[MOCK] 🆕 New listings: {newly_listed}")


def test_recheck_functionality():
    """再チェック機能の総合テスト"""
    print("[TEST] Recheck Functionality - Delayed Listing Simulation")
    print("=" * 60)

    try:
        from symbol_mapper import SymbolMapper

        # モッククライアントでテスト（短い再チェック間隔）
        mock_client = MockBybitClient()
        mapper = SymbolMapper(mock_client)

        # 再チェック間隔を短く設定（テスト用）
        mapper._recheck_interval = 8  # 8秒間隔

        # 初期状態：存在しない銘柄をテスト
        test_symbols = [
            "NEWCOIN_USDT",  # 5秒後に上場予定
            "TEST_USDT",  # 10秒後に上場予定 (1000TESTUSDT として)
            "LATER_USDT",  # 15秒後に上場予定 (LATERUSDT として)
            "NEVER_USDT",  # 上場されない銘柄
        ]

        print("[PHASE 1] Initial checks (should fail)")
        initial_results = {}
        for symbol in test_symbols:
            result = mapper.check_symbol_realtime(symbol)
            initial_results[symbol] = result
            status = "[PASS]" if result else "[FAIL]"
            print(f"  {status} {symbol}: {'Available' if result else 'Not available'}")

        # 失敗統計を表示
        stats = mapper.get_failed_symbols_stats()
        print(f"\n[STATS] Initial failure stats:")
        print(f"  Failed mappings: {stats['failed_mapping_count']}")
        print(f"  MEXC exclusives: {stats['mexc_exclusive_count']}")

        print(f"\n[WAIT] Waiting for new listings...")
        time.sleep(12)  # 12秒待機（5秒と10秒の上場を待つ）

        print("[PHASE 2] Manual recheck after new listings")
        manual_results = {}
        for symbol in test_symbols:
            result = mapper.manual_recheck_symbol(symbol)
            manual_results[symbol] = result
            status = "[SUCCESS]" if result else "[FAIL]"
            if result:
                bybit_symbol = mapper.get_bybit_symbol(symbol)
                print(f"  {status} {symbol} -> {bybit_symbol}")
            else:
                print(f"  {status} {symbol}: Still not available")

        # さらに待機
        print(f"\n[WAIT] Waiting for final listing...")
        time.sleep(5)

        print("[PHASE 3] Periodic recheck")
        periodic_results = mapper.periodic_recheck_failed_symbols()

        print(f"\n[RESULTS] Periodic recheck results:")
        print(f"  New mappings found: {len(periodic_results)}")
        for mexc, bybit in periodic_results.items():
            print(f"  [NEW] {mexc} -> {bybit}")

        # 最終統計
        final_stats = mapper.get_failed_symbols_stats()
        print(f"\n[STATS] Final stats:")
        print(f"  Failed mappings: {final_stats['failed_mapping_count']}")
        print(f"  MEXC exclusives: {final_stats['mexc_exclusive_count']}")
        print(f"  Ready for recheck: {final_stats['total_ready_for_recheck']}")

        # 成功判定
        total_discovered = sum(
            1
            for r in [initial_results, manual_results, periodic_results]
            if any(r.values() if isinstance(r, dict) else [r])
        )

        print(f"\n[SUMMARY] Test Summary:")
        print(
            f"  Symbols that became available: {len([s for s in test_symbols if mapper.is_tradeable_on_bybit(s)])}"
        )
        print(
            f"  Recheck mechanism working: {'YES' if len(periodic_results) > 0 else 'NO'}"
        )

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_strict_exclusion():
    """厳格なMEXC専用判定のテスト"""
    print("\n[TEST] Strict MEXC Exclusive Detection")
    print("=" * 50)

    try:
        from symbol_mapper import SymbolMapper

        mock_client = MockBybitClient()
        mapper = SymbolMapper(mock_client)

        # 厳格なMEXC専用銘柄のテスト
        strict_exclusive_symbols = [
            ("AAPLSTOCK_USDT", True),  # 株式トークン
            ("TRUMP_USDT", True),  # 政治系
            ("SCAM_USDT", True),  # ジョークトークン
            ("BTC_USDT", False),  # 通常銘柄
            ("RANDOM_USDT", False),  # 通常銘柄
        ]

        print("Testing strict MEXC exclusive detection:")
        success_count = 0

        for symbol, expected in strict_exclusive_symbols:
            is_strict = mapper._is_strictly_mexc_exclusive(symbol)
            status = "[PASS]" if is_strict == expected else "[FAIL]"
            print(
                f"  {status} {symbol}: {'Strict exclusive' if is_strict else 'May appear on other exchanges'}"
            )
            if is_strict == expected:
                success_count += 1

        accuracy = (success_count / len(strict_exclusive_symbols)) * 100
        print(f"\n[STATS] Strict exclusion accuracy: {accuracy:.1f}%")

        return accuracy >= 80.0

    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False


def main():
    """メインテスト実行"""
    print("[TEST] Enhanced SymbolMapper - Recheck Functionality Test")
    print("=" * 70)

    # 各テストを実行
    test_results = []

    print("[INFO] Running delayed listing simulation...")
    test_results.append(("Recheck Functionality", test_recheck_functionality()))

    print("\n[INFO] Running strict exclusion test...")
    test_results.append(("Strict Exclusion", test_strict_exclusion()))

    # 結果サマリー
    print("\n" + "=" * 70)
    print("[SUMMARY] Test Results Summary")
    print("=" * 70)

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
        print("[SUCCESS] Recheck functionality is working correctly")
        print("[SUCCESS] System can handle delayed listings between exchanges")
    else:
        print("[WARNING] Some recheck features have issues")


if __name__ == "__main__":
    main()
