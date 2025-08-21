#!/usr/bin/env python3
"""
特殊マッピング銘柄の価格比較テスト
MEXCとBybitの価格差を検証（1000倍表記考慮）
"""

import json
import time
from typing import Dict, List, Optional, Tuple

import requests


class PriceComparisonTester:
    """価格比較テスター"""

    def __init__(self):
        # 特殊マッピングケース（実際のBybit銘柄名で修正）
        self.special_mappings = {
            # 1000倍表記の特殊ケース（実際のBybit銘柄名で修正）
            "SHIB_USDT": ("SHIB1000USDT", 1000),  # 実際のBybitはSHIB1000USDT
            "PEPE_USDT": ("1000PEPEUSDT", 1000),  # Bybitは1000倍
            "BONK_USDT": ("1000BONKUSDT", 1000),  # Bybitは1000倍
            "FLOKI_USDT": ("1000FLOKIUSDT", 1000),  # Bybitは1000倍
            "LUNC_USDT": ("1000LUNCUSDT", 1000),  # Bybitは1000倍
            "XEC_USDT": ("1000XECUSDT", 1000),  # Bybitは1000倍
            "TURBO_USDT": ("1000TURBOUSDT", 1000),  # Bybitは1000倍
            # 通常のマッピング（倍率1）
            "FILECOIN_USDT": ("FILUSDT", 1),  # 名称変更
            "RAY_USDT": ("RAYDIUMUSDT", 1),  # 名称変更
            "LUNA_USDT": ("LUNA2USDT", 1),  # リブランド（実際のBybitはLUNA2USDT）
            "BTC_USDT": ("BTCUSDT", 1),  # 基本変換
            "ETH_USDT": ("ETHUSDT", 1),  # 基本変換
        }

        # APIエンドポイント
        self.mexc_futures_url = "https://contract.mexc.com/api/v1/contract/ticker"
        self.bybit_futures_url = "https://api.bybit.com/v5/market/tickers"

        # セッション設定
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "TradeBot/1.0"})
        self.session.timeout = 10

    def fetch_mexc_price(self, symbol: str) -> Optional[float]:
        """MEXCから価格を取得"""
        try:
            params = {"symbol": symbol}
            response = self.session.get(self.mexc_futures_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    return float(data["data"]["lastPrice"])
        except Exception as e:
            print(f"MEXC価格取得エラー {symbol}: {e}")
        return None

    def fetch_bybit_price(self, symbol: str) -> Optional[float]:
        """Bybitから価格を取得"""
        try:
            params = {"category": "linear", "symbol": symbol}
            response = self.session.get(self.bybit_futures_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                    tickers = data["result"]["list"]
                    for ticker in tickers:
                        if ticker["symbol"] == symbol:
                            return float(ticker["lastPrice"])
        except Exception as e:
            print(f"Bybit価格取得エラー {symbol}: {e}")
        return None

    def compare_prices(self) -> List[Dict]:
        """価格比較を実行"""
        results = []

        print("[PRICE COMPARISON] 特殊マッピング銘柄の価格比較開始")
        print("=" * 70)

        for mexc_symbol, (bybit_symbol, multiplier) in self.special_mappings.items():
            print(f"\n検証中 {mexc_symbol} vs {bybit_symbol} (倍率: {multiplier})")

            # 価格取得
            mexc_price = self.fetch_mexc_price(mexc_symbol)
            bybit_price = self.fetch_bybit_price(bybit_symbol)

            if mexc_price is None or bybit_price is None:
                result = {
                    "mexc_symbol": mexc_symbol,
                    "bybit_symbol": bybit_symbol,
                    "multiplier": multiplier,
                    "mexc_price": mexc_price,
                    "bybit_price": bybit_price,
                    "status": "ERROR",
                    "message": "価格取得失敗",
                }
                print(
                    f"   [ERROR] 価格取得失敗 - MEXC: {mexc_price}, Bybit: {bybit_price}"
                )
            else:
                # 1000倍表記の場合はBybit価格を1000で割って比較
                adjusted_bybit_price = (
                    bybit_price / multiplier if multiplier > 1 else bybit_price
                )

                # 価格差を計算
                price_diff = abs(mexc_price - adjusted_bybit_price)
                price_diff_percent = (
                    (price_diff / mexc_price) * 100 if mexc_price > 0 else float("inf")
                )

                # 許容範囲判定（2%以内を許容）
                is_acceptable = price_diff_percent <= 2.0

                result = {
                    "mexc_symbol": mexc_symbol,
                    "bybit_symbol": bybit_symbol,
                    "multiplier": multiplier,
                    "mexc_price": mexc_price,
                    "bybit_price": bybit_price,
                    "adjusted_bybit_price": adjusted_bybit_price,
                    "price_diff": price_diff,
                    "price_diff_percent": price_diff_percent,
                    "status": "OK" if is_acceptable else "WARNING",
                    "is_acceptable": is_acceptable,
                }

                status_icon = "[PASS]" if is_acceptable else "[WARN]"
                print(f"   {status_icon} MEXC: ${mexc_price}")
                print(
                    f"     Bybit: ${bybit_price} (調整後: ${adjusted_bybit_price:.8f})"
                )
                print(f"     価格差: ${price_diff:.8f} ({price_diff_percent:.3f}%)")

            results.append(result)

            # API制限を避けるため小休止
            time.sleep(0.5)

        return results

    def print_summary(self, results: List[Dict]):
        """結果のサマリーを表示"""
        print("\n" + "=" * 70)
        print("[SUMMARY] 価格比較結果サマリー")
        print("=" * 70)

        total_pairs = len(results)
        successful_comparisons = sum(
            1 for r in results if r["status"] in ["OK", "WARNING"]
        )
        acceptable_pairs = sum(1 for r in results if r.get("is_acceptable", False))
        error_pairs = sum(1 for r in results if r["status"] == "ERROR")
        warning_pairs = sum(1 for r in results if r["status"] == "WARNING")

        print(f"[STATS] 総ペア数: {total_pairs}")
        print(f"[STATS] 成功比較: {successful_comparisons}")
        print(f"[STATS] 許容範囲内: {acceptable_pairs}")
        print(f"[STATS] 注意必要: {warning_pairs}")
        print(f"[STATS] エラー: {error_pairs}")

        if warning_pairs > 0:
            print(f"\n[WARNING] 価格差が大きい銘柄:")
            for result in results:
                if result["status"] == "WARNING":
                    print(
                        f"   - {result['mexc_symbol']}: {result['price_diff_percent']:.3f}%差"
                    )

        success_rate = (
            (acceptable_pairs / successful_comparisons * 100)
            if successful_comparisons > 0
            else 0
        )
        print(f"\n[ACCURACY] マッピング精度: {success_rate:.1f}%")


def main():
    """メイン実行関数"""
    tester = PriceComparisonTester()
    results = tester.compare_prices()
    tester.print_summary(results)

    # 結果をJSONファイルに保存
    with open("price_comparison_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n[SAVED] 詳細結果を price_comparison_results.json に保存しました")


if __name__ == "__main__":
    main()
