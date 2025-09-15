"""
銘柄管理システム - MEXC & Bybit銘柄の取得・同期
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Set

import aiohttp

from config import Config

logger = logging.getLogger(__name__)


# 銘柄マッピング辞書：MEXCとBybitで名称が異なる銘柄の対応表
SYMBOL_MAPPING = {
    # MEXC → Bybit の変換
    # 1000倍表記の特殊ケース
    "SHIB_USDT": "1000SHIBUSDT",
    "PEPE_USDT": "1000PEPEUSDT", 
    "BONK_USDT": "1000BONKUSDT",
    "FLOKI_USDT": "1000FLOKIUSDT",
    "LUNC_USDT": "1000LUNCUSDT",
    "XEC_USDT": "1000XECUSDT",
    "TURBO_USDT": "1000TURBOUSDT",
    "RATS_USDT": "1000RATSUSDT",
    "SATS_USDT": "1000SATSUSDT",
    
    # 名称変更・リブランディング
    "FILECOIN_USDT": "FILUSDT",
    "LUNA_USDT": "LUNA2USDT", 
    "POLYGON_USDT": "POLUSDT",
    "MATIC_USDT": "POLUSDT",
    
    # 通貨単位の違い（USD → USDT変換）
    "BTC_USD": "BTCUSDT",
    "ETH_USD": "ETHUSDT", 
    "AVAX_USD": "AVAXUSDT",
    "LTC_USD": "LTCUSDT",
    
    # その他の特殊ケース
    "RAY_USDT": "RAYDIUMUSDT",
    "UNI_USDT": "UNIUSDT",
    "AAVE_USDT": "AAVEUSDT",
}

# Bybit → MEXC の逆マッピング（効率化のため）
REVERSE_SYMBOL_MAPPING = {v: k for k, v in SYMBOL_MAPPING.items()}

# 追加の逆マッピング（trade-miniから参考）
ADDITIONAL_REVERSE_MAPPING = {
    # Bybit専用の特殊形式
    "1000SHIBUSDT": "SHIB_USDT",
    "1000PEPEUSDT": "PEPE_USDT",
    "1000BONKUSDT": "BONK_USDT", 
    "1000FLOKIUSDT": "FLOKI_USDT",
    "1000LUNCUSDT": "LUNC_USDT",
    "1000XECUSDT": "XEC_USDT",
    "1000TURBOUSDT": "TURBO_USDT",
    "1000RATSUSDT": "RATS_USDT",
    "1000SATSUSDT": "SATS_USDT",
}

# 完全な逆マッピング辞書
COMPLETE_REVERSE_MAPPING = {**REVERSE_SYMBOL_MAPPING, **ADDITIONAL_REVERSE_MAPPING}


@dataclass
class SymbolInfo:
    """銘柄情報"""

    symbol: str
    mexc_available: bool = False
    bybit_available: bool = False
    updated_at: datetime = None


class SymbolManager:
    """銘柄管理システム"""

    def __init__(self, config: Config):
        self.config = config
        self.session = None

        # API URLs (先物取引)
        self.mexc_symbols_url = "https://contract.mexc.com/api/v1/contract/detail"
        self.bybit_symbols_url = "https://api.bybit.com/v5/market/instruments-info"

        # キャッシュ
        self.current_symbols: Dict[str, SymbolInfo] = {}
        self.last_sync_time = None

        # 同期間隔（秒）
        self.sync_interval = config.get(
            "symbols.sync_interval", 3600
        )  # デフォルト1時間

    def map_mexc_to_bybit(self, mexc_symbol: str) -> str:
        """MEXC銘柄名をBybit銘柄名に変換"""
        # 特殊ケースのマッピングをチェック
        if mexc_symbol in SYMBOL_MAPPING:
            return SYMBOL_MAPPING[mexc_symbol]
        
        # 基本変換：アンダースコアを削除 (BTC_USDT → BTCUSDT)
        return mexc_symbol.replace("_", "")
    
    def map_bybit_to_mexc(self, bybit_symbol: str) -> str:
        """Bybit銘柄名をMEXC銘柄名に変換"""
        # 完全な逆マッピングをチェック（特殊ケースを含む）
        if bybit_symbol in COMPLETE_REVERSE_MAPPING:
            return COMPLETE_REVERSE_MAPPING[bybit_symbol]
            
        # 基本逆変換：USDTの前にアンダースコア追加 (BTCUSDT → BTC_USDT)
        if bybit_symbol.endswith("USDT"):
            base = bybit_symbol[:-4]  # USDTを除去
            return f"{base}_USDT"
        elif bybit_symbol.endswith("USD"):
            base = bybit_symbol[:-3]  # USDを除去
            return f"{base}_USD"
        else:
            # その他の場合はそのまま返す
            return bybit_symbol
    
    def normalize_symbols(self, mexc_symbols: Set[str], bybit_symbols: Set[str]) -> Dict[str, SymbolInfo]:
        """
        両取引所の銘柄を正規化してマッピング統合
        
        Args:
            mexc_symbols: MEXC形式の銘柄セット (例: BTC_USDT)
            bybit_symbols: 既にMEXC形式に変換済みのBybit銘柄セット (例: BTC_USDT)
            
        Returns:
            正規化された銘柄情報の辞書（MEXC形式の銘柄名をキーとする）
        """
        normalized_symbols = {}
        current_time = datetime.now()
        
        # 全銘柄リストを作成（MEXC形式で統一済み）
        all_symbols = mexc_symbols | bybit_symbols
        
        # 各銘柄について両取引所での取引可否をチェック
        for symbol in all_symbols:
            symbol_info = SymbolInfo(
                symbol=symbol,
                mexc_available=(symbol in mexc_symbols),
                bybit_available=(symbol in bybit_symbols),
                updated_at=current_time
            )
            normalized_symbols[symbol] = symbol_info
        
        logger.info(f"🔗 銘柄マッピング完了: {len(normalized_symbols)}銘柄を正規化")
        return normalized_symbols

    async def initialize(self):
        """初期化"""
        try:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=10),
            )
            logger.info("🏷️ 銘柄管理システム初期化完了")

        except Exception as e:
            logger.error(f"銘柄管理システム初期化失敗: {e}")
            raise

    async def shutdown(self):
        """シャットダウン"""
        if self.session:
            await self.session.close()
        logger.info("銘柄管理システムシャットダウン完了")

    async def get_mexc_symbols(self) -> Set[str]:
        """MEXCの先物取引可能銘柄を取得"""
        try:
            logger.info("📡 MEXC先物銘柄一覧取得中...")

            async with self.session.get(self.mexc_symbols_url) as response:
                if response.status != 200:
                    logger.warning(f"MEXC Contract API error: {response.status}")
                    return set()

                data = await response.json()
                symbols = set()

                logger.debug(f"MEXC Contract API response keys: {list(data.keys())}")

                # MEXC先物APIは{success, code, data}構造でdata配列内に契約情報を返す
                if isinstance(data, dict) and "data" in data:
                    contracts = data["data"]
                    logger.debug(f"MEXC contract count in response: {len(contracts)}")
                    if len(contracts) > 0:
                        sample = contracts[0]
                        logger.debug(
                            f"MEXC sample contract keys: {list(sample.keys())}"
                        )

                    # USDT建て先物のみを対象
                    for contract_info in contracts:
                        symbol = contract_info.get("symbol", "")
                        state = contract_info.get("state", -1)  # 0=normal, 1=suspend

                        # USDT建て先物で正常状態の銘柄のみ（apiAllowedは常にfalseなので条件から除外）
                        if symbol.endswith("_USDT") and state == 0:
                            # MEXCの形式（アンダースコア付き）をそのまま使用
                            symbols.add(symbol)

                else:
                    logger.warning(f"Unexpected MEXC API response format: {type(data)}")
                    logger.debug(
                        f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not dict'}"
                    )

                logger.info(f"✅ MEXC: {len(symbols)}銘柄を取得 (USDT先物)")
                return symbols

        except Exception as e:
            logger.error(f"MEXC先物銘柄取得エラー: {e}")
            logger.debug(
                f"MEXC API response sample: {str(data)[:200] if 'data' in locals() else 'No data'}"
            )
            return set()

    async def get_bybit_symbols(self) -> Set[str]:
        """Bybitの取引可能銘柄を取得"""
        try:
            logger.info("📡 Bybit銘柄一覧取得中...")

            # USDT建て先物を取得
            params = {"category": "linear", "limit": 1000}

            async with self.session.get(
                self.bybit_symbols_url, params=params
            ) as response:
                if response.status != 200:
                    logger.warning(f"Bybit API error: {response.status}")
                    return set()

                data = await response.json()
                symbols = set()

                for symbol_info in data.get("result", {}).get("list", []):
                    symbol = symbol_info.get("symbol", "")
                    status = symbol_info.get("status", "")

                    if symbol.endswith("USDT") and status == "Trading":
                        # Bybit銘柄をMEXC形式に変換（特殊マッピング適用）
                        mexc_format_symbol = self.map_bybit_to_mexc(symbol)
                        symbols.add(mexc_format_symbol)

                logger.info(f"✅ Bybit: {len(symbols)}銘柄を取得")
                return symbols

        except Exception as e:
            logger.error(f"Bybit銘柄取得エラー: {e}")
            return set()

    async def sync_symbols(self) -> Dict[str, SymbolInfo]:
        """銘柄同期実行（マッピング機能付き）"""
        try:
            logger.info("🔄 銘柄同期開始...")
            start_time = time.time()

            # 各取引所の銘柄を並列取得
            mexc_task = asyncio.create_task(self.get_mexc_symbols())
            bybit_task = asyncio.create_task(self.get_bybit_symbols())

            mexc_symbols, bybit_symbols = await asyncio.gather(mexc_task, bybit_task)

            # 銘柄マッピングと正規化を実行
            updated_symbols = self.normalize_symbols(mexc_symbols, bybit_symbols)

            # 変更検出
            changes = self._detect_changes(updated_symbols)

            # キャッシュ更新
            self.current_symbols = updated_symbols
            self.last_sync_time = datetime.now()

            duration = time.time() - start_time
            
            # 統計情報
            mexc_count = sum(1 for info in updated_symbols.values() if info.mexc_available)
            bybit_count = sum(1 for info in updated_symbols.values() if info.bybit_available)
            both_count = sum(1 for info in updated_symbols.values() if info.mexc_available and info.bybit_available)
            
            # マッピング成功例をデバッグ表示
            successful_mappings = []
            for symbol, info in updated_symbols.items():
                if info.mexc_available and info.bybit_available:
                    successful_mappings.append(symbol)
            
            if successful_mappings:
                logger.info(f"🔗 マッピング成功例: {successful_mappings[:10]}")  # 最初の10件を表示
            
            logger.info(
                f"✅ 銘柄同期完了: 総計{len(updated_symbols)}銘柄 "
                f"(MEXC: {mexc_count}, Bybit: {bybit_count}, 両方: {both_count}), "
                f"変更: {len(changes)}, 所要時間: {duration:.2f}秒"
            )

            return updated_symbols

        except Exception as e:
            logger.error(f"銘柄同期エラー: {e}")
            return {}

    def _detect_changes(self, new_symbols: Dict[str, SymbolInfo]) -> List[Dict]:
        """銘柄変更を検出"""
        changes = []

        # 新規追加された銘柄
        for symbol, info in new_symbols.items():
            if symbol not in self.current_symbols:
                changes.append(
                    {
                        "type": "added",
                        "symbol": symbol,
                        "mexc_available": info.mexc_available,
                        "bybit_available": info.bybit_available,
                    }
                )

        # 削除された銘柄
        for symbol in self.current_symbols:
            if symbol not in new_symbols:
                changes.append(
                    {
                        "type": "removed",
                        "symbol": symbol,
                        "mexc_available": False,
                        "bybit_available": False,
                    }
                )

        # 取引可否が変更された銘柄
        for symbol, new_info in new_symbols.items():
            if symbol in self.current_symbols:
                old_info = self.current_symbols[symbol]
                if (
                    old_info.mexc_available != new_info.mexc_available
                    or old_info.bybit_available != new_info.bybit_available
                ):
                    changes.append(
                        {
                            "type": "updated",
                            "symbol": symbol,
                            "mexc_available": new_info.mexc_available,
                            "bybit_available": new_info.bybit_available,
                            "old_mexc": old_info.mexc_available,
                            "old_bybit": old_info.bybit_available,
                        }
                    )

        return changes

    def should_sync(self) -> bool:
        """同期が必要かチェック"""
        if self.last_sync_time is None:
            return True

        elapsed = (datetime.now() - self.last_sync_time).total_seconds()
        return elapsed >= self.sync_interval

    def get_symbols(self) -> Dict[str, SymbolInfo]:
        """現在の銘柄情報を取得"""
        return self.current_symbols.copy()

    def get_stats(self) -> Dict:
        """統計情報を取得"""
        if not self.current_symbols:
            return {}

        mexc_count = sum(
            1 for info in self.current_symbols.values() if info.mexc_available
        )
        bybit_count = sum(
            1 for info in self.current_symbols.values() if info.bybit_available
        )
        both_count = sum(
            1
            for info in self.current_symbols.values()
            if info.mexc_available and info.bybit_available
        )

        return {
            "total_symbols": len(self.current_symbols),
            "mexc_symbols": mexc_count,
            "bybit_symbols": bybit_count,
            "both_exchanges": both_count,
            "last_sync": (
                self.last_sync_time.isoformat() if self.last_sync_time else None
            ),
        }
