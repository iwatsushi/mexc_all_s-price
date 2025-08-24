"""
Bybit取引所クライアント（注文・決済専用）
"""

import hashlib
import hmac
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class BybitPosition:
    """Bybitポジションデータ"""

    symbol: str
    side: str  # "Buy" or "Sell"
    size: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    leverage: float


@dataclass
class BybitOrderResult:
    """Bybit注文結果"""

    success: bool
    order_id: str = ""
    message: str = ""
    avg_price: float = 0.0


class BybitClient:
    """Bybit REST APIクライアント（先物取引）"""

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment: str = "live",
        base_url: str = None,
    ):
        """
        初期化

        Args:
            api_key: Bybit API Key
            api_secret: Bybit API Secret
            environment: 環境選択 ("live", "demo", "testnet")
            base_url: カスタムAPIベースURL（指定時は優先使用）
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.environment = environment

        # API URL設定
        if base_url:
            self.base_url = base_url
        elif environment == "testnet":
            self.base_url = "https://api-testnet.bybit.com"
        elif environment == "demo":
            self.base_url = "https://api-demo.bybit.com"  # デモトレード用URL
        else:
            self.base_url = "https://api.bybit.com"  # 本番用URL

        logger.info(f"🔗 Bybit API URL: {self.base_url} (環境: {environment})")

        self.session = requests.Session()

        # 接続テスト
        self._test_connection()

    def _test_connection(self):
        """接続テスト"""
        try:
            response = self._send_request("GET", "/v5/market/time")
            if response.get("retCode") == 0:
                logger.info("✅ Bybit接続テスト成功")
            else:
                logger.warning(f"⚠️ Bybit接続テスト失敗: {response}")
        except Exception as e:
            logger.error(f"❌ Bybit接続テストエラー: {e}")

    def _generate_signature(self, timestamp: str, params: str) -> str:
        """署名を生成"""
        recv_window = "20000"  # 20秒に増加（タイムスタンプ同期問題対応）
        param_str = timestamp + self.api_key + recv_window + params
        return hmac.new(
            self.api_secret.encode("utf-8"), param_str.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def _get_server_time(self) -> int:
        """Bybitサーバー時刻を取得（時刻同期用）"""
        try:
            response = self.session.get(f"{self.base_url}/v5/market/time")
            if response.status_code == 200:
                result = response.json()
                if result.get("retCode") == 0:
                    return int(result.get("result", {}).get("timeSecond", 0)) * 1000
        except Exception:
            pass
        # フォールバック：ローカル時刻を使用
        return int(time.time() * 1000)

    def _send_request(
        self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """署名付きリクエストを送信"""
        # サーバー時刻同期でタイムスタンプエラーを回避
        timestamp = str(self._get_server_time())

        # パラメータを文字列に変換
        if params:
            param_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
        else:
            param_str = ""

        # 署名生成
        signature = self._generate_signature(timestamp, param_str)

        # ヘッダー設定
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "20000",  # 20秒に増加（タイムスタンプ同期問題対応）
            "Content-Type": "application/json",
        }

        url = self.base_url + endpoint

        try:
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=headers, json=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"{url}へのリクエストエラー: {e}")
            if hasattr(e, "response") and e.response:
                logger.error(f"レスポンス内容: {e.response.text}")
            return {"retCode": -1, "retMsg": str(e)}

    def get_wallet_balance(self) -> Dict[str, Any]:
        """USDT残高を取得"""
        params = {"accountType": "UNIFIED", "coin": "USDT"}
        return self._send_request("GET", "/v5/account/wallet-balance", params)

    def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """ポジション情報を取得"""
        params = {"category": "linear", "settleCoin": "USDT"}
        if symbol:
            # MEXCからBybitのシンボル形式に変換
            params["symbol"] = self._convert_mexc_to_bybit_symbol(symbol)

        return self._send_request("GET", "/v5/position/list", params)

    def _convert_mexc_to_bybit_symbol(self, mexc_symbol: str) -> str:
        """MEXCシンボルをBybitシンボルに変換"""
        # MEXC: BTC_USDT -> Bybit: BTCUSDT
        if mexc_symbol.endswith("_USDT"):
            return mexc_symbol.replace("_USDT", "USDT")
        return mexc_symbol

    def _convert_bybit_to_mexc_symbol(self, bybit_symbol: str) -> str:
        """BybitシンボルをMEXCシンボルに変換"""
        # Bybit: BTCUSDT -> MEXC: BTC_USDT
        if bybit_symbol.endswith("USDT"):
            base = bybit_symbol[:-4]
            return f"{base}_USDT"
        return bybit_symbol

    def check_symbol_availability(self, mexc_symbol: str) -> bool:
        """シンボルがBybitで取引可能かチェック"""
        bybit_symbol = self._convert_mexc_to_bybit_symbol(mexc_symbol)

        try:
            response = self._send_request(
                "GET",
                "/v5/market/instruments-info",
                {"category": "linear", "symbol": bybit_symbol},
            )

            if response.get("retCode") == 0:
                instruments = response.get("result", {}).get("list", [])
                if instruments:
                    status = instruments[0].get("status")
                    return status == "Trading"

            return False
        except Exception as e:
            logger.error(f"{mexc_symbol}の銘柄利用可能性チェックエラー: {e}")
            return False

    def place_market_order(
        self, mexc_symbol: str, side: str, qty: float
    ) -> BybitOrderResult:
        """
        成行注文を発注

        Args:
            mexc_symbol: MEXCシンボル（例：BTC_USDT）
            side: "LONG" or "SHORT"
            qty: 注文量（USDT）
        """
        bybit_symbol = self._convert_mexc_to_bybit_symbol(mexc_symbol)

        # サイド変換
        bybit_side = "Buy" if side == "LONG" else "Sell"

        # 注文IDを生成
        order_link_id = f"order_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": bybit_side,
            "orderType": "Market",
            "qty": str(qty),
            "orderLinkId": order_link_id,
            "timeInForce": "IOC",  # Immediate or Cancel
        }

        response = self._send_request("POST", "/v5/order/create", params)

        if response.get("retCode") == 0:
            result = response.get("result", {})
            return BybitOrderResult(
                success=True,
                order_id=result.get("orderId", ""),
                message="Order placed successfully",
            )
        else:
            return BybitOrderResult(
                success=False, message=response.get("retMsg", "Unknown error")
            )

    def close_position(self, mexc_symbol: str, side: str) -> BybitOrderResult:
        """
        ポジションを決済

        Args:
            mexc_symbol: MEXCシンボル
            side: 現在のポジション方向 "LONG" or "SHORT"
        """
        bybit_symbol = self._convert_mexc_to_bybit_symbol(mexc_symbol)

        # 現在のポジション情報を取得
        position_response = self.get_positions(mexc_symbol)
        if position_response.get("retCode") != 0:
            return BybitOrderResult(
                success=False, message="Failed to get position info"
            )

        positions = position_response.get("result", {}).get("list", [])
        target_position = None

        for pos in positions:
            if pos.get("symbol") == bybit_symbol:
                pos_side = pos.get("side")
                if (side == "LONG" and pos_side == "Buy") or (
                    side == "SHORT" and pos_side == "Sell"
                ):
                    if float(pos.get("size", 0)) > 0:  # ポジションサイズが0より大きい
                        target_position = pos
                        break

        if not target_position:
            return BybitOrderResult(
                success=False, message=f"No {side} position found for {mexc_symbol}"
            )

        position_size = target_position.get("size")

        # 決済サイド（現在のポジションと逆方向）
        close_side = "Sell" if side == "LONG" else "Buy"

        # 決済注文ID
        order_link_id = f"close_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": position_size,
            "orderLinkId": order_link_id,
            "timeInForce": "IOC",
            "reduceOnly": True,  # 決済注文フラグ
        }

        response = self._send_request("POST", "/v5/order/create", params)

        if response.get("retCode") == 0:
            result = response.get("result", {})
            return BybitOrderResult(
                success=True,
                order_id=result.get("orderId", ""),
                message="Position closed successfully",
            )
        else:
            return BybitOrderResult(
                success=False, message=response.get("retMsg", "Unknown error")
            )

    def set_leverage(self, mexc_symbol: str, leverage: float) -> bool:
        """
        レバレッジを設定

        Args:
            mexc_symbol: MEXCシンボル
            leverage: レバレッジ倍率
        """
        bybit_symbol = self._convert_mexc_to_bybit_symbol(mexc_symbol)

        params = {
            "category": "linear",
            "symbol": bybit_symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        }

        response = self._send_request("POST", "/v5/position/set-leverage", params)

        if response.get("retCode") == 0:
            logger.info(f"{mexc_symbol}のレバレッジを{leverage}倍に設定")
            return True
        else:
            logger.error(
                f"{mexc_symbol}のレバレッジ設定失敗: {response.get('retMsg')}"
            )
            return False

    def get_symbol_info(self, mexc_symbol: str) -> Dict[str, Any]:
        """銘柄情報を取得"""
        bybit_symbol = self._convert_mexc_to_bybit_symbol(mexc_symbol)

        params = {"category": "linear", "symbol": bybit_symbol}

        return self._send_request("GET", "/v5/market/instruments-info", params)

    def get_account_info(self) -> Dict[str, Any]:
        """アカウント情報を取得"""
        return self._send_request("GET", "/v5/account/info")

    def get_available_symbols(self) -> List[str]:
        """Bybitで取引可能なUSDTペアシンボル一覧を取得（MEXC形式で返す）"""
        try:
            response = self._send_request(
                "GET", "/v5/market/instruments-info", {"category": "linear"}
            )

            if response.get("retCode") != 0:
                return []

            instruments = response.get("result", {}).get("list", [])
            mexc_symbols = []

            for instrument in instruments:
                symbol = instrument.get("symbol", "")
                status = instrument.get("status", "")

                # USDTペアで取引中のもののみ
                if symbol.endswith("USDT") and status == "Trading":
                    mexc_symbol = self._convert_bybit_to_mexc_symbol(symbol)
                    mexc_symbols.append(mexc_symbol)

            logger.info(f"Bybitで{len(mexc_symbols)}個の取引可能USDTペアを発見")
            return sorted(mexc_symbols)

        except Exception as e:
            logger.error(f"利用可能銘柄取得エラー: {e}")
            return []
