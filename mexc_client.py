"""
MEXC取引所クライアント（WebSocket + REST API統合）
"""

import asyncio
import hashlib
import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import requests
import websockets

from config import Config

logger = logging.getLogger(__name__)


@dataclass
class TickData:
    """ティックデータ"""

    symbol: str
    price: float
    timestamp: datetime
    volume: float = 0.0


@dataclass
class PositionData:
    """ポジションデータ"""

    symbol: str
    side: str  # "LONG" or "SHORT"
    size: float
    entry_price: float
    mark_price: float
    pnl: float
    margin_type: str  # "isolated" or "cross"


@dataclass
class OrderResult:
    """注文結果"""

    success: bool
    order_id: str = ""
    message: str = ""


class MEXCWebSocketClient:
    """MEXC Futures WebSocket全銘柄価格購読クライアント"""

    def __init__(self, config: Config):
        self.config = config
        # MEXC Futures WebSocket URL
        self.ws_url = "wss://contract.mexc.com/edge"
        self.running = False
        self.shutdown_event = threading.Event()

        # データコールバック
        self.tick_callback: Optional[Callable[[TickData], None]] = None

        # WebSocket関連
        self._ws_task = None
        self._websocket = None
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = 5

    async def connect(self) -> bool:
        """WebSocket接続開始"""
        try:
            logger.info(f"Starting MEXC WebSocket connection: {self.ws_url}")

            self.running = True
            # WebSocketタスクを開始
            self._ws_task = asyncio.create_task(self._websocket_loop())
            logger.info("WebSocket task started")
            return True

        except Exception as e:
            logger.error(f"Failed to start MEXC WebSocket: {e}")
            return False

    async def disconnect(self):
        """WebSocket接続停止"""
        logger.info("Stopping MEXC WebSocket...")

        self.running = False
        self.shutdown_event.set()

        if self._websocket:
            await self._websocket.close()

        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass

        logger.info("MEXC WebSocket stopped")

    async def subscribe_all_tickers(self) -> bool:
        """全銘柄ティッカー購読開始"""
        if self.running:
            logger.info("MEXC WebSocket already running and subscribed")
            return True
        else:
            logger.warning("MEXC WebSocket not started")
            return False

    def set_tick_callback(self, callback: Callable[[TickData], None]):
        """価格データコールバックを設定"""
        self.tick_callback = callback

    async def _websocket_loop(self):
        """WebSocketメインループ（再接続対応）"""
        logger.info("🔄 MEXC WebSocket loop started")

        while self.running and not self.shutdown_event.is_set():
            try:
                await self._websocket_connection()
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")

                if (
                    self.running
                    and self._reconnect_attempts < self._max_reconnect_attempts
                ):
                    self._reconnect_attempts += 1
                    wait_time = min(
                        2**self._reconnect_attempts, 30
                    )  # 指数バックオフ、最大30秒
                    logger.info(
                        f"Reconnecting in {wait_time} seconds (attempt {self._reconnect_attempts})"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    logger.error("Max reconnection attempts reached")
                    break

        logger.info("MEXC WebSocket loop ended")

    async def _websocket_connection(self):
        """WebSocket接続処理"""

        async with websockets.connect(self.ws_url) as websocket:
            self._websocket = websocket
            self._reconnect_attempts = 0  # 成功したらリセット

            logger.info("WebSocket connected, subscribing to tickers...")

            # sub.tickers チャネルを購読
            subscribe_msg = {"method": "sub.tickers", "param": {}}
            await websocket.send(json.dumps(subscribe_msg))
            logger.info("Subscribed to sub.tickers channel")

            # メッセージ受信ループ
            async for message in websocket:
                try:
                    data = json.loads(message)

                    # 購読確認メッセージ
                    if data.get("channel") == "rs.sub.tickers":
                        logger.info(f"Subscription confirmed: {data.get('data')}")
                        continue

                    # ティッカーデータ処理
                    if data.get("channel") == "push.tickers" and "data" in data:
                        tickers = data["data"]
                        if isinstance(tickers, list):
                            logger.info(
                                f"📊 MEXC WebSocket received {len(tickers)} tickers"
                            )

                            # 各ティッカーを処理
                            if self.tick_callback:
                                logger.info(
                                    f"🔧 Processing {len(tickers)} tickers with callback"
                                )
                                processed_count = 0
                                for ticker in tickers:
                                    if isinstance(ticker, dict):
                                        symbol = ticker.get("symbol", "")
                                        price = float(ticker.get("lastPrice", 0))
                                        volume = float(ticker.get("volume24", 0))

                                        if symbol and price > 0:
                                            # 銘柄名はそのまま使用（MEXCとBybit形式が一致）
                                            # 例: "BTC_USDT" → "BTC_USDT"（変更なし）

                                            tick = TickData(
                                                symbol=symbol,
                                                price=price,
                                                timestamp=datetime.now(),
                                                volume=volume,
                                            )

                                            try:
                                                self.tick_callback(tick)
                                                processed_count += 1
                                            except Exception as e:
                                                logger.error(
                                                    f"Error in tick callback for {symbol}: {e}"
                                                )

                                if processed_count % 500 == 0 and processed_count > 0:
                                    logger.info(
                                        f"✅ Processed {processed_count} ticks via callback"
                                    )
                            else:
                                logger.warning("No tick callback set!")

                except json.JSONDecodeError:
                    logger.warning(f"Non-JSON message received: {message[:100]}...")
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}")

                if self.shutdown_event.is_set():
                    break


class MEXCRESTClient:
    """MEXC REST APIクライアント"""

    def __init__(self, config: Config):
        self.config = config
        self.api_key = config.mexc_api_key
        self.api_secret = config.mexc_api_secret
        self.base_url = config.mexc_api_url

        if not self.api_key or not self.api_secret:
            raise ValueError("MEXC API credentials not provided")

        self.session = requests.Session()

    def _sign_request(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """リクエストに署名するためのヘッダーを生成"""
        request_time = str(int(time.time() * 1000))
        to_sign = request_time + self.api_key
        if params:
            to_sign += json.dumps(params)

        signature = hmac.new(
            self.api_secret.encode("utf-8"), to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        headers = {
            "ApiKey": self.api_key,
            "Request-Time": request_time,
            "Signature": signature,
            "Content-Type": "application/json",
        }
        return headers

    def _send_request(
        self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """署名付きリクエストをMEXC APIに送信"""
        url = self.base_url + endpoint
        headers = self._sign_request(params)

        try:
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = self.session.post(url, headers=headers, json=params)
            elif method.upper() == "DELETE":
                response = self.session.delete(url, headers=headers, json=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending request to {url}: {e}")
            if hasattr(e, "response") and e.response:
                logger.error(f"Response content: {e.response.text}")
            return {"success": False, "message": str(e)}

    def get_balance(self) -> Dict[str, Any]:
        """アカウント残高を取得"""
        return self._send_request("GET", "/api/v1/private/account/assets")

    def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """ポジション情報を取得"""
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._send_request(
            "GET", "/api/v1/private/position/list/open_positions", params
        )

    def place_market_order(self, symbol: str, side: str, size: float) -> OrderResult:
        """成行注文を発注

        Args:
            symbol: 銘柄シンボル
            side: "LONG" or "SHORT"
            size: 注文サイズ
        """
        # サイド変換：LONG->1(開きロング), SHORT->2(開きショート)
        mexc_side = 1 if side == "LONG" else 2

        params = {
            "symbol": symbol,
            "side": mexc_side,
            "type": 5,  # 成行注文
            "vol": size,
        }

        response = self._send_request(
            "POST", "/api/v1/private/order/submit_plan", params
        )

        if response.get("success"):
            return OrderResult(
                success=True,
                order_id=str(response.get("data", "")),
                message="Order placed successfully",
            )
        else:
            return OrderResult(
                success=False, message=response.get("message", "Unknown error")
            )

    def close_position(self, symbol: str, side: str) -> OrderResult:
        """ポジションを決済

        Args:
            symbol: 銘柄シンボル
            side: 現在のポジション方向 "LONG" or "SHORT"
        """
        # 決済サイド変換：LONG->3(決済ロング), SHORT->4(決済ショート)
        mexc_side = 3 if side == "LONG" else 4

        # まず現在のポジションサイズを取得
        position_response = self.get_positions(symbol)
        if not position_response.get("success"):
            return OrderResult(success=False, message="Failed to get position info")

        positions = position_response.get("data", [])
        target_position = None

        for pos in positions:
            if pos.get("symbol") == symbol:
                pos_side = "LONG" if pos.get("side") == 1 else "SHORT"
                if pos_side == side:
                    target_position = pos
                    break

        if not target_position:
            return OrderResult(
                success=False, message=f"No {side} position found for {symbol}"
            )

        position_size = abs(float(target_position.get("vol", 0)))

        params = {
            "symbol": symbol,
            "side": mexc_side,
            "type": 5,  # 成行注文
            "vol": position_size,
        }

        response = self._send_request(
            "POST", "/api/v1/private/order/submit_plan", params
        )

        if response.get("success"):
            return OrderResult(
                success=True,
                order_id=str(response.get("data", "")),
                message="Position closed successfully",
            )
        else:
            return OrderResult(
                success=False, message=response.get("message", "Unknown error")
            )

    def set_margin_mode(self, symbol: str, mode: str) -> bool:
        """マージンモードを設定

        Args:
            symbol: 銘柄シンボル
            mode: "isolated" or "cross"
        """
        # MEXC APIのマージンモード設定は実装次第
        # ここでは簡略化してログ出力のみ
        logger.info(f"Setting margin mode for {symbol} to {mode}")
        return True

    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """銘柄情報を取得"""
        # 銘柄情報取得のエンドポイント
        return self._send_request("GET", "/api/v1/contract/detail", {"symbol": symbol})


class MEXCClient:
    """MEXC統合クライアント（WebSocket価格購読 + REST API）"""

    def __init__(self, config: Config):
        self.config = config
        self.websocket_client = MEXCWebSocketClient(config)
        self.rest_client = MEXCRESTClient(config)

    async def start(self) -> bool:
        """クライアント開始"""
        return await self.websocket_client.connect()

    async def stop(self):
        """クライアント停止"""
        await self.websocket_client.disconnect()

    def set_tick_callback(self, callback: Callable[[TickData], None]):
        """価格データコールバック設定"""
        self.websocket_client.set_tick_callback(callback)

    async def subscribe_all_tickers(self) -> bool:
        """全銘柄価格購読開始"""
        return await self.websocket_client.subscribe_all_tickers()

    # REST API メソッドをラップ
    def get_balance(self) -> Dict[str, Any]:
        """残高取得"""
        return self.rest_client.get_balance()

    def get_positions(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """ポジション取得"""
        return self.rest_client.get_positions(symbol)

    def place_market_order(self, symbol: str, side: str, size: float) -> OrderResult:
        """成行注文"""
        return self.rest_client.place_market_order(symbol, side, size)

    def close_position(self, symbol: str, side: str) -> OrderResult:
        """ポジション決済"""
        return self.rest_client.close_position(symbol, side)

    def set_margin_mode(self, symbol: str, mode: str) -> bool:
        """マージンモード設定"""
        return self.rest_client.set_margin_mode(symbol, mode)

    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        """銘柄情報取得"""
        return self.rest_client.get_symbol_info(symbol)
