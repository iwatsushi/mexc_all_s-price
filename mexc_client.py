"""
MEXC取引所クライアント（WebSocket + REST API統合）
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass

import requests
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

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
    """MEXC WebSocketクライアント"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ws_url = config.mexc_ws_url
        self.websocket = None
        self.connected = False
        self.shutdown_event = threading.Event()
        
        # データコールバック
        self.tick_callback: Optional[Callable[[TickData], None]] = None
        
        # 再接続設定
        self.reconnect_interval = config.mexc_reconnect_interval
        self.max_reconnect_attempts = config.mexc_max_reconnect_attempts
        self.reconnect_attempts = 0
        
        # 全銘柄購読フラグ
        self._all_tickers_subscribed = False
        
    async def connect(self) -> bool:
        """WebSocket接続を開始"""
        try:
            logger.info("Connecting to MEXC WebSocket...")
            
            self.websocket = await websockets.connect(
                self.ws_url,
                ping_interval=self.config.mexc_ping_interval,
                ping_timeout=10,
                close_timeout=10,
            )
            
            self.connected = True
            self.reconnect_attempts = 0
            
            # WebSocketメッセージ処理を開始
            asyncio.create_task(self._websocket_handler())
            
            logger.info("MEXC WebSocket connected successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MEXC WebSocket: {e}")
            return False
    
    async def disconnect(self):
        """WebSocket接続を終了"""
        logger.info("Disconnecting from MEXC WebSocket...")
        
        self.connected = False
        self.shutdown_event.set()
        
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.warning(f"Error closing websocket: {e}")
        
        self.websocket = None
        logger.info("MEXC WebSocket disconnected")
    
    async def subscribe_all_tickers(self) -> bool:
        """全銘柄ティッカー情報を購読"""
        try:
            if not self.connected or not self.websocket:
                return False
            
            subscribe_msg = {
                "method": "sub.tickers",
                "param": {}
            }
            
            await self.websocket.send(json.dumps(subscribe_msg))
            self._all_tickers_subscribed = True
            
            logger.info("Subscribed to MEXC all tickers")
            return True
            
        except Exception as e:
            logger.error(f"Failed to subscribe to MEXC all tickers: {e}")
            return False
    
    def set_tick_callback(self, callback: Callable[[TickData], None]):
        """ティックデータコールバックを設定"""
        self.tick_callback = callback
    
    async def _websocket_handler(self):
        """WebSocketメッセージハンドラー"""
        while not self.shutdown_event.is_set() and self.connected:
            try:
                if not self.websocket:
                    break
                
                # メッセージ受信（タイムアウト付き）
                try:
                    message = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
                    await self._process_message(message)
                except asyncio.TimeoutError:
                    continue
                    
            except ConnectionClosed:
                logger.warning("MEXC WebSocket connection closed")
                break
            except WebSocketException as e:
                logger.error(f"MEXC WebSocket error: {e}")
                break
            except Exception as e:
                logger.error(f"Error in MEXC WebSocket handler: {e}")
                break
        
        # 接続が切れた場合の再接続処理
        if not self.shutdown_event.is_set():
            await self._handle_reconnection()
    
    async def _handle_reconnection(self):
        """再接続処理"""
        self.connected = False
        
        while (
            not self.shutdown_event.is_set()
            and self.reconnect_attempts < self.max_reconnect_attempts
        ):
            self.reconnect_attempts += 1
            logger.info(
                f"Attempting to reconnect to MEXC ({self.reconnect_attempts}/{self.max_reconnect_attempts})"
            )
            
            await asyncio.sleep(self.reconnect_interval)
            
            if await self.connect():
                # 全銘柄ティッカーの再購読
                if self._all_tickers_subscribed:
                    await self.subscribe_all_tickers()
                break
        
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max reconnection attempts reached for MEXC")
    
    async def _process_message(self, message: str):
        """受信メッセージを処理"""
        try:
            data = json.loads(message)
            
            # ハートビート応答
            if data.get("msg") == "pong":
                return
            
            # 全銘柄ティッカーデータ
            if "method" in data and data.get("method") == "push.tickers":
                await self._process_all_tickers_data(data.get("data", []))
                
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse MEXC message: {e}")
        except Exception as e:
            logger.error(f"Error processing MEXC message: {e}")
    
    async def _process_all_tickers_data(self, tickers_data: List[Dict]):
        """全銘柄ティッカーデータ処理"""
        try:
            if not tickers_data or not self.tick_callback:
                return
            
            for ticker in tickers_data:
                if not ticker:
                    continue
                
                # シンボル名を取得・正規化
                symbol = ticker.get("symbol", "")
                if not symbol or not symbol.endswith("_USDT"):
                    continue
                
                # ティックデータを作成
                tick_data = TickData(
                    symbol=symbol,
                    price=float(ticker.get("lastPrice", 0)),
                    timestamp=datetime.fromtimestamp(ticker.get("time", 0) / 1000),
                    volume=float(ticker.get("volume24", 0))
                )
                
                # コールバック実行
                try:
                    self.tick_callback(tick_data)
                except Exception as e:
                    logger.error(f"Error in tick callback: {e}")
                    
        except Exception as e:
            logger.warning(f"Error processing MEXC all tickers data: {e}")


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
            if hasattr(e, 'response') and e.response:
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
        return self._send_request("GET", "/api/v1/private/position/list/open_positions", params)
    
    def place_market_order(
        self, 
        symbol: str, 
        side: str, 
        size: float
    ) -> OrderResult:
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
        
        response = self._send_request("POST", "/api/v1/private/order/submit_plan", params)
        
        if response.get("success"):
            return OrderResult(
                success=True,
                order_id=str(response.get("data", "")),
                message="Order placed successfully"
            )
        else:
            return OrderResult(
                success=False,
                message=response.get("message", "Unknown error")
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
            return OrderResult(
                success=False,
                message="Failed to get position info"
            )
        
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
                success=False,
                message=f"No {side} position found for {symbol}"
            )
        
        position_size = abs(float(target_position.get("vol", 0)))
        
        params = {
            "symbol": symbol,
            "side": mexc_side,
            "type": 5,  # 成行注文
            "vol": position_size,
        }
        
        response = self._send_request("POST", "/api/v1/private/order/submit_plan", params)
        
        if response.get("success"):
            return OrderResult(
                success=True,
                order_id=str(response.get("data", "")),
                message="Position closed successfully"
            )
        else:
            return OrderResult(
                success=False,
                message=response.get("message", "Unknown error")
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
    """MEXC統合クライアント（WebSocket + REST API）"""
    
    def __init__(self, config: Config):
        self.config = config
        self.ws_client = MEXCWebSocketClient(config)
        self.rest_client = MEXCRESTClient(config)
        
    async def start(self) -> bool:
        """クライアント開始"""
        return await self.ws_client.connect()
    
    async def stop(self):
        """クライアント停止"""
        await self.ws_client.disconnect()
    
    def set_tick_callback(self, callback: Callable[[TickData], None]):
        """ティックデータコールバック設定"""
        self.ws_client.set_tick_callback(callback)
    
    async def subscribe_all_tickers(self) -> bool:
        """全銘柄購読"""
        return await self.ws_client.subscribe_all_tickers()
    
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