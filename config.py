"""
設定管理モジュール
"""

import os
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


class Config:
    """設定管理クラス"""

    def __init__(self, config_path: str = "config.yml"):
        """
        設定を初期化

        Args:
            config_path: 設定ファイルパス
        """
        # 環境変数を読み込み
        load_dotenv()

        # YAML設定ファイルを読み込み
        with open(config_path, "r", encoding="utf-8") as file:
            self._config: Dict[str, Any] = yaml.safe_load(file)

    def get(self, key: str, default: Any = None) -> Any:
        """
        設定値を取得（ドット記法対応）

        Args:
            key: 設定キー（例：'strategy.long_threshold_percent'）
            default: デフォルト値

        Returns:
            設定値
        """
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    # API認証情報
    @property
    def mexc_api_key(self) -> str:
        """MEXC API Key"""
        return os.getenv("MEXC_API_KEY", "")

    @property
    def mexc_api_secret(self) -> str:
        """MEXC API Secret"""
        return os.getenv("MEXC_API_SECRET", "")

    # 戦略パラメータ
    @property
    def price_comparison_seconds(self) -> int:
        """価格比較秒数"""
        return self.get("strategy.price_comparison_seconds", 1)

    @property
    def long_threshold_percent(self) -> float:
        """ロング閾値（%）"""
        return self.get("strategy.long_threshold_percent", 2.0)

    @property
    def short_threshold_percent(self) -> float:
        """ショート閾値（%）"""
        return self.get("strategy.short_threshold_percent", 2.0)

    @property
    def reversal_threshold_percent(self) -> float:
        """反発閾値（%）"""
        return self.get("strategy.reversal_threshold_percent", 1.5)

    @property
    def min_profit_percent(self) -> float:
        """最小利益率（%）"""
        return self.get("strategy.min_profit_percent", 3.0)

    # ポジション管理
    @property
    def capital_usage_percent(self) -> float:
        """資金使用率（%）"""
        return self.get("position.capital_usage_percent", 10.0)

    @property
    def max_concurrent_positions(self) -> int:
        """最大同時ポジション数"""
        return self.get("position.max_concurrent_positions", 3)

    @property
    def cross_margin_threshold(self) -> float:
        """クロスマージン切替閾値（USDT）"""
        return self.get("position.cross_margin_threshold", 1000.0)

    @property
    def max_loss_on_2x_reversal(self) -> float:
        """2倍逆行時最大損失率（%）"""
        return self.get("position.max_loss_on_2x_reversal", 10.0)

    # データ管理
    @property
    def tick_data_retention_hours(self) -> int:
        """ティックデータ保持時間"""
        return self.get("data.tick_data_retention_hours", 6)

    @property
    def save_interval_seconds(self) -> int:
        """保存間隔（秒）"""
        return self.get("data.save_interval_seconds", 60)

    # MEXC API設定
    @property
    def mexc_ws_url(self) -> str:
        """MEXC WebSocket URL"""
        return self.get("mexc.ws_url", "wss://contract.mexc.com/ws")

    @property
    def mexc_api_url(self) -> str:
        """MEXC REST API URL"""
        return self.get("mexc.api_url", "https://contract.mexc.com")

    @property
    def mexc_reconnect_interval(self) -> int:
        """再接続間隔（秒）"""
        return self.get("mexc.reconnect_interval", 5)

    @property
    def mexc_max_reconnect_attempts(self) -> int:
        """最大再接続試行回数"""
        return self.get("mexc.max_reconnect_attempts", 10)

    @property
    def mexc_ping_interval(self) -> int:
        """Pingインターバル（秒）"""
        return self.get("mexc.ping_interval", 30)

    # QuestDB設定
    @property
    def questdb_host(self) -> str:
        """QuestDB ホスト"""
        return os.getenv("QUESTDB_HOST", self.get("questdb.host", "questdb"))

    @property
    def questdb_port(self) -> int:
        """QuestDB ポート"""
        return int(os.getenv("QUESTDB_PORT", self.get("questdb.port", 9000)))

    @property
    def questdb_ilp_port(self) -> int:
        """QuestDB ILP ポート"""
        return int(os.getenv("QUESTDB_ILP_PORT", self.get("questdb.ilp_port", 9009)))

    @property
    def tick_table_name(self) -> str:
        """ティックデータテーブル名"""
        return self.get("questdb.tick_table", "tick_data")

    @property
    def trade_table_name(self) -> str:
        """取引記録テーブル名"""
        return self.get("questdb.trade_table", "trade_records")

    # ログ設定
    @property
    def log_level(self) -> str:
        """ログレベル"""
        return os.getenv("LOG_LEVEL", self.get("logging.level", "INFO"))

    @property
    def log_file(self) -> str:
        """ログファイル名"""
        return self.get("logging.file", "trade_mini.log")

    # リスク管理
    @property
    def max_consecutive_losses(self) -> int:
        """最大連続損失回数"""
        return self.get("risk.max_consecutive_losses", 5)

    @property
    def max_trades_per_day(self) -> int:
        """1日最大取引回数"""
        return self.get("risk.max_trades_per_day", 50)

    @property
    def min_order_size(self) -> float:
        """最小注文サイズ（USDT）"""
        return self.get("risk.min_order_size", 10.0)
