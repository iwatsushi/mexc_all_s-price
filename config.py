"""
設定管理モジュール（データ収集専用）
"""

import os
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


class Config:
    """設定管理クラス（データ収集専用）"""

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
            key: 設定キー（例：'mexc.ping_interval'）
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

    # MEXC API設定
    @property
    def mexc_environment(self) -> str:
        """MEXC 環境（常にproduction）"""
        return self.get("mexc.environment", "production")

    @property
    def mexc_ws_url(self) -> str:
        """MEXC WebSocket URL"""
        return self.get("mexc.production.ws_url", "wss://contract.mexc.com/edge")

    @property
    def mexc_api_url(self) -> str:
        """MEXC REST API URL（将来の拡張用）"""
        return self.get("mexc.production.api_url", "https://api.mexc.com")

    @property
    def mexc_reconnect_interval(self) -> int:
        """MEXC 再接続間隔（秒）"""
        return self.get("mexc.reconnect_interval", 5)

    @property
    def mexc_max_reconnect_attempts(self) -> int:
        """MEXC 最大再接続試行回数"""
        return self.get("mexc.max_reconnect_attempts", 10)

    @property
    def mexc_ping_interval(self) -> int:
        """MEXC ping送信間隔（秒）"""
        return self.get("mexc.ping_interval", 20)

    # データ管理設定
    @property
    def tick_data_retention_hours(self) -> int:
        """ティックデータ保持時間（時間）"""
        return self.get("data.tick_data_retention_hours", 1)

    # QuestDB設定
    @property
    def questdb_host(self) -> str:
        """QuestDBホスト"""
        return self.get("questdb.host", "questdb")

    @property
    def questdb_port(self) -> int:
        """QuestDBポート"""
        return self.get("questdb.port", 9000)

    @property
    def questdb_ilp_port(self) -> int:
        """QuestDB ILPポート"""
        return self.get("questdb.ilp_port", 9009)

    @property
    def tick_table_name(self) -> str:
        """ティックデータテーブル名"""
        return self.get("questdb.tick_table", "tick_data")

    # ログ設定
    @property
    def log_level(self) -> str:
        """ログレベル"""
        return self.get("logging.level", "INFO")

    @property
    def log_file(self) -> str:
        """ログファイル名"""
        return self.get("logging.file", "data_collector.log")

    @property
    def log_max_size_mb(self) -> int:
        """ログファイル最大サイズ（MB）"""
        return self.get("logging.max_size_mb", 10)

    @property
    def log_backup_count(self) -> int:
        """ログファイルバックアップ数"""
        return self.get("logging.backup_count", 5)