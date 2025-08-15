# Trade Mini

仮想通貨先物取引の自動売買システム（コンパクト版）

## 概要

Trade Miniは、**MEXCのWebSocketからティックデータを取得し、Bybitで注文・決済を行う**ハイブリッド型の自動売買システムです。大きな価格変動を検知して瞬時にエントリーし、反発を検知して利確する戦略を実装しています。

## 主な機能

- **ハイブリッド取引システム**
  - MEXCのWebSocket APIから全USDT銘柄のリアルタイムティックデータを取得
  - Bybitで実際の注文・決済を実行（先物注文APIが完全対応済み）
  - 将来のMEXC注文API対応に備えた設定可能な仕組み
- **高度な銘柄マッピング**
  - MEXCとBybitの銘柄を自動マッピング
  - Bybitで取引可能な銘柄のみを対象とした戦略実行
- **N秒価格変動検知による自動エントリー**
- **反発検知による自動利確**
- **最大3銘柄同時ポジション管理**
- **資金管理（全資産の設定可能%使用）**
- **QuestDBによるデータ保存**
- **Dockerによる簡単デプロイ**

## 取引戦略

1. **データ取得**: MEXCのWebSocketから全USDT銘柄の1秒毎ティックデータをリアルタイム監視
2. **銘柄フィルタリング**: Bybitで取引可能な銘柄のみを戦略分析対象とする
3. **エントリー判定**: N秒前との価格比較でX%上昇→ロング、Y%下落→ショート
4. **注文実行**: Bybitで成行注文を実行（価格2倍逆行でも10%損失に収まるレバレッジ）
5. **利益管理**: M%利益達成後、建値に損切り設定
6. **決済実行**: オープン後最高値/最安値からZ%反発でBybitで決済

## セットアップ

### 🚨 デモトレード（推奨）

**実際の資金を使う前に、必ずデモトレードで動作確認してください。**

#### 1. デモトレード用の認証情報を設定

```bash
# .env.example を .env にコピー
cp .env.example .env

# .env ファイルを編集してAPI認証情報を入力
# MEXC API（ティックデータ取得用）
MEXC_API_KEY=your_mexc_api_key_here
MEXC_API_SECRET=your_mexc_api_secret_here

# Bybit API（注文・決済用）- テストネット推奨
BYBIT_API_KEY=your_bybit_testnet_api_key_here
BYBIT_API_SECRET=your_bybit_testnet_api_secret_here
BYBIT_TESTNET=true  # テストネット使用
```

#### 2. テストネット環境を有効化

`config.yml`で以下を確認・設定：
```yaml
trading:
  # 使用する取引所（Bybitを使用）
  exchange: "bybit"

# ティックデータはMEXCから取得、注文はBybitで実行
# デモトレードの場合は.envでBYBIT_TESTNET=trueに設定
```

#### 3. デモトレード実行

```bash
# データディレクトリ作成
mkdir -p data/questdb data/trade-mini logs

# Docker Compose で起動（デモトレード）
docker-compose up -d

# ログ確認
docker-compose logs -f trade-mini
```

### 💰 本番トレード

#### 1. 本番用の認証情報を設定

```bash
# .env.example を .env にコピー
cp .env.example .env

# .env ファイルを編集してAPI認証情報を入力
# MEXC API（ティックデータ取得用）
MEXC_API_KEY=your_mexc_api_key_here
MEXC_API_SECRET=your_mexc_api_secret_here

# Bybit API（注文・決済用）- 本番環境
BYBIT_API_KEY=your_bybit_api_key_here
BYBIT_API_SECRET=your_bybit_api_secret_here
BYBIT_TESTNET=false  # 本番環境使用
```

#### 2. 本番環境を設定

`config.yml`で本番環境に変更：
```yaml
trading:
  # 使用する取引所（Bybitを使用）
  exchange: "bybit"

# 本番取引の場合は.envでBYBIT_TESTNET=falseに設定
```

### 2. 設定パラメータの調整

`config.yml`で各種パラメータを調整:

```yaml
strategy:
  price_comparison_seconds: 1    # N秒前価格との比較
  long_threshold_percent: 2.0    # X% ロング閾値
  short_threshold_percent: 2.0   # Y% ショート閾値
  reversal_threshold_percent: 1.5 # Z% 反発閾値
  min_profit_percent: 3.0        # M% 最小利益率

position:
  capital_usage_percent: 10.0    # 全資産のα%使用
  max_concurrent_positions: 3    # 最大同時ポジション数
  cross_margin_threshold: 1000.0 # クロスマージン切替閾値
```

### 3. データディレクトリ作成

```bash
mkdir -p data/questdb data/trade-mini logs
```

### 4. Docker Compose で起動

```bash
# 本番環境
docker-compose up -d

# 開発環境（デバッグログ有効）
docker-compose --profile dev up -d

# ログ確認
docker-compose logs -f trade-mini
```

## 監視とメンテナンス

### ログ確認
```bash
# リアルタイムログ
docker-compose logs -f trade-mini

# QuestDBログ
docker-compose logs -f questdb
```

### QuestDB Web Console
ブラウザで `http://localhost:9000` にアクセス

### データ確認
```sql
-- ティックデータ確認
SELECT * FROM tick_data ORDER BY timestamp DESC LIMIT 100;

-- 取引記録確認
SELECT * FROM trade_records ORDER BY timestamp DESC LIMIT 50;
```

### 停止と再起動
```bash
# 停止
docker-compose down

# 再起動
docker-compose up -d

# データも削除して完全リセット
docker-compose down -v
```

## ディレクトリ構成

```
trade-mini/
├── main.py              # メインアプリケーション
├── config.py           # 設定管理
├── mexc_client.py      # MEXC APIクライアント（ティックデータ取得）
├── bybit_client.py     # Bybit APIクライアント（注文・決済）
├── symbol_mapper.py    # 銘柄マッピング管理
├── data_manager.py     # ティックデータ管理
├── strategy.py         # 取引戦略
├── position_manager.py # ポジション管理
├── questdb_client.py   # QuestDB保存
├── config.yml          # 設定ファイル
├── .env.example        # 認証情報テンプレート
├── requirements.txt    # Python依存関係
├── Dockerfile          # Dockerイメージ定義
├── docker-compose.yml  # Docker Compose設定
├── questdb.conf        # QuestDB設定
└── README.md           # このファイル
```

## リスク管理

⚠️ **重要な注意事項**

1. **テスト環境での検証を必須とする**（Bybitテストネット推奨）
2. **少額から始める**
3. **API権限は先物取引のみに限定**
4. **両取引所のAPI設定が必要**
   - MEXC: ティックデータ取得のため（読み取り権限のみでOK）
   - Bybit: 注文・決済のため（取引権限が必要）
5. **定期的な資金状況確認**
6. **想定以上の損失時は手動停止**

## パラメータ調整の指針

### 初期設定（保守的）
- `capital_usage_percent`: 5% （全資産の5%のみ使用）
- `long_threshold_percent`: 3% （大きな変動のみ取引）
- `short_threshold_percent`: 3%
- `reversal_threshold_percent`: 2% （早めの利確）

### アグレッシブ設定
- `capital_usage_percent`: 15%
- `long_threshold_percent`: 1.5%
- `short_threshold_percent`: 1.5%  
- `reversal_threshold_percent`: 1%

## トラブルシューティング

### よくある問題

1. **WebSocket接続エラー**
   - ネットワーク接続確認
   - MEXCサーバー状況確認

2. **API認証エラー**
   - API Key/Secret の確認
   - API権限の確認

3. **QuestDB接続エラー**
   - `docker-compose ps` でコンテナ状態確認
   - `docker-compose logs questdb` でログ確認

4. **ポジション開設失敗**
   - 残高不足
   - 銘柄の取引停止
   - レバレッジ制限

### ログレベル変更
`.env`ファイルで設定:
```
LOG_LEVEL=DEBUG  # より詳細なログ
LOG_LEVEL=INFO   # 通常のログ
LOG_LEVEL=WARN   # 警告のみ
```

## ライセンス

このソフトウェアは教育・研究目的で提供されています。
実際の取引での使用は自己責任でお願いします。

## サポート

- バグ報告: GitHubのIssues
- 機能要望: GitHubのIssues
- 質問: GitHubのDiscussions