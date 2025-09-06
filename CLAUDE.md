# MEXC Data Collector - Claude Code 設定ドキュメント
# Communication
- **IMPORTANT:** 以後の出力は日本語で行うこと。
- 英文のエラーやログは原文を引用 → 直後に日本語で要約。
- コードは原文維持、コメントと解説は日本語。
- ファイル名・CLI出力など機械可読部分は原文のまま。
- Dockerを起動してテストする場合は、force-recreateでbuildし直してからにしてください。
  docker-compose up -d --build --force-recreate



## プロジェクト概要
MEXC Data Collectorはシンプルな暗号通貨価格データ収集システムです：
- MEXCのWebSocket APIからリアルタイムティックデータを取得（全USDT銘柄）
- QuestDB時系列データベースに価格データを記録
- 軽量・高速なデータ収集に特化
- 取引機能なし（データ収集専用）

## 開発コマンド

### Docker操作
```bash
# アプリケーション起動
docker-compose up -d

# ログ確認
docker-compose logs -f trade-mini

# アプリケーション再起動
docker-compose restart trade-mini

# アプリケーション停止
docker-compose down

# コンテナ状態確認
docker-compose ps
```

### 監視・デバッグ
```bash
# リアルタイムログ確認
docker compose logs -f trade-mini

# データ収集統計を確認
docker compose logs trade-mini | grep -E "(Statistics|Batch processed|ticks processed)"

# QuestDB書き込み状況を確認
docker compose logs trade-mini | grep -E "(QuestDB|saved)"

# データベース接続確認
timeout 10 docker-compose logs questdb
```

### データ確認
```bash
# QuestDBコンソールアクセス（ブラウザ）
# http://localhost:9000

# 収集データの確認SQL例
# SELECT symbol, count(*) as tick_count, min(timestamp), max(timestamp)
# FROM tick_data
# GROUP BY symbol
# ORDER BY tick_count DESC
# LIMIT 10;
```

## アーキテクチャ

### 主要コンポーネント
- **main.py**: データ収集メインアプリケーション（MEXCDataCollector）
- **mexc_client.py**: MEXCのWebSocket APIクライアント
- **data_manager.py**: ティックデータの一時管理
- **questdb_client.py**: QuestDB時系列データベース保存クライアント
- **config.py**: 設定管理
- **websocket_monitor.py**: WebSocket監視・デバッグ用（オプション）

### シンプル・アーキテクチャ
- **WebSocket受信**: MEXCから全USDT銘柄のリアルタイム価格データを受信
- **データ処理**: 受信データを非同期で処理
- **QuestDB保存**: ILP形式でバッチ保存による高速書き込み

## データ収集機能（2025-09-06）

### シンプル・軽量設計
取引機能を完全に削除し、データ収集に特化：

**主な変更点**:
- 取引戦略・ポジション管理機能を削除
- Bybit取引機能を削除
- マルチプロセス・アーキテクチャを簡素化
- シンプルな非同期データ処理に変更

#### 主要機能：
1. **MEXCデータ受信**: WebSocket APIから全USDT銘柄の価格データを受信
2. **非同期処理**: asyncio使用による軽量・高速処理
3. **QuestDB保存**: ILP形式でのバッチ保存により高速書き込み
4. **統計監視**: 30秒間隔での処理統計表示

#### 技術詳細：
```python
# シンプルなデータフロー
WebSocket受信 → 非同期バッチ処理 → QuestDB保存
```

### パフォーマンス特性：
- **軽量**: 取引機能なしによる大幅なメモリ・CPU使用量削減
- **安定性**: シンプルな構造による高い安定性
- **保守性**: 理解しやすいコード構造

## 設定

### データ収集設定（config.yml）
- **questdb_host**: QuestDBサーバーホスト（デフォルト: questdb）
- **questdb_port**: QuestDBポート（デフォルト: 8812）
- **questdb_ilp_port**: QuestDB ILPポート（デフォルト: 9009）
- **tick_table_name**: ティックデータテーブル名（デフォルト: tick_data）
- **log_level**: ログレベル（デフォルト: INFO）
- **log_file**: ログファイル名（デフォルト: data_collector.log）

### 必要な設定項目のみ
取引機能を削除したため、以下の設定は不要になりました：
- Bybit API設定（api_key、api_secret等）
- 取引戦略パラメータ
- ポジション管理設定

## トラブルシューティング

### よくある問題
1. **WebSocket接続失敗**: MEXCサーバーの接続状況を確認
2. **QuestDB接続失敗**: QuestDBコンテナが起動しているかを確認
3. **データが保存されない**: QuestDBのILPポート（9009）が開いているかを確認
4. **メモリ使用量増加**: data_managerのバッファサイズを調整

### ログ分析
```bash
# データ収集統計を確認
docker-compose logs trade-mini | grep "Statistics"

# QuestDB書き込み状況を確認
docker-compose logs trade-mini | grep "QuestDB"

# エラーパターンを確認
docker-compose logs trade-mini | grep -E "(Error|ERROR)" | tail -20

# WebSocket接続状況を確認
docker-compose logs trade-mini | grep -E "(WebSocket|received)"
```

## 開発ノート

### コードスタイル
- シンプルで理解しやすいコード構造
- 適切な非同期処理（asyncio使用）
- 統計情報には 📊 絵文字でマーク
- エラーハンドリングを重視

### テスト戦略
- Dockerコンテナでの動作確認
- 1時間以上の安定動作確認
- QuestDBデータ書き込み確認
- メモリリーク検査

### パフォーマンス目標
- WebSocket接続安定性: 24時間以上の連続接続
- データ保存成功率: > 95%
- 処理遅延: < 1秒/バッチ
- メモリ使用量: 安定（リークなし）
