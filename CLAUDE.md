# Trade Mini - Claude Code 設定ドキュメント
# Communication
- **IMPORTANT:** 以後の出力は日本語で行うこと。
- 英文のエラーやログは原文を引用 → 直後に日本語で要約。
- コードは原文維持、コメントと解説は日本語。
- ファイル名・CLI出力など機械可読部分は原文のまま。
- Dockerを起動してテストする場合は、force-recreateでbuildし直してからにしてください。
  docker-compose up -d --build --force-recreate



## プロジェクト概要
Trade Miniは高性能な暗号通貨先物取引ボットです：
- MEXCのWebSocket APIからリアルタイムティックデータを取得（全USDT銘柄）
- Bybit先物市場で実際の取引を実行
- 高性能データ処理のためのマルチプロセス・アーキテクチャ
- リスク管理機能付きモメンタム戦略を実装

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

### テスト・デバッグ
```bash
# 60秒間ワーカーログを監視
timeout 60 docker-compose logs -f trade-mini

# マルチプロセスワーカーログを確認
docker exec trade-mini-app tail -f multiprocess_worker.log

# 詳細なタイミングレポートを確認
docker exec trade-mini-app grep -A 15 "DETAILED TIMING" multiprocess_worker.log

# 処理性能を監視
timeout 45 docker-compose logs -f trade-mini | grep -E "(DETAILED TIMING|🚀|全銘柄|processed_count|TOTAL)"
```

### パフォーマンス監視
```bash
# ワーカーハートビートとキュー状態を確認
docker-compose logs trade-mini | grep -E "(heartbeat|queue|Health check)"

# 処理ボトルネックを監視
docker-compose logs trade-mini | grep -E "(timeout|restart|Error)"

# データベース接続を確認
timeout 10 docker-compose logs questdb
```

## アーキテクチャ

### 主要コンポーネント
- **main.py**: マルチプロセス管理を行うメインオーケストレータ
- **strategy.py**: 取引戦略実装（高速処理に最適化済み）
- **data_manager.py**: 時間ベース保持機能付きティックデータ管理
- **mexc_client.py**: リアルタイム価格データ取得用MEXCクライアント
- **bybit_client.py**: 注文実行用Bybit REST APIクライアント
- **position_manager.py**: ポジション・リスク管理
- **questdb_client.py**: 時系列データベース保存

### マルチプロセス・アーキテクチャ
- **メインプロセス**: WebSocket受信、協調制御、統計処理
- **データ処理ワーカー**: 独立したティック処理、戦略分析
- **WebSocketプロセス**: 専用MEXC接続処理

## 最新の最適化（2025-08-23）

### 戦略処理の高速化
`process_tick_and_execute_trades`の処理時間を最適化しました：

**最適化前**: 67秒（ハートビートタイムアウトの原因）
**最適化後**: 約7秒（90%改善）

#### 主要な最適化技術：
1. **アルゴリズム分岐最適化**: 99%の銘柄が軽量処理パスを使用
2. **早期終了検索**: 時系列順序を活用してデータ検索を50%削減
3. **ロック競合回避**: 並列処理のためのロック使用を最小化
4. **無効なキャッシュ実装の削除**: 毎銘柄でクリアされる無意味なキャッシュを除去

#### 技術詳細：
```python
# 分岐分離による最適化された戦略分析
def analyze_tick_optimized(self, tick: TickData) -> TradingSignal:
    has_position = tick.symbol in self.position_trackers  # ロックフリーチェック

    if has_position:
        return self._analyze_existing_position(tick)  # 1%の銘柄
    else:
        return self._analyze_new_entry_fast(tick)     # 99%が軽量処理
```

### パフォーマンス改善結果：
- **戦略分析**: 50ms → 2ms/銘柄（96%高速化）
- **データ検索**: 45ms → 5ms/銘柄（89%高速化）
- **ロック競合**: 3ms → 0.1ms/銘柄（97%削減）
- **総処理時間**: 67秒 → 7秒（90%改善）

## 設定

### 取引パラメータ（config.yml）
- 価格比較時間窓: 10秒
- ロング閾値: 0.001%
- ショート閾値: 0.001%
- 反発閾値: 0.0015%
- 最小利益閾値: 0.005%
- 最大同時ポジション数: 3
- ポジションサイズ: 資本の3.5%

### 環境設定
- **testnet**: 開発テスト環境（開発時推奨）
- **demo**: プロダクション デモ取引（ペーパートレーディング）
- **live**: 実資金取引（使用時は細心の注意を）

## トラブルシューティング

### よくある問題
1. **ワーカーハートビートタイムアウト**: 通常は上記の最適化で解決
2. **キューの詰まり**: 処理が受信データに追いついているかを確認
3. **API接続問題**: Bybit APIクレデンシャルとIP許可リストを確認
4. **データベース接続**: QuestDBコンテナが実行中かを確認

### ログ分析
```bash
# ワーカー再起動パターンを確認
docker-compose logs trade-mini | grep "restart"

# 処理タイミングを監視
docker-compose logs trade-mini | grep "DETAILED TIMING" -A 10

# エラーパターンを確認
docker-compose logs trade-mini | grep -E "(Error|ERROR)" | tail -20
```

## 開発ノート

### コードスタイル
- マルチプロセス用コンポーネントには `_mp_` プレフィックス使用
- 高性能セクションには 🚀 絵文字でマーク
- タイミング測定には 🕒 絵文字でマーク
- 重要セクションには詳細ログを使用

### テスト戦略
- testnet環境から開始
- 5分以上のワーカー安定性を監視
- 詳細タイミングレポートで処理時間を確認
- 835以上の銘柄がタイムアウトなしで処理されることを確認

### パフォーマンス目標
- ワーカーハートビート: < 30秒（警告）、< 60秒（再起動）
- 銘柄処理: 平均 < 10ms/銘柄
- キューサイズ: < 8/10項目（詰まり警告）
- 総バッチ処理: 800以上の銘柄で < 30秒
