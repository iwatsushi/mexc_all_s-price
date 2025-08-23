# WebSocket監視モード

## 概要

`websocket_monitor.py`は、MEXCのWebSocket接続の受信頻度を確認するための専用モードです。データ処理やQuestDB保存は行わず、純粋にWebSocket受信とping送信のみを実行します。

## 特徴

- **軽量**: データ処理、戦略分析、取引実行は一切行わない
- **高速**: 受信メッセージの統計のみを収集
- **詳細統計**: 受信間隔、メッセージタイプ別統計、接続統計
- **自動再接続**: 接続が切断された場合の自動再接続機能
- **ping管理**: 15秒間隔での自動ping送信

## 使用方法

### 1. WebSocket監視モード専用実行

```bash
# 監視モードのみ実行
python websocket_monitor.py
```

### 2. メインアプリからのWebSocket監視モード

```bash
# コマンドライン引数で指定
python main.py --websocket-monitor
python main.py -w

# ヘルプ表示
python main.py --help
python main.py -h
```

### 3. Docker環境での実行

```bash
# コンテナ内でWebSocket監視
docker exec -it trade-mini python websocket_monitor.py

# またはmain.pyから
docker exec -it trade-mini python main.py -w
```

## 出力例

```
🔍 MEXC WebSocket Monitor Mode
Pure WebSocket receive + ping monitoring (no data processing)
Press Ctrl+C to stop monitoring
==================================================
2025-01-19 14:30:15 | INFO     | websocket_monitor:_connect_and_monitor:75 - 🔗 Connecting to MEXC WebSocket: wss://contract.mexc.com/edge
2025-01-19 14:30:15 | INFO     | websocket_monitor:_connect_and_monitor:85 - ✅ WebSocket connected successfully
2025-01-19 14:30:15 | INFO     | websocket_monitor:_connect_and_monitor:90 - 📡 Subscribed to sub.tickers channel
2025-01-19 14:30:15 | INFO     | websocket_monitor:_connect_and_monitor:94 - 💓 Ping manager initialized (15s interval)

📊 WebSocket Monitoring Stats:
   ⏱️  Uptime: 10.1s
   📨 Total messages: 25 (2.47/s)
   📈 Ticker messages: 23 (2.28/s)
   💓 Pings sent: 0
   🔄 Reconnections: 0
   📊 Message intervals: avg=0.400s, min=0.150s, max=0.850s
```

## 統計情報

### リアルタイム統計（10秒ごと表示）
- **Uptime**: 稼働時間
- **Total messages**: 総受信メッセージ数と受信レート（メッセージ/秒）
- **Ticker messages**: ティッカーメッセージ数と受信レート
- **Pings sent**: 送信したping数
- **Reconnections**: 再接続回数
- **Message intervals**: メッセージ受信間隔（平均・最小・最大）

### 最終統計
```
🏁 Final WebSocket Monitoring Statistics:
   ⏱️  Total uptime: 300.5s
   📨 Total messages: 1234
   📈 Ticker messages: 1200
   ✅ Subscription messages: 1
   💓 Pong messages: 20
   💓 Pings sent: 20
   🔗 Connection attempts: 1
   🔄 Reconnections: 0
   📊 Average rates:
      Total: 4.11 messages/s
      Tickers: 3.99 messages/s
   ⏱️  Message intervals: avg=0.250s, min=0.100s, max=1.200s
```

## 設定

### 監視間隔
- **ping送信間隔**: 15秒（MEXC推奨の10-20秒の中間値）
- **統計表示間隔**: 10秒
- **スタール検出**: 30秒（60秒で再接続）

### 再接続制御
- **最大再接続試行回数**: 10回
- **再接続待機時間**: 指数バックオフ（最大30秒）

## メッセージタイプ

1. **push.tickers**: 価格データ（主要）
2. **rs.sub.tickers**: 購読確認
3. **pong**: ping応答

## トラブルシューティング

### 接続エラー
```
WebSocket connection failed: [Errno 111] Connection refused
```
- ネットワーク接続を確認
- MEXCサーバーの状態を確認

### スタール検出
```
⚠️ Message stall detected: 35.2s since last message
```
- 一時的なネットワーク問題
- 60秒でスタール再接続が実行される

### 頻繁な再接続
- ネットワーク不安定
- ファイアウォール設定を確認
- プロキシ設定を確認

## パフォーマンス

- **CPU使用率**: 極めて低い（データ処理なし）
- **メモリ使用量**: 約10-20MB
- **ネットワーク**: 受信のみ（送信はping程度）

## 用途

1. **受信頻度確認**: MEXCからの実際の受信レートの測定
2. **接続安定性テスト**: 長時間の接続安定性確認
3. **ネットワーク診断**: WebSocket接続の品質診断
4. **デバッグ**: 本格的な取引前の接続テスト

## 注意事項

- このモードでは一切の取引は行われません
- データの永続化は行われません（統計のみ）
- QuestDBへの保存は行われません
- 戦略分析は実行されません
