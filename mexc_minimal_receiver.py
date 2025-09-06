"""
MEXC最小限受信テスト - 受信頻度確認専用
"""

import asyncio
import gzip
import json
import time
from datetime import datetime

import websockets


class MinimalMEXCReceiver:
    """最小限のMEXC受信テスト"""

    def __init__(self):
        self.ws_url = "wss://contract.mexc.com/edge"
        self.running = False

        # 統計（最小限）
        self.stats = {
            "start_time": None,
            "total_messages": 0,
            "ticker_batches": 0,
            "total_tickers": 0,
            "last_message_time": 0,
        }

        # 受信間隔測定
        self.last_receive_time = 0
        self.intervals = []
        self.max_intervals = 100  # 直近100件のみ保持

    async def run(self):
        """受信テスト実行"""
        print("🚀 MEXC最小限受信テスト開始...")
        print(f"接続先: {self.ws_url}")
        print("Press Ctrl+C to stop\n")

        self.running = True
        self.stats["start_time"] = time.time()

        try:
            async with websockets.connect(
                self.ws_url,
                ping_interval=None,  # 自前管理
                max_size=None,
                open_timeout=10,
                close_timeout=5,
            ) as websocket:
                print("✅ WebSocket接続成功")

                # 購読
                subscribe_msg = {"method": "sub.tickers", "param": {}, "gzip": True}
                await websocket.send(json.dumps(subscribe_msg))
                print("📡 全ティッカー購読完了\n")

                # 統計表示タスク開始
                stats_task = asyncio.create_task(self._stats_loop())

                # メイン受信ループ
                await self._receive_loop(websocket)

                # クリーンアップ
                stats_task.cancel()

        except Exception as e:
            print(f"❌ エラー: {e}")
        finally:
            self.running = False
            self._print_final_stats()

    async def _receive_loop(self, websocket):
        """メイン受信ループ"""
        while self.running:
            try:
                # メッセージ受信
                raw_message = await websocket.recv()
                receive_time = time.time()

                # 統計更新
                self._update_stats(receive_time)

                # メッセージ処理（最小限）
                self._process_message(raw_message, receive_time)

            except websockets.exceptions.ConnectionClosed:
                print("📡 WebSocket接続が閉じられました")
                break
            except Exception as e:
                print(f"⚠️ 受信エラー: {e}")
                await asyncio.sleep(0.1)

    def _update_stats(self, receive_time):
        """統計更新"""
        self.stats["total_messages"] += 1
        self.stats["last_message_time"] = receive_time

        # 受信間隔計算
        if self.last_receive_time > 0:
            interval = receive_time - self.last_receive_time
            self.intervals.append(interval)

            # 直近100件のみ保持
            if len(self.intervals) > self.max_intervals:
                self.intervals.pop(0)

        self.last_receive_time = receive_time

    def _process_message(self, raw_message, receive_time):
        """メッセージ処理（最小限）"""
        try:
            # gzip解凍
            if isinstance(raw_message, (bytes, bytearray)):
                decompressed = gzip.decompress(raw_message)
                data = json.loads(decompressed)
            else:
                data = json.loads(raw_message)

            # ティッカーデータのみカウント
            channel = data.get("channel", "")
            if channel == "push.tickers":
                tickers = data.get("data", [])
                if isinstance(tickers, list):
                    self.stats["ticker_batches"] += 1
                    self.stats["total_tickers"] += len(tickers)

        except Exception:
            # エラーは無視（最小限処理のため）
            pass

    async def _stats_loop(self):
        """統計表示ループ"""
        while self.running:
            await asyncio.sleep(10)  # 10秒間隔
            if self.running:
                self._print_current_stats()

    def _print_current_stats(self):
        """現在の統計表示"""
        if not self.stats["start_time"]:
            return

        uptime = time.time() - self.stats["start_time"]

        # レート計算
        msg_rate = self.stats["total_messages"] / uptime if uptime > 0 else 0
        batch_rate = self.stats["ticker_batches"] / uptime if uptime > 0 else 0
        ticker_rate = self.stats["total_tickers"] / uptime if uptime > 0 else 0

        # 受信間隔統計
        avg_interval = 0
        min_interval = 0
        max_interval = 0

        if self.intervals:
            avg_interval = sum(self.intervals) / len(self.intervals)
            min_interval = min(self.intervals)
            max_interval = max(self.intervals)

        print("📊 === 受信統計 ===")
        print(f"⏱️  稼働時間: {uptime:.1f}秒")
        print(f"📨 総メッセージ数: {self.stats['total_messages']} ({msg_rate:.2f}/秒)")
        print(
            f"📈 ティッカーバッチ数: {self.stats['ticker_batches']} ({batch_rate:.2f}/秒)"
        )
        print(
            f"🎯 総ティッカー数: {self.stats['total_tickers']} ({ticker_rate:.1f}/秒)"
        )

        if self.intervals:
            print(
                f"⏳ 受信間隔: avg={avg_interval:.3f}s, min={min_interval:.3f}s, max={max_interval:.3f}s"
            )

        print("=" * 40 + "\n")

    def _print_final_stats(self):
        """最終統計表示"""
        if not self.stats["start_time"]:
            return

        total_time = time.time() - self.stats["start_time"]

        print("\n🏁 === 最終統計 ===")
        print(f"⏱️  総稼働時間: {total_time:.1f}秒")
        print(f"📨 総メッセージ数: {self.stats['total_messages']}")
        print(f"📈 ティッカーバッチ数: {self.stats['ticker_batches']}")
        print(f"🎯 総ティッカー数: {self.stats['total_tickers']}")

        if total_time > 0:
            print(f"📊 平均レート:")
            print(f"   メッセージ: {self.stats['total_messages'] / total_time:.2f}/秒")
            print(f"   バッチ: {self.stats['ticker_batches'] / total_time:.2f}/秒")
            print(f"   ティッカー: {self.stats['total_tickers'] / total_time:.1f}/秒")

        if self.intervals:
            avg_interval = sum(self.intervals) / len(self.intervals)
            print(f"⏳ 平均受信間隔: {avg_interval:.3f}秒")

        print("=" * 40)


async def main():
    """メイン関数"""
    receiver = MinimalMEXCReceiver()
    try:
        await receiver.run()
    except KeyboardInterrupt:
        print("\n👋 ユーザーによる停止")
    except Exception as e:
        print(f"\n💥 予期しないエラー: {e}")


if __name__ == "__main__":
    asyncio.run(main())
