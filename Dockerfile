# MEXC Data Collector Dockerfile
FROM python:3.11-slim

# 作業ディレクトリ設定
WORKDIR /app

# システムパッケージ更新とタイムゾーン設定
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime \
    && echo "Asia/Tokyo" > /etc/timezone

# Python依存関係をインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションファイルをコピー（データ収集専用）
COPY config.py .
COPY mexc_client.py .
COPY questdb_client.py .
COPY main.py .
COPY config.yml .

# ログディレクトリ作成
RUN mkdir -p /app/logs

# 非rootユーザーで実行
RUN useradd -m -u 1000 trader && \
    chown -R trader:trader /app
USER trader

# ヘルスチェック（プロセス生存確認）
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# エントリーポイント
CMD ["python", "main.py"]