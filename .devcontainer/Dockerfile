# ベースイメージとして公式のPythonイメージを使用（例: Python 3.11）
FROM python:3.11-slim

# システムパッケージのアップデートと必要なツールのインストール
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 作業ディレクトリの設定
WORKDIR /workspace

# pipのアップグレード
RUN pip install --upgrade pip

# 必要なPythonパッケージをインストール
# requirements.txtを使用する方法もありますが、ここでは直接インストールします
RUN pip install --no-cache-dir boto3 requests tqdm

# （オプション）必要に応じてAWS CLIのインストール
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && apt-get update && apt-get install -y unzip \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# 非ルートユーザーの作成（セキュリティ向上のため）
RUN useradd -ms /bin/bash devuser
USER devuser

# エントリーポイントを指定（必要に応じて）
# ENTRYPOINT [ "python" ]