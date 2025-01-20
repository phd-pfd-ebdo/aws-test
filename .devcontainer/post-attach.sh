#!/bin/bash

# Poetryのインストール
curl -sSL https://install.python-poetry.org | python3.11

echo "export PATH="$HOME/.local/bin:$PATH"" >> ~/.bashrc

source ~/.bashrc

# Poetryの仮想環境を有効化
poetry config virtualenvs.in-project true

poetry install --no-root

poetry shell