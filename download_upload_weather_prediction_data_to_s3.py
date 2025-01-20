import os
import boto3
import requests
import pandas as pd
from tqdm import tqdm
import subprocess
from botocore.exceptions import NoCredentialsError, ClientError
from datetime import datetime
import re
import time
import logging
from dotenv import load_dotenv

# 環境変数の読み込み
load_dotenv()

# ログの設定
logging.basicConfig(
    filename='./log/download_upload.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 設定部分
S3_BUCKET_NAME = 'dermsplanner-dev-bucket-weather-prediction-data-491085428607'  # 置き換えてください
S3_STORAGE_CLASS = 'STANDARD'             # 例: 'STANDARD', 'GLACIER', 'DEEP_ARCHIVE' など
DOWNLOAD_DIR = './data/output/'                # ローカルのダウンロードディレクトリ
CSV_FILE_PATH = './data/input/ods_download_url.csv'                # URLリストが含まれるCSVファイルのパス
RETRIES = 1                                # ダウンロード・アップロードの再試行回数
BACKOFF_FACTOR = 5                         # 再試行時のバックオフ時間（秒）

# 環境変数からAPIキーを取得
API_KEY = os.getenv('API_KEY')
if not API_KEY:
    raise ValueError("API_KEY 環境変数が設定されていません。")

# S3クライアントの作成
s3_client = boto3.client('s3')

def parse_period(period_str):
    """
    '期間'フィールドを 'YYYYMMDD-YYYYMMDD' の形式に変換します。
    例: '2024年6月28日～2024年6月30日' -> '20240628-20240630'
    """
    try:
        # 正規表現で日付部分を抽出
        dates = re.findall(r'(\d{4})年(\d{1,2})月(\d{1,2})日', period_str)
        if len(dates) != 2:
            logging.error(f"期待される期間フォーマットではありません。期間: {period_str}")
            return None
        start_date = datetime(int(dates[0][0]), int(dates[0][1]), int(dates[0][2]))
        end_date = datetime(int(dates[1][0]), int(dates[1][1]), int(dates[1][2]))
        formatted_period = f"{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}"
        return formatted_period
    except Exception as e:
        logging.error(f"期間のパース中にエラーが発生しました。期間: {period_str} エラー: {e}")
        return None

def download_file(url, download_path, retries=3, backoff_factor=5):
    """
    ファイルをダウンロードし、指定されたパスに保存します。
    再試行機能を含みます。
    curlを使用してダウンロードを実行します。
    """
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"ダウンロード試行 {attempt}/{retries} - URL: {url}")
            # curlコマンドの作成
            curl_command = [
                'curl',
                '-L',  # リダイレクトを追跡
                url,
                '-H', f'x-api-key: {API_KEY}',
                '-o', download_path,
                '-f',  # HTTPエラー時に失敗として扱う
                '--retry', '0'  # curl自身の再試行を無効化
            ]

            # subprocessでcurlを実行
            result = subprocess.run(curl_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            if result.returncode == 0:
                logging.info(f"ダウンロード完了: {url}")
                return True
            else:
                error_message = result.stderr.decode().strip()
                logging.error(f"curlエラー: {error_message}")
                raise subprocess.CalledProcessError(result.returncode, curl_command, output=result.stdout, stderr=result.stderr)
        except subprocess.CalledProcessError as e:
            if attempt < retries:
                wait = backoff_factor * attempt
                logging.warning(f"ダウンロード失敗: {url}。{wait}秒後に再試行します... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait)
            else:
                logging.error(f"最大再試行回数に達しました。URL: {url}")
                return False
        except Exception as e:
            logging.error(f"ファイルのダウンロード中に予期せぬエラーが発生しました。URL: {url} エラー: {e}")
            if attempt < retries:
                wait = backoff_factor * attempt
                logging.info(f"{wait}秒後に再試行します... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait)
            else:
                logging.error(f"最大再試行回数に達しました。URL: {url}")
                return False

def upload_to_s3(file_path, bucket, s3_key, storage_class='STANDARD', retries=3, backoff_factor=5):
    """
    S3にファイルをアップロードします。
    再試行機能を含みます。
    """
    for attempt in range(1, retries + 1):
        try:
            s3_client.upload_file(
                Filename=file_path,
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={'StorageClass': storage_class}
            )
            logging.info(f"アップロード完了: {s3_key}")
            return True
        except FileNotFoundError:
            logging.error(f"ファイルが見つかりません。{file_path}")
            return False
        except NoCredentialsError:
            logging.error("AWS認証情報が見つかりません。")
            return False
        except ClientError as e:
            logging.error(f"S3へのアップロード中にエラーが発生しました。{e}")
            if attempt < retries:
                wait = backoff_factor * attempt
                logging.info(f"{wait}秒後に再試行します... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait)
            else:
                logging.error(f"最大再試行回数に達しました。S3キー: {s3_key}")
                return False

def read_urls_from_csv(csv_path):
    """
    CSVファイルからURLリストと関連情報を読み込みます。
    """
    try:
        df = pd.read_csv(csv_path, encoding='shift-jis', dtype=str)
        required_columns = ['配信履歴ID', 'データ名', '期間', 'URL', '有効期限']
        for col in required_columns:
            if col not in df.columns:
                logging.error(f"CSVファイルに '{col}' 列が見つかりません。")
                return []
        df = df.dropna(subset=['配信履歴ID', '期間', 'URL', '有効期限'])
        return df.to_dict('records')
    except Exception as e:
        logging.error(f"CSVファイルの読み込み中にエラーが発生しました。{e}")
        return []

def main():
    # ダウンロードディレクトリが存在しない場合は作成
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        logging.info(f"ダウンロードディレクトリを作成しました。: {DOWNLOAD_DIR}")
    
    # URLリストの読み込み
    records = read_urls_from_csv(CSV_FILE_PATH)
    if not records:
        print("INFO: 処理するURLがありません。スクリプトを終了します。")
        logging.info("処理するURLがありません。スクリプトを終了します。")
        return

    total_files = len(records)
    print(f"INFO: 処理するファイル数: {total_files}")
    logging.info(f"処理するファイル数: {total_files}")

    for idx, record in enumerate(records, start=1):
        delivery_id = record['配信履歴ID']
        period_str = record['期間']
        url = record['URL']
        expiration = record['有効期限']  # 現在は使用していませんが、必要に応じて使用可能

        # '期間' を 'YYYYMMDDHHMM-YYYYMMDDHHMM' に変換
        try:
            formatted_expiration = parse_period(period_str)
        except ValueError as ve:
            logging.error(f"有効期限のフォーマットエラー: {expiration} エラー: {ve}")
            print(f"WARNING: 有効期限のフォーマットに問題があるため、ファイル名を生成できません。スキップします。配信履歴ID: {delivery_id}")
            continue

        # ファイル名の生成
        file_name = f"{delivery_id}_{formatted_expiration}.zip"
        download_path = os.path.join(DOWNLOAD_DIR, file_name)

        print(f"\n[{idx}/{total_files}] 開始: {file_name} のダウンロード")
        logging.info(f"[{idx}/{total_files}] 開始: {file_name} のダウンロード, URL: {url}")
        success = download_file(url, download_path, retries=RETRIES, backoff_factor=BACKOFF_FACTOR)
        if not success:
            print(f"WARNING: ダウンロードに失敗しました。スキップします。URL: {url}")
            logging.warning(f"ダウンロードに失敗しました。スキップします。URL: {url}")
            continue

        print(f"[{idx}/{total_files}] 開始: {file_name} のS3へのアップロード")
        logging.info(f"[{idx}/{total_files}] 開始: {file_name} のS3へのアップロード, S3キー: {file_name}")
        s3_key = file_name  # 必要に応じてS3上のパスを変更可能
        upload_success = upload_to_s3(download_path, S3_BUCKET_NAME, s3_key, S3_STORAGE_CLASS, retries=RETRIES, backoff_factor=BACKOFF_FACTOR)
        if not upload_success:
            print(f"WARNING: アップロードに失敗しました。ローカルファイルを保持します。{download_path}")
            logging.warning(f"アップロードに失敗しました。ローカルファイルを保持します。{download_path}")
            continue

        # アップロードが成功したらローカルファイルを削除
        try:
            os.remove(download_path)
            print(f"[{idx}/{total_files}] 削除: ローカルファイルを削除しました。{download_path}")
            logging.info(f"[{idx}/{total_files}] 削除: ローカルファイルを削除しました。{download_path}")
        except OSError as e:
            print(f"WARNING: ローカルファイルの削除中にエラーが発生しました。{download_path} エラー: {e}")
            logging.warning(f"ローカルファイルの削除中にエラーが発生しました。{download_path} エラー: {e}")

if __name__ == "__main__":
    main()