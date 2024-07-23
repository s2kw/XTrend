import os
from pathlib import Path

# credentials.py が存在する場合、それを実行して環境変数を設定
credentials_path = Path(__file__).parent / "credentials.py"
if credentials_path.exists():
    exec(open(credentials_path).read())

from tweepy import Client, OAuth1UserHandler, API, errors as tweepy_errors
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime
import json

# Twitter API 認証
auth = OAuth1UserHandler(
    os.environ['TWITTER_CONSUMER_KEY'],
    os.environ['TWITTER_CONSUMER_SECRET'],
    os.environ['TWITTER_ACCESS_TOKEN'],
    os.environ['TWITTER_ACCESS_TOKEN_SECRET']
)
api = API(auth)

# API v2用のクライアント
client = Client(bearer_token=os.environ['TWITTER_BEARER_TOKEN'])

# Google Sheets API 認証
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# 環境変数から Google Credentials を取得し、一時ファイルとして保存
google_creds_json = os.environ.get('GOOGLE_CREDENTIALS')
if google_creds_json:
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        json.dump(json.loads(google_creds_json), temp_file)
        temp_file_path = temp_file.name

    creds = service_account.Credentials.from_service_account_file(
        temp_file_path, scopes=SCOPES)

    # 一時ファイルを削除
    os.unlink(temp_file_path)
else:
    # GitHub Actions 環境ではこちらを使用しないと思う
    creds = service_account.Credentials.from_service_account_file(
        'path/to/service_account.json', scopes=SCOPES)

sheets_service = build('sheets', 'v4', credentials=creds)

# Slack クライアント初期化
slack_client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])

# スプレッドシートID
SPREADSHEET_ID = '1j0YSHLZIc7mSUpN1uNtjQ_S3OatpcrAPMbBB_lXBQd0'
SHEET_NAME = 'out'

def get_trending_hashtags():
    try:
        trends = api.get_place_trends(23424856)  # 日本の WOEID
        return [trend['name'] for trend in trends[0]['trends'] if trend['name'].startswith('#')]
    except Exception as e:
        print(f"Error fetching trends: {e}")
        return []

def append_to_sheet(data):
    body = {
        'values': data
    }
    sheets_service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A:C',
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()

def get_sheet_data():
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{SHEET_NAME}!A:C'
    ).execute()
    return result.get('values', [])

def test_sheets_api():
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range='A1:A1'
        ).execute()
        print("Successfully connected to Google Sheets API")
        print(f"Test result: {result}")
    except Exception as e:
        print(f"Error connecting to Google Sheets API: {e}")

# main 関数の最初で呼び出す
test_sheets_api()

def analyze_data(data):
    df = pd.DataFrame(data[1:], columns=data[0])
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # 直近24時間のデータのみを分析
    last_24h = datetime.now() - pd.Timedelta(hours=24)
    df_recent = df[df['Timestamp'] > last_24h]
    
    # ハッシュタグの出現回数を集計
    hashtag_counts = df_recent['Hashtag'].value_counts()
    
    # 新しく出現したハッシュタグを特定
    new_hashtags = set(df_recent['Hashtag']) - set(df[df['Timestamp'] <= last_24h]['Hashtag'])
    
    return hashtag_counts.head(10).to_dict(), list(new_hashtags)

def post_to_slack(top_hashtags, new_hashtags):
    message = "最新のハッシュタグ分析結果:\n\n"
    message += "人気のハッシュタグ Top 10:\n"
    for hashtag, count in top_hashtags.items():
        message += f"- {hashtag}: {count}回\n"
    
    message += "\n新しく出現したハッシュタグ:\n"
    for hashtag in new_hashtags:
        message += f"- {hashtag}\n"

    try:
        slack_client.chat_postMessage(
            channel="#your-channel-name",
            text=message
        )
    except SlackApiError as e:
        print(f"Error posting message: {e}")

def main():
    trending_hashtags = get_trending_hashtags()
    current_time = datetime.now().isoformat()
    
    data_to_append = [[current_time, hashtag, 'Trending'] for hashtag in trending_hashtags]
    append_to_sheet(data_to_append)
    
    all_data = get_sheet_data()
    top_hashtags, new_hashtags = analyze_data(all_data)
    
    post_to_slack(top_hashtags, new_hashtags)

if __name__ == "__main__":
    main()