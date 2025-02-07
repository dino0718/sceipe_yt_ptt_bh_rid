import pymysql
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import language_v1
from google.cloud.language_v1 import Document
import os
import time
import random
from dotenv import load_dotenv
from datetime import datetime, date

# 讀取 .env 設定檔
load_dotenv()

# 從環境變數中獲取 API 密鑰與資料庫設定
API_KEY = os.getenv('YOUTUBE_API_KEY')
MYSQL_HOST = os.getenv('MARIADB_HOST')
MYSQL_USER = os.getenv('MARIADB_USER')
MYSQL_PASSWORD = os.getenv('MARIADB_PASSWORD')
MYSQL_DB = os.getenv('MARIADB_DB')

# 轉換 ISO 8601 格式為 MySQL 可用的 DATETIME 格式
def convert_to_mysql_datetime(iso_datetime):
    dt = datetime.strptime(iso_datetime.replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# 使用 Google Cloud Natural Language API 進行情感分析
def analyze_sentiment(text):
    if not text.strip():
        return 0.0
    client = language_v1.LanguageServiceClient()
    document = Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
    try:
        sentiment = client.analyze_sentiment(request={'document': document}).document_sentiment
        return round(sentiment.score, 6)
    except Exception as e:
        print(f"⚠️ Google NLP API 錯誤: {e}")
        return 0.0

# 建立 MySQL 連接
def connect_to_db():
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset="utf8mb4"
        )
        return conn
    except pymysql.MySQLError as e:
        print(f"MySQL 連線錯誤: {e}")
        return None

# 檢查資料表是否存在，不存在則創建
def create_tables_if_not_exist():
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'yt'")
            if not cur.fetchone():
                cur.execute("""
                    CREATE TABLE yt (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        video_id VARCHAR(255) NOT NULL,
                        title TEXT,
                        sentiment_score FLOAT(10, 6),
                        comment_content TEXT,
                        comment_sentiment_score FLOAT(10, 6),
                        site VARCHAR(50) NOT NULL,
                        search_keyword VARCHAR(100) NOT NULL,
                        capture_date DATE NOT NULL
                    )
                """)
            conn.commit()
        except pymysql.MySQLError as e:
            print(f"❌ 建立資料表時發生錯誤: {e}")
        finally:
            conn.close()

# **單條留言即時存入資料庫**
def save_to_db(video_id, title, sentiment_score, comment, site, search_keyword, capture_date):
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()

            # 插入單條留言
            video_query = """
                INSERT INTO yt (video_id, title, sentiment_score, comment_content, comment_sentiment_score, site, search_keyword, capture_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            video_params = (
                video_id,
                title,
                sentiment_score,
                comment['content'],  # 只存入單條留言
                comment['sentiment_score'],
                site,
                search_keyword,
                capture_date
            )
            cur.execute(video_query, video_params)

            # 立即提交到資料庫
            conn.commit()
            print(f"✅ 成功存入留言：{comment['content'][:30]}... (影片: {title[:30]})")

            # 隨機延遲避免 API 過載
            time.sleep(random.uniform(1, 2))

        except pymysql.MySQLError as e:
            print(f"❌ 資料儲存時發生錯誤: {e}")
        finally:
            conn.close()

def youtube_scraper():
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    create_tables_if_not_exist()

    # 取得今天的日期
    today = date.today().isoformat()

    # 讀取關鍵字
    with open('keywords_yt.txt', 'r') as file:
        keywords = file.readlines()

    for query in keywords:
        query = query.strip()
        print(f"🔍 正在搜尋關鍵字: {query}")

        # 取得影片
        videos = search_videos(query)
        time.sleep(random.uniform(3, 6))

        for video in videos:
            print(f"📄 正在爬取影片: {video['title']} ({video['video_id']})")

            # 取得留言
            comments = get_all_comments(video['video_id'])
            time.sleep(random.uniform(2, 5))

            if not comments:
                print(f"⚠️ 無法獲取評論，影片 ID：{video['video_id']}，標題：{video['title']}")
                continue

            # 計算影片的平均情感分數
            video_sentiment_score = sum([c['sentiment_score'] for c in comments]) / len(comments)

            # **即時存入每條留言**
            for comment in comments:
                save_to_db(
                    video_id=video['video_id'],
                    title=video['title'],
                    sentiment_score=video_sentiment_score,  # 影片的總體情感分數
                    comment=comment,
                    site="youtube",
                    search_keyword=query,
                    capture_date=today
                )

            time.sleep(random.uniform(1, 3))

    print("✅ 所有資料已成功保存至資料庫")

def search_videos(keyword, max_results=3):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    try:
        search_response = youtube.search().list(
            q=keyword,
            part='snippet',
            type='video',
            maxResults=max_results,
            order='date',
            videoDuration='medium',
            regionCode='TW'
        ).execute()

        return [{
            'video_id': item['id']['videoId'],
            'title': item['snippet'].get('title', 'No Title')
        } for item in search_response['items']]

    except HttpError as e:
        print(f"搜尋失敗，錯誤訊息：{e}")
        return []

def get_all_comments(video_id, max_comments=50):
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    comments = []
    try:
        request = youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=max_comments)
        response = request.execute()
        for item in response.get('items', []):
            text = item['snippet']['topLevelComment']['snippet'].get('textOriginal', '')
            comments.append({'content': text, 'sentiment_score': analyze_sentiment(text)})
    except HttpError:
        pass
    return comments

def main():
    youtube_scraper()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️ 程式被中斷，結束執行。")
