import requests
from bs4 import BeautifulSoup
import pymysql
import os
from dotenv import load_dotenv
from google.cloud import language_v1
from google.cloud.language_v1 import Document
from datetime import date

# 讀取環境變數
load_dotenv()
MYSQL_HOST = os.getenv('MARIADB_HOST')
MYSQL_USER = os.getenv('MARIADB_USER')
MYSQL_PASSWORD = os.getenv('MARIADB_PASSWORD')
MYSQL_DB = os.getenv('MARIADB_DB')

BASE_URL = "https://pttweb.tw/ALLPOST/*"  # 基本 URL
MAX_ARTICLES = 10  # 每個關鍵字最多抓取 10 篇文章

# 連接 MariaDB
def connect_to_db():
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset="utf8mb4",
            autocommit=True
        )
        return conn
    except pymysql.MySQLError as e:
        print(f"❌ MySQL 連線錯誤: {e}")
        return None

# 確保資料表存在，新增 site、search_keyword 與 capture_date 欄位
def create_table():
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ptt (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    sentiment_score FLOAT(10, 6),
                    site VARCHAR(50) NOT NULL,
                    search_keyword VARCHAR(100) NOT NULL,
                    capture_date DATE NOT NULL
                )
            """)
            conn.commit()
            print("✅ 資料表檢查完成")
        except pymysql.MySQLError as e:
            print(f"❌ 建立資料表時發生錯誤: {e}")
        finally:
            conn.close()

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

# 讀取關鍵字
def load_keywords(filename="keywords.txt"):
    try:
        with open(filename, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines() if line.strip()]
    except FileNotFoundError:
        print(f"❌ 關鍵字檔案 {filename} 不存在")
        return []

# 抓取文章列表（加入 timeout 與例外處理）
def fetch_article_links(keyword):
    search_url = f"{BASE_URL}{keyword}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(search_url, headers=headers, timeout=10)
    except requests.RequestException as e:
        print(f"❌ 連線搜尋結果失敗: {e}")
        return []

    if response.status_code != 200:
        print(f"❌ 無法取得搜尋結果，錯誤碼: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    articles = []
    for link in soup.select("div.articles a")[:MAX_ARTICLES]:
        title = link.select_one(".name").text.strip()
        url = f"https://pttweb.tw{link['href']}"
        articles.append({"title": title, "url": url})
    return articles

# 解析文章內容與留言（加入 timeout 與例外處理）
def parse_article(article_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        response = requests.get(article_url, headers=headers, timeout=10)
    except requests.RequestException as e:
        print(f"❌ 連線文章失敗: {e}")
        return None

    if response.status_code != 200:
        print(f"❌ 無法獲取文章，錯誤碼: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # 擷取標題
    title_element = soup.select_one("div.article span.value h1")
    title = title_element.text.strip() if title_element else "No Title"

    # 擷取內文
    content_element = soup.select_one("div.article")
    content = content_element.text.strip() if content_element else "No Content"

    # 擷取留言
    comments_data = []
    for push in soup.select("div.push span.f3.push-content"):
        comment_text = push.text.strip()
        if comment_text:
            sentiment_score = analyze_sentiment(comment_text)
            comments_data.append({"comment": comment_text, "sentiment_score": sentiment_score})

    print(f"📄 解析文章: {title[:30]} | 內文長度: {len(content)} | 留言數量: {len(comments_data)}")
    return {"title": title, "content": content, "comments": comments_data}

# 儲存至 MariaDB，允許重複文章，並新增 capture_date 欄位
def save_to_db(title, content, comment, sentiment_score, site, search_keyword, capture_date):
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            print(f"🔄 嘗試插入資料:\n標題: {title[:30]}\n內文: {content[:50]}\n留言: {comment[:50]}\n情感分數: {sentiment_score}\n來源: {site}\n關鍵字: {search_keyword}\n抓取日期: {capture_date}")
            sql_query = """
                INSERT INTO ptt (title, content, comment, sentiment_score, site, search_keyword, capture_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql_query, (title, content, comment, sentiment_score, site, search_keyword, capture_date))
            conn.commit()
            print(f"✅ 成功儲存: {title[:30]}...")
        except pymysql.MySQLError as e:
            print(f"❌ MySQL 錯誤: {e}")
        finally:
            conn.close()

# 主程式
def main():
    create_table()

    keywords = load_keywords()
    if not keywords:
        print("❌ 關鍵字清單為空，請檢查 keywords.txt")
        return

    # 取得今天日期，格式為 YYYY-MM-DD
    today = date.today().isoformat()

    for keyword in keywords:
        print(f"🔍 處理關鍵字: {keyword}")
        articles = fetch_article_links(keyword)
        for article in articles:
            print(f"📄 處理文章: {article['title']} | URL: {article['url']}")
            article_data = parse_article(article["url"])
            if article_data:
                for comment_data in article_data["comments"]:
                    save_to_db(
                        article_data["title"],
                        article_data["content"],
                        comment_data["comment"],
                        comment_data["sentiment_score"],
                        "ptt",
                        keyword,
                        today
                    )

    print("✅ 所有關鍵字處理完成")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️ 程式被中斷，結束執行。")
