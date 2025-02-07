import os
import json
import time
import pymysql
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from google.cloud import language_v1

# ✅ 指定 ChromeDriver 絕對路徑
CHROMEDRIVER_PATH = "google_driver/chromedriver-linux64/chromedriver"

# 讀取環境變數
load_dotenv()
MYSQL_HOST = os.getenv('MARIADB_HOST')
MYSQL_USER = os.getenv('MARIADB_USER')
MYSQL_PASSWORD = os.getenv('MARIADB_PASSWORD')
MYSQL_DB = os.getenv('MARIADB_DB')

# 設定 Selenium 瀏覽器選項
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # ✅ 不開啟視窗模式
options.add_argument("--no-sandbox")  
options.add_argument("--disable-dev-shm-usage")  
options.add_argument("--user-data-dir=/tmp/chrome-user-data")  
options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
)

# 設定 Google NLP API 客戶端
client = language_v1.LanguageServiceClient()

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

# 確保資料表存在
def create_table():
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS reddit (
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
            print("✅ Reddit 資料表檢查完成")
        except pymysql.MySQLError as e:
            print(f"❌ 建立資料表時發生錯誤: {e}")
        finally:
            conn.close()

# 使用 Google NLP 進行情感分析
def analyze_sentiment(text):
    if not text.strip():
        return 0.0
    document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
    try:
        sentiment = client.analyze_sentiment(request={'document': document}).document_sentiment
        return round(sentiment.score, 6)
    except Exception as e:
        print(f"⚠️ Google NLP API 錯誤: {e}")
        return 0.0

# 儲存至 MariaDB
def save_to_db(title, content, comment, sentiment_score, site, search_keyword, capture_date):
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            sql_query = """
                INSERT INTO reddit (title, content, comment, sentiment_score, site, search_keyword, capture_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql_query, (title, content, comment, sentiment_score, site, search_keyword, capture_date))
            conn.commit()
            print(f"✅ 已儲存: {title[:30]} - {comment[:30]}...")
        except pymysql.MySQLError as e:
            print(f"❌ MySQL 錯誤: {e}")
        finally:
            conn.close()

# 讀取關鍵字
def load_keywords(filename="keywords.txt"):
    try:
        with open(filename, "r", encoding="utf-8") as file:
            return [line.strip() for line in file.readlines() if line.strip()]
    except FileNotFoundError:
        print(f"❌ 關鍵字檔案 {filename} 不存在")
        return []

# 抓取 Reddit 文章
def fetch_reddit_articles(query):
    today = time.strftime("%Y-%m-%d")
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        print(f"🔍 搜索 Reddit: {query}")
        driver.get(f"https://www.reddit.com/search/?q={query}")

        time.sleep(5)  # 等待頁面載入
        soup = BeautifulSoup(driver.page_source, "html.parser")
        posts = soup.find_all("a", {"data-testid": "post-title"})[:20]  # ✅ 限制最多 20 篇文章

        print(f"📌 找到 {len(posts)} 則 Reddit 文章")

        for post in posts:
            title = post.get_text(strip=True)
            link = "https://www.reddit.com" + post['href']

            driver.get(link)
            time.sleep(5)
            post_soup = BeautifulSoup(driver.page_source, "html.parser")

            # 解析文章內容
            content_element = post_soup.find("div", {"id": lambda x: x and x.startswith('t3_')})
            content = content_element.get_text(strip=True) if content_element else "無法抓取內容"

            # 解析留言
            comments_section = post_soup.find_all("div", {"id": lambda x: x and "comment" in x})
            comments = [c.get_text(strip=True) for c in comments_section if c.get_text(strip=True)]
            comments = comments[:10] if comments else ["沒有找到留言"]

            # ✅ 即時儲存文章 & 留言
            for comment in comments:
                sentiment_score = analyze_sentiment(comment)
                save_to_db(title, content, comment, sentiment_score, "Reddit", query, today)

        print(f"✅ 關鍵字 {query} 處理完成！")
    except Exception as e:
        print(f"❌ 發生錯誤: {e}")
    finally:
        driver.quit()  # ✅ 確保 Selenium 關閉

# 主程式
def main():
    create_table()

    keywords = load_keywords()
    if not keywords:
        print("❌ 關鍵字清單為空，請檢查 keywords.txt")
        return

    for keyword in keywords:
        fetch_reddit_articles(keyword)

        print(f"🕒 等待 5 秒以避免被 Reddit 封鎖...")
        time.sleep(5)  # ✅ 減少封鎖風險

    print("✅ 所有關鍵字處理完成")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️ 程式被中斷，結束執行。")
