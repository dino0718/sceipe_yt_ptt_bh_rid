import os
import time
import random
from datetime import date
import pymysql
from dotenv import load_dotenv
from google.cloud import language_v1
from google.cloud.language_v1 import Document
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 讀取 .env 設定檔
load_dotenv()

# 資料庫連線參數
MYSQL_HOST = os.getenv('MARIADB_HOST')
MYSQL_USER = os.getenv('MARIADB_USER')
MYSQL_PASSWORD = os.getenv('MARIADB_PASSWORD')
MYSQL_DB = os.getenv('MARIADB_DB')

# Google NLP API：情感分析
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

# MySQL 連線
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

# 建立資料表（若不存在）
def create_bahamut_table_if_not_exist():
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'bahamut'")
            if not cur.fetchone():
                cur.execute("""
                    CREATE TABLE bahamut (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        article_url TEXT,
                        title TEXT,
                        content LONGTEXT,
                        comments LONGTEXT,
                        content_sentiment_score FLOAT(10,6),
                        comment_sentiment_score FLOAT(10,6),
                        site VARCHAR(50) NOT NULL,
                        search_keyword VARCHAR(100) NOT NULL,
                        capture_date DATE NOT NULL
                    )
                """)
            conn.commit()
            print("✅ 資料表 bahamut 檢查/建立完成")
        except pymysql.MySQLError as e:
            print(f"❌ 建立資料表 bahamut 時發生錯誤: {e}")
        finally:
            conn.close()

# **即時存入 MySQL**
def save_bahamut_to_db(data):
    conn = connect_to_db()
    if conn:
        try:
            cur = conn.cursor()
            insert_query = """
                INSERT INTO bahamut 
                (article_url, title, content, comments, content_sentiment_score, comment_sentiment_score, site, search_keyword, capture_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                data['article_url'],
                data['title'],
                data['content'],
                data['comments'],
                data['content_sentiment_score'],
                data['comment_sentiment_score'],
                data['site'],
                data['search_keyword'],
                data['capture_date']
            ))
            conn.commit()
            print(f"✅ 即時存入資料庫: {data['title'][:30]}...")
            time.sleep(random.uniform(1, 2))  # 模擬人類
        except pymysql.MySQLError as e:
            print(f"❌ 儲存資料時發生錯誤: {e}")
        finally:
            conn.close()

# 設定 Selenium
def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service("google_driver/chromedriver-linux64/chromedriver")
    return webdriver.Chrome(service=service, options=chrome_options)

# 搜尋巴哈
def search_bahamut(driver, keyword):
    driver.get("https://search.gamer.com.tw/")
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input.gsc-input'))
        )
        search_box = driver.find_element(By.CSS_SELECTOR, 'input.gsc-input')
        search_box.clear()
        search_box.send_keys(keyword)
        search_box.send_keys(Keys.ENTER)
        time.sleep(random.uniform(2, 4))  # 模擬人類
    except Exception as e:
        print("❌ 搜尋巴哈失敗:", e)

# 解析文章內容
def parse_detail_page(driver, url):
    result = {"content": "", "comments": "", "article_url": url}
    try:
        driver.execute_script("window.open(arguments[0]);", url)
        driver.switch_to.window(driver.window_handles[-1])
        time.sleep(random.uniform(2, 4))

        divs = driver.find_elements(By.XPATH, '//div[contains(text(), "== $0")]')
        result["content"] = "\n".join([d.text.strip() for d in divs if d.text.strip()])

        spans = driver.find_elements(By.CSS_SELECTOR, 'span.comment_content[data-formatted="yes"]')
        result["comments"] = "\n".join([sp.text.strip() for sp in spans if sp.text.strip()])

    except Exception as e:
        print("❌ 解析文章失敗:", e)
    finally:
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
    return result

# 爬取巴哈搜尋結果
def crawl_search_results(driver, keyword, max_page=2):
    today = date.today().isoformat()
    for page_num in range(1, max_page + 1):
        print(f"=== 抓取第 {page_num} 頁 ===")
        title_links = driver.find_elements(By.CSS_SELECTOR, 'div.gs-title > a.gs-title')
        for link in title_links:
            title_text = link.text.strip()
            detail_url = link.get_attribute('href')
            if not detail_url:
                continue

            detail_data = parse_detail_page(driver, detail_url)
            if detail_data["content"] or detail_data["comments"]:
                content_score = analyze_sentiment(detail_data["content"])
                comment_score = analyze_sentiment(detail_data["comments"])
                data = {
                    "article_url": detail_data["article_url"],
                    "title": title_text,
                    "content": detail_data["content"],
                    "comments": detail_data["comments"],
                    "content_sentiment_score": content_score,
                    "comment_sentiment_score": comment_score,
                    "site": "bahamut",
                    "search_keyword": keyword,
                    "capture_date": today
                }
                save_bahamut_to_db(data)

        time.sleep(random.uniform(2, 4))  # 模擬人類

def main():
    create_bahamut_table_if_not_exist()
    driver = init_driver()
    with open('keywords.txt', 'r', encoding='utf-8') as f:
        keywords = [k.strip() for k in f.readlines() if k.strip()]
    for keyword in keywords:
        search_bahamut(driver, keyword)
        crawl_search_results(driver, keyword)
    driver.quit()

if __name__ == "__main__":
    main()
