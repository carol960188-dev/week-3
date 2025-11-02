import requests
from bs4 import BeautifulSoup
import csv
import time

PTT_URL = "https://www.ptt.cc"
BOARD = "Steam"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def fetch_page(url):
    res = requests.get(url, headers=HEADERS, cookies={'over18': '1'})
    if res.status_code != 200:
        print(f"[WARN] Fail to fetch {url}")
        return None
    return res.text

def parse_article(link):
    
    html = fetch_page(link)
    if not html:
        return None, None, None

    soup = BeautifulSoup(html, "html.parser")

    
    title_tag = soup.find("meta", property="og:title")
    title = title_tag["content"] if title_tag else ""

    
    time_tag = soup.find(string="時間")
    publish_time = ""
    if time_tag and time_tag.parent.next_sibling:
        publish_time = time_tag.parent.next_sibling.text.strip()
    else:
        
        meta = soup.find_all("span", class_="article-meta-value")
        if len(meta) >= 4:
            publish_time = meta[3].text.strip()

    
    like_count = 0
    for tag in soup.select("span.hl.push-tag"):
        if "推" in tag.text:
            like_count += 1
        elif "噓" in tag.text:
            like_count -= 1

    return title, like_count, publish_time

def crawl_ptt():
    index_url = f"{PTT_URL}/bbs/{BOARD}/index.html"
    all_articles = []

    for i in range(3):  
        print(f"[INFO] Crawling page {i+1}: {index_url}")
        html = fetch_page(index_url)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        articles = soup.select("div.title a")

        for a in articles:
            title = a.text.strip()
            link = PTT_URL + a["href"]
            title, like_count, publish_time = parse_article(link)
            all_articles.append([title, like_count, publish_time])
            time.sleep(0.5)  

        
        prev_link = soup.select_one("a.btn.wide:contains('上頁')")
        if not prev_link:
            print("[WARN] ")
            break
        index_url = PTT_URL + prev_link["href"]

    
    with open("articles.csv", "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["ArticleTitle", "LikeCount", "PublishTime"])
        writer.writerows(all_articles)

    print(f"[OK] Saved articles.csv ({len(all_articles)} rows)")

if __name__ == "__main__":
    crawl_ptt()

