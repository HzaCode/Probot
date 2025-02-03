import requests
import time
import warnings
import csv
from urllib3.exceptions import InsecureRequestWarning
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

warnings.simplefilter('ignore', InsecureRequestWarning)

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

def get_search(search_word, page_num=1, page_size=50):
    url =
    headers = {
        "Host": "msearch.wanfangdata.com.cn",
        "User-Agent": "okhttp/3.3.1",
        "Accept": "application/json, text/plain, */*",
        "Connection": "keep-alive",
        "Referer": "https://msearch.wanfangdata.com.cn/",
    }
    params = {
        "searchWord": search_word,
        "pageNum": page_num,
        "pageSize": page_size,
        "sort": '["相关度:desc"]',
        "resultSearch": "[]",
        "entRecFlag": "false",
        "hasFullText": "false",
        "isOAResource": "false",
        "advancedSearchFlag": "true",
    }
    try:
        resp = session.get(url, headers=headers, params=params, timeout=10, verify=False)
        if resp.status_code == 200:
            jd = resp.json()
            total_count = jd.get("totalRow", 0)
            data_list = jd.get("data", [])
            return total_count, data_list
        else:
            print(f"Request failed, status code={resp.status_code}")
            return 0, []
    except Exception as e:
        print("Request exception:", e)
        return 0, []
    finally:
        time.sleep(2)

def generate_search_queries(core_keyword, additional_keywords):
    keyword_batches = []
    for keyword in additional_keywords:
        query = f"主题:({core_keyword} AND {keyword})"
        keyword_batches.append(query)
    return keyword_batches

def main():
    core_keyword = ""
    additional_keywords = [
        
    ]

    search_queries = generate_search_queries(core_keyword, additional_keywords)

    fetched_article_ids = set()
    all_results = []

    for search_query in search_queries:
        page_size = 50
        current_page = 1
        total_count = 1

        while current_page <= total_count:
            total_count, data_list = get_search(search_query, page_num=current_page, page_size=page_size)
            if not data_list:
                break

            for paper in data_list:
                article_id = paper.get("article_id", "").strip()
                
                if article_id in fetched_article_ids:
                    continue
                
                fetched_article_ids.add(article_id)

                note = f"https://www.wanfangdata.com.cn/details/detail.do?_type=perio&id={article_id}"
                abstract_text = paper.get("summary", "").strip()
                title = paper.get("title", "").strip()
                publish_year = paper.get("publish_year", "")
                publish_year = str(publish_year) if isinstance(publish_year, int) else publish_year
                article_date = f"{publish_year}" if len(publish_year) >= 4 and publish_year.isdigit() else ""

                json_content = {
                    "Note": note,
                    "item": search_query,
                    "ArticleTitle": title,
                    "AbstractText": abstract_text,
                    "ArticleDate": article_date,
                    "json_content": paper
                }

                if article_id and note and abstract_text and article_date:
                    all_results.append(json_content)

            print(f"Page {current_page}, fetched {len(data_list)} entries, total {len(all_results)}/{total_count}")
            current_page += 1

    print(f"\nFetched {len(all_results)} records in total.")

    csv_filename = "search_results_no_duplicates.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Note", "Item", "ArticleTitle", "AbstractText", "ArticleDate", "json_content"])
        for item in all_results:
            writer.writerow([
                item["Note"],
                item["item"],
                item["ArticleTitle"],
                item["AbstractText"],
                item["ArticleDate"],
                str(item["json_content"])
            ])

    print(f"Results saved locally to {csv_filename}.")

if __name__ == "__main__":
    main()
