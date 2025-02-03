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
        'Host': 'msearch.wanfangdata.com.cn',
        'User-Agent': 'okhttp/3.3.1',
        'Accept': 'application/json, text/plain, */*',
        'Connection': 'keep-alive',
        'Referer': 'https://msearch.wanfangdata.com.cn/',
    }
    params = {
        'searchWord': search_word,
        'pageNum': page_num,
        'pageSize': page_size,
        'sort': '["相关度:desc"]',
        'resultSearch': '[]',
        'entRecFlag': 'false',
        'hasFullText': 'false',
        'isOAResource': 'false',
        'advancedSearchFlag': 'true'
    }
    try:
        resp = session.get(url, headers=headers, params=params, timeout=10, verify=False)
        if resp.status_code == 200:
            jd = resp.json()
            total_count = jd.get('totalRow', 0)
            data_list = jd.get('data', [])
            return total_count, data_list
        else:
            print(f"Request failed, status code={resp.status_code}")
            return 0, []
    except Exception as e:
        print("Request exception:", e)
        return 0, []
    finally:
        time.sleep(2)

def main():
    search_query = '''
    (
      
      )
    )
    '''

    page_size = 50
    current_page = 1
    all_results = []

    while True:
        total_count, data_list = get_search(search_query, page_num=current_page, page_size=page_size)
        if not data_list:
            break

        all_results.extend(data_list)
        print(f"Page {current_page}, fetched {len(data_list)} entries, total {len(all_results)}/{total_count}")
        
        if len(all_results) >= total_count:
            break
        
        current_page += 1

    final_results = []
    for paper in all_results:
        article_id = paper.get("article id", "").strip()
        note = f"https://www.wanfangdata.com.cn/details/detail.do?_type=perio&id={article_id}"
        abstract_text = paper.get("summary", "").strip()
        original_date = paper.get("publishDate", "").strip()

        article_date = original_date if len(original_date) >= 4 and original_date[:4].isdigit() else ""

        if article_id and note and abstract_text and article_date:
            final_results.append({
                "ID": article_id,
                "Note": note,
                "AbstractText": abstract_text,
                "ArticleDate": article_date
            })

    print(f"\nFetched {len(all_results)} records, {len(final_results)} meet the conditions.")

    csv_filename = "output.csv"
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Note", "AbstractText", "ArticleDate"])
        for item in final_results:
            writer.writerow([item["ID"], item["Note"], item["AbstractText"], item["ArticleDate"]])

    print(f"Records meeting the conditions have been written to {csv_filename}.")

if __name__ == "__main__":
    main()
