import asyncio
import nest_asyncio
from pyppeteer import launch
import logging
import pandas as pd

nest_asyncio.apply()

logging.basicConfig(filename="results_log.txt", level=logging.INFO, format='%(asctime)s - %(message)s')

results = []

keywords = [



    
    
]

query_templates = [
    'TI=("{keyword}") and (TI=("{keywords_with_side_effects}"))',
    'TI=("{keyword}") and (TI=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))',
    'KY=("{keyword}") and (KY=("{keywords_with_side_effects}"))',
    'KY=("{keyword}") and (KY=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))',
    'TKA=("{keyword}") and (TKA=("{keywords_with_side_effects}"))',
    'TKA=("{keyword}") and (TKA=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))'
]

side_effects_keywords = [





    
]

exclude_keywords = [




    
]

def generate_queries(keyword):
    side_effects_str = " OR ".join(side_effects_keywords)
    exclude_str = " OR ".join(exclude_keywords)
    
    queries = []
    for template in query_templates:
        query = template.format(
            keyword=keyword,
            keywords_with_side_effects=side_effects_str,
            exclude_keywords=exclude_str
        )
        queries.append(query)
    return queries

all_queries = {}
for kw in keywords:
    all_queries[kw] = generate_queries(kw)

async def run_crawling():
    browser = await launch(
        executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    )
    page = await browser.newPage()
    
    seen_results = set()

    for kw, queries in all_queries.items():
        logging.info(f"\n=== Keyword: {kw} ===")
        print(f"\n=== Keyword: {kw} ===")
        
        for query_text in queries:
            logging.info(f"  Testing Query: {query_text}")
            print(f"  Testing Query: {query_text}")

            url = "https://chn.oversea.cnki.net/kns/AdvSearch?dbcode=CFLS&crossDbcodes=CJFQ,CDMD,CIPD,CCND,CYFD,CCJD,BDZK,CISD,SNAD,CJFN"
            await page.goto(url, timeout=0)

            await page.waitForSelector('li[name="majorSearch"]', timeout=60000)
            await page.click('li[name="majorSearch"]')

            await page.waitForSelector('.textarea-major', timeout=60000)
            text_area = await page.querySelector('.textarea-major')
            await text_area.click({'clickCount': 3})
            for _ in range(5):
                await text_area.press('Backspace')
            await text_area.type(query_text)

            await page.waitForSelector('.btn-search', timeout=60000)
            await page.click('.btn-search')

            page_number = 1
            while True:
                logging.info(f"\n=== Page {page_number} ===")
                print(f"\n=== Page {page_number} ===")
                try:
                    await page.waitForSelector('.result-table-list', timeout=15000)
                except:
                    logging.info("    No results or timed out. Moving to next page.")
                    print("    No results or timed out. Moving to next page.")
                    break

                result_elements = await page.querySelectorAll('.result-table-list tbody tr')
                if not result_elements:
                    logging.info("    No results found, moving to next query.")
                    print("    No results found, moving to next query.")
                    break

                for element in result_elements:
                    title_element = await element.querySelector('.name a')
                    abstract_element = await element.querySelector('.abstract')
                    date_element = await element.querySelector('.date')
                    filename_element = await element.querySelector('.fileName')
                    
                    title_text = ""
                    abstract_text = ""
                    article_date = ""
                    link = ""
                    filename = ""

                    if title_element:
                        title_text = await (await title_element.getProperty('innerText')).jsonValue()
                        link = await page.evaluate('(el) => el.href', title_element)
                        
                        
                        
                         # Extract filename, dbcode, and dbname
                        filename = await page.evaluate('(link) => new URL(link).searchParams.get("FileName")', link)
                        dbcode = await page.evaluate('(link) => new URL(link).searchParams.get("DbCode")', link)
                        dbname = await page.evaluate('(link) => new URL(link).searchParams.get("DbName")', link)

                        
                         # Construct the full URL
                        link = f"https://chn.oversea.cnki.net/KCMS/detail/detail.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename}"

                    if abstract_element:
                        abstract_text = await (await abstract_element.getProperty('innerText')).jsonValue()

                    if date_element:
                        article_date = await (await date_element.getProperty('innerText')).jsonValue()

                    detail_page = await browser.newPage()
                    await detail_page.goto(link, timeout=60000)

                    try:
                        await detail_page.waitForSelector("#ChDivSummary", timeout=10000)
                        detail_abstract = await detail_page.evaluate('document.querySelector("#ChDivSummary")?.innerText')
                    except:
                        detail_abstract = ""

                    filename = await detail_page.evaluate('document.querySelector("#paramfilename")?.value')
                    pmid = filename if filename else ""

                    article_data = {
                        "Note": link,
                        "item": query_text,
                        "ArticleTitle": title_text,
                        "AbstractText": detail_abstract or abstract_text,
                        "ArticleDate": article_date,
                        "PMID": pmid
                    }

                    if (title_text, link) not in seen_results:
                        seen_results.add((title_text, link))
                        results.append(article_data)
                        logging.info(f"    {article_data}")
                        print(f"    {title_text} - {link}")

                    await detail_page.close()

                next_button = await page.querySelector('#PageNext')
                if next_button:
                    await next_button.click()
                    page_number += 1
                    await page.waitForSelector('.result-table-list', timeout=10000)
                else:
                    break

    logging.info(f"\n=== Done. Total unique articles found: {len(seen_results)} ===")
    print(f"\n=== Done. Total unique articles found: {len(seen_results)} ===")

    df = pd.DataFrame(results)
    df.to_excel("results.xlsx", index=False)

    await browser.close()

def main():
    asyncio.run(run_crawling())

if __name__ == "__main__":
    main()
