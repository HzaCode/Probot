import asyncio
import nest_asyncio
import requests
import logging
import random
from pyppeteer import launch
from urllib.parse import urljoin
import time

nest_asyncio.apply()

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)"
]

keywords = [


    
]

side_effects_keywords = [


    

exclude_keywords = [




    
]

query_templates = [
    'TI=("{keyword}") and (TI=("{keywords_with_side_effects}"))',
    'TI=("{keyword}") and (TI=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))',
    'KY=("{keyword}") and (KY=("{keywords_with_side_effects}"))',
    'KY=("{keyword}") and (KY=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))',
    'TKA=("{keyword}") and (TKA=("{keywords_with_side_effects}"))',
    'TKA=("{keyword}") and (TKA=("{keywords_with_side_effects}")) NOT (TI=("{exclude_keywords}"))'
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

def recognize_captcha(image_bytes):
    url = ""
    appcode = ""
    headers = {"Authorization": f"APPCODE {appcode}"}
    files = {"image": ("captcha.jpg", image_bytes, "image/jpeg")}
    
    r = requests.post(url, files=files, headers=headers)
    if r.status_code == 200:
        result_json = r.json()
        return result_json.get("captcha_text", "")
    else:
        return ""

async def handle_captcha_if_present(page):
    try:
        await page.waitForSelector('.verifycode', timeout=3000)
        logging.info("Captcha detected. Starting to solve...")

        captcha_img_el = await page.querySelector('img#changeVercode')
        if not captcha_img_el:
            logging.info("captcha_img_el not found!")
            return

        captcha_src = await page.evaluate('(el) => el.getAttribute("src")', captcha_img_el)
        full_captcha_url = urljoin(page.url, captcha_src)

        logging.info(f"Captcha image URL: {full_captcha_url}")
        resp = requests.get(full_captcha_url)
        if resp.status_code != 200:
            logging.warning("Failed to download captcha image.")
            return

        captcha_bytes = resp.content
        captcha_result = recognize_captcha(captcha_bytes)
        logging.info(f"Captcha recognized as: {captcha_result}")

        await page.type('#vericode', captcha_result)
        await page.click('#checkCodeBtn')
        await asyncio.sleep(2)

    except Exception as e:
        logging.info(f"No captcha or error in captcha handling: {e}")

async def run_crawling():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    user_agent = random.choice(USER_AGENTS)
    browser = await launch(
        headless=True, 
        executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        args=['--start-maximized']
    )
    page = await browser.newPage()
    await page.setUserAgent(user_agent)

    seen_results = set()

    for kw, queries in all_queries.items():
        logging.info(f"\n=== Keyword: {kw} ===")
        print(f"\n=== Keyword: {kw} ===")

        keyword_results = []

        for query_text in queries:
            logging.info(f"  Testing Query: {query_text}")
            print(f"  Testing Query: {query_text}")

            url = "https://chn.oversea.cnki.net/kns/AdvSearch?dbcode=CFLS&crossDbcodes=CJFQ,CDMD,CIPD,CCND,CYFD,CCJD,BDZK,CISD,SNAD,CJFN"
            await page.goto(url, timeout=0)

            await page.waitForSelector('li[name="majorSearch"]', timeout=0)
            await page.click('li[name="majorSearch"]')

            await page.waitForSelector('.textarea-major', timeout=0)
            text_area = await page.querySelector('.textarea-major')

            await text_area.click({'clickCount': 3})
            for _ in range(5):
                await text_area.press('Backspace')
            await text_area.type(query_text)

            await page.waitForSelector('.btn-search', timeout=60000)
            await page.click('.btn-search')

            await handle_captcha_if_present(page)

            try:
                await page.waitForSelector('i.icon-detail', visible=True, timeout=160000)
                await page.evaluate('document.querySelector("i.icon-detail").scrollIntoView({block: "center"})')
                await page.click('i.icon-detail')
                await asyncio.sleep(2)
            except Exception as e:
                logging.warning(f"Detail icon not found or timed out: {e}")
                print(f"Detail icon not found or timed out: {e}")
                continue

            try:
                total_results_text = await page.evaluate(
                    'document.querySelector(".pagerTitleCell em") ? document.querySelector(".pagerTitleCell em").innerText : null'
                )
                if total_results_text:
                    total_results = int(total_results_text.strip())
                    total_pages = (total_results // 20) + (1 if (total_results % 20 != 0) else 0)
                else:
                    total_pages = 1

                logging.info(f"Total Pages: {total_pages}")
                print(f"Total Pages: {total_pages}")
            except Exception as e:
                logging.error(f"Error determining total pages: {e}")
                print(f"Error determining total pages: {e}")
                total_pages = 1

            page_number = 1
            while page_number <= total_pages:
                logging.info(f"\n=== Page {page_number} ===")
                print(f"\n=== Page {page_number} ===")

                try:
                    await page.waitForSelector('.result-detail-list', timeout=160000)
                except Exception as e:
                    logging.warning(f"Error waiting for results: {e}")
                    print(f"Error waiting for results: {e}")
                    break

                result_elements = await page.querySelectorAll('.result-detail-list dd h6 a')
                if not result_elements:
                    logging.info("    No results found, moving to next page.")
                    print("    No results found, moving to next page.")
                    break

                for element in result_elements:
                    try:
                        raw_link = await page.evaluate('(el) => el.href', element)
                        title_text = await (await element.getProperty('innerText')).jsonValue()

                        filename = await page.evaluate('(link) => new URL(link).searchParams.get("FileName")', raw_link)
                        dbcode = await page.evaluate('(link) => new URL(link).searchParams.get("DbCode")', raw_link)
                        dbname = await page.evaluate('(link) => new URL(link).searchParams.get("DbName")', raw_link)

                        if filename and dbcode and dbname:
                            full_url = (
                                f"https://chn.oversea.cnki.net/KCMS/detail/detail.aspx"
                                f"?dbcode={dbcode}&dbname={dbname}&filename={filename}"
                            )
                        else:
                            full_url = raw_link

                        abstract_text = ""
                        parent_dd = await element.xpath('ancestor::dd[1]')
                        if parent_dd:
                            abstract_element = await parent_dd[0].querySelector('div.middle > p.abstract')
                            if abstract_element:
                                abstract_text = await (await abstract_element.getProperty('textContent')).jsonValue()
                                abstract_text = abstract_text.strip()
                                if abstract_text.startswith("摘要："):
                                    abstract_text = abstract_text.replace("摘要：", "").strip()

                        article_data = {
                            "Keyword": kw,
                            "Query": query_text,
                            "ArticleTitle": title_text,
                            "Link": full_url,
                            "PMID": filename,
                            "AbstractText": abstract_text,
                        }

                        if (title_text, full_url) not in seen_results:
                            seen_results.add((title_text, full_url))
                            keyword_results.append(article_data)
                            logging.info(f"Saved: {article_data}")
                            print(f"Saved: {article_data}")
                        else:
                            logging.info(f"Duplicate found, skipping: {article_data}")
                            print(f"Duplicate found, skipping: {article_data}")

                    except Exception as e:
                        logging.error(f"Error extracting article info: {e}")
                        print(f"Error extracting article info: {e}")

                page_number += 1
                if page_number <= total_pages:
                    try:
                        await page.evaluate('document.querySelector("#PageNext").scrollIntoView({block: "center"})')
                        await page.click('#PageNext')
                        await asyncio.sleep(2)
                        await handle_captcha_if_present(page)
                    except Exception as e:
                        logging.warning(f"Error navigating to next page: {e}")
                        print(f"Error navigating to next page: {e}")
                        break

        logging.info(f"Keyword [{kw}] done, results count: {len(keyword_results)}")
        print(f"Keyword [{kw}] done, results count: {len(keyword_results)}")

    logging.info(f"\n=== All done. Total unique articles: {len(seen_results)} ===")
    print(f"\n=== All done. Total unique articles: {len(seen_results)} ===")

    await browser.close()

def main():
    asyncio.run(run_crawling())

if __name__ == "__main__":
    main()
