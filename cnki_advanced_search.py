import asyncio
import nest_asyncio
from pyppeteer import launch

nest_asyncio.apply()

async def main():
    browser = await launch(
        headless=True,
        executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    )
    page = await browser.newPage()

    await page.goto("https://kns.cnki.net/kns", timeout=60000)


    await page.waitForSelector('#txt_search', timeout=10000)
    search_query = ''  
    await page.type('#txt_search', search_query)
    await page.click('input.search-btn')

  
    await page.waitForSelector('.result-table-list', timeout=60000)

    result_elements = await page.querySelectorAll('.result-table-list tbody tr')

    for element in result_elements:
       
        title_element = await element.querySelector('.name a')
        title_text = await (await title_element.getProperty('innerText')).jsonValue()
        link = await (await title_element.getProperty('href')).jsonValue()

       
        abstract_element = await element.querySelector('.abstract')
        abstract_in_list = await (await abstract_element.getProperty('innerText')).jsonValue() if abstract_element else ""

       
        date_element = await element.querySelector('.date')
        article_date = await (await date_element.getProperty('innerText')).jsonValue() if date_element else ""

       
        detail_page = await browser.newPage()
        await detail_page.goto(link, timeout=60000)

      
        filename = await detail_page.evaluate('document.querySelector("#paramfilename")?.value')

       
        if filename:
            pmid = filename[6:]  
        else:
            pmid = ""

     
        detail_abstract = await detail_page.evaluate('document.querySelector("#ChDivSummary")?.innerText')

  
        article_data = {
            "Note": link,
            "item": search_query,
            "ArticleTitle": title_text,
            "AbstractText": detail_abstract or abstract_in_list,
            "ArticleDate": article_date,
            "PMID": pmid  
        }

        print(article_data)
        print("-" * 60)

        await detail_page.close()

    await asyncio.sleep(2)
    await browser.close()

asyncio.run(main())
