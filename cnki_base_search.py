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

    await page.waitForSelector('.sort-default', timeout=10000)
    await page.click('.sort-default')
    await page.waitForSelector('.sort-list li[data-val="FT"] a', timeout=10000)
    await page.click('.sort-list li[data-val="FT"] a')

    await page.waitForSelector('#txt_search', timeout=10000)
    await page.type('#txt_search', '')

    await page.waitForSelector('input.search-btn', timeout=10000)
    await page.click('input.search-btn')

    await page.waitForSelector('.result-table-list', timeout=60000)

    result_elements = await page.querySelectorAll('.result-table-list tbody tr')

    for element in result_elements:
        title_element = await element.querySelector('.name a')
        title_text = ""
        link = ""
        if title_element:
            title_text = await (await title_element.getProperty('innerText')).jsonValue()
            link = await (await title_element.getProperty('href')).jsonValue()

        author_element = await element.querySelector('.author')
        author_text = ""
        if author_element:
            author_text = await (await author_element.getProperty('innerText')).jsonValue()

        source_element = await element.querySelector('.source')
        source_text = ""
        if source_element:
            source_text = await (await source_element.getProperty('innerText')).jsonValue()

        date_element = await element.querySelector('.date')
        date_text = ""
        if date_element:
            date_text = await (await date_element.getProperty('innerText')).jsonValue()

        download_element = await element.querySelector('.download .downloadCnt')
        download_count = ""
        if download_element:
            download_count = await (await download_element.getProperty('innerText')).jsonValue()

        print(f"Title: {title_text}")
        print(f"Link: {link}")
        print(f"Authors: {author_text}")
        print(f"Source: {source_text}")
        print(f"Publication Date: {date_text}")
        print(f"Download Count: {download_count}")
        print("-" * 50)

    await asyncio.sleep(5)

    await browser.close()

await main()
