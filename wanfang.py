#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil
import tempfile
import time
import argparse
import logging
from datetime import datetime
import pandas as pd
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions


def setup_logging():
    log_file = os.path.join(os.getcwd(), f'check_and_download_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

#  Check link validity
def check_url_with_browser_simulation(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
    }
    try:
        resp = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
        if resp.status_code != 200:
            logging.info(f"Invalid link, status code: {resp.status_code} | {url}")
            return False
        if not resp.text.strip():
            logging.info(f"Empty response: {url}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        logging.warning(f"Request error: {e} | {url}")
        return False


def setup_chrome_options(temp_dir):
    options = ChromeOptions()
    prefs = {
        "download.default_directory": temp_dir,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    return options


def check_for_404(driver):
    return ("404" in driver.title) or ("Not Found" in driver.page_source)


def download_pdf(detail_url, target_dir, driver_path=None):
    article_id = detail_url.split('/')[-1].replace('.aspx', '')
    output_filename = f"{article_id}.pdf"
    target_path = os.path.join(target_dir, output_filename)
    if os.path.exists(target_path):
        logging.info(f"File already exists, skip: {target_path}")
        return True
    temp_dir = tempfile.mkdtemp()
    options = setup_chrome_options(temp_dir)
    if driver_path:
        driver = webdriver.Chrome(executable_path=driver_path, options=options)
    else:
        driver = webdriver.Chrome(options=options)
    try:
        driver.get(detail_url)
        time.sleep(2)
        if check_for_404(driver):
            logging.warning(f"404 found, skip: {detail_url}")
            return False
        timeout = 600
        start = time.time()
        downloaded = False
        while time.time() - start < timeout:
            files = os.listdir(temp_dir)
            if files:
                temp_file = os.path.join(temp_dir, files[0])
                size1 = os.path.getsize(temp_file)
                time.sleep(1)
                size2 = os.path.getsize(temp_file)
                if size1 == size2 and size1 > 0:
                    shutil.move(temp_file, target_path)
                    logging.info(f"Downloaded: {target_path}")
                    downloaded = True
                    break
            time.sleep(1)
        if not downloaded:
            logging.error(f"Timeout downloading: {detail_url}")
            return False
        return True
    except Exception as e:
        logging.error(f"Download error: {e} | {detail_url}")
        return False
    finally:
        driver.quit()
        shutil.rmtree(temp_dir, ignore_errors=True)


def check_and_download(url, target_dir, driver_path=None):
    if not check_url_with_browser_simulation(url):
        return False
    return download_pdf(url, target_dir, driver_path=driver_path)


def get_and_save_file_list(target_dir):
    os.makedirs(target_dir, exist_ok=True)
    files = [f for f in os.listdir(target_dir) if os.path.isfile(os.path.join(target_dir, f))]
    list_file = os.path.join(target_dir, "file_list.txt")
    with open(list_file, 'w', encoding='utf-8') as f:
        for item in files:
            f.write(f"{item}\n")
    logging.info(f"File list saved: {list_file}")
    return files


def main():
    setup_logging()
    parser = argparse.ArgumentParser(description="Check link validity and download PDFs")
    parser.add_argument("--excel", default=r"", help="Excel file path (must contain 'link' column)")
    parser.add_argument("--target_dir", default=r"", help="Directory to store downloaded files")
    parser.add_argument("--threads", type=int, default=5, help="Number of download threads")
    args = parser.parse_args()

    try:
        df = pd.read_excel(args.excel)
    except Exception as e:
        logging.error(f"Failed to read Excel: {e}")
        return

    if 'link' not in df.columns:
        logging.error("Column 'link' not found")
        return

    os.makedirs(args.target_dir, exist_ok=True)
    links = []
    for i, row in df.iterrows():
        url = row.get('link')
        if pd.isna(url) or not url:
            continue
        links.append(url.strip())

    links = list(set(links))
    if not links:
        logging.warning("No valid links found.")
        return

    logging.info(f"Processing {len(links)} links...")
    futures = []
    results = []

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        with tqdm(total=len(links), desc="Processing", unit="links") as bar:
            for link in links:
                futures.append(executor.submit(check_and_download, link, args.target_dir, args.driver_path))
            for fut in as_completed(futures):
                results.append(fut.result())
                bar.update(1)

    success = sum(1 for r in results if r)
    failure = len(results) - success
    logging.info(f"Done. Total: {len(links)}, Success: {success}, Failure: {failure}")

    file_list = get_and_save_file_list(args.target_dir)
    logging.info(f"Downloaded files: {len(file_list)}")

if __name__ == "__main__":
    main()
