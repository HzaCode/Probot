import asyncio
from pyppeteer import launch
import os
import time
import pandas as pd
from tqdm import tqdm
import logging
import nest_asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor
from PyPDF2 import PdfReader
import shutil
import hashlib
import random
import aiofiles
import contextlib
import tempfile
from shutil import disk_usage

nest_asyncio.apply()

DOWNLOAD_DIR = r""
TEMP_BASE_DIR = r""
EXCEL_PATH = r""
FAILED_EXCEL_PATH = r""
REMAINING_EXCEL_PATH = r""
LOG_FILE = "download_log.txt"

MAX_CONCURRENT_TASKS = 5
MAX_RETRIES = 2
RETRY_DELAY_MIN = 3
RETRY_DELAY_MAX = 8
SAVE_INTERVAL = 5
MIN_FILE_SIZE = 1000
PAGE_TIMEOUT = 60000
DOWNLOAD_WAIT_TIME = 20
MIN_DISK_SPACE_GB = 1

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

failed_records = []
remaining_records = []
processed_count = 0
lock = asyncio.Lock()

try:
    df = pd.read_excel(EXCEL_PATH)
    if "DOI" not in df.columns or "PMID" not in df.columns:
        logging.error("Excel file missing 'DOI' or 'PMID' column")
        sys.exit(1)
    doi_pmid_map = dict(zip(df["DOI"].astype(str), df["PMID"].astype(str)))
    logging.info("Excel file loaded successfully, DOI-PMID mapping created.")
except Exception as e:
    logging.error(f"Failed to read Excel file: {e}")
    sys.exit(1)

def signal_handler(sig, frame):
    logging.info("Interrupt signal received, saving current state...")
    save_to_excel(failed_records, FAILED_EXCEL_PATH)
    save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
    logging.info("State saved, program exiting")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def save_to_excel(records, file_path):
    if records:
        try:
            df_to_save = pd.DataFrame(records)
            df_to_save.to_excel(file_path, index=False)
            logging.info(f"Saved {len(records)} records to {file_path}")
        except Exception as e:
            logging.error(f"Failed to save Excel file {file_path}: {e}")

@contextlib.asynccontextmanager
async def browser_context():
    browser = None
    try:
        browser = await launch(
            headless=True,
            executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'],
            handleSIGINT=False,
            handleSIGTERM=False
        )
        if browser is None:
            raise ValueError("Browser launch failed, returned None")
        logging.debug("Browser launched successfully.")
        yield browser
    except Exception as e:
        logging.error(f"Browser launch failed: {e}")
        raise
    finally:
        if browser:
            try:
                await browser.close()
                logging.info("Browser closed successfully")
            except Exception as CLOSE_ERROR:
                logging.error(f"Error closing browser:{CLOSE_ERROR}")

async def download_and_rename_pdf(doi, pbar=None):
    global failed_records, remaining_records, processed_count
    logging.info(f"Starting to process DOI: {doi}")
    success = False
    retries = 0
    pmid = doi_pmid_map.get(doi, "Unknown_PMID")
    if not isinstance(pmid, str) or pmid.strip() == "":
        pmid = "Unknown_PMID"

    with tempfile.TemporaryDirectory(dir=TEMP_BASE_DIR) as temp_dir:
        while not success and retries < MAX_RETRIES:
            try:
                async with browser_context() as browser:
                    if browser is None:
                        logging.error(f"[DOI: {doi}] Browser instance is None")
                        raise ValueError("Browser instance not properly initialized")
                    page = await browser.newPage()
                    if page is None:
                        logging.error(f"[DOI: {doi}] Page creation failed, returned None")
                        raise ValueError("Page creation failed")
                    await page.setDefaultNavigationTimeout(PAGE_TIMEOUT)
                    await page._client.send('Page.setDownloadBehavior', {
                        'behavior': 'allow',
                        'downloadPath': temp_dir
                    })
                    url = f"
                    logging.debug(f"Navigating to: {url}")
                    response = await page.goto(url, {"waitUntil": "networkidle2"})
                    if response is None or not response.ok:
                        logging.error(f"[DOI: {doi}] Page load failed, status: {response.status if response else 'None'}")
                        raise ValueError("Page load failed")
                    logging.info(f"[DOI: {doi}] Page loaded successfully: {url}")

                    selectors = [
                        "div#buttons button:nth-child(2)",
                        "button.download",
                        "button.save-btn",
                        "a.download-button",
                        "a[download]",
                        "button[download]"
                    ]
                    download_button = None
                    for selector in selectors:
                        try:
                            download_button = await page.waitForSelector(selector, {"timeout": 20000})
                            if download_button:
                                logging.info(f"[DOI: {doi}] Found download button: {selector}")
                                break
                        except Exception as e:
                            logging.warning(f"[DOI: {doi}] Selector failed: {selector}, error: {e}")

                    if not download_button:
                        logging.warning(f"[DOI: {doi}] No download button found")
                    else:
                        await download_button.click()
                        logging.info(f"[DOI: {doi}] Download button clicked")

                    await asyncio.sleep(DOWNLOAD_WAIT_TIME)

                    pdf_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.pdf')]
                    if pdf_files:
                        latest_file = max(pdf_files, key=lambda f: os.path.getctime(os.path.join(temp_dir, f)))
                        file_path = os.path.join(temp_dir, latest_file)
                        if os.path.getsize(file_path) > MIN_FILE_SIZE:
                            new_name = f"PMID_{pmid}.pdf"
                            shutil.move(file_path, os.path.join(DOWNLOAD_DIR, new_name))
                            logging.info(f"[DOI: {doi}] Downloaded and renamed successfully: {new_name}")
                            success = True
                        else:
                            logging.warning(f"[DOI: {doi}] Downloaded file too small")
                    else:
                        logging.warning(f"[DOI: {doi}] No PDF file downloaded")
            except Exception as e:
                logging.error(f"[DOI: {doi}] Download failed: {e}")
                retries += 1
                if retries < MAX_RETRIES:
                    await asyncio.sleep(random.uniform(RETRY_DELAY_MIN, RETRY_DELAY_MAX))

    async with lock:
        if not success:
            logging.error(f"[DOI: {doi}] Failed after {MAX_RETRIES} attempts")
            failed_records.append({"DOI": doi, "PMID": pmid})
            
        # Clear to free memory
        
        remaining_records[:] = [r for r in remaining_records if r["DOI"] != doi]
        processed_count += 1
        if processed_count % SAVE_INTERVAL == 0:
            save_to_excel(failed_records, FAILED_EXCEL_PATH)
            save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
    if pbar:
        pbar.update(1)
    return success

async def process_all_dois():
    global failed_records, remaining_records, processed_count
    try:
        if os.path.exists(REMAINING_EXCEL_PATH):
            remaining_df = pd.read_excel(REMAINING_EXCEL_PATH)
            valid_dois = remaining_df["DOI"].drop_duplicates().tolist()
            logging.info(f"Loaded {len(valid_dois)} DOIs from {REMAINING_EXCEL_PATH}")
        else:
            valid_dois = df[df["DOI"].notna() & (df["DOI"].astype(str).str.strip() != "DOI 未找到")]["DOI"].drop_duplicates().tolist()
            remaining_records = [{"DOI": doi, "PMID": doi_pmid_map.get(doi, "Unknown_PMID")} for doi in valid_dois]
            save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
            logging.info(f"Initialized with {len(valid_dois)} DOIs")

        if not valid_dois:
            logging.warning("No DOIs to process, exiting")
            return

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        async def bounded_task(doi, pbar):
            async with semaphore:
                return await download_and_rename_pdf(doi, pbar)

        with tqdm(total=len(valid_dois), desc="Processing DOIs") as pbar:
            tasks = [bounded_task(doi, pbar) for doi in valid_dois]
            await asyncio.gather(*tasks)

        save_to_excel(failed_records, FAILED_EXCEL_PATH)
        save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
    except Exception as e:
        logging.error(f"Error processing all DOIs: {e}")
        save_to_excel(failed_records, FAILED_EXCEL_PATH)
        save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
        raise

def main():
    start_time = time.time()
    logging.info("Starting PDF download task")
    try:
        asyncio.run(process_all_dois())
    except KeyboardInterrupt:
        logging.info("Task interrupted by user")
    except Exception as e:
        logging.error(f"Main process failed: {e}")
    finally:
        save_to_excel(failed_records, FAILED_EXCEL_PATH)
        save_to_excel(remaining_records, REMAINING_EXCEL_PATH)
        logging.info(f"Task completed, elapsed time: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
