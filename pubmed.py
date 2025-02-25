import asyncio
from pyppeteer import launch
import os
import time
import pandas as pd
from tqdm import tqdm
import logging
import nest_asyncio
from PyPDF2 import PdfReader
import shutil
import hashlib

nest_asyncio.apply()

DOWNLOAD_DIR = r""
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

TEMP_BASE_DIR = r"C:\temp_downloads"
os.makedirs(TEMP_BASE_DIR, exist_ok=True)

log_file = "download_log.txt"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(log_file), logging.StreamHandler()])

df = pd.read_excel("excel_file.xlsx")

if "DOI" not in df.columns or "PMID" not in df.columns:
    logging.error("Excel file is missing 'DOI' or 'PMID' column")
    exit()

doi_pmid_map = dict(zip(df["DOI"].astype(str), df["PMID"].astype(str)))

MAX_CONCURRENT_TASKS = 5
MAX_RETRIES = 3
RETRY_DELAY = 5

def is_valid_pdf(file_path):
    try:
        with open(file_path, "rb") as f:
            PdfReader(f)
            return True
    except:
        return False

def calculate_file_hash(file_path):
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

def check_duplicate_in_target(file_path, target_dir):
    target_files = [f for f in os.listdir(target_dir) if f.lower().endswith('.pdf')]
    file_hash = calculate_file_hash(file_path)
    for target_file in target_files:
        if target_file.startswith("PMID_") and target_file.endswith(".pdf"):
            target_path = os.path.join(target_dir, target_file)
            if calculate_file_hash(target_path) == file_hash:
                return True
    return False

def check_filename_conflict(file_name, target_dir):
    target_path = os.path.join(target_dir, file_name)
    return os.path.exists(target_path)

def clean_duplicates(target_dir):
    pdf_files = [f for f in os.listdir(target_dir) if f.lower().endswith('.pdf') and f.startswith("PMID_")]
    pmid_files = {}
    duplicates = []
    for file_name in sorted(pdf_files):
        pmid_match = file_name.split("PMID_")[1].split(".pdf")[0].split("_")[0]
        if pmid_match in pmid_files:
            duplicates.append(file_name)
        else:
            pmid_files[pmid_match] = file_name
    for dup_file in duplicates:
        dup_path = os.path.join(target_dir, dup_file)
        try:
            os.remove(dup_path)
            logging.info(f"Removed duplicate file: {dup_file}")
        except:
            logging.error(f"Failed to remove duplicate file {dup_file}")
    if not duplicates:
        logging.info("No duplicate PMID files found in target directory")

async def download_and_rename_pdf(doi, pbar=None):
    success = False
    retries = 0
    while not success and retries < MAX_RETRIES:
        try:
            pmid = doi_pmid_map.get(doi, "Unknown_PMID")
            if not isinstance(pmid, str) or pmid.strip() == "":
                pmid = "Unknown_PMID"
            temp_dir = os.path.join(TEMP_BASE_DIR, f"temp_{doi.replace('/', '_')}_{time.time()}")
            os.makedirs(temp_dir, exist_ok=True)
            browser = await launch(headless=True, executablePath=r"C:\Program Files\Google\Chrome\Application\chrome.exe")
            page = await browser.newPage()
            await page._client.send('Page.setDownloadBehavior', {'behavior': 'allow', 'downloadPath': temp_dir})
            await page.goto(f"https://www.example.com/{doi}", {"waitUntil": "networkidle2"})
            logging.info(f"Page loaded: (Attempt {retries + 1}/{MAX_RETRIES})")
            selectors = ["div#buttons button:nth-child(2)", "button.download", "button.save-btn"]
            download_button = None
            for selector in selectors:
                try:
                    download_button = await page.waitForSelector(selector, {"timeout": 20000})
                    break
                except:
                    logging.warning(f"Selector {selector} failed")
            if not download_button:
                raise Exception("No valid selector found")
            await download_button.click()
            logging.info(f"Clicked download button directly (Attempt {retries + 1}/{MAX_RETRIES})")
            await asyncio.sleep(15)
            pdf_files = [f for f in os.listdir(temp_dir) if f.lower().endswith('.pdf')]
            if pdf_files:
                latest_files = sorted([os.path.join(temp_dir, f) for f in pdf_files], key=lambda x: (os.path.getctime(x), os.path.getsize(x)), reverse=True)
                latest_file = latest_files[0]
                old_name = os.path.basename(latest_file)
                file_size = os.path.getsize(latest_file)
                if file_size > 0 and is_valid_pdf(latest_file):
                    new_name = f"PMID_{pmid}.pdf"
                    new_path = os.path.join(DOWNLOAD_DIR, new_name)
                    if os.path.exists(new_path):
                        if check_duplicate_in_target(latest_file, DOWNLOAD_DIR):
                            logging.warning(f"Duplicate content found, skipping")
                            continue
                        else:
                            logging.error(f"File exists but content differs, skipping")
                            continue
                    if not check_duplicate_in_target(latest_file, DOWNLOAD_DIR):
                        shutil.move(latest_file, new_path)
                        logging.info(f"Moved and renamed file from {old_name} to {new_name}")
                        success = True
                    else:
                        logging.warning(f"Duplicate content found")
                else:
                    logging.warning(f"Invalid or empty file {old_name}")
            else:
                logging.warning(f"No new PDF file found, retrying in {RETRY_DELAY} seconds")
                retries += 1
                if retries < MAX_RETRIES:
                    await browser.close()
                    await asyncio.sleep(RETRY_DELAY)
                    continue
        except:
            logging.error(f"Download or rename failed (Attempt {retries + 1}/{MAX_RETRIES})")
            retries += 1
            if retries < MAX_RETRIES:
                if 'browser' in locals():
                    await browser.close()
                await asyncio.sleep(RETRY_DELAY)
                continue
        finally:
            if 'browser' in locals():
                logging.info(f"terminate chrome process (Attempt {retries + 1}/{MAX_RETRIES})...")
                await browser.close()
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir)
                    logging.info(f"Deleted temporary folder")
                except:
                    logging.error(f"Failed to delete temporary folder")
        if pbar and not success:
            logging.error(f"Failed after {MAX_RETRIES} attempts")
        if pbar:
            pbar.update(1)
    return success

async def process_all_dois():
    valid_dois = df[df["DOI"].notna() & (df["DOI"].astype(str).str.strip() != "DOI 未找到")]["DOI"].drop_duplicates()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    async def bounded_task(doi, pbar):
        async with semaphore:
            return await download_and_rename_pdf(doi, pbar)
    total_dois = len(valid_dois)
    with tqdm(total=total_dois, desc="Processing DOIs") as pbar:
        tasks = [bounded_task(doi.strip(), pbar) for doi in valid_dois]
        results = await asyncio.gather(*tasks)
    successful_dois = sum(1 for result in results if result)
    failed_dois = total_dois - successful_dois
    logging.info(f"Download Summary - Successful: {successful_dois}, Failed: {failed_dois}")
    clean_duplicates(DOWNLOAD_DIR)

def main():
    try:
        asyncio.run(process_all_dois())
    except:
        logging.error(f"Main process failed")

if __name__ == "__main__":
    main()
