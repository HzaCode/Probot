import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import shutil
import tempfile
from PyPDF2 import PdfReader
import pandas as pd
from tqdm import tqdm
import cv2
import numpy as np
from PIL import Image
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log.txt"),
        logging.StreamHandler()
    ]
)

def verify_pdf(file_path):
    try:
        with open(file_path, "rb") as f:
            if f.read(4) != b'%PDF':
                print(f"[{time.strftime('%H:%M:%S')}] Error: Not a valid PDF")
                logging.error(f"Not a valid PDF: {file_path}")
                return False
            pdf = PdfReader(file_path, strict=False)
            print(f"[{time.strftime('%H:%M:%S')}] PDF verified, pages: {len(pdf.pages)}")
            logging.info(f"PDF verified, pages: {len(pdf.pages)} for {file_path}")
            return True
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Error verifying PDF: {str(e)}")
        logging.error(f"Error verifying PDF: {str(e)} for {file_path}")
        return False

def is_green_button_present(driver, threshold=0.8):
    logging.info("Starting green button detection")
    try:
        screenshot_path = "screenshot.png"
        max_scrolls = 5
        scroll_increment = 500
        total_height = driver.execute_script("return document.body.scrollHeight")
        window_height = driver.execute_script("return window.innerHeight")
        logging.debug(f"Page total height: {total_height}, Window height: {window_height}")
        for i in range(max_scrolls):
            scroll_position = i * scroll_increment
            driver.execute_script(f"window.scrollTo(0, {scroll_position});")
            logging.debug(f"Scrolled to position: {scroll_position} ({i+1}/{max_scrolls})")
            time.sleep(1)
            driver.save_screenshot(screenshot_path)
            logging.debug(f"Screenshot saved: {screenshot_path} at position {scroll_position}")
            img = cv2.imread(screenshot_path)
            hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
            lower_green = np.array([35, 50, 50])
            upper_green = np.array([85, 255, 255])
            mask = cv2.inRange(hsv, lower_green, upper_green)
            contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            for contour in contours:
                area = cv2.contourArea(contour)
                if area > 10:
                    logging.info(f"Green button detected, area: {area} at position {scroll_position}")
                    return True
            if scroll_position + window_height >= total_height:
                logging.debug(f"Reached page bottom: {scroll_position} + {window_height} >= {total_height}")
                break
        logging.info("No green button detected after all scroll attempts")
        return False
    except Exception as e:
        logging.error(f"Error detecting green button: {str(e)}")
        return False
    finally:
        if os.path.exists(screenshot_path):
            os.remove(screenshot_path)
            logging.debug("Screenshot file cleaned up")

def check_existing_file(target_dir, output_filename):
    target_path = os.path.join(target_dir, output_filename)
    return os.path.exists(target_path)

def download_pdf(detail_url, target_dir="G:\\cnki"):
    article_id = detail_url.split('filename=')[-1]
    output_filename = f"{article_id}.pdf"
    
    if check_existing_file(target_dir, output_filename):
        print(f"[{time.strftime('%H:%M:%S')}] File {output_filename} already exists, skipping")
        logging.info(f"File {output_filename} already exists, skipping {detail_url}")
        return True

    temp_dir = tempfile.mkdtemp()

    firefox_options = Options()
    firefox_options.set_preference("browser.download.folderList", 2)
    firefox_options.set_preference("browser.download.dir", temp_dir)
    firefox_options.set_preference("browser.download.useDownloadDir", True)
    firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/pdf")
    firefox_options.set_preference("pdfjs.disabled", True)
    firefox_options.binary_location = "C:\\Program Files\\Mozilla Firefox\\firefox.exe"

    try:
        driver = webdriver.Firefox(options=firefox_options)
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Failed to start Firefox: {str(e)}")
        logging.error(f"Failed to start Firefox: {str(e)}")
        return False

    try:
        logging.info(f"Starting download: {detail_url}")
        print(f"[{time.strftime('%H:%M:%S')}] Starting download for {detail_url}")
        driver.get(detail_url)

        page_source = driver.page_source
        if "Very sorry, the resource you are looking for was not found" in page_source or "Resource not found" in page_source:
            print(f"[{time.strftime('%H:%M:%S')}] Resource not found, skipping")
            logging.info(f"Resource not found for {detail_url}, skipping")
            return False

        if not is_green_button_present(driver):
            print(f"[{time.strftime('%H:%M:%S')}] Failed to detect green button, skipping")
            logging.warning(f"Failed to detect green button for {detail_url}")
            return False

        try:
            download_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CLASS_NAME, "btn-download"))
            )
            print(f"[{time.strftime('%H:%M:%S')}] Detected download button")
            logging.info(f"Detected download button for {detail_url}")
            ActionChains(driver).move_to_element(download_button).perform()
            print(f"[{time.strftime('%H:%M:%S')}] Hovered over download button")
            logging.info(f"Hovered over download button for {detail_url}")
            time.sleep(0.5)
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Failed to hover over download button: {str(e)}")
            logging.error(f"Failed to hover over download button: {str(e)} for {detail_url}")
            driver.save_screenshot("hover_error.png")
            return False

        try:
            pdf_option = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, "pdfDown"))
            )
            pdf_option.click()
            print(f"[{time.strftime('%H:%M:%S')}] Clicked PDF download button")
            logging.info(f"Clicked PDF download button for {detail_url}")
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Failed to click PDF download: {str(e)}")
            logging.error(f"Failed to click PDF download: {str(e)} for {detail_url}")
            driver.save_screenshot("click_error.png")
            return False

        timeout = 60
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(temp_dir) if f.endswith(('.pdf', '.pdf.part'))]
            if files:
                filepath = os.path.join(temp_dir, files[0])
                try:
                    size1 = os.path.getsize(filepath)
                    time.sleep(0.5)
                    size2 = os.path.getsize(filepath)
                    if size1 == size2 and size1 > 0:
                        final_filepath = filepath
                        if filepath.endswith('.pdf.part'):
                            final_filepath = filepath.replace('.pdf.part', '.pdf')
                            extra_wait = 5
                            wait_start = time.time()
                            while time.time() - wait_start < extra_wait:
                                if os.path.exists(final_filepath) and os.path.getsize(final_filepath) > 0:
                                    filepath = final_filepath
                                    break
                                time.sleep(0.5)
                            else:
                                print(f"[{time.strftime('%H:%M:%S')}] .pdf.part did not convert to .pdf, timeout")
                                logging.warning(f".pdf.part did not convert to .pdf, timeout for {detail_url}")
                                return False
                        if verify_pdf(filepath):
                            target_path = os.path.join(target_dir, output_filename)
                            shutil.move(filepath, target_path)
                            print(f"[{time.strftime('%H:%M:%S')}] File moved to: {target_path}")
                            logging.info(f"File moved to: {target_path}")
                            return True
                        else:
                            return False
                except OSError as e:
                    print(f"[{time.strftime('%H:%M:%S')}] Error accessing file: {str(e)}")
                    logging.error(f"Error accessing file: {str(e)} for {detail_url}")
                    time.sleep(0.5)
                    continue
            time.sleep(0.5)
        print(f"[{time.strftime('%H:%M:%S')}] Download timed out")
        logging.warning(f"Download timed out for {detail_url}")
        return False

    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Error: {str(e)}")
        logging.error(f"Error: {str(e)} for {detail_url}")
        return False
    finally:
        driver.quit()
        shutil.rmtree(temp_dir, ignore_errors=True)
        if os.path.exists("hover_error.png"):
            os.remove("hover_error.png")
        if os.path.exists("click_error.png"):
            os.remove("click_error.png")

def main():
    excel_path = ""
    try:
        logging.info("Start reading Excel file")
        df = pd.read_excel(excel_path)
        if 'article_link' not in df.columns:
            raise ValueError("Excel file does not contain 'article_link' column")
        links = df['article_link'].dropna().tolist()
        logging.info(f"Successfully read {len(links)} links")
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] Failed to read Excel file: {str(e)}")
        logging.error(f"Failed to read Excel file: {str(e)}")
        return

    target_dir = ""
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        logging.info(f"Created target directory: {target_dir}")

    print(f"Found {len(links)} links, starting download...")
    logging.info(f"Found {len(links)} links, starting download")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(download_pdf, link, target_dir) for link in links]
        for future in tqdm(as_completed(futures), total=len(links), desc="Download progress", unit="file"):
            pass

if __name__ == "__main__":
    main()
