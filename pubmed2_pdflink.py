



import requests  
import pandas as pd 
from bs4 import BeautifulSoup  
import os  


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
INPUT_PATH = 
OUTPUT_DIR = 
OUTPUT_EXCEL = 

def check_url(url: str) -> dict:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return {
            'status': resp.status_code,
            'type': resp.headers.get('Content-Type', 'Unknown'),
            'size': resp.headers.get('Content-Length', 'Unknown'),
            'html': resp.text
        }
    except requests.exceptions.RequestException as e:
        return {'err': str(e), 'status': getattr(e.response, 'status_code', None), 'html': ''}

def download_pdf(pdf_url: str, save_path: str) -> None:
    try:
        if not pdf_url:
            raise ValueError("No PDF URL")
        resp = requests.get(pdf_url, headers=HEADERS, stream=True)
        resp.raise_for_status()
        if 'application/pdf' not in resp.headers.get('Content-Type', ''):
            print(f"Warning: {pdf_url} ain't PDF")
            return
        with open(save_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Done: {save_path}")
    except (requests.exceptions.RequestException, ValueError) as e:
        print(f"Failed: {pdf_url}, Err: {str(e)}")

def process_doi_data(df: pd.DataFrame, download_dir: str) -> pd.DataFrame:
    df['PDF_Avail'] = ''
    df['Checked_URL'] = ''
    df['PDF_Link'] = ''

    for index, row in df.iterrows():
        doi = str(row['DOI']).strip()
        if doi and doi.lower() != 'nan':
            url = f""
            result = check_url(url)
            df.at[index, 'Checked_URL'] = url
            
            if 'err' in result:
                df.at[index, 'PDF_Avail'] = 'No PDF'
            elif 'html' in result and result['html']:
                soup = BeautifulSoup(result['html'], 'html.parser')
                # Look for PDF link
                pdf_link = None
                embed = soup.find('embed', {'type': 'application/pdf'})
                if embed and embed.get('src'):
                    pdf_link = embed['src']
                else:
                    for a in soup.find_all('a', href=True):
                        if '.pdf' in a['href'].lower():
                            pdf_link = a['href']
                            break
                
                if pdf_link:
                    df.at[index, 'PDF_Avail'] = 'Has PDF'
                    df.at[index, 'PDF_Link'] = pdf_link
                    pmid = str(row['PMID'])
                    save_path = os.path.join(download_dir, f"{pmid}.pdf")
                    download_pdf(pdf_link, save_path)
                else:
                    df.at[index, 'PDF_Avail'] = 'No PDF'
    
    return df

def main():
    df = pd.read_excel(INPUT_PATH)
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    df = process_doi_data(df, OUTPUT_DIR)
    df.to_excel(OUTPUT_EXCEL, index=False)

if __name__ == "__main__":
    main()
