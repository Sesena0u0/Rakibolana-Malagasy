import requests
from bs4 import BeautifulSoup, Tag
import string
import csv
import time
import argparse
import os
import glob
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

BASE_URL = "https://www.rakibolana.org/rakibolana/alpha/"
FINAL_CSV_FILE = "rakibolana.csv"
TEMP_DIR = "temp_csvs"
MAX_WORKERS = 10
TIMEOUT = 90

session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False
)
adapter = HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def get_last_page_number(letter):
    """Finds the last page number for a given letter."""
    url = f"{BASE_URL}{letter}"
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        last_page_link = soup.find('a', attrs={'aria-label': 'Farany'})
        if last_page_link:
            href = last_page_link.get('href')
            return int(href.split('page=')[-1])
        else:
            pagination = soup.find('ul', class_='pagination')
            if not pagination:
                return 1
            else:
                page_links = pagination.find_all('a')
                max_page = 1
                for link in page_links:
                    href = link.get('href', '')
                    if 'page=' in href:
                        try:
                            page_num = int(href.split('page=')[-1])
                            if page_num > max_page:
                                max_page = page_num
                        except ValueError:
                            pass
                return max_page
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Could not get page count for letter {letter}: {e}")
        return 0

def scrape_page_to_temp_file(task):
    """
    Scrapes a single page and writes the result to a temporary CSV file.
    Each task is a tuple: (letter, page_number).
    """
    letter, page_number = task
    url = f"{BASE_URL}{letter}?page={page_number}"
    
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"  [ERROR] Failed to download page {page_number} for letter {letter}: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    content_divs = soup.find_all('div', class_='mb-3')

    if not content_divs:
        return None

    page_data = []
    for index, div_entry in enumerate(content_divs, 1):
        word, definition_text = "", ""
        b_tag = div_entry.find('b')
        if b_tag:
            a_tag_in_b = b_tag.find('a')
            if a_tag_in_b:
                word = a_tag_in_b.get_text(strip=True)

            definition_parts = []
            current_sibling = b_tag.next_sibling
            while current_sibling:
                if isinstance(current_sibling, str):
                    definition_parts.append(current_sibling)
                elif isinstance(current_sibling, Tag) and current_sibling.name == 'a' and 'tohiny' in current_sibling.get_text(strip=True).lower():
                    break
                current_sibling = current_sibling.next_sibling
            
            definition_text = "".join(definition_parts).strip().lstrip(':').strip()

        if word and definition_text:
            page_data.append([letter, page_number, index, word, definition_text])

    if not page_data:
        return None
        
    temp_filename = os.path.join(TEMP_DIR, f"temp_{letter}_{page_number}.csv")
    try:
        with open(temp_filename, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['letter', 'page', 'index_on_page', 'word', 'definition'])
            csv_writer.writerows(page_data)
        return temp_filename
    except IOError as e:
        print(f"  [ERROR] Could not write to temp file {temp_filename}: {e}")
        return None

def merge_csv_files(temp_files, final_filename):
    """Merges all temporary CSV files into a single final file."""
    print(f"\nMerging {len(temp_files)} temporary files into {final_filename}...")
    
    with open(final_filename, 'w', newline='', encoding='utf-8') as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(['letter', 'page', 'index_on_page', 'word', 'definition'])
        
        for i, filename in enumerate(temp_files):
            try:
                with open(filename, 'r', encoding='utf-8') as infile:
                    csv_reader = csv.reader(infile)
                    next(csv_reader)
                    for row in csv_reader:
                        csv_writer.writerow(row)
            except (IOError, StopIteration) as e:
                print(f"  [WARN] Could not process temp file {filename}: {e}")
            if (i + 1) % 100 == 0:
                print(f"  ...merged {i + 1}/{len(temp_files)} files.")
    
    print("Merging complete.")

def cleanup_temp_files(temp_files):
    """Deletes all temporary CSV files."""
    print("\nCleaning up temporary files...")
    deleted_count = 0
    for filename in temp_files:
        try:
            os.remove(filename)
            deleted_count += 1
        except OSError as e:
            print(f"  [WARN] Could not delete temp file {filename}: {e}")
    print(f"Cleanup complete. Deleted {deleted_count} files.")


def main():
    """Main function to orchestrate the scraping process."""
    parser = argparse.ArgumentParser(description="Scrape rakibolana.org with multithreading.")
    parser.add_argument('--letter', default='A', type=str.upper, help="The starting letter (A-Z).")
    parser.add_argument('--page', default=1, type=int, help="The starting page number for the given letter.")
    args = parser.parse_args()

    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    print("--- Phase 1: Discovering all pages to scrape ---")
    tasks = []
    all_letters = [chr(c) for c in range(ord(args.letter), ord('Z') + 1)]
    for letter in all_letters:
        print(f"Checking letter: {letter}...")
        last_page = get_last_page_number(letter)
        if last_page > 0:
            start_page = args.page if letter == args.letter else 1
            for page_num in range(start_page, last_page + 1):
                tasks.append((letter, page_num))
    print(f"Discovery complete. Found {len(tasks)} pages to scrape.")

    print(f"\n--- Phase 2: Scraping {len(tasks)} pages using up to {MAX_WORKERS} threads ---")
    temp_files = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {executor.submit(scrape_page_to_temp_file, task): task for task in tasks}
        
        for i, future in enumerate(as_completed(future_to_task)):
            task = future_to_task[future]
            try:
                result_filename = future.result()
                if result_filename:
                    temp_files.append(result_filename)
                
                progress = (i + 1) / len(tasks) * 100
                print(f"  Progress: {i + 1}/{len(tasks)} pages scraped ({progress:.2f}%)", end='\r')

            except Exception as exc:
                print(f"  [ERROR] Task {task} generated an exception: {exc}")
    
    print("\nScraping phase complete.")

    if temp_files:
        merge_csv_files(temp_files, FINAL_CSV_FILE)
    else:
        print("\nNo data was scraped, skipping merge.")

    all_temp_files = glob.glob(os.path.join(TEMP_DIR, "temp_*.csv"))
    if all_temp_files:
        cleanup_temp_files(all_temp_files)
        try:
            if not os.listdir(TEMP_DIR):
                os.rmdir(TEMP_DIR)
        except OSError:
            pass
    
    print(f"\nAll done! Final data saved to {FINAL_CSV_FILE}")

if __name__ == "__main__":
    main()
