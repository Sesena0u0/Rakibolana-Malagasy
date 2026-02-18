import requests
from bs4 import BeautifulSoup
import csv
import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin

BASE_URL = "https://www.rakibolana.org/rakibolana/alpha/"
FINAL_CSV_FILE = "rakibolana.csv"
TEMP_DIR = "temp_csvs"
MAX_WORKERS = 15
TIMEOUT = 60
TOHINY_TIMEOUT = 20

session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False
)
adapter = HTTPAdapter(
    pool_connections=MAX_WORKERS,
    pool_maxsize=MAX_WORKERS,
    max_retries=retry_strategy
)
session.mount("https://", adapter)
session.mount("http://", adapter)

tohiny_cache = {}


def clean_text(text):
    if not text:
        return ""
    text = text.replace("...", "")
    text = text.replace("â€¦", "")
    return text.strip()


def fetch_full_definitions(tohiny_href):
    full_url = urljoin(BASE_URL, tohiny_href)

    if full_url in tohiny_cache:
        return tohiny_cache[full_url]

    try:
        resp = session.get(full_url, timeout=TOHINY_TIMEOUT)
        if resp.status_code != 200:
            tohiny_cache[full_url] = []
            return []
    except requests.RequestException:
        tohiny_cache[full_url] = []
        return []

    soup = BeautifulSoup(resp.content, 'html.parser')
    main = soup.find('div', id='main') or soup

    defs = []
    for d in main.find_all('div', class_='mb-3'):
        if "comments-post" in d.get("class", []):
            continue

        for a in d.find_all('a', class_='text-danger'):
            a.decompose()
        for meta in d.find_all('div', class_='text-end'):
            meta.decompose()
        for b in d.find_all('b'):
            b.decompose()

        text = clean_text(d.get_text(" ", strip=True))
        if text:
            defs.append(text)

    tohiny_cache[full_url] = defs
    return defs


def extract_inline_definition(div_entry):
    for a in div_entry.find_all('a', class_='text-danger'):
        a.decompose()
    for meta in div_entry.find_all('div', class_='text-end'):
        meta.decompose()
    for b in div_entry.find_all('b'):
        b.decompose()
    for a in div_entry.find_all('a'):
        if "tohiny" in a.get_text(strip=True).lower():
            a.decompose()

    return clean_text(div_entry.get_text(" ", strip=True))


def get_last_page_number(letter):
    url = f"{BASE_URL}{letter}"
    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        last_page_link = soup.find('a', attrs={'aria-label': 'Farany'})
        if last_page_link:
            href = last_page_link.get('href')
            return int(href.split('page=')[-1])
        return 1
    except:
        return 0


def scrape_page_to_temp_file(task):
    letter, page_number = task
    url = f"{BASE_URL}{letter}?page={page_number}"

    try:
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
    except:
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    content_divs = soup.find_all('div', class_='mb-3')

    if not content_divs:
        return None

    page_data = []
    seen_words = set()

    for div in content_divs:
        b_tag = div.find('b')
        if not b_tag:
            continue

        a_word = b_tag.find('a')
        if not a_word:
            continue

        word = a_word.get_text(strip=True)

        if word in seen_words:
            continue

        tohiny_link = None
        for a in div.find_all('a'):
            if "tohiny" in a.get_text(strip=True).lower():
                tohiny_link = a.get('href')
                break

        if tohiny_link:
            defs = fetch_full_definitions(tohiny_link)
            if defs:
                for idx, d in enumerate(defs, 1):
                    page_data.append([letter, page_number, idx, word, d])
                seen_words.add(word)
                continue

        inline_def = extract_inline_definition(div)
        if inline_def:
            page_data.append([letter, page_number, 1, word, inline_def])
            seen_words.add(word)

    if not page_data:
        return None

    temp_filename = os.path.join(TEMP_DIR, f"temp_{letter}_{page_number}.csv")
    with open(temp_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['letter', 'page', 'index_on_page', 'word', 'definition'])
        writer.writerows(page_data)

    return temp_filename


def merge_csv_files(temp_files, final_filename):
    with open(final_filename, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['letter', 'page', 'index_on_page', 'word', 'definition'])

        for filename in temp_files:
            with open(filename, 'r', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                next(reader)
                for row in reader:
                    writer.writerow(row)


def cleanup_temp_files(temp_files):
    for filename in temp_files:
        try:
            os.remove(filename)
        except:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--letter', default='A', type=str.upper)
    parser.add_argument('--page', default=1, type=int)
    args = parser.parse_args()

    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    print("🔎 Phase 1: Discovering available pages...")
    tasks = []

    for letter in [chr(c) for c in range(ord(args.letter), ord('Z') + 1)]:
        last_page = get_last_page_number(letter)
        print(f"   Letter {letter} → {last_page} pages found")
        for page_num in range(1, last_page + 1):
            tasks.append((letter, page_num))

    print(f"\n🚀 Phase 2: Scraping {len(tasks)} pages using {MAX_WORKERS} threads...\n")

    temp_files = []
    completed = 0
    total = len(tasks)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_page_to_temp_file, task): task for task in tasks}

        for future in as_completed(futures):
            completed += 1
            result = future.result()
            if result:
                temp_files.append(result)

            percent = (completed / total) * 100
            print(f"Progress: {completed}/{total} ({percent:.2f}%)", end="\r")

    print("\n\n📦 Merging CSV files...")
    if temp_files:
        merge_csv_files(temp_files, FINAL_CSV_FILE)

    cleanup_temp_files(temp_files)

    print(f"\n✅ Finished! Data saved to {FINAL_CSV_FILE}")


if __name__ == "__main__":
    main()
