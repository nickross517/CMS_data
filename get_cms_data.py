from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from io import StringIO
import json
import logging
import os
import re
from threading import Lock

import pandas as pd
import requests


DATA_FOLDER = './data/'
METADATA_PATH = './CMS_METATDATA.jl'
DOWNLOAD_LIST = []
SOURCE_URL = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items'
MAX_WORKERS = 3

LOG_FILE = 'cms_downloader.log'
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

metadata_lock = Lock()
os.makedirs(DATA_FOLDER, exist_ok=True)


def load_metadata():
    # Loads metadata store if available otherwise uses an empty list and creates a new store
    if os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, 'r', encoding='utf-8') as f:
            metadata = [json.loads(line) for line in f if line.strip()]
            logger.info('Metadata loaded successfully')
            return metadata
    elif not os.path.exists(METADATA_PATH):
        with open(METADATA_PATH, 'w') as f:
            logger.info('No metadata detected, creating metadata file now')
            pass
    return []

def update_metadata(filename, modified_date):
    record = {"filename": filename, "modified_date": modified_date}
    with metadata_lock:
        with open(METADATA_PATH, 'a', encoding = 'utf-8') as f:
            f.write(json.dumps(record) + '\n')
            logger.info(f'Metadata updated for {filename} on {modified_date}')


def get_last_modified(filename, metadata):
    # Uses the first read of the metadata file so lock isnt needed
    # Parses the metadata file in reverse to get the last row that matches
    for record in reversed(list(metadata)):
        if record['filename'] == filename:
            return record['modified_date']
    return None

def convert_to_snake_case(col_name):
    col_name = col_name.lower()
    # Assumes slashes and spaces should be treated the same 
    col_name = re.sub(r'[ /]', '_', col_name)
    # clean up any other unneeded strings like quotes, parenthesis, and backslashes
    col_name = re.sub(r'[()"\'\\]', '', col_name)
    return col_name


def extract_csvs():
    r = requests.get(SOURCE_URL)
    r.raise_for_status()
    response = json.loads(r.text)
    logger.info('Request successful for source URL')
    hospital_list = []
    for resp in response:
        if resp['theme'] and resp['theme'][0] == 'Hospitals':
            hospital_list.append(resp)
        else:
            pass
    return hospital_list

# compare last mod to metastore
def check_for_new_csvs(hospital_list, metadata):
    for item in hospital_list:
        filename = item['distribution'][0]['downloadURL'].split('/')[-1]
        modified = item.get('modified')
        last_modified_date = get_last_modified(filename, metadata)
        if last_modified_date is None or (modified and modified > last_modified_date):
            DOWNLOAD_LIST.append(item['distribution'][0]['downloadURL'])
    return DOWNLOAD_LIST

def download_new_csvs(download_url):
    download_resp = requests.get(download_url)
    download_resp.raise_for_status()
    logger.info(f'Request made successfully for {download_url}')
    download_text = StringIO(download_resp.text)
    # Assumes pandas reading wont change formatting of the csv materially, otherwise use native python csv reader or read all cols as str
    result = pd.read_csv(download_text)
    result.columns = [convert_to_snake_case(col) for col in result.columns]
    
    current_time = datetime.now().isoformat()
    filename = download_url.split('/')[-1]
    result['downloaded_tsp'] = current_time
    result.to_csv(f'{DATA_FOLDER}/{filename}', index=False)
    logger.info(f'CSV saved for {download_url}')
    update_metadata(filename, current_time)

def main():
    metadata = load_metadata()
    hospital_list = extract_csvs()
    csvs_to_download = check_for_new_csvs(hospital_list, metadata)
    if len(csvs_to_download) == 0:
        logger.info('No new csvs to download at the moment')
    else:
        with ThreadPoolExecutor(max_workers = MAX_WORKERS) as executor:
            executor.map(download_new_csvs, csvs_to_download)

if __name__ == '__main__':
    main()