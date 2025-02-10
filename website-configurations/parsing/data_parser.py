import argparse
import gzip
import json
import re
import os
from utils import mark_reviewed, open_file, get_file_name
import urllib.parse
from postgres_functions import connect_to_db, setup_postgres, insert_into_db, shut_down_db
from helper_functions import (
    extractLogNumbers,
    prepare_to_parse,
    get_mapped_files,
    insert_error
)
from mhtml_parser import mhtml_parser
from facebook_parser import fbook_extract_config
from urllib.parse import urlparse
from datetime import datetime
from constants import GOOGLE_INSTALLATIONS, FACEBOOK_INSTALLATION, META_STRING, GOOGLE_STRING

LOG_FILE = os.environ.get("LOG_FILE") 
# mark websites as parsed 
REVIEWED_ITEMS_FILE = os.environ.get("REVIEWED_ITEMS_FILE")
BASE_PATH = os.environ.get("BASE_PATH")
# plaintext and hashed strings inserted by PII form
FABRICATED_DATA = os.environ.get("DATA")

### LOG FILES ###
def open_logs(path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        file_content = f.read()
        json_data = json.loads(file_content)
   
    return json_data

def process_logs(logs, cursor, cnx, website, date_string, date, vm_name):
    # log files are cumulative, so we only need to parse the last log file
    # they are numbered in order, but as a string
    highest_log, highest_log_mv3 = extractLogNumbers(logs)
    json_data = open_logs(f'{BASE_PATH}{date_string}/{vm_name}/{website}/{highest_log}')
    json_data_mv3 = open_logs(f'{BASE_PATH}{date_string}/{vm_name}/{website}/{highest_log_mv3}')
    merged = json_data_mv3 + json_data

    fileMap = {}
    results = {
        'google_installation': None,
        'meta_installation': None,
        'google_fdc': None,
        'google_fdc_ids': [],
        'meta_fdc': None,
        'meta_fdc_ids': [],
        'meta_config': None,
        'meta_config_ids':[]
    }
    
    for entry in merged:
        url = entry['url']
        filename = entry['filename']
        fileMap[filename] = url 

        google_id = identify_form_data_collection(url, GOOGLE_STRING)
        if google_id is not None:
            if google_id not in results['google_fdc_ids']:
                results['google_fdc_ids'].append(google_id)
            results['google_fdc'] = True

        meta_id, hashes = identify_form_data_collection(url, META_STRING)
        if meta_id is not None:
            if meta_id not in results['meta_fdc_ids']:
                results['meta_fdc_ids'].append(meta_id)
            results['meta_fdc'] = True
            extract_meta_query_params(url, cursor, cnx, website, date, vm_name, meta_id)

    return { 'fileMap': fileMap, 'results': results }

def extract_meta_query_params(url, cursor, cnx, website, date, vm_name, pixel_id):
    # extract individual PII keys
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    formatted_params = {}

    for key, value in query_params.items():
        if '[' in key and ']' in key:
            main_key, sub_key = key.split('[', 1)
            sub_key = sub_key.rstrip(']')

            if main_key not in formatted_params:
                formatted_params[main_key] = {}
            formatted_params[main_key][sub_key] = value[0] if len(value) == 1 else value
        else:
            formatted_params[key] = value[0] if len(value) == 1 else value

        for val in formatted_params['udff']:
            match_key = val
            match_value = formatted_params['udff'][val]
            insert_into_db(
                """INSERT INTO meta_dynamic_collection_tbl (
                website, date, vm_name, match_key, mode, match_value, pixel_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(website, date, vm_name, match_key, mode, match_value, pixel_id) DO NOTHING;
            """,
            (website, date, vm_name, match_key, 'udff', match_value, pixel_id),
            cursor,cnx)

def identify_form_data_collection(url, type):
    if type == GOOGLE_STRING and GOOGLE_STRING in url:
        if any(hash in url or hash.lower() in url for hash in FABRICATED_DATA):
            pattern = r'/(ccm|pagead)/form-data/(\d+)\?gtm'
            match = re.search(pattern, url)

            if match:
                pixelId = match.group(2)
            else:
                pixelId = 'Not Extracted'

            return pixelId
        else:
            None
        
    elif type == META_STRING and META_STRING in url:
        matched_hashes = [hash for hash in FABRICATED_DATA if hash in url or hash.lower() in url]
        if len(matched_hashes) > 0:
            # extract the pixel id
            pattern = r'id=(\d+)&'
            pattern_match = re.search(pattern, url)
            if pattern_match:
                pixelId = match.group(1)
            else:
                pixelId = 'Not Extracted'

            return pixelId, matched_hashes
        else:
            return None
    
def process_files(files, results, fileMap, cursor, extraction_date, date_string, vm_name, website_name, cnx):
    html_status = 'HTMLDownloadError'
    for file in files:
        file_split = file.split('/')
        hashed_file_name = file_split[len(file_split) - 1].split('-')[0]
        file_name = get_file_name(hashed_file_name, fileMap)
        file_path = f'{BASE_PATH}{date_string}/{vm_name}/{website_name}/{file}'

        if file_name == 'html':
            blobData = open_file(file_path)
            try:
                mhtmlResults = mhtml_parser(blobData)
                if mhtmlResults['jbkFormPresent'] == False:
                    html_status = 'FormInjectionError'
                else:
                    html_status = mhtmlResults['status']
            except: 
                html_status = 'HTMLReadError'

        if file_name == 'har':
            try:
                blobData = open_file(file_path)
            except:
                print("cant open file: ", file)
            try: 
                har_data = json.loads(blobData)
                for item in har_data["entries"]:
                    item_stringify = json.dumps(item)

                    identify_form_data_collection(item_stringify)
            except:
                print("error JSON-ifying har file")

        if any(google_str in file_name for google_str in GOOGLE_INSTALLATIONS):
            pattern = r'id=([A-Z]+-\w+)'
            match = re.search(pattern, file_name)
            if match:
                pixelId = match.group(1)
            else:
                pixelId = 'Not Extracted'
            # we leave parsing this file for future work, so for now all installations are considered FALSE
            results['google_installation'] = False

        if FACEBOOK_INSTALLATION in file_name.strip():
            # the presence of this file indicates that there is a Meta Pixel INSTALLED
            results['meta_installation'] = True
            blobData = open_file(f'{BASE_PATH}{date_string}/{vm_name}/{website_name}/{file}')
            try:
                meta_data = prepare_to_parse(blobData)
                meta_results, pixelId, matchingKeys = fbook_extract_config(meta_data, pk, cursor)
                # we parsed the file and found it had a data collection configuration
                if meta_results == True:
                    # indicate this website value is true; only requires one true pixel
                    results['meta_config'] = True
                    insert_into_db(
                       """INSERT INTO meta_static_collection_tbl (
                          website, date, vm_name, key_list, pixel_id
                          ) VALUES (%s, %s, %s, %s, %s)
                        """,
                         (website_name, extraction_date, vm_name, matchingKeys, pixelId),
                         cursor,
                         cnx
                        )
                    if pixelId not in results['meta_config_ids']:
                        results['meta_config_ids'].append(pixelId)
                # don't override a previous true pixel with a now false one
                elif results['meta_config'] != True:
                    results['meta_config'] = False
            except Exception as e:
                insert_error(website_name, date_string, vm_name, 'FACEBOOK FILE ERROR')

    pk = f'{date_string}-{vm_name}-{website_name}'
    insert_into_db(
        """INSERT INTO website_visits_tbl (
                pk, website, date, vm_name, 
                meta_install, meta_config, meta_fdc, 
                meta_config_ids, meta_fdc_ids, 
                google_install, google_fdc, google_fdc_ids,
                html_status, file_count, error_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (website, date, vm_name) DO UPDATE SET
                meta_config = excluded.meta_config, meta_collect = excluded.meta_collect,
                google_config = excluded.google_config, google_collect = excluded.google_collect,
                html_status = excluded.html_status, file_count = excluded.file_count, error_type = excluded.error_type
            """,
        (
            pk, website_name, extraction_date, vm_name,
            results['meta_installation'], results['meta_config'], results['meta_fdc'],
            results['meta_config_ids'], results['meta_fdc_ids'],
            results['google_installation'], results['google_fdc'], results['google_fdc_ids'],
            html_status, len(files), 'NO ERROR'
        ),
        cursor,
        cnx
    )

def process_broken_dirs(broken_dirs, cursor, cnx):
    for dir in broken_dirs:
        pk = f'{dir['date']}-{dir['vm_name']}-{dir['website']}'
        insert_into_db(
         """INSERT INTO website_visits_tbl (
                pk, website, date, vm_name, 
                meta_install, meta_config, meta_fdc, 
                meta_config_ids, meta_fdc_ids, 
                google_install, google_fdc, google_fdc_ids,
                html_status, file_count, error_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (website, date, vm_name) DO UPDATE SET
                meta_config = excluded.meta_config, meta_collect = excluded.meta_collect,
                google_config = excluded.google_config, google_collect = excluded.google_collect,
                html_status = excluded.html_status, file_count = excluded.file_count, error_type = excluded.error_type
            """,
        (
            pk, dir['website'], dir['date'], dir['vm_name'],
            False, False, False
            False, False,
            False, False, False,
            'NO STATUS', 0, dir['reason']
        ),
        cursor,
        cnx
    )

def main():
    '''
    The script can run in the following ways:
    1. Path: this will parse all files in a specified path (with any level of specificity)
    2. Query: this will parse all matching paths in postgres database

    Each one will assume redo (in the case it has already been parsed) unless specified
    '''

    # SETUP ARGS
    parser = argparse.ArgumentParser(description='Parse files')
    parser.add_argument('TYPE', choices=['query', 'path'], help='Select websites to parse by path or by querying the database (second option implies it is a re-parse)')
    parser.add_argument('LOCATION', help='Either a db query (including presets) or path')
    parser.add_argument('REDO', action="store_true", help='If a website should be re-parsed (default to true)')

    args = parser.parse_args()

    # SETUP DB
    cursor, cnx = connect_to_db()
    setup_postgres(cursor)

    # should re-parse websites already parsed
    should_review = args.REDO

    # websites to parse
    path_list = []
    if args.TYPE == 'path':
        path_list = args.LOCATION
    elif args.TYPE == 'query':
       path_list = []
       if args.LOCATION == 'all-dynamic-positive-facebook':
          query=f"SELECT date, vm_name, website from website_visits where meta_dynamic_collection_status='true'"
       if args.LOCATION == 'all-dynamic-positive-facebook-on-date':
          query=f"SELECT date, vm_name, website from website_visits where meta_dynamic_collection_status='true' and date='{args.date}'"
       if args.LOCATION == 'all-false-negative-facebook':
          query=f"SELECT date, vm_name, website from website_visits where meta_static_collection_status IS NULL and meta_dynamic_collection_status='true'"
       if args.LOCATION == 'all-false-negative-google':
          query=f"SELECT date, vm_name, website from website_visits where google_static_collection_status='false' and google_dynamic_collection_status='true'"
       else:
           query = args.LOCATION
       
       cursor.execute(query)
       query_results = cursor.fetchall()

       for row in query_results:
            date_str = row[2].strftime('%-m-%-d-%Y')
            row_str = date_str + '/' + row[3] + '/' + row[1]
            path_list.append(row_str)
       
    mapped_files, broken_dirs = get_mapped_files(path_list)
    process_broken_dirs(broken_dirs)

    directories = mapped_files.keys()
    for directory in directories:
        date_string = mapped_files[directory]['date']
        if should_review == False or f'{date_string}/{directory}' not in path_list:
            # extract string variables
            website_name = mapped_files[directory]['website_name']
            extraction_date = datetime.strptime(date_string, '%m-%d-%Y').date()
            vm_name = mapped_files[directory]['vm_name']
            log_list = mapped_files[directory][date_string][vm_name]['logs']

            print("processing website ", directory, " on vm ", vm_name, " and date ", extraction_date)

            # PART 1: process log files, to identify network events
            logObject = process_logs(log_list, cursor, cnx, website_name, date_string, extraction_date, vm_name)

            # PART 2: process files, to identify Meta config and HTML errors
            file_list = mapped_files[directory][date_string][vm_name]['files']

            results = logObject['results']
            fileMap = logObject['fileMap']

            process_files(file_list, results, fileMap, cursor, extraction_date, date_string, vm_name, website_name, cnx)
            
            # if this isn't part of a redo, mark a new website as parsed
            if should_review != True:
                mark_reviewed(f"{extraction_date}/{vm_name}/{website_name}")

if __name__ == "__main__":
    main()
