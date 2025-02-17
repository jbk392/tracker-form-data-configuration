import argparse
import gzip
import json
import re
import os
from postgres_functions import connect_to_db, setup_postgres, insert_into_db, get_domain_id, shut_down_db
from helper_functions import (
    extractLogNumbers,
    prepare_to_parse,
    get_mapped_files
)
from constants import GOOGLE_FILE_NAMES, GOOGLE_HASHED_VALUES, FACEBOOK_HASHED_VALUES, FACEBOOK_INSTALLATION
from mhtml_parser import mhtml_parser
from google_parser import gtag_extract_vtp_properties
from facebook_parser import fbook_extract_config
from urllib.parse import urlparse
from datetime import datetime
import urllib.parse

def logMissingFiles(files, website, date, vm_name, cursor, cnx):
    for file in files:
        insert_into_db(
        """INSERT INTO errors (
            website, date, vm_name, file_name, error_type, error
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
        (website, date, vm_name, file, 'collection error - missing files', 'Network.responseReceived Not Found'),
        cursor,
        cnx
    )

def logError(file, cursor, cnx, website, date, vm_name, error, errorType):
    insert_into_db(
        f"""INSERT INTO errors (
            website, date, vm_name, file_name, error_type, error
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
        (website, date, vm_name, file, errorType, error),
        cursor,
        cnx
    )

def logRetry(file, cursor, cnx, website, date, vm_name, reason):
    insert_into_db(
        f"""INSERT INTO errors (
            website, date, vm_name, file_name, error_type, error
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
        (website, date, vm_name, file, 'retry', reason),
        cursor,
        cnx
    )

def openLogs(log_name, path):
    json_data = []
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            file_content = f.read()
            json_data = json.loads(file_content)
    except:
        try:
            with open(path, 'r') as f:
                file_content = f.read()
                json_data = json.loads(file_content)
        except:
           print("cant open file: ", log_name)
    
    return json_data

def processLogs(logs, cursor, cnx, website, date_string, date, vm_name):
    # log files are cumulative, so we only need to parse the last log file
    # they are numbered in order, but as a string
    highest_log, highest_log_mv3 = extractLogNumbers(logs)

    json_data = openLogs(highest_log, f'{date_string}/{vm_name}/{website}/{highest_log}')
    json_data_mv3 = openLogs(highest_log_mv3, f'{date_string}/{vm_name}/{website}/{highest_log_mv3}')
    merged = json_data_mv3 + json_data

    fileMap = {}
    results = {
        'GOOGLE': {
            'DYNAMIC STATUS': None,
            'STATIC STATUS': None,
            'DYNAMIC PIXEL LIST': [],
            'STATIC PIXEL LIST': [],
            'GTM_PRESENT': False,
            'GTM': {
              'STATIC STATUS': None,
              'STATIC PIXEL LIST': []
          }
        },
        'META': {
            'DYNAMIC STATUS': None,
            'STATIC STATUS': None,
            'MATCHING KEYS': None,
            'DYNAMIC PIXEL LIST': [],
            'STATIC PIXEL LIST': []
        },
        'DOMAINS': []
    }

    requestedUrls = []
    receivedUrls = []

    # there are several errors we can catch from the logs
    # 2) the form submission was unsuccessful
    # 3) the html download function was never triggered
    # 4) errors uploading to the gcloud endpoint
    successfulInjection = None
    htmlDownloadError = None

    for entry in merged:
        url = entry['url']
        filename = entry['filename']
        fileMap[filename] = url   
        requestType = entry['requestType']
        if url == 'form-submitted':
            successfulInjection = True

        if url == 'failed-to-inject-form':
            successfulInjection = False

        if url == 'htmlDownload':
            htmlDownloadError = False


        if requestType == 'Network.requestWillBeSent':
            requestedUrls.append(url)
        elif requestType == 'Network.responseReceived':
            receivedUrls.append(url)

        ''' Known facebook urls:
        1) https://www.facebook.com/privacy_sandbox/pixel/register/trigger
	    2) https://www.facebook.com/tr/
        '''
        #if any(hash in url or hash.lower() in url for hash in FACEBOOK_HASHED_VALUES) and 'facebook' in url:
        matched_hashes = [hash for hash in FACEBOOK_HASHED_VALUES if hash in url or hash.lower() in url and 'facebook' in url]
        if len(matched_hashes) > 0:
            parse_query_params(url, cursor, cnx, website, date, vm_name, matched_hashes)

            pattern = r'id=(\d+)&'
            match = re.search(pattern, url)
            if match:
                pixelId = match.group(1)
            else:
                pixelId = 'Not Extracted'
            if pixelId not in results['META']['DYNAMIC PIXEL LIST']:
                results['META']['DYNAMIC PIXEL LIST'].append(pixelId)
            results['META']['DYNAMIC STATUS'] = True
          

        ''' Known google urls:
        1) https://www.googleadservices.com/pagead/conversion
        2) https://google.com/ccm/form-data/
        3) https://analytics.google.com/g/collect?v=2&tid=G-
        4) https://google.com/pagead/form-data/
        '''
        if any(hash in url or hash.lower() in url for hash in GOOGLE_HASHED_VALUES) and 'google' in url:
            pattern = r'/(ccm|pagead)/form-data/(\d+)\?gtm'
            match = re.search(pattern, url)
          
            if match:
                pixelId = match.group(2)
            else:
                pixelId = 'Not Extracted'
            if pixelId not in results['GOOGLE']['DYNAMIC PIXEL LIST']:
                results['GOOGLE']['DYNAMIC PIXEL LIST'].append(pixelId)
            results['GOOGLE']['DYNAMIC STATUS'] = True
        
        # I suspect this means our form was misplaced
        elif 'tv.1' in url:
            logRetry(url, cursor, cnx, website, date, vm_name, 'TV.1 ERROR')
        elif 'gtm.js' in url:
            results['GOOGLE']['GTM_PRESENT'] = True
        try:
           domain = urlparse(url).netloc
           domain_id = get_domain_id(cursor, cnx, domain)
           results['DOMAINS'].append(domain_id)
        except:
           results["DOMAINS"].append(url)

    requested_never_received = [item for item in requestedUrls if item not in receivedUrls]
    logMissingFiles(requested_never_received, website, date, vm_name, cursor, cnx)
    
    # don't stop processing on these, because it is possible the logging is the error
    if successfulInjection == False:
        results['FORM INJECTION STATUS'] = 'form injection failed'
        logRetry('global error', cursor, cnx, website, date, vm_name, "form injection failed")
    
    if successfulInjection == None:
        results['FORM INJECTION STATUS'] = 'form injection never attempted'
        logRetry('global error', cursor, cnx, website, date, vm_name, "form injection never attempted")

    if htmlDownloadError == True:
        logRetry('html error', cursor, cnx, website, date, vm_name, "html never downloaded")

    return { 'fileMap': fileMap, 'results': results }


# parse query parameters in meta network events
def parse_query_params(url, cursor, cnx, website, date, vm_name, matches):
	pattern = r'id=(\d+)&'
	match = re.search(pattern, url)
	if match:
		pixelId = match.group(1)
	else:
		pixelId = 'Not Extracted'

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

	for key, value in formatted_params.items():
		if isinstance(value, dict):
			match_mode = key
			for sub_key, sub_value in value.items():
				match_key = sub_key
				if match_key != 'rqm':
					insert_into_db("""INSERT INTO meta_match_keys (
                        website, date, vm_name, match_key, mode, pixel_id) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (website, date, vm_name, match_key, match_mode, pixelId),
                        cursor,cnx)
		else:
			match_mode = None
			match_key = key
        
		if match_key != 'rqm':
			insert_into_db("""INSERT INTO meta_match_keys (
			website, date, vm_name, match_key, mode, pixel_id) VALUES (%s, %s, %s, %s, %s, %s)""",
			(website, date, vm_name, match_key, match_mode, pixelId),
			cursor,cnx)


def getFileName(file, fileMap, cursor, cnx, website, date, vm_name):
    root, ext = os.path.splitext(file)
    try:
        file_name = fileMap[root]
    except:
        try:
            new_file = root+'-mv3'
            file_name = fileMap[new_file]
        except:
            file_name = root
            logError(file, cursor, cnx, website, date, vm_name, 'COULD NOT DECODE FILENAME', 'parsing_errors')
    return file_name

def analyze_tracking_status(website_name, extraction_date, vm_name, results, file_count, cursor, cnx):
    # there are three things we check for
    # 1) did the HTML download? (html_status)
    # 2) (if yes to 1) did our form get injected? 
    # 3) (if yes to 1) does the HTML indicate the website visit was unsuccessful?
    injected_form_present = None
    visit_status = 'success'
    if 'HTML' in results:
        if 'jbkFormPresent' in results['HTML']:
            injected_form_present = results['HTML']['jbkFormPresent']
        else:
            injected_form_present = False
        if 'status' in results['HTML']:
            visit_status = results['HTML']['status']
        else:
            visit_status = None

        if visit_status != 'success':
            html_status = 'HTMLDownloadSuccess'
            logError('html', cursor, cnx, website_name, extraction_date, vm_name, html_status, 'collection_errors')
    else:
        html_status = 'HTMLDownloadError'

    if file_count <= 10 and visit_status == 'success':
        visit_status = 'LOW FILE COUNT - SUSPECTED UNREACHABLE'
        logError('html', cursor, cnx, website_name, extraction_date, vm_name, 'HTML FILE MISSING', 'collection_errors')
    elif 'FORM INJECTION STATUS' in results and visit_status == 'success':
        visit_status = results['FORM INJECTION STATUS']

    if results['META']['STATIC STATUS'] == False and results['GOOGLE']['STATIC STATUS'] == False:
        classification = 'websitesWithoutGoogleOrMetaPixels'
    elif results['META']['DYNAMIC STATUS'] == True or results['GOOGLE']['DYNAMIC STATUS'] == True:
        classification = 'websitesWithAutoTracking'
    elif results['META']['STATIC STATUS'] == True or results['GOOGLE']['DYNAMIC STATUS'] == True:
        classification = 'websitesWithPixelsButNoAutoTracking'
    else:
        classification = 'unclassified'

    website_pk = f'{website_name}_{extraction_date}_{vm_name}'[:255]
   
    unique_domains = set(results['DOMAINS'])
    domain_string = ','.join(map(str, unique_domains))
    insert_into_db(
        """INSERT INTO website_visits (
            pk, website, date, vm_name, 
            meta_static_collection_status, meta_static_ids, meta_dynamic_collection_status, meta_dynamic_ids,
            google_static_collection_status, google_static_ids, google_dynamic_collection_status, google_dynamic_ids, 
            gtm_present, gtm_static_collection_status, gtm_static_ids,
            website_classification,
            injected_form_present, file_count, 
            html_status, visit_status, domains_list
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk) DO UPDATE SET 
                meta_static_collection_status = excluded.meta_static_collection_status,
                meta_dynamic_collection_status = excluded.meta_dynamic_collection_status,
                meta_static_ids = excluded.meta_static_ids,
                meta_dynamic_ids = excluded.meta_dynamic_ids,
                google_static_collection_status = excluded.google_static_collection_status,
                google_dynamic_collection_status = excluded.google_dynamic_collection_status,
                gtm_static_collection_status = excluded.gtm_static_collection_status,
                gtm_static_ids = excluded.gtm_static_ids,
                google_static_ids = excluded.google_static_ids,
                google_dynamic_ids = excluded.google_dynamic_ids,
                gtm_present = excluded.gtm_present,
                website_classification = excluded.website_classification,
                injected_form_present = excluded.injected_form_present,
                file_count = excluded.file_count,
                html_status = excluded.html_status,
                visit_status = excluded.visit_status,
                domains_list = excluded.domains_list
            """,
        (
            website_pk, website_name, extraction_date, vm_name,
            results['META']['STATIC STATUS'] ,  ','.join(results['META']['STATIC PIXEL LIST']), 
            results['META']['DYNAMIC STATUS'], ','.join(results['META']['DYNAMIC PIXEL LIST']),
            results['GOOGLE']['STATIC STATUS'], ','.join(results['GOOGLE']['STATIC PIXEL LIST']),
            results['GOOGLE']['DYNAMIC STATUS'], ','.join(results['GOOGLE']['DYNAMIC PIXEL LIST']),
            results['GOOGLE']['GTM_PRESENT'], results['GOOGLE']['GTM']['STATIC STATUS'], ','.join(results['GOOGLE']['GTM']['STATIC PIXEL LIST']),
            classification,
            injected_form_present, file_count, html_status, visit_status, domain_string
            ),
        cursor,
        cnx
    )

# in production each file was zipped, but this function allows for either case
def openFile(file):
    try:
        with gzip.open(file, 'rt', encoding='utf-8') as f:
           return f.read()
    except:
        try:
           with open(file, 'r') as f:
              return f.read()
        except:
           print("cant open file: ", file)

def processFiles(files, results, fileMap, cursor, extraction_date, date_string, vm_name, website_name, cnx):
    processErrors = []

    for file in files:
        file_split = file.split('/')
        hashed_file_name = file_split[len(file_split) - 1].split('-')[0]
        file_name = getFileName(hashed_file_name, fileMap, cursor, cnx, website_name, extraction_date, vm_name)

        pk = f'{date_string}-{vm_name}-{website_name}-{file_name}'
        if file_name == 'har':
            try:
                blobData = openFile(f'{date_string}/{vm_name}/{website_name}/{file}')
            except:
                print("cant open file: ", file)
            try: 
                har_data = json.loads(blobData)
                for item in har_data["entries"]:
                    item_stringify = json.dumps(item)

                    if any(hash in item_stringify or hash.lower() in item_stringify for hash in FACEBOOK_HASHED_VALUES) and 'facebook' in item_stringify:
                        results['META']['DYNAMIC STATUS'] = True

                    if any(hash in item_stringify or hash.lower() in item_stringify for hash in GOOGLE_HASHED_VALUES) and 'google' in item_stringify:
                        results['GOOGLE']['DYNAMIC STATUS'] = True
            except:
                print("error JSON-ifying har file")

        if file_name == 'html':
            blobData = openFile(f'{date_string}/{vm_name}/{website_name}/{file}')
            try:
                mhtmlResults = mhtml_parser(blobData)
                if mhtmlResults['status'] == 'CLOUDFLARE_CHALLENGE':
                    logRetry(f'{date_string}/{vm_name}/{website_name}/{file}', cursor, cnx, website_name, extraction_date, vm_name, 'CLOUDFLARE CHALLENGE')
                elif mhtmlResults['status'] == 'ERROR_403_404':
                    logRetry(f'{date_string}/{vm_name}/{website_name}/{file}', cursor, cnx, website_name, extraction_date, vm_name, '403 OR 404 NOT FOUND')
                results['HTML'] = mhtmlResults
            except:
                results['HTML'] = 'ERROR READING HTML FILE'
        if FACEBOOK_INSTALLATION in file_name.strip():
            blobData = openFile(f'{date_string}/{vm_name}/{website_name}/{file}')

            try:
                meta_data = prepare_to_parse(blobData)
                meta_results, pixelId, matchingKeys = fbook_extract_config(meta_data, pk, cursor)
                if meta_results == True:
                    # indicate this website value is true; only requires one true pixel
                    results['META']['STATIC STATUS'] = True
                    results['META']['MATCHING KEYS'] = matchingKeys
                    insert_into_db(
                       """INSERT INTO meta_static_keys (
                          pixel_id, website, date, vm_name, key_list) VALUES (%s, %s, %s, %s, %s)
                        """,
                        (pixelId, website_name, extraction_date, vm_name, matchingKeys),
                        cursor,
                        cnx
                    )
                    if pixelId not in results['META']['STATIC PIXEL LIST']:
                        results['META']['STATIC PIXEL LIST'].append(pixelId)
                elif results['META']['STATIC STATUS'] != True:
                    results['META']['STATIC STATUS'] = False
            except Exception as e:
                processErrors.append({'file': file_name, 'error': str(e)})
        if any(google_str in file_name for google_str in GOOGLE_FILE_NAMES):
            pattern = r'id=([A-Z]+-\w+)'
            match = re.search(pattern, file_name)
            if match:
                pixelId = match.group(1)
            else:
                pixelId = 'Not Extracted'
            blobData = openFile(f'{date_string}/{vm_name}/{website_name}/{file}')
            try:
                google_data = prepare_to_parse(blobData)
                google_results, gtm_results, css_values = gtag_extract_vtp_properties(google_data, cursor, pk)
                if len(gtm_results) and 'vtp_enableConversionLinker' in gtm_results:
                    results['GOOGLE']['GTM']['STATIC STATUS'] = True
                    results['GOOGLE']['GTM']['STATIC PIXEL LIST'] = gtm_results['vtp_conversionId']
                if google_results == True:
                    results['GOOGLE']['STATIC STATUS'] = True
                    if pixelId not in results['GOOGLE']['STATIC PIXEL LIST']:
                        results['GOOGLE']['STATIC PIXEL LIST'].append(pixelId)
                elif results['GOOGLE']['STATIC STATUS'] != True:
                    # this is a global value..one true is a global true
                    # but we want to distinguish between sites with no google, so set to false
                    results['GOOGLE']['STATIC STATUS'] = False
            except Exception as e:
                results['GOOGLE']['STATIC STATUS'] = True
                processErrors.append({'file': file_name, 'error': str(e), 'location': 'google file parsing'})

    analyze_tracking_status(website_name, extraction_date, vm_name, results, len(files), cursor, cnx)

def is_string_in_file(file_path, string_to_search):
    with open(file_path, 'r') as file:
        for line in file:
            if string_to_search in line:
                return True
    return False

def main():
    # argument is a path to a directory in the bucket
    parser = argparse.ArgumentParser(description='Parse files')
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('--path', help='directory to parse')
    group.add_argument('--query', help='lookup paths using postgres')
    parser.add_argument('--date', help='Specify the date in M-DD-YYYY format')
    parser.add_argument('--redo', help='parse already parsed path')

    args = parser.parse_args()

    cursor, cnx = connect_to_db()
    setup_postgres(cursor)

    if args.path:      
        mapped_files, broken_dirs = get_mapped_files(args.path)
        for unreachable_url in broken_dirs:
           logRetry(unreachable_url['website'], cursor, cnx, unreachable_url['website'], unreachable_url['date'], unreachable_url['vm_name'], unreachable_url['reason'])
    elif args.query:
       path_list = []
       if args.query == 'all-dynamic-positive-facebook':
          query=f"SELECT date, vm_name, website from website_visits where meta_dynamic_collection_status='true'"
       if args.query == 'all-dynamic-positive-facebook-on-date':
          query=f"SELECT date, vm_name, website from website_visits where meta_dynamic_collection_status='true' and date='{args.date}'"
       if args.query == 'all-false-negative-facebook':
          query=f"SELECT date, vm_name, website from website_visits where meta_static_collection_status IS NULL and meta_dynamic_collection_status='true'"
       if args.query == 'all-false-negative-google':
          query=f"SELECT date, vm_name, website from website_visits where google_static_collection_status='false' and google_dynamic_collection_status='true'"
       cursor.execute(query)
       query_results = cursor.fetchall()

       for row in query_results:
            date_str = row[0].strftime('%-m-%-d-%Y')
            row_str = date_str + '/' + row[1] + '/' + row[2]
            path_list.append(row_str)
       mapped_files, broken_dirs = get_mapped_files(path_list)
    
    prepare_files(mapped_files, cursor, cnx)
    shut_down_db(cursor, cnx)

def prepare_files(mapped_files, cursor, cnx):
    directories = mapped_files.keys()
    for directory in directories:
        date_string = mapped_files[directory]['date']
      
        website_name = mapped_files[directory]['website_name']
        extraction_date = datetime.strptime(date_string, '%m-%d-%Y').date()
        vm_name = mapped_files[directory]['vm_name']
        log_list = mapped_files[directory][date_string][vm_name]['logs']
        logObject = processLogs(log_list, cursor, cnx, website_name, date_string, extraction_date, vm_name)
        file_list = mapped_files[directory][date_string][vm_name]['files']

        results = logObject['results']
        fileMap = logObject['fileMap']

        print("processing website ", directory, " on vm ", vm_name, " and date ", extraction_date)
        processFiles(file_list, results, fileMap, cursor, extraction_date, date_string, vm_name, website_name, cnx)

if __name__ == "__main__":
    main()
