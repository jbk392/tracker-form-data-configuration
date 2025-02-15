import os
import csv
import math
from io import StringIO
from flask import Flask
from google.cloud import storage
import json
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

project_name = os.environ['PROJECT_NAME']
bucket_name = os.environ['BUCKET_NAME']
RETRY_COUNT = 1

def download_website_list(bucket):
    """Downloads a blob from the bucket."""
    blob = bucket.blob('data-collection-input.csv')
    blob_data = blob.download_as_text()

    # Convert the string to a file-like object
    data_file = StringIO(blob_data)

    # Create a CSV reader
    reader = csv.reader(data_file)

    # Skip the header
    next(reader)

    # Create a list from the CSV data
    reader_list = list(reader)

    return reader_list

def check_url_status(url, bucket, vmName):
    visited_websites_blob = bucket.blob(f'visited_websites_{vmName}.json')
    if not visited_websites_blob.exists():
        return None
    else:
        visited_websites = visited_websites_blob.download_as_text()
        try:
            visited_websites = json.loads(visited_websites)
            url_status = visited_websites.get(url, None)["count"]
            if url_status == None:
                return RETRY_COUNT
            else:
                return url_status
        except Exception as e:
            return RETRY_COUNT

@app.route('/get_urls', methods=['GET'])
def get_urls(request):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    vmName = request.args.get('vmName', default=None, type=str)
    vmNumber = int(vmName.split('-')[-1])

    website_list = download_website_list(bucket)

    totalWebsites = len(website_list)
    # TODO: paramaterize
    totalVMs = 50
    websitesPerVM = math.ceil(totalWebsites / totalVMs)

    startingIndex = (vmNumber - 1) * websitesPerVM
    endingIndex = vmNumber * websitesPerVM
    if vmNumber == totalVMs:
        # in case it is uneven, just finish the list with the last VM
        endingIndex = totalWebsites+1

    websitesToSend = []

    for url_item in website_list[startingIndex:endingIndex]:
        url = url_item[1]

        remaining_tries = check_url_status(url, bucket, vmName)
        retries = RETRY_COUNT
        if remaining_tries is not None:
            retries = remaining_tries
        
        if retries != 0:
            websitesToSend.append({
                'url': url,
                'remaining_visits': retries
            })

    response  = {"status": "success",
                "data": websitesToSend}
    return response, 200

