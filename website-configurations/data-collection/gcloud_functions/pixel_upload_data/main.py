import base64
from flask import Flask, jsonify
from google.cloud import storage
import functions_framework
from google.api_core.exceptions import GoogleAPICallError
import gzip
import sys
import json
import os

app = Flask(__name__)

project_name = os.environ.get("PROJECT_ID")
bucket_name = os.environ.get("BUCKET_NAME")

#@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def upload_to_bucket(blob, content, content_type):
    try:
        blob.upload_from_string(content, content_type=content_type)
    except ValueError as e:
        print(f"ValueError occurred: {e}")
        raise

@functions_framework.http
@app.route('/upload_data', methods=['POST', 'OPTIONS'])
def upload_data(request):
    # Set CORS headers for the preflight request
    if request.method == "OPTIONS":
        # Allows GET requests from any origin with the Content-Type
        # header and caches preflight response for an 3600s
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }

        return ("", 204, headers)

    # Set CORS headers for the main request
    headers = {"Access-Control-Allow-Origin": "*"}

    request_list = request.get_json()
    if not request_list:
        return ('Error: request list is empty', 400)
    else:
        for item in request.get_json():
            data = item['requestData']
            content = data.get('content')
            filename = data.get('filename')
            filetype = data.get('filetype')

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # Check if content is null
            if not content:
                return ('Error: content is null', 400)

            # convert data to a buffer
            if filetype == 'mhtml' or filetype == 'html':
                mime, content_str = content.split(',', 1)
                content_type = mime.split(';')[0].split(':')[1]
                content = base64.b64decode(content_str)
            else:
                if isinstance(content, dict):
                    content = json.dumps(content)
                content = content.encode('utf-8')
                if filetype == 'js':
                    content_type = 'application/javascript'
                elif filetype == 'metadata':
                    content_type = 'text/plain'
                elif filetype == 'har' or filetype == 'json':
                    content_type = 'application/json'
                else:
                    content_type = 'application/octet-stream'  # default to binary data

            file_size_in_bytes = sys.getsizeof(content)
            file_size_in_MB = file_size_in_bytes / (1024 * 1024)

            if file_size_in_MB > 256:
                blob = bucket.blob(filename + '.gz')
                blob.content_encoding = 'gzip'
                compressed_content = gzip.compress(content)
                upload_to_bucket(blob, compressed_content, content_type)
            else:
                blob = bucket.blob(filename)
                upload_to_bucket(blob, content, content_type)

        return jsonify(message='Files uploaded successfully'), 200
