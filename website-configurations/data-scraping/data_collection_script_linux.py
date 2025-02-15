#!/usr/bin/python3
import os
import time
from datetime import datetime
from pytz import timezone
import subprocess
import sys
from google.cloud import storage
from google.cloud.exceptions import NotFound
import requests
import json
from dotenv import load_dotenv
import signal

# load environment variables from .env file
load_dotenv()

# path to base directory (for logs, chrome extensions)
BASE_PATH = os.environ.get("BASE_PATH")

# redirect stdout and stderr to log files
if not os.path.exists(f"{BASE_PATH}/logs"):
    os.makedirs(f"{BASE_PATH}/logs")
log_file = open(f"{BASE_PATH}/logs/data_collection_log.log", "a")
sys.stdout = log_file
sys.stderr = log_file

#### INTIALIZE ENVIRONMENT ####
# set google auth credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.environ.get("SERVER_KEYFILE_PATH")
# setup bucket
storage_client = storage.Client()
bucket = storage_client.get_bucket(os.environ.get("BUCKET_NAME"))
print("starting script...")

tz = timezone('EST')
now = datetime.now(tz)
nowDate = now.strftime("%-m-%-d-%Y")

# get name of current VM 
def get_vm_name():
    vm_request_url = "http://metadata.google.internal/computeMetadata/v1/instance/name"
    vm_name_request = requests.get(vm_request_url, headers={'Metadata-Flavor': 'Google'})

    return vm_name_request.text

# get list of websites to visit
def get_urls(vm_name):
    params = {"vmName": vm_name}
    get_urls = "https://us-central1-dontcrimeme.cloudfunctions.net/pixel_get_urls"
    url_response = requests.get(get_urls, params=params)
    return url_response.json()['data']

def close_chrome():
    # Get the PID of the Chrome process
    command = ['pgrep', 'chrome']
    result = subprocess.run(command, capture_output=True, text=True)
    pids = result.stdout.strip().split('\n')
    for pid in pids:
            try:
                    pid_int = int(pid)
                    # Send the SIGTERM signal to the Chrome process
                    os.kill(pid_int, signal.SIGTERM)
            except ValueError:
                    print("invalid PID: ", pid)
            except ProcessLookupError:
                    print(f"Process {pid_int} not found. It may have already been terminated.")

# mark website as visited
def add_entry(bucket_name, blob_name, entry, remaining_visits, vm_name):
    # Create a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Get the blob
    blob = bucket.blob(blob_name)

    remaining_visits = remaining_visits - 1
    # Download the current content of the blob
    try:
       current_content = blob.download_as_text()
       current_content = json.loads(current_content)
       # initialize new url
       if not isinstance(current_content.get(entry), dict):
           current_content[entry] = {'count': 0, 'path': f'{nowDate}/{vm_name}/{entry}', 'total_visits': 0}

       # Add the new entry
       path = current_content[entry]['path']
       current_content[entry]['count'] = remaining_visits
       current_content[entry]['total_visits'] = current_content[entry]['total_visits']+1
    except NotFound:
       current_content = {entry: {'count': remaining_visits, 'path': f'{nowDate}/{vm_name}/{entry}', 'total_visits': 1}}

    # Update the blob with the new content
    blob.upload_from_string(json.dumps(current_content), content_type='application/json')

def modify_url(url, add_www=False):
    # modify the URL to add or remove www. as needed.
    if add_www:
        return f'www.{url}'
    else:
        return url

def check_url(url):
    def attempt_request(modified_url):
        error_type = None
        try:
            response = requests.get('https://'+modified_url, timeout=10)  # Adjust timeout as needed
            # Check for HTTP status codes
            if response.status_code == 403:
                error_type = "403 Forbidden"
            elif response.status_code == 404:
                error_type = "404 Not Found"
            elif response.status_code == 503:
                error_type = "503 Service Unavailable"
            elif response.status_code == 504:
                error_type = "504 Gateway Timeout"
            else:
                error_type = None
            return error_type
        except requests.exceptions.Timeout:
            return "Request timed out (the page never loads)"
        except requests.exceptions.TooManyRedirects:
            return "Too many redirects"
        except requests.exceptions.SSLError:
            return "SSL Error (connection not private)"
        except requests.exceptions.ConnectionError as e:
            if "Name or service not known" in str(e):
                return "DNS Error (site not reachable)"
            else:
                return "Connection Error"
        except requests.exceptions.RequestException as e:
            print(e)
            return "An error occurred while handling the request"

    # First attempt with the original URL
    result = attempt_request(url)
    if result in ["404 Not Found", "DNS Error (site not reachable)", "Connection Error"]:
        # If the first attempt fails, try modifying the URL
        modified_url = modify_url(url, add_www="www." not in url)
        result = attempt_request(modified_url)
    else:
        modified_url = url
    return {"error": result, "url": modified_url}

# turn the vm off
def shutdown_vm():
    vm_name = get_vm_name()
    subprocess.run(['gcloud', 'compute', 'instances', 'stop', vm_name, '--zone', 'us-central1-a', '--project', 'dontcrimeme'])

def main():
    vm_name = get_vm_name()
    website_list = get_urls(vm_name)

    # write vm name to a file so the chrome extension can access it
    with open(f"{BASE_PATH}/extensions/mv2/instance_name.txt", "w") as file:
                    file.write(vm_name)
    with open(f"{BASE_PATH}extensions/mv3/instance_name.txt", "w") as file:
                    file.write(vm_name)
    file.close()

    for item in website_list:
        url = item['url']
        upper_bound = item['remaining_visits'] + 1
        for i in range(1, upper_bound):
            with open(f"{BASE_PATH}/extensions/mv2/url_name.txt", "w") as f:
                f.write(url + '-' + str(i))
            with open(f"{BASE_PATH}/extensions/mv3/url_name.txt", "w") as f:
                f.write(url + '-' + str(i))
            f.close()

            validated_url = check_url(url)

            # open google with chrome extensions
            subprocess.Popen([
                "/usr/bin/google-chrome",
                    validated_url["url"],
                    "--args", "--auto-open-devtools-for-tabs", "--disable-extensions-http-throttling ",
                    f"--load-extension={BASE_PATH}/extensions/mv2,{BASE_PATH}/extensions/mv3",
                    "--no-default-browser-check", "--no-first-run", "--disable-notifications", "--disable-features=Translate", "--simulate-outdated-no-au='Tue, 31 Dec 2099 23:59:59 GMT"
            ])
            time.sleep(180) # give page time to load and download

            if validated_url["error"] is not None:
                # If all variations fail, add the original URL to the broken URLs list
                local_file_name = f"{BASE_PATH}/broken_urls.txt"
                remote_file_name = f'{nowDate}/{vm_name}/broken_urls.txt'
                blob = bucket.blob(remote_file_name)
                if blob.exists():
                        blob.download_to_filename(local_file_name)
                with open(local_file_name, 'a') as f:
                        new_entry = json.dumps(url)
                        f.write(f'{new_entry}\n')
                blob.upload_from_filename(local_file_name)

            add_entry('pixel_tracking_data', f'visited_websites_{vm_name}.json', url, item['remaining_visits'], vm_name)

            # take screenshot
            if not os.path.exists(f"{BASE_PATH}/screenshots"):
                os.makedirs(f"{BASE_PATH}/screenshots")
            print(f"taking screenshot for {url} at {datetime.now(tz)}")
            url_text = url.replace("/", "")
            screenshot_file = f"{BASE_PATH}/screenshots/screenshot-{url_text}-{nowDate}.png"
            subprocess.run(["gnome-screenshot", "-f", screenshot_file])

            # upload screenshot to bucket
            blob = bucket.blob(f"{nowDate}/{vm_name}/screenshots/screenshot-{url}-{i}-{nowDate}.png")
            try:
                print(f"uploading screenshot {blob} at {datetime.now(tz)}")
                blob.upload_from_filename(screenshot_file)
            except Exception as e:
                print(f"uploading screenshot failed with error {e} at {datetime.now(tz)}")

            # upload metadata file to bucket
            if not os.path.exists(f"{BASE_PATH}/metadata"):
                os.makedirs(f"{BASE_PATH}/metadata")
            metadata_file = f"{BASE_PATH}/metadata/metadata-{url_text}-{nowDate}.txt"
            with open(metadata_file, "a") as f:
                f.write(f"url: {url}\n")
                f.write(f"timestamp: {nowDate}")

            blob = bucket.blob(f"{nowDate}/{vm_name}/metadata/metadata-{url}-{i}-{nowDate}.txt")
            try:
                print(f"uploading metadata {blob} at {datetime.now(tz)}")
                blob.upload_from_filename(metadata_file)
            except Exception as e:
                print(f"uploading metadata failed with error {e} at {datetime.now(tz)}")

            # upload log file to bucket
            with open(metadata_file, "a") as f:
                f.write(f"url: {url}\n")
                f.write(f"timestamp: {nowDate}")

            blob = bucket.blob(f"{nowDate}/{vm_name}/logs/log-{url}-{i}-{nowDate}.txt")
            try:
                print(f"uploading log {blob} at {datetime.now(tz)}")
                log_file.flush()
                blob.upload_from_filename(f"{BASE_PATH}/logs/data_collection_log.log")
            except Exception as e:
                print(f"uploading log failed with error {e} at {datetime.now(tz)}")

            time.sleep(10)

            close_chrome()

if __name__ == "__main__":
    main()
    shutdown_vm()