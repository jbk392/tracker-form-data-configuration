import js2py
import re 
import os
import json
from postgres_functions import insert_into_db

# make javascript file parseable 
def prepare_to_parse(location, isBlob=True):
    if not isBlob:
        with open(location) as f:
            fileData = f.read()
            f.close()
    else:
        fileData = location

    parsedData = js2py.parse_js(fileData)       
    return parsedData

# log files are cumulative, so we only need to review the last file uploaded
def extractLogNumbers(logs):
    def extractNumber(filename):
        return int(''.join(filter(str.isdigit, filename.split('-')[1])))
    
    highest_mv3 = None
    highest_mv2 = None
    mv3_files = [f for f in logs if f.endswith('-mv3.json')]
    mv2_files = [f for f in logs if f.endswith('.json') and not f.endswith('-mv3.json')]

    # Handle possible error if log files were missed
    if mv3_files:
        highest_mv3 = max(mv3_files, key=extractNumber)
    else:
        highest_mv3
    if mv2_files:
        highest_mv2 = max(mv2_files, key=extractNumber)

    return highest_mv2, highest_mv3

def get_mapped_files(path):
    mapped_files = {}
    broken_dirs = []

    # organize files for each website
    # so we can easily access logs and config files
    def walk_path(subpath):
        not_dirs = ["broken_urls.txt", "screenshots", "logs", "metadata"]
        for root, dirs, files in os.walk(subpath):
            for file in files:
                path = os.path.join(root, file)
                path_split = path.split('/')

                # this may be need to modified if accomodate a different directory structure
                date = path_split[0]
                vm_name = path_split[1]
                website_name = path_split[2]

                # websites the VM could not reach
                # however, we still want a record of them in our parsed database
                if file == 'broken_urls.txt':
                    with open(path, 'r') as broken_urls:
                        for line in broken_urls:
                            json_data = json.loads(line.strip())
                            new_entry = {
                                'website': json_data['url'],
                                'date': date,
                                'vm_name': vm_name,
                                'reason': json_data['error']
                            }
                            broken_dirs.append(new_entry)
                
                if website_name not in not_dirs:
                    file_name = path_split[3]
                    path_key = f'{website_name}/{date}/{vm_name}'
                    if path_key not in mapped_files:
                        mapped_files[path_key] = {
                            'website_name': website_name,
                            'date': date,
                            'vm_name': vm_name,
                        }
                    if date not in mapped_files[path_key]:
                        mapped_files[path_key][date] = {}
                    if vm_name not in mapped_files[path_key][date]:
                        mapped_files[path_key][date][vm_name] = {'logs': [], 'files': []}

                    if 'logs' in path:
                        mapped_files[path_key][date][vm_name]['logs'].append(file_name)
                    else:
                        mapped_files[path_key][date][vm_name]['files'].append(file_name)

    if isinstance(path, list):
        for subpath in path:
            walk_path(subpath)
    else:
        walk_path(path)
    return mapped_files, broken_dirs

def insert_error(website, date, vm_name, error, cursor, cnx):
     insert_into_db("""INSERT INTO errors (
        website, date, vm_name, error) VALUES (%s, %s, %s, %s)
        ON CONFLICT(website, date, vm_name, error) DO NOTHING;""",
        (website, date, vm_name, error),
        cursor,cnx)