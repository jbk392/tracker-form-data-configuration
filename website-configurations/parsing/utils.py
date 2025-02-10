import os
import gzip

### PARSING ###
def mark_reviewed(reviewed_items_file, website_path):
    with open(reviewed_items_file, 'a') as file:
       file.write(f"{website_path}\n")

### FILE UTILS ###
def open_file(file):
    try:
        with gzip.open(file, 'rt', encoding='utf-8') as f:
           return f.read()
    except:
        try:
           with open(file, 'r') as f:
              return f.read()
        except:
           print("cant open file: ", file)

def get_file_name(file, fileMap):
    root, ext = os.path.splitext(file)
    try:
        file_name = fileMap[root]
    except:
        try:
            new_file = root+'-mv3'
            file_name = fileMap[new_file]
        except:
            file_name = root
    return file_name