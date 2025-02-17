# tracker-form-data-configuration
## website-configurations
### data-collection
Our data collection took place on gcloud linux e2-medium machines in the central region. Data was stored in a gcloud bucket. The code is made up of the following components:
1) data_collection_script_linux.py: this was the main script that started on VM boot and handled website visits. It fetches a list of urls to visit from an endpoint running in gcloud (using a file that lives in a gcloud bucket). It opens that website with custom chrome extensions and then takes and uploads a screenshot during each visit.
2) extensions: the chrome extensions handle file downloads (mv2 and mv3) and form injections (mv2 only). We use two to take advantage of features that only exist in manifest version 2 (webRequest blocking) and manifest version 3 (devtools). Both extensions upload data to the gcloud bucket using the same endpoint and hash file names to keep them a reasonable length. All files are uploaded with the structure `DATE(MM-DD-YYYY)/machine_name/url`.
3) gcloud_functions: run in google cloud, these functions facilitate communication with the gcloud bucket (i.e. uploading data and fetching a list of urls)

#### local-run
This folder has slightly modified versions of the mv2 extension that can be run locally on a Mac computer. This data can then be used as input to the data parser (see below). In order to run data-collection locally:
1) install the extension in chrome using developer mode 
2) modify the `local_data_collection_script.py` `test_websites` variable to include any urls you would like to visit and create an environment file (.env) with the path to your locally downloaded extension (something like MV2_PATH=/home/user/github/etc).
3) run with `python local_data_collection_script.py`

Note that this will download a lot of files to your machine. 

### data-parsing
The main file to run parsing is `data_parser.py`. It requires specifying a data source which can be either a path to downloaded files or a query (this option assumes you've already done parsing in a database and want to query websites that meet specific conditions; there are some querys explicitly defined in the code).

Additionally, the parser is assuming that the websites to parse are in the following directory format: MM-DD-YYYY/URL/files and that all folders are in the same parent directory as the parsers. If you want to modify this structure, you will have to update the `get_mapped_files()` function in `helper_functions.py` accordingly.

