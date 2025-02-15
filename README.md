# tracker-form-data-configuration
## website-configurations
### data-collection
Our data collection took place on gcloud linux e2-medium machines in the central region. Data was stored in a gcloud bucket. The code is made up of the following components:
1) data_collection_script_linux.py: this was the main script that started on VM boot and handled website visits. It fetches a list of urls to visit from an endpoint running in gcloud (using a file that lives in a gcloud bucket). It opens that website with custom chrome extensions and then takes and uploads a screenshot during each visit.
2) extensions: the chrome extensions handle file downloads (mv2 and mv3) and form injections (mv2 only). We use two to take advantage of features that only exist in manifest version 2 (webRequest blocking) and manifest version 3 (devtools). Both extensions upload data to the gcloud bucket using the same endpoint and hash file names to keep them a reasonable length. All files are uploaded with the structure `DATE(MM-DD-YYYY)/machine_name/url`.
3) gcloud_functions: run in google cloud, these functions facilitate communication with the gcloud bucket (i.e. uploading data and fetching a list of urls)