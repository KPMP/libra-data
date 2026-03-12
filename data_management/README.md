# Bulk Upload Script

process_bulk_uploads.py

## How to use

[Instructions here](https://docs.google.com/document/d/1M6wnlsYkw21HjGJC2u9mGEuPGpsyxj1m1U1NiSW7KIs/edit?tab=t.0#heading=h.azytq75vi7c1)

# Data Manager Dashboard Importer 

## Command Line Usage

1. Set the .env variables

2. Build the Docker image: 
   - `$ sh rebuild.sh`

3a. Run the Spectrack import service:
   - `$ python3.9 main.py -d spectrack -a update`

*NOTE*: If you are running this on an empty/new database, you will need to run it in insert mode first:
   - `$ python3.9 main.py -d spectrack -a insert`

OR

3b. Run the importer service from a local machine (eg: When running on a Mac and docker isn't available):
   - `$ python3.9 main.py --data_source redcap --action insert`

NOTE: The (non-Flask app) service options for the Data Importer are:
   - [-h] -a {update,insert} -d {redcap,spectrack}

NOTE: The service options for the Bulk Uploader are:
- [-h] -a {update,insert} -m 

## Shell Script with Alerting

Use the "update_st_with_alert.sh" script to run the Spectrack update with a Slack alert on errors. Provide a Slack passcode as a only parameter, e.g.:
   - `./update_st_with_alert.sh abc123def456ghi789`

## Service Endpoints

The following endpoints are available when running the script as a Flask web service:

### /v1/dlu/package
Method: POST

This endpoint adds a package to the DMD "dlu_package_inventory" table. The request body (JSON) should have the following fields (with sample data):

`{
   "dluPackageId": "package_id",
   "dluCreated": 1665768333,
   "dluSubmitter": "submitter",
   "dluTis": "tis",
   "dluPackageType": "package_type",
   "dluSubjectId": "subj_id",
   "dluError": True,
   "dluLfu": False,
   "knownSpecimen": "specimen",
   "redcapId": "redcap",
   "userPackageReady": True,
   "dvcValidationComplete": True,
   "packageValidated": True,
   "readyToMoveFromGlobus": True,
   "globusDluStatus": False,
   "removedFromGlobus": False,
   "promotionStatus": "promoted",
   "notes": "notes"
}`  
&nbsp;  
### /v1/dlu/package/<package_id>

Method: POST

This endpoint updates a package entry in the DMD "dlu_package_inventory" table. The JSON request body should have the fields/values (see above) that need to be updated, e.g. 

`{
   "promotionDluSucceeded": True
}`  
&nbsp;  
### /v1/dlu/file

Method: POST

This endpoint adds a file to the DMD "dlu_file" table. The request body (JSON) should have the following fields (with sample data):

`{
   "dluFileName": "name",
   "dluPackageId": "package_id",
   "dluFileId": "file_id",
   "dluFileSize": 12345,
   "dluMd5Checksum": "checksum",
}`  
&nbsp;  
### /v1/dlu/package/<package_id>/move

Method: POST

This endpoint does not move the package files, but rather marks the specified package as ready to be moved from Globus into the DLU filesystem. It updates the "ready_to_move_from_globus" field in the DMD dlu_package_inventory table to "yes," or it returns an error message and doesn't update any fields in the table.
&nbsp;
### /v1/dlu/package/ready

Method: GET

This endpoint retrieves the packages that are ready to be moved from Globus. The endpoint returns a list containing each result's dlu_package_id and globus_dlu_status.

## Development

### linting
`$ black .`

### rebuilding
`$ sh rebuild.sh`

### rebuild library modules on change
`$ python3 setup.py install --user`

## DLU Watcher

The dlu_watcher is controlled via the DluWatcher docker file and runs watch_files.py. It uses many of the same services as are used for the data-manager-service. The dlu watcher does a lot of background processing of data that is often triggered by the data managers updating records in the DMD tables.

### Slide Renaming
One major (new) functionality of the dlu-watcher is the slide renaming process. We periodically check the slide_manifest_import table for new records that we have not put into the slide_scan_curation table (this is a pretty brain-dead check of equal number of rows). If there are new rows in the slide_manifest_import table, we continue processing those rows.

The majority of the functionality for processing slide renaming lives in slide_management.py in services.

The first step is to process the new rows in slide_manifest_import and verify that we have all of the neccesary information in order to do an import. If the process is not picking up new records in the slide_manifest_import, check the log files in dlu_watcher for potential errors as we have not created a record in the slide_scan_curation table at this point, and do not want to since there are issues.

If records meet the preliminary checks, then they get added to the slide_scan_curation table. From here on, if there is an error, we will add that error to the appropriate records inside of slide_scan_curation.

### Moving files to DataLake
The other function of the dlu-watcher is to move files from Globus into the Data Lake. There are 2 main paths here.

#### Whole Slide Images 

For Whole slide images that are not a recalled package, we need to go down a different path to move the files into the data lake. We need to rename the files and then move the files in place and update the databases. This is a parallel path to non-WSI files, and calls many of the same functions, but ended up needing to be a partial duplicate of the process for non-wsi, because we were unable to rename the files on copy.

#### Non-WSI 

This will do a number of checks to make sure everything is copasetic and then call out to the appropriate locations to create new records in the databases, create directories in the data lake, and copy the data into the data lake. This also includes generating the md5checksums to store in the databases and can sometimes cause issues when we have extremely large files.

## Known Bug
There is a known [bug with docker on MacOS](https://github.com/docker/for-mac/issues/2670) in which the container is unable to talk to the host network. This problem may occur when attempting to connect to a tunnel created on the host machine. To work around this issue, you can either run this on a linux machine/windows machine, or bypass docker completely and run the script directly on your local machine.
