# Data Manager Dashboard Importer 

## Command Line Usage

1. Set the .env variables

2. Build the Docker image: 
   - `$ sh rebuild.sh`

3a. Run the importer service with the arguments in double quotes:
   - `$ sh run.sh "-d spectrack -a update"`

*NOTE*: If you are running this on an empty/new database, you will need to run it in insert mode first:
   - `$ sh run.sh "-d spectrack -a insert"`

OR

3b. Run the importer service from a local machine (eg: When running on a Mac and docker isn't available):
   - `$ python3.8 main.py --data_source redcap --action insert`

OR

3c. Run the service as a Flask app within the dataLake network
   - `$ sh run-service.sh`

OR

3d. Run the service as a Flask app on the host machine
   - `$ sh run-service-host.sh`

OR

3e. Run the bulk upload with the arguments in double quotes:
- `$ sh run.sh "-d spectrack -m"`

NOTE: The (non-Flask app) service options for the Data Importer are:
   - [-h] -a {update,insert} -d {redcap,spectrack}

NOTE: The service options for the Bulk Uploader are:
- [-h] -a {update,insert} -m 

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

## Running this on the DLU / DMD Server
The heavens-docker/libra directory has a docker-compose file for running this on the DLU / DMD server. 

## Known Bug
There is a known [bug with docker on MacOS](https://github.com/docker/for-mac/issues/2670) in which the container is unable to talk to the host network. This problem may occur when attempting to connect to a tunnel created on the host machine. To work around this issue, you can either run this on a linux machine/windows machine, or bypass docker completely and run the script directly on your local machine.
