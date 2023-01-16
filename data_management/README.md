# Data Manager Dashboard Importer 

## Command Line Usage

1. Set the .env variables

2. Build the Docker image: 
   - `$ sh rebuild.sh`

3a. Run the importer service with the arguments in double quotes:
   - `$ sh run.sh "-d spectrack -a update"`

OR

3b. Run the importer service from a local machine (eg: When running on a Mac and docker isn't available):
   - `$ python3 main.py --data_source redcap --action insert`

OR

3c. Run the service as a Flask app within the dataLake network
   - `$ sh run-service.sh`

OR

3d. Run the service as a Flask app on the host machine
   - `$ sh run-service-host.sh`


NOTE: The (non-Flask app) service options are:
   - [-h] -a {update,insert} -d {redcap,spectrack}

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
   "readyToPromoteDlu": True,
   "promotionDluSucceeded": False,
   "removedFromGlobus": False,
   "promotionStatus": "promoted",
   "notes": "notes"
}`


### /v1/dlu/package/<package_id>
Method: POST
This endpoint updates a package entry in the DMD "dlu_package_inventory" table. The JSON request body should have the fields/values (see above) that need to be updated, e.g. 
`{
   "promotionDluSucceeded": True
}`


### /v1/dlu/file
Method: POST
This endpoint adds a file to the DMD "dlu_file" table. The request body (JSON) should have the following fields (with sample data):
`{
   "dluFileName": "name",
   "dluPackageId": "package_id",
   "dluFileId": "file_id",
   "dluFileSize": 12345,
   "dluMd5Checksum": "checksum",
}

### /v1/dlu/package/<package_id>/move
Method: POST
This endpoint moves files for the specified package from Globus into the DLU filesystem. It also updates the DLU Mongo record for that package with the new files, updates the "promotionDluSucceeded" field in the DMD "dlu_package_inventory" table, and, if successful, updates the DLU state to "UPLOAD_SUCCEEDED". 

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
