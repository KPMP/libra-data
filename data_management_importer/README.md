# _name_

## Usage

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
