# [ALeRCE](http://alerce.science/) ZTF API

[ALeRCE](http://alerce.science/) API to query the ZTF Postgresql Database and other external catalogs.

The API is currently developed in [Flask](https://flask.palletsprojects.com/en/1.1.x/) Framework, and deployed inside a [Docker](https://www.docker.com/) container.

This API is used in ALeRCE [ZTF-Explorer](https://alerce.online/) to query the database and display the information.

## Documentation

A provisonary Documentation can be found [here](https://github.com/alercebroker/usecases/blob/master/api/ALeRCE%20ZTF%20DB%20API.md)

## Building image

Is required to have Docker running before creating the image.

To build the API image run inside the repository:
```
docker build -t psql_api .
```

## Running container

After a successful build run the container with
```
docker run -d -p 8085:8085                              \
           -e ZTF_HOST="<postgresql_host>"              \
           -e ZTF_PASSWORD="<postgresql_host>"          \
           -e ZTF_USER="<postgresql_user>"              \
           -e ZTF_DATABASE="<postgresql_host>"          \
           psql_api
```
other parameters for the API container are the following
```
ZTF_PORT                    (default 5432)
APP_WORKERS                 (default 2)
APP_BIND                    (default 0.0.0.0)
APP_PORT                    (default 8085)
FEATURES_TABLE              (default features_v2)
LATE_PROBABILITIES_TABLE    (default late_probabilities_v2)
EARLY_PROBABILITIES_TABLE   (default stamp_classification)
```
