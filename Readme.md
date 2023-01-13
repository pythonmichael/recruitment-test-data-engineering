# Code test for data engineering candidates: Solution

## Solution summary

- The database schema is defined in schema.sql.
- The dataloader docker image contains the data loading application. It can be found in the images/dataloader folder. The application was implemented using pyspark. The containerization of spark was based on the example [here](https://github.com/datamechanics/examples/tree/main/pyspark-example).
- The datasummerizer docker image contains the summerizing application. It can be found in the images/datasummerizer folder. The application was also implemented using pyspark.

## Detailed documentation

This can be found as comments in the scripts of the applications.

## Setup

### Requirements

Make sure you have recent versions of Docker and Docker Compose.

### Building the images

This will build all of the images referenced in the Docker Compose file.
```
docker compose build
```

### Starting MySQL

To start up the MySQL database.

```
docker compose up database
```

### Load the schema

```
docker compose exec --no-TTY database mysql --host=database --user=codetest --password=swordfish codetest <schema.sql
```

### Query the database

```
docker compose exec database mysql --host=database --user=codetest --password=swordfish codetest
```

### Starting the applications

To start up the data loader.

```
docker compose run dataloader driver local:///opt/application/dataloader.py
```

To start up the data summarizer.
```
docker compose run datasummarizer driver local:///opt/application/datasummarizer.py
```

### Cleaning up

To tidy up, bringing down all the containers and deleting them.

```
docker compose down
```
