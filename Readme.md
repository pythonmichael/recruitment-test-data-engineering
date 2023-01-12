# Code test for data engineering candidates: Solution

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

### Starting MySQL

To start up the ETL application.

```
docker compose up dataloader
```

### Cleaning up

To tidy up, bringing down all the containers and deleting them.

```
docker compose down
```
