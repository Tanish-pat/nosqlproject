# Steps to run

## Requirements

Ensure you have Docker and Docker Compose installed.

## Pipelines

There are 4 pipelines, steps to run each one are given below. You need to set the correct `PIPELINE_NUMBER` env variable.

Mongo (1)

```shell
docker compose -f docker-compose.mongo.yml build
docker compose -f docker-compose.mongo.yml run --rm app > output/mongo_results.txt
```

Pig (2)

```shell
docker compose -f docker-compose.pig.yml build
docker compose -f docker-compose.pig.yml run --rm app > output/pig_results.txt
```

Hive (Wait for 10-15 seconds before running) (3)

```shell
docker compose -f docker-compose.hive.yml build
docker compose -f docker-compose.hive.yml run --rm app > output/hive_results.txt
```

MapReduce (4)

```shell
docker compose -f docker-compose.mapreduce.yml build
docker compose -f docker-compose.mapreduce.yml run --rm app > output/mapreduce_results.txt
```

Cleanup to remove all running containers and volumes

```shell
docker compose -f docker-compose.mongo.yml -f docker-compose.pig.yml -f docker-compose.hive.yml -f docker-compose.mapreduce.yml down -v
```
