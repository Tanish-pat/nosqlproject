# Steps to run

## Requirements

Ensure you have Docker and Docker Compose installed.

## Pipelines

There are 4 pipelines: Mongo, Pig, Hive and MapReduce

To run,

```shell
docker compose up -d --build
```

Cleanup

```shell
docker compose down -v
```

Then start the respective app container (`docker attach mongo-app / pig-app / hive-app / mr-app`) by attaching it to the terminal and then pressing the corresponding choice (1-4) to start the pipeline.
