# Setup

## Prerequisites

Ensure that you have `docker` installed and the Docker deamon up and running. See their [docs](https://docs.docker.com/engine/install/) for setup instructions.

## Dev Env

If you're using [Nix](https://nixos.org/), run the below command in `spark-iceberg-assignment/` to enter into a shell environment with all the necessary packages:

```bash
nix develop
```

Otherwise, use your preferred package manager to install:
- JDK 11
- Scala 2.12
- sbt 1.10.11

## Package Job

Package the Spark job into a JAR by going into the `job/` directory:

```bash
cd ./job
```

and run `sbt` using the below command:

```bash
sudo sbt clean package
```

You can run it with `sudo` to avoid potential write permission issues.

## Setup Infra

Build the Livy Docker image:

```bash
docker build -t naif/livy:0.8.0 ./infra/livy
```

Run the HDFS cluster and Livy server with Docker compose:

```bash
docker compose -f ./infra/docker-compose.yml up -d
```

Wait for a few seconds to ensure that the HDFS cluster is ready.

# Submit Job

You have two options:
1. Submit the job using `spark-submit` from a Spark container.
2. Submit the job through a Livy server.

## Submission Through `spark-submit`

For the first option, run the following script:

```bash
sh client/submit-job.sh
```

You should see a trail of logs, followed by a table representing the snapshot IDs of the `recommendations` table.

```
+-------------------+-----------------------+
|snapshot_id        |committed_at           |
+-------------------+-----------------------+
|1635610743096655519|2025-07-19 12:00:56.831|
|517850691077462423 |2025-07-18 20:48:28.19 |
|1580682633391083550|2025-07-18 20:41:49.678|
|4927790649788899517|2025-07-18 20:41:16.341|
+-------------------+-----------------------+
```

## Submission Through Livy

For the second option, go to the `client/` directory:

```bash
cd ./client
```

and run `sbt` using the below command:

```bash
sbt run
```

You should see an output similar to the below:

```
Submitting job...
Polling http://localhost:8998/batches/2/state...
Polling http://localhost:8998/batches/2/state...
Successfully submitted job.
Starting session...
Polling http://localhost:8998/sessions/2/state...
Session '2' is ready!
Getting snapshot ID of 'my_catalog.default.recommendations' table...
+-------------------+-----------------------+
|snapshot_id        |committed_at           |
+-------------------+-----------------------+
|3064053014839129824|2025-07-19 12:04:55.623|
|1635610743096655519|2025-07-19 12:00:56.831|
|517850691077462423 |2025-07-18 20:48:28.19 |
|1580682633391083550|2025-07-18 20:41:49.678|
|4927790649788899517|2025-07-18 20:41:16.341|
+-------------------+-----------------------+
```

# Table Locations

The `recommendations` table is stored in `/warehouse/default/recommendations/data/` within the HDFS cluster as `parquet` files partitioned by `latest_event_date`.

Likewise, the `interactions` table is stored in `/warehouse/default/interactions/data/`.

To confirm this, enter into an interactive shell session with any HDFS container:

```bash
docker exec -it namenode bash
```

and explore the file system:

```bash
hdfs dfs -ls /warehouse/default/recommendations/data
```

