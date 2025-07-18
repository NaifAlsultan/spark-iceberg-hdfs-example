#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd

cd ../job

docker run \
  --network infra_hdfs_network \
  --volume "$(pwd)/target/scala-2.12/job_2.12-1.0.0.jar:/opt/jobs/job.jar" \
  --user root \
  --rm apache/spark:3.5.4-scala2.12-java11-ubuntu@sha256:6d4a66b59711f0772b4a2fde5283263e4cb9cc85b2646c31b9b3ade37af3045b \
  /opt/spark/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.1 \
    /opt/jobs/job.jar \
  | grep -Pzo '\+-[\s\S]*-\+'  

