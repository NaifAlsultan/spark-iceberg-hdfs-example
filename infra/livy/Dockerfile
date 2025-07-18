FROM apache/spark:3.5.4-scala2.12-java11-ubuntu@sha256:6d4a66b59711f0772b4a2fde5283263e4cb9cc85b2646c31b9b3ade37af3045b

ENV LIVY_VERSION=0.8.0-incubating \
    LIVY_HOME=/opt/livy

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl unzip && \
    curl -o /tmp/livy.zip -SL https://dlcdn.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip && \
    unzip /tmp/livy.zip -d /opt && \
    mv /opt/apache-livy-${LIVY_VERSION}_2.12-bin ${LIVY_HOME} && \
    rm -rf /tmp/livy.zip /var/lib/apt/lists/* && \
    mkdir -p ${LIVY_HOME}/logs

EXPOSE 8998

WORKDIR ${LIVY_HOME}

ENTRYPOINT ["./bin/livy-server"]
