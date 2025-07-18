#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

NAMENODE_DIR="/opt/hadoop/data/nameNode"

if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "Formatting NameNode as no existing metadata found."
    hdfs namenode -format -force -nonInteractive
else
    echo "NameNode already formatted. Skipping format step."
fi

echo "Starting HDFS NameNode Service..."
hdfs namenode
