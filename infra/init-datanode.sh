#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

DATANODE_DIR="/opt/hadoop/data/dataNode"

if [ -d "$DATANODE_DIR" ]; then
    rm -rf "$DATANODE_DIR"/*
    echo "DataNode directory cleaned successfully."
else
    echo "DataNode directory does not exist. Creating..."
    mkdir -p "$DATANODE_DIR"
fi

echo "Setting permissions for DataNode directory..."
chown -R hadoop:hadoop "$DATANODE_DIR"
chmod 755 "$DATANODE_DIR"

echo "Starting HDFS DataNode Service..."
hdfs datanode
