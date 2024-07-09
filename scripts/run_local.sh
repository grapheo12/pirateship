#!/bin/bash

NUM_NODES=$1
NUM_SECONDS=$2
CONFIG_DIR=$3
LOG_DIR=$4

mkdir -p $LOG_DIR

set -m

for i in $(seq 1 $NUM_NODES);
do
    echo "Spawning node$i"
    RUST_LOG=info ./target/debug/server $CONFIG_DIR/node$i.json > $LOG_DIR/node$i.log 2> $LOG_DIR/node$i.log & 
done

sleep 1
echo "Spawning client"
RUST_LOG=warn ./target/debug/client $CONFIG_DIR/client.json > $LOG_DIR/client.log 2> $LOG_DIR/client.log & 

echo "Running experiments"
sleep $NUM_SECONDS

echo "Killing client"
kill %%

for i in $(seq 1 $NUM_NODES);
do
    echo "Killing node$i"
    kill %$i
done

