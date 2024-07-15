#!/bin/bash

NUM_NODES=$1
NUM_SECONDS=$2
CONFIG_DIR=$3
LOG_DIR=$4
FLAMEGRAPH_PATH=/root/.cargo/bin/flamegraph

mkdir -p $LOG_DIR

set -m
set -o xtrace

leader_pid=0

for i in $(seq 1 $NUM_NODES);
do
    echo "Spawning node$i"
    RUST_BACKTRACE=1 RUST_LOG=debug ./target/release/server $CONFIG_DIR/node$i\_config.json > $LOG_DIR/node$i.log 2> $LOG_DIR/node$i.log &
    if [ $i -eq 1 ]; then
        leader_pid=$!
    fi 
done

# echo "Attaching perf to leader (pid: $leader_pid)"
# sudo $FLAMEGRAPH_PATH -o $LOG_DIR/leader_flame.svg -p $leader_pid &

sleep 1
echo "Spawning client"
RUST_LOG=info ./target/release/client $CONFIG_DIR/client1_config.json > $LOG_DIR/client.log 2> $LOG_DIR/client.log & 

echo "Running experiments"
sleep $NUM_SECONDS


echo "Killing client"
kill %%

for i in $(seq 1 $NUM_NODES);
do
    echo "Killing node$i"
    kill %$i
done

