#!/bin/bash

NUM_NODES=$1
NUM_SECONDS=$2
CONFIG_DIR=$3
LOG_DIR=$4
CLIENT_SSH_ID=$5
FLAMEGRAPH_PATH=/root/.cargo/bin/flamegraph

mkdir -p $LOG_DIR

set -m

leader_pid=0

for i in $(seq 1 $NUM_NODES);
do
    echo "Spawning node$i"
    RUST_LOG=debug ./target/release/server $CONFIG_DIR/node$i.json > $LOG_DIR/node$i.log 2> $LOG_DIR/node$i.log &
    if [ $i -eq 1 ]; then
        leader_pid=$!
    fi 
done

# echo "Attaching perf to leader (pid: $leader_pid)"
# sudo $FLAMEGRAPH_PATH -o $LOG_DIR/leader_flame.svg -p $leader_pid &
echo "Copying binaries and configs to client"
ssh $CLIENT_SSH_ID "mkdir -p ~/pft/$LOG_DIR"
scp -r $CONFIG_DIR $CLIENT_SSH_ID:~/pft
scp -r target $CLIENT_SSH_ID:~/pft

sleep 1
echo "Spawning client"
ssh $CLIENT_SSH_ID "RUST_LOG=info ~/pft/target/release/client ~/pft/$CONFIG_DIR/client.json > ~/pft/$LOG_DIR/client.log 2> ~/pft/$LOG_DIR/client.log" & 
sleep 1
client_pid=$(ssh $CLIENT_SSH_ID "pgrep client")
ssh $CLIENT_SSH_ID "sudo $FLAMEGRAPH_PATH -o /tmp/client_flame.svg -p $client_pid > /tmp/flame.log 2>/tmp/flame.log" &
echo "Remote client pid: $client_pid"

echo "Running experiments"
sleep $NUM_SECONDS


echo "Killing client"
kill %%
ssh $CLIENT_SSH_ID "kill $client_pid"
sleep 1

for i in $(seq 1 $NUM_NODES);
do
    echo "Killing node$i"
    kill %$i
done


echo "Retrieving client log"
scp $CLIENT_SSH_ID:~/pft/$LOG_DIR/client.log $LOG_DIR
scp $CLIENT_SSH_ID:/tmp/client_flame.svg $LOG_DIR
ssh $CLIENT_SSH_ID "rm -rf ~/pft"


num_client_threads=$(grep Tx: $LOG_DIR/client.log | cut -d':' -f 3 | sort | uniq | wc -l)
num_client_requests=0

for i in $(seq 0 $(( $num_client_threads - 1 )));
do
    thread_req=$(grep Tx:$i: $LOG_DIR/client.log | tail -n 1 | cut -d':' -f 4)
    echo "Thread num: $i Requests: $thread_req"
    num_client_requests=$(($thread_req + $num_client_requests))
done


echo "Total Client threads: $num_client_threads Total Requests: $num_client_requests"
