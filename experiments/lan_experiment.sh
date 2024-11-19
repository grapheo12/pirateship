#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 500 -c 700 -c 1000 -c 1200 -c 1500 -c 2000 -c 3000"
# ALL_CLIENTS="-c 1200 -c 1500"
# ALL_CLIENTS="-c 200 -c 1000"

start_time=$(date -Ins)
# start_time='2024-08-07T10:39:54.859389+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 180 -up 90 -down 5 $ALL_CLIENTS"

jq '.consensus_config.max_backlog_batch_size = 1500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make
$RUN_CMD

# Run chained_pbft
make chained_pbft_logger
$RUN_CMD

# Run lucky_raft
make lucky_raft_logger
$RUN_CMD --max_nodes 5


# Run signed_raft
make signed_raft_logger
$RUN_CMD --max_nodes 5


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 3 -l node1 -up 90 -down 5 -o plot.png \
    --legend "pirateship+byz" \
    --legend "pbft+onlybyz" \
    --legend "raft" \
    --legend "signed_raft"
