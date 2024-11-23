#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 1000 -c 1500 -c 1600"
# ALL_CLIENTS="-c 10 -c 1500"


start_time=$(date -Ins)
# start_time='2024-08-07T10:39:54.859389+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 30 -up 5 -down 5 $ALL_CLIENTS"

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 1 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
maken signed_raft_logger
$RUN_CMD

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make signed_raft_logger
$RUN_CMD --max_nodes 5


# Run lucky_raft
make lucky_raft_logger
$RUN_CMD --max_nodes 5


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 2 -c 3 -l node1 -up 5 -down 5 -o plot.png \
    --legend "signed_raft(sig=1)" \
    --legend "signed_raft(sig=50)" \
    --legend "raft"
