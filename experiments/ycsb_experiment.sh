#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

# ALL_CLIENTS="-c 1 -c 10 -c 100 -c 500 -c 700 -c 1000 -c 1200 -c 1500 -c 2000"
# ALL_CLIENTS="-c 100 -c 200 -c 300 -c 500 -c 700 -c 900 -c 1000 -c 1200 -c 1500 -c 1800 -c 2000 -c 2500"
ALL_CLIENTS="-c 1000 -c 1500 -c 2000 -c 2500"
ALL_CLIENTS="-c 1000"

start_time=$(date -Ins)
# start_time='2024-08-07T10:39:54.859389+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 60 -up 10 -down 10 $ALL_CLIENTS"


jq '.consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 100 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make pirateship_kvs
$RUN_CMD


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 2 -c 3 -l node1 -up 10 -down 10 -o plot.png \
    --legend "ycsb-a"
