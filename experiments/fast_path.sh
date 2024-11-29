#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

# ALL_CLIENTS="-c 10 -c 20 -c 50 -c 100 -c 500 -c 700 -c 1000 -c 1500 -c 1600"
ALL_CLIENTS="-c 100 -c 200 -c 300 -c 500 -c 700 -c 900 -c 1000 -c 1200 -c 1500 -c 1800 -c 2000 -c 2500"



start_time=$(date -Ins)
# start_time='2024-10-30T20:35:31.196252+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 120 -up 40 -down 20 $ALL_CLIENTS"

jq '.consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 4000' scripts/local_template.json > /tmp/local_template.json

# Run with fast path
make
$RUN_CMD

# Run with slow path
make pirateship_logger_nofast
$RUN_CMD


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 3 -l node1 -up 40 -down 20 -o plot.png \
    --legend "pirateship_fast+byz" \
    --legend "pirateship_slow+byz"
