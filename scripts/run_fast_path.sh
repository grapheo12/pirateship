#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

# ALL_CLIENTS="-c 1 -c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
ALL_CLIENTS="-c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
# # ALL_CLIENTS="-c 200 -c 500 -c 700 -c 1000"
# ALL_CLIENTS="-c 50 -c 500"

# start_time=$(date -Ins)
start_time='2024-10-30T20:35:31.196252+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 60 -up 2 -down 2 $ALL_CLIENTS"
# RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt scripts/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 1 -s 30 -up 2 -down 2 $ALL_CLIENTS"

# # Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 1
# jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 1' scripts/local_template.json > /tmp/local_template.json
# make
# $RUN_CMD

# # Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 50
# jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50' scripts/local_template.json > /tmp/local_template.json
# make pirateship_logger_nofast
# $RUN_CMD


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 3 -l node1 -up 5 -down 5 -o plot.png \
    --legend "pirateship_fast+byz" \
    --legend "pirateship_slow+byz"