#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 100"


start_time=$(date -Ins)
# start_time='2024-08-07T10:39:54.859389+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 1 -s 120 -up 40 -down 20 $ALL_CLIENTS"

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 1 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make
$RUN_CMD

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 2 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make
$RUN_CMD

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 5 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make
$RUN_CMD

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 10 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make
$RUN_CMD

# jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 10 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# # Run pirateship
# make signed_raft_logger
# $RUN_CMD

# jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 20 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# # Run pirateship
# make signed_raft_logger
# $RUN_CMD

# jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# # Run pirateship
# make signed_raft_logger
# $RUN_CMD

jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 2000' scripts/local_template.json > /tmp/local_template.json

# Run pirateship
make lucky_raft_logger
$RUN_CMD


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_latency_cdf.py \
    --path logs --end $end_time --start $start_time \
    -c 3 -up 40 -down 20 -o plot.png \
    --legend "sig=1" \
    --legend "sig=2" \
    --legend "sig=5" \
    --legend "sig=10" \
    --legend 'sig=$\infty$'
    # --legend "sig=10" \
    # --legend "sig=20" \
    # --legend "sig=50" \
