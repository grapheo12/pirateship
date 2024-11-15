#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 9000 -c 12000 -c 15000 -c 18000 -c 25000"
EXPERIMENT_CONFIG="-r 3 -s 180 -up 90 -down 5"
PLOT_CONFIG="-r 3 -c 5 -up 90 -down 5 -l node1"

make

# start_time="2024-11-15T19:25:48.046161+00:00"
start_time=$(date -Ins)

python3 experiments/setup_nodelist.py c1 ..

jq '.consensus_config.max_backlog_batch_size = 10000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 1 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..

python3 experiments/setup_nodelist.py c2 ..

jq '.consensus_config.max_backlog_batch_size = 10000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 1 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..


python3 experiments/setup_nodelist.py c3 ..

jq '.consensus_config.max_backlog_batch_size = 10000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 1 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..


python3 experiments/setup_nodelist.py c4 ..

jq '.consensus_config.max_backlog_batch_size = 10000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 1 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..


python3 experiments/setup_nodelist.py c5 ..

jq '.consensus_config.max_backlog_batch_size = 10000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 7000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..

end_time=$(date -Ins)

python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    $PLOT_CONFIG \
    -o plot.png \
    --legend "c1" \
    --legend "c2" \
    --legend "c3" \
    --legend "c4" \
    --legend "c5"
