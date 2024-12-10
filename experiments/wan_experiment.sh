#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

# ALL_CLIENTS="-c 1000 -c 2000 -c 5000"
# ALL_CLIENTS="-c 7000 -c 9000 -c 10000 -c 15000 -c 20000 -c 25000"
ALL_CLIENTS="-c 100 -c 200 -c 500 -c 1000 -c 2000 -c 3000 -c 5000 -c 7000 -c 9000"
# ALL_CLIENTS="-c 12000 -c 15000 -c 18000"
# ALL_CLIENTS="-c 15000 -c 18000 -c 25000 -c 35000 -c 45000"
# ALL_CLIENTS="-c 25000 -c 30000 -c 40000 -c 45000"
# ALL_CLIENTS="-c 40000 -c 45000"
EXPERIMENT_CONFIG="-r 3 -s 180 -up 90 -down 20"
PLOT_CONFIG="-r 3 -c 5 -up 90 -down 20 -l node1"

make

# start_time="2024-11-15T19:25:48.046161+00:00"
start_time=$(date -Ins)

python3 experiments/setup_nodelist.py c1 ..

jq '.consensus_config.batch_max_delay_ms = 2 | .consensus_config.signature_max_delay_ms = 2500 | .consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 1 | .consensus_config.view_timeout_ms = 8000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..

python3 experiments/setup_nodelist.py c2 ..

jq '.consensus_config.batch_max_delay_ms = 2 | .consensus_config.signature_max_delay_ms = 2500 | .consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 8000' scripts/local_template.json > /tmp/local_template.json

python3 scripts/run_remote_client_sweep.py \
    -nt /tmp/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem \
    $EXPERIMENT_CONFIG $ALL_CLIENTS

python3 experiments/setup_nodelist.py restore ..


python3 experiments/setup_nodelist.py c3 ..

jq '.consensus_config.batch_max_delay_ms = 2 | .consensus_config.signature_max_delay_ms = 2500 | .consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 3 | .consensus_config.view_timeout_ms = 8000' scripts/local_template.json > /tmp/local_template.json

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
    --legend "c1+byz" \
    --legend "c2+byz" \
    --legend "c3+byz"