#!/bin/bash
set -o xtrace

# ALL_CLIENTS="-c 1 -c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
ALL_CLIENTS="-c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
# # ALL_CLIENTS="-c 200 -c 500 -c 700 -c 1000"
# ALL_CLIENTS="-c 50 -c 500"

start_time=$(date -Ins)
# start_time='2024-08-16T10:24:40.324380+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 120 -up 2 -down 2 $ALL_CLIENTS"
# RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt scripts/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 1 -s 30 -up 2 -down 2 $ALL_CLIENTS"

# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 1
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 1' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD

# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 10
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 10' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD

# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 50
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD

# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 100
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 100' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD


# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 200
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 200' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD


# Run pirateship quorum_diversity_k = 3, signature_max_delay_blocks = 500
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 500' scripts/local_template.json > /tmp/local_template.json
make
$RUN_CMD

# Run chained_pbft
make chained_pbft_logger
$RUN_CMD

# Run lucky_raft
make lucky_raft_logger
$RUN_CMD --max_nodes 5


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 2 -l node1 -up 5 -down 5 -o plot.png \
    --legend "pirateship(sig=1)" \
    --legend "pirateship(sig=10)" \
    --legend "pirateship(sig=50)" \
    --legend "pirateship(sig=100)" \
    --legend "pirateship(sig=200)" \
    --legend "pirateship(sig=500)" \
    --legend "pbft" \
    --legend "raft" \
