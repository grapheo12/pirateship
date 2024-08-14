#!/bin/bash
set -o xtrace

ALL_CLIENTS="-c 1 -c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
# # ALL_CLIENTS="-c 200 -c 500 -c 700 -c 1000"
# ALL_CLIENTS="-c 50 -c 500"

start_time=$(date -Ins)
# start_time='2024-08-14T19:25:39.697429+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt scripts/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 30 -up 2 -down 2 $ALL_CLIENTS"
# RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt scripts/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 1 -s 30 -up 2 -down 2 $ALL_CLIENTS"

# Run cochin quorum_diversity_k = 3, signature_max_delay_blocks = 500
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 500' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD


# Run cochin quorum_diversity_k = 3, signature_max_delay_blocks = 50
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD

# Run cochin quorum_diversity_k = 3, signature_max_delay_blocks = 5
jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 5' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD

# Run cochin quorum_diversity_k = 5, signature_max_delay_blocks = 5
jq '.consensus_config.quorum_diversity_k = 5 | .consensus_config.signature_max_delay_blocks = 5' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD


# Run cochin quorum_diversity_k = 1, signature_max_delay_blocks = 5
jq '.consensus_config.quorum_diversity_k = 1 | .consensus_config.signature_max_delay_blocks = 5' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD


# Run cochin quorum_diversity_k = 7, signature_max_delay_blocks = 5
jq '.consensus_config.quorum_diversity_k = 7 | .consensus_config.signature_max_delay_blocks = 5' scripts/local_template.json > /tmp/local_template.json
mv /tmp/local_template.json scripts/local_template.json
make
$RUN_CMD

# Run chained_pbft
make chained_pbft_logger
$RUN_CMD

# Run lucky_raft
make lucky_raft_logger
$RUN_CMD


end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 2 -l node1 -up 2 -down 2 -o plot.png \
    --legend "cochin(k=3, sig=500)+byz" \
    --legend "cochin(k=3, sig=50)+byz" \
    --legend "cochin(k=3, sig=5)+byz" \
    --legend "cochin(k=5, sig=5)+byz" \
    --legend "cochin(k=1, sig=5)+byz" \
    --legend "cochin(k=7, sig=5)+byz" \
    --legend "pbft" \
    --legend "raft" \
