#!/bin/bash
set -o xtrace

ALL_CLIENTS="-c 1 -c 10 -c 50 -c 100 -c 200 -c 500 -c 700 -c 1000"
# ALL_CLIENTS="-c 200 -c 500 -c 700 -c 1000"

start_time=$(date -Ins)
# start_time='2024-08-07T09:19:23.307485+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt scripts/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 3 -s 120 -up 2 -down 2 $ALL_CLIENTS"

# Run cochin
make
$RUN_CMD

# Run jolteon
make jolteon_logger
$RUN_CMD

# Run chained_pbft
make chained_pbft_logger
$RUN_CMD

# Run diverse_raft
make diverse_raft_logger
$RUN_CMD

# Run signed_raft
make signed_raft_logger
$RUN_CMD

# Run lucky_raft
make lucky_raft_logger
$RUN_CMD



end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 2 -l node1 -up 2 -down 2 -o plot.png \
    --legend "cochin-20s" \
    --legend "jolteon-20s" \
    --legend "chained_pbft-20s" \
    --legend "diverse_raft" \
    --legend "signed_raft" \
    --legend "lucky_raft"
