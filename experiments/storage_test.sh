#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 2000"


start_time=$(date -Ins)
# start_time='2023-10-30T20:35:31.196252+00:00'
RUN_CMD="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -r 1 -s 60 -up 10 -down 10 $ALL_CLIENTS"

jq '.consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 4000' scripts/local_template.json > /tmp/local_template.json

# Run with all features enabled
make
$RUN_CMD

# Run without storage
make pirateship_logger_nostorage
$RUN_CMD


end_time=$(date -Ins)
# end_time='2025-10-30T20:35:31.196252+00:00'

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 3 -c 3 -l node1 -up 40 -down 20 -o plot.png \
    --legend "pirateship" \
    --legend "pirateship_nostorage"
