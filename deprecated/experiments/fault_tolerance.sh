#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 500"


jq '.consensus_config.max_backlog_batch_size = 500 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 4000' scripts/local_template.json > /tmp/local_template.json
make pirateship_logger_evil

python3 scripts/run_remote_equivocation_test.py \
    -nt /tmp/local_template.json -ct scripts/local_client_template.json \
    -ips ../nodelist.txt -i ../cluster_key.pem -s 120 -up 1 -down 1 $ALL_CLIENTS \
    -bsb 15000
