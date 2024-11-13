#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

ALL_CLIENTS="-c 700"

RUN_CMD="python3 scripts/run_remote_equivocation_test.py -nt /tmp/local_template.json -ct scripts/local_client_template.json -ips ../nodelist.txt -i ../cluster_key.pem -s 30 -up 8 -down 10 $ALL_CLIENTS"

jq '.consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 50' scripts/local_template.json > /tmp/local_template.json
make pirateship_logger_evil
$RUN_CMD