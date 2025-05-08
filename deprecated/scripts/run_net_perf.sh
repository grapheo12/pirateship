#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace


python3 scripts/run_net_perf.py \
    -nt scripts/local_template.json \
    -ct scripts/local_client_template.json \
    -ips ../nodelist.txt \
    -i ../cluster_key.pem \
    -r 1 -s 60 \
    -sz 4096
