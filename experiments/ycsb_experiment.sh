#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

# ALL_CLIENTS="-c 1 -c 10 -c 100 -c 500 -c 700 -c 1000 -c 1200 -c 1500 -c 2000"
# ALL_CLIENTS="-c 100 -c 200 -c 300 -c 500 -c 700 -c 900 -c 1000 -c 1200 -c 1500 -c 1800 -c 2000 -c 2500"
ALL_CLIENTS="-c 2000"
# ALL_CLIENTS="-c 1000"

start_time=$(date -Ins)
# start_time='2024-08-07T10:39:54.859389+00:00'
RUN_CMD_A="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb_a.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 120 -up 40 -down 10 $ALL_CLIENTS"
RUN_CMD_B="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb_b.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 120 -up 40 -down 10 $ALL_CLIENTS"
RUN_CMD_C="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb_c.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 120 -up 40 -down 10 $ALL_CLIENTS"
RUN_CMD_D="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb_d.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 120 -up 40 -down 10 $ALL_CLIENTS"
RUN_CMD_E="python3 scripts/run_remote_client_sweep.py -nt /tmp/local_template.json -ct scripts/local_client_template_ycsb_e.json -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -s 120 -up 40 -down 10 $ALL_CLIENTS"


jq '.consensus_config.max_backlog_batch_size = 1000 | .consensus_config.quorum_diversity_k = 3 | .consensus_config.signature_max_delay_blocks = 100 | .consensus_config.liveness_u = 2 | .consensus_config.view_timeout_ms = 4000' scripts/local_template.json > /tmp/local_template.json

make pirateship_kvs
$RUN_CMD_A

make pirateship_kvs
$RUN_CMD_B

make pirateship_kvs
$RUN_CMD_C

make pirateship_kvs
$RUN_CMD_D

make pirateship_kvs
$RUN_CMD_E

make signed_raft_kvs
$RUN_CMD_A --max_nodes 5

make signed_raft_kvs
$RUN_CMD_B --max_nodes 5

make signed_raft_kvs
$RUN_CMD_C --max_nodes 5

make signed_raft_kvs
$RUN_CMD_D --max_nodes 5

make signed_raft_kvs
$RUN_CMD_E --max_nodes 5

end_time=$(date -Ins)

# Plot together
python3 scripts/plot_time_range_client_sweep.py \
    --path logs --end $end_time --start $start_time \
    -r 2 -c 3 -l node1 -up 40 -down 10 -o plot.png \
    --legend "pirateship:(0R:100W)" \
    --legend "pirateship:(25R:75W)" \
    --legend "pirateship:(50R:50W)" \
    --legend "pirateship:(75R:25W)" \
    --legend "pirateship:(100R:0W)" \
    --legend "signed_raft:(0R:100W)" \
    --legend "signed_raft:(25R:75W)" \
    --legend "signed_raft:(50R:50W)" \
    --legend "signed_raft:(75R:25W)" \
    --legend "signed_raft:(100R:0W)" 