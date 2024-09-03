#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

make

cat <<EOF > /tmp/reconf.trace
5 ADD_LEARNER node5
5 ADD_LEARNER node6
5 ADD_LEARNER node7
5 ADD_LEARNER node8
5 ADD_LEARNER node9
5 ADD_LEARNER node10
5 ADD_LEARNER node11
10 UPGRADE_FULL_NODE node5
10 UPGRADE_FULL_NODE node6
10 UPGRADE_FULL_NODE node7
10 UPGRADE_FULL_NODE node8
10 UPGRADE_FULL_NODE node9
10 UPGRADE_FULL_NODE node10
10 DOWNGRADE_FULL_NODE node2
10 DOWNGRADE_FULL_NODE node3
10 DOWNGRADE_FULL_NODE node4
20 DEL_LEARNER node2
20 DEL_LEARNER node3
30 DOWNGRADE_FULL_NODE node1
30 DOWNGRADE_FULL_NODE node5
30 DOWNGRADE_FULL_NODE node6
40 END
EOF


python3 scripts/run_remote_reconfiguration_test.py \
    -nt scripts/local_template.json \
    -ct scripts/local_client_template.json \
    -tr /tmp/reconf.trace \
    -ips ../nodelist.txt -eips ../extra_nodelist.txt \
    -i ../cluster_key.pem -r 2 -c 10

