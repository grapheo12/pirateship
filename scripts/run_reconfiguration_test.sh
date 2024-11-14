#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

make

cat <<EOF > /tmp/reconf.trace
8 ADD_LEARNER node5
8 ADD_LEARNER node6
8 ADD_LEARNER node7
8 ADD_LEARNER node8
8 ADD_LEARNER node9
8 ADD_LEARNER node10
8 ADD_LEARNER node11
15 UPGRADE_FULL_NODE node5
15 UPGRADE_FULL_NODE node6
15 UPGRADE_FULL_NODE node7
15 UPGRADE_FULL_NODE node8
15 UPGRADE_FULL_NODE node9
15 UPGRADE_FULL_NODE node10
15 DOWNGRADE_FULL_NODE node2
15 DOWNGRADE_FULL_NODE node3
15 DOWNGRADE_FULL_NODE node4
25 DEL_LEARNER node2
25 DEL_LEARNER node3
35 DOWNGRADE_FULL_NODE node1
35 DOWNGRADE_FULL_NODE node5
35 DOWNGRADE_FULL_NODE node6
45 END
EOF


python3 scripts/run_remote_reconfiguration_test.py \
    -nt scripts/local_template.json \
    -ct scripts/local_client_template.json \
    -tr /tmp/reconf.trace \
    -ips ../nodelist.txt -eips ../extra_nodelist.txt \
    -i ../cluster_key.pem -r 5 -c 500

