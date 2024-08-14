#!/bin/sh

make

cat <<EOF > /tmp/reconf.trace
5 ADD_LEARNER node5
5 ADD_LEARNER node6
10 DEL_LEARNER node5
5 END
EOF

python3 scripts/run_remote_reconfiguration_test.py \
    -nt scripts/local_template.json \
    -ct scripts/local_client_template.json \
    -tr /tmp/reconf.trace \
    -ips ../nodelist.txt -eips ../extra_nodelist.txt \
    -i ../cluster_key.pem -r 2 -c 500
