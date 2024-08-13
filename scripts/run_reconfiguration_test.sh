#!/bin/sh

make

cat <<EOF > /tmp/reconf.trace
5 ADD_LEARNER node5 127.0.0.1:3005 node5.pft.org MCowBQYDK2VwAyEAOPjJTenaHOeFpsiNDWLbSefnCbnx7+1LFwMSgQr5w8g=
5 ADD_LEARNER node6 127.0.0.1:3006 node6.pft.org MCowBQYDK2VwAyEAOPjJTenaHOeFpsiNDWLbSefnCbnx7+1LFwMSgQr5w8g=
10 DEL_LEARNER node5
30 END
EOF

python3 scripts/run_remote_reconfiguration_test.py \
    -nt scripts/local_template.json \
    -ct scripts/local_client_template.json \
    -tr /tmp/reconf.trace \
    -ips ../nodelist.txt -i ../cluster_key.pem -r 2 -c 500

