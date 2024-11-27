#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

CWD=$(pwd)
pushd $CWD/deployment

DEV_VM=$(grep clientpool_vm0 nodelist_public.txt | cut -f 2)
DEV_USER=pftadmin
DEV_SSH_KEY=cluster_key.pem

popd


# Copy logs
now_time=$(date +%s)
log_dir=logs/$now_time
mkdir -p $log_dir
scp -r -i deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/plot.png logs/$now_time
scp -r -i deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/plot.png.pkl logs/$now_time
scp -r -i deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/logs logs/$now_time


# Cleanup
cat deployment/nodelist_public.txt | cut -f 2 | xargs -I{} ssh -i deployment/cluster_key.pem pftadmin@{} 'rm -r pft/*'
ssh -i deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM "rm -r pft-dev/logs/*"

