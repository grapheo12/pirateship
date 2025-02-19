#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

CWD=$(pwd)
pushd $CWD/deployment

DEV_VM=$(grep clientpool-vm0 nodelist_public.txt | cut -d' ' -f2)
DEV_USER=pftadmin
DEV_SSH_KEY=../cluster_key.pem
PORT=22 # TODO: hard coded in deploy-docker.py right now 2222 when using docker localy, 22 otherwise

popd


# Copy logs
now_time=$(date +%s)
log_dir=logs/$now_time
mkdir -p $log_dir
scp -o StrictHostKeyChecking=no -r -i -P $PORT deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/plot.png logs/$now_time
scp -o StrictHostKeyChecking=no -r -i -P $PORT deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/plot.png.pkl logs/$now_time
scp -o StrictHostKeyChecking=no -r -i -P $PORT deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM:~/pft-dev/logs logs/$now_time


# Cleanup
cat deployment/nodelist_public.txt | cut -f 2 | xargs -I{} ssh -i deployment/cluster_key.pem pftadmin@{} 'rm -r pft/*'
ssh -i deployment/$DEV_SSH_KEY $DEV_USER@$DEV_VM "rm -r pft-dev/logs/*"

