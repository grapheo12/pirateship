#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

CWD=$(pwd)

BUILD_CMD=$1
RUN_CMD=$2

# When running from an environment that is not part of the cluster's (extended) vnet
# we need to copy the repo over to one of the VMs that is in the vnet.
# Make sure to prepare the dev vm first.

# Then we build using BUILD_CMD
# Then run inside a screen using RUN_CMD

# Sample usage: sh deploy-dev-env.sh 'make' 'sh ~/pft-dev/scripts/run_remote_client_sweep.py ....'

pushd $CWD/deployment

echo $CWD

DEV_VM=$(grep clientpool_vm0 nodelist_public.txt | cut -f2)
DEV_VM=127.0.0.1
PORT=22 # TODO: hard coded in deploy-docker.py right now 2222 when using docker localy, 22 otherwise
echo $DEV_VM

DEV_USER=pftadmin
DEV_SSH_KEY=/home/ncrooks/.ssh/cluster_key.pem

ssh -o StrictHostKeyChecking=no -p $PORT -i $DEV_SSH_KEY $DEV_USER@$DEV_VM 'mkdir -p ~/pft-dev/logs'
scp -o StrictHostKeyChecking=no -r -q -P $PORT -i $DEV_SSH_KEY $CWD/benches $DEV_USER@$DEV_VM:~/pft-dev
scp -r -q -P $PORT -i $DEV_SSH_KEY $CWD/src $DEV_USER@$DEV_VM:~/pft-dev

# Need to clear pycache and venv locally before sending over
rm -r $CWD/scripts/venv
rm -r $CWD/scripts/__pycache__
scp -o StrictHostKeyChecking=no -r -q -P $PORT -i $DEV_SSH_KEY $CWD/scripts $DEV_USER@$DEV_VM:~/pft-dev
scp -o StrictHostKeyChecking=no -r -q -P $PORT -i $DEV_SSH_KEY $CWD/experiments $DEV_USER@$DEV_VM:~/pft-dev

# Need to redo virtualenv inside
ssh -o StrictHostKeyChecking=no  -p $PORT -i $DEV_SSH_KEY $DEV_USER@$DEV_VM 'virtualenv ~/pft-dev/scripts/venv && ~/pft-dev/scripts/venv/bin/pip3 install -r ~/pft-dev/scripts/requirements.txt'

# Copy nodelist and cluster_key
# Most experiments scripts expect them to be in ../nodelist.txt and ../cluster_key.pem respectively.
# We will respect that.
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY $DEV_SSH_KEY $DEV_USER@$DEV_VM:~/
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY nodelist.txt $DEV_USER@$DEV_VM:~/

# Extra nodelist may or may not be present
touch extra_nodelist.txt
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY extra_nodelist.txt $DEV_USER@$DEV_VM:~/

popd

# Makefile and other rust things
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY Makefile $DEV_USER@$DEV_VM:~/pft-dev
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY build.rs $DEV_USER@$DEV_VM:~/pft-dev
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY Cargo.toml $DEV_USER@$DEV_VM:~/pft-dev
scp -o StrictHostKeyChecking=no -q -P $PORT -i $DEV_SSH_KEY Cargo.lock $DEV_USER@$DEV_VM:~/pft-dev


# Now build
ssh  -o StrictHostKeyChecking=no -i $DEV_SSH_KEY -p $PORT $DEV_USER@$DEV_VM "cd ~/pft-dev && $BUILD_CMD"

# Now run in screen
ssh  -o StrictHostKeyChecking=no -i $DEV_SSH_KEY -p $PORT $DEV_USER@$DEV_VM "screen -dm bash -c 'cd ~/pft-dev && source scripts/venv/bin/activate && $RUN_CMD'"
