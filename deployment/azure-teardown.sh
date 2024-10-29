#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

CWD=$(pwd)

pushd $CWD/deployment/azure-tf

terraform destroy

popd

rm -f $CWD/deployment/cluster_key.pem
rm -f $CWD/deployment/extra_nodelist.txt
rm -f $CWD/deployment/nodelist_public.txt
rm -f $CWD/deployment/nodelist.txt

