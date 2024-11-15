#!/bin/sh

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.

set -o xtrace

CWD=$(pwd)
SETUP=$1
SETUP_FILE=setups/$1.tfvars

pushd $CWD/deployment/azure-tf

terraform init -upgrade
terraform plan -out main.tfplan --var-file=$SETUP_FILE
terraform apply "main.tfplan"

rm -f ../cluster_key.pem
terraform output --raw cluster_private_key > ../cluster_key.pem
chmod 400 ../cluster_key.pem

RG_NAME=$(terraform output --raw resource_group_name)

az vm list-ip-addresses\
    --resource-group $RG_NAME\
    --query "[].{Name:virtualMachine.name, IP:virtualMachine.network.privateIpAddresses[0]}"\
    --output tsv > ../nodelist.txt


az vm list-ip-addresses\
    --resource-group $RG_NAME\
    --query "[].{Name:virtualMachine.name, IP:virtualMachine.network.publicIpAddresses[0].ipAddress}"\
    --output tsv > ../nodelist_public.txt

sleep 1

DEV_VM=$(grep clientpool_vm0 ../nodelist_public.txt | cut -f 2)
DEV_USER=pftadmin
DEV_SSH_KEY=../cluster_key.pem

scp -q -i $DEV_SSH_KEY ../__prepare-dev-env.sh $DEV_USER@$DEV_VM:~/
scp -q -i $DEV_SSH_KEY ../ideal_bashrc $DEV_USER@$DEV_VM:~/
ssh -i $DEV_SSH_KEY $DEV_USER@$DEV_VM 'sh ~/__prepare-dev-env.sh'


popd

