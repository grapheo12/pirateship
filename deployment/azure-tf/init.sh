#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.


# Docker keys and repos
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update

# Install Docker
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Non-root run
sudo usermod -aG docker pftadmin

# Restart on reboot
sudo systemctl enable docker.service
sudo systemctl enable containerd.service

# Pirateship dependencies
sudo apt-get install -y screen
sudo apt-get install -y build-essential cmake clang llvm pkg-config
sudo apt-get install -y jq
sudo apt-get install -y protobuf-compiler
sudo apt-get install -y linux-tools-common linux-tools-generic linux-tools-`uname -r`
sudo apt-get install -y net-tools
sudo apt-get install -y ca-certificates curl libssl-dev
sudo apt-get install -y librocksdb-dev libprotobuf-dev
sudo apt-get install -y python3-pip python3-virtualenv


# Increase open file limits

echo "*	soft	nofile	50000" >> /etc/security/limits.conf
echo "*	hard	nofile	50000" >> /etc/security/limits.conf


# Mount the data disk


sudo mkfs.ext4 /dev/sdc
sudo mkdir /data
sudo mount /dev/sdc /data
sudo chmod -R 777 /data
