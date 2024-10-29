#!/bin/bash

# Copyright (c) Shubham Mishra. All rights reserved.
# Licensed under the MIT License.


curl https://sh.rustup.rs -sSf | sh -s -- -y


# By default bashrc is not read when using ssh 'command' mode
# So we need to remove/comment out those lines.

cp ideal_bashrc $HOME/.bashrc