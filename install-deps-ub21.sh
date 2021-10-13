#!/bin/bash

set -x

sudo apt update
sudo apt install -y cmake clang ninja-build
sudo apt install -y libibverbs-dev librdmacm-dev
sudo apt install -y libpmem-dev librpmem-dev libpmemblk-dev libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev
sudo apt install -y libboost-all-dev
