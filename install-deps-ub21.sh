#!/bin/bash

set -x

sudo apt update
sudo apt install -y cmake ninja-build g++-11
sudo apt install -y libibverbs-dev librdmacm-dev
sudo apt install -y libpmem-dev librpmem-dev libpmemblk-dev libpmemlog-dev libpmemobj-dev libpmempool-dev libpmempool-dev
sudo apt install -y libboost-all-dev
sudo apt install -y default-jre
sudo apt install -y libisal-dev
