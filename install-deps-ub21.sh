#!/bin/bash

set -x

sudo apt update
sudo apt install -y cmake ninja-build clang
sudo apt install -y libibverbs-dev librdmacm-dev
sudo apt install -y libpmem-dev ndctl
sudo apt install -y libboost-all-dev
sudo apt install -y libisal-dev
sudo apt install -y libgrpc++-dev protobuf-compiler-grpc

# for YCSB benchmarking
sudo apt install -y default-jre
