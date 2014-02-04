#!/bin/bash

# This script runs pagerank naive from hibench.
# The parameters are defined in the script

# BLOCK_SIZE = 64M, 128M
# NUM_MAPS_PER_NODE = 2 , 4  Determined by PAGES in conf/configure.sh
# NUM_REDS_PER_NODE = .5, 1, 2

BLOCK_SIZE=( "64" "128" ) # in MB
NUM_MAPS=( "2" "4" )
NUM_REDS=( ".5", "1" "2" )

NUM_MACHINES=$1

for i in "${BLOCK_SIZE[@]}"
do
for j in "${NUM_MAPS[@]}"
do
for k in "${NUM_REDS[@]}"
do
    TOTAL_SIZE=$(($i*$j*$NUM_MACHINES))
    PAGES=`echo ".00066*$TOTAL_SIZE" | bc`
    echo -e "$PAGES \n"
done
done
done
#750000   500 MB


