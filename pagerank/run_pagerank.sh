#!/bin/bash

# This script runs pagerank naive from hibench.
# The parameters are defined in the script

# BLOCK_SIZE = 64M, 128M
# NUM_MAPS_PER_NODE = 2 , 4  Determined by PAGES in conf/configure.sh
# NUM_REDS_PER_NODE = .5, 1, 2

HADOOP_HOME=~/hadoop-2.2.0/

BLOCK_SIZE=( "64" "128" ) # in MB
NUM_MAPS=( "2" "4" )
NUM_REDS=( ".5" "1" "2" )
result_file="/mnt/result.txt"
PAGERANK="/mnt/HiBench-master/pagerank/bin"
PREPARE=$PAGERANK/prepare.sh
RUN=$PAGERAN/run.sh
NUM_MACHINES=$1

for i in "${BLOCK_SIZE[@]}"
do
for j in "${NUM_MAPS[@]}"
do
for k in "${NUM_REDS[@]}"
do
for l in 1 2 3
do
    ./stop_cluster.sh
    rm -r /mnt/hadoop/*
    ./start_cluster.sh
    rm $result_file
    out_file="~/file_$i_$j_$k_$l"
    TOTAL_SIZE=$(($i*$j*$NUM_MACHINES))
    PAGES=`echo "750000*$TOTAL_SIZE/500" | bc`
    $prepare $PAGES $NUM_MAPS $NUM_REDS
    clear_cache_nodes="./run_command_nodes.sh ~/hadoop-1.2.1/bin/clear_buf_cache.sh"
    $run $PAGES $NUM_MAPS $NUM_REDS
    cp $result_file $out_file 
done
done
done
done



