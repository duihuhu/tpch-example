#!/bin/bash
ulimit -n 65535
export RAY_BACKEND_LOG_LEVEL=error
#ray start --head --port=6379 --num-cpus 40 
k=1
for((i=1; i<=18; i++))
do
    ray stop
    sleep 2
    ray start --head --port=6379 --num-cpus 40
    sleep 2
    for((j=1; j<=50; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'single'
        sleep 5
    done
done