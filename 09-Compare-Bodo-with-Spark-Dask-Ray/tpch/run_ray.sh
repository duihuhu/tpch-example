#!/bin/bash
ulimit -n 65535
ray start --head --port=6379 --num-cpus 40 
k=1
for((i=1; i<=22; i++))
do
    for((j=1; j<=15; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'single'
        sleep 5
        k=$(($k+1))
        if [ $k -gt 3 ]
        then
            ray stop
            sleep 2
            ray start --head --port=6379 --num-cpus 40 
            sleep 2
            k=0
        fi
    done
done 