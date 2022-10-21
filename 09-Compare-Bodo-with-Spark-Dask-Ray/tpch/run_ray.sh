#!/bin/bash
ulimit -n 65535
j=1
for((i=10; i<=22; i++))
do
    for((j=1; j<=100; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'single'
        sleep 5
        j=$j+1
        if [ $j -gt 5 ]
        then
            ray stop
            ray start --head --port=6379 --num-cpus 40 
            sleep 2
            $j=0
        fi
    done
done 