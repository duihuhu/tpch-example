#!/bin/bash
for((i=5; i<=22; i++))
do
    for((j=1; j<=2; j++))
    do
        python3.9 ray_queries_statical.py $i 'single'
        sleep 10
    done
done 