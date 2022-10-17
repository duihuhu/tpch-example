#!/bin/bash
for((i=1; i<=100; i++))
do
    python3.9 ray_queries_statical.py $i
    sleep 10
done