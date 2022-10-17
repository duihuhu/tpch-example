#!/bin/bash
for((i=1; i<=100; i++))
do
    python3.9 -W ignore ray_queries_statical.py
    sleep 10
done