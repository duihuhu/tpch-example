#!/bin/bash

user="root"
ip="10.118.0.224"

function login_start(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    export RAY_BACKEND_LOG_LEVEL=error
    ulimit -n 65535
    ray start  --address='10.118.0.221:6379' --num-cpus 40
    exit
EOF
}


function login_stop(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    ray stop
    exit
EOF
}


ulimit -n 65535
export RAY_BACKEND_LOG_LEVEL=error
# ray start --head --port=6379 --num-cpus 40 
# login_start
# sleep 5
for((i=1; i<=22; i++))
do
    login_stop
    ray stop
    sleep 5
    export RAY_BACKEND_LOG_LEVEL=error
    ulimit -n 65535
    ray start --head --port=6379 --num-cpus 40 
    login_start
    for((j=1; j<=40; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'cluster'
        sleep 5
    done
done 
