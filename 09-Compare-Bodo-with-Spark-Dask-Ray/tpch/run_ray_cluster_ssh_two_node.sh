#!/bin/bash

user="root"
ip="10.118.0.222"

ip2="10.118.0.224"

function login_start(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    ulimit -n 65535
    export RAY_BACKEND_LOG_LEVEL=error
    cd /usr/local/python3.8.10/bin
    ./ray start  --address='10.118.0.221:6379' --num-cpus 40 --disable-usage-stats
    exit
EOF
}

function login_start1(){
    ssh $user@$ip2 > /dev/null 2>&1 << EOF
    ulimit -n 65535
    export RAY_BACKEND_LOG_LEVEL=error
    ray start  --address='10.118.0.221:6379' --num-cpus 40 --disable-usage-stats
    exit
EOF
}

function login_stop(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    cd /usr/local/python3.8.10/bin
    ./ray stop
    exit
EOF
}

function login_stop1(){
    ssh $user@$ip2 > /dev/null 2>&1 << EOF
    ray stop
    exit
EOF
}


export RAY_BACKEND_LOG_LEVEL=error
ulimit -n 65535
ray start --head --port=6379 --num-cpus 40 --disable-usage-stats  --include-dashboard=false
sleep 5
# login_start
login_start1
sleep 6

for((i=1; i<=1; i++))
do
    for((j=1; j<=40; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries.py > log$j.txt
        sleep 2
        # login_stop
        login_stop1
        ray stop
        sleep 3
        export RAY_BACKEND_LOG_LEVEL=error
        ulimit -n 65535
        ray start --head --port=6379 --num-cpus 40 --disable-usage-stats  --include-dashboard=false
        sleep 5
        # login_start
        login_start1
        sleep 6
    done
done 