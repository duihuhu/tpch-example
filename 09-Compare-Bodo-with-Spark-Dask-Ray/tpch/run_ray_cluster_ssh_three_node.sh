#!/bin/bash

user="root"
ip="10.118.0.222"

ip2="10.118.0.224"

function login_start(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    echo "aa"
    ulimit -n 65535
    cd /usr/local/python3.8.10/bin
    ./ray start  --address='10.118.0.221:6379' --num-cpus 40 --disable-usage-stats
    exit
EOF
    sleep 2
    ssh $user@$ip2 > /dev/null 2>&1 << EOF
    echo "aa"
    ulimit -n 65535
    cd /usr/local/python3.8.10/bin
    ./ray start  --address='10.118.0.221:6379' --num-cpus 40 --disable-usage-stats
    exit
EOF
}


function login_stop(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    cd /usr/local/python3.8.10/bin
    ./ray stop
    exit
EOF
    sleep 2
    ssh $user@$ip2 > /dev/null 2>&1 << EOF
    cd /usr/local/python3.8.10/bin
    ./ray stop
    exit
EOF
}

export RAY_BACKEND_LOG_LEVEL=error
ulimit -n 65535
ray start --head --port=6379 --num-cpus 40 --disable-usage-stats  --include-dashboard=false
login_start
sleep 3

# for((i=1; i<=1; i++))
# do
#     for((j=1; j<=40; j++))
#     do
#         python3 -W ignore::DeprecationWarning ray_queries.py > log$j.txt
#         sleep 2
#         login_stop
#         ray stop
#         sleep 3
#         export RAY_BACKEND_LOG_LEVEL=error
#         ulimit -n 65535
#         ray start --head --port=6379 --num-cpus 40 --disable-usage-stats  --include-dashboard=false
#         login_start
#         sleep 3
#     done
# done 
