#!/bin/bash

user="root"
ip="10.118.0.222"

function login_start(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    echo "aa"
    ulimit -n 65535
    cd /usr/local/python3.8.10/bin
    ./ray start  --address='10.118.0.221:6379' --num-cpus 40
    sleep 3
    exit
EOF
}


function login_stop(){
    ssh $user@$ip > /dev/null 2>&1 << EOF
    cd /usr/local/python3.8.10/bin
    ./ray stop
    sleep 3
    exit
EOF
}


ulimit -n 65535
ray start --head --port=6379 --num-cpus 40 
sleep 2
ssh $user@$ip > /dev/null 2>&1 << EOF
    ulimit -n 65535
    cd /usr/local/python3.8.10/bin
    ./ray start  --address='10.118.0.221:6379' --num-cpus 40
    sleep 1000
    exit
EOF
echo "aaa"
# k=1
# for((i=1; i<=22; i++))
# do
#     for((j=1; j<=40; j++))
#     do
#         python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'cluster'
#         sleep 2
#         k=$(($k+1))
#         if [ $k -gt 3 ]
#         then
#             login_stop
#             sleep 2
#             ray stop
#             sleep 2
#             ulimit -n 65535
#             ray start --head --port=6379 --num-cpus 40 
#             sleep 2
#             login_start
#             sleep 2
#             k=0
#         fi
#     done
# done 
