#!/bin/bash

user="root"
ip="10.118.0.222"
passwd="hucc@1qaz"

function login_start(){
    /usr/bin/expect <<-EOF
    set timeout 10
    spawn ssh $user@$ip
    expect {
        "yes/no" { send "yes\n"; exp_continue }
        "password" { send "$passwd\r" }
    }
    expect "#" { send "ulimit -n 65535\r"}
    expect "#" { send "cd /usr/local/python3.8.10/bin\r"}
    expect "#" { send "./ray start  --address='10.118.0.221:6379' --num-cpus 40\r"}
    expect "#" { send "sleep 3\r"}
    expect "#" { send "exit\r"}
EOF
}


function login_stop(){
    /usr/bin/expect <<-EOF
    set timeout 10
    spawn ssh $user@$ip
    expect {
        "yes/no" { send "yes\n"; exp_continue }
        "password" { send "$passwd\r" }
    }
    expect "#" { send "ulimit -n 65535\r"}
    expect "#" { send "cd /usr/local/python3.8.10/bin\r"}
    expect "#" { send "./ray stop\r"}
    expect "#" { send "sleep 3\r"}
    expect "#" { send "exit\r"}
EOF
}


ulimit -n 65535
ray start --head --port=6379 --num-cpus 40 
sleep 3
login_start
sleep 3
k=1
for((i=1; i<=22; i++))
do
    for((j=1; j<=40; j++))
    do
        python3 -W ignore::DeprecationWarning ray_queries_statical.py $i 'cluster'
        sleep 3
        k=$(($k+1))
        if [ $k -gt 3 ]
        then
            login_stop
            sleep 3
            ray stop
            sleep 3
            ray start --head --port=6379 --num-cpus 40 
            sleep 3
            login_start
            sleep 3
            k=0
        fi
    done
done 
