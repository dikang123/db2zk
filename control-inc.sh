#!/bin/bash
source $HOME/.bashrc

WORKSPACE=$(cd $(dirname $0)/; pwd)
cd ${WORKSPACE}

app=db2zk-inc
pidfile=${app}.pid
logfile=${app}.log

function check_pid() {
    if [[ -f ${pidfile} ]];then
        pid=$(cat ${pidfile})
        if [[ -n ${pid} ]]; then
            running=$(ps -p $pid|grep -v "PID TTY" |wc -l)
            return ${running}
        fi
    fi
    return 0
}

function start() {
    check_pid
    running=$?
    if [[ ${running} -gt 0 ]];then
        echo -n "${app} now is running already, pid="
        cat ${pidfile}
        return 1
    fi

    nohup python ${app} &>/dev/null &
    echo $! > ${pidfile}
    echo "${app} started..., pid=$!"
}

function stop() {
    pid=$(cat ${pidfile})
    kill ${pid}
    rm ${pidfile}
    echo "${app} stoped..."
}

function restart() {
    stop
    sleep 1
    start
}

function help() {
    echo "$0 pid|start|stop|restart"
}

function pid() {
    cat ${pidfile}
}

if [ "$1" == "" ]; then
    help
elif [ "$1" == "stop" ];then
    stop
elif [ "$1" == "start" ];then
    start
elif [ "$1" == "restart" ];then
    restart
elif [ "$1" == "pid" ];then
    pid
else
    help
fi
