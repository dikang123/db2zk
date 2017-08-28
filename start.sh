#!/bin/bash

source $HOME/.bashrc

if [[ -f RUNNING ]];then
    echo "db2zk is running!"
    exit 0
fi

touch RUNNING

python db2zk.py

rm -f RUNNING
