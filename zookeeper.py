#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import logging
from kazoo.client import KazooClient

class ZooKeeper:
    def __init__(self, endpoint):
        logging.basicConfig()
        self.endpoint=endpoint
        self.zk=None

    def connect(self):
        self.zk = KazooClient(hosts=self.endpoint)
        self.zk.start()

    def close(self):
        self.zk.stop()

    def test(self):
        if not self.zk.exists("/test/dynamicDataSource"):
            self.zk.ensure_path("/test/dynamicDataSource")
        self.zk.create("/test/dynamicDataSource/dbconfig")

# __main__
if __name__=='__main__':
    endpoint="127.0.0.1:2181"
    zookeeper = ZooKeeper(endpoint)
    zookeeper.connect()
    zookeeper.test()
    zookeeper.close()
