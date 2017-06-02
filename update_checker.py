#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/31

import datetime
from zookeeper import ZooKeeper
from wechat import Wechat
from falcon import Falcon

endpoint="127.0.0.1:2181"
threshold = 720
now = datetime.datetime.now()

zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/service_dbconfig_state")
zookeeper.connect()
zookeeper.init()

zk_apps = zookeeper.getApp()
if zk_apps is None or len(zk_apps) == 0:
    pass
else:
    for app in zk_apps:
        zk_ips = zookeeper.getAppIp(app)
        if zk_ips is None or len(zk_ips) == 0:
            pass
        else:
            for ip in zk_ips:
                zk_paths = zookeeper.getAppIpPath(app, ip)
                if zk_paths is None or len(zk_paths) == 0:
                    print "%s/%s has no path" % (app, ip)
                else:
                    for path in zk_paths:
                        node, timestamp = zookeeper.getUpdateTime(app, ip, path)
                        print "%s: %s" % (node, timestamp)
                        update_time = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                        delta = (now - update_time).seconds / 60
                        if delta > threshold:
                            print "%s timeout: %d(minutes)" % (node, delta)

zookeeper.close()
