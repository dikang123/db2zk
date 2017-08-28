#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/8/23

import logging
import re

from zookeeper import ZooKeeper
from wechat import Wechat
from mail import Mail

logging.basicConfig(level=logging.INFO, filename='log/check-conn.log', filemode='a')

def isIP(target):
    pattern = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d])')
    match = pattern.match(target)
    if match == None:
        return False
    else:
        return True

def checkLoss(zk, loss, path=""):
    children = zk.getChildren(path)
    if children is None or len(children) == 0:
        segments = path.split("/")
        if len(segments) > 1:
            first = segments[0]
            second = segments[1]
            if isIP(first):
                app = "all"
                ip = first
                conn_data, conn_stat = zk.getNodeData(path)
                if conn_data is None or conn_data == "":
                    loss.append("Normal global node LOSS connection --- AppName:%s IP:%s Node:%s" % (app, ip, path))
                    logging.info("Normal global node LOSS connection --- AppName:%s IP:%s Node:%s" % (app, ip, path))
            elif isIP(second) and len(segments) > 2:
                app = first
                ip = second
                conn_data, conn_stat = zk.getNodeData(path)
                if conn_data is None or conn_data == "":
                    loss.append("Normal app node LOSS connection --- AppName:%s IP:%s Node:%s" % (app, ip, path))
                    logging.info("Normal app node LOSS connection --- AppName:%s IP:%s Node:%s" % (app, ip, path))
            else:
                pass
    else:
        for child in children:
            if path == "":
                subpath = child
            else:
                subpath = "%s/%s" % (path, child)
            checkLoss(zk, loss, subpath)

if __name__ == '__main__':
    logging.info("==========Check Start==========")

    endpoint = "127.0.0.1:3181"
    zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/service_conn_state")

    zookeeper.connect()
    zookeeper.init()
    
    loss = []
    checkLoss(zookeeper, loss)

    if loss is not None and len(loss) > 0:
        receiver = "yourname@domain.com"
        subject = "db2zk Connection LOSS Warning"
        mail = Mail(receiver, subject, loss)
        mail = mail.send()

        url="http://127.0.0.1:8080/msg"
        tos="10086"
        content="[P0][TEST][db2zk connection LOST warning, please check the email!]"
        wechat=Wechat(url, tos, content)
        wechat.send()
    
    zookeeper.close()
