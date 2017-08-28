#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/8/25

import logging
import re
import shelve
import time
from zookeeper import ZooKeeper
from wechat import Wechat
from mail import Mail

logging.basicConfig(level=logging.INFO, filename='log/check-update.log', filemode='a')

def isIP(target):
    pattern = re.compile(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d])')
    match = pattern.match(target)
    if match == None:
        return False
    else:
        return True

def checkUpdate(timeout, zk, actionTime,  path=""):
    trunck = "/dynamicDataSource/service_dbconfig_state"
    children = zk.getChildren(path)
    if children is None or len(children) == 0:
        segments = path.split("/")
        if len(segments) > 1:
            first = segments[0]
            second = segments[1]
            if isIP(first):
                # normal global node
                conn_data, conn_stat = zk.getNodeData(path)
                if conn_data != "":
                    update_data, update_stat = zk.getNodeDataWithRoot(trunck, path)
                    if update_data is None or update_data == "":
                        timeout.append("normal global node DEAD --- ConnTime:%s UpdateTime:%s actionTime:None" % (conn_data, update_data))
                        logging.info("normal global node DEAD --- ConnTime:%s UpdateTime:%s actionTime:None" % (conn_data, update_data))
                    else:
                        updateTime = int(time.strftime("%Y%m%d%H%M%S",time.strptime(str(update_data), "%Y-%m-%d %H:%M:%S")))
                        if updateTime - actionTime > 300: 
                            timeout.append("normal global node Timeout --- ConnTime:%s UpdateTime:%s actionTime:%d" % (conn_data, update_data, actionTime))
                            logging.info("normal global node Timeout --- ConnTime:%s UpdateTime:%s actionTime:%d" % (conn_data, update_data, actionTime))
            elif isIP(second) and len(segments) > 2:
                # normal app node
                conn_data, conn_stat = zk.getNodeData(path)
                if conn_data != "":
                    update_data, update_stat = zk.getNodeDataWithRoot(trunck, path)
                    if update_data is None or update_data == "":
                        timeout.append("normal app node DEAD --- ConnTime:%s UpdateTime:%s actionTime:None" % (conn_data, update_data))
                        logging.info("normal app node DEAD --- ConnTime:%s UpdateTime:%s actionTime:None" % (conn_data, update_data))
                    else:
                        updateTime = int(time.strftime("%Y%m%d%H%M%S",time.strptime(str(update_data), "%Y-%m-%d %H:%M:%S")))
                        if updateTime - actionTime > 300 or updateTime < actionTime: 
                            timeout.append("normal app node Timeout --- ConnTime:%s UpdateTime:%s actionTime:%d" % (conn_data, update_data, actionTime))
                            logging.info("normal app node Timeout --- ConnTime:%s UpdateTime:%s actionTime:%d" % (conn_data, update_data, actionTime))
            else:
                pass
    else:
        for child in children:
            if path == "":
                subpath = child
            else:
                subpath = "%s/%s" % (path, child)
            checkUpdate(timeout, zk, actionTime, subpath)


if __name__ == '__main__':
    logging.info("==========Check Start==========")

    endpoint="127.0.0.1:3181"

    zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/service_conn_state")
    zookeeper.connect()
    zookeeper.init()

    sNowTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    checkTime = int(sNowTime)
    globalTime = checkTime

    task = []
    book = shelve.open('data/book.db', writeback=True)
    logging.info("current book: %s", book)


    for app in book:
        actionTime = book[app]['actionTime']
        lastTime = book[app]['checkTime']
        if actionTime >= lastTime:
            task.append(app)
            if actionTime < globalTime:
                globalTime = actionTime
    logging.info("task: %s", task)
    
    timeout = []
    if task is None or len(task) == 0:
        pass
    else:
        children = zookeeper.getChildren("")
        if children is None or len(children) == 0:
            pass
        else:
            for child in children:
                if isIP(child):
                    checkUpdate(timeout, zookeeper, globalTime, child)
                if child in task:
                    checkUpdate(timeout, zookeeper, book[child]['actionTime'], child)
                    book[child]['checkTime'] = checkTime
    logging.info("current book: %s", book)
    book.close()

    if timeout is not None and len(timeout) > 0:
        receiver = "yourname@domain.com"
        subject = "db2zk APP response TIMEOUT Warning"
        mail = Mail(receiver, subject, timeout)
        mail = mail.send()

        url="http://127.0.0.1:8080/msg"
        tos="10086"
        content="[P0][TEST][db2zk APP response TIMEOUT warning, please check the email!]"
        wechat=Wechat(url, tos, content)
        wechat.send()
    
    zookeeper.close()
