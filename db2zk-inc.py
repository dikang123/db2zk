#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/8/24

import logging
import shelve
import signal
import sys
import time
from database import Database
from zookeeper import ZooKeeper

logging.basicConfig(level=logging.INFO, filename='log/db2zk-inc.log', filemode='a')

# vars
host="127.0.0.1"
port=3306
username="username"
password="password"
dbname="dbname"
endpoint="127.0.0.1:3181"
zk_db_data="""data"""

def stopSync(signum, stack):
    zookeeper.close()
    logging.info("=====Incremental Sync Killed %d =====" % signum)
    sys.exit(0)

signal.signal(signal.SIGINT, stopSync)
signal.signal(signal.SIGQUIT, stopSync)
signal.signal(signal.SIGTERM, stopSync)

zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/dbconfig")
zookeeper.connect()
zookeeper.init()

if __name__ == '__main__':
    logging.info("=====Incremental Sync Started=====")
    lastTime = 0

    while True:
        book = shelve.open('data/book.db', writeback=True)

        # query changed db and route record
        try:
            database = Database(host, port, username, password, dbname)
            database.connect()
            mysql_dbs, mysql_routes = database.queryUpdated(lastTime)
            database.close()
        except:
            logging.error("query from database ERROR")
            continue

        sNowTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        logging.info("next check time: %s" % sNowTime)
        lastTime = int(sNowTime)
        
        # sync db change to zookeeper
        if mysql_dbs is not None and len(mysql_dbs) > 0:
            for mysql_db in mysql_dbs:
                appName = mysql_db[1]
                address = mysql_db[2]
                port = mysql_db[3]
                actionTime = mysql_db[10]
                endpoint = "%s:%d" % (address, port)
                data = bytes(zk_db_data)
                zookeeper.createDb(appName, endpoint, data)
                if book.has_key(str(appName)):
                    account = book[str(appName)]
                    account['actionTime'] = actionTime
                    book[str(appName)] = account
                else:
                    account = {'actionTime': actionTime, 'checkTime': 0}
                    book[str(appName)] = account
        else:
            logging.info("No db record changed")

        # sync route change to zookeeper
        if mysql_routes is not None and len(mysql_routes) > 0:
            for mysql_route in mysql_routes:
                appName = mysql_route[1]
                address = mysql_route[2]
                port = mysql_route[3]
                endpoint = "%s:%d" % (address, port)
                data = bytes(mysql_route[4])
                routeKey = mysql_route[14]
                actionTime = mysql_route[18]
                routeValue = "%s_%s" % (appName.strip(), routeKey.strip())
                zookeeper.createRoute(appName, endpoint, routeValue, data)
                if book.has_key(str(appName)):
                    account = book[str(appName)]
                    account['actionTime'] = actionTime
                    book[str(appName)] = account
                else:
                    account = {'actionTime': actionTime, 'checkTime': 0}
                    book[str(appName)] = account
        else:
            logging.info("No route record changed")

        book.close()
        
        time.sleep(1)
    
    zookeeper.close()
