#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17
# Modify: 2017/8/24
# Modify: 2017/9/19

import datetime
import logging
import shelve
import sys
from database import Database
from zookeeper import ZooKeeper

logging.basicConfig(level=logging.INFO, filename='log/db2zk.log', filemode='w')

# vars
host="127.0.0.7"
port=3306
username="username"
password="password"
dbname="dbname"

endpoint="127.0.0.1:3181"

logging.info("=====Sync Started=====")

# only process last 6 hours changes or change from last synchronization
now = datetime.datetime.now()
    lastSync = now - datetime.timedelta(hours=6)
    tsRec = shelve.open('data/global.db', writeback=True)
    if tsRec.has_key('syncTime'):
            lastSync = tsRec['syncTime']
            tsRec['syncTime'] = now 
            tsRec.close()
            lastTime = int(lastSync.strftime('%Y%m%d%H%M%S'))
            logging.info("last sync time: %d" % lastTime)

# query db and route record
try:
    database = Database(host, port, username, password, dbname)
    database.connect()
    mysql_dbs, mysql_routes = database.query()
    mysql_dbs_updated, mysql_routes_updated = database.queryUpdated(lastTime)
    database.close()
except:
    logging.error("query from database ERROR")
    sys.exit(-1)

# sync db data to zk
zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/dbconfig")
zookeeper.connect()
zookeeper.init()

# delete vanishing app node from zookeeper
zk_apps_remain = []
zk_apps = zookeeper.getApplication()
for zk_app in zk_apps:
    is_remain = False
    for mysql_db in mysql_dbs:
        appName = mysql_db[1]
        if appName == zk_app:
            zk_apps_remain.append(zk_app)
            is_remain = True
            break
    if not is_remain:
        logging.info("Node %s is vanishing. So delete it from zookeeper" % zk_app)
        try:
            zookeeper.deleteApplication(zk_app)
        except:
            zookeeper.connect()
    else:
        logging.info("Node %s remained." % zk_app)

# delete vanishing db node from zookeeper
zk_pairs_remain = []
if zk_apps is not None and len(zk_apps_remain) > 0:
    zk_pairs = zookeeper.getDb(zk_apps_remain)
    for zk_pair in zk_pairs:
        is_remain = False
        for mysql_db in mysql_dbs:
            appName = mysql_db[1]
            address = mysql_db[2]
            port = mysql_db[3]
            endpoint = "%s:%d" % (address, port)
            if appName == zk_pair[0] and endpoint == zk_pair[1]:
                zk_pairs_remain.append(zk_pair)
                is_remain = True
                break
        if not is_remain:
            logging.info("Node %s/%s is vanishing. So delete it from zookeeper" % zk_pair)
            try:
                zookeeper.deleteDb(zk_pair)
            except:
                zookeeper.connect()
        else:
            logging.info("Node %s/%s remained." % zk_pair)

# delete vanishing route node from zookeeper
if len(zk_pairs_remain) > 0:
    zk_routes = zookeeper.getRoute(zk_pairs_remain)
    for (zk_pair, routes) in zk_routes.items():
        for route in routes:
            is_remain = False
            #logging.info("Check: %s/%s/%s" % (zk_pair[0], zk_pair[1], route))
            path = "%s/%s/%s" % (zk_pair[0], zk_pair[1], route)
            try:
                zk_data, stat = zookeeper.getNodeData(path)
            except:
                zookeeper.connect()
            data = zk_data.decode()
            for mysql_route in mysql_routes:
                appName = mysql_route[1]
                address = mysql_route[2]
                port = mysql_route[3]
                dbName = mysql_route[4]
                routeKey = mysql_route[14]

                endpoint = "%s:%d" % (address, port)
                routeValue = "%s_%s" % (appName, routeKey)

                if appName == zk_pair[0] and endpoint == zk_pair[1] and routeValue == route and dbName == data:
                    #logging.info("MATCHED: %s %s %s [%s]" % (appName, endpoint, routeValue, data))
                    is_remain = True
                    break
            if not is_remain:
                logging.info("Node %s/%s/%s [%s] is vanishing. So delete it from zookeeper" % (zk_pair[0], zk_pair[1], route, data))
                try:
                    zookeeper.deleteRoute(zk_pair, route)
                except:
                    zookeeper.connect()
                # create related route node
                for mysql_route in mysql_routes:
                    appName = mysql_route[1]
                    address = mysql_route[2]
                    port = mysql_route[3]
                    dbName = mysql_route[4]
                    #appName = mysql_route[13]
                    routeKey = mysql_route[14]

                    endpoint = "%s:%d" % (address, port)
                    routeValue = "%s_%s" % (appName, routeKey)

                    if appName == zk_pair[0] and routeValue == route:
                        logging.info("try to create related node: %s %s %s [%s]" % (appName, endpoint, routeValue, data))
                        try:
                            zookeeper.reCreateRoute(appName, endpoint, routeValue, data)
                        except:
                            zookeeper.connect()

logging.info("=====from db to zookeeper=====")

# create db node in zookeeper
for mysql_db in mysql_dbs_updated:
    appName = mysql_db[1]
    address = mysql_db[2]
    port = mysql_db[3]
    endpoint = "%s:%d" % (address, port)
    data = bytes(zk_db_data)
    zk_db_data="""data"""
    try:
        zookeeper.createDb(appName, endpoint, data)
    except:
        zookeeper.connect()

# create route node in zookeeper
for mysql_route in mysql_routes_updated:
    appName = mysql_route[1]
    address = mysql_route[2]
    port = mysql_route[3]
    endpoint = "%s:%d" % (address, port)
    data = bytes(mysql_route[4])
    routeKey = mysql_route[14]
    routeValue = "%s_%s" % (appName.strip(), routeKey.strip())
    try:
        zookeeper.createRoute(appName, endpoint, routeValue, data)
    except:
        zookeeper.connect()

zookeeper.close()
