#!/usr/bin/python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import sys
from database import Database
from zookeeper import ZooKeeper
from wechat import Wechat

# vars
host="127.0.0.1"
port=3306
username="username"
password="password"
dbname="dbname"

endpoint="127.0.0.1:2181"

print "=====Sync Started====="
# query db and route record
try:
    database = Database(host, port, username, password, dbname)
    database.connect()
    mysql_dbs, mysql_routes = database.query()
    database.close()
except:
    print "Fatal: query from database error"
    sys.exit(-1)

# sync db data to zk
zookeeper = ZooKeeper(endpoint, root="/dynamicDataSource/dbconfig")
zookeeper.connect()
zookeeper.init()

# delete vanishing db node from zookeeper
zk_dbs_remain = []
zk_dbs = zookeeper.getDb()
for zk_db in zk_dbs:
    is_remain = False
    for mysql_db in mysql_dbs:
        address = mysql_db[2]
        port = mysql_db[3]
        endpoint = "%s:%d" % (address, port)
        if endpoint == zk_db:
            zk_dbs_remain.append(zk_db)
            is_remain = True
            break
    if not is_remain:
        print "Node %s is vanishing. So delete it from zookeeper" % zk_db
        zookeeper.deleteDb(zk_db)
    else:
        print "Node %s remained." % zk_db


# delete vanishing route node from zookeeper
if zk_dbs is not None and len(zk_dbs_remain) > 0:
    zk_routes = zookeeper.getRoute(zk_dbs_remain)
    for (zk_db, routes) in zk_routes.items():
        for route in routes:
            is_remain = False
            print "Check: %s/%s" % (zk_db, route)
            for mysql_route in mysql_routes:
                address = mysql_route[2]
                port = mysql_route[3]
                endpoint = "%s:%d" % (address, port)
                appName = mysql_route[13]
                routeKey = mysql_route[14]
                routeValue = "%s_%s" % (appName, routeKey)
                print "Endpoint: %s" % endpoint
                print "Route: %s" % routeValue
                if endpoint == zk_db and routeValue == route:
                    print "Match!"
                    is_remain = True
                    break
            if not is_remain:
                print "Node %s/%s is vanishing. So delete it from zookeeper" % (zk_db, route)
                zookeeper.deleteRoute(zk_db, route)
else:
    zk_routes = None

# create db node in zookeeper
zk_db_data="""data"""

for mysql_db in mysql_dbs:
    address = mysql_db[2]
    port = mysql_db[3]
    endpoint = "%s:%d" % (address, port)
    data = bytes(zk_db_data)
    zookeeper.createDb(endpoint, data)

# create route node in zookeeper
for mysql_route in mysql_routes:
    address = mysql_route[2]
    port = mysql_route[3]
    endpoint = "%s:%d" % (address, port)
    data = bytes(mysql_route[4])
    appName = mysql_route[13]
    routeKey = mysql_route[14]
    routeValue = "%s_%s" % (appName, routeKey)
    zookeeper.createRoute(endpoint, routeValue, data)

zookeeper.close()

print "=====Sync Finished====="
