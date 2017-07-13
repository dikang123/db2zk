#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import logging
from kazoo.client import KazooClient

class ZooKeeper:
    def __init__(self, endpoint, root="/dynamicDataSource/dbconfig"):
        logging.basicConfig()
        self.endpoint=endpoint
        self.zk=None
        self.root=root

    def connect(self):
        self.zk = KazooClient(hosts=self.endpoint)
        self.zk.start()

    def close(self):
        self.zk.stop()

    def init(self):
        if not self.zk.exists(self.root):
            self.zk.ensure_path(self.root)

    def getDb(self):
        if self.zk.exists(self.root):
            dbs = self.zk.get_children(self.root)
            return dbs
        else:
            return None

    def getRoute(self, dbs=None):
        if dbs is not None:
            routes = {}
            for db in dbs:
                path = "%s/%s" % (self.root, db)
                if self.zk.exists(path):
                    children = self.zk.get_children(path)
                    routes[db] = children
                else:
                    print "warning: something stranger"
            return routes
        else:
            return None

    def deleteDb(self, db):
        node = "%s/%s" % (self.root, db)
        print node
        if self.zk.exists(node):
            self.zk.delete(node, recursive=True)

    def deleteRoute(self, db, route):
        node = "%s/%s/%s" % (self.root, db, route)
        print node
        if self.zk.exists(node):
            self.zk.delete(node, recursive=True)

    def createDb(self, db, data):
        node = "%s/%s" % (self.root, db)
        if not self.zk.exists(node):
            print "create node: %s/%s" % (self.root, db)
            self.zk.create(node, data)
        else:
            print "node exist: %s/%s" % (self.root, db)

    def createRoute(self, db, route, data):
        node = "%s/%s/%s" % (self.root, db, route)
        if not self.zk.exists(node):
            print "create node: %s/%s/%s" % (self.root, db, route)
            self.zk.create(node, data)
        else:
            print "node exist: %s/%s/%s" % (self.root, db, route)

    def getApp(self):
        if self.zk.exists(self.root):
            app = self.zk.get_children(self.root)
            return app
        else:
            return None

    def getAppIp(self, app):
        node = "%s/%s" % (self.root, app)
        if self.zk.exists(node):
            ip = self.zk.get_children(node)
            return ip
        else:
            return None

    def getAppIpPath(self, app, ip):
        node = "%s/%s/%s" % (self.root, app, ip)
        if self.zk.exists(node):
            path = self.zk.get_children(node)
            return path
        else:
            return None

    def getUpdateTime(self, app, ip, position):
        node = "%s/%s/%s/%s" % (self.root, app, ip, position)
        isLeaf = False
        while not isLeaf:
            children = self.zk.get_children(node)
            if len(children) == 0:
                isLeaf = True
            else:
                node = "%s/%s" % (node, children[0])
        data, stat = self.zk.get(node)
        return (node, data)

# __main__
if __name__=='__main__':
    endpoint="127.0.0.1:2181"
    zookeeper = ZooKeeper(endpoint)
    zookeeper.connect()
    zookeeper.init()
    dbs = zookeeper.getDb()
    print dbs
    if dbs is not None:
        routes = zookeeper.getRoute(dbs)
        print routes
    zookeeper.close()
