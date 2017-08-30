#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17
# Modify: 2017/8/24

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

    def getApplication(self):
        if self.zk.exists(self.root):
            apps = self.zk.get_children(self.root)
            return apps
        else:
            return None

    def getDb(self, apps=None):
        if apps is not None:
            pairs = []
            for app in apps:
                path = "%s/%s" % (self.root, app)
                if self.zk.exists(path):
                    dbs = self.zk.get_children(path)
                    for db in dbs:
                        pair = (app, db)
                        pairs.append(pair)
            return pairs
        else:
            return None

    def getRoute(self, pairs=None):
        if pairs is not None:
            routes = {}
            for pair in pairs:
                path = "%s/%s/%s" % (self.root, pair[0], pair[1])
                if self.zk.exists(path):
                    children = self.zk.get_children(path)
                    routes[pair] = children
                else:
                    logging.warning("%s is strange", path)
            return routes
        else:
            return None

    def deleteApplication(self, app):
        node = "%s/%s" % (self.root, app)
        if self.zk.exists(node):
            logging.info("delete node %s" % node)
            self.zk.delete(node, recursive=True)

    def deleteDb(self, pair):
        node = "%s/%s/%s" % (self.root, pair[0], pair[1])
        if self.zk.exists(node):
            logging.info("delete node %s" % node)
            self.zk.delete(node, recursive=True)

    def deleteRoute(self, pair, route):
        node = "%s/%s/%s/%s" % (self.root, pair[0], pair[1], route)
        if self.zk.exists(node):
            logging.info("delete node %s" % node)
            self.zk.delete(node, recursive=True)

    def createDb(self, appName, endpoint, data):
        applicationNode = "%s/%s" % (self.root, appName)
        dbNode = "%s/%s/%s" % (self.root, appName, endpoint)
        if not self.zk.exists(applicationNode):
            logging.info("create node: %s/%s" % (self.root, appName))
            self.zk.create(applicationNode, b"")
            logging.info("create node: %s/%s/%s" % (self.root, appName, endpoint))
            self.zk.create(dbNode, data)
        elif not self.zk.exists(dbNode):
            logging.info("create node: %s/%s/%s" % (self.root, appName, endpoint))
            self.zk.create(dbNode, data)
        else:
            logging.info("node exist: %s/%s/%s" % (self.root, appName, endpoint))

    def createRoute(self, appName, endpoint, route, data):
        node = "%s/%s/%s/%s" % (self.root, appName, endpoint, route)
        if not self.zk.exists(node):
            logging.info("create node: %s/%s/%s/%s" % (self.root, appName, endpoint, route))
            self.zk.create(node, data)
        else:
            zk_data, zk_stat = self.zk.get(node)
            if zk_data != data: 
                logging.info("node exists, data changed: %s/%s/%s/%s" % (self.root, appName, endpoint, route))
                self.zk.set(node, data)
            else:
                logging.info("node exists, data keep: %s/%s/%s/%s" % (self.root, appName, endpoint, route))

    # for checkers
    def getChildren(self, branch=""):
        node = self.root
        if branch != "":
            node = "%s/%s" % (self.root, branch)
        if self.zk.exists(node):
            children = self.zk.get_children(node)
            return children
        else:
            return None

    def getNodeData(self, branch):
        node = self.root
        if branch != "":
            node = "%s/%s" % (self.root, branch)
        if self.zk.exists(node):
            return self.zk.get(node)
        else:
            return (None, None)

    def getNodeDataWithRoot(self, trunck, branch):
        node = trunck
        if branch != "":
            node = "%s/%s" % (trunck, branch)
        if self.zk.exists(node):
            return self.zk.get(node)
        else:
            return (None, None)

# __main__
if __name__=='__main__':
    endpoint="127.0.0.1:3181"
    zookeeper = ZooKeeper(endpoint)
    zookeeper.connect()
    zookeeper.init()
    zookeeper.deleteApplication("supplyChainBillServer")
    zookeeper.close()
