#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17
# Modify: 2017/8/24

import logging
import MySQLdb

class Database:
    def __init__(self, host, port, username, password, dbname):
        logging.basicConfig()
        self.host=host
        self.port=port
        self.username=username
        self.password=password
        self.dbname=dbname
        db=None

    def connect(self):
        self.db = MySQLdb.connect(host=self.host, port=self.port, user=self.username, passwd=self.password, db=self.dbname, charset="utf8")
        self.db.autocommit(True)

    def close(self):
        self.db.close()

    def version(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        print "Database version : %s " % data

    def query(self):
        sqlDb="SELECT 12 fields FROM dbtable;"
        sqlRoute="SELECT 20 fields FROM dbtable JOIN routetable ON dbtable.dbID=routetable.dbID ORDER BY dbtable.dbID, routetable.routeKey;"
        try:
            # query db records from mysql
            cursor = self.db.cursor()
            cursor.execute(sqlDb)
            dbs = cursor.fetchall()
            # query route records from mysql
            cursor.execute(sqlRoute)
            routes = cursor.fetchall()
            # return data
            return dbs, routes
        except Exception,e:
            logging.error("query database error")
            return None, None

    def queryUpdated(self, actionTime):
        sqlDb="SELECT 12 fields FROM dbtable WHERE dbtable.actionTime>%d ORDER BY dbtable.dbID;" % actionTime
        sqlRoute="SELECT 20 fields FROM dbtable JOIN routetable ON dbtable.dbID=routetable.dbID WHERE routetable.actionTime>%d ORDER BY dbtable.dbID, routetable.routeKey;" % actionTime
        try:
            # query db records from mysql
            cursor = self.db.cursor()
            cursor.execute(sqlDb)
            dbs = cursor.fetchall()
            # query route records from mysql
            cursor.execute(sqlRoute)
            routes = cursor.fetchall()
            # return data
            logging.info("updated db: %s", dbs)
            logging.info("updated route: %s", routes)
            return dbs, routes
        except Exception,e:
            logging.error("query database error")
            return None, None

# __main__
if __name__=='__main__':
    host="127.0.0.1"
    port=3306
    username="username"
    password="password"
    dbname="dbname"
    database = Database(host, port, username, password, dbname)
    database.connect()
    database.version()
    database.queryUpdated(20170801000000)
    database.close()
