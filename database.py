#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import MySQLdb

class Database:
    def __init__(self, host, port, username, password, dbname):
        self.host=host
        self.port=port
        self.username=username
        self.password=password
        self.dbname=dbname
        db=None

    def connect(self):
        self.db = MySQLdb.connect(host=self.host, port=self.port, user=self.username, passwd=self.password, db=self.dbname, charset="utf8")

    def close(self):
        self.db.close()

    def test(self):
        cursor = self.db.cursor()
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        print "Database version : %s " % data

    def query(self):
        sql="SELECT * from tbl"
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            return results
        except:
            print "Error: unable to fecth data"

# __main__
if __name__=='__main__':
    host="127.0.0.1"
    port=3306
    username="username"
    password="password"
    dbname="dbname"
    database =Database(host, port, username, password, dbname)
    database.connect()
    database.query()
    database.close()
