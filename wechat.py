#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import logging
import requests

class Wechat:
    def __init__(self, url, tos, content):
        logging.basicConfig(level=logging.DEBUG)
        self.url=url
        self.tos=tos
        self.content=content

    def send(self):
        self.data={'tos':self.tos,'content':self.content}
        response = requests.post(self.url, data=self.data, timeout=5)
        code = response.status_code
        text = response.text
        logging.info("wechat api status:%d response:%s" % (code, text))

# __main__
if __name__=='__main__':
    url="http://127.0.0.1:8080/msg"
    tos="10086"
    content="[P0][Ignore][Python wechat alarming test]"
    wechat=Wechat(url, tos, content)
    wechat.send()
