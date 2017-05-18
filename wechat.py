#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/5/17

import requests

class Wechat:
    def __init__(self, url, tos, content):
        self.url=url
        self.tos=tos
        self.content=content


    def send(self):
        self.data={'tos':self.tos,'content':self.content}
        response = requests.post(self.url, data=self.data)
        code = response.status_code
        text = response.text
        print text
        return code


# __main__
if __name__=='__main__':
    url="http://127.0.0.1:8080/wechat"
    tos="10086"
    content="[P0][db2zk TEST!!]"
    wechat=Wechat(url, tos, content)
    wechat.send()
