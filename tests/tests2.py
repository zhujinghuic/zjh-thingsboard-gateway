import sys
from os import curdir, listdir
from random import choice
from string import ascii_lowercase


from thingsboard_gateway import TBGatewayService
from thingsboard_gateway.gateway.statistics_service import StatisticsService

import time


class Decorator:
    def __init__(self, func):
        self.func = func

    def defer_time(self):
        time.sleep(5)
        print("延时结束了")

    def __call__(self, *args, **kwargs):
        self.defer_time()
        self.func()


@Decorator
def f1():
    print("延时之后我才开始执行")


class A:
    def doit(self):
        print("A")

class B(A):
    def doit(self):
        print("B")

class C(B):
    def doit(self):
        super().doit()



if __name__ == '__main__':
    a = 'a'
    b = 'ab'
    print(a in b)
    print(C.doit())