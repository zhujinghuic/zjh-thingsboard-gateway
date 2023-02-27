import sys
from os import curdir, listdir
from random import choice
from string import ascii_lowercase


from thingsboard_gateway import TBGatewayService
from thingsboard_gateway.gateway.statistics_service import StatisticsService

import time

import time

# 装饰器
# https://blog.csdn.net/m0_67575344/article/details/124256673
class Timer:
    def __init__(self, a=None) -> None:
        self.a = a

    def __call__(self, func):
        print("调用_call_函数")
        # 嵌套函数
        def inner(a, b):
            rs = func(a, b)
            return rs
        return inner


def mul(a, b):
    return a * b

"""相当于 add = Time("aaa")(add)  先实例化Timer,然后再调用call函数，返回嵌套函数inner，相当于返回add = inner(a,b)
"""
@Timer("aaa")
def add(a, b):
    return a + b


if __name__ == '__main__':
    a = add(2, 3)
    print(a)