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
age = 12

class Timer:
    def __init__(self, func) -> None:
        self.func = func

    def __call__(self, *args, **kwargs):
        # 嵌套函数
        def inner():
            start = time.time()
            ret = self.func(*args, **kwargs)
            print(f'Time:{time.time() - start}')
            return ret
        if age > 12:
            return inner()
        else:
            print("xxx")


@Timer
def add(a, b):
    time.sleep(1)
    return a + b






if __name__ == '__main__':
    a = add(2, 3)
    print(a)