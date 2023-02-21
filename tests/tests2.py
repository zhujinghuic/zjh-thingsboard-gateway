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




if __name__ == '__main__':
    a = {
        'a': 2,
        'b': 3
    }
    b = ('a', 4)
    print(a.items())
    new_rpc_request_in_progress = {key: value for key, value in
                                   a.items() if value != 2}
    print(new_rpc_request_in_progress)