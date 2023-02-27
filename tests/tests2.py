import sys
from os import curdir, listdir
from random import choice
from string import ascii_lowercase

from thingsboard_gateway import TBGatewayService
from thingsboard_gateway.gateway.statistics_service import StatisticsService

import time

import time


# python 继承多态

# python 继承多态

class A:
    def __init__(self):
        print("A")
        print("A -- end")


class B:
    def __init__(self):
        print("B")
        super().__init__()
        print("B -- end")

    def doit(self):
        print("Bb")


class C:
    def __init__(self):
        print("C")
        super().__init__()
        print("C -- end")

    def doit(self):
        print("Cc")


class D(B, C):
    def __init__(self):
        print("D")
        super().__init__()
        print("D -- end")

    def doit(self):
        super().doit()


if __name__ == '__main__':
    d = D()
    d.doit()


