import base64
import binascii
import sys
from datetime import datetime
from os import curdir, listdir
from random import choice
from string import ascii_lowercase

import operator

import re

from thingsboard_gateway import TBGatewayService
from thingsboard_gateway.connectors.ps.ps_constant import ProtocolTypeEnum, SYS_IDENTIFIER, PACK_NUM, PackTypeEnum, \
    DOT_LEN
from thingsboard_gateway.gateway.statistics_service import StatisticsService

import time

import time


# python 继承多态

# python 继承多态
from thingsboard_gateway.tb_utility.byte_math_cal import ByteMathCal
from thingsboard_gateway.test3 import Szy206Parse


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


class E():
    def __init__(self):
        print("E")

    def doit(self):
        global a
        a['a'] = 1

class F():
    def __init__(self):
        print("F")

    def doit(self):
        global a
        # del a['a']
        print(a['a'])

a = {}

if __name__ == '__main__':
    a = Szy206Parse()
    a.data_parser("68 1A 68 B3 53 03 81 0D 00 C0 00 59 00 00 00 82 61 10 00 00 01 10 60 00 00 45 08 09 0A 35 16".replace(" ", ""))






