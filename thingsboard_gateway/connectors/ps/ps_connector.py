from socket import socket
from threading import Thread

from tests.connectors.connector_tests_base import log
from thingsboard_gateway.connectors.connector import Connector

CONN_ADDR = ('192.168.88.108', 6232)
MAX_CONN = 65535


# todo.conutie
class PsConnector(Connector):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__stopped = None
        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"
        self.config = config  # mqtt.json contents
        # self.__log = log
        self.server = None
        self.socket_list = {} # 已连结的客户端

    def open(self):
        self.__stopped = False
        try:
            server = socket.socket()
            self.server = server
            server.bind(CONN_ADDR)
            server.setblocking(False)  # 非阻塞
            server.listen(65535)  # 表示一个客户端最大的连接数
        except Exception as e:
            log.error(f'启动ps服务端失败, 失败原因', e)
            self.close()

    def close(self):
        self.__stopped = True
        try:
            self.server.close()
        except Exception as e:
            log.error(f'ps关闭服务失败, 失败原因', e)
            log.info('ps服务已关闭')
