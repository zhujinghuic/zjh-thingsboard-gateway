import socket
from threading import Thread

import selectors

from tests.connectors.connector_tests_base import log
from thingsboard_gateway.connectors.connector import Connector

CONN_ADDR = ('192.168.88.108', 6232)
MAX_CONN = 65535

"""  客户端未断开时关闭服务端如何同时释放客户端连接

  """
# todo.conutie
class PsConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__stopped = None
        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"
        self.config = config  # mqtt.json contents
        # self.__log = log
        # self.server = None
        self.socket_list = {} # 已连结的客户端
        self.server = None
        self.e_poll = None

    def open(self):
        self.__stopped = False
        try:
            self.server = socket.socket()
            self.server.bind(CONN_ADDR)
            self.server.setblocking(False)  # 非阻塞
            self.server.listen(65535)  # 表示一个客户端最大的连接数
            self.start() # 开启线程处理接收事件
        except Exception as e:
            log.exception(e)
            # log.error(f'启动ps服务端失败, 失败原因', e.)
            self.close()

    def close(self):
        self.__stopped = True
        try:
            self.server.close()
        except Exception as e:
            log.exception(e)
            log.info('ps服务已关闭')

    def run(self):
        self.receive()

    def receive(self):
        self.e_poll = selectors.EpollSelector()  # window没有epoll使用selectors.DefaultSelector()实现多路复用
        self.e_poll.register(self.server, selectors.EVENT_READ, self.acc_conn)
        while True:
            # 事件循环不断地调用select获取被激活的socket
            events = self.e_poll.select()
            # print(events)
            """[(SelectorKey(fileobj= < socket.socket
             laddr = ('127.0.0.1',9999) >,……data = < function acc_conn at 0xb71b96ec >), 1)]
            """
            for key, mask in events:
                call_back = key.data
                # print(key.data)
                call_back(key.fileobj)

    def acc_conn(self, server):
        conn, addr = server.accept()
        print('连接地址-', addr)
        # 也有注册一个epoll
        self.e_poll.register(conn, selectors.EVENT_READ, self.recv_data)

    def recv_data(self, server):
        data = server.recv(1024)

        if data:
            # print('接收的数据是：%s' % data.decode())
            # print('接收的数据是：', str(data)[2:-1].replace('\\x', ' '))
            print('接收的数据是：', str(data)[2:-1].replace('\\x', ' ').upper())
            # print(bytes.hex(data).upper())
            server.send(data)
        else:
            self.e_poll.unregister(server)
            server.close()

    def get_name(self):
        return "ps Connector"

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass

