import selectors
import socket
from threading import Thread

from tests.connectors.connector_tests_base import log
from thingsboard_gateway.connectors.connector import Connector
from thingsboard_gateway.connectors.ps.abstract_ps_package_process import PsPackageProcess
from thingsboard_gateway.connectors.ps.ps_constant import ProtocolTypeEnum, PS_PHOTO_PAK_
from thingsboard_gateway.connectors.ps.ps_global_variable import clients
from thingsboard_gateway.connectors.ps.ps_package_process import HeartbeatProcess, TwoCPackageProcess, PhotoProcess, \
    SingleWriteReply
from thingsboard_gateway.gateway.redis_client import RedisClient

CONN_ADDR = ('192.168.88.108', 6232)
MAX_CONN = 65535

"""  客户端未断开时关闭服务端如何同时释放客户端连接

  """


def get_device_unique_flag(msg):
    return msg[16:28]


def get_server_unique_flag(msg):
    return msg[30:42]


def handler(psPackageProcess: PsPackageProcess):
    psPackageProcess.convert()
    psPackageProcess.process()
    psPackageProcess.replyPackage()
    psPackageProcess.send_msg_to_tb()


class PsConnector(Connector, Thread):

    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__stopped = None
        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"
        self.config = config  # mqtt.json contents
        # self.__log = log
        self.connServer = None
        self.e_poll = None
        self.redis_client = RedisClient.get_redis_client()

    def open(self):
        self.__stopped = False
        try:
            self.connServer = socket.socket()
            self.connServer.bind(CONN_ADDR)
            self.connServer.setblocking(False)  # 非阻塞
            self.connServer.listen(65535)  # 表示一个客户端最大的连接数
            self.start()  # 开启线程处理接收事件
        except Exception as e:
            log.exception(e)
            # log.error(f'启动ps服务端失败, 失败原因', e.)
            self.close()

    def close(self):
        self.__stopped = True
        try:
            self.connServer.close()
        except Exception as e:
            log.exception(e)
            log.info('ps服务已关闭')

    def run(self):
        self.receive()

    def receive(self):
        self.e_poll = selectors.EpollSelector()  # window没有epoll使用selectors.DefaultSelector()实现多路复用
        self.e_poll.register(self.connServer, selectors.EVENT_READ, self.acc_conn)
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

    def recv_data(self, socket_client):
        data = socket_client.recv(1024)

        if data:
            # print(RedisClient.get_redis_client().get('test'))
            # print('接收的数据是：%s' % data.decode())
            # print('接收的数据是：', str(data)[2:-1].replace('\\x', ' '))
            # hexStr = str(data)[2:-1].replace('\\x', ' ').upper()
            text_list = bytes.hex(data).upper()
            # text_list = re.findall(".{2}", text_list)
            # new_hexStr = " ".join(text_list)
            new_hexStr = text_list
            print('接收的数据是：', new_hexStr)
            handler(self.decode(socket_client, new_hexStr))
            # print(bytes.hex(data).upper())
            # server.send(data)
        else:
            global clients
            if len(clients) > 0:
                del clients[str(socket_client.getpeername())]
            self.e_poll.unregister(socket_client)
            socket_client.close()

    # todo.counties
    def decode(self, socket_client, msg) -> PsPackageProcess:
        if self.redis_client.exists(PS_PHOTO_PAK_ + str(socket_client.getpeername())):
            return PhotoProcess(self.__gateway, msg, socket_client)

        if len(msg) == 46:
            if ProtocolTypeEnum.REGISTER.value == msg[-4:-2]:
                return HeartbeatProcess(self.__gateway, msg, socket_client, get_server_unique_flag(msg), get_device_unique_flag(msg))
        elif len(msg) > 46:
            if ProtocolTypeEnum.getEnum(msg) == ProtocolTypeEnum.CONTENT:
                return TwoCPackageProcess(self.__gateway, msg, socket_client, get_server_unique_flag(msg), get_device_unique_flag(msg),
                                          self.config)
            elif ProtocolTypeEnum.getEnum(msg) == ProtocolTypeEnum.SINGLE_WRITE_REPLY:
                return SingleWriteReply(self.__gateway, msg)
            elif ProtocolTypeEnum.getEnum(msg) == ProtocolTypeEnum.PHOTOGRAPH:
                return PhotoProcess(self.__gateway, msg, socket_client)

        # else:
        #     return PhotoProcess(self.__gateway, msg)

    def get_name(self):
        return "ps Connector"

    def is_connected(self):
        pass

    def on_attributes_update(self, content):
        pass

    def server_side_rpc_handler(self, content):
        pass
