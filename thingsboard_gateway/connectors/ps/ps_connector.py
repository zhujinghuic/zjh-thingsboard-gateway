from threading import Thread

from tests.connectors.connector_tests_base import log
from thingsboard_gateway.connectors.connector import Connector


# todo.conutie
class PsConnector(Connector, Thread):
    def __init__(self, gateway, config, connector_type):
        super().__init__()
        self.__stopped = None
        self.__gateway = gateway  # Reference to TB Gateway
        self._connector_type = connector_type  # Should be "mqtt"
        self.config = config  # mqtt.json contents

        self.__log = log

    def open(self):
        self.__stopped = False
        self.start()

    def close(self):
        self.__stopped = True
        try:
            self._client.disconnect()
        except Exception as e:
            log.exception(e)
        self._client.loop_stop()
        self.__log.info('%s has been stopped.', self.get_name())
