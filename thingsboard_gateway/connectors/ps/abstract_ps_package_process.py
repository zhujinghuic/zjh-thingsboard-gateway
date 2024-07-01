from abc import ABC
from datetime import datetime


class PsPackageProcess(ABC):

    def __init__(self, gateway, msg):
        self.gateway = gateway
        self.msg = msg

    def convert(self):
        pass

    def process(self):
        pass

    def replyPackage(self):
        pass

    def send_msg_to_tb(self):
        pass
        # self.gateway.tb_client.client.send_telemetry({"ts": str(datetime.now()), "values": self.msg})
        # self._published_events.put(self.tb_client.client.gw_send_telemetry(final_device_name,
        #                                                                    devices_data_in_event_pack[
        #                                                                        device]["telemetry"]))
