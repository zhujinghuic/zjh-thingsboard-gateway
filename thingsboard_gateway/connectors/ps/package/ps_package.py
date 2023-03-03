from abc import ABC


class PsPackage(ABC):

    def __init__(self, psFrameEnum) -> None:
        self.psFrameEnum = psFrameEnum
        self.psProtocolPrefix = ""
        self.deviceUniqueFlag = ""
        self.serverUniqueFlag = ""

    def set_send_pack_frame_prefix(self):
        pass