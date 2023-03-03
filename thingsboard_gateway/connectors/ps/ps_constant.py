from enum import Enum

SYS_IDENTIFIER = "123456"
PACK_NUM = "80"
DOT_LEN = "0B"
RTU_ADDRESS = "65"

class PsFrameEnum(Enum):
    REGISTER = "33"
    CONTENT = "2C"
    ANS_ACK = "55"
    WRITE = "05"
    PHOTOGRAPH = "EB"

    @classmethod
    def get_frame_type(cls, msg:str) -> Enum.value:
        fun = msg[44, 46]
        return PsFrameEnum.__getitem__(fun)

