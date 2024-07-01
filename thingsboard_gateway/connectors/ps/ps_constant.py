from enum import Enum

SYS_IDENTIFIER = '123456'
PACK_NUM = '80'
DOT_LEN = '0B'
RTU_ADDRESS = '65'
START_REGISTER_ADDRESS = 35001

# ps redis key
PS_PHOTO_PAK_ = "PS_PHOTO_PAK_"
PS_PHOTO_LAST_ = "PS_PHOTO_LAST_"
PS_PHOTO_INFO_HEAD_ = "PS_PHOTO_INFO_HEAD_"
PHOTO_DATA_ = "PHOTO_DATA_"

class ProtocolTypeEnum(Enum):
    REGISTER = '33'
    CONTENT = '2C'
    ANS_ACK = '55'
    SINGLE_WRITE_REPLY = '05'
    PHOTOGRAPH = 'EB'

    @classmethod
    def getEnum(cls, msg):
        fun = msg[44:46]
        # var0 = msg[30:42]
        # var1 = msg[16:27]

        # if fun == ProtocolTypeEnum.PHOTOGRAPH.value or (var0 != '000000000010' and var1 != '000000000010'):
        #     return ProtocolTypeEnum.PHOTOGRAPH
        # else:
        for name, val in ProtocolTypeEnum.__members__.items():
            if fun == ProtocolTypeEnum.REGISTER.value:
                return ProtocolTypeEnum.REGISTER
            elif fun == ProtocolTypeEnum.CONTENT.value:
                return ProtocolTypeEnum.CONTENT
            elif fun == ProtocolTypeEnum.ANS_ACK.value:
                return ProtocolTypeEnum.ANS_ACK
            elif fun == ProtocolTypeEnum.SINGLE_WRITE_REPLY.value:
                return ProtocolTypeEnum.SINGLE_WRITE_REPLY
            elif fun == ProtocolTypeEnum.PHOTOGRAPH.value:
                return ProtocolTypeEnum.PHOTOGRAPH
            else:
                return "NONE"


class PackTypeEnum(Enum):
    DIGITAL = '01'
    LINK = '06'


class FrameSchemeType(Enum):
    NONE_FLAG = '无符号'
    HAVE_FLAG = '有符号'
    DOUBLE = 'double'
