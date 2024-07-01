import binascii
import re
from datetime import datetime
from threading import RLock

import operator

from thingsboard_gateway.connectors.ps.abstract_ps_package_process import PsPackageProcess
from thingsboard_gateway.connectors.ps.ps_constant import SYS_IDENTIFIER, PACK_NUM, PackTypeEnum, DOT_LEN, \
    ProtocolTypeEnum, START_REGISTER_ADDRESS, FrameSchemeType, PS_PHOTO_INFO_HEAD_, PS_PHOTO_LAST_, PHOTO_DATA_, \
    PS_PHOTO_PAK_
from thingsboard_gateway.gateway.redis_client import RedisClient
from thingsboard_gateway.tb_utility.byte_math_cal import ByteMathCal
from thingsboard_gateway.connectors.ps.ps_global_variable import clients


class HeartbeatProcess(PsPackageProcess):

    def __init__(self, gateway, msg, server, serverUniqueFlag, deviceUniqueFlag):
        super(HeartbeatProcess, self).__init__(gateway, msg)
        self.serverUniqueFlag = serverUniqueFlag
        self.deviceUniqueFlag = deviceUniqueFlag
        self.server = server

    def process(self):
        global clients
        clients[str(self.server.getpeername())] = (self.deviceUniqueFlag, self.server)
        # clients.setdefault(str(self.server.getpeername()), (self.deviceUniqueFlag, self.server))

    def replyPackage(self):
        prefix = SYS_IDENTIFIER \
                 + '0017' \
                 + PACK_NUM \
                 + PackTypeEnum.LINK.value \
                 + DOT_LEN \
                 + self.serverUniqueFlag \
                 + DOT_LEN \
                 + self.deviceUniqueFlag \
                 + ProtocolTypeEnum.ANS_ACK.value

        reply_msg = prefix + ByteMathCal.yihuo(prefix)
        reply_msg = re.findall(".{2}", reply_msg)
        reply_msg = " ".join(reply_msg)
        self.server.send(bytes.fromhex(reply_msg))


class TwoCPackageProcess(PsPackageProcess):

    def __init__(self, gateway, msg, server, serverUniqueFlag, deviceUniqueFlag, config):
        super(TwoCPackageProcess, self).__init__(gateway, msg)
        self.server = server
        self.serverUniqueFlag = serverUniqueFlag
        self.deviceUniqueFlag = deviceUniqueFlag
        self.server = server
        self.config = config
        self.modbus_pak_content = self.msg[42:-6]

    def convert(self):
        holding_register = self.config['holdingRegister']
        register_address = START_REGISTER_ADDRESS
        sub_start_index = 0
        sub_end_index = 0
        content = self.modbus_pak_content[8:]
        for i in range(len(holding_register)):
            cur_address = holding_register[i]['address']
            cur_register_num = holding_register[i]['registerNum']
            cur_decimal_param_val = holding_register[i]['decimalPlaces']

            cur_refs = (holding_register[i]).get('refs', [])
            pre_address = holding_register[i - 1]['address']

            if register_address != cur_address and cur_address != START_REGISTER_ADDRESS:
                interval_address = cur_address - pre_address - 1
                sub_start_index = sub_start_index + interval_address * 4
                register_address = cur_address

            sub_end_index = sub_start_index + cur_register_num * 4
            param_val_str = content[sub_start_index:sub_end_index]
            sub_start_index = sub_end_index
            param_val = _convert_param_val(param_val_str, holding_register[i])

            if cur_decimal_param_val != 0:
                param_val = float(param_val) * float('1e-' + str(cur_decimal_param_val))

            (holding_register[i]).setdefault('paramVal', param_val)

            if len(cur_refs) != 0:
                param_val_arr = list(param_val)
                for j in range(len(param_val_arr)):
                    ref = cur_refs[j]
                    ref.setdefault('paramVal', param_val_arr[j])
                    # ref.setdefault('remark', ref['keyVal'])

            register_address = register_address + cur_register_num

        # print(holding_register)

    def replyPackage(self):
        modbus_pak_content_prefix = self.modbus_pak_content[0:8]

        prefix = SYS_IDENTIFIER \
                 + '001C' \
                 + PACK_NUM \
                 + PackTypeEnum.DIGITAL.value \
                 + DOT_LEN \
                 + self.serverUniqueFlag \
                 + DOT_LEN \
                 + self.deviceUniqueFlag \
                 + modbus_pak_content_prefix \
                 + ByteMathCal.cal_crc16(bytes.fromhex(modbus_pak_content_prefix))

        reply_msg = prefix + ByteMathCal.yihuo(prefix)
        reply_msg = re.findall(".{2}", reply_msg)
        reply_msg = " ".join(reply_msg)
        self.server.send(bytes.fromhex(reply_msg))


class PhotoProcess(PsPackageProcess):

    def __init__(self, gateway, msg, server):
        super(PhotoProcess, self).__init__(gateway, msg)
        self.server = server
        global clients
        self.deviceUniqueFlag = clients.get(str(self.server.getpeername()))[0]
        self.full_pak = PHOTO_DATA_ + "@" + self.deviceUniqueFlag
        self.last_pak = PS_PHOTO_LAST_ + "@" + self.deviceUniqueFlag
        self.head_pak = PS_PHOTO_INFO_HEAD_ + "@" + self.deviceUniqueFlag
        self.pak_flag = PS_PHOTO_PAK_ + str(self.server.getpeername())
        self.redis_client = RedisClient.get_redis_client()

    def convert(self):
        with RLock():
            try:
                content = ''
                self.redis_client.set(self.pak_flag, "true")
                if self.msg.startswith(SYS_IDENTIFIER):
                    self.redis_client.sadd(self.full_pak, self.msg)
                    self.redis_client.set(self.last_pak, self.msg)
                    content = self.msg
                else:
                    last_pak = self.redis_client.get(self.last_pak)
                    content = last_pak + self.msg
                    self.redis_client.set(self.last_pak, content)
                    if len(content) > 74:
                        self.redis_client.srem(self.full_pak, last_pak)
                        self.redis_client.sadd(self.full_pak, content)

                size = self.redis_client.scard(self.full_pak)
                if size == 1 and len(content) == 74:
                    self.redis_client.spop(self.full_pak)
                    self.redis_client.set(self.head_pak, content)

            except Exception as e:
                logging_error = e
                print(e)
                self.clear_photo_data()

    def process(self):
        try:
            all_photo_pak = ''
            sort_list = []
            last_pak = self.redis_client.get(self.last_pak)
            head_pak = self.redis_client.get(self.head_pak)
            last_pak_len = int(last_pak[56:58], 16)
            head_pak_len = int(head_pak[64:66], 16)

            if head_pak_len - 1 != last_pak_len:
                return

            check_bit = ByteMathCal.yihuo(last_pak[0:-2])
            last_hex = ByteMathCal.yihuo(last_pak[-2:])
            if check_bit.upper() == last_hex.upper():
                photo_data = self.redis_client.smembers(self.full_pak)
                for data in photo_data:
                    sort_list.append(
                        {"num": int(data[56:58], 16), "content": data[60:-6]}
                    )
                sort_list = sorted(sort_list, key=operator.itemgetter('num'))
                for data in sort_list:
                    all_photo_pak = all_photo_pak + ' ' + data['content']

                file_path = "/home/" + self.deviceUniqueFlag + str(datetime().now()) + ".jpeg"
                data = bytes.fromhex(all_photo_pak)
                with open(file_path, 'wb') as file:
                    file.write(data)
                self.clear_photo_data()
        except Exception as e:
            logging_error = e
            print(e)
            self.clear_photo_data()

    def clear_photo_data(self):
        self.redis_client.delete(self.head_pak)
        self.redis_client.delete(self.full_pak)
        self.redis_client.delete(self.last_pak)
        self.redis_client.delete(self.pak_flag)


class SingleWriteReply(PsPackageProcess):
    def __init__(self, gateway, msg):
        super(SingleWriteReply, self).__init__(gateway, msg)

    def process(self):
        pass


def _convert_param_val(param_val_str='', register=None):
    val = None
    if register['scheme'] == FrameSchemeType.NONE_FLAG.value:
        val = int(param_val_str, 16)
    elif register['scheme'] == FrameSchemeType.HAVE_FLAG.value:
        val = ''
    elif register['scheme'] == FrameSchemeType.DOUBLE.value:
        val = round(float(param_val_str), 2)
    else:
        unit = register['unit']
        if '按位解释' == unit:
            position_num = 0
            refs = register['refs']
            if len(refs) <= 8:
                position_num = 8
                param_val_str = param_val_str[0:2]
            else:
                position_num = 16

            val = ByteMathCal.hex_str_2_binary_str(param_val_str)

            if len(val) < position_num:
                replenish = len(refs) - len(val)
                for i in range(replenish):
                    val = val + "0"
    return val
