import datetime


def char_2_string(ch, decimalPoint, isNegative):
    # 将char 2位分为一组 并倒序排列
    length = len(ch) // 2
    dataArray = [None] * length
    for i in range(0, len(ch), 2):
        c = ch[i] + ch[i + 1]
        dataArray[length - 1] = c
        length -= 1

    stringBuilder = []

    for j in range(len(dataArray)):
        if j == 0:
            if isNegative:
                # 第一位代表+ -值
                if int(dataArray[j][0]) != 0:
                    stringBuilder.append("-")
                else:
                    stringBuilder.append("+")
                stringBuilder.append(dataArray[j][1])
            else:
                stringBuilder.append(dataArray[j])
        else:
            stringBuilder.append(dataArray[j])

    resultDataStr = "".join(stringBuilder)
    # 添加小数点
    if decimalPoint != 0:
        resultDataStr = resultDataStr[:decimalPoint] + "." + resultDataStr[decimalPoint:]

    return resultDataStr

def parent_data_info(szy206InfoDTO, dataContent, oneDataLength, decimalPoint, isNegative, mainBody, mainHead):
    dataLength = len(dataContent)
    if dataLength % oneDataLength == 0:
        cycleNum = 0
        while dataLength > 0:
            try:
                data = dataContent[cycleNum * oneDataLength:(cycleNum + 1) * oneDataLength]
                cycleNum += 1
                dataLength -= oneDataLength
                dataChar = list(data)
                result = char_2_string(dataChar, decimalPoint, isNegative)
                # 数据值
                resultData = float(result)
                mainBody["B3"] = resultData
                mainHead["B3"] = "流量（水量）"

                print(
                    "遥测地址：" + szy206InfoDTO.getStationAddr() + " 类型：" + "流量（水量）" + " 结果：" + result)
            except Exception as e:
                print(e)

def parse_content(content, szy206_info_dto):
    main_body = {}
    main_head = {}
    index = len(content)

    if index > 18:
        time = content[index - 10:index - 2]
        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        day = int(time[6:8])
        hour = int(time[4:6])
        minutes = int(time[2:4])
        seconds = int(time[0:2])
        date_time_str = f"{year}/{month}/{day} {hour}:{minutes}:{seconds}"
        date_time = datetime.datetime.strptime(date_time_str, "%Y/%m/%d %H:%M:%S")
        time_str = date_time.strftime("%Y-%m-%d %H:%M:%S")
        szy206_info_dto.dataTime = time_str

        main_body["dataTime"] = time_str
        main_head["dataTime"] = "Data Time Mean"

        index -= 10
        warning_status = content[index - 8:index]
        index -= 8
        data_content = content[0:index]

        if szy206_info_dto.dataType == "0001B":
            one_data_length = 8
            decimal_point = 5
            parent_data_info(szy206_info_dto, data_content, one_data_length, decimal_point, True, main_body,
                             main_head)
        elif szy206_info_dto.dataType == "0010B":
            # 每N个数组 代表一个数据
            one_data_length = 10
            # 小数点位数
            decimal_point = 7
            data_length = len(data_content)
            if data_length % one_data_length == 0:
                cycle_num = 0
                flag = 2
                while data_length > 0:
                    try:
                        data = data_content[cycle_num * one_data_length:(cycle_num + 1) * one_data_length]
                        cycle_num += 1
                        data_length -= one_data_length
                        data_char = list(data)
                        # 实时流量在前 累计流量在后
                        if flag % 2 == 0:
                            # 流量实时数据
                            result = char_2_string(data_char, decimal_point, True)
                            result_data = float(result)
                            main_body["flow"] = result_data
                            main_head["flow"] = "流量"
                            print(
                                "遥测地址：" + szy206_info_dto.get_station_addr() + " 类型：流量" + " 结果：" + str(result_data))
                        else:
                            # 累计流量(水量)
                            result = char_2_string(data_char, 0, True)
                            result_data = float(result)
                            main_body["countFlow"] = result_data
                            main_head["countFlow"] = "累计流量"
                            print(
                                "遥测地址：" + szy206_info_dto.get_station_addr() + " 类型：累计流量" + " 结果：" + str(
                                    result_data))
                        flag += 1

                    except Exception as e:
                        print(e)




class Szy206InfoDTO:
    def __init__(self):
        self.startStr = ""
        self.dataType = ""
        self.stationAddr = ""
        self.deviceId = ""
        self.functionCode = ""
        self.content = ""
        self.crcCode = ""
        self.endStr = ""
        self.dataTime = ""
        self.mainBody = {}
        self.mainHead = {}


class Szy206Parse:

    def __init__(self):
        print("")

    def data_parser(self, msg):
        szy206_info_dto = Szy206InfoDTO()

        index = 0
        start_str = msg[index:index + 6]
        szy206_info_dto.startStr = start_str
        index += 6

        data_type = msg[index:index + 2]
        szy206_info_dto.dataType = data_type
        index += 2

        station_addr = msg[index:index + 10]
        szy206_info_dto.stationAddr = station_addr
        szy206_info_dto.deviceId = station_addr
        index += 10

        function_code = msg[index:index + 2]
        szy206_info_dto.functionCode = function_code
        index += 2

        content = msg[index:-4]
        szy206_info_dto.content = content
        index = len(msg) - 4

        crc_code = msg[index:index + 2]
        szy206_info_dto.crcCode = crc_code
        index += 2

        end_str = msg[index:index + 2]
        szy206_info_dto.endStr = end_str

        parse_content(content, szy206_info_dto)

        return szy206_info_dto





