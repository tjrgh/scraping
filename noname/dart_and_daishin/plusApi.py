import sys
import win32com.client
import ctypes
import time
from itertools import zip_longest

# PLUS 공통 OBJECT
code_manager = win32com.client.Dispatch('CpUtil.CpCodeMgr')
cp_status = win32com.client.Dispatch('CpUtil.CpCybos')


# PLUS 실행 기본 체크 함수
def init_plus_check():
    # 프로세스가 관리자 권한으로 실행 여부
    if ctypes.windll.shell32.IsUserAnAdmin():
        print('정상: 관리자권한으로 실행된 프로세스입니다.')
    else:
        print('오류: 일반권한으로 실행됨. 관리자 권한으로 실행해 주세요')
        return False

    # 연결 여부 체크
    if cp_status.IsConnect is 0:
        print("PLUS가 정상적으로 연결되지 않음. ")
        return False

    return True


# 종목 코드, 종목 명 가져오기
def get_stock_list(market_type):
    stocks = dict()

    plus_status = init_plus_check()
    if not plus_status:
        return stocks

    request_obj = win32com.client.Dispatch('CpSysDib.MarketEye')
    # 대신api로부터 종목 리스트 가져옴.
    if market_type == '코스피':
        code_list = code_manager.GetStockListByMarket(1)  # 코스피
    else:
        code_list = code_manager.GetStockListByMarket(2)  # 코스닥

    request_field = [0, 17]  # 종목코드, 종목명
    # 가져온 종목 리스트에 대해 데이터들 가져옴.
    for zip_code in zip_longest(*(iter(code_list),) * 200, fillvalue=None):
        codes = list(zip_code)
        while None in codes:
            codes.remove(None)
        request_obj.SetInputValue(0, request_field)
        request_obj.SetInputValue(1, codes)
        request_obj.BlockRequest() # 대신api로부터 데이터 받아옴.
        # stocks변수에 데이터 저장.
        for idx in range(request_obj.GetHeaderValue(2)):
            stocks[request_obj.GetDataValue(0, idx)] = request_obj.GetDataValue(1, idx)
    return stocks

# 입력받은 종목 코드 목록에 대한 여러 정보들을 담고있는 dict 반환.
def get_detail_data(code_list):
    request_field = [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        20,
        21,
        22,
        23,
        24,
        25,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        62,
        63,
        64,
        65,
        66,
        67,
        70,
        71,
        72,
        73,
        74,
        75,
        76,
        77,
        78,
        79,
        80,
        81,
        82,
        83,
        84,
        85,
        86,
        87,
        88,
        89,
        90,
        91,
        92,
        93,
        94,
        95,
        96,
        97,
        98,
        99,
        100,
        101,
        102,
        103,
        104,
        105,
        106,
        107,
        108,
        109,
        110,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        121,
        122,
        123,
        124,
        125,
        126,
        127,
        128,
        149,
        150,
        153,
        154,
        155,
        156,
        157,
        158,
        159,
        160,
        161,
        162,
        163,
        164,
        165
    ]

    request_obj = win32com.client.Dispatch('CpSysDib.MarketEye')

    request_field_1 = request_field[:64]
    request_field_2 = [0] + request_field[64:]

    data = dict()
    for zip_code in zip_longest(*(iter(code_list),) * 200, fillvalue=None):
        codes = list(zip_code)
        while None in codes:
            codes.remove(None)
        request_obj.SetInputValue(0, request_field_1)
        request_obj.SetInputValue(1, codes)
        request_obj.BlockRequest()

        for idx in range(request_obj.GetHeaderValue(2)):
            d = list()
            for idx2 in range(len(request_field_1)):
                d.append(request_obj.GetDataValue(idx2, idx))
            data[request_obj.getDataValue(0, idx)] = d

        request_obj.SetInputValue(0, request_field_2)
        request_obj.SetInputValue(1, codes)
        request_obj.BlockRequest()

        for idx in range(request_obj.GetHeaderValue(2)):
            code = request_obj.getDataValue(0, idx)
            for idx2 in range(1, len(request_field_2)):
                data[code].append(request_obj.GetDataValue(idx2, idx))
    return data

def get_history(code_list):
    request_obj = win32com.client.Dispatch('Dscbo1.StockWeek')
    request_obj.SetInputValue(0, code_list[0])
    request_obj.BlockRequest()

    dates = []
    opens = []
    highs = []
    lows = []
    closes = []
    diffs = []
    vols = []
    diffps = []
    foreign_vols = []
    foreign_diff = []
    foreign_p = []

    while True:
        ret = request_obj.BlockRequest()
        if request_obj.GetDibStatus() != 0:
            print("통신상태", request_obj.GetDibStatus(), request_obj.GetDibMsg1())
            return False

        cnt = request_obj.GetHeaderValue(1)
        for i in range(cnt):
            dates.append(request_obj.GetDataValue(0, i))
            opens.append(request_obj.GetDataValue(1, i))
            highs.append(request_obj.GetDataValue(2, i))
            lows.append(request_obj.GetDataValue(3, i))
            closes.append(request_obj.GetDataValue(4, i))

            temp = request_obj.GetDataValue(5, i)
            diffs.append(temp)
            vols.append(request_obj.GetDataValue(6, i))

            temp2 = request_obj.GetDataValue(10, i)
            if temp < 0:
                temp2 *= -1
            diffps.append(temp2)

            foreign_vols.append(request_obj.GetDataValue(7, i))  # 외인보유
            foreign_diff.append(request_obj.GetDataValue(8, i))  # 외인보유 전일대비
            foreign_p.append(request_obj.GetDataValue(9, i))  # 외인비중

            print('--- '
                  + str(len(dates))
                  + ': ' + str(request_obj.GetDataValue(0, i))
                  + ', ' + str(request_obj.GetDataValue(1, i))
                  + ' ---')
            time.sleep(0.26)

        if not request_obj.Continue:
            break

    week = {
        'close': closes,
        'diff': diffs,
        'diffp': diffps,
        'vol': vols,
        'open': opens,
        'high': highs,
        'low': lows,
        'for_v': foreign_vols,
        'for_d': foreign_diff,
        'for_p': foreign_p,
    }
    return week, dates
