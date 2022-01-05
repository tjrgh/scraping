import datetime
import random
import time
from . import constant_var as constant

def report_error(msg=""):
    date_time = time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time()))
    with open(constant.error_file_path + "/wait_log_" +
              time.strftime("%Y-%m-%d", time.localtime(time.time())) + ".txt", "a", encoding="UTF-8") as f:
        f.write(date_time + "_" + msg + "\n")

def wait(wait_time, term=1, search_count=None, search_count_max=None):
    result = {"search_count": search_count}

    # 해당 날짜마다 다른 랜덤 대기 시간 생성.
    today_seed = datetime.date.today().year * datetime.date.today().month * datetime.date.today().day
    random.seed(today_seed)
    start_time = datetime.time(int(random.triangular(8, 9, 8)), int(random.randrange(0, 59, 1)))
    break_time = datetime.time(int(random.triangular(12, 13, 13)), int(random.randrange(0, 59, 1)))
    # end_time = datetime.time(int(random.triangular(18, 19, 19)), int(random.randrange(0, 59, 1)))
    end_time = datetime.time(int(random.triangular(23, 23, 23)), int(random.randrange(0, 59, 1)))

    now = datetime.datetime.now().time()
    # 검색 쿼리 횟수 제한
    if (search_count != None):
        if (search_count >= search_count_max):
            print("----------------------------------\n")
            print(str(datetime.datetime.now()) + "search count limit break term start.")
            report_error(msg="search count limit break term start.")
            while (datetime.datetime.now().time() < datetime.time(6, 0)) | \
                    (datetime.datetime.now().time() > datetime.time(7, 0)):
                time.sleep(10)
            else:
                print("----------------------------------\n")
                print(str(datetime.datetime.now()) + "search count limit break term end.")
                report_error(msg="search count limit break term end.")
                result["search_count"] = 0

    # 시작시간, 중간 쉬는 시간, 끝시간에 따른 대기.
    if (start_time >= now) | (end_time <= now):
        print("----------------------------------\n")
        if start_time >= now:
            print(str(datetime.datetime.now()) + "start break term start.")
            report_error(msg="start break term start.")
        elif end_time <= now:
            print(str(datetime.datetime.now()) + "end break term start.")
            report_error(msg="end break term start.")
        while (start_time >= datetime.datetime.now().time()) | \
                (end_time <= datetime.datetime.now().time()):
            time.sleep(10)
        else:
            print("----------------------------------\n")
            print(str(datetime.datetime.now()) + "start/end break term end.")
            report_error(msg="start/end break term end.")
            result["search_count"] = 0
            print((str(datetime.datetime.now()) + "break_time : " + str(break_time)))
            print((str(datetime.datetime.now()) + "start_time : " + str(start_time)))
            print((str(datetime.datetime.now()) + "end_time : " + str(end_time)))

    elif (break_time < now) & \
            ((datetime.datetime.combine(datetime.date.today(), break_time)
              + datetime.timedelta(minutes=30)).time() > now):
        print("----------------------------------\n")
        print(str(datetime.datetime.now()) + "middle break term start.")
        report_error(msg="middle break term start.")
        time.sleep(random.normalvariate(3000, 400))
        print("----------------------------------\n")
        print(str(datetime.datetime.now()) + "middle break term end.")
        report_error(msg="middle break term end.")

    # 랜덤 몇 초 더 대기.
    random_value = random.randrange(1, 100, 1)
    if random_value % 20 == 0:
        print(str(datetime.datetime.now()) + "more sleep...")
        time.sleep(random.triangular(wait_time, wait_time + term + 10, wait_time + term + 5))
    print(str(datetime.datetime.now()) + "...")
    time.sleep(random.triangular(wait_time, wait_time + term, wait_time))
    # 랜덤 3~5분 대기.
    random_value3 = random.randrange(1, 100, 1)
    if random_value3 % 100 == 0:
        print(str(datetime.datetime.now()) + "3~5minute sleep")
        report_error(msg="3~5minute sleep")
        time.sleep(random.uniform(180, 300))
    # 랜덤 10~20분 대기.
    random_value2 = random.randrange(1, 1000, 1)
    if random_value2 % 500 == 0:
        print(str(datetime.datetime.now()) + "10~20minute sleep")
        report_error(msg="10~20minute sleep")
        time.sleep(random.uniform(600, 1200))

    return result