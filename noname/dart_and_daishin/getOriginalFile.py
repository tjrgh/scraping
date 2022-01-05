from dartparameters import KEY, base_url, urls, data_type
import requests
from io import BytesIO, StringIO
from zipfile import ZipFile
from datetime import date, datetime, timedelta
import json
import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extras import RealDictCursor
import pytz
import time

"""
https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001 참고

"""

days_ago = 0
today = date.today() - timedelta(days_ago)
today_dt = datetime.utcnow() - timedelta(days_ago)
print(today)
print(today_dt)

code_list = []
try:
    with psycopg2.connect(
        user='openmetric',
        password=')!metricAdmin01',
        host='192.168.0.16',
        port='5432',
        database='openmetric'
    ) as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = "select receipt_no from dart_search_data where data_date = '"\
                + pytz.timezone('UTC').localize(today_dt).strftime('%Y-%m-%d') + "'"
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            code_list.append(row['receipt_no'])
except (Exception, psycopg2.Error) as e:
    print('database connect error: ', e)
except Exception as e:
    print('error: ', e)

code_list = set(code_list)

file_dir = './' + today.isoformat().replace('-', '')
os.makedirs(file_dir, exist_ok=True)

# start_date = "2021-01-05"
# end_date = "2021-01-05"
search_params = {
    'crtfc_key': KEY,
    'bgn_de': today.isoformat().replace('-', ''),
    'end_de': today.isoformat().replace('-', ''),
    # 'bgn_de': start_date.replace('-', ''),
    # 'end_de': end_date.replace('-', ''),
    'page_count': 100
}

data_type_list = list(data_type.keys())
data_list = []

for ty in data_type_list:
    page = 1
    search_params['pblntf_ty'] = ty
    search_params['page_no'] = page
    print('type: ', data_type[ty])
    response = requests.get(base_url+urls['search'], params=search_params)
    data = json.loads(response.content)
    if data['status'] == '013':
        continue
    total_page = data['total_page']
    no_new = False
    print('  total page: ', total_page)
    while page <= total_page:
        print('    current page: ', page)
        lists = data['list']
        for idx, d in enumerate(lists):
            if d['rcept_no'] in code_list:
                no_new = True
                break
            file_params = {
                'crtfc_key': KEY,
                'rcept_no': d['rcept_no']
            }
            res = requests.get(base_url+urls['origin_file'], params=file_params)
            time.sleep(.7)
            contents = ''
            try:
                with ZipFile(BytesIO(res.content)) as zf:
                    file_list = zf.namelist()
                    while len(file_list) > 0:
                        file_name = file_list.pop()
                        contents = zf.open(file_name).read().decode('cp949')
                        with open(file_dir + '/' + file_name, 'w') as f:
                            f.write(contents)
                        break
                data_list.append(d)
                # soup = BeautifulSoup(contents, 'html.parser')
                # print(soup.prettify())
            except Exception as e:
                pass
            d['contents'] = contents
            d['data_type'] = ty
            d['rcept_dt'] = pytz.timezone('UTC').localize(today_dt)
            d['created_at'] = pytz.timezone('UTC').localize(today_dt)
            d['updated_at'] = pytz.timezone('UTC').localize(today_dt)
        if no_new:
            break
        page += 1
        search_params['page_no'] = page
        response = requests.get(base_url + urls['search'], params=search_params)
        data = json.loads(response.content)

try:
    with psycopg2.connect(
        user='openmetric',
        password=')!metricAdmin01',
        host='192.168.0.16',
        port='5432',
        database='openmetric'
    ) as conn:
        cursor = conn.cursor()
        columns = (
            'corp_code',
            'corp_name',
            'stock_code',
            'corp_type',
            'title',
            'receipt_no',
            'upload_name',
            'data_date',
            'rm',
            'contents',
            'data_type',
            'created_at',
            'updated_at'
        )
        query = "INSERT INTO dart_search_data ({}) VALUES %s".format(','.join(columns))
        values = [[value for value in data_list.values()] for data_list in data_list]
        execute_values(cursor, query, values)
        conn.commit()
except (Exception, psycopg2.Error) as e:
    print('database connect error: ', e)
except Exception as e:
    print('error: ', e)
