from parsexml import parse_xml
import psycopg2
from psycopg2.extras import RealDictCursor
import io
import pytz
from datetime import datetime

import requests
import zipfile
from io import BytesIO

"""
dart로부터 corp_code와 종목 code 목록 받아와서 stock_basic_info테이블의 corp_code 업데이트하고 dart_corp_code_data 테이블에 저장.
"""
# KEY = 'd8eab1795cffd46d35c7490d5248ce3f2585c83b'
KEY = '39febb6a75a1a669098245e4bf0fed8f7f12a485'

url = 'https://opendart.fss.or.kr/api/corpCode.xml'
params = {
    'crtfc_key': KEY
}
response = requests.get(url, params=params) #종목 리스트 받아오기.

z = zipfile.ZipFile(BytesIO(response.content))
z.extractall()

#%% get df from xml
cols = ['corp_code', 'corp_name', 'stock_code', 'modify_date']
df = parse_xml('CORPCODE.xml', cols)

has_stock_code = df[df.stock_code != ' ']
rows = []
for idx, row in has_stock_code.iterrows():
    rows.append(dict(row))


#%% db
with psycopg2.connect(
    user='openmetric',
    password=')!metricAdmin01',
    host='192.168.0.16',
    port=5432,
    database='openmetric'
) as conn:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    update stocks_basic_info
    set corp_code = %(corp_code)s
    where right(code, 6) = %(stock_code)s
    """
    cur.executemany(query, rows)
    conn.commit()

#%%
# print(df[df['stock_code'] == '000545'])
# print(len(has_stock_code.index))

#%% db2
# stock_code_list = tuple(has_stock_code.stock_code)
# with psycopg2.connect(
#     user='openmetric',
#     password=')!metricAdmin01',
#     host='192.168.10.16',
#     port=5432,
#     database='openmetric'
# ) as conn:
#     cur = conn.cursor(cursor_factory=RealDictCursor)
#     query = """
#     select code, corp_code, name
#     from stocks_basic_info
#     where right(code, 6) in %s
#     """
#     cur.execute(query, (stock_code_list,))
#     corp_code_list = [d['corp_code'] for d in cur.fetchall()]
#

#%% insert
with psycopg2.connect(
    user='openmetric',
    password=')!metricAdmin01',
    host='192.168.0.16',
    port=5432,
    database='openmetric'
) as conn:
    cur = conn.cursor()
    csv_like = io.StringIO()
    df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
    df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
    df['corp_name'] = [x.replace(',', '') if ',' in x else x for x in df['corp_name'].tolist()]
    # print(df)
    df.to_csv(csv_like, header=False, index=False)
    csv_like.seek(0)
    cur.copy_from(
        csv_like,
        'dart_corp_code_data',
        sep=',',
        columns=(
            'corp_code',
            'corp_name',
            'stock_code',
            'modify_date',
            'created_at',
            'updated_at'
        )
    )
    conn.commit()
