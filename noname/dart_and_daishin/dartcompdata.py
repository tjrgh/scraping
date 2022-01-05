from dartparameters import KEY, base_url, urls
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
import time
import pandas as pd
import io
import pytz
from datetime import datetime

"""
dart_corp_code_data 테이블의 종목 목록에 대해 기업 개요 정보를 dart api로부터 받아와 dart_corp_basic_data 테이블에 저장. 
"""
#%% get corp codes
with psycopg2.connect(
    user='openmetric',
    password=')!metricAdmin01',
    host='192.168.0.16',
    port=5432,
    database='openmetric'
) as conn:
    cur = conn.cursor(cursor_factory=RealDictCursor)
    query = """
    select corp_code
    from dart_corp_code_data
    where stock_code != ' '
    """
    cur.execute(query)
    corp_code_list = [d['corp_code'] for d in cur.fetchall()]
print(len(corp_code_list))
#%% get corp data
params = {
    'crtfc_key': KEY
}

data = {}
for code in corp_code_list:
    print(code)
    params['corp_code'] = code
    response = requests.get(base_url+urls['company_data'], params=params)
    comp = response.json()
    del comp['status']
    del comp['message']
    comp['corp_code'] = code
    data[code] = comp
    time.sleep(.7)

#%% insert into db
# df = pd.DataFrame.from_dict(data, orient='index')

with psycopg2.connect(
    user='openmetric',
    password=')!metricAdmin01',
    host='192.168.0.16',
    port=5432,
    database='openmetric'
) as conn:
    cur = conn.cursor()
    csv_like = io.StringIO()

    # df['created_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
    # df['updated_at'] = pytz.timezone('UTC').localize(datetime.utcnow())
    # df['phn_no'] = [x.replace(' ', '') if ' ' in x else x for x in df['phn_no'].tolist()]
    # df['fax_no'] = [x.replace(' ', '') if ' ' in x else x for x in df['fax_no'].tolist()]
    # df.to_csv(csv_like, header=False, index=False, sep='^')
    # df.to_csv('compdata.csv', header=False, index=False, sep='^')

    df = pd.read_csv('compdata.csv', sep='^', header=None, dtype=str)
    # print(df)
    mask = df[8].str.len() > 10
    df.drop(df[mask].index, inplace=True)
    # print(df[7])
    # print(df)
    df.to_csv(csv_like, header=False, index=False, sep='^')
    csv_like.seek(0)

    cur.copy_from(
        csv_like,
        'dart_corp_basic_data',
        sep='^',
        columns=(
            'corp_code',
            'corp_name',
            'corp_name_eng',
            'stock_name',
            'stock_code',
            'ceo_name',
            'corp_type',
            'corp_reg_number',
            'business_reg_number',
            'address',
            'homepage',
            'ir_homepage',
            'phone_number',
            'fax_number',
            'industry_code',
            'est_date',
            'acc_month',
            'created_at',
            'updated_at'
        )
    )
    conn.commit()
