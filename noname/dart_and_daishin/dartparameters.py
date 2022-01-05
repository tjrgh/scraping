KEY = 'd8eab1795cffd46d35c7490d5248ce3f2585c83b'
LYK_KEY = '39febb6a75a1a669098245e4bf0fed8f7f12a485'
base_url = 'https://opendart.fss.or.kr/api'
urls = {
    'corp_code': '/corpCode.xml',
    'company_data': '/company.json',
    'isu_inc_dec_status': '/irdsSttus.json',
    'origin_file': '/document.xml',
    'search': '/list.json',
    'financial_statement': '/fnlttSinglAcntAll.json',
}
report_code = {
    '1분기': '11013',
    '반기': '11012',
    '3분기': '11014',
    '사업': '11011'
}
corp_type = {
    '유가': 'Y',
    '코스닥': 'K',
    '코넥스': 'N',
    '기타': 'E',
}
data_type = {
    'A': '정기공시',
    'B': '주요사항보고',
    'C': '발행공시',
    'D': '지분공시',
    'E': '기타공시',
    'F': '외부감사관련',
    'G': '펀드공시',
    'H': '자산유동화',
    'I': '거래소공시',
    'J': '공정위공시',
}