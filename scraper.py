import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import os

URL_BASE = "https://openapi.koreainvestment.com:9443"
# GitHub Actions의 Secrets에서 환경변수를 불러옵니다.
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

def safe_float(text):
    try: return float(text.replace(',', '').replace('%', '').strip())
    except: return 0.0

def get_target_stock_list():
    target_list = []
    for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
        for page in range(1, 7): # 시총 상위 약 300개 스캔
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(res.text, 'html.parser')
            for tr in soup.select('table.type_2 tbody tr'):
                tds = tr.select('td')
                if len(tds) > 11:
                    name_tag = tr.select_one('a.tltle')
                    if name_tag:
                        marcap = safe_float(tds[6].text)
                        if marcap >= 8000:
                            target_list.append({
                                '종목명': name_tag.text, '종목코드': name_tag['href'].split('code=')[-1], 
                                '소속': market_name, '현재가': int(safe_float(tds[2].text)),
                                '등락률': safe_float(tds[4].text), '시가총액': int(marcap),
                                'PER': safe_float(tds[10].text), 'ROE': safe_float(tds[11].text)
                            })
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

def run_scraper():
    print("🚀 수집기 봇 가동 시작...")
    df_target = get_target_stock_list()
    token = get_kis_access_token()
    
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET, "tr_id": "FHPTJ04160001", "custtype": "P"
    }
    url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"

    # 전 영업일 기준 데이터 추출
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=40)).strftime("%Y%m%d") 
    
    data_list = []
    
    for i, row in enumerate(df_target.itertuples()):
        code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": end_date,
            "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"
        }
        
        try:
            res = requests.get(url, headers=headers, params=params)
            
            f_vol_sum, p_vol_sum, t_vol_sum, pef_vol_sum = 0, 0, 0, 0
            foreign_streak, pension_streak = 0, 0
            f_buying, p_buying = True, True  # 연속 매수 스위치
            
            if res.status_code == 200 and res.json().get('rt_cd') == "0":
                daily_list = res.json().get('output2', [])
                if daily_list:
                    # 1. 최근 20일(약 1달) 누적 수급 계산
                    for daily in daily_list[:20]: 
                        f_vol_sum += int(daily.get('frgn_ntby_qty', 0))
                        p_vol_sum += int(daily.get('fund_ntby_qty', 0))
                        t_vol_sum += int(daily.get('ivtr_ntby_qty', 0))
                        pef_vol_sum += int(daily.get('pe_fund_ntby_vol', 0))

                    # 2. 외인 및 연기금 독립적인 연속 매수일 계산
                    for daily in daily_list:
                        f_qty = int(daily.get('frgn_ntby_qty', 0))
                        p_qty = int(daily.get('fund_ntby_qty', 0))
                        
                        if f_buying:
                            if f_qty > 0: foreign_streak += 1
                            else: f_buying = False
                            
                        if p_buying:
                            if p_qty > 0: pension_streak += 1
                            else: p_buying = False
                            
                        if not f_buying and not p_buying:
                            break

            marcap_won = marcap * 100_000_000
            f_str = (f_vol_sum * prpr / marcap_won) * 100 if marcap_won else 0
            p_str = (p_vol_sum * prpr / marcap_won) * 100 if marcap_won else 0
            t_str = (t_vol_sum * prpr / marcap_won) * 100 if marcap_won else 0
            pef_str = (pef_vol_sum * prpr / marcap_won) * 100 if marcap_won else 0
            
            # 1. 수급 강도 점수 (최대 40점) 
            # - 단순 곱셈 시 1회성 대량 매수(아웃라이어)에 점수가 왜곡되는 것을 막기 위해 min/max 캡(Cap)을 씌웁니다.
            strength_score = (max(-10, min(10, p_str)) * 2.0) + \
                             (max(-5, min(5, t_str)) * 1.5) + \
                             (max(-5, min(5, pef_str)) * 1.5) + \
                             (max(-5, min(5, f_str)) * 1.0)
            
            # 2. 매집 연속성 프리미엄 (최대 30점)
            # - 기관/외인이 하루 왕창 사고 마는 게 아니라, 'N일 연속' 매집 중일 때 폭발적인 가점을 줍니다.
            streak_score = 0
            streak_score += min(20, pension_streak * 3.0) # 연기금은 1일 연속당 3점씩 가점 (최대 20점)
            streak_score += min(10, foreign_streak * 1.5) # 외국인은 1일 연속당 1.5점 가점 (최대 10점)
            
            # 3. 펀더멘털 가치 프리미엄 (최대 30점)
            # - 네이버에서 긁어온 ROE(수익성)와 PER(저평가)을 수급과 결합해 '우량주'에 가중치를 줍니다.
            fund_score = 0
            if row.ROE >= 15: fund_score += 15       # 워렌 버핏 기준 (초우량)
            elif row.ROE >= 8: fund_score += 8       # 은행 이자보다 높은 수익성
            
            if 0 < row.PER <= 10: fund_score += 15   # 극도의 저평가 상태
            elif 10 < row.PER <= 20: fund_score += 8 # 적정 주가 수준

            # 4. 종합점수 산출 (기본점수 30점 + 강도 + 연속성 + 펀더멘털)
            # - 마이너스가 되지 않도록 방어하고, 100점을 넘지 않도록 정규화(Normalization) 합니다.
            ai_score = 30 + strength_score + streak_score + fund_score
            ai_score = max(0, min(100, int(ai_score)))
            
            data_list.append({
                '종목명': name, '종목코드': code, '소속': row.소속, 'AI수급점수': int(ai_score),
                '현재가': prpr, '등락률': row.등락률,
                '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                '외인연속': foreign_streak,    # 분리된 외인 연속매수
                '연기금연속': pension_streak,  # 분리된 연기금 연속매수
                '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
            })
        except Exception as e:
            print(f"Error parsing {name}: {e}")
            pass 
            
        time.sleep(0.2) # API 한도 보호
        
    df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
    df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
    print("✅ 데이터 수집 및 data.csv 저장 완료!")

if __name__ == "__main__":
    run_scraper()