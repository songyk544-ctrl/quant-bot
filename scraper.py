import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, timezone 
from bs4 import BeautifulSoup
import os

URL_BASE = "https://openapi.koreainvestment.com:9443"
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
        for page in range(1, 7): 
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

    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST)

    if now_kst.hour > 15 or (now_kst.hour == 15 and now_kst.minute >= 40):
        end_date = now_kst.strftime("%Y%m%d") 
    else:
        end_date = (now_kst - timedelta(days=1)).strftime("%Y%m%d") 

    start_date = (now_kst - timedelta(days=40)).strftime("%Y%m%d") 

    data_list = []
    history_list = [] 

    for i, row in enumerate(df_target.itertuples()):
        code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
        
        # 대표님이 완벽하게 디버깅하신 params 주석 처리 유지!
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": end_date, #"FID_INPUT_DATE_2": end_date,
            "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"
        }

        try:
            res = requests.get(url, headers=headers, params=params)

            # API 금액은 백만 원 단위임
            f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum = 0, 0, 0, 0
            foreign_streak, pension_streak = 0, 0
            f_buying, p_buying = True, True  
            
            closes = [] 
            vol_tr_sum_5d = 0 

            if res.status_code == 200 and res.json().get('rt_cd') == "0":
                daily_list = res.json().get('output2', [])
                if daily_list:
                    for idx, daily in enumerate(daily_list[:20]): 
                        f_pbmn = float(daily.get('frgn_ntby_tr_pbmn', 0)) # API 데이터 (백만원 단위)
                        p_pbmn = float(daily.get('fund_ntby_tr_pbmn', 0))
                        t_pbmn = float(daily.get('ivtr_ntby_tr_pbmn', 0))
                        pef_pbmn = float(daily.get('pe_fund_ntby_tr_pbmn', 0))

                        f_amt_sum += f_pbmn
                        p_amt_sum += p_pbmn
                        t_amt_sum += t_pbmn
                        pef_amt_sum += pef_pbmn
                        
                        close_prc = float(daily.get('stck_clpr', 0))
                        closes.append(close_prc)
                        
                        if idx < 5:
                            vol_tr_sum_5d += float(daily.get('acml_tr_pbmn', 0)) # 거래대금 (백만원 단위)

                        # 📈 차트용 DB(history.csv)에 기록
                        history_list.append({
                            '종목명': name,
                            '일자': daily.get('stck_bsop_date', ''),
                            '종가': close_prc,
                            '외인': f_pbmn, # 백만원
                            '연기금': p_pbmn,
                            '투신': t_pbmn,
                            '사모': pef_pbmn
                        })

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

            # 🔥 [긴급 수정] 모든 단위를 '백만 원'으로 통일
            # 네이버 시총(억원)에 100을 곱해 단위를 '백만 원'으로 맞춤. API 데이터와 1:1 대응
            marcap_million = marcap * 100 

            f_str = (f_amt_sum / marcap_million) * 100 if marcap_million else 0
            p_str = (p_amt_sum / marcap_million) * 100 if marcap_million else 0
            t_str = (t_amt_sum / marcap_million) * 100 if marcap_million else 0
            pef_str = (pef_amt_sum / marcap_million) * 100 if marcap_million else 0

            # 🎯 기술적 지표 계산 (이격도 & 손바뀜)
            ma20 = sum(closes) / len(closes) if closes else prpr
            gap_20 = (prpr / ma20) * 100 if ma20 else 100 
            turnover_rate = (vol_tr_sum_5d / marcap_million) * 100 if marcap_million else 0 # (백만원 / 백만원) * 100
            
            tech_score = 0
            if 98 <= gap_20 <= 105: tech_score += 10 # 완벽한 눌림목
            elif gap_20 > 115: tech_score -= 10      # 단기 과열
            if turnover_rate >= 15: tech_score += 10 # 손바뀜 우수

            strength_score = (max(-10, min(10, p_str)) * 2.0) + (max(-5, min(5, t_str)) * 1.5) + (max(-5, min(5, pef_str)) * 1.5) + (max(-5, min(5, f_str)) * 1.0)
            streak_score = min(20, pension_streak * 3.0) + min(10, foreign_streak * 1.5)

            fund_score = 0
            if row.ROE >= 15: fund_score += 15
            elif row.ROE >= 8: fund_score += 8
            if 0 < row.PER <= 10: fund_score += 15
            elif 10 < row.PER <= 20: fund_score += 8

            # AI 점수 총합
            ai_score = 20 + strength_score + streak_score + fund_score + tech_score
            ai_score = max(0, min(100, int(ai_score)))

            data_list.append({
                '종목명': name, '종목코드': code, '소속': row.소속, 'AI수급점수': int(ai_score),
                '현재가': prpr, '등락률': row.등락률,
                '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                '외인연속': foreign_streak,
                '연기금연속': pension_streak,
                '이격도(%)': round(gap_20, 1),
                '손바뀜(%)': round(turnover_rate, 1),
                '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
            })
        except Exception as e:
            print(f"Error parsing {name}: {e}")
            pass 

        time.sleep(0.2) 

    df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
    df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
    
    # 🔥 차트용 데이터베이스 별도 생성
    pd.DataFrame(history_list).to_csv("history.csv", index=False, encoding='utf-8-sig')
    print("✅ 데이터 수집 및 data.csv / history.csv 저장 완료!")

if __name__ == "__main__":
    run_scraper()
