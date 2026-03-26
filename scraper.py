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

# 네이버 크롤링용 안전 변환
def safe_float(text):
    try: return float(text.replace(',', '').replace('%', '').strip())
    except: return 0.0

# 🔥 [해결 2] 한투 API에서 빈 문자열("")이 올 때 뻗지 않도록 막아주는 방패 함수
def safe_api_float(val):
    try: return float(val) if val else 0.0
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
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": end_date, 
            "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"
        }

        try:
            res = requests.get(url, headers=headers, params=params)

            f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum = 0, 0, 0, 0
            foreign_streak, pension_streak = 0, 0
            f_buying, p_buying = True, True  
            
            closes = [] 
            vol_tr_sum_5d = 0 

            if res.status_code == 200 and res.json().get('rt_cd') == "0":
                daily_list = res.json().get('output2', [])
                if daily_list:
                    for idx, daily in enumerate(daily_list[:20]): 
                        # API에서 온 빈 값을 안전하게 변환
                        close_prc = safe_api_float(daily.get('stck_clpr'))
                        closes.append(close_prc)
                        
                        f_qty = safe_api_float(daily.get('frgn_ntby_qty'))
                        p_qty = safe_api_float(daily.get('fund_ntby_qty'))
                        t_qty = safe_api_float(daily.get('ivtr_ntby_qty'))
                        pef_qty = safe_api_float(daily.get('pe_fund_ntby_vol'))
                        
                        f_amt = f_qty * close_prc
                        p_amt = p_qty * close_prc
                        t_amt = t_qty * close_prc
                        pef_amt = pef_qty * close_prc
                        
                        f_amt_sum += f_amt
                        p_amt_sum += p_amt
                        t_amt_sum += t_amt
                        pef_amt_sum += pef_amt
                        
                        daily_vol = safe_api_float(daily.get('acml_vol'))
                        if idx < 5:
                            vol_tr_sum_5d += (daily_vol * close_prc)

                        history_list.append({
                            '종목명': name,
                            '일자': daily.get('stck_bsop_date', ''),
                            '종가': close_prc,
                            '외인': f_amt / 1_000_000,
                            '연기금': p_amt / 1_000_000,
                            '투신': t_amt / 1_000_000,
                            '사모': pef_amt / 1_000_000
                        })

                    for daily in daily_list:
                        f_qty = safe_api_float(daily.get('frgn_ntby_qty'))
                        p_qty = safe_api_float(daily.get('fund_ntby_qty'))

                        if f_buying:
                            if f_qty > 0: foreign_streak += 1
                            else: f_buying = False

                        if p_buying:
                            if p_qty > 0: pension_streak += 1
                            else: p_buying = False

                        if not f_buying and not p_buying:
                            break

            # 🔥 [해결 1] 실수로 날려먹었던 이격도(gap_20) 계산식 완벽 복구
            ma20 = sum(closes) / len(closes) if closes else prpr
            gap_20 = (prpr / ma20) * 100 if ma20 else 100 
            
            marcap_won = marcap * 100_000_000 

            f_str = (f_amt_sum / marcap_won) * 100 if marcap_won else 0
            p_str = (p_amt_sum / marcap_won) * 100 if marcap_won else 0
            t_str = (t_amt_sum / marcap_won) * 100 if marcap_won else 0
            pef_str = (pef_amt_sum / marcap_won) * 100 if marcap_won else 0

            turnover_rate = (vol_tr_sum_5d / marcap_won) * 100 if marcap_won else 0 
            
            # ---------------------------------------------------------
            # 👇 여기서부터 교체해 주세요! (기존 tech_score = 0 부분부터) 👇
            
            tech_score = 0
            # 🔥 [수정 1] 역배열 폭락주 철퇴 & 진짜 상승 추세 눌림목만 가점!
            if 101 <= gap_20 <= 108: 
                tech_score += 15 # 20일선 '위에서' 지지받는 진짜 상승장 눌림목 (+15점)
            elif gap_20 < 95: 
                tech_score -= 20 # 20일선 한참 아래로 깨진 폭락주 (강력 감점 -20점)
            elif gap_20 > 115: 
                tech_score -= 10 # 단기 너무 급등한 과열주 (추격매수 방지 -10점)
                
            # 손바뀜 기준을 살짝 낮춰서(15->10) 거래대금 터진 종목 우대
            if turnover_rate >= 10: tech_score += 15 

            # 🔥 [수정 2] 수급 강도 증폭! (기존 대비 가중치 10배 폭발)
            # 이제 시총 대비 1%만 들어와도 20점이 꽂힙니다. (진짜 돈이 깡패!)
            strength_score = (max(-10, min(10, p_str)) * 20.0) + (max(-5, min(5, t_str)) * 15.0) + (max(-5, min(5, pef_str)) * 15.0) + (max(-5, min(5, f_str)) * 10.0)
            
            streak_score = min(20, pension_streak * 3.0) + min(10, foreign_streak * 1.5)

            # 🔥 [수정 3] 가치주 함정(저 PER) 비중 대폭 축소 (최대 30점 -> 15점)
            fund_score = 0
            if row.ROE >= 15: fund_score += 10
            elif row.ROE >= 8: fund_score += 5
            if 0 < row.PER <= 15: fund_score += 5 # 성장주도 포함되도록 PER 기준 완화

            # AI 점수 총합 계산 (기본점수 20 -> 0으로 낮추고 실력으로만 평가)
            ai_score = strength_score + streak_score + fund_score + tech_score
            ai_score = max(0, min(100, int(ai_score)))
            
            # 👆 여기까지 덮어써 주시면 됩니다! (data_list.append 바로 위까지) 👆
            # ---------------------------------------------------------


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
    
    pd.DataFrame(history_list).to_csv("history.csv", index=False, encoding='utf-8-sig')
    print("✅ 데이터 수집 완료! 에러 방어 적용 완료!")

if __name__ == "__main__":
    run_scraper()
