import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, timezone 
from bs4 import BeautifulSoup
import os
import google.generativeai as genai

URL_BASE = "https://openapi.koreainvestment.com:9443"
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

def safe_float(text):
    try: return float(text.replace(',', '').replace('%', '').strip())
    except: return 0.0

def safe_api_float(val):
    try: return float(val) if val else 0.0
    except: return 0.0

def get_target_stock_list():
    target_list = []
    noise_keywords = ['KODEX', 'TIGER', 'RISE', 'ACE', 'KBSTAR', 'HANARO', 'KOSEF', 'SOL', 'PLUS', 'ARIRANG', 'ETN', '스팩', '인버스', '레버리지', 'CD금리', 'KOFR']
    
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
                        stock_name = name_tag.text
                        if any(keyword in stock_name for keyword in noise_keywords):
                            continue
                            
                        marcap = safe_float(tds[6].text)
                        if marcap >= 8000:
                            target_list.append({
                                '종목명': stock_name, '종목코드': name_tag['href'].split('code=')[-1], 
                                '소속': market_name, '현재가': int(safe_float(tds[2].text)),
                                '등락률': safe_float(tds[4].text), '시가총액': int(marcap),
                                'PER': safe_float(tds[10].text), 'ROE': safe_float(tds[11].text)
                            })
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

def run_scraper():
    print("🚀 수집기 봇 가동 시작 (V7.0 트렌드 누적 적용)...")
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
                            '종목명': name, '일자': daily.get('stck_bsop_date', ''),
                            '종가': close_prc, '외인': f_amt / 1_000_000, '연기금': p_amt / 1_000_000,
                            '투신': t_amt / 1_000_000, '사모': pef_amt / 1_000_000
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
                        if not f_buying and not p_buying: break

            ma20 = sum(closes) / len(closes) if closes else prpr
            gap_20 = (prpr / ma20) * 100 if ma20 else 100 
            marcap_won = marcap * 100_000_000 

            f_str = (f_amt_sum / marcap_won) * 100 if marcap_won else 0
            p_str = (p_amt_sum / marcap_won) * 100 if marcap_won else 0
            t_str = (t_amt_sum / marcap_won) * 100 if marcap_won else 0
            pef_str = (pef_amt_sum / marcap_won) * 100 if marcap_won else 0
            turnover_rate = (vol_tr_sum_5d / marcap_won) * 100 if marcap_won else 0 
            
            tech_score = 0
            if 101 <= gap_20 <= 108: tech_score += 15 
            elif gap_20 < 95: tech_score -= 20 
            elif gap_20 > 115: tech_score -= 10      
            if turnover_rate >= 10: tech_score += 15

            strength_score = (max(-10, min(10, p_str)) * 20.0) + (max(-5, min(5, t_str)) * 15.0) + (max(-5, min(5, pef_str)) * 15.0) + (max(-5, min(5, f_str)) * 10.0)
            streak_score = min(20, pension_streak * 3.0) + min(10, foreign_streak * 1.5)

            fund_score = 0
            if row.ROE >= 15: fund_score += 10
            elif row.ROE >= 8: fund_score += 5
            if 0 < row.PER <= 15: fund_score += 5 

            ai_score = strength_score + streak_score + fund_score + tech_score
            ai_score = max(0, min(100, int(ai_score)))

            data_list.append({
                '종목명': name, '종목코드': code, '소속': row.소속, 'AI수급점수': int(ai_score),
                '현재가': prpr, '등락률': row.등락률,
                '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                '외인연속': foreign_streak, '연기금연속': pension_streak,
                '이격도(%)': round(gap_20, 1), '손바뀜(%)': round(turnover_rate, 1),
                '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
            })
        except Exception as e:
            pass 
        time.sleep(0.2) 

    df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
    df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
    
    df_history = pd.DataFrame(history_list)
    df_history.to_csv("history.csv", index=False, encoding='utf-8-sig')
    print("✅ 데이터 수집 완료! (노이즈 제거 완료)")

    # 🔥 [V7.0 추가] 날짜별 점수/순위 트렌드 누적 저장
    today_date = datetime.now(KST).strftime("%Y-%m-%d")
    df_trend_new = df_final[['종목명', '종목코드', 'AI수급점수']].copy()
    df_trend_new['순위'] = df_trend_new['AI수급점수'].rank(method='min', ascending=False).astype(int)
    df_trend_new['날짜'] = today_date

    trend_file = "score_trend.csv"
    if os.path.exists(trend_file):
        df_trend_old = pd.read_csv(trend_file)
        # 중복 방지 (하루에 여러번 돌려도 최신값으로 덮어씀)
        df_trend_old = df_trend_old[df_trend_old['날짜'] != today_date]
        df_trend_combined = pd.concat([df_trend_old, df_trend_new], ignore_index=True)
        df_trend_combined.to_csv(trend_file, index=False, encoding='utf-8-sig')
    else:
        df_trend_new.to_csv(trend_file, index=False, encoding='utf-8-sig')

    # 🔥 AI 마감 리포트 생성 (에러 방지용으로 파이썬이 강제 제목/날짜 입력)
    if GEMINI_API_KEY:
        print("📝 AI 마감 리포트 생성 중...")
        try:
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel('gemini-2.5-flash')
            
            top_N_names = df_final.head(20)['종목명'].tolist()
            latest_date = df_history['일자'].max()
            df_today = df_history[(df_history['일자'] == latest_date) & (df_history['종목명'].isin(top_N_names))]
            
            df_merged = pd.merge(
                df_final.head(20)[['종목명', '소속', 'AI수급점수', '이격도(%)', '손바뀜(%)']],
                df_today[['종목명', '외인', '연기금']],
                on='종목명', how='left'
            )
            df_merged.rename(columns={'외인': '당일_외인순매수(백만)', '연기금': '당일_연기금순매수(백만)'}, inplace=True)
            top_data_str = df_merged.to_string(index=False)
            
            prompt = f"""
            너는 여의도 최고의 탑다운 퀀트 애널리스트야.
            아래는 ETF 노이즈가 제거된 최상위 20개 종목 데이터야.
            
            {top_data_str}

            다음 순서로 전문가 수준의 마감 리포트를 작성해 줘.
            (🚨주의: 제목은 쓰지 말고, 바로 '1. 🌐 글로벌 매크로' 본문부터 시작해!)
            
            1. 🌐 글로벌 매크로 & 실시간 이벤트 브리핑: 
               - 오늘 시장에 가장 큰 영향을 미친 글로벌 뉴스(미국 증시, 금리, 환율 등)와 거시적 관점을 상세히 짚어줘.
            
            2. 🌪️ 국내 증시 섹터 및 당일 수급 동향 (🚨외부 종목 언급 금지): 
               - 오직 제공된 '표 데이터(20개 종목)'만 보고, 오늘 외인과 기관이 어떤 섹터에 집중했는지 추론해 줘. KODEX 등 표에 없는 ETF는 절대 언급하지 마.
               
            3. 🎯 내일의 Top 3 관심종목 & 추천 사유 (🚨외부 종목 언급 금지): 
               - 반드시 표 안의 20개 종목 중에서만 3개를 골라 거시적 환경에 맞는 이유를 작성해 줘.
            """
            
            # 🚨 에러 나던 검색 툴 싹 빼고 가장 안정적인 기본 호출만 사용!
            response = model.generate_content(prompt)
            
            today_str = datetime.now(KST).strftime("%Y년 %m월 %d일")
            with open("report.md", "w", encoding="utf-8") as f:
                f.write(f"## 🌐 여의도 탑다운 퀀트 애널리스트 마감 리포트 ({today_str})\n\n")
                f.write(response.text)
                
            print(f"✅ AI 리포트 저장 완료! ({today_str})")
            
        except Exception as e:
            print(f"⚠️ AI 리포트 생성 실패: {e}")
    else:
        print("⚠️ GEMINI_API_KEY가 없어 리포트를 건너뜁니다.")

if __name__ == "__main__":
    run_scraper()
