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

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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

def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ 텔레그램 토큰/Chat ID가 없어 알림을 건너뜁니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace('**', '*') 
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000], "parse_mode": "Markdown"}
    try:
        res = requests.post(url, data=data)
        if res.status_code == 200: print("✅ 텔레그램 발송 완료!")
        else: print(f"⚠️ 텔레그램 서버 거절: {res.text}")
    except Exception as e:
        print(f"⚠️ 텔레그램 네트워크 에러: {e}")

def run_scraper():
    print("🚀 수집기 봇 가동 시작 (V8.2 섹터 태깅 탑재)...")
    df_target = get_target_stock_list()
    token = get_kis_access_token()
    headers = {
        "authorization": f"Bearer {token}", "appkey": KIS_APP_KEY,
        "appsecret": KIS_APP_SECRET, "tr_id": "FHPTJ04160001", "custtype": "P"
    }
    url_kis = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"

    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST)
    if now_kst.hour > 15 or (now_kst.hour == 15 and now_kst.minute >= 40):
        end_date = now_kst.strftime("%Y%m%d") 
    else:
        end_date = (now_kst - timedelta(days=1)).strftime("%Y%m%d") 

    data_list, history_list = [], []

    for i, row in enumerate(df_target.itertuples()):
        code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
        
        # 🔥 [V8.2] 네이버 금융에서 개별 종목의 '업종/섹터' 긁어오기
        sector_name = "분류안됨"
        try:
            url_nv = f"https://finance.naver.com/item/main.naver?code={code}"
            res_nv = requests.get(url_nv, headers={'User-Agent': 'Mozilla/5.0'})
            soup_nv = BeautifulSoup(res_nv.text, 'html.parser')
            sector_tag = soup_nv.select_one('div.trade_compare h4.h_sub a')
            if sector_tag:
                sector_name = sector_tag.text.strip()
        except:
            pass

        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": end_date, "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"}
        try:
            res = requests.get(url_kis, headers=headers, params=params)
            f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum = 0, 0, 0, 0
            foreign_streak, pension_streak, f_buying, p_buying = 0, 0, True, True  
            closes, vol_tr_sum_5d = [], 0 

            if res.status_code == 200 and res.json().get('rt_cd') == "0":
                daily_list = res.json().get('output2', [])
                if daily_list:
                    for idx, daily in enumerate(daily_list[:20]): 
                        close_prc = safe_api_float(daily.get('stck_clpr'))
                        closes.append(close_prc)
                        f_amt = safe_api_float(daily.get('frgn_ntby_qty')) * close_prc
                        p_amt = safe_api_float(daily.get('fund_ntby_qty')) * close_prc
                        t_amt = safe_api_float(daily.get('ivtr_ntby_qty')) * close_prc
                        pef_amt = safe_api_float(daily.get('pe_fund_ntby_vol')) * close_prc
                        
                        f_amt_sum += f_amt; p_amt_sum += p_amt; t_amt_sum += t_amt; pef_amt_sum += pef_amt
                        if idx < 5: vol_tr_sum_5d += (safe_api_float(daily.get('acml_vol')) * close_prc)

                        history_list.append({
                            '종목명': name, '일자': daily.get('stck_bsop_date', ''),
                            '종가': close_prc, '외인': f_amt / 1_000_000, '연기금': p_amt / 1_000_000,
                            '투신': t_amt / 1_000_000, '사모': pef_amt / 1_000_000
                        })

                    for daily in daily_list:
                        f_qty, p_qty = safe_api_float(daily.get('frgn_ntby_qty')), safe_api_float(daily.get('fund_ntby_qty'))
                        if f_buying:
                            if f_qty > 0: foreign_streak += 1
                            else: f_buying = False
                        if p_buying:
                            if p_qty > 0: pension_streak += 1
                            else: p_buying = False
                        if not f_buying and not p_buying: break

            ma20 = sum(closes) / len(closes) if closes else prpr
            gap_20, marcap_won = (prpr / ma20) * 100 if ma20 else 100, marcap * 100_000_000 
            f_str, p_str, t_str, pef_str = [(amt / marcap_won) * 100 if marcap_won else 0 for amt in (f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum)]
            turnover_rate = (vol_tr_sum_5d / marcap_won) * 100 if marcap_won else 0 
            
            tech_score = 15 if 101 <= gap_20 <= 108 else (-20 if gap_20 < 95 else (-10 if gap_20 > 115 else 0))
            if turnover_rate >= 10: tech_score += 15
            strength_score = (max(-10, min(10, p_str)) * 20.0) + (max(-5, min(5, t_str)) * 15.0) + (max(-5, min(5, pef_str)) * 15.0) + (max(-5, min(5, f_str)) * 10.0)
            streak_score = min(20, pension_streak * 3.0) + min(10, foreign_streak * 1.5)
            fund_score = (10 if row.ROE >= 15 else (5 if row.ROE >= 8 else 0)) + (5 if 0 < row.PER <= 15 else 0)

            ai_score = max(0, min(100, int(strength_score + streak_score + fund_score + tech_score)))

            data_list.append({
                '종목명': name, '종목코드': code, '소속': row.소속, 
                '섹터': sector_name, # 🔥 수집된 섹터 데이터 추가
                'AI수급점수': ai_score, '현재가': prpr, '등락률': row.등락률, 
                '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                '외인연속': foreign_streak, '연기금연속': pension_streak, '이격도(%)': round(gap_20, 1), '손바뀜(%)': round(turnover_rate, 1),
                '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
            })
        except Exception as e:
            pass 
        time.sleep(0.2) 

    df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
    df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
    df_history = pd.DataFrame(history_list)
    df_history.to_csv("history.csv", index=False, encoding='utf-8-sig')

    today_date = datetime.now(KST).strftime("%Y-%m-%d")
    df_trend_new = df_final[['종목명', '종목코드', 'AI수급점수']].copy()
    df_trend_new['순위'] = df_trend_new['AI수급점수'].rank(method='min', ascending=False).astype(int)
    df_trend_new['날짜'] = today_date

    trend_file = "score_trend.csv"
    if os.path.exists(trend_file):
        df_trend_old = pd.read_csv(trend_file)
        df_trend_old = df_trend_old[df_trend_old['날짜'] != today_date]
        df_trend_combined = pd.concat([df_trend_old, df_trend_new], ignore_index=True)
        df_trend_combined.to_csv(trend_file, index=False, encoding='utf-8-sig')
    else:
        df_trend_new.to_csv(trend_file, index=False, encoding='utf-8-sig')

    if GEMINI_API_KEY:
        try:
            genai.configure(api_key=GEMINI_API_KEY)
            model = genai.GenerativeModel('gemini-2.5-flash')
            top_N_names = df_final.head(20)['종목명'].tolist()
            latest_date = df_history['일자'].max()
            df_today = df_history[(df_history['일자'] == latest_date) & (df_history['종목명'].isin(top_N_names))]
            
            # 🔥 섹터 정보까지 AI에게 넘겨서 훨씬 정교한 시황 분석을 유도
            df_merged = pd.merge(df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)']], df_today[['종목명', '외인', '연기금']], on='종목명', how='left')
            df_merged.rename(columns={'외인': '당일_외인순매수(백만)', '연기금': '당일_연기금순매수(백만)'}, inplace=True)
            
            prompt = f"""
            너는 여의도 최고의 탑다운 퀀트 애널리스트야.
            아래는 ETF 노이즈가 제거된 최상위 20개 종목 데이터야.
            
            {df_merged.to_string(index=False)}

            다음 순서로 전문가 수준의 마감 리포트를 작성해 줘.
            (🚨주의: 제목은 쓰지 말고, 바로 '1. 🌐 글로벌 매크로' 본문부터 시작해!)
            
            1. 🌐 글로벌 매크로 & 실시간 이벤트 브리핑: 오늘 시장에 큰 영향을 미친 글로벌 뉴스(미국 증시, 금리 등)를 상세히 짚어줘.
            2. 🌪️ 국내 증시 섹터 및 당일 수급 동향: '섹터' 열을 분석해서, 오늘 외인/기관의 자금이 어느 업종(테마)에 집중되었는지 핵심을 짚어줘.
            3. 🎯 내일의 Top 3 관심종목 & 추천 사유: 반드시 표 안의 20개 종목 중에서만 3개를 골라 거시적 환경에 맞는 이유를 작성.
            """
            
            response = model.generate_content(prompt)
            today_str = datetime.now(KST).strftime("%Y년 %m월 %d일")
            
            report_body = f"## 🌐 여의도 탑다운 퀀트 애널리스트 마감 리포트 ({today_str})\n\n{response.text}"
            with open("report.md", "w", encoding="utf-8") as f:
                f.write(report_body)
                
            top3_str = ", ".join(top_N_names[:3])
            
            # 🚨 [커스텀 필요] 아래 변수 안의 주소를 대표님의 실제 스트림릿 주소로 꼭 바꿔주세요!
            MY_STREAMLIT_URL = "https://ge82mjcdoxngn3p6udv5sy.streamlit.app"
            
            tg_message = f"🔔 *[장 마감 수급 요약]*\n🗓 {today_str}\n\n🏆 *오늘의 수급 Top 3*\n: {top3_str}\n\n---\n\n{response.text}\n\n📊 [대시보드 바로가기]({MY_STREAMLIT_URL})"
            send_telegram_message(tg_message)

        except Exception as e:
            print(f"⚠️ AI 리포트 생성 실패: {e}")

if __name__ == "__main__":
    run_scraper()
