import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, timezone 
from bs4 import BeautifulSoup
import os
from google import genai
from google.genai import types
import yfinance as yf

URL_BASE = "https://openapi.koreainvestment.com:9443"
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET}
    try:
        res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
        return res.json().get("access_token")
    except:
        return None

def safe_float(text):
    try: return float(text.replace(',', '').replace('%', '').strip())
    except: return 0.0

def safe_api_float(val):
    try: return float(val) if val else 0.0
    except: return 0.0

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    diffs = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in diffs]
    losses = [-d if d < 0 else 0 for d in diffs]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(diffs)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

def calculate_dynamic_score(f_str, p_str, t_str, pef_str, vol_surge, rsi_val, gap_20, foreign_streak, pension_streak, current_vix):
    if current_vix < 25:
        raw_str_sum = (t_str * 3) + (pef_str * 3) + (f_str * 2) + (p_str * 1)
        strength_score = max(0, min(25, raw_str_sum * 3))
        streak_score = max(0, min(15, (foreign_streak * 1.5) + (pension_streak * 1.0)))
        supply_score = strength_score + streak_score

        v_score = 20 if vol_surge >= 200 else (15 if vol_surge >= 150 else (10 if vol_surge >= 100 else 0))
        r_score = 20 if 55 <= rsi_val <= 70 else (10 if 50 <= rsi_val < 55 else 0)
        momentum_score = v_score + r_score

        tech_score = 20 if 101 <= gap_20 <= 108 else (-20 if gap_20 < 95 else 0)
    else:
        raw_str_sum = (p_str * 4) + (f_str * 2) + (t_str * 0.5) + (pef_str * 0.5)
        strength_score = max(0, min(25, raw_str_sum * 4))
        streak_score = max(0, min(15, (pension_streak * 2.0) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score

        v_score = 20 if vol_surge >= 200 else (15 if vol_surge >= 150 else (10 if vol_surge >= 100 else 0))
        r_score = 20 if 55 <= rsi_val <= 70 else (10 if 50 <= rsi_val < 55 else 0)
        momentum_score = v_score + r_score

        tech_score = 20 if 98 <= gap_20 <= 105 else (-20 if gap_20 > 110 else 0)

    return max(0, min(100, int(supply_score + momentum_score + tech_score)))

def get_target_stock_list():
    target_list = []
    noise_keywords = ['KODEX', 'TIGER', 'RISE', 'ACE', 'KBSTAR', 'HANARO', 'KOSEF', 'SOL', 'PLUS', 'ARIRANG', 'ETN', '스팩', '인버스', '레버리지', 'CD금리', 'KOFR']
    custom_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
        for page in range(1, 7): 
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, headers=custom_headers, timeout=10)
                soup = BeautifulSoup(res.text, 'html.parser')
                for tr in soup.select('table.type_2 tbody tr'):
                    tds = tr.select('td')
                    if len(tds) > 11:
                        name_tag = tr.select_one('a.tltle')
                        if name_tag:
                            stock_name = name_tag.text
                            if any(keyword in stock_name for keyword in noise_keywords): continue
                            marcap = safe_float(tds[6].text)
                            if marcap >= 8000:
                                target_list.append({
                                    '종목명': stock_name, '종목코드': name_tag['href'].split('code=')[-1], 
                                    '소속': market_name, '현재가': int(safe_float(tds[2].text)),
                                    '등락률': safe_float(tds[4].text), '시가총액': int(marcap),
                                    'PER': safe_float(tds[10].text), 'ROE': safe_float(tds[11].text) # 🔥 대시보드용 부활
                                })
            except Exception as e: print(f"⚠️ 네이버 금융 파싱 에러: {e}")
            time.sleep(0.5) 
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace('**', '*') 
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000], "parse_mode": "Markdown"}
    try: requests.post(url, data=data)
    except: pass

def get_live_macro_and_news():
    tickers = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11", "S&P500": "^GSPC", "NASDAQ": "^IXIC", "환율": "KRW=X", "WTI유": "CL=F", "미 국채(10y)": "^TNX", "VIX": "^VIX"}
    macro_str = ""
    for name, ticker in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="2d")
            if len(hist) >= 2:
                curr, prev = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
                change_pct = ((curr - prev) / prev) * 100
                macro_str += f"- {name}: {curr:.2f} ({change_pct:+.2f}%)\n"
        except: pass
    
    news_str = ""
    try:
        res = requests.get("https://finance.naver.com/news/mainnews.naver", headers={'User-Agent': 'Mozilla/5.0'})
        soup = BeautifulSoup(res.text, 'html.parser')
        headlines = [tag.text.strip() for tag in soup.select('.articleSubject a')[:5]]
        news_str = "\n".join([f"- {h}" for h in headlines])
    except: news_str = "- 뉴스 수집 실패"
    
    return macro_str, news_str

def run_scraper():
    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST)
    
    try:
        vix_hist = yf.Ticker("^VIX").history(period="1d")
        current_vix = float(vix_hist['Close'].iloc[-1])
    except:
        current_vix = 15.0 
    
    regime = "공포/하락장 (방어수 우대)" if current_vix >= 25 else "평온/강세장 (공격수 우대)"
    print(f"🚀 수집기 봇 가동 시작 (V32.1 대시보드 렌더링 픽스)...\n🌍 실시간 VIX 지수: {current_vix:.2f} ➔ [{regime}] 가동")
    
    is_eod_updated = (now_kst.hour > 15) or (now_kst.hour == 15 and now_kst.minute >= 40)
    today_date = now_kst.strftime("%Y-%m-%d")
    
    if not is_eod_updated and os.path.exists("data.csv"):
        print("⚡ [Fast-Track 모드] 실시간 주가 갱신 및 랭킹 산출을 진행합니다.")
        df_target = get_target_stock_list()
        df_saved = pd.read_csv("data.csv")
        
        updated_rows = []
        for _, row in df_saved.iterrows():
            row_dict = row.to_dict()
            live_info = df_target[df_target['종목명'] == row_dict['종목명']]
            if not live_info.empty:
                row_dict['현재가'] = live_info.iloc[0]['현재가']
                row_dict['등락률'] = live_info.iloc[0]['등락률']
                row_dict['시가총액'] = live_info.iloc[0]['시가총액']
                # 🔥 망가진 CSV라도 실시간으로 PER/ROE를 채워넣어 대시보드 자가 치유
                row_dict['PER'] = live_info.iloc[0]['PER']
                row_dict['ROE'] = live_info.iloc[0]['ROE']

            new_score = calculate_dynamic_score(
                f_str=row_dict.get('외인강도(%)', 0), p_str=row_dict.get('연기금강도(%)', 0),
                t_str=row_dict.get('투신강도(%)', 0), pef_str=row_dict.get('사모강도(%)', 0),
                vol_surge=row_dict.get('거래급증(%)', 0), rsi_val=row_dict.get('RSI', 50),
                gap_20=row_dict.get('이격도(%)', 100), foreign_streak=row_dict.get('외인연속', 0),
                pension_streak=row_dict.get('연기금연속', 0), current_vix=current_vix
            )
            row_dict['AI수급점수'] = new_score
            updated_rows.append(row_dict)
            
        df_final = pd.DataFrame(updated_rows).sort_values('AI수급점수', ascending=False)
        df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
        
    else:
        print("📥 [Full-Parsing 모드] 장 마감 KIS 수급 데이터를 전체 수집합니다.")
        df_target = get_target_stock_list()
        token = get_kis_access_token()
        headers = {"authorization": f"Bearer {token}", "appkey": KIS_APP_KEY, "appsecret": KIS_APP_SECRET, "tr_id": "FHPTJ04160001", "custtype": "P"}
        url_kis = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
        end_date = now_kst.strftime("%Y%m%d") if is_eod_updated else (now_kst - timedelta(days=1)).strftime("%Y%m%d") 

        data_list, history_list = [], []

        for i, row in enumerate(df_target.itertuples()):
            code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
            sector_name = "분류안됨"
            try:
                res_nv = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                sector_tag = BeautifulSoup(res_nv.text, 'html.parser').select_one('div.trade_compare h4.h_sub a')
                if sector_tag: sector_name = sector_tag.text.strip()
            except: pass

            params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": end_date, "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"}
            
            try:
                res = requests.get(url_kis, headers=headers, params=params, timeout=5)
                f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum = 0, 0, 0, 0
                foreign_streak, pension_streak, f_buying, p_buying = 0, 0, True, True  
                closes, volumes, vol_tr_sum_5d = [], [], 0 

                if res.status_code == 200 and res.json().get('rt_cd') == "0":
                    daily_list = res.json().get('output2', [])
                    if daily_list:
                        for idx, daily in enumerate(daily_list[:20]): 
                            close_prc = safe_api_float(daily.get('stck_clpr'))
                            vol = safe_api_float(daily.get('acml_vol'))
                            closes.append(close_prc)
                            volumes.append(vol)
                            
                            f_amt = safe_api_float(daily.get('frgn_ntby_qty')) * close_prc
                            p_amt = safe_api_float(daily.get('fund_ntby_qty')) * close_prc
                            t_amt = safe_api_float(daily.get('ivtr_ntby_qty')) * close_prc
                            pef_amt = safe_api_float(daily.get('pe_fund_ntby_vol')) * close_prc
                            
                            f_amt_sum += f_amt; p_amt_sum += p_amt; t_amt_sum += t_amt; pef_amt_sum += pef_amt
                            if idx < 5: vol_tr_sum_5d += (vol * close_prc)

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
                
                rsi_val = calculate_rsi(closes[::-1])
                if len(volumes) > 1:
                    past_vols = volumes[1:6]
                    avg_vol = sum(past_vols) / len(past_vols) if past_vols else 0
                    vol_surge = (volumes[0] / avg_vol * 100) if avg_vol > 0 else 0
                else:
                    vol_surge = 0

                ai_score = calculate_dynamic_score(f_str, p_str, t_str, pef_str, vol_surge, rsi_val, gap_20, foreign_streak, pension_streak, current_vix)

                data_list.append({
                    '종목명': name, '종목코드': code, '소속': row.소속, '섹터': sector_name, 'AI수급점수': ai_score,
                    '현재가': prpr, '등락률': row.등락률, '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                    '외인연속': foreign_streak, '연기금연속': pension_streak, '이격도(%)': round(gap_20, 1), '손바뀜(%)': round(turnover_rate, 1),
                    'RSI': round(rsi_val, 1), '거래급증(%)': round(vol_surge, 1),
                    '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE # 🔥 대시보드용 부활
                })
            except: pass 
            time.sleep(0.2) 

        if not data_list: return

        df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
        df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')
        
        df_history = pd.DataFrame(history_list)
        df_history.to_csv("history.csv", index=False, encoding='utf-8-sig')

        df_trend_new = df_final[['종목명', '종목코드', 'AI수급점수']].copy()
        df_trend_new['순위'] = df_trend_new['AI수급점수'].rank(method='first', ascending=False).astype(int)
        df_trend_new['날짜'] = today_date

        trend_file = "score_trend.csv"
        if os.path.exists(trend_file):
            df_trend_old = pd.read_csv(trend_file)
            df_trend_old = df_trend_old[df_trend_old['날짜'] != today_date]
            pd.concat([df_trend_old, df_trend_new], ignore_index=True).to_csv(trend_file, index=False, encoding='utf-8-sig')
        else:
            df_trend_new.to_csv(trend_file, index=False, encoding='utf-8-sig')
       
    portfolio_file = "portfolio.csv"
    perf_file = "performance_trend.csv"
    eval_msg = ""
    top3_names = df_final.head(3)['종목명'].tolist() 

    if os.path.exists(portfolio_file):
        try:
            df_port = pd.read_csv(portfolio_file)
            last_date = str(df_port['날짜'].iloc[0]) if not df_port.empty and '날짜' in df_port.columns else ""
            
            returns, eval_details = [], []
            for _, row in df_port.iterrows():
                p_stock = row['종목명']
                p_buy = row['매수가']
                today_row = df_final[df_final['종목명'] == p_stock]
                p_sell = today_row.iloc[0]['현재가'] if not today_row.empty else p_buy
                ret = ((p_sell - p_buy) / p_buy) * 100
                returns.append(ret)
                mark = "🔴" if ret > 0 else "🔵" if ret < 0 else "⚫"
                eval_details.append(f"- {p_stock}: {ret:+.2f}% {mark}")
            
            daily_ret = sum(returns) / len(returns) if returns else 0

            if is_eod_updated:
                eval_msg = "📝 *[전일 추천 Top 3 최종 성적표]*\n" + "\n".join(eval_details) + f"\n➡️ *오늘 포트폴리오 최종 수익률: {daily_ret:+.2f}%*\n\n"
                
                if last_date != today_date:
                    cum_ret = daily_ret
                    if os.path.exists(perf_file):
                        df_perf = pd.read_csv(perf_file)
                        if not df_perf.empty:
                            df_perf = df_perf[df_perf['날짜'] != today_date]
                            cum_ret = df_perf['누적수익률'].iloc[-1] + daily_ret if len(df_perf) > 0 else daily_ret
                        else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])
                    else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])
                        
                    new_perf = pd.DataFrame([{'날짜': today_date, '일간수익률': daily_ret, '누적수익률': cum_ret}])
                    pd.concat([df_perf, new_perf], ignore_index=True).to_csv(perf_file, index=False, encoding='utf-8-sig')
                    
                    top3_df = df_final.head(3)[['종목명', '현재가']].rename(columns={'현재가': '매수가'})
                    top3_df['날짜'] = today_date
                    top3_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
                    print("💡 [EOD] 포트폴리오 최종 정산 및 종목 교체 완료.")
                else:
                    print("💡 [EOD] 오늘 이미 포트폴리오가 갱신되었습니다.")
            else:
                eval_msg = "📝 *[현재 포트폴리오 장중 수익률]*\n" + "\n".join(eval_details) + f"\n➡️ *실시간 수익률: {daily_ret:+.2f}%*\n\n"
                print("⚡ [장중] 포트폴리오 실시간 수익률만 계산 (저장 생략).")
        except Exception as e: print(f"⚠️ 포트폴리오 처리 에러: {e}")
    else:
        if is_eod_updated:
            top3_df = df_final.head(3)[['종목명', '현재가']].rename(columns={'현재가': '매수가'})
            top3_df['날짜'] = today_date
            top3_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')

    if GEMINI_API_KEY:
        try:
            client = genai.Client(api_key=GEMINI_API_KEY)
            
            top_N_names = df_final.head(20)['종목명'].tolist()
            if os.path.exists("history.csv"):
                df_history = pd.read_csv("history.csv")
                latest_date = df_history['일자'].max()
                df_today = df_history[(df_history['일자'] == latest_date) & (df_history['종목명'].isin(top_N_names))]
                df_merged = pd.merge(df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']], df_today[['종목명', '외인', '연기금']], on='종목명', how='left')
                df_merged.rename(columns={'외인': '당일_외인순매수(백만)', '연기금': '당일_연기금순매수(백만)'}, inplace=True)
            else:
                df_merged = df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']]
            
            macro_str, news_str = get_live_macro_and_news()
            
            prompt = f"""
            너는 여의도 최고의 탑다운 퀀트 애널리스트야. 오늘은 {now_kst.strftime("%Y년 %m월 %d일")}이야.
            현재 VIX 지수는 {current_vix:.2f}로 {regime} 모드로 포트폴리오가 구성되었어.
            
            [1. 매크로 지표]
            {macro_str}
            
            [2. 금융 속보]
            {news_str}
            
            [3. 최상위 20개 종목 수급 및 모멘텀 데이터]
            {df_merged.to_string(index=False)}

            다음 순서로 전문가 수준의 마감 리포트를 작성해 줘.
            1. 🌐 글로벌 매크로 브리핑: 구글 검색을 활용하여 오늘 시장을 움직인 뉴스를 요약해.
            2. 🌪️ 섹터 및 수급 동향: RSI와 거래급증(%)을 참고하여 쏠림 현상을 분석해.
            3. 🎯 Top 3 관심종목 & 추천 사유: 반드시 표 안의 20개 종목 중에서만 3개를 골라 구글 검색 이슈와 맞물려 설명해.
[🚨 절대 엄수 사항] 텔레그램 전송용이므로 마크다운 표(Table)는 절대 사용하지 마.
            """
            
            config = types.GenerateContentConfig(tools=[{"google_search": {}}])
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt, config=config)
            
            with open("report.md", "w", encoding="utf-8") as f:
                f.write(f"## 🌐 여의도 탑다운 퀀트 애널리스트 마감 리포트 ({now_kst.strftime('%Y-%m-%d')})\n\n{response.text}")
                
            top3_str = ", ".join(top3_names)
            MY_STREAMLIT_URL = "https://ge82mjcdoxngn3p6udv5sy.streamlit.app"
            
            timing_tag = "[장중 실시간 브리핑]" if not is_eod_updated else "[장 마감 수급 요약]"
            tg_message = f"🔔 *{timing_tag}*\n🗓 {now_kst.strftime('%Y-%m-%d %H:%M')}\n📊 VIX 국면: {regime}\n\n{eval_msg}🏆 *오늘의 퀀트 픽 Top 3*\n: {top3_str}\n\n---\n\n{response.text}\n\n📊 [대시보드 바로가기]({MY_STREAMLIT_URL})"
            send_telegram_message(tg_message)
        except Exception as e: print(f"⚠️ AI 리포트 생성 실패: {e}")

if __name__ == "__main__":
    run_scraper()
