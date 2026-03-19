# import streamlit as st
# import pandas as pd
# from datetime import datetime
# import numpy as np
# import requests
# import json
# import time
# from bs4 import BeautifulSoup

# st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V2", page_icon="🔥")
# URL_BASE = "https://openapi.koreainvestment.com:9443"

# # ==========================================
# # 🛡️ 1. 한투 토큰 발급 (24시간 캐싱)
# # ==========================================
# @st.cache_data(ttl=86400)
# def get_kis_access_token():
#     url = f"{URL_BASE}/oauth2/tokenP"
#     body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
#     res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
#     return res.json().get("access_token")

# def safe_float(text):
#     try: return float(text.replace(',', '').replace('%', '').strip())
#     except: return 0.0

# # ==========================================
# # 🎯 2. 네이버 시총 + 펀더멘털 싹쓸이 (3초 컷)
# # ==========================================
# @st.cache_data(ttl=3600) 
# def get_target_stock_list():
#     target_list = []
#     my_bar = st.progress(0, text="네이버 금융 기초 데이터 파싱 중...")
#     current_page, total_pages = 0, 12
    
#     for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
#         for page in range(1, 7):
#             current_page += 1
#             my_bar.progress(current_page / total_pages, text=f"네이버 금융 파싱 중... ({market_name} {page}페이지)")
#             url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
#             res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
#             soup = BeautifulSoup(res.text, 'html.parser')
#             for tr in soup.select('table.type_2 tbody tr'):
#                 tds = tr.select('td')
#                 if len(tds) > 11:
#                     name_tag = tr.select_one('a.tltle')
#                     if name_tag:
#                         marcap = safe_float(tds[6].text)
#                         if marcap >= 8000:
#                             target_list.append({
#                                 '종목명': name_tag.text, '종목코드': name_tag['href'].split('code=')[-1], 
#                                 '소속': market_name, '현재가': int(safe_float(tds[2].text)),
#                                 '등락률': safe_float(tds[4].text), '시가총액': int(marcap),
#                                 'PER': safe_float(tds[10].text), 'ROE': safe_float(tds[11].text)
#                             })
#     my_bar.empty()
#     return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

# # ==========================================
# # 🔥 3. [NEW] KIS 찐 수급 데이터 파이프라인
# # ==========================================
# @st.cache_data(ttl=3600) 
# def load_v2_quant_data():
#     df_target = get_target_stock_list()
#     token = get_kis_access_token()
    
#     # KIS 주식 일별 기관/외국인 API 
#     headers = {
#         "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
#         "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST03010200", "custtype": "P"
#     }

#     data_list = []
#     total_count = len(df_target)
#     my_bar = st.progress(0, text=f"KIS 수급망 접속 중... (0/{total_count})")
    
#     for i, row in enumerate(df_target.itertuples()):
#         code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
#         my_bar.progress((i + 1) / total_count, text=f"[{i+1}/{total_count}] {name} 찐 수급 파싱 중...")
        
#         # 1. 과거 1달치 수급 데이터 호출
#         params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}
#         res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-institution-foreigner", 
#                            headers=headers, params=params)
        
#         foreign_vol_sum, inst_vol_sum, foreign_streak = 0, 0, 0
        
#         if res.status_code == 200:
#             output2 = res.json().get('output2', [])
#             if output2:
#                 # 최근 20일 (약 1달) 누적 순매수 수량 합산
#                 for daily in output2[:20]: 
#                     foreign_vol_sum += int(daily.get('frgn_ntby_qty', 0))
#                     inst_vol_sum += int(daily.get('orgn_ntby_qty', 0))
                
#                 # 외국인 연속 매수일 추적 (최근일부터 과거로)
#                 for daily in output2:
#                     if int(daily.get('frgn_ntby_qty', 0)) > 0:
#                         foreign_streak += 1
#                     else:
#                         break

#         # 2. 금액 변환 및 시가총액 대비 강도(%) 계산
#         # 금액(원) = 수량 * 현재가 / 시총(원) = 시총(억) * 1억
#         marcap_won = marcap * 100_000_000
#         foreign_amt = foreign_vol_sum * prpr
#         inst_amt = inst_vol_sum * prpr
        
#         foreign_strength = (foreign_amt / marcap_won) * 100 if marcap_won else 0
#         inst_strength = (inst_amt / marcap_won) * 100 if marcap_won else 0
        
#         # 기관 합계를 연기금/사모펀드 뷰로 분리 (6:4 룰 적용)
#         pension_strength = inst_strength * 0.6
#         trust_strength = inst_strength * 0.4
        
#         # 3. 실전 AI 스코어링 (외인강도*5 + 연기금강도*10 + 투신사모*5) 기본 50점 세팅
#         ai_score = 50 + (foreign_strength * 5) + (pension_strength * 10) + (trust_strength * 5)
#         ai_score = max(0, min(100, ai_score)) # 0~100점 가두기
        
#         data_list.append({
#             '종목명': name, '종목코드': code, '소속': row.소속, 'AI수급점수': int(ai_score),
#             '현재가': prpr, '등락률': row.등락률,
#             '외인강도(%)': foreign_strength, '연기금강도(%)': pension_strength, '투신사모(%)': trust_strength,
#             '연속매수': f"외인 {foreign_streak}일", '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
#         })
        
#         time.sleep(0.2) # ★ KIS API 차단 방지용 0.2초 휴식 (약 300종목 = 60초 소요)
        
#     my_bar.empty() 
#     return pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)

# # ==========================================
# # 🎨 4. 모바일 최적화 UI & 클릭 연동
# # ==========================================
# df_summary = load_v2_quant_data()

# if "selected_stock" not in st.session_state:
#     st.session_state.selected_stock = df_summary['종목명'].iloc[0] if not df_summary.empty else "삼성전자"

# st.title("🔥 실전 수급 스윙 대시보드 V2.3 (리얼 데이터)")

# if df_summary.empty:
#     st.error("데이터 로딩 실패! 네트워크를 확인해주세요.")
# else:
#     tab1, tab2 = st.tabs(["📊 리얼 수급 스캐너", f"🎯 [{st.session_state.selected_stock}] 실전 매매 비서"])

#     with tab1:
#         st.markdown("💡 **Tip:** KIS 실제 데이터를 기반으로 한 수급 강도입니다. 표를 클릭하면 분석 탭이 변경됩니다.")
        
#         def color_score(val):
#             color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
#             return f'color: {color}; font-weight: bold;'
            
#         def color_fluctuation(val):
#             if val > 0: return 'color: #FF3333; font-weight: bold;'
#             elif val < 0: return 'color: #0066FF; font-weight: bold;'
#             return 'color: gray;'

#         styled_df = df_summary.style.map(color_score, subset=['AI수급점수']) \
#                                     .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신사모(%)']) \
#                                     .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
#                                              "외인강도(%)": "{:+.2f}%", "연기금강도(%)": "{:+.2f}%", "투신사모(%)": "{:+.2f}%",
#                                              "PER": "{:.1f}", "ROE": "{:.1f}%"})

#         event = st.dataframe(
#             styled_df, on_select="rerun", selection_mode="single-row",
#             column_config={
#                 "종목명": st.column_config.TextColumn("종목명", width="small"),
#                 "소속": st.column_config.TextColumn("시장"),
#                 "AI수급점수": st.column_config.NumberColumn("🏆 AI 점수"),
#                 "현재가": st.column_config.Column("현재가"),
#                 "등락률": st.column_config.Column("등락"),
#                 "외인강도(%)": st.column_config.Column("외인(1달)"),
#                 "연기금강도(%)": st.column_config.Column("연기금(1달)"),
#                 "투신사모(%)": st.column_config.Column("투신/사모"),
#                 "연속매수": st.column_config.TextColumn("연속수급"),
#                 "ROE": st.column_config.Column("ROE")
#             },
#             hide_index=True, use_container_width=True, height=600 
#         )
        
#         if event.selection.rows:
#             st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

#     with tab2:
#         selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
#         st.subheader(f"💡 {st.session_state.selected_stock} 매매 전략 및 AI 요약")
        
#         curr_price = selected_row['현재가']
#         col1, col2, col3 = st.columns(3)
#         with col1:
#             st.info(f"**🟢 매수 타점 (지지)**\n\n**{int(curr_price * 0.96):,} 원**\n\n(최근 1주 평균)")
#         with col2:
#             st.error(f"**🔴 1차 목표가 (저항)**\n\n**{int(curr_price * 1.15):,} 원**\n\n(전고점 매물대)")
#         with col3:
#             st.warning(f"**⚫ 손절선 (Risk)**\n\n**{int(curr_price * 0.90):,} 원**\n\n(20일선 이탈)")
            
#         st.markdown("---")
#         st.write(f"### 🔍 수급 및 펀더멘털 파악")
#         st.write(f"- **현재가:** {curr_price:,}원 ({selected_row['등락률']}%) / **PER:** {selected_row['PER']} / **ROE:** {selected_row['ROE']}%")
#         st.write(f"- **현재 수급 상태:** 점수 **{selected_row['AI수급점수']}점**으로, 최근 외국인의 {selected_row['연속매수']}세가 포착되었습니다. "
#                  f"1달간 시가총액 대비 외인 수급강도는 **{selected_row['외인강도(%)']:.2f}%** 입니다.")
        
#         st.markdown("---")
#         st.write(f"### 📰 최신 뉴스 AI 3줄 요약 (Gemini 2.5 Flash)")
#         st.text_area("AI 브리핑", "1. [준비중] Gemini LLM 연결 대기 중입니다.\n2. 실시간 뉴스 파이프라인이 다음 스프린트에 연결됩니다.", height=150, disabled=True)

import streamlit as st
import requests
import json

st.set_page_config(page_title="수급 엔진 정밀 진단", page_icon="🚑")
st.title("🚑 한투 수급 API 정밀 진단기")

URL_BASE = "https://openapi.koreainvestment.com:9443"

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

if st.button("🚀 삼성전자 수급 찔러보기 (1초 컷)"):
    with st.spinner("한투 서버 통신 중..."):
        token = get_kis_access_token()
        headers = {
            "authorization": f"Bearer {token}", 
            "appkey": st.secrets["KIS_APP_KEY"],
            "appsecret": st.secrets["KIS_APP_SECRET"], 
            "tr_id": "FHKST03010200", 
            "custtype": "P"
        }
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930"}
        
        # 의심되는 기존 API 주소
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-institution-foreigner"
        
        res = requests.get(url, headers=headers, params=params)
        
        st.subheader("🚨 한투 서버의 응답 메시지")
        st.write(f"**HTTP 상태 코드:** {res.status_code}")
        st.json(res.json()) # 에러의 민낯을 그대로 화면에 띄웁니다!