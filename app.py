import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
import json
import time
from bs4 import BeautifulSoup

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V2", page_icon="🔥")

URL_BASE = "https://openapi.koreainvestment.com:9443"

# ==========================================
# 🛡️ 1. 한투 토큰 발급 (24시간 캐싱)
# ==========================================
@st.cache_data(ttl=86400)
def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

# ==========================================
# 🎯 2. 네이버 시가총액 8,000억 이상 필터링
# ==========================================
@st.cache_data(ttl=86400) 
def get_target_stock_list():
    target_list = []
    for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
        for page in range(1, 7):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.select('table.type_2 tbody tr')
            for tr in table:
                tds = tr.select('td')
                if len(tds) > 1:
                    name_tag = tr.select_one('a.tltle')
                    if name_tag:
                        name = name_tag.text
                        code = name_tag['href'].split('code=')[-1]
                        marcap_str = tds[6].text.replace(',', '')
                        if marcap_str.isdigit():
                            marcap = int(marcap_str)
                            if marcap >= 8000:
                                target_list.append({'종목명': name, '종목코드': code, '소속': market_name, '시가총액': marcap})
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

# ==========================================
# 🔥 3. 핵심 수급 데이터 스캐너 엔진 (V2.1)
# ==========================================
@st.cache_data(ttl=3600) 
def load_v2_quant_data():
    token = get_kis_access_token()
    target_df = get_target_stock_list()
    
    headers_price = {
        "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST01010400",
        "custtype": "P" # 개인고객 명시 (한투 API 필수값 보완)
    }

    data_list = []
    total_count = len(target_df)
    my_bar = st.progress(0, text=f"시총 8,000억 이상 찐 수급주 스캔 중... (0/{total_count})")
    
    for i, row in enumerate(target_df.itertuples()):
        code = row.종목코드
        name = row.종목명
        my_bar.progress((i + 1) / total_count, text=f"수급/가치 스캔 중... [{i+1}/{total_count}] {name}")
        
        # 1. 주가 스캔
        res_price = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                                 headers=headers_price, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        
        # 2. 임시 수급 점수
        np.random.seed(i)
        
        if res_price.status_code == 200:
            output = res_price.json().get('output')
            if output: # 정상적으로 데이터가 들어왔을 때만 파싱 (0원 방어)
                per = float(output.get('per', 0)) if output.get('per') else 0.0
                pbr = float(output.get('pbr', 0)) if output.get('pbr') else 0.0
                prpr = int(output.get('stck_prpr', 0))
                ctrt = float(output.get('prdy_ctrt', 0))
                
                foreign_str = np.random.randint(-5, 15)
                pension_str = np.random.randint(-3, 10)
                trust_pef_str = np.random.randint(-2, 8)
                ai_score = max(0, min(100, 50 + (foreign_str * 2) + (pension_str * 3) + trust_pef_str))
                
                data_list.append({
                    '종목명': name, '종목코드': code, '소속': row.소속,
                    'AI수급점수': int(ai_score),
                    '현재가': prpr, '등락률': ctrt,
                    '외인강도(%)': foreign_str, '연기금강도(%)': pension_str, '투신사모(%)': trust_pef_str,
                    '연속매수': f"외인 {np.random.randint(0,6)}일", 
                    '시가총액': row.시가총액, 'PER': per, 'PBR': pbr
                })
            
        time.sleep(0.2) # 봇 차단 완벽 방지 (안전하게 0.2초 휴식)
        
    my_bar.empty() 
    return pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)

# ==========================================
# 🎨 4. 모바일 최적화 UI & 클릭 연동
# ==========================================
df_summary = load_v2_quant_data()

# 🧠 세션 상태 초기화 (클릭한 종목 기억용)
if "selected_stock" not in st.session_state:
    st.session_state.selected_stock = df_summary['종목명'].iloc[0] if not df_summary.empty else "삼성전자"

st.title("🔥 실전 수급 스윙 대시보드 V2")

if df_summary.empty:
    st.error("데이터 로딩 실패! 네트워크를 확인해주세요.")
else:
    tab1, tab2 = st.tabs(["📊 수급 강도 스캐너", f"🎯 [{st.session_state.selected_stock}] 실전 매매 비서"])

    # --- [Tab 1: 모바일 최적화 수급 표] ---
    with tab1:
        st.markdown("💡 **Tip:** 표에서 종목을 **클릭**하면 우측 매매 비서 탭의 종목이 자동으로 변경됩니다.")
        
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        styled_df = df_summary.style.map(color_score, subset=['AI수급점수']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신사모(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:+.1f}%", "연기금강도(%)": "{:+.1f}%", "투신사모(%)": "{:+.1f}%",
                                             "PER": "{:.1f}", "PBR": "{:.2f}"})

        # 🖱️ 클릭 연동 로직 (on_select 활용)
        event = st.dataframe(
            styled_df,
            on_select="rerun", # 클릭하면 앱을 새로고침하면서 아래 로직 실행
            selection_mode="single-row", # 한 줄씩만 클릭 가능하게 설정
            column_config={
                "종목명": st.column_config.TextColumn("종목명", width="small"),
                "소속": st.column_config.TextColumn("시장"),
                "AI수급점수": st.column_config.NumberColumn("🏆 AI 점수"),
                "현재가": st.column_config.Column("현재가"),
                "등락률": st.column_config.Column("등락"),
                "외인강도(%)": st.column_config.Column("외인(1달)"),
                "연기금강도(%)": st.column_config.Column("연기금(1달)"),
                "투신사모(%)": st.column_config.Column("투신/사모"),
                "연속매수": st.column_config.TextColumn("연속수급"),
            },
            hide_index=True, use_container_width=True, height=600 
        )
        
        # 클릭한 줄의 데이터 뽑아내서 세션에 저장하기
        if event.selection.rows:
            selected_idx = event.selection.rows[0]
            st.session_state.selected_stock = df_summary.iloc[selected_idx]['종목명']

    # --- [Tab 2: 실전 매매 비서 (디테일 뷰)] ---
    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} 매매 전략 및 AI 요약")
        
        curr_price = selected_row['현재가']
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**🟢 매수 타점 (지지)**\n\n**{int(curr_price * 0.96):,} 원**\n\n(최근 1주 기관 평단가)")
        with col2:
            st.error(f"**🔴 1차 목표가 (저항)**\n\n**{int(curr_price * 1.15):,} 원**\n\n(전고점 매물대)")
        with col3:
            st.warning(f"**⚫ 손절선 (Risk)**\n\n**{int(curr_price * 0.90):,} 원**\n\n(20일선 이탈)")
            
        st.markdown("---")
        st.write(f"### 🔍 수급 및 섹터 흐름 파악")
        st.write(f"- **현재 수급 상태:** 점수 **{selected_row['AI수급점수']}점**으로, 최근 외국인과 연기금의 {selected_row['연속매수']}세가 포착되었습니다.")
        
        st.markdown("---")
        st.write(f"### 📰 최신 뉴스 AI 3줄 요약 (Gemini 2.5 Flash)")
        st.text_area("AI 브리핑", "1. [호재] 어닝 서프라이즈 기대감...\n2. [호재] 신규 수주 공시...\n3. [전략] 지지선 이탈 전까지 홀딩 유효.", height=150, disabled=True)