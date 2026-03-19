import streamlit as st
import pandas as pd
from datetime import datetime
import numpy as np
import requests
import json
import time
from bs4 import BeautifulSoup

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V2", page_icon="🔥")

URL_BASE = "https://openapi.koreainvestment.com:9443"

# ==========================================
# 🛡️ 1. 한투 토큰 발급 (수급 API용으로 유지)
# ==========================================
@st.cache_data(ttl=86400)
def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

# 텍스트 데이터를 숫자로 안전하게 바꾸는 마법의 함수
def safe_float(text):
    try:
        return float(text.replace(',', '').replace('%', '').strip())
    except:
        return 0.0

# ==========================================
# 🎯 2. 네이버 시총 + 펀더멘털 싹쓸이 엔진 (대표님 아이디어 적용!)
# ==========================================
@st.cache_data(ttl=3600) 
def get_target_stock_list():
    target_list = []
    my_bar = st.progress(0, text="네이버 금융 데이터 파싱 중...")
    
    total_pages = 12 # 코스피 6 + 코스닥 6
    current_page = 0
    
    for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
        for page in range(1, 7):
            current_page += 1
            my_bar.progress(current_page / total_pages, text=f"네이버 금융 파싱 중... ({market_name} {page}페이지)")
            
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.select('table.type_2 tbody tr')
            
            for tr in table:
                tds = tr.select('td')
                if len(tds) > 11: # 데이터가 꽉 찬 줄만 취급
                    name_tag = tr.select_one('a.tltle')
                    if name_tag:
                        name = name_tag.text
                        code = name_tag['href'].split('code=')[-1]
                        
                        # 네이버 표에서 한방에 싹쓸이!
                        marcap = safe_float(tds[6].text) # 시가총액 (억)
                        if marcap >= 8000:
                            target_list.append({
                                '종목명': name, '종목코드': code, '소속': market_name,
                                '현재가': int(safe_float(tds[2].text)),
                                '등락률': safe_float(tds[4].text),
                                '시가총액': int(marcap),
                                'PER': safe_float(tds[10].text),
                                'ROE': safe_float(tds[11].text) # PBR 대신 더 중요한 ROE를 가져옵니다!
                            })
    my_bar.empty()
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

# ==========================================
# 🔥 3. 핵심 수급 데이터 스캐너 (초고속 버전)
# ==========================================
@st.cache_data(ttl=3600) 
def load_v2_quant_data():
    # 이제 KIS 주가 API 호출 루프가 통째로 날아갔습니다!
    df = get_target_stock_list()
    
    # 임시 수급 점수 (다음 스프린트에서 KIS 수급 API로 교체될 부분)
    # 현재가 등은 이미 df에 다 들어있으므로, 컬럼만 추가해주면 끝입니다. 엄청 빠릅니다!
    np.random.seed(42) # 전체 배열에 대한 랜덤
    
    df['외인강도(%)'] = np.random.randint(-5, 15, size=len(df))
    df['연기금강도(%)'] = np.random.randint(-3, 10, size=len(df))
    df['투신사모(%)'] = np.random.randint(-2, 8, size=len(df))
    df['연속매수'] = [f"외인 {x}일" for x in np.random.randint(0, 6, size=len(df))]
    
    # AI 점수 계산
    df['AI수급점수'] = np.clip(50 + (df['외인강도(%)'] * 2) + (df['연기금강도(%)'] * 3) + df['투신사모(%)'], 0, 100).astype(int)
    
    return df.sort_values('AI수급점수', ascending=False)

# ==========================================
# 🎨 4. 모바일 최적화 UI & 클릭 연동
# ==========================================
df_summary = load_v2_quant_data()

if "selected_stock" not in st.session_state:
    st.session_state.selected_stock = df_summary['종목명'].iloc[0] if not df_summary.empty else "삼성전자"

st.title("🔥 실전 수급 스윙 대시보드 V2.2")

if df_summary.empty:
    st.error("데이터 로딩 실패! 네트워크를 확인해주세요.")
else:
    tab1, tab2 = st.tabs(["📊 수급 강도 스캐너", f"🎯 [{st.session_state.selected_stock}] 실전 매매 비서"])

    with tab1:
        st.markdown("💡 **Tip:** 표에서 종목을 **클릭**하면 우측 매매 비서 탭의 종목이 자동으로 변경됩니다.")
        
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        # PBR을 빼고 주식의 진정한 체력인 ROE로 포맷을 바꿨습니다!
        styled_df = df_summary.style.map(color_score, subset=['AI수급점수']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신사모(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:+.1f}%", "연기금강도(%)": "{:+.1f}%", "투신사모(%)": "{:+.1f}%",
                                             "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df,
            on_select="rerun",
            selection_mode="single-row",
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
                "ROE": st.column_config.Column("ROE")
            },
            hide_index=True, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            selected_idx = event.selection.rows[0]
            st.session_state.selected_stock = df_summary.iloc[selected_idx]['종목명']

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
        st.write(f"### 🔍 수급 및 펀더멘털 파악")
        st.write(f"- **현재가:** {curr_price:,}원 ({selected_row['등락률']}%) / **PER:** {selected_row['PER']} / **ROE:** {selected_row['ROE']}%")
        st.write(f"- **현재 수급 상태:** 점수 **{selected_row['AI수급점수']}점**으로, 최근 외국인과 연기금의 {selected_row['연속매수']}세가 포착되었습니다.")
        
        st.markdown("---")
        st.write(f"### 📰 최신 뉴스 AI 3줄 요약 (Gemini 2.5 Flash)")
        st.text_area("AI 브리핑", "1. [호재] 어닝 서프라이즈 기대감...\n2. [호재] 신규 수주 공시...\n3. [전략] 지지선 이탈 전까지 홀딩 유효.", height=150, disabled=True)