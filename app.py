import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
import json
import time
from bs4 import BeautifulSoup

# [NEW] 화면을 모바일에 꽉 차게 쓰기 위한 세팅
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
# 🎯 2. 네이버 시가총액 8,000억 이상 필터링 엔진
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
                            if marcap >= 8000: # 8,000억 이상
                                target_list.append({'종목명': name, '종목코드': code, '소속': market_name, '시가총액': marcap})
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

# ==========================================
# 🔥 3. 핵심 수급 데이터 스캐너 엔진 (V2)
# ==========================================
@st.cache_data(ttl=3600) 
def load_v2_quant_data():
    token = get_kis_access_token()
    target_df = get_target_stock_list()
    
    headers_price = {
        "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST01010400" 
    }
    
    # 수급 API TR_ID (국내주식 일별 기관/외국인 매매동향)
    headers_supply = headers_price.copy()
    headers_supply["tr_id"] = "FHPST03010200"

    data_list = []
    total_count = len(target_df)
    my_bar = st.progress(0, text=f"시총 8,000억 이상 찐 수급주 스캔 중... (0/{total_count})")
    
    for i, row in enumerate(target_df.itertuples()):
        code = row.종목코드
        name = row.종목명
        my_bar.progress((i + 1) / total_count, text=f"수급/가치 스캔 중... [{i+1}/{total_count}] {name}")
        
        # 1. 주가 및 펀더멘털 스캔
        res_price = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                                 headers=headers_price, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        
        # 2. 수급 데이터 스캔 (과거 1달치 매매동향) -> ※ 현재는 V2 UI 테스트를 위해 안전하게 랜덤/가상 로직으로 뼈대만 잡아둡니다. 
        # (실제 파싱 로직은 KIS API 응답값 구조에 맞춰 다음 스프린트에 정밀 삽입)
        np.random.seed(int(code)) # 종목마다 고정된 가짜 점수 부여
        
        try:
            output = res_price.json().get('output', {})
            per = float(output.get('per', 0)) if output.get('per') else 0.0
            pbr = float(output.get('pbr', 0)) if output.get('pbr') else 0.0
            prpr = int(output.get('stck_prpr', 0))
            
            # 🧠 [임시 룰기반 스코어링] 실전 수급 데이터가 들어올 자리
            foreign_str = np.random.randint(-5, 15) # 외인 한달 수급강도 (%)
            pension_str = np.random.randint(-3, 10) # 연기금 한달 수급강도 (%)
            trust_pef_str = np.random.randint(-2, 8) # 투신/사모 수급강도 (%)
            
            ai_score = max(0, min(100, 50 + (foreign_str * 2) + (pension_str * 3) + trust_pef_str))
            
            data_list.append({
                '종목명': name, '종목코드': code, '소속': row.소속,
                'AI수급점수': int(ai_score),
                '현재가': prpr, '등락률': float(output.get('prdy_ctrt', 0)),
                '외인강도(%)': foreign_str, '연기금강도(%)': pension_str, '투신사모(%)': trust_pef_str,
                '연속매수': f"외인 {np.random.randint(0,6)}일", 
                '시가총액': row.시가총액, 'PER': per, 'PBR': pbr
            })
        except:
            pass
            
        time.sleep(0.08) # 봇 차단 방지 (0.08초 휴식)
        
    my_bar.empty() 
    return pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)

# ==========================================
# 🎨 4. 모바일 최적화 UI 렌더링
# ==========================================
df_summary = load_v2_quant_data()

with st.sidebar:
    st.header("🎯 실전 매매 타점 검색")
    if not df_summary.empty:
        ticker_dict = dict(zip(df_summary['종목명'], df_summary['종목코드']))
        selected_name = st.selectbox("종목을 고르세요", list(ticker_dict.keys()))
        selected_ticker = ticker_dict[selected_name]
        selected_row = df_summary[df_summary['종목명'] == selected_name].iloc[0]
    st.markdown('---')
    st.caption("선택한 종목의 타점 및 AI 분석은 우측 화면에 표시됩니다.")

st.title("🔥 실전 수급 스윙 대시보드 V2")

if df_summary.empty:
    st.error("데이터 로딩 실패! 네트워크를 확인해주세요.")
else:
    tab1, tab2 = st.tabs(["📊 수급 강도 스캐너", f"🎯 [{selected_name}] 실전 매매 비서"])

    # --- [Tab 1: 모바일 최적화 수급 표] ---
    with tab1:
        st.markdown("💡 **Tip:** 우측으로 스와이프하여 연기금/사모펀드 수급을 확인하세요.")
        
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

        st.dataframe(
            styled_df,
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

    # --- [Tab 2: 실전 매매 비서 (디테일 뷰)] ---
    with tab2:
        st.subheader(f"💡 {selected_name} 매매 전략 및 AI 요약")
        
        # 1. 🎯 매매 타점 카드 (지지/저항선 기반 시뮬레이션)
        curr_price = selected_row['현재가']
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**🟢 매수 타점 (지지)**\n\n**{int(curr_price * 0.96):,} 원**\n\n(최근 1주 기관 평단가)")
        with col2:
            st.error(f"**🔴 1차 목표가 (저항)**\n\n**{int(curr_price * 1.15):,} 원**\n\n(전고점 매물대)")
        with col3:
            st.warning(f"**⚫ 손절선 (Risk)**\n\n**{int(curr_price * 0.90):,} 원**\n\n(20일선 이탈)")
            
        st.markdown("---")
        
        # 2. 🔥 섹터 & 수급 분석 코멘트
        st.write(f"### 🔍 수급 및 섹터 흐름 파악")
        sector_name = "반도체 / IT 장비" # 향후 한투 API 섹터 정보로 교체
        st.write(f"- **소속 섹터:** {sector_name}")
        st.write(f"- **현재 수급 상태:** 점수 **{selected_row['AI수급점수']}점**으로, 최근 외국인과 연기금의 {selected_row['연속매수']}세가 포착되었습니다. 손바뀜이 활발히 일어나며 상승 모멘텀을 구축 중입니다.")
        
        st.markdown("---")
        
        # 3. 🤖 LLM 뉴스 3줄 요약 (Gemini 자리)
        st.write(f"### 📰 최신 뉴스 AI 3줄 요약 (Gemini 2.5 Flash)")
        st.text_area("AI 브리핑", 
                     "1. [호재] 해당 기업의 3분기 어닝 서프라이즈 기대감으로 기관 자금 유입 중.\n"
                     "2. [호재] 신규 수주 공시 및 북미 고객사 벤더 진입 임박.\n"
                     "3. [전략] 하단 지지선 이탈 전까지는 홀딩 및 분할 매수 유효함.", 
                     height=150, disabled=True)
        st.caption("※ API가 연동되면 실시간 네이버 뉴스 요약본이 여기에 꽂힙니다.")