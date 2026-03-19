import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objects as go
import requests
import json
import time
import FinanceDataReader as fdr
from bs4 import BeautifulSoup

st.set_page_config(layout="wide", page_title="나만의 퀀트 비서", page_icon="🤖")

URL_BASE = "https://openapi.koreainvestment.com:9443"

# 1. 🛡️ 한투 토큰 발급 (24시간 캐싱: 하루 1번만 발급받아 카톡 경고 방지)
@st.cache_data(ttl=86400)
def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"]
    }
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

@st.cache_data(ttl=86400) # 하루에 한 번만 명단을 긁어와서 서버를 쉬게 합니다.
def get_target_stock_list():
    target_list = []
    # 0: KOSPI, 1: KOSDAQ
    for sosok in [0, 1]:
        # 1~6페이지 (페이지당 50개, 약 시총 상위 300위까지 넉넉히 스캔)
        for page in range(1, 7):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            soup = BeautifulSoup(res.text, 'html.parser')
            
            table = soup.select('table.type_2 tbody tr')
            for tr in table:
                tds = tr.select('td')
                if len(tds) > 1: # 빈 선(구분선) 제외
                    name_tag = tr.select_one('a.tltle')
                    if name_tag:
                        name = name_tag.text
                        code = name_tag['href'].split('code=')[-1]
                        
                        # 시가총액은 7번째 열(인덱스 6)에 있습니다. (단위: 억원)
                        marcap_str = tds[6].text.replace(',', '')
                        if marcap_str.isdigit():
                            marcap = int(marcap_str)
                            # 💡 핵심 기획: 시가총액 8,000억 이상만 통과!
                            if marcap >= 8000:
                                target_list.append({'Name': name, 'Code': code, 'Marcap': marcap})
                                
    # 코스피, 코스닥 합쳐서 시가총액 내림차순으로 정렬
    return pd.DataFrame(target_list).sort_values('Marcap', ascending=False)

# ==========================================
# 📊 서머리 데이터 엔진 (업그레이드 버전)
# ==========================================
@st.cache_data(ttl=3600) 
def load_summary_data():
    token = get_kis_access_token()
    
    # [1단계] 방금 만든 네이버 크롤러로 8천억 이상 명단을 가져옵니다. (FDR 대체)
    target_df = get_target_stock_list()
    
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"],
        "tr_id": "FHKST01010400" 
    }

    data_list = []
    total_count = len(target_df)
    
    my_bar = st.progress(0, text=f"시총 8,000억 이상 {total_count}개 종목 스캔 준비 중...")
    
    # [2단계] 명단을 들고 한투 VIP 서버에서 수급/가치 데이터를 빼옵니다.
    for i, row in enumerate(target_df.itertuples()):
        code = row.Code
        name = row.Name
        marcap = row.Marcap # 이미 '억' 단위
        
        my_bar.progress((i + 1) / total_count, text=f"수급/가치 스캔 중... [{i+1}/{total_count}] {name}")
        
        res = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                           headers=headers, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code})
        
        if res.status_code == 200:
            output = res.json().get('output', {})
            try:
                per = float(output.get('per', 0)) if output.get('per') else 0.0
                pbr = float(output.get('pbr', 0)) if output.get('pbr') else 0.0
                
                data_list.append({
                    '종목명': name, '종목코드': code, 
                    '종가': int(output.get('stck_prpr', 0)),
                    '등락률': float(output.get('prdy_ctrt', 0)),
                    'PER': per, 'PBR': pbr, '시가총액': int(marcap)
                })
            except:
                pass
                
        time.sleep(0.08)
        
    my_bar.empty() 
    df = pd.DataFrame(data_list)
    
    if not df.empty:
        valid_per = df['PER'] > 0
        valid_pbr = df['PBR'] > 0
        df.loc[valid_per, 'PER_Score'] = (1.0 - df.loc[valid_per, 'PER'].rank(pct=True)) * 100
        df.loc[valid_pbr, 'PBR_Score'] = (1.0 - df.loc[valid_pbr, 'PBR'].rank(pct=True)) * 100
        df['PER_Score'] = df['PER_Score'].fillna(20)
        df['PBR_Score'] = df['PBR_Score'].fillna(20)
        df['AI_Score'] = ((df['PER_Score'] + df['PBR_Score']) / 2).astype(int)
        return df[['종목명', '종목코드', 'AI_Score', '종가', '등락률', 'PER', 'PBR', '시가총액']]
    return pd.DataFrame()

# 3. 📈 디테일 데이터 엔진 (한투 일봉 차트 API)
@st.cache_data
def load_detail_data(ticker):
    token = get_kis_access_token()
    
    # 현재 BPS 가져오기 (PBR 계산용)
    headers_price = {
        "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST01010400"
    }
    res_price = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-price", 
                             headers=headers_price, params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker})
    bps = float(res_price.json().get('output', {}).get('bps', 1))

    # 과거 3년치 일봉 데이터 가져오기
    start_date = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y%m%d")
    end_date = datetime.today().strftime("%Y%m%d")
    
    headers_hist = {
        "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
        "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST03010100"
    }
    params_hist = {
        "FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": start_date, "FID_INPUT_DATE_2": end_date,
        "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0" 
    }
    
    res_hist = requests.get(f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", 
                            headers=headers_hist, params=params_hist)
    
    hist_data = res_hist.json().get('output2', [])
    df_hist = pd.DataFrame(hist_data)
    
    if not df_hist.empty:
        df_hist['Date'] = pd.to_datetime(df_hist['stck_bsop_date'])
        df_hist['종가'] = df_hist['stck_clpr'].astype(float)
        df_hist.set_index('Date', inplace=True)
        df_hist.sort_index(inplace=True)
        df_hist['BPS'] = bps
        df_hist['PBR'] = df_hist['종가'] / bps 
        
    return df_hist

# ==========================================
# 🎨 UI 렌더링 영역 (기존 코드 완벽 일치)
# ==========================================
df_summary = load_summary_data()

with st.sidebar:
    st.header("🔍 종목 상세 검색")
    if not df_summary.empty:
        ticker_dict = dict(zip(df_summary['종목명'], df_summary['종목코드']))
        selected_name = st.selectbox("분석할 종목을 고르세요", list(ticker_dict.keys()))
        selected_ticker = ticker_dict[selected_name]
    st.markdown('---')
    st.caption("※ 여기서 선택한 종목은 'Tab 2'에 상세 분석됩니다.")

st.title("🤖 퀀트 비서 서머리 대시보드")
if df_summary.empty:
    st.error("데이터 로딩 실패! 터미널의 에러 로그를 확인해주세요.")
else:
    tab1, tab2 = st.tabs(["🏆 스코어링 랭킹 보드", f"📊 [{selected_name}] 상세 분석"])

    with tab1:
        st.markdown("💡 **Tip:** 열 이름을 클릭하면 내림차순/오름차순으로 정렬됩니다.")
        
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        def format_fluctuation(val):
            if val > 0: return f"🔺 +{val:.2f}%"
            elif val < 0: return f"🔻 {val:.2f}%"
            return f"➖ {val:.2f}%"

        styled_df = df_summary.style.map(color_fluctuation, subset=['등락률']) \
                                    .format({"종가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": format_fluctuation, "PER": "{:.1f}", "PBR": "{:.2f}"})

        st.dataframe(
            styled_df,
            column_config={
                "종목명": st.column_config.TextColumn("종목명", width="medium"),
                "종목코드": st.column_config.TextColumn("코드"),
                "AI_Score": st.column_config.ProgressColumn("퀀트 점수", format="%d 점", min_value=0, max_value=100),
                "종가": st.column_config.Column("현재가 (원)"),
                "등락률": st.column_config.Column("등락률 (%)"),
                "PER": st.column_config.Column("PER (배)"),
                "PBR": st.column_config.Column("PBR (배)"),
                "시가총액": st.column_config.Column("시가총액 (억 원)") 
            },
            hide_index=True, use_container_width=True, height=600 
        )

    with tab2:
        st.info("여기에 선택한 종목의 'AI 요약 브리핑', 'PER/PBR 밴드 차트', 그리고 '보조 수급 차트'가 들어갈 예정입니다.")
        st.subheader(f"📈 {selected_name} PBR 밴드 (과거 3년 가치평가)")

        try:
            df_detail = load_detail_data(selected_ticker)
            
            if not df_detail.empty:
                min_pbr = df_detail['PBR'].min()
                max_pbr = df_detail['PBR'].max()
                pbr_levels = np.linspace(min_pbr, max_pbr, 5)

                fig = go.Figure()
                fig.add_trace(go.Scatter(x=df_detail.index, y=df_detail['종가'], name='실제 주가', line=dict(color='white', width=2)))

                colors = ['#3498DB', '#2ECC71', '#F1C40F', '#E67E22', '#E74C3C'] 

                for i, p_level in enumerate(pbr_levels):
                    band_price = df_detail['BPS'] * p_level
                    fig.add_trace(go.Scatter(x=df_detail.index, y=band_price, name=f'PBR {p_level:.2f}x', line=dict(color=colors[i], width=1, dash='dot')))
                    
                fig.update_layout(
                    height=600, template="plotly_dark", margin=dict(l=10, r=10, b=10, t=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), hovermode="x unified"
                )
                st.plotly_chart(fig, use_container_width=True)
                st.info("💡 **해석 방법:** 흰색 실선(주가)이 파란색 점선(하단 밴드)에 가까울수록 역사적 저평가 구간이며, 빨간색 점선(상단 밴드)에 닿을수록 고평가(과열) 구간입니다.")
            else:
                st.warning("과거 데이터를 가져오지 못했습니다.")
                
        except Exception as e:
            st.error(f"차트 데이터를 불러오는 중 에러가 발생했습니다: {e}")