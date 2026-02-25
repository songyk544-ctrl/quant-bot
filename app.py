import streamlit as st
from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 페이지 기본 설정
st.set_page_config(layout="wide", page_title="나만의 AI 퀀트 비서", page_icon="📈")

# 함수 적용
@st.cache_data
def load_summary_data():
    today = datetime.today()

    # 주말/공휴일을 대비해 최근 5일 중 데이터가 있는 가장 마지막 거래일을 찾습니다.
    for i in range(5):
        target_date = (today - timedelta(days=i)).strftime("%Y%m%d")
        df_cap = stock.get_market_cap(target_date, market="KOSPI")

        if not df_cap.empty:
            df_ohlcv = stock.get_market_ohlcv(target_date, market="KOSPI")
            df_fundamental = stock.get_market_fundamental(target_date, market="KOSPI")

            df = pd.concat([df_cap, df_ohlcv["등락률"], df_fundamental[['PER', 'PBR']]], axis=1)

            top_200 = df.sort_values(by="시가총액", ascending=False).head(200)

            top_200['종목명'] = [stock.get_market_ticker_name(t) for t in top_200.index]
            top_200 = top_200.reset_index().rename(columns={'티커':'종목코드'})

            # [임시 데이터] 추후 AI/XGBoost가 계산할 퀀트점수 뼈대
            np.random.seed(42)
            top_200['AI_Score'] = np.random.randint(60,100,size=200)

            # 화면에 보여줄 컬럼만
            display_cols = ["종목명", "종목코드", "AI_Score", "종가", "등락률", "PER", "PBR", "시가총액"]
            return top_200[display_cols]
    
    return pd.DataFrame()


# 데이터 로딩
with st.spinner("KRX에서 상위 200개 종목의 펀더멘털을 스캔 중입니다..."):
    df_summary = load_summary_data()

st.title("🤖 퀀트 비서 서머리 대시보드")

# 화면을 두 개의 탭으로 깔끔하게 나눕니다.

tab1, tab2 = st.tabs(["🏆 스코어링 랭킹 보드", "🔍 개별 종목 상세 (차트/뉴스)"])

with tab1:
    st.markdown("💡 **Tip:** 열 이름(AI_Score, 등락률 등)을 클릭하면 해당 기준으로 정렬됩니다.")

    st.dataframe(
        df_summary,
        column_config={
            "종목명": st.column_config.TextColumn("종목명", width="medium"),
            "종목코드": st.column_config.TextColumn("코드"),
            "AI_Score": st.column_config.ProgressColumn(
                "퀀트 점수", 
                help="향후 알고리즘이 계산할 종합 매력도",
                format="%d 점",
                min_value=0,
                max_value=100,
            ),
            "종가": st.column_config.NumberColumn("현재가", format="%d 원"),
            "등락률": st.column_config.NumberColumn("등락률", format="%.2f %%"),
            "PER": st.column_config.NumberColumn("PER", format="%.1f 배"),
            "PBR": st.column_config.NumberColumn("PBR", format="%.2f 배"),
            "시가총액": st.column_config.NumberColumn("시총", format="%d")
        },
        hide_index=True,
        use_container_width=True,
        height=600 # 스크롤 하기 편하게 높이 지정
    )
with tab2:
    st.info("여기에 선택한 종목의 'AI 요약 브리핑', 'PER/PBR 밴드 차트', 그리고 '보조 수급 차트'가 들어갈 예정입니다.")
