import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

st.set_page_config(layout="wide", page_title="나만의 퀀트 비서", page_icon="🤖")

@st.cache_data
def load_summary_data():
    today = datetime.today()
    
    for i in range(5):
        target_date = (today - timedelta(days=i)).strftime("%Y%m%d")
        df_cap = stock.get_market_cap(target_date, market="KOSPI")
        
        if not df_cap.empty:
            df_ohlcv = stock.get_market_ohlcv(target_date, market="KOSPI")
            df_fundamental = stock.get_market_fundamental(target_date, market="KOSPI")
            
            df = pd.concat([df_cap, df_ohlcv['등락률'], df_fundamental[['PER', 'PBR']]], axis=1)
            top_200 = df.sort_values(by='시가총액', ascending=False).head(200)
            
            # [마법의 1줄] 시가총액을 1억(100,000,000)으로 나누어 '억' 단위로 변환합니다.
            top_200['시가총액'] = top_200['시가총액'] / 100000000
            
            top_200['종목명'] = [stock.get_market_ticker_name(t) for t in top_200.index]
            top_200 = top_200.reset_index().rename(columns={'티커': '종목코드'})
            
            np.random.seed(42) 
            top_200['AI_Score'] = np.random.randint(60, 100, size=200)
            
            display_cols = ['종목명', '종목코드', 'AI_Score', '종가', '등락률', 'PER', 'PBR', '시가총액']
            return top_200[display_cols]
            
    return pd.DataFrame()

with st.spinner("KRX에서 상위 200개 종목의 펀더멘털을 스캔 중입니다..."):
    df_summary = load_summary_data()

st.title("🤖 퀀트 비서 서머리 대시보드")
tab1, tab2 = st.tabs(["🏆 스코어링 랭킹 보드", "🔍 개별 종목 상세 (차트/뉴스)"])

with tab1:
    st.markdown("💡 **Tip:** 열 이름을 클릭하면 내림차순/오름차순으로 정렬됩니다.")
    
    def color_fluctuation(val):
        if val > 0:
            return 'color: #FF3333; font-weight: bold;'
        elif val < 0:
            return 'color: #0066FF; font-weight: bold;'
        return 'color: gray;'

    def format_fluctuation(val):
        if val > 0:
            return f"🔺 +{val:.2f}%"
        elif val < 0:
            return f"🔻 {val:.2f}%"
        return f"➖ {val:.2f}%"

    # 시가총액 포맷을 '{:,.0f}' 로 유지하면 억 단위 변환된 숫자에 예쁘게 콤마가 찍힙니다.
    styled_df = df_summary.style.map(color_fluctuation, subset=['등락률']) \
                                .format({
                                    "종가": "{:,.0f}",
                                    "시가총액": "{:,.0f}", 
                                    "등락률": format_fluctuation,
                                    "PER": "{:.1f}",
                                    "PBR": "{:.2f}"
                                })

    st.dataframe(
        styled_df,
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
            "종가": st.column_config.Column("현재가 (원)"),
            "등락률": st.column_config.Column("등락률 (%)"),
            "PER": st.column_config.Column("PER (배)"),
            "PBR": st.column_config.Column("PBR (배)"),
            # 단위가 '억 원'임을 명시해 줍니다.
            "시가총액": st.column_config.Column("시가총액 (억 원)") 
        },
        hide_index=True,
        use_container_width=True,
        height=600 
    )

with tab2:
    st.info("여기에 선택한 종목의 'AI 요약 브리핑', 'PER/PBR 밴드 차트', 그리고 '보조 수급 차트'가 들어갈 예정입니다.")