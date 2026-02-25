import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import plotly.graph_objects as go

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
            top_200['시가총액'] = top_200['시가총액'] / 100_000_000
            
            top_200['종목명'] = [stock.get_market_ticker_name(t) for t in top_200.index]
            top_200 = top_200.reset_index().rename(columns={'티커': '종목코드'})
            
            # ==========================================
            # 🧠 [NEW] AI 퀀트 스코어링 엔진 (가치투자 기반)
            # ==========================================
            # 1. 적자 기업(PER 0 이하) 필터링
            valid_per = top_200['PER'] > 0
            valid_pbr = top_200['PBR'] > 0

            # 2. 백분위 랭킹 계산 (rank(pct=True)는 0.0 ~ 1.0 사이의 비율을 반환)
            # PER/PBR은 낮을수록 좋으므로, (1 - 비율)을 하여 점수를 뒤집어 줍니다.
            top_200.loc[valid_per, 'PER_Score'] = (1.0 - top_200.loc[valid_per, 'PER'].rank(pct=True)) * 100
            top_200.loc[valid_pbr, 'PBR_Score'] = (1.0 - top_200.loc[valid_pbr, 'PBR'].rank(pct=True)) * 100

            # 3. 결측치나 적자 기업은 기본 패널티 점수(20점) 부여
            top_200['PER_Score'] = top_200['PER_Score'].fillna(20)
            top_200['PBR_Score'] = top_200['PBR_Score'].fillna(20)

            # 4. 최종 AI Score 산출 (두 지표의 평균)
            top_200['AI_Score'] = ((top_200['PER_Score'] + top_200['PBR_Score']) / 2).astype(int)

            display_cols = ['종목명', '종목코드', 'AI_Score', '종가', '등락률', 'PER', 'PBR', '시가총액']
            return top_200[display_cols]
            
    return pd.DataFrame()

@st.cache_data
def load_detail_data(ticker):
    today = datetime.today()
    start_date = (today - timedelta(days=365 * 3)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")

    df_price = stock.get_market_ohlcv(start_date, end_date, ticker)
    df_fund = stock.get_market_fundamental(start_date, end_date, ticker)

    df = pd.concat([df_price['종가'], df_fund[['BPS', 'PBR']]], axis=1).dropna()
    return df

with st.spinner("KRX 데이터 동기화 중..."):
    df_summary = load_summary_data()

with st.sidebar:
    st.header("🔍 종목 상세 검색")
    # 200개 종목명과 코드를 딕셔너리로 묶어 선택하기 쉽게 만듭니다.
    ticker_dict = dict(zip(df_summary['종목명'], df_summary['종목코드']))
    selected_name = st.selectbox("분석할 종목을 고르세요", list(ticker_dict.keys()))
    selected_ticker = ticker_dict[selected_name]
    st.markdown('---')
    st.caption("※ 여기서 선택한 종목은 'Tab 2'에 상세 분석됩니다.")

st.title("🤖 퀀트 비서 서머리 대시보드")
tab1, tab2 = st.tabs(["🏆 스코어링 랭킹 보드", f"📊 [{selected_name}] 상세 분석"])

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
    st.subheader(f"📈 {selected_name} PBR 밴드 (과거 3년 가치평가)")

    try:
        df_detail = load_detail_data(selected_ticker)

        min_pbr = df_detail['PBR'].min()
        max_pbr = df_detail['PBR'].max()
        pbr_levels = np.linspace(min_pbr, max_pbr, 5)

        fig = go.Figure()

        # 1. 주가 그리기 (흰색 굵은 선)
        fig.add_trace(go.Scatter(x=df_detail.index, y=df_detail['종가'], name='실제 주가', line=dict(color='white', width=2)))

        colors = ['#3498DB', '#2ECC71', '#F1C40F', '#E67E22', '#E74C3C'] # 파랑(저평가) -> 빨강(고평가)

        for i, p_level in enumerate(pbr_levels):
            band_price = df_detail['BPS'] * p_level
            fig.add_trace(go.Scatter(
                x=df_detail.index,
                y=band_price,
                name=f'PBR {p_level:.2f}x',
                line=dict(color=colors[i], width=1, dash='dot')
            ))
            
        fig.update_layout(
            height=600,
            template="plotly_dark",
            margin=dict(l=10, r=10, b=10, t=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified" # 마우스를 올리면 모든 선의 값을 한 번에 보여줌
        )

        st.plotly_chart(fig, use_container_width=True)
        
        st.info("💡 **해석 방법:** 흰색 실선(주가)이 파란색 점선(하단 밴드)에 가까울수록 역사적 저평가 구간이며, 빨간색 점선(상단 밴드)에 닿을수록 고평가(과열) 구간입니다.")
        
    except Exception as e:
        st.error(f"차트 데이터를 불러오는 중 에러가 발생했습니다: {e}")