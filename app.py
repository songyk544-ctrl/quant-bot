import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 페이지 기본 설정 (앱을 모바일/PC 화면에 넓게 꽉 차게 쓰기)
st.set_page_config(layout="wide", page_title="나만의 AI 퀀트 비서", page_icon="📈")

# 함수 적용
@st.cache_data
def get_top_200_tickers():
    today_str = datetime.today().strftime("%Y%m%d")
    df_cap = stock.get_market_cap(today_str)
    top_200 = df_cap.sort_values(by='시가총액', ascending=False).head(200)
    return dict(zip([stock.get_market_ticker_name(t) for t in top_200.index], top_200.index))

@st.cache_data
def load_full_data(start, end, ticker):
    df_price = stock.get_market_ohlcv(start, end, ticker)
    df_investor = stock.get_market_net_purchases_of_equities_by_ticker(start, end, ticker)
    return pd.concat([df_price, df_investor], axis=1)

# 데이터 로딩 시작
with st.spinner("데이터 엔진 가동 중..."):
    TICKER_MAP = get_top_200_tickers()

with st.sidebar:
    selected_name = st.selectbox("분석할 종목 선택", list(TICKER_MAP.keys()))
    st.markdown("---")
    st.write("※ 매일 아침 자동으로 시총 순위가 갱신됩니다.")

today = datetime.today()
start_date = (today - timedelta(days=250)).strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")
selected_ticker = TICKER_MAP[selected_name]


# 3. 메인화면
selected_ticker = TICKER_MAP[selected_name]
st.title(f"📈 {selected_name} 분석 리포트")

# 향후 RAG 에이전트가 들어갈 VIP 존을 미리 만들어 둡니다.
st.info("🤖 **AI 비서 브리핑 (예정)**: 뉴스와 수급을 분석한 결과가 곧 여기에 배달됩니다.")

try:
    df = load_full_data(start_date, end_date, selected_ticker)

    # 상단 요약 정보
    m1, m2, m3 = st.columns(3)
    curr_p = int(df.iloc[-1]['종가'])
    prev_p = int(df.iloc[-2]['종가'])
    change = curr_p - prev_p
    m1.metric("현재가", f"{curr_p:,}원", f"{change:,}원")
    m2.metric("거래량", f"{int(df.iloc[-1]['거래량']):,}주")
    m3.metric("변동률", f"{(change/prev_p)*100:.2f}%")

    # --- 차트 시작 ---
    # 1. 이동평균선(MA) 계산
    df['MA5'] = df['종가'].rolling(window=5).mean()
    df['MA20'] = df['종가'].rolling(window=20).mean()
    df['MA60'] = df['종가'].rolling(window=60).mean()
    df['MA120'] = df['종가'].rolling(window=120).mean()

    fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, row_heights=[0.6, 0.2, 0.2],
                        subplot_titles=("주가 및 이동평균선", "거래량", "투자자별 수급"))
    
    # 캔들 차트 추가
    fig.add_trace(go.Candlestick(
        x=df.index.astype(str), open=df['시가'], high=df['고가'], 
        low=df['저가'], close=df['종가'], name="주가"
    ), row=1, col=1)

    # 이동 평균선 추가
    for ma, color in zip(['MA5', 'MA20', 'MA60', 'MA120'], ['white', 'gold', 'purple', 'green']):
        fig.add_trace(go.Scatter(x=df.index.astype(str), y=df[ma], name=ma,
                                 line=dict(width=1, color=color)), row=1, col=1)
    
    # 거래량
    fig.add_trace(go.Bar(
        x=df.index.astype(str), y=df['거래량'], name="거래량",
        marker_color="lightgray", opacity=0.7
    ), row=2, col=1)

    # 수급 보조 지표
    fig.add_trace(go.Bar(x=df.index.astype(str), y=df['외국인'], name="외국인", marker_color='red'), row=3, col=1)
    fig.add_trace(go.Bar(x=df.index.astype(str), y=df['기관합계'], name="기관", marker_color='blue'), row=3, col=1)
    fig.add_trace(go.Bar(x=df.index.astype(str), y=df['연기금'], name="연기금", marker_color='orange'), row=3, col=1)

    # 레이아웃 업데이트
    fig.update_layout(
        height=900,
        margin=dict(l=10, r=10, b=10, t=10),
        xaxis_rangeslider_visible=False,
        xaxis_type='category',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig.update_xaxes(nticks=12, row=3, col=1)

    st.plotly_chart(fig, use_container_width=True)


    with st.expander("🔍 상세 데이터 보기"):
        st.dataframe(df.tail(10).sort_index(ascending=False), use_container_width=True)

except Exception as e:
    st.error(f"데이터를 불러오는 중 에러가 발생했습니다: {e}")