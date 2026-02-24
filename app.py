import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go

# 페이지 기본 설정 (앱을 모바일/PC 화면에 넓게 꽉 차게 쓰기)
st.set_page_config(layout="wide", page_title="나만의 AI 퀀트 비서", page_icon="📈")

# 함수 적용
@st.cache_data
def get_top_200_tickers():
    today_str = datetime.today().strftime("%Y%m%d")
    df_cap = stock.get_market_cap(today_str)
    top_200 = df_cap.sort_values(by="시가총액", ascending=False).head(200)

    ticker_list = top_200.index.tolist()
    name_list = [stock.get_market_ticker_name(t) for t in ticker_list]

    return dict(zip(name_list, ticker_list))

@st.cache_data
def load_data(start, end, ticker):
    df = stock.get_market_ohlcv(start, end, ticker)
    return df

# 데이터 로딩 시작
with st.spinner("데이터 엔진 가동 중..."):
    TICKER_MAP = get_top_200_tickers()

today = datetime.today()
one_month_ago = today - timedelta(days=30)
start_date = one_month_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

# 2. 사이드바
with st.sidebar:
    st.header("🏆 시총 상위 200")
    # 사용자가 선택한 종목 이름을 변수에 저장 (드롭다운 메뉴)
    selected_name = st.selectbox("분석할 종목을 선택하세요", list(TICKER_MAP.keys()))

    st.markdown("---")
    st.write("※ 매일 아침 자동으로 시총 순위가 갱신됩니다.")

# 3. 메인화면
selected_ticker = TICKER_MAP[selected_name]
st.title(f"📈 {selected_name} 분석 리포트")

# 향후 RAG 에이전트가 들어갈 VIP 존을 미리 만들어 둡니다.
st.info("🤖 **AI 비서 브리핑 (예정)**: 뉴스와 수급을 분석한 결과가 곧 여기에 배달됩니다.")

try:
    df = load_data(start_date, end_date, selected_ticker)

    # 상단 요약 정보
    m1, m2, m3 = st.columns(3)
    curr_p = int(df.iloc[-1]['종가'])
    prev_p = int(df.iloc[-2]['종가'])
    change = curr_p - prev_p
    m1.metric("현재가", f"{curr_p:,}원", f"{change:,}원")
    m2.metric("거래량", f"{int(df.iloc[-1]['거래량']):,}주")
    m3.metric("변동률", f"{(change/prev_p)*100:.2f}%")

    ## chart 영역
    st.subheader("🕯️ 주가 캔들 차트")

    fig = go.Figure(data=[go.Candlestick(
        x=df.index,
        open=df['시가'],
        high=df['고가'],
        low=df['저가'],
        close=df['종가'],
        increasing_line_color='red',
        decreasing_line_color='blue'
    )])

    fig.update_layout(
        height=500,
        margin=dict(l=10, r=10, b=10, t=10),
        xaxis_rangeslider_visible=False,
        xaxis_type='category'
    )

    fig.update_xaxes(nticks=10)

    st.plotly_chart(fig, use_container_width=True)

    # --- 차트 끝 ---

    with st.expander("🔍 상세 데이터 보기"):
        st.dataframe(df.tail(10).sort_index(ascending=False), use_container_width=True)

except Exception as e:
    st.error(f"데이터를 불러오는 중 에러가 발생했습니다: {e}")