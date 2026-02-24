import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta

# 페이지 기본 설정 (앱을 모바일/PC 화면에 넓게 꽉 차게 쓰기)
st.set_page_config(layout="wide", page_title="나만의 AI 퀀트 비서", page_icon="📈")

# 시가총액 상위 200개 종목 리스트 불러오는 함수
@st.cache_data
def get_top_200_tickers():
    today_str = datetime.today().strftime("%Y%m%d")
    df_cap = stock.get_market_cap(today_str)
    top_200 = df_cap.sort_values(by="시가총액", ascending=False).head(200)

    ticker_list = top_200.index.tolist()
    name_list = [stock.get_market_ticker_name(t) for t in ticker_list]

    return dict(zip(name_list, ticker_list))

# 데이터 불러오기 함수 (캐싱 적용)
@st.cache_data
def load_data(start, end, ticker):
    df = stock.get_market_ohlcv(start, end, ticker)
    return df

# 데이터 로딩 시작
with st.spinner("거래소에서 시총 상위 200개 종목을 불러오는 중..."):
    TICKER_MAP = get_top_200_tickers()

today = datetime.today()
one_month_ago = today - timedelta(days=30)
start_date = one_month_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

# ==========================================
# 2. 화면 왼쪽 서랍장 (사이드바 - 마스터 영역)
# ==========================================
with st.sidebar:
    st.header("🏆 시총 상위 200")
    # 사용자가 선택한 종목 이름을 변수에 저장 (드롭다운 메뉴)
    selected_name = st.selectbox("분석할 종목을 선택하세요", list(TICKER_MAP.keys()))

    st.markdown("---")
    st.write("※ 매일 아침 자동으로 시총 순위가 갱신됩니다.")

# ==========================================
# 3. 화면 오른쪽 메인 (디테일 영역)
# ==========================================
selected_ticker = TICKER_MAP[selected_name]

st.title(f"📈 {selected_name} 상세 분석 대시보드")
# 향후 RAG 에이전트가 들어갈 VIP 존을 미리 만들어 둡니다.
st.info("🤖 **AI 비서 브리핑 (예정)**: 뉴스와 수급을 분석한 결과가 곧 여기에 배달됩니다.")

try:
    df = load_data(start_date, end_date, selected_ticker)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📊 최근 주가 흐름")
        st.dataframe(df.tail(10), use_container_width=True)

    with col2:
        st.subheader("💡 종목 퀵 정보")
        # 1. 등락 계산 (오늘 종가 - 어제 종가)
        current_price = int(df.iloc[-1]['종가'])
        yesterday_price = int(df.iloc[-2]['종가'])
        change = current_price - yesterday_price

        # 2. 전일 대비 수익률 계산
        change_rate = (change / yesterday_price) * 100

        # 3. metric 표시
        st.metric(
            label="현재 종가",
            value=f"{current_price:,}원",
            delta=f"{change:,}원 ({change_rate:.2f}%)"
        )

        # 거래량도 동일하게 전일 대비 변화량 표시
        current_vol = int(df.iloc[-1]["거래량"])
        yesterday_vol = int(df.iloc[-2]["거래량"])
        vol_change = current_vol - yesterday_vol

        st.metric(
            label="오늘 거래량",
            value=f"{current_vol:,}주",
            delta=f"{vol_change:,}주",
            delta_color="normal"
        )


except Exception as e:
    st.error(f"데이터를 불러오는 중 에러가 발생했습니다: {e}")