import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta

# 0. 페이지 기본 설정 (앱을 모바일/PC 화면에 넓게 꽉 차게 쓰기)
st.set_page_config(layout="wide", page_title="나만의 퀀트 비서", page_icon="📈")

# 1. 데이터 세팅 (지금은 3개만 테스트)
TICKER_MAP = {
    "SK하이닉스": "000660",
    "삼성전자": "005930",
    "현대차": "005380"
}

today = datetime.today()
one_month_ago = today - timedelta(days=30)
start_date = one_month_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")

# 2. 데이터 불러오기 함수 (캐싱 적용)
@st.cache_data
def load_data(start, end, ticker):
    df = stock.get_market_ohlcv(start, end, ticker)
    return df
# ==========================================
# 2. 화면 왼쪽 서랍장 (사이드바 - 마스터 영역)
# ==========================================
with st.sidebar:
    st.header("📋 관심 종목 리스트")
    # 사용자가 선택한 종목 이름을 변수에 저장 (드롭다운 메뉴)
    selected_name = st.selectbox("분석할 종목을 선택하세요", list(TICKER_MAP.keys()))

    st.markdown("---")
    st.write("💡 (예정) 향후 코스피 시총 상위 200개 종목의 AI 스코어 랭킹이 여기에 리스트업 됩니다.")

# ==========================================
# 3. 화면 오른쪽 메인 (디테일 영역)
# ==========================================
selected_ticker = TICKER_MAP[selected_name]

st.title(f"📈 {selected_name} 상세 분석 대시보드")
# 향후 RAG 에이전트가 들어갈 VIP 존을 미리 만들어 둡니다.
st.info("🤖 **AI 비서 브리핑 (예정)**: 조만간 여기에 최신 뉴스와 수급을 분석한 3줄 요약이 들어옵니다.")

try:
    df = load_data(start_date, end_date, selected_ticker)

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📊 최근 주가 흐름 (데이터 표)")
        st.dataframe(df.tail(10), use_container_width=True)

    with col2:
        st.subheader("💡 종목 퀵 정보")
        # 데이터프레임의 가장 마지막 줄(최근 거래일) 데이터를 뽑아서 예쁘게 보여줍니다.
        st.metric(label="마지막 거래일 종가", value=f"{df.iloc[-1]['종가']:,}원")
        st.metric(label="거래량", value=f"{df.iloc[-1]['거래량']:,}주")
        
except Exception as e:
    st.error(f"데이터를 불러오는 중 에러가 발생했습니다: {e}")