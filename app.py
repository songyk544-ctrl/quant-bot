import streamlit as st
from pykrx import stock
import pandas as pd
from datetime import datetime, timedelta

st.title("📈 나만의 주식 퀀트 비서")
st.write("SK하이닉스(000660) 최근 주가 데이터 통신 테스트")

# 1. 날짜 설정 (오늘 기준으로 한 달 전부터 오늘까지)
today = datetime.today()
one_month_ago = today - timedelta(days=30)

start_date = one_month_ago.strftime("%Y%m%d")
end_date = today.strftime("%Y%m%d")
ticker = "000660"

# 2. 데이터 불러오기 함수 (캐싱 적용)
@st.cache_data
def load_data(start, end, ticker):
    df = stock.get_market_ohlcv(start, end, ticker)
    return df

try:
    df = load_data(start_date, end_date, ticker)

    st.dataframe(df.tail(10), use_container_width=True)
    st.success("성공! 한국거래소 서버와 무사히 통신을 완료했습니다. 🚀")

except Exception as e:
    st.error(f"데이터를 불러오는 중 에러가 발생했습니다: {e}")