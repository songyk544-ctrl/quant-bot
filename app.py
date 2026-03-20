import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V4", page_icon="⚡")

st.title("⚡ 실전 수급 스윙 대시보드 V4 (초고속 아키텍처)")

st.caption("🌐 글로벌 매크로 동향: 전일 뉴욕 증시 및 주요 환율 데이터 연동 대기 중")

@st.cache_data(ttl=600) # CSV 파일 로딩은 매우 빠르므로 10분마다 새로고침해도 무방
def load_data():
    if os.path.exists("data.csv"):
        return pd.read_csv("data.csv")
    else:
        return pd.DataFrame()

df_summary = load_data()

if df_summary.empty:
    st.warning("⏳ 데이터 수집 봇이 아직 data.csv를 생성하지 않았습니다. GitHub Actions를 확인해주세요.")
else:
    st.success("✅ GitHub 파이프라인 데이터 로딩 완료 (0.1초)")
    
    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2 = st.tabs(["📊 리얼 수급 스캐너", f"🎯 [{st.session_state.selected_stock}] 매매 비서"])

    with tab1:
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        # 포맷팅 적용 (문자열이 아닌 숫자형 포맷팅)
        styled_df = df_summary.style.map(color_score, subset=['Score']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:+.2f}%", "연기금강도(%)": "{:+.2f}%", 
                                             "투신강도(%)": "{:+.2f}%", "사모강도(%)": "{:+.2f}%",
                                             "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={
                "종목명": st.column_config.TextColumn("종목명", width="small"),
                "소속": st.column_config.TextColumn("시장"),
                "Score": st.column_config.NumberColumn("🏆 Score"),
                "현재가": st.column_config.Column("현재가"),
                "등락률": st.column_config.Column("등락"),
                "외인강도(%)": st.column_config.Column("외인(1달)"),
                "연기금강도(%)": st.column_config.Column("연기금(1달)"),
                "투신강도(%)": st.column_config.Column("투신(1달)"),
                "사모강도(%)": st.column_config.Column("사모(1달)"),
                "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"),
                "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"),
                "ROE": st.column_config.Column("ROE")
            },
            hide_index=True, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} 매매 전략 및 AI 요약")
        st.write(f"- **현재가:** {selected_row['현재가']:,}원 ({selected_row['등락률']}%) / **PER:** {selected_row['PER']} / **ROE:** {selected_row['ROE']}%")
        st.write(f"- **현재 수급 상태:** 종합점수 **{selected_row['Score']}점** / 최근 연속 수급: 외인 **{selected_row['외인연속']}일**, 연기금 **{selected_row['연기금연속']}일** 포착")
        st.write(f"- **주요 세력별 강도 (1달):** 외인 {selected_row['외인강도(%)']:.2f}% / 연기금 {selected_row['연기금강도(%)']:.2f}% / 투신 {selected_row['투신강도(%)']:.2f}% / 사모 {selected_row['사모강도(%)']:.2f}%")