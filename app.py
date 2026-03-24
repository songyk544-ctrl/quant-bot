import streamlit as st
import pandas as pd
import altair as alt # 🔥 Altair 고급 차트 라이브러리 추가
import os

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V5.1", page_icon="⚡")
st.title("⚡ 실전 수급 스윙 대시보드 V5.1 (UI 보완)")
st.caption("🌐 글로벌 매크로 동향: 전일 뉴욕 증시 및 주요 환율 데이터 연동 대기 중")

@st.cache_data(ttl=600) 
def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 데이터 수집 봇이 아직 파일을 생성하지 않았습니다. GitHub Actions를 확인해주세요.")
else:
    st.success("✅ GitHub 파이프라인 데이터 로딩 완료 (0.1초)")
    
    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2 = st.tabs(["📊 리얼 수급 랭킹", f"📈 [{st.session_state.selected_stock}] 차트 & AI 브리핑"])

    with tab1:
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        styled_df = df_summary.style.map(color_score, subset=['AI수급점수']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:+.2f}%", "연기금강도(%)": "{:+.2f}%", 
                                             "투신강도(%)": "{:+.2f}%", "사모강도(%)": "{:+.2f}%",
                                             "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%",
                                             "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={
                "종목명": st.column_config.TextColumn("종목명", width="small"),
                "AI수급점수": st.column_config.NumberColumn("🏆 AI 점수"),
                "현재가": st.column_config.Column("현재가"),
                "이격도(%)": st.column_config.Column("20일이격"),
                "손바뀜(%)": st.column_config.Column("5일손바뀜"),
                "외인강도(%)": st.column_config.Column("외인(1달)"),
                "연기금강도(%)": st.column_config.Column("연기금(1달)"),
                "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"),
                "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"),
                "소속": None, "PER": None, "투신강도(%)": None, "사모강도(%)": None 
            },
            hide_index=True, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} AI 퀀트 브리핑 (LLM 연동 대기중)")
        st.write(f"- **현재가:** {selected_row['현재가']:,}원 ({selected_row['등락률']}%)")
        st.write(f"- **수급 종합점수:** **{selected_row['AI수급점수']}점** (외인 {selected_row['외인연속']}일 / 연기금 {selected_row['연기금연속']}일 연속 매수)")
        tech_status = "🟢 완벽한 눌림목 타점" if 98 <= selected_row['이격도(%)'] <= 105 else ("🔴 단기 과열 주의" if selected_row['이격도(%)'] > 115 else "⚫ 추세 관망")
        st.write(f"- **기술적 분석:** 20일 이평선 이격도 {selected_row['이격도(%)']}% ({tech_status}) / 최근 5일 손바뀜 {selected_row['손바뀜(%)']}%")
        
        st.markdown("---")
        
        if not df_history.empty:
            # 선택한 종목 데이터만 필터링
            target_hist = df_history[df_history['종목명'] == st.session_state.selected_stock].copy()
            
            if not target_hist.empty:
                # 일자를 오름차순(과거->현재)으로 정렬하고 날짜 형식으로 변환
                target_hist['일자'] = pd.to_datetime(target_hist['일자'].astype(str))
                target_hist = target_hist.sort_values('일자')
                
                col1, col2 = st.columns(2)
                
                # 가중치별 색상 정의 (색맹 친화적 팔레트)
                color_scale = alt.Scale(
                    domain=['외인', '연기금', '투신', '사모'],
                    range=['#FF4B4B', '#1C83E1', '#F1C40F', '#83C9FF'] # 빨, 파, 노, 연두
                )

                with col1:
                    st.markdown("##### 📈 일봉 차트 (최근 20일 종가)")
                    # 🔥 해결 1: Y축 스케일 자동 조정 (Scale(zero=False))
                    # X축을 Ordinal(:O)로 설정하여 주말 공백 제거
                    chart_close = alt.Chart(target_hist).mark_line(color='#1C83E1').encode(
                        x=alt.X('일자:O', axis=alt.Axis(format='%Y-%m-%d', title=None)), # 일자 포맷 및 공백 제거
                        y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None) # Y축 0부터 시작 금지!
                    ).properties(height=280)
                    
                    st.altair_chart(chart_close, use_container_width=True)
                    
                with col2:
                    st.markdown("##### 📊 세력별 순매수 대금 (백만원)")
                    
                    # Altair를 위한 데이터 변환 (Long-form 데이터)
                    bar_data = target_hist.melt('일자', value_vars=['외인', '연기금', '투신', '사모'], 
                                                 var_name='투자자', value_name='금액')
                    
                    # 🔥 해결 2: X축을 Ordinal(:O)로 설정하여 주말(거래 없는 날) 공백 완벽 제거
                    chart_bar = alt.Chart(bar_data).mark_bar().encode(
                        x=alt.X('일자:O', axis=alt.Axis(format='%Y-%m-%d', title=None)), # 공백 없이 꽉 채움
                        y=alt.Y('금액:Q', title=None),
                        color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None)), # 범례 제목 제거
                        order=alt.Order('투자자:N', sort='descending') # 누적 바 쌓는 순서
                    ).properties(height=280)
                    
                    st.altair_chart(chart_bar, use_container_width=True)
