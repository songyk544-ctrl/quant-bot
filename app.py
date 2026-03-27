import streamlit as st
import pandas as pd
import altair as alt
import os

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V7.0", page_icon="⚡")
st.title("⚡ 실전 수급 스윙 대시보드 V7.0")
st.caption("🌐 AI 매크로 리포트 및 랭킹 모멘텀(Trend) 트래킹 탑재")

def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 데이터 수집 봇이 아직 파일을 생성하지 않았습니다.")
else:
    # 🔥 [V7.0] 랭킹 추세 계산 로직
    df_summary['현재_순위'] = df_summary['AI수급점수'].rank(method='min', ascending=False).astype(int)
    if os.path.exists("score_trend.csv"):
        df_trend = pd.read_csv("score_trend.csv")
        dates = sorted(df_trend['날짜'].unique(), reverse=True)
        
        # 어제(또는 이전) 데이터가 있을 경우에만 비교
        if len(dates) >= 2:
            yday_data = df_trend[df_trend['날짜'] == dates[1]][['종목명', '순위']]
            yday_data.columns = ['종목명', '전일_순위']
            df_summary = pd.merge(df_summary, yday_data, on='종목명', how='left')
            df_summary['전일_순위'] = df_summary['전일_순위'].fillna(df_summary['현재_순위'])
            df_summary['순위_변동'] = df_summary['전일_순위'] - df_summary['현재_순위']
            df_summary['랭킹추세'] = df_summary['순위_변동'].apply(
                lambda x: f"🔺{int(x)}" if x > 0 else (f"🔻{abs(int(x))}" if x < 0 else "-")
            )
        else:
            df_summary['랭킹추세'] = "-"
    else:
        df_summary['랭킹추세'] = "-"

    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2, tab3 = st.tabs(["📊 리얼 수급 랭킹", f"📈 [{st.session_state.selected_stock}] 상세 차트", "📝 당일 AI 마감 리포트"])

    with tab1:
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if pd.isna(val): return 'color: gray;'
            if isinstance(val, (int, float)):
                if val > 0: return 'color: #FF3333; font-weight: bold;'
                elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        df_display = df_summary.set_index('종목명')

        styled_df = df_display.style.map(color_score, subset=['AI수급점수']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", 
                                             "투신강도(%)": "{:.2f}%", "사모강도(%)": "{:.2f}%",
                                             "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%",
                                             "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={
                "_index": st.column_config.TextColumn("종목명", width="small"),
                "랭킹추세": st.column_config.Column("전일대비"), # 🔥 추세 컬럼 노출
                "AI수급점수": st.column_config.NumberColumn("🏆 AI 점수"),
                "현재가": st.column_config.Column("현재가"),
                "등락률": st.column_config.Column("등락"),
                "외인강도(%)": st.column_config.Column("외인(1달)"),
                "연기금강도(%)": st.column_config.Column("연기금(1달)"),
                "이격도(%)": st.column_config.Column("이격도"),
                "손바뀜(%)": st.column_config.Column("손바뀜"),
                "투신강도(%)": st.column_config.Column("투신(1달)"),
                "사모강도(%)": st.column_config.Column("사모(1달)"),
                "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"),
                "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"),
                "시가총액": st.column_config.Column("시총(억)"),
                "소속": st.column_config.Column("시장")
            },
            # 불필요한 계산용 컬럼들은 화면에서 숨김 처리
            column_order=["_index", "랭킹추세", "AI수급점수", "현재가", "등락률", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "시가총액", "소속"],
            hide_index=False, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} 기술적 분석")
        st.write(f"- **수급 종합점수:** **{selected_row['AI수급점수']}점** (전일대비: {selected_row['랭킹추세']})")
        tech_status = "🟢 완벽한 눌림목 타점" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴 하락 추세 (관망)" if selected_row['이격도(%)'] < 95 else "⚫ 보유자 영역")
        st.write(f"- **기술적 타점:** 20일선 이격도 {selected_row['이격도(%)']}% ({tech_status}) / 5일 손바뀜 {selected_row['손바뀜(%)']}%")
        
        st.markdown("---")
        
        if not df_history.empty:
            target_hist = df_history[df_history['종목명'] == st.session_state.selected_stock].copy()
            if not target_hist.empty:
                target_hist['일자'] = pd.to_datetime(target_hist['일자'].astype(str))
                target_hist = target_hist.sort_values('일자')
                target_hist['일자_표시'] = target_hist['일자'].dt.strftime('%m/%d')
                
                col1, col2 = st.columns(2)
                color_scale = alt.Scale(domain=['외인', '연기금', '투신', '사모'], range=['#FF4B4B', '#1C83E1', '#F1C40F', '#83C9FF'])
                
                with col1:
                    st.markdown("##### 📈 일봉 차트 (최근 20일 종가)")
                    chart_close = alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)
                    ).properties(height=280)
                    st.altair_chart(chart_close, use_container_width=True)
                with col2:
                    st.markdown("##### 📊 당일 세력별 순매수 대금 (백만원)")
                    bar_data = target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='투자자', value_name='금액')
                    chart_bar = alt.Chart(bar_data).mark_bar().encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('금액:Q', title=None),
                        color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                        order=alt.Order('투자자:N', sort='descending')
                    ).properties(height=280)
                    st.altair_chart(chart_bar, use_container_width=True)

        # 🔥 [V7.0 추가] 과거 AI 점수 트렌드 차트
        st.markdown("---")
        st.markdown(f"##### 🎢 과거 AI 점수 트렌드 ({st.session_state.selected_stock})")
        if os.path.exists("score_trend.csv"):
            df_trend = pd.read_csv("score_trend.csv")
            df_stock_trend = df_trend[df_trend['종목명'] == st.session_state.selected_stock].sort_values('날짜')
            if not df_stock_trend.empty and len(df_stock_trend) >= 2:
                chart_trend = alt.Chart(df_stock_trend).mark_line(color='#E74C3C', point=True).encode(
                    x=alt.X('날짜:O', axis=alt.Axis(title=None, labelAngle=-45)),
                    y=alt.Y('AI수급점수:Q', scale=alt.Scale(domain=[0, 100]), title=None)
                ).properties(height=200)
                st.altair_chart(chart_trend, use_container_width=True)
            else:
                st.caption("⏳ 최소 이틀 이상의 데이터가 쌓이면 트렌드 차트가 표시됩니다.")

    with tab3:
        st.subheader("📰 오늘의 Top-Down 마감 시황 & 관심종목")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f:
                report_content = f.read()
            st.markdown(report_content)
        else:
            st.info("⏳ 오늘의 AI 마감 리포트가 아직 생성되지 않았습니다.")
