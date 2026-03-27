import streamlit as st
import pandas as pd
import altair as alt
import os

# 1. 브라우저 탭 이름 & 아이콘 고급화
st.set_page_config(layout="wide", page_title="DeepAlpha Terminal", page_icon="🏛️")

# 2. 메인 타이틀 & 서브타이틀 기관급 워딩으로 변경
st.title("🏛️ DeepAlpha Quant Terminal")
st.caption("AI-Driven Institutional Fund Flow & Macro Analysis Platform")

def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ Market data is currently being aggregated. Please check back shortly.")
else:
    # 랭킹 추세 계산 로직 (기존과 동일)
    df_summary['현재_순위'] = df_summary['AI수급점수'].rank(method='min', ascending=False).astype(int)
    if os.path.exists("score_trend.csv"):
        df_trend = pd.read_csv("score_trend.csv")
        dates = sorted(df_trend['날짜'].unique(), reverse=True)
        
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

    # 3. 탭 이름 세련되게 변경
    tab1, tab2, tab3 = st.tabs([
        "📊 Market Screener", 
        f"📈 Advanced Charting", 
        "🌍 AI Macro Insight"
    ])

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

        # 4. 표 컬럼 이름 고급화 (영문/국문 혼용으로 엣지있게)
        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={
                "_index": st.column_config.TextColumn("Ticker", width="small"),
                "랭킹추세": st.column_config.Column("Momentum"),
                "AI수급점수": st.column_config.NumberColumn("🏆 Alpha Score"),
                "현재가": st.column_config.Column("Price(KRW)"),
                "등락률": st.column_config.Column("Change(%)"),
                "외인강도(%)": st.column_config.Column("Foreign(1M)"),
                "연기금강도(%)": st.column_config.Column("Pension(1M)"),
                "이격도(%)": st.column_config.Column("Disparity(20D)"),
                "손바뀜(%)": st.column_config.Column("Turnover(5D)"),
                "투신강도(%)": st.column_config.Column("Trust(1M)"),
                "사모강도(%)": st.column_config.Column("PEF(1M)"),
                "외인연속": st.column_config.NumberColumn("F_Streak", format="%d Days"),
                "연기금연속": st.column_config.NumberColumn("P_Streak", format="%d Days"),
                "시가총액": st.column_config.Column("Market Cap(억)"),
                "소속": st.column_config.Column("Market")
            },
            column_order=["_index", "랭킹추세", "AI수급점수", "현재가", "등락률", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "시가총액", "소속"],
            hide_index=False, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} : Technical & Flow Analysis")
        st.write(f"- **DeepAlpha Score:** **{selected_row['AI수급점수']} / 100** (Daily Momentum: {selected_row['랭킹추세']})")
        
        # 5. 유치했던 진단 멘트를 세련된 트레이딩 용어로 변경
        tech_status = "🟢 최적 매수 구간 (Golden Zone)" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴 리스크 관리 구간 (Wait & See)" if selected_row['이격도(%)'] < 95 else "⚫ 추세 추종 구간 (Trend Following)")
        st.write(f"- **Technical Positioning:** 20D Disparity {selected_row['이격도(%)']}% ({tech_status}) / 5D Turnover {selected_row['손바뀜(%)']}%")
        
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
                    st.markdown("##### 📈 Price Action (20D)")
                    chart_close = alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)
                    ).properties(height=280)
                    st.altair_chart(chart_close, use_container_width=True)
                with col2:
                    st.markdown("##### 📊 Institutional Net Flow (KRW Mil)")
                    bar_data = target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='Investor', value_name='Amount')
                    chart_bar = alt.Chart(bar_data).mark_bar().encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('Amount:Q', title=None),
                        color=alt.Color('Investor:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                        order=alt.Order('Investor:N', sort='descending')
                    ).properties(height=280)
                    st.altair_chart(chart_bar, use_container_width=True)

        st.markdown("---")
        st.markdown(f"##### 🎢 Alpha Score Trend ({st.session_state.selected_stock})")
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
                st.caption("⏳ Analyzing trend... Data accumulation in progress.")

    with tab3:
        st.subheader("📰 Daily Top-Down Macro Briefing")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f:
                report_content = f.read()
            st.markdown(report_content)
        else:
            st.info("⏳ AI Macro Insight report is being generated. Please wait.")
