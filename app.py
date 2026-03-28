import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf

# 1. 브라우저 탭 이름 & 아이콘
st.set_page_config(layout="wide", page_title="DeepAlpha 퀀트 터미널", page_icon="🏛️")

# 2. 메인 타이틀 & 서브타이틀
st.title("🏛️ DeepAlpha 퀀트 터미널")
st.caption("AI 기반 기관/외인 수급 및 글로벌 매크로 분석 플랫폼")

# 매크로 지표 캐싱
@st.cache_data(ttl=1800)
def get_macro_data():
    tickers = {
        "🇰🇷 KOSPI": "^KS11",
        "🇰🇷 KOSDAQ": "^KQ11",
        "🇺🇸 S&P500": "^GSPC",
        "🇺🇸 NASDAQ": "^IXIC",
        "💵 환율": "KRW=X",
        "🛢️ WTI유": "CL=F",
        "📉 미 국채(10y)": "^TNX",
        "😨 VIX": "^VIX"
    }
    macro_info = {}
    for name, ticker in tickers.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if len(hist) >= 2:
                current = hist['Close'].iloc[-1]
                prev = hist['Close'].iloc[-2]
                change = current - prev
                change_pct = (change / prev) * 100
                macro_info[name] = {"value": current, "change": change, "change_pct": change_pct}
            else:
                macro_info[name] = None
        except:
            macro_info[name] = None
    return macro_info

# 모바일 가로 스크롤 티커 UI
macro_data = get_macro_data()
ticker_style = """
<style>
.ticker-wrap {
    width: 100%; overflow-x: auto; white-space: nowrap;
    background-color: #1E1E2E; padding: 12px 15px; border-radius: 8px;
    border: 1px solid #333; margin-bottom: 20px; -webkit-overflow-scrolling: touch;
}
.ticker-wrap::-webkit-scrollbar { display: none; }
.ticker-item { display: inline-block; margin-right: 30px; font-size: 15px; font-family: 'Inter', sans-serif; }
</style>
"""
ticker_html = "<div class='ticker-wrap'>"
for name, data in macro_data.items():
    if data:
        color = "#FF3333" if data['change'] > 0 else "#0066FF" if data['change'] < 0 else "#888888"
        arrow = "▲" if data['change'] > 0 else "▼" if data['change'] < 0 else "-"
        if "환율" in name: val_str = f"{data['value']:,.1f}원"
        elif "국채" in name or "VIX" in name: val_str = f"{data['value']:.2f}"
        else: val_str = f"{data['value']:,.2f}"
        ticker_html += f"<div class='ticker-item'><span style='color: #DDDDDD;'>{name}</span> <b>{val_str}</b> <span style='color: {color}; font-weight: bold;'>{arrow} {abs(data['change_pct']):.2f}%</span></div>"
    else:
        ticker_html += f"<div class='ticker-item'><span style='color: #DDDDDD;'>{name}</span> <span style='color: #888888;'>데이터 지연</span></div>"
ticker_html += "</div>"
st.markdown(ticker_style + ticker_html, unsafe_allow_html=True)

# 메인 데이터 로드
def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 시장 데이터를 집계 중입니다. 잠시 후 다시 확인해 주세요.")
else:
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

    tab1, tab2, tab3 = st.tabs(["📊 시장 수급 스크리너", f"📈 개별 종목 정밀 분석", "🌍 AI 매크로 인사이트"])

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
                "섹터": st.column_config.Column("테마/섹터"),  # 🔥 V8.2 섹터 컬럼 UI 추가
                "랭킹추세": st.column_config.Column("모멘텀"),
                "AI수급점수": st.column_config.NumberColumn("🏆 AI점수"),
                "현재가": st.column_config.Column("현재가(원)"),
                "등락률": st.column_config.Column("등락(%)"),
                "외인강도(%)": st.column_config.Column("외인(1M)"),
                "연기금강도(%)": st.column_config.Column("연기금(1M)"),
                "이격도(%)": st.column_config.Column("이격도(20D)"),
                "손바뀜(%)": st.column_config.Column("손바뀜(5D)"),
                "투신강도(%)": st.column_config.Column("투신(1M)"),
                "사모강도(%)": st.column_config.Column("사모(1M)"),
                "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"),
                "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"),
                "시가총액": st.column_config.Column("시총(억)"),
                "소속": st.column_config.Column("시장")
            },
            # 종목명 바로 옆에 섹터가 오도록 배치 순서 조정
            column_order=["_index", "섹터", "랭킹추세", "AI수급점수", "현재가", "등락률", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "시가총액", "소속"],
            hide_index=False, use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        # 상세 차트 탭에도 섹터 표기
        sector_disp = selected_row.get('섹터', '분류안됨')
        st.subheader(f"💡 {st.session_state.selected_stock} [{sector_disp}] : 수급 및 기술적 분석")
        st.write(f"- **종합 AI 점수:** **{selected_row['AI수급점수']} / 100** (전일대비 모멘텀: {selected_row['랭킹추세']})")
        
        tech_status = "🟢 최적 매수 구간" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴 리스크 관리 구간" if selected_row['이격도(%)'] < 95 else "⚫ 추세 추종 구간")
        st.write(f"- **기술적 위치:** 20일선 이격도 {selected_row['이격도(%)']}% ({tech_status}) / 5일 손바뀜 {selected_row['손바뀜(%)']}%")
        
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
                    st.markdown("##### 📈 20일 종가 차트")
                    chart_close = alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)
                    ).properties(height=280)
                    st.altair_chart(chart_close, use_container_width=True)
                with col2:
                    st.markdown("##### 📊 주체별 순매수 대금 (백만 원)")
                    bar_data = target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='투자자', value_name='금액')
                    chart_bar = alt.Chart(bar_data).mark_bar().encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('금액:Q', title=None),
                        color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                        order=alt.Order('투자자:N', sort='descending')
                    ).properties(height=280)
                    st.altair_chart(chart_bar, use_container_width=True)

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
                st.caption("⏳ 데이터 누적 및 트렌드 분석 중입니다.")

    with tab3:
        st.subheader("📰 오늘의 Top-Down 매크로 리포트")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f:
                report_content = f.read()
            st.markdown(report_content)
        else:
            st.info("⏳ AI 매크로 리포트를 생성 중입니다.")
