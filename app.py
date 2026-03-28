import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf
from gtts import gTTS
import io
import google.generativeai as genai

st.set_page_config(layout="wide", page_title="DeepAlpha 퀀트 터미널", page_icon="🏛️")
st.title("🏛️ DeepAlpha 퀀트 터미널")
st.caption("AI 기반 기관/외인 수급 및 글로벌 매크로 분석 플랫폼")

# --- AI API 설정 ---
gemini_key = st.secrets.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY"))
if gemini_key:
    genai.configure(api_key=gemini_key)
    model = genai.GenerativeModel('gemini-2.5-flash')
else: model = None

@st.cache_data(ttl=1800)
def get_macro_data():
    tickers = {"🇰🇷 KOSPI": "^KS11", "🇰🇷 KOSDAQ": "^KQ11", "🇺🇸 S&P500": "^GSPC", "🇺🇸 NASDAQ": "^IXIC", "💵 환율": "KRW=X", "🛢️ WTI유": "CL=F", "📉 미 국채(10y)": "^TNX", "😨 VIX": "^VIX"}
    macro_info = {}
    for name, ticker in tickers.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if len(hist) >= 2:
                current, prev = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
                macro_info[name] = {"value": current, "change": current - prev, "change_pct": ((current - prev) / prev) * 100}
            else: macro_info[name] = None
        except: macro_info[name] = None
    return macro_info

macro_data = get_macro_data()
ticker_style = """<style>.ticker-wrap { width: 100%; overflow-x: auto; white-space: nowrap; background-color: #1E1E2E; padding: 12px 15px; border-radius: 8px; border: 1px solid #333; margin-bottom: 20px; -webkit-overflow-scrolling: touch; } .ticker-wrap::-webkit-scrollbar { display: none; } .ticker-item { display: inline-block; margin-right: 30px; font-size: 15px; font-family: 'Inter', sans-serif; }</style>"""
ticker_html = f"<div class='ticker-wrap'>{ticker_style}"
for name, data in macro_data.items():
    if data:
        color = "#FF3333" if data['change'] > 0 else "#0066FF" if data['change'] < 0 else "#888888"
        arrow = "▲" if data['change'] > 0 else "▼" if data['change'] < 0 else "-"
        val_str = f"{data['value']:,.1f}원" if "환율" in name else (f"{data['value']:.2f}" if "국채" in name or "VIX" in name else f"{data['value']:,.2f}")
        ticker_html += f"<div class='ticker-item'><span style='color: #DDDDDD;'>{name}</span> <b>{val_str}</b> <span style='color: {color}; font-weight: bold;'>{arrow} {abs(data['change_pct']):.2f}%</span></div>"
    else: ticker_html += f"<div class='ticker-item'><span style='color: #DDDDDD;'>{name}</span> <span style='color: #888888;'>데이터 지연</span></div>"
ticker_html += "</div>"
st.markdown(ticker_html, unsafe_allow_html=True)

def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

@st.cache_data(show_spinner=False)
def generate_audio(text):
    tts = gTTS(text=text, lang='ko', slow=False)
    fp = io.BytesIO()
    tts.write_to_fp(fp)
    return fp.getvalue()

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 시장 데이터를 집계 중입니다.")
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
            df_summary['랭킹추세'] = (df_summary['전일_순위'] - df_summary['현재_순위']).apply(lambda x: f"🔺{int(x)}" if x > 0 else (f"🔻{abs(int(x))}" if x < 0 else "-"))
        else: df_summary['랭킹추세'] = "-"
    else: df_summary['랭킹추세'] = "-"

    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌍 매크로 인사이트", "📊 수급 스크리너", f"📈 종목 분석", "🏆 백테스트", "💬 Ask DeepAlpha"])

    with tab1:
        st.subheader("📰 오늘의 Top-Down 매크로 리포트")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()
            st.markdown("##### 🎧 시황 라디오 듣기")
            try:
                clean_text = report_content.replace("#", "").replace("*", "").replace("-", " ").replace("🌐", "").replace("🌪️", "").replace("🎯", "")
                st.audio(generate_audio(clean_text), format="audio/mp3")
            except: st.error("오디오 생성 중 오류가 발생했습니다.")
            st.markdown("---")
            st.markdown(report_content)
        else: st.info("⏳ AI 매크로 리포트를 생성 중입니다.")

    with tab2:
        def color_score(val): return f'color: {"#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"}; font-weight: bold;'
        def color_fluctuation(val):
            if pd.isna(val): return 'color: gray;'
            if isinstance(val, (int, float)): return 'color: #FF3333; font-weight: bold;' if val > 0 else ('color: #0066FF; font-weight: bold;' if val < 0 else 'color: gray;')
            return 'color: gray;'

        styled_df = df_summary.set_index('종목명').style.map(color_score, subset=['AI수급점수']).map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']).format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", "투신강도(%)": "{:.2f}%", "사모강도(%)": "{:.2f}%", "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%", "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={"_index": st.column_config.TextColumn("종목명", width="small"), "섹터": st.column_config.Column("테마/섹터"), "랭킹추세": st.column_config.Column("모멘텀"), "AI수급점수": st.column_config.NumberColumn("🏆 AI점수"), "현재가": st.column_config.Column("현재가(원)"), "등락률": st.column_config.Column("등락(%)"), "외인강도(%)": st.column_config.Column("외인(1M)"), "연기금강도(%)": st.column_config.Column("연기금(1M)"), "이격도(%)": st.column_config.Column("이격도(20D)"), "손바뀜(%)": st.column_config.Column("손바뀜(5D)"), "투신강도(%)": st.column_config.Column("투신(1M)"), "사모강도(%)": st.column_config.Column("사모(1M)"), "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"), "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"), "시가총액": st.column_config.Column("시총(억)"), "소속": st.column_config.Column("시장")},
            column_order=["_index", "섹터", "랭킹추세", "AI수급점수", "현재가", "등락률", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "시가총액", "소속"],
            hide_index=False, use_container_width=True, height=600 
        )
        if event.selection.rows: st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab3:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} [{selected_row.get('섹터', '분류안됨')}] : 수급 및 기술적 분석")
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
                    st.altair_chart(alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)).properties(height=280), use_container_width=True)
                with col2:
                    st.markdown("##### 📊 주체별 순매수 대금 (백만 원)")
                    st.altair_chart(alt.Chart(target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='투자자', value_name='금액')).mark_bar().encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('금액:Q', title=None), color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')), order=alt.Order('투자자:N', sort='descending')).properties(height=280), use_container_width=True)

        st.markdown("---")
        st.markdown(f"##### 🎢 과거 AI 점수 트렌드 ({st.session_state.selected_stock})")
        if os.path.exists("score_trend.csv"):
            df_trend = pd.read_csv("score_trend.csv")
            df_stock_trend = df_trend[df_trend['종목명'] == st.session_state.selected_stock].sort_values('날짜')
            if not df_stock_trend.empty and len(df_stock_trend) >= 2:
                st.altair_chart(alt.Chart(df_stock_trend).mark_line(color='#E74C3C', point=True).encode(x=alt.X('날짜:O', axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('AI수급점수:Q', scale=alt.Scale(domain=[0, 100]), title=None)).properties(height=200), use_container_width=True)

    with tab4:
        st.subheader("🏆 DeepAlpha 모델 가상 포트폴리오 백테스트")
        if os.path.exists("performance_trend.csv"):
            df_perf = pd.read_csv("performance_trend.csv")
            if not df_perf.empty:
                st.metric(label="현재 누적 수익률", value=f"{df_perf['누적수익률'].iloc[-1]:+.2f}%", delta=f"전일 대비 {df_perf['일간수익률'].iloc[-1]:+.2f}%")
                st.altair_chart(alt.Chart(df_perf).mark_area(color=alt.Gradient(gradient='linear', stops=[alt.GradientStop(color='#E74C3C', offset=0), alt.GradientStop(color='transparent', offset=1)], x1=1, x2=1, y1=1, y2=0), line={'color': '#E74C3C'}).encode(x=alt.X('날짜:O', axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('누적수익률:Q', title="누적 수익률 (%)")).properties(height=300), use_container_width=True)
            else: st.info("⏳ 데이터 대기 중")
        else: st.info("⏳ 데이터 대기 중")

    # 🔥 [V12.2] 타이핑 이펙트(Streaming)가 적용된 챗봇
    with tab5:
        st.subheader("💬 Ask DeepAlpha (AI 퀀트 비서)")
        
        if not model:
            st.error("⚠️ Streamlit Secrets에 GEMINI_API_KEY가 설정되지 않아 챗봇을 사용할 수 없습니다.")
        else:
            if "messages" not in st.session_state:
                st.session_state.messages = [{"role": "assistant", "content": "안녕하세요! 오늘의 시장 주도주나 글로벌 매크로 동향에 대해 무엇이든 물어보세요."}]

            chat_container = st.container()
            with chat_container:
                for msg in st.session_state.messages:
                    if msg["role"] == "assistant":
                        st.chat_message("assistant", avatar="🏛️").write(msg["content"])
                    else:
                        st.chat_message("user", avatar="👤").write(msg["content"])

            top_stock = df_summary.iloc[0]['종목명'] if not df_summary.empty else "삼성전자"
            top_sector = df_summary['섹터'].value_counts().idxmax() if not df_summary.empty else "반도체"

            st.markdown("<br>", unsafe_allow_html=True)
            st.caption("🔍 실시간 데이터 기반 추천 질문 (클릭 시 자동 분석)")
            col1, col2, col3 = st.columns(3)
            
            if col1.button(f"🔥 {top_sector} 섹터 동향", use_container_width=True):
                st.session_state.trigger_prompt = f"오늘 핫한 '{top_sector}' 섹터의 수급 동향을 짚어주고, 이 섹터가 현재 미국의 통화정책이나 글로벌 이슈와 어떤 연관성이 있는지 분석해줘."
            if col2.button(f"🏆 {top_stock} 매크로 분석", use_container_width=True):
                st.session_state.trigger_prompt = f"오늘 수급 1위인 '{top_stock}'의 매력 포인트를 설명해주고, 이 종목에 영향을 줄 수 있는 해외 지수나 환율 동향을 함께 코멘트해줘."
            if col3.button("🌍 전체 탑다운 뷰", use_container_width=True):
                st.session_state.trigger_prompt = "현재 글로벌 매크로 환경(미국 국채금리, 기술주 흐름 등)을 바탕으로 오늘 우리 증시의 전체적인 기관/외인 수급 흐름을 총평해줘."

            user_input = st.chat_input("종목명이나 궁금한 시황을 입력하세요...")
            prompt = st.session_state.pop("trigger_prompt", user_input)

            if prompt:
                st.session_state.messages.append({"role": "user", "content": prompt})
                with chat_container:
                    st.chat_message("user", avatar="👤").write(prompt)

                context_data = df_summary.head(20).to_string(index=False)
                system_prompt = f"""
                너는 'DeepAlpha'의 수석 퀀트 애널리스트야.
                아래는 오늘 시장에서 AI 알고리즘으로 추출한 상위 20개 종목의 수급/기술적 데이터야.
                {context_data}
                
                사용자의 질문: {prompt}
                
                [핵심 지시사항]
                1. 주어진 데이터를 바탕으로 정확하게 답변할 것.
                2. 종목이나 섹터를 분석할 때는 반드시 '미국 기준금리, 나스닥/S&P500 지수 흐름, 환율' 등 글로벌 매크로 이벤트와 연결지어서 큰 그림(Top-Down)의 뷰를 함께 제시할 것.
                """

                # 🔥 [V12.2 핵심] AI의 답변을 통째로 받지 않고 스트리밍(stream=True)으로 받아옵니다.
                with chat_container:
                    with st.chat_message("assistant", avatar="🏛️"):
                        try:
                            response = model.generate_content(system_prompt, stream=True)
                            
                            # 스트림릿의 st.write_stream에 넘겨주기 위한 생성기(Generator) 함수
                            def stream_generator():
                                for chunk in response:
                                    if chunk.text:
                                        yield chunk.text
                                        
                            # 타자 치듯 촤르르륵 출력되는 마법의 함수!
                            bot_reply = st.write_stream(stream_generator)
                            
                        except Exception as e:
                            bot_reply = f"앗, 분석 중 에러가 발생했습니다: {e}"
                            st.write(bot_reply)

                    # 최종 완성된 답변을 세션에 저장하여 다음 새로고침 때도 남아있게 합니다.
                    st.session_state.messages.append({"role": "assistant", "content": bot_reply})
