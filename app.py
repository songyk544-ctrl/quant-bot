import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf
from gtts import gTTS
import io
from google import genai
from google.genai import types
from datetime import datetime

st.set_page_config(layout="wide", page_title="DeepAlpha 퀀트 터미널", page_icon="🏛️")

# --- 🔥 [V16.0] 고급스러운 블러(Blur) 페이월 UI 함수 ---
def show_premium_paywall(message="이 콘텐츠는 VIP 회원 전용입니다."):
    st.markdown(f"""
    <div style="position: relative; margin-top: 10px; margin-bottom: 30px;">
        <!-- 흐릿하게 보이는 가짜 백그라운드 콘텐츠 -->
        <div style="filter: blur(8px); opacity: 0.4; pointer-events: none; user-select: none;">
            <h4 style="color: #888;">████████ 기술적 분석 및 수급 동향</h4>
            <p>██████████████████████████████████████████████████████</p>
            <p>████████████████████████████████████</p>
            <div style="height: 150px; background: linear-gradient(90deg, #333 0%, #222 50%, #333 100%); border-radius: 10px; margin-top: 10px;"></div>
        </div>
        <!-- 중앙에 떠오르는 고급스러운 자물쇠 오버레이 -->
        <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; background: rgba(20, 20, 30, 0.85); padding: 30px; border-radius: 15px; border: 1px solid #FFD700; box-shadow: 0 10px 30px rgba(255, 215, 0, 0.15); width: 85%; backdrop-filter: blur(5px);">
            <h2 style="margin:0; color:#FFD700; font-weight: 800; letter-spacing: 1px;">🔒 PREMIUM ONLY</h2>
            <p style="color:#FFF; margin-top:15px; font-size: 1.1em;">{message}</p>
            <p style="font-size:0.85em; color:#AAA; margin-top: 5px;">좌측 <b>[>]</b> 메뉴를 열어 VIP 코드를 입력해주세요.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

# 사이드바 VIP 로그인 로직
VIP_CODE = "ALPHA2026"
st.sidebar.markdown("## 💎 프리미엄 멤버십")
st.sidebar.caption("VIP 코드를 입력하고 전체 주도주와 상세 분석 데이터를 확인하세요.")
user_code = st.sidebar.text_input("🔑 VIP 엑세스 코드", type="password")

is_vip = (user_code == VIP_CODE)

if is_vip:
    st.sidebar.success("✅ VIP 인증 완료! 모든 데이터가 개방되었습니다.")
else:
    st.sidebar.info("👀 현재 무료 버전을 체험 중입니다.")

st.title("🏛️ DeepAlpha 퀀트 터미널")
st.caption("AI 기반 기관/외인 수급 및 글로벌 매크로 분석 플랫폼")

gemini_key = st.secrets.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY"))
if gemini_key:
    client = genai.Client(api_key=gemini_key)
else: client = None

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
macro_summary_text = ""
for name, data in macro_data.items():
    if data:
        color = "#FF3333" if data['change'] > 0 else "#0066FF" if data['change'] < 0 else "#888888"
        arrow = "▲" if data['change'] > 0 else "▼" if data['change'] < 0 else "-"
        val_str = f"{data['value']:,.1f}원" if "환율" in name else (f"{data['value']:.2f}" if "국채" in name or "VIX" in name else f"{data['value']:,.2f}")
        ticker_html += f"<div class='ticker-item'><span style='color: #DDDDDD;'>{name}</span> <b>{val_str}</b> <span style='color: {color}; font-weight: bold;'>{arrow} {abs(data['change_pct']):.2f}%</span></div>"
        macro_summary_text += f"{name}: {val_str} ({arrow} {abs(data['change_pct']):.2f}%)\n"
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌍 인사이트", "📊 스크리너", f"📈 종목 분석", "🏆 백테스트", "💬 챗봇"])

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

        if is_vip:
            df_display = df_summary.set_index('종목명')
        else:
            # 무료 회원은 5개만 렌더링
            df_display = df_summary.head(5).set_index('종목명')

        styled_df = df_display.style.map(color_score, subset=['AI수급점수']).map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']).format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", "투신강도(%)": "{:.2f}%", "사모강도(%)": "{:.2f}%", "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%", "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={"_index": st.column_config.TextColumn("종목명", width="small"), "섹터": st.column_config.Column("테마/섹터"), "랭킹추세": st.column_config.Column("모멘텀"), "AI수급점수": st.column_config.NumberColumn("🏆 AI점수"), "현재가": st.column_config.Column("현재가(원)"), "등락률": st.column_config.Column("등락(%)"), "외인강도(%)": st.column_config.Column("외인(1M)"), "연기금강도(%)": st.column_config.Column("연기금(1M)"), "이격도(%)": st.column_config.Column("이격도(20D)"), "손바뀜(%)": st.column_config.Column("손바뀜(5D)"), "투신강도(%)": st.column_config.Column("투신(1M)"), "사모강도(%)": st.column_config.Column("사모(1M)"), "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"), "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"), "시가총액": st.column_config.Column("시총(억)"), "소속": st.column_config.Column("시장")},
            column_order=["_index", "섹터", "랭킹추세", "AI수급점수", "현재가", "등락률", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "시가총액", "소속"],
            hide_index=False, use_container_width=True, height=250 if not is_vip else 600
        )
        if event.selection.rows: 
            selected_name = df_display.iloc[event.selection.rows[0]].name
            st.session_state.selected_stock = selected_name

        # 🔥 표 밑에 예쁜 블러 페이월 띄우기
        if not is_vip:
            show_premium_paywall("6위부터 20위까지의 숨겨진 AI 쏠림 주도주를 확인하세요.")

    with tab3:
        # 🔥 종목 분석 탭 블러 처리 로직
        free_tier_stocks = df_summary.head(5)['종목명'].values
        target_stock = st.session_state.selected_stock
        selected_row = df_summary[df_summary['종목명'] == target_stock].iloc[0]
        
        st.subheader(f"💡 {target_stock} [{selected_row.get('섹터', '분류안됨')}]")
        
        if not is_vip and target_stock not in free_tier_stocks:
            # 무료 회원이 6위 이하 종목을 보려고 할 때 멋지게 막음
            show_premium_paywall(f"'{target_stock}'의 상세 수급 분석과 차트는 VIP 전용입니다.")
        else:
            # 정상적인 차트 렌더링
            st.write(f"- **종합 AI 점수:** **{selected_row['AI수급점수']} / 100** (전일대비 모멘텀: {selected_row['랭킹추세']})")
            tech_status = "🟢 최적 매수 구간" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴 리스크 관리 구간" if selected_row['이격도(%)'] < 95 else "⚫ 추세 추종 구간")
            st.write(f"- **기술적 위치:** 20일선 이격도 {selected_row['이격도(%)']}% ({tech_status}) / 5일 손바뀜 {selected_row['손바뀜(%)']}%")
            st.markdown("---")
            
            if not df_history.empty:
                target_hist = df_history[df_history['종목명'] == target_stock].copy()
                if not target_hist.empty:
                    target_hist['일자'] = pd.to_datetime(target_hist['일자'].astype(str))
                    target_hist = target_hist.sort_values('일자')
                    target_hist['일자_표시'] = target_hist['일자'].dt.strftime('%m/%d')
                    
                    col1, col2 = st.columns(2)
                    color_scale = alt.Scale(domain=['외인', '연기금', '투신', '사모'], range=['#FF4B4B', '#1C83E1', '#F1C40F', '#83C9FF'])
                    with col1:
                        st.altair_chart(alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)).properties(height=280), use_container_width=True)
                    with col2:
                        st.altair_chart(alt.Chart(target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='투자자', value_name='금액')).mark_bar().encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('금액:Q', title=None), color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')), order=alt.Order('투자자:N', sort='descending')).properties(height=280), use_container_width=True)

    with tab4:
        st.subheader("🏆 DeepAlpha 모델 가상 포트폴리오 백테스트")
        if os.path.exists("performance_trend.csv"):
            df_perf = pd.read_csv("performance_trend.csv")
            if not df_perf.empty:
                st.metric(label="현재 누적 수익률", value=f"{df_perf['누적수익률'].iloc[-1]:+.2f}%", delta=f"전일 대비 {df_perf['일간수익률'].iloc[-1]:+.2f}%")
                st.altair_chart(alt.Chart(df_perf).mark_area(color=alt.Gradient(gradient='linear', stops=[alt.GradientStop(color='#E74C3C', offset=0), alt.GradientStop(color='transparent', offset=1)], x1=1, x2=1, y1=1, y2=0), line={'color': '#E74C3C'}).encode(x=alt.X('날짜:O', axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('누적수익률:Q', title="누적 수익률 (%)")).properties(height=300), use_container_width=True)
            else: st.info("⏳ 데이터 대기 중")
        else: st.info("⏳ 데이터 대기 중")

    with tab5:
        st.subheader("💬 Ask DeepAlpha (AI 퀀트 비서)")
        
        if not client:
            st.error("⚠️ Streamlit Secrets에 GEMINI_API_KEY가 설정되지 않아 챗봇을 사용할 수 없습니다.")
        else:
            if "messages" not in st.session_state:
                st.session_state.messages = [{"role": "assistant", "content": "안녕하세요! 종목 분석이나 최신 글로벌 금융 뉴스에 대해 무엇이든 물어보세요."}]

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
                st.session_state.trigger_prompt = f"오늘 핫한 '{top_sector}' 섹터의 수급 동향을 짚어주고, 이 섹터가 현재 미국의 최신 이슈와 어떤 연관성이 있는지 검색해서 분석해줘."
            if col2.button(f"🏆 {top_stock} 매크로 분석", use_container_width=True):
                st.session_state.trigger_prompt = f"오늘 수급 1위인 '{top_stock}'의 매력 포인트를 설명해주고, 이 종목에 영향을 줄 수 있는 최신 글로벌 뉴스를 검색해서 함께 코멘트해줘."
            if col3.button("🌍 오늘의 글로벌 뉴스 검색", use_container_width=True):
                st.session_state.trigger_prompt = "지금 당장 구글을 검색해서, 오늘 글로벌 주식 시장에 영향을 미친 가장 중요한 뉴스 3가지를 요약해줘."

            user_input = st.chat_input("종목명이나 궁금한 최신 뉴스를 입력하세요...")
            prompt = st.session_state.pop("trigger_prompt", user_input)

            if prompt:
                st.session_state.messages.append({"role": "user", "content": prompt})
                with chat_container:
                    st.chat_message("user", avatar="👤").write(prompt)

                today_str = datetime.now().strftime("%Y년 %m월 %d일")
                
                if is_vip:
                    context_data = df_summary.head(20).to_string(index=False)
                    vip_instruction = ""
                else:
                    context_data = df_summary.head(5).to_string(index=False)
                    vip_instruction = "\n**[중요] 현재 사용자는 무료 회원이므로 상위 5개 종목만 볼 수 있어. 6위 이하의 종목을 물어보면 데이터를 숨기고, 반드시 VIP 프리미엄 코드를 입력해야 상세 분석이 가능하다고 답변해!**"
                
                system_prompt = f"""
                너는 'DeepAlpha'의 수석 퀀트 애널리스트야. 오늘은 {today_str}이야.
                
                [1. 실시간 매크로 전광판 데이터]
                {macro_summary_text}
                
                [2. 사용자에게 허락된 수급 데이터]
                {context_data}
                
                사용자의 질문: {prompt}
                
                [핵심 지시사항]
                1. 내장된 '구글 검색 도구'를 적극적으로 활용해서 가장 최신 시점의 뉴스와 정보를 기반으로 대답해.{vip_instruction}
                """

                with chat_container:
                    with st.chat_message("assistant", avatar="🏛️"):
                        try:
                            config = types.GenerateContentConfig(
                                tools=[{"google_search": {}}]
                            )
                            response = client.models.generate_content_stream(
                                model='gemini-2.5-flash',
                                contents=system_prompt,
                                config=config
                            )
                            
                            def stream_generator():
                                for chunk in response:
                                    if chunk.text:
                                        yield chunk.text
                                        
                            bot_reply = st.write_stream(stream_generator)
                            
                        except Exception as e:
                            bot_reply = f"앗, 구글 검색 및 분석 중 에러가 발생했습니다: {e}"
                            st.write(bot_reply)

                    st.session_state.messages.append({"role": "assistant", "content": bot_reply})
