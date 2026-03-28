import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf
import io
import requests
import base64
from google import genai
from google.genai import types
from datetime import datetime
import plotly.express as px  # 🔥 [V20.0] 히트맵 차트를 위한 라이브러리 추가

st.set_page_config(layout="wide", page_title="DeepAlpha 퀀트 터미널", page_icon="🏛️")

# 고급스러운 블러(Blur) 페이월 UI 함수
def show_premium_paywall(message="이 콘텐츠는 VIP 회원 전용입니다."):
    st.markdown(f"""
    <div style="position: relative; margin-top: 10px; margin-bottom: 30px;">
        <div style="filter: blur(8px); opacity: 0.4; pointer-events: none; user-select: none;">
            <h4 style="color: #888;">████████ 데이터 분석 및 리포트</h4>
            <p>██████████████████████████████████████████████████████</p>
            <p>████████████████████████████████████</p>
            <div style="height: 150px; background: linear-gradient(90deg, #333 0%, #222 50%, #333 100%); border-radius: 10px; margin-top: 10px;"></div>
        </div>
        <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; background: rgba(20, 20, 30, 0.85); padding: 30px; border-radius: 15px; border: 1px solid #FFD700; box-shadow: 0 10px 30px rgba(255, 215, 0, 0.15); width: 85%; backdrop-filter: blur(5px);">
            <h2 style="margin:0; color:#FFD700; font-weight: 800; letter-spacing: 1px;">🔒 PREMIUM ONLY</h2>
            <p style="color:#FFF; margin-top:15px; font-size: 1.1em; font-weight: bold;">{message}</p>
            <p style="font-size:0.85em; color:#AAA; margin-top: 5px;">좌측 <b>[>]</b> 사이드바를 열어 VIP 코드를 입력해주세요.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

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

# --- AI API 설정 ---
gemini_key = st.secrets.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY"))
tts_api_key = st.secrets.get("GOOGLE_TTS_API_KEY", os.environ.get("GOOGLE_TTS_API_KEY"))

if gemini_key:
    client = genai.Client(api_key=gemini_key)
else: client = None

# [V19.2] 글자 수 제한을 무시하는 깍둑썰기(Chunking) 오디오 생성 함수 (유지)
@st.cache_data(show_spinner=False)
def generate_audio_premium(text):
    if not tts_api_key:
        print("⚠️ 에러: TTS API 키가 없습니다.")
        return None
        
    url = f"https://texttospeech.googleapis.com/v1/text:synthesize?key={tts_api_key}"
    headers = {"Content-Type": "application/json; charset=utf-8"}
    
    chunk_size = 1000
    text_chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
    combined_audio = b""
    
    for i, chunk in enumerate(text_chunks):
        data = {
            "input": {"text": chunk},
            "voice": {
                "languageCode": "ko-KR", 
                # 👇 찾아내신 구글의 최신 LLM 기반 음성 모델로 업그레이드!
                # (구글 콘솔의 '음성 목록'에서 마음에 드는 Chirp 목소리 이름을 넣으시면 됩니다. 통상적으로 아래와 같은 형식을 띱니다.)
                "name": "ko-KR-Chirp3-HD-Sulafat" 
            },
            "audioConfig": {
                "audioEncoding": "MP3"
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=data)
            if response.status_code == 200:
                audio_base64 = response.json().get("audioContent")
                if audio_base64:
                    combined_audio += base64.b64decode(audio_base64)
            else:
                print(f"⚠️ 구글 TTS API 거절됨 (조각 {i+1}): {response.text}")
                return None
        except Exception as e:
            print(f"⚠️ 통신 에러 발생: {e}")
            return None
            
    return combined_audio

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

    # 🔥 [V20.0 핵심] 탭이 6개로 늘어났습니다! (2번째에 섹터 히트맵 추가)
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🌍 매크로 인사이트", "🗺️ 섹터 히트맵", "📊 수급 스크리너", "📈 종목 분석", "🏆 백테스트", "💬 Ask DeepAlpha"])

    with tab1:
        st.subheader("📰 오늘의 Top-Down 매크로 리포트")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()
            
            if is_vip:
                st.markdown("##### 🎧 프리미엄 시황 라디오 듣기")
                clean_text = report_content.replace("#", "").replace("*", "").replace("-", " ").replace("🌐", "").replace("🌪️", "").replace("🎯", "")
                
                audio_data = generate_audio_premium(clean_text)
                if audio_data:
                    st.audio(audio_data, format="audio/mp3")
                else:
                    st.error("오디오 생성 중 오류가 발생했습니다. 사이드바의 API키를 확인해주세요.")
                st.markdown("---")
                st.markdown(report_content)
            else:
                teaser_text = report_content[:250] + "...\n\n"
                st.markdown(teaser_text)
                show_premium_paywall("심층 매크로 분석 리포트 전문과 프리미엄 아나운서 오디오는 VIP 전용입니다.")
        else: st.info("⏳ AI 매크로 리포트를 생성 중입니다.")

    # 🔥 [V20.0 핵심] 2번째 탭: 섹터 히트맵(Treemap)
    with tab2:
        st.subheader("🗺️ 시가총액 & 수급 섹터 히트맵")
        st.caption("사각형의 크기는 '시가총액', 색상은 '당일 등락률'을 나타냅니다. 어느 섹터에 돈이 몰리는지 한눈에 파악하세요.")
        
        if not is_vip:
            show_premium_paywall("전체 시장의 섹터별 자금 흐름을 조망하는 히트맵 분석은 VIP 전용입니다.")
        else:
            if not df_summary.empty:
                # 데이터 전처리: 결측치 제거 및 숫자로 확실히 변환
                df_hm = df_summary.copy()
                df_hm['섹터'] = df_hm['섹터'].fillna("기타")
                df_hm['시가총액'] = pd.to_numeric(df_hm['시가총액'], errors='coerce').fillna(0)
                df_hm['등락률'] = pd.to_numeric(df_hm['등락률'], errors='coerce').fillna(0)
                
                # 히트맵(Treemap) 그리기
                fig = px.treemap(
                    df_hm,
                    path=[px.Constant("국내 증시 주요 섹터"), '섹터', '종목명'],
                    values='시가총액',
                    color='등락률',
                    # 🟦 파란색(하락) -> ⬛ 검은색(보합) -> 🟥 빨간색(상승) (한국 증시 패치)
                    color_continuous_scale=['#0066FF', '#1E1E2E', '#FF3333'], 
                    color_continuous_midpoint=0,
                    custom_data=['등락률', 'AI수급점수']
                )
                
                # 호버(마우스 오버) 텍스트 및 박스 디자인 다듬기
                fig.update_traces(
                    texttemplate="<b>%{label}</b><br>%{customdata[0]:.2f}%",
                    hovertemplate="<b>%{label}</b><br>시가총액: %{value:,.0f}억<br>등락률: %{customdata[0]:.2f}%<br>AI점수: %{customdata[1]}점<extra></extra>",
                    textfont_color="white"
                )
                fig.update_layout(
                    margin=dict(t=30, l=10, r=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=550
                )
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("데이터 대기 중입니다.")

    # 기존의 수급 스크리너는 탭 3으로 밀려납니다.
    with tab3:
        def color_score(val): return f'color: {"#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"}; font-weight: bold;'
        def color_fluctuation(val):
            if pd.isna(val): return 'color: gray;'
            if isinstance(val, (int, float)): return 'color: #FF3333; font-weight: bold;' if val > 0 else ('color: #0066FF; font-weight: bold;' if val < 0 else 'color: gray;')
            return 'color: gray;'

        if is_vip:
            df_display = df_summary.set_index('종목명')
        else:
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

        if not is_vip:
            show_premium_paywall("6위부터 20위까지의 숨겨진 AI 쏠림 주도주를 확인하세요.")

    with tab4:
        free_tier_stocks = df_summary.head(5)['종목명'].values
        target_stock = st.session_state.selected_stock
        selected_row = df_summary[df_summary['종목명'] == target_stock].iloc[0]
        
        st.subheader(f"💡 {target_stock} [{selected_row.get('섹터', '분류안됨')}]")
        
        if not is_vip and target_stock not in free_tier_stocks:
            show_premium_paywall(f"'{target_stock}'의 상세 수급 분석과 차트는 VIP 전용입니다.")
        else:
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("🏆 종합 AI 점수", f"{selected_row['AI수급점수']}점", f"모멘텀 {selected_row['랭킹추세']}")
            col_m2.metric("💰 시가총액", f"{selected_row['시가총액']:,.0f}억")
            col_m3.metric("📊 PER / ROE", f"{selected_row['PER']:.1f} / {selected_row['ROE']:.1f}%")
            
            tech_status = "🟢최적 매수" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴리스크 관리" if selected_row['이격도(%)'] < 95 else "⚫추세 추종")
            col_m4.metric("📈 20일선 이격도", f"{selected_row['이격도(%)']}%", tech_status, delta_color="off")
            
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
                        st.markdown("##### 📈 20일 종가 추이")
                        st.altair_chart(alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)).properties(height=280), use_container_width=True)
                    with col2:
                        st.markdown("##### 📊 주체별 순매수 대금 (백만 원)")
                        st.altair_chart(alt.Chart(target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], var_name='투자자', value_name='금액')).mark_bar().encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('금액:Q', title=None), color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')), order=alt.Order('투자자:N', sort='descending')).properties(height=280), use_container_width=True)

            st.markdown("---")
            
            st.markdown(f"##### 🤖 DeepAlpha 실시간 종목 진단")
            st.caption("구글 검색 엔진을 활용하여 해당 종목의 최신 호재/악재 및 글로벌 시황 연계 분석을 제공합니다.")
            
            if st.button(f"✨ '{target_stock}' 실시간 심층 리포트 생성", use_container_width=True):
                if not client:
                    st.error("AI 챗봇용 제미나이 API 키가 설정되지 않았습니다.")
                else:
                    with st.spinner(f"구글 검색으로 '{target_stock}'의 매크로 연계 모멘텀을 수집 중입니다..."):
                        today_str = datetime.now().strftime("%Y년 %m월 %d일")
                        
                        prompt = f"""
                        너는 여의도 최고의 탑다운 퀀트 애널리스트야. 오늘은 {today_str}이야.
                        종목명 '{target_stock}'(섹터: {selected_row.get('섹터', '알수없음')})에 대해 '구글 검색'을 반드시 돌려서 아래 양식으로 밀도 있는 브리핑을 해줘.
                        
                        [분석 필수 조건]
                        단순한 개별 종목 뉴스를 나열하지 마. 현재 진행 중인 **글로벌 매크로 이벤트(미국 금리, 환율 동향, 나스닥/S&P500 등 거시 경제 흐름)가 이 특정 종목이나 소속 섹터에 어떤 영향을 미칠지** 반드시 연계해서 입체적으로 코멘트할 것. 만약 사용자가 놓칠만한 리스크가 있다면 그것도 추가로 짚어줘.
                        
                        [출력 양식]
                        1. 📰 최신 모멘텀 & 핵심 뉴스: 구글 검색을 통해 알아낸 이 종목의 가장 최근(오늘/이번 주) 호재 및 악재 이슈
                        2. 🌍 글로벌 시황 연계 분석: (필수 작성) 현재 글로벌 매크로 환경이나 해외 동종 업계(Peer) 흐름이 해당 종목에 주는 영향 
                        3. 💡 수급 및 펀더멘털 평가: PER {selected_row['PER']}, ROE {selected_row['ROE']} 및 기관/외인 수급 강도 분석
                        4. 🎯 단기 투자 전략 및 리스크 관리: 현재 이격도({selected_row['이격도(%)']}%)를 고려한 매수/보유/관망 의견과 주의해야 할 매크로 변수
                        """
                        try:
                            config = types.GenerateContentConfig(tools=[{"google_search": {}}])
                            response = client.models.generate_content_stream(
                                model='gemini-2.5-flash',
                                contents=prompt,
                                config=config
                            )
                            
                            st.success("✅ 실시간 검색 및 탑다운 분석 완료!")
                            def stream_generator():
                                for chunk in response:
                                    if chunk.text: yield chunk.text
                                    
                            with st.container():
                                st.write_stream(stream_generator)
                                
                        except Exception as e:
                            st.error(f"분석 중 오류 발생: {e}")

    with tab5:
        st.subheader("🏆 DeepAlpha 모델 가상 포트폴리오 백테스트")
        if not is_vip:
            show_premium_paywall("가상 포트폴리오 누적 수익률 및 성과 분석은 VIP 전용입니다.")
        else:
            if os.path.exists("performance_trend.csv"):
                df_perf = pd.read_csv("performance_trend.csv")
                if not df_perf.empty:
                    st.metric(label="현재 누적 수익률", value=f"{df_perf['누적수익률'].iloc[-1]:+.2f}%", delta=f"전일 대비 {df_perf['일간수익률'].iloc[-1]:+.2f}%")
                    st.altair_chart(alt.Chart(df_perf).mark_area(color=alt.Gradient(gradient='linear', stops=[alt.GradientStop(color='#E74C3C', offset=0), alt.GradientStop(color='transparent', offset=1)], x1=1, x2=1, y1=1, y2=0), line={'color': '#E74C3C'}).encode(x=alt.X('날짜:O', axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('누적수익률:Q', title="누적 수익률 (%)")).properties(height=300), use_container_width=True)
                else: st.info("⏳ 데이터 대기 중")
            else: st.info("⏳ 데이터 대기 중")

    with tab6:
        st.subheader("💬 Ask DeepAlpha (AI 퀀트 비서)")
        
        if not is_vip:
            show_premium_paywall("실시간 AI 퀀트 애널리스트와의 1:1 무제한 질의응답은 VIP 전용입니다.")
        else:
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
                    
                    context_data = df_summary.head(20).to_string(index=False)
                    system_prompt = f"""
                    너는 'DeepAlpha'의 수석 퀀트 애널리스트야. 오늘은 {today_str}이야.
                    
                    [1. 실시간 매크로 전광판 데이터]
                    {macro_summary_text}
                    
                    [2. 제공된 수급 데이터]
                    {context_data}
                    
                    사용자의 질문: {prompt}
                    
                    [핵심 지시사항]
                    1. 내장된 '구글 검색 도구'를 적극적으로 활용해서 가장 최신 시점의 뉴스와 정보를 기반으로 대답해.
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