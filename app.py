import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from google import genai
from google.genai import types
from datetime import datetime
import plotly.express as px
import urllib.parse

st.set_page_config(layout="wide", page_title="DeepAlpha 퀀트 터미널", page_icon="🏛️")

# --- 🔥 고급스러운 블러(Blur) 페이월 UI 함수 ---
def show_premium_paywall(message="이 콘텐츠는 접근 코드 인증 후 이용할 수 있습니다."):
    st.markdown(f"""
    <div style="position: relative; margin-top: 10px; margin-bottom: 30px;">
        <div style="filter: blur(8px); opacity: 0.4; pointer-events: none; user-select: none;">
            <h4 style="color: #888;">████████ 데이터 분석 및 리포트</h4>
            <p>██████████████████████████████████████████████████████</p>
            <p>████████████████████████████████████</p>
            <div style="height: 150px; background: linear-gradient(90deg, #333 0%, #222 50%, #333 100%); border-radius: 10px; margin-top: 10px;"></div>
        </div>
        <div style="position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); text-align: center; background: rgba(20, 20, 30, 0.85); padding: 30px; border-radius: 15px; border: 1px solid #FFD700; box-shadow: 0 10px 30px rgba(255, 215, 0, 0.15); width: 85%; backdrop-filter: blur(5px);">
            <h2 style="margin:0; color:#FFD700; font-weight: 800; letter-spacing: 1px;">🔒 CODE REQUIRED</h2>
            <p style="color:#FFF; margin-top:15px; font-size: 1.1em; font-weight: bold;">{message}</p>
            <p style="font-size:0.85em; color:#AAA; margin-top: 5px;">좌측 <b>[>]</b> 사이드바를 열어 공유 코드를 입력해주세요.</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

# --- 사이드바 접근 코드 인증 로직 ---
VIP_CODE = "ALPHA2026"
st.sidebar.markdown("## 🔐 접근 코드 인증")
st.sidebar.caption("공유받은 코드를 입력하면 전체 주도주와 상세 분석 데이터를 볼 수 있습니다.")
user_code = st.sidebar.text_input("🔑 접근 코드 입력", type="password")

is_vip = (user_code == VIP_CODE)

if is_vip:
    st.sidebar.success("✅ 코드 인증 완료! 전체 데이터가 열렸습니다.")
else:
    st.sidebar.info("👀 현재 공개 화면만 표시 중입니다. 코드를 입력하면 전체 화면이 열립니다.")

st.title("🏛️ DeepAlpha 퀀트 터미널")
st.caption("AI 기반 기관/외인 수급 및 글로벌 매크로 분석 플랫폼")

# --- AI API 설정 ---
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
st.markdown(
    """
    <style>
    .stock-title-wrap { display:flex; align-items:center; margin-top:5px; margin-bottom:16px; gap:8px; flex-wrap:wrap; }
    .stock-sector-chip { background: linear-gradient(135deg, #1C83E1, #0A58A3); color:white; padding:4px 12px; border-radius:20px; font-size:0.85em; font-weight:700; }
    .stock-grid { display:grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap:10px; margin:8px 0 14px 0; }
    .stock-card { background:#181825; border:1px solid #2B2B3A; border-radius:10px; padding:10px 12px; }
    .stock-label { color:#A8A8B3; font-size:0.78em; margin-bottom:4px; }
    .stock-value { color:#FFF; font-size:1.15em; font-weight:800; line-height:1.15; }
    .stock-sub { color:#9AA0B1; font-size:0.78em; margin-top:3px; }
    @media (max-width: 900px) {
        .stock-grid { grid-template-columns: repeat(2, minmax(110px, 1fr)); }
    }
    </style>
    """,
    unsafe_allow_html=True
)

def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

def safe_get(row, col_name, default=0.0):
    return row[col_name] if col_name in row.index and pd.notna(row[col_name]) else default

def format_pct(value):
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "-"

def resolve_daily_return(df_hist, trade_date, stock_name):
    """history.csv(일자/종목명/등락률)에서 해당 일자의 종목 등락률을 조회합니다."""
    if df_hist.empty or not {"일자", "종목명", "등락률"}.issubset(df_hist.columns):
        return None
    matched = df_hist[(df_hist["일자"].astype(str) == str(trade_date)) & (df_hist["종목명"] == stock_name)]
    if matched.empty:
        return None
    return float(matched.iloc[-1]["등락률"])

# 🔥 [신규 추가] 매크로 주요 시황 스크래핑 함수
@st.cache_data(ttl=1800)
def get_macro_headline_news():
    """네이버 금융 메인의 '주요 뉴스' 5개를 긁어옵니다."""
    headlines = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get("https://finance.naver.com/news/mainnews.naver", headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        titles = soup.select('.articleSubject a')
        for t in titles[:5]:
            headlines.append(t.text.strip())
    except:
        pass
    return headlines

# 🔥 [업그레이드] 제목 + 요약 본문(Snippet) 동시 추출
@st.cache_data(ttl=600)
def get_naver_news(stock_name):
    """네이버 통합 검색에서 뉴스의 '제목'과 '요약 본문'을 함께 긁어옵니다."""
    news_list = []
    if not stock_name: return news_list
    
    encoded_name = urllib.parse.quote(stock_name)
    url = f"https://search.naver.com/search.naver?where=news&query={encoded_name}&sm=tab_opt&sort=0"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.naver.com/'
    }
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
        soup = BeautifulSoup(res.text, 'html.parser')
        
        news_items = soup.select('.news_wrap.api_ani_send')
        for item in news_items:
            tit_tag = item.select_one('.news_tit')
            desc_tag = item.select_one('.api_txt_lines.dsc_txt_wrap')
            
            if tit_tag:
                title = tit_tag.get('title') or tit_tag.text
                desc = desc_tag.text if desc_tag else ""
                full_text = f"제목: {title.strip()} / 핵심내용: {desc.strip()}"
                if full_text not in news_list:
                    news_list.append(full_text)
            if len(news_list) >= 5: break
            
        if not news_list:
            encoded_euckr = urllib.parse.quote(stock_name.encode('euc-kr'))
            fin_url = f"https://finance.naver.com/news/news_search.naver?q={encoded_euckr}"
            res_fin = requests.get(fin_url, headers=headers, timeout=5)
            soup_fin = BeautifulSoup(res_fin.text, 'html.parser')
            
            fin_titles = soup_fin.select('.articleSubject a')
            for t in fin_titles:
                title_text = t.get('title') or t.text
                if title_text and title_text.strip() not in news_list:
                    news_list.append(f"제목: {title_text.strip()}")
                if len(news_list) >= 5: break
                
    except Exception as e:
        pass
        
    if not news_list:
        news_list = ["현재 실시간 뉴스를 불러오지 못했습니다 (최근 특이 뉴스 없음)."]
        
    return news_list

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 시장 데이터를 집계 중입니다.")
else:
    df_summary['현재_순위'] = df_summary['AI수급점수'].rank(method='first', ascending=False).astype(int)
    
    if os.path.exists("score_trend.csv"):
        df_trend = pd.read_csv("score_trend.csv")
        dates = sorted(df_trend['날짜'].unique(), reverse=True)
        if len(dates) >= 2:
            yday_data = df_trend[df_trend['날짜'] == dates[1]][['종목명', '순위']]
            yday_data.columns = ['종목명', '전일_순위']
            df_summary = pd.merge(df_summary, yday_data, on='종목명', how='left')
            df_summary['전일_순위'] = df_summary['전일_순위'].fillna(df_summary['현재_순위'])
            df_summary['랭킹추세'] = (df_summary['전일_순위'] - df_summary['현재_순위']).apply(lambda x: f"▲ {int(x)}" if x > 0 else (f"▼ {abs(int(x))}" if x < 0 else "-"))
        else: df_summary['랭킹추세'] = "-"
    else: df_summary['랭킹추세'] = "-"

    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["🌍 매크로 인사이트", "🗺️ 섹터 히트맵", "📊 수급 스크리너", "📈 종목 분석", "🏆 백테스트", "⚔️ 주도주 매치업"])

    # --- 탭 1: 매크로 인사이트 ---
    with tab1:
        st.subheader("📰 오늘의 Top-Down 매크로 리포트")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()

            if is_vip:
                st.markdown(report_content)
            else:
                teaser_text = report_content[:250] + "...\n\n"
                st.markdown(teaser_text)
                show_premium_paywall("심층 매크로 분석 리포트 전문은 코드 인증 후 확인할 수 있습니다.")
        else: st.info("⏳ AI 매크로 리포트를 생성 중입니다.")

    # --- 탭 2: 섹터 히트맵 ---
    with tab2:
        st.subheader("🗺️ 시가총액 & 수급 섹터 히트맵")
        st.caption("사각형의 크기는 '시가총액', 색상은 '당일 등락률'을 나타냅니다. 어느 섹터에 돈이 몰리는지 한눈에 파악하세요.")

        if not is_vip:
            show_premium_paywall("전체 시장의 섹터별 자금 흐름 히트맵은 코드 인증 후 확인할 수 있습니다.")
        else:
            if not df_summary.empty:
                df_hm = df_summary.copy()
                df_hm['섹터'] = df_hm['섹터'].fillna("기타")
                df_hm['시가총액'] = pd.to_numeric(df_hm['시가총액'], errors='coerce').fillna(0)
                df_hm['등락률'] = pd.to_numeric(df_hm['등락률'], errors='coerce').fillna(0)

                fig = px.treemap(
                    df_hm,
                    path=[px.Constant("국내 증시 주요 섹터"), '섹터', '종목명'],
                    values='시가총액',
                    color='등락률',
                    color_continuous_scale=['#0066FF', '#1E1E2E', '#FF3333'], 
                    color_continuous_midpoint=0,
                    custom_data=['등락률', 'AI수급점수']
                )

                fig.update_traces(
                    texttemplate="<b>%{label}</b><br>%{customdata[0]:.2f}%",
                    hovertemplate="<b>%{label}</b><br>시가총액: %{value:,.0f}억<br>등락률: %{customdata[0]:.2f}%<br>AI점수: %{customdata[1]}점<extra></extra>",
                    textfont_color="white"
                )
                fig.update_layout(
                    margin=dict(t=30, l=10, r=10, b=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    height=550,
                    coloraxis_showscale=False
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("데이터 대기 중입니다.")

    # --- 탭 3: 수급 스크리너 ---
    with tab3:
        if "view_mode" not in st.session_state:
            st.session_state.view_mode = "card"
            
        col_v1, col_v2, col_v3 = st.columns([1, 1, 2])
        with col_v1:
            if st.button("📱 모바일 카드 뷰", use_container_width=True, type="primary" if st.session_state.view_mode == "card" else "secondary"):
                st.session_state.view_mode = "card"
                st.rerun()
        with col_v2:
            if st.button("📊 데이터 표 뷰", use_container_width=True, type="primary" if st.session_state.view_mode == "table" else "secondary"):
                st.session_state.view_mode = "table"
                st.rerun()
        
        st.markdown("<div style='margin-bottom: 15px;'></div>", unsafe_allow_html=True)
        
        df_display = df_summary if is_vip else df_summary.head(5)

        if st.session_state.view_mode == "card":
            st.caption("✨ 직관적인 모바일 카드 뷰입니다. 상세 분석은 '종목 분석' 탭의 검색창을 이용해주세요.")
            
            html_lines = []
            for idx, row in df_display.iterrows():
                rank = int(row['현재_순위'])
                name = row['종목명']
                sector = safe_get(row, '섹터', '분류안됨')
                price = f"{safe_get(row, '현재가', 0):,.0f}"
                chg = float(safe_get(row, '등락률', 0))
                chg_color = "#FF4B4B" if chg > 0 else "#1C83E1" if chg < 0 else "#AAAAAA"
                chg_str = f"▲ {chg:.2f}%" if chg > 0 else f"▼ {abs(chg):.2f}%" if chg < 0 else "0.00%"
                ai_score = int(safe_get(row, 'AI수급점수', 0))
                rank_chg = safe_get(row, '랭킹추세', '-')
                f_str = f"{float(safe_get(row, '외인강도(%)', 0)):.1f}%"
                p_str = f"{float(safe_get(row, '연기금강도(%)', 0)):.1f}%"
                
                rc_color = "#FF4B4B" if "▲" in str(rank_chg) else ("#1C83E1" if "▼" in str(rank_chg) else "#888888")
                
                card_html = f"""
<div style="background-color: #1E1E2E; padding: 16px; border-radius: 12px; margin-bottom: 12px; border: 1px solid #333; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; gap: 10px;">
<div style="display: flex; flex-direction: column; gap: 8px;">
<div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
<span style="background: #2b2b36; border: 1px solid #444; color: #FFD700; font-size: 0.7em; font-weight: 800; padding: 4px 8px; border-radius: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.3); white-space: nowrap;">🏆 {rank}위</span>
<span style="font-size: 0.8em; font-weight: bold; color: {rc_color}; white-space: nowrap;">{rank_chg}</span>
<span style="font-size: 1.15em; font-weight: 800; color: #FFF; line-height: 1.2;">{name}</span>
</div>
<div><span style="font-size: 0.75em; color: #AAA; padding: 3px 6px; background: #2A2A35; border-radius: 4px;">{sector}</span></div>
</div>
<div style="text-align: right; min-width: 80px;">
<div style="font-size: 1.1em; font-weight: 700; color: #FFF;">{price}원</div>
<div style="font-size: 0.9em; font-weight: 800; color: {chg_color};">{chg_str}</div>
</div>
</div>
<div style="display: flex; justify-content: space-between; font-size: 0.85em; color: #DDD; background: #181825; padding: 10px; border-radius: 8px; align-items: center; flex-wrap: wrap; gap: 8px;">
<div>⚡ AI점수: <b style="color:#FFD700; font-size: 1.1em;">{ai_score}점</b></div>
<div>🔴외인 <b style="color:#FF4B4B;">{f_str}</b> <span style="color:#444;">|</span> 🔵기금 <b style="color:#1C83E1;">{p_str}</b></div>
</div>
</div>
"""
                html_lines.append(card_html.strip())
            
            cards_container_html = f"<div style='padding: 5px;'>{''.join(html_lines)}</div>"
            st.markdown(cards_container_html, unsafe_allow_html=True)
            
            if not is_vip:
                show_premium_paywall("6위부터 20위까지의 숨겨진 AI 쏠림 주도주를 확인하세요.")
                
        else:
            col_t1, col_t2 = st.columns([0.6, 0.4])
            with col_t2:
                show_advanced = st.toggle("🔍 상세 수급/지표 보기", value=False)

            def color_score(val): return f'color: {"#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"}; font-weight: bold;'
            def color_fluctuation(val):
                if pd.isna(val): return 'color: gray;'
                if isinstance(val, (int, float)): return 'color: #FF3333; font-weight: bold;' if val > 0 else ('color: #0066FF; font-weight: bold;' if val < 0 else 'color: gray;')
                return 'color: gray;'
                
            def color_momentum(val):
                if isinstance(val, str):
                    if '▲' in val: return 'color: #FF3333; font-weight: bold;'
                    elif '▼' in val: return 'color: #0066FF; font-weight: bold;'
                return 'color: gray;'

            df_display_table = df_display.set_index('종목명')

            styled_df = df_display_table.style.map(color_score, subset=['AI수급점수']).map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']).map(color_momentum, subset=['랭킹추세'])
            
            format_dict = {"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", "투신강도(%)": "{:.2f}%", "사모강도(%)": "{:.2f}%", "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%"}
            if 'PER' in df_display_table.columns: format_dict["PER"] = "{:.1f}"
            if 'ROE' in df_display_table.columns: format_dict["ROE"] = "{:.1f}%"
            styled_df = styled_df.format(format_dict)

            base_columns = ["_index", "섹터", "랭킹추세", "AI수급점수", "현재가", "등락률", "시가총액", "소속"]
            advanced_columns = ["외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속"]
            current_columns = base_columns + advanced_columns if show_advanced else base_columns

            event = st.dataframe(
                styled_df, on_select="rerun", selection_mode="single-row",
                column_config={
                    "_index": st.column_config.TextColumn("종목명", width="small"), 
                    "섹터": st.column_config.Column("테마/섹터", width="medium"), 
                    "랭킹추세": st.column_config.Column("순위변동", width="small"), 
                    "AI수급점수": st.column_config.NumberColumn("🏆 AI점수", width="small"), 
                    "현재가": st.column_config.Column("현재가(원)", width="small"), 
                    "등락률": st.column_config.Column("등락(%)", width="small"), 
                    "외인강도(%)": st.column_config.Column("외인(1M)", width="small"), 
                    "연기금강도(%)": st.column_config.Column("연기금(1M)", width="small"), 
                    "이격도(%)": st.column_config.Column("이격도(20D)", width="small"), 
                    "손바뀜(%)": st.column_config.Column("손바뀜(5D)", width="small"), 
                    "투신강도(%)": st.column_config.Column("투신(1M)", width="small"), 
                    "사모강도(%)": st.column_config.Column("사모(1M)", width="small"), 
                    "외인연속": st.column_config.NumberColumn("외인연속", format="%d일", width="small"), 
                    "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일", width="small"), 
                    "시가총액": st.column_config.Column("시총(억)", width="small"), 
                    "소속": st.column_config.Column("시장", width="small")
                },
                column_order=current_columns,
                hide_index=False, use_container_width=True, height=250 if not is_vip else 600
            )
            if event.selection.rows: 
                selected_name = df_display_table.iloc[event.selection.rows[0]].name
                st.session_state.selected_stock = selected_name
                st.session_state.stock_selector = selected_name
                st.rerun()

            if not is_vip:
                show_premium_paywall("6위부터 20위까지의 숨겨진 AI 쏠림 주도주를 확인하세요.")

    # --- 탭 4: 종목 분석 (네이버 통합 검색 뉴스 + 매크로 시황 융합 분석) ---
    with tab4:
        free_tier_stocks = df_summary.head(5)['종목명'].values
        stock_list = df_summary['종목명'].tolist()
        
        if "selected_stock" not in st.session_state or st.session_state.selected_stock not in stock_list:
            st.session_state.selected_stock = stock_list[0]
            
        if "stock_selector" not in st.session_state or st.session_state.stock_selector not in stock_list:
            st.session_state.stock_selector = st.session_state.selected_stock

        def on_stock_change():
            st.session_state.selected_stock = st.session_state.stock_selector

        st.selectbox(
            "🔍 분석할 종목을 검색/선택하세요",
            options=stock_list,
            key="stock_selector",
            on_change=on_stock_change
        )
        target_stock = st.session_state.stock_selector
        
        selected_row = df_summary[df_summary['종목명'] == target_stock].iloc[0]
        
        sector_name = safe_get(selected_row, '섹터', '분류안됨')
        cur_rank = safe_get(selected_row, '현재_순위', 0)
        ai_score = safe_get(selected_row, 'AI수급점수', 0)
        rank_trend = safe_get(selected_row, '랭킹추세', '-')
        marcap = safe_get(selected_row, '시가총액', 0)
        per_val = safe_get(selected_row, 'PER', 0.0)
        roe_val = safe_get(selected_row, 'ROE', 0.0)
        gap_20 = safe_get(selected_row, '이격도(%)', 100)
        target_code = safe_get(selected_row, '종목코드', '')

        st.markdown(
            f"""
            <div class="stock-title-wrap">
                <h2 style="margin: 0; color: #FFFFFF;">💡 {target_stock}</h2>
                <span class="stock-sector-chip">{sector_name}</span>
            </div>
            """,
            unsafe_allow_html=True
        )

        if not is_vip and target_stock not in free_tier_stocks:
            show_premium_paywall(f"'{target_stock}'의 상세 수급 분석과 차트는 코드 인증 후 확인할 수 있습니다.")
        else:
            tech_status = "🟢최적 매수" if 101 <= gap_20 <= 108 else ("🔴리스크 관리" if gap_20 < 95 else "⚫추세 추종")
            f_str_val = float(safe_get(selected_row, '외인강도(%)', 0))
            p_str_val = float(safe_get(selected_row, '연기금강도(%)', 0))
            t_str_val = float(safe_get(selected_row, '투신강도(%)', 0))
            pef_str_val = float(safe_get(selected_row, '사모강도(%)', 0))
            f_streak = int(safe_get(selected_row, '외인연속', 0))
            p_streak = int(safe_get(selected_row, '연기금연속', 0))

            st.markdown(
                f"""
                <div class="stock-grid">
                    <div class="stock-card">
                        <div class="stock-label">🏆 AI 점수</div>
                        <div class="stock-value">{int(ai_score)}점</div>
                        <div class="stock-sub">전체 {int(cur_rank)}위 / {rank_trend}</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">💰 시가총액</div>
                        <div class="stock-value">{marcap:,.0f}억</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">📊 PER / ROE</div>
                        <div class="stock-value">{per_val:.1f} / {roe_val:.1f}%</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">📈 20일선 이격도</div>
                        <div class="stock-value">{gap_20}%</div>
                        <div class="stock-sub">{tech_status}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
            st.markdown("##### 🛒 최근 1개월 수급 강도")
            st.markdown(
                f"""
                <div class="stock-grid">
                    <div class="stock-card">
                        <div class="stock-label">🔴 외인 강도</div>
                        <div class="stock-value">{f_str_val:.1f}%</div>
                        <div class="stock-sub">{f_streak}일 연속 순매수</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">🔵 연기금 강도</div>
                        <div class="stock-value">{p_str_val:.1f}%</div>
                        <div class="stock-sub">{p_streak}일 연속 순매수</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">🟡 투신 강도</div>
                        <div class="stock-value">{t_str_val:.1f}%</div>
                    </div>
                    <div class="stock-card">
                        <div class="stock-label">🟣 사모 강도</div>
                        <div class="stock-value">{pef_str_val:.1f}%</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )

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

            st.markdown("##### 🤖 DeepAlpha 실시간 종목 진단")
            st.caption("최신 시황 뉴스와 종목 데이터를 바탕으로 AI가 종목별 핵심 포인트를 정리합니다.")

            if st.button(f"✨ '{target_stock}' 뉴스/시황 기반 심층 리포트 생성", use_container_width=True):
                if not client:
                    st.error("AI용 API 키가 설정되지 않았습니다.")
                else:
                    with st.spinner(f"네이버 금융(거시 시황) 및 통합검색(종목 요약)에서 팩트 데이터를 파싱하고 있습니다..."):
                        macro_news = get_macro_headline_news()
                        news_list = get_naver_news(target_stock)
                        
                        st.markdown("###### 📡 수집된 팩트 데이터")
                        with st.expander("파싱된 최신 시황 및 종목 뉴스 원본 보기"):
                            st.write("**[오늘의 주요 시황 뉴스]**")
                            for mn in macro_news: st.caption(f"- {mn}")
                            if not macro_news: st.caption("시황 뉴스가 없습니다.")
                            
                            st.write("**[최신 통합 검색 뉴스 (종목 요약)]**")
                            for n in news_list: st.caption(f"- {n}")
                            if not news_list: st.caption("최근 종목 뉴스가 없습니다.")
                        
                        today_str = datetime.now().strftime("%Y년 %m월 %d일")
                        
                        prompt = f"""
                        너는 국내 주식시장을 분석하는 수석 퀀트 애널리스트야. 오늘은 {today_str}이야.
                        내가 제공하는 아래의 [팩트 데이터]만을 기반으로 종목명 '{target_stock}'(섹터: {sector_name})에 대한 심층 브리핑을 작성해.
                        인터넷 검색을 시도하지 말고 오직 제공된 텍스트만 활용해. 주요 시황 뉴스를 통해 현재 시장의 분위기를 파악하고, 이것이 해당 종목에 미칠 영향을 반드시 연계해서 분석해.
                        
                        [팩트 데이터: 수급 및 펀더멘털]
                        - PER: {per_val:.1f}, ROE: {roe_val:.1f}%
                        - 20일선 이격도: {gap_20}%
                        - 외국인 강도: {f_str_val:.1f}% (연속 {f_streak}일)
                        - 연기금 강도: {p_str_val:.1f}% (연속 {p_streak}일)
                        
                        [팩트 데이터: 오늘의 거시 경제/시황 주요 뉴스]
                        {chr(10).join(macro_news) if macro_news else "시황 뉴스 없음"}
                        
                        [팩트 데이터: 네이버 최신 뉴스 검색결과 (제목 및 본문 요약)]
                        {chr(10).join(news_list) if news_list else "최신 종목 뉴스 없음"}
                        
                        [출력 양식]
                        1. 📰 최신 모멘텀 요약 (종목 뉴스 요약본 기반 구체적 분석)
                        2. 🌍 매크로 시황 연계 분석 (거시 경제 주요 뉴스와 종목의 연관성 및 방향성)
                        3. 💡 수급 및 펀더멘털 평가 (PER, ROE, 기관/외인 수급 해석)
                        4. 🎯 단기 투자 전략 및 리스크 관리 (시장 분위기와 이격도를 종합적으로 고려)
                        """
                        try:
                            response = client.models.generate_content_stream(
                                model='gemma-4-31b-it',
                                contents=prompt
                            )

                            st.success("✅ AI 분석이 완료되었습니다.")
                            def stream_generator():
                                for chunk in response:
                                    if chunk.text: yield chunk.text

                            with st.container():
                                st.write_stream(stream_generator)

                        except Exception as e:
                            st.error(f"분석 중 오류 발생: {e}")

    # --- 탭 5: 백테스트 ---
    with tab5:
        st.subheader("🏆 DeepAlpha 모델 가상 포트폴리오 백테스트")
        if not is_vip:
            show_premium_paywall("가상 포트폴리오 누적 수익률 및 성과 분석은 코드 인증 후 확인할 수 있습니다.")
        else:
            if os.path.exists("performance_trend.csv"):
                df_perf = pd.read_csv("performance_trend.csv")
                if not df_perf.empty:
                    df_perf['날짜_dt'] = pd.to_datetime(df_perf['날짜'])
                    min_date = df_perf['날짜_dt'].min().date()
                    max_date = df_perf['날짜_dt'].max().date()
                    
                    selected_start_date = st.date_input("🗓️ 벤치마크 시작(기준)일 선택", min_value=min_date, max_value=max_date, value=min_date)
                    df_filtered = df_perf[df_perf['날짜_dt'].dt.date >= selected_start_date].copy()
                    
                    if not df_filtered.empty:
                        base_port_ret = df_filtered.iloc[0]['누적수익률']
                        df_filtered['조정_포트수익률'] = df_filtered['누적수익률'] - base_port_ret
                        
                        try:
                            kospi_hist = yf.Ticker('^KS11').history(period="1y")
                            kospi_hist.index = kospi_hist.index.tz_localize(None).normalize()
                            
                            base_k_df = kospi_hist[kospi_hist.index <= pd.to_datetime(selected_start_date)]
                            base_k = float(base_k_df['Close'].iloc[-1]) if not base_k_df.empty else None
                            
                            kospi_rets = []
                            for d in df_filtered['날짜_dt']:
                                k_sub = kospi_hist[kospi_hist.index <= d]
                                if not k_sub.empty and base_k is not None:
                                    val = float(k_sub['Close'].iloc[-1])
                                    ret = ((val - base_k) / base_k) * 100
                                    kospi_rets.append(ret)
                                else:
                                    kospi_rets.append(0)
                            df_filtered['KOSPI 누적수익률'] = kospi_rets
                        except:
                            df_filtered['KOSPI 누적수익률'] = 0
                        
                        col_b1, col_b2 = st.columns(2)
                        current_port_ret = df_filtered['조정_포트수익률'].iloc[-1]
                        current_kospi_ret = df_filtered['KOSPI 누적수익률'].iloc[-1]
                        
                        if len(df_filtered) > 1:
                            port_daily_diff = df_filtered['조정_포트수익률'].iloc[-1] - df_filtered['조정_포트수익률'].iloc[-2]
                            kospi_daily_diff = df_filtered['KOSPI 누적수익률'].iloc[-1] - df_filtered['KOSPI 누적수익률'].iloc[-2]
                        else:
                            port_daily_diff = 0.0
                            kospi_daily_diff = 0.0

                        col_b1.metric(label="🏆 DeepAlpha 누적 수익률", value=f"{current_port_ret:+.2f}%", delta=f"{port_daily_diff:.2f}")
                        col_b2.metric(label="📉 KOSPI 누적 수익률", value=f"{current_kospi_ret:+.2f}%", delta=f"{kospi_daily_diff:.2f}")
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        df_filtered['날짜_표시'] = df_filtered['날짜_dt'].dt.strftime('%m/%d')
                        df_melt = df_filtered.melt(id_vars=['날짜_표시'], value_vars=['조정_포트수익률', 'KOSPI 누적수익률'], var_name='포트폴리오', value_name='수익률(%)')
                        
                        base_chart = alt.Chart(df_melt).mark_line(point=True).encode(
                            x=alt.X('날짜_표시:O', axis=alt.Axis(title=None, labelAngle=-45)),
                            y=alt.Y('수익률(%):Q', title="누적 수익률 (%)"),
                            color=alt.Color('포트폴리오:N', scale=alt.Scale(domain=['조정_포트수익률', 'KOSPI 누적수익률'], range=['#E74C3C', '#AAAAAA']), legend=alt.Legend(title=None, orient='bottom'))
                        ).properties(height=300)

                        st.altair_chart(base_chart, use_container_width=True)

                        if os.path.exists("score_trend.csv"):
                            df_rank = pd.read_csv("score_trend.csv")
                            if not df_rank.empty and {"날짜", "종목명", "순위"}.issubset(df_rank.columns):
                                df_rank = df_rank[df_rank["순위"].isin([1, 2, 3])].copy()
                                df_rank["날짜"] = df_rank["날짜"].astype(str)
                                selected_dates = set(df_filtered["날짜_dt"].dt.strftime("%Y-%m-%d").tolist())
                                df_rank = df_rank[df_rank["날짜"].isin(selected_dates)]

                                rank_rows = []
                                for dt in sorted(df_rank["날짜"].unique(), reverse=True):
                                    day_slice = df_rank[df_rank["날짜"] == dt].sort_values("순위")
                                    day_returns = []
                                    day_data = {"날짜": dt}

                                    for rank_no in [1, 2, 3]:
                                        rank_row = day_slice[day_slice["순위"] == rank_no]
                                        if rank_row.empty:
                                            day_data[f"{rank_no}위 종목(등락률)"] = "-"
                                            continue
                                        stock_name = rank_row.iloc[0]["종목명"]
                                        day_ret = resolve_daily_return(df_history, dt, stock_name)
                                        if day_ret is not None:
                                            day_returns.append(day_ret)
                                        day_data[f"{rank_no}위 종목(등락률)"] = f"{stock_name} ({format_pct(day_ret)})" if day_ret is not None else f"{stock_name} (-)"

                                    perf_row = df_filtered[df_filtered["날짜_dt"].dt.strftime("%Y-%m-%d") == dt]
                                    if perf_row.empty:
                                        continue
                                    day_data["Top3 평균 등락률"] = format_pct(sum(day_returns) / len(day_returns)) if day_returns else "-"
                                    day_data["포트폴리오 누적수익률"] = f"{float(perf_row.iloc[0]['조정_포트수익률']):+.2f}%"
                                    day_data["KOSPI 누적수익률"] = f"{float(perf_row.iloc[0]['KOSPI 누적수익률']):+.2f}%"
                                    rank_rows.append(day_data)

                                if rank_rows:
                                    st.markdown("##### 📋 날짜별 Top3 구성 종목 및 성과")
                                    st.dataframe(pd.DataFrame(rank_rows), hide_index=True, use_container_width=True)
                    else:
                        st.info("선택하신 날짜에 해당하는 백테스트 데이터가 없습니다.")
                else: st.info("⏳ 데이터 대기 중")
            else: st.info("⏳ 데이터 대기 중")

    # --- 탭 6: 주도주 매치업 (신설) ---
    with tab6:
        st.subheader("⚔️ 주도주 AI 비교 분석 (매치업)")
        st.caption("선택한 종목들의 뉴스/시황/퀀트 데이터를 종합해 단기 관점의 상대 우위를 비교합니다.")

        if not is_vip:
            show_premium_paywall("AI 기반 다중 종목 비교 분석 기능은 코드 인증 후 이용할 수 있습니다.")
        else:
            if not client:
                st.error("⚠️ Streamlit Secrets에 GEMINI_API_KEY가 설정되지 않아 AI 매치업을 사용할 수 없습니다.")
            else:
                stock_list_full = df_summary['종목명'].tolist()
                matchup_stocks = st.multiselect("비교할 종목을 2~3개 선택하세요", options=stock_list_full, max_selections=3)
                
                if len(matchup_stocks) > 1:
                    if st.button("🚀 AI 매치업 시작", use_container_width=True, type="primary"):
                        with st.spinner("선택된 종목들의 최신 데이터와 매크로 시황을 긁어오고 있습니다..."):
                            macro_news = get_macro_headline_news()
                            matchup_data = []
                            for ms in matchup_stocks:
                                s_row = df_summary[df_summary['종목명'] == ms].iloc[0]
                                n_news = get_naver_news(ms)
                                
                                matchup_data.append(f"""
                                === [후보 종목: {ms}] ===
                                - 섹터: {safe_get(s_row, '섹터', '분류안됨')} / AI점수: {safe_get(s_row, 'AI수급점수', 0)}점
                                - 이격도: {safe_get(s_row, '이격도(%)', 100)}% / 외국인연속: {safe_get(s_row, '외인연속', 0)}일 / 연기금연속: {safe_get(s_row, '연기금연속', 0)}일
                                - 최근 뉴스 (요약 포함): {chr(10).join(n_news[:3]) if n_news else '없음'}
                                """)
                            
                            combined_data_str = "\n".join(matchup_data)
                            
                            prompt = f"""
                            너는 수석 퀀트 애널리스트야. 내가 아래에 제공한 {len(matchup_stocks)}개 종목의 [후보 종목 데이터]를 꼼꼼히 비교 분석해.
                            또한, [오늘의 주요 시황 뉴스]를 바탕으로 글로벌 매크로 환경을 고려했을 때, 현재 단기 스윙(눌림목 및 수급 모멘텀) 관점에서 어떤 종목이 가장 유리한지 승자를 명확히 판정하고 그 이유를 설명해.
                            
                            [팩트 데이터: 오늘의 주요 시황 뉴스]
                            {chr(10).join(macro_news) if macro_news else "시황 뉴스 없음"}
                            
                            [후보 종목 데이터]
                            {combined_data_str}
                            
                            다음 양식으로 답변해줘:
                            🏆 **최종 승자**: (종목명)
                            🔍 **선정 이유**: (수급, 이격도, 종목 뉴스 요약 내용, 시황을 종합하여 3~4줄로 핵심만 요약)
                            ⚖️ **탈락 종목 코멘트**: (왜 승자보다 아쉬운지 짧게 분석)
                            """
                            
                            try:
                                response = client.models.generate_content_stream(
                                    model='gemma-4-31b-it',
                                    contents=prompt
                                )
                                st.success("✅ 매치업 판정 완료!")
                                def stream_generator():
                                    for chunk in response:
                                        if chunk.text: yield chunk.text
                                st.write_stream(stream_generator)
                            except Exception as e:
                                st.error(f"분석 중 오류 발생: {e}")
                else:
                    st.info("비교 분석을 위해 최소 2개의 종목을 선택해주세요.")

