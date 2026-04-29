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
import plotly.graph_objects as go
import urllib.parse
import re
import json
import base64
from textwrap import dedent
from email.utils import parsedate_to_datetime
from db_utils import read_table, write_table, migrate_csv_to_sqlite_once, table_exists, csv_exists, resolve_csv_path, DATA_DIR
from news_utils import (
    normalize_text as _normalize_text,
    extract_source as _extract_source,
    event_tags as _event_tags,
    title_signature as _title_signature,
    is_similar_title as _is_similar_title,
    score_news_candidate as _score_news_candidate_base,
)

BRAND_LOGO_PATH = "assets/brand/q_edge_cut.png"
PAGE_ICON = BRAND_LOGO_PATH if os.path.exists(BRAND_LOGO_PATH) else "Q"

st.set_page_config(layout="wide", page_title="QEdge", page_icon=PAGE_ICON)


def _logo_data_uri(path):
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return ""

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
ADMIN_CODE = st.secrets.get("ADMIN_CODE", "MASTER2026")
st.sidebar.markdown("## 접근 코드 인증")
st.sidebar.caption("공유받은 코드를 입력하면 전체 주도주와 상세 분석 데이터를 볼 수 있습니다.")
user_code = st.sidebar.text_input("접근 코드 입력", type="password")

is_admin = (user_code == ADMIN_CODE)
is_vip = (user_code == VIP_CODE) or is_admin

if is_vip:
    st.sidebar.success("코드 인증이 완료되었습니다. 전체 데이터를 확인할 수 있습니다.")
else:
    st.sidebar.info("현재 공개 화면만 표시 중입니다. 코드를 입력하면 전체 화면이 열립니다.")
if is_admin:
    st.sidebar.success("관리자 모드가 활성화되었습니다.")

ui_fx_mode = "시그니처"
logo_uri = _logo_data_uri(BRAND_LOGO_PATH)

db_ready = table_exists("data")
csv_ready = csv_exists("data.csv")
source_badge = "S" if db_ready else ("C" if csv_ready else "N")
logo_html = f'<img class="qe-brand-head-img" src="{logo_uri}" alt="QEdge logo"/>' if logo_uri else ""
st.markdown(
    f"""
    <div class="qe-brand-head-wrap">
        {logo_html}
        <h1 class="qe-brand-wordmark"><span class="qe-brand-word-q">Q</span><span class="qe-brand-word-edge">Edge</span></h1>
        <span style="background:#172033; color:#AFC2E8; border:1px solid #2E3A55; border-radius:999px; padding:2px 8px; font-size:0.74em;">
            {source_badge}
        </span>
    </div>
    """,
    unsafe_allow_html=True,
)
if logo_uri:
    st.sidebar.markdown(
        f"""
        <div class="qe-sidebar-brand-footer">
            <img class="qe-brand-side-img" src="{logo_uri}" alt="QEdge logo"/>
            <div class="qe-brand-side-wordmark"><span class="qe-brand-word-q">Q</span><span class="qe-brand-word-edge">Edge</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
st.caption("수급·뉴스·매크로를 한 화면에서 보는 퀀트 대시보드")
st.markdown(
    "<div style='margin-bottom:4px;'></div>",
    unsafe_allow_html=True,
)

# --- AI API 설정 ---
gemini_key = st.secrets.get("GEMINI_API_KEY", os.environ.get("GEMINI_API_KEY"))

if gemini_key:
    client = genai.Client(api_key=gemini_key)
else: client = None

@st.cache_data(ttl=1800)
def get_macro_data():
    tickers = {"🇰🇷 KOSPI": "^KS11", "🇰🇷 KOSDAQ": "^KQ11", "🇺🇸 S&P500": "^GSPC", "🇺🇸 NASDAQ": "^IXIC", "💵 환율": "KRW=X", "🛢️ WTI유": "CL=F", "📉 미 국채(10y)": "^TNX", "😨 VIX": "^VIX"}
    macro_info = {}

    def _fetch_chart_close_pair(symbol):
        """Yahoo Chart API 직접 호출로 최근 종가 2개를 가져옵니다."""
        try:
            encoded_symbol = urllib.parse.quote(symbol, safe="")
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}?range=10d&interval=1d"
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
            if res.status_code != 200:
                return None
            payload = res.json()
            result = ((payload or {}).get("chart") or {}).get("result") or []
            if not result:
                return None
            quote = (((result[0].get("indicators") or {}).get("quote") or [{}])[0])
            closes = [float(x) for x in (quote.get("close") or []) if x is not None]
            if len(closes) < 2:
                return None
            return closes[-1], closes[-2]
        except Exception:
            return None

    for name, ticker in tickers.items():
        try:
            # 1) direct API 우선 (환경별 yfinance 경고/빈응답 이슈 회피)
            pair = _fetch_chart_close_pair(ticker)
            if pair is None:
                # 2) fallback: yfinance
                t = yf.Ticker(ticker)
                hist = t.history(period="5d")
                if len(hist) >= 2:
                    pair = (float(hist['Close'].iloc[-1]), float(hist['Close'].iloc[-2]))

            if pair is not None and pair[1] != 0:
                current, prev = pair
                macro_info[name] = {"value": current, "change": current - prev, "change_pct": ((current - prev) / prev) * 100}
            else:
                macro_info[name] = None
        except Exception as e:
            print(f"[WARN] 매크로 카드 지표 수집 실패({name}/{ticker}): {e}")
            macro_info[name] = None
    return macro_info

macro_data = get_macro_data()
def render_macro_cards(ticker_names):
    cards = []
    for name in ticker_names:
        data = macro_data.get(name)
        if data:
            color = "#FF4B4B" if data['change'] > 0 else "#3B82F6" if data['change'] < 0 else "#9CA3AF"
            arrow = "▲" if data['change'] > 0 else "▼" if data['change'] < 0 else "-"
            val_str = f"{data['value']:,.1f}원" if "환율" in name else (f"{data['value']:.2f}" if "국채" in name or "VIX" in name else f"{data['value']:,.2f}")
            cards.append(f"<div class='macro-card'><div class='macro-label'>{name}</div><div class='macro-value'>{val_str}</div><div class='macro-change' style='color:{color};'>{arrow} {abs(data['change_pct']):.2f}%</div></div>")
        else:
            cards.append(f"<div class='macro-card'><div class='macro-label'>{name}</div><div class='macro-value'>-</div><div class='macro-change' style='color:#6B7280;'>데이터 지연</div></div>")
    return f"<div class='macro-strip'>{''.join(cards)}</div>"

st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button {
      border-radius: 10px !important;
      border: 1px solid #334155 !important;
      background: linear-gradient(145deg, #172033, #111827) !important;
      color: #E5E7EB !important;
      font-weight: 700 !important;
      transition: all .14s ease !important;
    }
    div[data-testid="stButton"] > button:hover {
      border-color: #475569 !important;
      box-shadow: 0 8px 20px rgba(0,0,0,0.24) !important;
      transform: translateY(-1px);
    }
    div[data-testid="stButton"] > button:focus:not(:active) {
      border-color: #60A5FA !important;
      box-shadow: 0 0 0 1px rgba(96,165,250,.55) !important;
    }
    [data-testid="stToggle"] label { color:#D1D5DB !important; font-weight:600 !important; }
    [data-testid="stRadio"] label { color:#CBD5E1 !important; }
    [data-testid="stRadio"] div[role="radiogroup"] > label {
      background: linear-gradient(145deg, #111827, #0f172a);
      border: 1px solid #334155;
      border-radius: 999px;
      padding: 6px 10px;
      margin-right: 8px;
      transition: all .14s ease;
    }
    [data-testid="stRadio"] div[role="radiogroup"] > label:hover {
      border-color: #475569;
      box-shadow: 0 8px 18px rgba(0,0,0,0.2);
    }
    :root {
      --qe-bg-1: #171A24;
      --qe-bg-2: #151A25;
      --qe-bg-3: #181825;
      --qe-border: #2B3242;
      --qe-border-soft: #2A3242;
      --qe-text-main: #F5F7FA;
      --qe-text-sub: #AAB2C5;
      --qe-shadow: 0 10px 26px rgba(0,0,0,0.26);
      --qe-radius-sm: 10px;
      --qe-radius-md: 12px;
    }
    .macro-strip { display:flex; gap:8px; overflow-x:auto; margin-bottom: 8px; padding-bottom:2px; -webkit-overflow-scrolling: touch; }
    .macro-strip::-webkit-scrollbar { display:none; }
    .qe-brand-head-wrap { display:flex; align-items:center; gap:10px; margin-bottom:4px; }
    .qe-brand-head-img {
      width:46px;
      height:46px;
      object-fit:contain;
      object-position:left center;
      filter:none;
      padding:0;
      margin:0;
    }
    .qe-brand-wordmark {
      margin:0;
      line-height:1;
      color:#F8FAFC;
      font-family:"Inter","Pretendard","Segoe UI","Noto Sans KR",sans-serif;
      letter-spacing:0.01em;
      font-size:3.0rem;
    }
    .qe-brand-word-q { font-weight:900; }
    .qe-brand-word-edge { font-weight:500; }
    .qe-brand-side-wrap { display:flex; justify-content:center; margin:4px 0 10px 0; }
    .qe-brand-side-img {
      width:26px;
      height:26px;
      object-fit:contain;
      filter:none;
      padding:0;
    }
    .qe-sidebar-brand-footer {
      position:fixed;
      left:16px;
      bottom:14px;
      display:flex;
      align-items:center;
      gap:7px;
      opacity:0.92;
      pointer-events:none;
      z-index:1000;
    }
    .qe-brand-side-wordmark {
      color:#CFD7E6;
      font-family:"Inter","Pretendard","Segoe UI","Noto Sans KR",sans-serif;
      font-size:1.02rem;
      letter-spacing:0.01em;
      line-height:1;
    }
    .macro-card {
      background:linear-gradient(140deg, var(--qe-bg-2), #111827);
      border:1px solid var(--qe-border-soft);
      border-radius:var(--qe-radius-sm);
      padding:8px 10px;
      min-width:120px;
      flex:0 0 auto;
      transition:transform .15s ease, border-color .15s ease, box-shadow .15s ease;
    }
    .macro-card:hover { transform:translateY(-1px); border-color:#41516E; box-shadow:var(--qe-shadow); }
    .macro-label { color:#A7B0C2; font-size:0.74em; margin-bottom:4px; white-space: nowrap; overflow:hidden; text-overflow: ellipsis; }
    .macro-value { color:var(--qe-text-main); font-size:0.98em; font-weight:700; line-height:1.2; }
    .macro-change { font-size:0.8em; font-weight:700; margin-top:3px; }
    @media (max-width: 900px) {
      .macro-card { min-width:108px; padding:7px 9px; }
      .macro-label { font-size:0.7em; }
      .macro-value { font-size:0.9em; }
      .macro-change { font-size:0.75em; }
      .qe-brand-head-img { width:40px; height:40px; }
      .qe-brand-wordmark { font-size:2.4rem; }
      .qe-sidebar-brand-footer { left:12px; bottom:10px; }
      .qe-brand-side-img { width:22px; height:22px; }
      .qe-brand-side-wordmark { font-size:0.92rem; }
    }
    </style>
    """,
    unsafe_allow_html=True
)

if ui_fx_mode == "프로":
    st.markdown(
        """
        <style>
        .podium-card-1, .podium-card-2, .podium-card-3 {
          box-shadow:0 8px 18px rgba(0,0,0,0.2) !important;
        }
        .podium-badge-1, .podium-badge-2, .podium-badge-3 {
          animation-duration:3.8s !important;
        }
        .podium-name-1, .podium-name-2, .podium-name-3 {
          animation-duration:4.8s !important;
          text-shadow:none !important;
        }
        .pf-animated-glow {
          animation-duration:4.2s !important;
          opacity:0.6;
          filter:drop-shadow(0 0 3px rgba(80,130,255,0.38));
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

core_tickers = ["🇰🇷 KOSPI", "🇰🇷 KOSDAQ", "💵 환율"]
extra_tickers = ["🛢️ WTI유", "📉 미 국채(10y)", "😨 VIX"]
st.markdown(render_macro_cards(core_tickers), unsafe_allow_html=True)
with st.expander("글로벌 지표 보기", expanded=False):
    st.markdown(render_macro_cards(["🇺🇸 S&P500", "🇺🇸 NASDAQ"]), unsafe_allow_html=True)
    st.markdown(render_macro_cards(extra_tickers), unsafe_allow_html=True)
st.markdown(
    """
    <style>
    .stock-title-wrap { display:flex; align-items:center; margin-top:5px; margin-bottom:16px; gap:8px; flex-wrap:wrap; }
    .podium-card-1 { border-color:#6A5520 !important; box-shadow:0 10px 24px rgba(0,0,0,0.24), 0 0 0 1px rgba(212,175,55,0.28), 0 0 18px rgba(212,175,55,0.18); }
    .podium-card-2 { border-color:#5A6270 !important; box-shadow:0 10px 24px rgba(0,0,0,0.24), 0 0 0 1px rgba(192,198,210,0.24), 0 0 16px rgba(192,198,210,0.14); }
    .podium-card-3 { border-color:#6D4A2C !important; box-shadow:0 10px 24px rgba(0,0,0,0.24), 0 0 0 1px rgba(184,115,51,0.26), 0 0 16px rgba(184,115,51,0.14); }
    .podium-badge-1 { background:linear-gradient(135deg,#6A5520,#3A2F13); border:1px solid #9D7A2F !important; color:#F9E3A0 !important; animation:top3PulseGold 2.2s ease-in-out infinite; }
    .podium-badge-2 { background:linear-gradient(135deg,#5E6777,#3A404B); border:1px solid #8A94A8 !important; color:#E8ECF5 !important; animation:top3PulseSilver 2.2s ease-in-out infinite; }
    .podium-badge-3 { background:linear-gradient(135deg,#6D4A2C,#3D2A1B); border:1px solid #A66A3A !important; color:#F0C5A0 !important; animation:top3PulseBronze 2.2s ease-in-out infinite; }
    .podium-name-1 {
      background:linear-gradient(100deg,#F7E7A3 0%, #D9B24C 35%, #FFF0B8 55%, #C9962A 70%, #F7E7A3 100%);
      background-size:220% auto;
      -webkit-background-clip:text;
      -webkit-text-fill-color:transparent;
      animation:goldShine 2.7s linear infinite;
      text-shadow:0 0 10px rgba(217,178,76,0.2);
    }
    .podium-name-2 {
      background:linear-gradient(100deg,#F2F6FF 0%, #C8CFDC 35%, #FFFFFF 55%, #A8B0C0 70%, #F2F6FF 100%);
      background-size:220% auto;
      -webkit-background-clip:text;
      -webkit-text-fill-color:transparent;
      animation:goldShine 2.9s linear infinite;
      text-shadow:0 0 9px rgba(190,200,220,0.2);
    }
    .podium-name-3 {
      background:linear-gradient(100deg,#F2CCAE 0%, #C07B4A 35%, #FFDABF 55%, #9A5F35 70%, #F2CCAE 100%);
      background-size:220% auto;
      -webkit-background-clip:text;
      -webkit-text-fill-color:transparent;
      animation:goldShine 3.1s linear infinite;
      text-shadow:0 0 9px rgba(184,115,51,0.2);
    }
    @keyframes goldShine {
      0% { background-position:0% 50%; }
      100% { background-position:220% 50%; }
    }
    @keyframes top3PulseGold {
      0%, 100% { box-shadow:0 0 0 rgba(217,178,76,0.0); }
      50% { box-shadow:0 0 12px rgba(217,178,76,0.28); }
    }
    @keyframes top3PulseSilver {
      0%, 100% { box-shadow:0 0 0 rgba(196,204,217,0.0); }
      50% { box-shadow:0 0 12px rgba(196,204,217,0.26); }
    }
    @keyframes top3PulseBronze {
      0%, 100% { box-shadow:0 0 0 rgba(184,115,51,0.0); }
      50% { box-shadow:0 0 12px rgba(184,115,51,0.28); }
    }
    .premium-panel {
      background:linear-gradient(140deg, #101827, #0b1220);
      border:1px solid #27324A;
      border-radius:14px;
      padding:11px 13px;
      margin:6px 0 10px 0;
      box-shadow:0 10px 28px rgba(0,0,0,0.28);
    }
    .premium-chip-row { display:flex; gap:8px; flex-wrap:wrap; margin-top:6px; }
    .premium-chip {
      background:#151f31;
      border:1px solid #2A344A;
      border-radius:999px;
      padding:4px 10px;
      font-size:0.78em;
      color:#D1D5DB;
      letter-spacing:.01em;
    }
    .hero-grid { display:grid; grid-template-columns:repeat(2,minmax(140px,1fr)); gap:10px; margin:8px 0 10px 0; }
    .hero-card {
      background:linear-gradient(145deg, #141C2E, #101827);
      border:1px solid #2C3A56;
      border-radius:12px;
      padding:10px 12px;
      box-shadow:0 8px 22px rgba(0,0,0,0.22);
    }
    .hero-label { color:#9FB0CC; font-size:0.76em; margin-bottom:4px; }
    .hero-value { color:#F8FAFC; font-size:1.4em; font-weight:800; line-height:1.15; }
    .hero-sub { color:#A7B0C2; font-size:0.8em; margin-top:4px; }
    .pf-kpi-grid { display:grid; grid-template-columns:repeat(4,minmax(120px,1fr)); gap:8px; margin:6px 0 10px 0; }
    .pf-kpi-card {
      background:linear-gradient(145deg,#131c2f,#0f1728);
      border:1px solid #2A344A;
      border-radius:12px;
      padding:9px 10px;
      box-shadow:0 8px 20px rgba(0,0,0,0.2);
      transition:transform .2s ease, border-color .2s ease, box-shadow .2s ease;
    }
    .pf-kpi-card:hover { transform:translateY(-1px); border-color:#3A4A69; box-shadow:0 10px 24px rgba(0,0,0,0.26); }
    .pf-kpi-label { color:#9FB0CC; font-size:0.74em; }
    .pf-kpi-value { color:#F8FAFC; font-size:1.12em; font-weight:800; margin-top:4px; line-height:1.15; }
    .pf-animated-card { position:relative; overflow:hidden; }
    .pf-animated-glow {
      position:absolute;
      top:0;
      left:-45%;
      width:42%;
      height:2px;
      background:linear-gradient(90deg, rgba(80,130,255,0.0), rgba(80,130,255,0.85), rgba(80,130,255,0.0));
      filter:drop-shadow(0 0 6px rgba(80,130,255,0.6));
      animation:pfGlowSweep 2.8s linear infinite;
      pointer-events:none;
    }
    @keyframes pfGlowSweep {
      0% { left:-45%; opacity:0.18; }
      20% { opacity:0.95; }
      80% { opacity:0.95; }
      100% { left:103%; opacity:0.18; }
    }
    @media (prefers-reduced-motion: reduce) {
      .pf-animated-glow { animation:none; opacity:0.45; left:0; width:100%; }
      .pf-kpi-card, .stock-card, .kpi-card { transition:none; }
      .podium-name-1, .podium-name-2, .podium-name-3,
      .podium-badge-1, .podium-badge-2, .podium-badge-3 { animation:none; }
    }
    .trend-strip { margin-top:8px; display:grid; grid-template-columns:repeat(2,minmax(160px,1fr)); gap:8px; }
    .trend-item {
      background:#101a2c;
      border:1px solid #263650;
      border-radius:10px;
      padding:7px 9px;
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:6px;
    }
    .trend-name { color:#9FB0CC; font-size:0.74em; }
    .trend-line { display:flex; align-items:center; justify-content:flex-end; min-width:100px; }
    .stock-sector-chip { background: linear-gradient(135deg, #36C06A, #1E9A52); color:white; padding:4px 12px; border-radius:20px; font-size:0.85em; font-weight:700; }
    .stock-grid { display:grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap:10px; margin:8px 0 14px 0; }
    .stock-card {
      background:linear-gradient(145deg, var(--qe-bg-3), #121826);
      border:1px solid var(--qe-border);
      border-radius:var(--qe-radius-sm);
      padding:10px 12px;
      transition:transform .15s ease, border-color .15s ease, box-shadow .15s ease;
    }
    .stock-card:hover { transform:translateY(-1px); border-color:#42506A; box-shadow:var(--qe-shadow); }
    .stock-label { color:#A8A8B3; font-size:0.78em; margin-bottom:4px; letter-spacing:.02em; }
    .stock-value { color:var(--qe-text-main); font-size:1.15em; font-weight:800; line-height:1.15; }
    .stock-sub { color:#9AA0B1; font-size:0.78em; margin-top:3px; }
    .kpi-grid { display:grid; grid-template-columns: repeat(2, minmax(170px, 1fr)); gap:10px; margin:10px 0 8px 0; }
    .kpi-card {
      background:linear-gradient(150deg, var(--qe-bg-1), #121826);
      border:1px solid var(--qe-border);
      border-radius:var(--qe-radius-md);
      padding:12px 14px;
      box-shadow:0 6px 18px rgba(0,0,0,0.18);
      transition:transform .15s ease, border-color .15s ease, box-shadow .15s ease;
    }
    .kpi-card:hover { transform:translateY(-1px); border-color:#42506A; box-shadow:var(--qe-shadow); }
    .kpi-title { color:var(--qe-text-sub); font-size:0.8em; margin-bottom:6px; letter-spacing:.02em; }
    .kpi-value { color:var(--qe-text-main); font-size:2.0em; font-weight:800; line-height:1.1; }
    .kpi-delta { font-size:0.9em; font-weight:700; margin-top:8px; display:inline-block; padding:2px 8px; border-radius:999px; }
    .kpi-meta { color:#9CA3AF; font-size:0.82em; margin-top:8px; }
    .score-kpi-grid { display:grid; grid-template-columns: repeat(3, minmax(120px, 1fr)); gap:8px; margin-top:8px; }
    .score-kpi { background:#151A25; border:1px solid #2A3242; border-radius:10px; padding:9px 10px; }
    .score-kpi-label { color:#9CA3AF; font-size:0.74em; margin-bottom:4px; }
    .score-kpi-value { color:#E5E7EB; font-size:1.35em; font-weight:800; line-height:1.15; }
    @media (max-width: 900px) {
        .premium-panel { padding:10px 11px; border-radius:12px; }
        .premium-chip { font-size:0.74em; padding:3px 9px; }
        .hero-grid { grid-template-columns:repeat(2,minmax(120px,1fr)); gap:8px; }
        .hero-value { font-size:1.2em; }
        .pf-kpi-grid { grid-template-columns:repeat(2,minmax(120px,1fr)); }
        .pf-kpi-value { font-size:1.02em; }
        .trend-strip { grid-template-columns:repeat(1,minmax(140px,1fr)); gap:6px; }
        .trend-line { min-width:88px; }
        .stock-grid { grid-template-columns: repeat(2, minmax(110px, 1fr)); }
        .kpi-grid { grid-template-columns: repeat(2, minmax(140px, 1fr)); gap:8px; }
        .kpi-value { font-size:1.8em; }
        .score-kpi-grid { grid-template-columns: repeat(3, minmax(90px, 1fr)); gap:6px; }
        .score-kpi-value { font-size:1.18em; }
    }
    </style>
    """,
    unsafe_allow_html=True
)

def load_data():
    df_summary = read_table_prefer_db("data.csv")
    df_hist = read_table_prefer_db("history.csv")
    return df_summary, df_hist


def _table_name_for(csv_path):
    base, _ = os.path.splitext(csv_path)
    return os.path.basename(base)


def read_table_prefer_db(csv_path, **kwargs):
    return read_table(
        _table_name_for(csv_path),
        csv_fallback=csv_path,
        read_csv_kwargs=kwargs,
    )


def write_table_dual(df, csv_path, **kwargs):
    write_table(
        _table_name_for(csv_path),
        df,
        csv_path=csv_path,
        csv_kwargs=kwargs,
    )

def load_score_trend_safe():
    """머지 충돌/깨진 CSV를 방어적으로 읽어 앱 크래시를 막습니다."""
    base_cols = ['날짜', '종목명', '순위']
    if not csv_exists("score_trend.csv") and not table_exists("score_trend"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("score_trend.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)

    # BOM/공백 정리
    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]

    # 충돌 마커가 컬럼으로 들어온 경우 제거
    bad_cols = [c for c in df.columns if any(x in str(c) for x in ["<<<<<<<", "=======", ">>>>>>>"])]
    if bad_cols:
        df = df.drop(columns=bad_cols, errors='ignore')

    if not set(base_cols).issubset(df.columns):
        return pd.DataFrame(columns=base_cols)

    # 충돌 마커가 데이터 행으로 들어온 경우 제거
    marker_pat = r"^(<<<<<<<|=======|>>>>>>>)"
    df = df[~df['날짜'].astype(str).str.contains(marker_pat, regex=True, na=False)]
    df = df[~df['종목명'].astype(str).str.contains(marker_pat, regex=True, na=False)]

    df['날짜'] = df['날짜'].astype(str).str.strip()
    df = df.dropna(subset=['날짜', '종목명', '순위'])
    return df

def load_performance_trend_safe():
    """머지 충돌/깨진 performance_trend.csv를 방어적으로 읽습니다."""
    base_cols = ['날짜', '일간수익률', '누적수익률']
    if not csv_exists("performance_trend.csv") and not table_exists("performance_trend"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("performance_trend.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)

    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
    bad_cols = [c for c in df.columns if any(x in str(c) for x in ["<<<<<<<", "=======", ">>>>>>>"])]
    if bad_cols:
        df = df.drop(columns=bad_cols, errors='ignore')

    if not set(base_cols).issubset(df.columns):
        return pd.DataFrame(columns=base_cols)

    marker_pat = r"^(<<<<<<<|=======|>>>>>>>)"
    df = df[~df['날짜'].astype(str).str.contains(marker_pat, regex=True, na=False)]
    for c in ['일간수익률', '누적수익률']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['날짜'])
    return df[base_cols]

def load_theme_suggestions_safe():
    base_cols = ["날짜", "종목코드", "종목명", "현재섹터", "추천테마", "신뢰도", "근거", "승인상태"]
    if not csv_exists("theme_suggestions.csv") and not table_exists("theme_suggestions"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("theme_suggestions.csv", dtype=str, on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = ""
    return df[base_cols]

def promote_themes_to_map(approved_df):
    """
    승인된 추천 테마를 theme_map.csv로 승격하고,
    theme_suggestions.csv의 승인상태를 approved로 반영합니다.
    """
    if approved_df.empty:
        return 0
    map_path = resolve_csv_path("theme_map.csv")
    if os.path.exists(map_path):
        try:
            df_map = pd.read_csv(map_path, dtype=str)
        except Exception:
            df_map = pd.DataFrame(columns=["종목코드", "종목명", "테마"])
    else:
        df_map = pd.DataFrame(columns=["종목코드", "종목명", "테마"])
    for c in ["종목코드", "종목명", "테마"]:
        if c not in df_map.columns:
            df_map[c] = ""
    df_map["종목코드"] = df_map["종목코드"].fillna("").astype(str).str.strip().str.zfill(6)

    promoted_rows = []
    for _, row in approved_df.iterrows():
        code = str(row.get("종목코드", "") or "").strip().zfill(6)
        name = str(row.get("종목명", "") or "").strip()
        theme = str(row.get("추천테마", "") or "").strip()
        if not code or not name or not theme:
            continue
        promoted_rows.append({"종목코드": code, "종목명": name, "테마": theme})
    if not promoted_rows:
        return 0
    df_new = pd.DataFrame(promoted_rows)
    df_map = pd.concat([df_map, df_new], ignore_index=True)
    df_map = df_map.drop_duplicates(subset=["종목코드"], keep="last").sort_values("종목코드")
    df_map.to_csv(map_path, index=False, encoding="utf-8-sig")

    # suggestions 승인상태 업데이트
    sugg = load_theme_suggestions_safe()
    if not sugg.empty:
        approved_codes = set(df_new["종목코드"].astype(str).tolist())
        sugg["종목코드"] = sugg["종목코드"].fillna("").astype(str).str.strip().str.zfill(6)
        sugg.loc[sugg["종목코드"].isin(approved_codes), "승인상태"] = "approved"
        write_table_dual(sugg, "theme_suggestions.csv", index=False, encoding="utf-8-sig")
    return len(df_new)

def _github_state_config():
    repo = str(st.secrets.get("GITHUB_REPO", os.environ.get("GITHUB_REPO", "songyk544-ctrl/quant-bot"))).strip()
    token = str(st.secrets.get("GITHUB_TOKEN", os.environ.get("GITHUB_TOKEN", ""))).strip()
    branch = str(st.secrets.get("GITHUB_BRANCH", os.environ.get("GITHUB_BRANCH", "main"))).strip() or "main"
    return {"repo": repo, "token": token, "branch": branch}


def _github_get_json(path, default_obj):
    cfg = _github_state_config()
    if not cfg["repo"] or not cfg["token"]:
        return default_obj
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {cfg['token']}",
        "Accept": "application/vnd.github+json",
    }
    try:
        resp = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=8)
        if resp.status_code == 404:
            return default_obj
        resp.raise_for_status()
        data = resp.json()
        raw = base64.b64decode(data.get("content", "").encode("utf-8")).decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        print(f"[WARN] GitHub state load failed ({path}): {e}")
        return default_obj


def _github_put_json(path, payload_obj, commit_message):
    cfg = _github_state_config()
    if not cfg["repo"] or not cfg["token"]:
        return False
    url = f"https://api.github.com/repos/{cfg['repo']}/contents/{path}"
    headers = {
        "Authorization": f"Bearer {cfg['token']}",
        "Accept": "application/vnd.github+json",
    }
    try:
        sha = None
        get_resp = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=8)
        if get_resp.status_code == 200:
            sha = get_resp.json().get("sha")
        body = {
            "message": commit_message,
            "branch": cfg["branch"],
            "content": base64.b64encode(
                json.dumps(payload_obj, ensure_ascii=False, indent=2).encode("utf-8")
            ).decode("utf-8"),
        }
        if sha:
            body["sha"] = sha
        put_resp = requests.put(url, headers=headers, json=body, timeout=10)
        put_resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[WARN] GitHub state save failed ({path}): {e}")
        return False


def _load_local_json(path, default_obj):
    if not os.path.exists(path):
        return default_obj
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_obj


def _save_local_json(path, payload_obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload_obj, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"[WARN] local state save failed ({path}): {e}")
        return False


def _load_user_state():
    defaults = {
        "thresholds": {"ai_warn_threshold": 65, "ai_critical_threshold": 55},
        "portfolio": [],
    }
    state = _github_get_json("user_state_admin.json", default_obj=defaults)
    if not isinstance(state, dict):
        state = defaults.copy()
    state.setdefault("thresholds", defaults["thresholds"])
    state.setdefault("portfolio", defaults["portfolio"])
    # 로컬 마이그레이션 (원격 미설정/최초 실행 시)
    if not state.get("portfolio") and os.path.exists("my_portfolio.csv"):
        try:
            pf = pd.read_csv("my_portfolio.csv")
            state["portfolio"] = pf.to_dict("records")
        except Exception:
            pass
    if os.path.exists("admin_ui_settings.json"):
        local_thr = _load_local_json("admin_ui_settings.json", {})
        if isinstance(local_thr, dict):
            state["thresholds"]["ai_warn_threshold"] = int(local_thr.get("ai_warn_threshold", state["thresholds"]["ai_warn_threshold"]))
            state["thresholds"]["ai_critical_threshold"] = int(local_thr.get("ai_critical_threshold", state["thresholds"]["ai_critical_threshold"]))
    return state


def _save_user_state(state_obj, message):
    _save_local_json("user_state_admin.json", state_obj)
    _save_local_json("admin_ui_settings.json", state_obj.get("thresholds", {}))
    try:
        pf = pd.DataFrame(state_obj.get("portfolio", []))
        if pf.empty:
            pf = pd.DataFrame(columns=["종목명", "수량", "매수가"])
        pf.to_csv("my_portfolio.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass
    _github_put_json("user_state_admin.json", state_obj, message)


def load_admin_risk_thresholds():
    """관리자 리스크 임계값을 로드(원격 GitHub state 우선, 로컬 fallback)."""
    defaults = {"ai_warn_threshold": 65, "ai_critical_threshold": 55}
    state = _load_user_state()
    obj = state.get("thresholds", defaults)
    warn = int(obj.get("ai_warn_threshold", defaults["ai_warn_threshold"]))
    critical = int(obj.get("ai_critical_threshold", defaults["ai_critical_threshold"]))
    return {
        "ai_warn_threshold": max(0, min(100, warn)),
        "ai_critical_threshold": max(0, min(100, critical)),
    }


def save_admin_risk_thresholds(ai_warn_threshold, ai_critical_threshold):
    """관리자 리스크 임계값 저장(원격 GitHub state + 로컬 동시 반영)."""
    state = _load_user_state()
    state["thresholds"] = {
        "ai_warn_threshold": int(max(0, min(100, ai_warn_threshold))),
        "ai_critical_threshold": int(max(0, min(100, ai_critical_threshold))),
    }
    _save_user_state(state, "chore(state): update admin risk thresholds")


def load_admin_portfolio_df(base_cols):
    state = _load_user_state()
    rows = state.get("portfolio", [])
    if not isinstance(rows, list):
        rows = []
    df_port = pd.DataFrame(rows) if rows else pd.DataFrame(columns=base_cols)
    for c in base_cols:
        if c not in df_port.columns:
            df_port[c] = None
    df_port = df_port[base_cols].copy()
    df_port["종목명"] = df_port["종목명"].fillna("").astype(str).str.strip()
    df_port["수량"] = pd.to_numeric(df_port["수량"], errors="coerce").fillna(0)
    df_port["매수가"] = pd.to_numeric(df_port["매수가"], errors="coerce").fillna(0.0)
    df_port = df_port[df_port["종목명"] != ""].copy()
    return df_port


def save_admin_portfolio_df(df_portfolio):
    base_cols = ["종목명", "수량", "매수가"]
    save_df = df_portfolio.copy()
    for c in base_cols:
        if c not in save_df.columns:
            save_df[c] = None
    save_df = save_df[base_cols]
    save_df["종목명"] = save_df["종목명"].fillna("").astype(str).str.strip()
    save_df = save_df[save_df["종목명"] != ""].copy()
    save_df["수량"] = pd.to_numeric(save_df["수량"], errors="coerce").fillna(0)
    save_df["매수가"] = pd.to_numeric(save_df["매수가"], errors="coerce").fillna(0.0)
    state = _load_user_state()
    state["portfolio"] = save_df.to_dict("records")
    _save_user_state(state, "chore(state): update admin portfolio")

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

def make_trend_svg(values, width=96, height=24):
    """최근 값 흐름을 미니 라인(SVG)으로 렌더링."""
    arr = [float(v) for v in values if pd.notna(v)]
    if not arr:
        return '<svg width="96" height="24" viewBox="0 0 96 24"><line x1="0" y1="12" x2="96" y2="12" stroke="#334155" stroke-width="1.2"/></svg>'
    if len(arr) > 12:
        arr = arr[-12:]
    mn, mx = min(arr), max(arr)
    if mx - mn < 1e-9:
        mx = mn + 1.0
    n = len(arr)
    pad = 2.0
    step = (width - pad * 2) / max(1, n - 1)
    points = []
    for i, v in enumerate(arr):
        x = pad + i * step
        y = pad + (height - pad * 2) * (1.0 - ((v - mn) / (mx - mn)))
        points.append(f"{x:.2f},{y:.2f}")
    trend_up = arr[-1] >= arr[0]
    line_color = "#36C06A" if trend_up else "#E04B4B"
    fill_color = "rgba(54,192,106,0.16)" if trend_up else "rgba(224,75,75,0.16)"
    baseline_y = pad + (height - pad * 2) * (1.0 - ((0.0 - mn) / (mx - mn))) if (mn <= 0.0 <= mx) else (height - pad)
    poly_points = " ".join(points + [f"{width - pad:.2f},{height - pad:.2f}", f"{pad:.2f},{height - pad:.2f}"])
    return (
        f'<svg width="{int(width)}" height="{int(height)}" viewBox="0 0 {int(width)} {int(height)}">'
        f'<line x1="{pad:.2f}" y1="{baseline_y:.2f}" x2="{width-pad:.2f}" y2="{baseline_y:.2f}" stroke="#2A3347" stroke-width="1"/>'
        f'<polygon points="{poly_points}" fill="{fill_color}"/>'
        f'<polyline points="{" ".join(points)}" fill="none" stroke="{line_color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>'
        f'</svg>'
    )

def calc_signed_streak(values):
    """최근 연속 순매수/순매도 일수 계산(+면 매수, -면 매도)."""
    arr = [float(v) for v in values if pd.notna(v)]
    if not arr:
        return 0
    streak = 0
    direction = 0
    for v in reversed(arr):
        sign = 1 if v > 0 else (-1 if v < 0 else 0)
        if sign == 0:
            break
        if direction == 0:
            direction = sign
            streak = 1
            continue
        if sign == direction:
            streak += 1
        else:
            break
    return direction * streak

def fetch_yahoo_chart_history(ticker_symbol, range_period="2y", interval="1d"):
    """yfinance 실패 시 Yahoo Chart API 직접 호출 fallback."""
    try:
        encoded_symbol = urllib.parse.quote(ticker_symbol, safe="")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}?range={range_period}&interval={interval}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
        if res.status_code != 200:
            return pd.DataFrame()
        payload = res.json()
        result = ((payload or {}).get("chart") or {}).get("result") or []
        if not result:
            return pd.DataFrame()
        data = result[0]
        timestamps = data.get("timestamp") or []
        quotes = (((data.get("indicators") or {}).get("quote") or [{}])[0])
        closes = quotes.get("close") or []
        if not timestamps or not closes:
            return pd.DataFrame()
        df = pd.DataFrame({"ts": timestamps, "Close": closes}).dropna(subset=["Close"])
        if df.empty:
            return pd.DataFrame()
        df["Date"] = pd.to_datetime(df["ts"], unit="s").dt.tz_localize(None).dt.normalize()
        return df.set_index("Date")[["Close"]].sort_index()
    except Exception:
        return pd.DataFrame()

def format_report_for_readability(report_text):
    """리포트 상단 메타(작성일/시장상태/작성자)가 한 줄로 붙는 경우 가독성을 위해 줄바꿈을 보정."""
    if not report_text:
        return report_text
    formatted = report_text
    for marker in ["**날짜:**", "**시장 상태:**", "**시장상태:**", "**작성자:**"]:
        formatted = formatted.replace(f" {marker}", f"\n{marker}")
    return formatted

def render_section_header(title, subtitle="", badge_text=""):
    badge_html = (
        f'<span style="background:#172033; border:1px solid #33435F; color:#BFDBFE; border-radius:999px; padding:3px 10px; font-size:0.76em; font-weight:700;">{badge_text}</span>'
        if badge_text else ""
    )
    html = dedent(f"""
<div style="background:linear-gradient(140deg,#101827,#0e1524); border:1px solid #27324A; border-radius:14px; padding:12px 14px; margin:4px 0 10px 0;">
  <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap;">
    <div style="color:#F3F6FB; font-size:1.04em; font-weight:800;">{title}</div>
    {badge_html}
  </div>
  <div style="color:#9FB0CC; font-size:0.84em; margin-top:4px;">{subtitle}</div>
</div>
""").strip()
    st.markdown(html, unsafe_allow_html=True)

def apply_altair_theme(chart):
    return chart.configure_view(strokeOpacity=0).configure_axis(
        gridColor="#263247",
        domainColor="#334155",
        tickColor="#334155",
        labelColor="#CBD5E1",
        titleColor="#CBD5E1"
    ).configure_legend(
        labelColor="#CBD5E1",
        titleColor="#CBD5E1"
    )

def render_empty_state(title, message):
    st.markdown(
        dedent(f"""
<div style="background:linear-gradient(145deg,#111827,#0f172a); border:1px solid #2A344A; border-radius:12px; padding:12px 14px; margin:8px 0;">
  <div style="color:#E5E7EB; font-weight:800; font-size:0.95em;">{title}</div>
  <div style="color:#94A3B8; font-size:0.84em; margin-top:4px;">{message}</div>
</div>
""").strip(),
        unsafe_allow_html=True,
    )

def build_quality_badge(row):
    """종목 데이터 충실도 기반 간단 신뢰도 배지."""
    score = 0
    if "정성점수" in row.index and pd.notna(row.get("정성점수")):
        score += 1
    if "Quant점수" in row.index and pd.notna(row.get("Quant점수")):
        score += 1
    if "외인연속" in row.index and pd.notna(row.get("외인연속")):
        score += 1
    if "연기금연속" in row.index and pd.notna(row.get("연기금연속")):
        score += 1
    if score >= 4:
        return "신뢰도 높음"
    if score >= 2:
        return "신뢰도 보통"
    return "신뢰도 낮음"

def pick_watch_candidate(ranked_df, macro_news_refs):
    """
    관망 후보를 5위 고정 대신 동적으로 선택:
    - 상위권(최대 12개) 내에서
    - 뉴스 키워드 정합성(섹터/종목명) + 최근 순위 상승폭(랭크 모멘텀) 반영
    """
    if ranked_df.empty:
        return None
    if len(ranked_df) == 1:
        return ranked_df.iloc[0]

    # 최상위 1개는 매수 후보로 사용하므로 관망 후보군에서 제외
    pool = ranked_df.iloc[1:min(12, len(ranked_df))].copy()
    if pool.empty:
        return ranked_df.iloc[min(1, len(ranked_df) - 1)]

    macro_text = " ".join(macro_news_refs or []).lower()
    theme_keywords = ["반도체", "바이오", "2차전지", "전력", "조선", "방산", "금리", "환율", "원유", "ai", "자동차", "게임"]
    rank_momentum = {}
    try:
        df_trend = load_score_trend_safe()
        if not df_trend.empty and {"날짜", "종목명", "순위"}.issubset(df_trend.columns):
            dates = sorted(df_trend["날짜"].astype(str).unique(), reverse=True)
            if len(dates) >= 2:
                curr_dt, prev_dt = dates[0], dates[1]
                curr = df_trend[df_trend["날짜"].astype(str) == curr_dt][["종목명", "순위"]].copy()
                prev = df_trend[df_trend["날짜"].astype(str) == prev_dt][["종목명", "순위"]].copy()
                curr.columns = ["종목명", "curr_rank"]
                prev.columns = ["종목명", "prev_rank"]
                merged = pd.merge(curr, prev, on="종목명", how="left")
                for _, row in merged.iterrows():
                    try:
                        c_rank = int(row["curr_rank"])
                        p_rank = int(row["prev_rank"]) if pd.notna(row["prev_rank"]) else c_rank
                        rank_momentum[str(row["종목명"])] = p_rank - c_rank
                    except Exception:
                        continue
    except Exception:
        rank_momentum = {}

    def _score_row(r):
        name = str(r.get("종목명", "") or "").lower()
        sector = str(r.get("테마표시", r.get("섹터", "")) or "").lower()
        text = f"{name} {sector}"
        theme_hits = sum(1 for kw in theme_keywords if kw in macro_text and kw in text)
        rank_delta = float(rank_momentum.get(str(r.get("종목명", "")), 0.0))
        # 하루 순위 개선폭(전일순위-현재순위)을 완만하게 반영
        momentum_score = max(-2.0, min(3.0, rank_delta * 0.4))
        return (theme_hits * 2.0) + momentum_score

    pool["_watch_score"] = pool.apply(_score_row, axis=1)
    best = pool.sort_values(["_watch_score", "AI수급점수"], ascending=False).iloc[0]
    return best

def render_action_brief(df_summary_local, macro_news_refs):
    """오늘의 액션 브리프 3카드(매수 후보/관망/리스크)."""
    if df_summary_local.empty:
        st.info("오늘의 액션 브리프를 만들 데이터가 아직 없습니다.")
        return

    ranked = df_summary_local.sort_values("AI수급점수", ascending=False).reset_index(drop=True)
    buy_row = ranked.iloc[0]
    watch_row = pick_watch_candidate(ranked, macro_news_refs)
    if watch_row is None:
        watch_idx = min(4, len(ranked) - 1)
        watch_row = ranked.iloc[watch_idx]

    try:
        vix_data = macro_data.get("😨 VIX")
        vix_val = float(vix_data["value"]) if vix_data and vix_data.get("value") is not None else None
    except Exception:
        vix_val = None
    risk_text = "주의" if (vix_val is not None and vix_val >= 22) else "중립"
    risk_detail = f"VIX {vix_val:.2f}" if vix_val is not None else "VIX 데이터 지연"
    if len(macro_news_refs or []) == 0:
        risk_text = "주의"
        risk_detail = "시황 뉴스 지연"

    updated_at = datetime.now().strftime("%H:%M")
    buy_conf = str(buy_row.get("신호등급", "-"))
    watch_conf = str(watch_row.get("신호등급", "-"))
    def _brief_card(title, value, badge_html, meta_text):
        return (
            f'<div class="kpi-card">'
            f'<div class="kpi-title">{title}</div>'
            f'<div class="kpi-value" style="font-size:1.45em;">{value}</div>'
            f'{badge_html}'
            f'<div class="kpi-meta">{meta_text}</div>'
            f'</div>'
        )

    card_buy = _brief_card(
        "오늘의 매수 후보",
        buy_row.get("종목명", "-"),
        f'<span class="kpi-delta" style="background:rgba(54,192,106,0.18); color:#36C06A;">AI {float(buy_row.get("AI수급점수", 0)):.1f}</span>',
        f"{build_quality_badge(buy_row)} · 신호 {buy_conf} · 갱신 {updated_at}"
    )
    card_watch = _brief_card(
        "관망 후보",
        watch_row.get("종목명", "-"),
        f'<span class="kpi-delta" style="background:rgba(59,130,246,0.16); color:#60A5FA;">AI {float(watch_row.get("AI수급점수", 0)):.1f}</span>',
        f"신호 {watch_conf} · 추세 확인 필요 · 갱신 {updated_at}"
    )
    card_risk = _brief_card(
        "리스크 경보",
        risk_text,
        f'<span class="kpi-delta" style="background:rgba(224,75,75,0.16); color:#E04B4B;">{risk_detail}</span>',
        f"뉴스 {len(macro_news_refs or [])}건 반영 · 갱신 {updated_at}"
    )
    st.markdown(f'<div class="kpi-grid">{card_buy}{card_watch}{card_risk}</div>', unsafe_allow_html=True)

def _request_html(url, headers, timeout=4, retries=2):
    """가벼운 재시도로 일시적 네트워크 실패를 완화합니다."""
    for attempt in range(retries + 1):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except Exception:
            if attempt == retries:
                return None

def _parse_news_datetime(raw_text):
    """네이버 뉴스 표기(방금 전/분 전/시간 전/YYYY.MM.DD.)를 datetime으로 변환."""
    if not raw_text:
        return None
    txt = raw_text.strip()
    now = datetime.now()

    if "방금 전" in txt:
        return now
    m = re.search(r"(\d+)\s*분 전", txt)
    if m:
        return now - pd.Timedelta(minutes=int(m.group(1)))
    m = re.search(r"(\d+)\s*시간 전", txt)
    if m:
        return now - pd.Timedelta(hours=int(m.group(1)))

    if re.match(r"\d{4}\.\d{2}\.\d{2}\.", txt):
        try:
            return datetime.strptime(txt, "%Y.%m.%d.")
        except Exception:
            return None
    return None

def _score_news_candidate(candidate):
    return _score_news_candidate_base(candidate, include_relevance=True)

def _is_relevant_to_stock(stock_name, text):
    """종목명 직접 포함 여부와 토큰 일치율로 뉴스 연관성을 추정."""
    text_norm = _normalize_text(text).lower()
    name_norm = _normalize_text(stock_name).lower()
    if not name_norm:
        return True
    if name_norm in text_norm:
        return True
    tokens = [t for t in re.split(r"\s+", name_norm) if len(t) >= 2]
    if not tokens:
        return False
    matched = sum(1 for t in tokens if t in text_norm)
    return (matched / len(tokens)) >= 0.6

def _parse_short_yy_mm_dd(text):
    txt = _normalize_text(text).replace(".", "-")
    m = re.match(r"^(\d{2})-(\d{2})-(\d{2})$", txt)
    if not m:
        return None
    yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
    yyyy = 2000 + yy if yy < 70 else 1900 + yy
    try:
        return datetime(yyyy, mm, dd)
    except Exception:
        return None

@st.cache_data(ttl=1800)
def get_stock_disclosure_report_context(stock_name, stock_code):
    """개별 종목의 최근 공시/리포트를 요약 텍스트로 반환."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    lines = []

    # 공시
    notice_url = f"https://finance.naver.com/item/news_notice.naver?code={stock_code}"
    res_notice = _request_html(notice_url, headers=headers, timeout=4, retries=1)
    if res_notice is not None:
        try:
            res_notice.encoding = res_notice.apparent_encoding or "utf-8"
            soup = BeautifulSoup(res_notice.text, "html.parser")
            cutoff_dt = datetime.now() - pd.Timedelta(days=10)
            count = 0
            for tr in soup.select("table tr"):
                a_tag = tr.select_one("a[href*='news_notice_read']")
                if not a_tag:
                    continue
                tds = tr.select("td")
                date_text = _normalize_text(tds[-1].text if tds else "")
                try:
                    dt = datetime.strptime(date_text, "%Y.%m.%d")
                except Exception:
                    dt = None
                if dt is not None and dt < cutoff_dt:
                    continue
                title = _normalize_text(a_tag.text)
                if title:
                    lines.append(f"- 공시: {title} ({date_text})")
                    count += 1
                if count >= 3:
                    break
        except Exception:
            pass

    # 증권사 리포트
    research_url = f"https://finance.naver.com/research/company_list.naver?keyword={urllib.parse.quote(stock_name)}"
    res_research = _request_html(research_url, headers=headers, timeout=4, retries=1)
    if res_research is not None:
        try:
            res_research.encoding = res_research.apparent_encoding or "utf-8"
            soup = BeautifulSoup(res_research.text, "html.parser")
            cutoff_dt = datetime.now() - pd.Timedelta(days=14)
            count = 0
            for tr in soup.select("table tr"):
                report_tag = tr.select_one("a[href*='/research/company_read.naver']")
                stock_tag = tr.select_one("a[href*='/item/main.naver?code=']")
                if not report_tag:
                    continue
                report_stock = _normalize_text(stock_tag.text if stock_tag else "")
                if stock_name not in report_stock:
                    continue
                report_title = _normalize_text(report_tag.text)
                tds = tr.select("td")
                date_text = _normalize_text(tds[-2].text if len(tds) >= 2 else "")
                dt = _parse_short_yy_mm_dd(date_text)
                if dt is not None and dt < cutoff_dt:
                    continue
                lines.append(f"- 리포트: {report_title} ({date_text})")
                count += 1
                if count >= 2:
                    break
        except Exception:
            pass

    return "\n".join(lines) if lines else "최근 공시/리포트 데이터 없음"

# 🔥 [신규 추가] 매크로 주요 시황 스크래핑 함수
@st.cache_data(ttl=1800)
def get_macro_headline_news():
    """반복 이슈 Top5 + 최신 뉴스 Top5를 함께 반환."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    cutoff_dt = datetime.now() - pd.Timedelta(hours=72)
    candidates = []
    signatures = []

    def add_candidate(title, desc="", news_dt=None, source="일반"):
        title = _normalize_text(title)
        desc = _normalize_text(desc)
        if not title:
            return
        sig = _title_signature(title)
        if _is_similar_title(sig, signatures):
            return
        signatures.append(sig)
        candidates.append({
            "title": title,
            "desc": desc,
            "dt": news_dt,
            "source": source,
            "tags": _event_tags(f"{title} {desc}")
        })

    try:
        res = _request_html("https://finance.naver.com/news/mainnews.naver", headers=headers, timeout=4, retries=1)
        if res is not None:
            soup = BeautifulSoup(res.text, 'html.parser')
            subjects = soup.select('.articleSubject a')
            summaries = soup.select('.articleSummary')
            for i in range(min(30, len(subjects))):
                add_candidate(
                    title=_normalize_text(subjects[i].text),
                    desc=_normalize_text(summaries[i].text if i < len(summaries) else ""),
                    source="네이버금융"
                )

        if len(candidates) < 40:
            rss_url = "https://news.google.com/rss/search?q=%EC%A6%9D%EC%8B%9C%20OR%20%EC%BD%94%EC%8A%A4%ED%94%BC%20OR%20%EA%B8%88%EB%A6%AC&hl=ko&gl=KR&ceid=KR:ko"
            res_rss = _request_html(rss_url, headers=headers, timeout=5, retries=1)
            if res_rss is not None:
                rss_soup = BeautifulSoup(res_rss.text, "xml")
                for item in rss_soup.select("item")[:40]:
                    title = _normalize_text(item.title.text if item.title else "")
                    pub_date = _normalize_text(item.pubDate.text if item.pubDate else "")
                    try:
                        dt = parsedate_to_datetime(pub_date).replace(tzinfo=None)
                    except Exception:
                        dt = None
                    if dt is not None and dt < cutoff_dt:
                        continue
                    add_candidate(title=title, news_dt=dt, source="GoogleNewsRSS")
                    if len(candidates) >= 40:
                        break
    except Exception:
        pass

    if not candidates:
        return []

    # 반복 이슈 Top5
    topic_stats = {}
    now = datetime.now()
    for item in candidates:
        # 의미 태그 없는 기사까지 '일반'으로 묶으면 일반이 과대표집됨.
        tags = [t for t in (item.get("tags") or []) if t and t != "일반"]
        if not tags:
            continue
        source = item.get("source", "일반")
        dt = item.get("dt")
        recency = 0.4 if dt is None else (1.0 / (1.0 + max(0.0, (now - dt).total_seconds() / 86400.0)))
        for tag in tags:
            stat = topic_stats.setdefault(tag, {"count": 0, "sources": set(), "recency_sum": 0.0})
            stat["count"] += 1
            stat["sources"].add(source)
            stat["recency_sum"] += recency

    ranked_topics = []
    for tag, st in topic_stats.items():
        avg_recency = st["recency_sum"] / max(1, st["count"])
        score = (st["count"] * 1.0) + (len(st["sources"]) * 0.7) + (avg_recency * 2.0)
        ranked_topics.append((score, tag, st["count"], len(st["sources"])))
    ranked_topics.sort(reverse=True)

    topic_lines = [f"[반복] {tag} (빈도 {cnt} / 출처 {src_cnt})" for _, tag, cnt, src_cnt in ranked_topics[:5]]
    if not topic_lines:
        topic_lines = ["[반복] 유의미한 공통 키워드 없음"]
    latest_lines = [f"[최신] {item['title']}" for item in sorted(candidates, key=_score_news_candidate, reverse=True)[:5]]
    return topic_lines + latest_lines

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
        res = _request_html(url, headers=headers, timeout=4, retries=1)
        if res is None:
            return ["현재 뉴스 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해주세요."]
        soup = BeautifulSoup(res.text, 'html.parser')
        cutoff_dt = datetime.now() - pd.Timedelta(hours=48)
        candidates = []
        signatures = []

        def add_candidate(title, desc="", news_dt=None, source="일반"):
            title = _normalize_text(title)
            desc = _normalize_text(desc)
            if not title:
                return
            signature = _title_signature(title)
            if _is_similar_title(signature, signatures):
                return
            signatures.append(signature)
            text_for_tag = f"{title} {desc}"
            candidates.append({
                "title": title,
                "desc": desc,
                "dt": news_dt,
                "source": source,
                "tags": _event_tags(text_for_tag),
                "is_relevant": _is_relevant_to_stock(stock_name, text_for_tag)
            })

        news_items = soup.select('.news_wrap.api_ani_send')
        for item in news_items:
            tit_tag = item.select_one('.news_tit')
            desc_tag = item.select_one('.api_txt_lines.dsc_txt_wrap')
            time_tag = item.select_one('.info_group span.info:last-child')
            source_tag = item.select_one('.info_group a.info.press')
            
            if tit_tag:
                title = _normalize_text(tit_tag.get('title') or tit_tag.text)
                desc = _normalize_text(desc_tag.text if desc_tag else "")
                news_dt = _parse_news_datetime(time_tag.text if time_tag else "")
                if news_dt is not None and news_dt < cutoff_dt:
                    continue
                source = _normalize_text(source_tag.text if source_tag else _extract_source(title))
                add_candidate(title=title, desc=desc, news_dt=news_dt, source=source)
            if len(candidates) >= 10:
                break
            
        if not candidates:
            # 네이버 금융 검색은 인코딩/DOM 변동이 있어 utf/euc-kr를 모두 시도
            fin_urls = [
                f"https://finance.naver.com/news/news_search.naver?q={urllib.parse.quote(stock_name)}",
                f"https://finance.naver.com/news/news_search.naver?q={urllib.parse.quote(stock_name.encode('euc-kr'))}",
            ]
            for fin_url in fin_urls:
                res_fin = _request_html(fin_url, headers=headers, timeout=4, retries=1)
                if res_fin is None:
                    continue
                soup_fin = BeautifulSoup(res_fin.text, 'html.parser')
                title_nodes = soup_fin.select('.articleSubject a')
                date_nodes = soup_fin.select('.wdate')
                summary_nodes = soup_fin.select('.articleSummary')

                for idx, t_tag in enumerate(title_nodes):
                    if not t_tag:
                        continue
                    title_text = _normalize_text(t_tag.get('title') or t_tag.text)
                    dt_text = _normalize_text(date_nodes[idx].text if idx < len(date_nodes) else "")
                    summary_text = _normalize_text(summary_nodes[idx].text if idx < len(summary_nodes) else "")
                    try:
                        news_dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M")
                    except Exception:
                        news_dt = None
                    if news_dt is not None and news_dt < cutoff_dt:
                        continue
                    add_candidate(title=title_text, desc=summary_text, news_dt=news_dt, source="네이버금융")
                    if len(candidates) >= 10:
                        break
                if candidates:
                    break

        # 2차 fallback: 무료 RSS (Google News) - 후보가 부족할 때 보강
        if len(candidates) < 5:
            rss_queries = [
                f"{stock_name} 주식",
                f"{stock_name} 증권",
                stock_name
            ]
            for q in rss_queries:
                rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(q)}&hl=ko&gl=KR&ceid=KR:ko"
                res_rss = _request_html(rss_url, headers=headers, timeout=6, retries=1)
                if res_rss is None:
                    continue
                rss_soup = BeautifulSoup(res_rss.text, "xml")
                for item in rss_soup.select("item")[:20]:
                    title_text = _normalize_text(item.title.text if item.title else "")
                    pub_date = _normalize_text(item.pubDate.text if item.pubDate else "")
                    try:
                        dt = parsedate_to_datetime(pub_date).replace(tzinfo=None)
                    except Exception:
                        dt = None
                    # RSS는 기사량 확보를 위해 72시간까지 허용
                    if dt is not None and dt < (datetime.now() - pd.Timedelta(hours=72)):
                        continue
                    add_candidate(title=title_text, news_dt=dt, source="GoogleNewsRSS")
                    if len(candidates) >= 10:
                        break
                if len(candidates) >= 10:
                    break

        if candidates:
            ranked = sorted(candidates, key=_score_news_candidate, reverse=True)
            relevant_ranked = [c for c in ranked if c.get("is_relevant")]
            if len(relevant_ranked) >= 3:
                ranked = relevant_ranked
            ranked = ranked[:5]
            for c in ranked:
                tag_text = ",".join(c["tags"][:2]) if c["tags"] else "일반"
                if c["desc"]:
                    news_list.append(f"제목: {c['title']} / 핵심내용: {c['desc']} / 출처: {c['source']} / 태그: {tag_text}")
                else:
                    news_list.append(f"제목: {c['title']} / 출처: {c['source']} / 태그: {tag_text}")
                
    except Exception as e:
        pass
        
    if not news_list:
        news_list = ["현재 뉴스 데이터를 가져오지 못했습니다. 잠시 후 다시 시도해주세요."]
        
    return news_list

migrate_csv_to_sqlite_once([
    ("data", "data.csv"),
    ("history", "history.csv"),
    ("score_trend", "score_trend.csv"),
    ("performance_trend", "performance_trend.csv"),
    ("theme_suggestions", "theme_suggestions.csv"),
    ("theme_map", "theme_map.csv"),
    ("dart_map", "dart_map.csv"),
    ("portfolio", "portfolio.csv"),
    ("theme_quality_trend", "theme_quality_trend.csv"),
])

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 시장 데이터를 집계 중입니다.")
else:
    if "신호신뢰도" in df_summary.columns:
        df_summary["신호신뢰도"] = pd.to_numeric(df_summary["신호신뢰도"], errors="coerce").fillna(0.0)
    else:
        df_summary["신호신뢰도"] = 0.0
    if "신호등급" not in df_summary.columns:
        df_summary["신호등급"] = "-"
    if "점수변화(안정화)" in df_summary.columns:
        df_summary["점수변화(안정화)"] = pd.to_numeric(df_summary["점수변화(안정화)"], errors="coerce").fillna(0.0)
    else:
        df_summary["점수변화(안정화)"] = 0.0

    df_summary['현재_순위'] = df_summary['AI수급점수'].rank(method='first', ascending=False).astype(int)
    if "테마" in df_summary.columns:
        df_summary["테마표시"] = df_summary["테마"].fillna("").astype(str).str.strip()
        fallback_sector = df_summary["섹터"] if "섹터" in df_summary.columns else "분류안됨"
        df_summary["테마표시"] = df_summary["테마표시"].where(df_summary["테마표시"] != "", fallback_sector)
    else:
        df_summary["테마표시"] = df_summary["섹터"] if "섹터" in df_summary.columns else "분류안됨"
    
    if csv_exists("score_trend.csv") or table_exists("score_trend"):
        df_trend = load_score_trend_safe()
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

    tab_labels = ["매크로", "테마 히트맵", "알파 레이더", "종목 분석", "백테스트", "주도주 비교"]
    if is_admin:
        tab_labels.append("🔒 포트폴리오")
    tabs = st.tabs(tab_labels)
    tab1, tab2, tab3, tab4, tab5, tab6 = tabs[:6]
    tab7 = tabs[6] if is_admin and len(tabs) > 6 else None

    # --- 탭 1: 매크로 인사이트 ---
    with tab1:
        render_section_header("오늘의 매크로 리포트", "핵심 매크로/뉴스/리스크를 먼저 확인하고 세부 분석으로 내려갑니다.")
        macro_refs = get_macro_headline_news()
        st.markdown("##### 오늘의 액션 브리프")
        st.caption("매수/관망/리스크를 먼저 확인하고 세부 탭으로 내려가세요.")
        render_action_brief(df_summary, macro_refs)
        st.markdown("---")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()
            report_content = format_report_for_readability(report_content)

            if is_vip:
                st.markdown(report_content)
            else:
                teaser_text = report_content[:250] + "...\n\n"
                st.markdown(teaser_text)
                show_premium_paywall("심층 매크로 분석 리포트 전문은 코드 인증 후 확인할 수 있습니다.")
        else:
            render_empty_state("리포트 생성 대기", "AI 매크로 리포트를 준비 중입니다. 잠시 후 새로고침해 주세요.")
        with st.expander("참고한 시황 뉴스 제목 보기"):
            if macro_refs:
                for item in macro_refs:
                    st.caption(f"- {item}")
            else:
                st.caption("- 시황 뉴스 데이터를 불러오지 못했습니다.")

    # --- 탭 2: 테마 히트맵 ---
    with tab2:
        render_section_header("시가총액 및 수급 테마 히트맵", "사각형 크기는 시가총액, 색상은 당일 등락률입니다.", badge_text="Theme Flow")

        if not is_vip:
            show_premium_paywall("전체 시장의 테마별 자금 흐름 히트맵은 코드 인증 후 확인할 수 있습니다.")
        else:
            if not df_summary.empty:
                df_hm = df_summary.copy()
                df_hm['테마표시'] = df_hm['테마표시'].fillna("기타")
                df_hm['시가총액'] = pd.to_numeric(df_hm['시가총액'], errors='coerce').fillna(0)
                df_hm['등락률'] = pd.to_numeric(df_hm['등락률'], errors='coerce').fillna(0)

                fig = px.treemap(
                    df_hm,
                    path=[px.Constant("국내 증시 주요 테마"), '테마표시', '종목명'],
                    values='시가총액',
                    color='등락률',
                    color_continuous_scale=['#E04B4B', '#242735', '#36C06A'],
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
                    coloraxis_showscale=False,
                    font=dict(color="#CBD5E1")
                )

                st.plotly_chart(fig, width='stretch')
            else:
                render_empty_state("데이터 대기", "테마 히트맵 데이터가 아직 준비되지 않았습니다.")

    # --- 탭 3: 수급 스크리너 ---
    with tab3:
        render_section_header("알파 레이더", "핵심 후보를 카드/테이블로 빠르게 스캔하고, 상세 지표는 필요 시 확장해서 확인합니다.")
        if "view_mode" not in st.session_state:
            st.session_state.view_mode = "card"
            
        col_v1, col_v2, col_v3 = st.columns([1, 1, 2])
        with col_v1:
            if st.button("카드 보기", use_container_width=True, type="primary" if st.session_state.view_mode == "card" else "secondary"):
                st.session_state.view_mode = "card"
                st.rerun()
        with col_v2:
            if st.button("테이블 보기", use_container_width=True, type="primary" if st.session_state.view_mode == "table" else "secondary"):
                st.session_state.view_mode = "table"
                st.rerun()
        
        st.markdown("<div style='margin-bottom: 15px;'></div>", unsafe_allow_html=True)
        
        df_display = df_summary if is_vip else df_summary.head(5)

        if st.session_state.view_mode == "card":
            st.caption("모바일에서 빠르게 확인할 수 있는 카드 보기입니다. 상세 내용은 '종목 분석' 탭에서 확인하세요.")
            
            html_lines = []
            for idx, row in df_display.iterrows():
                rank = int(row['현재_순위'])
                name = row['종목명']
                sector = safe_get(row, '테마표시', safe_get(row, '섹터', '분류안됨'))
                price = f"{safe_get(row, '현재가', 0):,.0f}"
                chg = float(safe_get(row, '등락률', 0))
                chg_color = "#FF4B4B" if chg > 0 else "#3B82F6" if chg < 0 else "#AAAAAA"
                chg_str = f"▲ {chg:.2f}%" if chg > 0 else f"▼ {abs(chg):.2f}%" if chg < 0 else "0.00%"
                ai_score = float(safe_get(row, 'AI수급점수', 0))
                rank_chg = safe_get(row, '랭킹추세', '-')
                f_str = f"{float(safe_get(row, '외인강도(%)', 0)):.1f}%"
                p_str = f"{float(safe_get(row, '연기금강도(%)', 0)):.1f}%"
                
                rc_color = "#FF4B4B" if "▲" in str(rank_chg) else ("#3B82F6" if "▼" in str(rank_chg) else "#888888")
                
                card_cls = {1: "podium-card-1", 2: "podium-card-2", 3: "podium-card-3"}.get(rank, "")
                rank_badge_cls = {1: "podium-badge-1", 2: "podium-badge-2", 3: "podium-badge-3"}.get(rank, "")
                name_cls = {1: "podium-name-1", 2: "podium-name-2", 3: "podium-name-3"}.get(rank, "")
                card_html = f"""
<div class="{card_cls}" style="background-color: #1E1E2E; padding: 16px; border-radius: 12px; margin-bottom: 12px; border: 1px solid #333; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
<div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 12px; gap: 10px;">
<div style="display: flex; flex-direction: column; gap: 8px;">
<div style="display: flex; align-items: center; gap: 6px; flex-wrap: wrap;">
<span class="{rank_badge_cls}" style="background: #2b2b36; border: 1px solid #444; color: #FFD700; font-size: 0.7em; font-weight: 800; padding: 4px 8px; border-radius: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.3); white-space: nowrap;">🏆 {rank}위</span>
<span style="font-size: 0.8em; font-weight: bold; color: {rc_color}; white-space: nowrap;">{rank_chg}</span>
<span class="{name_cls}" style="font-size: 1.15em; font-weight: 800; color: #FFF; line-height: 1.2;">{name}</span>
</div>
<div><span style="font-size: 0.75em; color: #AAA; padding: 3px 6px; background: #2A2A35; border-radius: 4px;">{sector}</span></div>
</div>
<div style="text-align: right; min-width: 80px;">
<div style="font-size: 1.1em; font-weight: 700; color: #FFF;">{price}원</div>
<div style="font-size: 0.9em; font-weight: 800; color: {chg_color};">{chg_str}</div>
</div>
</div>
<div style="display: flex; justify-content: space-between; font-size: 0.85em; color: #DDD; background: #181825; padding: 10px; border-radius: 8px; align-items: center; flex-wrap: wrap; gap: 8px;">
<div>⚡ AI점수: <b style="color:#FFD700; font-size: 1.1em;">{ai_score:.1f}점</b></div>
<div>외인 <b style="color:#36C06A;">{f_str}</b> <span style="color:#444;">|</span> 기금 <b style="color:#E04B4B;">{p_str}</b></div>
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
                show_advanced = st.toggle("상세 수급/지표 보기", value=False)

            def color_score(val): return f'color: {"#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"}; font-weight: bold;'
            def color_fluctuation(val):
                if pd.isna(val): return 'color: gray;'
                if isinstance(val, (int, float)): return 'color: #FF4B4B; font-weight: bold;' if val > 0 else ('color: #3B82F6; font-weight: bold;' if val < 0 else 'color: gray;')
                return 'color: gray;'
                
            def color_momentum(val):
                if isinstance(val, str):
                    if '▲' in val: return 'color: #FF4B4B; font-weight: bold;'
                    elif '▼' in val: return 'color: #3B82F6; font-weight: bold;'
                return 'color: gray;'

            df_display_table = df_display.set_index('종목명')

            style_target = df_display_table
            styled_df = style_target
            score_cols = [c for c in ['AI수급점수'] if c in style_target.columns]
            flow_cols = [c for c in ['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)'] if c in style_target.columns]
            momentum_cols = [c for c in ['랭킹추세'] if c in style_target.columns]
            
            format_dict = {
                "AI수급점수": "{:.2f}",
                "현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%",
                "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", "투신강도(%)": "{:.2f}%",
                "사모강도(%)": "{:.2f}%", "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%",
                "신호신뢰도": "{:.1f}", "점수변화(안정화)": "{:+.2f}"
            }
            if 'PER' in df_display_table.columns: format_dict["PER"] = "{:.1f}"
            if 'ROE' in df_display_table.columns: format_dict["ROE"] = "{:.1f}%"
            try:
                styler = style_target.style
                # pandas 버전 호환: map 지원 시 우선 사용, 아니면 applymap 사용
                if score_cols:
                    if hasattr(styler, "map"):
                        styler = styler.map(color_score, subset=score_cols)
                    else:
                        styler = styler.applymap(color_score, subset=score_cols)
                if flow_cols:
                    if hasattr(styler, "map"):
                        styler = styler.map(color_fluctuation, subset=flow_cols)
                    else:
                        styler = styler.applymap(color_fluctuation, subset=flow_cols)
                if momentum_cols:
                    if hasattr(styler, "map"):
                        styler = styler.map(color_momentum, subset=momentum_cols)
                    else:
                        styler = styler.applymap(color_momentum, subset=momentum_cols)
                styled_df = styler.format(format_dict)
            except Exception as e:
                # 모바일/서버 런타임에서 Styler 호환 이슈 시 plain dataframe으로 안전하게 대체
                print(f"[WARN] 스크리너 Styler 적용 실패, 기본 테이블로 대체: {e}")
                styled_df = style_target

            base_columns = ["_index", "테마표시", "랭킹추세", "AI수급점수", "신호등급", "신호신뢰도", "점수변화(안정화)", "현재가", "등락률", "시가총액", "소속"]
            advanced_columns = ["외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속"]
            current_columns = base_columns + advanced_columns if show_advanced else base_columns

            event = st.dataframe(
                styled_df, on_select="rerun", selection_mode="single-row",
                column_config={
                    "_index": st.column_config.TextColumn("종목명", width="small"), 
                    "테마표시": st.column_config.Column("테마", width="medium"), 
                    "랭킹추세": st.column_config.Column("순위변동", width="small"), 
                    "AI수급점수": st.column_config.NumberColumn("🏆 AI점수", width="small", format="%.2f"),
                    "신호등급": st.column_config.Column("신호등급", width="small"),
                    "신호신뢰도": st.column_config.NumberColumn("신뢰도", width="small"),
                    "점수변화(안정화)": st.column_config.NumberColumn("안정화Δ", width="small"),
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
                hide_index=False, width='stretch', height=250 if not is_vip else 600
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
        render_section_header("종목 분석", "핵심 점수와 실행 신호를 먼저 확인하고, 상세 분석은 펼쳐서 점검합니다.", badge_text="Focused View")
        free_tier_stocks = df_summary.head(5)['종목명'].values
        stock_list = df_summary['종목명'].tolist()
        
        if "selected_stock" not in st.session_state or st.session_state.selected_stock not in stock_list:
            st.session_state.selected_stock = stock_list[0]
            
        if "stock_selector" not in st.session_state or st.session_state.stock_selector not in stock_list:
            st.session_state.stock_selector = st.session_state.selected_stock

        def on_stock_change():
            st.session_state.selected_stock = st.session_state.stock_selector

        st.selectbox(
            "분석할 종목을 검색/선택하세요",
            options=stock_list,
            key="stock_selector",
            on_change=on_stock_change
        )
        target_stock = st.session_state.stock_selector
        
        selected_row = df_summary[df_summary['종목명'] == target_stock].iloc[0]
        
        sector_name = safe_get(selected_row, '테마표시', safe_get(selected_row, '섹터', '분류안됨'))
        cur_rank = safe_get(selected_row, '현재_순위', 0)
        selected_rank = int(pd.to_numeric(cur_rank, errors="coerce") or 0)
        selected_name_cls = {1: "podium-name-1", 2: "podium-name-2", 3: "podium-name-3"}.get(selected_rank, "")
        ai_score = safe_get(selected_row, 'AI수급점수', 0)
        quant_score = float(safe_get(selected_row, 'Quant점수', ai_score))
        qual_score = float(safe_get(selected_row, '정성점수', 50))
        qual_adj = float(safe_get(selected_row, '정성보정치', 0))
        score_mode = safe_get(selected_row, '점수모드', '기본')
        rank_trend = safe_get(selected_row, '랭킹추세', '-')
        signal_grade = safe_get(selected_row, '신호등급', '-')
        signal_conf = float(safe_get(selected_row, '신호신뢰도', 0))
        score_delta = float(safe_get(selected_row, '점수변화(안정화)', 0))
        marcap = safe_get(selected_row, '시가총액', 0)
        per_val = safe_get(selected_row, 'PER', 0.0)
        roe_val = safe_get(selected_row, 'ROE', 0.0)
        gap_20 = safe_get(selected_row, '이격도(%)', 100)
        target_code = safe_get(selected_row, '종목코드', '')
        cur_price = float(safe_get(selected_row, '현재가', 0))
        day_chg = float(safe_get(selected_row, '등락률', 0))
        day_chg_color = "#FF4B4B" if day_chg > 0 else "#3B82F6" if day_chg < 0 else "#A0A0A0"
        day_chg_text = f"+{day_chg:.2f}%" if day_chg > 0 else f"{day_chg:.2f}%"

        title_html = dedent(
            f"""
            <div class="stock-title-wrap">
                <h2 class="{selected_name_cls}" style="margin: 0; color: #FFFFFF;">{target_stock}</h2>
                <span class="stock-sector-chip">{sector_name}</span>
                <span style="padding:4px 10px; border-radius:16px; background:#242735; color:#D6DAE5; font-size:0.82em;">현재가 {cur_price:,.0f}원</span>
                <span style="padding:4px 10px; border-radius:16px; background:#242735; color:{day_chg_color}; font-size:0.82em; font-weight:700;">당일 {day_chg_text}</span>
            </div>
            """
        ).strip()
        st.markdown(title_html, unsafe_allow_html=True)
        st.markdown(
            f"""
            <div class="premium-panel">
                <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap;">
                    <div style="color:#E5E7EB; font-size:0.95em; font-weight:800;">실전 해설</div>
                    <div style="color:#9CA3AF; font-size:0.78em;">모바일은 터치 기반으로 아래 해설을 확인하세요</div>
                </div>
                <div class="premium-chip-row">
                    <span class="premium-chip">신호등급 {signal_grade}</span>
                    <span class="premium-chip">신호신뢰도 {signal_conf:.1f}</span>
                    <span class="premium-chip">안정화Δ {score_delta:+.2f}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("지표 해설", expanded=False):
            st.markdown(
                """
                <div style="background:linear-gradient(135deg,#121827,#0f172a); border:1px solid #2B364C; border-radius:14px; padding:12px 14px; margin:2px 0 4px 0;">
                    <div style="color:#E5E7EB; font-weight:800; font-size:0.98em; margin-bottom:8px;">종목 분석 지표 해설</div>
                    <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:8px;">
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#A5B4FC; font-size:0.78em; font-weight:700;">신호등급</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">High/Medium/Low. VIX 레짐별 기준으로 등급이 조정됩니다.</div>
                        </div>
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#93C5FD; font-size:0.78em; font-weight:700;">신호신뢰도</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">Quant/AI/정성/부정뉴스를 합친 0~100 신뢰 점수입니다.</div>
                        </div>
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#86EFAC; font-size:0.78em; font-weight:700;">안정화Δ</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">전일 대비 스무딩 적용 후 점수 변화량입니다.</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
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
            display_final = round(float(ai_score), 1)

            trend_map = {"외인": make_trend_svg([]), "연기금": make_trend_svg([]), "투신": make_trend_svg([]), "사모": make_trend_svg([])}
            signed_streak = {"외인": f_streak, "연기금": p_streak, "투신": 0, "사모": 0}
            if not df_history.empty and {"종목명", "일자", "외인", "연기금", "투신", "사모"}.issubset(df_history.columns):
                h = df_history[df_history["종목명"] == target_stock].copy()
                if not h.empty:
                    raw_dates = h["일자"].astype(str).str.replace("-", "", regex=False).str.strip()
                    h["일자_dt"] = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce")
                    if h["일자_dt"].notna().sum() == 0:
                        h["일자_dt"] = pd.to_datetime(h["일자"], errors="coerce")
                    h = h.dropna(subset=["일자_dt"]).sort_values("일자_dt")
                    for col in ["외인", "연기금", "투신", "사모"]:
                        vals = pd.to_numeric(h[col], errors="coerce").dropna().tolist()[-8:]
                        trend_map[col] = make_trend_svg(vals, width=96, height=24)
                    signed_streak["외인"] = calc_signed_streak(pd.to_numeric(h["외인"], errors="coerce").tolist()[-20:])
                    signed_streak["연기금"] = calc_signed_streak(pd.to_numeric(h["연기금"], errors="coerce").tolist()[-20:])
                    signed_streak["투신"] = calc_signed_streak(pd.to_numeric(h["투신"], errors="coerce").tolist()[-20:])
                    signed_streak["사모"] = calc_signed_streak(pd.to_numeric(h["사모"], errors="coerce").tolist()[-20:])

            def _streak_text(v):
                if v > 0:
                    return f"매수 {v}일"
                if v < 0:
                    return f"매도 {abs(v)}일"
                return "중립"

            st.markdown(
                f"""
                <div class="hero-grid">
                    <div class="hero-card">
                        <div class="hero-label">핵심 점수</div>
                        <div class="hero-value">{display_final:.1f}점</div>
                        <div class="hero-sub">전체 {int(cur_rank)}위 · {rank_trend}</div>
                    </div>
                    <div class="hero-card">
                        <div class="hero-label">실행 신호</div>
                        <div class="hero-value">{signal_grade}</div>
                        <div class="hero-sub">신뢰도 {signal_conf:.1f} · 안정화Δ {score_delta:+.2f}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
            st.markdown(
                f"""
                <div class="trend-strip">
                    <div class="trend-item">
                        <div class="trend-name">외인 {f_str_val:+.1f}% · {_streak_text(signed_streak['외인'])}</div>
                        <div class="trend-line">{trend_map['외인']}</div>
                    </div>
                    <div class="trend-item">
                        <div class="trend-name">연기금 {p_str_val:+.1f}% · {_streak_text(signed_streak['연기금'])}</div>
                        <div class="trend-line">{trend_map['연기금']}</div>
                    </div>
                    <div class="trend-item">
                        <div class="trend-name">투신 {t_str_val:+.1f}% · {_streak_text(signed_streak['투신'])}</div>
                        <div class="trend-line">{trend_map['투신']}</div>
                    </div>
                    <div class="trend-item">
                        <div class="trend-name">사모 {pef_str_val:+.1f}% · {_streak_text(signed_streak['사모'])}</div>
                        <div class="trend-line">{trend_map['사모']}</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.caption("라인 색상: 최근 흐름이 시작점 대비 상승이면 초록, 하락이면 빨강입니다.")

            with st.expander("상세 분석 펼치기", expanded=False):
                st.markdown(
                    f"""
                    <div class="stock-grid">
                        <div class="stock-card">
                            <div class="stock-label">🏆 AI 점수</div>
                            <div class="stock-value">{float(ai_score):.1f}점</div>
                            <div class="stock-sub">전체 {int(cur_rank)}위 / {rank_trend} / 신호 {signal_grade}({signal_conf:.1f})</div>
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
                            <div class="stock-sub">{tech_status} · 안정화Δ {score_delta:+.2f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                st.markdown("##### 점수 산출 구조")
                gauge = go.Figure(go.Indicator(
                    mode="gauge",
                    value=display_final,
                    gauge={
                        "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#64748B", "tickfont": {"size": 10}},
                        "bar": {"color": "#A78BFA", "thickness": 0.42},
                        "bgcolor": "#0F172A",
                        "borderwidth": 0,
                        "steps": [
                            {"range": [0, 40], "color": "#2B3445"},
                            {"range": [40, 70], "color": "#334155"},
                            {"range": [70, 100], "color": "#475569"}
                        ],
                    }
                ))
                gauge.update_layout(
                    height=145,
                    margin=dict(l=8, r=8, t=8, b=2),
                    paper_bgcolor="rgba(0,0,0,0)",
                    font={"color": "#E5E7EB"},
                    annotations=[{
                        "x": 0.5, "y": 0.13, "xref": "paper", "yref": "paper",
                        "text": f"<b>{display_final:.1f}점</b>",
                        "showarrow": False, "font": {"size": 24, "color": "#E5E7EB"}
                    }]
                )
                st.plotly_chart(gauge, width='stretch')
                st.markdown(
                    f"""
                    <div class="score-kpi-grid">
                        <div class="score-kpi">
                            <div class="score-kpi-label">최종 점수</div>
                            <div class="score-kpi-value">{display_final:.1f}</div>
                        </div>
                        <div class="score-kpi">
                            <div class="score-kpi-label">정량 점수</div>
                            <div class="score-kpi-value">{quant_score:.1f}</div>
                        </div>
                        <div class="score-kpi">
                            <div class="score-kpi-label">정성 보정</div>
                            <div class="score-kpi-value">{qual_adj:+.1f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                st.caption(f"정성 점수 {qual_score:.1f} | {score_mode}")

                st.markdown("##### 최근 1개월 수급 강도")
                st.markdown(
                    f"""
                    <div class="stock-grid">
                        <div class="stock-card">
                            <div class="stock-label">🔴 외인 강도</div>
                            <div class="stock-value">{f_str_val:.1f}%</div>
                            <div class="stock-sub">{_streak_text(signed_streak['외인'])}</div>
                        </div>
                        <div class="stock-card">
                            <div class="stock-label">🔵 연기금 강도</div>
                            <div class="stock-value">{p_str_val:.1f}%</div>
                            <div class="stock-sub">{_streak_text(signed_streak['연기금'])}</div>
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

                    # 주체별 최근 1주/1개월 순매입 금액(백만 원) 및 시총 대비 비중
                    wk_slice = target_hist.tail(5)
                    mo_slice = target_hist.tail(20)
                    marcap_in_million = float(marcap) * 100 if marcap else 0.0  # 시총(억원) -> 백만원

                    flow_rows = []
                    for investor in ["외인", "연기금", "투신", "사모"]:
                        wk_amt = float(wk_slice[investor].sum()) if investor in wk_slice.columns else 0.0
                        mo_amt = float(mo_slice[investor].sum()) if investor in mo_slice.columns else 0.0
                        wk_ratio = (wk_amt / marcap_in_million) * 100 if marcap_in_million > 0 else 0.0
                        mo_ratio = (mo_amt / marcap_in_million) * 100 if marcap_in_million > 0 else 0.0
                        flow_rows.append({
                            "주체": investor,
                            "1주 순매입(백만 원)": wk_amt,
                            "1개월 순매입(백만 원)": mo_amt,
                            "시총 대비(1주)": wk_ratio,
                            "시총 대비(1개월)": mo_ratio
                        })

                    df_flow = pd.DataFrame(flow_rows)

                    def calc_consecutive_signed_days(df_hist_local, investor_col):
                        if investor_col not in df_hist_local.columns:
                            return 0
                        streak = 0
                        direction = 0
                        # 최신일 기준으로 연속 순매수/순매도 일수를 계산
                        for val in df_hist_local.sort_values('일자', ascending=False)[investor_col].tolist():
                            if pd.isna(val):
                                break
                            sign = 1 if float(val) > 0 else (-1 if float(val) < 0 else 0)
                            if sign == 0:
                                break
                            if direction == 0:
                                direction = sign
                                streak = 1
                            elif sign == direction:
                                streak += 1
                            else:
                                break
                        return direction * streak

                    streak_df = pd.DataFrame([
                        {"주체": "외인", "연속순수급일": calc_consecutive_signed_days(target_hist, "외인")},
                        {"주체": "연기금", "연속순수급일": calc_consecutive_signed_days(target_hist, "연기금")},
                        {"주체": "투신", "연속순수급일": calc_consecutive_signed_days(target_hist, "투신")},
                        {"주체": "사모", "연속순수급일": calc_consecutive_signed_days(target_hist, "사모")},
                    ])
                    streak_df["표시"] = streak_df["연속순수급일"].apply(
                        lambda x: f"매수 {int(x)}일" if x > 0 else (f"매도 {abs(int(x))}일" if x < 0 else "중립")
                    )
                    streak_df["강도"] = streak_df["연속순수급일"].abs().apply(
                        lambda x: "strong" if x >= 5 else ("mid" if x >= 2 else ("weak" if x >= 1 else "none"))
                    )
                    streak_df["방향"] = streak_df["연속순수급일"].apply(lambda x: "buy" if x > 0 else ("sell" if x < 0 else "flat"))

                    col1, col2 = st.columns(2)
                    color_scale = alt.Scale(domain=['외인', '연기금', '투신', '사모'], range=['#36C06A', '#E04B4B', '#3BA7FF', '#B08CFF'])
                    with col1:
                        st.markdown("##### 20일 종가 추이")
                        st.altair_chart(
                            apply_altair_theme(
                                alt.Chart(target_hist).mark_line(color='#36C06A', point=True).encode(
                                    x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                                    y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)
                                ).properties(height=280)
                            ),
                            width='stretch'
                        )
                    with col2:
                        chart_mode_options = ["연속 순수급", "1주 강도(시총대비)", "1개월 강도(시총대비)"]
                        st.markdown(
                            "<div style='color:#94A3B8; font-size:0.78em; margin-bottom:4px;'>수급 차트 모드</div>",
                            unsafe_allow_html=True,
                        )
                        if hasattr(st, "segmented_control"):
                            chart_mode = st.segmented_control(
                                "수급 차트 모드",
                                options=chart_mode_options,
                                default=chart_mode_options[0],
                                key="detail_flow_chart_mode",
                                label_visibility="collapsed",
                            )
                            if not chart_mode:
                                chart_mode = chart_mode_options[0]
                        else:
                            chart_mode = st.radio(
                                "수급 차트 모드",
                                chart_mode_options,
                                horizontal=True,
                                key="detail_flow_chart_mode_fallback",
                                label_visibility="collapsed"
                            )

                        if chart_mode == "연속 순수급":
                            chart_df = streak_df.copy()
                            val_col = "연속순수급일"
                            label_col = "표시"
                            x_title = "연속 순수급일"
                        elif chart_mode == "1주 강도(시총대비)":
                            chart_df = df_flow.copy()
                            chart_df["값"] = pd.to_numeric(chart_df["시총 대비(1주)"], errors="coerce").fillna(0.0)
                            chart_df["표시"] = chart_df["값"].apply(lambda v: f"{v:+.2f}%")
                            chart_df["방향"] = chart_df["값"].apply(lambda v: "buy" if v > 0 else ("sell" if v < 0 else "flat"))
                            val_col = "값"
                            label_col = "표시"
                            x_title = "시총 대비 1주 순수급(%)"
                        else:
                            chart_df = df_flow.copy()
                            chart_df["값"] = pd.to_numeric(chart_df["시총 대비(1개월)"], errors="coerce").fillna(0.0)
                            chart_df["표시"] = chart_df["값"].apply(lambda v: f"{v:+.2f}%")
                            chart_df["방향"] = chart_df["값"].apply(lambda v: "buy" if v > 0 else ("sell" if v < 0 else "flat"))
                            val_col = "값"
                            label_col = "표시"
                            x_title = "시총 대비 1개월 순수급(%)"

                        abs_max = float(pd.to_numeric(chart_df[val_col], errors="coerce").abs().max()) if not chart_df.empty else 1.0
                        abs_max = max(1.0, round(abs_max, 2))
                        x_domain = [-abs_max, abs_max]

                        st.markdown(f"##### 주체별 {chart_mode}")
                        bar = alt.Chart(chart_df).mark_bar().encode(
                            y=alt.Y('주체:N', sort=['외인', '연기금', '투신', '사모'], axis=alt.Axis(title=None)),
                            x=alt.X(
                                f'{val_col}:Q',
                                axis=alt.Axis(title=x_title, tickMinStep=1),
                                scale=alt.Scale(domain=x_domain)
                            ),
                            color=alt.Color(
                                "방향:N",
                                scale=alt.Scale(
                                    domain=["sell", "flat", "buy"],
                                    range=["#E04B4B", "#64748B", "#36C06A"]
                                ),
                                legend=None
                            ),
                            tooltip=[alt.Tooltip('주체:N'), alt.Tooltip(f'{label_col}:N', title='값')]
                        ).properties(height=280)
                        zero_rule = alt.Chart(pd.DataFrame({"x": [0]})).mark_rule(
                            color="#94A3B8", strokeDash=[5, 4], opacity=0.7
                        ).encode(x="x:Q")
                        text = alt.Chart(chart_df).mark_text(
                            align='left', baseline='middle', dx=6, color='#E5E7EB'
                        ).encode(
                            y=alt.Y('주체:N', sort=['외인', '연기금', '투신', '사모']),
                            x=alt.X(f'{val_col}:Q', scale=alt.Scale(domain=x_domain)),
                            text=f'{label_col}:N'
                        )
                        st.altair_chart(apply_altair_theme(zero_rule + bar + text), width='stretch')

                    with st.expander("주체별 순매입 현황 표 (1주 / 1개월)", expanded=False):
                        st.dataframe(
                            df_flow.style.format({
                                "1주 순매입(백만 원)": "{:+,.0f}",
                                "1개월 순매입(백만 원)": "{:+,.0f}",
                                "시총 대비(1주)": "{:+.2f}%",
                                "시총 대비(1개월)": "{:+.2f}%"
                            }),
                            hide_index=True,
                            width='stretch'
                        )

                    with st.expander("주체별 순매수 대금 추이(백만 원)"):
                        amount_df = target_hist.melt(
                            id_vars=['일자_표시'],
                            value_vars=['외인', '연기금', '투신', '사모'],
                            var_name='투자자',
                            value_name='금액'
                        )
                        st.altair_chart(
                            apply_altair_theme(
                                alt.Chart(amount_df).mark_bar().encode(
                                    x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                                    y=alt.Y('금액:Q', title=None),
                                    color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                                    order=alt.Order('투자자:N', sort='descending')
                                ).properties(height=280)
                            ),
                            width='stretch'
                        )

            st.markdown("---")

            st.markdown("##### QEdge 종목 진단")
            st.caption("최신 시황 뉴스와 종목 데이터를 바탕으로 AI가 종목별 핵심 포인트를 정리합니다.")

            if st.button(f"'{target_stock}' 뉴스·시황 리포트 생성", use_container_width=True):
                if not client:
                    st.error("AI용 API 키가 설정되지 않았습니다.")
                else:
                    with st.spinner("뉴스/시황/공시/리포트를 수집해 분석을 준비하고 있습니다..."):
                        macro_news = get_macro_headline_news()
                        news_list = get_naver_news(target_stock)
                        event_context = get_stock_disclosure_report_context(target_stock, target_code)
                        
                        st.markdown("###### 분석에 사용한 데이터")
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
                        내가 제공하는 아래의 [팩트 데이터]만을 기반으로 종목명 '{target_stock}'(테마: {sector_name})에 대한 심층 브리핑을 작성해.
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

                        [팩트 데이터: 최근 공시 및 증권사 리포트]
                        {event_context}
                        
                        [출력 양식]
                        1. 📰 최신 모멘텀 요약 (종목 뉴스 요약본 기반 구체적 분석)
                        2. 🌍 매크로 시황 연계 분석 (거시 경제 주요 뉴스와 종목의 연관성 및 방향성)
                        3. 💡 수급 및 펀더멘털 평가 (PER, ROE, 기관/외인 수급 해석)
                        4. 🧾 공시/리포트 체크포인트 (최근 발행 데이터가 시사하는 기회/리스크)
                        5. 🎯 단기 투자 전략 및 리스크 관리 (시장 분위기와 이격도를 종합적으로 고려)
                        """
                        try:
                            response = client.models.generate_content_stream(
                                model='gemma-4-31b-it',
                                contents=prompt
                            )

                            st.success("분석이 완료되었습니다.")
                            def stream_generator():
                                for chunk in response:
                                    if chunk.text: yield chunk.text

                            with st.container():
                                st.write_stream(stream_generator)

                        except Exception as e:
                            st.error(f"분석 중 오류 발생: {e}")

    # --- 탭 5: 백테스트 ---
    with tab5:
        render_section_header("QEdge 모델 가상 포트폴리오 백테스트", "수익률뿐 아니라 낙폭(MDD)과 리스크 상태까지 함께 확인합니다.", badge_text="Backtest")
        st.markdown(
            """
            <div style="display:flex; gap:10px; flex-wrap:wrap; margin:4px 0 10px 0;">
                <span title="MDD(최대낙폭): 누적수익 곡선이 고점 대비 얼마나 내려왔는지 보여주는 핵심 리스크 지표입니다." style="cursor:help; color:#FCA5A5; font-size:0.86em;">ℹ️ MDD</span>
                <span title="리스크상태: 최대낙폭 기반으로 Low/Medium/High를 표시합니다. High는 변동성 방어가 필요한 구간입니다." style="cursor:help; color:#FCD34D; font-size:0.86em;">ℹ️ 리스크상태</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.expander("리스크 해설", expanded=False):
            st.markdown(
                """
                <div style="background:linear-gradient(135deg,#121827,#0f172a); border:1px solid #2B364C; border-radius:14px; padding:12px 14px; margin:2px 0 4px 0;">
                    <div style="color:#E5E7EB; font-weight:800; font-size:0.98em; margin-bottom:8px;">백테스트 리스크 해설</div>
                    <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:8px;">
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#FCA5A5; font-size:0.78em; font-weight:700;">MDD (최대낙폭)</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">누적수익이 고점 대비 얼마나 하락했는지 보여주는 핵심 리스크 지표입니다.</div>
                        </div>
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#FCD34D; font-size:0.78em; font-weight:700;">리스크상태</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">MDD 기반 위험 단계(Low/Medium/High)이며 High는 방어 우선 구간입니다.</div>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        if not is_vip:
            show_premium_paywall("가상 포트폴리오 누적 수익률 및 성과 분석은 코드 인증 후 확인할 수 있습니다.")
        else:
            if csv_exists("performance_trend.csv") or table_exists("performance_trend"):
                df_perf = load_performance_trend_safe()
                if not df_perf.empty:
                    df_perf['날짜_dt'] = pd.to_datetime(df_perf['날짜'])
                    min_date = df_perf['날짜_dt'].min().date()
                    max_date = df_perf['날짜_dt'].max().date()
                    
                    selected_start_date = st.date_input("🗓️ 벤치마크 시작(기준)일 선택", min_value=min_date, max_value=max_date, value=min_date)
                    df_filtered = df_perf[df_perf['날짜_dt'].dt.date >= selected_start_date].copy()
                    # 주말/공휴일 제거: history.csv의 실제 거래일 기준 우선, 없으면 주말만 제거
                    if not df_filtered.empty:
                        if not df_history.empty and '일자' in df_history.columns:
                            raw_dates = df_history['일자'].astype(str).str.replace("-", "", regex=False).str.strip()
                            # history.csv 일자 포맷은 보통 YYYYMMDD 이므로 해당 포맷 우선 파싱
                            trading_dates = pd.to_datetime(raw_dates, format="%Y%m%d", errors='coerce')
                            if trading_dates.notna().sum() == 0:
                                # 예외 케이스(YYYY-MM-DD 등) fallback
                                trading_dates = pd.to_datetime(df_history['일자'], errors='coerce')
                            trading_dates = trading_dates.dt.normalize().dropna()
                            trading_date_set = set(trading_dates.astype(str))
                            df_filtered = df_filtered[df_filtered['날짜_dt'].dt.normalize().astype(str).isin(trading_date_set)].copy()
                            # 거래일 매칭이 실패하면 차트 전체 소실을 막기 위해 주말 필터로 안전 대체
                            if df_filtered.empty:
                                df_filtered = df_perf[df_perf['날짜_dt'].dt.date >= selected_start_date].copy()
                                df_filtered = df_filtered[df_filtered['날짜_dt'].dt.weekday < 5].copy()
                        else:
                            df_filtered = df_filtered[df_filtered['날짜_dt'].dt.weekday < 5].copy()
                    
                    if not df_filtered.empty:
                        base_port_ret = df_filtered.iloc[0]['누적수익률']
                        df_filtered['조정_포트수익률'] = df_filtered['누적수익률'] - base_port_ret
                        
                        benchmark_fetch_errors = []
                        try:
                            def compute_benchmark_returns(ticker_symbol):
                                # 1) Yahoo Chart API 직접 호출(환경별 yfinance 빈응답 이슈 회피)
                                hist = fetch_yahoo_chart_history(ticker_symbol, range_period="2y", interval="1d")
                                # 2) direct 호출 실패 시 yfinance fallback
                                if hist.empty:
                                    hist = yf.Ticker(ticker_symbol).history(period="2y")
                                if hist.empty:
                                    benchmark_fetch_errors.append(ticker_symbol)
                                    return [float("nan")] * len(df_filtered)

                                idx = hist.index
                                if getattr(idx, "tz", None) is not None:
                                    idx = idx.tz_localize(None)
                                hist.index = idx.normalize()
                                hist = hist.dropna(subset=['Close'])
                                if hist.empty:
                                    benchmark_fetch_errors.append(ticker_symbol)
                                    return [float("nan")] * len(df_filtered)

                                base_df = hist[hist.index <= pd.to_datetime(selected_start_date)]
                                base_close = float(base_df['Close'].dropna().iloc[-1]) if not base_df.empty and not base_df['Close'].dropna().empty else None
                                if base_close is None or base_close == 0:
                                    benchmark_fetch_errors.append(ticker_symbol)
                                    return [float("nan")] * len(df_filtered)

                                rets = []
                                for d in df_filtered['날짜_dt']:
                                    sub = hist[hist.index <= d]
                                    close_series = sub['Close'].dropna() if not sub.empty else pd.Series(dtype=float)
                                    if not close_series.empty:
                                        val = float(close_series.iloc[-1])
                                        ret = ((val - base_close) / base_close) * 100
                                        if pd.isna(ret):
                                            ret = rets[-1] if rets else float("nan")
                                        rets.append(ret)
                                    else:
                                        rets.append(rets[-1] if rets else float("nan"))
                                return rets

                            df_filtered['KOSPI 누적수익률'] = compute_benchmark_returns('^KS11')
                        except Exception as e:
                            print(f"[WARN] 벤치마크 수익률 계산 실패(^KS11): {e}")
                            df_filtered['KOSPI 누적수익률'] = [float("nan")] * len(df_filtered)
                            benchmark_fetch_errors = ['^KS11']
                        
                        def _safe_last(series):
                            val = series.iloc[-1] if len(series) > 0 else float("nan")
                            return 0.0 if pd.isna(val) else float(val)

                        def _safe_daily_diff(series):
                            if len(series) <= 1:
                                return 0.0
                            a, b = series.iloc[-1], series.iloc[-2]
                            if pd.isna(a) or pd.isna(b):
                                return 0.0
                            return float(a - b)

                        current_port_ret = _safe_last(df_filtered['조정_포트수익률'])
                        current_kospi_ret = _safe_last(df_filtered['KOSPI 누적수익률'])
                        current_mdd = float(df_filtered["최대낙폭(%)"].min()) if "최대낙폭(%)" in df_filtered.columns else 0.0
                        current_risk_state = str(df_filtered["리스크상태"].iloc[-1]) if "리스크상태" in df_filtered.columns else "-"

                        port_daily_diff = _safe_daily_diff(df_filtered['조정_포트수익률'])
                        kospi_daily_diff = _safe_daily_diff(df_filtered['KOSPI 누적수익률'])

                        alpha_kospi = current_port_ret - current_kospi_ret
                        alpha_color = "#36C06A" if alpha_kospi >= 0 else "#E04B4B"
                        port_delta_color = "#36C06A" if port_daily_diff >= 0 else "#E04B4B"
                        kospi_delta_color = "#36C06A" if kospi_daily_diff >= 0 else "#E04B4B"
                        trading_days = len(df_filtered)

                        st.markdown(
                            f"""
                            <div class="kpi-grid">
                                <div class="kpi-card">
                                    <div class="kpi-title">QEdge 누적 수익률</div>
                                    <div class="kpi-value">{current_port_ret:+.2f}%</div>
                                    <span class="kpi-delta" style="background: rgba(54,192,106,0.18); color:{port_delta_color};">일간 {port_daily_diff:+.2f}%</span>
                                    <div class="kpi-meta">시작일 이후 {trading_days}거래일</div>
                                </div>
                                <div class="kpi-card">
                                    <div class="kpi-title">KOSPI 누적 수익률</div>
                                    <div class="kpi-value">{current_kospi_ret:+.2f}%</div>
                                    <span class="kpi-delta" style="background: rgba(59,130,246,0.16); color:{kospi_delta_color};">일간 {kospi_daily_diff:+.2f}%</span>
                                    <div class="kpi-meta">초과 성과 <span style="color:{alpha_color}; font-weight:700;">{alpha_kospi:+.2f}%p</span></div>
                                </div>
                                <div class="kpi-card">
                                    <div class="kpi-title">리스크 지표 (MDD)</div>
                                    <div class="kpi-value">{current_mdd:.2f}%</div>
                                    <span class="kpi-delta" style="background: rgba(224,75,75,0.16); color:#FCA5A5;">상태 {current_risk_state}</span>
                                    <div class="kpi-meta">낙폭 기반 안정성 모니터링</div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                        
                        st.markdown("<br>", unsafe_allow_html=True)
                        
                        df_filtered['날짜_표시'] = df_filtered['날짜_dt'].dt.strftime('%m/%d')
                        df_melt = df_filtered.melt(
                            id_vars=['날짜_표시'],
                            value_vars=['조정_포트수익률', 'KOSPI 누적수익률'],
                            var_name='포트폴리오',
                            value_name='수익률(%)'
                        )
                        
                        base_chart = alt.Chart(df_melt).mark_line(point=True).encode(
                            x=alt.X('날짜_표시:O', axis=alt.Axis(title=None, labelAngle=-45)),
                            y=alt.Y('수익률(%):Q', title="누적 수익률 (%)"),
                            color=alt.Color(
                                '포트폴리오:N',
                                scale=alt.Scale(
                                    domain=['조정_포트수익률', 'KOSPI 누적수익률'],
                                    range=['#E74C3C', '#AAAAAA']
                                ),
                                legend=alt.Legend(title=None, orient='bottom')
                            )
                        ).properties(height=300)

                        st.altair_chart(apply_altair_theme(base_chart), width='stretch')
                        if benchmark_fetch_errors:
                            labels = {
                                '^KS11': 'KOSPI'
                            }
                            err_names = ", ".join(labels.get(x, x) for x in sorted(set(benchmark_fetch_errors)))
                            st.caption(f"일부 벤치마크 데이터가 지연되어 표시되지 않았습니다: {err_names}")

                        if csv_exists("score_trend.csv") or table_exists("score_trend"):
                            df_rank = load_score_trend_safe()
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
                                    with st.expander("날짜별 Top3 구성 종목 및 성과", expanded=False):
                                        st.dataframe(pd.DataFrame(rank_rows), hide_index=True, width='stretch')
                    else:
                        render_empty_state("백테스트 데이터 없음", "선택하신 기간에 해당하는 데이터가 없습니다.")
                else:
                    render_empty_state("데이터 대기", "백테스트 데이터를 준비 중입니다.")
            else:
                render_empty_state("데이터 대기", "백테스트 파일이 아직 생성되지 않았습니다.")

    # --- 탭 6: 리더스 페어 분석 ---
    with tab6:
        render_section_header("리더스 페어 분석", "두 종목의 뉴스/수급/정량 지표를 교차 비교해 상대 우위를 평가합니다.")
        st.caption("두 종목의 뉴스, 시황, 퀀트 데이터를 교차 검토해 단기 상대 우위를 비교합니다.")

        if not is_vip:
            show_premium_paywall("AI 기반 다중 종목 비교 분석 기능은 코드 인증 후 이용할 수 있습니다.")
        else:
            if not client:
                st.error("⚠️ Streamlit Secrets에 GEMINI_API_KEY가 설정되지 않아 비교 분석을 사용할 수 없습니다.")
            else:
                stock_list_full = df_summary['종목명'].tolist()
                if "leaders_pair_warn" not in st.session_state:
                    st.session_state["leaders_pair_warn"] = False

                def _enforce_pair_limit():
                    selected = st.session_state.get("leaders_pair_multiselect", [])
                    if len(selected) > 2:
                        st.session_state["leaders_pair_multiselect"] = selected[:2]
                        st.session_state["leaders_pair_warn"] = True

                matchup_stocks = st.multiselect(
                    "비교할 종목 2개를 선택하세요",
                    options=stock_list_full,
                    key="leaders_pair_multiselect",
                    on_change=_enforce_pair_limit
                )
                matchup_stocks = st.session_state.get("leaders_pair_multiselect", matchup_stocks)
                if st.session_state.get("leaders_pair_warn", False):
                    st.warning("비교는 2개 종목만 가능합니다. 최근 선택 종목은 제외했습니다.")
                    st.session_state["leaders_pair_warn"] = False
                ready_to_run = len(matchup_stocks) == 2
                st.caption(f"선택 상태: {len(matchup_stocks)}/2")
                if st.button("비교 분석 시작", use_container_width=True, type="primary", disabled=not ready_to_run):
                    if ready_to_run:
                        with st.spinner("선택 종목의 뉴스/시황/참고자료를 수집하고 있습니다..."):
                            macro_news = get_macro_headline_news()
                            matchup_data = []
                            evidence_lines = []
                            for ms in matchup_stocks:
                                s_row = df_summary[df_summary['종목명'] == ms].iloc[0]
                                n_news = get_naver_news(ms)
                                stock_code = safe_get(s_row, '종목코드', '')
                                event_context = get_stock_disclosure_report_context(ms, stock_code)
                                
                                matchup_data.append(f"""
                                === [후보 종목: {ms}] ===
                                - 테마: {safe_get(s_row, '테마표시', safe_get(s_row, '섹터', '분류안됨'))} / AI점수: {safe_get(s_row, 'AI수급점수', 0)}점
                                - 이격도: {safe_get(s_row, '이격도(%)', 100)}% / 외국인연속: {safe_get(s_row, '외인연속', 0)}일 / 연기금연속: {safe_get(s_row, '연기금연속', 0)}일
                                - 최근 뉴스 (요약 포함): {chr(10).join(n_news[:3]) if n_news else '없음'}
                                - 최근 공시/리포트: {event_context}
                                """)
                                evidence_lines.append(f"[{ms}]")
                                for n in n_news[:3]:
                                    evidence_lines.append(f"- {n}")
                                if event_context and event_context != "최근 공시/리포트 데이터 없음":
                                    for line in str(event_context).splitlines():
                                        evidence_lines.append(f"  {line}")
                            
                            combined_data_str = "\n".join(matchup_data)
                            with st.expander("비교 분석에 사용한 뉴스/참고자료 보기"):
                                st.write("**[오늘의 시황 뉴스]**")
                                for mn in macro_news:
                                    st.caption(f"- {mn}")
                                if not macro_news:
                                    st.caption("- 시황 뉴스 없음")
                                st.write("**[종목별 참고 자료]**")
                                for line in evidence_lines:
                                    st.caption(line)
                            
                            prompt = f"""
                            너는 QEdge 수석 퀀트 애널리스트야. 내가 아래에 제공한 2개 종목의 [후보 종목 데이터]를 비교 분석해.
                            또한, [오늘의 주요 시황 뉴스]를 바탕으로 글로벌 매크로 환경을 고려했을 때 현재 단기 관점에서 어떤 종목이 가장 유리한지 결론과 근거를 설명해.
                            
                            [팩트 데이터: 오늘의 주요 시황 뉴스]
                            {chr(10).join(macro_news) if macro_news else "시황 뉴스 없음"}
                            
                            [후보 종목 데이터]
                            {combined_data_str}
                            
                            다음 양식으로 답변해줘:
                            🏆 **최종 선택 종목**: (종목명)
                            🔍 **선정 이유**: (수급, 이격도, 종목 뉴스/공시/리포트, 시황을 종합하여 3~4줄로 핵심만 요약)
                            ⚖️ **탈락 종목 코멘트**: (왜 승자보다 아쉬운지 짧게 분석)
                            📚 **참고 출처**: (분석에 반영한 뉴스/공시/리포트 제목을 핵심만 bullet로 명시)
                            """
                            
                            try:
                                response = client.models.generate_content_stream(
                                    model='gemma-4-31b-it',
                                    contents=prompt
                                )
                                st.success("비교 분석이 완료되었습니다.")
                                def stream_generator():
                                    for chunk in response:
                                        if chunk.text: yield chunk.text
                                st.write_stream(stream_generator)
                            except Exception as e:
                                st.error(f"분석 중 오류 발생: {e}")
                if not ready_to_run:
                    st.info("비교 분석을 위해 종목 2개를 선택해주세요.")

    # --- 탭 7: 관리자 전용 포트폴리오 ---
    if is_admin and tab7 is not None:
        with tab7:
            render_section_header("🔒 관리자 포트폴리오", "웹에서 포트폴리오를 직접 편집하고 리스크를 실시간 점검합니다.", badge_text="Admin")
            st.caption("웹에서 포트폴리오를 직접 편집하고 수급 이탈 리스크를 실시간 점검합니다.")
            saved_thr = load_admin_risk_thresholds()
            if "admin_ai_warn_threshold" not in st.session_state:
                st.session_state["admin_ai_warn_threshold"] = int(saved_thr["ai_warn_threshold"])
            if "admin_ai_critical_threshold" not in st.session_state:
                st.session_state["admin_ai_critical_threshold"] = int(saved_thr["ai_critical_threshold"])
            ai_warn_threshold = int(st.session_state.get("admin_ai_warn_threshold", saved_thr["ai_warn_threshold"]))
            ai_critical_threshold = int(st.session_state.get("admin_ai_critical_threshold", saved_thr["ai_critical_threshold"]))

            base_cols = ["종목명", "수량", "매수가"]
            df_port_saved = load_admin_portfolio_df(base_cols)
            stock_options = sorted(df_summary["종목명"].dropna().astype(str).unique().tolist())

            if df_port_saved.empty:
                render_empty_state("포트폴리오 비어 있음", "상단 에디터에서 보유 종목을 추가해 주세요.")
            else:
                join_cols = ["종목명", "현재가", "등락률", "AI수급점수", "신호등급", "신호신뢰도", "외인강도(%)", "연기금강도(%)"]
                df_joined = pd.merge(
                    df_port_saved,
                    df_summary[join_cols].copy(),
                    on="종목명",
                    how="left",
                )

                f_strength = pd.to_numeric(df_joined["외인강도(%)"], errors="coerce").fillna(0.0)
                p_strength = pd.to_numeric(df_joined["연기금강도(%)"], errors="coerce").fillna(0.0)
                ai_score = pd.to_numeric(df_joined["AI수급점수"], errors="coerce").fillna(0.0)
                qty_num = pd.to_numeric(df_joined["수량"], errors="coerce").fillna(0.0)
                buy_num = pd.to_numeric(df_joined["매수가"], errors="coerce").fillna(0.0)
                cur_num = pd.to_numeric(df_joined["현재가"], errors="coerce").fillna(0.0)
                risk_a = (f_strength < 0) & (p_strength < 0)
                risk_b = ai_score < float(ai_warn_threshold)
                # 오탐 완화: 단순 단일 신호보다 복합신호(A && AI<임계값) 우선, 매우 낮은 AI는 단독 경보
                df_joined["수급이탈위험"] = (risk_a & risk_b) | (ai_score < float(ai_critical_threshold))

                total_buy_amount = float((qty_num * buy_num).sum())
                total_eval_amount = float((qty_num * cur_num).sum())
                total_profit_amount = total_eval_amount - total_buy_amount
                total_profit_pct = (total_profit_amount / total_buy_amount * 100.0) if total_buy_amount > 0 else 0.0
                pnl_color = "#36C06A" if total_profit_amount >= 0 else "#E04B4B"
                profit_amount_color = "#36C06A" if total_profit_amount >= 0 else "#E04B4B"
                st.markdown("#### 포트폴리오 손익 요약")
                st.markdown(
                    f"""
                    <div class="pf-kpi-grid">
                        <div class="pf-kpi-card">
                            <div class="pf-kpi-label">총 매수금액</div>
                            <div class="pf-kpi-value">{total_buy_amount:,.0f}원</div>
                        </div>
                        <div class="pf-kpi-card">
                            <div class="pf-kpi-label">현재 평가금액</div>
                            <div class="pf-kpi-value">{total_eval_amount:,.0f}원</div>
                        </div>
                        <div class="pf-kpi-card">
                            <div class="pf-kpi-label">총 수익 금액</div>
                            <div class="pf-kpi-value" style="color:{profit_amount_color};">{total_profit_amount:+,.0f}원</div>
                        </div>
                        <div class="pf-kpi-card">
                            <div class="pf-kpi-label">현재 수익률</div>
                            <div class="pf-kpi-value" style="color:{pnl_color};">{total_profit_pct:+.2f}%</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                risk_rows = df_joined[df_joined["수급이탈위험"]].copy()

                st.markdown(
                    f"""
                    <div style="background:linear-gradient(135deg, #121827, #0f1523); border:1px solid #2A344A; border-radius:14px; padding:10px 14px; margin:10px 0 10px 0;">
                        <div style="color:#E5E7EB; font-size:1.0em; font-weight:800;">내 포트폴리오 현황</div>
                        <div style="color:#9CA3AF; font-size:0.84em; margin-top:2px;">종목별 매수금액/수익금액을 테이블과 카드에서 바로 확인하세요.</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                df_view = df_joined.copy()
                df_view["매수금액"] = (pd.to_numeric(df_view["수량"], errors="coerce").fillna(0.0) * pd.to_numeric(df_view["매수가"], errors="coerce").fillna(0.0))
                df_view["평가금액"] = (pd.to_numeric(df_view["수량"], errors="coerce").fillna(0.0) * pd.to_numeric(df_view["현재가"], errors="coerce").fillna(0.0))
                df_view["수익금액"] = df_view["평가금액"] - df_view["매수금액"]
                df_view["비중(%)"] = (df_view["평가금액"] / total_eval_amount * 100.0) if total_eval_amount > 0 else 0.0
                df_view["수익률(%)"] = (
                    (pd.to_numeric(df_view["현재가"], errors="coerce").fillna(0.0) - pd.to_numeric(df_view["매수가"], errors="coerce").fillna(0.0))
                    / pd.to_numeric(df_view["매수가"], errors="coerce").replace(0, pd.NA)
                ) * 100.0
                df_view["상태"] = df_view["수급이탈위험"].apply(lambda x: "⚠ 경보" if bool(x) else "정상")
                display_cols = [
                    "종목명", "상태", "비중(%)", "매수금액", "수익금액", "수익률(%)", "AI수급점수", "신호등급", "신호신뢰도",
                    "외인강도(%)", "연기금강도(%)", "현재가", "수량", "매수가"
                ]
                df_display_port = df_view[display_cols].copy()

                def _row_style(row):
                    return ["background-color: rgba(224,75,75,0.12);" if row.get("상태") == "⚠ 경보" else "" for _ in row]

                st.dataframe(
                    df_display_port.style.apply(_row_style, axis=1).format({
                        "비중(%)": "{:.1f}%",
                        "매수금액": "{:,.0f}원",
                        "수익금액": "{:+,.0f}원",
                        "수익률(%)": "{:+.2f}%",
                        "AI수급점수": "{:.2f}",
                        "신호신뢰도": "{:.1f}",
                        "외인강도(%)": "{:+.1f}%",
                        "연기금강도(%)": "{:+.1f}%",
                        "현재가": "{:,.0f}",
                        "수량": "{:,.0f}",
                        "매수가": "{:,.0f}"
                    }),
                    width='stretch',
                    hide_index=True
                )

                with st.expander("카드형 상세 보기", expanded=False):
                    card_cols = st.columns(2)
                    for i, (_, row) in enumerate(df_joined.iterrows()):
                        with card_cols[i % 2]:
                            cur = float(pd.to_numeric(row.get("현재가"), errors="coerce") or 0.0)
                            chg = float(pd.to_numeric(row.get("등락률"), errors="coerce") or 0.0)
                            qty = float(pd.to_numeric(row.get("수량"), errors="coerce") or 0.0)
                            buy = float(pd.to_numeric(row.get("매수가"), errors="coerce") or 0.0)
                            ai = float(pd.to_numeric(row.get("AI수급점수"), errors="coerce") or 0.0)
                            fs = float(pd.to_numeric(row.get("외인강도(%)"), errors="coerce") or 0.0)
                            ps = float(pd.to_numeric(row.get("연기금강도(%)"), errors="coerce") or 0.0)
                            sig = float(pd.to_numeric(row.get("신호신뢰도"), errors="coerce") or 0.0)
                            pnl = ((cur - buy) / buy * 100.0) if buy > 0 and cur > 0 else None
                            buy_amt = qty * buy
                            eval_amt = qty * cur
                            profit_amt = eval_amt - buy_amt
                            weight_pct = (eval_amt / total_eval_amount * 100.0) if total_eval_amount > 0 else 0.0
                            risk_flag = bool(row.get("수급이탈위험", False))
                            border = "#E04B4B" if risk_flag else "#2C3242"
                            bg = "rgba(224,75,75,0.14)" if risk_flag else "linear-gradient(135deg, #171A24, #121A2C)"
                            pnl_txt = f"{pnl:+.2f}%" if pnl is not None else "-"
                            pnl_color = "#36C06A" if pnl is not None and pnl >= 0 else "#E04B4B"
                            amt_color = "#36C06A" if profit_amt >= 0 else "#E04B4B"
                            chg_color = "#36C06A" if chg >= 0 else "#E04B4B"
                            risk_badge = (
                                '<span style="background:rgba(224,75,75,0.16); color:#FCA5A5; border:1px solid rgba(224,75,75,0.35); border-radius:999px; padding:3px 10px; font-size:0.76em;">⚠ 비중 축소 권고</span>'
                                if risk_flag
                                else '<span style="background:rgba(54,192,106,0.16); color:#86EFAC; border:1px solid rgba(54,192,106,0.35); border-radius:999px; padding:3px 10px; font-size:0.76em;">안정</span>'
                            )
                            st.markdown(
                                f"""
                                <div class="pf-animated-card" style="background:{bg}; border:1px solid {border}; border-radius:16px; padding:12px 14px; margin-bottom:10px; box-shadow:0 10px 28px rgba(0,0,0,0.24);">
                                    <div class="pf-animated-glow"></div>
                                    <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
                                        <div style="color:#F5F7FA; font-size:1.15em; font-weight:800;">{row.get('종목명', '-')}</div>
                                        <div style="color:{pnl_color}; font-size:1.25em; font-weight:900;">{pnl_txt}</div>
                                    </div>
                                    <div style="color:#9CA3AF; margin-top:5px; font-size:0.85em;">비중 {weight_pct:.1f}% | 등락률 <span style="color:{chg_color}; font-weight:700;">{chg:+.2f}%</span></div>
                                    <div style="color:#D1D5DB; margin-top:6px; font-size:0.87em;">현재가 {cur:,.0f}원 · 보유 {qty:,.0f}주 · 매수가 {buy:,.0f}원</div>
                                    <div style="display:flex; gap:7px; flex-wrap:wrap; margin-top:8px;">
                                        <span style="background:#182236; color:#D1D5DB; border:1px solid #2D3A55; border-radius:999px; padding:3px 9px; font-size:0.79em;">매수금액 {buy_amt:,.0f}원</span>
                                        <span style="background:#182236; color:{amt_color}; border:1px solid #2D3A55; border-radius:999px; padding:3px 9px; font-size:0.79em;">수익금액 {profit_amt:+,.0f}원</span>
                                    </div>
                                    <div style="margin-top:8px;">{risk_badge}</div>
                                    <div style="color:#AAB2C5; margin-top:8px; font-size:0.84em;">🏆 AI {ai:.1f} | 신호 {row.get('신호등급','-')} ({sig:.1f}) | 외인 {fs:+.1f}% · 기금 {ps:+.1f}%</div>
                                </div>
                                """,
                                unsafe_allow_html=True,
                            )

            st.markdown("---")
            with st.expander("포트폴리오 편집 및 저장", expanded=False):
                pf_count = int(df_port_saved["종목명"].fillna("").astype(str).str.strip().ne("").sum()) if not df_port_saved.empty else 0
                pf_qty_sum = float(pd.to_numeric(df_port_saved.get("수량", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
                pf_cost_sum = float(
                    (
                        pd.to_numeric(df_port_saved.get("수량", pd.Series(dtype=float)), errors="coerce").fillna(0)
                        * pd.to_numeric(df_port_saved.get("매수가", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
                    ).sum()
                )

                st.markdown(
                    f"""
                    <div style="background:linear-gradient(135deg, #121827, #0f1523); border:1px solid #2A344A; border-radius:14px; padding:12px 14px; margin:8px 0 10px 0;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                            <div style="color:#E5E7EB; font-size:1.02em; font-weight:800;">포트폴리오 에디터</div>
                            <div style="color:#93C5FD; font-size:0.82em;">자동완성 종목 선택 지원</div>
                        </div>
                        <div style="display:flex; gap:8px; flex-wrap:wrap;">
                            <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">보유 종목 {pf_count}개</span>
                            <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">총 수량 {pf_qty_sum:,.0f}</span>
                            <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">총 매입금액 {pf_cost_sum:,.0f}원</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                edited_portfolio = st.data_editor(
                    df_port_saved if not df_port_saved.empty else pd.DataFrame(columns=base_cols),
                    num_rows="dynamic",
                    use_container_width=True,
                    key="admin_portfolio_editor",
                    column_config={
                        "종목명": st.column_config.SelectboxColumn("종목명", options=stock_options, required=False),
                        "수량": st.column_config.NumberColumn("수량", min_value=0, step=1),
                        "매수가": st.column_config.NumberColumn("매수가(원)", min_value=0.0, step=100.0, format="%.0f원"),
                    },
                )
                st.caption("팁: 종목명 셀을 클릭하면 현재 스크리너 종목 리스트에서 빠르게 선택할 수 있습니다.")

                if st.button("포트폴리오 저장", type="primary", use_container_width=True):
                    save_admin_portfolio_df(edited_portfolio)
                    st.success("포트폴리오를 저장했습니다. 상단 현황에 즉시 반영됩니다.")
                    st.rerun()

            st.markdown("---")
            render_section_header("관리 설정", "내 포트폴리오 확인 이후에 사용하는 관리자 전용 설정입니다.")

            with st.expander("리스크 경보 임계값 설정", expanded=False):
                st.markdown(
                    """
                    <div style="background:linear-gradient(135deg, #141A26, #101624); border:1px solid #2A344A; border-radius:12px; padding:12px 14px; margin-bottom:10px;">
                        <div style="color:#E5E7EB; font-weight:700; margin-bottom:6px;">경보 규칙 안내</div>
                        <div style="color:#AAB2C5; font-size:0.92em; line-height:1.55;">
                            • <b>복합 경보</b>: 외인/연기금이 <b>동시에 매도 전환</b>이고, AI 점수가 기준 미만일 때<br/>
                            • <b>단독 급락 경보</b>: AI 점수만 급락 기준 미만일 때
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                col_thr1, col_thr2 = st.columns(2)
                with col_thr1:
                    ai_warn_threshold_new = st.slider(
                        "복합 경보 AI 기준 (외인·연기금 동반 매도일 때 적용)",
                        min_value=0,
                        max_value=100,
                        value=int(st.session_state.get("admin_ai_warn_threshold", 65)),
                        step=1,
                        key="admin_ai_warn_threshold",
                    )
                with col_thr2:
                    ai_critical_threshold_new = st.slider(
                        "단독 급락 경보 AI 기준 (AI만으로 경보)",
                        min_value=0,
                        max_value=100,
                        value=int(st.session_state.get("admin_ai_critical_threshold", 55)),
                        step=1,
                        key="admin_ai_critical_threshold",
                    )
                if (
                    int(ai_warn_threshold_new) != int(saved_thr["ai_warn_threshold"])
                    or int(ai_critical_threshold_new) != int(saved_thr["ai_critical_threshold"])
                ):
                    save_admin_risk_thresholds(ai_warn_threshold_new, ai_critical_threshold_new)
                st.markdown(
                    f"""
                    <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px;">
                        <span style="background:rgba(59,130,246,0.16); color:#93C5FD; border:1px solid rgba(59,130,246,0.35); border-radius:999px; padding:4px 10px; font-size:0.82em;">
                            복합 경보 AI 기준: {int(ai_warn_threshold_new)}
                        </span>
                        <span style="background:rgba(224,75,75,0.16); color:#FCA5A5; border:1px solid rgba(224,75,75,0.35); border-radius:999px; padding:4px 10px; font-size:0.82em;">
                            단독 급락 기준: {int(ai_critical_threshold_new)}
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            with st.expander("테마 추천 승인 (Top40 자동 추천)", expanded=False):
                df_sugg = load_theme_suggestions_safe()
                if df_sugg.empty:
                    st.info("아직 추천 테마가 없습니다. scraper 배치 실행 후 확인하세요.")
                else:
                    df_sugg = df_sugg[df_sugg["승인상태"].astype(str).str.lower() != "approved"].copy()
                    if df_sugg.empty:
                        st.success("승인 대기 중인 추천 테마가 없습니다.")
                    else:
                        df_sugg["선택"] = False
                        df_sugg["신뢰도"] = pd.to_numeric(df_sugg["신뢰도"], errors="coerce").fillna(0.0)
                        df_sugg = df_sugg.sort_values(["신뢰도", "종목명"], ascending=[False, True])
                        edited_sugg = st.data_editor(
                            df_sugg,
                            use_container_width=True,
                            num_rows="fixed",
                            key="theme_suggestion_editor",
                            column_config={
                                "선택": st.column_config.CheckboxColumn("승인"),
                                "추천테마": st.column_config.TextColumn("추천테마"),
                                "신뢰도": st.column_config.NumberColumn("신뢰도", format="%.2f"),
                                "근거": st.column_config.TextColumn("근거"),
                            },
                        )
                        st.caption("승인할 행을 체크하고 [승인 테마 승격]을 누르면 theme_map.csv로 반영됩니다.")
                        if st.button("승인 테마 승격", use_container_width=True, type="primary", key="promote_theme_btn"):
                            picked = edited_sugg[edited_sugg["선택"] == True].copy()
                            promoted = promote_themes_to_map(picked)
                            if promoted > 0:
                                st.success(f"{promoted}개 테마를 theme_map.csv로 승격했습니다.")
                                st.rerun()
                            else:
                                st.warning("승격할 항목이 없습니다. 체크 또는 추천테마 값을 확인하세요.")
