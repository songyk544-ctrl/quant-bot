import streamlit as st
import pandas as pd
import altair as alt
import os
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from google import genai
from google.genai import types
from datetime import datetime, timezone, timedelta
import plotly.express as px
import plotly.graph_objects as go
import urllib.parse
import re
import json
import base64
import html
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

APP_NAME = "AlphaPulse"
APP_NAME_LEFT = "Alpha"
APP_NAME_RIGHT = "Pulse"
BRAND_LOGO_PATH = "assets/brand/alpha_pulse_cut.png"
PAGE_ICON = "assets/brand/alpha_pulse_favicon.png" if os.path.exists("assets/brand/alpha_pulse_favicon.png") else BRAND_LOGO_PATH

st.set_page_config(layout="wide", page_title=APP_NAME, page_icon=PAGE_ICON)

KST = timezone(timedelta(hours=9))

def now_kst():
    return datetime.now(KST)


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
logo_html = f'<img class="qe-brand-head-img" src="{logo_uri}" alt="{APP_NAME} logo"/>' if logo_uri else ""
if logo_uri:
    st.sidebar.markdown(
        f"""
        <div class="qe-sidebar-brand-footer">
            <img class="qe-brand-side-img" src="{logo_uri}" alt="{APP_NAME} logo"/>
            <div class="qe-brand-side-wordmark"><span class="qe-brand-word-q">{APP_NAME_LEFT}</span><span class="qe-brand-word-edge">{APP_NAME_RIGHT}</span></div>
        </div>
        """,
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

def _macro_item(name):
    data = macro_data.get(name)
    return data if isinstance(data, dict) else None

def _macro_value(name, key="value"):
    data = _macro_item(name)
    if not data:
        return None
    try:
        return float(data.get(key))
    except Exception:
        return None

def _macro_change_pct(name):
    return _macro_value(name, "change_pct")

def _macro_value_text(name, data):
    if not data:
        return "-"
    value = data.get("value")
    if value is None:
        return "-"
    if "환율" in name:
        return f"{float(value):,.1f}원"
    if "국채" in name or "VIX" in name:
        return f"{float(value):.2f}"
    return f"{float(value):,.2f}"

def _macro_change_style(name, change_pct):
    if change_pct is None:
        return "#6B7280", "대기", "-"

    is_risk_indicator = any(key in name for key in ["환율", "VIX", "국채", "WTI"])
    if abs(change_pct) < 0.05:
        return "#9CA3AF", "보합", "-"
    if is_risk_indicator:
        if change_pct > 0:
            return "#F97316", "부담", "▲"
        return "#36C06A", "완화", "▼"
    if change_pct > 0:
        return "#FF4B4B", "상승", "▲"
    return "#3B82F6", "하락", "▼"

def render_macro_cards(ticker_names):
    cards = []
    for name in ticker_names:
        data = macro_data.get(name)
        if data:
            change_pct = data.get("change_pct")
            color, label, arrow = _macro_change_style(name, change_pct)
            val_str = _macro_value_text(name, data)
            pct_text = f"{abs(change_pct):.2f}%" if change_pct is not None else "-"
            cards.append(f"<div class='macro-card'><div class='macro-label'>{name}</div><div class='macro-value'>{val_str}</div><div class='macro-change' style='color:{color};'>{label} {arrow} {pct_text}</div></div>")
        else:
            cards.append(f"<div class='macro-card'><div class='macro-label'>{name}</div><div class='macro-value'>-</div><div class='macro-change' style='color:#6B7280;'>데이터 지연</div></div>")
    return f"<div class='macro-strip'>{''.join(cards)}</div>"

def build_market_regime_summary():
    kospi = _macro_change_pct("🇰🇷 KOSPI")
    kosdaq = _macro_change_pct("🇰🇷 KOSDAQ")
    fx = _macro_change_pct("💵 환율")
    vix = _macro_value("😨 VIX")
    vix_chg = _macro_change_pct("😨 VIX")
    us10y = _macro_change_pct("📉 미 국채(10y)")

    score = 0
    reasons = []

    if kospi is not None:
        score += 1 if kospi >= 0 else -1
        reasons.append(f"KOSPI {kospi:+.2f}%")
    if kosdaq is not None:
        score += 1 if kosdaq >= 0 else -1
        reasons.append(f"KOSDAQ {kosdaq:+.2f}%")
    if fx is not None:
        score += -1 if fx > 0.25 else (1 if fx < -0.25 else 0)
        reasons.append(f"환율 {fx:+.2f}%")
    if vix is not None:
        score += -2 if vix >= 24 else (-1 if vix >= 20 else 1)
        reasons.append(f"VIX {vix:.1f}")
    if vix_chg is not None and abs(vix_chg) >= 3:
        score += -1 if vix_chg > 0 else 1
    if us10y is not None and abs(us10y) >= 1.5:
        score += -1 if us10y > 0 else 1

    if score >= 2:
        regime = "공격 가능"
        mode = "눌림목 우선, 돌파 일부 허용"
        tone = "우호"
        color = "#36C06A"
        guide = "신규후보는 1~3개까지 선별 매수 가능"
    elif score <= -2:
        regime = "방어 우선"
        mode = "신규매수 축소, 보유종목 점검"
        tone = "방어"
        color = "#F97316"
        guide = "돌파매매는 줄이고 연기금 수급 지속 종목만 관찰"
    else:
        regime = "중립"
        mode = "눌림목 중심, 돌파는 확인 후 진입"
        tone = "중립"
        color = "#60A5FA"
        guide = "종가 기준으로 거래대금과 수급 동행을 한 번 더 확인"

    return {
        "score": score,
        "regime": regime,
        "mode": mode,
        "tone": tone,
        "color": color,
        "guide": guide,
        "reasons": reasons[:4],
    }

def summarize_macro_news_refs(macro_news_refs):
    refs = macro_news_refs or []
    buckets = {
        "정책/금리": ["금리", "국채", "fed", "fomc", "연준", "채권"],
        "환율/원화": ["환율", "원화", "달러"],
        "반도체/AI": ["반도체", "ai", "엔비디아", "hbm", "sk하이닉스", "삼성전자"],
        "바이오/헬스": ["바이오", "제약", "헬스", "임상"],
        "중국/수출": ["중국", "수출", "관세", "무역"],
    }
    hits = []
    joined = " ".join(refs).lower()
    for label, keys in buckets.items():
        if any(key in joined for key in keys):
            hits.append(label)
    return hits[:3], refs[:3]

def render_market_regime_panel(df_summary_local, macro_news_refs):
    summary = build_market_regime_summary()
    tags, top_news = summarize_macro_news_refs(macro_news_refs)
    new_count = 0
    if not df_summary_local.empty and "매수후보" in df_summary_local.columns:
        new_count = int((df_summary_local["매수후보"].astype(str) == "신규후보").sum())
    tag_text = " · ".join(tags) if tags else "주요 키워드 대기"
    reason_text = " · ".join(summary["reasons"]) if summary["reasons"] else "지표 데이터 지연"
    news_html = "".join([f"<li>{html.escape(str(item))}</li>" for item in top_news]) or "<li>시황 뉴스 수집 대기</li>"

    st.markdown(
        dedent(f"""
<div class="market-regime-panel">
  <div class="market-regime-top">
    <div>
      <div class="regime-label">시장 상태</div>
      <div class="regime-title" style="color:{summary['color']};">{summary['regime']} <span>{summary['tone']}</span></div>
      <div class="regime-meta">{reason_text}</div>
    </div>
    <div class="regime-score">환경점수 {summary['score']:+d}</div>
  </div>
  <div class="regime-grid">
    <div class="regime-mini">
      <div class="regime-mini-label">전략 모드</div>
      <div class="regime-mini-value">{summary['mode']}</div>
    </div>
    <div class="regime-mini">
      <div class="regime-mini-label">오늘 신규후보</div>
      <div class="regime-mini-value">{new_count}개</div>
    </div>
    <div class="regime-mini">
      <div class="regime-mini-label">뉴스 키워드</div>
      <div class="regime-mini-value">{tag_text}</div>
    </div>
  </div>
  <ul class="regime-news">{news_html}</ul>
</div>
""").strip(),
        unsafe_allow_html=True,
    )

def render_product_header(df_summary_local):
    summary = build_market_regime_summary()
    updated_at = now_kst().strftime("%m/%d %H:%M")
    new_count = 0
    watch_count = 0
    avoid_count = 0
    top_name = "-"
    top_entry = "관찰"
    risk_count = 0
    if df_summary_local is not None and not df_summary_local.empty:
        if "매수후보" in df_summary_local.columns:
            buy_state = df_summary_local["매수후보"].astype(str)
            new_count = int((buy_state == "신규후보").sum())
            watch_count = int((buy_state == "관찰").sum())
            avoid_count = int((buy_state == "제외").sum())
        top_row = df_summary_local.iloc[0]
        top_name = str(top_row.get("종목명", "-"))
        top_entry = str(top_row.get("진입유형", "관찰"))
        if "매도점검" in df_summary_local.columns:
            risk_count = int(df_summary_local["매도점검"].astype(str).str.contains("주의|축소|이탈|청산", regex=True).sum())

    logo_block = logo_html or '<div class="qe-logo-fallback">Q</div>'
    st.markdown(
        dedent(f"""
<div class="qe-product-header">
  <div class="qe-product-brand">
    {logo_block}
    <div>
      <div class="qe-brand-line"><span class="qe-brand-word-q">{APP_NAME_LEFT}</span><span class="qe-brand-word-edge">{APP_NAME_RIGHT}</span></div>
      <div class="qe-product-sub">국내주식 스윙 후보 · 수급/매크로 상태판</div>
    </div>
  </div>
  <div class="qe-product-status">
    <span class="qe-status-chip qe-status-strong">신규후보 {new_count}개</span>
    <span class="qe-status-chip" style="color:{summary['color']}; border-color:{summary['color']};">{summary['regime']}</span>
    <span class="qe-status-chip">관찰 {watch_count} · 제외 {avoid_count}</span>
    <span class="qe-status-chip">Top {html.escape(top_name)} · {html.escape(top_entry)}</span>
    <span class="qe-status-chip">갱신 {updated_at}</span>
  </div>
  <div class="qe-product-guide">{summary['mode']} · 보유점검 {risk_count}개</div>
</div>
""").strip(),
        unsafe_allow_html=True,
    )

def render_theme_flow_summary(df_summary_local):
    if df_summary_local.empty or "테마표시" not in df_summary_local.columns:
        return
    theme_df = df_summary_local.copy()
    theme_df["테마표시"] = theme_df["테마표시"].fillna("기타").astype(str)
    buy_state = theme_df["매수후보"].astype(str) if "매수후보" in theme_df.columns else pd.Series("", index=theme_df.index)
    entry_state = theme_df["진입유형"].astype(str) if "진입유형" in theme_df.columns else pd.Series("", index=theme_df.index)
    theme_df["is_new"] = buy_state.eq("신규후보")
    theme_df["is_pullback"] = entry_state.eq("눌림목")
    theme_df["is_breakout"] = entry_state.eq("돌파")
    for col in ["연기금10일강도(%)", "기관동행점수", "AI수급점수"]:
        if col not in theme_df.columns:
            theme_df[col] = 0.0
        theme_df[col] = pd.to_numeric(theme_df[col], errors="coerce").fillna(0.0)

    theme_summary = (
        theme_df.groupby("테마표시", as_index=False)
        .agg(
            종목수=("종목명", "count"),
            신규후보=("is_new", "sum"),
            눌림목=("is_pullback", "sum"),
            돌파=("is_breakout", "sum"),
            연기금10D=("연기금10일강도(%)", "mean"),
            기관동행=("기관동행점수", "mean"),
            평균AI=("AI수급점수", "mean"),
        )
        .sort_values(["신규후보", "연기금10D", "기관동행", "평균AI"], ascending=[False, False, False, False])
        .head(4)
    )
    cards = []
    for _, row in theme_summary.iterrows():
        signal = "신규 주도" if int(row["신규후보"]) > 0 else ("수급 관찰" if float(row["연기금10D"]) > 0 else "대기")
        color = "#86EFAC" if signal == "신규 주도" else ("#FCD34D" if signal == "수급 관찰" else "#CBD5E1")
        cards.append(
            dedent(f"""
<div class="theme-flow-card">
  <div class="theme-flow-top">
    <div class="theme-flow-name">{html.escape(str(row['테마표시']))}</div>
    <span style="color:{color};">{signal}</span>
  </div>
  <div class="theme-flow-metrics">
    <span>신규 {int(row['신규후보'])}</span>
    <span>눌림 {int(row['눌림목'])}</span>
    <span>돌파 {int(row['돌파'])}</span>
  </div>
  <div class="theme-flow-meta">기금10D {float(row['연기금10D']):+.2f}% · 기관동행 {float(row['기관동행']):.1f}</div>
</div>
""").strip()
        )
    st.markdown(dedent(f"""
<div class="theme-flow-grid">
{''.join(cards)}
</div>
""").strip(), unsafe_allow_html=True)

def render_stock_decision_panel(row):
    buy_candidate = str(row.get("매수후보", "관찰"))
    entry_type = str(row.get("진입유형", "관찰"))
    sell_check = str(row.get("매도점검", "보유/관찰"))
    swing_priority = float(pd.to_numeric(row.get("스윙우선순위", 0), errors="coerce") or 0)
    inst_score = float(pd.to_numeric(row.get("기관동행점수", 0), errors="coerce") or 0)
    pension_10d = float(pd.to_numeric(row.get("연기금10일강도(%)", 0), errors="coerce") or 0)
    signal_grade = str(row.get("신호등급", "-"))

    if buy_candidate == "신규후보":
        entry_decision = "진입 후보"
        entry_guide = f"{entry_type} 조건. 종가 기준 거래대금과 기금 수급 유지 확인"
        entry_color = "#86EFAC"
    elif entry_type in ["눌림목", "돌파"] and swing_priority >= 50:
        entry_decision = "관찰 후 진입"
        entry_guide = "가격 확인은 가능하지만 신규후보 우선순위는 아님"
        entry_color = "#FCD34D"
    else:
        entry_decision = "진입 보류"
        entry_guide = "수급/가격 조건이 완전히 맞을 때까지 대기"
        entry_color = "#CBD5E1"

    hold_decision = "보유 관찰" if "주의" not in sell_check and "축소" not in sell_check and "청산" not in sell_check else "보유 축소 점검"
    hold_color = "#86EFAC" if hold_decision == "보유 관찰" else "#F97316"
    sell_decision = sell_check if sell_check else "보유/관찰"
    sell_color = "#FCA5A5" if any(x in sell_decision for x in ["주의", "축소", "청산", "이탈"]) else "#CBD5E1"

    st.markdown(
        dedent(f"""
<div class="decision-grid">
  <div class="decision-card">
    <div class="decision-label">진입 판단</div>
    <div class="decision-value" style="color:{entry_color};">{entry_decision}</div>
    <div class="decision-meta">{entry_guide}</div>
  </div>
  <div class="decision-card">
    <div class="decision-label">보유 판단</div>
    <div class="decision-value" style="color:{hold_color};">{hold_decision}</div>
    <div class="decision-meta">스윙 {swing_priority:.1f} · 기관동행 {inst_score:.1f} · 신호 {html.escape(signal_grade)}</div>
  </div>
  <div class="decision-card">
    <div class="decision-label">매도 점검</div>
    <div class="decision-value" style="color:{sell_color};">{html.escape(sell_decision)}</div>
    <div class="decision-meta">연기금10D {pension_10d:+.2f}% · 종가 기준으로 재확인</div>
  </div>
</div>
""").strip(),
        unsafe_allow_html=True,
    )

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
    .qe-product-header {
      background:#0f1726;
      border:1px solid #2A344A;
      border-radius:12px;
      padding:12px 14px;
      margin:6px 0 10px 0;
      box-shadow:0 8px 20px rgba(0,0,0,0.16);
    }
    .qe-product-brand { display:flex; align-items:center; gap:10px; margin-bottom:9px; }
    .qe-brand-line {
      color:#F8FAFC;
      font-family:"Inter","Pretendard","Segoe UI","Noto Sans KR",sans-serif;
      font-size:1.72rem;
      line-height:1;
      letter-spacing:0.01em;
    }
    .qe-product-sub { color:#94A3B8; font-size:0.82em; margin-top:4px; }
    .qe-logo-fallback {
      width:42px;
      height:42px;
      border-radius:10px;
      display:flex;
      align-items:center;
      justify-content:center;
      color:#F8FAFC;
      background:#172033;
      border:1px solid #334155;
      font-weight:900;
    }
    .qe-product-status { display:flex; gap:7px; flex-wrap:wrap; align-items:center; }
    .qe-status-chip {
      background:#151f31;
      border:1px solid #2D3A55;
      border-radius:999px;
      color:#CBD5E1;
      font-size:0.78em;
      font-weight:800;
      padding:4px 9px;
      white-space:nowrap;
    }
    .qe-status-strong { color:#86EFAC; border-color:#2F6B4A; background:#13281f; }
    .qe-product-guide { color:#A7B0C2; font-size:0.82em; margin-top:8px; line-height:1.35; }
    .theme-flow-grid {
      display:grid;
      grid-template-columns:repeat(4,minmax(140px,1fr));
      gap:8px;
      margin:8px 0 12px 0;
    }
    .theme-flow-card {
      background:#111b2d;
      border:1px solid #26324A;
      border-radius:10px;
      padding:10px 11px;
      min-width:0;
    }
    .theme-flow-top { display:flex; justify-content:space-between; gap:8px; align-items:center; font-size:0.8em; font-weight:800; }
    .theme-flow-name { color:#F8FAFC; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .theme-flow-metrics { display:flex; gap:6px; flex-wrap:wrap; color:#CBD5E1; font-size:0.76em; margin-top:8px; }
    .theme-flow-metrics span { background:#172033; border:1px solid #2D3A55; border-radius:999px; padding:3px 7px; }
    .theme-flow-meta { color:#9CA3AF; font-size:0.76em; margin-top:7px; line-height:1.35; }
    .decision-grid {
      display:grid;
      grid-template-columns:repeat(3,minmax(150px,1fr));
      gap:8px;
      margin:8px 0 10px 0;
    }
    .decision-card {
      background:#111b2d;
      border:1px solid #26324A;
      border-radius:10px;
      padding:10px 11px;
    }
    .decision-label { color:#8EA0BE; font-size:0.73em; font-weight:800; margin-bottom:4px; }
    .decision-value { font-size:1.04em; font-weight:900; line-height:1.2; }
    .decision-meta { color:#9CA3AF; font-size:0.76em; margin-top:5px; line-height:1.35; }
    .alpha-card-new {
      position:relative;
      overflow:hidden;
      border-color:#2F6B4A !important;
      box-shadow:0 8px 22px rgba(48,218,169,0.14), inset 0 0 0 1px rgba(48,218,169,0.12) !important;
      animation:alphaNewPulse 2.8s ease-in-out infinite;
    }
    .alpha-card-new::before {
      content:"";
      position:absolute;
      top:0;
      left:-45%;
      width:36%;
      height:100%;
      background:linear-gradient(90deg, transparent, rgba(48,218,169,0.14), transparent);
      transform:skewX(-16deg);
      animation:alphaSweep 3.8s ease-in-out infinite;
      pointer-events:none;
    }
    @keyframes alphaNewPulse {
      0%, 100% { border-color:#2F6B4A; }
      50% { border-color:#30DAA9; }
    }
    @keyframes alphaSweep {
      0% { left:-45%; opacity:0; }
      20% { opacity:1; }
      70% { opacity:0.85; }
      100% { left:115%; opacity:0; }
    }
    .alpha-card-head { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; margin-bottom:9px; }
    .alpha-card-left { min-width:0; flex:1 1 auto; }
    .alpha-chip-row { display:flex; align-items:center; gap:6px; flex-wrap:wrap; margin-bottom:7px; }
    .alpha-name-row { display:flex; align-items:flex-start; gap:8px; flex-wrap:wrap; }
    .alpha-stock-name { color:#FFF; font-size:1.08em; font-weight:850; line-height:1.22; word-break:keep-all; overflow-wrap:anywhere; }
    .alpha-price-box { min-width:88px; text-align:right; flex:0 0 auto; }
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
    .market-regime-panel {
      background:#0f1726;
      border:1px solid #2A344A;
      border-radius:12px;
      padding:12px 13px;
      margin:8px 0 12px 0;
      box-shadow:0 8px 20px rgba(0,0,0,0.18);
    }
    .market-regime-top {
      display:flex;
      justify-content:space-between;
      gap:10px;
      align-items:flex-start;
      border-bottom:1px solid #253047;
      padding-bottom:10px;
      margin-bottom:10px;
    }
    .regime-label { color:#94A3B8; font-size:0.76em; font-weight:700; margin-bottom:3px; }
    .regime-title { font-size:1.38em; font-weight:900; line-height:1.15; }
    .regime-title span { color:#CBD5E1; font-size:0.62em; font-weight:800; margin-left:4px; }
    .regime-meta { color:#9CA3AF; font-size:0.8em; margin-top:5px; line-height:1.35; }
    .regime-score {
      background:#151f31;
      border:1px solid #32405A;
      border-radius:999px;
      color:#DDE6F5;
      font-size:0.78em;
      font-weight:800;
      padding:5px 9px;
      white-space:nowrap;
    }
    .regime-grid { display:grid; grid-template-columns:1.1fr .8fr 1fr; gap:8px; }
    .regime-mini {
      background:#111b2d;
      border:1px solid #26324A;
      border-radius:10px;
      padding:9px 10px;
      min-width:0;
    }
    .regime-mini-label { color:#8EA0BE; font-size:0.72em; font-weight:700; margin-bottom:4px; }
    .regime-mini-value { color:#F8FAFC; font-size:0.96em; font-weight:850; line-height:1.25; }
    .regime-mini-meta { color:#9CA3AF; font-size:0.74em; margin-top:4px; line-height:1.35; }
    .regime-news {
      margin:10px 0 0 16px;
      padding:0;
      color:#B8C2D6;
      font-size:0.8em;
      line-height:1.45;
    }
    .regime-news li { margin:3px 0; }
    @media (max-width: 900px) {
      .macro-card { min-width:108px; padding:7px 9px; }
      .macro-label { font-size:0.7em; }
      .macro-value { font-size:0.9em; }
      .macro-change { font-size:0.75em; }
      .market-regime-panel { padding:11px; border-radius:10px; }
      .market-regime-top { align-items:flex-start; }
      .regime-title { font-size:1.18em; }
      .regime-grid { grid-template-columns:1fr; gap:7px; }
      .qe-brand-head-img { width:40px; height:40px; }
      .qe-brand-wordmark { font-size:2.4rem; }
      .qe-product-header { padding:11px; border-radius:10px; }
      .qe-brand-line { font-size:1.42rem; }
      .qe-product-status { gap:6px; }
      .qe-status-chip { font-size:0.72em; padding:4px 8px; }
      .theme-flow-grid { grid-template-columns:1fr 1fr; gap:7px; }
      .decision-grid { grid-template-columns:1fr; gap:7px; }
      .alpha-card-head { align-items:flex-start; }
      .alpha-name-row { display:block; }
      .alpha-stock-name { display:block; font-size:1.12em; margin-bottom:7px; }
      .alpha-price-box { min-width:78px; }
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


def load_swing_trades_safe():
    base_cols = [
        "거래ID", "진입일", "종목명", "종목코드", "진입순위", "AI수급점수",
        "진입유형", "스윙우선순위", "진입코멘트", "보유일수", "청산방식", "청산사유", "진입가", "청산일", "청산가", "수익률", "상태",
    ]
    if not csv_exists("swing_trades.csv") and not table_exists("swing_trades"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("swing_trades.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    for c in ["진입순위", "보유일수"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["AI수급점수", "스윙우선순위", "진입가", "청산가", "수익률"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["진입일_dt"] = pd.to_datetime(df["진입일"], errors="coerce")
    df["청산일_dt"] = pd.to_datetime(df["청산일"], errors="coerce")
    return df.dropna(subset=["진입일_dt"])[base_cols + ["진입일_dt", "청산일_dt"]]


def load_swing_performance_safe():
    base_cols = ["날짜", "일간수익률", "누적수익률", "최대낙폭(%)", "리스크상태", "종료거래수", "승률(%)"]
    if not csv_exists("swing_performance.csv") and not table_exists("swing_performance"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("swing_performance.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    for c in ["일간수익률", "누적수익률", "최대낙폭(%)", "종료거래수", "승률(%)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["날짜_dt"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df.dropna(subset=["날짜_dt"])[base_cols + ["날짜_dt"]]


def build_capital_limited_swing_sim(df_trades, df_history, initial_cash=5_000_000, max_positions=3, start_date=None):
    """초기자금과 동시보유 제한을 둔 실제 포트폴리오형 스윙 시뮬레이션."""
    perf_cols = ["날짜", "평가금액", "현금", "투자금액", "수익률(%)", "일간수익률", "보유종목수", "실현손익"]
    pos_cols = ["종목명", "진입일", "진입가", "수량", "매수금액", "현재가", "평가금액", "평가손익", "평가수익률", "보유일수", "상태"]
    closed_cols = ["진입일", "청산일", "종목명", "보유일수", "매수금액", "청산금액", "실현손익", "수익률", "청산사유"]
    if df_trades.empty or df_history.empty:
        return pd.DataFrame(columns=perf_cols), pd.DataFrame(columns=pos_cols), pd.DataFrame(columns=closed_cols)

    hist = df_history.copy()
    if not {"종목명", "일자", "종가"}.issubset(hist.columns):
        return pd.DataFrame(columns=perf_cols), pd.DataFrame(columns=pos_cols), pd.DataFrame(columns=closed_cols)
    raw_dates = hist["일자"].astype(str).str.replace("-", "", regex=False).str.strip()
    hist["일자_dt"] = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce")
    if hist["일자_dt"].notna().sum() == 0:
        hist["일자_dt"] = pd.to_datetime(hist["일자"], errors="coerce")
    hist["종가"] = pd.to_numeric(hist["종가"], errors="coerce")
    hist = hist.dropna(subset=["일자_dt", "종목명", "종가"]).sort_values(["종목명", "일자_dt"])
    if hist.empty:
        return pd.DataFrame(columns=perf_cols), pd.DataFrame(columns=pos_cols), pd.DataFrame(columns=closed_cols)

    prices = {str(name): grp.set_index("일자_dt")["종가"].sort_index() for name, grp in hist.groupby("종목명")}
    dates = sorted(hist["일자_dt"].dt.normalize().unique())
    if start_date is not None:
        sim_start = pd.to_datetime(start_date).normalize()
        dates = [d for d in dates if pd.to_datetime(d).normalize() >= sim_start]
    if not dates:
        return pd.DataFrame(columns=perf_cols), pd.DataFrame(columns=pos_cols), pd.DataFrame(columns=closed_cols)
    signal = df_trades[df_trades["청산방식"].astype(str).eq("시그널")].copy()
    if signal.empty:
        signal = df_trades[df_trades["보유일수"].eq(10)].copy()
    signal = signal.dropna(subset=["진입일_dt"]).sort_values(["진입일_dt", "진입순위", "스윙우선순위"], ascending=[True, True, False])

    cash = float(initial_cash)
    positions = []
    closed_rows = []
    perf_rows = []
    prev_equity = float(initial_cash)
    per_slot_cash = float(initial_cash) / max(1, int(max_positions))

    for cur_date in dates:
        cur_date = pd.to_datetime(cur_date).normalize()
        realized_today = 0.0

        remaining = []
        for pos in positions:
            exit_date = pd.to_datetime(pos["청산일_dt"]).normalize() if pd.notna(pos.get("청산일_dt")) else None
            should_exit = exit_date is not None and exit_date <= cur_date and str(pos.get("상태", "")).lower() == "closed"
            if should_exit:
                px = prices.get(pos["종목명"])
                exit_price = float(px.loc[px.index <= cur_date].iloc[-1]) if px is not None and not px.loc[px.index <= cur_date].empty else float(pos["진입가"])
                exit_value = exit_price * pos["수량"]
                pnl = exit_value - pos["매수금액"]
                cash += exit_value
                realized_today += pnl
                closed_rows.append({
                    "진입일": pos["진입일"],
                    "청산일": cur_date.strftime("%Y-%m-%d"),
                    "종목명": pos["종목명"],
                    "보유일수": max(1, len([d for d in dates if pd.to_datetime(pos["진입일_dt"]).normalize() <= pd.to_datetime(d).normalize() <= cur_date]) - 1),
                    "매수금액": round(pos["매수금액"], 0),
                    "청산금액": round(exit_value, 0),
                    "실현손익": round(pnl, 0),
                    "수익률": round((pnl / pos["매수금액"]) * 100.0, 4) if pos["매수금액"] else 0.0,
                    "청산사유": pos.get("청산사유", ""),
                })
            else:
                remaining.append(pos)
        positions = remaining

        todays = signal[signal["진입일_dt"].dt.normalize().eq(cur_date)].copy()
        held_names = {p["종목명"] for p in positions}
        for _, sig in todays.iterrows():
            if len(positions) >= int(max_positions):
                break
            name = str(sig.get("종목명", "")).strip()
            if not name or name in held_names:
                continue
            entry_price = float(pd.to_numeric(sig.get("진입가"), errors="coerce") or 0.0)
            if entry_price <= 0:
                continue
            budget = min(per_slot_cash, cash)
            qty = int(budget // entry_price)
            if qty <= 0:
                continue
            buy_amount = qty * entry_price
            cash -= buy_amount
            held_names.add(name)
            positions.append({
                "종목명": name,
                "진입일": str(sig.get("진입일", "")),
                "진입일_dt": pd.to_datetime(sig.get("진입일_dt")),
                "진입가": entry_price,
                "수량": qty,
                "매수금액": buy_amount,
                "청산일_dt": sig.get("청산일_dt"),
                "청산사유": str(sig.get("청산사유", "")),
                "상태": str(sig.get("상태", "")),
            })

        invested_value = 0.0
        for pos in positions:
            px = prices.get(pos["종목명"])
            if px is not None and not px.loc[px.index <= cur_date].empty:
                mark_price = float(px.loc[px.index <= cur_date].iloc[-1])
            else:
                mark_price = float(pos["진입가"])
            invested_value += mark_price * pos["수량"]
        equity = cash + invested_value
        daily_ret = ((equity - prev_equity) / prev_equity * 100.0) if prev_equity else 0.0
        perf_rows.append({
            "날짜": cur_date.strftime("%Y-%m-%d"),
            "평가금액": round(equity, 0),
            "현금": round(cash, 0),
            "투자금액": round(invested_value, 0),
            "수익률(%)": round(((equity - float(initial_cash)) / float(initial_cash)) * 100.0, 4),
            "일간수익률": round(daily_ret, 4),
            "보유종목수": len(positions),
            "실현손익": round(realized_today, 0),
        })
        prev_equity = equity

    latest_date = pd.to_datetime(dates[-1]).normalize()
    pos_rows = []
    for pos in positions:
        px = prices.get(pos["종목명"])
        cur_price = float(px.loc[px.index <= latest_date].iloc[-1]) if px is not None and not px.loc[px.index <= latest_date].empty else float(pos["진입가"])
        value = cur_price * pos["수량"]
        pnl = value - pos["매수금액"]
        hold_days = max(0, len([d for d in dates if pd.to_datetime(pos["진입일_dt"]).normalize() <= pd.to_datetime(d).normalize() <= latest_date]) - 1)
        pos_rows.append({
            "종목명": pos["종목명"],
            "진입일": pos["진입일"],
            "진입가": round(pos["진입가"], 0),
            "수량": pos["수량"],
            "매수금액": round(pos["매수금액"], 0),
            "현재가": round(cur_price, 0),
            "평가금액": round(value, 0),
            "평가손익": round(pnl, 0),
            "평가수익률": round((pnl / pos["매수금액"]) * 100.0, 4) if pos["매수금액"] else 0.0,
            "보유일수": hold_days,
            "상태": "보유 중",
        })
    return pd.DataFrame(perf_rows), pd.DataFrame(pos_rows, columns=pos_cols), pd.DataFrame(closed_rows, columns=closed_cols)


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

    if "스윙우선순위" in df_summary_local.columns:
        ranked = df_summary_local.copy()
        if "매수후보" in ranked.columns:
            ranked["_candidate_order"] = ranked["매수후보"].map({"신규후보": 0, "관찰": 1, "제외": 2}).fillna(1)
        else:
            ranked["_candidate_order"] = 1
        ranked = ranked.sort_values(["_candidate_order", "스윙우선순위", "AI수급점수"], ascending=[True, False, False]).drop(columns=["_candidate_order"], errors="ignore").reset_index(drop=True)
    else:
        ranked = df_summary_local.sort_values("AI수급점수", ascending=False).reset_index(drop=True)
    buy_pool = ranked[ranked.get("매수후보", "").astype(str) == "신규후보"] if "매수후보" in ranked.columns else pd.DataFrame()
    buy_row = buy_pool.iloc[0] if not buy_pool.empty else ranked.iloc[0]
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

    updated_at = now_kst().strftime("%H:%M")
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
        f'<span class="kpi-delta" style="background:rgba(54,192,106,0.18); color:#36C06A;">스윙 {float(buy_row.get("스윙우선순위", buy_row.get("AI수급점수", 0))):.1f}</span>',
        f"{build_quality_badge(buy_row)} · {buy_conf} · {updated_at}"
    )
    card_watch = _brief_card(
        "관망 후보",
        watch_row.get("종목명", "-"),
        f'<span class="kpi-delta" style="background:rgba(59,130,246,0.16); color:#60A5FA;">스윙 {float(watch_row.get("스윙우선순위", watch_row.get("AI수급점수", 0))):.1f}</span>',
        f"{watch_conf} · {updated_at}"
    )
    st.markdown(f'<div class="kpi-grid">{card_buy}{card_watch}</div>', unsafe_allow_html=True)

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

    df_summary['AI순위'] = df_summary['AI수급점수'].rank(method='first', ascending=False).astype(int)
    if "스윙우선순위" in df_summary.columns:
        df_summary["스윙우선순위"] = pd.to_numeric(df_summary["스윙우선순위"], errors="coerce").fillna(0.0)
    else:
        df_summary["스윙우선순위"] = pd.to_numeric(df_summary["AI수급점수"], errors="coerce").fillna(0.0)
    if "매수후보" not in df_summary.columns:
        df_summary["매수후보"] = "관찰"
    if "진입유형" not in df_summary.columns:
        df_summary["진입유형"] = "관찰"
    if "매도점검" not in df_summary.columns:
        df_summary["매도점검"] = "보유/관찰"
    if "정배열" not in df_summary.columns:
        df_summary["정배열"] = True
    if "추세품질점수" not in df_summary.columns:
        df_summary["추세품질점수"] = 50.0
    for supply_col, default_value in {
        "수급품질점수": 0.0,
        "주도주점수": 0.0,
        "수급흡수율": 0.0,
        "수급지속일수": 0,
        "거래대금활력": 0.0,
        "20일평균거래대금(억)": 0.0,
        "종목체급": "소형",
        "전략슬리브": "관찰",
    }.items():
        if supply_col not in df_summary.columns:
            df_summary[supply_col] = default_value
    for numeric_col in ["수급품질점수", "주도주점수", "수급흡수율", "수급지속일수", "거래대금활력", "20일평균거래대금(억)"]:
        df_summary[numeric_col] = pd.to_numeric(df_summary[numeric_col], errors="coerce").fillna(0.0)
    for ma_col in ["MA5", "MA10", "MA20"]:
        if ma_col not in df_summary.columns:
            df_summary[ma_col] = 0.0
    candidate_order = {"신규후보": 0, "관찰": 1, "제외": 2}
    df_summary = df_summary.sort_values(
        ["스윙우선순위", "AI수급점수"],
        ascending=[False, False],
    ).reset_index(drop=True)
    df_summary['현재_순위'] = range(1, len(df_summary) + 1)
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
            yday_slice = df_trend[df_trend['날짜'] == dates[1]].copy()
            if "스윙우선순위" in yday_slice.columns:
                if "매수후보" not in yday_slice.columns:
                    yday_slice["매수후보"] = "관찰"
                yday_slice["스윙우선순위"] = pd.to_numeric(yday_slice["스윙우선순위"], errors="coerce").fillna(0.0)
                yday_slice["AI수급점수"] = pd.to_numeric(yday_slice["AI수급점수"], errors="coerce").fillna(0.0)
                yday_slice = yday_slice.sort_values(
                    ["스윙우선순위", "AI수급점수"],
                    ascending=[False, False],
                ).reset_index(drop=True)
                yday_slice["전일_순위"] = range(1, len(yday_slice) + 1)
                yday_data = yday_slice[["종목명", "전일_순위"]]
            else:
                yday_data = yday_slice[['종목명', '순위']]
                yday_data.columns = ['종목명', '전일_순위']
            df_summary = pd.merge(df_summary, yday_data, on='종목명', how='left')
            df_summary['전일_순위'] = df_summary['전일_순위'].fillna(df_summary['현재_순위'])
            df_summary['랭킹추세'] = (df_summary['전일_순위'] - df_summary['현재_순위']).apply(lambda x: f"▲ {int(x)}" if x > 0 else (f"▼ {abs(int(x))}" if x < 0 else "-"))
        else: df_summary['랭킹추세'] = "-"
    else: df_summary['랭킹추세'] = "-"

    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    render_product_header(df_summary)
    st.markdown(render_macro_cards(core_tickers), unsafe_allow_html=True)

    tab_labels = ["매크로", "테마 히트맵", "알파 레이더", "종목 분석", "백테스트", "주도주 비교"]
    if is_admin:
        tab_labels.append("보유 점검")
    tabs = st.tabs(tab_labels)
    tab1, tab2, tab3, tab4, tab5, tab6 = tabs[:6]
    tab7 = tabs[6] if is_admin and len(tabs) > 6 else None

    # --- 탭 1: 매크로 인사이트 ---
    with tab1:
        render_section_header("오늘의 매크로 리포트", "시장 상태와 매매 강도를 먼저 확인하고 세부 리포트로 내려갑니다.", badge_text="Macro Mode")
        macro_refs = get_macro_headline_news()
        render_market_regime_panel(df_summary, macro_refs)
        st.markdown("##### 오늘의 액션 브리프")
        st.caption("신규매수는 시장 상태와 수급 후보가 동시에 맞을 때만 압축해서 봅니다.")
        render_action_brief(df_summary, macro_refs)
        st.markdown("---")
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()
            report_content = format_report_for_readability(report_content)

            if is_vip:
                with st.expander("심층 매크로 리포트 전문 보기", expanded=False):
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
                render_theme_flow_summary(df_summary)
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
        st.caption("스윙점수는 전체 매매 우선순위이고, 진입유형/신규후보는 정배열·눌림목·돌파·테마중복·시장상태까지 통과한 실행 구분입니다.")
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
        
        df_display_all = df_summary if is_vip else df_summary.head(5)
        if "alpha_card_limit" not in st.session_state:
            st.session_state.alpha_card_limit = 10
        if st.session_state.view_mode == "table":
            df_display = df_display_all
        else:
            df_display = df_display_all.head(int(st.session_state.alpha_card_limit))

        if st.session_state.view_mode == "card":
            html_lines = []
            current_group = None
            for idx, row in df_display.iterrows():
                rank = int(row['현재_순위'])
                name = row['종목명']
                sector = safe_get(row, '테마표시', safe_get(row, '섹터', '분류안됨'))
                price = f"{safe_get(row, '현재가', 0):,.0f}"
                chg = float(safe_get(row, '등락률', 0))
                chg_color = "#FF4B4B" if chg > 0 else "#3B82F6" if chg < 0 else "#AAAAAA"
                chg_str = f"▲ {chg:.2f}%" if chg > 0 else f"▼ {abs(chg):.2f}%" if chg < 0 else "0.00%"
                ai_score = float(safe_get(row, 'AI수급점수', 0))
                ai_rank = int(safe_get(row, 'AI순위', 0))
                rank_chg = safe_get(row, '랭킹추세', '-')
                f_str = f"{float(safe_get(row, '외인강도(%)', 0)):.1f}%"
                p_str = f"{float(safe_get(row, '연기금강도(%)', 0)):.1f}%"
                entry_type = safe_get(row, '진입유형', '관찰')
                buy_tag = safe_get(row, '매수후보', '관찰')
                swing_score = float(safe_get(row, '스윙우선순위', 0))
                inst_score = float(safe_get(row, '기관동행점수', 0))
                sell_check = safe_get(row, '매도점검', '보유/관찰')
                group_label = "오늘 매수 후보" if buy_tag == "신규후보" else ("관찰 후보" if buy_tag == "관찰" else "제외/후순위")
                if group_label != current_group:
                    current_group = group_label
                    html_lines.append(f'<div style="color:#A7B0C2; font-size:0.82em; font-weight:800; margin:10px 0 6px 2px;">{group_label}</div>')
                tag_bg = "#183323" if buy_tag == "신규후보" else ("#3A2F13" if entry_type in ["눌림목", "돌파"] else "#2A2A35")
                tag_color = "#36C06A" if buy_tag == "신규후보" else ("#FCD34D" if entry_type in ["눌림목", "돌파"] else "#CBD5E1")
                
                rc_color = "#FF4B4B" if "▲" in str(rank_chg) else ("#3B82F6" if "▼" in str(rank_chg) else "#888888")
                
                card_cls = "alpha-card-new" if buy_tag == "신규후보" else ""
                rank_badge_cls = ""
                name_cls = ""
                card_html = f"""
<div class="{card_cls}" style="background:#111b2d; padding:12px 13px; border-radius:10px; margin-bottom:9px; border:1px solid #26324A; box-shadow:0 6px 16px rgba(0,0,0,0.14);">
<div class="alpha-card-head">
<div class="alpha-card-left">
<div class="alpha-chip-row">
<span class="{rank_badge_cls}" style="background:#172033; border:1px solid #334155; color:#DDE6F5; font-size:0.7em; font-weight:800; padding:4px 8px; border-radius:999px; white-space:nowrap;">스윙 {rank}위</span>
<span style="font-size: 0.72em; font-weight: 800; color: {tag_color}; background:{tag_bg}; border:1px solid #374151; padding:4px 7px; border-radius:999px; white-space:nowrap;">{buy_tag} · {entry_type}</span>
<span style="font-size: 0.8em; font-weight: bold; color: {rc_color}; white-space: nowrap;">{rank_chg}</span>
</div>
<div class="alpha-name-row">
<span class="{name_cls} alpha-stock-name">{html.escape(str(name))}</span>
<span style="font-size:0.74em; color:#AAB2C5; padding:3px 7px; background:#172033; border:1px solid #2D3A55; border-radius:999px; display:inline-block;">{html.escape(str(sector))}</span>
</div>
</div>
<div class="alpha-price-box">
<div style="font-size: 1.1em; font-weight: 700; color: #FFF;">{price}원</div>
<div style="font-size: 0.9em; font-weight: 800; color: {chg_color};">{chg_str}</div>
</div>
</div>
<div style="display:flex; justify-content:space-between; font-size:0.83em; color:#DDD; background:#0f1726; padding:9px 10px; border-radius:8px; align-items:center; flex-wrap:wrap; gap:8px; border:1px solid #243047;">
<div>스윙 <b style="color:#FCD34D;">{swing_score:.1f}</b> <span style="color:#7E899E;">/ AI {ai_score:.1f} · {ai_rank}위</span></div>
<div>기금 <b style="color:#FCA5A5;">{p_str}</b> <span style="color:#3A4558;">|</span> 기관동행 <b style="color:#86EFAC;">{inst_score:.1f}</b></div>
<div style="width:100%; color:#A7B0C0; font-size:0.82em;">점검: {html.escape(str(sell_check))}</div>
</div>
</div>
"""
                html_lines.append(card_html.strip())
            
            cards_container_html = f"<div style='padding: 5px;'>{''.join(html_lines)}</div>"
            st.markdown(cards_container_html, unsafe_allow_html=True)

            total_cards = len(df_display_all)
            shown_cards = len(df_display)
            if is_vip and shown_cards < total_cards:
                if st.button(f"10종목 더보기 ({shown_cards}/{total_cards})", use_container_width=True):
                    st.session_state.alpha_card_limit = min(total_cards, int(st.session_state.alpha_card_limit) + 10)
                    st.rerun()
            
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
                "신호신뢰도": "{:.1f}", "점수변화(안정화)": "{:+.2f}",
                "스윙우선순위": "{:.2f}", "기관동행점수": "{:.2f}",
                "수급품질점수": "{:.1f}", "주도주점수": "{:.1f}", "수급흡수율": "{:.2f}", "거래대금활력": "{:.2f}",
                "20일평균거래대금(억)": "{:,.0f}",
                "연기금5일강도(%)": "{:.2f}%", "연기금10일강도(%)": "{:.2f}%"
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

            base_columns = ["_index", "매수후보", "진입유형", "스윙우선순위", "테마표시", "AI수급점수", "AI순위", "매도점검", "현재가", "등락률", "소속"]
            advanced_columns = ["전략슬리브", "기관동행점수", "수급품질점수", "주도주점수", "수급흡수율", "수급지속일수", "종목체급", "거래대금활력", "20일평균거래대금(억)", "정배열", "추세품질점수", "MA5", "MA10", "MA20", "연기금5일강도(%)", "연기금10일강도(%)", "외인강도(%)", "연기금강도(%)", "투신강도(%)", "사모강도(%)", "이격도(%)", "손바뀜(%)", "외인연속", "연기금연속", "신호등급", "신호신뢰도", "점수변화(안정화)", "시가총액"]
            current_columns = base_columns + advanced_columns if show_advanced else base_columns

            event = st.dataframe(
                styled_df, on_select="rerun", selection_mode="single-row",
                column_config={
                    "_index": st.column_config.TextColumn("종목명", width="small"), 
                    "매수후보": st.column_config.Column("후보", width="small"),
                    "진입유형": st.column_config.Column("진입", width="small"),
                    "스윙우선순위": st.column_config.NumberColumn("스윙", width="small"),
                    "매도점검": st.column_config.Column("점검", width="small"),
                    "전략슬리브": st.column_config.Column("슬리브", width="small"),
                    "기관동행점수": st.column_config.NumberColumn("기관동행", width="small"),
                    "수급품질점수": st.column_config.NumberColumn("수급품질", width="small", format="%.1f"),
                    "주도주점수": st.column_config.NumberColumn("주도주", width="small", format="%.1f"),
                    "수급흡수율": st.column_config.NumberColumn("흡수율", width="small", format="%.2f"),
                    "수급지속일수": st.column_config.NumberColumn("수급일수", width="small", format="%d일"),
                    "종목체급": st.column_config.Column("체급", width="small"),
                    "거래대금활력": st.column_config.NumberColumn("거래활력", width="small", format="%.2f"),
                    "20일평균거래대금(억)": st.column_config.NumberColumn("평균대금", width="small", format="%.0f억"),
                    "정배열": st.column_config.CheckboxColumn("정배열", width="small"),
                    "추세품질점수": st.column_config.NumberColumn("추세품질", width="small", format="%.0f"),
                    "MA5": st.column_config.NumberColumn("MA5", width="small", format="%.0f"),
                    "MA10": st.column_config.NumberColumn("MA10", width="small", format="%.0f"),
                    "MA20": st.column_config.NumberColumn("MA20", width="small", format="%.0f"),
                    "연기금5일강도(%)": st.column_config.NumberColumn("기금5D", width="small"),
                    "연기금10일강도(%)": st.column_config.NumberColumn("기금10D", width="small"),
                    "테마표시": st.column_config.Column("테마", width="medium"), 
                    "랭킹추세": st.column_config.Column("순위변동", width="small"), 
                    "AI수급점수": st.column_config.NumberColumn("🏆 AI점수", width="small", format="%.2f"),
                    "AI순위": st.column_config.NumberColumn("AI순위", width="small", format="%d위"),
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
        entry_type = safe_get(selected_row, '진입유형', '관찰')
        buy_candidate = safe_get(selected_row, '매수후보', '관찰')
        swing_priority = float(safe_get(selected_row, '스윙우선순위', 0))
        inst_score = float(safe_get(selected_row, '기관동행점수', 0))
        sell_check = safe_get(selected_row, '매도점검', '보유/관찰')
        entry_comment = safe_get(selected_row, '진입코멘트', '추가 확인 필요')

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
        render_stock_decision_panel(selected_row)
        st.markdown(
            f"""
            <div class="premium-panel">
                <div style="display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap;">
                    <div style="color:#E5E7EB; font-size:0.95em; font-weight:800;">실전 해설</div>
                    <div style="color:#9CA3AF; font-size:0.78em;">모바일은 터치 기반으로 아래 해설을 확인하세요</div>
                </div>
                <div class="premium-chip-row">
                    <span class="premium-chip">후보 {buy_candidate}</span>
                    <span class="premium-chip">진입유형 {entry_type}</span>
                    <span class="premium-chip">스윙 {swing_priority:.1f}</span>
                    <span class="premium-chip">기관동행 {inst_score:.1f}</span>
                    <span class="premium-chip">점검 {sell_check}</span>
                    <span class="premium-chip">신호등급 {signal_grade}</span>
                    <span class="premium-chip">신호신뢰도 {signal_conf:.1f}</span>
                    <span class="premium-chip">안정화Δ {score_delta:+.2f}</span>
                </div>
                <div style="color:#CBD5E1; font-size:0.84em; margin-top:8px;">{entry_comment}</div>
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
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">
                                <b>한눈에 보는 실행 우선순위</b>입니다. <br/>
                                • <b>High</b>: 지금 조건에서 상대적으로 유리한 구간 <br/>
                                • <b>Medium</b>: 관찰/분할 접근 구간 <br/>
                                • <b>Low</b>: 보수적 대응(진입 지연/비중 축소) 권장 <br/>
                                ※ 장세(VIX)가 불안할수록 등급 기준이 더 보수적으로 바뀝니다.
                            </div>
                        </div>
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#93C5FD; font-size:0.78em; font-weight:700;">신호신뢰도</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">
                                <b>0~100점 종합 점수</b>로, 정량(수급/기술) + 정성(뉴스/공시 톤)을 함께 반영합니다. <br/>
                                • 높을수록 “신호가 깨질 확률”이 상대적으로 낮다는 의미 <br/>
                                • 낮을수록 뉴스 노이즈/수급 불일치 가능성을 의심해야 합니다. <br/>
                                <span style="color:#94A3B8;">실전 팁: 같은 등급이면 신호신뢰도가 더 높은 종목을 우선 확인하세요.</span>
                            </div>
                        </div>
                        <div style="background:#161F31; border:1px solid #2A344A; border-radius:10px; padding:8px 10px;">
                            <div style="color:#86EFAC; font-size:0.78em; font-weight:700;">안정화Δ</div>
                            <div style="color:#D1D5DB; font-size:0.82em; margin-top:4px;">
                                <b>전일 대비 점수 변화량</b>입니다(과도한 출렁임을 줄인 값). <br/>
                                • <b>+값</b>: 점수가 개선되는 흐름 <br/>
                                • <b>-값</b>: 점수가 약해지는 흐름 <br/>
                                • 절대값이 클수록 변화 속도가 빠릅니다.
                            </div>
                        </div>
                    </div>
                    <div style="margin-top:8px; background:#111827; border:1px dashed #334155; border-radius:10px; padding:8px 10px; color:#CBD5E1; font-size:0.80em; line-height:1.5;">
                        <b>읽는 순서 추천</b>: 신호등급 → 신호신뢰도 → 안정화Δ <br/>
                        예) <b>High + 신뢰도 높음 + Δ 플러스</b>면 우선 검토, <b>Low + 신뢰도 낮음 + Δ 마이너스</b>면 보수적으로 대응하세요.
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
            display_final = round(float(swing_priority), 1)

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
                        <div class="hero-label">스윙 점수</div>
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
            # 선택 종목의 최근 순위 추이(낮을수록 상위이므로 축을 반전해 표시)
            if csv_exists("score_trend.csv") or table_exists("score_trend"):
                df_rank_trend = load_score_trend_safe()
                if not df_rank_trend.empty and {"날짜", "종목명", "순위"}.issubset(df_rank_trend.columns):
                    if "스윙우선순위" in df_rank_trend.columns:
                        rt = df_rank_trend.copy()
                        if "매수후보" not in rt.columns:
                            rt["매수후보"] = "관찰"
                        rt["스윙우선순위"] = pd.to_numeric(rt["스윙우선순위"], errors="coerce").fillna(0.0)
                        rt["AI수급점수"] = pd.to_numeric(rt["AI수급점수"], errors="coerce").fillna(0.0)
                        rt["_display_order"] = rt["매수후보"].map(candidate_order).fillna(1)
                        rt = rt.sort_values(["날짜", "_display_order", "스윙우선순위", "AI수급점수"], ascending=[True, True, False, False])
                        rt["순위"] = rt.groupby("날짜").cumcount() + 1
                        df_rank_trend = rt.drop(columns=["_display_order"], errors="ignore")
                    target_rank = df_rank_trend[df_rank_trend["종목명"] == target_stock].copy()
                    if not target_rank.empty:
                        target_rank["순위"] = pd.to_numeric(target_rank["순위"], errors="coerce")
                        target_rank = target_rank.dropna(subset=["순위"])
                        target_rank["날짜_dt"] = pd.to_datetime(target_rank["날짜"], errors="coerce")
                        target_rank = target_rank.dropna(subset=["날짜_dt"]).sort_values("날짜_dt")
                        target_rank = target_rank.drop_duplicates(subset=["날짜_dt"], keep="last").tail(20)
                        if not target_rank.empty:
                            target_rank["날짜_표시"] = target_rank["날짜_dt"].dt.strftime("%m/%d")
                            rank_min = float(target_rank["순위"].min())
                            rank_max = float(target_rank["순위"].max())
                            pad = max(1.0, (rank_max - rank_min) * 0.12)
                            rank_chart = (
                                alt.Chart(target_rank)
                                .mark_line(color="#7DD3FC", point=True)
                                .encode(
                                    x=alt.X("날짜_표시:O", sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                                    y=alt.Y(
                                        "순위:Q",
                                        title="순위 (낮을수록 상위)",
                                        scale=alt.Scale(domain=[rank_max + pad, max(1.0, rank_min - pad)])
                                    ),
                                    tooltip=[
                                        alt.Tooltip("날짜:O"),
                                        alt.Tooltip("순위:Q", format=".0f"),
                                    ],
                                )
                                .properties(height=190)
                            )
                            st.markdown("##### 최근 스윙 순위 추이")
                            st.altair_chart(apply_altair_theme(rank_chart), width="stretch")

            with st.expander("상세 분석 펼치기", expanded=False):
                st.markdown(
                    f"""
                    <div class="stock-grid">
                        <div class="stock-card">
                            <div class="stock-label">스윙 점수</div>
                            <div class="stock-value">{float(swing_priority):.1f}점</div>
                            <div class="stock-sub">전체 {int(cur_rank)}위 / {rank_trend} / AI {float(ai_score):.1f}</div>
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

                st.markdown("##### 스윙 점수 구조")
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
                            <div class="score-kpi-label">스윙 점수</div>
                            <div class="score-kpi-value">{display_final:.1f}</div>
                        </div>
                        <div class="score-kpi">
                            <div class="score-kpi-label">AI 점수</div>
                            <div class="score-kpi-value">{float(ai_score):.1f}</div>
                        </div>
                        <div class="score-kpi">
                            <div class="score-kpi-label">정성 보정</div>
                            <div class="score-kpi-value">{qual_adj:+.1f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                trend_quality = float(safe_get(selected_row, "추세품질점수", 50))
                is_aligned = bool(safe_get(selected_row, "정배열", True))
                neg_news_cnt = float(safe_get(selected_row, "뉴스부정키워드수", 0))
                entry_effect = 7.0 if entry_type == "눌림목" else (5.0 if entry_type == "돌파" else (-7.0 if entry_type == "과열주의" else (-18.0 if entry_type == "회피" else 0.0)))
                trend_effect = max(0.0, min(5.0, (trend_quality - 55.0) / 9.0))
                if not is_aligned:
                    trend_effect -= 18.0
                if trend_quality < 55:
                    trend_effect -= 6.0
                if trend_quality < 45:
                    trend_effect -= 8.0
                news_effect = -neg_news_cnt * 1.2
                st.markdown(
                    f"""
                    <div class="stock-grid">
                        <div class="stock-card">
                            <div class="stock-label">AI 기반값</div>
                            <div class="stock-value">{float(ai_score) * 0.58:.1f}</div>
                            <div class="stock-sub">AI {float(ai_score):.1f} × 0.58</div>
                        </div>
                        <div class="stock-card">
                            <div class="stock-label">기관동행 기여</div>
                            <div class="stock-value">{inst_score * 0.85:.1f}</div>
                            <div class="stock-sub">기관동행 {inst_score:.1f} × 0.85</div>
                        </div>
                        <div class="stock-card">
                            <div class="stock-label">진입유형 효과</div>
                            <div class="stock-value">{entry_effect:+.1f}</div>
                            <div class="stock-sub">{entry_type}</div>
                        </div>
                        <div class="stock-card">
                            <div class="stock-label">추세/뉴스 보정</div>
                            <div class="stock-value">{trend_effect + news_effect:+.1f}</div>
                            <div class="stock-sub">추세품질 {trend_quality:.0f} · 정배열 {'Y' if is_aligned else 'N'} · 부정뉴스 {neg_news_cnt:.0f}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                st.caption(
                    f"정성 점수 {qual_score:.1f} | {score_mode} · "
                    "AI점수는 수급/기술/뉴스를 섞은 원점수, 스윙점수는 AI점수에 기관동행·진입유형·추세품질·시장위험을 다시 반영한 매매 우선순위입니다."
                )

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

            st.markdown(f"##### {APP_NAME} 종목 진단")
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
                        
                        today_str = now_kst().strftime("%Y년 %m월 %d일")
                        
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
        render_section_header(f"{APP_NAME} 스윙 백테스트", "종가 진입 후 D+5/D+10 종가 청산 기준으로 성과를 확인합니다.", badge_text="Backtest")
        if not is_vip:
            show_premium_paywall("가상 포트폴리오 누적 수익률 및 성과 분석은 코드 인증 후 확인할 수 있습니다.")
        else:
            df_swing_perf = load_swing_performance_safe()
            df_swing_trades = load_swing_trades_safe()
            available_dates = pd.to_datetime(df_swing_trades.get("진입일_dt", pd.Series(dtype="datetime64[ns]")), errors="coerce").dropna()
            if available_dates.empty and not df_swing_perf.empty:
                available_dates = pd.to_datetime(df_swing_perf.get("날짜_dt", pd.Series(dtype="datetime64[ns]")), errors="coerce").dropna()
            if not available_dates.empty:
                min_date = available_dates.min().date()
                hist_dates = pd.to_datetime(df_history.get("일자", pd.Series(dtype=str)).astype(str).str.replace("-", "", regex=False), format="%Y%m%d", errors="coerce").dropna()
                max_date = hist_dates.max().date() if not hist_dates.empty else available_dates.max().date()
                selected_start_date = st.date_input("🗓️ 벤치마크 시작(기준)일 선택", min_value=min_date, max_value=max_date, value=min_date)
                bt_col1, bt_col2 = st.columns(2)
                with bt_col1:
                    cash_text = st.text_input("초기 투자금", value="5,000,000")
                    try:
                        backtest_initial_cash = int(str(cash_text).replace(",", "").strip())
                    except Exception:
                        backtest_initial_cash = 5_000_000
                    backtest_initial_cash = max(1_000_000, min(100_000_000, backtest_initial_cash))
                    st.caption(f"적용 금액: {backtest_initial_cash:,}원")
                with bt_col2:
                    backtest_max_positions = st.number_input(
                        "최대 보유 종목수",
                        min_value=1,
                        max_value=5,
                        value=3,
                        step=1,
                        format="%d",
                    )
                portfolio_perf, portfolio_positions, portfolio_closed = build_capital_limited_swing_sim(
                    df_swing_trades,
                    df_history,
                    initial_cash=backtest_initial_cash,
                    max_positions=backtest_max_positions,
                    start_date=selected_start_date,
                )
            else:
                portfolio_perf, portfolio_positions, portfolio_closed = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
                backtest_initial_cash = 5_000_000
                backtest_max_positions = 3

            if not portfolio_perf.empty:
                portfolio_perf["날짜_dt"] = pd.to_datetime(portfolio_perf["날짜"], errors="coerce")
                df_filtered = portfolio_perf[portfolio_perf['날짜_dt'].dt.date >= selected_start_date].copy()

                if not df_filtered.empty:
                    df_filtered["전략 누적수익률"] = pd.to_numeric(df_filtered["수익률(%)"], errors="coerce").fillna(0.0)
                    equity = 1.0 + (df_filtered["전략 누적수익률"] / 100.0)
                    df_filtered["기간 MDD"] = (((equity / equity.cummax()) - 1.0) * 100.0).round(2)

                    benchmark_fetch_errors = []
                    try:
                        def compute_benchmark_returns(ticker_symbol):
                            hist = fetch_yahoo_chart_history(ticker_symbol, range_period="2y", interval="1d")
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
                            base_df = hist[hist.index <= pd.to_datetime(selected_start_date)]
                            base_close = float(base_df['Close'].dropna().iloc[-1]) if not base_df.empty and not base_df['Close'].dropna().empty else None
                            if base_close is None or base_close == 0:
                                benchmark_fetch_errors.append(ticker_symbol)
                                return [float("nan")] * len(df_filtered)

                            rets = []
                            for d in df_filtered['날짜_dt']:
                                close_series = hist[hist.index <= d]['Close'].dropna()
                                if close_series.empty:
                                    rets.append(rets[-1] if rets else float("nan"))
                                    continue
                                ret = ((float(close_series.iloc[-1]) - base_close) / base_close) * 100.0
                                rets.append(ret if not pd.isna(ret) else (rets[-1] if rets else float("nan")))
                            return rets

                        df_filtered['KOSPI 누적수익률'] = compute_benchmark_returns('^KS11')
                        df_filtered['NASDAQ 누적수익률'] = compute_benchmark_returns('^IXIC')
                    except Exception as e:
                        print(f"[WARN] 벤치마크 수익률 계산 실패: {e}")
                        df_filtered['KOSPI 누적수익률'] = [float("nan")] * len(df_filtered)
                        df_filtered['NASDAQ 누적수익률'] = [float("nan")] * len(df_filtered)
                        benchmark_fetch_errors = ['^KS11', '^IXIC']

                    chart_df = df_filtered.copy()
                    baseline_row = {c: None for c in chart_df.columns}
                    baseline_row.update({
                        "날짜": selected_start_date.strftime("%Y-%m-%d"),
                        "날짜_dt": pd.to_datetime(selected_start_date),
                        "일간수익률": 0.0,
                        "전략 누적수익률": 0.0,
                        "기간 MDD": 0.0,
                        "KOSPI 누적수익률": 0.0,
                        "NASDAQ 누적수익률": 0.0,
                        "평가금액": float(backtest_initial_cash),
                        "현금": float(backtest_initial_cash),
                        "투자금액": 0,
                        "보유종목수": 0,
                    })
                    chart_df = pd.concat([pd.DataFrame([baseline_row]), chart_df], ignore_index=True)
                    chart_df = chart_df.drop_duplicates(subset=["날짜_dt"], keep="first").sort_values("날짜_dt")

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

                    closed_trades = portfolio_closed.copy()
                    if "수익률" not in closed_trades.columns:
                        closed_trades["수익률"] = 0.0
                    closed_trades["수익률"] = pd.to_numeric(closed_trades["수익률"], errors="coerce").fillna(0.0)
                    if "청산일" in closed_trades.columns:
                        closed_trades["청산일_dt"] = pd.to_datetime(closed_trades["청산일"], errors="coerce")
                    if "보유일수" not in closed_trades.columns:
                        closed_trades["보유일수"] = 0
                    primary_open_trades = portfolio_positions.copy()
                    latest_entry_date = pd.to_datetime(primary_open_trades["진입일"], errors="coerce").max() if not primary_open_trades.empty else None
                    new_candidates = primary_open_trades[pd.to_datetime(primary_open_trades["진입일"], errors="coerce").eq(latest_entry_date)].copy() if latest_entry_date is not None else pd.DataFrame()
                    win_rate = (closed_trades["수익률"].gt(0).mean() * 100.0) if not closed_trades.empty else 0.0
                    avg_ret = float(closed_trades["수익률"].mean()) if not closed_trades.empty else 0.0
                    d5 = closed_trades[closed_trades["보유일수"] == 5]
                    signal_closed = closed_trades[closed_trades.get("청산방식", "").astype(str).eq("시그널")] if "청산방식" in closed_trades.columns else closed_trades
                    d10 = closed_trades[closed_trades["보유일수"] == 10]
                    d5_ret = float(d5["수익률"].mean()) if not d5.empty else 0.0
                    d10_ret = float(d10["수익률"].mean()) if not d10.empty else 0.0

                    current_port_ret = _safe_last(df_filtered['전략 누적수익률'])
                    current_equity = _safe_last(df_filtered["평가금액"])
                    current_cash = _safe_last(df_filtered["현금"])
                    current_kospi_ret = _safe_last(df_filtered['KOSPI 누적수익률'])
                    current_nasdaq_ret = _safe_last(df_filtered['NASDAQ 누적수익률'])
                    current_mdd = float(df_filtered["기간 MDD"].min()) if "기간 MDD" in df_filtered.columns else 0.0
                    current_risk_state = "High" if current_mdd <= -8 else ("Medium" if current_mdd <= -4 else "Low")
                    port_daily_diff = _safe_daily_diff(df_filtered['전략 누적수익률'])
                    kospi_daily_diff = _safe_daily_diff(df_filtered['KOSPI 누적수익률'])
                    nasdaq_daily_diff = _safe_daily_diff(df_filtered['NASDAQ 누적수익률'])
                    alpha_kospi = current_port_ret - current_kospi_ret
                    alpha_nasdaq = current_port_ret - current_nasdaq_ret
                    alpha_color = "#36C06A" if alpha_kospi >= 0 else "#E04B4B"
                    alpha_nasdaq_color = "#36C06A" if alpha_nasdaq >= 0 else "#E04B4B"
                    port_delta_color = "#36C06A" if port_daily_diff >= 0 else "#E04B4B"
                    kospi_delta_color = "#36C06A" if kospi_daily_diff >= 0 else "#E04B4B"
                    nasdaq_delta_color = "#36C06A" if nasdaq_daily_diff >= 0 else "#E04B4B"

                    st.markdown(
                        f"""
                        <div class="kpi-grid">
                            <div class="kpi-card">
                                <div class="kpi-title">1,000만원 포트폴리오 수익률</div>
                                <div class="kpi-value">{current_port_ret:+.2f}%</div>
                                <span class="kpi-delta" style="background: rgba(54,192,106,0.18); color:{port_delta_color};">최근 {port_daily_diff:+.2f}%</span>
                                <div class="kpi-meta">평가금액 {current_equity:,.0f}원 · 현금 {current_cash:,.0f}원</div>
                            </div>
                            <div class="kpi-card">
                                <div class="kpi-title">KOSPI 누적 수익률</div>
                                <div class="kpi-value">{current_kospi_ret:+.2f}%</div>
                                <span class="kpi-delta" style="background: rgba(59,130,246,0.16); color:{kospi_delta_color};">최근 {kospi_daily_diff:+.2f}%</span>
                                <div class="kpi-meta">초과 성과 <span style="color:{alpha_color}; font-weight:700;">{alpha_kospi:+.2f}%p</span></div>
                            </div>
                            <div class="kpi-card">
                                <div class="kpi-title">NASDAQ 누적 수익률</div>
                                <div class="kpi-value">{current_nasdaq_ret:+.2f}%</div>
                                <span class="kpi-delta" style="background: rgba(167,139,250,0.16); color:{nasdaq_delta_color};">최근 {nasdaq_daily_diff:+.2f}%</span>
                                <div class="kpi-meta">초과 성과 <span style="color:{alpha_nasdaq_color}; font-weight:700;">{alpha_nasdaq:+.2f}%p</span></div>
                            </div>
                            <div class="kpi-card">
                                <div class="kpi-title">진행 중 스윙</div>
                                <div class="kpi-value">{len(primary_open_trades):,}</div>
                                <span class="kpi-delta" style="background: rgba(252,211,77,0.16); color:#FCD34D;">최대 5종목</span>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    st.markdown(
                        f"""
                        <div class="score-kpi-grid">
                            <div class="score-kpi"><div class="score-kpi-label">종료 거래</div><div class="score-kpi-value">{len(closed_trades):,}</div></div>
                            <div class="score-kpi"><div class="score-kpi-label">승률</div><div class="score-kpi-value">{win_rate:.1f}%</div></div>
                            <div class="score-kpi"><div class="score-kpi-label">평균 수익률</div><div class="score-kpi-value">{avg_ret:+.2f}%</div></div>
                        </div>
                        <div class="score-kpi-grid">
                            <div class="score-kpi"><div class="score-kpi-label">D+5 평균</div><div class="score-kpi-value">{d5_ret:+.2f}%</div></div>
                            <div class="score-kpi"><div class="score-kpi-label">시그널 종료</div><div class="score-kpi-value">{len(signal_closed):,}</div></div>
                            <div class="score-kpi"><div class="score-kpi-label">신규 후보</div><div class="score-kpi-value">{len(new_candidates):,}</div></div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                    st.markdown("<br>", unsafe_allow_html=True)
                    chart_df['날짜_표시'] = chart_df['날짜_dt'].dt.strftime('%m/%d')
                    df_melt = chart_df.melt(
                        id_vars=['날짜_표시'],
                        value_vars=['전략 누적수익률', 'KOSPI 누적수익률', 'NASDAQ 누적수익률'],
                        var_name='포트폴리오',
                        value_name='차트수익률'
                    )
                    base_chart = alt.Chart(df_melt).mark_line(point=True).encode(
                        x=alt.X('날짜_표시:O', axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('차트수익률:Q', title="누적 수익률 (%)"),
                        color=alt.Color(
                            '포트폴리오:N',
                            scale=alt.Scale(
                                domain=['전략 누적수익률', 'KOSPI 누적수익률', 'NASDAQ 누적수익률'],
                                range=['#E74C3C', '#AAAAAA', '#A78BFA']
                            ),
                            legend=alt.Legend(title=None, orient='bottom')
                        )
                    ).properties(height=300)
                    st.altair_chart(apply_altair_theme(base_chart), width='stretch')
                    slot_cash = float(backtest_initial_cash) / max(1, int(backtest_max_positions))
                    st.caption(f"전략선은 초기자금 {int(backtest_initial_cash):,}원, 최대 {int(backtest_max_positions)}종목, 종목당 약 {int(slot_cash):,}원 배정, 중복 보유 금지 기준의 가상 포트폴리오 평가수익률입니다.")

                    if benchmark_fetch_errors:
                        err_names = ", ".join("KOSPI" if x == "^KS11" else ("NASDAQ" if x == "^IXIC" else x) for x in sorted(set(benchmark_fetch_errors)))
                        st.caption(f"일부 벤치마크 데이터가 지연되어 표시되지 않았습니다: {err_names}")

                    with st.expander("리스크 보조 지표", expanded=False):
                        st.caption(f"MDD는 누적수익률이 고점 대비 얼마나 내려왔는지 보는 지표입니다. 선택 기간 기준 현재 최대낙폭은 {current_mdd:.2f}%이고 상태는 {current_risk_state}입니다.")

                    if not new_candidates.empty:
                        today_view = new_candidates.copy()
                        today_view["추천구분"] = "신규 후보"
                        today_cols = ["진입일", "종목명", "매수금액", "진입가", "수량", "추천구분"]
                        st.markdown("#### 오늘 신규 스윙 후보")
                        st.dataframe(
                            today_view[today_cols].style.format({
                                "매수금액": "{:,.0f}원",
                                "진입가": "{:,.0f}",
                                "수량": "{:,.0f}",
                            }),
                            hide_index=True,
                            width='stretch'
                        )

                    if not primary_open_trades.empty:
                        open_view = primary_open_trades.copy()
                        current_signal_cols = ["종목명", "스윙우선순위", "현재_순위", "매수후보", "진입유형", "매도점검"]
                        signal_now = df_summary[[c for c in current_signal_cols if c in df_summary.columns]].copy()
                        open_view = pd.merge(open_view, signal_now, on="종목명", how="left")
                        open_view["매도알림"] = open_view["매도점검"].fillna("보유/관찰").astype(str).apply(
                            lambda x: "매도 점검" if any(k in x for k in ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]) else "보유"
                        )
                        sell_alerts = open_view[open_view["매도알림"].eq("매도 점검")].copy()
                        if not sell_alerts.empty:
                            st.warning("진행 중 포지션 중 매도 점검 신호가 있습니다.")
                            st.dataframe(
                                sell_alerts[["종목명", "평가수익률", "스윙우선순위", "매수후보", "진입유형", "매도점검"]].style.format({
                                    "평가수익률": "{:+.2f}%",
                                    "스윙우선순위": "{:.2f}",
                                }),
                                hide_index=True,
                                width="stretch",
                            )

                        open_summary = open_view.rename(columns={
                            "진입일": "최근진입일",
                            "평가수익률": "최근평가수익률",
                            "스윙우선순위": "최근점수",
                        }).copy()
                        open_summary["신호횟수"] = 1
                        open_summary["최고순위"] = pd.to_numeric(open_summary.get("현재_순위", 0), errors="coerce").fillna(0).astype(int)
                        open_summary["평균평가수익률"] = pd.to_numeric(open_summary["최근평가수익률"], errors="coerce").fillna(0.0)
                        open_summary["최근점수"] = pd.to_numeric(open_summary["최근점수"], errors="coerce").fillna(0.0)
                        open_summary["방향"] = open_summary["최근평가수익률"].apply(lambda v: "plus" if v >= 0 else "minus")
                        open_summary = open_summary.sort_values(["최근평가수익률", "최근점수"], ascending=[False, False])

                        st.markdown("#### 진행 중 종목 요약")
                        base_open_chart = alt.Chart(open_summary).encode(
                            y=alt.Y(
                                "종목명:N",
                                sort="-x",
                                title=None,
                                axis=alt.Axis(labelLimit=180, labelPadding=6),
                            ),
                            x=alt.X("최근평가수익률:Q", title="현재평가수익률 (%)"),
                        )
                        open_bar = base_open_chart.mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
                            color=alt.Color(
                                "방향:N",
                                scale=alt.Scale(domain=["plus", "minus"], range=["#36C06A", "#E04B4B"]),
                                legend=None,
                            ),
                            tooltip=[
                                alt.Tooltip("종목명:N"),
                                alt.Tooltip("최근평가수익률:Q", format="+.2f", title="최근 평가수익률"),
                                alt.Tooltip("평균평가수익률:Q", format="+.2f", title="평균 평가수익률"),
                                alt.Tooltip("신호횟수:Q", title="신호 횟수"),
                                alt.Tooltip("최근진입일:N", title="최근 진입일"),
                            ],
                        )
                        open_text = base_open_chart.mark_text(
                            align="left",
                            baseline="middle",
                            dx=5,
                            color="#CBD5E1",
                            fontSize=11,
                        ).encode(text=alt.Text("최근평가수익률:Q", format="+.1f"))
                        open_chart = (open_bar + open_text).properties(height=max(220, min(520, 32 * len(open_summary))))
                        st.altair_chart(apply_altair_theme(open_chart), width="stretch")

                        summary_cols = ["종목명", "최근진입일", "보유일수", "최고순위", "최근점수", "최근평가수익률", "매도알림"]
                        st.dataframe(
                            open_summary[summary_cols].style.format({
                                "최근점수": "{:.2f}",
                                "최근평가수익률": "{:+.2f}%",
                            }),
                            hide_index=True,
                            width='stretch'
                        )

                        open_cols = ["진입일", "종목명", "보유일수", "진입가", "현재가", "수량", "매수금액", "평가금액", "평가손익", "평가수익률", "매도점검", "상태"]
                        with st.expander(f"진행 중인 {APP_NAME} 스윙 후보", expanded=False):
                            st.dataframe(
                                open_view.sort_values(["진입일", "종목명"], ascending=[False, True])[open_cols].style.format({
                                    "진입가": "{:,.0f}",
                                    "현재가": "{:,.0f}",
                                    "매수금액": "{:,.0f}원",
                                    "평가금액": "{:,.0f}원",
                                    "평가손익": "{:+,.0f}원",
                                    "평가수익률": "{:+.2f}%",
                                }),
                                hide_index=True,
                                width='stretch'
                            )

                    if not closed_trades.empty:
                        portfolio_log_cols = ["진입일", "청산일", "종목명", "보유일수", "매수금액", "청산금액", "실현손익", "수익률", "청산사유"]
                        legacy_log_cols = ["진입일", "청산일", "종목명", "진입순위", "보유일수", "청산방식", "청산사유", "진입가", "청산가", "수익률", "진입유형"]
                        view_cols = [c for c in portfolio_log_cols if c in closed_trades.columns]
                        if len(view_cols) < 5:
                            view_cols = [c for c in legacy_log_cols if c in closed_trades.columns]
                        trade_view = closed_trades.sort_values("청산일_dt", ascending=False)[view_cols].head(80)
                        with st.expander("포트폴리오 진입/청산 거래 로그", expanded=False):
                            format_cols = {
                                "매수금액": "{:,.0f}원",
                                "청산금액": "{:,.0f}원",
                                "실현손익": "{:+,.0f}원",
                                "진입가": "{:,.0f}",
                                "청산가": "{:,.0f}",
                                "수익률": "{:+.2f}%",
                            }
                            st.dataframe(
                                trade_view.style.format({k: v for k, v in format_cols.items() if k in trade_view.columns}),
                                hide_index=True,
                                width='stretch'
                            )
                else:
                    render_empty_state("백테스트 데이터 없음", "선택하신 기간에 해당하는 스윙 성과 데이터가 없습니다.")
            else:
                render_empty_state("데이터 대기", "swing_performance.csv가 아직 생성되지 않았습니다. 다음 스크래퍼 실행 후 표시됩니다.")

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
                            너는 {APP_NAME} 수석 퀀트 애널리스트야. 내가 아래에 제공한 2개 종목의 [후보 종목 데이터]를 비교 분석해.
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
            render_section_header("보유 점검", "보유 종목의 손익과 수급 이탈 리스크를 먼저 확인하고 필요할 때만 편집합니다.", badge_text="Portfolio")
            st.caption("매일 보는 화면은 보유 리스크 점검, 편집 기능은 아래 접힘 영역에 배치했습니다.")
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
                join_cols = [
                    "종목명", "현재가", "등락률", "스윙우선순위", "현재_순위", "AI수급점수", "AI순위",
                    "매수후보", "진입유형", "매도점검", "신호등급", "신호신뢰도", "외인강도(%)", "연기금강도(%)"
                ]
                df_joined = pd.merge(
                    df_port_saved,
                    df_summary[join_cols].copy(),
                    on="종목명",
                    how="left",
                )

                f_strength = pd.to_numeric(df_joined["외인강도(%)"], errors="coerce").fillna(0.0)
                p_strength = pd.to_numeric(df_joined["연기금강도(%)"], errors="coerce").fillna(0.0)
                ai_score = pd.to_numeric(df_joined["AI수급점수"], errors="coerce").fillna(0.0)
                swing_score = pd.to_numeric(df_joined["스윙우선순위"], errors="coerce").fillna(0.0)
                qty_num = pd.to_numeric(df_joined["수량"], errors="coerce").fillna(0.0)
                buy_num = pd.to_numeric(df_joined["매수가"], errors="coerce").fillna(0.0)
                cur_num = pd.to_numeric(df_joined["현재가"], errors="coerce").fillna(0.0)
                risk_a = (f_strength < 0) & (p_strength < 0)
                risk_b = swing_score < float(ai_warn_threshold)
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
                    "종목명", "상태", "비중(%)", "매수금액", "수익금액", "수익률(%)",
                    "스윙우선순위", "현재_순위", "AI수급점수", "AI순위", "매수후보", "진입유형", "매도점검", "신호등급", "신호신뢰도",
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
                        "스윙우선순위": "{:.2f}",
                        "현재_순위": "{:.0f}위",
                        "AI수급점수": "{:.2f}",
                        "AI순위": "{:.0f}위",
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
                            swing = float(pd.to_numeric(row.get("스윙우선순위"), errors="coerce") or 0.0)
                            swing_rank = int(pd.to_numeric(row.get("현재_순위"), errors="coerce") or 0)
                            ai_rank = int(pd.to_numeric(row.get("AI순위"), errors="coerce") or 0)
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
                                    <div style="color:#AAB2C5; margin-top:8px; font-size:0.84em;">스윙 {swing:.1f} · {swing_rank}위 | AI {ai:.1f} · {ai_rank}위 | 신호 {row.get('신호등급','-')} ({sig:.1f})</div>
                                    <div style="color:#AAB2C5; margin-top:4px; font-size:0.84em;">{row.get('매수후보','-')} · {row.get('진입유형','-')} · 점검 {row.get('매도점검','-')} | 외인 {fs:+.1f}% · 기금 {ps:+.1f}%</div>
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
                            • <b>복합 경보</b>: 외인/연기금이 <b>동시에 매도 전환</b>이고, 스윙 점수가 기준 미만일 때<br/>
                            • <b>단독 급락 경보</b>: AI 점수가 급락 기준 미만일 때
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
