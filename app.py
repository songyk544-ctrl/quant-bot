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
from email.utils import parsedate_to_datetime

st.set_page_config(layout="wide", page_title="QEdge", page_icon="Q")

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
st.sidebar.markdown("## 접근 코드 인증")
st.sidebar.caption("공유받은 코드를 입력하면 전체 주도주와 상세 분석 데이터를 볼 수 있습니다.")
user_code = st.sidebar.text_input("접근 코드 입력", type="password")

is_vip = (user_code == VIP_CODE)

if is_vip:
    st.sidebar.success("코드 인증이 완료되었습니다. 전체 데이터를 확인할 수 있습니다.")
else:
    st.sidebar.info("현재 공개 화면만 표시 중입니다. 코드를 입력하면 전체 화면이 열립니다.")

st.title("QEdge")
st.caption("수급·뉴스·매크로를 한 화면에서 보는 퀀트 대시보드")

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
    .macro-strip { display:flex; gap:8px; overflow-x:auto; margin-bottom: 8px; padding-bottom:2px; -webkit-overflow-scrolling: touch; }
    .macro-strip::-webkit-scrollbar { display:none; }
    .macro-card { background:#151A25; border:1px solid #2A3242; border-radius:10px; padding:8px 10px; min-width:120px; flex:0 0 auto; }
    .macro-label { color:#A7B0C2; font-size:0.74em; margin-bottom:4px; white-space: nowrap; overflow:hidden; text-overflow: ellipsis; }
    .macro-value { color:#F5F7FA; font-size:0.98em; font-weight:700; line-height:1.2; }
    .macro-change { font-size:0.8em; font-weight:700; margin-top:3px; }
    @media (max-width: 900px) {
      .macro-card { min-width:108px; padding:7px 9px; }
      .macro-label { font-size:0.7em; }
      .macro-value { font-size:0.9em; }
      .macro-change { font-size:0.75em; }
    }
    </style>
    """,
    unsafe_allow_html=True
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
    .stock-sector-chip { background: linear-gradient(135deg, #36C06A, #1E9A52); color:white; padding:4px 12px; border-radius:20px; font-size:0.85em; font-weight:700; }
    .stock-grid { display:grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap:10px; margin:8px 0 14px 0; }
    .stock-card { background:#181825; border:1px solid #2B2B3A; border-radius:10px; padding:10px 12px; }
    .stock-label { color:#A8A8B3; font-size:0.78em; margin-bottom:4px; }
    .stock-value { color:#FFF; font-size:1.15em; font-weight:800; line-height:1.15; }
    .stock-sub { color:#9AA0B1; font-size:0.78em; margin-top:3px; }
    .kpi-grid { display:grid; grid-template-columns: repeat(2, minmax(170px, 1fr)); gap:10px; margin:10px 0 8px 0; }
    .kpi-card { background:#171A24; border:1px solid #2C3242; border-radius:12px; padding:12px 14px; }
    .kpi-title { color:#AAB2C5; font-size:0.8em; margin-bottom:6px; }
    .kpi-value { color:#F5F7FA; font-size:2.0em; font-weight:800; line-height:1.1; }
    .kpi-delta { font-size:0.9em; font-weight:700; margin-top:8px; display:inline-block; padding:2px 8px; border-radius:999px; }
    .kpi-meta { color:#9CA3AF; font-size:0.82em; margin-top:8px; }
    .score-kpi-grid { display:grid; grid-template-columns: repeat(3, minmax(120px, 1fr)); gap:8px; margin-top:8px; }
    .score-kpi { background:#151A25; border:1px solid #2A3242; border-radius:10px; padding:9px 10px; }
    .score-kpi-label { color:#9CA3AF; font-size:0.74em; margin-bottom:4px; }
    .score-kpi-value { color:#E5E7EB; font-size:1.35em; font-weight:800; line-height:1.15; }
    @media (max-width: 900px) {
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

def _request_html(url, headers, timeout=4, retries=2):
    """가벼운 재시도로 일시적 네트워크 실패를 완화합니다."""
    for attempt in range(retries + 1):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except Exception:
            if attempt == retries:
                return None

def _normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()

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

def _extract_source(title_text, fallback="일반"):
    if " - " in title_text:
        maybe_source = _normalize_text(title_text.split(" - ")[-1])
        if 1 < len(maybe_source) <= 12:
            return maybe_source
    return fallback

def _event_tags(text):
    rules = {
        "실적": ["실적", "영업이익", "매출", "어닝", "가이던스"],
        "수주": ["수주", "계약", "공급", "협약", "납품"],
        "정책": ["정부", "정책", "규제", "법안", "금리", "관세"],
        "수급": ["외국인", "기관", "연기금", "순매수", "공매도"],
        "리스크": ["소송", "리콜", "악재", "부진", "감소", "하향"]
    }
    tags = []
    for tag, keywords in rules.items():
        if any(k in text for k in keywords):
            tags.append(tag)
    return tags

def _title_signature(title_text):
    normalized = re.sub(r"[^0-9A-Za-z가-힣 ]+", " ", title_text.lower())
    tokens = [t for t in normalized.split() if len(t) > 1]
    return set(tokens[:12])

def _is_similar_title(sig, signature_list, threshold=0.75):
    for other in signature_list:
        if not sig or not other:
            continue
        inter = len(sig & other)
        union = len(sig | other)
        if union > 0 and (inter / union) >= threshold:
            return True
    return False

def _source_weight(source):
    weights = {
        "연합뉴스": 1.0, "뉴시스": 0.95, "이데일리": 0.9, "매일경제": 0.9,
        "한국경제": 0.9, "머니투데이": 0.88, "서울경제": 0.88, "일반": 0.82
    }
    return weights.get(source, 0.84)

def _score_news_candidate(candidate):
    now = datetime.now()
    news_dt = candidate.get("dt")
    age_score = 0.7
    if news_dt is not None:
        diff_h = max(0.0, (now - news_dt).total_seconds() / 3600)
        if diff_h <= 6:
            age_score = 1.1
        elif diff_h <= 24:
            age_score = 1.0
        elif diff_h <= 48:
            age_score = 0.85
        else:
            age_score = 0.6

    tag_bonus = min(0.35, 0.12 * len(candidate.get("tags", [])))
    text_quality = 0.08 if len(candidate.get("desc", "")) >= 20 else 0.0
    relevance_bonus = 0.18 if candidate.get("is_relevant", False) else -0.08
    return _source_weight(candidate.get("source", "일반")) + age_score + tag_bonus + text_quality + relevance_bonus

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
    """네이버 금융 메인의 '주요 뉴스' 5개를 긁어옵니다."""
    headlines = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = _request_html("https://finance.naver.com/news/mainnews.naver", headers=headers, timeout=4, retries=1)
        if res is None:
            return headlines
        soup = BeautifulSoup(res.text, 'html.parser')
        titles = soup.select('.articleSubject a')
        seen = set()
        for t in titles:
            title = _normalize_text(t.text)
            if not title or title in seen:
                continue
            seen.add(title)
            headlines.append(title)
            if len(headlines) >= 5:
                break
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
            encoded_euckr = urllib.parse.quote(stock_name.encode('euc-kr'))
            fin_url = f"https://finance.naver.com/news/news_search.naver?q={encoded_euckr}"
            res_fin = _request_html(fin_url, headers=headers, timeout=4, retries=1)
            if res_fin is not None:
                soup_fin = BeautifulSoup(res_fin.text, 'html.parser')

                # 네이버 금융 뉴스검색: 날짜/제목 함께 읽어서 최신성 필터 적용
                rows = soup_fin.select('table.type5 tr')
                for tr in rows:
                    t_tag = tr.select_one('.articleSubject a')
                    d_tag = tr.select_one('.wdate')
                    if not t_tag:
                        continue
                    title_text = _normalize_text(t_tag.get('title') or t_tag.text)
                    dt_text = _normalize_text(d_tag.text if d_tag else "")
                    try:
                        news_dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M")
                    except Exception:
                        news_dt = None
                    if news_dt is not None and news_dt < cutoff_dt:
                        continue
                    add_candidate(title=title_text, news_dt=news_dt, source="네이버금융")
                    if len(candidates) >= 10:
                        break

        # 2차 fallback: 무료 RSS (Google News) - 속도 영향 최소화를 위해 마지막에만 사용
        if not candidates:
            rss_url = f"https://news.google.com/rss/search?q={urllib.parse.quote(stock_name + ' 주식')}&hl=ko&gl=KR&ceid=KR:ko"
            res_rss = _request_html(rss_url, headers=headers, timeout=4, retries=0)
            if res_rss is not None:
                rss_soup = BeautifulSoup(res_rss.text, "xml")
                for item in rss_soup.select("item")[:10]:
                    title_text = _normalize_text(item.title.text if item.title else "")
                    pub_date = _normalize_text(item.pubDate.text if item.pubDate else "")
                    try:
                        dt = parsedate_to_datetime(pub_date).replace(tzinfo=None)
                    except Exception:
                        dt = None
                    if dt is not None and dt < cutoff_dt:
                        continue
                    add_candidate(title=title_text, news_dt=dt, source="GoogleNewsRSS")
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

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["매크로", "섹터 히트맵", "수급 스크리너", "종목 분석", "백테스트", "주도주 비교"])

    # --- 탭 1: 매크로 인사이트 ---
    with tab1:
        st.subheader("오늘의 매크로 리포트")
        macro_refs = get_macro_headline_news()
        if os.path.exists("report.md"):
            with open("report.md", "r", encoding="utf-8") as f: report_content = f.read()

            if is_vip:
                st.markdown(report_content)
            else:
                teaser_text = report_content[:250] + "...\n\n"
                st.markdown(teaser_text)
                show_premium_paywall("심층 매크로 분석 리포트 전문은 코드 인증 후 확인할 수 있습니다.")
        else: st.info("⏳ AI 매크로 리포트를 생성 중입니다.")
        with st.expander("참고한 시황 뉴스 제목 보기"):
            if macro_refs:
                for item in macro_refs:
                    st.caption(f"- {item}")
            else:
                st.caption("- 시황 뉴스 데이터를 불러오지 못했습니다.")

    # --- 탭 2: 섹터 히트맵 ---
    with tab2:
        st.subheader("시가총액 및 수급 섹터 히트맵")
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
                sector = safe_get(row, '섹터', '분류안됨')
                price = f"{safe_get(row, '현재가', 0):,.0f}"
                chg = float(safe_get(row, '등락률', 0))
                chg_color = "#FF4B4B" if chg > 0 else "#3B82F6" if chg < 0 else "#AAAAAA"
                chg_str = f"▲ {chg:.2f}%" if chg > 0 else f"▼ {abs(chg):.2f}%" if chg < 0 else "0.00%"
                ai_score = int(safe_get(row, 'AI수급점수', 0))
                rank_chg = safe_get(row, '랭킹추세', '-')
                f_str = f"{float(safe_get(row, '외인강도(%)', 0)):.1f}%"
                p_str = f"{float(safe_get(row, '연기금강도(%)', 0)):.1f}%"
                
                rc_color = "#FF4B4B" if "▲" in str(rank_chg) else ("#3B82F6" if "▼" in str(rank_chg) else "#888888")
                
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

            styled_df = (
                df_display_table.style
                .applymap(color_score, subset=['AI수급점수'])
                .applymap(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)'])
                .applymap(color_momentum, subset=['랭킹추세'])
            )
            
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
            "분석할 종목을 검색/선택하세요",
            options=stock_list,
            key="stock_selector",
            on_change=on_stock_change
        )
        target_stock = st.session_state.stock_selector
        
        selected_row = df_summary[df_summary['종목명'] == target_stock].iloc[0]
        
        sector_name = safe_get(selected_row, '섹터', '분류안됨')
        cur_rank = safe_get(selected_row, '현재_순위', 0)
        ai_score = safe_get(selected_row, 'AI수급점수', 0)
        quant_score = float(safe_get(selected_row, 'Quant점수', ai_score))
        qual_score = float(safe_get(selected_row, '정성점수', 50))
        qual_adj = float(safe_get(selected_row, '정성보정치', 0))
        score_mode = safe_get(selected_row, '점수모드', '기본')
        rank_trend = safe_get(selected_row, '랭킹추세', '-')
        marcap = safe_get(selected_row, '시가총액', 0)
        per_val = safe_get(selected_row, 'PER', 0.0)
        roe_val = safe_get(selected_row, 'ROE', 0.0)
        gap_20 = safe_get(selected_row, '이격도(%)', 100)
        target_code = safe_get(selected_row, '종목코드', '')
        cur_price = float(safe_get(selected_row, '현재가', 0))
        day_chg = float(safe_get(selected_row, '등락률', 0))
        day_chg_color = "#FF4B4B" if day_chg > 0 else "#3B82F6" if day_chg < 0 else "#A0A0A0"
        day_chg_text = f"+{day_chg:.2f}%" if day_chg > 0 else f"{day_chg:.2f}%"

        st.markdown(
            f"""
            <div class="stock-title-wrap">
                <h2 style="margin: 0; color: #FFFFFF;">{target_stock}</h2>
                <span class="stock-sector-chip">{sector_name}</span>
                <span style="padding:4px 10px; border-radius:16px; background:#242735; color:#D6DAE5; font-size:0.82em;">현재가 {cur_price:,.0f}원</span>
                <span style="padding:4px 10px; border-radius:16px; background:#242735; color:{day_chg_color}; font-size:0.82em; font-weight:700;">당일 {day_chg_text}</span>
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

            st.markdown("##### 점수 산출 구조")
            display_final = round(float(ai_score), 1)
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
            st.plotly_chart(gauge, use_container_width=True)
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

                    # 주체별 최근 1주/1개월 순매입 금액(백만 원) 및 시총 대비 비중
                    wk_slice = target_hist.tail(5)
                    mo_slice = target_hist.tail(20)
                    marcap_in_million = float(marcap) * 100 if marcap else 0.0  # 시총(억원) -> 백만원

                    flow_rows = []
                    for investor in ["외인", "연기금", "투신", "사모"]:
                        wk_amt = float(wk_slice[investor].sum()) if investor in wk_slice.columns else 0.0
                        mo_amt = float(mo_slice[investor].sum()) if investor in mo_slice.columns else 0.0
                        mo_ratio = (mo_amt / marcap_in_million) * 100 if marcap_in_million > 0 else 0.0
                        flow_rows.append({
                            "주체": investor,
                            "1주 순매입(백만 원)": wk_amt,
                            "1개월 순매입(백만 원)": mo_amt,
                            "시총 대비(1개월)": mo_ratio
                        })

                    df_flow = pd.DataFrame(flow_rows)
                    st.markdown("##### 주체별 순매입 현황 (1주 / 1개월)")
                    st.dataframe(
                        df_flow.style.format({
                            "1주 순매입(백만 원)": "{:+,.0f}",
                            "1개월 순매입(백만 원)": "{:+,.0f}",
                            "시총 대비(1개월)": "{:+.2f}%"
                        }),
                        hide_index=True,
                        use_container_width=True
                    )

                    def calc_consecutive_buy_days(df_hist_local, investor_col):
                        if investor_col not in df_hist_local.columns:
                            return 0
                        streak = 0
                        # 최신일 기준으로 연속 순매수 일수를 계산
                        for val in df_hist_local.sort_values('일자', ascending=False)[investor_col].tolist():
                            if pd.isna(val) or float(val) <= 0:
                                break
                            streak += 1
                        return streak

                    streak_df = pd.DataFrame([
                        {"주체": "외인", "연속순매수일": calc_consecutive_buy_days(target_hist, "외인")},
                        {"주체": "연기금", "연속순매수일": calc_consecutive_buy_days(target_hist, "연기금")},
                        {"주체": "투신", "연속순매수일": calc_consecutive_buy_days(target_hist, "투신")},
                        {"주체": "사모", "연속순매수일": calc_consecutive_buy_days(target_hist, "사모")},
                    ])
                    streak_df["표시"] = streak_df["연속순매수일"].astype(str) + "일"
                    streak_df["강도"] = streak_df["연속순매수일"].apply(
                        lambda x: "strong" if x >= 5 else ("mid" if x >= 2 else ("weak" if x >= 1 else "none"))
                    )

                    col1, col2 = st.columns(2)
                    color_scale = alt.Scale(domain=['외인', '연기금', '투신', '사모'], range=['#36C06A', '#E04B4B', '#3BA7FF', '#B08CFF'])
                    with col1:
                        st.markdown("##### 20일 종가 추이")
                        st.altair_chart(alt.Chart(target_hist).mark_line(color='#36C06A', point=True).encode(x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)).properties(height=280), use_container_width=True)
                    with col2:
                        st.markdown("##### 주체별 연속 순매수")
                        streak_chart = alt.Chart(streak_df).mark_bar(cornerRadiusEnd=6).encode(
                            y=alt.Y('주체:N', sort=['외인', '연기금', '투신', '사모'], axis=alt.Axis(title=None)),
                            x=alt.X('연속순매수일:Q', axis=alt.Axis(title=None, tickMinStep=1)),
                            color=alt.Color(
                                "강도:N",
                                scale=alt.Scale(
                                    domain=["none", "weak", "mid", "strong"],
                                    range=["#4B5563", "#4FAF78", "#93D8A8", "#36C06A"]
                                ),
                                legend=None
                            ),
                            tooltip=[alt.Tooltip('주체:N'), alt.Tooltip('연속순매수일:Q', title='연속 순매수일')]
                        ).properties(height=280)
                        streak_text = alt.Chart(streak_df).mark_text(align='left', baseline='middle', dx=6, color='#E5E7EB').encode(
                            y=alt.Y('주체:N', sort=['외인', '연기금', '투신', '사모']),
                            x=alt.X('연속순매수일:Q'),
                            text='표시:N'
                        )
                        st.altair_chart(streak_chart + streak_text, use_container_width=True)

                    with st.expander("주체별 순매수 대금 추이(백만 원)"):
                        amount_df = target_hist.melt(
                            id_vars=['일자_표시'],
                            value_vars=['외인', '연기금', '투신', '사모'],
                            var_name='투자자',
                            value_name='금액'
                        )
                        st.altair_chart(
                            alt.Chart(amount_df).mark_bar().encode(
                                x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                                y=alt.Y('금액:Q', title=None),
                                color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                                order=alt.Order('투자자:N', sort='descending')
                            ).properties(height=280),
                            use_container_width=True
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
        st.subheader("QEdge 모델 가상 포트폴리오 백테스트")
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
                            kospi_hist = kospi_hist.dropna(subset=['Close'])
                            
                            base_k_df = kospi_hist[kospi_hist.index <= pd.to_datetime(selected_start_date)]
                            base_k = float(base_k_df['Close'].dropna().iloc[-1]) if not base_k_df.empty and not base_k_df['Close'].dropna().empty else None
                            
                            kospi_rets = []
                            for d in df_filtered['날짜_dt']:
                                k_sub = kospi_hist[kospi_hist.index <= d]
                                k_close = k_sub['Close'].dropna() if not k_sub.empty else pd.Series(dtype=float)
                                if not k_close.empty and base_k is not None and base_k != 0:
                                    val = float(k_close.iloc[-1])
                                    ret = ((val - base_k) / base_k) * 100
                                    if pd.isna(ret):
                                        ret = kospi_rets[-1] if kospi_rets else 0.0
                                    kospi_rets.append(ret)
                                else:
                                    kospi_rets.append(kospi_rets[-1] if kospi_rets else 0.0)
                            df_filtered['KOSPI 누적수익률'] = kospi_rets
                        except:
                            df_filtered['KOSPI 누적수익률'] = 0
                        
                        current_port_ret = df_filtered['조정_포트수익률'].iloc[-1]
                        current_kospi_ret = df_filtered['KOSPI 누적수익률'].iloc[-1]
                        
                        if len(df_filtered) > 1:
                            port_daily_diff = df_filtered['조정_포트수익률'].iloc[-1] - df_filtered['조정_포트수익률'].iloc[-2]
                            kospi_daily_diff = df_filtered['KOSPI 누적수익률'].iloc[-1] - df_filtered['KOSPI 누적수익률'].iloc[-2]
                        else:
                            port_daily_diff = 0.0
                            kospi_daily_diff = 0.0

                        alpha_ret = current_port_ret - current_kospi_ret
                        alpha_color = "#36C06A" if alpha_ret >= 0 else "#E04B4B"
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
                                    <div class="kpi-meta">초과 성과 <span style="color:{alpha_color}; font-weight:700;">{alpha_ret:+.2f}%p</span></div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                        
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
                                    with st.expander("날짜별 Top3 구성 종목 및 성과", expanded=False):
                                        st.dataframe(pd.DataFrame(rank_rows), hide_index=True, use_container_width=True)
                    else:
                        st.info("선택하신 날짜에 해당하는 백테스트 데이터가 없습니다.")
                else: st.info("⏳ 데이터 대기 중")
            else: st.info("⏳ 데이터 대기 중")

    # --- 탭 6: 리더스 페어 분석 ---
    with tab6:
        st.subheader("리더스 페어 분석")
        st.caption("두 종목의 뉴스, 시황, 퀀트 데이터를 교차 검토해 단기 상대 우위를 비교합니다.")

        if not is_vip:
            show_premium_paywall("AI 기반 다중 종목 비교 분석 기능은 코드 인증 후 이용할 수 있습니다.")
        else:
            if not client:
                st.error("⚠️ Streamlit Secrets에 GEMINI_API_KEY가 설정되지 않아 비교 분석을 사용할 수 없습니다.")
            else:
                stock_list_full = df_summary['종목명'].tolist()
                matchup_stocks = st.multiselect(
                    "비교할 종목 2개를 선택하세요",
                    options=stock_list_full,
                    key="leaders_pair_multiselect"
                )
                if len(matchup_stocks) > 2:
                    st.session_state["leaders_pair_multiselect"] = matchup_stocks[:2]
                    matchup_stocks = st.session_state["leaders_pair_multiselect"]
                    st.warning("비교는 2개 종목만 가능합니다. 최근 선택 종목은 제외했습니다.")
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
                                - 섹터: {safe_get(s_row, '섹터', '분류안됨')} / AI점수: {safe_get(s_row, 'AI수급점수', 0)}점
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

