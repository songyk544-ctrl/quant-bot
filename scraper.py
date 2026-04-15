import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, timezone 
from bs4 import BeautifulSoup
import os
from google import genai
from google.genai import types
import yfinance as yf
import re
from email.utils import parsedate_to_datetime
from pathlib import Path
import tomllib
import argparse

URL_BASE = "https://openapi.koreainvestment.com:9443"
KIS_APP_KEY = os.environ.get("KIS_APP_KEY")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def resolve_kis_credentials():
    """
    KIS 키 로딩 우선순위:
    1) OS 환경변수
    2) .streamlit/secrets.toml
    3) secrets.toml
    """
    app_key = os.environ.get("KIS_APP_KEY")
    app_secret = os.environ.get("KIS_APP_SECRET")
    if app_key and app_secret:
        return app_key, app_secret

    candidate_files = [
        Path(".streamlit/secrets.toml"),
        Path("secrets.toml"),
    ]
    for p in candidate_files:
        try:
            if p.exists():
                with p.open("rb") as f:
                    parsed = tomllib.load(f)
                app_key = parsed.get("KIS_APP_KEY")
                app_secret = parsed.get("KIS_APP_SECRET")
                if app_key and app_secret:
                    return str(app_key), str(app_secret)
        except Exception:
            continue
    return None, None

def resolve_gemini_api_key():
    """
    실행 환경별 키 로딩 통합:
    1) OS 환경변수 GEMINI_API_KEY
    2) 프로젝트 .streamlit/secrets.toml
    3) 프로젝트 secrets.toml
    """
    env_key = os.environ.get("GEMINI_API_KEY")
    if env_key:
        return env_key

    candidate_files = [
        Path(".streamlit/secrets.toml"),
        Path("secrets.toml"),
    ]
    for p in candidate_files:
        try:
            if p.exists():
                with p.open("rb") as f:
                    parsed = tomllib.load(f)
                key = parsed.get("GEMINI_API_KEY")
                if key:
                    return str(key)
        except Exception:
            continue
    return None

def get_kis_access_token():
    kis_app_key, kis_app_secret = resolve_kis_credentials()
    if not kis_app_key or not kis_app_secret:
        return None
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": kis_app_key, "appsecret": kis_app_secret}
    try:
        res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
        return res.json().get("access_token")
    except:
        return None

def safe_float(text):
    try: return float(text.replace(',', '').replace('%', '').strip())
    except: return 0.0

def safe_api_float(val):
    try: return float(val) if val else 0.0
    except: return 0.0

def calculate_rsi(prices, period=14):
    if len(prices) < period + 1: return 50.0
    diffs = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in diffs]
    losses = [-d if d < 0 else 0 for d in diffs]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(diffs)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0: return 100.0
    return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))

# 🔥 [수급 로직 업데이트] 외인 비중 대폭 축소, 기관(투신/사모/연기금) 비중 극대화 (눌림목 최적화)
def calculate_dynamic_score(f_str, p_str, t_str, pef_str, vol_surge, rsi_val, gap_20, foreign_streak, pension_streak, turnover_rate, is_ma20_rising, per_val, roe_val, current_vix):
    
    if current_vix < 25:
        # 🚀 상승장: 기관 주도 폭발적 모멘텀
        zombie_penalty = 0 
        fund_score = 0
        
        # 기관(투신4, 사모4, 연기금2)에 압도적 가중치, 외인(0.5)은 보조 지표로 강등
        raw_str_sum = (t_str * 4) + (pef_str * 4) + (p_str * 2) + (f_str * 0.5)
        strength_score = max(0, min(20, raw_str_sum * 2))
        streak_score = max(0, min(10, (pension_streak * 1.5) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score # 최대 30점

        turnover_score = 20 if turnover_rate >= 10 else (10 if turnover_rate >= 5 else 0)
        v_score = 10 if vol_surge >= 150 else 0
        r_score = 15 if 60 <= rsi_val <= 85 else (5 if 50 <= rsi_val < 60 else 0)
        momentum_score = turnover_score + v_score + r_score # 최대 45점

        tech_score = 25 if 102 <= gap_20 <= 115 else (10 if 98 <= gap_20 < 102 else 0) # 최대 25점
        
    else:
        # 🛡️ 하락장: 기관 방어 스윙 (연기금 주도, 투신/사모 보조)
        zombie_penalty = -30 if turnover_rate < 1.5 else 0 
        
        # 연기금(5) 압도적 방어력, 투신/사모(2) 단가 관리 포착, 외인(0.5) 강등
        raw_str_sum = (p_str * 5) + (t_str * 2) + (pef_str * 2) + (f_str * 0.5)
        strength_score = max(0, min(20, raw_str_sum * 2))
        streak_score = max(0, min(10, (pension_streak * 2.5) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score # 최대 30점

        turnover_score = 5 if turnover_rate >= 3 else 0
        v_score = 5 if vol_surge >= 100 else 0
        r_score = 10 if 45 <= rsi_val <= 60 else 0
        momentum_score = turnover_score + v_score + r_score # 최대 20점

        if is_ma20_rising:
            tech_score = 20 if 98 <= gap_20 <= 103 else (10 if 103 < gap_20 <= 108 else 0) # 최대 20점
        else:
            tech_score = -20 

        fund_score = (15 if roe_val >= 15 else (10 if roe_val >= 8 else 0)) + (15 if 0 < per_val <= 15 else 0)
        if per_val <= 0: 
            fund_score -= 20 

    return max(0, min(100, int(supply_score + momentum_score + tech_score + fund_score + zombie_penalty)))

def calculate_qualitative_score(sector_name, per_val, roe_val, foreign_streak, pension_streak, macro_news_text, macro_recency_score=50.0, repeated_topics_text=""):
    """
    정성 점수(0~100): 뉴스-섹터 정합성 + 펀더멘털 힌트 + 연속 순매수 안정성
    데이터가 부족하면 기본 50점(중립)을 유지합니다.
    """
    score = 50.0
    text = (macro_news_text or "").lower()
    topic_text = (repeated_topics_text or "").lower()
    sector = (sector_name or "분류안됨").lower()

    # 시황 뉴스 반감기(최신 뉴스일수록 영향력↑, 오래된 뉴스일수록 영향력↓)
    decay_factor = max(0.35, min(1.0, float(macro_recency_score) / 100.0))

    # 시황 뉴스와 섹터의 키워드 정합성
    sector_theme_map = {
        "반도체": ["반도체", "ai", "hbm", "메모리"],
        "전기": ["전력", "전기", "배터리", "2차전지", "ess"],
        "건설": ["건설", "인프라", "플랜트", "수주"],
        "화장품": ["화장품", "소비", "면세", "중국 소비"],
        "제약": ["제약", "바이오", "임상", "허가"],
        "방산": ["방산", "국방", "수출"],
        "조선": ["조선", "선박", "해운", "lng"],
        "기계": ["기계", "자동화", "설비투자"],
        "증권": ["증권", "거래대금", "금리", "유동성"],
    }
    for sector_key, keywords in sector_theme_map.items():
        if sector_key in sector and any(k.lower() in text for k in keywords):
            score += 8 * decay_factor
            break

    # 리스크 키워드 감점
    if any(k in text for k in ["긴축", "관세", "하락", "리스크", "소송", "악재"]):
        score -= 4 * decay_factor

    # 반복 이슈 반영(최신 뉴스와 분리된 지속성 시그널)
    if any(k in topic_text for k in ["실적", "수주", "정책", "수급"]):
        score += 3 * decay_factor
    if any(k in topic_text for k in ["리스크", "하락", "긴축", "관세"]):
        score -= 3 * decay_factor

    # 펀더멘털 보정 (정량에서 이미 반영되지만 정성 관점에서 소폭 보정)
    if roe_val >= 15:
        score += 5
    elif roe_val >= 8:
        score += 2
    else:
        score -= 2

    if 0 < per_val <= 15:
        score += 3
    elif per_val <= 0:
        score -= 5

    # 연속 순매수 안정성
    score += min(5, pension_streak * 0.8)
    score += min(2, foreign_streak * 0.2)

    return max(0, min(100, score))

def blend_quant_qual_score(quant_score, qual_score, current_vix):
    """
    방식 B(가감형)만 사용:
      Final = Quant + clamp((Qual-50)*sensitivity, -limit, +limit)
    """
    if current_vix < 25:
        sensitivity = 0.4
        limit = 10
        mode = "상승장 (보수적 반영)"
    else:
        sensitivity = 0.6
        limit = 20
        mode = "하락장 (민감 반영)"

    qual_adj = (qual_score - 50) * sensitivity
    qual_adj = max(-limit, min(limit, qual_adj))
    final_score = max(0, min(100, quant_score + qual_adj))
    return int(round(final_score)), round(qual_adj, 2), mode

def score_disclosures_and_reports(disclosures, reports):
    """공시/리포트 이벤트를 점수화해 정성점수(0~100)로 반환."""
    score = 50.0
    positive_keys = ["실적", "수주", "계약", "자기주식", "소각", "기업설명회", "가이던스", "상향", "증가"]
    negative_keys = ["소송", "정정", "하향", "감소", "리스크", "악화", "손실"]

    for text in disclosures + reports:
        t = str(text)
        if any(k in t for k in positive_keys):
            score += 3.5
        if any(k in t for k in negative_keys):
            score -= 4.0

    # 이벤트 과다 누적 방지
    return max(20.0, min(80.0, score))

def apply_enhanced_qual_for_top_candidates(df_final, current_vix, top_n=40):
    """
    전 종목 기본점수 이후 상위 후보(top_n)만 공시/리포트를 심화 반영.
    운영 안정성을 위해 상위권만 추가 크롤링합니다.
    """
    if df_final.empty:
        return df_final

    df_out = df_final.copy()
    if "AI수급점수" not in df_out.columns:
        return df_out

    top_idx = df_out.sort_values("AI수급점수", ascending=False).head(top_n).index
    for idx in top_idx:
        row = df_out.loc[idx]
        name = row.get("종목명", "")
        code = row.get("종목코드", "")
        disclosures = get_recent_disclosures(code, name, max_items=3)
        reports = get_recent_analyst_reports(name, max_items=2)
        event_qual = score_disclosures_and_reports(disclosures, reports)

        base_qual = float(row.get("정성점수", 50))
        blended_qual = (base_qual * 0.7) + (event_qual * 0.3)
        final_score, qual_adj, score_mode = blend_quant_qual_score(float(row.get("Quant점수", row.get("AI수급점수", 0))), blended_qual, current_vix)

        df_out.at[idx, "정성점수"] = round(blended_qual, 2)
        df_out.at[idx, "정성보정치"] = qual_adj
        df_out.at[idx, "점수모드"] = score_mode
        df_out.at[idx, "AI수급점수"] = final_score

    return df_out.sort_values("AI수급점수", ascending=False)

def get_target_stock_list():
    target_list = []
    noise_keywords = ['KODEX', 'TIGER', 'RISE', 'ACE', 'KBSTAR', 'HANARO', 'KOSEF', 'SOL', 'PLUS', 'ARIRANG', 'ETN', '스팩', '인버스', '레버리지', 'CD금리', 'KOFR']
    custom_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    for sosok, market_name in [(0, 'KOSPI'), (1, 'KOSDAQ')]:
        for page in range(1, 7): 
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                res = requests.get(url, headers=custom_headers, timeout=10)
                soup = BeautifulSoup(res.text, 'html.parser')
                for tr in soup.select('table.type_2 tbody tr'):
                    tds = tr.select('td')
                    if len(tds) > 11:
                        name_tag = tr.select_one('a.tltle')
                        if name_tag:
                            stock_name = name_tag.text
                            if any(keyword in stock_name for keyword in noise_keywords): continue
                            marcap = safe_float(tds[6].text)
                            if marcap >= 8000:
                                target_list.append({
                                    '종목명': stock_name, '종목코드': name_tag['href'].split('code=')[-1], 
                                    '소속': market_name, '현재가': int(safe_float(tds[2].text)),
                                    '등락률': safe_float(tds[4].text), '시가총액': int(marcap),
                                    'PER': safe_float(tds[10].text), 'ROE': safe_float(tds[11].text)
                                })
            except: pass
            time.sleep(0.5) 
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace('**', '*') 
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000], "parse_mode": "Markdown"}
    try: requests.post(url, data=data)
    except: pass

def _request_html(url, headers, timeout=4, retries=2):
    for attempt in range(retries + 1):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except Exception as e:
            if attempt == retries:
                print(f"⚠️ 요청 실패: {url} ({e})")
                return None

def _normalize_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()

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
    return _source_weight(candidate.get("source", "일반")) + age_score + tag_bonus + text_quality

def _build_macro_topic_lines(candidates, top_n=5):
    """최근 기사 후보에서 반복 이슈 TopN을 가볍게 추출."""
    if not candidates:
        return []
    now = datetime.now()
    topic_stats = {}
    for item in candidates:
        tags = item.get("tags") or ["일반"]
        source = item.get("source", "일반")
        dt = item.get("dt")
        if dt is not None:
            elapsed_days = max(0.0, (now - dt).total_seconds() / 86400.0)
            recency = 1.0 / (1.0 + elapsed_days)
        else:
            recency = 0.4
        for tag in tags:
            stat = topic_stats.setdefault(tag, {"count": 0, "sources": set(), "recency_sum": 0.0})
            stat["count"] += 1
            stat["sources"].add(source)
            stat["recency_sum"] += recency

    ranked = []
    for tag, st in topic_stats.items():
        avg_recency = st["recency_sum"] / max(1, st["count"])
        score = (st["count"] * 1.0) + (len(st["sources"]) * 0.7) + (avg_recency * 2.0)
        ranked.append((score, tag, st["count"], len(st["sources"]), avg_recency))
    ranked.sort(reverse=True)

    lines = []
    for _, tag, cnt, src_cnt, avg_recency in ranked[:top_n]:
        lines.append(f"- 반복이슈: {tag} | 빈도 {cnt}건 | 출처 {src_cnt}곳 | 최신성 {avg_recency*100:.0f}")
    return lines

def _parse_short_yy_mm_dd(text):
    """네이버 리서치 날짜(예: 26.04.14)를 datetime으로 변환."""
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

def get_recent_disclosures(stock_code, stock_name, max_items=3):
    """네이버 금융 공시 페이지에서 최근 공시를 가져옵니다."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://finance.naver.com/item/news_notice.naver?code={stock_code}"
    res = _request_html(url, headers=headers, timeout=4, retries=1)
    if res is None:
        return []
    try:
        # 한글 깨짐 방지
        res.encoding = res.apparent_encoding or "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        rows = soup.select("table tr")
        cutoff_dt = datetime.now() - timedelta(days=10)
        items = []

        for tr in rows:
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
                items.append(f"{stock_name} 공시: {title} ({date_text})")
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        print(f"⚠️ 공시 수집 실패 [{stock_name}]: {e}")
        return []

def get_recent_analyst_reports(stock_name, max_items=3):
    """네이버 증권 리서치에서 종목 키워드 리포트를 조회합니다."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://finance.naver.com/research/company_list.naver?keyword={requests.utils.quote(stock_name)}"
    res = _request_html(url, headers=headers, timeout=4, retries=1)
    if res is None:
        return []
    try:
        res.encoding = res.apparent_encoding or "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        cutoff_dt = datetime.now() - timedelta(days=14)
        items = []
        seen = set()

        for tr in soup.select("table tr"):
            report_tag = tr.select_one("a[href*='/research/company_read.naver']")
            stock_tag = tr.select_one("a[href*='/item/main.naver?code=']")
            if not report_tag:
                continue
            report_title = _normalize_text(report_tag.text)
            report_stock = _normalize_text(stock_tag.text if stock_tag else "")
            if stock_name not in report_stock:
                continue
            tds = tr.select("td")
            date_text = _normalize_text(tds[-2].text if len(tds) >= 2 else "")
            dt = _parse_short_yy_mm_dd(date_text)
            if dt is not None and dt < cutoff_dt:
                continue
            key = f"{report_stock}|{report_title}"
            if key in seen:
                continue
            seen.add(key)
            items.append(f"{report_stock} 리포트: {report_title} ({date_text})")
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        print(f"⚠️ 리포트 수집 실패 [{stock_name}]: {e}")
        return []

def build_top3_event_context(df_final):
    """Top3 종목의 공시/리포트를 모아 프롬프트 컨텍스트를 구성합니다."""
    if df_final.empty:
        return "데이터 없음"
    top3 = df_final.head(3)[["종목명", "종목코드"]].to_dict("records")
    lines = []
    for row in top3:
        name, code = row["종목명"], row["종목코드"]
        disclosures = get_recent_disclosures(code, name, max_items=3)
        reports = get_recent_analyst_reports(name, max_items=2)
        if not disclosures and not reports:
            lines.append(f"- {name}: 최근 공시/리포트 데이터 없음")
            continue
        for d in disclosures:
            lines.append(f"- {d}")
        for r in reports:
            lines.append(f"- {r}")
    return "\n".join(lines) if lines else "최근 공시/리포트 데이터 없음"

def get_live_macro_and_news():
    tickers = {"KOSPI": "^KS11", "KOSDAQ": "^KQ11", "S&P500": "^GSPC", "NASDAQ": "^IXIC", "환율": "KRW=X", "WTI유": "CL=F", "미 국채(10y)": "^TNX", "VIX": "^VIX"}
    macro_str = ""
    for name, ticker in tickers.items():
        try:
            hist = yf.Ticker(ticker).history(period="2d")
            if len(hist) >= 2:
                curr, prev = hist['Close'].iloc[-1], hist['Close'].iloc[-2]
                if pd.isna(curr) or pd.isna(prev) or float(prev) == 0.0:
                    continue
                change_pct = ((curr - prev) / prev) * 100
                macro_str += f"- {name}: {curr:.2f} ({change_pct:+.2f}%)\n"
        except: pass
    
    # 🔥 [기능 업데이트] 네이버 시황 뉴스 제목 + 요약 본문 동시 스크래핑
    news_str = ""
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        cutoff_dt = datetime.now() - timedelta(hours=72)
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
            tags = _event_tags(f"{title} {desc}")
            candidates.append({
                "title": title,
                "desc": desc,
                "dt": news_dt,
                "source": source,
                "tags": tags
            })

        res = _request_html("https://finance.naver.com/news/mainnews.naver", headers=headers, timeout=4, retries=1)
        if res is not None:
            soup = BeautifulSoup(res.text, 'html.parser')
            subjects = soup.select('.articleSubject a')
            summaries = soup.select('.articleSummary')

            for i in range(min(20, len(subjects))):
                title = _normalize_text(subjects[i].text)
                desc = _normalize_text(summaries[i].text if i < len(summaries) else "")
                add_candidate(title=title, desc=desc, source=_extract_source(title, "네이버금융"))
                if len(candidates) >= 12:
                    break

        # 네이버 금융 검색 fallback (최신성 필터)
        if not candidates:
            fin_res = _request_html("https://finance.naver.com/news/news_search.naver?q=%EC%BD%94%EC%8A%A4%ED%94%BC", headers=headers, timeout=4, retries=1)
            if fin_res is not None:
                soup_fin = BeautifulSoup(fin_res.text, "html.parser")
                for tr in soup_fin.select("table.type5 tr"):
                    t_tag = tr.select_one(".articleSubject a")
                    d_tag = tr.select_one(".wdate")
                    if not t_tag:
                        continue
                    title = _normalize_text(t_tag.get("title") or t_tag.text)
                    dt_text = _normalize_text(d_tag.text if d_tag else "")
                    try:
                        news_dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M")
                    except Exception:
                        news_dt = None
                    if news_dt is not None and news_dt < cutoff_dt:
                        continue
                    add_candidate(title=title, news_dt=news_dt, source="네이버금융")
                    if len(candidates) >= 12:
                        break

        # 무료 RSS 확장 수집(반복 이슈 계산용)
        if len(candidates) < 40:
            rss_url = "https://news.google.com/rss/search?q=%EC%A6%9D%EC%8B%9C%20OR%20%EC%BD%94%EC%8A%A4%ED%94%BC%20OR%20%EA%B8%88%EB%A6%AC&hl=ko&gl=KR&ceid=KR:ko"
            rss_res = _request_html(rss_url, headers=headers, timeout=5, retries=1)
            if rss_res is not None:
                soup_rss = BeautifulSoup(rss_res.text, "xml")
                for item in soup_rss.select("item")[:40]:
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

        topic_lines = _build_macro_topic_lines(candidates, top_n=5)
        repeated_topics_text = " ".join(topic_lines)
        final_news = sorted(candidates, key=_score_news_candidate, reverse=True)[:5]
        news_lines = []
        if topic_lines:
            news_lines.append("[반복 핵심 이슈 Top5]")
            news_lines.extend(topic_lines)
            news_lines.append("[최신 뉴스 Top5]")
        for item in final_news:
            tag_text = ",".join(item["tags"][:2]) if item["tags"] else "일반"
            if item["desc"]:
                news_lines.append(f"- 제목: {item['title']} / 내용: {item['desc']} / 출처: {item['source']} / 태그: {tag_text}")
            else:
                news_lines.append(f"- 제목: {item['title']} / 출처: {item['source']} / 태그: {tag_text}")

        # GitHub Actions 로그에서 수집 품질을 바로 확인할 수 있도록 요약 출력
        source_stats = {}
        for item in final_news:
            source_stats[item["source"]] = source_stats.get(item["source"], 0) + 1
        print(f"📰 뉴스 수집 품질: raw={len(candidates)} final={len(final_news)} sources={source_stats}")
        # 반감기 점수(0~100): 최근 기사일수록 높은 점수
        recency_scores = []
        now_dt = datetime.now()
        for item in final_news:
            dt = item.get("dt")
            if dt is None:
                recency_scores.append(40.0)
                continue
            elapsed_days = max(0.0, (now_dt - dt).total_seconds() / 86400.0)
            # half-life = 1일 기준
            decay = 1.0 / (1.0 + elapsed_days)
            recency_scores.append(decay * 100.0)
        macro_recency_score = round(sum(recency_scores) / len(recency_scores), 2) if recency_scores else 50.0

        news_str = "\n".join(news_lines) if news_lines else "- 뉴스 수집 실패"
    except Exception as e:
        print(f"⚠️ 시황 뉴스 수집 실패: {e}")
        news_str = "- 뉴스 수집 실패"
        macro_recency_score = 50.0
        repeated_topics_text = ""
    
    return macro_str, news_str, macro_recency_score, repeated_topics_text

def run_scraper(manual_full_parse=False):
    print("🚀 수집기 봇 가동 시작 (V40.4 기관 수급 눌림목 최적화 & 백테스트 방어)...")
    KST = timezone(timedelta(hours=9))
    now_kst = datetime.now(KST)
    
    try:
        vix_hist = yf.Ticker("^VIX").history(period="1d")
        current_vix = float(vix_hist['Close'].iloc[-1])
    except:
        current_vix = 15.0 
    
    regime = "공포/하락장 (안전제일 눌림목 & 펀더멘털 방어)" if current_vix >= 25 else "평온/강세장 (핫섹터 폭발적 모멘텀)"
    print(f"🌍 실시간 VIX 지수: {current_vix:.2f} ➔ [{regime}] 가동")
    macro_str_for_scoring, news_str_for_scoring, macro_recency_score, repeated_topics_text = get_live_macro_and_news()

    is_eod_updated = (now_kst.hour > 15) or (now_kst.hour == 15 and now_kst.minute >= 40)
    target_kis_date = now_kst.strftime("%Y%m%d") if is_eod_updated else (now_kst - timedelta(days=1)).strftime("%Y%m%d")
    today_date = now_kst.strftime("%Y-%m-%d")

    already_fetched_kis = False
    if os.path.exists("history.csv"):
        try:
            df_hist_check = pd.read_csv("history.csv")
            if not df_hist_check.empty and '일자' in df_hist_check.columns:
                latest_kis_date = str(df_hist_check['일자'].max()).replace("-", "")
                if latest_kis_date == target_kis_date:
                    already_fetched_kis = True
        except: pass

    force_full_parse = False
    if os.path.exists("data.csv"):
        df_check = pd.read_csv("data.csv")
        if '추세상승' not in df_check.columns:
            force_full_parse = True
            print("⚠️ 기존 데이터에 [추세상승] 정보가 없습니다. 새로운 로직 적용을 위해 1회 한투 API를 강제 호출합니다.")
    if manual_full_parse:
        force_full_parse = True
        print("🛠️ 수동 강제 옵션(--full-parse)으로 풀 파싱 모드를 실행합니다.")

    is_test_mode = False # 🔥 테스트 모드 여부를 감지하는 플래그 설정

    # ==========================================
    # 1. 슈퍼 캐시 모드 (테스트 모드)
    # ==========================================
    if already_fetched_kis and os.path.exists("data.csv") and not force_full_parse:
        is_test_mode = True # KIS API를 스킵하는 테스트 워크플로우임을 확정
        print(f"⚡ [슈퍼 캐시 모드] 기준일({target_kis_date}) 수급 데이터 존재 확인. KIS API를 스킵합니다.")
        
        df_target = get_target_stock_list()
        df_final = pd.read_csv("data.csv")
        
        updated_rows = []
        for idx, row in df_final.iterrows():
            row_dict = row.to_dict()
            live_info = df_target[df_target['종목명'] == row_dict['종목명']]
            
            old_price = row_dict.get('현재가', 1)
            old_gap = row_dict.get('이격도(%)', 100)
            new_gap = old_gap
            
            if not live_info.empty:
                new_price = live_info.iloc[0]['현재가']
                new_gap = old_gap * (new_price / old_price) if old_price > 0 else old_gap
                
                row_dict['현재가'] = new_price
                row_dict['등락률'] = live_info.iloc[0]['등락률']
                row_dict['시가총액'] = live_info.iloc[0]['시가총액']
                row_dict['PER'] = live_info.iloc[0]['PER']
                row_dict['ROE'] = live_info.iloc[0]['ROE']
                row_dict['이격도(%)'] = new_gap

            is_ma20_rising_flag = row_dict.get('추세상승', True)

            quant_score = calculate_dynamic_score(
                f_str=row_dict.get('외인강도(%)', 0), p_str=row_dict.get('연기금강도(%)', 0),
                t_str=row_dict.get('투신강도(%)', 0), pef_str=row_dict.get('사모강도(%)', 0),
                vol_surge=row_dict.get('거래급증(%)', 0), rsi_val=row_dict.get('RSI', 50),
                gap_20=new_gap, foreign_streak=row_dict.get('외인연속', 0),
                pension_streak=row_dict.get('연기금연속', 0), 
                turnover_rate=row_dict.get('손바뀜(%)', 0), 
                is_ma20_rising=is_ma20_rising_flag, 
                per_val=row_dict.get('PER', 0), roe_val=row_dict.get('ROE', 0), 
                current_vix=current_vix
            )
            qual_score = calculate_qualitative_score(
                sector_name=row_dict.get('섹터', '분류안됨'),
                per_val=row_dict.get('PER', 0),
                roe_val=row_dict.get('ROE', 0),
                foreign_streak=row_dict.get('외인연속', 0),
                pension_streak=row_dict.get('연기금연속', 0),
                macro_news_text=news_str_for_scoring,
                macro_recency_score=macro_recency_score,
                repeated_topics_text=repeated_topics_text
            )
            final_score, qual_adj, score_mode = blend_quant_qual_score(quant_score, qual_score, current_vix)
            row_dict['Quant점수'] = int(round(quant_score))
            row_dict['정성점수'] = round(qual_score, 2)
            row_dict['정성보정치'] = qual_adj
            row_dict['점수모드'] = score_mode
            row_dict['AI수급점수'] = final_score
            updated_rows.append(row_dict)

        df_final = pd.DataFrame(updated_rows).sort_values('AI수급점수', ascending=False)
        
        eval_msg = "⚡ (슈퍼 캐시 모드로 재산출된 랭킹입니다.)\n\n"
            
    # ==========================================
    # 2. 풀 파싱 모드 (정규 수집)
    # ==========================================
    else:
        print("📥 [풀 파싱 모드] 새로운 수급 데이터 및 추세 정보를 KIS API로부터 수집합니다.")
        df_target = get_target_stock_list()
        token = get_kis_access_token()
        kis_app_key, kis_app_secret = resolve_kis_credentials()
        if not token or not kis_app_key or not kis_app_secret:
            print("❌ KIS 인증 정보가 유효하지 않아 풀 파싱을 중단합니다.")
            return
        headers = {"authorization": f"Bearer {token}", "appkey": kis_app_key, "appsecret": kis_app_secret, "tr_id": "FHPTJ04160001", "custtype": "P"}
        url_kis = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"

        data_list, history_list = [], []

        for i, row in enumerate(df_target.itertuples()):
            code, name, prpr, marcap = row.종목코드, row.종목명, row.현재가, row.시가총액
            sector_name = "분류안됨"
            try:
                res_nv = requests.get(f"https://finance.naver.com/item/main.naver?code={code}", headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
                sector_tag = BeautifulSoup(res_nv.text, 'html.parser').select_one('div.trade_compare h4.h_sub a')
                if sector_tag: sector_name = sector_tag.text.strip()
            except: pass

            params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code, "FID_INPUT_DATE_1": target_kis_date, "FID_ORG_ADJ_PRC": "0", "FID_ETC_CLS_CODE": "0"}
            
            try:
                res = requests.get(url_kis, headers=headers, params=params, timeout=5)
                f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum = 0, 0, 0, 0
                foreign_streak, pension_streak, f_buying, p_buying = 0, 0, True, True  
                closes, volumes, vol_tr_sum_5d = [], [], 0 

                if res.status_code == 200 and res.json().get('rt_cd') == "0":
                    daily_list = res.json().get('output2', [])
                    if daily_list:
                        for idx, daily in enumerate(daily_list[:20]): 
                            close_prc = safe_api_float(daily.get('stck_clpr'))
                            vol = safe_api_float(daily.get('acml_vol'))
                            closes.append(close_prc)
                            volumes.append(vol)
                            
                            f_amt = safe_api_float(daily.get('frgn_ntby_qty')) * close_prc
                            p_amt = safe_api_float(daily.get('fund_ntby_qty')) * close_prc
                            t_amt = safe_api_float(daily.get('ivtr_ntby_qty')) * close_prc
                            pef_amt = safe_api_float(daily.get('pe_fund_ntby_vol')) * close_prc
                            
                            f_amt_sum += f_amt; p_amt_sum += p_amt; t_amt_sum += t_amt; pef_amt_sum += pef_amt
                            if idx < 5: vol_tr_sum_5d += (vol * close_prc)

                            history_list.append({
                                '종목명': name, '일자': daily.get('stck_bsop_date', ''),
                                '종가': close_prc, '외인': f_amt / 1_000_000, '연기금': p_amt / 1_000_000,
                                '투신': t_amt / 1_000_000, '사모': pef_amt / 1_000_000
                            })

                        for daily in daily_list:
                            f_qty, p_qty = safe_api_float(daily.get('frgn_ntby_qty')), safe_api_float(daily.get('fund_ntby_qty'))
                            if f_buying:
                                if f_qty > 0: foreign_streak += 1
                                else: f_buying = False
                            if p_buying:
                                if p_qty > 0: pension_streak += 1
                                else: p_buying = False
                            if not f_buying and not p_buying: break

                ma20 = sum(closes) / len(closes) if closes else prpr
                gap_20, marcap_won = (prpr / ma20) * 100 if ma20 else 100, marcap * 100_000_000 
                f_str, p_str, t_str, pef_str = [(amt / marcap_won) * 100 if marcap_won else 0 for amt in (f_amt_sum, p_amt_sum, t_amt_sum, pef_amt_sum)]
                turnover_rate = (vol_tr_sum_5d / marcap_won) * 100 if marcap_won else 0 
                
                rsi_val = calculate_rsi(closes[::-1])
                if len(volumes) > 1:
                    past_vols = volumes[1:6]
                    avg_vol = sum(past_vols) / len(past_vols) if past_vols else 0
                    vol_surge = (volumes[0] / avg_vol * 100) if avg_vol > 0 else 0
                else:
                    vol_surge = 0

                is_ma20_rising = False
                if len(closes) >= 20:
                    is_ma20_rising = closes[0] >= closes[-1]
                else:
                    is_ma20_rising = gap_20 >= 100 

                quant_score = calculate_dynamic_score(f_str, p_str, t_str, pef_str, vol_surge, rsi_val, gap_20, foreign_streak, pension_streak, turnover_rate, is_ma20_rising, row.PER, row.ROE, current_vix)
                qual_score = calculate_qualitative_score(
                    sector_name=sector_name,
                    per_val=row.PER,
                    roe_val=row.ROE,
                    foreign_streak=foreign_streak,
                    pension_streak=pension_streak,
                    macro_news_text=news_str_for_scoring,
                    macro_recency_score=macro_recency_score,
                    repeated_topics_text=repeated_topics_text
                )
                final_score, qual_adj, score_mode = blend_quant_qual_score(quant_score, qual_score, current_vix)

                data_list.append({
                    '종목명': name, '종목코드': code, '소속': row.소속, '섹터': sector_name, 'AI수급점수': final_score,
                    'Quant점수': int(round(quant_score)), '정성점수': round(qual_score, 2), '정성보정치': qual_adj, '점수모드': score_mode,
                    '현재가': prpr, '등락률': row.등락률, '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                    '외인연속': foreign_streak, '연기금연속': pension_streak, '이격도(%)': round(gap_20, 1), '손바뀜(%)': round(turnover_rate, 1),
                    'RSI': round(rsi_val, 1), '거래급증(%)': round(vol_surge, 1),
                    '추세상승': is_ma20_rising, 
                    '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE
                })
            except: pass 
            time.sleep(0.2) 

        if not data_list: return

        df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
        
        df_history = pd.DataFrame(history_list)
        if not df_history.empty:
            df_history.to_csv("history.csv", index=False, encoding='utf-8-sig')
        else:
            # 빈 수집 결과로 기존 history.csv를 덮어써서 파일이 깨지는 상황 방지
            print("⚠️ history_list가 비어 있어 history.csv 갱신을 건너뜁니다. (기존 파일 유지)")

        eval_msg = ""
    
    # 상위 후보군 정성 심화(공시/리포트) 재보정
    df_final = apply_enhanced_qual_for_top_candidates(df_final, current_vix=current_vix, top_n=40)
    df_final.to_csv("data.csv", index=False, encoding='utf-8-sig')

    df_trend_new = df_final[['종목명', '종목코드', 'AI수급점수']].copy()
    df_trend_new['순위'] = df_trend_new['AI수급점수'].rank(method='min', ascending=False).astype(int)
    df_trend_new['날짜'] = today_date

    trend_file = "score_trend.csv"
    if os.path.exists(trend_file):
        df_trend_old = pd.read_csv(trend_file)
        df_trend_old = df_trend_old[df_trend_old['날짜'] != today_date]
        pd.concat([df_trend_old, df_trend_new], ignore_index=True).to_csv(trend_file, index=False, encoding='utf-8-sig')
    else:
        df_trend_new.to_csv(trend_file, index=False, encoding='utf-8-sig')

    # ==========================================
    # 백테스트 정산 로직
    # ==========================================
    portfolio_file = "portfolio.csv"
    perf_file = "performance_trend.csv"
    top3_names = df_final.head(3)['종목명'].tolist() 

    if os.path.exists(portfolio_file):
        try:
            df_port = pd.read_csv(portfolio_file)
            
            returns, eval_details = [], []
            for _, row in df_port.iterrows():
                p_stock = row['종목명']
                p_buy = row['매수가']
                today_row = df_final[df_final['종목명'] == p_stock]
                p_sell = today_row.iloc[0]['현재가'] if not today_row.empty else p_buy
                ret = ((p_sell - p_buy) / p_buy) * 100
                returns.append(ret)
                mark = "🔴" if ret > 0 else "🔵" if ret < 0 else "⚫"
                eval_details.append(f"- {p_stock}: {ret:+.2f}% {mark}")
            
            daily_ret = sum(returns) / len(returns) if returns else 0
            
            # 🔥 [방어 로직] KIS API를 스킵하는 테스트 모드일 때는 포트폴리오 수익률을 절대 갱신하지 않음
            if not is_test_mode:
                cum_ret = daily_ret
                if os.path.exists(perf_file):
                    df_perf = pd.read_csv(perf_file)
                    if not df_perf.empty:
                        df_perf = df_perf[df_perf['날짜'] != today_date] 
                        cum_ret = df_perf['누적수익률'].iloc[-1] + daily_ret if len(df_perf) > 0 else daily_ret
                    else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])
                else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])
                    
                new_perf = pd.DataFrame([{'날짜': today_date, '일간수익률': daily_ret, '누적수익률': cum_ret}])
                pd.concat([df_perf, new_perf], ignore_index=True).to_csv(perf_file, index=False, encoding='utf-8-sig')

            if is_eod_updated:
                eval_msg += "📝 *[전일 추천 Top 3 최종 성적표]*\n" + "\n".join(eval_details) + f"\n➡️ *오늘 포트폴리오 최종 수익률: {daily_ret:+.2f}%*\n\n"
                
                if not is_test_mode:
                    top3_df = df_final.head(3)[['종목명', '현재가']].rename(columns={'현재가': '매수가'})
                    top3_df['날짜'] = today_date
                    top3_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
            else:
                eval_msg += "📝 *[현재 포트폴리오 장중 수익률]*\n" + "\n".join(eval_details) + f"\n➡️ *실시간 수익률: {daily_ret:+.2f}%*\n\n"
        except: pass
    else:
        if is_eod_updated and not is_test_mode:
            top3_df = df_final.head(3)[['종목명', '현재가']].rename(columns={'현재가': '매수가'})
            top3_df['날짜'] = today_date
            top3_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')

    # ==========================================
    # 텔레그램 발송 및 AI 리포트
    # ==========================================
    gemini_api_key = resolve_gemini_api_key()
    if gemini_api_key:
        try:
            client = genai.Client(api_key=gemini_api_key)
            top3_str = ", ".join(top3_names)
            MY_STREAMLIT_URL = "https://ge82mjcdoxngn3p6udv5sy.streamlit.app"
            top_N_names = df_final.head(20)['종목명'].tolist()
            if os.path.exists("history.csv"):
                try:
                    df_history = pd.read_csv("history.csv")
                    required_cols = {"일자", "종목명", "외인", "연기금"}
                    if not df_history.empty and required_cols.issubset(df_history.columns):
                        latest_date = df_history['일자'].max()
                        df_today = df_history[(df_history['일자'] == latest_date) & (df_history['종목명'].isin(top_N_names))]
                        df_merged = pd.merge(df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']], df_today[['종목명', '외인', '연기금']], on='종목명', how='left')
                        df_merged.rename(columns={'외인': '당일_외인순매수(백만)', '연기금': '당일_연기금순매수(백만)'}, inplace=True)
                    else:
                        df_merged = df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']]
                except Exception:
                    df_merged = df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']]
            else:
                df_merged = df_final.head(20)[['종목명', '섹터', 'AI수급점수', '손바뀜(%)', 'RSI', '거래급증(%)']]
            
            macro_str, news_str = macro_str_for_scoring, news_str_for_scoring
            top3_event_context = build_top3_event_context(df_final)
            print("📌 Top3 공시/리포트 컨텍스트 수집 완료")
            session_label = "장 마감" if is_eod_updated else "장중"
            
            prompt = f"""
            너는 QEdge 수석 퀀트 애널리스트야. 오늘은 {now_kst.strftime("%Y년 %m월 %d일")}이야.
            현재 세션은 {session_label}이고, VIX 지수는 {current_vix:.2f}로 {regime} 모드야.
            
            [1. 매크로 지표]
            {macro_str}
            
            [2. 네이버 금융 주요 뉴스 (제목 및 내용 요약)]
            {news_str}

            [2-1. 반복 핵심 이슈 Top5]
            {repeated_topics_text if repeated_topics_text else "반복 이슈 데이터 없음"}
            
            [3. 최상위 20개 종목 수급 및 모멘텀 동향]
            {df_merged.to_string(index=False)}

            [4. Top 3 최근 공시/증권사 리포트]
            {top3_event_context}

            다음 순서로 전문가 수준의 리포트를 작성해 줘.
            1. 글로벌 매크로 요약
            2. 섹터 및 수급 동향
            3. Top 3 관심종목 및 근거
            4. 공시/리포트 체크포인트
            [주의] 제공 텍스트 외의 외부 검색 없이 작성.
            """
            
            response = client.models.generate_content(
                model='gemma-4-31b-it',
                contents=prompt
            )
            
            with open("report.md", "w", encoding="utf-8") as f:
                f.write(f"## 🌐 QEdge 데일리 퀀트 리포트 ({now_kst.strftime('%Y-%m-%d %H:%M')})\n\n{response.text}")

            if not is_eod_updated:
                tg_message = f"🔔 *[장중 요약]*\n🗓 {now_kst.strftime('%Y-%m-%d %H:%M')}\n📊 VIX 국면: {regime}\n\n{eval_msg}🏆 *QEdge Top 3*\n: {top3_str}\n\n📊 [대시보드 바로가기]({MY_STREAMLIT_URL})"
                send_telegram_message(tg_message)
            else:
                tg_message = f"🔔 *[장 마감 요약 (테스트 모드)]*\n🗓 {now_kst.strftime('%Y-%m-%d %H:%M')}\n\n{eval_msg}🏆 *QEdge Top 3*\n: {top3_str}\n\n---\n\n{response.text}\n\n📊 [대시보드 바로가기]({MY_STREAMLIT_URL})"
                send_telegram_message(tg_message)
        except Exception as e:
            print(f"⚠️ AI 리포트 생성 실패: {e}")
            fallback_report = f"""## 🌐 QEdge 데일리 퀀트 리포트 ({now_kst.strftime('%Y-%m-%d %H:%M')})

AI 리포트 생성에 실패하여 자동 요약본으로 대체합니다.

### Top 3
- {top3_names[0] if len(top3_names) > 0 else '-'}
- {top3_names[1] if len(top3_names) > 1 else '-'}
- {top3_names[2] if len(top3_names) > 2 else '-'}

### VIX / Regime
- VIX: {current_vix:.2f}
- Mode: {regime}

### Macro
{macro_str_for_scoring if macro_str_for_scoring else '- 데이터 없음'}

### News
{news_str_for_scoring if news_str_for_scoring else '- 데이터 없음'}

### Repeated Topics
{repeated_topics_text if repeated_topics_text else '- 데이터 없음'}
"""
            with open("report.md", "w", encoding="utf-8") as f:
                f.write(fallback_report)
    else:
        # API 키가 없더라도 report.md는 매 실행 최신화
        fallback_report = f"""## 🌐 QEdge 데일리 퀀트 리포트 ({now_kst.strftime('%Y-%m-%d %H:%M')})

Gemini API 키가 없어 자동 요약본으로 생성했습니다.

### Top 3
- {top3_names[0] if len(top3_names) > 0 else '-'}
- {top3_names[1] if len(top3_names) > 1 else '-'}
- {top3_names[2] if len(top3_names) > 2 else '-'}

### VIX / Regime
- VIX: {current_vix:.2f}
- Mode: {regime}

### Macro
{macro_str_for_scoring if macro_str_for_scoring else '- 데이터 없음'}

### News
{news_str_for_scoring if news_str_for_scoring else '- 데이터 없음'}

### Repeated Topics
{repeated_topics_text if repeated_topics_text else '- 데이터 없음'}
"""
        with open("report.md", "w", encoding="utf-8") as f:
            f.write(fallback_report)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QEdge scraper runner")
    parser.add_argument("--full-parse", action="store_true", help="슈퍼 캐시를 무시하고 KIS 풀 파싱을 강제 실행")
    args = parser.parse_args()
    run_scraper(manual_full_parse=args.full_parse)
