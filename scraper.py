import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta, timezone 
from bs4 import BeautifulSoup, FeatureNotFound
import os
from google import genai
from google.genai import types
import yfinance as yf
import re
from email.utils import parsedate_to_datetime
from pathlib import Path
import tomllib
import argparse
import io
from db_utils import read_table, write_table, migrate_csv_to_sqlite_once, csv_exists, resolve_csv_path
from news_utils import (
    normalize_text as _normalize_text,
    extract_source as _extract_source,
    event_tags as _event_tags,
    title_signature as _title_signature,
    is_similar_title as _is_similar_title,
    score_news_candidate as _score_news_candidate_base,
)

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

def safe_read_csv_with_conflict_guard(path, **kwargs):
    """
    git merge conflict 마커가 섞인 CSV를 최대한 복구해 읽습니다.
    복구 불가 시 빈 DataFrame 반환.
    """
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        pass
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            lines = f.readlines()
        cleaned = []
        for line in lines:
            s = line.lstrip()
            if s.startswith("<<<<<<<") or s.startswith("=======") or s.startswith(">>>>>>>"):
                continue
            cleaned.append(line)
        if not cleaned:
            return pd.DataFrame()
        return pd.read_csv(io.StringIO("".join(cleaned)), **kwargs)
    except Exception:
        return pd.DataFrame()


def _table_name_for(csv_path):
    return Path(csv_path).stem


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


def _bytes_to_mb(n_bytes):
    return float(n_bytes) / (1024.0 * 1024.0)


def emit_weekly_storage_report():
    """
    주간 1회 저장소 용량 로그를 출력하고 data/storage_report.csv에 누적합니다.
    """
    report_csv = "storage_report.csv"
    report_path = resolve_csv_path(report_csv)
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        existing = read_table_prefer_db(report_csv, on_bad_lines="skip")
    except Exception:
        existing = pd.DataFrame()

    should_log = True
    if not existing.empty and "날짜" in existing.columns:
        try:
            last_date = pd.to_datetime(existing["날짜"], errors="coerce").dropna().max()
            if pd.notna(last_date):
                should_log = (datetime.now() - last_date.to_pydatetime()).days >= 7
        except Exception:
            should_log = True

    db_size = os.path.getsize("quantbot.db") if os.path.exists("quantbot.db") else 0
    data_dir = Path("data")
    csv_files = list(data_dir.glob("*.csv")) if data_dir.exists() else []
    csv_size = sum(p.stat().st_size for p in csv_files if p.is_file())
    total_size = db_size + csv_size

    # 콘솔에는 매 실행 출력(운영 가시성)
    print(
        f"[STORAGE] DB={_bytes_to_mb(db_size):.2f}MB | "
        f"CSV={_bytes_to_mb(csv_size):.2f}MB ({len(csv_files)} files) | "
        f"TOTAL={_bytes_to_mb(total_size):.2f}MB"
    )

    if not should_log:
        return

    row = pd.DataFrame([{
        "날짜": today,
        "db_mb": round(_bytes_to_mb(db_size), 3),
        "csv_mb": round(_bytes_to_mb(csv_size), 3),
        "csv_files": int(len(csv_files)),
        "total_mb": round(_bytes_to_mb(total_size), 3),
    }])

    if existing.empty:
        out = row
    else:
        out = pd.concat([existing, row], ignore_index=True)
        out = out.drop_duplicates(subset=["날짜"], keep="last")
    write_table_dual(out, report_csv, index=False, encoding="utf-8-sig")
    print(f"[STORAGE] Weekly report updated -> {report_path}")

def resolve_dart_api_key():
    """
    DART Open API 키 로딩 (resolve_gemini_api_key와 동일 우선순위):
    1) OS 환경변수 DART_API_KEY
    2) .streamlit/secrets.toml
    3) secrets.toml
    """
    env_key = os.environ.get("DART_API_KEY")
    if env_key:
        return str(env_key).strip()

    candidate_files = [
        Path(".streamlit/secrets.toml"),
        Path("secrets.toml"),
    ]
    for p in candidate_files:
        try:
            if p.exists():
                with p.open("rb") as f:
                    parsed = tomllib.load(f)
                key = parsed.get("DART_API_KEY")
                if key:
                    return str(key).strip()
        except Exception:
            continue
    return None

_THEME_MAP_CACHE = None

def load_theme_map():
    """
    theme_map.csv를 로드해 {종목코드/종목명 -> 테마} 매핑을 구성합니다.
    파일 형식 예: 종목코드,종목명,테마
    """
    global _THEME_MAP_CACHE
    if _THEME_MAP_CACHE is not None:
        return _THEME_MAP_CACHE
    path = Path(resolve_csv_path("theme_map.csv"))
    code_map, name_map = {}, {}
    if not path.exists():
        _THEME_MAP_CACHE = (code_map, name_map)
        return _THEME_MAP_CACHE
    try:
        df = pd.read_csv(path, dtype=str)
        cols = {str(c).strip(): c for c in df.columns}
        code_col = cols.get("종목코드")
        name_col = cols.get("종목명")
        theme_col = cols.get("테마")
        if theme_col is None:
            _THEME_MAP_CACHE = (code_map, name_map)
            return _THEME_MAP_CACHE
        for _, row in df.iterrows():
            theme = str(row.get(theme_col, "") or "").strip()
            if not theme:
                continue
            if code_col:
                code = str(row.get(code_col, "") or "").strip().zfill(6)
                if len(code) == 6 and code.isdigit():
                    code_map[code] = theme
            if name_col:
                name = str(row.get(name_col, "") or "").strip()
                if name:
                    name_map[name] = theme
    except Exception as e:
        print(f"[WARN] theme_map.csv 로드 실패: {e}")
    _THEME_MAP_CACHE = (code_map, name_map)
    return _THEME_MAP_CACHE

def resolve_theme_label(stock_code, stock_name, sector_name):
    """수동 테마 사전 우선, 없으면 기존 섹터를 테마로 사용."""
    code_map, name_map = load_theme_map()
    code = str(stock_code or "").strip().zfill(6)
    name = str(stock_name or "").strip()
    if code in code_map:
        return code_map[code]
    if name in name_map:
        return name_map[name]
    return sector_name or "분류안됨"

def infer_theme_candidate(stock_name, sector_name):
    """종목명/섹터 기반 경량 테마 추론."""
    name = str(stock_name or "").lower()
    sector = str(sector_name or "").lower()
    name_rules = [
        (["로보", "robot"], "로봇;자동화", "종목명 키워드(로보/robot)", 0.92),
        (["태양", "solar", "에너지솔루션"], "태양광;신재생", "종목명 키워드(태양/solar)", 0.9),
        (["전선", "전기"], "전력망;전선", "종목명 키워드(전선/전기)", 0.88),
        (["이노텍", "모듈"], "IT부품;카메라모듈", "종목명 키워드(이노텍/모듈)", 0.85),
        (["반도체", "세미", "hpsp", "isc"], "반도체;반도체장비", "종목명 키워드(반도체/세미)", 0.86),
        (["바이오", "제약"], "바이오;제약", "종목명 키워드(바이오/제약)", 0.84),
    ]
    for keys, theme, reason, conf in name_rules:
        if any(k in name for k in keys):
            return theme, reason, conf

    sector_rules = [
        ("반도체", "반도체;반도체장비", "섹터 기반(반도체)", 0.78),
        ("전기", "2차전지;전력", "섹터 기반(전기)", 0.72),
        ("전자장비", "PCB;전자부품", "섹터 기반(전자장비)", 0.7),
        ("기계", "기계;자동화", "섹터 기반(기계)", 0.68),
        ("화학", "화학소재;2차전지소재", "섹터 기반(화학)", 0.66),
        ("에너지", "신재생;에너지", "섹터 기반(에너지)", 0.72),
        ("건설", "건설;플랜트", "섹터 기반(건설)", 0.7),
        ("조선", "조선;방산", "섹터 기반(조선)", 0.72),
        ("제약", "바이오;제약", "섹터 기반(제약)", 0.72),
    ]
    for key, theme, reason, conf in sector_rules:
        if key in sector:
            return theme, reason, conf
    return str(sector_name or "분류안됨"), "섹터 fallback", 0.5

def generate_theme_suggestions(df_final, today_date, top_n=40):
    """
    상위 후보(top_n)에 대해 theme_suggestions.csv를 생성/갱신합니다.
    - 수동 theme_map에 없는 종목만 pending 추천으로 기록
    - 기존 pending은 최신 값으로 갱신
    """
    if df_final.empty:
        return
    code_map, name_map = load_theme_map()
    top_df = df_final.sort_values("AI수급점수", ascending=False).head(top_n).copy()
    rows = []
    for _, row in top_df.iterrows():
        code = str(row.get("종목코드", "") or "").strip().zfill(6)
        name = str(row.get("종목명", "") or "").strip()
        sector = str(row.get("섹터", "") or "").strip()
        if not name:
            continue
        # 수동 확정값이 있는 종목은 추천 큐에서 제외
        if (code and code in code_map) or (name in name_map):
            continue
        theme, reason, conf = infer_theme_candidate(name, sector)
        rows.append({
            "날짜": str(today_date),
            "종목코드": code,
            "종목명": name,
            "현재섹터": sector,
            "추천테마": theme,
            "신뢰도": round(float(conf), 3),
            "근거": reason,
            "승인상태": "pending",
        })

    if not rows:
        return

    path = Path(resolve_csv_path("theme_suggestions.csv"))
    new_df = pd.DataFrame(rows)
    if path.exists():
        old_df = safe_read_csv_with_conflict_guard(path, dtype=str)
        if old_df.empty:
            out_df = new_df
        else:
            for col in ["날짜", "종목코드", "종목명", "현재섹터", "추천테마", "신뢰도", "근거", "승인상태"]:
                if col not in old_df.columns:
                    old_df[col] = ""
            # pending/rejected만 최신 추천으로 갱신하고 approved 이력은 유지
            old_keep = old_df[old_df["승인상태"].astype(str) == "approved"].copy()
            merged = pd.concat([old_keep, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["종목코드"], keep="last")
            out_df = merged
    else:
        out_df = new_df
    write_table_dual(out_df, str(path), index=False, encoding="utf-8-sig")

_DART_STOCK_TO_CORP = None

def load_dart_stock_to_corp_map():
    """dart_map.csv가 있으면 {stock_code(6자리): corp_code} 로드 (전역 캐시)."""
    global _DART_STOCK_TO_CORP
    if _DART_STOCK_TO_CORP is not None:
        return _DART_STOCK_TO_CORP
    path = Path(resolve_csv_path("dart_map.csv"))
    if not path.exists():
        _DART_STOCK_TO_CORP = {}
        return _DART_STOCK_TO_CORP
    try:
        df = read_table_prefer_db(str(path), dtype=str)
        m = {}
        for _, row in df.iterrows():
            sc = str(row.get("stock_code", "") or "").strip()
            cc = str(row.get("corp_code", "") or "").strip()
            if len(sc) == 6 and sc.isdigit() and cc:
                m[sc] = cc
        _DART_STOCK_TO_CORP = m
    except Exception:
        _DART_STOCK_TO_CORP = {}
    return _DART_STOCK_TO_CORP

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
def calculate_dynamic_score(
    f_str, p_str, t_str, pef_str,
    vol_surge, rsi_val, gap_20,
    foreign_streak, pension_streak,
    turnover_rate, is_ma20_rising,
    per_val, roe_val, current_vix,
    dip_buying_ratio=0.0
):
    
    if current_vix < 25:
        # 🚀 상승장: V41.1 눌림목 + 모멘텀 복원
        zombie_penalty = 0 
        fund_score = 0
        
        # 기관(투신4, 사모4, 연기금2)에 압도적 가중치, 외인(0.5)은 보조 지표로 강등
        raw_str_sum = (t_str * 4) + (pef_str * 4) + (p_str * 2) + (f_str * 0.5)
        strength_score = max(0, min(20, raw_str_sum * 2))
        streak_score = max(0, min(10, (pension_streak * 1.5) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score # 최대 30점

        # 모멘텀(최대 45): V40.4 수준으로 강하게 복원
        turnover_score = 20 if turnover_rate >= 10 else (10 if turnover_rate >= 5 else 0)
        v_score = 10 if vol_surge >= 150 else 0
        r_score = 15 if 60 <= rsi_val <= 85 else (5 if 50 <= rsi_val < 60 else 0)
        momentum_score = turnover_score + v_score + r_score

        # 이격도/음봉매집 결합(최대 25)
        if 102 <= gap_20 <= 108:
            tech_score = 10
            if float(dip_buying_ratio) >= 0.6:
                tech_score += 15
        elif 98 <= gap_20 < 102:
            tech_score = 5
            if float(dip_buying_ratio) >= 0.6:
                tech_score += 15
        else:
            tech_score = 0

        return max(0, min(100, int(supply_score + momentum_score + tech_score)))
        
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

def calculate_qualitative_score(
    sector_name, per_val, roe_val, foreign_streak, pension_streak,
    macro_news_text, macro_recency_score=50.0, repeated_topics_text="", return_details=False
):
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
    positive_tone_keys = ["호재", "상향", "증가", "개선", "수주", "체결", "흑자", "서프라이즈", "기대", "확대"]
    neutral_tone_keys = ["전망", "관측", "분석", "주목", "설명", "동향", "점검", "리포트", "이슈"]
    negative_tone_keys = ["긴축", "관세", "하락", "리스크", "소송", "악재", "부진", "감소", "충격", "약세"]

    positive_hits = sum(1 for k in positive_tone_keys if k in text)
    neutral_hits = sum(1 for k in neutral_tone_keys if k in text)
    negative_hits = sum(1 for k in negative_tone_keys if k in text)

    # 테마 매칭 점수는 "호재 톤일 때 강화 / 중립 톤일 때 약화"시켜 군집 편향을 완화
    if positive_hits > negative_hits:
        theme_tone_mult = 1.0
    elif neutral_hits >= max(1, positive_hits):
        theme_tone_mult = 0.35
    else:
        theme_tone_mult = 0.55

    theme_boost = 0.0
    for sector_key, keywords in sector_theme_map.items():
        if sector_key in sector and any(k.lower() in text for k in keywords):
            theme_boost = 8 * decay_factor * theme_tone_mult
            break
    # 테마 가점 상한(캡): 설명형 기사 과다 노출 시 과도한 누적 방지
    score += min(4.5, theme_boost)

    # 리스크 키워드 감점
    if negative_hits > 0:
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

    final_score = max(0, min(100, score))
    if not return_details:
        return final_score
    details = {
        "theme_boost_raw": round(float(theme_boost), 3),
        "theme_boost_applied": round(float(min(4.5, theme_boost)), 3),
        "theme_tone_mult": round(float(theme_tone_mult), 3),
        "positive_hits": int(positive_hits),
        "neutral_hits": int(neutral_hits),
        "negative_hits": int(negative_hits),
        "decay_factor": round(float(decay_factor), 3),
    }
    return final_score, details

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
    return round(final_score, 2), round(qual_adj, 2), mode

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

def apply_theme_crowding_penalty(df_final, top_n=40, crowd_ratio=0.3):
    """
    상위권(top_n)에서 특정 테마가 과도하게 몰릴 때 완만한 감점으로 군집 편향 완화.
    - crowd_ratio(기본 30%) 초과분만 감점
    - 종목당 최대 -3점 제한
    """
    if df_final.empty or "AI수급점수" not in df_final.columns:
        return df_final
    df_out = df_final.copy()
    top_slice = df_out.sort_values("AI수급점수", ascending=False).head(top_n).copy()
    if top_slice.empty:
        return df_out

    theme_col = "테마" if "테마" in top_slice.columns else ("섹터" if "섹터" in top_slice.columns else None)
    if not theme_col:
        return df_out

    themes = top_slice[theme_col].fillna("").astype(str).str.split(";").str[0].str.strip()
    counts = themes.value_counts()
    if counts.empty:
        return df_out

    total = max(1, len(top_slice))
    for idx, row in top_slice.iterrows():
        key = str(row.get(theme_col, "") or "").split(";")[0].strip()
        if not key:
            continue
        ratio = float(counts.get(key, 0)) / total
        if ratio <= crowd_ratio:
            continue
        overload = ratio - crowd_ratio
        penalty = min(3.0, overload * 10.0)
        new_score = float(df_out.at[idx, "AI수급점수"]) - penalty
        df_out.at[idx, "AI수급점수"] = max(0.0, round(new_score, 2))
    return df_out.sort_values("AI수급점수", ascending=False)

def apply_score_stability(df_final, today_date, max_daily_delta=8.0, smooth_alpha=0.7):
    """
    1) 점수 변동성 관리:
    - 전일 점수와 EMA 블렌딩(smooth_alpha)
    - 일일 점수 변화 상한(max_daily_delta) 적용
    """
    if df_final.empty or "AI수급점수" not in df_final.columns or "종목명" not in df_final.columns:
        return df_final
    if not csv_exists("score_trend.csv"):
        return df_final
    try:
        df_tr = load_score_trend_safe()
    except Exception:
        return df_final
    if df_tr.empty or "날짜" not in df_tr.columns:
        return df_final

    dates = sorted(df_tr["날짜"].astype(str).unique(), reverse=True)
    prev_date = None
    for d in dates:
        if str(d) != str(today_date):
            prev_date = str(d)
            break
    if not prev_date:
        return df_final
    prev_df = df_tr[df_tr["날짜"].astype(str) == prev_date][["종목명", "AI수급점수"]].copy()
    prev_df["AI수급점수"] = pd.to_numeric(prev_df["AI수급점수"], errors="coerce")
    prev_map = dict(zip(prev_df["종목명"].astype(str), prev_df["AI수급점수"]))

    out = df_final.copy()
    smoothed = []
    deltas = []
    for _, row in out.iterrows():
        name = str(row.get("종목명", ""))
        cur = float(pd.to_numeric(row.get("AI수급점수"), errors="coerce") or 0.0)
        prev = prev_map.get(name, None)
        if prev is None or pd.isna(prev):
            new_score = cur
            delta = 0.0
        else:
            ema_score = (smooth_alpha * cur) + ((1.0 - smooth_alpha) * float(prev))
            low = float(prev) - max_daily_delta
            high = float(prev) + max_daily_delta
            new_score = max(low, min(high, ema_score))
            delta = new_score - float(prev)
        smoothed.append(round(float(new_score), 2))
        deltas.append(round(float(delta), 2))
    out["AI수급점수"] = smoothed
    out["점수변화(안정화)"] = deltas
    return out.sort_values("AI수급점수", ascending=False)

def add_signal_confidence(df_final, current_vix=20.0):
    """
    2) 신호 신뢰도 계층화:
    Quant/정성/점수모드/뉴스톤 정보를 합쳐 0~100 신뢰도 산출 및 등급 부여.
    """
    if df_final.empty:
        return df_final
    out = df_final.copy()
    conf_scores = []
    if current_vix >= 28:
        high_cut, mid_cut = 72.0, 52.0
        regime_penalty = 2.0
        regime_label = "RiskOff"
    elif current_vix >= 22:
        high_cut, mid_cut = 74.0, 54.0
        regime_penalty = 1.0
        regime_label = "Neutral"
    else:
        high_cut, mid_cut = 76.0, 56.0
        regime_penalty = 0.0
        regime_label = "RiskOn"

    for _, row in out.iterrows():
        q = float(pd.to_numeric(row.get("Quant점수"), errors="coerce") or 0.0)
        qual = float(pd.to_numeric(row.get("정성점수"), errors="coerce") or 50.0)
        ai = float(pd.to_numeric(row.get("AI수급점수"), errors="coerce") or 0.0)
        tone_neg = float(pd.to_numeric(row.get("뉴스부정키워드수"), errors="coerce") or 0.0)
        mode = str(row.get("점수모드", ""))

        score = 0.45 * q + 0.35 * ai + 0.2 * qual
        if "하락장" in mode:
            score -= 3.0
        score -= regime_penalty
        score -= min(8.0, tone_neg * 1.5)
        score = max(0.0, min(100.0, score))
        conf_scores.append(round(score, 2))
    score_s = pd.Series(conf_scores, dtype=float)
    # 당일 횡단면 기반 임계치(등급 분별력 확보) + 레짐별 가산
    q_high = float(score_s.quantile(0.88)) if len(score_s) else high_cut
    q_mid = float(score_s.quantile(0.62)) if len(score_s) else mid_cut
    if regime_label == "RiskOff":
        high_bias, mid_bias = 1.2, 0.8
    elif regime_label == "Neutral":
        high_bias, mid_bias = 0.6, 0.4
    else:
        high_bias, mid_bias = 0.0, 0.0
    eff_high = q_high + high_bias
    eff_mid = q_mid + mid_bias
    if eff_mid >= eff_high:
        eff_mid = eff_high - 4.0
    conf_labels = []
    for s in score_s:
        if s >= eff_high:
            conf_labels.append("High")
        elif s >= eff_mid:
            conf_labels.append("Medium")
        else:
            conf_labels.append("Low")
    out["신호신뢰도"] = score_s.round(2)
    out["신호등급"] = conf_labels
    out["신호레짐"] = regime_label
    return out

def apply_theme_contribution_guard(df_final, today_date, current_vix=20.0, top_n=40):
    """
    4) 테마/뉴스 품질 모니터링 자동화:
    - 상위권 뉴스테마가점 분포를 기록(theme_quality_trend.csv)
    - 과도 기여 시 자동 완화(가점 과열 구간만 부분 감점)
    """
    if df_final.empty or "뉴스테마가점" not in df_final.columns:
        return df_final, {}

    out = df_final.copy()
    out["뉴스테마가점"] = pd.to_numeric(out["뉴스테마가점"], errors="coerce").fillna(0.0)
    top_slice = out.sort_values("AI수급점수", ascending=False).head(top_n).copy()
    if top_slice.empty:
        return out, {}

    avg_bonus = float(top_slice["뉴스테마가점"].mean())
    p90_bonus = float(top_slice["뉴스테마가점"].quantile(0.9))
    median_bonus = float(top_slice["뉴스테마가점"].median())
    max_bonus = float(top_slice["뉴스테마가점"].max())

    dominant_theme = "-"
    dominant_ratio = 0.0
    if "테마" in top_slice.columns:
        tc = top_slice["테마"].fillna("기타").astype(str).value_counts(normalize=True)
        if len(tc) > 0:
            dominant_theme = str(tc.index[0])
            dominant_ratio = float(tc.iloc[0])

    # VIX가 높을수록 테마 과열 허용치를 낮춰 더 빨리 완화
    if current_vix >= 28:
        avg_thr, p90_thr = 2.4, 4.2
    elif current_vix >= 22:
        avg_thr, p90_thr = 2.7, 4.4
    else:
        avg_thr, p90_thr = 3.0, 4.6

    over_avg = max(0.0, avg_bonus - avg_thr)
    over_p90 = max(0.0, p90_bonus - p90_thr)
    guard_strength = min(2.2, (over_avg * 0.9) + (over_p90 * 0.7))

    affected = 0
    if guard_strength > 0:
        hot_mask = out["뉴스테마가점"] >= max(median_bonus, 0.8)
        if hot_mask.any():
            scale = ((out.loc[hot_mask, "뉴스테마가점"] - median_bonus).clip(lower=0) / max(0.5, max_bonus - median_bonus)).clip(0, 1)
            penalty = (guard_strength * scale).round(2)
            out.loc[hot_mask, "AI수급점수"] = (pd.to_numeric(out.loc[hot_mask, "AI수급점수"], errors="coerce").fillna(0.0) - penalty).clip(lower=0.0)
            out.loc[hot_mask, "테마가점완화"] = penalty
            affected = int(hot_mask.sum())
    if "테마가점완화" not in out.columns:
        out["테마가점완화"] = 0.0
    out["AI수급점수"] = pd.to_numeric(out["AI수급점수"], errors="coerce").fillna(0.0).round(2)

    metric = {
        "날짜": str(today_date),
        "상위N": int(top_n),
        "평균테마가점": round(avg_bonus, 3),
        "P90테마가점": round(p90_bonus, 3),
        "지배테마": dominant_theme,
        "지배테마비중": round(dominant_ratio, 3),
        "완화강도": round(float(guard_strength), 3),
        "완화종목수": int(affected),
        "VIX": round(float(current_vix), 2),
    }
    trend_path = "theme_quality_trend.csv"
    try:
        if os.path.exists(trend_path):
            tr = read_table_prefer_db(trend_path, on_bad_lines="skip")
        else:
            tr = pd.DataFrame(columns=list(metric.keys()))
        if not tr.empty and "날짜" in tr.columns:
            tr = tr[tr["날짜"].astype(str) != str(today_date)]
        tr_concat = pd.concat([tr, pd.DataFrame([metric])], ignore_index=True)
        write_table_dual(tr_concat, trend_path, index=False, encoding="utf-8-sig")
    except Exception as e:
        print(f"[WARN] theme_quality_trend.csv 저장 실패: {e}")

    return out.sort_values("AI수급점수", ascending=False), metric

def apply_pullback_trade_rules(df_final, current_vix=20.0):
    """
    눌림목 매매용 미세 조정(과최적화 방지 목적의 소폭 가감):
    1) 정상 눌림 진입 우대
    2) 추세 유지 + 유동성 확인 시 우대
    3) 과열 추격 구간 감점
    4) 약세 붕괴 구간 감점
    """
    if df_final.empty or "AI수급점수" not in df_final.columns:
        return df_final
    out = df_final.copy()
    ai = pd.to_numeric(out.get("AI수급점수"), errors="coerce").fillna(0.0)
    gap = pd.to_numeric(out.get("이격도(%)"), errors="coerce").fillna(100.0)
    rsi = pd.to_numeric(out.get("RSI"), errors="coerce").fillna(50.0)
    turn = pd.to_numeric(out.get("손바뀜(%)"), errors="coerce").fillna(0.0)
    trend_up = out.get("추세상승", True).astype(bool) if "추세상승" in out.columns else pd.Series([True] * len(out), index=out.index)

    # 레짐별 가중치: 변동성 높은 장에서 추격 패널티를 조금 더 강화
    if current_vix >= 28:
        pos_mult, neg_mult = 0.8, 1.25
    elif current_vix >= 22:
        pos_mult, neg_mult = 0.9, 1.1
    else:
        pos_mult, neg_mult = 1.0, 1.0

    good_pullback = (gap.between(98, 106)) & (rsi.between(48, 68))
    trend_liquidity_ok = trend_up & (turn.between(4, 18))
    overheated = (gap > 112) | (rsi > 84)
    breakdown = (gap < 95) | (rsi < 40) | (turn < 1.2)

    pullback_bonus = ((good_pullback.astype(float) * 1.1) + (trend_liquidity_ok.astype(float) * 0.6)) * pos_mult
    pullback_penalty = ((overheated.astype(float) * 1.0) + (breakdown.astype(float) * 1.4)) * neg_mult
    adj = (pullback_bonus - pullback_penalty).clip(-2.6, 1.8).round(2)

    out["눌림목가감"] = adj
    out["AI수급점수"] = (ai + adj).clip(lower=0.0, upper=100.0).round(2)
    signal_series = pd.Series("대기", index=out.index, dtype="object")
    signal_series.loc[good_pullback & trend_liquidity_ok] = "관심"
    signal_series.loc[breakdown] = "회피"
    out["눌림목신호"] = signal_series
    return out.sort_values("AI수급점수", ascending=False)

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
            except Exception as e:
                print(f"[WARN] 시가총액 페이지 수집 실패: {e}")
            time.sleep(0.5) 
    return pd.DataFrame(target_list).sort_values('시가총액', ascending=False)

def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace('**', '*') 
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000], "parse_mode": "Markdown"}
    try:
        requests.post(url, data=data)
    except Exception as e:
        print(f"[WARN] 텔레그램 전송 실패: {e}")

def _request_html(url, headers, timeout=4, retries=2):
    for attempt in range(retries + 1):
        try:
            return requests.get(url, headers=headers, timeout=timeout)
        except Exception as e:
            if attempt == retries:
                print(f"⚠️ 요청 실패: {url} ({e})")
                return None

def load_score_trend_safe():
    """머지 충돌/깨진 score_trend.csv를 방어적으로 로드."""
    base_cols = ['종목명', '종목코드', 'AI수급점수', '순위', '날짜']
    if not csv_exists("score_trend.csv"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("score_trend.csv", on_bad_lines="skip")
    if df is None:
        return pd.DataFrame(columns=base_cols)
    if df.empty:
        return pd.DataFrame(columns=base_cols)

    df.columns = [str(c).replace('\ufeff', '').strip() for c in df.columns]
    bad_cols = [c for c in df.columns if any(x in str(c) for x in ["<<<<<<<", "=======", ">>>>>>>"])]
    if bad_cols:
        df = df.drop(columns=bad_cols, errors='ignore')

    if '날짜' not in df.columns:
        return pd.DataFrame(columns=base_cols)

    marker_pat = r"^(<<<<<<<|=======|>>>>>>>)"
    if '날짜' in df.columns:
        df = df[~df['날짜'].astype(str).str.contains(marker_pat, regex=True, na=False)]
    if '종목명' in df.columns:
        df = df[~df['종목명'].astype(str).str.contains(marker_pat, regex=True, na=False)]

    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    return df[base_cols]

def _score_news_candidate(candidate):
    return _score_news_candidate_base(candidate, include_relevance=False)

def _build_macro_topic_lines(candidates, top_n=5):
    """최근 기사 후보에서 반복 이슈 TopN을 가볍게 추출."""
    if not candidates:
        return []
    now = datetime.now()
    topic_stats = {}
    for item in candidates:
        # 태그가 없는 기사를 '일반'으로 몰아넣으면 일반이 항상 1위를 차지해 정보가 희석됨.
        # 반복 이슈는 의미 태그가 잡힌 기사만 집계한다.
        tags = [t for t in (item.get("tags") or []) if t and t != "일반"]
        if not tags:
            continue
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

def _normalize_stock_code_6(stock_code):
    s = str(stock_code or "").strip()
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s if len(s) == 6 else ""

def _fetch_dart_list_json_pages(corp_code, api_key, bgn_de, end_de, pblntf_ty):
    """DART list.json 전체 페이지 수집. 실패 시 None."""
    url = "https://opendart.fss.or.kr/api/list.json"
    merged = []
    page_no = 1
    while True:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": str(page_no),
            "page_count": "100",
            "pblntf_ty": pblntf_ty,
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
        except Exception:
            return None
        status = str(data.get("status", ""))
        # DART: 013 = 조회된 데이터 없음(요청은 유효). 이 경우 빈 list로 처리.
        if status == "013":
            return merged
        if status != "000":
            return None
        batch = data.get("list") or []
        merged.extend(batch)
        try:
            total_page = int(data.get("total_page") or 1)
        except Exception:
            total_page = 1
        if page_no >= total_page:
            break
        page_no += 1
    return merged

def _parse_rss_soup(xml_text):
    """환경에 xml parser(lxml)가 없어도 RSS를 파싱할 수 있도록 폴백."""
    try:
        return BeautifulSoup(xml_text, "xml")
    except FeatureNotFound:
        return BeautifulSoup(xml_text, "html.parser")

def _get_recent_disclosures_naver(stock_code, stock_name, max_items=3):
    """네이버 금융 공시 페이지에서 최근 공시 (기존 로직)."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://finance.naver.com/item/news_notice.naver?code={stock_code}"
    res = _request_html(url, headers=headers, timeout=4, retries=1)
    if res is None:
        return []
    try:
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

def get_recent_disclosures(stock_code, stock_name, max_items=3):
    """DART Open API 우선, 실패 시 네이버 금융 공시 크롤링 Fallback."""
    sc6 = _normalize_stock_code_6(stock_code)
    dart_key = resolve_dart_api_key()
    dart_map = load_dart_stock_to_corp_map()
    corp_code = dart_map.get(sc6) if sc6 and dart_map else None

    if dart_key and corp_code:
        today = datetime.now().date()
        end_de = today.strftime("%Y%m%d")
        bgn_de = (today - timedelta(days=14)).strftime("%Y%m%d")
        combined = []
        for pty in ("I", "A"):
            batch = _fetch_dart_list_json_pages(corp_code, dart_key, bgn_de, end_de, pty)
            if batch is None:
                combined = None
                break
            combined.extend(batch)
        if combined is not None and combined:
            by_rcept = {}
            for it in combined:
                rno = it.get("rcept_no") or ""
                rdt = str(it.get("rcept_dt") or "")
                nm = str(it.get("report_nm") or "").strip()
                if not nm:
                    continue
                key = rno if rno else f"{rdt}|{nm}"
                prev = by_rcept.get(key)
                if prev is None or (rdt > (prev.get("rcept_dt") or "")):
                    by_rcept[key] = {"report_nm": nm, "rcept_dt": rdt}
            sorted_items = sorted(
                by_rcept.values(),
                key=lambda x: str(x.get("rcept_dt") or ""),
                reverse=True,
            )
            out = []
            for it in sorted_items[:max_items]:
                rdt = str(it.get("rcept_dt") or "")
                nm = it.get("report_nm") or ""
                out.append(f"- [공시] {nm} ({rdt})")
            if out:
                return out

    return _get_recent_disclosures_naver(stock_code, stock_name, max_items=max_items)

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
        except Exception as e:
            print(f"[WARN] 매크로 지표 수집 실패({name}/{ticker}): {e}")
    
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
                soup_rss = _parse_rss_soup(rss_res.text)
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
        print(f"[INFO] 뉴스 수집 품질: raw={len(candidates)} final={len(final_news)} sources={source_stats}")
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
        print(f"[WARN] 시황 뉴스 수집 실패: {e}")
        news_str = "- 뉴스 수집 실패"
        macro_recency_score = 50.0
        repeated_topics_text = ""
    
    return macro_str, news_str, macro_recency_score, repeated_topics_text

def run_scraper(manual_full_parse=False):
    print("🚀 수집기 봇 가동 시작 (V40.4 기관 수급 눌림목 최적화 & 백테스트 방어)...")
    migrate_csv_to_sqlite_once([
        ("data", "data.csv"),
        ("history", "history.csv"),
        ("score_trend", "score_trend.csv"),
        ("performance_trend", "performance_trend.csv"),
        ("theme_suggestions", "theme_suggestions.csv"),
        ("theme_quality_trend", "theme_quality_trend.csv"),
        ("portfolio", "portfolio.csv"),
        ("dart_map", "dart_map.csv"),
        ("theme_map", "theme_map.csv"),
    ])
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
    ref_date = now_kst.date() if is_eod_updated else (now_kst - timedelta(days=1)).date()
    # 주말에는 직전 영업일(금요일) 기준으로 캐시/수집 일자를 맞춤
    while ref_date.weekday() >= 5:
        ref_date = ref_date - timedelta(days=1)
    target_kis_date = ref_date.strftime("%Y%m%d")
    # 성과/트렌드 기록일은 실제 거래 기준일(ref_date)로 고정해 주말 실행 시 날짜 왜곡을 방지
    today_date = ref_date.strftime("%Y-%m-%d")

    already_fetched_kis = False
    if csv_exists("history.csv"):
        try:
            df_hist_check = read_table_prefer_db("history.csv")
            if not df_hist_check.empty and '일자' in df_hist_check.columns:
                latest_kis_date = str(df_hist_check['일자'].max()).replace("-", "")
                if latest_kis_date == target_kis_date:
                    already_fetched_kis = True
        except Exception as e:
            print(f"[WARN] history.csv 점검 실패: {e}")

    force_full_parse = False
    if csv_exists("data.csv"):
        df_check = read_table_prefer_db("data.csv")
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
    if already_fetched_kis and csv_exists("data.csv") and not force_full_parse:
        is_test_mode = True # KIS API를 스킵하는 테스트 워크플로우임을 확정
        print(f"⚡ [슈퍼 캐시 모드] 기준일({target_kis_date}) 수급 데이터 존재 확인. KIS API를 스킵합니다.")
        
        df_target = get_target_stock_list()
        df_final = read_table_prefer_db("data.csv")
        
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
            dip_buying_ratio = float(row_dict.get('음봉매집률', 0.0))

            quant_score = calculate_dynamic_score(
                f_str=row_dict.get('외인강도(%)', 0), p_str=row_dict.get('연기금강도(%)', 0),
                t_str=row_dict.get('투신강도(%)', 0), pef_str=row_dict.get('사모강도(%)', 0),
                vol_surge=row_dict.get('거래급증(%)', 0), rsi_val=row_dict.get('RSI', 50),
                gap_20=new_gap, foreign_streak=row_dict.get('외인연속', 0),
                pension_streak=row_dict.get('연기금연속', 0), 
                turnover_rate=row_dict.get('손바뀜(%)', 0), 
                is_ma20_rising=is_ma20_rising_flag, 
                per_val=row_dict.get('PER', 0), roe_val=row_dict.get('ROE', 0), 
                current_vix=current_vix,
                dip_buying_ratio=dip_buying_ratio
            )
            theme_name = resolve_theme_label(row_dict.get('종목코드', ''), row_dict.get('종목명', ''), row_dict.get('섹터', '분류안됨'))
            qual_score, qual_details = calculate_qualitative_score(
                sector_name=theme_name,
                per_val=row_dict.get('PER', 0),
                roe_val=row_dict.get('ROE', 0),
                foreign_streak=row_dict.get('외인연속', 0),
                pension_streak=row_dict.get('연기금연속', 0),
                macro_news_text=news_str_for_scoring,
                macro_recency_score=macro_recency_score,
                repeated_topics_text=repeated_topics_text,
                return_details=True
            )
            final_score, qual_adj, score_mode = blend_quant_qual_score(quant_score, qual_score, current_vix)
            row_dict['Quant점수'] = int(round(quant_score))
            row_dict['정성점수'] = round(qual_score, 2)
            row_dict['정성보정치'] = qual_adj
            row_dict['점수모드'] = score_mode
            row_dict['AI수급점수'] = final_score
            row_dict['테마'] = theme_name
            row_dict['뉴스테마가점'] = qual_details.get("theme_boost_applied", 0.0)
            row_dict['뉴스톤계수'] = qual_details.get("theme_tone_mult", 0.0)
            row_dict['뉴스부정키워드수'] = qual_details.get("negative_hits", 0)
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
            except Exception as e:
                print(f"[WARN] 섹터 수집 실패({name}/{code}): {e}")
            theme_name = resolve_theme_label(code, name, sector_name)

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

                # V41.1 눌림목 핵심 지표 계산(음봉 매집률)
                dip_buying_ratio = 0.0
                if len(closes) >= 20:
                    total_pt = 0.0
                    dip_pt = 0.0
                    for i in range(0, min(19, len(closes) - 1)):
                        close_today = closes[i]
                        close_prev = closes[i + 1]
                        p_amt_i = safe_api_float(daily_list[i].get('fund_ntby_qty')) * close_today
                        t_amt_i = safe_api_float(daily_list[i].get('ivtr_ntby_qty')) * close_today
                        pt_amt_i = p_amt_i + t_amt_i
                        total_pt += pt_amt_i
                        if close_today < close_prev:
                            dip_pt += pt_amt_i
                    if total_pt > 0:
                        dip_buying_ratio = max(0.0, min(1.0, dip_pt / total_pt))

                quant_score = calculate_dynamic_score(
                    f_str, p_str, t_str, pef_str, vol_surge, rsi_val, gap_20,
                    foreign_streak, pension_streak, turnover_rate, is_ma20_rising,
                    row.PER, row.ROE, current_vix,
                    dip_buying_ratio=dip_buying_ratio
                )
                qual_score, qual_details = calculate_qualitative_score(
                    sector_name=theme_name,
                    per_val=row.PER,
                    roe_val=row.ROE,
                    foreign_streak=foreign_streak,
                    pension_streak=pension_streak,
                    macro_news_text=news_str_for_scoring,
                    macro_recency_score=macro_recency_score,
                    repeated_topics_text=repeated_topics_text,
                    return_details=True
                )
                final_score, qual_adj, score_mode = blend_quant_qual_score(quant_score, qual_score, current_vix)

                data_list.append({
                    '종목명': name, '종목코드': code, '소속': row.소속, '섹터': sector_name, '테마': theme_name, 'AI수급점수': final_score,
                    'Quant점수': int(round(quant_score)), '정성점수': round(qual_score, 2), '정성보정치': qual_adj, '점수모드': score_mode,
                    '현재가': prpr, '등락률': row.등락률, '외인강도(%)': f_str, '연기금강도(%)': p_str, '투신강도(%)': t_str, '사모강도(%)': pef_str,
                    '외인연속': foreign_streak, '연기금연속': pension_streak, '이격도(%)': round(gap_20, 1), '손바뀜(%)': round(turnover_rate, 1),
                    'RSI': round(rsi_val, 1), '거래급증(%)': round(vol_surge, 1),
                    '추세상승': is_ma20_rising,
                    '음봉매집률': round(dip_buying_ratio, 4),
                    '시가총액': marcap, 'PER': row.PER, 'ROE': row.ROE,
                    '뉴스테마가점': qual_details.get("theme_boost_applied", 0.0),
                    '뉴스톤계수': qual_details.get("theme_tone_mult", 0.0),
                    '뉴스부정키워드수': qual_details.get("negative_hits", 0)
                })
            except Exception as e:
                print(f"[WARN] 종목 처리 실패({name}/{code}): {e}")
            time.sleep(0.2) 

        if not data_list: return

        df_final = pd.DataFrame(data_list).sort_values('AI수급점수', ascending=False)
        
        df_history = pd.DataFrame(history_list)
        if not df_history.empty:
            write_table_dual(df_history, "history.csv", index=False, encoding='utf-8-sig')
        else:
            # 빈 수집 결과로 기존 history.csv를 덮어써서 파일이 깨지는 상황 방지
            print("⚠️ history_list가 비어 있어 history.csv 갱신을 건너뜁니다. (기존 파일 유지)")

        eval_msg = ""
    
    # 상위 후보군 정성 심화(공시/리포트) 재보정
    df_final = apply_enhanced_qual_for_top_candidates(df_final, current_vix=current_vix, top_n=40)
    # 상위권 특정 테마 쏠림 완화
    df_final = apply_theme_crowding_penalty(df_final, top_n=40, crowd_ratio=0.3)
    # 테마 가점 품질 모니터링 + 과열 자동 완화
    df_final, theme_quality_metric = apply_theme_contribution_guard(
        df_final, today_date=today_date, current_vix=current_vix, top_n=40
    )
    # 눌림목 전용 미세 룰(과열 추격 억제 + 정상 눌림 우대)
    df_final = apply_pullback_trade_rules(df_final, current_vix=current_vix)
    # 일별 점수 변동성 완화(스윙 관점 안정화)
    df_final = apply_score_stability(df_final, today_date=today_date, max_daily_delta=8.0, smooth_alpha=0.7)
    # 신호 신뢰도 계층화(VIX 레짐별 임계치)
    df_final = add_signal_confidence(df_final, current_vix=current_vix)
    generate_theme_suggestions(df_final, today_date=today_date, top_n=40)
    write_table_dual(df_final, "data.csv", index=False, encoding='utf-8-sig')

    df_trend_new = df_final[['종목명', '종목코드', 'AI수급점수']].copy()
    df_trend_new['순위'] = df_trend_new['AI수급점수'].rank(method='min', ascending=False).astype(int)
    df_trend_new['날짜'] = today_date

    trend_file = "score_trend.csv"
    if csv_exists(trend_file):
        df_trend_old = load_score_trend_safe()
        df_trend_old = df_trend_old[df_trend_old['날짜'] != today_date]
        trend_concat = pd.concat([df_trend_old, df_trend_new], ignore_index=True)
        write_table_dual(trend_concat, trend_file, index=False, encoding='utf-8-sig')
    else:
        write_table_dual(df_trend_new, trend_file, index=False, encoding='utf-8-sig')

    # ==========================================
    # 백테스트 정산 로직
    # ==========================================
    portfolio_file = "portfolio.csv"
    perf_file = "performance_trend.csv"
    top3_names = df_final.head(3)['종목명'].tolist() 
    tq = theme_quality_metric if isinstance(theme_quality_metric, dict) else {}
    theme_quality_text = (
        f"- 평균테마가점: {float(tq.get('평균테마가점', 0.0)):.2f}\n"
        f"- P90테마가점: {float(tq.get('P90테마가점', 0.0)):.2f}\n"
        f"- 지배테마/비중: {tq.get('지배테마', '-')} / {float(tq.get('지배테마비중', 0.0))*100:.1f}%\n"
        f"- 완화강도/완화종목수: {float(tq.get('완화강도', 0.0)):.2f} / {int(tq.get('완화종목수', 0))}"
    )

    if os.path.exists(portfolio_file):
        try:
            df_port = safe_read_csv_with_conflict_guard(portfolio_file)
            if df_port.empty or not {"종목명", "매수가"}.issubset(df_port.columns):
                raise ValueError("portfolio.csv 형식 오류 또는 비어 있음")
            
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
                if csv_exists(perf_file):
                    df_perf = read_table_prefer_db(perf_file)
                    if not df_perf.empty:
                        df_perf = df_perf[df_perf['날짜'] != today_date] 
                        cum_ret = df_perf['누적수익률'].iloc[-1] + daily_ret if len(df_perf) > 0 else daily_ret
                    else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])
                else: df_perf = pd.DataFrame(columns=['날짜', '일간수익률', '누적수익률'])

                perf_concat = pd.concat(
                    [df_perf, pd.DataFrame([{'날짜': today_date, '일간수익률': daily_ret, '누적수익률': cum_ret}])],
                    ignore_index=True
                )
                # 누적수익률은 기존 값 신뢰 대신 일간수익률로 항상 재계산해
                # 중간 데이터 꼬임(누적 리셋) 시에도 자동 복구되도록 한다.
                perf_concat['일간수익률'] = pd.to_numeric(perf_concat['일간수익률'], errors='coerce').fillna(0.0)
                perf_concat['날짜_dt'] = pd.to_datetime(perf_concat['날짜'], errors='coerce')
                perf_concat = perf_concat.dropna(subset=['날짜_dt']).sort_values('날짜_dt')
                perf_concat = perf_concat.drop_duplicates(subset=['날짜'], keep='last')
                perf_concat['누적수익률'] = perf_concat['일간수익률'].cumsum().round(6)
                perf_concat = perf_concat.drop(columns=['날짜_dt'], errors='ignore')
                equity = 1.0 + (perf_concat['누적수익률'] / 100.0)
                rolling_peak = equity.cummax().replace(0, 1e-9)
                drawdown = ((equity / rolling_peak) - 1.0) * 100.0
                perf_concat['최대낙폭(%)'] = drawdown.round(2)
                perf_concat['리스크상태'] = perf_concat['최대낙폭(%)'].apply(
                    lambda x: "High" if x <= -8 else ("Medium" if x <= -4 else "Low")
                )
                write_table_dual(perf_concat, perf_file, index=False, encoding='utf-8-sig')

            if is_eod_updated:
                eval_msg += "📝 *[전일 추천 Top 3 최종 성적표]*\n" + "\n".join(eval_details) + f"\n➡️ *오늘 포트폴리오 최종 수익률: {daily_ret:+.2f}%*\n\n"
                
                if not is_test_mode:
                    top3_df = df_final.head(3)[['종목명', '현재가']].rename(columns={'현재가': '매수가'})
                    top3_df['날짜'] = today_date
                    top3_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
            else:
                eval_msg += "📝 *[현재 포트폴리오 장중 수익률]*\n" + "\n".join(eval_details) + f"\n➡️ *실시간 수익률: {daily_ret:+.2f}%*\n\n"
        except Exception as e:
            print(f"[WARN] 백테스트 정산 실패: {e}")
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
            if csv_exists("history.csv"):
                try:
                    df_history = read_table_prefer_db("history.csv")
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

            [5. 테마 가점 품질 모니터링]
            {theme_quality_text}

            다음 순서로 전문가 수준의 리포트를 작성해 줘.
            1. 글로벌 매크로 요약
            2. 섹터 및 수급 동향
            3. Top 3 관심종목 및 근거
            4. 공시/리포트 체크포인트
            5. 테마 과열 여부와 완화 상태
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

### Theme Quality Monitor
{theme_quality_text}
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

### Theme Quality Monitor
{theme_quality_text}
"""
        with open("report.md", "w", encoding="utf-8") as f:
            f.write(fallback_report)

    # 주간 1회 용량 리포트 누적 + 매 실행 콘솔 요약
    emit_weekly_storage_report()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QEdge scraper runner")
    parser.add_argument("--full-parse", action="store_true", help="슈퍼 캐시를 무시하고 KIS 풀 파싱을 강제 실행")
    args = parser.parse_args()
    run_scraper(manual_full_parse=args.full_parse)
