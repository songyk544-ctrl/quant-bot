import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import time
import tomllib

import requests


URL_BASE = "https://openapi.koreainvestment.com:9443"


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


def get_kis_access_token():
    kis_app_key, kis_app_secret = resolve_kis_credentials()
    if not kis_app_key or not kis_app_secret:
        return None
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": kis_app_key, "appsecret": kis_app_secret}
    try:
        res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
        return res.json().get("access_token")
    except Exception:
        return None


def safe_api_float(val):
    try:
        return float(val) if val else 0.0
    except Exception:
        return 0.0


def history_row_from_kis_daily(name, daily):
    close_prc = safe_api_float(daily.get("stck_clpr"))
    vol = safe_api_float(daily.get("acml_vol"))
    trade_value = vol * close_prc
    f_amt = safe_api_float(daily.get("frgn_ntby_qty")) * close_prc
    p_amt = safe_api_float(daily.get("fund_ntby_qty")) * close_prc
    t_amt = safe_api_float(daily.get("ivtr_ntby_qty")) * close_prc
    pef_amt = safe_api_float(daily.get("pe_fund_ntby_vol")) * close_prc
    return {
        "종목명": name,
        "일자": daily.get("stck_bsop_date", ""),
        "종가": close_prc,
        "외인": f_amt / 1_000_000,
        "연기금": p_amt / 1_000_000,
        "투신": t_amt / 1_000_000,
        "사모": pef_amt / 1_000_000,
        "거래량": vol,
        "거래대금(억)": trade_value / 100_000_000,
    }


def previous_kis_cursor_date(oldest_raw_date):
    try:
        cursor = datetime.strptime(str(oldest_raw_date), "%Y%m%d").date() - timedelta(days=1)
    except Exception:
        return None
    while cursor.weekday() >= 5:
        cursor = cursor - timedelta(days=1)
    return cursor.strftime("%Y%m%d")


def collect_kis_history_backfill(name, code, url_kis, headers, input_date, cutoff, max_pages):
    rows = []
    seen_dates = set()
    cursor_date = input_date
    empty_pages = 0
    failed_pages = 0

    for _ in range(1, max_pages + 1):
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": cursor_date,
            "FID_ORG_ADJ_PRC": "0",
            "FID_ETC_CLS_CODE": "0",
        }
        res = requests.get(url_kis, headers=headers, params=params, timeout=8)
        payload = res.json() if res.text else {}
        if res.status_code != 200 or payload.get("rt_cd") != "0":
            failed_pages += 1
            if failed_pages <= 2:
                print(
                    f"[WARN] super-parse 응답 실패({name}/{code}): "
                    f"status={res.status_code} rt_cd={payload.get('rt_cd')} msg={payload.get('msg1')}"
                )
            break

        daily_rows = payload.get("output2", [])
        if not daily_rows:
            empty_pages += 1
            break

        page_dates = []
        added_on_page = 0
        for daily in daily_rows:
            raw_date = str(daily.get("stck_bsop_date", ""))
            if not raw_date:
                continue
            try:
                day = datetime.strptime(raw_date, "%Y%m%d").date()
            except Exception:
                continue
            page_dates.append(raw_date)
            if day >= cutoff and raw_date not in seen_dates:
                rows.append(history_row_from_kis_daily(name, daily))
                seen_dates.add(raw_date)
                added_on_page += 1

        if not page_dates:
            empty_pages += 1
            break

        oldest_raw_date = min(page_dates)
        oldest_day = datetime.strptime(oldest_raw_date, "%Y%m%d").date()
        if oldest_day <= cutoff:
            break

        next_cursor_date = previous_kis_cursor_date(oldest_raw_date)
        if not next_cursor_date or next_cursor_date >= cursor_date or added_on_page == 0:
            break
        cursor_date = next_cursor_date
        time.sleep(0.03)

    return rows, empty_pages, failed_pages, len(seen_dates)
