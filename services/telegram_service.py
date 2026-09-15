from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

try:
    import requests
except Exception:
    requests = None

from db_utils import csv_exists
from repositories.data_repository import read_table_prefer_db
from services.portfolio_simulator_service import build_capital_limited_swing_sim


TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
STRATEGY_SETTINGS_PATH = Path("data") / "strategy_settings.json"
USER_STATE_PATH = Path("user_state_admin.json")
CORE_BOOK_START_DATE = "2026-04-27"
CORE_BOOK_PROFILE = "현재값"
DASHBOARD_URL = "https://ge82mjcdoxngn3p6udv5sy.streamlit.app"
RISK_KEYWORDS = ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[INFO] Telegram 환경변수가 없어 메시지 전송을 건너뜁니다.")
        return False
    if requests is None:
        print("[WARN] requests 패키지가 없어 Telegram 전송을 건너뜁니다.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": str(text or "")[:4000],
        "disable_web_page_preview": True,
    }
    try:
        response = requests.post(url, data=data, timeout=10)
        if not response.ok:
            print(f"[WARN] Telegram 전송 실패: HTTP {response.status_code}")
            return False
        return True
    except Exception as exc:
        print(f"[WARN] Telegram 전송 실패: {exc}")
        return False


def _load_strategy_settings():
    merged = {}
    try:
        if STRATEGY_SETTINGS_PATH.exists():
            with STRATEGY_SETTINGS_PATH.open("r", encoding="utf-8") as file:
                merged.update(json.load(file) or {})
    except Exception:
        pass
    try:
        if USER_STATE_PATH.exists():
            with USER_STATE_PATH.open("r", encoding="utf-8") as file:
                user_state = json.load(file) or {}
            merged.update(user_state.get("strategy_settings", {}) or {})
    except Exception:
        pass
    return merged


def _money(value):
    try:
        return f"{float(value):,.0f}원"
    except Exception:
        return "-"


def _number(value, digits=1, suffix=""):
    try:
        return f"{float(value):+.{digits}f}{suffix}"
    except Exception:
        return "-"


def _score(value):
    try:
        return f"{float(value):.1f}"
    except Exception:
        return "-"


def _name(value, limit=14):
    text = str(value or "-").strip()
    return text if len(text) <= limit else f"{text[: limit - 1]}…"


def _current_snapshot(df_final):
    frame = df_final.copy() if df_final is not None else pd.DataFrame()
    if frame.empty or "종목명" not in frame.columns:
        return pd.DataFrame()
    frame = frame.drop_duplicates("종목명", keep="first")
    return frame.set_index("종목명", drop=False)


def _build_core_book():
    if not csv_exists("swing_trades.csv") or not csv_exists("history.csv"):
        return {}, pd.DataFrame(), pd.DataFrame()
    settings = _load_strategy_settings()
    try:
        initial_cash = int(str(settings.get("initial_cash", 5_000_000)).replace(",", ""))
    except Exception:
        initial_cash = 5_000_000
    trades = read_table_prefer_db("swing_trades.csv", on_bad_lines="skip")
    history = read_table_prefer_db("history.csv", on_bad_lines="skip")
    perf, positions, closed = build_capital_limited_swing_sim(
        trades,
        history,
        initial_cash=initial_cash,
        max_positions=3,
        start_date=CORE_BOOK_START_DATE,
        score_mode="adaptive",
        adaptive_profile=CORE_BOOK_PROFILE,
    )
    if perf.empty:
        return {}, positions, closed
    last = perf.iloc[-1]
    summary = {
        "initial_cash": initial_cash,
        "equity": last.get("평가금액", initial_cash),
        "return_pct": last.get("수익률(%)", 0.0),
        "cash": last.get("현금", 0.0),
    }
    return summary, positions, closed


def _append_source_health(lines, source_health):
    health = source_health or {}
    status = str(health.get("상태", ""))
    if status == "ok":
        lines.append(
            f"데이터: KIS 공식 종목 마스터 · {int(float(health.get('종목수', 0) or 0)):,}종목"
        )
        return
    if status.startswith("fallback"):
        count = int(float(health.get("연속실패거래일", 1) or 1))
        lines.extend(
            [
                "[데이터 수집 경보]",
                f"공식 종목군 수집 실패 · 캐시 사용 {count}거래일째",
                f"대상 {int(float(health.get('종목수', 0) or 0)):,}종목",
                "신규 상장 및 시가총액 변동이 누락될 수 있습니다.",
            ]
        )


def build_telegram_action_message(
    df_final,
    now_kst,
    current_vix,
    regime,
    is_eod_updated,
    source_health=None,
):
    session_label = "장마감" if is_eod_updated else "장중"
    lines = [
        f"AlphaPulse {session_label} 운용 브리핑",
        now_kst.strftime("%Y-%m-%d %H:%M KST"),
        f"시장: VIX {float(current_vix):.1f} · {regime}",
        "",
    ]
    snapshot = _current_snapshot(df_final)

    try:
        core, positions, closed = _build_core_book()
    except Exception as exc:
        print(f"[WARN] Telegram 기준 포트폴리오 구성 실패: {exc}")
        core, positions, closed = {}, pd.DataFrame(), pd.DataFrame()

    if core:
        lines.extend(
            [
                "[Alpha Core]",
                f"기준 2026-04-27 · 공격/방어 v1",
                f"평가 {_money(core.get('equity'))} · 수익률 {_number(core.get('return_pct'), 1, '%')}",
                f"현금 {_money(core.get('cash'))}",
                "",
            ]
        )

    enriched = positions.copy()
    if not enriched.empty and "종목명" in enriched.columns and not snapshot.empty:
        extra_cols = [
            c for c in ["매도점검", "진입유형", "스윙우선순위", "매수후보"]
            if c in snapshot.columns
        ]
        if extra_cols:
            enriched = enriched.merge(
                snapshot[extra_cols],
                left_on="종목명",
                right_index=True,
                how="left",
            )
    if not enriched.empty:
        enriched["매도점검"] = enriched.get(
            "매도점검", pd.Series("보유/관찰", index=enriched.index)
        ).fillna("보유/관찰").astype(str)
        risk_mask = enriched["매도점검"].apply(
            lambda value: any(word in value for word in RISK_KEYWORDS)
        )
        risk_positions = enriched[risk_mask]
        hold_positions = enriched[~risk_mask]
    else:
        risk_positions = pd.DataFrame()
        hold_positions = pd.DataFrame()

    lines.append("[오늘 할 일]")
    if risk_positions.empty:
        lines.append("매도·축소 신호 없음")
    else:
        for index, (_, row) in enumerate(risk_positions.head(3).iterrows(), start=1):
            lines.append(f"{index}. {_name(row.get('종목명'))} · {row.get('매도점검', '-')}")
            lines.append(
                f"   평가 {_number(row.get('평가수익률', 0.0), 1, '%')} · "
                f"{int(float(row.get('수량', 0) or 0))}주"
            )
    lines.append("")

    lines.append("[현재 보유]")
    if positions.empty:
        lines.append("보유 종목 없음 · 현금 대기")
    else:
        ordered = pd.concat([risk_positions, hold_positions], ignore_index=True)
        for index, (_, row) in enumerate(ordered.head(3).iterrows(), start=1):
            lines.append(
                f"{index}. {_name(row.get('종목명'))} · "
                f"{_number(row.get('평가수익률', 0.0), 1, '%')} · "
                f"{int(float(row.get('수량', 0) or 0))}주"
            )
            lines.append(f"   점검: {row.get('매도점검', '보유/관찰')}")
    lines.append("")

    candidates = pd.DataFrame()
    if not snapshot.empty and "매수후보" in snapshot.columns:
        candidates = snapshot[
            snapshot["매수후보"].astype(str).eq("신규후보")
        ].copy()
        if not positions.empty and "종목명" in positions.columns:
            held_names = set(positions["종목명"].dropna().astype(str))
            candidates = candidates[~candidates["종목명"].astype(str).isin(held_names)]
        sort_cols = [
            c for c in ["스윙우선순위", "AI수급점수"] if c in candidates.columns
        ]
        if sort_cols:
            for col in sort_cols:
                candidates[col] = pd.to_numeric(candidates[col], errors="coerce").fillna(0)
            candidates = candidates.sort_values(sort_cols, ascending=False)

    lines.append("[신규 후보]")
    if candidates.empty:
        lines.append("오늘 신규 매수 후보 없음")
    else:
        for index, (_, row) in enumerate(candidates.head(3).iterrows(), start=1):
            lines.append(
                f"{index}. {_name(row.get('종목명'))} · "
                f"{row.get('진입유형', '-')} · 스윙 {_score(row.get('스윙우선순위'))}"
            )
            lines.append(f"   현재 {_money(row.get('현재가'))}")

    if not closed.empty and "청산일" in closed.columns:
        recent = closed.copy()
        recent["청산일_dt"] = pd.to_datetime(recent["청산일"], errors="coerce")
        recent = recent.dropna(subset=["청산일_dt"]).sort_values("청산일_dt", ascending=False)
        if not recent.empty:
            latest_date = recent["청산일_dt"].max()
            recent = recent[recent["청산일_dt"].eq(latest_date)].head(2)
            lines.extend(["", "[최근 청산]"])
            for _, row in recent.iterrows():
                lines.append(
                    f"{_name(row.get('종목명'))} · {_number(row.get('수익률'), 1, '%')} · "
                    f"{row.get('청산사유', '-')}"
                )

    lines.append("")
    _append_source_health(lines, source_health)
    lines.extend(["", f"대시보드: {DASHBOARD_URL}"])
    return "\n".join(lines)
