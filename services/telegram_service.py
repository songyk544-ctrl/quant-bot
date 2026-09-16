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


def _build_legacy_telegram_action_message(
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


def _market_action(snapshot, current_vix, candidate_count):
    returns = pd.to_numeric(snapshot.get("등락률", pd.Series(dtype=float)), errors="coerce").dropna()
    up_ratio = float((returns > 0).mean()) if not returns.empty else 0.5
    if {"현재가", "MA20"}.issubset(snapshot.columns):
        current = pd.to_numeric(snapshot["현재가"], errors="coerce")
        ma20 = pd.to_numeric(snapshot["MA20"], errors="coerce")
        valid = current.notna() & ma20.notna() & (ma20 > 0)
        ma20_ratio = float((current[valid] >= ma20[valid]).mean()) if valid.any() else 0.5
    elif "정배열" in snapshot.columns:
        ma20_ratio = float(snapshot["정배열"].fillna(False).astype(bool).mean())
    else:
        ma20_ratio = 0.5
    try:
        vix = float(current_vix)
    except Exception:
        vix = 20.0

    if vix >= 28.0 or up_ratio <= 0.32 or ma20_ratio <= 0.38:
        return {
            "label": "신규매수 중단",
            "positions": 0,
            "total_weight": 0,
            "position_weight": 0,
            "reason": "시장 위험 또는 시장 폭이 방어 기준에 해당합니다.",
            "up_ratio": up_ratio,
            "ma20_ratio": ma20_ratio,
        }
    if candidate_count <= 0 or vix >= 25.0 or up_ratio <= 0.43 or ma20_ratio <= 0.46:
        return {
            "label": "관찰",
            "positions": min(1, candidate_count),
            "total_weight": 20 if candidate_count else 0,
            "position_weight": 20 if candidate_count else 0,
            "reason": "시장 또는 후보 확인 조건이 충분하지 않습니다.",
            "up_ratio": up_ratio,
            "ma20_ratio": ma20_ratio,
        }
    if vix >= 22.0 or up_ratio <= 0.52 or ma20_ratio <= 0.52:
        return {
            "label": "선별 매수",
            "positions": min(2, candidate_count),
            "total_weight": 50,
            "position_weight": 30,
            "reason": "시장 전반보다 확인된 수급·진입 후보만 선별할 구간입니다.",
            "up_ratio": up_ratio,
            "ma20_ratio": ma20_ratio,
        }
    return {
        "label": "매수 가능",
        "positions": min(3, candidate_count),
        "total_weight": 90,
        "position_weight": 35,
        "reason": "시장 폭과 후보 조건이 모두 진입 기준을 충족합니다.",
        "up_ratio": up_ratio,
        "ma20_ratio": ma20_ratio,
    }


def _candidate_reason_lines(row):
    reasons = []
    core_actor = str(row.get("핵심수급주체", "")).strip()
    flow_score = pd.to_numeric(row.get("체급별수급점수"), errors="coerce")
    if core_actor and pd.notna(flow_score):
        reasons.append(f"{core_actor} 중심 수급 점수 {float(flow_score):.1f}")
    rel5 = pd.to_numeric(row.get("시장대비5일(%p)"), errors="coerce")
    rel20 = pd.to_numeric(row.get("시장대비20일(%p)"), errors="coerce")
    if pd.notna(rel5) or pd.notna(rel20):
        rel5_text = f"{float(rel5):+.1f}%p" if pd.notna(rel5) else "-"
        rel20_text = f"{float(rel20):+.1f}%p" if pd.notna(rel20) else "-"
        reasons.append(f"시장 대비 5일 {rel5_text} · 20일 {rel20_text}")
    setup = str(row.get("V5진입상태", row.get("진입유형", ""))).strip()
    if setup:
        reasons.append(f"진입 상태 {setup}")
    comment = str(row.get("진입코멘트", "")).strip()
    if comment and comment not in {"-", "nan", "추가 확인 필요"}:
        reasons.append(comment)
    return reasons[:3]


def build_telegram_action_message(
    df_final,
    now_kst,
    current_vix,
    regime,
    is_eod_updated,
    source_health=None,
):
    """모바일에서 빠르게 판단할 수 있는 수급 추천 중심 장마감 브리프."""
    session_label = "장마감" if is_eod_updated else "장중"
    snapshot = _current_snapshot(df_final)
    try:
        _, positions, _ = _build_core_book()
    except Exception as exc:
        print(f"[WARN] Telegram 보유종목 구성 실패: {exc}")
        positions = pd.DataFrame()

    candidates = pd.DataFrame()
    v5_enabled = os.environ.get("TELEGRAM_USE_V5", "").strip().lower() in {"1", "true", "yes"}
    uses_v5 = v5_enabled and not snapshot.empty and "V5추천상태" in snapshot.columns
    if uses_v5:
        candidates = snapshot[snapshot["V5추천상태"].astype(str).eq("추천")].copy()
    if not uses_v5 and candidates.empty and not snapshot.empty and "매수후보" in snapshot.columns:
        candidates = snapshot[snapshot["매수후보"].astype(str).eq("신규후보")].copy()

    if not positions.empty and "종목명" in positions.columns and not candidates.empty:
        held_names = set(positions["종목명"].dropna().astype(str))
        candidates = candidates[~candidates["종목명"].astype(str).isin(held_names)]
    score_cols = [c for c in (["V5종합점수"] if uses_v5 else ["스윙우선순위", "AI수급점수"]) if c in candidates.columns]
    if score_cols:
        for col in score_cols:
            candidates[col] = pd.to_numeric(candidates[col], errors="coerce").fillna(0.0)
        candidates = candidates.sort_values(score_cols, ascending=False)
    candidates = candidates.head(3)
    action = _market_action(snapshot, current_vix, len(candidates))

    lines = [
        f"AlphaPulse {session_label} 수급 브리프",
        now_kst.strftime("%Y-%m-%d %H:%M KST"),
        "",
        "[오늘의 판단]",
        action["label"],
        (
            f"최대 {action['positions']}종목 · 총 투자비중 {action['total_weight']}% 이내 · "
            f"종목당 최대 {action['position_weight']}%"
        ),
        action["reason"],
        "",
        "[오늘의 수급 추천]",
    ]
    if candidates.empty:
        lines.append("오늘 기준을 통과한 신규 후보가 없습니다.")
    else:
        if action["positions"] <= 0:
            lines.append("시장 방어 기준으로 아래 종목은 관찰만 하며 신규 진입하지 않습니다.")
        for rank, (_, row) in enumerate(candidates.iterrows(), start=1):
            setup = row.get("V5진입상태", row.get("진입유형", "관찰"))
            decision = f"관찰 · {setup}" if action["positions"] <= 0 else str(setup)
            lines.extend(["", f"{rank}위 {_name(row.get('종목명'))}", f"판정: {decision}", "추천 이유"])
            reasons = _candidate_reason_lines(row)
            if reasons:
                for reason_rank, reason in enumerate(reasons, start=1):
                    lines.append(f"{reason_rank}. {reason}")
            else:
                lines.append("1. 수급·추세 후보 기준 통과")
            check = str(row.get("매도점검", "보유/관찰"))
            if check not in {"", "보유/관찰", "nan"}:
                lines.append(f"주의: {check}")

    risk_positions = pd.DataFrame()
    if not positions.empty and "종목명" in positions.columns and not snapshot.empty:
        extra = [c for c in ["매도점검", "진입유형"] if c in snapshot.columns]
        enriched = positions.merge(snapshot[extra], left_on="종목명", right_index=True, how="left") if extra else positions.copy()
        checks = enriched.get("매도점검", pd.Series("보유/관찰", index=enriched.index)).fillna("보유/관찰").astype(str)
        risk_positions = enriched[checks.apply(lambda value: any(word in value for word in RISK_KEYWORDS))]
    if not risk_positions.empty:
        lines.extend(["", "[보유 위험 점검]"])
        for rank, (_, row) in enumerate(risk_positions.head(3).iterrows(), start=1):
            lines.append(f"{rank}. {_name(row.get('종목명'))} · {row.get('매도점검', '점검 필요')}")

    lines.extend([
        "",
        "[추가 시황]",
        f"시장 상태: {regime} · VIX {float(current_vix):.1f}",
        f"상승 종목 {action['up_ratio'] * 100:.0f}% · 정배열 {action['ma20_ratio'] * 100:.0f}%",
    ])
    _append_source_health(lines, source_health)
    lines.extend(["", f"대시보드: {DASHBOARD_URL}"])
    return "\n".join(lines)
