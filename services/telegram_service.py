import os
import json
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
CORE_BOOK_START_DATE = "2026-04-27"
CORE_BOOK_PROFILE = "현재값"


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if requests is None:
        print("[WARN] requests 패키지가 없어 텔레그램 전송을 건너뜁니다.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace("**", "*")
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000]}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"[WARN] 텔레그램 전송 실패: {e}")


def _load_strategy_settings():
    try:
        if STRATEGY_SETTINGS_PATH.exists():
            with STRATEGY_SETTINGS_PATH.open("r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def build_telegram_action_message(df_final, now_kst, current_vix, regime, is_eod_updated):
    dashboard_url = "https://ge82mjcdoxngn3p6udv5sy.streamlit.app"
    session_label = "장마감" if is_eod_updated else "장중"
    lines = [
        f"🔔 AlphaPulse {session_label} 액션 브리프",
        f"🗓 {now_kst.strftime('%Y-%m-%d %H:%M')} KST",
        f"📊 VIX {current_vix:.2f} / {regime}",
        "",
    ]

    def _fmt_money(v):
        try:
            return f"{float(v):,.0f}원"
        except Exception:
            return "-"

    def _fmt_score(v):
        try:
            return f"{float(v):.1f}"
        except Exception:
            return "-"

    def _compact_name(v, limit=12):
        name = str(v or "-").strip()
        return name if len(name) <= limit else name[: limit - 1] + "…"

    def _append_core_book_section():
        try:
            if not csv_exists("swing_trades.csv") or not csv_exists("history.csv"):
                return
            settings = _load_strategy_settings()
            initial_cash = int(str(settings.get("initial_cash", 5_000_000)).replace(",", ""))
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
                return
            last = perf.iloc[-1]
            lines.append("📘 Alpha Core 운용 로그")
            lines.append(
                f"- 기준 {CORE_BOOK_START_DATE[5:].replace('-', '/')} · v1 · 평가 {_fmt_money(last.get('평가금액', initial_cash))} · 수익률 {_fmt_score(last.get('수익률(%)', 0.0))}%"
            )
            entry_logs = []
            if not positions.empty:
                for _, row in positions.iterrows():
                    entry_logs.append({
                        "진입일": row.get("진입일", "-"),
                        "종목명": row.get("종목명", "-"),
                        "수량": row.get("수량", 0),
                        "상태": "보유",
                    })
            if not closed.empty:
                for _, row in closed.iterrows():
                    entry_logs.append({
                        "진입일": row.get("진입일", "-"),
                        "종목명": row.get("종목명", "-"),
                        "수량": row.get("수량", 0),
                        "상태": "청산",
                    })
            if entry_logs:
                entry_view = pd.DataFrame(entry_logs)
                entry_view["진입일_dt"] = pd.to_datetime(entry_view["진입일"], errors="coerce")
                entry_view = entry_view.sort_values("진입일_dt", ascending=False)
                lines.append("- 최근 진입")
                for i, (_, row) in enumerate(entry_view.head(3).iterrows(), start=1):
                    lines.append(
                        f"  {i}. {_compact_name(row.get('종목명'))} · {row.get('진입일','-')} · {int(float(row.get('수량', 0) or 0))}주 · {row.get('상태','-')}"
                    )
            if positions.empty:
                lines.append("- 현재 보유: 없음")
            else:
                pos_view = positions.copy()
                if "평가수익률" in pos_view.columns:
                    pos_view["평가수익률"] = pd.to_numeric(pos_view["평가수익률"], errors="coerce").fillna(0.0)
                    pos_view = pos_view.sort_values("평가수익률", ascending=False)
                lines.append("- 현재 보유")
                for i, (_, row) in enumerate(pos_view.head(3).iterrows(), start=1):
                    lines.append(
                        f"  {i}. {_compact_name(row.get('종목명'))} · {int(float(row.get('수량', 0) or 0))}주 · {_fmt_score(row.get('평가수익률', 0.0))}%"
                    )
            if not closed.empty:
                closed_view = closed.copy()
                if "청산일" in closed_view.columns:
                    closed_view["청산일_dt"] = pd.to_datetime(closed_view["청산일"], errors="coerce")
                    closed_view = closed_view.sort_values("청산일_dt", ascending=False)
                lines.append("- 최근 청산")
                for i, (_, row) in enumerate(closed_view.head(3).iterrows(), start=1):
                    lines.append(
                        f"  {i}. {_compact_name(row.get('종목명'))} · {row.get('청산일','-')} · {_fmt_score(row.get('수익률', 0.0))}% · {row.get('청산사유','-')}"
                    )
            lines.append("")
        except Exception as e:
            print(f"[WARN] 텔레그램 기준 포트폴리오 로그 구성 실패: {e}")

    df = df_final.copy() if df_final is not None else pd.DataFrame()
    if not df.empty:
        for c in ["스윙우선순위", "AI수급점수", "주도주점수", "수급품질점수", "현재가"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    open_view = pd.DataFrame()
    try:
        if csv_exists("swing_trades.csv"):
            trades = read_table_prefer_db("swing_trades.csv", on_bad_lines="skip")
            if not trades.empty and {"청산방식", "상태", "종목명", "진입일"}.issubset(trades.columns):
                signal = trades[
                    trades["청산방식"].astype(str).eq("시그널")
                    & trades["상태"].astype(str).str.lower().eq("open")
                ].copy()
                if not signal.empty:
                    signal["진입일_dt"] = pd.to_datetime(signal["진입일"], errors="coerce")
                    signal = signal.sort_values(["종목명", "진입일_dt"]).drop_duplicates("종목명", keep="last")
                    merge_cols = [
                        c
                        for c in ["종목명", "매수후보", "진입유형", "전략슬리브", "스윙우선순위", "매도점검", "현재가"]
                        if c in df.columns
                    ]
                    open_view = pd.merge(signal, df[merge_cols], on="종목명", how="left") if merge_cols else signal
                    for base_col in ["매수후보", "진입유형", "전략슬리브", "스윙우선순위", "매도점검", "현재가"]:
                        x_col = f"{base_col}_x"
                        y_col = f"{base_col}_y"
                        if y_col in open_view.columns:
                            open_view[base_col] = (
                                open_view[y_col].combine_first(open_view[x_col])
                                if x_col in open_view.columns
                                else open_view[y_col]
                            )
                        elif x_col in open_view.columns:
                            open_view[base_col] = open_view[x_col]
    except Exception as e:
        print(f"[WARN] 텔레그램 보유 포지션 구성 실패: {e}")

    risk_keywords = ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]
    if not open_view.empty:
        open_view["매도점검"] = open_view.get("매도점검", "보유/관찰").fillna("보유/관찰").astype(str)
        sell_alerts = open_view[open_view["매도점검"].apply(lambda x: any(k in x for k in risk_keywords))].copy()
        hold_view = open_view.drop(sell_alerts.index, errors="ignore").copy()

        lines.append("🚨 매도/축소 점검")
        if sell_alerts.empty:
            lines.append("- 없음")
        else:
            for i, (_, row) in enumerate(sell_alerts.head(3).iterrows(), start=1):
                lines.extend(
                    [
                        f"{i}. {_compact_name(row.get('종목명'))}",
                        f"   점검: {row.get('매도점검','-')}",
                        f"   진입 {row.get('진입일','-')} · 현재 {_fmt_money(row.get('현재가', 0))}",
                    ]
                )
        lines.append("")

        lines.append("✅ 보유 유지")
        if hold_view.empty:
            lines.append("- 없음")
        else:
            if "스윙우선순위" not in hold_view.columns:
                hold_view["스윙우선순위"] = 0.0
            hold_view["스윙우선순위"] = pd.to_numeric(hold_view["스윙우선순위"], errors="coerce").fillna(0.0)
            hold_view = hold_view.sort_values("스윙우선순위", ascending=False)
            for i, (_, row) in enumerate(hold_view.head(3).iterrows(), start=1):
                lines.extend(
                    [
                        f"{i}. {_compact_name(row.get('종목명'))}",
                        f"   {row.get('진입유형','-')} · 스윙 {_fmt_score(row.get('스윙우선순위', 0.0))}",
                        f"   점검: {row.get('매도점검','보유/관찰')}",
                    ]
                )
        lines.append("")
    else:
        lines.append("✅ 보유 포지션")
        lines.append("- 진행 중인 시그널 포지션 없음")
        lines.append("")

    top_candidate = None
    if df.empty or "매수후보" not in df.columns:
        candidates = pd.DataFrame()
    else:
        candidates = df[df["매수후보"].astype(str).eq("신규후보")].copy()
        if candidates.empty:
            candidates = pd.DataFrame()
        else:
            candidates = candidates.sort_values(["스윙우선순위", "AI수급점수"], ascending=[False, False])
            top_candidate = candidates.iloc[0]

    lines.append("🎯 오늘 1순위")
    if top_candidate is None:
        lines.append("- 신규 매수 후보 없음")
    else:
        lines.extend(
            [
                f"1. {_compact_name(top_candidate.get('종목명'))}",
                f"   {top_candidate.get('전략슬리브','-')} · {top_candidate.get('진입유형','-')}",
                f"   스윙 {_fmt_score(top_candidate.get('스윙우선순위', 0.0))} · 현재 {_fmt_money(top_candidate.get('현재가', 0))}",
            ]
        )
    lines.append("")

    lines.append("🆕 신규 후보군")
    if candidates.empty:
        lines.append("- 없음")
    else:
        for i, (_, row) in enumerate(candidates.head(3).iterrows(), start=1):
            lines.extend(
                [
                    f"{i}. {_compact_name(row.get('종목명'))}",
                    f"   {row.get('전략슬리브','-')} · {row.get('진입유형','-')}",
                    f"   스윙 {_fmt_score(row.get('스윙우선순위', 0.0))} · 현재 {_fmt_money(row.get('현재가', 0))}",
                ]
            )

    lines.append("")
    _append_core_book_section()

    lines.extend(["", f"📊 대시보드: {dashboard_url}"])
    return "\n".join(lines)
