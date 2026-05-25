import os

import pandas as pd
import requests

from db_utils import csv_exists
from repositories.data_repository import read_table_prefer_db


TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    clean_text = text.replace("**", "*")
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": clean_text[:4000]}
    try:
        requests.post(url, data=data, timeout=10)
    except Exception as e:
        print(f"[WARN] 텔레그램 전송 실패: {e}")


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

    lines.extend(["", f"📊 대시보드: {dashboard_url}"])
    return "\n".join(lines)
