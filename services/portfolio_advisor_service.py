import pandas as pd


PORTFOLIO_BASE_COLS = ["종목명", "수량", "매수가"]
PORTFOLIO_JOIN_COLS = [
    "종목명", "현재가", "등락률", "스윙우선순위", "현재_순위", "AI수급점수", "AI순위",
    "매수후보", "진입유형", "매도점검", "신호등급", "신호신뢰도", "외인강도(%)", "연기금강도(%)",
]
SELL_WORDS = ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]


def enrich_portfolio_holdings(df_portfolio, df_summary, swing_warn_threshold=55, ai_critical_threshold=35):
    """보유 포트폴리오에 현재 스크리너 지표와 리스크 플래그를 붙입니다."""
    if df_portfolio is None or df_portfolio.empty:
        return pd.DataFrame(), {}
    df_summary = df_summary.copy() if df_summary is not None else pd.DataFrame()
    df_portfolio = df_portfolio.copy()

    join_cols = [c for c in PORTFOLIO_JOIN_COLS if c in df_summary.columns]
    if "종목명" not in join_cols:
        join_cols = ["종목명"]
        df_summary["종목명"] = []
    df_joined = pd.merge(
        df_portfolio,
        df_summary[join_cols].copy(),
        on="종목명",
        how="left",
    )

    f_strength = pd.to_numeric(df_joined.get("외인강도(%)", 0.0), errors="coerce").fillna(0.0)
    p_strength = pd.to_numeric(df_joined.get("연기금강도(%)", 0.0), errors="coerce").fillna(0.0)
    ai_score = pd.to_numeric(df_joined.get("AI수급점수", 0.0), errors="coerce").fillna(0.0)
    swing_score = pd.to_numeric(df_joined.get("스윙우선순위", 0.0), errors="coerce").fillna(0.0)
    qty_num = pd.to_numeric(df_joined.get("수량", 0.0), errors="coerce").fillna(0.0)
    buy_num = pd.to_numeric(df_joined.get("매수가", 0.0), errors="coerce").fillna(0.0)
    cur_num = pd.to_numeric(df_joined.get("현재가", 0.0), errors="coerce").fillna(0.0)

    flow_break = (f_strength < 0) & (p_strength < 0)
    swing_weak = swing_score < float(swing_warn_threshold)
    ai_break = ai_score < float(ai_critical_threshold)
    df_joined["수급동반약화"] = flow_break
    df_joined["스윙약화"] = swing_weak
    df_joined["AI급락"] = ai_break
    df_joined["수급이탈위험"] = (flow_break & swing_weak) | ai_break

    def _risk_reason(row):
        reasons = []
        if bool(row.get("AI급락", False)):
            reasons.append("AI수급 급락")
        if bool(row.get("수급동반약화", False)) and bool(row.get("스윙약화", False)):
            reasons.append("수급동반약화+스윙약화")
        if not reasons:
            return "-"
        return " / ".join(reasons)

    df_joined["리스크사유"] = df_joined.apply(_risk_reason, axis=1)
    sell_check_text = df_joined.get("매도점검", pd.Series([""] * len(df_joined))).fillna("").astype(str)
    sell_watch_mask = sell_check_text.apply(lambda x: any(w in x for w in SELL_WORDS))
    df_joined["매도점검위험"] = sell_watch_mask
    df_joined.loc[sell_watch_mask & df_joined["리스크사유"].eq("-"), "리스크사유"] = "매도점검"
    overlap = sell_watch_mask & ~df_joined["리스크사유"].eq("매도점검")
    df_joined.loc[overlap, "리스크사유"] = df_joined.loc[overlap, "리스크사유"] + " / 매도점검"

    total_buy_amount = float((qty_num * buy_num).sum())
    total_eval_amount = float((qty_num * cur_num).sum())
    total_profit_amount = total_eval_amount - total_buy_amount
    total_profit_pct = (total_profit_amount / total_buy_amount * 100.0) if total_buy_amount > 0 else 0.0
    summary = {
        "total_buy_amount": total_buy_amount,
        "total_eval_amount": total_eval_amount,
        "total_profit_amount": total_profit_amount,
        "total_profit_pct": total_profit_pct,
    }
    return df_joined, summary


def build_replacement_pool(df_summary, held_names, risk_rows, capital_guard_active=False):
    """약해진 보유 종목과 비교할 신규 교체 후보를 계산합니다."""
    if df_summary is None or df_summary.empty:
        return pd.DataFrame()
    replacement_pool = df_summary[~df_summary["종목명"].astype(str).isin(held_names)].copy()
    if "매수후보" in replacement_pool.columns:
        replacement_pool = replacement_pool[replacement_pool["매수후보"].astype(str).eq("신규후보")].copy()
    if "스윙우선순위" not in replacement_pool.columns:
        return replacement_pool.iloc[0:0].copy()
    replacement_pool["스윙우선순위"] = pd.to_numeric(replacement_pool["스윙우선순위"], errors="coerce").fillna(0.0)
    weakest_risk_score = 0.0
    if risk_rows is not None and not risk_rows.empty and "스윙우선순위" in risk_rows.columns:
        weakest_risk_score = float(pd.to_numeric(risk_rows["스윙우선순위"], errors="coerce").fillna(0.0).min())
    if capital_guard_active:
        replacement_pool = replacement_pool.iloc[0:0].copy()
    elif risk_rows is not None and not risk_rows.empty:
        replacement_pool = replacement_pool[replacement_pool["스윙우선순위"] >= weakest_risk_score + 5.0].copy()
    else:
        replacement_pool = replacement_pool.iloc[0:0].copy()
    return replacement_pool.sort_values("스윙우선순위", ascending=False).head(3)


def portfolio_assistant_verdict(risk_rows, replacement_pool, capital_guard_active=False, capital_emergency_active=False):
    if capital_emergency_active:
        return {
            "verdict": "신규매수 중지",
            "meta": "계좌 낙폭이 비상 구간입니다. 새 후보보다 손실 확대 차단과 현금 비중 회복이 우선입니다.",
            "color": "#FCA5A5",
        }
    if capital_guard_active:
        return {
            "verdict": "방어 운용",
            "meta": "계좌 낙폭이 방어 구간입니다. 신규매수는 막고 보유 종목 리스크만 점검합니다.",
            "color": "#FCD34D",
        }
    if risk_rows is None or risk_rows.empty:
        return {
            "verdict": "보유 유지",
            "meta": "현재 보유 종목에서 명확한 매도 점검 신호가 없습니다.",
            "color": "#86EFAC",
        }
    if replacement_pool is not None and not replacement_pool.empty:
        return {
            "verdict": "교체 점검",
            "meta": "약해진 보유 종목과 더 강한 신규 후보를 비교하세요.",
            "color": "#FCD34D",
        }
    return {
        "verdict": "비중 축소 점검",
        "meta": "새 후보보다 기존 리스크 관리가 우선입니다.",
        "color": "#FCA5A5",
    }
