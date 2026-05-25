import pandas as pd

from services.scoring_service import (
    ADAPTIVE_THRESHOLD_PROFILES,
    build_market_state_features,
    choose_adaptive_target_positions,
    passes_relative_strength_filter,
)

PERF_COLS = ["날짜", "평가금액", "현금", "투자금액", "수익률(%)", "일간수익률", "보유종목수", "실현손익"]
POSITION_COLS = ["종목명", "진입일", "진입가", "수량", "매수금액", "현재가", "평가금액", "평가손익", "평가수익률", "보유일수", "상태"]
CLOSED_COLS = ["진입일", "청산일", "종목명", "보유일수", "수량", "진입가", "청산가", "매수금액", "청산금액", "실현손익", "수익률", "청산사유"]


def empty_capital_limited_result():
    return (
        pd.DataFrame(columns=PERF_COLS),
        pd.DataFrame(columns=POSITION_COLS),
        pd.DataFrame(columns=CLOSED_COLS),
    )


def build_capital_limited_swing_sim(df_trades, df_history, initial_cash=5_000_000, max_positions=3, start_date=None, score_mode="swing", adaptive_profile="현재값"):
    """초기자금과 동시보유 제한을 둔 실제 포트폴리오형 스윙 시뮬레이션."""
    if df_trades.empty or df_history.empty:
        return empty_capital_limited_result()

    trades = df_trades.copy()
    if "진입일_dt" not in trades.columns and "진입일" in trades.columns:
        trades["진입일_dt"] = pd.to_datetime(trades["진입일"], errors="coerce")
    if "청산일_dt" not in trades.columns and "청산일" in trades.columns:
        trades["청산일_dt"] = pd.to_datetime(trades["청산일"], errors="coerce")

    hist = df_history.copy()
    if not {"종목명", "일자", "종가"}.issubset(hist.columns):
        return empty_capital_limited_result()
    raw_dates = hist["일자"].astype(str).str.replace("-", "", regex=False).str.strip()
    hist["일자_dt"] = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce")
    if hist["일자_dt"].notna().sum() == 0:
        hist["일자_dt"] = pd.to_datetime(hist["일자"], errors="coerce")
    hist["종가"] = pd.to_numeric(hist["종가"], errors="coerce")
    hist = hist.dropna(subset=["일자_dt", "종목명", "종가"]).sort_values(["종목명", "일자_dt"])
    if hist.empty:
        return empty_capital_limited_result()

    prices = {str(name): grp.set_index("일자_dt")["종가"].sort_index() for name, grp in hist.groupby("종목명")}
    dates = sorted(hist["일자_dt"].dt.normalize().unique())
    if start_date is not None:
        sim_start = pd.to_datetime(start_date).normalize()
        dates = [d for d in dates if pd.to_datetime(d).normalize() >= sim_start]
    if not dates:
        return empty_capital_limited_result()

    signal = trades[trades["청산방식"].astype(str).eq("시그널")].copy()
    if signal.empty:
        signal = trades[trades["보유일수"].eq(10)].copy()
    score_mode_key = str(score_mode).lower()
    adaptive_mode = score_mode_key in {"adaptive", "attack_defense", "dynamic"}
    adaptive_rules = ADAPTIVE_THRESHOLD_PROFILES.get(str(adaptive_profile), ADAPTIVE_THRESHOLD_PROFILES["현재값"])
    score_col = "AI수급점수" if score_mode_key == "ai" else "스윙우선순위"
    fallback_score_col = "스윙우선순위" if score_col == "AI수급점수" else "AI수급점수"
    if score_col not in signal.columns and fallback_score_col in signal.columns:
        score_col = fallback_score_col
    if score_col not in signal.columns:
        signal[score_col] = 0.0
    signal[score_col] = pd.to_numeric(signal[score_col], errors="coerce").fillna(0.0)
    signal = signal.dropna(subset=["진입일_dt"]).sort_values(
        ["진입일_dt", score_col, "진입순위"],
        ascending=[True, False, True],
    )

    _, market_state, entry_features = build_market_state_features(hist)
    relative_strength_mode = str(adaptive_profile) == "v3 상대강도"

    def _target_positions_for_day(cur_date, todays):
        if not adaptive_mode:
            return int(max_positions), "고정"
        state = market_state.loc[cur_date] if cur_date in market_state.index else None
        return choose_adaptive_target_positions(cur_date, todays, score_col, max_positions, adaptive_rules, market_state_row=state)

    cash = float(initial_cash)
    positions = []
    closed_rows = []
    perf_rows = []
    prev_equity = float(initial_cash)

    def _last_price(name, cur_date, fallback):
        px = prices.get(name)
        if px is not None and not px.loc[px.index <= cur_date].empty:
            return float(px.loc[px.index <= cur_date].iloc[-1])
        return float(fallback)

    def _holding_days(entry_date, cur_date, minimum=1):
        return max(minimum, len([d for d in dates if pd.to_datetime(entry_date).normalize() <= pd.to_datetime(d).normalize() <= cur_date]) - 1)

    def _mark_positions_value(cur_date):
        total = 0.0
        for pos in positions:
            mark_price = _last_price(pos["종목명"], cur_date, pos["진입가"])
            total += mark_price * pos["수량"]
        return total

    def _passes_relative_strength_filter(sig, cur_date, market_mode):
        if not relative_strength_mode:
            return True
        return passes_relative_strength_filter(sig, cur_date, market_mode, entry_features, market_state, score_col)

    for cur_date in dates:
        cur_date = pd.to_datetime(cur_date).normalize()
        realized_today = 0.0
        switched_today = False

        remaining = []
        for pos in positions:
            exit_date = pd.to_datetime(pos["청산일_dt"]).normalize() if pd.notna(pos.get("청산일_dt")) else None
            should_exit = exit_date is not None and exit_date <= cur_date and str(pos.get("상태", "")).lower() == "closed"
            if should_exit:
                exit_price = _last_price(pos["종목명"], cur_date, pos["진입가"])
                exit_value = exit_price * pos["수량"]
                pnl = exit_value - pos["매수금액"]
                cash += exit_value
                realized_today += pnl
                closed_rows.append({
                    "진입일": pos["진입일"],
                    "청산일": cur_date.strftime("%Y-%m-%d"),
                    "종목명": pos["종목명"],
                    "보유일수": _holding_days(pos["진입일_dt"], cur_date),
                    "수량": pos["수량"],
                    "진입가": round(pos["진입가"], 0),
                    "청산가": round(exit_price, 0),
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
        if not todays.empty:
            todays[score_col] = pd.to_numeric(todays[score_col], errors="coerce").fillna(0.0)
            todays = todays.sort_values([score_col, "진입순위"], ascending=[False, True])
        target_positions, market_mode = _target_positions_for_day(cur_date, todays)
        held_names = {p["종목명"] for p in positions}
        for _, sig in todays.iterrows():
            if target_positions <= 0:
                break
            name = str(sig.get("종목명", "")).strip()
            if not name or name in held_names:
                continue
            entry_price = float(pd.to_numeric(sig.get("진입가"), errors="coerce") or 0.0)
            if entry_price <= 0:
                continue
            if not _passes_relative_strength_filter(sig, cur_date, market_mode):
                continue
            new_score = float(pd.to_numeric(sig.get(score_col, 0.0), errors="coerce") or 0.0)
            swing_score = float(pd.to_numeric(sig.get("스윙우선순위", 0.0), errors="coerce") or 0.0)
            new_entry_type = str(sig.get("진입유형", ""))
            if len(positions) >= int(target_positions):
                if switched_today:
                    break
                weakest_idx = None
                weakest_score = 0.0
                weakest_ret = 0.0
                for pos_idx, pos in enumerate(positions):
                    mark_price = _last_price(pos["종목명"], cur_date, pos["진입가"])
                    pos_ret = ((mark_price - float(pos["진입가"])) / float(pos["진입가"]) * 100.0) if float(pos["진입가"]) > 0 else 0.0
                    pos_score = float(pos.get("전략점수", pos.get("스윙우선순위", 0.0)) or 0.0)
                    if weakest_idx is None or (pos_ret, pos_score) < (weakest_ret, weakest_score):
                        weakest_idx = pos_idx
                        weakest_score = pos_score
                        weakest_ret = pos_ret
                new_is_strong = new_score >= 65.0 or ("주도" in new_entry_type and new_score >= 58.0)
                old_is_weak = weakest_ret <= -3.5 and new_score >= weakest_score + 12.0
                protect_winner = weakest_ret >= 0.0
                if weakest_idx is None or not (new_is_strong and old_is_weak) or protect_winner:
                    continue
                old_pos = positions.pop(weakest_idx)
                exit_price = _last_price(old_pos["종목명"], cur_date, old_pos["진입가"])
                exit_value = exit_price * old_pos["수량"]
                pnl = exit_value - old_pos["매수금액"]
                cash += exit_value
                realized_today += pnl
                held_names.discard(old_pos["종목명"])
                closed_rows.append({
                    "진입일": old_pos["진입일"],
                    "청산일": cur_date.strftime("%Y-%m-%d"),
                    "종목명": old_pos["종목명"],
                    "보유일수": _holding_days(old_pos["진입일_dt"], cur_date),
                    "수량": old_pos["수량"],
                    "진입가": round(old_pos["진입가"], 0),
                    "청산가": round(exit_price, 0),
                    "매수금액": round(old_pos["매수금액"], 0),
                    "청산금액": round(exit_value, 0),
                    "실현손익": round(pnl, 0),
                    "수익률": round((pnl / old_pos["매수금액"]) * 100.0, 4) if old_pos["매수금액"] else 0.0,
                    "청산사유": "더강한후보교체",
                })
                switched_today = True

            current_equity_for_buy = cash + _mark_positions_value(cur_date)
            dynamic_slot_cash = current_equity_for_buy / max(1, int(target_positions))
            budget = min(dynamic_slot_cash, cash)
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
                "스윙우선순위": swing_score,
                "전략점수": new_score,
                "진입유형": new_entry_type,
            })

        invested_value = _mark_positions_value(cur_date)
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
            "목표보유종목수": int(target_positions),
            "시장모드": market_mode,
            "실현손익": round(realized_today, 0),
        })
        prev_equity = equity

    latest_date = pd.to_datetime(dates[-1]).normalize()
    pos_rows = []
    for pos in positions:
        cur_price = _last_price(pos["종목명"], latest_date, pos["진입가"])
        value = cur_price * pos["수량"]
        pnl = value - pos["매수금액"]
        hold_days = max(0, _holding_days(pos["진입일_dt"], latest_date, minimum=0))
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
    return pd.DataFrame(perf_rows), pd.DataFrame(pos_rows, columns=POSITION_COLS), pd.DataFrame(closed_rows, columns=CLOSED_COLS)


def compute_trade_quality_metrics(closed_trades):
    trades = closed_trades.copy() if closed_trades is not None else pd.DataFrame()
    if "수익률" not in trades.columns:
        trades["수익률"] = 0.0
    trades["수익률"] = pd.to_numeric(trades["수익률"], errors="coerce").fillna(0.0)
    if "보유일수" not in trades.columns:
        trades["보유일수"] = 0
    trades["보유일수"] = pd.to_numeric(trades["보유일수"], errors="coerce").fillna(0).astype(int)

    win_rate = float(trades["수익률"].gt(0).mean() * 100.0) if not trades.empty else 0.0
    avg_ret = float(trades["수익률"].mean()) if not trades.empty else 0.0
    d5 = trades[trades["보유일수"] == 5]
    d10 = trades[trades["보유일수"] == 10]
    win_trades = trades[trades["수익률"] > 0].copy()
    loss_trades = trades[trades["수익률"] <= 0].copy()
    avg_win_ret = float(win_trades["수익률"].mean()) if not win_trades.empty else 0.0
    avg_loss_ret = float(loss_trades["수익률"].mean()) if not loss_trades.empty else 0.0
    payoff_ratio = (avg_win_ret / abs(avg_loss_ret)) if avg_loss_ret < 0 else 0.0
    expectancy = float((win_rate / 100.0 * avg_win_ret) + ((1.0 - win_rate / 100.0) * avg_loss_ret))
    signal_closed = trades[trades.get("청산방식", "").astype(str).eq("시그널")] if "청산방식" in trades.columns else trades
    return {
        "win_rate": win_rate,
        "avg_ret": avg_ret,
        "d5_ret": float(d5["수익률"].mean()) if not d5.empty else 0.0,
        "d10_ret": float(d10["수익률"].mean()) if not d10.empty else 0.0,
        "avg_win_ret": avg_win_ret,
        "avg_loss_ret": avg_loss_ret,
        "payoff_ratio": payoff_ratio,
        "expectancy": expectancy,
        "signal_closed_count": int(len(signal_closed)),
    }


def build_start_date_stability(df_trades, df_history, available_dates, selected_start_date, initial_cash, max_positions, limit=10, score_mode="swing", adaptive_profile="현재값"):
    date_series = pd.Series(pd.to_datetime(available_dates, errors="coerce")).dropna()
    start_candidates = pd.Series(date_series.dt.date.unique()).sort_values()
    start_candidates = [d for d in start_candidates.tolist() if d >= selected_start_date]
    start_candidates = start_candidates[: int(limit)]
    rows = []
    for start_d in start_candidates:
        sim_perf, _, sim_closed = build_capital_limited_swing_sim(
            df_trades,
            df_history,
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_d,
            score_mode=score_mode,
            adaptive_profile=adaptive_profile,
        )
        if sim_perf.empty:
            continue
        sim_perf = sim_perf.copy()
        sim_perf["수익률(%)"] = pd.to_numeric(sim_perf.get("수익률(%)", 0.0), errors="coerce").fillna(0.0)
        sim_equity = 1.0 + (sim_perf["수익률(%)"] / 100.0)
        sim_mdd = float(((sim_equity / sim_equity.cummax()) - 1.0).min() * 100.0) if not sim_equity.empty else 0.0
        sim_ret = float(sim_perf["수익률(%)"].iloc[-1])
        sim_metrics = compute_trade_quality_metrics(sim_closed)
        rows.append({
            "시작일": start_d.strftime("%Y-%m-%d"),
            "전략수익률(%)": sim_ret,
            "MDD(%)": sim_mdd,
            "승률(%)": sim_metrics["win_rate"],
            "손익비": sim_metrics["payoff_ratio"],
            "거래당기대값(%)": sim_metrics["expectancy"],
            "종료거래": int(len(sim_closed)),
        })
    return pd.DataFrame(rows)


def build_adaptive_threshold_sensitivity(df_trades, df_history, available_dates, selected_start_date, initial_cash, max_positions, limit=5):
    date_series = pd.Series(pd.to_datetime(available_dates, errors="coerce")).dropna()
    start_candidates = pd.Series(date_series.dt.date.unique()).sort_values()
    start_candidates = [d for d in start_candidates.tolist() if d >= selected_start_date]
    if len(start_candidates) > int(limit):
        positions = sorted(set([0, len(start_candidates) - 1] + [
            round(i * (len(start_candidates) - 1) / max(1, int(limit) - 1)) for i in range(int(limit))
        ]))
        start_candidates = [start_candidates[i] for i in positions[: int(limit)]]

    detail_rows = []
    profile_names = ["현재값", "v2 견고형", "v3 상대강도"]
    for profile_name in profile_names:
        for start_d in start_candidates:
            sim_perf, _, sim_closed = build_capital_limited_swing_sim(
                df_trades,
                df_history,
                initial_cash=initial_cash,
                max_positions=max_positions,
                start_date=start_d,
                score_mode="adaptive",
                adaptive_profile=profile_name,
            )
            if sim_perf.empty:
                continue
            sim_perf = sim_perf.copy()
            sim_perf["수익률(%)"] = pd.to_numeric(sim_perf.get("수익률(%)", 0.0), errors="coerce").fillna(0.0)
            sim_equity = 1.0 + (sim_perf["수익률(%)"] / 100.0)
            sim_mdd = float(((sim_equity / sim_equity.cummax()) - 1.0).min() * 100.0) if not sim_equity.empty else 0.0
            sim_metrics = compute_trade_quality_metrics(sim_closed)
            avg_positions = float(pd.to_numeric(sim_perf.get("보유종목수", 0), errors="coerce").fillna(0.0).mean())
            detail_rows.append({
                "프로필": profile_name,
                "시작일": start_d.strftime("%Y-%m-%d"),
                "전략수익률(%)": float(sim_perf["수익률(%)"].iloc[-1]),
                "MDD(%)": sim_mdd,
                "승률(%)": sim_metrics["win_rate"],
                "거래당기대값(%)": sim_metrics["expectancy"],
                "손익비": sim_metrics["payoff_ratio"],
                "평균보유종목수": avg_positions,
                "종료거래": int(len(sim_closed)),
            })

    detail = pd.DataFrame(detail_rows)
    if detail.empty:
        return pd.DataFrame(), detail
    summary = (
        detail.groupby("프로필", as_index=False)
        .agg(
            평균수익률=("전략수익률(%)", "mean"),
            최저수익률=("전략수익률(%)", "min"),
            최고수익률=("전략수익률(%)", "max"),
            수익률표준편차=("전략수익률(%)", "std"),
            평균MDD=("MDD(%)", "mean"),
            최악MDD=("MDD(%)", "min"),
            평균승률=("승률(%)", "mean"),
            평균기대값=("거래당기대값(%)", "mean"),
            평균보유종목수=("평균보유종목수", "mean"),
        )
    )
    summary["수익률표준편차"] = summary["수익률표준편차"].fillna(0.0)
    summary["견고성점수"] = (
        summary["최저수익률"].clip(lower=-50, upper=100)
        + summary["평균수익률"] * 0.35
        + summary["평균기대값"] * 3.0
        + summary["최악MDD"].clip(lower=-80, upper=0) * 0.45
        - summary["수익률표준편차"].fillna(0.0) * 0.25
    )
    summary = summary.sort_values("견고성점수", ascending=False)
    return summary, detail
