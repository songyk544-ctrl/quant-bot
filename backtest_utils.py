import pandas as pd


PERF_COLS = ["날짜", "평가금액", "현금", "투자금액", "수익률(%)", "일간수익률", "보유종목수", "실현손익"]
POSITION_COLS = ["종목명", "진입일", "진입가", "수량", "매수금액", "현재가", "평가금액", "평가손익", "평가수익률", "보유일수", "상태"]
CLOSED_COLS = ["진입일", "청산일", "종목명", "보유일수", "매수금액", "청산금액", "실현손익", "수익률", "청산사유"]


def empty_capital_limited_result():
    return (
        pd.DataFrame(columns=PERF_COLS),
        pd.DataFrame(columns=POSITION_COLS),
        pd.DataFrame(columns=CLOSED_COLS),
    )


def build_capital_limited_swing_sim(df_trades, df_history, initial_cash=5_000_000, max_positions=3, start_date=None):
    """초기자금과 동시보유 제한을 둔 실제 포트폴리오형 스윙 시뮬레이션."""
    if df_trades.empty or df_history.empty:
        return empty_capital_limited_result()

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

    signal = df_trades[df_trades["청산방식"].astype(str).eq("시그널")].copy()
    if signal.empty:
        signal = df_trades[df_trades["보유일수"].eq(10)].copy()
    signal = signal.dropna(subset=["진입일_dt"]).sort_values(
        ["진입일_dt", "진입순위", "스윙우선순위"],
        ascending=[True, True, False],
    )

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
        held_names = {p["종목명"] for p in positions}
        for _, sig in todays.iterrows():
            name = str(sig.get("종목명", "")).strip()
            if not name or name in held_names:
                continue
            entry_price = float(pd.to_numeric(sig.get("진입가"), errors="coerce") or 0.0)
            if entry_price <= 0:
                continue
            new_score = float(pd.to_numeric(sig.get("스윙우선순위", 0.0), errors="coerce") or 0.0)
            new_entry_type = str(sig.get("진입유형", ""))
            if len(positions) >= int(max_positions):
                if switched_today:
                    break
                weakest_idx = None
                weakest_score = 0.0
                weakest_ret = 0.0
                for pos_idx, pos in enumerate(positions):
                    mark_price = _last_price(pos["종목명"], cur_date, pos["진입가"])
                    pos_ret = ((mark_price - float(pos["진입가"])) / float(pos["진입가"]) * 100.0) if float(pos["진입가"]) > 0 else 0.0
                    pos_score = float(pos.get("스윙우선순위", 0.0) or 0.0)
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
                    "매수금액": round(old_pos["매수금액"], 0),
                    "청산금액": round(exit_value, 0),
                    "실현손익": round(pnl, 0),
                    "수익률": round((pnl / old_pos["매수금액"]) * 100.0, 4) if old_pos["매수금액"] else 0.0,
                    "청산사유": "더강한후보교체",
                })
                switched_today = True

            current_equity_for_buy = cash + _mark_positions_value(cur_date)
            dynamic_slot_cash = current_equity_for_buy / max(1, int(max_positions))
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
                "스윙우선순위": new_score,
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


def build_start_date_stability(df_trades, df_history, available_dates, selected_start_date, initial_cash, max_positions, limit=10):
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
