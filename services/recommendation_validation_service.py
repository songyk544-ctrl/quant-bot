import pandas as pd

from db_utils import read_table


RISK_WORDS = ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]


def _load_csv(name):
    return read_table(name, name, read_csv_kwargs={"encoding": "utf-8-sig", "on_bad_lines": "skip"})


def _to_date(series):
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def trade_returns(price_df, entry_date, entry_price, horizons=(1, 3, 5, 10)):
    out = {f"D+{h} 수익률": float("nan") for h in horizons}
    out["최대역행폭"] = float("nan")
    out["최대유리폭"] = float("nan")
    if price_df.empty or entry_price <= 0:
        return out

    future = price_df[price_df["일자_dt"] >= entry_date].sort_values("일자_dt").copy()
    if future.empty:
        return out

    closes = pd.to_numeric(future["종가"], errors="coerce").dropna().reset_index(drop=True)
    if closes.empty:
        return out

    returns = ((closes - float(entry_price)) / float(entry_price) * 100.0)
    for h in horizons:
        if len(returns) > h:
            out[f"D+{h} 수익률"] = float(returns.iloc[h])
    out["최대역행폭"] = float(returns.min())
    out["최대유리폭"] = float(returns.max())
    return out


def build_market_state(hist):
    if hist.empty:
        return pd.DataFrame(columns=["추천일", "시장상태", "시장평균등락", "상승비율"])
    market = hist.sort_values(["종목명", "일자_dt"]).copy()
    market["전일종가"] = market.groupby("종목명")["종가"].shift(1)
    market["등락률"] = ((market["종가"] - market["전일종가"]) / market["전일종가"] * 100.0).replace([float("inf"), -float("inf")], pd.NA)
    daily = market.dropna(subset=["등락률"]).groupby("일자_dt").agg(
        시장평균등락=("등락률", "mean"),
        상승비율=("등락률", lambda s: float((s > 0).mean() * 100.0)),
    ).reset_index()
    daily["5일평균등락"] = daily["시장평균등락"].rolling(5, min_periods=1).mean()

    def classify(row):
        if row["시장평균등락"] >= 0.3 and row["5일평균등락"] >= 0.0 and row["상승비율"] >= 55:
            return "상승장"
        if row["시장평균등락"] <= -0.3 and row["5일평균등락"] <= 0.0 and row["상승비율"] <= 45:
            return "하락장"
        return "혼조장"

    daily["시장상태"] = daily.apply(classify, axis=1)
    daily["추천일"] = daily["일자_dt"].dt.strftime("%Y-%m-%d")
    return daily[["추천일", "시장상태", "시장평균등락", "상승비율"]]


def build_recommendation_validation(score_trend=None, swing_trades=None, history=None, score_mode="swing", max_positions=3):
    score_trend = score_trend if score_trend is not None else _load_csv("score_trend.csv")
    swing_trades = swing_trades if swing_trades is not None else _load_csv("swing_trades.csv")
    history = history if history is not None else _load_csv("history.csv")
    _ = score_trend

    if swing_trades.empty or history.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    trades = swing_trades.copy()
    trades["진입일_dt"] = _to_date(trades.get("진입일", pd.Series(dtype=str)))
    trades["청산일_dt"] = _to_date(trades.get("청산일", pd.Series(dtype=str)))
    trades["진입가"] = pd.to_numeric(trades.get("진입가", 0.0), errors="coerce").fillna(0.0)
    trades["수익률"] = pd.to_numeric(trades.get("수익률", 0.0), errors="coerce").fillna(0.0)
    hist = history.copy()
    hist["일자_dt"] = pd.to_datetime(
        hist.get("일자", pd.Series(dtype=str)).astype(str).str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    ).dt.normalize()
    if hist["일자_dt"].isna().all():
        hist["일자_dt"] = _to_date(hist.get("일자", pd.Series(dtype=str)))
    hist["종가"] = pd.to_numeric(hist.get("종가", 0.0), errors="coerce")
    hist = hist.dropna(subset=["종목명", "일자_dt", "종가"]).sort_values(["종목명", "일자_dt"])
    price_map = {str(name): grp.copy() for name, grp in hist.groupby("종목명")}
    market_state = build_market_state(hist)

    signal = trades[trades.get("청산방식", "").astype(str).eq("시그널")].copy()
    if signal.empty:
        signal = trades.sort_values(["진입일_dt", "종목명"]).drop_duplicates(["진입일_dt", "종목명"]).copy()
    score_mode_key = str(score_mode).lower()
    score_col = "AI수급점수" if score_mode_key == "ai" else "스윙우선순위"
    fallback_score_col = "스윙우선순위" if score_col == "AI수급점수" else "AI수급점수"
    if score_col not in signal.columns and fallback_score_col in signal.columns:
        score_col = fallback_score_col
    if score_col not in signal.columns:
        signal[score_col] = 0.0
    signal[score_col] = pd.to_numeric(signal[score_col], errors="coerce").fillna(0.0)
    signal = signal.dropna(subset=["진입일_dt"]).sort_values(["진입일_dt", score_col, "진입순위"], ascending=[True, False, True])

    def _target_for_day(day_text, day_df):
        if score_mode_key not in {"adaptive", "attack_defense", "dynamic"}:
            return int(max_positions), "고정"
        state_row = market_state[market_state["추천일"].astype(str).eq(str(day_text))]
        avg_ret = float(state_row["시장평균등락"].iloc[0]) if not state_row.empty else 0.0
        up_ratio = float(state_row["상승비율"].iloc[0]) if not state_row.empty else 50.0
        max_score = float(pd.to_numeric(day_df[score_col], errors="coerce").max()) if not day_df.empty else 0.0
        has_leader = day_df.get("진입유형", pd.Series(dtype=str)).astype(str).str.contains("주도", na=False).any() if not day_df.empty else False
        if avg_ret <= -1.2 or up_ratio <= 32:
            return 0, "방어"
        if avg_ret <= -0.35 or up_ratio <= 43:
            return 1, "선별"
        if max_score >= 65 and up_ratio >= 52:
            return int(max_positions), "공격"
        if has_leader and max_score >= 58 and up_ratio >= 48:
            return min(int(max_positions), 2), "공격대기"
        return min(int(max_positions), 1), "관찰"

    filtered = []
    for day, day_df in signal.groupby(signal["진입일_dt"].dt.strftime("%Y-%m-%d"), sort=True):
        day_df = day_df.sort_values([score_col, "진입순위"], ascending=[False, True]).copy()
        target_count, mode_label = _target_for_day(day, day_df)
        day_df["검증모드"] = mode_label
        if target_count > 0:
            filtered.append(day_df.head(int(target_count)))
    signal = pd.concat(filtered, ignore_index=True) if filtered else signal.iloc[0:0].copy()

    detail_rows = []
    for _, row in signal.iterrows():
        name = str(row.get("종목명", "")).strip()
        entry_date = row.get("진입일_dt")
        entry_price = float(row.get("진입가", 0.0) or 0.0)
        if not name or pd.isna(entry_date) or entry_price <= 0:
            continue
        metrics = trade_returns(price_map.get(name, pd.DataFrame()), entry_date, entry_price)
        signal_ret = float(row.get("수익률", 0.0) or 0.0)
        detail_rows.append({
            "추천일": pd.to_datetime(entry_date).strftime("%Y-%m-%d"),
            "종목명": name,
            "진입유형": row.get("진입유형", "-"),
            "진입순위": int(row.get("진입순위", 0) or 0),
            "스윙우선순위": float(row.get("스윙우선순위", 0.0) or 0.0),
            "AI수급점수": float(row.get("AI수급점수", 0.0) or 0.0),
            "검증모드": row.get("검증모드", "고정"),
            "시그널 수익률": signal_ret,
            "승패": "승" if signal_ret > 0 else "패",
            **metrics,
        })

    detail = pd.DataFrame(detail_rows)
    if detail.empty:
        return pd.DataFrame(), detail, pd.DataFrame()

    summary = detail.groupby("추천일", as_index=False).agg(
        추천종목수=("종목명", "count"),
        검증모드=("검증모드", lambda s: "/".join(sorted(set(map(str, s))))),
        D1평균=("D+1 수익률", "mean"),
        D3평균=("D+3 수익률", "mean"),
        D5평균=("D+5 수익률", "mean"),
        D10평균=("D+10 수익률", "mean"),
        시그널평균=("시그널 수익률", "mean"),
        승률=("시그널 수익률", lambda s: float((s > 0).mean() * 100.0)),
        최대역행폭=("최대역행폭", "min"),
        최대유리폭=("최대유리폭", "max"),
        급락수=("최대역행폭", lambda s: int((s <= -5.0).sum())),
    )
    summary["평균수익"] = detail.groupby("추천일")["시그널 수익률"].apply(lambda s: float(s[s > 0].mean()) if (s > 0).any() else 0.0).values
    summary["평균손실"] = detail.groupby("추천일")["시그널 수익률"].apply(lambda s: float(s[s <= 0].mean()) if (s <= 0).any() else 0.0).values
    summary["손익비"] = summary.apply(lambda r: (r["평균수익"] / abs(r["평균손실"])) if r["평균손실"] < 0 else 0.0, axis=1)
    summary["거래당기대값"] = summary["시그널평균"]
    summary = pd.merge(summary, market_state, on="추천일", how="left")
    summary["시장상태"] = summary["시장상태"].fillna("혼조장")

    regime_summary = summary.groupby("시장상태", as_index=False).agg(
        날짜수=("추천일", "count"),
        평균승률=("승률", "mean"),
        평균기대값=("거래당기대값", "mean"),
        평균최대역행폭=("최대역행폭", "mean"),
        급락수=("급락수", "sum"),
    )
    return summary, detail, regime_summary
