import altair as alt
import pandas as pd
import streamlit as st

from db_utils import read_table


RISK_WORDS = ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]


def _load_csv(name):
    return read_table(name, name, read_csv_kwargs={"encoding": "utf-8-sig", "on_bad_lines": "skip"})


def _to_date(series):
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def _trade_returns(price_df, entry_date, entry_price, horizons=(1, 3, 5, 10)):
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


def build_recommendation_validation(score_trend=None, swing_trades=None, history=None):
    score_trend = score_trend if score_trend is not None else _load_csv("score_trend.csv")
    swing_trades = swing_trades if swing_trades is not None else _load_csv("swing_trades.csv")
    history = history if history is not None else _load_csv("history.csv")

    if swing_trades.empty or history.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    trades = swing_trades.copy()
    trades["진입일_dt"] = _to_date(trades.get("진입일", pd.Series(dtype=str)))
    trades["청산일_dt"] = _to_date(trades.get("청산일", pd.Series(dtype=str)))
    trades["진입가"] = pd.to_numeric(trades.get("진입가", 0.0), errors="coerce").fillna(0.0)
    trades["수익률"] = pd.to_numeric(trades.get("수익률", 0.0), errors="coerce").fillna(0.0)
    signal = trades[trades.get("청산방식", "").astype(str).eq("시그널")].copy()
    if signal.empty:
        signal = trades.sort_values(["진입일_dt", "종목명"]).drop_duplicates(["진입일_dt", "종목명"]).copy()

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

    detail_rows = []
    for _, row in signal.iterrows():
        name = str(row.get("종목명", "")).strip()
        entry_date = row.get("진입일_dt")
        entry_price = float(row.get("진입가", 0.0) or 0.0)
        if not name or pd.isna(entry_date) or entry_price <= 0:
            continue
        metrics = _trade_returns(price_map.get(name, pd.DataFrame()), entry_date, entry_price)
        signal_ret = float(row.get("수익률", 0.0) or 0.0)
        detail_rows.append({
            "추천일": pd.to_datetime(entry_date).strftime("%Y-%m-%d"),
            "종목명": name,
            "진입유형": row.get("진입유형", "-"),
            "진입순위": int(row.get("진입순위", 0) or 0),
            "스윙우선순위": float(row.get("스윙우선순위", 0.0) or 0.0),
            "시그널 수익률": signal_ret,
            "승패": "승" if signal_ret > 0 else "패",
            **metrics,
        })

    detail = pd.DataFrame(detail_rows)
    if detail.empty:
        return pd.DataFrame(), detail, pd.DataFrame()

    market_state = _build_market_state(hist)
    summary = detail.groupby("추천일", as_index=False).agg(
        추천종목수=("종목명", "count"),
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


def _build_market_state(hist):
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


def render_recommendation_validation_tab(render_section_header, apply_altair_theme, render_empty_state):
    render_section_header("추천 검증", "매일 나온 스윙 추천이 이후 며칠 동안 실제로 유효했는지 시장 상태별로 검증합니다.", badge_text="Validation")
    st.caption("이 화면은 수익률을 예쁘게 보이기 위한 화면이 아니라, 실제 투자에 쓰기 전에 추천 신뢰도를 확인하기 위한 방어 장치입니다.")

    summary, detail, regime_summary = build_recommendation_validation()
    if summary.empty:
        render_empty_state("검증 데이터 없음", "swing_trades.csv와 history.csv가 충분히 쌓인 뒤 추천 검증을 표시할 수 있습니다.")
        return

    latest = summary.sort_values("추천일").iloc[-1]
    weak_days = int(summary["거래당기대값"].lt(0).sum())
    crash_days = int(summary["급락수"].gt(0).sum())
    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="kpi-title">검증 추천일</div><div class="kpi-value">{len(summary):,}</div><div class="kpi-meta">최근 {latest['추천일']}</div></div>
            <div class="kpi-card"><div class="kpi-title">최근 시장 상태</div><div class="kpi-value">{latest['시장상태']}</div><div class="kpi-meta">상승비율 {float(latest.get('상승비율', 0.0)):.1f}%</div></div>
            <div class="kpi-card"><div class="kpi-title">기대값 음수일</div><div class="kpi-value">{weak_days:,}</div><div class="kpi-meta">추천 중지 검토 구간</div></div>
            <div class="kpi-card"><div class="kpi-title">-5% 역행 발생일</div><div class="kpi-value">{crash_days:,}</div><div class="kpi-meta">손실 방어 우선 구간</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not regime_summary.empty:
        st.markdown("#### 시장 상태별 추천 성과")
        regime_cols = ["시장상태", "날짜수", "평균승률", "평균기대값", "평균최대역행폭", "급락수"]
        st.dataframe(
            regime_summary[regime_cols].style.format({
                "평균승률": "{:.1f}%",
                "평균기대값": "{:+.2f}%",
                "평균최대역행폭": "{:+.2f}%",
            }),
            hide_index=True,
            column_order=regime_cols,
            width="stretch",
        )

    chart_df = summary.copy()
    chart = alt.Chart(chart_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("추천일:O", title=None, axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("거래당기대값:Q", title="거래당 기대값(%)"),
        color=alt.Color("시장상태:N", scale=alt.Scale(domain=["상승장", "혼조장", "하락장"], range=["#36C06A", "#FCD34D", "#E04B4B"])),
        tooltip=["추천일:N", "시장상태:N", alt.Tooltip("승률:Q", format=".1f"), alt.Tooltip("거래당기대값:Q", format="+.2f"), alt.Tooltip("최대역행폭:Q", format="+.2f")],
    ).properties(height=280)
    st.altair_chart(apply_altair_theme(chart), width="stretch")

    st.markdown("#### 추천일별 검증")
    view_cols = ["추천일", "시장상태", "추천종목수", "D1평균", "D3평균", "D5평균", "D10평균", "시그널평균", "승률", "손익비", "거래당기대값", "최대역행폭", "최대유리폭", "급락수"]
    st.dataframe(
        summary[view_cols].sort_values("추천일", ascending=False).style.format({
            "D1평균": "{:+.2f}%",
            "D3평균": "{:+.2f}%",
            "D5평균": "{:+.2f}%",
            "D10평균": "{:+.2f}%",
            "시그널평균": "{:+.2f}%",
            "승률": "{:.1f}%",
            "손익비": "{:.2f}x",
            "거래당기대값": "{:+.2f}%",
            "최대역행폭": "{:+.2f}%",
            "최대유리폭": "{:+.2f}%",
            "시장평균등락": "{:+.2f}%",
        }),
        hide_index=True,
        column_order=view_cols,
        width="stretch",
    )

    with st.expander("종목별 추천 검증 로그", expanded=False):
        detail_cols = ["추천일", "종목명", "진입유형", "진입순위", "D+1 수익률", "D+3 수익률", "D+5 수익률", "D+10 수익률", "시그널 수익률", "최대역행폭", "최대유리폭"]
        st.dataframe(
            detail[detail_cols].sort_values(["추천일", "진입순위"], ascending=[False, True]).style.format({
                "D+1 수익률": "{:+.2f}%",
                "D+3 수익률": "{:+.2f}%",
                "D+5 수익률": "{:+.2f}%",
                "D+10 수익률": "{:+.2f}%",
                "시그널 수익률": "{:+.2f}%",
                "최대역행폭": "{:+.2f}%",
                "최대유리폭": "{:+.2f}%",
                "스윙우선순위": "{:.2f}",
            }),
            hide_index=True,
            column_order=detail_cols,
            width="stretch",
        )
