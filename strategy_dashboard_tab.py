import json
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
import yfinance as yf

from backtest_utils import (
    build_adaptive_threshold_sensitivity,
    build_capital_limited_swing_sim,
    build_start_date_stability,
    compute_trade_quality_metrics,
)


STRATEGY_SETTINGS_PATH = Path("data") / "strategy_settings.json"
DEFAULT_BENCHMARK_START_DATE = pd.Timestamp("2026-04-27").date()


def _load_strategy_settings():
    try:
        if STRATEGY_SETTINGS_PATH.exists():
            with STRATEGY_SETTINGS_PATH.open("r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _save_strategy_settings(settings):
    try:
        STRATEGY_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with STRATEGY_SETTINGS_PATH.open("w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False


def _parse_cash_amount(value, default=5_000_000):
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        return default


def _mark_strategy_date_manual():
    st.session_state["strategy_quick_range"] = "직접 선택"


def render_strategy_dashboard_tab(
    app_name,
    is_vip,
    df_summary,
    df_history,
    render_section_header,
    show_premium_paywall,
    render_empty_state,
    apply_altair_theme,
    load_swing_performance_safe,
    load_swing_trades_safe,
    fetch_yahoo_chart_history,
    macro_data=None,
):
    render_section_header(f"{app_name} 전략 대시보드", "백테스트, 현재 포지션, 신규 후보를 한 화면에서 운용 관점으로 확인합니다.", badge_text="Strategy")
    if not is_vip:
        show_premium_paywall("가상 포트폴리오 누적 수익률 및 운용 대시보드는 코드 인증 후 확인할 수 있습니다.")
    else:
        strategy_settings = _load_strategy_settings()
        saved_initial_cash = _parse_cash_amount(strategy_settings.get("initial_cash", 5_000_000))
        saved_start_date = pd.to_datetime(
            strategy_settings.get("benchmark_start_date", DEFAULT_BENCHMARK_START_DATE),
            errors="coerce",
        )
        saved_start_date = saved_start_date.date() if not pd.isna(saved_start_date) else DEFAULT_BENCHMARK_START_DATE
        df_swing_perf = load_swing_performance_safe()
        df_swing_trades = load_swing_trades_safe()
        available_dates = pd.to_datetime(df_swing_trades.get("진입일_dt", pd.Series(dtype="datetime64[ns]")), errors="coerce").dropna()
        if available_dates.empty and not df_swing_perf.empty:
            available_dates = pd.to_datetime(df_swing_perf.get("날짜_dt", pd.Series(dtype="datetime64[ns]")), errors="coerce").dropna()
        if not available_dates.empty:
            min_date = available_dates.min().date()
            hist_dates = pd.to_datetime(df_history.get("일자", pd.Series(dtype=str)).astype(str).str.replace("-", "", regex=False), format="%Y%m%d", errors="coerce").dropna()
            max_date = hist_dates.max().date() if not hist_dates.empty else available_dates.max().date()
            default_start_date = max(min_date, min(max_date, saved_start_date))
            if "strategy_backtest_start_date" not in st.session_state:
                st.session_state["strategy_backtest_start_date"] = default_start_date
            current_start = pd.to_datetime(st.session_state.get("strategy_backtest_start_date", min_date), errors="coerce")
            current_start = current_start.date() if not pd.isna(current_start) else min_date
            st.session_state["strategy_backtest_start_date"] = max(min_date, min(max_date, current_start))
            quick_ranges = [
                ("직접 선택", st.session_state["strategy_backtest_start_date"]),
                ("최근 1주", max_date - pd.Timedelta(days=7)),
                ("최근 1개월", max_date - pd.Timedelta(days=31)),
                ("최근 3개월", max_date - pd.Timedelta(days=93)),
                ("최근 6개월", max_date - pd.Timedelta(days=186)),
                ("올해", pd.Timestamp(max_date).replace(month=1, day=1)),
            ]
            quick_labels = [label for label, _ in quick_ranges]
            if hasattr(st, "segmented_control"):
                quick_label = st.segmented_control(
                    "기간 빠른 선택",
                    quick_labels,
                    default="직접 선택",
                    key="strategy_quick_range",
                )
            else:
                quick_label = st.radio(
                    "기간 빠른 선택",
                    quick_labels,
                    horizontal=True,
                    index=0,
                    key="strategy_quick_range",
                )
            if quick_label and quick_label != "직접 선택":
                target_dt = dict(quick_ranges)[quick_label]
                target_date = pd.to_datetime(target_dt).date()
                target_date = max(min_date, min(max_date, target_date))
                st.session_state["strategy_backtest_start_date"] = target_date
            selected_start_date = st.date_input(
                "🗓️ 벤치마크 시작(기준)일 선택",
                min_value=min_date,
                max_value=max_date,
                key="strategy_backtest_start_date",
                on_change=_mark_strategy_date_manual,
            )
            st.markdown("**초기 투자금**")
            bt_col1, bt_col_save = st.columns([4, 1], gap="small")
            with bt_col1:
                if "strategy_initial_cash_text" not in st.session_state:
                    st.session_state["strategy_initial_cash_text"] = f"{saved_initial_cash:,}"
                cash_text = st.text_input("초기 투자금", key="strategy_initial_cash_text", label_visibility="collapsed")
                backtest_initial_cash = _parse_cash_amount(cash_text, saved_initial_cash)
                backtest_initial_cash = max(1_000_000, min(100_000_000, backtest_initial_cash))
            with bt_col_save:
                if st.button("저장", key="save_strategy_initial_cash", use_container_width=True):
                    settings = _load_strategy_settings()
                    settings["initial_cash"] = int(backtest_initial_cash)
                    settings["benchmark_start_date"] = selected_start_date.strftime("%Y-%m-%d")
                    if _save_strategy_settings(settings):
                        st.toast("기준일과 초기 투자금을 저장했습니다.")
                    else:
                        st.warning("저장 실패")
            st.caption(f"적용 금액: {backtest_initial_cash:,}원")
            backtest_max_positions = st.number_input(
                "최대 보유 종목수",
                min_value=1,
                max_value=5,
                value=3,
                step=1,
                format="%d",
            )
            score_options = ["공격/방어", "스윙점수", "AI점수"]
            if hasattr(st, "segmented_control"):
                score_mode_label = st.segmented_control(
                    "백테스트 모드",
                    score_options,
                    default="공격/방어",
                    key="strategy_score_mode",
                    help="공격/방어는 시장 상태와 후보 강도에 따라 보유 종목 수를 0~최대값으로 조절합니다.",
                )
            else:
                score_mode_label = st.radio(
                    "백테스트 모드",
                    score_options,
                    horizontal=True,
                    index=0,
                    key="strategy_score_mode",
                    help="공격/방어는 시장 상태와 후보 강도에 따라 보유 종목 수를 0~최대값으로 조절합니다.",
                )
            score_mode = "adaptive" if score_mode_label == "공격/방어" else ("ai" if score_mode_label == "AI점수" else "swing")
            adaptive_profile_label = "v1 기존"
            adaptive_profile = "현재값"
            if score_mode == "adaptive":
                profile_options = ["v1 기존", "v2 견고형", "v3 상대강도"]
                if "strategy_adaptive_profile" not in st.session_state:
                    st.session_state["strategy_adaptive_profile"] = "v1 기존"
                if hasattr(st, "segmented_control"):
                    adaptive_profile_label = st.segmented_control(
                        "공격/방어 프로필",
                        profile_options,
                        default="v1 기존",
                        key="strategy_adaptive_profile",
                        help="v1은 기존 임계값, v2는 1년 데이터에서 시작일별 최저 수익률과 MDD를 함께 본 견고형 후보입니다.",
                    )
                else:
                    adaptive_profile_label = st.radio(
                        "공격/방어 프로필",
                        profile_options,
                        horizontal=True,
                        index=0,
                        key="strategy_adaptive_profile",
                        help="v1은 기존 임계값, v2는 1년 데이터에서 시작일별 최저 수익률과 MDD를 함께 본 견고형 후보입니다.",
                    )
                adaptive_profile = "현재값" if adaptive_profile_label == "v1 기존" else adaptive_profile_label
                with st.expander("v1/v2 변경 기준", expanded=False):
                    st.markdown(
                        """
                        <div style="font-size:0.82rem; line-height:1.55; color:#AAB2C5;">
                        <b>v1 기존</b>: 최근 구간 수익률은 좋았지만 일부 과거 시작일에서 크게 깨진 기존 임계값입니다.<br>
                        <b>v2 견고형</b>: 1년 데이터에서 시작일별 최저 수익률 방어를 더 중시한 개별주 중심 후보입니다.<br>
                        <b>v3 상대강도</b>: v2 후보 중 최근 5일 시장 대비 강하고 거래대금이 붙은 종목만 진입하는 필터형입니다.
                        </div>
                        """
                        ,
                        unsafe_allow_html=True,
                    )
            portfolio_perf, portfolio_positions, portfolio_closed = build_capital_limited_swing_sim(
                df_swing_trades,
                df_history,
                initial_cash=backtest_initial_cash,
                max_positions=backtest_max_positions,
                start_date=selected_start_date,
                score_mode=score_mode,
                adaptive_profile=adaptive_profile,
            )
        else:
            portfolio_perf, portfolio_positions, portfolio_closed = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            backtest_initial_cash = 5_000_000
            backtest_max_positions = 3

        if not portfolio_perf.empty:
            portfolio_perf["날짜_dt"] = pd.to_datetime(portfolio_perf["날짜"], errors="coerce")
            df_filtered = portfolio_perf[portfolio_perf['날짜_dt'].dt.date >= selected_start_date].copy()

            if not df_filtered.empty:
                df_filtered["전략 누적수익률"] = pd.to_numeric(df_filtered["수익률(%)"], errors="coerce").fillna(0.0)
                equity = 1.0 + (df_filtered["전략 누적수익률"] / 100.0)
                df_filtered["기간 MDD"] = (((equity / equity.cummax()) - 1.0) * 100.0).round(2)

                benchmark_fetch_errors = []
                try:
                    def compute_benchmark_returns(ticker_symbol):
                        hist = fetch_yahoo_chart_history(ticker_symbol, range_period="2y", interval="1d")
                        if hist.empty:
                            hist = yf.Ticker(ticker_symbol).history(period="2y")
                        if hist.empty:
                            benchmark_fetch_errors.append(ticker_symbol)
                            return [float("nan")] * len(df_filtered)

                        idx = hist.index
                        if getattr(idx, "tz", None) is not None:
                            idx = idx.tz_localize(None)
                        hist.index = idx.normalize()
                        hist = hist.dropna(subset=['Close'])
                        base_df = hist[hist.index <= pd.to_datetime(selected_start_date)]
                        base_close = float(base_df['Close'].dropna().iloc[-1]) if not base_df.empty and not base_df['Close'].dropna().empty else None
                        if base_close is None or base_close == 0:
                            benchmark_fetch_errors.append(ticker_symbol)
                            return [float("nan")] * len(df_filtered)

                        rets = []
                        for d in df_filtered['날짜_dt']:
                            close_series = hist[hist.index <= d]['Close'].dropna()
                            if close_series.empty:
                                rets.append(rets[-1] if rets else float("nan"))
                                continue
                            ret = ((float(close_series.iloc[-1]) - base_close) / base_close) * 100.0
                            rets.append(ret if not pd.isna(ret) else (rets[-1] if rets else float("nan")))
                        return rets

                    df_filtered['KOSPI 누적수익률'] = compute_benchmark_returns('^KS11')
                    df_filtered['NASDAQ 누적수익률'] = compute_benchmark_returns('^IXIC')
                except Exception as e:
                    print(f"[WARN] 벤치마크 수익률 계산 실패: {e}")
                    df_filtered['KOSPI 누적수익률'] = [float("nan")] * len(df_filtered)
                    df_filtered['NASDAQ 누적수익률'] = [float("nan")] * len(df_filtered)
                    benchmark_fetch_errors = ['^KS11', '^IXIC']

                chart_df = df_filtered.copy()
                baseline_row = {c: None for c in chart_df.columns}
                baseline_row.update({
                    "날짜": selected_start_date.strftime("%Y-%m-%d"),
                    "날짜_dt": pd.to_datetime(selected_start_date),
                    "일간수익률": 0.0,
                    "전략 누적수익률": 0.0,
                    "기간 MDD": 0.0,
                    "KOSPI 누적수익률": 0.0,
                    "NASDAQ 누적수익률": 0.0,
                    "평가금액": float(backtest_initial_cash),
                    "현금": float(backtest_initial_cash),
                    "투자금액": 0,
                    "보유종목수": 0,
                })
                chart_df = pd.concat([pd.DataFrame([baseline_row]), chart_df], ignore_index=True)
                chart_df = chart_df.drop_duplicates(subset=["날짜_dt"], keep="first").sort_values("날짜_dt")

                def _safe_last(series):
                    val = series.iloc[-1] if len(series) > 0 else float("nan")
                    return 0.0 if pd.isna(val) else float(val)

                def _safe_daily_diff(series):
                    if len(series) <= 1:
                        return 0.0
                    a, b = series.iloc[-1], series.iloc[-2]
                    if pd.isna(a) or pd.isna(b):
                        return 0.0
                    return float(a - b)

                closed_trades = portfolio_closed.copy()
                if "수익률" not in closed_trades.columns:
                    closed_trades["수익률"] = 0.0
                closed_trades["수익률"] = pd.to_numeric(closed_trades["수익률"], errors="coerce").fillna(0.0)
                if "청산일" in closed_trades.columns:
                    closed_trades["청산일_dt"] = pd.to_datetime(closed_trades["청산일"], errors="coerce")
                if "보유일수" not in closed_trades.columns:
                    closed_trades["보유일수"] = 0
                primary_open_trades = portfolio_positions.copy()
                new_candidates = df_summary[df_summary.get("매수후보", pd.Series("", index=df_summary.index)).astype(str).eq("신규후보")].copy()
                if not new_candidates.empty:
                    for c in ["스윙우선순위", "AI수급점수", "현재가"]:
                        if c in new_candidates.columns:
                            new_candidates[c] = pd.to_numeric(new_candidates[c], errors="coerce").fillna(0.0)
                    new_candidates = new_candidates.sort_values(["스윙우선순위", "AI수급점수"], ascending=[False, False])
                trade_metrics = compute_trade_quality_metrics(closed_trades)
                win_rate = trade_metrics["win_rate"]
                avg_ret = trade_metrics["avg_ret"]
                d5_ret = trade_metrics["d5_ret"]
                avg_win_ret = trade_metrics["avg_win_ret"]
                avg_loss_ret = trade_metrics["avg_loss_ret"]
                payoff_ratio = trade_metrics["payoff_ratio"]
                expectancy = trade_metrics["expectancy"]
                signal_closed_count = trade_metrics["signal_closed_count"]
                expectancy_color = "#36C06A" if expectancy >= 0 else "#E04B4B"

                current_port_ret = _safe_last(df_filtered['전략 누적수익률'])
                current_equity = _safe_last(df_filtered["평가금액"])
                current_cash = _safe_last(df_filtered["현금"])
                current_kospi_ret = _safe_last(df_filtered['KOSPI 누적수익률'])
                current_nasdaq_ret = _safe_last(df_filtered['NASDAQ 누적수익률'])
                current_mdd = float(df_filtered["기간 MDD"].min()) if "기간 MDD" in df_filtered.columns else 0.0
                current_risk_state = "High" if current_mdd <= -8 else ("Medium" if current_mdd <= -4 else "Low")
                port_daily_diff = _safe_daily_diff(df_filtered['전략 누적수익률'])
                kospi_daily_diff = _safe_daily_diff(df_filtered['KOSPI 누적수익률'])
                nasdaq_daily_diff = _safe_daily_diff(df_filtered['NASDAQ 누적수익률'])
                alpha_kospi = current_port_ret - current_kospi_ret
                alpha_nasdaq = current_port_ret - current_nasdaq_ret
                alpha_color = "#36C06A" if alpha_kospi >= 0 else "#E04B4B"
                alpha_nasdaq_color = "#36C06A" if alpha_nasdaq >= 0 else "#E04B4B"
                port_delta_color = "#36C06A" if port_daily_diff >= 0 else "#E04B4B"
                kospi_delta_color = "#36C06A" if kospi_daily_diff >= 0 else "#E04B4B"
                nasdaq_delta_color = "#36C06A" if nasdaq_daily_diff >= 0 else "#E04B4B"

                st.markdown(
                    f"""
                    <div class="kpi-grid">
                        <div class="kpi-card">
                            <div class="kpi-title">{int(backtest_initial_cash):,}원 포트폴리오 수익률</div>
                            <div class="kpi-value">{current_port_ret:+.2f}%</div>
                            <span class="kpi-delta" style="background: rgba(54,192,106,0.18); color:{port_delta_color};">최근 {port_daily_diff:+.2f}%</span>
                            <div class="kpi-meta">평가금액 {current_equity:,.0f}원 · 현금 {current_cash:,.0f}원</div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-title">KOSPI 누적 수익률</div>
                            <div class="kpi-value">{current_kospi_ret:+.2f}%</div>
                            <span class="kpi-delta" style="background: rgba(59,130,246,0.16); color:{kospi_delta_color};">최근 {kospi_daily_diff:+.2f}%</span>
                            <div class="kpi-meta">초과 성과 <span style="color:{alpha_color}; font-weight:700;">{alpha_kospi:+.2f}%p</span></div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-title">NASDAQ 누적 수익률</div>
                            <div class="kpi-value">{current_nasdaq_ret:+.2f}%</div>
                            <span class="kpi-delta" style="background: rgba(167,139,250,0.16); color:{nasdaq_delta_color};">최근 {nasdaq_daily_diff:+.2f}%</span>
                            <div class="kpi-meta">초과 성과 <span style="color:{alpha_nasdaq_color}; font-weight:700;">{alpha_nasdaq:+.2f}%p</span></div>
                        </div>
                        <div class="kpi-card">
                            <div class="kpi-title">진행 중 스윙</div>
                            <div class="kpi-value">{len(primary_open_trades):,}</div>
                            <span class="kpi-delta" style="background: rgba(252,211,77,0.16); color:#FCD34D;">최대 5종목</span>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                st.markdown(
                    f"""
                    <div class="score-kpi-grid">
                        <div class="score-kpi"><div class="score-kpi-label">종료 거래</div><div class="score-kpi-value">{len(closed_trades):,}</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">승률</div><div class="score-kpi-value">{win_rate:.1f}%</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">거래당 기대값</div><div class="score-kpi-value" style="color:{expectancy_color};">{expectancy:+.2f}%</div></div>
                    </div>
                    <div class="score-kpi-grid">
                        <div class="score-kpi"><div class="score-kpi-label">평균수익 / 평균손실</div><div class="score-kpi-value">{avg_win_ret:+.2f}% / {avg_loss_ret:+.2f}%</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">손익비</div><div class="score-kpi-value">{payoff_ratio:.2f}x</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">5거래일 평균수익</div><div class="score-kpi-value">{d5_ret:+.2f}%</div></div>
                    </div>
                    <div class="score-kpi-grid">
                        <div class="score-kpi"><div class="score-kpi-label">전체 평균 수익률</div><div class="score-kpi-value">{avg_ret:+.2f}%</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">시그널 종료</div><div class="score-kpi-value">{signal_closed_count:,}</div></div>
                        <div class="score-kpi"><div class="score-kpi-label">신규 후보</div><div class="score-kpi-value">{len(new_candidates):,}</div></div>
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                st.caption("승률은 단독 판단 지표가 아닙니다. 거래당 기대값이 플러스이고 평균수익이 평균손실보다 충분히 크면 승률이 50% 미만이어도 전략성이 있을 수 있습니다. 5거래일 평균수익은 매수 후 5거래일에 청산된 거래들의 평균 수익률입니다.")

                st.markdown("<br>", unsafe_allow_html=True)
                chart_df["날짜_label"] = pd.to_datetime(chart_df["날짜_dt"], errors="coerce").dt.strftime("%y/%m/%d")
                chart_df = chart_df.dropna(subset=["날짜_label"]).copy()
                chart_date_order = chart_df.sort_values("날짜_dt")["날짜_label"].drop_duplicates().tolist()
                df_melt = chart_df.melt(
                    id_vars=['날짜_dt', '날짜_label'],
                    value_vars=['전략 누적수익률', 'KOSPI 누적수익률', 'NASDAQ 누적수익률'],
                    var_name='포트폴리오',
                    value_name='차트수익률'
                )
                df_melt['포트폴리오'] = df_melt['포트폴리오'].replace({
                    '전략 누적수익률': '전략',
                    'KOSPI 누적수익률': 'KOSPI',
                    'NASDAQ 누적수익률': 'NASDAQ',
                })
                base_chart = alt.Chart(df_melt).mark_line(point=True).encode(
                    x=alt.X(
                        '날짜_label:O',
                        title=None,
                        sort=chart_date_order,
                        axis=alt.Axis(labelAngle=-45, labelLimit=72),
                    ),
                    y=alt.Y('차트수익률:Q', title="누적 수익률 (%)"),
                    color=alt.Color(
                        '포트폴리오:N',
                        scale=alt.Scale(
                            domain=['전략', 'KOSPI', 'NASDAQ'],
                            range=['#E74C3C', '#AAAAAA', '#A78BFA']
                        ),
                        legend=alt.Legend(title=None, orient='bottom', direction='horizontal', columns=3, labelLimit=80)
                    )
                ).properties(height=300)
                st.altair_chart(apply_altair_theme(base_chart), width='stretch')
                slot_cash = float(current_equity) / max(1, int(backtest_max_positions))
                st.caption(f"전략선은 초기자금 {int(backtest_initial_cash):,}원, 최대 {int(backtest_max_positions)}종목, 매수 시점 총자산 기준 종목당 약 1/{int(backtest_max_positions)} 재투자, 중복 보유 금지 기준의 가상 포트폴리오 평가수익률입니다. 현재 기준 다음 슬롯은 약 {int(slot_cash):,}원입니다.")

                if benchmark_fetch_errors:
                    err_names = ", ".join("KOSPI" if x == "^KS11" else ("NASDAQ" if x == "^IXIC" else x) for x in sorted(set(benchmark_fetch_errors)))
                    st.caption(f"일부 벤치마크 데이터가 지연되어 표시되지 않았습니다: {err_names}")

                with st.expander("리스크 보조 지표", expanded=False):
                    st.caption(f"MDD는 누적수익률이 고점 대비 얼마나 내려왔는지 보는 지표입니다. 선택 기간 기준 현재 최대낙폭은 {current_mdd:.2f}%이고 상태는 {current_risk_state}입니다.")

                with st.expander("시작일 안정성 점검", expanded=False):
                    st.caption("선택한 시작일 이후 가능한 시작일을 최대 10개까지 다시 돌려, 시작일에 따라 전략 성과가 얼마나 흔들리는지 확인합니다. 버튼을 눌렀을 때만 계산합니다.")
                    if st.button("최근 시작일 10개 안정성 분석 실행", use_container_width=True):
                        try:
                            stability_df = build_start_date_stability(
                                df_swing_trades,
                                df_history,
                                available_dates,
                                selected_start_date,
                                backtest_initial_cash,
                                backtest_max_positions,
                                limit=10,
                                score_mode=score_mode,
                                adaptive_profile=adaptive_profile,
                            )
                            if stability_df.empty:
                                st.info("분석 가능한 시작일 데이터가 부족합니다.")
                            else:
                                avg_start_ret = float(stability_df["전략수익률(%)"].mean())
                                min_start_ret = float(stability_df["전략수익률(%)"].min())
                                max_start_ret = float(stability_df["전략수익률(%)"].max())
                                avg_start_expectancy = float(stability_df["거래당기대값(%)"].mean()) if "거래당기대값(%)" in stability_df.columns else 0.0
                                negative_expectancy_count = int(stability_df["거래당기대값(%)"].lt(0).sum()) if "거래당기대값(%)" in stability_df.columns else 0
                                spread_ret = max_start_ret - min_start_ret
                                stability_label = "안정" if spread_ret < 8 and negative_expectancy_count == 0 else ("점검" if spread_ret < 18 and negative_expectancy_count <= 1 else "민감도 높음")
                                stability_color = "#36C06A" if stability_label == "안정" else ("#FCD34D" if stability_label == "점검" else "#E04B4B")
                                st.markdown(
                                    f"""
                                    <div class="stability-panel">
                                        <div class="stability-grid">
                                            <div class="stability-card">
                                                <div class="stability-label">평균 시작일 수익률</div>
                                                <div class="stability-value">{avg_start_ret:+.2f}%</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">최저 / 최고 수익률</div>
                                                <div class="stability-value">{min_start_ret:+.2f}% / {max_start_ret:+.2f}%</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">평균 거래당 기대값</div>
                                                <div class="stability-value">{avg_start_expectancy:+.2f}%</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">기대값 음수 시작일</div>
                                                <div class="stability-value">{negative_expectancy_count:,}개</div>
                                            </div>
                                        </div>
                                        <div class="stability-verdict">
                                            <div>
                                                <div class="stability-verdict-title">시작일 안정성 판정</div>
                                                <div class="stability-verdict-value" style="color:{stability_color};">{stability_label}</div>
                                            </div>
                                            <div class="stability-verdict-meta">
                                                수익률 편차 {spread_ret:.2f}%p<br>
                                                분석 시작일 {len(stability_df):,}개
                                            </div>
                                        </div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                                stability_cols = ["시작일", "전략수익률(%)", "MDD(%)", "승률(%)", "손익비", "거래당기대값(%)", "종료거래"]
                                stability_cols = [c for c in stability_cols if c in stability_df.columns]
                                st.dataframe(
                                    stability_df[stability_cols].style.format({
                                        "전략수익률(%)": "{:+.2f}%",
                                        "MDD(%)": "{:+.2f}%",
                                        "승률(%)": "{:.1f}%",
                                        "손익비": "{:.2f}x",
                                        "거래당기대값(%)": "{:+.2f}%",
                                    }),
                                    hide_index=True,
                                    column_order=stability_cols,
                                    width="stretch",
                                )
                        except Exception as e:
                            st.warning(f"시작일 안정성 분석 중 오류가 발생했습니다: {e}")

                with st.expander("공격/방어 민감도 점검", expanded=False):
                    st.caption("v1, v2, v3를 같은 시작일 묶음으로 비교해 성과 편차와 과최적화 가능성을 확인합니다. 버튼을 눌렀을 때만 계산합니다.")
                    if st.button("공격/방어 임계값 민감도 분석 실행", use_container_width=True):
                        try:
                            sensitivity_summary, sensitivity_detail = build_adaptive_threshold_sensitivity(
                                df_swing_trades,
                                df_history,
                                available_dates,
                                selected_start_date,
                                backtest_initial_cash,
                                backtest_max_positions,
                                limit=5,
                            )
                            if sensitivity_summary.empty:
                                st.info("민감도 분석 가능한 데이터가 부족합니다.")
                            else:
                                best_row = sensitivity_summary.iloc[0]
                                current_row = sensitivity_summary[sensitivity_summary["프로필"].eq("현재값")]
                                current_score = float(current_row["견고성점수"].iloc[0]) if not current_row.empty else 0.0
                                best_score = float(best_row.get("견고성점수", 0.0))
                                overfit_gap = best_score - current_score
                                if best_row["프로필"] == "현재값" and float(best_row.get("최저수익률", 0.0)) > 0:
                                    overfit_label = "양호"
                                    overfit_color = "#36C06A"
                                elif overfit_gap <= 12:
                                    overfit_label = "점검"
                                    overfit_color = "#FCD34D"
                                else:
                                    overfit_label = "민감도 높음"
                                    overfit_color = "#E04B4B"
                                st.markdown(
                                    f"""
                                    <div class="stability-panel">
                                        <div class="stability-grid">
                                            <div class="stability-card">
                                                <div class="stability-label">최상위 프로필</div>
                                                <div class="stability-value">{best_row['프로필']}</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">최상위 평균수익률</div>
                                                <div class="stability-value">{float(best_row['평균수익률']):+.2f}%</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">최상위 최저수익률</div>
                                                <div class="stability-value">{float(best_row['최저수익률']):+.2f}%</div>
                                            </div>
                                            <div class="stability-card">
                                                <div class="stability-label">최상위 최악 MDD</div>
                                                <div class="stability-value">{float(best_row['최악MDD']):+.2f}%</div>
                                            </div>
                                        </div>
                                        <div class="stability-verdict">
                                            <div>
                                                <div class="stability-verdict-title">과최적화 보조 판정</div>
                                                <div class="stability-verdict-value" style="color:{overfit_color};">{overfit_label}</div>
                                            </div>
                                            <div class="stability-verdict-meta">
                                                현재값 대비 견고성 차이 {overfit_gap:+.2f}<br>
                                                분석 프로필 {len(sensitivity_summary):,}개
                                            </div>
                                        </div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True,
                                )
                                sensitivity_cols = [
                                    "프로필", "견고성점수", "평균수익률", "최저수익률", "최고수익률",
                                    "수익률표준편차", "최악MDD", "평균승률", "평균기대값", "평균보유종목수"
                                ]
                                sensitivity_cols = [c for c in sensitivity_cols if c in sensitivity_summary.columns]
                                st.dataframe(
                                    sensitivity_summary[sensitivity_cols].style.format({
                                        "견고성점수": "{:.2f}",
                                        "평균수익률": "{:+.2f}%",
                                        "최저수익률": "{:+.2f}%",
                                        "최고수익률": "{:+.2f}%",
                                        "수익률표준편차": "{:.2f}",
                                        "최악MDD": "{:+.2f}%",
                                        "평균승률": "{:.1f}%",
                                        "평균기대값": "{:+.2f}%",
                                        "평균보유종목수": "{:.2f}",
                                    }),
                                    hide_index=True,
                                    column_order=sensitivity_cols,
                                    width="stretch",
                                )
                                with st.expander("시작일별 상세", expanded=False):
                                    detail_cols = [
                                        "프로필", "시작일", "전략수익률(%)", "MDD(%)",
                                        "승률(%)", "거래당기대값(%)", "손익비", "평균보유종목수", "종료거래"
                                    ]
                                    detail_cols = [c for c in detail_cols if c in sensitivity_detail.columns]
                                    st.dataframe(
                                        sensitivity_detail[detail_cols].style.format({
                                            "전략수익률(%)": "{:+.2f}%",
                                            "MDD(%)": "{:+.2f}%",
                                            "승률(%)": "{:.1f}%",
                                            "거래당기대값(%)": "{:+.2f}%",
                                            "손익비": "{:.2f}x",
                                            "평균보유종목수": "{:.2f}",
                                        }),
                                        hide_index=True,
                                        column_order=detail_cols,
                                        width="stretch",
                                    )
                        except Exception as e:
                            st.warning(f"공격/방어 민감도 분석 중 오류가 발생했습니다: {e}")

                if not new_candidates.empty:
                    today_view = new_candidates.copy()
                    today_view["추천구분"] = "신규 후보"
                    today_cols = [
                        "종목명", "전략슬리브", "진입유형", "스윙우선순위",
                        "AI수급점수", "현재가", "매도점검", "추천구분"
                    ]
                    today_cols = [c for c in today_cols if c in today_view.columns]
                    st.markdown("#### 오늘 신규 스윙 후보")
                    st.dataframe(
                        today_view[today_cols].style.format({
                            "스윙우선순위": "{:.2f}",
                            "AI수급점수": "{:.2f}",
                            "현재가": "{:,.0f}원",
                        }),
                        hide_index=True,
                        column_order=today_cols,
                        width='stretch'
                    )

                if not primary_open_trades.empty:
                    open_view = primary_open_trades.copy()
                    current_signal_cols = ["종목명", "스윙우선순위", "현재_순위", "매수후보", "진입유형", "매도점검"]
                    signal_now = df_summary[[c for c in current_signal_cols if c in df_summary.columns]].copy()
                    open_view = pd.merge(open_view, signal_now, on="종목명", how="left")
                    open_view["매도알림"] = open_view["매도점검"].fillna("보유/관찰").astype(str).apply(
                        lambda x: "매도 점검" if any(k in x for k in ["매도", "제외", "훼손", "축소", "주의", "청산", "이탈"]) else "보유"
                    )
                    sell_alerts = open_view[open_view["매도알림"].eq("매도 점검")].copy()
                    if not sell_alerts.empty:
                        st.warning("진행 중 포지션 중 매도 점검 신호가 있습니다.")
                        st.dataframe(
                            sell_alerts[["종목명", "평가수익률", "스윙우선순위", "매수후보", "진입유형", "매도점검"]].style.format({
                                "평가수익률": "{:+.2f}%",
                                "스윙우선순위": "{:.2f}",
                            }),
                            hide_index=True,
                            column_order=["종목명", "평가수익률", "스윙우선순위", "매수후보", "진입유형", "매도점검"],
                            width="stretch",
                        )

                    open_summary = open_view.rename(columns={
                        "진입일": "최근진입일",
                        "평가수익률": "최근평가수익률",
                        "스윙우선순위": "최근점수",
                    }).copy()
                    open_summary["신호횟수"] = 1
                    open_summary["최고순위"] = pd.to_numeric(open_summary.get("현재_순위", 0), errors="coerce").fillna(0).astype(int)
                    open_summary["평균평가수익률"] = pd.to_numeric(open_summary["최근평가수익률"], errors="coerce").fillna(0.0)
                    open_summary["최근점수"] = pd.to_numeric(open_summary["최근점수"], errors="coerce").fillna(0.0)
                    open_summary["방향"] = open_summary["최근평가수익률"].apply(lambda v: "plus" if v >= 0 else "minus")
                    open_summary = open_summary.sort_values(["최근평가수익률", "최근점수"], ascending=[False, False])

                    st.markdown("#### 진행 중 종목 요약")
                    base_open_chart = alt.Chart(open_summary).encode(
                        y=alt.Y(
                            "종목명:N",
                            sort="-x",
                            title=None,
                            axis=alt.Axis(labelLimit=180, labelPadding=6),
                        ),
                        x=alt.X("최근평가수익률:Q", title="현재평가수익률 (%)"),
                    )
                    open_bar = base_open_chart.mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4).encode(
                        color=alt.Color(
                            "방향:N",
                            scale=alt.Scale(domain=["plus", "minus"], range=["#36C06A", "#E04B4B"]),
                            legend=None,
                        ),
                        tooltip=[
                            alt.Tooltip("종목명:N"),
                            alt.Tooltip("최근평가수익률:Q", format="+.2f", title="최근 평가수익률"),
                            alt.Tooltip("평균평가수익률:Q", format="+.2f", title="평균 평가수익률"),
                            alt.Tooltip("신호횟수:Q", title="신호 횟수"),
                            alt.Tooltip("최근진입일:N", title="최근 진입일"),
                        ],
                    )
                    open_text = base_open_chart.mark_text(
                        align="left",
                        baseline="middle",
                        dx=5,
                        color="#CBD5E1",
                        fontSize=11,
                    ).encode(text=alt.Text("최근평가수익률:Q", format="+.1f"))
                    open_chart = (open_bar + open_text).properties(height=max(220, min(520, 32 * len(open_summary))))
                    st.altair_chart(apply_altair_theme(open_chart), width="stretch")

                    summary_cols = [
                        "종목명", "최근진입일", "보유일수", "진입가", "현재가", "수량",
                        "최고순위", "최근점수", "최근평가수익률", "매도알림"
                    ]
                    summary_cols = [c for c in summary_cols if c in open_summary.columns]
                    st.dataframe(
                        open_summary[summary_cols].style.format({
                            "진입가": "{:,.0f}",
                            "현재가": "{:,.0f}",
                            "수량": "{:,.0f}",
                            "최근점수": "{:.2f}",
                            "최근평가수익률": "{:+.2f}%",
                        }),
                        hide_index=True,
                        column_order=summary_cols,
                        width='stretch'
                    )
                else:
                    latest_perf_row = df_filtered.sort_values("날짜_dt").iloc[-1] if not df_filtered.empty else pd.Series(dtype=object)
                    latest_mode = str(latest_perf_row.get("시장모드", score_mode_label))
                    target_count = int(pd.to_numeric(latest_perf_row.get("목표보유종목수", 0), errors="coerce") or 0)
                    cash_value = float(pd.to_numeric(latest_perf_row.get("현금", 0.0), errors="coerce") or 0.0)
                    st.info(
                        f"현재 {score_mode_label} 모드는 {latest_mode} 상태입니다. "
                        f"목표 보유 종목수 {target_count}개로 판단되어 현금 대기 중입니다. "
                        f"현금 {cash_value:,.0f}원 기준으로 다음 진입 신호를 기다립니다."
                    )

                if not closed_trades.empty:
                    portfolio_log_cols = ["진입일", "청산일", "종목명", "보유일수", "수량", "진입가", "청산가", "매수금액", "청산금액", "실현손익", "수익률", "청산사유"]
                    legacy_log_cols = ["진입일", "청산일", "종목명", "진입순위", "추천소스", "보유일수", "청산방식", "청산사유", "진입가", "청산가", "수익률", "진입유형"]
                    view_cols = [c for c in portfolio_log_cols if c in closed_trades.columns]
                    if len(view_cols) < 5:
                        view_cols = [c for c in legacy_log_cols if c in closed_trades.columns]
                    trade_view = closed_trades.sort_values("청산일_dt", ascending=False)[view_cols].head(80)
                    with st.expander("포트폴리오 진입/청산 거래 로그", expanded=False):
                        format_cols = {
                            "수량": "{:.0f}",
                            "진입가": "{:,.0f}",
                            "청산가": "{:,.0f}",
                            "매수금액": "{:,.0f}원",
                            "청산금액": "{:,.0f}원",
                            "실현손익": "{:+,.0f}원",
                            "진입가": "{:,.0f}",
                            "청산가": "{:,.0f}",
                            "수익률": "{:+.2f}%",
                        }
                        st.dataframe(
                            trade_view.style.format({k: v for k, v in format_cols.items() if k in trade_view.columns}),
                            hide_index=True,
                            column_order=view_cols,
                            width='stretch'
                        )
            else:
                render_empty_state("백테스트 데이터 없음", "선택하신 기간에 해당하는 스윙 성과 데이터가 없습니다.")
        else:
            render_empty_state("데이터 대기", "swing_performance.csv가 아직 생성되지 않았습니다. 다음 스크래퍼 실행 후 표시됩니다.")

