from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from services.portfolio_simulator_service import simulate_portfolio


@dataclass(frozen=True)
class StrategyMode:
    label: str
    score_mode: str
    adaptive_profile: str = "현재값"


DEFAULT_STRATEGY_MODES = [
    StrategyMode("공격/방어 v1", "adaptive", "현재값"),
    StrategyMode("공격/방어 v2", "adaptive", "v2 견고형"),
    StrategyMode("공격/방어 v3", "adaptive", "v3 상대강도"),
    StrategyMode("스윙점수", "swing", "현재값"),
    StrategyMode("AI점수", "ai", "현재값"),
]

DEFAULT_ADAPTIVE_PROFILES = ["현재값", "v2 견고형", "v3 상대강도"]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def _date_candidates(available_dates, selected_start_date, limit=10, sample_evenly=False):
    date_series = pd.Series(pd.to_datetime(available_dates, errors="coerce")).dropna()
    if date_series.empty:
        return []
    selected = pd.to_datetime(selected_start_date, errors="coerce")
    if pd.isna(selected):
        selected = date_series.min()
    start_candidates = pd.Series(date_series.dt.date.unique()).sort_values().tolist()
    start_candidates = [d for d in start_candidates if d >= selected.date()]
    if not sample_evenly:
        return start_candidates[: int(limit)]
    if len(start_candidates) > int(limit):
        positions = sorted(set([0, len(start_candidates) - 1] + [
            round(i * (len(start_candidates) - 1) / max(1, int(limit) - 1)) for i in range(int(limit))
        ]))
        start_candidates = [start_candidates[i] for i in positions[: int(limit)]]
    return start_candidates


def _avg_holding_days(closed_trades: pd.DataFrame) -> float:
    if closed_trades is None or closed_trades.empty or "보유일수" not in closed_trades.columns:
        return 0.0
    holds = pd.to_numeric(closed_trades["보유일수"], errors="coerce").dropna()
    return float(holds.mean()) if not holds.empty else 0.0


def _avg_cash_ratio(daily_performance: pd.DataFrame) -> float:
    if daily_performance is None or daily_performance.empty:
        return 100.0
    if "현금" not in daily_performance.columns or "평가금액" not in daily_performance.columns:
        return 0.0
    cash = pd.to_numeric(daily_performance["현금"], errors="coerce")
    equity = pd.to_numeric(daily_performance["평가금액"], errors="coerce")
    ratio = (cash / equity.replace(0, pd.NA)).dropna()
    return float(ratio.mean() * 100.0) if not ratio.empty else 0.0


def _avg_position_count(daily_performance: pd.DataFrame) -> float:
    if daily_performance is None or daily_performance.empty or "보유종목수" not in daily_performance.columns:
        return 0.0
    positions = pd.to_numeric(daily_performance["보유종목수"], errors="coerce").dropna()
    return float(positions.mean()) if not positions.empty else 0.0


def _legacy_mdd_from_return_column(daily_performance: pd.DataFrame) -> float:
    if daily_performance is None or daily_performance.empty or "수익률(%)" not in daily_performance.columns:
        return 0.0
    returns = pd.to_numeric(daily_performance["수익률(%)"], errors="coerce").fillna(0.0)
    equity = 1.0 + (returns / 100.0)
    return float(((equity / equity.cummax()) - 1.0).min() * 100.0) if not equity.empty else 0.0


def summarize_simulation_result(
    result: dict,
    label: str | None = None,
    score_mode: str | None = None,
    adaptive_profile: str | None = None,
    start_date: Any = None,
) -> dict:
    summary = result.get("summary", {}) if isinstance(result, dict) else {}
    daily = result.get("daily_performance", pd.DataFrame()) if isinstance(result, dict) else pd.DataFrame()
    closed = result.get("closed_trades", pd.DataFrame()) if isinstance(result, dict) else pd.DataFrame()
    row = {
        "전략": label or "",
        "점수모드": score_mode or "",
        "프로필": adaptive_profile or "",
        "시작일": pd.to_datetime(start_date).strftime("%Y-%m-%d") if start_date is not None and not pd.isna(pd.to_datetime(start_date, errors="coerce")) else "",
        "전략수익률(%)": _safe_float(summary.get("total_return_pct")),
        "MDD(%)": _safe_float(summary.get("max_drawdown_pct")),
        "승률(%)": _safe_float(summary.get("win_rate")),
        "손익비": _safe_float(summary.get("payoff_ratio")),
        "거래당기대값(%)": _safe_float(summary.get("expectancy")),
        "종료거래": _safe_int(summary.get("closed_trades")),
        "현금비중(%)": _safe_float(summary.get("cash_ratio")) * 100.0,
        "평균현금비중(%)": _avg_cash_ratio(daily),
        "평균보유종목수": _avg_position_count(daily),
        "평균보유일": _avg_holding_days(closed),
    }
    return row


def evaluate_strategy_modes(
    df_trades,
    df_history,
    selected_start_date=None,
    initial_cash=5_000_000,
    max_positions=3,
    modes=None,
):
    modes = modes or DEFAULT_STRATEGY_MODES
    rows = []
    runs = {}
    for mode in modes:
        if isinstance(mode, dict):
            strategy = StrategyMode(
                label=mode.get("label", ""),
                score_mode=mode.get("score_mode", "swing"),
                adaptive_profile=mode.get("adaptive_profile", "현재값"),
            )
        else:
            strategy = mode
        result = simulate_portfolio(
            df_trades,
            df_history,
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=selected_start_date,
            score_mode=strategy.score_mode,
            adaptive_profile=strategy.adaptive_profile,
        )
        rows.append(summarize_simulation_result(
            result,
            label=strategy.label,
            score_mode=strategy.score_mode,
            adaptive_profile=strategy.adaptive_profile,
            start_date=selected_start_date,
        ))
        runs[strategy.label] = result
    summary = pd.DataFrame(rows)
    return {
        "summary": summary,
        "runs": runs,
        "overfit_risk": detect_overfit_risk(summary),
    }


def build_start_date_stability(
    df_trades,
    df_history,
    available_dates,
    selected_start_date,
    initial_cash,
    max_positions,
    limit=10,
    score_mode="swing",
    adaptive_profile="현재값",
):
    rows = []
    for start_d in _date_candidates(available_dates, selected_start_date, limit=limit, sample_evenly=False):
        result = simulate_portfolio(
            df_trades,
            df_history,
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_d,
            score_mode=score_mode,
            adaptive_profile=adaptive_profile,
        )
        daily = result.get("daily_performance", pd.DataFrame())
        if daily.empty:
            continue
        row = summarize_simulation_result(
            result,
            score_mode=score_mode,
            adaptive_profile=adaptive_profile,
            start_date=start_d,
        )
        row["MDD(%)"] = _legacy_mdd_from_return_column(daily)
        rows.append({
            "시작일": row["시작일"],
            "전략수익률(%)": row["전략수익률(%)"],
            "MDD(%)": row["MDD(%)"],
            "승률(%)": row["승률(%)"],
            "손익비": row["손익비"],
            "거래당기대값(%)": row["거래당기대값(%)"],
            "종료거래": row["종료거래"],
            "현금비중(%)": row["현금비중(%)"],
            "평균현금비중(%)": row["평균현금비중(%)"],
            "평균보유종목수": row["평균보유종목수"],
            "평균보유일": row["평균보유일"],
        })
    return pd.DataFrame(rows)


def build_adaptive_threshold_sensitivity(
    df_trades,
    df_history,
    available_dates,
    selected_start_date,
    initial_cash,
    max_positions,
    limit=5,
    profiles=None,
):
    start_candidates = _date_candidates(
        available_dates,
        selected_start_date,
        limit=limit,
        sample_evenly=True,
    )
    detail_rows = []
    for profile_name in profiles or DEFAULT_ADAPTIVE_PROFILES:
        for start_d in start_candidates:
            result = simulate_portfolio(
                df_trades,
                df_history,
                initial_cash=initial_cash,
                max_positions=max_positions,
                start_date=start_d,
                score_mode="adaptive",
                adaptive_profile=profile_name,
            )
            daily = result.get("daily_performance", pd.DataFrame())
            if daily.empty:
                continue
            row = summarize_simulation_result(
                result,
                label=profile_name,
                score_mode="adaptive",
                adaptive_profile=profile_name,
                start_date=start_d,
            )
            row["MDD(%)"] = _legacy_mdd_from_return_column(daily)
            detail_rows.append({
                "프로필": profile_name,
                "시작일": row["시작일"],
                "전략수익률(%)": row["전략수익률(%)"],
                "MDD(%)": row["MDD(%)"],
                "승률(%)": row["승률(%)"],
                "거래당기대값(%)": row["거래당기대값(%)"],
                "손익비": row["손익비"],
                "평균보유종목수": row["평균보유종목수"],
                "평균현금비중(%)": row["평균현금비중(%)"],
                "평균보유일": row["평균보유일"],
                "종료거래": row["종료거래"],
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
            평균현금비중=("평균현금비중(%)", "mean"),
            평균보유일=("평균보유일", "mean"),
            음수시작일수=("전략수익률(%)", lambda s: int((s < 0).sum())),
        )
    )
    summary["수익률표준편차"] = summary["수익률표준편차"].fillna(0.0)
    summary["양수시작비율"] = (
        detail.groupby("프로필")["전략수익률(%)"].apply(lambda s: float((s > 0).mean() * 100.0)).values
    )
    summary["견고성점수"] = (
        summary["최저수익률"].clip(lower=-50, upper=100)
        + summary["평균수익률"] * 0.35
        + summary["평균기대값"] * 3.0
        + summary["최악MDD"].clip(lower=-80, upper=0) * 0.45
        - summary["수익률표준편차"].fillna(0.0) * 0.25
    )
    summary = summary.sort_values("견고성점수", ascending=False)
    summary["과최적화위험"] = summary.apply(_profile_overfit_label, axis=1)
    return summary, detail


def _profile_overfit_label(row) -> str:
    if _safe_float(row.get("최저수익률")) < 0 and _safe_float(row.get("수익률표준편차")) >= 25:
        return "높음"
    if _safe_float(row.get("양수시작비율")) < 70 or _safe_float(row.get("음수시작일수")) >= 2:
        return "주의"
    return "낮음"


def detect_overfit_risk(evaluation_df, return_col="전략수익률(%)") -> dict:
    if evaluation_df is None or evaluation_df.empty or return_col not in evaluation_df.columns:
        return {
            "risk_level": "unknown",
            "reason": "평가 가능한 결과가 없습니다.",
            "return_spread": 0.0,
            "return_std": 0.0,
            "positive_ratio": 0.0,
        }
    returns = pd.to_numeric(evaluation_df[return_col], errors="coerce").dropna()
    if returns.empty:
        return {
            "risk_level": "unknown",
            "reason": "수익률 컬럼이 비어 있습니다.",
            "return_spread": 0.0,
            "return_std": 0.0,
            "positive_ratio": 0.0,
        }
    spread = float(returns.max() - returns.min())
    std = float(returns.std()) if len(returns) > 1 else 0.0
    positive_ratio = float((returns > 0).mean() * 100.0)
    worst = float(returns.min())
    if worst < 0 and (spread >= 40 or std >= 20 or positive_ratio < 60):
        level = "high"
        reason = "일부 시작일/파라미터에서만 성과가 좋고 손실 구간이 섞여 있습니다."
    elif spread >= 25 or std >= 12 or positive_ratio < 75:
        level = "medium"
        reason = "성과 편차가 있어 추가 기간 검증이 필요합니다."
    else:
        level = "low"
        reason = "현재 평가 묶음에서는 성과 편차가 제한적입니다."
    return {
        "risk_level": level,
        "reason": reason,
        "return_spread": spread,
        "return_std": std,
        "positive_ratio": positive_ratio,
        "worst_return": worst,
        "best_return": float(returns.max()),
    }
