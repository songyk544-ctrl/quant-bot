from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from repositories.data_repository import (
    load_data,
    load_score_trend_safe,
    load_swing_trades_safe,
)
from services.gpt_datapack_service import (
    DATA_OPTIONS,
    TEMPLATE_PROMPTS,
    build_prompt,
    build_stock_data_pack,
    format_markdown_table,
    load_history_for_datapack,
)
from services.portfolio_advisor_service import (
    build_replacement_pool,
    enrich_portfolio_holdings,
    portfolio_assistant_verdict,
)
from services.portfolio_simulator_service import simulate_portfolio
from services.recommendation_validation_service import build_recommendation_validation
from services.strategy_evaluation_service import (
    build_adaptive_threshold_sensitivity,
    build_start_date_stability,
    detect_overfit_risk,
    evaluate_strategy_modes,
)


DEFAULT_INITIAL_CASH = 5_000_000
DEFAULT_MAX_POSITIONS = 3
DEFAULT_ADAPTIVE_PROFILE = "v2 견고형"


def _ok(summary=None, data=None, warnings=None, next_actions=None, **extra):
    return {
        "status": "ok",
        "error": None,
        "summary": summary or {},
        "data": data or {},
        "warnings": warnings or [],
        "next_actions": next_actions or [],
        **extra,
    }


def _fail(error, summary=None, data=None, warnings=None, next_actions=None, **extra):
    return {
        "status": "error",
        "error": str(error),
        "summary": summary or {},
        "data": data or {},
        "warnings": warnings or [],
        "next_actions": next_actions or ["입력값과 최신 CSV 데이터 상태를 확인하세요."],
        **extra,
    }


def _records(df, limit=20):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    out = df.head(int(limit)).copy()
    out = out.where(pd.notna(out), None)
    return out.to_dict(orient="records")


def _tail_records(df, limit=20):
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []
    out = df.tail(int(limit)).copy()
    out = out.where(pd.notna(out), None)
    return out.to_dict(orient="records")


def _shape(df):
    return list(df.shape) if isinstance(df, pd.DataFrame) else [0, 0]


def _load_core_frames():
    df_summary, df_history = load_data()
    df_trades = load_swing_trades_safe()
    df_score = load_score_trend_safe()
    return df_summary, df_history, df_trades, df_score


def _available_dates(df_trades, df_history=None):
    if df_trades is not None and not df_trades.empty:
        if "진입일_dt" in df_trades.columns:
            dates = pd.to_datetime(df_trades["진입일_dt"], errors="coerce").dropna()
            if not dates.empty:
                return dates
        if "진입일" in df_trades.columns:
            dates = pd.to_datetime(df_trades["진입일"], errors="coerce").dropna()
            if not dates.empty:
                return dates
    if df_history is not None and not df_history.empty and "일자" in df_history.columns:
        dates = pd.to_datetime(
            df_history["일자"].astype(str).str.replace("-", "", regex=False),
            format="%Y%m%d",
            errors="coerce",
        ).dropna()
        if dates.empty:
            dates = pd.to_datetime(df_history["일자"], errors="coerce").dropna()
        return dates
    return pd.Series(dtype="datetime64[ns]")


def _default_start_date(available_dates, lookback_days=31):
    dates = pd.to_datetime(available_dates, errors="coerce").dropna()
    if dates.empty:
        return None
    min_date = dates.min().date()
    max_date = dates.max().date()
    target = (pd.Timestamp(max_date) - pd.Timedelta(days=int(lookback_days))).date()
    return max(min_date, min(max_date, target))


def _portfolio_frame():
    for name in ("my_portfolio.csv", "portfolio.csv"):
        root_path = Path(name)
        data_path = Path("data") / name
        df = pd.DataFrame()
        for path in (root_path, data_path):
            if path.exists():
                try:
                    df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
                    break
                except Exception:
                    df = pd.DataFrame()
        if df is not None and not df.empty:
            df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
            return df
    return pd.DataFrame()


def simulate_portfolio_tool(
    initial_cash=DEFAULT_INITIAL_CASH,
    max_positions=DEFAULT_MAX_POSITIONS,
    start_date=None,
    score_mode="adaptive",
    adaptive_profile=DEFAULT_ADAPTIVE_PROFILE,
    row_limit=20,
):
    """Run one portfolio simulation and return an agent-friendly dict."""
    try:
        _, df_history, df_trades, _ = _load_core_frames()
        available = _available_dates(df_trades, df_history)
        start_date = start_date or _default_start_date(available)
        result = simulate_portfolio(
            df_trades,
            df_history,
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_date,
            score_mode=score_mode,
            adaptive_profile=adaptive_profile,
        )
        summary = dict(result.get("summary", {}))
        return _ok(
            summary={
                **summary,
                "start_date": str(start_date),
                "score_mode": score_mode,
                "adaptive_profile": adaptive_profile,
            },
            data={
                "daily_performance_tail": _tail_records(result.get("daily_performance"), row_limit),
                "open_positions": _records(result.get("open_positions"), row_limit),
                "closed_trades_tail": _tail_records(result.get("closed_trades"), row_limit),
                "shapes": {
                    "daily_performance": _shape(result.get("daily_performance")),
                    "open_positions": _shape(result.get("open_positions")),
                    "closed_trades": _shape(result.get("closed_trades")),
                },
            },
            warnings=[] if not df_trades.empty and not df_history.empty else ["백테스트 원천 데이터가 비어 있습니다."],
            next_actions=["필요하면 check_overfit_risk_tool로 시작일 민감도를 확인하세요."],
        )
    except Exception as exc:
        return _fail(exc)


def compare_strategy_modes_tool(
    initial_cash=DEFAULT_INITIAL_CASH,
    max_positions=DEFAULT_MAX_POSITIONS,
    start_date=None,
    lookback_days=31,
    row_limit=20,
):
    """Compare swing, AI, and adaptive strategy modes from the same start date."""
    try:
        _, df_history, df_trades, _ = _load_core_frames()
        available = _available_dates(df_trades, df_history)
        start_date = start_date or _default_start_date(available, lookback_days=lookback_days)
        result = evaluate_strategy_modes(
            df_trades,
            df_history,
            selected_start_date=start_date,
            initial_cash=initial_cash,
            max_positions=max_positions,
        )
        summary_df = result.get("summary", pd.DataFrame())
        best = {}
        if not summary_df.empty and "전략수익률(%)" in summary_df.columns:
            best = summary_df.sort_values("전략수익률(%)", ascending=False).head(1).to_dict(orient="records")[0]
        return _ok(
            summary={
                "start_date": str(start_date),
                "best_strategy": best,
                "overfit_risk": result.get("overfit_risk", {}),
            },
            data={
                "mode_summary": _records(summary_df, row_limit),
            },
            warnings=[],
            next_actions=["최고 수익률만 보지 말고 MDD와 현금비중을 같이 확인하세요."],
        )
    except Exception as exc:
        return _fail(exc)


def check_overfit_risk_tool(
    initial_cash=DEFAULT_INITIAL_CASH,
    max_positions=DEFAULT_MAX_POSITIONS,
    selected_start_date=None,
    score_mode="adaptive",
    adaptive_profile=DEFAULT_ADAPTIVE_PROFILE,
    stability_limit=8,
    sensitivity_limit=5,
    row_limit=20,
):
    """Check whether performance is concentrated in a few start dates or profiles."""
    try:
        _, df_history, df_trades, _ = _load_core_frames()
        available = _available_dates(df_trades, df_history)
        selected_start_date = selected_start_date or _default_start_date(available, lookback_days=186)
        stability = build_start_date_stability(
            df_trades,
            df_history,
            available,
            selected_start_date,
            initial_cash,
            max_positions,
            limit=stability_limit,
            score_mode=score_mode,
            adaptive_profile=adaptive_profile,
        )
        sensitivity_summary, sensitivity_detail = build_adaptive_threshold_sensitivity(
            df_trades,
            df_history,
            available,
            selected_start_date,
            initial_cash,
            max_positions,
            limit=sensitivity_limit,
        )
        risk = detect_overfit_risk(stability)
        return _ok(
            summary={
                "selected_start_date": str(selected_start_date),
                "risk": risk,
                "stability_rows": int(len(stability)),
                "sensitivity_profiles": int(len(sensitivity_summary)),
            },
            data={
                "start_date_stability": _records(stability, row_limit),
                "adaptive_sensitivity_summary": _records(sensitivity_summary, row_limit),
                "adaptive_sensitivity_detail": _records(sensitivity_detail, row_limit),
            },
            warnings=["시작일 표본 수가 적으면 과최적화 판단 신뢰도가 낮습니다."] if len(stability) < 5 else [],
            next_actions=["기간을 6개월 이상으로 넓혀 같은 판단이 유지되는지 확인하세요."],
        )
    except Exception as exc:
        return _fail(exc)


def get_strategy_health_report_tool(
    initial_cash=DEFAULT_INITIAL_CASH,
    max_positions=DEFAULT_MAX_POSITIONS,
    start_date=None,
    lookback_days=31,
):
    """Return a compact health report for the current strategy dashboard."""
    try:
        sim = simulate_portfolio_tool(
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_date,
            score_mode="adaptive",
            adaptive_profile=DEFAULT_ADAPTIVE_PROFILE,
            row_limit=10,
        )
        compare = compare_strategy_modes_tool(
            initial_cash=initial_cash,
            max_positions=max_positions,
            start_date=start_date,
            lookback_days=lookback_days,
            row_limit=10,
        )
        if sim["status"] != "ok":
            return sim
        if compare["status"] != "ok":
            return compare
        sim_summary = sim.get("summary", {})
        risk_notes = []
        if float(sim_summary.get("max_drawdown_pct", 0.0) or 0.0) <= -15:
            risk_notes.append("MDD가 -15% 이하입니다. 신규 진입보다 방어 점검이 우선입니다.")
        if float(sim_summary.get("win_rate", 0.0) or 0.0) < 45:
            risk_notes.append("승률이 45% 미만입니다. 손익비와 손절 기준을 같이 확인하세요.")
        return _ok(
            summary={
                "portfolio": sim_summary,
                "mode_comparison": compare.get("summary", {}),
            },
            data={
                "open_positions": sim.get("data", {}).get("open_positions", []),
                "mode_summary": compare.get("data", {}).get("mode_summary", []),
            },
            warnings=risk_notes,
            next_actions=[
                "check_overfit_risk_tool로 시작일별 안정성을 확인하세요.",
                "build_stock_datapack_tool로 보유 종목의 최근 주가/수급 데이터를 GPT에 넘겨 정성 검토하세요.",
            ],
        )
    except Exception as exc:
        return _fail(exc)


def get_recommendation_validation_tool(score_mode="adaptive", max_positions=DEFAULT_MAX_POSITIONS, row_limit=20):
    """Validate recommendation quality by recommendation date and market regime."""
    try:
        df_summary, df_history, df_trades, df_score = _load_core_frames()
        daily, detail, regime = build_recommendation_validation(
            score_trend=df_score,
            swing_trades=df_trades,
            history=df_history,
            score_mode=score_mode,
            max_positions=max_positions,
        )
        return _ok(
            summary={
                "score_mode": score_mode,
                "daily_rows": int(len(daily)),
                "detail_rows": int(len(detail)),
                "regime_rows": int(len(regime)),
            },
            data={
                "daily_summary_tail": _tail_records(daily, row_limit),
                "detail_tail": _tail_records(detail, row_limit),
                "regime_summary": _records(regime, row_limit),
            },
            warnings=[] if not daily.empty else ["추천 검증 결과가 비어 있습니다."],
            next_actions=["하락장/혼조장 승률이 낮다면 신규후보 제한 조건을 강화하세요."],
        )
    except Exception as exc:
        return _fail(exc)


def build_stock_datapack_tool(
    stock_name,
    days=60,
    selected_labels=None,
    template_name="종합 검토",
    include_internal=False,
    max_prompt_chars=12_000,
):
    """Build a Markdown datapack and prompt for external GPT analysis."""
    try:
        df_summary, _ = load_data()
        df_history = load_history_for_datapack()
        selected_labels = selected_labels or list(DATA_OPTIONS.keys())
        if template_name not in TEMPLATE_PROMPTS:
            template_name = "종합 검토"
        pack = build_stock_data_pack(df_history, stock_name, days, selected_labels)
        table_text = format_markdown_table(pack)
        prompt = build_prompt(stock_name, days, template_name, table_text, include_internal, df_summary)
        if len(prompt) > int(max_prompt_chars):
            prompt = prompt[: int(max_prompt_chars)] + "\n...(길이 제한으로 일부 생략)"
        return _ok(
            summary={
                "stock_name": stock_name,
                "days": int(days),
                "rows": int(len(pack)),
                "columns": list(pack.columns) if not pack.empty else [],
                "template_name": template_name,
            },
            data={
                "markdown_table": table_text,
                "prompt": prompt,
                "records": _records(pack, int(days)),
            },
            warnings=[] if not pack.empty else [f"{stock_name} 데이터가 없습니다."],
            next_actions=["프롬프트를 GPT에 붙여넣고 기술적 추세, 수급 지속성, 손절 기준을 따로 물어보세요."],
        )
    except Exception as exc:
        return _fail(exc)


def get_portfolio_advice_tool(row_limit=20, capital_guard_active=False, capital_emergency_active=False):
    """Inspect current holdings and return assistant-style portfolio advice."""
    try:
        df_summary, _ = load_data()
        df_portfolio = _portfolio_frame()
        enriched, portfolio_summary = enrich_portfolio_holdings(df_portfolio, df_summary)
        if enriched.empty:
            return _ok(
                summary={"portfolio": portfolio_summary, "verdict": "보유 데이터 없음"},
                data={"holdings": []},
                warnings=["my_portfolio.csv 또는 portfolio.csv에 보유 종목이 없습니다."],
                next_actions=["보유 종목 CSV를 최신 상태로 저장하세요."],
            )
        risk_mask = pd.Series(False, index=enriched.index)
        for col in ["수급이탈위험", "매도점검위험"]:
            if col in enriched.columns:
                risk_mask = risk_mask | enriched[col].fillna(False).astype(bool)
        risk_rows = enriched[risk_mask].copy()
        held_names = set(enriched.get("종목명", pd.Series(dtype=str)).astype(str).tolist())
        replacement_pool = build_replacement_pool(
            df_summary,
            held_names,
            risk_rows,
            capital_guard_active=capital_guard_active or capital_emergency_active,
        )
        verdict = portfolio_assistant_verdict(
            risk_rows,
            replacement_pool,
            capital_guard_active=capital_guard_active,
            capital_emergency_active=capital_emergency_active,
        )
        return _ok(
            summary={
                "portfolio": portfolio_summary,
                "verdict": verdict,
                "holding_count": int(len(enriched)),
                "risk_count": int(len(risk_rows)),
                "replacement_count": int(len(replacement_pool)),
            },
            data={
                "holdings": _records(enriched, row_limit),
                "risk_rows": _records(risk_rows, row_limit),
                "replacement_pool": _records(replacement_pool, row_limit),
            },
            warnings=[] if risk_rows.empty else ["보유 종목 중 리스크 점검 대상이 있습니다."],
            next_actions=["리스크 종목은 수급 훼손과 매도점검 문구를 먼저 확인하세요."],
        )
    except Exception as exc:
        return _fail(exc)


__all__ = [
    "build_stock_datapack_tool",
    "check_overfit_risk_tool",
    "compare_strategy_modes_tool",
    "get_portfolio_advice_tool",
    "get_recommendation_validation_tool",
    "get_strategy_health_report_tool",
    "simulate_portfolio_tool",
]
