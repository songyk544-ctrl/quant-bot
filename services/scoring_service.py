import re
from datetime import date

import pandas as pd


ADAPTIVE_THRESHOLD_PROFILES = {
    "현재값": {
        "severe_avg_ret": -1.2,
        "severe_up_ratio": 0.32,
        "severe_ma20_ratio": 0.38,
        "weak_avg_ret": -0.35,
        "weak_up_ratio": 0.43,
        "weak_ma20_ratio": 0.46,
        "attack_score": 65.0,
        "attack_up_ratio": 0.52,
        "attack_ma20_ratio": 0.52,
        "leader_score": 58.0,
        "leader_up_ratio": 0.48,
    },
    "v2 견고형": {
        "severe_avg_ret": -1.3057,
        "severe_up_ratio": 0.2851,
        "severe_ma20_ratio": 0.3951,
        "weak_avg_ret": -0.4783,
        "weak_up_ratio": 0.4382,
        "weak_ma20_ratio": 0.4688,
        "attack_score": 62.406,
        "attack_up_ratio": 0.5306,
        "attack_ma20_ratio": 0.493,
        "leader_score": 58.0355,
        "leader_up_ratio": 0.4549,
    },
    "v3 상대강도": {
        "severe_avg_ret": -1.3057,
        "severe_up_ratio": 0.2851,
        "severe_ma20_ratio": 0.3951,
        "weak_avg_ret": -0.4783,
        "weak_up_ratio": 0.4382,
        "weak_ma20_ratio": 0.4688,
        "attack_score": 62.406,
        "attack_up_ratio": 0.5306,
        "attack_ma20_ratio": 0.493,
        "leader_score": 58.0355,
        "leader_up_ratio": 0.4549,
    },
    "v4 수급확인": {
        "severe_avg_ret": -1.3057,
        "severe_up_ratio": 0.2851,
        "severe_ma20_ratio": 0.3951,
        "weak_avg_ret": -0.4783,
        "weak_up_ratio": 0.4382,
        "weak_ma20_ratio": 0.4688,
        "attack_score": 62.406,
        "attack_up_ratio": 0.5306,
        "attack_ma20_ratio": 0.493,
        "leader_score": 58.0355,
        "leader_up_ratio": 0.4549,
    },
}


V5_FLOW_WEIGHTS = {
    "대형": {"외국인": 0.45, "연기금": 0.25, "투신사모": 0.15, "지속성": 0.15},
    "중형": {"외국인": 0.25, "연기금": 0.40, "투신사모": 0.20, "지속성": 0.15},
    "소형": {"외국인": 0.10, "연기금": 0.50, "투신사모": 0.25, "지속성": 0.15},
}


def classify_stock_tier(market_cap_100m, avg_trading_value_20d_100m):
    """시가총액과 20일 평균 거래대금으로 수급 해석에 사용할 체급을 정한다."""
    market_cap_value = pd.to_numeric(market_cap_100m, errors="coerce")
    trading_value_value = pd.to_numeric(avg_trading_value_20d_100m, errors="coerce")
    market_cap = float(market_cap_value) if pd.notna(market_cap_value) else 0.0
    trading_value = float(trading_value_value) if pd.notna(trading_value_value) else 0.0
    if market_cap >= 100_000 or trading_value >= 1_000:
        return "대형"
    if market_cap >= 10_000 or trading_value >= 200:
        return "중형"
    return "소형"


def calculate_size_aware_flow_score(
    stock_tier,
    foreign_rank,
    pension_rank,
    trust_private_rank,
    persistence_ratio,
    foreign_positive=True,
    pension_positive=True,
    trust_private_positive=True,
    liquidity_ok=True,
    return_details=False,
):
    """체급별 핵심 매수 주체의 상대순위와 지속성을 0~100점으로 환산한다."""
    tier = stock_tier if stock_tier in V5_FLOW_WEIGHTS else "소형"
    weights = V5_FLOW_WEIGHTS[tier]

    def _pct(value):
        parsed = pd.to_numeric(value, errors="coerce")
        return max(0.0, min(1.0, float(parsed))) if pd.notna(parsed) else 0.0

    components = {
        "외국인": _pct(foreign_rank) * 100.0 if foreign_positive else 0.0,
        "연기금": _pct(pension_rank) * 100.0 if pension_positive else 0.0,
        "투신사모": _pct(trust_private_rank) * 100.0 if trust_private_positive else 0.0,
        "지속성": _pct(persistence_ratio) * 100.0,
    }
    raw_score = sum(components[key] * weights[key] for key in weights)
    core_actor = "외국인" if tier == "대형" else "연기금"
    core_positive = foreign_positive if core_actor == "외국인" else pension_positive
    penalty = 0.0
    warnings = []
    if not core_positive:
        penalty += 25.0
        warnings.append(f"핵심 주체 {core_actor} 순매수 미확인")
    if not liquidity_ok:
        penalty += 30.0
        warnings.append("거래대금 기준 미달")
    score = max(0.0, min(100.0, raw_score - penalty))
    reason = (
        f"{tier}주 핵심 {core_actor}, 외국인 {components['외국인']:.0f}, "
        f"연기금 {components['연기금']:.0f}, 투신·사모 {components['투신사모']:.0f}, "
        f"지속성 {components['지속성']:.0f}"
    )
    result = {
        "score": round(score, 2),
        "stock_tier": tier,
        "core_actor": core_actor,
        "core_flow_positive": bool(core_positive),
        "liquidity_ok": bool(liquidity_ok),
        "components": {key: round(value, 2) for key, value in components.items()},
        "warnings": warnings,
        "reason": reason,
    }
    return result if return_details else result["score"]


def calculate_market_leadership_score(
    relative_return_5d,
    relative_return_20d,
    trading_value_rank,
    trend_quality,
    drawdown_from_20d_high,
    return_details=False,
):
    """시총 대신 시장 대비 성과, 유동성, 추세 지속성으로 주도성을 평가한다."""

    def _value(value, default=0.0):
        parsed = pd.to_numeric(value, errors="coerce")
        return float(parsed) if pd.notna(parsed) else float(default)

    rel5 = _value(relative_return_5d)
    rel20 = _value(relative_return_20d)
    liquidity_rank = max(0.0, min(1.0, _value(trading_value_rank)))
    trend = max(0.0, min(100.0, _value(trend_quality)))
    drawdown = _value(drawdown_from_20d_high)

    rel5_score = max(0.0, min(25.0, (rel5 + 2.0) / 10.0 * 25.0))
    rel20_score = max(0.0, min(30.0, (rel20 + 3.0) / 18.0 * 30.0))
    liquidity_score = liquidity_rank * 20.0
    trend_score = trend / 100.0 * 20.0
    high_score = 5.0 if -10.0 <= drawdown <= 0.5 else (2.0 if -15.0 <= drawdown < -10.0 else 0.0)
    penalty = 15.0 if rel5 < -4.0 else 0.0
    score = max(0.0, min(100.0, rel5_score + rel20_score + liquidity_score + trend_score + high_score - penalty))
    result = {
        "score": round(score, 2),
        "relative_return_5d": round(rel5, 4),
        "relative_return_20d": round(rel20, 4),
        "trading_value_rank": round(liquidity_rank, 4),
        "trend_quality": round(trend, 2),
        "drawdown_from_20d_high": round(drawdown, 4),
        "reason": (
            f"시장대비 5일 {rel5:+.1f}%p·20일 {rel20:+.1f}%p, "
            f"거래대금 상위 {liquidity_rank * 100:.0f}%, 추세 {trend:.0f}"
        ),
    }
    return result if return_details else result["score"]


def evaluate_v5_entry_setup(
    return_20d,
    ma20_gap,
    drawdown_from_20d_high,
    breakout_from_prior_20d_high,
    trading_value_vitality,
    rsi,
    trend_rising,
    flow_score,
    return_details=False,
):
    """눌림목과 실제 전고점 돌파를 별도 관문으로 판정한다."""

    def _value(value, default=0.0):
        parsed = pd.to_numeric(value, errors="coerce")
        return float(parsed) if pd.notna(parsed) else float(default)

    ret20 = _value(return_20d)
    gap = _value(ma20_gap)
    drawdown = _value(drawdown_from_20d_high)
    breakout = _value(breakout_from_prior_20d_high)
    vitality = _value(trading_value_vitality)
    rsi_value = _value(rsi, 50.0)
    flow = _value(flow_score)

    pullback_checks = {
        "prior_uptrend": ret20 >= 3.0 and bool(trend_rising),
        "healthy_drawdown": -10.0 <= drawdown <= -2.0,
        "ma20_support": -2.0 <= gap <= 6.0,
        "volume_control": 0.55 <= vitality <= 1.6,
        "rsi_ok": 42.0 <= rsi_value <= 72.0,
        "flow_ok": flow >= 55.0,
    }
    breakout_checks = {
        "prior_uptrend": ret20 >= 3.0 and bool(trend_rising),
        "actual_breakout": breakout >= 0.0,
        "volume_confirmation": vitality >= 1.15,
        "not_extended": gap <= 12.0 and rsi_value <= 82.0,
        "flow_ok": flow >= 55.0,
    }
    pullback_passed = all(pullback_checks.values())
    breakout_passed = all(breakout_checks.values())
    if pullback_passed:
        setup, passed, quality = "눌림확인", True, 100.0
    elif breakout_passed:
        setup, passed, quality = "돌파확인", True, 95.0
    elif ret20 >= 3.0 and -12.0 <= drawdown <= -1.0:
        setup, passed = "눌림대기", False
        quality = sum(pullback_checks.values()) / len(pullback_checks) * 100.0
    elif ret20 >= 3.0 and breakout >= -3.0:
        setup, passed = "돌파대기", False
        quality = sum(breakout_checks.values()) / len(breakout_checks) * 100.0
    else:
        setup, passed, quality = "조건미충족", False, 0.0

    failed = []
    selected_checks = pullback_checks if setup.startswith("눌림") else breakout_checks
    for key, value in selected_checks.items():
        if not value:
            failed.append(key)
    result = {
        "passed": bool(passed),
        "setup": setup,
        "quality_score": round(float(quality), 2),
        "failed_checks": failed,
        "reason": (
            f"{setup}: 20일 {ret20:+.1f}%, 고점대비 {drawdown:+.1f}%, "
            f"MA20 이격 {gap:+.1f}%, 거래대금 {vitality:.2f}배"
        ),
    }
    return result if return_details else result["passed"]


def calculate_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50.0
    diffs = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in diffs]
    losses = [-d if d < 0 else 0 for d in diffs]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(diffs)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calculate_trend_quality(closes):
    """
    최근 종가 리스트로 정배열/추세품질을 계산합니다.
    closes는 최신일이 먼저 오는 순서도, 오래된 일자가 먼저 오는 순서도 허용합니다.
    """
    vals = [float(x) for x in closes if pd.notna(x)]
    if not vals:
        return {"ma5": 0.0, "ma10": 0.0, "ma20": 0.0, "aligned": False, "score": 0.0, "reason": "가격 데이터 부족"}

    # 기존 scraper 호출은 최신일 역순 리스트를 넘기므로 내부 계산은 기존 방식 그대로 유지합니다.
    current = vals[0]
    ma5 = sum(vals[:5]) / min(5, len(vals))
    ma10 = sum(vals[:10]) / min(10, len(vals))
    ma20 = sum(vals[:20]) / min(20, len(vals))
    prev5 = sum(vals[5:10]) / len(vals[5:10]) if len(vals) >= 10 else ma5
    prev10 = sum(vals[10:20]) / len(vals[10:20]) if len(vals) >= 20 else ma10

    above20 = current >= ma20
    short_above_mid = ma5 >= ma10
    mid_above_long = ma10 >= ma20
    short_slope = ma5 >= prev5
    mid_slope = ma10 >= prev10
    aligned = bool(above20 and short_above_mid and mid_above_long)
    score = (
        (20 if above20 else 0)
        + (20 if short_above_mid else 0)
        + (20 if mid_above_long else 0)
        + (20 if short_slope else 0)
        + (20 if mid_slope else 0)
    )
    reason_bits = []
    if aligned:
        reason_bits.append("정배열")
    if short_slope:
        reason_bits.append("단기 상승")
    if mid_slope:
        reason_bits.append("중기 상승")
    return {
        "ma5": round(ma5, 2),
        "ma10": round(ma10, 2),
        "ma20": round(ma20, 2),
        "aligned": aligned,
        "score": round(float(score), 2),
        "reason": ", ".join(reason_bits) if reason_bits else "추세 약함",
    }


def calculate_dynamic_score(
    f_str,
    p_str,
    t_str,
    pef_str,
    vol_surge,
    rsi_val,
    gap_20,
    foreign_streak,
    pension_streak,
    turnover_rate,
    is_ma20_rising,
    per_val,
    roe_val,
    current_vix,
    dip_buying_ratio=0.0,
    return_details=False,
):
    if current_vix < 25:
        zombie_penalty = 0
        fund_score = 0
        raw_str_sum = (t_str * 4) + (pef_str * 4) + (p_str * 2) + (f_str * 0.5)
        strength_score = max(0, min(20, raw_str_sum * 2))
        streak_score = max(0, min(10, (pension_streak * 1.5) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score

        turnover_score = 20 if turnover_rate >= 10 else (10 if turnover_rate >= 5 else 0)
        v_score = 10 if vol_surge >= 150 else 0
        r_score = 15 if 60 <= rsi_val <= 85 else (5 if 50 <= rsi_val < 60 else 0)
        momentum_score = turnover_score + v_score + r_score

        if 102 <= gap_20 <= 108:
            tech_score = 10
            if float(dip_buying_ratio) >= 0.6:
                tech_score += 15
        elif 98 <= gap_20 < 102:
            tech_score = 5
            if float(dip_buying_ratio) >= 0.6:
                tech_score += 15
        else:
            tech_score = 0
        regime = "상승장"
    else:
        zombie_penalty = -30 if turnover_rate < 1.5 else 0
        raw_str_sum = (p_str * 5) + (t_str * 2) + (pef_str * 2) + (f_str * 0.5)
        strength_score = max(0, min(20, raw_str_sum * 2))
        streak_score = max(0, min(10, (pension_streak * 2.5) + (foreign_streak * 0.5)))
        supply_score = strength_score + streak_score

        turnover_score = 5 if turnover_rate >= 3 else 0
        v_score = 5 if vol_surge >= 100 else 0
        r_score = 10 if 45 <= rsi_val <= 60 else 0
        momentum_score = turnover_score + v_score + r_score

        if is_ma20_rising:
            tech_score = 20 if 98 <= gap_20 <= 103 else (10 if 103 < gap_20 <= 108 else 0)
        else:
            tech_score = -20

        fund_score = (15 if roe_val >= 15 else (10 if roe_val >= 8 else 0)) + (15 if 0 < per_val <= 15 else 0)
        if per_val <= 0:
            fund_score -= 20
        regime = "하락장"

    final = max(0, min(100, int(supply_score + momentum_score + tech_score + fund_score + zombie_penalty)))
    if not return_details:
        return final
    return {
        "score": final,
        "regime": regime,
        "supply_score": round(float(supply_score), 2),
        "momentum_score": round(float(momentum_score), 2),
        "tech_score": round(float(tech_score), 2),
        "fund_score": round(float(fund_score), 2),
        "penalty": round(float(zombie_penalty), 2),
        "reason": f"{regime}: 수급 {supply_score:.1f}, 모멘텀 {momentum_score:.1f}, 기술 {tech_score:.1f}",
    }


def calculate_qualitative_score(
    sector_name,
    per_val,
    roe_val,
    foreign_streak,
    pension_streak,
    macro_news_text,
    macro_recency_score=50.0,
    repeated_topics_text="",
    return_details=False,
):
    score = 50.0
    text = (macro_news_text or "").lower()
    topic_text = (repeated_topics_text or "").lower()
    sector = (sector_name or "분류안됨").lower()
    decay_factor = max(0.35, min(1.0, float(macro_recency_score) / 100.0))

    sector_theme_map = {
        "반도체": ["반도체", "ai", "hbm", "메모리"],
        "전기": ["전력", "전기", "배터리", "2차전지", "ess"],
        "건설": ["건설", "인프라", "플랜트", "수주"],
        "화장품": ["화장품", "소비", "면세", "중국 소비"],
        "제약": ["제약", "바이오", "임상", "허가"],
        "방산": ["방산", "국방", "수출"],
        "조선": ["조선", "선박", "해운", "lng"],
        "기계": ["기계", "자동화", "설비투자"],
        "증권": ["증권", "거래대금", "금리", "유동성"],
    }
    positive_tone_keys = ["호재", "상향", "증가", "개선", "수주", "체결", "흑자", "서프라이즈", "기대", "확대"]
    neutral_tone_keys = ["전망", "관측", "분석", "주목", "설명", "동향", "점검", "리포트", "이슈"]
    negative_tone_keys = ["긴축", "관세", "하락", "리스크", "소송", "악재", "부진", "감소", "충격", "약세"]

    positive_hits = sum(1 for k in positive_tone_keys if k in text)
    neutral_hits = sum(1 for k in neutral_tone_keys if k in text)
    negative_hits = sum(1 for k in negative_tone_keys if k in text)

    if positive_hits > negative_hits:
        theme_tone_mult = 1.0
    elif neutral_hits >= max(1, positive_hits):
        theme_tone_mult = 0.35
    else:
        theme_tone_mult = 0.55

    theme_boost = 0.0
    for sector_key, keywords in sector_theme_map.items():
        if sector_key in sector and any(k.lower() in text for k in keywords):
            theme_boost = 8 * decay_factor * theme_tone_mult
            break
    score += min(4.5, theme_boost)

    if negative_hits > 0:
        score -= 4 * decay_factor

    if any(k in topic_text for k in ["실적", "수주", "정책", "수급"]):
        score += 3 * decay_factor
    if any(k in topic_text for k in ["리스크", "하락", "긴축", "관세"]):
        score -= 3 * decay_factor

    if roe_val >= 15:
        score += 5
    elif roe_val >= 8:
        score += 2
    else:
        score -= 2

    if 0 < per_val <= 15:
        score += 3
    elif per_val <= 0:
        score -= 5

    score += min(5, pension_streak * 0.8)
    score += min(2, foreign_streak * 0.2)

    final_score = max(0, min(100, score))
    details = {
        "theme_boost_raw": round(float(theme_boost), 3),
        "theme_boost_applied": round(float(min(4.5, theme_boost)), 3),
        "theme_tone_mult": round(float(theme_tone_mult), 3),
        "positive_hits": int(positive_hits),
        "neutral_hits": int(neutral_hits),
        "negative_hits": int(negative_hits),
        "decay_factor": round(float(decay_factor), 3),
        "reason": f"뉴스톤 +{positive_hits}/중립 {neutral_hits}/부정 {negative_hits}, 섹터가점 {min(4.5, theme_boost):.1f}",
    }
    if not return_details:
        return final_score
    return final_score, details


def blend_quant_qual_score(quant_score, qual_score, current_vix, return_details=False):
    if current_vix < 25:
        sensitivity = 0.4
        limit = 10
        mode = "상승장 (보수적 반영)"
    else:
        sensitivity = 0.6
        limit = 20
        mode = "하락장 (민감 반영)"

    qual_adj = (qual_score - 50) * sensitivity
    qual_adj = max(-limit, min(limit, qual_adj))
    final_score = max(0, min(100, quant_score + qual_adj))
    result = (round(final_score, 2), round(qual_adj, 2), mode)
    if not return_details:
        return result
    return {
        "score": result[0],
        "qual_adjustment": result[1],
        "mode": result[2],
        "reason": f"정량 {quant_score:.1f}에 정성 보정 {result[1]:+.1f} 적용",
    }


def score_disclosures_and_reports(disclosures, reports, return_details=False):
    score = 50.0
    positive_keys = ["실적", "수주", "계약", "자기주식", "소각", "기업설명회", "가이던스", "상향", "증가"]
    negative_keys = ["소송", "정정", "하향", "감소", "리스크", "악화", "손실"]
    pos_hits = 0
    neg_hits = 0

    for text in disclosures + reports:
        t = str(text)
        if any(k in t for k in positive_keys):
            score += 3.5
            pos_hits += 1
        if any(k in t for k in negative_keys):
            score -= 4.0
            neg_hits += 1

    final = max(20.0, min(80.0, score))
    if not return_details:
        return final
    return {
        "score": final,
        "positive_hits": pos_hits,
        "negative_hits": neg_hits,
        "reason": f"긍정 이벤트 {pos_hits}개, 부정 이벤트 {neg_hits}개",
    }


def score_v5_disclosures(disclosures, as_of_date=None, return_details=False):
    """최근 공시 제목을 V5 전용 이벤트 점수와 위험 신호로 변환한다.

    공시 제목만으로 계약 규모나 재무 영향까지 단정할 수 없으므로 점수 폭은
    제한한다. 다만 거래정지, 횡령, 계약 해지처럼 제목만으로도 중대한 사건은
    V5 신규 후보를 차단한다.
    """
    items = [str(item).strip() for item in (disclosures or []) if str(item).strip()]
    reference = pd.to_datetime(as_of_date, errors="coerce") if as_of_date is not None else pd.Timestamp(date.today())
    if pd.isna(reference):
        reference = pd.Timestamp(date.today())
    reference = reference.normalize()

    blocking_rules = [
        ("거래정지", ["거래정지", "매매거래정지"]),
        ("상장폐지 위험", ["상장폐지", "상장적격성 실질심사"]),
        ("회계·법적 중대 위험", ["횡령", "배임", "감사의견거절", "부도", "파산", "회생절차"]),
        ("계약 취소", ["계약해지", "계약 해지", "수주취소", "수주 취소", "공급계약 해지"]),
    ]
    caution_rules = [
        ("주주가치 희석", ["유상증자", "전환사채", "신주인수권부사채", "교환사채", "전환청구권행사"]),
        ("소송·분쟁", ["소송", "중재", "가압류", "가처분"]),
        ("실적 악화", ["적자전환", "영업손실", "손상차손", "하향", "감소"]),
        ("정정 공시", ["정정"]),
    ]
    positive_rules = [
        ("주주환원", ["자기주식취득", "자기주식 취득", "자기주식소각", "자기주식 소각", "소각결정", "소각 결정"]),
        ("수주·계약", ["단일판매", "공급계약체결", "공급계약 체결", "수주"]),
        ("실적 개선", ["흑자전환", "영업이익 증가", "매출액 증가", "실적 개선"]),
        ("배당", ["현금배당", "현금ㆍ현물배당", "배당결정", "배당 결정"]),
    ]

    def _event_date(text):
        compact = re.search(r"\b(20\d{6})\b", text)
        dotted = re.search(r"\b(20\d{2})[.\-/](\d{1,2})[.\-/](\d{1,2})\b", text)
        raw = compact.group(1) if compact else (
            f"{dotted.group(1)}{int(dotted.group(2)):02d}{int(dotted.group(3)):02d}" if dotted else ""
        )
        parsed = pd.to_datetime(raw, format="%Y%m%d", errors="coerce")
        return parsed.normalize() if pd.notna(parsed) else reference

    def _recency_weight(text):
        age = max(0, int((reference - _event_date(text)).days))
        if age <= 3:
            return 1.0
        if age <= 7:
            return 0.8
        if age <= 14:
            return 0.55
        return 0.3

    adjustment = 0.0
    blocked = False
    events = []
    for text in items:
        normalized = re.sub(r"\s+", " ", text)
        weight = _recency_weight(normalized)
        matched = False
        for label, keywords in blocking_rules:
            if any(keyword in normalized for keyword in keywords):
                adjustment -= 10.0 * weight
                blocked = True
                events.append((label, -10.0 * weight))
                matched = True
                break
        if matched:
            continue
        for label, keywords in caution_rules:
            if any(keyword in normalized for keyword in keywords):
                penalty = -6.0 if label == "주주가치 희석" else (-4.0 if label != "정정 공시" else -1.5)
                adjustment += penalty * weight
                events.append((label, penalty * weight))
                matched = True
                break
        if matched:
            continue
        for label, keywords in positive_rules:
            if any(keyword in normalized for keyword in keywords):
                bonus = 5.0 if label in {"수주·계약", "주주환원"} else (4.0 if label == "실적 개선" else 2.0)
                adjustment += bonus * weight
                events.append((label, bonus * weight))
                break

    adjustment = max(-10.0, min(6.0, adjustment))
    score = max(20.0, min(80.0, 50.0 + adjustment * 3.0))
    if blocked:
        risk_level = "차단"
    elif adjustment <= -2.0:
        risk_level = "주의"
    elif adjustment >= 2.0:
        risk_level = "우호"
    else:
        risk_level = "중립"

    if events:
        ordered = sorted(events, key=lambda item: abs(item[1]), reverse=True)
        event_summary = ", ".join(dict.fromkeys(label for label, _ in ordered[:3]))
        reason = f"{event_summary} · V5 {adjustment:+.1f}점"
    else:
        reason = "최근 중요 공시 신호 없음"

    result = {
        "score": round(score, 2),
        "adjustment": round(adjustment, 2),
        "risk_level": risk_level,
        "blocked": blocked,
        "reason": reason,
        "event_count": len(events),
        "warnings": ["공시 제목 기반 판정으로 금액·매출 대비 규모는 별도 확인 필요"] if events else [],
    }
    return result if return_details else result["score"]


def build_market_state_features(hist):
    market_frame = hist.copy()
    market_frame["전일종가"] = market_frame.groupby("종목명")["종가"].shift(1)
    market_frame["일간등락률"] = ((market_frame["종가"] / market_frame["전일종가"]) - 1.0) * 100.0
    market_frame["일간등락률"] = pd.to_numeric(market_frame["일간등락률"], errors="coerce").replace([float("inf"), -float("inf")], pd.NA)
    market_frame["5일수익률"] = market_frame.groupby("종목명")["종가"].pct_change(5) * 100.0
    market_frame["20일수익률"] = market_frame.groupby("종목명")["종가"].pct_change(20) * 100.0
    if "거래대금(억)" in market_frame.columns:
        market_frame["거래대금_값"] = pd.to_numeric(market_frame["거래대금(억)"], errors="coerce")
    elif "거래량" in market_frame.columns:
        market_frame["거래대금_값"] = pd.to_numeric(market_frame["거래량"], errors="coerce")
    else:
        market_frame["거래대금_값"] = 0.0
    market_frame["거래대금20"] = market_frame.groupby("종목명")["거래대금_값"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    market_frame["거래대금가속"] = (market_frame["거래대금_값"] / market_frame["거래대금20"]).replace([float("inf"), -float("inf")], pd.NA)
    market_frame["MA20"] = market_frame.groupby("종목명")["종가"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    market_frame["MA20상회"] = market_frame["종가"] >= market_frame["MA20"]
    market_frame["MA20_5일전"] = market_frame.groupby("종목명")["MA20"].shift(5)
    market_frame["MA20상승"] = market_frame["MA20"] >= market_frame["MA20_5일전"]
    market_frame["20일고가"] = market_frame.groupby("종목명")["종가"].transform(
        lambda s: s.rolling(20, min_periods=15).max()
    )
    market_frame["MA20이격률"] = ((market_frame["종가"] / market_frame["MA20"]) - 1.0) * 100.0
    market_frame["20일고점낙폭"] = ((market_frame["종가"] / market_frame["20일고가"]) - 1.0) * 100.0
    for col in ["연기금", "투신", "사모"]:
        if col not in market_frame.columns:
            market_frame[col] = 0.0
        market_frame[col] = pd.to_numeric(market_frame[col], errors="coerce").fillna(0.0)
        market_frame[f"{col}5일누적"] = market_frame.groupby("종목명")[col].transform(
            lambda s: s.rolling(5, min_periods=3).sum()
        )
    market_state = (
        market_frame.dropna(subset=["일간등락률"])
        .groupby(market_frame["일자_dt"].dt.normalize())
        .agg(
            평균등락률=("일간등락률", "mean"),
            시장5일수익률=("5일수익률", "mean"),
            상승비율=("일간등락률", lambda s: float((s > 0).mean())),
            MA20상회비율=("MA20상회", "mean"),
        )
    )
    entry_features = market_frame.set_index(["일자_dt", "종목명"])[
        [
            "5일수익률",
            "20일수익률",
            "거래대금가속",
            "MA20이격률",
            "20일고점낙폭",
            "MA20상승",
            "연기금5일누적",
            "투신5일누적",
            "사모5일누적",
        ]
    ].sort_index()
    return market_frame, market_state, entry_features


def choose_adaptive_target_positions(cur_date, todays, score_col, max_positions, adaptive_rules=None, market_state_row=None, return_explanation=False):
    rules = adaptive_rules or ADAPTIVE_THRESHOLD_PROFILES["현재값"]
    state = market_state_row
    avg_ret = float(state.get("평균등락률", 0.0)) if state is not None else 0.0
    up_ratio = float(state.get("상승비율", 0.5)) if state is not None else 0.5
    ma20_ratio = float(state.get("MA20상회비율", 0.5)) if state is not None else 0.5
    max_score = float(pd.to_numeric(todays[score_col], errors="coerce").max()) if not todays.empty else 0.0
    has_leader = False
    if not todays.empty and "진입유형" in todays.columns:
        has_leader = todays["진입유형"].astype(str).str.contains("주도", na=False).any()

    if avg_ret <= rules["severe_avg_ret"] or up_ratio <= rules["severe_up_ratio"] or ma20_ratio <= rules["severe_ma20_ratio"]:
        result = (0, "방어", "시장 평균/상승비율/MA20 상회비율 중 하나가 비상 기준을 하회")
    elif avg_ret <= rules["weak_avg_ret"] or up_ratio <= rules["weak_up_ratio"] or ma20_ratio <= rules["weak_ma20_ratio"]:
        result = (1, "선별", "시장 폭이 약해 신규 진입을 1종목으로 제한")
    elif max_score >= rules["attack_score"] and up_ratio >= rules["attack_up_ratio"] and ma20_ratio >= rules["attack_ma20_ratio"]:
        result = (int(max_positions), "공격", "시장 폭과 후보 점수가 모두 공격 기준 충족")
    elif has_leader and max_score >= rules["leader_score"] and up_ratio >= rules["leader_up_ratio"]:
        result = (min(int(max_positions), 2), "공격대기", "주도 후보가 있으나 전체 시장 확인 필요")
    else:
        result = (min(int(max_positions), 1), "관찰", "공격 조건 미충족으로 관찰/소수 진입")

    if not return_explanation:
        return result[0], result[1]
    return {
        "target_positions": result[0],
        "market_mode": result[1],
        "reason": result[2],
        "avg_ret": round(avg_ret, 4),
        "up_ratio": round(up_ratio, 4),
        "ma20_ratio": round(ma20_ratio, 4),
        "max_score": round(max_score, 2),
        "has_leader": bool(has_leader),
    }


def passes_relative_strength_filter(sig, cur_date, market_mode, entry_features, market_state, score_col, return_explanation=False):
    if market_mode == "방어":
        return {"passed": False, "reason": "방어 모드"} if return_explanation else False
    name = str(sig.get("종목명", "")).strip()
    try:
        feature = entry_features.loc[(cur_date, name)]
    except Exception:
        return {"passed": False, "reason": "상대강도 데이터 없음"} if return_explanation else False
    stock_ret5 = float(pd.to_numeric(feature.get("5일수익률", 0.0), errors="coerce") or 0.0)
    volume_accel = float(pd.to_numeric(feature.get("거래대금가속", 0.0), errors="coerce") or 0.0)
    state = market_state.loc[cur_date] if cur_date in market_state.index else None
    market_ret5 = float(state.get("시장5일수익률", 0.0)) if state is not None else 0.0
    relative_ret5 = stock_ret5 - market_ret5
    score_value = float(pd.to_numeric(sig.get(score_col, 0.0), errors="coerce") or 0.0)
    is_leader = "주도" in str(sig.get("진입유형", ""))

    passed = False
    reason = "상대강도 조건 미충족"
    if stock_ret5 <= -4.0:
        reason = "최근 5일 절대수익률 약세"
    elif stock_ret5 >= 18.0 and volume_accel >= 1.25:
        reason = "단기 급등+거래대금 과열"
    elif volume_accel >= 4.0:
        reason = "거래대금 과열"
    elif relative_ret5 >= 3.0 and stock_ret5 <= 14.0:
        passed, reason = True, "시장 대비 상대강도 우수"
    elif relative_ret5 >= 1.2 and volume_accel >= 0.85 and stock_ret5 <= 12.0:
        passed, reason = True, "상대강도와 거래대금 회복 확인"
    elif is_leader and relative_ret5 >= 0.0 and volume_accel >= 0.75 and score_value >= 58.0:
        passed, reason = True, "주도 후보 상대강도 유지"
    elif score_value >= 66.0 and relative_ret5 >= -0.5 and volume_accel >= 1.0 and stock_ret5 <= 10.0:
        passed, reason = True, "고점수 후보의 상대강도 방어"

    if not return_explanation:
        return passed
    return {
        "passed": bool(passed),
        "reason": reason,
        "stock_ret5": round(stock_ret5, 4),
        "market_ret5": round(market_ret5, 4),
        "relative_ret5": round(relative_ret5, 4),
        "volume_accel": round(volume_accel, 4),
        "score": round(score_value, 2),
    }


def passes_confirmed_pullback_filter(
    sig,
    cur_date,
    market_mode,
    entry_features,
    score_col,
    return_explanation=False,
):
    """주도주가 건전하게 눌리고 핵심 기관 수급이 유지된 경우만 통과시킨다."""
    if market_mode == "방어":
        result = {"passed": False, "reason": "방어 모드"}
        return result if return_explanation else False

    name = str(sig.get("종목명", "")).strip()
    entry_type = str(sig.get("진입유형", ""))
    try:
        feature = entry_features.loc[(cur_date, name)]
        if isinstance(feature, pd.DataFrame):
            feature = feature.iloc[-1]
    except Exception:
        result = {"passed": False, "reason": "눌림/수급 확인 데이터 없음"}
        return result if return_explanation else False

    def _value(key, default=0.0):
        value = pd.to_numeric(feature.get(key, default), errors="coerce")
        return float(value) if pd.notna(value) else float(default)

    stock_ret5 = _value("5일수익률")
    stock_ret20 = _value("20일수익률")
    volume_accel = _value("거래대금가속")
    ma20_gap = _value("MA20이격률")
    drawdown20 = _value("20일고점낙폭")
    ma20_rising_raw = feature.get("MA20상승", False)
    ma20_rising = bool(ma20_rising_raw) if pd.notna(ma20_rising_raw) else False
    pension5 = _value("연기금5일누적")
    trust5 = _value("투신5일누적")
    private5 = _value("사모5일누적")
    score_value = float(pd.to_numeric(sig.get(score_col, 0.0), errors="coerce") or 0.0)

    checks = [
        ("주도눌림" in entry_type, "주도눌림 후보 아님"),
        (stock_ret20 > 0.0 and ma20_rising, "20일 상승추세 미확인"),
        (0.0 <= ma20_gap <= 6.0, "MA20 지지 구간 이탈"),
        (-10.0 <= drawdown20 <= -2.0, "고점 대비 눌림 폭 부적합"),
        (-5.0 <= stock_ret5 <= 6.0, "단기 낙폭 또는 반등 과도"),
        (0.65 <= volume_accel <= 2.0, "거래대금 부족 또는 과열"),
        (pension5 > 0.0 or (trust5 + private5) > 0.0, "핵심 기관 5일 수급 미확인"),
    ]
    failed_reason = next((reason for passed, reason in checks if not passed), "")
    passed = not failed_reason
    result = {
        "passed": passed,
        "reason": "주도주 눌림과 핵심 기관 수급 확인" if passed else failed_reason,
        "stock_ret5": round(stock_ret5, 4),
        "stock_ret20": round(stock_ret20, 4),
        "ma20_gap": round(ma20_gap, 4),
        "drawdown20": round(drawdown20, 4),
        "volume_accel": round(volume_accel, 4),
        "ma20_rising": ma20_rising,
        "pension5": round(pension5, 4),
        "trust_private5": round(trust5 + private5, 4),
        "score": round(score_value, 2),
    }
    return result if return_explanation else passed
