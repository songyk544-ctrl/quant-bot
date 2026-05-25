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
}


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


def build_market_state_features(hist):
    market_frame = hist.copy()
    market_frame["전일종가"] = market_frame.groupby("종목명")["종가"].shift(1)
    market_frame["일간등락률"] = ((market_frame["종가"] / market_frame["전일종가"]) - 1.0) * 100.0
    market_frame["일간등락률"] = pd.to_numeric(market_frame["일간등락률"], errors="coerce").replace([float("inf"), -float("inf")], pd.NA)
    market_frame["5일수익률"] = market_frame.groupby("종목명")["종가"].pct_change(5) * 100.0
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
    entry_features = market_frame.set_index(["일자_dt", "종목명"])[["5일수익률", "거래대금가속"]].sort_index()
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
