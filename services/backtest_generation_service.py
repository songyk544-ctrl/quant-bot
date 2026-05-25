import pandas as pd


def build_backtest_candidate_scores(actual_score, replay_score, top_n=3):
    """
    실제 일일 스냅샷과 history 기반 리플레이 스냅샷을 합쳐
    백테스트에서 사용할 날짜별 신규 후보만 반환합니다.
    """
    actual = actual_score.copy() if actual_score is not None and not actual_score.empty else pd.DataFrame()
    replay = replay_score.copy() if replay_score is not None and not replay_score.empty else pd.DataFrame()

    if not actual.empty and "날짜" in actual.columns:
        actual["추천소스"] = "실제스냅샷"
    if not replay.empty:
        replay["추천소스"] = "리플레이"

    if not actual.empty and not replay.empty:
        # 실제 일일 스냅샷을 우선하되, 과거 스냅샷에 신규후보가 없던 날짜는
        # 누적 history 기반 replay로 보강합니다.
        score = pd.concat([actual, replay], ignore_index=True, sort=False)
    elif not actual.empty:
        score = actual
    else:
        score = replay

    if score.empty:
        return pd.DataFrame(), None

    score = score.copy()
    score["순위"] = pd.to_numeric(score["순위"], errors="coerce")
    score["AI수급점수"] = pd.to_numeric(score["AI수급점수"], errors="coerce")
    score["날짜_dt"] = pd.to_datetime(score["날짜"], errors="coerce")
    score = score.dropna(subset=["날짜_dt", "종목명", "순위"])
    if score.empty:
        return pd.DataFrame(), None

    backtest_start_date = score["날짜_dt"].min().normalize()
    day_slices = []
    for _, day_df in score.groupby("날짜_dt", sort=True):
        day = day_df.copy()
        if "추천소스" in day.columns:
            actual_day = day[day["추천소스"].astype(str) == "실제스냅샷"].copy()
            has_actual_candidate = (
                not actual_day.empty
                and "매수후보" in actual_day.columns
                and (actual_day["매수후보"].astype(str) == "신규후보").any()
            )
            if has_actual_candidate:
                day = actual_day
            else:
                replay_day = day[day["추천소스"].astype(str) == "리플레이"].copy()
                if not replay_day.empty:
                    day = replay_day

        if "매수후보" in day.columns and (day["매수후보"].astype(str) == "신규후보").any():
            picked = day[day["매수후보"].astype(str) == "신규후보"].copy()
            if "스윙우선순위" in picked.columns:
                picked = picked.sort_values(["스윙우선순위", "AI수급점수"], ascending=[False, False])
            else:
                picked = picked.sort_values(["순위", "AI수급점수"], ascending=[True, False])
        else:
            continue

        picked = picked.reset_index(drop=True)
        picked["순위"] = range(1, len(picked) + 1)
        day_slices.append(picked.head(int(top_n)))

    selected = pd.concat(day_slices, ignore_index=True) if day_slices else pd.DataFrame()
    return selected, backtest_start_date
