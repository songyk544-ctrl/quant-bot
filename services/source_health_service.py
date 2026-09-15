from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd


SOURCE_HEALTH_COLUMNS = [
    "확인시각",
    "기준거래일",
    "상태",
    "소스",
    "fallback사용",
    "연속실패거래일",
    "종목수",
    "메시지",
]


def _load_health(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=SOURCE_HEALTH_COLUMNS)
    try:
        frame = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception:
        return pd.DataFrame(columns=SOURCE_HEALTH_COLUMNS)
    for col in SOURCE_HEALTH_COLUMNS:
        if col not in frame.columns:
            frame[col] = ""
    return frame[SOURCE_HEALTH_COLUMNS]


def record_source_health(
    *,
    checked_at,
    trade_date,
    source: str,
    primary_ok: bool,
    stock_count: int,
    message: str,
    path: str | Path = "data/source_health.csv",
) -> dict:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_health(path)
    trade_date_text = pd.to_datetime(trade_date).strftime("%Y-%m-%d")

    failure_days = 0
    if not primary_ok and not existing.empty:
        previous = existing.copy()
        previous["기준거래일_dt"] = pd.to_datetime(previous["기준거래일"], errors="coerce")
        previous = previous.dropna(subset=["기준거래일_dt"]).sort_values(
            ["기준거래일_dt", "확인시각"]
        )
        if not previous.empty:
            last = previous.iloc[-1]
            last_status = str(last.get("상태", ""))
            last_date = last["기준거래일_dt"].strftime("%Y-%m-%d")
            if last_status.startswith("fallback"):
                previous_count = int(pd.to_numeric(last.get("연속실패거래일", 0), errors="coerce") or 0)
                failure_days = previous_count if last_date == trade_date_text else previous_count + 1

    if primary_ok:
        status = "ok"
        failure_days = 0
    else:
        failure_days = max(1, failure_days)
        status = "fallback_once" if failure_days == 1 else "fallback_blocked"

    row = {
        "확인시각": pd.to_datetime(checked_at).strftime("%Y-%m-%d %H:%M:%S"),
        "기준거래일": trade_date_text,
        "상태": status,
        "소스": source,
        "fallback사용": not primary_ok,
        "연속실패거래일": failure_days,
        "종목수": int(stock_count),
        "메시지": str(message),
    }
    out = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
    out = out.tail(120)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return row


def latest_source_health(path: str | Path = "data/source_health.csv") -> dict:
    health = _load_health(path)
    return health.iloc[-1].to_dict() if not health.empty else {}


def should_fail_workflow(health: dict | None) -> bool:
    return str((health or {}).get("상태", "")) == "fallback_blocked"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail-on-blocked", action="store_true")
    parser.add_argument("--path", default="data/source_health.csv")
    args = parser.parse_args(argv)
    health = latest_source_health(args.path)
    if not health:
        print("[source-health] 기록 없음")
        return 0
    print(
        "[source-health] "
        f"status={health.get('상태')} source={health.get('소스')} "
        f"stocks={health.get('종목수')} failures={health.get('연속실패거래일')}"
    )
    if args.fail_on_blocked and should_fail_workflow(health):
        print(
            "::error title=Stock universe source unavailable::"
            "공식 종목 마스터 수집이 2거래일 이상 실패했습니다. "
            "캐시 데이터는 저장했지만 종목군 갱신 상태를 확인해야 합니다."
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
