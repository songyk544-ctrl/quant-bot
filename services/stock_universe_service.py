from __future__ import annotations

import io
import re
import zipfile

import pandas as pd


TARGET_STOCK_COLUMNS = [
    "종목명",
    "종목코드",
    "소속",
    "현재가",
    "등락률",
    "시가총액",
    "PER",
    "ROE",
]

NOISE_KEYWORDS = [
    "KODEX", "TIGER", "RISE", "ACE", "KBSTAR", "HANARO", "KOSEF",
    "SOL", "PLUS", "ARIRANG", "ETF", "ETN", "스팩", "인버스",
    "레버리지", "CD금리", "KOFR",
]

KIS_MASTER_SPECS = {
    "KOSPI": {
        "url": "https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip",
        "tail_width": 227,
        "widths": [
            2, 1, 4, 4, 4,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 9, 5, 5, 1,
            1, 1, 2, 1, 1,
            1, 2, 2, 2, 3,
            1, 3, 12, 12, 8,
            15, 21, 2, 7, 1,
            1, 1, 1, 1, 9,
            9, 9, 5, 9, 8,
            9, 3, 1, 1, 1,
        ],
        "price_index": 31,
        "market_cap_index": 65,
    },
    "KOSDAQ": {
        "url": "https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip",
        "tail_width": 221,
        "widths": [
            2, 1,
            4, 4, 4, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 1,
            1, 1, 1, 1, 9,
            5, 5, 1, 1, 1,
            2, 1, 1, 1, 2,
            2, 2, 3, 1, 3,
            12, 12, 8, 15, 21,
            2, 7, 1, 1, 1,
            1, 9, 9, 9, 5,
            9, 8, 9, 3, 1,
            1, 1,
        ],
        "price_index": 26,
        "market_cap_index": 59,
    },
}


def _safe_number(value, default=0.0):
    try:
        text = str(value or "").strip().replace(",", "")
        return float(text) if text else default
    except Exception:
        return default


def _split_fixed_width(text: str, widths: list[int]) -> list[str]:
    values = []
    offset = 0
    for width in widths:
        values.append(text[offset : offset + width].strip())
        offset += width
    return values


def _extract_archive_text(content: bytes) -> str:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".mst")]
        if not members:
            raise ValueError("KIS 종목 마스터 압축파일에 .mst 파일이 없습니다.")
        return archive.read(members[0]).decode("cp949", errors="replace")


def _parse_market_master(content: bytes, market_name: str) -> pd.DataFrame:
    spec = KIS_MASTER_SPECS[market_name]
    rows = []
    for raw_line in _extract_archive_text(content).splitlines():
        if len(raw_line) <= spec["tail_width"] + 21:
            continue
        head = raw_line[: -spec["tail_width"]]
        tail = raw_line[-spec["tail_width"] :]
        code_match = re.search(r"\d{6}", head[:9])
        if not code_match:
            continue
        values = _split_fixed_width(tail, spec["widths"])
        if len(values) <= spec["market_cap_index"]:
            continue
        rows.append(
            {
                "종목명": head[21:].strip(),
                "종목코드": code_match.group(0),
                "소속": market_name,
                "현재가": int(_safe_number(values[spec["price_index"]])),
                "등락률": 0.0,
                "시가총액": int(_safe_number(values[spec["market_cap_index"]])),
                "PER": 0.0,
                "ROE": 0.0,
            }
        )
    return pd.DataFrame(rows, columns=TARGET_STOCK_COLUMNS)


def _merge_cached_fundamentals(universe: pd.DataFrame, cached: pd.DataFrame | None) -> pd.DataFrame:
    if cached is None or cached.empty or "종목코드" not in cached.columns:
        return universe
    cached = cached.copy()
    cached["종목코드"] = (
        cached["종목코드"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    keep = [c for c in ["종목코드", "PER", "ROE", "등락률"] if c in cached.columns]
    if len(keep) <= 1:
        return universe
    cached = cached[keep].drop_duplicates("종목코드", keep="first")
    merged = universe.merge(cached, on="종목코드", how="left", suffixes=("", "_cached"))
    for col in ["PER", "ROE", "등락률"]:
        cached_col = f"{col}_cached"
        if cached_col in merged.columns:
            prior = pd.to_numeric(merged[cached_col], errors="coerce")
            current = pd.to_numeric(merged[col], errors="coerce").fillna(0.0)
            merged[col] = prior.where(prior.notna(), current).fillna(0.0)
            merged = merged.drop(columns=[cached_col])
    return merged


def fetch_kis_master_universe(
    http_get,
    cached: pd.DataFrame | None = None,
    min_market_cap: int = 8_000,
    timeout: int = 20,
) -> tuple[pd.DataFrame, dict]:
    frames = []
    errors = []
    for market_name, spec in KIS_MASTER_SPECS.items():
        try:
            response = http_get(spec["url"], timeout=timeout)
            response.raise_for_status()
            market_df = _parse_market_master(response.content, market_name)
            if market_df.empty:
                raise ValueError(f"{market_name} 종목 마스터가 비어 있습니다.")
            frames.append(market_df)
        except Exception as exc:
            errors.append(f"{market_name}: {exc}")

    if errors or len(frames) != len(KIS_MASTER_SPECS):
        return pd.DataFrame(columns=TARGET_STOCK_COLUMNS), {
            "ok": False,
            "source": "KIS_MASTER",
            "message": "; ".join(errors) or "KIS 종목 마스터 일부가 누락되었습니다.",
        }

    universe = pd.concat(frames, ignore_index=True)
    universe = _merge_cached_fundamentals(universe, cached)
    universe = universe[
        ~universe["종목명"].astype(str).apply(
            lambda name: any(keyword in name for keyword in NOISE_KEYWORDS)
        )
    ].copy()
    universe["시가총액"] = pd.to_numeric(universe["시가총액"], errors="coerce").fillna(0)
    universe = universe[universe["시가총액"] >= int(min_market_cap)].copy()
    universe = universe.drop_duplicates("종목코드", keep="first")
    universe = universe.sort_values("시가총액", ascending=False).reset_index(drop=True)

    cached_count = len(cached) if cached is not None else 0
    minimum_expected = max(100, int(cached_count * 0.7)) if cached_count else 100
    markets = set(universe["소속"].dropna().astype(str))
    if len(universe) < minimum_expected or not {"KOSPI", "KOSDAQ"}.issubset(markets):
        return pd.DataFrame(columns=TARGET_STOCK_COLUMNS), {
            "ok": False,
            "source": "KIS_MASTER",
            "message": (
                f"KIS 종목 마스터 품질검사 실패: {len(universe):,}개 "
                f"(최소 {minimum_expected:,}개), 시장={sorted(markets)}"
            ),
        }

    return universe[TARGET_STOCK_COLUMNS], {
        "ok": True,
        "source": "KIS_MASTER",
        "message": f"KIS 공식 종목 마스터 {len(universe):,}개",
    }
