import os

import pandas as pd

from db_utils import read_table, write_table, table_exists, csv_exists, resolve_csv_path


def _table_name_for(csv_path):
    base, _ = os.path.splitext(csv_path)
    return os.path.basename(base)


def read_table_prefer_db(csv_path, **kwargs):
    return read_table(
        _table_name_for(csv_path),
        csv_fallback=csv_path,
        read_csv_kwargs=kwargs,
    )


def write_table_dual(df, csv_path, **kwargs):
    write_table(
        _table_name_for(csv_path),
        df,
        csv_path=csv_path,
        csv_kwargs=kwargs,
    )


def load_data():
    df_summary = read_table_prefer_db("data.csv")
    df_hist = read_table_prefer_db("history.csv")
    return df_summary, df_hist


def load_score_trend_safe():
    """머지 충돌/깨진 CSV를 방어적으로 읽어 앱 크래시를 막습니다."""
    base_cols = ["날짜", "종목명", "순위"]
    if not csv_exists("score_trend.csv") and not table_exists("score_trend"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("score_trend.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    bad_cols = [c for c in df.columns if any(x in str(c) for x in ["<<<<<<<", "=======", ">>>>>>>"])]
    if bad_cols:
        df = df.drop(columns=bad_cols, errors="ignore")

    if not set(base_cols).issubset(df.columns):
        return pd.DataFrame(columns=base_cols)

    marker_pat = r"^(<<<<<<<|=======|>>>>>>>)"
    df = df[~df["날짜"].astype(str).str.contains(marker_pat, regex=True, na=False)]
    df = df[~df["종목명"].astype(str).str.contains(marker_pat, regex=True, na=False)]
    df["날짜"] = df["날짜"].astype(str).str.strip()
    df = df.dropna(subset=["날짜", "종목명", "순위"])
    return df


def load_performance_trend_safe():
    """머지 충돌/깨진 performance_trend.csv를 방어적으로 읽습니다."""
    base_cols = ["날짜", "일간수익률", "누적수익률"]
    if not csv_exists("performance_trend.csv") and not table_exists("performance_trend"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("performance_trend.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)

    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    bad_cols = [c for c in df.columns if any(x in str(c) for x in ["<<<<<<<", "=======", ">>>>>>>"])]
    if bad_cols:
        df = df.drop(columns=bad_cols, errors="ignore")

    if not set(base_cols).issubset(df.columns):
        return pd.DataFrame(columns=base_cols)

    marker_pat = r"^(<<<<<<<|=======|>>>>>>>)"
    df = df[~df["날짜"].astype(str).str.contains(marker_pat, regex=True, na=False)]
    for c in ["일간수익률", "누적수익률"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["날짜"])
    return df[base_cols]


def load_swing_trades_safe():
    base_cols = [
        "거래ID", "진입일", "종목명", "종목코드", "진입순위", "AI수급점수",
        "진입유형", "스윙우선순위", "추천소스", "진입코멘트", "보유일수", "청산방식", "청산사유", "진입가", "청산일", "청산가", "수익률", "상태",
    ]
    if not csv_exists("swing_trades.csv") and not table_exists("swing_trades"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("swing_trades.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    for c in ["진입순위", "보유일수"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["AI수급점수", "스윙우선순위", "진입가", "청산가", "수익률"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["진입일_dt"] = pd.to_datetime(df["진입일"], errors="coerce")
    df["청산일_dt"] = pd.to_datetime(df["청산일"], errors="coerce")
    return df.dropna(subset=["진입일_dt"])[base_cols + ["진입일_dt", "청산일_dt"]]


def load_swing_performance_safe():
    base_cols = ["날짜", "일간수익률", "누적수익률", "최대낙폭(%)", "리스크상태", "종료거래수", "승률(%)"]
    if not csv_exists("swing_performance.csv") and not table_exists("swing_performance"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("swing_performance.csv", on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = None
    for c in ["일간수익률", "누적수익률", "최대낙폭(%)", "종료거래수", "승률(%)"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["날짜_dt"] = pd.to_datetime(df["날짜"], errors="coerce")
    return df.dropna(subset=["날짜_dt"])[base_cols + ["날짜_dt"]]


def load_theme_suggestions_safe():
    base_cols = ["날짜", "종목코드", "종목명", "현재섹터", "추천테마", "신뢰도", "근거", "승인상태"]
    if not csv_exists("theme_suggestions.csv") and not table_exists("theme_suggestions"):
        return pd.DataFrame(columns=base_cols)
    df = read_table_prefer_db("theme_suggestions.csv", dtype=str, on_bad_lines="skip")
    if df is None or df.empty:
        return pd.DataFrame(columns=base_cols)
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    for c in base_cols:
        if c not in df.columns:
            df[c] = ""
    return df[base_cols]


def promote_themes_to_map(approved_df):
    """
    승인된 추천 테마를 theme_map.csv로 승격하고,
    theme_suggestions.csv의 승인상태를 approved로 반영합니다.
    """
    if approved_df.empty:
        return 0
    map_path = resolve_csv_path("theme_map.csv")
    if os.path.exists(map_path):
        try:
            df_map = pd.read_csv(map_path, dtype=str)
        except Exception:
            df_map = pd.DataFrame(columns=["종목코드", "종목명", "테마"])
    else:
        df_map = pd.DataFrame(columns=["종목코드", "종목명", "테마"])
    for c in ["종목코드", "종목명", "테마"]:
        if c not in df_map.columns:
            df_map[c] = ""
    df_map["종목코드"] = df_map["종목코드"].fillna("").astype(str).str.strip().str.zfill(6)

    promoted_rows = []
    for _, row in approved_df.iterrows():
        code = str(row.get("종목코드", "") or "").strip().zfill(6)
        name = str(row.get("종목명", "") or "").strip()
        theme = str(row.get("추천테마", "") or "").strip()
        if not code or not name or not theme:
            continue
        promoted_rows.append({"종목코드": code, "종목명": name, "테마": theme})
    if not promoted_rows:
        return 0
    df_new = pd.DataFrame(promoted_rows)
    df_map = pd.concat([df_map, df_new], ignore_index=True)
    df_map = df_map.drop_duplicates(subset=["종목코드"], keep="last").sort_values("종목코드")
    df_map.to_csv(map_path, index=False, encoding="utf-8-sig")

    sugg = load_theme_suggestions_safe()
    if not sugg.empty:
        approved_codes = set(df_new["종목코드"].astype(str).tolist())
        sugg["종목코드"] = sugg["종목코드"].fillna("").astype(str).str.strip().str.zfill(6)
        sugg.loc[sugg["종목코드"].isin(approved_codes), "승인상태"] = "approved"
        write_table_dual(sugg, "theme_suggestions.csv", index=False, encoding="utf-8-sig")
    return len(df_new)
