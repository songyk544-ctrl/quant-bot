import pandas as pd

from db_utils import read_table


DATA_OPTIONS = {
    "종가": "종가",
    "등락률": "등락률(%)",
    "거래량": "거래량",
    "거래대금": "거래대금(억)",
    "외인 순매수": "외인",
    "연기금 순매수": "연기금",
    "투신 순매수": "투신",
    "사모 순매수": "사모",
    "5일 평균거래대금": "5일평균거래대금(억)",
    "20일 평균거래대금": "20일평균거래대금(억)",
    "5일 가격 위치": "5일가격위치(%)",
    "20일 가격 위치": "20일가격위치(%)",
}


TEMPLATE_PROMPTS = {
    "기술적 추세": "아래 표를 기반으로 가격 추세, 이동평균 대비 위치, 최근 변동성을 분석해줘. 매수보다 손절/리스크 관점도 같이 봐줘.",
    "수급 지속성": "아래 표를 기반으로 외인, 연기금, 투신, 사모 수급이 지속적인지 분석해줘. 일시적 유입과 추세적 유입을 구분해줘.",
    "손절/리스크 점검": "아래 표를 기반으로 현재 보유를 유지해도 되는지, 손절/축소가 필요한지 리스크 우선으로 판단해줘.",
    "종합 검토": "아래 주가, 거래량, 거래대금, 수급 데이터를 기반으로 기술적 추세와 수급 지속성을 함께 분석해줘. 결론은 보유/관찰/축소/회피로 나눠줘.",
}


def clip_text(text, max_chars=5000):
    text = "" if text is None else str(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...(길이 제한으로 일부 생략)"


def load_history_for_datapack():
    df = read_table("history.csv", "history.csv", read_csv_kwargs={"encoding": "utf-8-sig", "on_bad_lines": "skip"})
    if df.empty:
        return df
    df = df.copy()
    df["일자_dt"] = pd.to_datetime(
        df["일자"].astype(str).str.replace("-", "", regex=False),
        format="%Y%m%d",
        errors="coerce",
    )
    if df["일자_dt"].isna().all():
        df["일자_dt"] = pd.to_datetime(df["일자"], errors="coerce")
    for col in ["종가", "외인", "연기금", "투신", "사모", "거래량", "거래대금(억)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["종목명", "일자_dt"]).sort_values(["종목명", "일자_dt"])


def build_stock_data_pack(df_history, stock_name, days, selected_labels):
    stock = df_history[df_history["종목명"].astype(str).eq(str(stock_name))].copy()
    if stock.empty:
        return pd.DataFrame()
    stock = stock.sort_values("일자_dt").tail(int(days)).copy()
    stock["일자"] = stock["일자_dt"].dt.strftime("%Y-%m-%d")
    stock["등락률(%)"] = stock["종가"].pct_change() * 100.0
    stock["5일평균거래대금(억)"] = stock["거래대금(억)"].rolling(5, min_periods=1).mean()
    stock["20일평균거래대금(억)"] = stock["거래대금(억)"].rolling(20, min_periods=1).mean()
    ma5 = stock["종가"].rolling(5, min_periods=1).mean()
    ma20 = stock["종가"].rolling(20, min_periods=1).mean()
    stock["5일가격위치(%)"] = (stock["종가"] / ma5 - 1.0) * 100.0
    stock["20일가격위치(%)"] = (stock["종가"] / ma20 - 1.0) * 100.0

    cols = ["일자"]
    for label in selected_labels:
        col = DATA_OPTIONS.get(label)
        if col in stock.columns and col not in cols:
            cols.append(col)
    return stock[cols].copy()


def format_markdown_table(df):
    if df.empty:
        return "데이터가 없습니다."
    out = df.copy()
    for col in out.columns:
        if col == "일자":
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
        if "거래량" in col:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:,.0f}")
        elif "거래대금" in col:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:,.1f}")
        elif col in ["종가", "외인", "연기금", "투신", "사모"]:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:,.0f}")
        else:
            out[col] = out[col].map(lambda x: "" if pd.isna(x) else f"{x:+.2f}")
    headers = [str(c) for c in out.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in out.iterrows():
        vals = [str(row.get(c, "")).replace("\n", " ") for c in out.columns]
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines)


def build_prompt(stock_name, days, template_name, table_text, include_internal, df_summary):
    guide = TEMPLATE_PROMPTS.get(template_name, TEMPLATE_PROMPTS["종합 검토"])
    internal_text = ""
    if include_internal and df_summary is not None and not df_summary.empty:
        row = df_summary[df_summary["종목명"].astype(str).eq(str(stock_name))]
        if not row.empty:
            r = row.iloc[0]
            internal_text = f"""

[앱 내부 참고값]
- 매수후보: {r.get('매수후보', '-')}
- 진입유형: {r.get('진입유형', '-')}
- 진입코멘트: {r.get('진입코멘트', '-')}
- 매도점검: {r.get('매도점검', '-')}
"""
    return f"""아래는 {stock_name}의 최근 {days}거래일 주가/거래량/수급 데이터입니다.
AI점수 같은 내부 점수보다 실제 가격, 거래량, 거래대금, 수급 흐름을 중심으로 분석해주세요.

[분석 요청]
{guide}

[중요 기준]
- 매수 추천보다 리스크와 손절 기준을 먼저 봐주세요.
- 수급이 하루만 좋은지, 여러 날 누적되는지 구분해주세요.
- 가격이 반등 중인지, 하락 추세에서 단기 반등인지 구분해주세요.
- 데이터가 부족하면 부족하다고 말해주세요.
{internal_text}
[데이터]
{table_text}
"""
