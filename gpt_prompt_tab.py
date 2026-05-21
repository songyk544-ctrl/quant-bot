import html
import json

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

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


def render_clipboard_copy_button(text, button_label="GPT 데이터팩 클립보드에 복사"):
    payload = json.dumps(str(text or ""))
    components.html(
        f"""
        <div style="font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
            <button id="copyPromptBtn" style="
                width:100%;
                border:1px solid #2F6B4A;
                background:linear-gradient(135deg,#123220,#0f2419);
                color:#DFFFEF;
                border-radius:10px;
                padding:11px 14px;
                font-size:14px;
                font-weight:800;
                cursor:pointer;
            ">{html.escape(button_label)}</button>
            <div id="copyPromptStatus" style="margin-top:6px; color:#9CA3AF; font-size:12px;"></div>
        </div>
        <script>
        const promptText = {payload};
        const btn = document.getElementById("copyPromptBtn");
        const status = document.getElementById("copyPromptStatus");
        async function copyPrompt() {{
            try {{
                if (navigator.clipboard && window.isSecureContext) {{
                    await navigator.clipboard.writeText(promptText);
                }} else {{
                    const area = document.createElement("textarea");
                    area.value = promptText;
                    area.style.position = "fixed";
                    area.style.left = "-9999px";
                    document.body.appendChild(area);
                    area.focus();
                    area.select();
                    document.execCommand("copy");
                    document.body.removeChild(area);
                }}
                status.textContent = "복사 완료. ChatGPT에 바로 붙여넣으세요.";
                status.style.color = "#86EFAC";
            }} catch (err) {{
                status.textContent = "자동 복사가 막혔습니다. 아래 원문 영역에서 직접 복사하세요.";
                status.style.color = "#FCA5A5";
            }}
        }}
        btn.addEventListener("click", copyPrompt);
        </script>
        """,
        height=76,
    )


def _load_history():
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


def _build_stock_data_pack(df_history, stock_name, days, selected_labels):
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


def _format_markdown_table(df):
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


def _build_prompt(stock_name, days, template_name, table_text, include_internal, df_summary):
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


def render_gpt_prompt_tab(df_summary, core_tickers, build_context, render_section_header):
    render_section_header("GPT 데이터팩", "ChatGPT에 붙여넣을 수 있는 주가·거래량·수급 원자료 표를 만듭니다.", badge_text="Copy Mode")
    st.caption("관리자 전용입니다. AI점수보다 GPT가 직접 해석할 수 있는 실제 가격/거래량/수급 데이터를 우선합니다.")

    history = _load_history()
    if history.empty:
        st.info("history.csv 데이터가 부족해서 데이터팩을 만들 수 없습니다.")
        return

    stock_options = sorted(history["종목명"].dropna().astype(str).unique().tolist())
    default_index = 0
    if df_summary is not None and not df_summary.empty and "매수후보" in df_summary.columns:
        candidates = df_summary[df_summary["매수후보"].astype(str).eq("신규후보")]
        if not candidates.empty:
            first = str(candidates.iloc[0].get("종목명", ""))
            if first in stock_options:
                default_index = stock_options.index(first)

    col1, col2 = st.columns([1.1, 0.9])
    with col1:
        stock_name = st.selectbox("종목", stock_options, index=default_index)
    with col2:
        days = st.selectbox("기간", [20, 60, 120], index=1, format_func=lambda x: f"최근 {x}거래일")

    selected_labels = st.multiselect(
        "포함할 데이터",
        list(DATA_OPTIONS.keys()),
        default=["종가", "등락률", "거래량", "거래대금", "외인 순매수", "연기금 순매수", "투신 순매수", "사모 순매수", "20일 가격 위치"],
    )
    template_name = st.radio("분석 요청 템플릿", list(TEMPLATE_PROMPTS.keys()), horizontal=True, index=3)
    include_internal = st.checkbox("앱 내부 참고값을 접힌 프롬프트에 포함", value=False)

    pack_df = _build_stock_data_pack(history, stock_name, int(days), selected_labels)
    table_text = _format_markdown_table(pack_df)
    prompt_text = _build_prompt(stock_name, int(days), template_name, table_text, include_internal, df_summary)

    render_clipboard_copy_button(prompt_text)
    st.caption(f"생성된 데이터팩은 약 {len(prompt_text):,}자입니다. 표는 Markdown 형식입니다.")

    st.markdown("#### 미리보기")
    if not pack_df.empty and "일자" in pack_df.columns:
        st.caption(f"선택 기간 반영: {len(pack_df):,}거래일 · {pack_df['일자'].iloc[0]} ~ {pack_df['일자'].iloc[-1]}")
    st.dataframe(pack_df, hide_index=True, width="stretch")

    with st.expander("복사용 데이터팩 원문", expanded=False):
        st.text_area("Markdown 원문", value=prompt_text, height=420)

    with st.expander("기존 앱 요약 컨텍스트 보기", expanded=False):
        legacy_context = clip_text(build_context(df_summary, core_tickers), 9000) if build_context else ""
        st.text(legacy_context)
