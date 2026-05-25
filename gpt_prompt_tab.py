import html
import json

import streamlit as st
import streamlit.components.v1 as components

from services.gpt_datapack_service import (
    DATA_OPTIONS,
    TEMPLATE_PROMPTS,
    build_prompt,
    build_stock_data_pack,
    clip_text,
    format_markdown_table,
    load_history_for_datapack,
)


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


def render_gpt_prompt_tab(df_summary, core_tickers, build_context, render_section_header):
    render_section_header("GPT 데이터팩", "ChatGPT에 붙여넣을 수 있는 주가·거래량·수급 원자료 표를 만듭니다.", badge_text="Copy Mode")
    st.caption("관리자 전용입니다. AI점수보다 GPT가 직접 해석할 수 있는 실제 가격/거래량/수급 데이터를 우선합니다.")

    history = load_history_for_datapack()
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

    pack_df = build_stock_data_pack(history, stock_name, int(days), selected_labels)
    table_text = format_markdown_table(pack_df)
    prompt_text = build_prompt(stock_name, int(days), template_name, table_text, include_internal, df_summary)

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
