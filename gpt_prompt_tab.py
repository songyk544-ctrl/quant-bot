import html
import json

import streamlit as st
import streamlit.components.v1 as components


PROMPT_MODES = ["포트폴리오 점검", "오늘 매수 후보", "매도 점검", "백테스트 진단", "자유 질문"]

DEFAULT_QUESTIONS = {
    "포트폴리오 점검": "내 현재 포트폴리오에서 유지, 축소 점검, 교체검토 종목을 구분해줘.",
    "오늘 매수 후보": "오늘 신규 매수 후보 중 실제로 1~3개만 고른다면 무엇을 우선 볼지 정리해줘.",
    "매도 점검": "보유 종목 중 수급이 깨졌거나 지지선 훼손 가능성이 있는 종목을 먼저 짚어줘.",
    "백테스트 진단": "현재 백테스트 성과가 신뢰 가능한지, 어떤 장에서 약할 수 있는지 평가해줘.",
    "자유 질문": "",
}


def clip_text(text, max_chars=5000):
    text = "" if text is None else str(text)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...(길이 제한으로 일부 생략)"


def build_gpt_handoff_prompt(context_text, user_question, prompt_mode="포트폴리오 점검"):
    mode_guides = {
        "포트폴리오 점검": "내 보유 종목을 먼저 점검하고, 유지/축소/교체검토를 구분해줘.",
        "오늘 매수 후보": "오늘 신규 후보 중 실제로 관심을 둘 후보를 1~3개로 압축하고, 진입 보류 조건도 같이 써줘.",
        "매도 점검": "보유 종목 중 매도/축소/추적주의가 필요한 종목을 우선순위로 정리해줘.",
        "백테스트 진단": "현재 백테스트 성과와 약점을 보고, 과최적화 가능성과 개선 포인트를 짚어줘.",
        "자유 질문": "사용자 질문에 맞춰 앱 데이터 안에서만 근거를 찾아 답해줘.",
    }
    mode_guide = mode_guides.get(prompt_mode, mode_guides["자유 질문"])
    question = str(user_question or "").strip() or mode_guide

    return f"""너는 국내주식 스윙 트레이딩 운용 보조 AI다.
사용자는 연기금/기관 수급, 외인 수급, 주도주 눌림목/돌파, 1~2주 보유 관점을 중요하게 본다.

아래 [앱 데이터]만 근거로 답해라.
없는 데이터는 추측하지 말고 "앱 데이터상 확인 불가"라고 말해라.
매수/매도는 단정하지 말고 유지, 점검, 교체검토, 신규관찰처럼 실행 단계로 말해라.
기존 보유 종목을 단순히 새 후보가 좋아 보인다는 이유만으로 교체하지 말고, 기존 종목 약화 + 새 후보 우위가 같이 있을 때만 교체검토라고 말해라.
스윙점수와 AI점수를 그대로 믿지 말고, 반드시 기술적 위치(정배열/MA5·MA10·MA20/이격도/RSI), 거래대금, 수급 지속성, 매도점검 문구, 뉴스 리스크를 함께 검증해라.
점수는 높은데 역배열, 거래대금 저하, 과열, 수급 약화, 부정 뉴스가 있으면 명확히 보류 또는 점검으로 낮춰 말해라.
반대로 점수가 조금 낮아도 대형 주도주, 정배열, 거래대금 활력, 수급 유지가 좋으면 관찰 가치가 있는지 따로 짚어라.
답변은 모바일에서 보기 좋게 짧고 명확하게 작성해라.

[분석 모드]
{prompt_mode}

[이번 요청]
{question}

[앱 데이터]
{context_text}

[답변 형식]
1. 한 줄 결론
2. 보유 종목 점검
3. 신규 후보/교체 후보
4. 기술적/수급상 반박 포인트
5. 오늘 바로 확인할 액션
6. 데이터상 부족한 점
"""


def render_clipboard_copy_button(text, button_label="프롬프트 클립보드에 복사"):
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
                status.textContent = "자동 복사가 막혔습니다. 아래 텍스트 영역에서 전체 선택 후 복사하세요.";
                status.style.color = "#FCA5A5";
            }}
        }}
        btn.addEventListener("click", copyPrompt);
        </script>
        """,
        height=76,
    )


def render_gpt_prompt_tab(df_summary, core_tickers, build_context, render_section_header):
    render_section_header("GPT 프롬프트 빌더", "앱 데이터를 ChatGPT에 바로 넘길 수 있는 운용 프롬프트로 압축합니다.", badge_text="Copy Mode")
    st.caption("관리자 전용입니다. 느린 내장 AI 호출 없이, 아래 프롬프트를 복사해서 ChatGPT에 붙여넣어 사용하세요.")

    prompt_col1, prompt_col2 = st.columns([0.95, 1.05])
    with prompt_col1:
        prompt_mode = st.selectbox("분석 모드", PROMPT_MODES, index=0)
    with prompt_col2:
        context_chars = st.select_slider(
            "데이터 포함량",
            options=[6000, 9000, 12000, 16000],
            value=12000,
            format_func=lambda x: f"{x:,}자",
        )

    user_prompt_question = st.text_area(
        "ChatGPT에게 물어볼 질문",
        value=DEFAULT_QUESTIONS.get(prompt_mode, ""),
        height=92,
    )

    context_text = clip_text(build_context(df_summary, core_tickers), int(context_chars))
    handoff_prompt = build_gpt_handoff_prompt(context_text, user_prompt_question, prompt_mode)

    render_clipboard_copy_button(handoff_prompt)
    st.caption(f"현재 생성된 프롬프트는 약 {len(handoff_prompt):,}자입니다. 버튼을 눌러 ChatGPT에 바로 붙여넣으세요.")

    with st.expander("복사용 프롬프트 원문 보기", expanded=False):
        st.text_area(
            "프롬프트 원문",
            value=handoff_prompt,
            height=420,
            help="복사 버튼이 브라우저 정책상 막히면 이 영역에서 전체 선택 후 복사하세요.",
        )

    with st.expander("프롬프트에 포함된 앱 데이터만 보기", expanded=False):
        st.text(context_text)
