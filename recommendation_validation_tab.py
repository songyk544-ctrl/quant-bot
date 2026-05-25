import altair as alt
import streamlit as st

from services.recommendation_validation_service import build_recommendation_validation


def render_recommendation_validation_tab(render_section_header, apply_altair_theme, render_empty_state):
    render_section_header("추천 검증", "매일 나온 스윙 추천이 이후 며칠 동안 실제로 유효했는지 시장 상태별로 검증합니다.", badge_text="Validation")
    st.caption("이 화면은 수익률을 예쁘게 보이기 위한 화면이 아니라, 실제 투자에 쓰기 전에 추천 신뢰도를 확인하기 위한 방어 장치입니다.")

    score_options = ["공격/방어", "스윙점수", "AI점수"]
    if hasattr(st, "segmented_control"):
        score_mode_label = st.segmented_control(
            "검증 기준",
            score_options,
            default="공격/방어",
            key="validation_score_mode",
            help="공격/방어는 시장 상태에 따라 검증 대상 추천 수를 0~3개로 조절합니다.",
        )
    else:
        score_mode_label = st.radio(
            "검증 기준",
            score_options,
            horizontal=True,
            index=0,
            key="validation_score_mode",
            help="공격/방어는 시장 상태에 따라 검증 대상 추천 수를 0~3개로 조절합니다.",
        )
    score_mode = "adaptive" if score_mode_label == "공격/방어" else ("ai" if score_mode_label == "AI점수" else "swing")

    summary, detail, regime_summary = build_recommendation_validation(score_mode=score_mode)
    if summary.empty:
        render_empty_state("검증 데이터 없음", "swing_trades.csv와 history.csv가 충분히 쌓인 뒤 추천 검증을 표시할 수 있습니다.")
        return

    latest = summary.sort_values("추천일").iloc[-1]
    weak_days = int(summary["거래당기대값"].lt(0).sum())
    crash_days = int(summary["급락수"].gt(0).sum())
    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="kpi-title">검증 추천일</div><div class="kpi-value">{len(summary):,}</div><div class="kpi-meta">최근 {latest['추천일']}</div></div>
            <div class="kpi-card"><div class="kpi-title">최근 시장 상태</div><div class="kpi-value">{latest['시장상태']}</div><div class="kpi-meta">상승비율 {float(latest.get('상승비율', 0.0)):.1f}%</div></div>
            <div class="kpi-card"><div class="kpi-title">기대값 음수일</div><div class="kpi-value">{weak_days:,}</div><div class="kpi-meta">추천 중지 검토 구간</div></div>
            <div class="kpi-card"><div class="kpi-title">-5% 역행 발생일</div><div class="kpi-value">{crash_days:,}</div><div class="kpi-meta">손실 방어 우선 구간</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if not regime_summary.empty:
        st.markdown("#### 시장 상태별 추천 성과")
        regime_cols = ["시장상태", "날짜수", "평균승률", "평균기대값", "평균최대역행폭", "급락수"]
        st.dataframe(
            regime_summary[regime_cols].style.format({
                "평균승률": "{:.1f}%",
                "평균기대값": "{:+.2f}%",
                "평균최대역행폭": "{:+.2f}%",
            }),
            hide_index=True,
            column_order=regime_cols,
            width="stretch",
        )

    chart_df = summary.copy()
    chart = alt.Chart(chart_df).mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
        x=alt.X("추천일:O", title=None, axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("거래당기대값:Q", title="거래당 기대값(%)"),
        color=alt.Color("시장상태:N", scale=alt.Scale(domain=["상승장", "혼조장", "하락장"], range=["#36C06A", "#FCD34D", "#E04B4B"])),
        tooltip=["추천일:N", "시장상태:N", alt.Tooltip("승률:Q", format=".1f"), alt.Tooltip("거래당기대값:Q", format="+.2f"), alt.Tooltip("최대역행폭:Q", format="+.2f")],
    ).properties(height=280)
    st.altair_chart(apply_altair_theme(chart), width="stretch")

    st.markdown("#### 추천일별 검증")
    view_cols = ["추천일", "시장상태", "검증모드", "추천종목수", "D1평균", "D3평균", "D5평균", "D10평균", "시그널평균", "승률", "손익비", "거래당기대값", "최대역행폭", "최대유리폭", "급락수"]
    st.dataframe(
        summary[view_cols].sort_values("추천일", ascending=False).style.format({
            "D1평균": "{:+.2f}%",
            "D3평균": "{:+.2f}%",
            "D5평균": "{:+.2f}%",
            "D10평균": "{:+.2f}%",
            "시그널평균": "{:+.2f}%",
            "승률": "{:.1f}%",
            "손익비": "{:.2f}x",
            "거래당기대값": "{:+.2f}%",
            "최대역행폭": "{:+.2f}%",
            "최대유리폭": "{:+.2f}%",
            "시장평균등락": "{:+.2f}%",
        }),
        hide_index=True,
        column_order=view_cols,
        width="stretch",
    )

    with st.expander("종목별 추천 검증 로그", expanded=False):
        detail_cols = ["추천일", "종목명", "검증모드", "진입유형", "진입순위", "스윙우선순위", "AI수급점수", "D+1 수익률", "D+3 수익률", "D+5 수익률", "D+10 수익률", "시그널 수익률", "최대역행폭", "최대유리폭"]
        st.dataframe(
            detail[detail_cols].sort_values(["추천일", "진입순위"], ascending=[False, True]).style.format({
                "D+1 수익률": "{:+.2f}%",
                "D+3 수익률": "{:+.2f}%",
                "D+5 수익률": "{:+.2f}%",
                "D+10 수익률": "{:+.2f}%",
                "시그널 수익률": "{:+.2f}%",
                "최대역행폭": "{:+.2f}%",
                "최대유리폭": "{:+.2f}%",
                "스윙우선순위": "{:.2f}",
                "AI수급점수": "{:.2f}",
            }),
            hide_index=True,
            column_order=detail_cols,
            width="stretch",
        )
