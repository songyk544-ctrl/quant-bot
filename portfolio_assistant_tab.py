import html

import pandas as pd
import plotly.express as px
import streamlit as st

from services.portfolio_advisor_service import (
    build_replacement_pool,
    enrich_portfolio_holdings,
    portfolio_assistant_verdict,
)


def render_portfolio_assistant_tab(
    df_summary,
    render_section_header,
    render_empty_state,
    load_admin_risk_thresholds,
    save_admin_risk_thresholds,
    load_admin_portfolio_df,
    save_admin_portfolio_df,
):
    render_section_header("포트폴리오 비서", "보유·매도점검·교체 후보를 백테스트 기준과 연결해 먼저 판단합니다.", badge_text="Assistant")
    st.caption("표보다 결론을 먼저 봅니다. 편집 기능은 아래 접힘 영역에 따로 두었습니다.")
    saved_thr = load_admin_risk_thresholds()
    if "admin_swing_warn_threshold" not in st.session_state:
        st.session_state["admin_swing_warn_threshold"] = int(saved_thr["swing_warn_threshold"])
    if "admin_ai_critical_threshold" not in st.session_state:
        st.session_state["admin_ai_critical_threshold"] = int(saved_thr["ai_critical_threshold"])
    swing_warn_threshold = int(st.session_state.get("admin_swing_warn_threshold", saved_thr["swing_warn_threshold"]))
    ai_critical_threshold = int(st.session_state.get("admin_ai_critical_threshold", saved_thr["ai_critical_threshold"]))

    base_cols = ["종목명", "수량", "매수가"]
    df_port_saved = load_admin_portfolio_df(base_cols)
    stock_options = sorted(df_summary["종목명"].dropna().astype(str).unique().tolist())

    if df_port_saved.empty:
        render_empty_state("포트폴리오 비어 있음", "상단 에디터에서 보유 종목을 추가해 주세요.")
    else:
        df_joined, portfolio_summary = enrich_portfolio_holdings(
            df_port_saved,
            df_summary,
            swing_warn_threshold=swing_warn_threshold,
            ai_critical_threshold=ai_critical_threshold,
        )
        qty_num = pd.to_numeric(df_joined["수량"], errors="coerce").fillna(0.0)
        buy_num = pd.to_numeric(df_joined["매수가"], errors="coerce").fillna(0.0)
        cur_num = pd.to_numeric(df_joined["현재가"], errors="coerce").fillna(0.0)
        total_buy_amount = float(portfolio_summary["total_buy_amount"])
        total_eval_amount = float(portfolio_summary["total_eval_amount"])
        total_profit_amount = float(portfolio_summary["total_profit_amount"])
        total_profit_pct = float(portfolio_summary["total_profit_pct"])
        st.markdown("#### 실전 계좌 방어 모드")
        guard_col1, guard_col2 = st.columns(2)
        with guard_col1:
            live_initial_capital = st.number_input(
                "실전 초기 투자금",
                min_value=0,
                max_value=1_000_000_000,
                value=int(st.session_state.get("live_initial_capital", 5_200_000)),
                step=100_000,
                format="%d",
            )
        with guard_col2:
            live_current_equity = st.number_input(
                "실전 현재 평가금액",
                min_value=0,
                max_value=1_000_000_000,
                value=int(st.session_state.get("live_current_equity", 4_200_000)),
                step=100_000,
                format="%d",
            )
        st.session_state["live_initial_capital"] = int(live_initial_capital)
        st.session_state["live_current_equity"] = int(live_current_equity)
        live_drawdown_pct = (
            (float(live_current_equity) - float(live_initial_capital)) / float(live_initial_capital) * 100.0
            if live_initial_capital > 0 else 0.0
        )
        capital_guard_active = live_drawdown_pct <= -8.0 or total_profit_pct <= -8.0
        capital_emergency_active = live_drawdown_pct <= -15.0 or total_profit_pct <= -15.0
        if capital_emergency_active:
            st.error(
                f"실전 비상 모드: 초기자금 대비 {live_drawdown_pct:+.2f}%입니다. "
                "신규매수/교체매수보다 현금화와 손실 확대 차단을 먼저 봅니다."
            )
        elif capital_guard_active:
            st.warning(
                f"방어 모드: 초기자금 대비 {live_drawdown_pct:+.2f}%입니다. "
                "신규매수는 중지하고 보유 종목 매도점검을 우선합니다."
            )
        else:
            st.success(f"정상 운용 구간: 초기자금 대비 {live_drawdown_pct:+.2f}%입니다.")

        pnl_color = "#36C06A" if total_profit_amount >= 0 else "#E04B4B"
        profit_amount_color = "#36C06A" if total_profit_amount >= 0 else "#E04B4B"
        st.markdown("#### 포트폴리오 손익 요약")
        st.markdown(
            f"""
            <div class="pf-kpi-grid">
                <div class="pf-kpi-card">
                    <div class="pf-kpi-label">총 매수금액</div>
                    <div class="pf-kpi-value">{total_buy_amount:,.0f}원</div>
                </div>
                <div class="pf-kpi-card">
                    <div class="pf-kpi-label">현재 평가금액</div>
                    <div class="pf-kpi-value">{total_eval_amount:,.0f}원</div>
                </div>
                <div class="pf-kpi-card">
                    <div class="pf-kpi-label">총 수익 금액</div>
                    <div class="pf-kpi-value" style="color:{profit_amount_color};">{total_profit_amount:+,.0f}원</div>
                </div>
                <div class="pf-kpi-card">
                    <div class="pf-kpi-label">현재 수익률</div>
                    <div class="pf-kpi-value" style="color:{pnl_color};">{total_profit_pct:+.2f}%</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        alloc_df = df_joined.copy()
        alloc_df["평가금액"] = (
            pd.to_numeric(alloc_df["수량"], errors="coerce").fillna(0.0)
            * pd.to_numeric(alloc_df["현재가"], errors="coerce").fillna(0.0)
        )
        alloc_df = alloc_df[alloc_df["평가금액"] > 0].copy()
        if not alloc_df.empty:
            alloc_df["비중(%)"] = alloc_df["평가금액"] / alloc_df["평가금액"].sum() * 100.0
            alloc_df = alloc_df.sort_values("평가금액", ascending=False)
            chart_col, summary_col = st.columns([1.05, 1.15])
            with chart_col:
                alloc_chart = px.pie(
                    alloc_df,
                    names="종목명",
                    values="평가금액",
                    hole=0.58,
                    color_discrete_sequence=["#36C06A", "#60A5FA", "#FCD34D", "#F97316", "#A78BFA", "#E04B4B"],
                )
                alloc_chart.update_traces(
                    textposition="inside",
                    texttemplate="%{label}<br>%{percent}",
                    hovertemplate="%{label}<br>평가금액 %{value:,.0f}원<br>비중 %{percent}<extra></extra>",
                )
                alloc_chart.update_layout(
                    height=300,
                    margin=dict(l=0, r=0, t=8, b=8),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(color="#E5E7EB"),
                    showlegend=False,
                    annotations=[dict(text="평가<br>비중", x=0.5, y=0.5, font_size=15, showarrow=False, font_color="#E5E7EB")],
                )
                st.plotly_chart(alloc_chart, width="stretch", config={"displayModeBar": False})
            with summary_col:
                top_alloc = alloc_df.iloc[0]
                top_name = html.escape(str(top_alloc.get("종목명", "-")))
                top_weight = float(pd.to_numeric(top_alloc.get("비중(%)"), errors="coerce") or 0.0)
                top_eval = float(pd.to_numeric(top_alloc.get("평가금액"), errors="coerce") or 0.0)
                concentration_label = "분산 양호" if top_weight < 45 else ("집중 점검" if top_weight < 60 else "집중 높음")
                st.markdown(
                    f"""
                    <div class="decision-card" style="min-height:250px;">
                        <div class="decision-label">비중 요약</div>
                        <div class="decision-value">{concentration_label}</div>
                        <div class="decision-meta">
                            최대 비중: {top_name} {top_weight:.1f}%<br>
                            해당 평가금액: {top_eval:,.0f}원<br>
                            상세 손익과 매도 점검은 아래 현황 표 하나에서 확인합니다.
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        sell_watch_mask = df_joined["매도점검위험"].fillna(False).astype(bool)
        risk_rows = df_joined[df_joined["수급이탈위험"] | sell_watch_mask].copy()
        hold_rows = df_joined[~df_joined.index.isin(risk_rows.index)].copy()
        held_names = set(df_joined["종목명"].fillna("").astype(str))
        replacement_pool = build_replacement_pool(
            df_summary,
            held_names,
            risk_rows,
            capital_guard_active=capital_guard_active,
        )
        risk_names = ", ".join(risk_rows["종목명"].dropna().astype(str).head(3).tolist()) if not risk_rows.empty else "없음"
        hold_names = ", ".join(hold_rows["종목명"].dropna().astype(str).head(3).tolist()) if not hold_rows.empty else "없음"
        replacement_names = ", ".join(replacement_pool["종목명"].dropna().astype(str).head(3).tolist()) if not replacement_pool.empty else "없음"
        verdict = portfolio_assistant_verdict(
            risk_rows,
            replacement_pool,
            capital_guard_active=capital_guard_active,
            capital_emergency_active=capital_emergency_active,
        )
        assistant_verdict = verdict["verdict"]
        assistant_meta = verdict["meta"]
        verdict_color = verdict["color"]
        risk_strength = "없음" if risk_rows.empty else ("강함" if len(risk_rows) >= 2 else "점검")

        st.markdown("#### 오늘의 포트폴리오 결론")
        st.markdown(
            f"""
            <div class="decision-grid">
                <div class="decision-card">
                    <div class="decision-label">오늘 판단</div>
                    <div class="decision-value" style="color:{verdict_color};">{assistant_verdict}</div>
                    <div class="decision-meta">{assistant_meta}</div>
                </div>
                <div class="decision-card">
                    <div class="decision-label">매도/축소 점검</div>
                    <div class="decision-value">{len(risk_rows):,}개</div>
                    <div class="decision-meta">강도: {risk_strength}<br>{html.escape(risk_names)}</div>
                </div>
                <div class="decision-card">
                    <div class="decision-label">교체 후보</div>
                    <div class="decision-value">{len(replacement_pool):,}개</div>
                    <div class="decision-meta">{html.escape(replacement_names)}</div>
                </div>
            </div>
            <div class="decision-card" style="margin-bottom:10px;">
                <div class="decision-label">비서 코멘트</div>
                <div class="decision-meta">
                    유지 후보: {html.escape(hold_names)}<br>
                    교체는 기존 종목이 약해지고 새 후보가 확실히 강할 때만 검토합니다. 단순히 새 후보가 좋아 보인다는 이유만으로 갈아타지 않습니다.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown(
            f"""
            <div style="background:linear-gradient(135deg, #121827, #0f1523); border:1px solid #2A344A; border-radius:14px; padding:10px 14px; margin:10px 0 10px 0;">
                <div style="color:#E5E7EB; font-size:1.0em; font-weight:800;">내 포트폴리오 현황</div>
                <div style="color:#9CA3AF; font-size:0.84em; margin-top:2px;">손익, 비중, 스윙 순위, 매도 점검을 한 표로 압축했습니다.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        df_view = df_joined.copy()
        df_view["매수금액"] = (pd.to_numeric(df_view["수량"], errors="coerce").fillna(0.0) * pd.to_numeric(df_view["매수가"], errors="coerce").fillna(0.0))
        df_view["평가금액"] = (pd.to_numeric(df_view["수량"], errors="coerce").fillna(0.0) * pd.to_numeric(df_view["현재가"], errors="coerce").fillna(0.0))
        df_view["수익금액"] = df_view["평가금액"] - df_view["매수금액"]
        df_view["비중(%)"] = (df_view["평가금액"] / total_eval_amount * 100.0) if total_eval_amount > 0 else 0.0
        df_view["수익률(%)"] = (
            (pd.to_numeric(df_view["현재가"], errors="coerce").fillna(0.0) - pd.to_numeric(df_view["매수가"], errors="coerce").fillna(0.0))
            / pd.to_numeric(df_view["매수가"], errors="coerce").replace(0, pd.NA)
        ) * 100.0
        df_view["상태"] = (df_view["수급이탈위험"] | df_view["매도점검위험"]).apply(lambda x: "⚠ 경보" if bool(x) else "정상")
        display_cols = [
            "종목명", "상태", "리스크사유", "비중(%)", "수익률(%)", "수익금액",
            "스윙우선순위", "현재_순위", "매도점검",
            "외인강도(%)", "연기금강도(%)", "현재가", "수량", "매수가"
        ]
        df_display_port = df_view[display_cols].copy()

        def _row_style(row):
            return ["background-color: rgba(224,75,75,0.12);" if row.get("상태") == "⚠ 경보" else "" for _ in row]

        st.dataframe(
            df_display_port.style.apply(_row_style, axis=1).format({
                "비중(%)": "{:.1f}%",
                "수익금액": "{:+,.0f}원",
                "수익률(%)": "{:+.2f}%",
                "스윙우선순위": "{:.2f}",
                "현재_순위": "{:.0f}위",
                "외인강도(%)": "{:+.1f}%",
                "연기금강도(%)": "{:+.1f}%",
                "현재가": "{:,.0f}",
                "수량": "{:,.0f}",
                "매수가": "{:,.0f}"
            }),
            width='stretch',
            hide_index=True,
            column_order=display_cols
        )

        with st.expander("카드형 상세 보기", expanded=False):
            card_cols = st.columns(2)
            for i, (_, row) in enumerate(df_joined.iterrows()):
                with card_cols[i % 2]:
                    cur = float(pd.to_numeric(row.get("현재가"), errors="coerce") or 0.0)
                    chg = float(pd.to_numeric(row.get("등락률"), errors="coerce") or 0.0)
                    qty = float(pd.to_numeric(row.get("수량"), errors="coerce") or 0.0)
                    buy = float(pd.to_numeric(row.get("매수가"), errors="coerce") or 0.0)
                    ai = float(pd.to_numeric(row.get("AI수급점수"), errors="coerce") or 0.0)
                    swing = float(pd.to_numeric(row.get("스윙우선순위"), errors="coerce") or 0.0)
                    swing_rank = int(pd.to_numeric(row.get("현재_순위"), errors="coerce") or 0)
                    ai_rank = int(pd.to_numeric(row.get("AI순위"), errors="coerce") or 0)
                    fs = float(pd.to_numeric(row.get("외인강도(%)"), errors="coerce") or 0.0)
                    ps = float(pd.to_numeric(row.get("연기금강도(%)"), errors="coerce") or 0.0)
                    sig = float(pd.to_numeric(row.get("신호신뢰도"), errors="coerce") or 0.0)
                    pnl = ((cur - buy) / buy * 100.0) if buy > 0 and cur > 0 else None
                    buy_amt = qty * buy
                    eval_amt = qty * cur
                    profit_amt = eval_amt - buy_amt
                    weight_pct = (eval_amt / total_eval_amount * 100.0) if total_eval_amount > 0 else 0.0
                    risk_flag = bool(row.get("수급이탈위험", False)) or bool(row.get("매도점검위험", False))
                    border = "#E04B4B" if risk_flag else "#2C3242"
                    bg = "rgba(224,75,75,0.14)" if risk_flag else "linear-gradient(135deg, #171A24, #121A2C)"
                    pnl_txt = f"{pnl:+.2f}%" if pnl is not None else "-"
                    pnl_color = "#36C06A" if pnl is not None and pnl >= 0 else "#E04B4B"
                    amt_color = "#36C06A" if profit_amt >= 0 else "#E04B4B"
                    chg_color = "#36C06A" if chg >= 0 else "#E04B4B"
                    risk_reason = html.escape(str(row.get("리스크사유", "-")))
                    risk_badge = (
                        '<span style="background:rgba(224,75,75,0.16); color:#FCA5A5; border:1px solid rgba(224,75,75,0.35); border-radius:999px; padding:3px 10px; font-size:0.76em;">⚠ 비중 축소 권고</span>'
                        if risk_flag
                        else '<span style="background:rgba(54,192,106,0.16); color:#86EFAC; border:1px solid rgba(54,192,106,0.35); border-radius:999px; padding:3px 10px; font-size:0.76em;">안정</span>'
                    )
                    st.markdown(
                        f"""
                        <div class="pf-animated-card" style="background:{bg}; border:1px solid {border}; border-radius:16px; padding:12px 14px; margin-bottom:10px; box-shadow:0 10px 28px rgba(0,0,0,0.24);">
                            <div class="pf-animated-glow"></div>
                            <div style="display:flex; justify-content:space-between; align-items:center; gap:8px;">
                                <div style="color:#F5F7FA; font-size:1.15em; font-weight:800;">{row.get('종목명', '-')}</div>
                                <div style="color:{pnl_color}; font-size:1.25em; font-weight:900;">{pnl_txt}</div>
                            </div>
                            <div style="color:#9CA3AF; margin-top:5px; font-size:0.85em;">비중 {weight_pct:.1f}% | 등락률 <span style="color:{chg_color}; font-weight:700;">{chg:+.2f}%</span></div>
                            <div style="color:#D1D5DB; margin-top:6px; font-size:0.87em;">현재가 {cur:,.0f}원 · 보유 {qty:,.0f}주 · 매수가 {buy:,.0f}원</div>
                            <div style="display:flex; gap:7px; flex-wrap:wrap; margin-top:8px;">
                                <span style="background:#182236; color:#D1D5DB; border:1px solid #2D3A55; border-radius:999px; padding:3px 9px; font-size:0.79em;">매수금액 {buy_amt:,.0f}원</span>
                                <span style="background:#182236; color:{amt_color}; border:1px solid #2D3A55; border-radius:999px; padding:3px 9px; font-size:0.79em;">수익금액 {profit_amt:+,.0f}원</span>
                            </div>
                            <div style="margin-top:8px;">{risk_badge}</div>
                            <div style="color:#AAB2C5; margin-top:6px; font-size:0.82em;">경보 사유: {risk_reason}</div>
                            <div style="color:#AAB2C5; margin-top:8px; font-size:0.84em;">스윙 {swing:.1f} · {swing_rank}위 | AI {ai:.1f} · {ai_rank}위 | 신호 {row.get('신호등급','-')} ({sig:.1f})</div>
                            <div style="color:#AAB2C5; margin-top:4px; font-size:0.84em;">{row.get('매수후보','-')} · {row.get('진입유형','-')} · 점검 {row.get('매도점검','-')} | 외인 {fs:+.1f}% · 기금 {ps:+.1f}%</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    st.markdown("---")
    with st.expander("포트폴리오 편집 및 저장", expanded=False):
        pf_count = int(df_port_saved["종목명"].fillna("").astype(str).str.strip().ne("").sum()) if not df_port_saved.empty else 0
        pf_qty_sum = float(pd.to_numeric(df_port_saved.get("수량", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
        pf_cost_sum = float(
            (
                pd.to_numeric(df_port_saved.get("수량", pd.Series(dtype=float)), errors="coerce").fillna(0)
                * pd.to_numeric(df_port_saved.get("매수가", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
            ).sum()
        )

        st.markdown(
            f"""
            <div style="background:linear-gradient(135deg, #121827, #0f1523); border:1px solid #2A344A; border-radius:14px; padding:12px 14px; margin:8px 0 10px 0;">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                    <div style="color:#E5E7EB; font-size:1.02em; font-weight:800;">포트폴리오 에디터</div>
                    <div style="color:#93C5FD; font-size:0.82em;">자동완성 종목 선택 지원</div>
                </div>
                <div style="display:flex; gap:8px; flex-wrap:wrap;">
                    <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">보유 종목 {pf_count}개</span>
                    <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">총 수량 {pf_qty_sum:,.0f}</span>
                    <span style="background:#1b2233; color:#D1D5DB; border:1px solid #2B364C; border-radius:999px; padding:4px 10px; font-size:0.82em;">총 매입금액 {pf_cost_sum:,.0f}원</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        edited_portfolio = st.data_editor(
            df_port_saved if not df_port_saved.empty else pd.DataFrame(columns=base_cols),
            num_rows="dynamic",
            use_container_width=True,
            key="admin_portfolio_editor",
            column_config={
                "종목명": st.column_config.SelectboxColumn("종목명", options=stock_options, required=False),
                "수량": st.column_config.NumberColumn("수량", min_value=0, step=1),
                "매수가": st.column_config.NumberColumn("매수가(원)", min_value=0.0, step=100.0, format="%.0f원"),
            },
        )
        st.caption("팁: 종목명 셀을 클릭하면 현재 스크리너 종목 리스트에서 빠르게 선택할 수 있습니다.")

        if st.button("포트폴리오 저장", type="primary", use_container_width=True):
            save_admin_portfolio_df(edited_portfolio)
            st.success("포트폴리오를 저장했습니다. 상단 현황에 즉시 반영됩니다.")
            st.rerun()

    st.markdown("---")
    render_section_header("관리 설정", "내 포트폴리오 확인 이후에 사용하는 관리자 전용 설정입니다.")

    with st.expander("리스크 경보 임계값 설정", expanded=False):
        st.markdown(
            """
            <div style="background:linear-gradient(135deg, #141A26, #101624); border:1px solid #2A344A; border-radius:12px; padding:12px 14px; margin-bottom:10px;">
                <div style="color:#E5E7EB; font-weight:700; margin-bottom:6px;">경보 규칙 안내</div>
                <div style="color:#AAB2C5; font-size:0.92em; line-height:1.55;">
                    • <b>매도점검 경보</b>: 매도점검 문구에 매도/제외/훼손/축소/주의/청산/이탈이 포함될 때<br/>
                    • <b>복합 경보</b>: 외인과 연기금이 동시에 약해지고, 스윙 점수가 기준 미만일 때<br/>
                    • <b>단독 급락 경보</b>: AI수급점수가 급락 기준 미만일 때
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        col_thr1, col_thr2 = st.columns(2)
        with col_thr1:
            swing_warn_threshold_new = st.slider(
                "스윙 약화 기준 (수급 동반 약화 때 적용)",
                min_value=0,
                max_value=100,
                value=int(st.session_state.get("admin_swing_warn_threshold", 55)),
                step=1,
                key="admin_swing_warn_threshold",
            )
        with col_thr2:
            ai_critical_threshold_new = st.slider(
                "AI수급 단독 급락 기준",
                min_value=0,
                max_value=100,
                value=int(st.session_state.get("admin_ai_critical_threshold", 35)),
                step=1,
                key="admin_ai_critical_threshold",
            )
        if (
            int(swing_warn_threshold_new) != int(saved_thr["swing_warn_threshold"])
            or int(ai_critical_threshold_new) != int(saved_thr["ai_critical_threshold"])
        ):
            save_admin_risk_thresholds(swing_warn_threshold_new, ai_critical_threshold_new)
        st.markdown(
            f"""
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:6px;">
                <span style="background:rgba(59,130,246,0.16); color:#93C5FD; border:1px solid rgba(59,130,246,0.35); border-radius:999px; padding:4px 10px; font-size:0.82em;">
                    스윙 약화 기준: {int(swing_warn_threshold_new)}
                </span>
                <span style="background:rgba(224,75,75,0.16); color:#FCA5A5; border:1px solid rgba(224,75,75,0.35); border-radius:999px; padding:4px 10px; font-size:0.82em;">
                    AI수급 급락 기준: {int(ai_critical_threshold_new)}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

