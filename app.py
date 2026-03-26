import streamlit as st
import pandas as pd
import altair as alt
import os
import google.generativeai as genai

st.set_page_config(layout="wide", page_title="수급 퀀트 비서 V6.1", page_icon="⚡")
st.title("⚡ 실전 수급 스윙 대시보드 V6.1")
st.caption("🌐 당일 리얼타임 수급 + 글로벌 매크로 AI 리포트 연동")

def load_data():
    df_summary = pd.read_csv("data.csv") if os.path.exists("data.csv") else pd.DataFrame()
    df_hist = pd.read_csv("history.csv") if os.path.exists("history.csv") else pd.DataFrame()
    return df_summary, df_hist

df_summary, df_history = load_data()

if df_summary.empty:
    st.warning("⏳ 데이터 수집 봇이 아직 파일을 생성하지 않았습니다. GitHub Actions를 확인해주세요.")
else:
    if "selected_stock" not in st.session_state:
        st.session_state.selected_stock = df_summary['종목명'].iloc[0]

    tab1, tab2, tab3 = st.tabs(["📊 리얼 수급 랭킹", f"📈 [{st.session_state.selected_stock}] 퀀트 차트", "📝 당일 AI 마감 리포트"])

    with tab1:
        def color_score(val):
            color = "#E74C3C" if val >= 80 else "#F1C40F" if val >= 60 else "gray"
            return f'color: {color}; font-weight: bold;'
            
        def color_fluctuation(val):
            if val > 0: return 'color: #FF3333; font-weight: bold;'
            elif val < 0: return 'color: #0066FF; font-weight: bold;'
            return 'color: gray;'

        df_display = df_summary.set_index('종목명')

        styled_df = df_display.style.map(color_score, subset=['AI수급점수']) \
                                    .map(color_fluctuation, subset=['등락률', '외인강도(%)', '연기금강도(%)', '투신강도(%)', '사모강도(%)']) \
                                    .format({"현재가": "{:,.0f}", "시가총액": "{:,.0f}", "등락률": "{:.2f}%", 
                                             "외인강도(%)": "{:.2f}%", "연기금강도(%)": "{:.2f}%", 
                                             "투신강도(%)": "{:.2f}%", "사모강도(%)": "{:.2f}%",
                                             "이격도(%)": "{:.1f}%", "손바뀜(%)": "{:.1f}%",
                                             "PER": "{:.1f}", "ROE": "{:.1f}%"})

        event = st.dataframe(
            styled_df, on_select="rerun", selection_mode="single-row",
            column_config={
                "_index": st.column_config.TextColumn("종목명", width="small"),
                "AI수급점수": st.column_config.NumberColumn("🏆 AI 점수"),
                "현재가": st.column_config.Column("현재가"),
                "등락률": st.column_config.Column("등락"),
                "외인강도(%)": st.column_config.Column("외인(1달)"),
                "연기금강도(%)": st.column_config.Column("연기금(1달)"),
                "투신강도(%)": st.column_config.Column("투신(1달)"),
                "사모강도(%)": st.column_config.Column("사모(1달)"),
                "외인연속": st.column_config.NumberColumn("외인연속", format="%d일"),
                "연기금연속": st.column_config.NumberColumn("기금연속", format="%d일"),
                "이격도(%)": st.column_config.Column("이격도"),
                "손바뀜(%)": st.column_config.Column("손바뀜"),
                "시가총액": st.column_config.Column("시총(억)"),
                "PER": st.column_config.Column("PER"),
                "ROE": st.column_config.Column("ROE"),
                "소속": st.column_config.Column("시장")
            },
            hide_index=False, 
            use_container_width=True, height=600 
        )
        
        if event.selection.rows:
            st.session_state.selected_stock = df_summary.iloc[event.selection.rows[0]]['종목명']

    with tab2:
        selected_row = df_summary[df_summary['종목명'] == st.session_state.selected_stock].iloc[0]
        st.subheader(f"💡 {st.session_state.selected_stock} 기술적 분석")
        st.write(f"- **현재가:** {selected_row['현재가']:,}원 ({selected_row['등락률']}%)")
        st.write(f"- **수급 종합점수:** **{selected_row['AI수급점수']}점** (외인 {selected_row['외인연속']}일 / 연기금 {selected_row['연기금연속']}일 연속 매수)")
        
        tech_status = "🟢 완벽한 눌림목 타점" if 101 <= selected_row['이격도(%)'] <= 108 else ("🔴 하락 추세 (관망)" if selected_row['이격도(%)'] < 95 else "⚫ 보유자 영역")
        st.write(f"- **기술적 타점:** 20일선 이격도 {selected_row['이격도(%)']}% ({tech_status}) / 5일 손바뀜 {selected_row['손바뀜(%)']}%")
        
        st.markdown("---")
        
        if not df_history.empty:
            target_hist = df_history[df_history['종목명'] == st.session_state.selected_stock].copy()
            
            if not target_hist.empty:
                target_hist['일자'] = pd.to_datetime(target_hist['일자'].astype(str))
                target_hist = target_hist.sort_values('일자')
                target_hist['일자_표시'] = target_hist['일자'].dt.strftime('%m/%d')

                col1, col2 = st.columns(2)
                
                color_scale = alt.Scale(
                    domain=['외인', '연기금', '투신', '사모'],
                    range=['#FF4B4B', '#1C83E1', '#F1C40F', '#83C9FF'] 
                )

                with col1:
                    st.markdown("##### 📈 일봉 차트 (최근 20일 종가)")
                    chart_close = alt.Chart(target_hist).mark_line(color='#1C83E1', point=True).encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('종가:Q', scale=alt.Scale(zero=False), title=None)
                    ).properties(height=280)
                    st.altair_chart(chart_close, use_container_width=True)
                    
                with col2:
                    st.markdown("##### 📊 당일 세력별 순매수 대금 (백만원)")
                    bar_data = target_hist.melt(id_vars=['일자_표시'], value_vars=['외인', '연기금', '투신', '사모'], 
                                                 var_name='투자자', value_name='금액')
                    chart_bar = alt.Chart(bar_data).mark_bar().encode(
                        x=alt.X('일자_표시:O', sort=None, axis=alt.Axis(title=None, labelAngle=-45)),
                        y=alt.Y('금액:Q', title=None),
                        color=alt.Color('투자자:N', scale=color_scale, legend=alt.Legend(title=None, orient='bottom', direction='horizontal')),
                        order=alt.Order('투자자:N', sort='descending')
                    ).properties(height=280)
                    st.altair_chart(chart_bar, use_container_width=True)

    with tab3:
        st.subheader("📰 오늘의 Top-Down 마감 시황 & 관심종목")
        st.write("실시간 글로벌 매크로 이벤트와 금일 수급 데이터를 종합하여 AI가 리포트를 작성합니다.")
        
        if st.button("🚀 오늘의 AI 리포트 생성하기", use_container_width=True):
            api_key = st.secrets.get("GEMINI_API_KEY")
            
            if not api_key:
                st.error("🔑 Streamlit 설정(Secrets)에 GEMINI_API_KEY를 입력해주세요!")
            else:
                with st.spinner("AI가 글로벌 매크로 이벤트와 당일 수급 데이터를 융합 분석 중입니다... (약 10초 소요)"):
                    try:
                        genai.configure(api_key=api_key)
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        
                        # 🔥 [핵심 추가] 대표님 아이디어 적용! 당일 history 데이터와 summary 데이터를 결합합니다.
                        top_15_names = df_summary.head(15)['종목명'].tolist()
                        latest_date = df_history['일자'].max() # history에서 가장 최근(오늘) 날짜 추출
                        
                        # 오늘자 history 데이터만 필터링
                        df_today = df_history[(df_history['일자'] == latest_date) & (df_history['종목명'].isin(top_15_names))]
                        
                        # data.csv의 누적 스펙과 history.csv의 오늘자 순매수 금액(백만원)을 합칩니다!
                        df_merged = pd.merge(
                            df_summary.head(15)[['종목명', '소속', 'AI수급점수', '이격도(%)', '손바뀜(%)']],
                            df_today[['종목명', '외인', '연기금']], # 당일 외인/연기금 순매수액 (단위: 백만원)
                            on='종목명', how='left'
                        )
                        
                        # 컬럼명 직관적으로 변경
                        df_merged.rename(columns={'외인': '당일_외인순매수(백만)', '연기금': '당일_연기금순매수(백만)'}, inplace=True)
                        top_data_str = df_merged.to_string(index=False)
                        
                        prompt = f"""
                        너는 여의도 최고의 탑다운(Top-Down) 퀀트 애널리스트야.
                        아래는 오늘자 수급 및 기술적 지표 최상위 15개 종목의 리얼타임 데이터야.
                        ('당일_외인/연기금순매수'의 단위는 백만 원이야. 즉 10,000은 100억 원을 의미해.)

                        {top_data_str}

                        다음 순서로 전문가 수준의 마감 리포트를 작성해 줘:
                        1. 🌐 글로벌 매크로 & 이벤트 브리핑: 
                           - 현재 시장에 큰 영향을 미치고 있는 최신 글로벌 이벤트(예: 뉴욕 증시, 금리 정책, 환율 변화, 지정학적 리스크 등)를 반드시 짚어줘.
                           - 내가 혹시 놓쳤을 법한 글로벌 리스크나 거시적 관점의 추가 코멘트를 꼭 포함해서 오늘 시장의 배경을 설명해 줘.
                        2. 🌪️ 국내 증시 섹터 및 당일 수급 동향: 
                           - 위 데이터를 보고, 오늘 외인과 기관의 뭉칫돈이 어떤 시장(코스피/코스닥)의 어떤 섹터(예: 반도체, 바이오, 전력기기 등)로 집중되었는지 당일 매수 금액을 바탕으로 추론해서 요약해 줘.
                        3. 🎯 내일의 Top 3 관심종목 & 매크로 관점 추천 사유: 
                           - 위 데이터 중 이격도(눌림목)와 손바뀜이 좋으면서, 당일 수급이 강력하고 거시적 환경에 부합하는 3종목을 꼽고 그 이유를 냉철하게 작성해 줘.
                        """
                        
                        response = model.generate_content(prompt)
                        st.markdown(response.text)
                        
                    except Exception as e:
                        st.error(f"⚠️ 리포트 생성 중 오류 발생: {e}")
