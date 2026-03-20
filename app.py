import streamlit as st
import requests

st.set_page_config(layout="wide", page_title="다음 금융 수급 검증기", page_icon="🔬")
st.title("🔬 V2.82 진짜 연기금/사모펀드 탐지기 (Daum API)")

if st.button("🚀 삼성전자 전영업일 수급 1초 검증 (Daum)"):
    with st.spinner("다음 금융 서버 몰래 접속 중..."):
        # 다음 금융의 숨겨진 수급 API 주소
        url = "https://finance.daum.net/api/investor/days"
        
        # 봇 차단을 피하기 위한 위장 신분증(Headers)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://finance.daum.net/quotes/A005930",
        }
        
        # 삼성전자(A005930) 1페이지 요청
        params = {
            "symbolCode": "A005930",
            "page": 1,
            "perPage": 5
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            # data 리스트의 [1]번 인덱스가 확정된 전영업일 데이터입니다.
            target_daily = data['data'][1] 
            
            st.success(f"✅ 통신 성공! 타겟 영업일자: {target_daily['date']} (전영업일 확정 데이터)")
            
            # 1. 원본 JSON (한투와 비교해보세요!)
            st.write("### 📦 원본 JSON 데이터 (Daum Finance)")
            st.json(target_daily)
            
            # 2. 분류 결과
            st.write("---")
            st.write("### 🔍 상세 분류 결과")
            st.info(f"**외국인:** {target_daily.get('foreign', 0):,} 주")
            st.warning(f"**연기금:** {target_daily.get('pension', 0):,} 주")
            st.error(f"**투신:** {target_daily.get('investmentTrust', 0):,} 주")
            st.error(f"**사모펀드:** {target_daily.get('privateEquity', 0):,} 주")
        else:
            st.error(f"통신 에러! 상태 코드: {res.status_code}")
