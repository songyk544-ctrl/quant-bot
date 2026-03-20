import streamlit as st
import requests
import json
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="신규 API 검증기", page_icon="🔬")
st.title("🔬 V2.91 한투 신규 수급 API (날짜 지정 완료)")

URL_BASE = "https://openapi.koreainvestment.com:9443"

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

if st.button("🚀 삼성전자 '신규 API' 수급 1초 검증"):
    with st.spinner("한투 서버 통신 중..."):
        token = get_kis_access_token()
        headers = {
            "authorization": f"Bearer {token}", 
            "appkey": st.secrets["KIS_APP_KEY"],
            "appsecret": st.secrets["KIS_APP_SECRET"], 
            "tr_id": "FHPTJ04160001", # 대표님이 발굴하신 TR ID
            "custtype": "P"
        }
        
        # 날짜 계산 (오늘부터 30일 전까지)
        today_str = datetime.now().strftime("%Y%m%d")
        month_ago_str = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily" 
        
        # 💡 에러의 원인이었던 날짜(DATE_1, DATE_2) 파라미터 추가!
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_INPUT_ISCD": "005930", # 삼성전자
            "FID_INPUT_DATE_1": month_ago_str, # 시작일자 (30일 전)
            "FID_INPUT_DATE_2": today_str      # 종료일자 (오늘)
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        st.write(f"**HTTP 상태 코드:** {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            st.success("✅ 통신 성공! 아래 원본 JSON을 확인해주세요.")
            
            output_list = data.get('output', [])
            if output_list:
                st.write("### 📦 최근 확정 데이터 (배열 첫 번째)")
                st.json(output_list[0]) # 가장 최근 날짜의 데이터 출력
                
                # 테스트로 키워드 뽑아보기
                daily = output_list[0]
                st.write("---")
                st.write("### 🔍 주요 수급 키워드 탐색")
                # 연기금, 투신, 사모 등 의심되는 단어가 들어간 데이터만 필터링해서 보여줍니다.
                filtered_data = {k: v for k, v in daily.items() if any(keyword in k for keyword in ['pnsn', 'pef', 'itst', 'ivtr', 'frgn'])}
                st.json(filtered_data)
                
            else:
                st.warning("output 데이터가 비어있습니다. 전체 응답을 확인하세요.")
                st.json(data)
                
        else:
            st.error("통신 에러!")
            st.json(res.json())
