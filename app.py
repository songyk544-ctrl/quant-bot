import streamlit as st
import requests
import json
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="신규 API 검증기", page_icon="🔬")
st.title("🔬 V2.92 한투 신규 수급 API (필수 파라미터 완비)")

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
            "tr_id": "FHPTJ04160001", 
            "custtype": "P"
        }
        
        today_str = datetime.now().strftime("%Y%m%d")
        month_ago_str = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily" 
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_INPUT_ISCD": "005930", 
            "FID_INPUT_DATE_1": month_ago_str, 
            "FID_INPUT_DATE_2": today_str,
            "FID_ORG_ADJ_PRC": "0"  # 🔥 누락되었던 필수 파라미터 (0: 수정주가) 추가
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        st.write(f"**HTTP 상태 코드:** {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            st.success("✅ 통신 성공! 아래 원본 JSON을 확인해주세요.")
            
            output_list = data.get('output', [])
            if output_list:
                st.write("### 📦 최근 데이터 (배열 첫 번째)")
                st.json(output_list[0]) 
                
                daily = output_list[0]
                st.write("---")
                st.write("### 🔍 주요 수급 키워드 탐색")
                filtered_data = {k: v for k, v in daily.items() if any(keyword in k for keyword in ['pnsn', 'pef', 'itst', 'ivtr', 'frgn'])}
                st.json(filtered_data)
                
            else:
                st.warning("output 데이터가 비어있습니다. 전체 응답을 확인하세요.")
                st.json(data)
                
        else:
            st.error("통신 에러!")
            st.json(res.json())
