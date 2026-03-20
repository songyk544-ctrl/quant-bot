import streamlit as st
import requests
import json

st.set_page_config(layout="wide", page_title="신규 API 검증기", page_icon="🔬")
st.title("🔬 V2.9 한투 신규 수급 API (FHPTJ04160001) 검증기")

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
            "tr_id": "FHPTJ04160001", # 🔥 대표님이 찾아낸 전설의 TR ID
            "custtype": "P"
        }
        # 🔥 대표님이 찾아낸 진짜 수급 URL
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily" 
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_INPUT_ISCD": "005930" # 삼성전자
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        st.write(f"**HTTP 상태 코드:** {res.status_code}")
        
        if res.status_code == 200:
            data = res.json()
            st.success("✅ 통신 성공! 아래 원본 JSON을 확인해주세요.")
            
            # output 리스트 중 첫 번째(어제) 또는 두 번째 데이터 까보기
            output_list = data.get('output', [])
            if output_list:
                st.write("### 📦 1영업일 전 (또는 오늘 장중) 데이터")
                st.json(output_list[0])
                
                if len(output_list) > 1:
                    st.write("### 📦 2영업일 전 (확정) 데이터")
                    st.json(output_list[1])
            else:
                st.warning("output 데이터가 비어있습니다. 전체 응답을 확인하세요.")
                st.json(data)
                
        else:
            st.error("통신 에러!")
            st.json(res.json())
