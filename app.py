import streamlit as st
import requests
import json

st.set_page_config(layout="wide", page_title="단일 종목 정밀 검증기", page_icon="🔬")
st.title("🔬 V2.8 한투 수급 API 정밀 검증기 (단일 종목)")

URL_BASE = "https://openapi.koreainvestment.com:9443"

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

if st.button("🚀 삼성전자 수급 데이터 1초 검증"):
    with st.spinner("한투 서버 통신 중..."):
        token = get_kis_access_token()
        headers = {
            "authorization": f"Bearer {token}", "appkey": st.secrets["KIS_APP_KEY"],
            "appsecret": st.secrets["KIS_APP_SECRET"], "tr_id": "FHKST01010900", "custtype": "P"
        }
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/inquire-investor"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930"} # 삼성전자
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            # output이 리스트인지, output2에 있는지 확인
            daily_list = data.get('output', []) if isinstance(data.get('output'), list) else data.get('output2', [])
            
            if daily_list:
                st.success("✅ 통신 성공! 아래는 어제(최근 1영업일)의 수급 원본 데이터입니다.")
                
                # 1. 원본 JSON (한투가 뱉는 진짜 이름표 확인용)
                st.json(daily_list[0]) 
                
                # 2. 스마트 탐지기 파싱 테스트
                f_qty, p_qty, t_qty, pef_qty = 0, 0, 0, 0
                daily = daily_list[0]
                
                for k, v in daily.items():
                    if v and 'qty' in k: # 수량(qty) 데이터만 스캔
                        try:
                            val = int(v)
                            if 'frgn' in k: f_qty = val
                            elif 'pnsn' in k: p_qty = val
                            elif 'ivtr' in k or 'itst' in k: t_qty = val
                            elif 'pef' in k or 'sppi' in k: pef_qty = val
                        except: pass
                        
                st.write("---")
                st.write("### 🔍 스마트 탐지기 분류 결과 (어제 하루치 수량)")
                st.info(f"**외국인:** {f_qty:,} 주")
                st.warning(f"**연기금:** {p_qty:,} 주")
                st.error(f"**투신:** {t_qty:,} 주")
                st.error(f"**사모펀드:** {pef_qty:,} 주")
                
            else:
                st.error("데이터가 비어있습니다. API 한도를 초과했거나 응답이 없습니다.")
                st.json(data)
        else:
            st.error(f"통신 에러! 상태 코드: {res.status_code}")
            st.json(res.json())
