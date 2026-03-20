import streamlit as st
import requests
import json
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="신규 API 검증기", page_icon="🔬")
st.title("🔬 V2.95 한투 신규 수급 API (전영업일 우회 테스트)")

URL_BASE = "https://openapi.koreainvestment.com:9443"

def get_kis_access_token():
    url = f"{URL_BASE}/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": st.secrets["KIS_APP_KEY"], "appsecret": st.secrets["KIS_APP_SECRET"]}
    res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
    return res.json().get("access_token")

if st.button("🚀 삼성전자 '어제' 수급 1초 검증"):
    with st.spinner("한투 서버 통신 중..."):
        token = get_kis_access_token()
        headers = {
            "authorization": f"Bearer {token}", 
            "appkey": st.secrets["KIS_APP_KEY"],
            "appsecret": st.secrets["KIS_APP_SECRET"], 
            "tr_id": "FHPTJ04160001", 
            "custtype": "P"
        }
        
        # 🔥 핵심: 오늘 날짜가 아닌 '어제(전 영업일)' 및 '한 달 전' 날짜로 세팅!
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        month_ago_str = (datetime.now() - timedelta(days=31)).strftime("%Y%m%d")
        
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily" 
        
        # 파라미터에 어제 날짜를 투입하여 장중 시간 제한 우회 시도
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_INPUT_ISCD": "005930", 
            "FID_INPUT_DATE_1": month_ago_str, # 시작일자
            "FID_INPUT_DATE_2": yesterday_str, # 종료일자 (어제!)
            "FID_ORG_ADJ_PRC": "0",
            "FID_ETC_CLS_CODE": "0" 
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            
            if data.get('rt_cd') == "0":
                st.success("✅ 통신 성공! 대표님의 가설이 맞았습니다. 시간 제한을 우회했습니다!")
                output2_list = data.get('output2', []) 
                
                if output2_list:
                    # 조회 종료일을 어제로 했으므로, 배열의 첫 번째 값이 가장 최근 확정 데이터(어제)
                    target_daily = output2_list[0] 
                    target_date = target_daily.get('stck_bsop_date', '알수없음')
                    
                    st.write(f"### 📦 [{target_date}] 기준 리얼 수급 데이터")
                    
                    f_qty = int(target_daily.get('frgn_ntby_qty', 0))    # 외국인
                    p_qty = int(target_daily.get('fund_ntby_qty', 0))    # 기금(연기금)
                    t_qty = int(target_daily.get('ivtr_ntby_qty', 0))    # 투자신탁(투신)
                    pef_qty = int(target_daily.get('pe_fund_ntby_vol', 0)) # 사모펀드
                    
                    st.info(f"**외국인:** {f_qty:,} 주")
                    st.warning(f"**연기금(기금):** {p_qty:,} 주")
                    st.error(f"**투신:** {t_qty:,} 주")
                    st.error(f"**사모펀드:** {pef_qty:,} 주")
                    
                    st.write("---")
                    st.write("🔍 원본 JSON 구조 확인")
                    st.json(target_daily)
            else:
                st.error(f"❌ API 로직 에러: {data.get('msg1')}")
                st.json(data)
        else:
            st.error(f"❌ HTTP 통신 에러: {res.status_code}")
            st.json(res.json())
