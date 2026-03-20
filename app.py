import streamlit as st
import requests
import json
from datetime import datetime

st.set_page_config(layout="wide", page_title="신규 API 검증기", page_icon="🔬")
st.title("🔬 V2.94 한투 신규 수급 API (완벽 매핑 완료)")

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
        
        # 기준일자: 오늘 (오늘을 기준으로 과거 데이터를 배열로 던져줍니다)
        today_str = datetime.now().strftime("%Y%m%d")
        
        url = f"{URL_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily" 
        
        # 🔥 대표님이 주신 명세서와 100% 일치하는 5개의 파라미터
        params = {
            "FID_COND_MRKT_DIV_CODE": "J", 
            "FID_INPUT_ISCD": "005930", 
            "FID_INPUT_DATE_1": today_str, # 입력 날짜1
            "FID_ORG_ADJ_PRC": "0",        # 수정주가 원주가 가격 (0: 반영)
            "FID_ETC_CLS_CODE": "0"        # 기타 구분 코드 (0: 수량)
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            
            # 응답 코드가 0이면 성공
            if data.get('rt_cd') == "0":
                st.success("✅ 통신 성공! 데이터 추출을 시작합니다.")
                output2_list = data.get('output2', []) # 리스트 데이터는 output2에 담김
                
                if output2_list:
                    # 장중이 아닌, 가장 최근 확정일자(어제) 데이터를 안전하게 가져오기 위함
                    target_daily = output2_list[1] if len(output2_list) > 1 else output2_list[0]
                    target_date = target_daily.get('stck_bsop_date', '알수없음')
                    
                    st.write(f"### 📦 [{target_date}] 기준 리얼 수급 데이터")
                    
                    # 🔥 명세서에서 발췌한 정확한 이름표로 데이터 추출
                    f_qty = int(target_daily.get('frgn_ntby_qty', 0))    # 외국인
                    p_qty = int(target_daily.get('fund_ntby_qty', 0))    # 기금(연기금)
                    t_qty = int(target_daily.get('ivtr_ntby_qty', 0))    # 투자신탁(투신)
                    pef_qty = int(target_daily.get('pe_fund_ntby_vol', 0)) # 사모펀드 (혼자 vol 사용)
                    o_qty = int(target_daily.get('orgn_ntby_qty', 0))    # 기관계 합계
                    
                    st.info(f"**외국인:** {f_qty:,} 주")
                    st.warning(f"**연기금(기금):** {p_qty:,} 주")
                    st.error(f"**투신:** {t_qty:,} 주")
                    st.error(f"**사모펀드:** {pef_qty:,} 주")
                    st.success(f"**기관합계:** {o_qty:,} 주")
                    
                    st.write("---")
                    st.write("🔍 원본 JSON 구조 확인 (디버깅용)")
                    st.json(target_daily)
                    
            else:
                st.error(f"❌ API 로직 에러: {data.get('msg1')}")
                st.json(data)
                
        else:
            st.error(f"❌ HTTP 통신 에러: {res.status_code}")
            st.json(res.json())
