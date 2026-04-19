"""
DART Open API 기업고유코드 ZIP 다운로드 → 압축 해제 → XML 파싱 → dart_map.csv 생성.

실행: python make_dart_map.py

DART_API_KEY: 환경변수 또는 .streamlit/secrets.toml / secrets.toml
"""
import io
import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import requests
import tomllib

CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"


def resolve_dart_api_key():
    key = os.environ.get("DART_API_KEY")
    if key:
        return str(key).strip()
    for p in (Path(".streamlit/secrets.toml"), Path("secrets.toml")):
        try:
            if p.exists():
                with p.open("rb") as f:
                    parsed = tomllib.load(f)
                k = parsed.get("DART_API_KEY")
                if k:
                    return str(k).strip()
        except Exception:
            continue
    return None


def main():
    api_key = resolve_dart_api_key()
    if not api_key:
        print("[ERROR] DART_API_KEY를 찾을 수 없습니다. 환경변수 또는 secrets.toml을 설정하세요.")
        sys.exit(1)

    print("[INFO] DART corpCode.xml 다운로드 중...")
    try:
        res = requests.get(CORP_CODE_URL, params={"crtfc_key": api_key}, timeout=60)
        res.raise_for_status()
    except Exception as e:
        print(f"[ERROR] 다운로드 실패: {e}")
        sys.exit(1)

    if not res.content or len(res.content) < 100:
        print("[ERROR] 응답이 비어 있거나 비정상입니다.")
        sys.exit(1)

    try:
        zf = zipfile.ZipFile(io.BytesIO(res.content))
        names = zf.namelist()
        xml_name = next((n for n in names if n.lower().endswith(".xml")), names[0] if names else None)
        if not xml_name:
            print("[ERROR] ZIP 내부에 XML이 없습니다.")
            sys.exit(1)
        xml_bytes = zf.read(xml_name)
    except zipfile.BadZipFile:
        print("[ERROR] ZIP 형식이 아닙니다. API 키 또는 응답을 확인하세요.")
        sys.exit(1)

    # Open DART CORPCODE.xml: <result> 아래 <list>가 행 단위로 반복 (자식에 corp_code, corp_name, stock_code)
    root = ET.fromstring(xml_bytes)
    rows = []
    for row_el in root.findall(".//list"):
        code_el = row_el.find("corp_code")
        name_el = row_el.find("corp_name")
        stock_el = row_el.find("stock_code")
        if code_el is None or name_el is None:
            continue
        corp_code = (code_el.text or "").strip()
        corp_name = (name_el.text or "").strip()
        stock_code = (stock_el.text if stock_el is not None else None) or ""
        stock_code = str(stock_code).strip()
        # 상장(종목코드 6자리)만
        if len(stock_code) == 6 and stock_code.isdigit():
            rows.append({"corp_code": corp_code, "corp_name": corp_name, "stock_code": stock_code})

    if not rows:
        print("[ERROR] 파싱 결과가 없습니다. XML 구조를 확인하세요.")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["stock_code"], keep="first")
    out = Path("dart_map.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[OK] 저장 완료: {out.resolve()} ({len(df)}건)")


if __name__ == "__main__":
    main()
