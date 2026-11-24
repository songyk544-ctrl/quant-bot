# Forward Test 신호 보존 후보

기존 Quant Data Scraper의 출력 뒤에 연결할 오프라인 도구다. 현재 전략, CSV 게시, 수집 스케줄의 변경은 없다. [Issue #68](https://github.com/songyk544-ctrl/s8-agent/issues/68) · [Issue #74](https://github.com/songyk544-ctrl/s8-agent/issues/74)

## 캡처, 독립 조회, 원장 추가

1. 같은 실행의 `receipt.json`에 `session`, `run_id`, `code_sha`, `config_sha256`, `generated_at`, `outcome`과 파일별 `files` SHA-256을 넣는다. V3/V4 후보 필터는 당시 공식 26거래일 `calendar`가 필요하다. `source_health.csv`의 마지막 행은 `KIS_MASTER`, `ok`, fallback 미사용, 연속 실패 `0`, 대상 거래일 및 종목 수 일치를 요구한다. 건강 확인 시각은 해당 거래일 기준이며 캡처 생성 이후일 수 없다. 증빙이 부족하면 사용 가능 신호로 인정하지 않는다.
2. 비공개 저장 경로의 읽기 전용 뷰로 캡처를 전달한다. **독립 읽기 주체**가 실제로 그 파일을 읽고 `observe`를 실행한다. 파일을 읽은 뒤의 UTC 시각과 캡처 내용 해시, 거래일, 실행 ID, 코드·설정 해시 및 생성 시각을 Ed25519로 서명한다. 관측 시각을 CLI 입력으로 받지 않는다. 독립 읽기 주체의 계정만 개인키로 서명할 수 있어야 하며, 원장 작성자에는 별도 경로로 승인한 공개키만 제공한다. staging과 운영의 키·경로는 별도로 운영한다. OpenSSL 3이 필요하다.
3. 원장 작성자는 고정 공개키로 영수증의 서명과 캡처 일치 여부를 확인한다. 조회 시각이 생성보다 이전이거나 기록 시점의 UTC보다 미래면 거절한다. 세 전략과 검증한 영수증을 SQLite에 함께 저장한다. 같은 실행과 동일 캡처는 다시 조회한 뒤에도 기존 영수증과 시각을 유지한다. 동일 실행의 다른 캡처는 거절하고, 별도 실행에서 생긴 정정은 다음 리비전에 추가한다.
4. `export`는 SQLite를 읽기 전용으로 연다. 내보내기 전에 스키마, 상태별 신호·사유, 입력 시각, 해시 체인, 영수증 연결 및 소비자의 최대 5,000행·2MiB 범위를 검증한다. 기존에 서명 영수증 없이 만든 초안 DB는 원본을 보존하고 검증된 원장으로 사용하지 않는다. 필요한 경우 별도 원장으로 시작한다.

```sh
# 작성자: 운영과 분리된 입력과 경로에서 실행
umask 077
python -m services.forward_signal_archive capture --receipt receipt.json --data-dir staged-inputs > /private/staging/capture.json

# 독립 읽기 주체: 실제 소비 측 읽기 경로에서 실행
python -m services.forward_signal_archive observe --capture /private/reader-view/capture.json \
  --reader-private-key /reader-keys/staging-private.pem > /private/receipt-transfer/observation.json

# 원장 작성자: 별도로 고정한 검증 공개키 사용
python -m services.forward_signal_archive publish --archive /private/archive/signals.sqlite3 \
  --capture /private/staging/capture.json --publication-receipt /private/receipt-transfer/observation.json \
  --reader-public-key /writer-config/staging-reader-public.pem
python -m services.forward_signal_archive export --archive /private/archive/signals.sqlite3 > /private/delivery/signals.json
```

V5는 검증된 같은 실행의 원본 추천을 보존한다. V3/V4는 같은 실행의 공통 후보와 보관 이력에 기존 상대강도·눌림목 필터를 적용한다. 완전한 입력에서 나온 무신호는 빈 배열로 표시하고, 실패와 결측은 `null`과 사유를 기록한다. 최종 특징 파일은 해시로 연결하며 뉴스·공시 원문과 자격정보는 보존 대상에서 제외한다.

## 검증과 운영 승인

- `ALPHAPULSE_CONSUMER_CHECKOUT=/path/to/s8-agent python -m unittest discover -s tests -v`는 임시 비공개 fixture에서 캡처 CLI → 별도 읽기 프로세스 → 서명 검증·원장 추가 → 읽기 전용 export → 고정된 s8-agent 소비자 CLI까지 실행한다. `s8-agent` 후보 커밋 `4273235587518803a267a7e233e7f4ca9503eb61`로 확인했다. 환경 변수를 생략하면 Quant-bot CI에서 생산자와 영수증, 임시 export까지 검증한다.
- 신뢰하지 않는 키나 변조된 캡처·시각, 잘못된 상태, 중간 SQLite 오류, 재시도, 정정, 동시 원장 추가를 검사한다. 실패한 최신 리비전은 기존 사용 가능 신호를 대체하지 않는다.
- 독립 조회 영수증은 서명 주체가 해당 캡처를 읽었다는 증거다. 조회 전 최초 노출 시각, 다른 저장 경로 또는 최종 원장의 가용 시각은 별도다. 실제 Forward 등록 전에는 기존 실행 ID와 산출물 커밋·해시 대조, 운영 읽기 주체의 실제 권한 분리와 시계 동기화, 승인된 비공개 경로에서 캡처와 최종 원장의 조회 가능 시각, 거래 가능 근거를 확인해야 한다. 이번 검증은 로컬 fixture에서 수행했고 실제 비공개 게시와 Forward 등록에는 별도 승인이 필요하다.
