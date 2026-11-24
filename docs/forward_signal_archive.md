# Forward Test 신호 보존 후보

기존 Quant Data Scraper의 출력 뒤에 연결할 오프라인 후보이다. 관련 작업은 [s8-agent Issue #68](https://github.com/songyk544-ctrl/s8-agent/issues/68)에 기록한다.

## 기존 흐름의 연결 지점

현재 실행은 `scraper.py`로 출력물을 만든 뒤 `Commit and Push CSV`에서 공개 CSV를 게시한다. 적용 시 같은 실행의 종료 증거와 CSV 출력 커밋을 확인하고, 기존 공개 Git 게시 단계와 분리된 비공개 저장소에서 아래 도구를 호출한다. 게시 실패와 입력 결측을 각각 기록한다.

1. **캡처.** 같은 실행에서 생성한 CSV의 바이트 SHA-256과 시각을 고정한다. `receipt.json`은 `session`, `run_id`, `code_sha`, `config_sha256`, `generated_at`, `outcome`과 파일명별 `files` 해시를 받는다. V3/V4에는 당시 공식 26거래일 `calendar`가 필요하다. 설정 해시는 당시 설정 및 의존성 잠금값을 묶어 만든다. 성공 시 실제로 관측한 시각과 그 실행의 파일을 연결한다.
2. **가용 시각 확정.** 승인된 비공개 경로에서 같은 캡처를 조회할 수 있게 된 사실을 확인한 뒤 `publication.json`을 만든다. `capture_sha256`은 JSON 배열을 표준 정렬 키·구분자로 직렬화해 계산한 해시다. `available_at`은 조회 가능한 시각을 관측한 값이다. 독립적인 게시 및 시각 검증은 승인된 실제 실행에서 증빙을 작성하는 쪽에 필요하다.
3. **원장에 추가.** 공개 체크아웃 밖 SQLite에 세 전략의 기록을 한 트랜잭션으로 추가한다. 동일 실행과 내용의 재시도는 기존 기록을 유지한다. 내용이 달라졌으면 충돌로 처리한다. 정정은 별도 생성 실행의 다음 리비전으로 추가한다. 내보낸 JSON은 해시와 함께 승인된 비공개 경로에서 소비한다.

```sh
python -m services.forward_signal_archive capture --receipt receipt.json --data-dir data > /private/capture.json
python -m services.forward_signal_archive publish --archive /private/signals.sqlite3 \
  --capture /private/capture.json --publication-receipt /private/publication.json
python -m services.forward_signal_archive export --archive /private/signals.sqlite3 > /private/signals.json
```

V5는 검증된 같은 실행의 `data.csv`에 저장된 추천을 보존한다. V3/V4는 같은 실행의 공통 후보와 보관 이력에 기존 상대강도·눌림목 필터를 적용한 결과이다. 완전한 입력에서 나온 무신호는 빈 배열, 실패와 결측은 `null`과 사유로 남긴다.

보존 내용은 종목 코드, 원본 또는 후보 필터 범위, 시각, 코드·설정·입력 해시다. 최종 특징 파일은 해시로 연결한다. 뉴스·공시 원문과 자격정보는 보존 대상에서 제외한다. 게시되지 않은 V5 원시 정성 입력을 해시만으로 재구성할 수는 없다.

임시 Git 체크아웃과 외부 SQLite의 fixture로 첫 게시, 재시도, 실패, 정정, 중복과 동시 실행을 검증한다. 실제 적용 전에는 생성 파일의 출처, 비공개 보존 위치, 캡처 바이트 및 가용 시각을 독립적으로 확인해야 한다.
