# 누적 history 읽기 실패 시 보존

기존 `merge_history_snapshot()`은 `history.csv` 읽기에 실패하면 새 구간만 계속 반환했습니다. CSV adapter는 파싱 예외를 빈 DataFrame으로 반환하므로 예외가 발생하지 않은 것처럼 진행할 수도 있었습니다. 이후 기존 호출부가 그 결과를 저장하면 과거 누적 범위를 잃을 수 있습니다. 비어 있지 않은 새 응답의 날짜가 모두 잘못됐을 때 빈 결과를 반환하는 경로도 있었습니다.

이번 변경은 이 경로를 CSV 교체 전에 고정 RuntimeError로 중단합니다.

- `history_existing_snapshot_unreadable`: 기존 파일 로딩/정규화 예외. 예외 원문은 출력하지 않음.
- `history_existing_snapshot_invalid`: 빈 기존 복원 결과 또는 기존 행이 날짜/이름 정규화에서 사라짐. `on_bad_lines=skip`으로 손상 행을 조용히 제외하지 않음.
- `history_new_snapshot_invalid`: 비어 있지 않은 새 입력에서 유효한 날짜/이름이 하나도 없음.

정상 병합의 종목명+날짜 dedup, 같은 날짜 최신 입력 우선, 숫자 정규화와 V3/V4/V5 전략은 바꾸지 않습니다. 자동 복구·기존 파일 삭제/초기화·원본 데이터 정정은 추가하지 않습니다. 해당 오류가 나면 기존 파일을 보존하고 원천/마지막 정상 snapshot을 검토해야 합니다. 이 수정 자체가 손상된 원천을 복구하지는 않습니다.

운영 데이터 유실을 관측했다고 주장하지 않습니다. 기존 adapter의 읽기 실패를 Mock으로 재생하면 수정 전에는 새1행을 반환하는 것을 확인했고, 수정 후에는 실제 손상된 **임시 CSV**를 바이트 단위로 보존하는 테스트를 추가했습니다. 테스트는 scraper 전체를 import/실행하지 않고 AST에서 병합 함수 두 개만 격리하므로 KIS·LLM·Telegram 호출이나 키 로딩이 없습니다.

전체 오프라인 검증:

```sh
python -m pip install pandas==2.2.3 numpy==2.2.6
python -m unittest discover -s tests -v
```

21개 통과(기존14+신규7). 새 Offline tests CI는 읽기 권한만 사용하며 Secret을 전달하지 않습니다. 기존 운영 `update.yml`의 일정·키·scraper 실행·게시 정책은 변경하지 않았습니다. 실 KIS 호출·운영 workflow dispatch/rerun·데이터 CSV 수정은 실행하지 않았습니다. main 자동 병합 금지입니다.
