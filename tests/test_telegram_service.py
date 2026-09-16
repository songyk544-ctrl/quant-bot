import unittest
from datetime import datetime
from unittest.mock import patch

import pandas as pd

from services.telegram_service import build_telegram_action_message


class TelegramServiceTest(unittest.TestCase):
    @patch("services.telegram_service._build_core_book")
    def test_action_message_is_compact_and_excludes_held_candidates(self, core_book):
        positions = pd.DataFrame(
            [
                {
                    "종목명": "보유종목",
                    "수량": 3,
                    "평가수익률": -2.5,
                }
            ]
        )
        core_book.return_value = (
            {"equity": 4_900_000, "return_pct": -2.0, "cash": 100_000},
            positions,
            pd.DataFrame(),
        )
        snapshot = pd.DataFrame(
            [
                {
                    "종목명": "보유종목",
                    "매수후보": "신규후보",
                    "매도점검": "수급훼손",
                    "진입유형": "눌림목",
                    "스윙우선순위": 80.0,
                    "현재가": 10_000,
                },
                {
                    "종목명": "새후보",
                    "매수후보": "신규후보",
                    "매도점검": "보유/관찰",
                    "진입유형": "돌파",
                    "스윙우선순위": 75.0,
                    "현재가": 20_000,
                    "핵심수급주체": "연기금",
                    "체급별수급점수": 72.0,
                    "시장대비5일(%p)": 3.2,
                    "시장대비20일(%p)": 7.1,
                },
            ]
        )

        message = build_telegram_action_message(
            snapshot,
            datetime(2026, 9, 15, 16, 10),
            18.0,
            "혼조장",
            True,
            source_health={
                "상태": "fallback_once",
                "종목수": 324,
                "연속실패거래일": 1,
            },
        )

        self.assertIn("[오늘의 판단]", message)
        self.assertIn("[오늘의 수급 추천]", message)
        self.assertIn("[추가 시황]", message)
        self.assertIn("[데이터 수집 경보]", message)
        self.assertIn("1위 새후보", message)
        self.assertIn("연기금 중심 수급 점수 72.0", message)
        self.assertNotIn("4,900,000원", message)
        self.assertNotIn("기준 2026-04-27", message)
        candidate_section = message.split("[오늘의 수급 추천]", 1)[1].split("[보유 위험 점검]", 1)[0]
        self.assertNotIn("보유종목", candidate_section)
        self.assertNotIn("|", message)

    @patch("services.telegram_service._build_core_book")
    @patch.dict("os.environ", {"TELEGRAM_USE_V5": "1"})
    def test_v5_empty_result_does_not_fall_back_to_legacy_candidate(self, core_book):
        core_book.return_value = ({}, pd.DataFrame(), pd.DataFrame())
        snapshot = pd.DataFrame([
            {
                "종목명": "기존후보",
                "매수후보": "신규후보",
                "V5추천상태": "관찰",
                "등락률": 1.0,
                "현재가": 10_000,
                "MA20": 9_500,
            }
        ])
        message = build_telegram_action_message(
            snapshot,
            datetime(2026, 9, 15, 16, 10),
            18.0,
            "혼조장",
            True,
            source_health={"상태": "ok", "종목수": 350},
        )
        self.assertIn("오늘 기준을 통과한 신규 후보가 없습니다.", message)
        self.assertNotIn("1위 기존후보", message)
        self.assertNotIn("KIS 공식 종목 마스터", message)


if __name__ == "__main__":
    unittest.main()
