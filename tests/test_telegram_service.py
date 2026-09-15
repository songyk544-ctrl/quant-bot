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

        self.assertIn("[오늘 할 일]", message)
        self.assertIn("[현재 보유]", message)
        self.assertIn("[신규 후보]", message)
        self.assertIn("[데이터 수집 경보]", message)
        self.assertIn("새후보 · 돌파 · 스윙 75.0", message)
        candidate_section = message.split("[신규 후보]", 1)[1]
        self.assertNotIn("보유종목", candidate_section)
        self.assertNotIn("|", message)


if __name__ == "__main__":
    unittest.main()
