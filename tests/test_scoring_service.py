import unittest

import pandas as pd

from services.scoring_service import passes_confirmed_pullback_filter


class ConfirmedPullbackFilterTest(unittest.TestCase):
    def _features(self, **overrides):
        values = {
            "5일수익률": 1.5,
            "20일수익률": 12.0,
            "거래대금가속": 1.1,
            "MA20이격률": 3.0,
            "20일고점낙폭": -5.0,
            "MA20상승": True,
            "연기금5일누적": 100.0,
            "투신5일누적": -20.0,
            "사모5일누적": 10.0,
        }
        values.update(overrides)
        index = pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-09-15"), "테스트종목")],
            names=["일자_dt", "종목명"],
        )
        return pd.DataFrame([values], index=index)

    def test_passes_when_leader_pullback_and_institutional_flow_are_confirmed(self):
        result = passes_confirmed_pullback_filter(
            {"종목명": "테스트종목", "진입유형": "주도눌림", "스윙우선순위": 70},
            pd.Timestamp("2026-09-15"),
            "공격대기",
            self._features(),
            "스윙우선순위",
            return_explanation=True,
        )
        self.assertTrue(result["passed"])
        self.assertIn("핵심 기관", result["reason"])

    def test_rejects_breakout_or_unconfirmed_institutional_flow(self):
        breakout = passes_confirmed_pullback_filter(
            {"종목명": "테스트종목", "진입유형": "주도돌파"},
            pd.Timestamp("2026-09-15"),
            "공격",
            self._features(),
            "스윙우선순위",
            return_explanation=True,
        )
        no_flow = passes_confirmed_pullback_filter(
            {"종목명": "테스트종목", "진입유형": "주도눌림"},
            pd.Timestamp("2026-09-15"),
            "공격",
            self._features(연기금5일누적=-10, 투신5일누적=-20, 사모5일누적=-30),
            "스윙우선순위",
            return_explanation=True,
        )
        self.assertFalse(breakout["passed"])
        self.assertEqual(breakout["reason"], "주도눌림 후보 아님")
        self.assertFalse(no_flow["passed"])
        self.assertEqual(no_flow["reason"], "핵심 기관 5일 수급 미확인")


if __name__ == "__main__":
    unittest.main()
