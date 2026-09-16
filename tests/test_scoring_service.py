import unittest

import pandas as pd

from services.scoring_service import (
    calculate_market_leadership_score,
    calculate_size_aware_flow_score,
    classify_stock_tier,
    evaluate_v5_entry_setup,
    passes_confirmed_pullback_filter,
)


class V5CoreScoringTest(unittest.TestCase):
    def test_stock_tier_uses_market_cap_and_liquidity(self):
        self.assertEqual(classify_stock_tier(150_000, 100), "대형")
        self.assertEqual(classify_stock_tier(5_000, 1_200), "대형")
        self.assertEqual(classify_stock_tier(20_000, 150), "중형")
        self.assertEqual(classify_stock_tier(3_000, 80), "소형")

    def test_large_and_small_caps_use_different_core_flow_actors(self):
        large = calculate_size_aware_flow_score(
            "대형", 0.9, 0.2, 0.3, 0.8,
            foreign_positive=True,
            pension_positive=False,
            return_details=True,
        )
        small = calculate_size_aware_flow_score(
            "소형", 0.9, 0.2, 0.3, 0.8,
            foreign_positive=True,
            pension_positive=False,
            return_details=True,
        )
        self.assertEqual(large["core_actor"], "외국인")
        self.assertEqual(small["core_actor"], "연기금")
        self.assertGreater(large["score"], small["score"])
        self.assertTrue(small["warnings"])

    def test_market_leadership_rewards_relative_strength(self):
        leader = calculate_market_leadership_score(5, 12, 0.9, 90, -4, return_details=True)
        laggard = calculate_market_leadership_score(-5, -8, 0.9, 90, -4, return_details=True)
        self.assertGreater(leader["score"], laggard["score"])
        self.assertIn("시장대비", leader["reason"])

    def test_entry_setup_requires_confirmed_pullback_or_breakout(self):
        pullback = evaluate_v5_entry_setup(
            12, 2, -5, -4, 0.9, 58, True, 70, return_details=True
        )
        waiting = evaluate_v5_entry_setup(
            12, 2, -5, -4, 0.3, 58, True, 70, return_details=True
        )
        breakout = evaluate_v5_entry_setup(
            15, 8, 0, 0.5, 1.4, 75, True, 68, return_details=True
        )
        self.assertTrue(pullback["passed"])
        self.assertEqual(pullback["setup"], "눌림확인")
        self.assertFalse(waiting["passed"])
        self.assertEqual(waiting["setup"], "눌림대기")
        self.assertTrue(breakout["passed"])
        self.assertEqual(breakout["setup"], "돌파확인")


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
