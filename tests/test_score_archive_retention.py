"""Mock/temporary CSV checks; AST-only loader, no source startup or APIs."""
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd


def loader(exists, read):
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'scraper.py').read_text())
    body = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'load_score_trend_safe']
    namespace = {'pd': pd, 'csv_exists': exists, 'read_table_prefer_db': read}
    exec(compile(ast.Module(body=body, type_ignores=[]), 'score_loader_only', 'exec'), namespace)
    return namespace['load_score_trend_safe']


class ScoreArchiveRetentionTest(unittest.TestCase):
    def test_numeric_only_csv_does_not_lose_leading_zeroes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'score.csv'
            path.write_text('날짜,종목명,종목코드\n2026-11-16,Mock,005930\n')
            result = loader(lambda _: True, lambda *a, **kw: pd.read_csv(path, **kw))()
            self.assertEqual(result['종목코드'].iloc[0], '005930')

    def test_round_trip_preserves_code_v5_and_unknown_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'score.csv'
            original = pd.DataFrame([
                {'날짜': '2026-11-16', '종목명': 'Mock', '종목코드': '005930', 'AI수급점수': 60,
                 'V5종합점수': 72.5, 'V5추천상태': '추천', 'V5추천사유': 'archived', 'extra_source': 'kept'},
                {'날짜': '2026-11-16', '종목명': 'Mock2', '종목코드': '00680K', 'AI수급점수': 50,
                 'V5종합점수': 40.0, 'V5추천상태': '관찰', 'V5추천사유': 'archived', 'extra_source': 'kept'}])
            original.to_csv(path, index=False)
            before = path.read_bytes()
            read = Mock(side_effect=lambda *a, **kw: pd.read_csv(path, **kw))
            result = loader(lambda _: True, read)()
            self.assertEqual(result['종목코드'].tolist(), ['005930', '00680K'])
            self.assertEqual(result['V5종합점수'].tolist(), [72.5, 40.0])
            self.assertEqual(result['V5추천상태'].tolist(), ['추천', '관찰'])
            self.assertEqual(result['extra_source'].tolist(), ['kept', 'kept'])
            self.assertEqual(path.read_bytes(), before)
            self.assertNotIn('on_bad_lines', read.call_args.kwargs)

    def test_invalid_existing_code_is_preserved_without_guessing(self):
        frame = pd.DataFrame([{'날짜': '2026-11-16', '종목名': 'Mock', '종목명': 'Mock', '종목코드': '5930'}])
        self.assertEqual(loader(lambda _: True, lambda *a, **kw: frame)()['종목코드'].iloc[0], '5930')

    def test_existing_empty_or_wrong_schema_blocks_replacement(self):
        for frame in [pd.DataFrame(), pd.DataFrame([{'wrong': 'schema'}])]:
            with self.subTest(columns=list(frame.columns)):
                with self.assertRaisesRegex(RuntimeError, '^score_trend_existing_snapshot_invalid$'):
                    loader(lambda _: True, lambda *a, **kw: frame)()

    def test_exception_diagnostic_does_not_disclose_private_detail(self):
        with self.assertRaisesRegex(RuntimeError, '^score_trend_existing_snapshot_unreadable$'):
            loader(lambda _: True, Mock(side_effect=OSError('private-detail')))()

    def test_conflict_marker_rows_are_not_silently_dropped(self):
        frame = pd.DataFrame([{'날짜': '2026-11-16', '종목명': 'Mock', '종목코드': '005930'},
                              {'날짜': '<<<<<<< branch', '종목명': 'Mock', '종목코드': '005930'}])
        with self.assertRaisesRegex(RuntimeError, '^score_trend_existing_snapshot_invalid$'):
            loader(lambda _: True, lambda *a, **kw: frame)()

    def test_missing_file_allows_first_snapshot(self):
        read = Mock(side_effect=AssertionError('missing file must not be read'))
        self.assertTrue(loader(lambda _: False, read)().empty)
        read.assert_not_called()


if __name__ == '__main__':
    unittest.main()
