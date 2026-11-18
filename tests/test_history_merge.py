"""Offline merge tests; load only the two functions, never scraper API/LLM startup."""
import ast
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import pandas as pd

from db_utils import read_table


def functions(exists,loader):
    module=ast.parse((Path(__file__).resolve().parents[1]/'scraper.py').read_text())
    body=[node for node in module.body if isinstance(node,ast.FunctionDef) and node.name in
          ('_normalize_history_frame','merge_history_snapshot')]
    namespace={'pd':pd,'csv_exists':exists,'read_table_prefer_db':loader}
    exec(compile(ast.Module(body=body,type_ignores=[]),'scraper_history_functions','exec'),namespace)
    return namespace['merge_history_snapshot']


def frame(day='20261116',price=100):
    return pd.DataFrame([{'종목명':'Mock','일자':day,'종가':price,'외인':1,'연기금':2,
                          '투신':3,'사모':4,'거래량':100,'거래대금(억)':1}])


class HistoryMergeTest(unittest.TestCase):
    def test_read_exception_blocks_replacement_without_leaking_exception(self):
        merge=functions(lambda _:True,Mock(side_effect=OSError('private-error-detail')))
        with self.assertRaisesRegex(RuntimeError,'^history_existing_snapshot_unreadable$'):
            merge(frame())

    def test_adapter_empty_result_blocks_replacement(self):
        merge=functions(lambda _:True,lambda *a,**kw:pd.DataFrame())
        with self.assertRaisesRegex(RuntimeError,'^history_existing_snapshot_invalid$'):merge(frame())

    def test_partial_date_normalization_cannot_drop_stored_rows(self):
        previous=pd.concat([frame('20261113'),frame('not-a-date')],ignore_index=True)
        merge=functions(lambda _:True,lambda *a,**kw:previous)
        with self.assertRaisesRegex(RuntimeError,'^history_existing_snapshot_invalid$'):merge(frame())

    def test_malformed_csv_is_neither_silently_skipped_nor_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'history.csv'
            path.write_bytes(b'\xffinvalid-original-data\x00')
            original=path.read_bytes()
            merge=functions(lambda _:True,lambda *a,**kw:read_table('history',csv_fallback=str(path),read_csv_kwargs=kw))
            with self.assertRaisesRegex(RuntimeError,'^history_existing_snapshot_invalid$'):
                merged=merge(frame())
                merged.to_csv(path,index=False)
            self.assertEqual(path.read_bytes(),original)

    def test_good_merge_preserves_old_dates_and_existing_latest_revision_rule(self):
        previous=pd.concat([frame('20261112'),frame('20261113')],ignore_index=True)
        loader=Mock(return_value=previous)
        merged=functions(lambda _:True,loader)(pd.concat([frame('20261113',101),frame()],ignore_index=True))
        self.assertEqual(merged['일자'].tolist(),['20261116','20261113','20261112'])
        self.assertEqual(merged.loc[merged['일자']=='20261113','종가'].iloc[0],101)
        self.assertNotIn('on_bad_lines',loader.call_args.kwargs)
        self.assertEqual(previous['종가'].tolist(),[100,100])

    def test_first_snapshot_does_not_need_a_previous_file(self):
        loader=Mock(side_effect=AssertionError('should not read missing file'))
        self.assertEqual(len(functions(lambda _:False,loader)(frame())),1)
        loader.assert_not_called()


class NewHistoryValidationTest(unittest.TestCase):
    def test_nonempty_new_response_with_no_valid_dates_cannot_erase_history(self):
        loader=Mock(return_value=frame('20261113'))
        merge=functions(lambda _:True,loader)
        with self.assertRaisesRegex(RuntimeError,'^history_new_snapshot_invalid$'):
            merge(frame('not-a-date'))
        loader.assert_not_called()


if __name__=='__main__':unittest.main()
