import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from services.forward_signal_archive import capture, digest, export, publish


class ForwardSignalArchiveTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.checkout = root / 'checkout'
        self.checkout.mkdir()
        subprocess.run(['git', 'init', '-q', str(self.checkout)], check=True)
        self.data = self.checkout / 'data'
        self.data.mkdir()
        self.archive = root / 'private.sqlite3'
        days = [date(2026, 10, 1) + timedelta(days=i) for i in range(50)]
        self.days = [d.isoformat() for d in days if d.weekday() < 5][:26]
        self.session = self.days[-1]
        self.observed = self.session + 'T19:00:00+09:00'
        self.receipt = {'session': self.session, 'run_id': 44, 'generated_at': self.session + 'T18:00:00+09:00',
                        'code_sha': 'a' * 40, 'config_sha256': 'b' * 64,
                        'calendar': self.days, 'outcome': 'success', 'files': {}}
        self.stocks = [{'종목명': 'Mock', '종목코드': '005930', '현재가': '125', 'V5종합점수': '71',
                        'V5추천상태': '추천', '매수후보': '신규후보', '진입유형': '주도눌림',
                        'AI수급점수': '74', '스윙우선순위': '76'}]
        self.write('data.csv', self.stocks)
        self.write('source_health.csv', [{'기준거래일': self.session, '상태': 'ok', 'fallback사용': 'False', '종목수': '1'}])
        self.write('daily_candidates.csv', [{'날짜': self.session}])
        self.write('history.csv', [{'종목명': 'Mock', '일자': day, '종가': str(100 + i),
                                    '거래대금(억)': '50', '연기금': '1', '투신': '1', '사모': '1'}
                                   for i, day in enumerate(self.days)])

    def write(self, name, rows):
        p = self.data / name
        with p.open('w', encoding='utf-8', newline='') as dest:
            writer = csv.DictWriter(dest, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        self.receipt['files'][name] = sha256(p.read_bytes()).hexdigest()

    def test_first_publish_retry_failure_and_correction(self):
        captured = capture(self.receipt, self.data)
        self.assertEqual([r['scope'] for r in captured],
                         ['archived_candidate_filter', 'archived_candidate_filter', 'operational_recommendation'])
        self.assertEqual(captured[0]['signals'], ['005930'])
        self.assertEqual(captured[1]['status'], 'no_signal')
        self.assertEqual(captured[2]['signals'], ['005930'])
        self.assertEqual(publish(self.archive, captured, self.observed), 3)
        first = export(self.archive)
        self.assertEqual(publish(self.archive, captured, self.observed), 0)
        failed = capture({**self.receipt, 'run_id': 45, 'outcome': 'failed'}, self.data)
        self.assertTrue(all(row['signals'] is None and row['status'] == 'failed' for row in failed))
        self.assertEqual(publish(self.archive, failed, self.session + 'T19:10:00+09:00'), 3)
        self.stocks[0]['V5추천상태'] = '관찰'
        self.write('data.csv', self.stocks)
        corrected = capture({**self.receipt, 'run_id': 46}, self.data)
        self.assertEqual(publish(self.archive, corrected, self.session + 'T19:20:00+09:00'), 3)
        records = [r for r in export(self.archive) if r['strategy'] == 'V5']
        self.assertEqual([r['status'] for r in records], ['signals', 'failed', 'no_signal'])
        self.assertEqual(records[0], next(r for r in first if r['strategy'] == 'V5'))
        self.assertEqual(records[1]['previous_sha256'], digest(records[0]))
        self.assertEqual(records[2]['previous_sha256'], digest(records[1]))
        self.assertFalse((self.checkout / 'private.sqlite3').exists())

    def test_missing_and_mismatched_inputs(self):
        (self.data / 'history.csv').unlink()
        captured = capture(self.receipt, self.data)
        self.assertEqual([r['status'] for r in captured], ['missing_inputs', 'missing_inputs', 'signals'])
        self.receipt['files']['data.csv'] = '0' * 64
        self.assertTrue(all(r['signals'] is None for r in capture(self.receipt, self.data)))

    def test_concurrent_first_publication_is_atomic_and_idempotent(self):
        captured = capture(self.receipt, self.data)
        with ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(publish, self.archive, captured, self.observed) for _ in range(2)]
            self.assertEqual(sorted(task.result() for task in tasks), [0, 3])
        self.assertEqual(len(export(self.archive)), 3)
        changed = [*captured[:2], {**captured[2], 'signals': ['000660']}]
        with self.assertRaisesRegex(ValueError, 'conflicting_run_retry'):
            publish(self.archive, changed, self.observed)
        self.assertEqual(len(export(self.archive)), 3)

    def test_failed_publication_leaves_previous_revision_intact(self):
        captured = capture(self.receipt, self.data)
        self.assertEqual(publish(self.archive, captured, self.observed), 3)
        old = export(self.archive)
        new = capture({**self.receipt, 'run_id': 54}, self.data)
        with self.assertRaisesRegex(ValueError, 'publication_out_of_order'):
            publish(self.archive, new, self.session + 'T18:50:00+09:00')
        self.assertEqual(export(self.archive), old)
        with self.assertRaisesRegex(ValueError, 'outside_quant_checkout'):
            publish(self.checkout / 'private.sqlite3', new, self.observed)
