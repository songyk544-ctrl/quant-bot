import csv
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from services.forward_signal_archive import canonical, capture, digest, export, publish
from services.forward_signal_observation import observe


class ForwardSignalArchiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        keys = TemporaryDirectory()
        cls.addClassCleanup(keys.cleanup)
        directory = Path(keys.name)
        cls.private_key = directory / 'reader-private.pem'
        cls.public_key = directory / 'reader-public.pem'
        cls.untrusted_key = directory / 'untrusted-public.pem'
        for private, public in [(cls.private_key, cls.public_key), (directory / 'untrusted.pem', cls.untrusted_key)]:
            subprocess.run(['openssl', 'genpkey', '-algorithm', 'ED25519', '-out', str(private)], check=True)
            subprocess.run(['openssl', 'pkey', '-in', str(private), '-pubout', '-out', str(public)], check=True)

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.checkout = root / 'checkout'
        self.checkout.mkdir()
        subprocess.run(['git', 'init', '-q', str(self.checkout)], check=True)
        self.data = self.checkout / 'data'
        self.data.mkdir()
        self.staging = root / 'private-staging'
        self.staging.mkdir(mode=0o700)
        self.staged_capture = self.staging / 'capture.json'
        self.archive = self.staging / 'private.sqlite3'
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
        self.health = {'기준거래일': self.session, '확인시각': self.session + ' 17:59:00', '상태': 'ok',
                       '소스': 'KIS_MASTER', '연속실패거래일': '0', 'fallback사용': 'False', '종목수': '1'}
        self.write('source_health.csv', [self.health])
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

    def stamp(self, records, at=None):
        self.staged_capture.write_text(canonical(records), encoding='utf-8')
        when = datetime.fromisoformat(at or self.observed)
        with patch('services.forward_signal_observation.datetime') as clock:
            clock.now.return_value = when
            return observe(self.staged_capture, self.private_key)

    def commit(self, records, at=None):
        return publish(self.archive, records, self.stamp(records, at), self.public_key)

    def test_first_publish_retry_failure_and_correction(self):
        captured = capture(self.receipt, self.data)
        self.assertEqual([r['scope'] for r in captured],
                         ['archived_candidate_filter', 'archived_candidate_filter', 'operational_recommendation'])
        self.assertEqual(captured[0]['signals'], ['005930'])
        self.assertEqual(captured[1]['status'], 'no_signal')
        self.assertEqual(captured[2]['signals'], ['005930'])
        self.assertEqual(self.commit(captured), 3)
        first = export(self.archive)
        self.assertEqual(self.commit(captured), 0)
        failed = capture({**self.receipt, 'run_id': 45, 'outcome': 'failed'}, self.data)
        self.assertTrue(all(row['signals'] is None and row['status'] == 'failed' for row in failed))
        self.assertEqual(self.commit(failed, self.session + 'T19:10:00+09:00'), 3)
        self.stocks[0]['V5추천상태'] = '관찰'
        self.write('data.csv', self.stocks)
        corrected = capture({**self.receipt, 'run_id': 46}, self.data)
        self.assertEqual(self.commit(corrected, self.session + 'T19:20:00+09:00'), 3)
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
        proof = self.stamp(captured)
        with ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(publish, self.archive, captured, proof, self.public_key) for _ in range(2)]
            self.assertEqual(sorted(task.result() for task in tasks), [0, 3])
        self.assertEqual(len(export(self.archive)), 3)
        changed = [*captured[:2], {**captured[2], 'signals': ['000660']}]
        with self.assertRaisesRegex(ValueError, 'conflicting_run_retry'):
            self.commit(changed)
        self.assertEqual(len(export(self.archive)), 3)

    def test_failed_publication_leaves_previous_revision_intact(self):
        captured = capture(self.receipt, self.data)
        self.assertEqual(self.commit(captured), 3)
        old = export(self.archive)
        new = capture({**self.receipt, 'run_id': 54}, self.data)
        with self.assertRaisesRegex(ValueError, 'publication_out_of_order'):
            self.commit(new, self.session + 'T18:50:00+09:00')
        self.assertEqual(export(self.archive), old)
        with self.assertRaisesRegex(ValueError, 'outside_quant_checkout'):
            publish(self.checkout / 'private.sqlite3', new, self.stamp(new), self.public_key)

    def test_health_must_match_verified_source_and_run_time(self):
        for override in ({'소스': 'UNVERIFIED_FIXTURE'}, {'연속실패거래일': '2'}, {'연속실패거래일': ''},
                         {'fallback사용': 'True'}, {'기준거래일': self.days[-2]},
                         {'확인시각': self.session + ' 18:01:00'}, {'확인시각': self.days[-2] + ' 17:00:00'}):
            with self.subTest(override=override):
                self.write('source_health.csv', [{**self.health, **override}])
                self.assertTrue(all(r['status'] == 'missing_inputs' and not r['inputs'] and r['signals'] is None
                                    for r in capture(self.receipt, self.data)))

    def test_manifest_rejects_invalid_batch_before_writing(self):
        good = capture(self.receipt, self.data)
        proof = self.stamp(good)
        self.assertEqual(publish(self.archive, good, proof, self.public_key), 3)
        previous = export(self.archive)
        bad_evidence = deepcopy(good[2]['inputs'])
        bad_evidence['universe']['available_at'] = self.observed
        for changes in ({'status': 'unsupported'}, {'schema': True}, {'extra': 1}, {'scope': 'archived_candidate_filter'},
                        {'status': 'no_signal'}, {'signals': ['005930', '005930']}, {'signals': ['ABCDEF']},
                        {'signals': [None]}, {'status': 'failed', 'signals': None, 'reason': ''},
                        {'inputs': {}}, {'inputs': bad_evidence}, {'code_sha': 'wrong'}, {'run_id': 46}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                publish(self.archive, [*good[:2], {**good[2], **changes}], proof, self.public_key)
        self.assertEqual(export(self.archive), previous)

    def test_observer_signs_read_payload_and_publisher_rejects_forgery(self):
        captured = capture(self.receipt, self.data)
        with self.assertRaises(FileNotFoundError):
            observe(self.staged_capture, self.private_key)
        proof = self.stamp(captured)
        changed = [*captured[:2], {**captured[2], 'signals': ['000660']}]
        with self.assertRaisesRegex(ValueError, 'reader_receipt_capture_mismatch'):
            publish(self.archive, changed, proof, self.public_key)
        with self.assertRaisesRegex(ValueError, 'invalid_reader_receipt'):
            publish(self.archive, captured, {'available_at': self.observed, 'capture_sha256': digest(captured)},
                    self.public_key)
        forged = {**proof, 'available_at': self.session + 'T10:05:00+00:00'}
        with self.assertRaisesRegex(ValueError, 'reader_signature_operation_failed'):
            publish(self.archive, captured, forged, self.public_key)
        with self.assertRaisesRegex(ValueError, 'reader_signature_operation_failed'):
            publish(self.archive, captured, proof, self.untrusted_key)
        self.assertFalse(self.archive.exists())
        self.assertEqual(publish(self.archive, captured, proof, self.public_key), 3)
        later_proof = self.stamp(captured, self.session + 'T20:00:00+09:00')
        self.assertEqual(publish(self.archive, captured, later_proof, self.public_key), 0)
        self.assertTrue(all(r['published_at'] == proof['available_at'] for r in export(self.archive)))
        with sqlite3.connect(self.archive) as db:
            stored, = db.execute('SELECT body FROM observations').fetchone()
        self.assertEqual(json.loads(stored), proof)

    def test_archive_rejects_unsigned_prior_batch_without_mutation(self):
        captured = capture(self.receipt, self.data)
        self.commit(captured)
        with sqlite3.connect(self.archive) as db:
            db.execute('DROP TABLE observations')
        before = self.archive.read_bytes()
        with self.assertRaisesRegex(ValueError, 'archive_observations_missing'):
            self.commit(captured)
        with self.assertRaises(sqlite3.OperationalError):
            export(self.archive)
        self.assertEqual(self.archive.read_bytes(), before)

    def test_private_staging_cli_round_trip(self):
        reader_share = self.staging / 'reader-view'
        reader_share.mkdir(mode=0o700)
        run_receipt = self.staging / 'run.json'
        proof_file = self.staging / 'observation.json'
        delivered = reader_share / 'manifest.json'
        staged = reader_share / 'capture.json'
        cli = [sys.executable, '-m', 'services.forward_signal_archive']

        def run(*args):
            return subprocess.run([*cli, *map(str, args)], capture_output=True, check=True, timeout=20).stdout

        def stage(run_id, outcome):
            run_receipt.write_text(canonical({**self.receipt, 'run_id': run_id, 'outcome': outcome}), encoding='utf-8')
            staged.write_bytes(run('capture', '--receipt', run_receipt, '--data-dir', self.data))
            staged.chmod(0o400)
            try:
                proof_file.write_bytes(run('observe', '--capture', staged, '--reader-private-key', self.private_key))
            finally:
                staged.chmod(0o600)
            return json.loads(proof_file.read_bytes())

        def append():
            return json.loads(run('publish', '--archive', self.archive, '--capture', staged,
                                  '--publication-receipt', proof_file, '--reader-public-key', self.public_key))

        first = stage(44, 'success')
        self.assertEqual(append(), {'appended': 3})
        self.assertEqual(append(), {'appended': 0})
        failed = stage(45, 'failed')
        self.assertEqual(append(), {'appended': 3})
        self.stocks[0]['V5추천상태'] = '관찰'
        self.write('data.csv', self.stocks)
        corrected = stage(46, 'success')
        self.assertEqual(append(), {'appended': 3})
        delivered.write_bytes(run('export', '--archive', self.archive))
        records = json.loads(delivered.read_bytes())
        self.assertEqual(len(records), 9)
        self.assertEqual([row['published_at'] for row in records if row['strategy'] == 'V5'],
                         [proof['available_at'] for proof in (first, failed, corrected)])

        consumer_checkout = os.environ.get('ALPHAPULSE_CONSUMER_CHECKOUT')
        if consumer_checkout:
            def as_of(cutoff):
                result = subprocess.run([sys.executable, '-m', 'backend.backtest_inputs.signal_manifest',
                                         '--manifest', str(delivered), '--as-of', cutoff], cwd=consumer_checkout,
                                        capture_output=True, check=True, timeout=20)
                return json.loads(result.stdout)['streams']

            before_first = (datetime.fromisoformat(first['available_at']) - timedelta(microseconds=1)).isoformat()
            self.assertEqual(as_of(before_first), [])
            initial = next(r for r in as_of(first['available_at']) if r['strategy'] == 'V5')
            after_failure = next(r for r in as_of(failed['available_at']) if r['strategy'] == 'V5')
            after_correction = next(r for r in as_of(corrected['available_at']) if r['strategy'] == 'V5')
            self.assertEqual(initial['latest_usable_output']['signals'], ['005930'])
            self.assertEqual(after_failure['latest_attempt']['status'], 'failed')
            self.assertEqual(after_failure['latest_usable_output'], initial['latest_usable_output'])
            self.assertEqual(after_correction['latest_usable_output']['status'], 'no_signal')
            for fields in ({'status': 'unsupported'}, {'signals': ['005930', '005930']}, {'extra': 1},
                           {'inputs': {}}, {'previous_sha256': '0' * 64}):
                # Compare against the actual pinned s8-agent CLI, including schema and chain rejection.
                bad = deepcopy(records)
                bad[0].update(fields)
                delivered.write_text(canonical(bad), encoding='utf-8')
                with self.subTest(fields=fields), self.assertRaises(subprocess.CalledProcessError) as caught:
                    as_of(corrected['available_at'])
                self.assertEqual(caught.exception.returncode, 2)

    def test_rolls_back_when_second_stream_write_fails(self):
        captured = capture(self.receipt, self.data)
        self.commit(captured)
        before = export(self.archive)
        with sqlite3.connect(self.archive) as db:
            db.execute('''CREATE TRIGGER reject_second_stream BEFORE INSERT ON manifest
                          WHEN new.run_id = 45 AND new.stream LIKE '%"V4"%'
                          BEGIN SELECT RAISE(ABORT, 'fixture write refused'); END''')
        failed = capture({**self.receipt, 'run_id': 45, 'outcome': 'failed'}, self.data)
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'fixture write refused'):
            self.commit(failed, self.session + 'T19:10:00+09:00')
        self.assertEqual(export(self.archive), before)
        with sqlite3.connect(self.archive) as db:
            db.execute('DROP TRIGGER reject_second_stream')
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0], 1)
        self.assertEqual(self.commit(failed, self.session + 'T19:10:00+09:00'), 3)
        self.assertEqual(len(export(self.archive)), 6)

    def test_consumer_size_limit_rolls_back_new_batch(self):
        captured = capture(self.receipt, self.data)
        self.commit(captured)
        before = export(self.archive)
        before_size = len(canonical(before).encode()) + 1
        correction = capture({**self.receipt, 'run_id': 45}, self.data)
        with patch('services.forward_signal_contract.MAX_MANIFEST_BYTES', before_size):
            with self.assertRaisesRegex(ValueError, 'signal_manifest_too_large'):
                self.commit(correction, self.session + 'T19:10:00+09:00')
        self.assertEqual(export(self.archive), before)
