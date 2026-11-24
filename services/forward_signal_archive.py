"""Offline candidate adapter for the existing Quant Data Scraper publication."""
import argparse
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import re
import sqlite3

import pandas as pd

from .backtest_generation_service import build_backtest_candidate_scores
from .scoring_service import (
    ADAPTIVE_THRESHOLD_PROFILES, build_market_state_features, choose_adaptive_target_positions,
    passes_confirmed_pullback_filter, passes_relative_strength_filter,
)

ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))
CODE = re.compile(r'(?=.*[0-9])[0-9A-Z]{6}')
V5_STATES = {'추천', '통과대기', '통과', '관찰', '제외'}
FILES = ('data.csv', 'daily_candidates.csv', 'source_health.csv', 'history.csv')
SCORE = '스윙우선순위'
PROFILES = {'V3': 'v3 상대강도', 'V4': 'v4 수급확인'}
HISTORY_FIELDS = ('종가', '거래대금(억)', '연기금', '투신', '사모')
MAX_FILE = 16 * 1024 * 1024


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return sha256(canonical(value).encode()).hexdigest()


def instant(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None:
        raise ValueError('timezone_required')
    return parsed.astimezone(timezone.utc)


def _require(ok, error):
    if not ok:
        raise ValueError(error)


def _result(codes):
    return {'status': 'signals' if codes else 'no_signal', 'signals': codes, 'reason': None}


def _missing(reason, *, failed=False):
    return {'status': 'failed' if failed else 'missing_inputs', 'signals': None, 'reason': reason}


def _candidate_filters(stocks, history, session, window):
    _require(isinstance(window, list) and len(window) == 26 and window == sorted(set(window))
             and window[-1] == session and all(date.fromisoformat(item).isoformat() == item for item in window),
             'official_warmup_required')
    names = set(stocks['종목명'])
    _require(set(stocks['매수후보']) <= {'신규후보', '관찰', '제외', '보류'}, 'unknown_candidate_state')
    candidates = stocks.loc[stocks['매수후보'].eq('신규후보')].copy()
    candidates['날짜'] = session
    candidates['순위'] = range(1, len(candidates) + 1)
    for field in (SCORE, 'AI수급점수'):
        candidates[field] = pd.to_numeric(candidates[field], errors='raise')
        _require(candidates[field].notna().all() and candidates[field].abs().le(1e9).all(),
                 'invalid_candidate_score')
    _require(candidates['진입유형'].str.strip().ne('').all(), 'missing_entry_type')
    ranked, _ = build_backtest_candidate_scores(candidates, None, top_n=3)
    ranked_names = set(ranked['종목명']) if not ranked.empty else set()
    history['일자_dt'] = pd.to_datetime(history['일자'], format='mixed', errors='coerce')
    market_days = set(pd.to_datetime(window[-20:]))
    allowed_days = set(pd.to_datetime(window))
    needs = history['일자_dt'].isin(market_days) | (
        history['일자_dt'].isin(allowed_days) & history['종목명'].isin(ranked_names))
    frame = history.loc[needs & history['종목명'].isin(names)].copy()
    _require(not frame.duplicated(['종목명', '일자_dt']).any()
             and len(frame) == len(names) * 20 + len(ranked_names) * 6, 'incomplete_candidate_history')
    for field in HISTORY_FIELDS:
        frame[field] = pd.to_numeric(frame[field], errors='raise')
        _require(frame[field].notna().all() and frame[field].abs().le(1e20).all(), 'invalid_candidate_history')
    _require(frame['종가'].gt(0).all() and frame['거래대금(억)'].ge(0).all(), 'invalid_candidate_history')
    if ranked.empty:
        return dict.fromkeys(PROFILES, _result([]))
    current = pd.Timestamp(session)
    _, market, features = build_market_state_features(frame.sort_values(['종목명', '일자_dt']))
    _require(market.loc[current].notna().all(), 'missing_market_features')
    _require(all(features.loc[(current, name)].notna().all() for name in ranked_names), 'missing_entry_features')
    output = {}
    for version, profile in PROFILES.items():
        cap, mode = choose_adaptive_target_positions(current, ranked, SCORE, 3, ADAPTIVE_THRESHOLD_PROFILES[profile],
                                                       market_state_row=market.loc[current])
        selected = []
        for _, row in ranked.iterrows():
            if not cap:
                break
            if version == 'V3':
                passed = passes_relative_strength_filter(row, current, mode, features, market, SCORE)
            else:
                passed = passes_confirmed_pullback_filter(row, current, mode, features, SCORE)
            if passed:
                selected.append(row['종목코드'])
        output[version] = _result(selected[:cap])
    return output


def capture(receipt, directory):
    """Read only outputs from one existing run supplied by its reviewed publisher."""
    session, run_id = receipt['session'], receipt['run_id']
    generated = instant(receipt['generated_at'])
    _require(date.fromisoformat(session).isoformat() == session <= generated.astimezone(KST).date().isoformat()
             and type(run_id) is int and run_id > 0 and re.fullmatch(r'[0-9a-f]{40}', receipt['code_sha'])
             and re.fullmatch(r'[0-9a-f]{64}', receipt['config_sha256'])
             and receipt['outcome'] in {'success', 'failed'}, 'invalid_capture_receipt')
    evidence, tables = {}, {}
    results = {}
    if receipt['outcome'] == 'failed':
        results = dict.fromkeys(('V3', 'V4', 'V5'), _missing('upstream_run_failed', failed=True))
    else:
        try:
            for name in FILES:
                path = directory / name
                if name not in receipt['files'] or not path.is_file():
                    continue
                with path.open('rb') as source:
                    raw = source.read(MAX_FILE + 1)
                checksum = sha256(raw).hexdigest()
                _require(len(raw) <= MAX_FILE and checksum == receipt['files'][name], 'capture_file_digest_mismatch')
                tables[name] = pd.read_csv(BytesIO(raw), dtype=str, keep_default_na=False)
                evidence[{'data.csv': 'features', 'daily_candidates.csv': 'daily_candidates',
                          'source_health.csv': 'source_health', 'history.csv': 'history'}[name]] = {
                    'sha256': checksum, 'source': f'quant_bot_run/{name}',
                    'observed_at': generated.isoformat(), 'available_at': generated.isoformat(),
                }
            health, stocks = tables['source_health.csv'].iloc[-1], tables['data.csv']
            _require(health['기준거래일'] == session and health['상태'] == 'ok'
                     and health['fallback사용'].lower() == 'false'
                     and int(health['종목수']) == len(stocks)
                     and not stocks.empty and stocks['종목명'].str.strip().ne('').all()
                     and stocks['종목명'].is_unique and stocks['종목코드'].is_unique
                     and stocks['종목코드'].str.fullmatch(CODE).all(), 'daily_source_unverified')
            if 'daily_candidates.csv' in tables:
                _require(set(tables['daily_candidates.csv']['날짜']) <= {session}, 'candidate_date_mismatch')
            evidence['universe'] = {**evidence['features'], 'source': 'quant_bot_run/data.csv_universe_columns',
                                    'sha256': digest(sorted(zip(stocks['종목코드'], stocks['종목명'])))}
        except (KeyError, IndexError, OSError, ValueError):
            results = dict.fromkeys(('V3', 'V4', 'V5'), _missing('daily_snapshot_unverified'))
            evidence = {}
        else:
            have_fields = {'V5추천상태', 'V5종합점수', '현재가'} <= set(stocks.columns)
            if (have_fields and set(stocks['V5추천상태']) <= V5_STATES
                    and pd.to_numeric(stocks['V5종합점수'], errors='coerce').between(0, 100).all()
                    and pd.to_numeric(stocks['현재가'], errors='coerce').between(1, 1e15).all()):
                results['V5'] = _result(stocks.loc[stocks['V5추천상태'].eq('추천'), '종목코드'].tolist())
            else:
                results['V5'] = _missing('original_v5_output_unavailable')
            try:
                results.update(_candidate_filters(stocks, tables['history.csv'], session, receipt.get('calendar')))
                evidence['calendar'] = {**evidence['features'], 'source': 'verified_open_dates',
                                        'sha256': digest(receipt['calendar'])}
            except (KeyError, IndexError, ValueError):
                results.update(dict.fromkeys(PROFILES, _missing('candidate_filter_inputs_unverified')))
    return [{'schema': 1, 'session': session, 'strategy': version,
             'scope': 'operational_recommendation' if version == 'V5' else 'archived_candidate_filter',
             'run_id': run_id, 'code_sha': receipt['code_sha'], 'config_sha256': receipt['config_sha256'],
             'generated_at': generated.isoformat(),
             'inputs': {key: value for key, value in evidence.items()
                        if version != 'V5' or key not in {'history', 'calendar'}},
             **results[version]} for version in ('V3', 'V4', 'V5')]


def publish(path, receipts, observed_at):
    """Commit all stream revisions together. Repeating an identical run is a no-op."""
    _require(not path.is_symlink(), 'archive_path_is_symlink')
    path = path.resolve()
    inside_git = any((parent / '.git').is_file() or (parent / '.git' / 'HEAD').is_file() for parent in path.parents)
    _require(not path.is_relative_to(ROOT) and not inside_git,
             'archive_must_be_outside_quant_checkout')
    observed = instant(observed_at)
    _require({row['strategy'] for row in receipts} == {'V3', 'V4', 'V5'} and len(receipts) == 3,
             'incomplete_capture')
    _require(len({(row['session'], row['run_id'], row['code_sha'], row['config_sha256'], row['generated_at'])
                  for row in receipts}) == 1, 'mixed_capture_receipt')
    os.close(os.open(path, os.O_CREAT | os.O_RDWR, 0o600))
    with closing(sqlite3.connect(path, timeout=15, isolation_level=None)) as db:
        db.execute('PRAGMA synchronous=FULL')
        db.execute('CREATE TABLE IF NOT EXISTS manifest (stream TEXT, run_id INTEGER, revision INTEGER, body TEXT, '
                   'UNIQUE(stream, run_id), UNIQUE(stream, revision))')
        db.execute('BEGIN IMMEDIATE')
        appended = 0
        try:
            for record in receipts:
                _require(instant(record['generated_at']) <= observed, 'publication_before_capture')
                stream = canonical([record[key] for key in ('session', 'strategy', 'scope')])
                existing = db.execute('SELECT body FROM manifest WHERE stream=? AND run_id=?',
                                      (stream, record['run_id'])).fetchone()
                if existing:
                    old = json.loads(existing[0])
                    for key in ('revision', 'published_at', 'previous_sha256'):
                        del old[key]
                    _require(old == record, 'conflicting_run_retry')
                    continue
                prev = db.execute('SELECT revision, body FROM manifest WHERE stream=? ORDER BY revision DESC LIMIT 1',
                                  (stream,)).fetchone()
                previous = json.loads(prev[1]) if prev else None
                _require(previous is None or instant(previous['published_at']) <= observed, 'publication_out_of_order')
                row = {**record, 'revision': prev[0] + 1 if prev else 1,
                       'previous_sha256': digest(previous) if prev else None, 'published_at': observed.isoformat()}
                db.execute('INSERT INTO manifest VALUES (?, ?, ?, ?)',
                           (stream, record['run_id'], row['revision'], canonical(row)))
                appended += 1
        except Exception:
            db.rollback()
            raise
        db.commit()
    return appended


def export(path):
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        return [json.loads(value) for (value,) in db.execute('SELECT body FROM manifest ORDER BY stream, revision')]


def _read_json(path):
    with path.open('rb') as source:
        raw = source.read(MAX_FILE + 1)
    _require(len(raw) <= MAX_FILE, 'receipt_too_large')
    return json.loads(raw)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    stage = commands.add_parser('capture')
    stage.add_argument('--receipt', required=True, type=Path)
    stage.add_argument('--data-dir', required=True, type=Path)
    append = commands.add_parser('publish')
    append.add_argument('--archive', required=True, type=Path)
    append.add_argument('--capture', required=True, type=Path)
    append.add_argument('--publication-receipt', required=True, type=Path)
    read = commands.add_parser('export')
    read.add_argument('--archive', required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == 'capture':
            print(canonical(capture(_read_json(args.receipt), args.data_dir)))
        elif args.command == 'publish':
            receipt, records = _read_json(args.publication_receipt), _read_json(args.capture)
            _require(receipt['capture_sha256'] == digest(records), 'capture_publication_digest_mismatch')
            print(canonical({'appended': publish(args.archive, records, receipt['available_at'])}))
        else:
            print(canonical(export(args.archive)))
    except (ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
        parser.exit(2, f'archive rejected ({type(error).__name__})\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
