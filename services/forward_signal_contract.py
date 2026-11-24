"""Producer validation for the s8-agent schema-1 signal manifest wire format."""
from datetime import date, datetime, timedelta, timezone
from hashlib import sha256
import json
import re

KST = timezone(timedelta(hours=9))
CODE = re.compile(r'(?=.*[0-9])[0-9A-Z]{6}')
SHA1 = re.compile(r'[0-9a-f]{40}')
SHA256 = re.compile(r'[0-9a-f]{64}')
USABLE = {'signals', 'no_signal'}
STATES = USABLE | {'missing_inputs', 'failed'}
SCOPES = {'V3': 'archived_candidate_filter', 'V4': 'archived_candidate_filter',
          'V5': 'operational_recommendation'}
CAPTURE_FIELDS = {'schema', 'session', 'strategy', 'scope', 'run_id', 'code_sha', 'config_sha256',
                  'generated_at', 'inputs', 'status', 'signals', 'reason'}
APPEND_FIELDS = {'revision', 'previous_sha256', 'published_at'}
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_RECORDS = 5000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return sha256(canonical(value).encode()).hexdigest()


def instant(value):
    if not isinstance(value, str):
        raise ValueError('invalid_timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() is None:
        raise ValueError('timezone_required')
    return parsed.astimezone(timezone.utc)


def require(ok, error):
    if not ok:
        raise ValueError(error)


def _matches(pattern, value):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def _validate_record(record, *, appended=False):
    fields = CAPTURE_FIELDS | APPEND_FIELDS if appended else CAPTURE_FIELDS
    require(isinstance(record, dict) and set(record) == fields
            and type(record['schema']) is int and record['schema'] == 1, 'signal_manifest_schema_invalid')
    session, strategy, scope, run_id = (record[k] for k in ('session', 'strategy', 'scope', 'run_id'))
    require(_matches(r'\d{4}-\d{2}-\d{2}', session) and isinstance(strategy, str)
            and strategy in SCOPES and scope == SCOPES[strategy], 'signal_manifest_identity_invalid')
    session_day = date.fromisoformat(session)
    require(type(run_id) is int and 0 < run_id <= 2**63 - 1 and _matches(SHA1, record['code_sha'])
            and _matches(SHA256, record['config_sha256']), 'signal_manifest_source_invalid')
    generated = instant(record['generated_at'])
    require(session_day <= generated.astimezone(KST).date(), 'signal_manifest_time_invalid')
    if appended:
        require(type(record['revision']) is int and record['revision'] > 0, 'signal_manifest_source_invalid')
        require(generated <= instant(record['published_at']), 'signal_manifest_time_invalid')

    status, signals, reason, inputs = (record[k] for k in ('status', 'signals', 'reason', 'inputs'))
    require(isinstance(status, str) and status in STATES and isinstance(inputs, dict), 'signal_manifest_status_invalid')
    if status in USABLE:
        require(isinstance(signals, list) and bool(signals) == (status == 'signals') and reason is None
                and {'universe', 'features'} <= inputs.keys()
                and all(_matches(CODE, code) for code in signals) and len(signals) == len(set(signals)),
                'signal_manifest_output_invalid')
    else:
        require(signals is None and _matches(r'[a-z0-9_]{1,90}', reason), 'signal_manifest_output_invalid')
    for label, evidence in inputs.items():
        require(_matches(r'[a-z0-9_]{1,80}', label) and isinstance(evidence, dict)
                and set(evidence) == {'sha256', 'source', 'observed_at', 'available_at'}
                and _matches(SHA256, evidence['sha256']) and isinstance(evidence['source'], str)
                and 0 < len(evidence['source']) <= 120, 'signal_manifest_input_invalid')
        require(instant(evidence['observed_at']) <= instant(evidence['available_at']) <= generated,
                'signal_manifest_input_after_generation')


def validate_capture(records):
    require(isinstance(records, list) and len(records) == len(SCOPES), 'incomplete_capture')
    for record in records:
        _validate_record(record)
    require([record['strategy'] for record in records] == list(SCOPES), 'incomplete_capture')
    identity = ('session', 'run_id', 'code_sha', 'config_sha256', 'generated_at')
    require(len({tuple(record[k] for k in identity) for record in records}) == 1, 'mixed_capture_receipt')
    return {key: records[0][key] for key in identity}


def validate_manifest(records):
    require(0 < len(records) <= MAX_MANIFEST_RECORDS, 'signal_manifest_record_limit')
    history = {}
    for record in records:
        _validate_record(record, appended=True)
        key = tuple(record[k] for k in ('session', 'strategy', 'scope'))
        published = instant(record['published_at'])
        revision, prior_hash, prior_publication = history.get(key, (0, None, None))
        require(record['revision'] == revision + 1 and record['previous_sha256'] == prior_hash
                and (prior_publication is None or prior_publication <= published),
                'signal_manifest_append_chain_invalid')
        history[key] = record['revision'], digest(record), published
    require(len(canonical(records).encode()) + 1 <= MAX_MANIFEST_BYTES, 'signal_manifest_too_large')
