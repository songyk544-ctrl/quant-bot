"""Attest the earliest verified read of a private signal capture with an Ed25519 reader key."""
import base64
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

from .forward_signal_contract import canonical, digest, instant, require, validate_capture

MAX_JSON = 16 * 1024 * 1024
MAX_KEY = 4096
ED25519_DER_HEADER = bytes.fromhex('302a300506032b6570032100')


def read_json(path, *, limit=MAX_JSON):
    def object_pairs(items):
        result = dict(items)
        require(len(result) == len(items), 'duplicate_json_key')
        return result

    with path.open('rb') as source:
        raw = source.read(limit + 1)
    require(len(raw) <= limit, 'receipt_too_large')
    return json.loads(raw, object_pairs_hook=object_pairs)


def _openssl(args):
    try:
        return subprocess.run(['openssl', *args], capture_output=True, check=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        raise ValueError('reader_signature_operation_failed') from None


def _observation_payload(records, observed_at):
    return {'schema': 1, **validate_capture(records), 'capture_sha256': digest(records),
            'available_at': instant(observed_at).isoformat()}


def observe(path, reader_private_key):
    """Run as the separate reader against the capture as seen in its read-only share."""
    records = read_json(path)
    observed_at = datetime.now(timezone.utc)
    proof = _observation_payload(records, observed_at.isoformat())
    require(instant(proof['generated_at']) <= observed_at, 'publication_before_capture')
    with TemporaryDirectory() as directory:
        payload = Path(directory) / 'observation.json'
        payload.write_bytes(canonical(proof).encode())
        signature = _openssl(['pkeyutl', '-sign', '-rawin', '-inkey', str(reader_private_key), '-in', str(payload)])
    require(len(signature) == 64, 'invalid_reader_signature')
    return {**proof, 'signature': base64.b64encode(signature).decode('ascii')}


def verify_observation(records, receipt, reader_public_key):
    """Verify with a public key supplied by the operator, never by the receipt."""
    require(isinstance(receipt, dict) and 'available_at' in receipt and 'signature' in receipt,
            'invalid_reader_receipt')
    proof = _observation_payload(records, receipt['available_at'])
    require(type(receipt.get('schema')) is int and type(receipt.get('run_id')) is int
            and proof == {k: v for k, v in receipt.items() if k != 'signature'}, 'reader_receipt_capture_mismatch')
    encoded_signature = receipt['signature']
    require(isinstance(encoded_signature, str) and len(encoded_signature) == 88, 'invalid_reader_signature')
    signature = base64.b64decode(encoded_signature, validate=True)
    require(len(signature) == 64, 'invalid_reader_signature')
    observed = instant(proof['available_at'])
    require(instant(proof['generated_at']) <= observed <= datetime.now(timezone.utc), 'invalid_reader_time')
    with reader_public_key.open('rb') as source:
        public_key = source.read(MAX_KEY + 1)
    require(0 < len(public_key) <= MAX_KEY, 'invalid_reader_public_key')
    with TemporaryDirectory() as directory:
        work = Path(directory)
        key_file, payload, signature_file = (work / name for name in ('key.pem', 'observation.json', 'signature'))
        key_file.write_bytes(public_key)
        der = _openssl(['pkey', '-pubin', '-in', str(key_file), '-outform', 'DER'])
        require(der.startswith(ED25519_DER_HEADER) and len(der) == len(ED25519_DER_HEADER) + 32,
                'invalid_reader_public_key')
        payload.write_bytes(canonical(proof).encode())
        signature_file.write_bytes(signature)
        _openssl(['pkeyutl', '-verify', '-rawin', '-pubin', '-inkey', str(key_file),
                  '-in', str(payload), '-sigfile', str(signature_file)])
    return observed, sha256(der).hexdigest()
