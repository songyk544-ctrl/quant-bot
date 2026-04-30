import os
import shutil
from pathlib import Path

import pandas as pd


DATA_DIR = "data"


def resolve_csv_path(csv_path: str, migrate_legacy_root: bool = True) -> str:
    p = Path(csv_path)
    if p.is_absolute():
        return str(p)
    if p.parent != Path("."):
        return str(p)
    data_dir = Path(DATA_DIR)
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / p.name
    legacy = Path(p.name)
    if migrate_legacy_root and legacy.exists() and not target.exists():
        try:
            shutil.move(str(legacy), str(target))
        except Exception:
            pass
    return str(target)


def csv_exists(csv_path: str) -> bool:
    resolved = Path(resolve_csv_path(csv_path))
    legacy = Path(csv_path)
    return resolved.exists() or legacy.exists()


def table_exists(table_name: str, db_path: str = "quantbot.db") -> bool:
    """
    CSV 단일 운영 모드에서는 DB 테이블 존재를 사용하지 않습니다.
    기존 호출부 호환을 위해 항상 False를 반환합니다.
    """
    _ = (table_name, db_path)
    return False


def table_columns(table_name: str, db_path: str = "quantbot.db") -> list[str]:
    _ = (table_name, db_path)
    return []


def read_table(
    table_name: str,
    csv_fallback: str | None = None,
    read_csv_kwargs: dict | None = None,
    db_path: str = "quantbot.db",
) -> pd.DataFrame:
    _ = (table_name, db_path)
    resolved_csv = resolve_csv_path(csv_fallback) if csv_fallback else None
    if resolved_csv and os.path.exists(resolved_csv):
        try:
            kwargs = read_csv_kwargs or {}
            kwargs.setdefault("encoding", "utf-8-sig")
            return pd.read_csv(resolved_csv, **kwargs)
        except Exception:
            return pd.DataFrame()
    if csv_fallback and os.path.exists(csv_fallback):
        # 레거시 루트 fallback (이동 실패 시)
        try:
            kwargs = read_csv_kwargs or {}
            kwargs.setdefault("encoding", "utf-8-sig")
            return pd.read_csv(csv_fallback, **kwargs)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def write_table(
    table_name: str,
    df: pd.DataFrame,
    csv_path: str | None = None,
    csv_kwargs: dict | None = None,
    db_path: str = "quantbot.db",
):
    _ = (table_name, db_path)
    if csv_path:
        kwargs = csv_kwargs or {}
        resolved_csv = resolve_csv_path(csv_path)
        df.to_csv(resolved_csv, **kwargs)


def migrate_csv_to_sqlite_once(table_csv_pairs: list[tuple[str, str]], db_path: str = "quantbot.db"):
    """
    CSV 단일 운영 모드:
    - legacy 루트 CSV를 data/로 정리
    - 기존 호출부 호환을 위해 함수명/시그니처 유지
    """
    _ = db_path
    for table_name, csv_path in table_csv_pairs:
        _ = table_name
        resolved_csv = resolve_csv_path(csv_path)
        if os.path.exists(resolved_csv):
            continue
        if os.path.exists(csv_path):
            try:
                Path(resolved_csv).parent.mkdir(parents=True, exist_ok=True)
                shutil.move(csv_path, resolved_csv)
            except Exception:
                continue
