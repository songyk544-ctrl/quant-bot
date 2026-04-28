import os
import sqlite3
import shutil
from pathlib import Path

import pandas as pd


DB_PATH = "quantbot.db"
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


def table_exists(table_name: str, db_path: str = DB_PATH) -> bool:
    if not os.path.exists(db_path):
        return False
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
        return row is not None
    except Exception:
        return False


def read_table(
    table_name: str,
    csv_fallback: str | None = None,
    read_csv_kwargs: dict | None = None,
    db_path: str = DB_PATH,
) -> pd.DataFrame:
    if table_exists(table_name, db_path=db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                return pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        except Exception:
            pass
    resolved_csv = resolve_csv_path(csv_fallback) if csv_fallback else None
    if resolved_csv and os.path.exists(resolved_csv):
        try:
            kwargs = read_csv_kwargs or {}
            return pd.read_csv(resolved_csv, **kwargs)
        except Exception:
            return pd.DataFrame()
    if csv_fallback and os.path.exists(csv_fallback):
        # 레거시 루트 fallback (이동 실패 시)
        try:
            kwargs = read_csv_kwargs or {}
            return pd.read_csv(csv_fallback, **kwargs)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def write_table(
    table_name: str,
    df: pd.DataFrame,
    csv_path: str | None = None,
    csv_kwargs: dict | None = None,
    db_path: str = DB_PATH,
):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    if csv_path:
        kwargs = csv_kwargs or {}
        resolved_csv = resolve_csv_path(csv_path)
        df.to_csv(resolved_csv, **kwargs)


def migrate_csv_to_sqlite_once(table_csv_pairs: list[tuple[str, str]], db_path: str = DB_PATH):
    for table_name, csv_path in table_csv_pairs:
        resolved_csv = resolve_csv_path(csv_path)
        source_csv = resolved_csv if os.path.exists(resolved_csv) else csv_path
        if table_exists(table_name, db_path=db_path) or not os.path.exists(source_csv):
            continue
        try:
            df = pd.read_csv(source_csv, on_bad_lines="skip")
            write_table(table_name, df, csv_path=None, db_path=db_path)
        except Exception:
            continue
