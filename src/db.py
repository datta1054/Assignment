import sqlite3
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple, List


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def execute_script(conn: sqlite3.Connection, sql: str) -> None:
    conn.executescript(sql)


def executemany(conn: sqlite3.Connection, sql: str, rows: Iterable[tuple[Any, ...]]) -> None:
    conn.executemany(sql, rows)


def fetch_all(
    conn: sqlite3.Connection,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> List[Tuple[Any, ...]]:
    cur = conn.cursor()
    cur.execute(sql, params or ())
    return cur.fetchall()
