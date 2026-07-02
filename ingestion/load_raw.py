"""Land source data into the warehouse `raw` schema (bronze layer).

Design decisions:
  - Every column is TEXT. Bronze holds data exactly as the source sent it;
    typing/cleaning happens in dbt staging where it's version-controlled,
    documented and testable. A bad value should break a dbt test, not an
    ingestion script at 2am.
  - Loads are idempotent full refreshes (TRUNCATE + INSERT). The sources are
    small, slowly-published reference/statistical datasets — incremental
    loading would add state and failure modes for zero benefit here.
  - Two audit columns on every table: `_ingested_at` (when) and
    `_source_resource` (which upstream file/endpoint), giving row-level
    lineage back to the source system.

Usage:
    python -m ingestion.load_raw --source all [--limit 5000]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Iterable, Iterator

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from ingestion import abs_client, data_sa_client

log = logging.getLogger(__name__)

BATCH_SIZE = 5_000


def connect():
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_POSTGRES_PORT", "5433")),
        dbname=os.environ.get("WAREHOUSE_POSTGRES_DB", "sa_warehouse"),
        user=os.environ.get("WAREHOUSE_POSTGRES_USER", "warehouse"),
        password=os.environ.get("WAREHOUSE_POSTGRES_PASSWORD", "warehouse"),
    )


def recreate_table(cur, table: str, columns: list[str]) -> None:
    """(Re)create raw.<table> with all-TEXT columns plus audit columns.

    DROP + CREATE rather than TRUNCATE so a schema change upstream (new/renamed
    column) is absorbed automatically on the next run.
    """
    cur.execute(sql.SQL("DROP TABLE IF EXISTS raw.{}").format(sql.Identifier(table)))
    cols = sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(c)) for c in columns)
    cur.execute(
        sql.SQL(
            "CREATE TABLE raw.{} ({}, _source_resource text, _ingested_at timestamptz DEFAULT now())"
        ).format(sql.Identifier(table), cols)
    )


def insert_rows(cur, table: str, columns: list[str], rows: Iterable[tuple]) -> int:
    stmt = sql.SQL("INSERT INTO raw.{} ({}) VALUES %s").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns + ["_source_resource"]),
    )
    batch: list[tuple] = []
    total = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            execute_values(cur, stmt, batch)
            total += len(batch)
            batch = []
    if batch:
        execute_values(cur, stmt, batch)
        total += len(batch)
    return total


TABLE_BY_ENTITY = {
    "crashes": "data_sa_crashes",
    "casualties": "data_sa_casualties",
    "units": "data_sa_units",
}


def load_road_crashes(conn, limit: int | None) -> None:
    """Land the three road-crash entity tables (crash/casualty/units).

    All three come from ONE extract archive so REPORT_ID joins between them
    are internally consistent (see data_sa_client for why this matters).
    Columns come from each CSV's own header row.
    """
    seen = set()
    for entity, source_name, rows_iter in data_sa_client.fetch_extract_tables(limit=limit):
        table = TABLE_BY_ENTITY[entity]
        seen.add(entity)
        first = next(rows_iter)
        columns = list(first.keys())

        def as_tuples(first=first, rows_iter=rows_iter, columns=columns, source=source_name):
            yield tuple(first.get(c) for c in columns) + (source,)
            for row in rows_iter:
                yield tuple(row.get(c) for c in columns) + (source,)

        with conn.cursor() as cur:
            recreate_table(cur, table, columns)
            n = insert_rows(cur, table, columns, as_tuples())
            log.info("raw.%s <- %s: %d rows", table, source_name, n)
    conn.commit()

    missing = set(TABLE_BY_ENTITY) - seen
    if missing:
        raise RuntimeError(f"Extract archive was missing expected entities: {missing}")


def load_abs_erp(conn, limit: int | None) -> None:
    rows_iter = abs_client.fetch_erp_rows(limit=limit)
    first = next(rows_iter)
    columns = list(first.keys())

    def as_tuples() -> Iterator[tuple]:
        yield tuple(first.get(c) for c in columns) + (abs_client.ERP_DATAFLOW,)
        for row in rows_iter:
            yield tuple(row.get(c) for c in columns) + (abs_client.ERP_DATAFLOW,)

    with conn.cursor() as cur:
        recreate_table(cur, "abs_erp_lga", columns)
        n = insert_rows(cur, "abs_erp_lga", columns, as_tuples())
        log.info("raw.abs_erp_lga: %d rows", n)
    conn.commit()


def load_abs_lga_codelist(conn) -> None:
    columns = ["code", "name"]
    rows = (
        (item["code"], item["name"], abs_client.LGA_CODELIST)
        for item in abs_client.fetch_lga_codelist()
    )
    with conn.cursor() as cur:
        recreate_table(cur, "abs_lga_codelist", columns)
        n = insert_rows(cur, "abs_lga_codelist", columns, rows)
        log.info("raw.abs_lga_codelist: %d rows", n)
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw source data into the warehouse")
    parser.add_argument("--source", choices=["data_sa", "abs", "all"], default="all")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max rows per resource (CI uses this to keep runs fast; omit for full loads)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    conn = connect()
    try:
        if args.source in ("data_sa", "all"):
            load_road_crashes(conn, args.limit)
        if args.source in ("abs", "all"):
            load_abs_erp(conn, args.limit)
            load_abs_lga_codelist(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
