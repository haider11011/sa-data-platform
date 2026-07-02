"""Client for the Data.SA (data.sa.gov.au) road crash dataset.

Discovery findings that shaped this design (2026-07):

  - The `road-crash-data` CKAN package publishes rolling 5-year extracts as
    ZIP archives, each containing three relational CSVs: Crash (one row per
    crash), Casualty (one row per person injured) and Units (one row per
    vehicle/pedestrian involved), linked by REPORT_ID.
  - REPORT_ID is only consistent *within* one extract: crashes are renumbered
    on every extract (e.g. crash 2020-12948 in the 2024 extract is not crash
    2020-12948 in the 2023 extract), and the ID embeds the extract date as a
    suffix ("2020-1-19/09/2025"). Mixing extracts therefore silently breaks
    joins and creates undetectable duplicates.
  - The portal's DataStore API is not reliable here: it loaded the crash-level
    table for one extract but the casualty-level table for the others.

  => We download exactly ONE archive — the latest 5-year extract, resolved
     dynamically from package metadata (never a hardcoded resource UUID) —
     and land all three entity tables from it, so every REPORT_ID join in the
     warehouse is internally consistent by construction.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import time
import zipfile
from typing import Iterator

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://data.sa.gov.au/data/api/3/action"
ROAD_CRASH_PACKAGE = "road-crash-data"
# e.g. "Road Crash Data 2020 to 2024 (5 years)" -> captures the end year
FIVE_YEAR_FILE_PATTERN = re.compile(r"Road Crash Data \d{4} to (\d{4}) \(5 years\)")

# ZIP member name -> logical entity, e.g. "2020-2024_DATA_SA_Crash.csv"
ENTITY_BY_MEMBER_SUFFIX = {
    "_Crash.csv": "crashes",
    "_Casualty.csv": "casualties",
    "_Units.csv": "units",
}

MAX_RETRIES = 4


def _get(url: str, params: dict | None = None) -> requests.Response:
    """GET with retry + exponential backoff — public portals throttle and blip."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=300)
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 2**attempt
            log.warning("Request failed (attempt %d), retrying in %ds", attempt + 1, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def find_latest_extract() -> dict:
    """Return the resource metadata of the most recent 5-year ZIP extract."""
    payload = _get(f"{BASE_URL}/package_show", params={"id": ROAD_CRASH_PACKAGE}).json()
    if not payload.get("success"):
        raise RuntimeError(f"CKAN API returned success=false: {payload.get('error')}")

    candidates = []
    for resource in payload["result"]["resources"]:
        match = FIVE_YEAR_FILE_PATTERN.match(resource.get("name", ""))
        if match and resource.get("url", "").endswith(".zip"):
            candidates.append((int(match.group(1)), resource))
    if not candidates:
        raise RuntimeError(
            f"No 5-year ZIP extracts found in package '{ROAD_CRASH_PACKAGE}' — "
            "the portal layout may have changed; inspect package_show output."
        )
    end_year, resource = max(candidates, key=lambda c: c[0])
    log.info("Latest extract: %s (to %d)", resource["name"], end_year)
    return resource


def fetch_extract_tables(limit: int | None = None) -> Iterator[tuple[str, str, Iterator[dict]]]:
    """Download the latest extract and yield (entity, source_name, rows) per CSV.

    Rows are dicts keyed by the CSV's own headers; empty strings become None so
    the warehouse gets real NULLs. ``limit`` caps rows per entity (CI uses this
    to keep runs fast while still exercising the real download path).
    """
    resource = find_latest_extract()
    resp = _get(resource["url"])
    archive = zipfile.ZipFile(io.BytesIO(resp.content))

    for member in archive.namelist():
        entity = next(
            (e for suffix, e in ENTITY_BY_MEMBER_SUFFIX.items() if member.endswith(suffix)),
            None,
        )
        if entity is None:
            log.warning("Skipping unrecognised archive member: %s", member)
            continue

        def rows(member=member) -> Iterator[dict]:
            with archive.open(member) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                for i, row in enumerate(reader):
                    if limit is not None and i >= limit:
                        return
                    yield {k: (v if v != "" else None) for k, v in row.items()}

        yield entity, f"{resource['name']} [{member}]", rows()
