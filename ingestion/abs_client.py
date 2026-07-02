"""Client for the ABS Data API (data.api.abs.gov.au), which is SDMX-based.

Two endpoints are used, both keyless:
  - /rest/data/...        the observations themselves (requested as CSV — the
                          simplest format to land untyped into a raw table)
  - /rest/codelist/...    the LGA code -> name reference list, needed to
                          conform the ABS numeric LGA codes with the free-text
                          LGA names in the Data.SA crash data.

Dataflow: ABS_ANNUAL_ERP_LGA2024 — Estimated Resident Population by LGA,
annual, with sex and age-band breakdowns. Dimension order (from the DSD):
MEASURE . SEX_ABS . AGE . LGA_2024 . REGION_TYPE . FREQUENCY
Verified codes: MEASURE=ERP, SEX_ABS in {1,2,3}, AGE total is 'TOT',
REGION_TYPE=LGA2024, FREQUENCY=A. Leaving a dimension empty = wildcard.

We pull ALL Australian LGAs, not just South Australia: raw should stay
faithful to the source; filtering to SA (LGA codes starting '4') is business
logic and belongs in the dbt staging layer.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from typing import Iterator

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://data.api.abs.gov.au/rest"
ERP_DATAFLOW = "ABS,ABS_ANNUAL_ERP_LGA2024,1.0.0"
LGA_CODELIST = "CL_LGA_2024"
START_PERIOD = "2015"

MAX_RETRIES = 4


def _get(url: str, params: dict | None = None, headers: dict | None = None) -> requests.Response:
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=300)
            resp.raise_for_status()
            return resp
        except requests.RequestException:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = 2**attempt
            log.warning("Request failed (attempt %d), retrying in %ds", attempt + 1, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


SEX_CODES = ("1", "2", "3")  # male, female, persons


def fetch_erp_rows(limit: int | None = None) -> Iterator[dict]:
    """Yield ERP observations for all sexes and age bands, all LGAs, 2015+.

    One request per sex code: the API rejects (HTTP 422) keys that wildcard
    both SEX_ABS and AGE at once — the response would exceed its size cap —
    but accepts any single wildcarded dimension. ``limit`` applies per request
    so CI still exercises every code path.
    """
    for sex in SEX_CODES:
        # Key positions: MEASURE.SEX_ABS.AGE.LGA_2024.REGION_TYPE.FREQUENCY
        url = f"{BASE_URL}/data/{ERP_DATAFLOW}/ERP.{sex}...LGA2024.A"
        resp = _get(url, params={"startPeriod": START_PERIOD, "format": "csv"})
        reader = csv.DictReader(io.StringIO(resp.text))
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            yield row


def fetch_lga_codelist() -> Iterator[dict]:
    """Yield the full LGA_2024 codelist as {code, name} rows.

    This is reference data (a few hundred rows) that lets dbt translate the
    ABS LGA codes in the ERP data into names matchable against the crash data.
    """
    resp = _get(
        f"{BASE_URL}/codelist/ABS/{LGA_CODELIST}",
        headers={"Accept": "application/vnd.sdmx.structure+json"},
    )
    codes = resp.json()["data"]["codelists"][0]["codes"]
    for code in codes:
        yield {"code": code["id"], "name": code["name"]}
