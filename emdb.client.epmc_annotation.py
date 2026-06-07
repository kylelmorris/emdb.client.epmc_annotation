#!/usr/bin/env python3
"""
emdb.client.epmc_annotation_search.py

Query EMDB and annotate entries using Europe PMC text-mining rules.
Supports:
- EMDB Solr filtering
- Single-rule EPMC annotation (--epmc)
- Multi-rule CSV annotation (--epmc_query_csv)
- AND/OR keyword logic
- Correct parentheses handling for AND-block queries
- Per-rule caches stored in epmc_cache/
- tqdm progress bars
- Summary reporting (--summary)
- Verbosity control (--verbose)
- Query debugging (--debug-query)
- DOI normalisation
- Robust EMDB retry wrapper
"""

import argparse
import json
import re
import shlex
import sys
import time
import os
import shutil
from typing import List, Dict, Tuple
from urllib.parse import quote

import pandas as pd
import requests
from tqdm import tqdm
from emdb.client import EMDB


# ============================================================================
# Suppress harmless Pydantic warnings
# ============================================================================
import warnings
warnings.filterwarnings(
    "ignore",
    message="Field .* has conflict with protected namespace"
)


# ============================================================================
# Cache Directory
# ============================================================================
CACHE_DIR = "epmc_cache"
CITED_BY_COUNT_COLUMN = "citedByCount"
EPMC_FULLTEXT_AVAILABLE_COLUMN = "epmcFulltextAvailable"
DOI_CITATION_CACHE_FILE = "doi_citation_counts.json"
DOI_FULLTEXT_CACHE_FILE = "doi_fulltext_coverage.json"

def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)


def format_command_arg(arg: str) -> str:
    """Shell-quote an argument for the saved summary command."""
    if arg == "*":
        return '"*"'
    return shlex.quote(arg)


# ============================================================================
# Cache Helpers
# ============================================================================
def _make_cache_key(annotation_group: str, annotation_value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", f"{annotation_group}_{annotation_value}")

def minimal_cache_path(annotation_group: str, annotation_value: str) -> str:
    return os.path.join(CACHE_DIR, f"cache_{_make_cache_key(annotation_group, annotation_value)}.json")

def full_cache_path(annotation_group: str, annotation_value: str) -> str:
    return os.path.join(CACHE_DIR, f"cache_{_make_cache_key(annotation_group, annotation_value)}_full.json")

def doi_citation_cache_path() -> str:
    return os.path.join(CACHE_DIR, DOI_CITATION_CACHE_FILE)

def doi_fulltext_cache_path() -> str:
    return os.path.join(CACHE_DIR, DOI_FULLTEXT_CACHE_FILE)

def load_minimal_cache(annotation_group: str, annotation_value: str) -> Dict:
    try:
        with open(minimal_cache_path(annotation_group, annotation_value), "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_minimal_cache(annotation_group: str, annotation_value: str, data: Dict, verbose: bool):
    path = minimal_cache_path(annotation_group, annotation_value)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if verbose:
        print(f"[CACHE] Saved minimal cache: {path}")

def save_full_cache(annotation_group: str, annotation_value: str, data: Dict, verbose: bool):
    path = full_cache_path(annotation_group, annotation_value)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if verbose:
        print(f"[CACHE] Saved full cache: {path}")

def load_doi_citation_cache() -> Dict:
    try:
        with open(doi_citation_cache_path(), "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}

    if "citation_counts" not in data:
        data["citation_counts"] = {}
    if "not_found" not in data:
        data["not_found"] = []
    return data

def save_doi_citation_cache(data: Dict, verbose: bool):
    path = doi_citation_cache_path()
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    if verbose:
        print(f"[CACHE] Saved DOI citation cache: {path}")

def load_doi_fulltext_cache() -> Dict:
    try:
        with open(doi_fulltext_cache_path(), "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {}

    if "has_fulltext" not in data:
        data["has_fulltext"] = []
    if "no_fulltext" not in data:
        data["no_fulltext"] = []
    return data

def save_doi_fulltext_cache(data: Dict, verbose: bool):
    path = doi_fulltext_cache_path()
    with open(path, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    if verbose:
        print(f"[CACHE] Saved DOI full-text coverage cache: {path}")


# ============================================================================
# Term Parsing
# ============================================================================
def clean_terms(value) -> List[str]:
    """Convert CSV field to clean list of strings."""
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    text = str(value).strip()
    if text.lower() in ("", "nan", "none"):
        return []
    return [p.strip() for p in text.split(",") if p.strip() and p.lower() not in ("nan", "none")]


# ============================================================================
# CSV Encoding Helpers
# ============================================================================
def read_epmc_query_csv(csv_path: str, verbose: bool = False) -> pd.DataFrame:
    """Read an EPMC query CSV, converting common legacy encodings to UTF-8."""
    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
        if "annotation_group" not in df.columns and "annotation_column" in df.columns:
            df = df.rename(columns={"annotation_column": "annotation_group"})
        return df
    except UnicodeDecodeError as utf8_error:
        fallback_encodings = ("utf-8-sig", "mac_roman", "cp1252", "latin1")

        for encoding in fallback_encodings:
            try:
                df = pd.read_csv(csv_path, encoding=encoding)
            except UnicodeDecodeError:
                continue

            backup_path = f"{csv_path}.encoding.bak"
            backup_idx = 1
            while os.path.exists(backup_path):
                backup_idx += 1
                backup_path = f"{csv_path}.encoding.bak{backup_idx}"
            shutil.copy2(csv_path, backup_path)

            if "annotation_group" not in df.columns and "annotation_column" in df.columns:
                df = df.rename(columns={"annotation_column": "annotation_group"})
            df.to_csv(csv_path, index=False, encoding="utf-8")
            print(
                f"[EPMC] Converted annotation CSV from {encoding} to UTF-8: {csv_path}"
            )
            print(f"[EPMC] Original CSV backup: {backup_path}")
            return df

        raise UnicodeDecodeError(
            utf8_error.encoding,
            utf8_error.object,
            utf8_error.start,
            utf8_error.end,
            (
                f"{utf8_error.reason}. Could not read '{csv_path}' as UTF-8 or "
                f"fallback encodings: {', '.join(fallback_encodings)}"
            ),
        ) from utf8_error


# ============================================================================
# DOI Normalisation
# ============================================================================
def clean_doi_for_query(doi: str) -> str | None:
    """Normalise DOI for matching but do not modify DataFrame."""
    if not doi or not isinstance(doi, str):
        return None

    d = doi.strip().lower()

    prefixes = [
        "doi:", "https://doi.org/", "http://doi.org/",
        "https://dx.doi.org/", "http://dx.doi.org/"
    ]
    for p in prefixes:
        if d.startswith(p):
            d = d.replace(p, "")

    d = d.replace("[", "").replace("]", "")
    d = d.replace("'", "").replace('"', "")
    d = d.strip()

    return d if "/" in d else None


def extract_dois(value) -> List[str]:
    """Extract & normalise DOIs from EMDB xref_DOI field."""
    if not isinstance(value, str):
        return []

    s = value.replace("[", "").replace("]", "")
    s = s.replace("'", "").replace('"', "")
    raw_parts = re.split(r"[;, \t]+", s)

    cleaned = []
    for part in raw_parts:
        doi = clean_doi_for_query(part)
        if doi:
            cleaned.append(doi)
    return cleaned


def count_unique_dois(series: pd.Series) -> int:
    """Count unique normalised DOI values from an xref_DOI-like Series."""
    dois = set()
    for raw in series:
        dois.update(extract_dois(raw))
    return len(dois)


def sum_unique_doi_citations(series: pd.Series, citation_counts: Dict[str, int]) -> int:
    """Sum citation counts once per unique normalised DOI in a Series."""
    dois = set()
    for raw in series:
        dois.update(extract_dois(raw))
    return sum(citation_counts.get(doi, 0) for doi in dois)


def calculate_h_index(citation_values: List[int]) -> int:
    """Return h where h publications each have at least h citations."""
    h_index = 0
    for rank, citations in enumerate(sorted(citation_values, reverse=True), start=1):
        if citations >= rank:
            h_index = rank
        else:
            break
    return h_index


def h_index_for_dois(series: pd.Series, citation_counts: Dict[str, int]) -> int:
    """Calculate h-index from unique DOI citation counts in a Series."""
    dois = set()
    for raw in series:
        dois.update(extract_dois(raw))
    return calculate_h_index([citation_counts.get(doi, 0) for doi in dois])


def row_cited_by_count(value, citation_counts: Dict[str, int]):
    """Return a numeric citation count for a row's known DOI(s), or blank."""
    dois = set(extract_dois(value))
    known_dois = dois.intersection(citation_counts)
    if not known_dois:
        return ""
    return sum(citation_counts[doi] for doi in known_dois)


def collect_unique_dois(series: pd.Series) -> List[str]:
    """Return sorted unique normalised DOIs from an xref_DOI-like Series."""
    dois = set()
    for raw in series:
        dois.update(extract_dois(raw))
    return sorted(dois)


def row_fulltext_availability(value, searchable_dois: set, unsearchable_dois: set) -> str:
    """Return row-level Europe PMC full-text availability for known DOI(s)."""
    dois = set(extract_dois(value))
    if not dois:
        return ""

    searchable_count = len(dois.intersection(searchable_dois))
    unsearchable_count = len(dois.intersection(unsearchable_dois))
    unknown_count = len(dois) - searchable_count - unsearchable_count

    if searchable_count == len(dois):
        return "yes"
    if unsearchable_count == len(dois):
        return "no"
    if unknown_count == len(dois):
        return "unknown"

    parts = []
    if searchable_count:
        parts.append(f"yes:{searchable_count}")
    if unsearchable_count:
        parts.append(f"no:{unsearchable_count}")
    if unknown_count:
        parts.append(f"unknown:{unknown_count}")
    return "; ".join(parts)


# ============================================================================
# Europe PMC Query Builder (with parentheses fix)
# ============================================================================
def build_epmc_query_with_and_or(sections: List[str],
                                 or_terms: List[str],
                                 and_terms: List[str],
                                 debug=False,
                                 tag="") -> str:
    """
    Build an EPMC query with OR-group AND OR-group semantics.

    string_OR semantics:
        any of these terms may satisfy the primary concept

    string_AND semantics:
        at least one of these terms must also be present

    Final structure:
        (OR block) AND ( OR of AND-groups )
    """

    # -----------------------------
    # OR BLOCK
    # -----------------------------
    or_parts = []
    for s in sections:
        for t in or_terms:
            or_parts.append(f'{s}:"{t}"')

    # -----------------------------
    # AND BLOCK (group wrapped in parentheses)
    # -----------------------------
    and_group_parts = []
    for t in and_terms:
        ors = [f'{s}:"{t}"' for s in sections]
        and_group_parts.append(" OR ".join(ors))

    blocks = []

    # OR block
    if or_parts:
        blocks.append(" OR ".join(or_parts))

    # AND block in parentheses
    if and_group_parts:
        blocks.append("(" + " OR ".join(and_group_parts) + ")")

    final_query = " AND ".join(blocks) if blocks else "*"

    if debug:
        print(f"\n[DEBUG QUERY] {tag}")
        print("Sections:", sections)
        print("OR terms:", or_terms)
        print("AND terms:", and_terms)
        print("OR block:", " OR ".join(or_parts))
        print("AND block:", "(" + " OR ".join(and_group_parts) + ")")
        print("Final query:", final_query)
        print()

    return final_query


# ============================================================================
# Europe PMC CursorMark Pagination
# ============================================================================
def epmc_cursor_paged_query(query: str,
                            annotation_group: str,
                            annotation_value: str,
                            debug_query: bool,
                            full_cache: bool,
                            verbose: bool) -> Tuple[List[str], Dict, Dict[str, int]]:

    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    cursor = "*"
    dois = []
    citation_counts = {}
    full_pages = {}
    tag = f"{annotation_group}|{annotation_value}"

    print(f"[EPMC:{tag}] Fetching paginated results...")
    pbar = tqdm(unit="page", desc=f"EPMC:{tag}")

    while True:
        params = {
            "query": query,
            "format": "json",
            "cursorMark": cursor,
            "pageSize": 1000,
            "resultType": "core"
        }

        try:
            if debug_query:
                prepared = requests.Request("GET", SEARCH_URL, params=params).prepare()
                print(f"[DEBUG EPMC URL] {prepared.url}")
            r = requests.get(SEARCH_URL, params=params, timeout=20)
            data = r.json()
        except Exception as e:
            print(f"[EPMC:{tag}] ERROR: {e}")
            break

        results = data.get("resultList", {}).get("result", [])
        for rec in results:
            doi = rec.get("doi")
            doi_clean = clean_doi_for_query(doi)
            if doi_clean:
                dois.append(doi_clean)
                if "citedByCount" in rec:
                    try:
                        cited_by_count = int(rec.get("citedByCount") or 0)
                    except (TypeError, ValueError):
                        cited_by_count = 0
                    citation_counts[doi_clean] = max(
                        citation_counts.get(doi_clean, 0),
                        cited_by_count,
                    )

        if full_cache:
            full_pages[cursor] = data

        next_cursor = data.get("nextCursorMark")
        pbar.update(1)

        if not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        time.sleep(0.05)

    pbar.close()
    return sorted(set(dois)), full_pages, citation_counts


def epmc_fetch_citation_counts_for_dois(dois: List[str],
                                        seed_counts: Dict[str, int],
                                        debug_query: bool,
                                        verbose: bool,
                                        batch_size: int = 50) -> Dict[str, int]:
    """Fetch Europe PMC citedByCount values for DOI records needed by output."""
    if not dois:
        return {}

    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    cache = load_doi_citation_cache()
    citation_counts = {
        str(doi).lower(): int(count)
        for doi, count in cache.get("citation_counts", {}).items()
        if count is not None
    }
    citation_counts.update(seed_counts)
    not_found = set(cache.get("not_found", []))

    missing = [
        doi for doi in dois
        if doi not in citation_counts and doi not in not_found
    ]

    if verbose:
        print(
            f"[EPMC citations] {len(dois)} unique DOI(s); "
            f"{len(citation_counts)} cached count(s); {len(missing)} to query."
        )

    if missing:
        pbar = tqdm(
            total=(len(missing) + batch_size - 1) // batch_size,
            unit="batch",
            desc="EPMC citations",
        )
        for start in range(0, len(missing), batch_size):
            batch = missing[start:start + batch_size]
            query = " OR ".join(f'DOI:"{doi}"' for doi in batch)
            params = {
                "query": query,
                "format": "json",
                "pageSize": max(1000, len(batch)),
                "resultType": "core",
            }

            found = set()
            try:
                if debug_query:
                    prepared = requests.Request("GET", SEARCH_URL, params=params).prepare()
                    print(f"[DEBUG EPMC CITATION URL] {prepared.url}")
                r = requests.get(SEARCH_URL, params=params, timeout=20)
                data = r.json()
            except Exception as e:
                print(f"[EPMC citations] ERROR: {e}")
                pbar.update(1)
                continue

            for rec in data.get("resultList", {}).get("result", []):
                doi_clean = clean_doi_for_query(rec.get("doi"))
                if not doi_clean or doi_clean not in batch:
                    continue
                found.add(doi_clean)
                try:
                    cited_by_count = int(rec.get("citedByCount") or 0)
                except (TypeError, ValueError):
                    cited_by_count = 0
                citation_counts[doi_clean] = max(
                    citation_counts.get(doi_clean, 0),
                    cited_by_count,
                )

            not_found.update(set(batch) - found)
            pbar.update(1)
            time.sleep(0.05)

        pbar.close()
        cache["citation_counts"] = citation_counts
        cache["not_found"] = sorted(not_found)
        cache["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        save_doi_citation_cache(cache, verbose)

    return citation_counts


def epmc_fetch_fulltext_coverage_for_dois(dois: List[str],
                                          debug_query: bool,
                                          verbose: bool,
                                          batch_size: int = 50) -> Dict:
    """Return DOI-level Europe PMC full-text coverage for section-search caveats."""
    if not dois:
        return {
            "total_dois": 0,
            "has_fulltext": set(),
            "no_fulltext": set(),
        }

    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    cache = load_doi_fulltext_cache()
    has_fulltext = set(cache.get("has_fulltext", []))
    no_fulltext = set(cache.get("no_fulltext", []))
    no_fulltext.difference_update(has_fulltext)

    missing = [
        doi for doi in dois
        if doi not in has_fulltext and doi not in no_fulltext
    ]

    if verbose:
        print(
            f"[EPMC coverage] {len(dois)} unique DOI(s); "
            f"{len(has_fulltext)} cached full-text DOI(s); "
            f"{len(no_fulltext)} cached non-full-text DOI(s); "
            f"{len(missing)} to query."
        )

    if missing:
        pbar = tqdm(
            total=(len(missing) + batch_size - 1) // batch_size,
            unit="batch",
            desc="EPMC coverage",
        )
        for start in range(0, len(missing), batch_size):
            batch = missing[start:start + batch_size]
            doi_block = " OR ".join(f'DOI:"{doi}"' for doi in batch)
            query = f"({doi_block}) AND HAS_FT:y"
            params = {
                "query": query,
                "format": "json",
                "pageSize": max(1000, len(batch)),
                "resultType": "core",
            }

            found = set()
            try:
                if debug_query:
                    prepared = requests.Request("GET", SEARCH_URL, params=params).prepare()
                    print(f"[DEBUG EPMC COVERAGE URL] {prepared.url}")
                r = requests.get(SEARCH_URL, params=params, timeout=20)
                data = r.json()
            except Exception as e:
                print(f"[EPMC coverage] ERROR: {e}")
                pbar.update(1)
                continue

            for rec in data.get("resultList", {}).get("result", []):
                doi_clean = clean_doi_for_query(rec.get("doi"))
                if doi_clean and doi_clean in batch:
                    found.add(doi_clean)

            has_fulltext.update(found)
            no_fulltext.update(set(batch) - found)
            pbar.update(1)
            time.sleep(0.05)

        pbar.close()
        cache["has_fulltext"] = sorted(has_fulltext)
        cache["no_fulltext"] = sorted(no_fulltext)
        cache["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        cache["coverage_query"] = "DOI batch query with HAS_FT:y"
        save_doi_fulltext_cache(cache, verbose)

    doi_set = set(dois)
    searchable = doi_set.intersection(has_fulltext)
    unsearchable = doi_set.intersection(no_fulltext)
    unknown = doi_set - searchable - unsearchable
    return {
        "total_dois": len(doi_set),
        "has_fulltext": searchable,
        "no_fulltext": unsearchable,
        "unknown": unknown,
    }


# ============================================================================
# EMDB API Retry Wrapper
# ============================================================================
def robust_csv_search(client, query, fields, retries=5, delay=5, verbose=False):
    attempt = 1
    while True:
        try:
            if verbose:
                print(f"[EMDB] Attempt {attempt}/{retries}: csv_search()")
            return client.csv_search(query=query, fields=fields)
        except Exception as e:
            if attempt >= retries:
                print(f"[EMDB] ERROR: Failed after {retries} attempts")
                raise
            print(f"[EMDB] Warning: {e} — retrying in {delay}s")
            time.sleep(delay)
            attempt += 1


def robust_http_csv_search(query, fields, retries=5, delay=5, verbose=False,
                           debug_query=False,
                           emdb_api_base="https://www.ebi.ac.uk/emdb/api"):
    """Direct HTTP equivalent of EMDB().csv_search()."""
    from io import StringIO

    attempt = 1
    while True:
        try:
            if verbose:
                print(f"[EMDB HTTP] Attempt {attempt}/{retries}: GET /search/<query>")

            endpoint = f"{emdb_api_base.rstrip('/')}/search/{query}"
            params = {
                "rows": 1000000,
                "wt": "csv",
                "download": "false"
            }
            if fields:
                params["fl"] = fields

            if debug_query:
                prepared = requests.Request("GET", endpoint, params=params).prepare()
                print(f"[DEBUG EMDB HTTP URL] {prepared.url}")

            response = requests.get(endpoint, params=params, timeout=20)
            response.raise_for_status()
            return pd.read_csv(StringIO(response.text.strip()))
        except Exception as e:
            if attempt >= retries:
                print(f"[EMDB HTTP] ERROR: Failed after {retries} attempts")
                raise
            print(f"[EMDB HTTP] Warning: {e} — retrying in {delay}s")
            time.sleep(delay)
            attempt += 1


# ============================================================================
# Solr Filter Parsing
# ============================================================================
def parse_filter_expression(expr: str) -> str:
    if "=" in expr and not any(op in expr for op in (">", "<")):
        f, v = expr.split("=", 1)
        return f"{f}:{v}"
    if ">=" in expr:
        f, v = expr.split(">=", 1)
        return f"{f}:[{v} TO *]"
    if "<=" in expr:
        f, v = expr.split("<=", 1)
        return f"{f}:[* TO {v}]"
    if ">" in expr:
        f, v = expr.split(">", 1)
        return f"{f}:[{v} TO *]"
    if "<" in expr:
        f, v = expr.split("<", 1)
        return f"{f}:[* TO {v}]"
    return expr

def build_query(method: str, status: str, filters: List[str]) -> str:
    clauses = []
    if method != "*":
        clauses.append(f"structure_determination_method:{method}")
    if status != "*":
        clauses.append(f"current_status:{status}")
    clauses.extend(parse_filter_expression(f) for f in filters)
    return " AND ".join(clauses) if clauses else "*:*"


def build_emdb_search_query_url(query: str) -> str:
    encoded_query = quote(query, safe=":")
    return f"https://www.ebi.ac.uk/emdb/emsearch/{encoded_query}"


def normalise_fields_arg(fields: str) -> str:
    """Normalise a comma-separated --fields argument by trimming whitespace."""
    parts = [part.strip() for part in fields.split(",")]
    return ",".join(part for part in parts if part)


def ensure_required_fields(fields: str, required_fields: List[str]) -> str:
    """Append required fields once, preserving user-supplied order."""
    parts = [part.strip() for part in fields.split(",") if part.strip()]
    seen = {part for part in parts}
    for field in required_fields:
        if field not in seen:
            parts.append(field)
            seen.add(field)
    return ",".join(parts)


def get_annotation_summary_columns(df: pd.DataFrame, args) -> List[str]:
    """Return annotation columns relevant to the chosen annotation mode."""
    if args.epmc:
        return [args.annotation_group] if args.annotation_group in df.columns else []

    if args.epmc_query_csv:
        rules = read_epmc_query_csv(args.epmc_query_csv)
        if "annotation_group" not in rules.columns:
            return []
        ordered = []
        seen = set()
        for col in rules["annotation_group"].tolist():
            if col in df.columns and col not in seen:
                ordered.append(col)
                seen.add(col)
        return ordered

    ignore = {"emdb_id", "xref_DOI"}
    return [
        col for col in df.columns
        if col not in ignore and df[col].dtype == object
    ]


def build_analysis_description() -> str:
    return (
        "This report summarises rule-based annotations assigned to EMDB entries by "
        "matching linked publication DOIs from EMDB against Europe PMC search results "
        "using the configured key-phrase rules and publication sections. An annotation "
        "indicates that keyword-based evidence was identified in at least one "
        "publication linked to an EMDB entry. These data are therefore useful for "
        "exploratory analysis, frequency summaries, and follow-up review. Caution is "
        "advised in treating these annotations as definitive proof of method use, "
        "facility use, software use, or experimental provenance, as results depend on "
        "the EMDB query, DOI linkage, Europe PMC text availability, and the rule set used."
    )


# ============================================================================
# Annotation Logic
# ============================================================================
def annotate_with_multiple_matches(df: pd.DataFrame,
                                   doi_hits: List[str],
                                   doi_col: str,
                                   annotation_group: str,
                                   annotation_value: str,
                                   citations: bool,
                                   citation_counts: Dict[str, int],
                                   verbose: bool) -> pd.DataFrame:

    doi_set = set(doi_hits)
    tag = f"{annotation_group}|{annotation_value}"
    print(f"[EPMC:{tag}] Annotating...")

    if doi_col not in df.columns:
        raise ValueError(
            f"EPMC annotation requires a '{doi_col}' column in the EMDB results. "
            f"Include it in --fields or let the script auto-add it."
        )

    if annotation_group not in df.columns:
        df[annotation_group] = ""

    colvals = df[annotation_group].tolist()

    for idx, raw in enumerate(tqdm(df[doi_col], desc=f"Annot {annotation_group}", unit="entry")):
        for d in extract_dois(raw):
            if d in doi_set:
                colvals[idx] = (
                    annotation_value
                    if colvals[idx] == ""
                    else f"{colvals[idx]}; {annotation_value}"
                )
                break

    df[annotation_group] = colvals
    return df


# ============================================================================
# Multi-rule Annotation
# ============================================================================
def run_multi_epmc_annotations(df: pd.DataFrame,
                               csv_path: str,
                               full_cache: bool,
                               verbose: bool,
                               debug_query: bool,
                               citations: bool) -> Tuple[pd.DataFrame, Dict[str, int]]:

    print(f"[EPMC] Loading annotation CSV: {csv_path}")
    rules = read_epmc_query_csv(csv_path, verbose=verbose)

    required = {"annotation_group", "annotation", "section", "string_OR", "string_AND"}
    if not required.issubset(rules.columns):
        raise ValueError(f"CSV must contain columns: {required}")

    all_citation_counts = {}

    for idx, row in rules.iterrows():

        annotation_group = row["annotation_group"]
        annotation_value  = row["annotation"]
        sections  = clean_terms(row["section"])
        or_terms  = clean_terms(row["string_OR"])
        and_terms = clean_terms(row["string_AND"])

        tag = f"{annotation_group}|{annotation_value}"

        print(f"\n[EPMC Rule {idx+1}/{len(rules)}] {tag}")

        epmc_query = build_epmc_query_with_and_or(
            sections, or_terms, and_terms,
            debug=debug_query,
            tag=tag
        )

        cached = load_minimal_cache(annotation_group, annotation_value)

        if cached.get("query") == epmc_query and (not citations or "citation_counts" in cached):
            doi_hits = cached["results"]
            citation_counts = cached.get("citation_counts", {})
            if verbose:
                print(f"[EPMC:{tag}] Using cached results")
        else:
            doi_hits, full_pages, citation_counts = epmc_cursor_paged_query(
                epmc_query, annotation_group, annotation_value,
                debug_query,
                full_cache, verbose
            )

            save_minimal_cache(annotation_group, annotation_value, {
                "query": epmc_query,
                "results": doi_hits,
                "annotation_group": annotation_group,
                "annotation_value": annotation_value,
                "sections": sections,
                "or_terms": or_terms,
                "and_terms": and_terms,
                "citation_counts": citation_counts,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            }, verbose)

            if full_cache:
                save_full_cache(annotation_group, annotation_value, {
                    "query": epmc_query,
                    "pages": full_pages,
                }, verbose)

        all_citation_counts.update(citation_counts)
        df = annotate_with_multiple_matches(
            df, doi_hits, "xref_DOI",
            annotation_group, annotation_value,
            citations, citation_counts,
            verbose
        )

    return df, all_citation_counts


# ============================================================================
# Argparse
# ============================================================================
def get_args():
    parser = argparse.ArgumentParser(description="Query EMDB and annotate using Europe PMC.")

    parser.add_argument("--method", type=str, default="*")
    parser.add_argument("--status", type=str, default="REL")
    parser.add_argument("--fields", type=str, default="emdb_id,xref_DOI")
    parser.add_argument("--where", nargs="*", default=[])

    parser.add_argument("--output", type=str, default="results",
                        help="Basename for output CSV + summary.")

    parser.add_argument("--print", dest="print_df", action="store_true")
    parser.add_argument("--summary", dest="summary", action="store_true",
                        help="Write summary outputs (default: on).")
    parser.add_argument("--no-summary", dest="summary", action="store_false",
                        help="Disable summary text and summary-count CSV outputs.")
    parser.set_defaults(summary=True)
    parser.add_argument(
        "--publications",
        action="store_true",
        help=(
            "Add publication_count, annotated_publications, and total_publications "
            "to the summary-count CSV using DOI values from the main output xref_DOI column."
        )
    )
    parser.add_argument(
        "--citations",
        action="store_true",
        help=(
            "Add Europe PMC citedByCount values to the output CSV and "
            "summary-count CSV."
        )
    )
    parser.add_argument(
        "--h-index",
        dest="h_index",
        action="store_true",
        help=(
            "Add hIndex to the summary-count CSV using Europe PMC citedByCount "
            "values for each annotation value's unique DOI set."
        )
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help=(
            "Report DOI-level Europe PMC full-text coverage for section-based "
            "annotation searches."
        )
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug-query", action="store_true",
                        help="Print full query breakdown before executing.")

    parser.add_argument("--epmc", action="store_true")
    parser.add_argument("--section", type=str, default="METHODS")
    parser.add_argument("--string", type=str)
    parser.add_argument("--annotation_group", type=str, default="EPMC_ANNOTATION")

    parser.add_argument("--epmc_query_csv", type=str)
    parser.add_argument("--full_cache", action="store_true")
    parser.add_argument(
        "--emdb_source",
        choices=["client", "http"],
        default="http",
        help="How to query EMDB: Python client (client) or direct HTTP endpoint (http)."
    )
    parser.add_argument(
        "--emdb_api_base",
        type=str,
        default="https://www.ebi.ac.uk/emdb/api",
        help="Base URL for EMDB API when --emdb_source http is used."
    )

    return parser.parse_args()


# ============================================================================
# Main
# ============================================================================
def main():
    args = get_args()
    ensure_cache_dir()

    basename = args.output.replace(".csv", "").strip()
    output_csv  = f"{basename}.csv"
    summary_txt = f"{basename}_summary.txt"
    summary_counts_csv = f"{basename}_summary_counts.csv"

    if args.verbose:
        print("[Arguments]")
        for k, v in vars(args).items():
            print(f"  {k}: {v}")
        print()

    args.fields = normalise_fields_arg(args.fields)
    if args.epmc or args.epmc_query_csv or args.publications or args.citations or args.h_index or args.coverage:
        args.fields = ensure_required_fields(args.fields, ["xref_DOI"])
    if args.epmc_query_csv:
        read_epmc_query_csv(args.epmc_query_csv, verbose=args.verbose)

    # -----------------------------
    # Build EMDB query
    # -----------------------------
    query = build_query(args.method, args.status, args.where)

    if args.debug_query:
        print(f"[DEBUG EMDB QUERY] {query}\n")

    if args.emdb_source == "client":
        client = EMDB()
        df = robust_csv_search(
            client, query, args.fields,
            retries=5, delay=5, verbose=args.verbose
        )
    else:
        df = robust_http_csv_search(
            query, args.fields,
            retries=5, delay=5, verbose=args.verbose,
            debug_query=args.debug_query,
            emdb_api_base=args.emdb_api_base
        )

    print(f"[EMDB] Retrieved {len(df)} entries.\n")
    all_citation_counts = {}
    coverage_stats = None

    # -----------------------------
    # Single-rule annotation
    # -----------------------------
    if args.epmc:
        if not args.string:
            print("ERROR: --string is required with --epmc")
            sys.exit(1)

        annotation_group = args.annotation_group
        annotation_value  = "Y"
        sections = clean_terms(args.section)
        or_terms = clean_terms(args.string)
        and_terms = []

        tag = f"{annotation_group}|{annotation_value}"

        epmc_query = build_epmc_query_with_and_or(
            sections, or_terms, and_terms,
            debug=args.debug_query,
            tag=tag
        )

        cached = load_minimal_cache(annotation_group, annotation_value)

        if cached.get("query") == epmc_query and (not args.citations or "citation_counts" in cached):
            doi_hits = cached["results"]
            citation_counts = cached.get("citation_counts", {})
            if args.verbose:
                print(f"[EPMC:{tag}] Using cached results")
        else:
            doi_hits, full_pages, citation_counts = epmc_cursor_paged_query(
                epmc_query, annotation_group, annotation_value,
                args.debug_query,
                args.full_cache, args.verbose
            )

            save_minimal_cache(annotation_group, annotation_value, {
                "query": epmc_query,
                "results": doi_hits,
                "citation_counts": citation_counts,
            }, args.verbose)

            if args.full_cache:
                save_full_cache(annotation_group, annotation_value, {
                    "query": epmc_query,
                    "pages": full_pages
                }, args.verbose)

        all_citation_counts.update(citation_counts)
        df = annotate_with_multiple_matches(
            df, doi_hits, "xref_DOI",
            annotation_group, annotation_value,
            args.citations, citation_counts,
            args.verbose
        )

    # -----------------------------
    # Multi-rule annotation
    # -----------------------------
    elif args.epmc_query_csv:
        df, all_citation_counts = run_multi_epmc_annotations(
            df, args.epmc_query_csv,
            args.full_cache,
            args.verbose,
            args.debug_query,
            args.citations
        )

    if args.citations or args.h_index:
        if "xref_DOI" not in df.columns:
            raise ValueError(
                "--citations/--h-index requires an xref_DOI column. The script should "
                "auto-add it to --fields; check the EMDB response includes xref_DOI."
            )
        all_citation_counts = epmc_fetch_citation_counts_for_dois(
            collect_unique_dois(df["xref_DOI"]),
            all_citation_counts,
            args.debug_query,
            args.verbose,
        )

    if args.citations:
        df[CITED_BY_COUNT_COLUMN] = [
            row_cited_by_count(raw, all_citation_counts)
            for raw in df["xref_DOI"]
        ]

    if args.coverage:
        if "xref_DOI" not in df.columns:
            raise ValueError(
                "--coverage requires an xref_DOI column. The script should "
                "auto-add it to --fields; check the EMDB response includes xref_DOI."
            )
        coverage_stats = epmc_fetch_fulltext_coverage_for_dois(
            collect_unique_dois(df["xref_DOI"]),
            args.debug_query,
            args.verbose,
        )
        df[EPMC_FULLTEXT_AVAILABLE_COLUMN] = [
            row_fulltext_availability(
                raw,
                coverage_stats["has_fulltext"],
                coverage_stats["no_fulltext"],
            )
            for raw in df["xref_DOI"]
        ]

        if CITED_BY_COUNT_COLUMN in df.columns:
            columns = list(df.columns)
            columns.remove(EPMC_FULLTEXT_AVAILABLE_COLUMN)
            insert_at = columns.index(CITED_BY_COUNT_COLUMN) + 1
            columns.insert(insert_at, EPMC_FULLTEXT_AVAILABLE_COLUMN)
            df = df[columns]

    # -----------------------------
    # Output CSV
    # -----------------------------
    if args.print_df:
        print(df.to_string(index=False))

    df.to_csv(output_csv, index=False)
    print(f"[Output] Saved CSV: {output_csv}")

    # -----------------------------
    # Summary
    # -----------------------------
    if args.summary:
        print("\n[Summary Report]\n")
        summary_lines = [
            "Analysis description:",
            build_analysis_description(),
            "",
        ]
        summary_rows = []

        # Command used
        script_path = os.path.abspath(sys.argv[0])
        command_args = [os.path.basename(sys.argv[0]), *sys.argv[1:]]
        cmd = " ".join(format_command_arg(arg) for arg in command_args)
        summary_lines.append("Script path:")
        summary_lines.append(f"  {script_path}\n")
        summary_lines.append("Command used:")
        summary_lines.append(f"  {cmd}\n")

        print("Script path:")
        print(f"  {script_path}\n")
        print("Command used:")
        print(f"  {cmd}\n")

        emdb_search_query_url = build_emdb_search_query_url(query)
        summary_lines.append("Search query URL:")
        summary_lines.append(f"  {emdb_search_query_url}\n")

        print("Search query URL:")
        print(f"  {emdb_search_query_url}\n")

        total = len(df)
        annotation_cols = get_annotation_summary_columns(df, args)
        if (args.publications or args.citations or args.h_index or args.coverage) and "xref_DOI" not in df.columns:
            raise ValueError(
                "--publications/--citations/--h-index/--coverage requires an xref_DOI column. The script should "
                "auto-add it to --fields; check the EMDB response includes xref_DOI."
            )
        total_publications = (
            count_unique_dois(df["xref_DOI"]) if args.publications else None
        )
        if coverage_stats:
            searchable_dois = coverage_stats["has_fulltext"]
            unsearchable_dois = coverage_stats["no_fulltext"]
            unknown_dois = coverage_stats.get("unknown", set())
            coverage_total = coverage_stats["total_dois"]
            coverage_searchable = len(searchable_dois)
            coverage_unsearchable = len(unsearchable_dois)
            coverage_unknown = len(unknown_dois)
            coverage_percent = (
                coverage_searchable / coverage_total * 100
                if coverage_total else 0.0
            )
            coverage_lines = [
                "Europe PMC full-text coverage:",
                f"  Unique DOI-linked publications: {coverage_total}",
                f"  Searchable in Europe PMC full text (HAS_FT:y): {coverage_searchable}",
                f"  Not searchable in Europe PMC full text: {coverage_unsearchable}",
                f"  Full-text coverage unknown/not resolved: {coverage_unknown}",
                f"  DOI full-text coverage: {coverage_percent:.2f}%",
                (
                    "  Interpretation: section-based annotations are lower-bound "
                    f"counts because up to {coverage_unsearchable} DOI-linked "
                    "publication(s) could not be searched for the configured "
                    "full-text section strings."
                ),
                "",
            ]
            for line in coverage_lines:
                print(line)
                summary_lines.append(line)
        else:
            searchable_dois = set()
            unsearchable_dois = set()
            unknown_dois = set()
            coverage_total = coverage_searchable = coverage_unsearchable = coverage_unknown = 0
            coverage_percent = 0.0

        for col in annotation_cols:
            series = df[col].fillna("").astype(str)
            annotated_mask = series.str.strip().ne("")
            n_annotated = annotated_mask.sum()
            annotated_publications = (
                count_unique_dois(df.loc[annotated_mask, "xref_DOI"])
                if args.publications else None
            )

            print(f"Column: {col}")
            print(f"  Annotated entries: {n_annotated} / {total}")

            summary_lines.append(f"Column: {col}")
            summary_lines.append(f"  Annotated entries: {n_annotated} / {total}")

            value_counts = (
                series[annotated_mask]
                .value_counts()
                .sort_index()
            )

            if value_counts.empty:
                print("  Unique values: (none)\n")
                summary_lines.append("  Unique values: (none)\n")
                continue

            print("  Unique values:")
            summary_lines.append("  Unique values:")
            for value, cnt in value_counts.items():
                print(f"     - {value} ({cnt})")
                summary_lines.append(f"     - {value} ({cnt})")
                value_mask = series.eq(value)
                row = {
                    "annotation_group": col,
                    "annotation_value": value,
                    "entry_count": int(cnt),
                    "percentage": f"{(cnt / total * 100) if total else 0.0:.6f}",
                    "annotation_group_count": int(n_annotated),
                    "total_entries": int(total),
                }
                if args.publications:
                    row.update({
                        "publication_count": count_unique_dois(
                            df.loc[value_mask, "xref_DOI"]
                        ),
                        "annotated_publications": int(annotated_publications),
                        "total_publications": int(total_publications),
                    })
                if args.citations:
                    row["citedByCount"] = sum_unique_doi_citations(
                        df.loc[value_mask, "xref_DOI"],
                        all_citation_counts,
                    )
                if args.h_index:
                    row["hIndex"] = h_index_for_dois(
                        df.loc[value_mask, "xref_DOI"],
                        all_citation_counts,
                    )
                if args.coverage:
                    row_dois = set(collect_unique_dois(df.loc[value_mask, "xref_DOI"]))
                    row_total_dois = len(row_dois)
                    row_searchable_dois = row_dois.intersection(searchable_dois)
                    row_unsearchable_dois = row_dois.intersection(unsearchable_dois)
                    row_unknown_dois = row_dois.intersection(unknown_dois)
                    row_searchable_count = len(row_searchable_dois)
                    row_unsearchable_count = len(row_unsearchable_dois)
                    row_unknown_count = len(row_unknown_dois)
                    row_coverage_percent = (
                        row_searchable_count / row_total_dois * 100
                        if row_total_dois else 0.0
                    )
                    row.update({
                        "epmc_total_dois": int(row_total_dois),
                        "epmc_fulltext_searchable_dois": int(row_searchable_count),
                        "epmc_not_fulltext_searchable_dois": int(row_unsearchable_count),
                        "epmc_fulltext_unknown_dois": int(row_unknown_count),
                        "epmc_fulltext_coverage_percentage": f"{row_coverage_percent:.6f}",
                    })
                summary_rows.append(row)
            print()
            summary_lines.append("")

        with open(summary_txt, "w") as f:
            f.write("\n".join(summary_lines))

        pd.DataFrame(summary_rows).to_csv(summary_counts_csv, index=False)

        print(f"[Summary] Written to: {summary_txt}")
        print(f"[Summary] Written CSV counts: {summary_counts_csv}")


# ============================================================================
# Entrypoint
# ============================================================================
if __name__ == "__main__":
    main()
