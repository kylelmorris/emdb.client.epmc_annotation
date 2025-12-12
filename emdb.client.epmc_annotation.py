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
import sys
import time
import os
from typing import List, Dict, Tuple

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

def ensure_cache_dir():
    if not os.path.isdir(CACHE_DIR):
        os.makedirs(CACHE_DIR)


# ============================================================================
# Cache Helpers
# ============================================================================
def _make_cache_key(annotation_column: str, annotation_value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", f"{annotation_column}_{annotation_value}")

def minimal_cache_path(annotation_column: str, annotation_value: str) -> str:
    return os.path.join(CACHE_DIR, f"cache_{_make_cache_key(annotation_column, annotation_value)}.json")

def full_cache_path(annotation_column: str, annotation_value: str) -> str:
    return os.path.join(CACHE_DIR, f"cache_{_make_cache_key(annotation_column, annotation_value)}_full.json")

def load_minimal_cache(annotation_column: str, annotation_value: str) -> Dict:
    try:
        with open(minimal_cache_path(annotation_column, annotation_value), "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_minimal_cache(annotation_column: str, annotation_value: str, data: Dict, verbose: bool):
    path = minimal_cache_path(annotation_column, annotation_value)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if verbose:
        print(f"[CACHE] Saved minimal cache: {path}")

def save_full_cache(annotation_column: str, annotation_value: str, data: Dict, verbose: bool):
    path = full_cache_path(annotation_column, annotation_value)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    if verbose:
        print(f"[CACHE] Saved full cache: {path}")


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


# ============================================================================
# Europe PMC Query Builder (with parentheses fix)
# ============================================================================
def build_epmc_query_with_and_or(sections: List[str],
                                 or_terms: List[str],
                                 and_terms: List[str],
                                 debug=False,
                                 tag="") -> str:
    """
    Correct AND/OR logic + parentheses for AND-block.

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
                            annotation_column: str,
                            annotation_value: str,
                            full_cache: bool,
                            verbose: bool) -> Tuple[List[str], Dict]:

    SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    cursor = "*"
    dois = []
    full_pages = {}
    tag = f"{annotation_column}|{annotation_value}"

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

        if full_cache:
            full_pages[cursor] = data

        next_cursor = data.get("nextCursorMark")
        pbar.update(1)

        if not next_cursor or next_cursor == cursor:
            break

        cursor = next_cursor
        time.sleep(0.05)

    pbar.close()
    return sorted(set(dois)), full_pages


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


# ============================================================================
# Annotation Logic
# ============================================================================
def annotate_with_multiple_matches(df: pd.DataFrame,
                                   doi_hits: List[str],
                                   doi_col: str,
                                   annotation_column: str,
                                   annotation_value: str,
                                   verbose: bool) -> pd.DataFrame:

    doi_set = set(doi_hits)
    tag = f"{annotation_column}|{annotation_value}"
    print(f"[EPMC:{tag}] Annotating...")

    if annotation_column not in df.columns:
        df[annotation_column] = ""

    colvals = df[annotation_column].tolist()

    for idx, raw in enumerate(tqdm(df[doi_col], desc=f"Annot {annotation_column}", unit="entry")):
        for d in extract_dois(raw):
            if d in doi_set:
                colvals[idx] = (
                    annotation_value
                    if colvals[idx] == ""
                    else f"{colvals[idx]}; {annotation_value}"
                )
                break

    df[annotation_column] = colvals
    return df


# ============================================================================
# Multi-rule Annotation
# ============================================================================
def run_multi_epmc_annotations(df: pd.DataFrame,
                               csv_path: str,
                               full_cache: bool,
                               verbose: bool,
                               debug_query: bool) -> pd.DataFrame:

    print(f"[EPMC] Loading annotation CSV: {csv_path}")
    rules = pd.read_csv(csv_path)

    required = {"annotation_column", "annotation", "section", "string_OR", "string_AND"}
    if not required.issubset(rules.columns):
        raise ValueError(f"CSV must contain columns: {required}")

    for idx, row in rules.iterrows():

        annotation_column = row["annotation_column"]
        annotation_value  = row["annotation"]
        sections  = clean_terms(row["section"])
        or_terms  = clean_terms(row["string_OR"])
        and_terms = clean_terms(row["string_AND"])

        tag = f"{annotation_column}|{annotation_value}"

        print(f"\n[EPMC Rule {idx+1}/{len(rules)}] {tag}")

        epmc_query = build_epmc_query_with_and_or(
            sections, or_terms, and_terms,
            debug=debug_query,
            tag=tag
        )

        cached = load_minimal_cache(annotation_column, annotation_value)

        if cached.get("query") == epmc_query:
            doi_hits = cached["results"]
            if verbose:
                print(f"[EPMC:{tag}] Using cached results")
        else:
            doi_hits, full_pages = epmc_cursor_paged_query(
                epmc_query, annotation_column, annotation_value,
                full_cache, verbose
            )

            save_minimal_cache(annotation_column, annotation_value, {
                "query": epmc_query,
                "results": doi_hits,
                "annotation_column": annotation_column,
                "annotation_value": annotation_value,
                "sections": sections,
                "or_terms": or_terms,
                "and_terms": and_terms,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            }, verbose)

            if full_cache:
                save_full_cache(annotation_column, annotation_value, {
                    "query": epmc_query,
                    "pages": full_pages,
                }, verbose)

        df = annotate_with_multiple_matches(
            df, doi_hits, "xref_DOI",
            annotation_column, annotation_value,
            verbose
        )

    return df


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
    parser.add_argument("--summary", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug-query", action="store_true",
                        help="Print full query breakdown before executing.")

    parser.add_argument("--epmc", action="store_true")
    parser.add_argument("--section", type=str, default="METHODS")
    parser.add_argument("--string", type=str)
    parser.add_argument("--annotation_column", type=str, default="EPMC_ANNOTATION")

    parser.add_argument("--epmc_query_csv", type=str)
    parser.add_argument("--full_cache", action="store_true")

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

    if args.verbose:
        print("[Arguments]")
        for k, v in vars(args).items():
            print(f"  {k}: {v}")
        print()

    # -----------------------------
    # Build EMDB query
    # -----------------------------
    query = build_query(args.method, args.status, args.where)

    if args.debug_query:
        print(f"[DEBUG EMDB QUERY] {query}\n")

    client = EMDB()

    df = robust_csv_search(
        client, query, args.fields,
        retries=5, delay=5, verbose=args.verbose
    )

    print(f"[EMDB] Retrieved {len(df)} entries.\n")

    # -----------------------------
    # Single-rule annotation
    # -----------------------------
    if args.epmc:
        if not args.string:
            print("ERROR: --string is required with --epmc")
            sys.exit(1)

        annotation_column = args.annotation_column
        annotation_value  = "Y"
        sections = clean_terms(args.section)
        or_terms = clean_terms(args.string)
        and_terms = []

        tag = f"{annotation_column}|{annotation_value}"

        epmc_query = build_epmc_query_with_and_or(
            sections, or_terms, and_terms,
            debug=args.debug_query,
            tag=tag
        )

        cached = load_minimal_cache(annotation_column, annotation_value)

        if cached.get("query") == epmc_query:
            doi_hits = cached["results"]
            if args.verbose:
                print(f"[EPMC:{tag}] Using cached results")
        else:
            doi_hits, full_pages = epmc_cursor_paged_query(
                epmc_query, annotation_column, annotation_value,
                args.full_cache, args.verbose
            )

            save_minimal_cache(annotation_column, annotation_value, {
                "query": epmc_query,
                "results": doi_hits,
            }, args.verbose)

            if args.full_cache:
                save_full_cache(annotation_column, annotation_value, {
                    "query": epmc_query,
                    "pages": full_pages
                }, args.verbose)

        df = annotate_with_multiple_matches(
            df, doi_hits, "xref_DOI",
            annotation_column, annotation_value,
            args.verbose
        )

    # -----------------------------
    # Multi-rule annotation
    # -----------------------------
    elif args.epmc_query_csv:
        df = run_multi_epmc_annotations(
            df, args.epmc_query_csv,
            args.full_cache,
            args.verbose,
            args.debug_query
        )

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
        summary_lines = []

        # Command used
        cmd = " ".join(sys.argv)
        summary_lines.append("Command used:")
        summary_lines.append(f"  {cmd}\n")

        print("Command used:")
        print(f"  {cmd}\n")

        total = len(df)
        ignore = {"emdb_id", "xref_DOI"}

        annotation_cols = [
            col for col in df.columns
            if col not in ignore and df[col].dtype == object
        ]

        for col in annotation_cols:
            series = df[col].fillna("").astype(str)
            annotated_mask = series.str.strip().ne("")
            n_annotated = annotated_mask.sum()

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
            print()
            summary_lines.append("")

        with open(summary_txt, "w") as f:
            f.write("\n".join(summary_lines))

        print(f"[Summary] Written to: {summary_txt}")


# ============================================================================
# Entrypoint
# ============================================================================
if __name__ == "__main__":
    main()
