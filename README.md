Readme

# EMDB Europe PMC Annotation Search

This script queries the **Electron Microscopy Data Bank (EMDB)** and labels EMDB entries with annotations identified by associated publication key phrase matching via the **Europe PMC (EPMC)** API.

It is designed to support **large-scale, rule-based annotation** of EMDB entries using flexible key phrase matching rules defined either on the command line or via a CSV file.

Typical use cases include:

- identifying entries associated with specific **facilities**, **instrumentation**, or **methods**
- labelling entries based on **key phrase mentions in METHODS / SUPPLEMENTARY / ACKNOWLEDGEMENTS**
- fast archive-wide annotation prototyping, analyses and reporting

---

## Features

- Query EMDB using **Solr-style filters**
- Choose EMDB source:
  - Python client (`--emdb_source client`, default)
  - direct EMDB HTTP API (`--emdb_source http`)
- Retrieve EMDB entries as DataFrame and CSV

- Query Europe PMC for entry labeling:
  - simple keyword rules (`--epmc`)
  - complex multi-rule CSV definitions (`--epmc_query_csv`)

- Caching of Europe PMC results for fast requery
- Summary report generation
---

## Requirements

- Python â¥ 3.10
- `emdb` Python client
- `pandas`
- `requests`
- `tqdm`

Install dependencies (example):

```bash
pip install emdb pandas requests tqdm
```

---

## Basic Usage

### Query EMDB only

```bash
./emdb.client.epmc_annotation_search.py \
  --method '*' \
  --status REL \
  --fields emdb_id,xref_DOI \
  --output emdb_results
```

This will produce:

```
emdb_results.csv
```

### Query EMDB using direct HTTP endpoint (without `EMDB().csv_search()`)

```bash
./emdb.client.epmc_annotation_search.py \
  --emdb_source http \
  --method '*' \
  --status REL \
  --fields emdb_id,xref_DOI,current_status,institution_name,country_name \
  --output emdb_results_http
```

Optional:

- `--emdb_api_base https://www.ebi.ac.uk/emdb/api` (defaults to this)

---

## Query EMDB and label entries by Europe PMC Annotation (Single Rule)

Annotate entries where publications mention a term in a specific section:

```bash
./emdb.client.epmc_annotation_search.py \
  --epmc \
  --section METHODS \
  --string "cryoFIB,focused ion beam" \
  --annotation_column CryoFIB \
  --output emdb_cryoFIB \
  --summary
```

---

## Query EMDB and label entries by Multi-Rule Annotation via CSV (Recommended)

```bash
./emdb.client.epmc_annotation_search.py \
  --epmc_query_csv search_table/epmc_annotation_search.csv \
  --full_cache \
  --fields emdb_id,current_status,structure_determination_method,institution_name,country_name,xref_DOI
  --output outputs/emdb_epmc_annotations \
  --summary
```

To label entries with multiple annotations based on key phrase matching in publications, search for more than one key phrase at the same time by providing a formatted csv as follows:

Example CSV:

```example.csv
annotation_column,annotation,section,string_OR,string_AND
FIB-SEM,Yes,METHODS,SUPPL,"FIB-SEM,cryoFIB",""
Facility,eBIC,METHODS,SUPPL,ACK_FUND,"electron Bio-Imaging Centre,eBIC","Diamond"
Software,modelangelo,METHODS,”modelAngelo, model angelo”
```

These two rows will generate two independent EPMC search queries, returning DOI's for entry matching and annotation:

Query row 1: METHODS:"electron Bio-Imaging Centre" OR METHODS:"eBIC" OR ACK_FUND:"electron Bio-Imaging Centre" OR ACK_FUND:"eBIC" AND (METHODS:"Diamond" OR ACK_FUND:"Diamond")
Query row 2: METHODS:"modelAngelo" OR METHODS:"model angelo"

Note the sections of a publication that are searched (i.e. METHODS, SUPPL, FUND_ACK) should follow the EPMC search syntax described here: https://europepmc.org/searchsyntax

---

## Output

- `<basename>.csv`
- `<basename>_summary.txt` (if `--summary` is used)

---

## License and usage

Apache license

Use should follow the fair usage of:
- EMDB Python client
- Europe PMC API usage policies
