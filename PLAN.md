# Data Monkey — Polars MCP Server

## Overview

A Model Context Protocol (MCP) server that exposes Polars data operations as tools callable by any MCP client (Hermes Agent, Claude, Cursor, etc.). The name "data monkey" reflects the playful act of manipulating and wrangling data at scale.

**Core philosophy:** Give the LLM agent the full power of Polars — read, write, analyze, aggregate, transform — without needing to run Python scripts manually.

---

## Tool Design

### 1. `read_file` — Read a data file into a Polars DataFrame

**Inputs:** `file_path` (str), `file_type` (str, optional — auto-detect if omitted)

**Supported formats:** CSV, Parquet, JSON, Excel (.xlsx/.xls), NDJSON, Avro, Delta Lake

**Returns:** JSON representation of the DataFrame with metadata (schema, row count, column names)

**Details:**
- Auto-detect file type from extension
- For CSV: expose optional params (`separator`, `has_header`, `null_values`, `try_parse_dates`)
- For large files: support `n_rows` (sample) parameter to avoid OOM
- Return schema info alongside sample data so the agent understands column types

---

### 2. `write_file` — Write a Polars DataFrame to disk

**Inputs:** `df_json` (str — JSON-serialized DataFrame), `file_path` (str), `file_type` (str — csv/parquet/json/excel)

**Returns:** Confirmation with file size and row count

**Details:**
- Supports all major output formats
- CSV: expose `separator`, `include_header` options
- Parquet: expose `compression` option (lz4, zstd, snappy, uncompressed)

---

### 3. `analyze` — Exploratory Data Analysis

**Inputs:** `df_json` (str), `operations` (list of str)

**Supported operations:** `describe`, `info`, `missing_values`, `duplicates`, `correlations`, `distribution`

**Returns:** Structured analysis summary

**Details:**
- `describe`: descriptive statistics (mean, std, min, max, quartiles)
- `info`: column types, null counts, memory usage
- `missing_values`: null count per column with percentage
- `duplicates`: count and sample of duplicate rows
- `correlations`: Pearson/Spearman correlations for numeric columns
- `distribution`: value counts for categorical columns, histograms for numeric

---

### 4. `query` — Filter and transform data (SQL-like)

**Inputs:** `df_json` (str), `expression` (str)

**Returns:** Result as JSON

**Details:**
- Accept Polars DSL expressions in string form: `.filter(col("age") > 25).select(["name", "salary"])`
- Support full Polars expression syntax for maximum flexibility
- Return result as JSON with metadata

---

### 5. `group_by` — Aggregate/grouped operations

**Inputs:** `df_json` (str), `group_by_columns` (list of str), `aggregations` (dict of column → agg function)

**Returns:** Aggregated DataFrame as JSON

**Supported aggregations:** `sum`, `mean`, `median`, `min`, `max`, `count`, `std`, `first`, `last`, `n_unique`, `approx_n_unique`

**Details:**
- Multi-column group-by support
- Multiple aggregations per group
- Result returned as JSON

---

### 6. `join` — Merge DataFrames

**Inputs:** `df_json_1` (str), `df_json_2` (str), `left_on` (list of str), `right_on` (list of str), `join_type` (str: inner/left/right/anti/semi/cross)

**Returns:** Joined DataFrame as JSON

**Details:**
- Support all Polars join types
- Self-join support via same DataFrame

---

### 7. `pivot` — Reshape data

**Inputs:** `df_json` (str), `index` (str), `columns` (str), `values` (str), `aggregate_function` (str)

**Returns:** Pivoted DataFrame as JSON

---

### 8. `merge_files` — Concatenate files (accumulation)

**Inputs:** `file_paths` (list of str), `file_type` (str), `mode` (str: vertical/horizontal)

**Returns:** Merged DataFrame as JSON with metadata

**Details:**
- `vertical`: row-wise concatenation (stack datasets)
- `horizontal`: column-wise concatenation (merge side by side)

---

### 9. `sql_query` — Run SQL on a DataFrame

**Inputs:** `df_json` (str), `sql` (str)

**Returns:** SQL query result as JSON

**Details:**
- Polars has a built-in SQL engine — leverage it for familiarity
- Full SQL support: SELECT, WHERE, GROUP BY, JOIN, ORDER BY, aggregates, window functions

---

### 10. `save_df` — Save a DataFrame from memory to disk

**Inputs:** `df_json` (str), `file_path` (str), `file_type` (str)

**Returns:** File path and metadata

---

## Architecture

```
data-monkey/
├── PLAN.md                    # This file
├── pyproject.toml             # Project config + dependencies
├── src/
│   └── data_monkey/
│       ├── __init__.py
│       └── server.py          # MCP server entry point
├── tests/
│   ├── test_read.py
│   ├── test_analyze.py
│   ├── test_aggregate.py
│   └── test_join.py
├── data/                      # Sample data for testing
│   ├── sales.csv
│   ├── customers.parquet
│   └── ...
└── README.md
```

## Data Serialization

Since MCP tools exchange JSON, DataFrames are serialized to/from JSON using Polars' built-in JSON round-trip:

```python
# Serialize DataFrame to JSON string for MCP
df_json = df.write_json()

# Deserialize from JSON string to DataFrame
df = pl.read_json(io.StringIO(df_json))
```

For large DataFrames, this can be verbose. We'll use a compact format and return `row_count` metadata so the agent knows the scale.

## Security Considerations

1. **Path validation:** All file paths must be within a configured safe directory (e.g., `/workspace/data/` or user-specified allowlist)
2. **No shell execution:** Only Polars-native operations — no subprocess calls
3. **Size limits:** Enforce a maximum DataFrame size (e.g., 100K rows) to prevent OOM
4. **Format restrictions:** Only allow safe, parseable formats (no pickle/serializer files)

## Integration with Hermes Agent

After building, configure in `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  data-monkey:
    command: "uvx"
    args: ["--from", "data-monkey", "data_monkey.server:main"]
```

This makes all tools available as `mcp_data_monkey_read_file`, `mcp_data_monkey_analyze`, etc.

---

## Development Phases

| Phase | Scope | Status |
|-------|-------|--------|
| 1. Foundation | MCP boilerplate, read_file, write_file | ✅ Complete |
| 2. Analysis | analyze, describe, info, missing_values | ✅ Complete |
| 3. Transform | query, group_by, join, pivot | ✅ Complete |
| 4. SQL Engine | sql_query | ✅ Complete |
| 5. Accumulation | merge_files, save_df | ✅ Complete |
| 6. Polish | Error handling, path validation, docs, tests | ✅ Complete |
