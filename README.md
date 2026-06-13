# Data Monkey 🐒

An [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server that exposes [Polars](https://pola.rs/) data operations as tools callable by any MCP client (Hermes Agent, Claude, Cursor, etc.).

Read, write, analyze, aggregate, transform, pivot, join, and run SQL on tabular data files — all through MCP tools.

## Features

| Tool | Description |
|------|-------------|
| `read_file` | Read CSV, Parquet, JSON, Excel, Avro, Feather, ORC into a DataFrame |
| `write_file` | Write a DataFrame to disk in any supported format |
| `analyze` | Exploratory data analysis: describe, missing values, duplicates, correlations, distributions |
| `query` | Execute Polars DSL expression chains on DataFrames |
| `group_by` | Group and aggregate with sum, mean, median, min, max, count, std, and more |
| `join` | Inner, left, right, anti, semi, and cross joins |
| `pivot` | Reshape from long to wide format |
| `merge_files` | Vertical (stack rows) or horizontal (side-by-side) merge of multiple files |
| `sql_query` | Run SQL queries directly on DataFrames via Polars' built-in SQL engine |
| `save_df` | Save a DataFrame to disk (alias for `write_file`) |
| `describe_df` | Quick DataFrame summary: shape, dtypes, null counts, memory usage |
| `schema` | Get column names and data types |

## Installation

### Using uvx (recommended)

```bash
# The package is installed in development mode — just use it:
uvx --from /path/to/data-monkey data-monkey
```

### Installing globally

```bash
uv tool install -e /path/to/data-monkey
data-monkey
```

### As an npm package

Not applicable — this is a Python MCP server.

## Configuration

### Hermes Agent

Add to `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  data-monkey:
    command: "uvx"
    args:
      - "--from"
      - "/path/to/data-monkey"
      - "data-monkey"
```

After restarting Hermes, tools become available as:
- `mcp_data_monkey_read_file`
- `mcp_data_monkey_analyze`
- `mcp_data_monkey_group_by`
- etc.

### Other MCP Clients

The server exposes stdio transport. Point your MCP client's config at:

```json
{
  "data-monkey": {
    "command": "uv",
    "args": ["run", "--from", "/path/to/data-monkey", "data-monkey"]
  }
}
```

## Usage Examples

### Read a CSV file

```json
{
  "name": "read_file",
  "arguments": {
    "file_path": "data/sales.csv"
  }
}
```

### Analyze a DataFrame

```json
{
  "name": "analyze",
  "arguments": {
    "df_json": "<JSON from read_file>",
    "operations": ["describe", "missing_values", "duplicates"]
  }
}
```

### Query with Polars DSL

```json
{
  "name": "query",
  "arguments": {
    "df_json": "<JSON from read_file>",
    "expression": ".filter(pl.col(\"quantity\") > 10).select([\"region\", \"product\", \"quantity\"]).sort(\"quantity\", descending=True)"
  }
}
```

### Group by with aggregation

```json
{
  "name": "group_by",
  "arguments": {
    "df_json": "<JSON from read_file>",
    "group_by_columns": ["region", "product"],
    "aggregations": {
      "quantity": "sum",
      "price": "mean"
    }
  }
}
```

### Run SQL on data

```json
{
  "name": "sql_query",
  "arguments": {
    "df_json": "<JSON from read_file>",
    "sql": "SELECT region, product, SUM(quantity) as total_qty, AVG(price) as avg_price FROM df GROUP BY region, product ORDER BY total_qty DESC"
  }
}
```

### Merge multiple files

```json
{
  "name": "merge_files",
  "arguments": {
    "file_paths": ["data/sales_q1.csv", "data/sales_q2.csv", "data/sales_q3.csv"],
    "file_type": "csv",
    "mode": "vertical"
  }
}
```

## Supported File Formats

**Read:** CSV, TSV, Parquet, JSON, NDJSON, Excel (.xlsx/.xls), Avro, Feather, ORC, Delta Lake
**Write:** CSV, Parquet, JSON, Excel, Feather, ORC

## Architecture

```
data-monkey/
├── PLAN.md                    # Design document
├── pyproject.toml             # Project config
├── README.md                  # This file
├── src/
│   └── data_monkey/
│       ├── __init__.py
│       └── server.py          # MCP server with all tool handlers
├── data/                      # Sample data for testing
│   └── sales.csv
└── .venv/                     # Virtual environment (from uv)
```

## Data Flow

1. `read_file` loads a file → returns JSON-serialized DataFrame + metadata
2. Other tools accept `df_json` (the JSON string) as input
3. Tools return JSON-serialized results
4. `write_file` / `save_df` writes results to disk

This round-trip through JSON is the MCP transport mechanism. For large DataFrames, use `n_rows` on `read_file` to sample, or save to Parquet for efficient storage.

## Development

```bash
# Install in dev mode
cd data-monkey
uv pip install -e .

# Run the server
uv run python -m data_monkey.server

# Test with a sample file
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"processId":null,"clientInfo":{"name":"test","version":"1.0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' | uv run python -m data_monkey.server
```

## License

MIT
