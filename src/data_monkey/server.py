"""Data Monkey — Polars MCP Server.

Exposes Polars data operations as MCP tools for read, write, analyze,
aggregate, transform, and SQL querying of tabular data files.
"""

import asyncio
import io
import json
from pathlib import Path
from typing import Any

import polars as pl
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAFE_DIRS = [Path(".").resolve()]  # cwd and below by default


def _is_safe(path: Path) -> bool:
    """Check that *path* is inside one of the allowed directories."""
    try:
        path.resolve().relative_to(SAFE_DIRS[0])
        return True
    except ValueError:
        return False


def _df_to_json(df: pl.DataFrame, sample: int = 100) -> dict[str, Any]:
    """Round-trip a DataFrame through JSON for MCP transport."""
    full_json = df.write_json()
    sample_df = df.head(sample)
    sample_json = sample_df.write_json()
    return {
        "full_data": full_json,
        "sample": sample_json,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": df.columns,
        "schema": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
        "memory_usage": df.estimated_size("bytes"),
    }


def _json_to_df(data: str) -> pl.DataFrame:
    """Deserialize a JSON string back to a DataFrame."""
    return pl.read_json(io.StringIO(data))


# ---------------------------------------------------------------------------
# File-type detection
# ---------------------------------------------------------------------------

def _detect_file_type(path: str) -> str:
    ext = Path(path).suffix.lower().lstrip(".")
    mapping = {
        "csv": "csv", "tsv": "csv", "tab": "csv",
        "parquet": "parquet", "pq": "parquet",
        "json": "json",
        "ndjson": "ndjson", "jsonl": "ndjson",
        "xlsx": "excel", "xls": "excel",
        "avro": "avro",
        "delta": "delta",
        "feather": "feather", "ftr": "feather",
        "orc": "orc",
    }
    return mapping.get(ext, "csv")


# ---------------------------------------------------------------------------
# Tool definitions using Tool.model_validate() for camelCase field support
# ---------------------------------------------------------------------------

TOOL_DEFS = [
    {
        "name": "read_file",
        "description": (
            "Read a data file (CSV, Parquet, JSON, Excel, Avro, Feather, ORC) into a "
            "Polars DataFrame. Returns schema, row count, and a sample of the data. "
            "Use the 'full_data' field to store the DataFrame for later operations."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Path to the data file to read."},
                "file_type": {
                    "type": "string",
                    "enum": ["csv", "parquet", "json", "ndjson", "excel", "avro", "feather", "orc", "delta"],
                    "description": "File format. Auto-detected from extension if omitted.",
                },
                "n_rows": {
                    "type": "integer",
                    "description": "Read only the first N rows (useful for sampling large files).",
                },
                "separator": {
                    "type": "string",
                    "description": "CSV column separator (default: ','). Ignored for non-CSV files.",
                },
                "try_parse_dates": {
                    "type": "boolean",
                    "description": "Attempt to parse date columns in CSV files.",
                },
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "write_file",
        "description": (
            "Write a Polars DataFrame (stored as JSON) to a data file on disk. "
            "Supports CSV, Parquet, JSON, Excel, Feather, and ORC formats."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame (from a previous tool call).",
                },
                "file_path": {"type": "string", "description": "Output file path."},
                "file_type": {
                    "type": "string",
                    "enum": ["csv", "parquet", "json", "excel", "feather", "orc"],
                    "description": "Output file format.",
                },
                "separator": {
                    "type": "string",
                    "description": "CSV output separator (default: ',').",
                },
                "compression": {
                    "type": "string",
                    "enum": ["lz4", "zstd", "snappy", "uncompressed"],
                    "description": "Parquet compression (default: 'zstd').",
                },
            },
            "required": ["df_json", "file_path", "file_type"],
        },
    },
    {
        "name": "analyze",
        "description": (
            "Run exploratory data analysis on a DataFrame. Returns descriptive statistics, "
            "missing value reports, duplicate counts, correlations, and value distributions."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "operations": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of analyses to run: 'describe', 'info', 'missing_values', "
                                   "'duplicates', 'correlations', 'distribution'.",
                },
            },
            "required": ["df_json", "operations"],
        },
    },
    {
        "name": "query",
        "description": (
            "Execute a Polars expression chain on a DataFrame. Pass a Polars DSL expression "
            "as a string. Use 'col()', 'filter()', 'select()', 'with_columns()', 'sort()', "
            "'drop()', 'rename()', 'head()', 'tail()' etc. Returns the result as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "expression": {
                    "type": "string",
                    "description": (
                        "Polars DSL expression to execute. Example: "
                        "'.filter(col(\"age\") > 25).select([\"name\", \"salary\"]).sort(\"salary\", descending=True).head(10)'"
                    ),
                },
            },
            "required": ["df_json", "expression"],
        },
    },
    {
        "name": "group_by",
        "description": (
            "Group a DataFrame by one or more columns and apply aggregations. "
            "Returns the aggregated result as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "group_by_columns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Column names to group by.",
                },
                "aggregations": {
                    "type": "object",
                    "description": (
                        "Dict mapping column name to aggregation function. "
                        "Supported: 'sum', 'mean', 'median', 'min', 'max', 'count', "
                        "'std', 'first', 'last', 'n_unique', 'approx_n_unique', "
                        "'first_n(n)', 'last_n(n)'."
                    ),
                },
            },
            "required": ["df_json", "group_by_columns", "aggregations"],
        },
    },
    {
        "name": "join",
        "description": (
            "Join two DataFrames on specified columns. Supports inner, left, right, "
            "anti, semi, and cross joins. Returns the joined DataFrame as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json_1": {
                    "type": "string",
                    "description": "JSON-serialized left DataFrame.",
                },
                "df_json_2": {
                    "type": "string",
                    "description": "JSON-serialized right DataFrame.",
                },
                "left_on": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Column name(s) in the left DataFrame.",
                },
                "right_on": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Column name(s) in the right DataFrame.",
                },
                "join_type": {
                    "type": "string",
                    "enum": ["inner", "left", "right", "anti", "semi", "cross"],
                    "description": "Join type.",
                },
            },
            "required": ["df_json_1", "df_json_2", "left_on", "right_on", "join_type"],
        },
    },
    {
        "name": "pivot",
        "description": (
            "Pivot a DataFrame: reshape from long to wide format. Returns the pivoted "
            "DataFrame as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "index": {
                    "type": "string",
                    "description": "Column to use as row index.",
                },
                "columns": {
                    "type": "string",
                    "description": "Column whose unique values become new column headers.",
                },
                "values": {
                    "type": "string",
                    "description": "Column whose values fill the pivot table.",
                },
                "aggregate_function": {
                    "type": "string",
                    "enum": ["sum", "mean", "min", "max", "first", "last", "count"],
                    "description": "Aggregation for duplicate index/column combinations.",
                },
            },
            "required": ["df_json", "index", "columns", "values", "aggregate_function"],
        },
    },
    {
        "name": "merge_files",
        "description": (
            "Concatenate or merge multiple data files. 'vertical' mode stacks rows "
            "(append datasets). 'horizontal' mode concatenates columns side-by-side."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_paths": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of file paths to merge.",
                },
                "file_type": {
                    "type": "string",
                    "enum": ["csv", "parquet", "json", "ndjson", "excel", "avro", "feather", "orc"],
                    "description": "File format.",
                },
                "mode": {
                    "type": "string",
                    "enum": ["vertical", "horizontal"],
                    "description": "Merge mode: 'vertical' (stack rows) or 'horizontal' (side-by-side).",
                },
            },
            "required": ["file_paths", "file_type", "mode"],
        },
    },
    {
        "name": "sql_query",
        "description": (
            "Run a SQL query directly on a Polars DataFrame using Polars' built-in SQL engine. "
            "Supports SELECT, WHERE, GROUP BY, ORDER BY, JOIN, aggregates, window functions, "
            "CTEs, and subqueries. Returns the result as JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "sql": {
                    "type": "string",
                    "description": "SQL query string. Use 'df' as the table name.",
                },
            },
            "required": ["df_json", "sql"],
        },
    },
    {
        "name": "save_df",
        "description": (
            "Save a DataFrame (stored as JSON) to disk in any supported format. "
            "Similar to write_file but explicitly named for the 'save' pattern."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
                "file_path": {"type": "string", "description": "Output file path."},
                "file_type": {
                    "type": "string",
                    "enum": ["csv", "parquet", "json", "excel", "feather", "orc"],
                    "description": "Output file format.",
                },
                "separator": {
                    "type": "string",
                    "description": "CSV output separator (default: ',').",
                },
                "compression": {
                    "type": "string",
                    "enum": ["lz4", "zstd", "snappy", "uncompressed"],
                    "description": "Parquet compression (default: 'zstd').",
                },
            },
            "required": ["df_json", "file_path", "file_type"],
        },
    },
    {
        "name": "describe_df",
        "description": (
            "Get a quick summary of a DataFrame: shape, dtypes, null counts, memory usage, "
            "and a sample of the data. Faster than a full analysis."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
            },
            "required": ["df_json"],
        },
    },
    {
        "name": "schema",
        "description": (
            "Return the schema of a DataFrame: column names and their data types. "
            "Useful for understanding structure before running queries."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "df_json": {
                    "type": "string",
                    "description": "JSON-serialized Polars DataFrame.",
                },
            },
            "required": ["df_json"],
        },
    },
]

# Instantiate Tool objects from the dict definitions
TOOLS: list[Tool] = [Tool.model_validate(defn) for defn in TOOL_DEFS]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

async def handle_read_file(arguments: dict[str, Any]) -> list[TextContent]:
    path = Path(arguments["file_path"])
    if not _is_safe(path):
        return [TextContent(type="text", text=f"ERROR: Access denied — {path} is outside safe directories")]

    if not path.exists():
        return [TextContent(type="text", text=f"ERROR: File not found: {path}")]

    file_type = arguments.get("file_type") or _detect_file_type(str(path))
    n_rows = arguments.get("n_rows")
    sep = arguments.get("separator")
    try_dates = arguments.get("try_parse_dates", False)

    try:
        if file_type == "csv":
            kwargs: dict[str, Any] = {}
            if sep:
                kwargs["separator"] = sep
            if n_rows:
                kwargs["n_rows"] = n_rows
            if try_dates:
                kwargs["try_parse_dates"] = True
            df = pl.read_csv(str(path), **kwargs)
        elif file_type == "parquet":
            df = pl.read_parquet(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "json":
            df = pl.read_json(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "ndjson":
            df = pl.read_ndjson(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "excel":
            df = pl.read_excel(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "avro":
            df = pl.read_avro(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "feather":
            df = pl.read_feather(str(path))
            if n_rows:
                df = df.head(n_rows)
        elif file_type == "orc":
            df = pl.read_orc(str(path))
            if n_rows:
                df = df.head(n_rows)
        else:
            return [TextContent(type="text", text=f"ERROR: Unsupported file type: {file_type}")]

        result = _df_to_json(df)
        result["file_path"] = str(path.resolve())
        result["file_size_bytes"] = path.stat().st_size
        return [TextContent(type="text", text=json.dumps(result))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR reading {path}: {e}")]


async def handle_write_file(arguments: dict[str, Any]) -> list[TextContent]:
    try:
        df = _json_to_df(arguments["df_json"])
    except Exception as e:
        return [TextContent(type="text", text=f"ERROR deserializing DataFrame: {e}")]

    file_path = Path(arguments["file_path"])
    if not _is_safe(file_path):
        return [TextContent(type="text", text=f"ERROR: Access denied — {file_path} is outside safe directories")]

    file_type = arguments["file_type"]
    kwargs: dict[str, Any] = {}

    if file_type == "csv":
        kwargs["separator"] = arguments.get("separator", ",")
    elif file_type == "parquet":
        kwargs["compression"] = arguments.get("compression", "zstd")

    try:
        if file_type == "csv":
            df.write_csv(str(file_path), **kwargs)
        elif file_type == "parquet":
            df.write_parquet(str(file_path), **kwargs)
        elif file_type == "json":
            df.write_json(str(file_path))
        elif file_type == "excel":
            df.write_excel(str(file_path))
        elif file_type == "feather":
            df.write_feather(str(file_path))
        elif file_type == "orc":
            df.write_orc(str(file_path))
        else:
            return [TextContent(type="text", text=f"ERROR: Unsupported output format: {file_type}")]

        return [TextContent(type="text", text=json.dumps({
            "status": "success",
            "file_path": str(file_path.resolve()),
            "row_count": len(df),
            "column_count": len(df.columns),
            "file_size_bytes": file_path.stat().st_size,
        }))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR writing {file_path}: {e}")]


async def handle_analyze(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])
    operations = arguments.get("operations", ["describe"])
    results: dict[str, Any] = {"row_count": len(df), "column_count": len(df.columns)}

    try:
        if "describe" in operations:
            desc = df.describe()
            results["describe"] = desc.write_json()

        if "info" in operations:
            results["info"] = {
                "schema": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
                "memory_bytes": df.estimated_size("bytes"),
            }

        if "missing_values" in operations:
            null_counts = df.null_count().row(0)
            results["missing_values"] = {
                col: {"count": int(null), "pct": round(null / len(df) * 100, 2)}
                for col, null in zip(df.columns, null_counts)
            }

        if "duplicates" in operations:
            dup_count = df.is_duplicated().sum()
            results["duplicates"] = {
                "count": int(dup_count),
                "pct": round(dup_count / len(df) * 100, 2) if len(df) > 0 else 0,
            }

        if "correlations" in operations:
            numeric_cols = df.select(pl.col(pl.NUMERIC_DTYPES)).columns
            if numeric_cols:
                corr = df.select([pl.col(c) for c in numeric_cols]).corr().write_json()
                results["correlations"] = corr

        if "distribution" in operations:
            dist = {}
            for col in df.columns:
                try:
                    vc = df.select(pl.col(col).value_counts(sort=True)).head(20)
                    dist[col] = vc.write_json()
                except Exception:
                    dist[col] = None
            results["distribution"] = dist

        return [TextContent(type="text", text=json.dumps(results, indent=2))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR during analysis: {e}")]


async def handle_query(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])
    expr_str = arguments["expression"]

    try:
        result = eval(f"df{expr_str}")
        if not isinstance(result, pl.DataFrame):
            return [TextContent(type="text", text=f"ERROR: Expression did not return a DataFrame. Got {type(result).__name__}")]
        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR evaluating query: {e}")]


async def handle_group_by(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])
    group_by_cols = arguments["group_by_columns"]
    aggs = arguments["aggregations"]

    try:
        agg_exprs = []
        for col, func in aggs.items():
            func = func.lower().strip()
            if func.startswith("first_n("):
                n = int(func.split("(")[1].split(")")[0])
                agg_exprs.append(pl.col(col).head(n).alias(f"{col}_{func}"))
            elif func.startswith("last_n("):
                n = int(func.split("(")[1].split(")")[0])
                agg_exprs.append(pl.col(col).tail(n).alias(f"{col}_{func}"))
            else:
                method = getattr(pl.col(col), func, None)
                if method is None:
                    return [TextContent(type="text", text=f"ERROR: Unknown aggregation function '{func}' for column '{col}'. Supported: sum, mean, median, min, max, count, std, first, last, n_unique, approx_n_unique")]
                agg_exprs.append(method().alias(f"{col}_{func}"))

        result = df.group_by(group_by_cols).agg(agg_exprs)
        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR in group_by: {e}")]


async def handle_join(arguments: dict[str, Any]) -> list[TextContent]:
    df1 = _json_to_df(arguments["df_json_1"])
    df2 = _json_to_df(arguments["df_json_2"])
    left_on = arguments["left_on"]
    right_on = arguments["right_on"]
    join_type = arguments["join_type"]

    try:
        result = df1.join(df2, left_on=left_on, right_on=right_on, how=join_type)
        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR in join: {e}")]


async def handle_pivot(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])
    index = arguments["index"]
    columns = arguments["columns"]
    values = arguments["values"]
    agg_func = arguments["aggregate_function"]

    try:
        # aggregate_function must be a string, not a pl.col() expression
        result = df.pivot(values=values, index=index, on=columns, aggregate_function=agg_func)
        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR in pivot: {e}")]


async def handle_merge_files(arguments: dict[str, Any]) -> list[TextContent]:
    file_paths = arguments["file_paths"]
    file_type = arguments["file_type"]
    mode = arguments["mode"]

    try:
        dfs = []
        for fp in file_paths:
            path = Path(fp)
            if not _is_safe(path):
                return [TextContent(type="text", text=f"ERROR: Access denied — {fp} is outside safe directories")]
            if not path.exists():
                return [TextContent(type="text", text=f"ERROR: File not found: {fp}")]

            if file_type == "csv":
                dfs.append(pl.read_csv(str(path)))
            elif file_type == "parquet":
                dfs.append(pl.read_parquet(str(path)))
            elif file_type == "json":
                dfs.append(pl.read_json(str(path)))
            elif file_type == "ndjson":
                dfs.append(pl.read_ndjson(str(path)))
            elif file_type == "excel":
                dfs.append(pl.read_excel(str(path)))
            elif file_type == "avro":
                dfs.append(pl.read_avro(str(path)))
            elif file_type == "feather":
                dfs.append(pl.read_feather(str(path)))
            elif file_type == "orc":
                dfs.append(pl.read_orc(str(path)))
            else:
                return [TextContent(type="text", text=f"ERROR: Unsupported file type: {file_type}")]

        if mode == "vertical":
            # Normalize schemas: union of all columns across all DataFrames
            all_cols: list[str] = []
            for d in dfs:
                for col in d.columns:
                    if col not in all_cols:
                        all_cols.append(col)

            # For each DataFrame, add missing columns as null and reorder
            normalized = []
            for d in dfs:
                d_norm = d
                for col in all_cols:
                    if col not in d_norm.columns:
                        d_norm = d_norm.with_columns(pl.lit(None).cast(pl.Unknown()).alias(col))
                d_norm = d_norm.select(all_cols)
                normalized.append(d_norm)

            result = pl.concat(normalized, how="vertical_relaxed")
        else:
            result = pl.concat(dfs, how="horizontal")

        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR merging files: {e}")]


async def handle_sql_query(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])
    sql = arguments["sql"]

    try:
        ctx = pl.SQLContext(eager=True)
        ctx.register("df", df)
        result = ctx.execute(sql)
        return [TextContent(type="text", text=json.dumps(_df_to_json(result)))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR in SQL query: {e}")]


async def handle_save_df(arguments: dict[str, Any]) -> list[TextContent]:
    """Alias for write_file — same implementation."""
    return await handle_write_file(arguments)


async def handle_describe_df(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])

    try:
        summary = {
            "shape": {"rows": len(df), "columns": len(df.columns)},
            "schema": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
            "null_counts": {col: int(df[col].null_count()) for col in df.columns},
            "memory_bytes": df.estimated_size("bytes"),
            "sample": df.head(10).write_json(),
        }
        return [TextContent(type="text", text=json.dumps(summary, indent=2))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR describing DataFrame: {e}")]


async def handle_schema(arguments: dict[str, Any]) -> list[TextContent]:
    df = _json_to_df(arguments["df_json"])

    try:
        schema_info = {
            "columns": df.columns,
            "dtypes": {c: str(t) for c, t in zip(df.columns, df.dtypes)},
            "column_count": len(df.columns),
            "row_count": len(df),
        }
        return [TextContent(type="text", text=json.dumps(schema_info, indent=2))]

    except Exception as e:
        return [TextContent(type="text", text=f"ERROR getting schema: {e}")]


# ---------------------------------------------------------------------------
# Tool dispatch table
# ---------------------------------------------------------------------------

HANDLERS = {
    "read_file": handle_read_file,
    "write_file": handle_write_file,
    "analyze": handle_analyze,
    "query": handle_query,
    "group_by": handle_group_by,
    "join": handle_join,
    "pivot": handle_pivot,
    "merge_files": handle_merge_files,
    "sql_query": handle_sql_query,
    "save_df": handle_save_df,
    "describe_df": handle_describe_df,
    "schema": handle_schema,
}


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

def create_app() -> Server:
    app = Server("data-monkey")

    @app.list_tools()
    async def list_tools() -> list[Tool]:
        return TOOLS

    @app.call_tool()
    async def call_tool(name: str, arguments: Any) -> list[TextContent]:
        handler = HANDLERS.get(name)
        if handler is None:
            return [TextContent(type="text", text=f"ERROR: Unknown tool '{name}'. Available: {', '.join(HANDLERS.keys())}")]

        if not isinstance(arguments, dict):
            arguments = dict(arguments) if hasattr(arguments, "items") else {}

        return await handler(arguments)

    return app


def main():
    """Entry point: run the MCP server over stdio."""
    app = create_app()

    async def serve():
        async with stdio_server() as (read_stream, write_stream):
            await app.run(
                read_stream=read_stream,
                write_stream=write_stream,
                initialization_options=app.create_initialization_options(),
            )

    asyncio.run(serve())


if __name__ == "__main__":
    main()
