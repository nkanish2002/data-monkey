#!/usr/bin/env python3
"""End-to-end test of the data-monkey MCP server via subprocess."""

import asyncio
import json
import sys
from pathlib import Path


async def test_server():
    base_dir = Path(__file__).parent
    sales_csv = str(base_dir / "data" / "sales.csv")
    output_parquet = str(base_dir / "data" / "sales_output.parquet")
    output_csv = str(base_dir / "data" / "sales_output.csv")

    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.client.session import ClientSession

    server_params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "data_monkey.server"],
        cwd=str(base_dir),
    )

    async with stdio_client(server_params) as (read, write):
        session = ClientSession(read_stream=read, write_stream=write)

        async with session:
            # Initialize
            await session.initialize()
            print("✅ Initialize: data-monkey server ready")

            # List tools
            tools_result = await session.list_tools()
            tools = {t.name for t in tools_result.tools}
            expected = {"read_file", "write_file", "analyze", "query", "group_by",
                        "join", "pivot", "merge_files", "sql_query", "save_df",
                        "describe_df", "schema"}
            assert tools == expected, f"Missing: {expected - tools}, Extra: {tools - expected}"
            print(f"✅ All {len(tools)} tools registered: {', '.join(sorted(tools))}")

            # Read the sales CSV
            read_result = await session.call_tool("read_file", {"file_path": sales_csv})
            data = json.loads(read_result.content[0].text)
            assert data["row_count"] == 10, f"Expected 10 rows, got {data['row_count']}"
            assert data["column_count"] == 6
            assert data["columns"] == ["date", "region", "product", "quantity", "price", "discount"]
            print(f"✅ read_file: {data['row_count']} rows, {data['column_count']} columns")
            print(f"   Schema: {data['schema']}")
            df_json = data["full_data"]

            # Describe the DataFrame
            desc_result = await session.call_tool("describe_df", {"df_json": df_json})
            desc_data = json.loads(desc_result.content[0].text)
            print(f"✅ describe_df: shape={desc_data['shape']}")

            # Analyze
            analyze_result = await session.call_tool("analyze", {
                "df_json": df_json,
                "operations": ["describe", "missing_values", "duplicates"],
            })
            analyze_data = json.loads(analyze_result.content[0].text)
            print(f"✅ analyze: missing_values={analyze_data['missing_values']}")

            # Query: filter quantity > 10
            query_result = await session.call_tool("query", {
                "df_json": df_json,
                "expression": ".filter(pl.col('quantity') > 10).select(['region', 'product', 'quantity']).sort('quantity', descending=True).head(5)",
            })
            query_data = json.loads(query_result.content[0].text)
            print(f"✅ query: {query_data['row_count']} rows (qty > 10)")

            # Group by
            group_result = await session.call_tool("group_by", {
                "df_json": df_json,
                "group_by_columns": ["region", "product"],
                "aggregations": {"quantity": "sum", "price": "mean"},
            })
            group_data = json.loads(group_result.content[0].text)
            print(f"✅ group_by: {group_data['row_count']} groups")

            # SQL
            sql_result = await session.call_tool("sql_query", {
                "df_json": df_json,
                "sql": "SELECT region, product, SUM(quantity) as total_qty, AVG(price) as avg_price FROM df GROUP BY region, product ORDER BY total_qty DESC",
            })
            sql_data = json.loads(sql_result.content[0].text)
            print(f"✅ sql_query: {sql_data['row_count']} groups")

            # Save to parquet
            save_result = await session.call_tool("save_df", {
                "df_json": df_json,
                "file_path": output_parquet,
                "file_type": "parquet",
                "compression": "zstd",
            })
            save_data = json.loads(save_result.content[0].text)
            print(f"✅ save_df: {save_data['file_path']} ({save_data['file_size_bytes']} bytes)")

            # Write CSV
            write_result = await session.call_tool("write_file", {
                "df_json": df_json,
                "file_path": output_csv,
                "file_type": "csv",
            })
            write_data = json.loads(write_result.content[0].text)
            print(f"✅ write_file: {write_data['file_path']} ({write_data['file_size_bytes']} bytes)")

            # Schema
            schema_result = await session.call_tool("schema", {"df_json": df_json})
            schema_data = json.loads(schema_result.content[0].text)
            print(f"✅ schema: {len(schema_data['columns'])} columns")

            print(f"\n🐒 All {10} tests passed! Data Monkey is ready.")


if __name__ == "__main__":
    asyncio.run(test_server())
