from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List
import logging
import json
import duckdb
import pandas as pd
from nodejobs.dependencies.BaseService import BaseService


logger = logging.getLogger(__name__)
class CsvContext: 
    """
    Loads a CSV into memory and exposes simple analytics over DuckDB.
    - Keeps a DuckDB in-memory connection.
    - Registers the DataFrame as a view: 'df'.
    """
    csv_path: str
    limit_cap: int = 10_000

    def __init__(self,csv_path) -> None:
        self.csv_path = csv_path
        self.df: pd.DataFrame = pd.read_csv(self.csv_path)
        self.con: duckdb.DuckDBPyConnection = duckdb.connect(database=":memory:")
        # Register as a virtual table named 'df'
        self.con.register("df", self.df)
        logger.info("Loaded CSV '%s' with %d rows, %d cols",
                    self.csv_path, len(self.df), len(self.df.columns))

    # ---------- Query methods used by the MCP tools ----------

    def schema(self) ->str:
        return json.dumps({
            "columns": list(self.df.columns),
            "dtypes": {c: str(t) for c, t in self.df.dtypes.items()},
            "rows": int(len(self.df)),
        })

    def head(self, n: int = 5) -> str:
        n = max(1, min(int(n), self.limit_cap))
        return json.dumps(self.df.head(n).to_dict(orient="records"))

    def sql(self, query: str, limit: int = 200) -> str:
        """
        Run SQL against the registered 'df' table.
        We cap results to avoid flooding the agent/terminal.
        """
        limit = max(1, min(int(limit), self.limit_cap))
        wrapped = f"SELECT * FROM ({query}) AS sub LIMIT {limit}"
        logger.debug("Executing SQL: %s", wrapped)
        res = self.con.execute(wrapped).fetch_df()
        return json.dumps(res.to_dict(orient="records"))

    def summary(self) -> str:
        desc = (
            self.df.describe(include="all")
            .fillna("")
            .to_dict()
        )
        return json.dumps({"summary": desc})



class CsvProtocolService(BaseService):
    @classmethod
    def get_command_map(cls):
        return {
            "run_stdio": {"required_args": [], "method": cls.run_stdio},
            "run_http": {"required_args": [], "method": cls.run_http},
        }

    @classmethod
    def instance_mcp(cls, csv_path:str):

        from fastmcp import FastMCP
        mcp = FastMCP("csv-chat-service")

        context:CsvContext = CsvContext(csv_path)
        @mcp.tool
        def csv_schema() -> str:
            """Return column names, dtypes, and row count."""
            return context.schema()

        @mcp.tool
        def csv_head(n: int = 5) -> str:
            """Return the first n rows as JSON records."""
            rows = context.head(n=n)
            return rows

        @mcp.tool
        def sql(query: str, limit: int = 200) ->str:
            """
            Run a SQL query against the in-memory table 'df'.
            Example: SELECT col, AVG(val) FROM df GROUP BY col ORDER BY 2 DESC LIMIT 10;
            """
            return context.sql(query=query, limit=limit)

        @mcp.tool
        def summary() -> str:
            """Summary statistics for numeric/object columns."""
            return context.summary()
        return mcp

    @classmethod
    def run_stdio(cls, csv_path:str):
        mcp = cls.instance_mcp(csv_path)
        mcp.run()

    @classmethod
    def run_http(cls, csv_path:str, transport: str = "http", host: str = "127.0.0.1", port: int = 8765, path: str = "/mcp"):
        mcp = cls.instance_mcp(csv_path)
        print("server")
        mcp.run(transport=str(transport), host=str(host), port=int(port), path=str(path))


if __name__ == "__main__":
    CsvProtocolService.run_cli()
# python3 csv_mcp.py run_stdio - Spins up the MCP server as a process
# python3 csv_mcp.py run_http csv_path=./data/extract_2/sensors.csv