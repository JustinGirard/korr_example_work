from __future__ import annotations
import asyncio
import os
import sys
from typing import Optional
raise Exception("Draft -- Justin was still setting up the MCP when he stopped")
from agents import Agent, run_demo_loop   # REPL helper
from agents.mcp.server import MCPServerStdio  # stdio transport for MCP servers
from csv_agent.agent.prompts import AGENT_INSTRUCTIONS

AGENT_INSTRUCTIONS = """\
You are a senior data analyst. A CSV is mounted behind MCP tools.

Use these tools to answer questions:
- csv_schema(): inspect columns and row count.
- csv_head(n): preview sample rows.
- sql(query, limit): run SQL against the table 'df' (the CSV).
- summary(): quick stats.

When aggregating/filtering/sorting, prefer sql().
Include brief reasoning and, when you use sql(), include the SQL you ran.
Keep outputs concise; cap large tables.
"""

from decelium_wallet.commands.BaseService import BaseService
class CsvAgentCLI(BaseService):
    """
    Spawns the MCP server as a subprocess via stdio and opens a terminal chat.
    """

    def __init__(self, csv_path: str, python_exe: Optional[str] = None) -> None:
        self.csv_path = csv_path
        self.python_exe = python_exe or sys.executable

        # Configure the stdio MCP server parameters (spawn our server module)
        self.server = MCPServerStdio(
            params={
                "command": self.python_exe,
                "args": ["csv_mcp.py", 
                         f"csv_path={self.csv_path}", ],
            },
            cache_tools_list=True,
            name="csv-server",
        )

        # Construct the agent; pass MCP servers so tools are auto-discovered
        self.agent = Agent(
            name="CSV Analyst",
            instructions=AGENT_INSTRUCTIONS,
            mcp_servers=[self.server],
            model_config={"model": "gpt-4o-mini"},
        )

    async def run(self) -> None:
        await run_demo_loop(self.agent)


async def _main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m csv_agent.agent.agent_cli <path/to/file.csv>")
        raise SystemExit(2)

    if not os.getenv("OPENAI_API_KEY"):
        print("ERROR: Set OPENAI_API_KEY first")
        raise SystemExit(2)

    csv_path = sys.argv[1]
    cli = CsvAgentCLI(csv_path=csv_path)
    await cli.run()


if __name__ == "__main__":
    asyncio.run(_main())



