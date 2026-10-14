"""Make a real read-only MCP call through the installed AI Dev Kit server."""

import asyncio
import json
import os
import subprocess
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parents[1]


async def main():
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_")}
    env["DATABRICKS_CONFIG_PROFILE"] = "personal-env"
    env["DATABRICKS_HOST"] = "https://dbc-ab9a5afe-c151.cloud.databricks.com"
    server = StdioServerParameters(
        command=str(ROOT / ".state/mcp-venv/bin/python"),
        args=[str(ROOT / ".state/ai-dev-kit/databricks-mcp-server/run_server.py")],
        env=env,
    )
    async with stdio_client(server) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        available = await session.list_tools()
        names = {tool.name for tool in available.tools}
        if "get_current_user" not in names:
            raise RuntimeError("AI Dev Kit did not expose get_current_user")
        result = await session.call_tool("get_current_user", arguments={})
        if result.isError:
            raise RuntimeError("MCP get_current_user failed; inspect local stderr")
        payload = getattr(result, "structuredContent", None)
        if not payload:
            payload = json.loads(next(c.text for c in result.content if c.type == "text"))
        if not payload.get("username"):
            raise RuntimeError("MCP returned no identity")
        report = {
            "profile": "personal-env",
            "host": env["DATABRICKS_HOST"],
            "tool_count": len(names),
            "called_tool": "get_current_user",
            "username": payload["username"],
            "success": True,
            "ai_dev_kit_commit": subprocess.check_output(
                ["git", "-C", str(ROOT / ".state/ai-dev-kit"), "rev-parse", "HEAD"], text=True
            ).strip(),
        }
        (ROOT / ".state/mcp-check.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
