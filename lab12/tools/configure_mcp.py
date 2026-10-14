"""Register the local AI Dev Kit for Codex sessions started in lab12/."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
config = ROOT / ".codex/config.toml"
python = ROOT / ".state/mcp-venv/bin/python"
server = ROOT / ".state/ai-dev-kit/databricks-mcp-server/run_server.py"
if not python.exists() or not server.exists():
    raise SystemExit("Install the AI Dev Kit MCP server first")
block = (
    "[mcp_servers.databricks_lab12]\n"
    f"command = {json.dumps(str(python))}\n"
    f"args = [{json.dumps(str(server))}]\n"
    "startup_timeout_sec = 60\n"
    "tool_timeout_sec = 120\n"
    "[mcp_servers.databricks_lab12.env]\n"
    'DATABRICKS_CONFIG_PROFILE = "personal-env"\n'
    'DATABRICKS_HOST = "https://dbc-ab9a5afe-c151.cloud.databricks.com"\n'
)
if config.exists() and config.read_text() != block:
    raise SystemExit("Existing local Codex configuration differs; refusing to overwrite")
config.parent.mkdir(exist_ok=True)
config.write_text(block)
print(f"Registered DEV-only MCP in {config}")
