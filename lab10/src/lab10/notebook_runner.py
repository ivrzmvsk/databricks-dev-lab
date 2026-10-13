"""Run the actual task notebooks through an injected SQL-only pipeline.

The warehouse recovery runner and local tests use the same notebook code as
the Job. Databricks supplies widgets/Spark itself; injected runs skip that
bootstrap and return the notebook's result without calling notebook.exit.
"""

import io
import runpy
from contextlib import redirect_stdout
from pathlib import Path


def run_notebook(name, pipeline):
    if name not in ("00_setup", "01_federation", "02_cdc", "03_validate"):
        raise ValueError("Unknown LAB 10 notebook")
    path = Path(__file__).resolve().parents[2] / "notebooks" / f"{name}.py"
    with redirect_stdout(io.StringIO()):
        namespace = runpy.run_path(str(path), init_globals={"_injected_pipeline": pipeline})
    return namespace["result"]
