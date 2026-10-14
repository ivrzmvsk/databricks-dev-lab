"""Local corpus preparation and explicit DEV query entry points."""

import argparse
import json
import time
from dataclasses import asdict
from pathlib import Path

from lab12.config import Settings
from lab12.corpus import prepare
from lab12.retrieval import LexicalPreview


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare", "preview", "ask"])
    parser.add_argument("--corpus", type=Path, default=Path("dataset/board_games.json"))
    parser.add_argument("--output", type=Path, default=Path(".state/prepared"))
    parser.add_argument("--query", default="")
    parser.add_argument("--filters", default="{}", help="JSON metadata filters")
    parser.add_argument("--max-tokens", type=int, default=400)
    parser.add_argument("--overlap-tokens", type=int, default=60)
    parser.add_argument("--warehouse-id", default="")
    args = parser.parse_args()
    if args.command == "ask":
        from databricks.sdk import WorkspaceClient

        from lab12.observability import configure, verify_trace
        from lab12.qa import Assistant, ServingModel
        from lab12.retrieval import VectorRetriever

        settings = Settings()
        workspace = WorkspaceClient(profile="personal-env")
        if not args.warehouse_id:
            parser.error("ask requires --warehouse-id for governed MLflow logging")
        user = workspace.current_user.me().user_name
        experiment_id = configure(
            workspace,
            settings,
            f"/Users/{user}/lab12-board-dev",
            args.warehouse_id,
            profile="personal-env",
        )
        assistant = Assistant(
            VectorRetriever(workspace, settings),
            ServingModel(workspace, settings),
            settings,
            tracing=True,
        )
        started_ms = int(time.time() * 1000)
        result = assistant.ask(args.query, json.loads(args.filters))
        import mlflow

        mlflow.flush_trace_async_logging()
        trace = (
            verify_trace(
                experiment_id,
                settings.host,
                started_ms,
                mlflow.get_last_active_trace_id(thread_local=True),
            )
            if result.finish_reason
            else None
        )
        print(json.dumps({**asdict(result), "trace": trace}, ensure_ascii=False, indent=2))
        return
    docs, chunks = prepare(args.corpus, args.max_tokens, args.overlap_tokens)
    if args.command == "preview":
        print("OFFLINE LEXICAL PREVIEW — no LLM or Vector Search call")
        hits = LexicalPreview(chunks).search(args.query, json.loads(args.filters))
        print(json.dumps([asdict(h) for h in hits], ensure_ascii=False, indent=2))
        return
    args.output.mkdir(parents=True, exist_ok=True)
    for name, rows in [("documents", [asdict(d) for d in docs]), ("chunks", chunks)]:
        (args.output / f"{name}.jsonl").write_text(
            "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8"
        )
    print(
        json.dumps(
            {
                "documents": len(docs),
                "chunks": len(chunks),
                "max_chunk_tokens": max(c["token_count"] for c in chunks),
                "output": str(args.output),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
