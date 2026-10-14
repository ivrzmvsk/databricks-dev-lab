from pathlib import Path

import pytest

from lab12.corpus import load_documents
from lab12.evaluation import evaluation_records
from lab12.release import automated_gate


def test_ground_truth_matches_frozen_source():
    docs = load_documents(Path(__file__).resolve().parents[1] / "dataset/board_games.json")
    records = evaluation_records(docs)
    assert len(records) == 8
    assert len({r["inputs"]["case_id"] for r in records}) == 8
    assert sum(r["expectations"]["required_status"] == "refused" for r in records) == 2
    assert sum(r["expectations"]["required_status"] == "clarification" for r in records) == 0
    assert all(r["expectations"]["expected_facts"] for r in records)


def test_quality_gate_rejects_missing_and_bad_metrics():
    with pytest.raises(ValueError, match="gate failed"):
        automated_gate({"questions": 8, "completed": 8, "errors": 0, "metrics": {}})
    report = {
        "questions": 8,
        "completed": 8,
        "errors": 0,
        "scored_counts": {
            "required_behavior": 8,
            "retrieval_recall": 6,
            "citation_presence": 6,
        },
        "metrics": {
            "correctness/mean": 0.9,
            "retrieval_recall/mean": 0.95,
            "required_behavior/mean": 1.0,
            "citation_presence/mean": 1.0,
        },
    }
    automated_gate(report)
    report["metrics"]["retrieval_recall/mean"] = 0.8
    with pytest.raises(ValueError, match="retrieval_recall"):
        automated_gate(report)


def test_smoke_evaluation_cannot_pass_release():
    report = {
        "questions": 3,
        "completed": 8,
        "errors": 0,
        "scored_counts": {
            "required_behavior": 8,
            "retrieval_recall": 6,
            "citation_presence": 6,
        },
        "metrics": {
            "correctness/mean": 1.0,
            "retrieval_recall/mean": 1.0,
            "required_behavior/mean": 1.0,
            "citation_presence/mean": 1.0,
        },
    }
    with pytest.raises(ValueError, match="8-question"):
        automated_gate(report)


def test_databricks_dataset_uses_records_instead_of_mlflow_tags(monkeypatch):
    import sys
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from lab12.config import Settings
    from lab12.evaluation import get_or_create_dataset

    api = MagicMock()
    api.search_datasets.return_value = []
    monkeypatch.setitem(sys.modules, "mlflow.genai", SimpleNamespace(datasets=api))
    record = {"inputs": {"case_id": "q01"}, "expectations": {"expected_facts": ["fact"]}}
    dataset = get_or_create_dataset(Settings(), "123", [record], "corpus-hash")
    api.create_dataset.assert_called_once_with(
        name="workspace.ivanrazumovskyi_lab12_steam.evaluation_board_v2", experiment_id="123"
    )
    saved = dataset.merge_records.call_args.args[0]
    assert saved[0]["expectations"]["corpus_sha256"] == "corpus-hash"
    assert "corpus_sha256" not in record["expectations"]


def test_changed_dataset_expectations_are_rejected(monkeypatch):
    import sys
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from lab12.config import Settings
    from lab12.evaluation import get_or_create_dataset

    api, found = MagicMock(), MagicMock()
    found.name = Settings().table("evaluation_board_v2")
    found.to_df.return_value.__getitem__.return_value.to_dict.return_value = [
        {"inputs": {"case_id": "q01"}, "expectations": {"expected_facts": ["different"]}}
    ]
    api.search_datasets.return_value = [found]
    api.get_dataset.return_value = found
    monkeypatch.setitem(sys.modules, "mlflow.genai", SimpleNamespace(datasets=api))
    record = {"inputs": {"case_id": "q01"}, "expectations": {"expected_facts": ["fact"]}}
    with pytest.raises(ValueError, match="different corpus or expectations"):
        get_or_create_dataset(Settings(), "123", [record], "corpus-hash")
    api.create_dataset.assert_not_called()
    found.merge_records.assert_not_called()


def test_comparison_expectations_identify_each_game():
    docs = load_documents(Path(__file__).resolve().parents[1] / "dataset/board_games.json")
    records = evaluation_records(docs)
    comparison = records[4]["expectations"]["expected_facts"]
    assert comparison[0].startswith("Festival of Masks:")
    assert comparison[1].startswith("Pocket Orchard:")


def test_uc_json_outputs_are_counted_but_invalid_outputs_are_not():
    from lab12.evaluation import decode_output

    answer = {"answer": "An ocean planet [1].", "status": "answered"}
    assert decode_output(answer) == answer
    assert decode_output('{"answer":"An ocean planet [1].","status":"answered"}') == answer
    assert decode_output("broken JSON") == {}
    assert decode_output(None) == {}
    assert decode_output("[]") == {}


def test_offline_retrieval_finds_expected_sources():
    from lab12.corpus import prepare
    from lab12.retrieval import LexicalPreview

    docs, chunks = prepare(Path(__file__).resolve().parents[1] / "dataset/board_games.json")
    retriever = LexicalPreview(chunks, top_k=3)
    for record in evaluation_records(docs):
        expected = set(record["expectations"]["expected_doc_ids"])
        if expected:
            inputs = record["inputs"]
            hits = retriever.search(inputs["query"], inputs["filters"])
            assert expected <= {h.doc_id for h in hits}, inputs["case_id"]
