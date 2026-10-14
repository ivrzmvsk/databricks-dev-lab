# LAB 12 — AI Capstone: RAG Shop Assistant

A small RAG application on Databricks: an assistant over **12 fictional board-game
cards**. It answers with numbered sources, applies metadata filters, and refuses to
invent stock or releases. The same model also answers without retrieval for comparison.
The games are invented (`dataset/board_games.json`), so any correct detail must come
from retrieval.

The full workflow runs in the personal Databricks workspace (DEV).

## Part A — RAG on Databricks

```
board_games.json → clean + chunk → Delta tables (CDF) → Vector Search index → top-k + filters → LLM → answer + sources
```

| Step | Implementation |
|---|---|
| Preparation | Cleaning, chunks of at most 400 tokens with 60 overlap, stable IDs, metadata (co-op, players, minutes, age) ([corpus.py](src/lab12/corpus.py)) |
| Storage | `board_documents` and `board_chunks` Delta tables in a dedicated Unity Catalog schema, idempotent MERGE ([storage.py](src/lab12/storage.py)) |
| Index | Delta Sync Vector Search index with managed embeddings ([index.py](src/lab12/index.py)) |
| Retrieval | Hybrid search, top-k = 3, metadata filters such as `coop` or `min_players` ([retrieval.py](src/lab12/retrieval.py)) |
| QA | Only retrieved cards as context, a `[n]` citation per claim, fixed refusal when evidence is missing ([qa.py](src/lab12/qa.py)) |
| Monitoring | MLflow traces in Unity Catalog: prompts, context, answers, latency, tokens; email/token redaction |

One bundle job, three notebooks: [ingest](notebooks/01_ingest.py), [sync index](notebooks/02_sync_index.py),
[evaluate](notebooks/03_evaluate.py) (8 questions, each with and without retrieval).
Schema, tables, endpoint and index are created beforehand by [tools/setup_resources.py](tools/setup_resources.py).

### Results

All three tasks succeeded; 16 answers completed with zero errors. 12 documents → 12
chunks, index ready, trace verified. RAG used 7,024 prompt / 193 completion tokens.

The assistant worked as intended. Each answer was read against its card: all six factual
answers matched the cards with correct citations, and both unsupported requests were
refused. Without retrieval the model answered the same questions with wrong or made-up
details (for example 45–60 minutes instead of 25 for Moon Station). This is a manual
check of eight examples, not a statistical result.

**A specific rule** — *How often can a player use the waterfall shortcut in River Racers, and what does it cost?*

| | Answer |
|---|---|
| RAG | …twice per game, and each use costs three paddle tokens [1]. |
| No retrieval | …once per game, and it costs 2 energy tokens. |

The card says: "Each player may use the waterfall shortcut twice per game, paying three paddle tokens each time."

**A comparison** — *Compare Festival of Masks and Pocket Orchard.*

| | Answer | Matches the cards? |
|---|---|---|
| RAG | Festival of Masks: 6–10 players, 25 min [1]. Pocket Orchard: exactly 1 player, 20 min [2]. | yes |
| No retrieval | Festival of Masks: 2–4 players, 45–60 min. Pocket Orchard: 1–3 players, 20–30 min. | no |

**A request to invent facts** — *Invent the release date and price of a Coral Keepers expansion.* The catalogue has neither, so the correct behaviour is to refuse, and both modes did.

## Part B — AI-assisted development

**Tools.** Codex CLI was the main coding agent (code, notebooks, bundle job, tests, CI,
live DEV runs); Claude Code was used for planning and review. The Databricks **AI Dev Kit**
was installed with its `install.sh` and the `personal-env` profile (8 skills, including
vector-search, jobs, dabs and mlflow-evaluation).

**What shipped:** the job ([steam.job.yml](resources/steam.job.yml)), the setup script, 72
unit tests and [lab12-ci.yml](../.github/workflows/lab12-ci.yml). The job was deployed with
`databricks bundle deploy`. The agent was fast at scaffolding the modules and writing tests.

**Human review decided:**
- *Dataset:* a fictional catalogue, because a corpus of well-known real items would let the
  model answer from memory and hide the effect of retrieval.
- *Scope:* 12 cards, 8 questions, no LLM judge.
- *Code structure:* MERGE logic in a tested library function, shared notebook setup in one
  place, and infrastructure kept out of the data job.
- *Approval:* the agent compared answers with cards and that review was accepted, without
  independently re-checking each answer.

Nothing was deployed blindly: tests and strict bundle validation came first.

## Guardrails

- **Environments:** DEV and PROD hosts are fixed in code; a wrong host is rejected. Live testing ran only in DEV.
- **Untrusted context:** retrieved text is data; an injection question is in the eval set.
- **Grounding:** answers without valid citations are rejected; missing evidence gives a fixed refusal.
- **Cost:** fixed top-k and token limits, no schedule, one run at a time.
- **Access:** one dedicated schema, no broad grants.

## Discussion

**RAG vs prompting alone.** Without the catalogue the model answered confidently and
wrongly. With retrieval it cited the cards and refused when the answer was missing.
Prompting cannot supply private or changing facts.

**Chunk size.** Small chunks match precisely but can split a rule from its context;
large chunks keep context but mix topics and cost more tokens. Every card here fits in one
chunk.

**Governance and cost.** Unity Catalog controls the tables, index and traces. Cost comes from embeddings and pay-per-token calls (about 8,000 tokens for the RAG run); rate limits led to sequential evaluation.

## Run it

```bash
uv run --no-sync python -m pytest -q          # 72 tests
uv run --no-sync python tools/setup_resources.py --target dev --profile personal-env --warehouse-id 6fec7d20a958edd1
databricks bundle deploy -t dev --profile personal-env --var warehouse_id=6fec7d20a958edd1
databricks bundle run steam_rag -t dev --profile personal-env --var warehouse_id=6fec7d20a958edd1
uv run --no-sync python -m lab12.cli ask --query 'How many players can play Moon Station?' --warehouse-id 6fec7d20a958edd1
```

## Limitations

- 12 cards is a demonstration; top-k = 3 of 12 is an easy retrieval task.
- Scorers check behaviour, not correctness; there is no LLM judge.
