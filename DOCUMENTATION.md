# Talend → Databricks Migration System — Full Technical Documentation

This is the detailed companion to [README.md](README.md). README is the quick
operational cheat-sheet (reset a halt, add a real job, etc.); this document
explains **what the system is, why it's built this way, how every piece
works, and what's still unfinished** — written so someone picking this up
cold (including another AI assistant continuing development) doesn't have to
reverse-engineer intent from code alone.

---

## 1. What this is, in one paragraph

An autonomous pipeline that takes a Talend job's raw export files (`.item`
XML, `.properties`, contexts, routines, embedded Java — read as **text**,
never parsed by a deterministic XML parser), has LLM agents understand and
translate it to PySpark/Lakeflow Python, deploys the result as its own
Databricks Job, runs it, and — within a strict, plain-code-enforced budget —
fixes execution errors and retries. It works through a queue of pipelines in
batches, stopping itself (`control.halted`) if things are going wrong often
enough that a human should look. A separate self-improvement loop reads the
failure history after each batch and proposes (bounded, auto-evaluated)
changes to translation knowledge. A React dashboard app (AppKit on
Databricks Apps) gives a human full visibility and control over all of this
without needing the Databricks workspace UI.

**Why no deterministic Talend XML parser anywhere?** This was a deliberate,
repeated design choice (see `prompts/*.md`): Talend's `.item` format is rich,
version-dependent, and full of edge cases (globalMap contexts, joblets,
tRunJob parameter passing, conditional routing) that are exactly the kind of
"infinite long tail" a hand-written parser fights forever. Handing the raw
text to an LLM with a very specific extraction contract (the prompts) trades
determinism for coverage, and the VERIFIER agent + unit-tested Java
translation exist specifically to catch what the SPEC agent's understanding
gets wrong.

---

## 2. The end-to-end pipeline, one job at a time

This is `src/orchestration/process_job.py`, the real wiring between agents
and plain-code control flow:

```
read_raw_files(volume path)
        │
        ▼
 SPEC agent  ───disapproved──▶  regenerate (up to MAX_SPEC_ROUNDS=2)
        │  approved (or rounds exhausted → spec_uncertain=true)
        ▼
 VERIFIER agent  (different model, re-reads raw files independently,
                  audits SPEC's output for omissions/hallucinations)
        │
        ▼
 write_lineage()  → `lineage` table (source/target table + columns,
                     read off the raw XML's own elementParameters,
                     never guessed — feeds the separate validation step)
        │
        ▼
 translate_all_java_blocks()  → JAVAROUTINE agent, one call per inline
                                 Java block found in the spec, each
                                 immediately unit-tested against that
                                 block's java_cases fixture
        │
        ▼
 ┌─────────────────────── fix-cycle loop (batch_runner.run_job_with_fix_cycles) ───────────────────────┐
 │  CODEGEN agent → full PySpark module                                                                 │
 │       │                                                                                               │
 │       ▼                                                                                               │
 │  check_syntax() + scan_for_secrets()   (free, plain-code — rejects before spending a review call)    │
 │       │                                                                                               │
 │       ▼                                                                                               │
 │  CODE_REVIEW agent  (up to MAX_REVIEW_ROUNDS=3; a persistent high-severity finding currently          │
 │                       consumes a real fix cycle — documented gap, see §8)                              │
 │       │ approved                                                                                       │
 │       ▼                                                                                               │
 │  deploy_dev()  → upload code to Workspace, create/update the Databricks Job                           │
 │       │                                                                                               │
 │       ▼                                                                                               │
 │  run_job()  → run-now, poll to terminal state                                                         │
 │       │                                                                                               │
 │       ├─ success ──────────────────────────────────────────────────▶ JobOutcome(COMPLETED)           │
 │       ├─ infra error (quota/cluster/network) → ONE same-code retry, doesn't consume a fix cycle       │
 │       │        └─ still infra → JobOutcome(INFRA_RETRY_EXHAUSTED) → re-queued to PENDING, NOT FAILED  │
 │       └─ code error → loop back to CODEGEN with the exact error, up to max_fix_cycles (control table) │
 └────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Every agent call is logged to `attempt_log` (one row per call, raw `details`
JSON, cost in tokens). Nothing in this loop is an LLM decision except "is
this code acceptable" (CODE_REVIEW) and "is this error infra or code"
(the plain-code heuristic `_classify_error_text` in `bundle_tools.py` — note
this is **not** currently an LLM call despite earlier design notes
mentioning one; it's a keyword match against `INFRA_ERROR_MARKERS`). Counters
— fix cycles used, review rounds used, spec rounds used, batch failure
counts — are **always** plain Python, enforced in `batch_runner.py` and
`process_job.py`, never left to a model to self-regulate. This split is
intentional and tested (`tests/test_batch_runner.py` uses fake
generate/deploy callables with zero Databricks or LLM dependency).

### 2.1 Why two separate retry concepts (infra retry vs. fix cycle)?

An infra failure (quota exceeded, cluster couldn't start, network blip) says
nothing about whether the generated code is correct — retrying the *same*
code once is the right response, and it must not burn down the fix-cycle
budget that exists to bound *code-quality* iteration. `run_job_with_fix_cycles`
in `src/orchestration/batch_runner.py` keeps these as two separate counters
with different consequences:
- Infra error → 1 free retry with identical code → if it fails again,
  `INFRA_RETRY_EXHAUSTED` → the job goes back to `PENDING` (not `FAILED`) so
  it's picked up again in a later batch, and it counts toward a separate
  *consecutive infra failure* streak (3 in a row halts the batch — likely a
  workspace-wide problem, not a per-job one).
- Code error → consumes one fix cycle, CODEGEN gets the previous code + the
  exact error text and must produce something different (duplicate-code
  submissions are detected via SHA-256 and treated as an immediate `FAILED`,
  see `_hash_code`/`seen_hashes` in `batch_runner.py`).

### 2.2 The three uncertainty flags a `COMPLETED` job can still carry

A job reaching `COMPLETED` does **not** mean every agent was fully confident.
Three independent flags survive onto the `queue` row:
- `spec_uncertain` — VERIFIER never approved within `MAX_SPEC_ROUNDS` (2); the
  SPEC agent's best-effort output was used anyway. Check `job_spec.verifier_result`.
- `java_uncertain` — at least one Java block's translation (`translation_tier`
  in `java_block_log`) was best-effort rather than a confident 1:1 mapping.
- `unresolved_dependencies` — an external library had no Python/JAR
  equivalent found.

---

## 3. The batch loop and the halt rule

`src/orchestration/batch_loop.py` is the actual driver that runs as the
`talend_migration_orchestrator` Databricks Job (`resources/jobs/orchestrator.yml`,
no schedule — run manually or chained). Each iteration:

1. Reads `migration.control` fresh (every batch — never cached). If
   `kill_switch` is set, requeues any `IN_PROGRESS` rows back to `PENDING`
   and exits immediately, no new batch starts. If `halted` is set, exits
   without touching anything (a human must clear it — see README §"Resetting
   a halt").
2. Picks a batch: **resumability first** — any `IN_PROGRESS` rows left by a
   crashed prior run are picked up again before any new `PENDING` work
   (`queue_ops.select_next_batch`), ordered by `complexity_score ASC` (cheap,
   likely-to-succeed jobs first).
3. Runs the batch's jobs **concurrently** (`ThreadPoolExecutor`, one worker
   per job in the batch) — justified in the code because each job's time is
   dominated by network-bound model/Databricks API calls, not CPU. Trade-off,
   explicitly documented in the code: once dispatched, all jobs in a batch
   run to completion even if an early failure would already have tripped the
   halt rule — true mid-flight cancellation isn't practical with a thread
   pool, so halt checks happen *between* batches, not mid-batch.
4. After the batch: if `failed_count >= control.halt_threshold` (default 3)
   *or* 3 consecutive `INFRA_RETRY_EXHAUSTED` results occur, sets
   `control.halted = true` with a specific `halt_reason`, requeues any
   leftover `IN_PROGRESS` to `PENDING`, and stops.
5. Writes one `batch_log` row per batch (`job_ids`, `completed_count`,
   `failed_count`, `halted`, `halt_reason`).
6. Loops until the queue is empty, a halt trips, or `--max-batches` is hit.

`--pipeline-ids` (used by the dashboard's "Run N selected" and single
re-run buttons, via the `talend_migration_app_run` job) bypasses all of this:
it runs **exactly** the named jobs once, marks them `IN_PROGRESS` directly
regardless of `batch_size`, and exits without falling through to
queue-driven batches. This is how the app can trigger an ad-hoc run without
disturbing the main queue's ordering.

---

## 4. Data model — every table in `talend_migration.migration`

All DDL lives in `setup/bootstrap_workspace.py` (idempotent — safe to re-run,
every step checks existence first). Full column list and *why* each table
exists:

| Table | Purpose | Written by |
|---|---|---|
| `control` | Singleton (`id=1`) control-plane row: `kill_switch`, `halted`/`halt_reason`, `batch_size`, `max_fix_cycles`, `halt_threshold`, spec/review round caps, token/DBU budgets, run timeout, and (added this session) `source_path`/`destination_path` for the dashboard's Sources page. Read before every batch, every agent call, every deploy/run. | `batch_loop.py`, `control.py`, dashboard's `/api/control/update` |
| `queue` | One row per pipeline: status (`PENDING`/`IN_PROGRESS`/`COMPLETED`/`FAILED`), `fix_attempts`, `complexity_score`, the three uncertainty flags, `error_signature` (short) + `last_error` (full), `batch_id`, timing columns, `cost`. | `queue_ops.py`, `batch_loop.py` |
| `job_spec` | SPEC agent's JSON output per pipeline, plus VERIFIER's result and any uncertainties. | *(not currently written by `process_job.py` directly — spec/verifier results go to `attempt_log`; this table exists in the schema but the live write path for it predates this session's work and should be double-checked before relying on it)* |
| `attempt_log` | One row per agent call, ever. `agent` ∈ {SPEC, VERIFIER, JAVAROUTINE, CODEGEN, CODE_REVIEW, MANUAL_FIX}. `details` is JSON-as-string (see §9 on the serialization gotcha this caused). This is the system's complete audit trail and what the IMPROVER agent reads. | `process_job.py`'s `log_attempt()`, `deploy_manual_code.py` |
| `java_block_log` | Per-block Java→Python translation outcome and unit-test pass rate against `java_cases` fixtures. | *(JAVAROUTINE translation path — verify current write site before depending on it)* |
| `batch_log` | One row per batch: `job_ids` (JSON array), counts, `halted`, `halt_reason`. | `batch_loop.py` |
| `improvement_log` | One row per IMPROVER proposal: topic, before/after metrics, whether it was merged. | `improvement_cycle.py` |
| `eval_results` | One row per pipeline per eval-gate run (used to decide whether an improvement proposal merges). | `eval_gate.py` |
| `lineage` | **Added this session.** Per-component source→target mapping extracted by the SPEC agent directly from the Talend XML's own `elementParameters` (e.g. a `tDBOutput`'s `TABLE`/`SCHEMA` fields) — never guessed. One row per `(pipeline_id, component)`. This is what makes data validation possible without a deterministic parser: the SPEC agent already reads this information to build the job spec, so it was a small addition (`lineage[]` in `prompts/spec_agent.md`, `PROMPT_VERSION` bumped 1→2) to also surface it structurally. | `process_job.py`'s `write_lineage()` |
| `validation_log` | **Added this session.** Destination-side validation stats (row count, whole-row checksum) computed by `run_validation.py` against the *real* target table. `talend_row_count`/`talend_column_checksums` are always `NULL` — there is no Talend-side baseline to compare against (these Talend jobs have never actually run), and the system never fabricates one. The columns exist so a real baseline can be wired in later without a schema change. | `run_validation.py` |

**On SQL string escaping:** every write goes through `src/tools/sql_exec.py`'s
`esc()` (doubles single quotes) and constructs statements via direct string
interpolation, not parameterized queries — a deliberate choice noted in the
code because the Statement Execution API's named-parameter support is
limited for DDL/complex DML. Every caller is expected to route all values
through `esc()`; there is no ORM layer catching a missed one.

**On reading these values back out through the dashboard:** the Statement
Execution API serializes *every* column type as a JSON string on the wire —
`DOUBLE`, `BIGINT`, `BOOLEAN`, even a `details` field that's already valid
JSON — regardless of what the TypeScript query-type generator claims. This
caused three real bug classes in the dashboard (numeric `.toFixed` crashes,
`"false"` being truthy, and inconsistent JSON-already-parsed-or-not states)
and is the reason `client/src/ls/utils.ts`'s `num()`/`bool()`/`text()`
coercion helpers exist and are applied almost everywhere a raw query result
touches the UI. Any new query/column added to the dashboard needs the same
treatment — never trust the generated TypeScript type's `number`/`boolean`
at runtime.

---

## 5. The agents, in detail

All prompts live in `prompts/*.md`; the Python modules in `src/agents/`
**only** handle chunking, the model call, JSON parsing/repair, and merging —
they never interpret Talend semantics themselves (that's entirely the
prompt's job). Each agent module calls `src/tools/model_client.py`'s
`query_model_json()`, which:
- Disables Claude's extended thinking (`"thinking": {"type": "disabled"}`) —
  without this, these models were observed spending the entire token budget
  on invisible reasoning and returning an almost-empty answer.
- Retries on malformed JSON up to twice, distinguishing **truncation**
  (doubles `max_tokens` up to the server's hard 8192 cap, then asks for more
  concise prose instead of more structure) from a **genuine formatting slip**
  (feeds the exact parse error back for a correction).
- Uses the SDK's raw `api_client.do()` rather than the typed
  `serving_endpoints.query()`, specifically to send the Anthropic-only
  `thinking` field the typed method can't express.

| Agent | File | Model (default) | What it does | Hard caps enforced in plain code |
|---|---|---|---|---|
| **SPEC** | `spec_agent.py` | `databricks-claude-sonnet-5` | Reads raw files (chunked if large, merged after), emits the structured job spec JSON — components, connections, java_blocks, context vars, and (v2) `lineage[]`. | None itself; regeneration is capped by VERIFIER's loop (§below) |
| **VERIFIER** | `verifier_agent.py` | `databricks-llama-4-maverick` (deliberately a *different* model from SPEC) | Independently re-reads the same raw files and audits the SPEC output for missing/wrong/extra findings. | `MAX_SPEC_ROUNDS = 2` |
| **JAVAROUTINE** | `javaroutine_agent.py` | `databricks-claude-sonnet-5` | Translates one inline Java block to a Python `run(input: dict) -> dict` function; the module then unit-tests it in-process against that block's `java_cases` fixture via plain `exec()` + dict comparison — no model call decides pass/fail. | Fix-cycle budget shared with CODEGEN (via `process_job.py`) |
| **CODEGEN** | `codegen_agent.py` | `databricks-claude-sonnet-5`, auto-escalates to `databricks-claude-opus-5` on `fix_cycle >= 2` | Spec + translated Java blocks → full PySpark module. On a fix pass, gets the previous code and exact error and must produce something *different* (duplicate-code detection is in `batch_runner.py`, not here). | `max_fix_cycles` (control table, default 2) |
| **CODE_REVIEW** | `code_review_agent.py` | `databricks-claude-sonnet-5` | Reviews one generated module for forbidden APIs, embedded secrets, unsafe I/O, missing Java blocks, obvious bugs. Runs *after* the free `check_syntax`/`scan_for_secrets` gates. | `MAX_REVIEW_ROUNDS = 3`; **gap** — see §8 |
| **IMPROVER** | `improver_agent.py` | `databricks-claude-opus-5` | Reads new `attempt_log` rows since the last cycle, proposes knowledge/prompt changes for recurring root causes. | `MAX_PROPOSALS_PER_CYCLE = 3`; any proposal targeting a `PROTECTED_PREFIXES` path (`src/orchestration/`, `src/evaluation/`, `eval/thresholds.yml`, `databricks.yml`, `resources/`, `.github/workflows/`, `CODEOWNERS`) is rejected and logged, never silently dropped |

**MANUAL_FIX** is not a model-driven agent — it's the label used when a human
edits generated code in the dashboard and redeploys it directly
(`src/orchestration/deploy_manual_code.py`), skipping CODEGEN but still going
through the same `check_syntax`/`scan_for_secrets` gates and the same
`queue`/`attempt_log` bookkeeping, so the history stays honest about what
actually happened.

---

## 6. The self-improvement loop

`src/orchestration/improvement_cycle.py`, run manually or chained after the
orchestrator job finishes (`resources/jobs/improve_cycle.yml`, no
schedule/trigger yet — same reasoning as the orchestrator job):

1. Pull every `attempt_log` row since the last cycle (or `--since-timestamp`).
2. Ask IMPROVER for proposals (already capped/filtered — §5 table).
3. For each surviving proposal: create a local git branch
   (`improve/<topic>`), write the proposed file, commit, then run the real
   eval gate (`src/evaluation/eval_gate.py`) — **not a simulation**: it
   actually re-runs `process_job()` for the failed jobs the proposal targets
   plus up to 5 random `COMPLETED` jobs, under the same 2-fix-cycle limit as
   production, and records each into `eval_results`.
4. `run_eval()`'s verdict is currently conservative by necessity: there's no
   stored baseline to diff against yet (first eval ever run for this repo),
   so it requires the **entire** eval set to reach `COMPLETED` to `PASS`.
   On `PASS`, the branch is merged into `main` locally; on `FAIL`, the branch
   is left unmerged for inspection — neither path auto-deletes anything.
5. **No remote was configured for most of this project's life** (checked live
   via `git remote` at import time, `HAS_REMOTE`), so "open a PR" meant
   "commit locally, ready to push once a remote exists" — this is printed
   explicitly each run, not silently skipped. *This has now changed*: this
   repo was pushed to `github.com/ritwick25/talendtodatabricksmigration`
   this session, so a future run of `improvement_cycle.py` **can** actually
   push branches and open real PRs if `GITHUB_TOKEN`/`gh` auth is set up
   wherever it runs — that wiring (push + `gh pr create` instead of a local
   merge) has not been added yet.

---

## 7. Data validation subsystem (added this session)

Originally the system had a `validation_status` column that was always
`NULL` by design ("this system does not validate output data; that's run
separately" — README). This session built the "separately" part for real,
driven by the user's explicit framing: *assume a destination connection
exists (native Databricks schema, or a Unity Catalog Foreign Catalog over
something like Redshift via Lakehouse Federation), compute real stats
against it, and leave the Talend-side baseline columns blank since no
Talend execution of these jobs has ever happened.*

- **`lineage` table** (§4) is the structural prerequisite: SPEC agent prompt
  v2 now also emits `lineage[]` (subjob, component, source/target
  type/system/table/columns, confidence) straight from the same raw XML
  elementParameters it already reads to build the job spec.
- **`control.destination_path`** (new column) holds a `catalog.schema`-style
  reference, set by a human on the dashboard's Sources page. Because Unity
  Catalog Foreign Catalogs are queried through the exact same
  `catalog.schema.table` SQL syntax as native tables via the same SQL
  warehouse, **this works unchanged regardless of what's actually behind
  that catalog** — no destination-specific code was needed.
- **`src/orchestration/run_validation.py`** (new): for each target pipeline,
  looks up its most recent `lineage` row with a non-empty `target_table`,
  resolves `destination_path.<bare_table_name>`, and computes:
  - `destination_row_count` — `SELECT COUNT(*)`
  - `destination_column_checksums` — a single whole-row, order-independent
    checksum (`SUM(hash(to_json(struct(*))))`), not per-column, since there's
    no baseline to compare individual columns against yet either way
  - Writes one `validation_log` row with a `status` that's one of
    `NO_LINEAGE`, `UNRESOLVED_TARGET` (target table name wasn't a plain
    identifier — likely an unresolved context variable), `NO_DESTINATION_CONFIGURED`,
    `DESTINATION_UNREADABLE` (the query itself failed), or `DESTINATION_ONLY`
    (success, but still no baseline to compare against) — every failure mode
    is a distinct, explained status, never a silent skip.
- **Deployed as its own job**, `talend_migration_app_validate`
  (`resources/jobs/app_validate.yml`), triggerable from the dashboard's
  Validation page either for explicit `pipeline_ids` or (empty parameter)
  for every `COMPLETED` pipeline with no `validation_log` row yet.
- **What's still a known gap, by design, not oversight**: the
  `talend_row_count`/`talend_column_checksums` columns, and therefore any
  actual *comparison*, are deferred until a real Talend-side execution or
  export of row-level results exists. The UI (`ValidationPage.tsx`) shows
  these as a permanent blank/dash, not a fake zero.

---

## 8. Known gaps and limitations — read this before extending

Documented explicitly so the next person (human or AI) doesn't have to
rediscover these the hard way:

1. **CODE_REVIEW high-severity feedback currently consumes a real fix cycle.**
   `process_job.py`'s `make_deploy_and_run_fn` has no separate CODEGEN call
   for a review-rejection round — a persistent high-severity finding
   surfaces as a code-classified failure through the normal fix-cycle path.
   The code comment calls this out directly as a documented limitation for
   a future phase to close (a true review-fix loop that doesn't cost a
   deploy-and-run attempt).
2. **CODEGEN doesn't parameterize output tables from `control.destination_path`.**
   The validation subsystem (§7) can *read* the real destination once it's
   configured, but the generated migration code itself still writes
   wherever the spec/prompt decided, not to a path derived from
   `destination_path`. Wiring these together (so "set a destination" also
   changes where jobs actually write) is unbuilt.
3. **The dashboard's generated-code viewer can only read code deployed
   through the current `bundle_tools.deploy_dev()` path** (workspace path
   `/Workspace/Users/<owner>/talend_migration/generated/<pipeline_id>/job.py`).
   Earlier in this project's life, some jobs were deployed via a separate,
   now-legacy per-job Databricks Asset Bundle path
   (`/Workspace/Users/<owner>/.bundle/mig_<pipeline_id>/dev/files/src/job.py`,
   job name `[dev app_l5540h_multimodal_rag_chat] mig_<pipeline_id>`). The
   dashboard app's own service principal has **no permission** to list or
   read any job/workspace path beyond what's explicitly granted in
   `databricks.yml`'s `resources:` — confirmed empirically (not guessed) by
   testing a dynamic `jobs.list()`/`.get()` lookup live and watching it come
   back empty every time, for every job, regardless of API usage pattern.
   `GET /api/code/:pipelineId` in `server/custom-routes.ts` now gives an
   honest explanatory message instead of a guess when a job falls into this
   category — the job may well be `COMPLETED` (ran successfully) even
   though this viewer can't show its code. The real fix is either an
   explicit permission grant on the legacy jobs/paths, or re-running those
   jobs through "Re-run full migration" so they land under the readable path.
4. **Talend-side validation baseline does not exist** (§7) — by the user's
   own explicit instruction, not an oversight.
5. **No scheduling on any job** (`orchestrator`, `improve_cycle`) — deliberate,
   out-of-scope-for-now per the original spec; run manually or via the
   dashboard's trigger buttons.
6. **`job_05_json_xml_integration` has a real, unresolved Python bug**:
   `ValueError: The truth value of an array with more than one element is
   ambiguous`, in generated code's `tjavarow_1_run`, pattern
   `item_names.get(...) or []` (a numpy/array-valued dict entry hitting `or`,
   which calls `bool()` on an array). Confirmed via a real re-run this
   session: SPEC/VERIFIER/CODE_REVIEW all passed, deploy succeeded, the Spark
   job itself failed with this bug. Not yet fixed.
7. **`improvement_cycle.py`'s "open a PR" is still a local merge-only flow**
   (§6) even though a GitHub remote now exists as of this session's push —
   the push-and-open-a-real-PR wiring hasn't been added.
8. **`job_spec`/`java_block_log` write paths should be double-checked** before
   relying on them — they're declared in the schema and clearly intended
   (per README) but this session's work didn't trace or exercise their
   current write sites with the same rigor as the tables listed as "written
   by" specific files in §4.

---

## 9. Hard-won debugging lessons (so they aren't re-learned)

These cost real wall-clock time this session and are worth knowing before
touching the relevant code again:

- **The Databricks SQL Statement Execution API serializes every value as a
  JSON string**, regardless of the column's real SQL type or what a
  generated TypeScript type claims. Always coerce (`num()`/`bool()`/`text()`
  in the dashboard's `client/src/ls/utils.ts`) — never trust `typeof` at
  runtime for a value that came from a SQL query result.
- **A Databricks Apps service principal only has access to what's explicitly
  granted** in `databricks.yml`'s `resources:` list for that app — not
  "anything the owning user can see." A `jobs.list()`/`workspace` call for
  anything outside the granted bindings silently returns empty, it does not
  error. Confirmed by testing two different API-call shapes live and getting
  the same empty result both times before concluding it was a permission
  boundary, not a code bug.
- **AppKit's `files` plugin defaults an unconfigured volume to
  `files.policy.publicRead()`** (read-only) — any upload feature needs an
  explicit `files.policy.allowAll()` (or a narrower custom policy) or every
  upload silently 403s (`[appkit:files] Policy denied "upload" ...` in the
  app's own logs — check there first, not just the browser console).
- **`getWorkspaceClient({})` from `@databricks/appkit`** returns the raw
  `@databricks/sdk-experimental` `WorkspaceClient` directly — no
  `.toLegacyWorkspaceClient()` wrapping needed; `.workspace`, `.jobs`,
  `.currentUser`, `.statementExecution` are all directly on it.
  `useAnalyticsQuery` has **no polling/refetch** built in — it fires once
  per mount/`autoStart` transition; anything that needs live updates (e.g.
  `JobDrawer.tsx` watching an actively re-running job) has to force a
  remount itself (the `key={pipelineId-tick}` pattern, with `tick` ticking
  via `setInterval` while open).
- **A Databricks Jobs serverless Python task has no `databricks` CLI binary
  and no `__file__`** (exec() context) — every orchestration script under
  `src/orchestration/` resolves its own root via
  `inspect.currentframe().f_code.co_filename` and talks to Databricks only
  through the `databricks-sdk` Python package directly, never a CLI
  subprocess. The same environment also ships an **older** pre-installed
  `databricks-sdk` missing some typed dataclasses (`compute.Environment`,
  `Library`) — `src/tools/bundle_tools.py` deliberately uses raw REST dict
  bodies via `client.api_client.do(...)` instead of typed SDK objects, for
  exactly this reason.
- **`/workspace/import` with `format="SOURCE"` + `language="PYTHON"`
  creates a NOTEBOOK object**, which a `spark_python_task` cannot read
  (`OSError: Operation not supported`, confirmed from a real run). Use
  `format="AUTO"` with no `language` to get a plain FILE object. Also,
  `overwrite=True` on that same call **refuses to replace an object across a
  type change** (FILE vs NOTEBOOK) — `deploy_dev()` now deletes the path
  first, unconditionally, before every import, to stay idempotent regardless
  of what was there before.
- **`com.crealytics.spark.excel` (a Scala/JAR Spark data source) cannot
  attach to serverless compute.** Any Excel-reading generated code should
  use `pandas.read_excel(engine="openpyxl")` → `spark.createDataFrame()`
  instead, with `openpyxl` passed through `deploy_dev(pip_dependencies=...)`.

---

## 10. The dashboard app (`apps/migration-dashboard/migration-dashboard`)

A Databricks App built on **AppKit** (`@databricks/appkit` backend +
`@databricks/appkit-ui` React components), giving a human everything needed
to run and watch the whole system without the Databricks workspace UI.

### 10.1 Plugins (`server/server.ts`)
- `analytics()` — executes the SQL files in `config/queries/*.sql` against a
  configured warehouse; `appkit generate-types` turns each file's declared
  `@param`s and real `DESCRIBE QUERY` result shape into
  `shared/appkit-types/analytics.d.ts`, consumed client-side via
  `useAnalyticsQuery('<query_name>', params)`. **Regenerate after every
  query file add/change** (`npx appkit generate-types --wait`) or the
  TypeScript types drift from reality.
- `files({ volumes: { files: { policy: files.policy.allowAll() } } })` — UC
  Volume access for raw-file upload (Sources page) and the code-editor's
  "Save & redeploy" upload. Non-default policy, see §9.
- `jobs({ jobs: { apprun, manualfix, validate } })` — typed trigger config
  (Zod schemas for each job's `job_parameters`) for the three
  app-triggerable jobs (§11).
- `server()` — the HTTP server itself; `onPluginsReady` mounts
  `registerCustomRoutes(app)` from `server/custom-routes.ts` for the two
  routes AppKit's generated plugins don't cover (control-table writes,
  generated-code viewer/editor).

### 10.2 Routes / pages (`client/src/ls/`, `App.tsx`)

Rebuilt this session on a ported design system (`lakeshift.css`, scoped
under a `.ls` prefix to avoid colliding with the rest of the app) from a
static mockup the user supplied, then wired to entirely real data:

| Route | File | Real data source |
|---|---|---|
| `/` Overview | `pages/OverviewPage.tsx` | `queue_list`, `queue_summary`, `control_status`, `improvement_log_list`, `attempt_log_recent` — KPI tiles, batch-outcome chart, learning curve, status donut, fix-cycle histogram, recent-activity trace |
| `/sources` | `pages/SourcesPage.tsx` | `control_status`, `queue_list` — source/destination path config (`updateControl()`), raw-file upload (`uploadSourceFile()`), pending-pipeline bulk-select + "Run N selected" |
| `/agents` | `pages/AgentsPage.tsx` | `control_status`, `queue_list`, `attempt_log_recent` — control-plane KPIs, agent ribbon (aggregated per-agent outcomes), latest-batch lanes, live trace |
| `/pipelines` | `pages/PipelinesPage.tsx` | `queue_list` — filterable/searchable table, review-blocked and uncertainty filters |
| `/validation` | `pages/ValidationPage.tsx` | `queue_list`, `validation_log_list` — trigger `runValidationJob()`, display real destination stats, Talend-baseline columns always blank (§7) |
| `/improve` | `pages/ImprovePage.tsx` | `improvement_log_list` — real IMPROVER proposal history, merged/rejected |
| `/guard` | `pages/GuardPage.tsx` | `control_status` — real control-table limits and `PROTECTED_PATHS` (a verbatim copy of `improver_agent.py`'s `PROTECTED_PREFIXES`, kept in sync manually — see `domain.ts`) |

A shared `Shell.tsx` (sidebar/topbar/halt-banner) and `ShellState.tsx`
(drawer + toast context) wrap every route. `JobDrawer.tsx` is the per-job
detail view (opened from any table row): pipeline-stage stepper, generated
code viewer/editor with "Save & redeploy" (routes through
`deploy_manual_code.py`, logged as `MANUAL_FIX`), agent-summary table, and
(added this session) **live polling** — a `tick` state incrementing via
`setInterval` every 8s while open, forcing the two `useAnalyticsQuery` calls
to remount and refetch via a `key={pipelineId-tick}` on an extracted
`DrawerBody` component, since `useAnalyticsQuery` itself has no refetch
mechanism (§9).

### 10.3 Every helper function — `client/src/lib/api.ts`
- `runAppJob({ pipelineIds, batchSize, maxBatches })` → triggers
  `talend_migration_app_run`
- `runValidationJob(pipelineIds?)` → triggers `talend_migration_app_validate`
- `runManualFixJob(pipelineId, codeVolumePath)` → triggers
  `talend_migration_app_manual_fix`
- `fetchGeneratedCode(pipelineId)` → `GET /api/code/:pipelineId`
- `submitFixedCode(pipelineId, code)` → uploads edited code to the `files`
  volume, returns the Volume path for `runManualFixJob`
- `uploadSourceFile(relativePath, file)` → raw-file upload to the `files`
  volume's `raw/` prefix
- `updateControl(fields: ControlUpdate)` → `POST /api/control/update`

### 10.4 Every custom backend route — `server/custom-routes.ts`
- `POST /api/control/update` — Zod-validated partial update of the
  `control` singleton row (kill switch, halt/clear, batch/fix-cycle
  defaults, `source_path`/`destination_path`), via `statementExecution`
  directly (the analytics plugin's query files are read-only).
- `GET /api/code/:pipelineId` — exports the generated `job.py` from
  `/Workspace/Users/<owner>/talend_migration/generated/<pipeline_id>/job.py`
  via the Workspace API (not a Volume path — generated code is a Workspace
  FILE object, outside the `files` plugin's reach). Returns an honest 404
  with an explanation (not a guess) for jobs deployed through the legacy
  per-job-bundle path the app's service principal can't read (§8.3).

### 10.5 Deployment

```bash
cd apps/migration-dashboard/migration-dashboard
npx tsc -b tsconfig.client.json --noEmit   # typecheck
npm run lint:ast-grep                      # AppKit's ast-grep lint rules
databricks apps deploy --profile dev       # builds, validates, uploads, deploys, restarts
```

`databricks apps deploy` itself runs type generation, ast-grep lint,
type-checking, and the Vite build as pre-flight validation before uploading
anything — a failed validation step aborts before touching the running app.

Connection details committed in `apps/migration-dashboard/migration-dashboard/databricks.yml`
and `app.yaml` are **non-secret resource identifiers only** (workspace host,
SQL warehouse ID, job IDs, UC volume path) — never credentials. See
`CONNECTION_SETUP.md` for exactly what's required locally and where it lives
(deliberately **not** committed — `.gitignore` excludes `.env` and
`.databrickscfg` everywhere in this repo).

---

## 11. Every Databricks Job in this system

| Job (bundle resource name) | File | Trigger | What it runs |
|---|---|---|---|
| `talend_migration_orchestrator` | `resources/jobs/orchestrator.yml` | Manual / chained | `batch_loop.py` — the continuous queue-driven loop (§3) |
| `talend_migration_improve_cycle` | `resources/jobs/improve_cycle.yml` | Manual / chained after orchestrator | `improvement_cycle.py` — self-improvement (§6) |
| `talend_migration_app_run` | `resources/jobs/app_run.yml` | Dashboard app (single/bulk trigger) | `batch_loop.py --pipeline-ids ...` — runs exactly the named jobs once, exits |
| `talend_migration_app_manual_fix` | `resources/jobs/app_manual_fix.yml` | Dashboard app ("Save & redeploy") | `deploy_manual_code.py` — deploys human-edited code as-is, skipping CODEGEN |
| `talend_migration_app_validate` | `resources/jobs/app_validate.yml` | Dashboard app (Validation page) | `run_validation.py` — destination-side stats (§7) |

All five share the same `serverless` environment spec pattern
(`databricks-sdk`, `mlflow`, `pyyaml` as base pip dependencies); the
migration jobs generated per-pipeline (`mig_<pipeline_id>`, created/updated
by `deploy_dev()`) are separate, dynamically-created jobs, one per pipeline,
not declared in `resources/jobs/*.yml` at all.

---

## 12. Repository map

```
talend-databricks-migration/
├── databricks.yml                 bundle root (orchestrator/improve jobs, workspace host)
├── setup/bootstrap_workspace.py    idempotent one-time workspace provisioning
├── prompts/*.md                   the ENTIRE Talend-understanding logic — read these first
├── src/
│   ├── agents/                    one file per LLM agent, thin wrappers around prompts + model_client
│   ├── orchestration/             all plain-code control flow (no Talend/Java understanding here)
│   ├── tools/                     SDK wrappers (SQL exec, model client, workspace file I/O, bundle deploy)
│   └── evaluation/eval_gate.py    the self-improvement loop's real regression gate
├── resources/jobs/*.yml           Databricks Job definitions (bundle resources)
├── knowledge/                     the ONLY place IMPROVER (and humans) may add translation knowledge
├── eval/thresholds.yml            self-improvement pass/fail thresholds (protected path)
├── samples/, generated/           sample Talend exports in; generated PySpark code out (local copy)
├── tests/test_batch_runner.py     unit tests for the plain-code fix-cycle/halt logic, zero live deps
├── docs/branch_protection.md
└── apps/migration-dashboard/migration-dashboard/   the Databricks App (AppKit + React)
    ├── databricks.yml, app.yaml   app bundle + resource bindings (non-secret connection details)
    ├── server/                    server.ts (plugins), custom-routes.ts (control update, code viewer)
    ├── config/queries/*.sql       every SQL query the dashboard can run (source of generated types)
    └── client/src/ls/             the actual UI: Shell, pages/, JobDrawer, domain.ts, utils.ts
```

---

## 13. Running things locally — command reference

```bash
# One-time, per workspace
python3 setup/bootstrap_workspace.py --profile dev [--grant-to you@example.com]

# Run the queue-driven loop directly (same code the orchestrator job runs)
python3 -m src.orchestration.batch_loop --profile dev [--max-batches N] [--pipeline-ids job_01,job_02]

# Deploy + run a human-edited fix for one job, skipping CODEGEN
python3 -m src.orchestration.deploy_manual_code --profile dev --pipeline-id job_01_... \
    --code-volume-path /Volumes/.../job.py [--pip-dependencies openpyxl]

# Compute real destination-side validation stats
python3 -m src.orchestration.run_validation --profile dev [--pipeline-ids job_01,job_02]

# Self-improvement cycle
python3 -m src.orchestration.improvement_cycle --profile dev [--since-timestamp ...]

# Unit tests (zero live Databricks/LLM dependency)
pytest tests/

# Dashboard app
cd apps/migration-dashboard/migration-dashboard
npm run dev                                 # local dev server, hot reload
npx tsc -b tsconfig.client.json --noEmit     # typecheck
npm run lint:ast-grep                        # AppKit lint rules
databricks apps deploy --profile dev         # build + deploy to the real app
```

All of the above need a working `~/.databrickscfg` `[dev]` profile (or
equivalent env vars) — see `CONNECTION_SETUP.md` for exactly what that
profile needs and where it must live; the actual secret value is
intentionally not in this repository.
