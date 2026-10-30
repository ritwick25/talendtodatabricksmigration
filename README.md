# Talend-to-Databricks migration system

An autonomous, self-improving system that reads Talend jobs (raw `.item`/
`.properties`/contexts/routines/Java — understood entirely by LLM agents,
no deterministic XML parser), translates them to PySpark/Lakeflow code,
deploys each as a Databricks job in dev, runs it, and fixes execution
errors within a bounded budget. Runs batch after batch until the queue is
empty or a halt rule trips.

## First-time setup / moving to a new workspace

See [setup/README.md](setup/README.md) — one script (`setup/bootstrap_workspace.py`)
provisions the catalog, schemas, volume, secret scope, and all
control-plane tables. Idempotent, safe to re-run.

## Resetting a halt

The orchestrator stops itself and sets `control.halted = true` with a
`halt_reason` when 3 jobs in a batch fail, or after 3 consecutive infra
failures. It does not resume on its own. To resume:

```sql
UPDATE talend_migration.migration.control SET halted = false, halt_reason = NULL WHERE id = 1;
```

Check `batch_log` and `attempt_log` for the halt's root cause first.

## Adding real jobs

Synthetic sample jobs (`source = 'SAMPLE'`) live under
`/Volumes/talend_migration/samples/talend_export/samples/`. Place real
Talend exports under `.../talend_export/real/` — the same ingestion
pipeline enqueues them with `source = 'REAL'`, no code changes needed.

## Adding JARs

External JARs referenced by Java routines go in
`/Volumes/talend_migration/samples/talend_export/jars/`, attached to
generated jobs as libraries.

## Adding knowledge

`knowledge/component_mappings/`, `knowledge/error_playbook/`, and
`knowledge/examples/` are the only places the self-improvement cycle (and
you) may add translation knowledge. Agents can only add files here (and
to `knowledge/examples/java/`), never edit `eval/thresholds.yml` or
anything under `src/orchestration/` or `src/evaluation/` (see
`CODEOWNERS`).

## Reading uncertainty flags

A `COMPLETED` job can still carry:
- `java_uncertain` — a Java block fell back to best-effort translation;
  check `java_block_log` for the block and tier.
- `spec_uncertain` — the VERIFIER agent never fully approved the spec;
  check `job_spec.verifier_result` for what it flagged.
- `unresolved_dependencies` — an external library had no Python/JAR
  equivalent.

`validation_status` is always `NULL` — this system does not validate
output data; that's run separately. The `lineage` table (per-job,
component- and column-level source->target mappings) exists specifically
to support that separate validation work.
