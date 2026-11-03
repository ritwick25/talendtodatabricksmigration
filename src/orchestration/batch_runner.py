"""Plain-code control-plane logic: per-job fix-cycle budget, infra-error
retry, and the batch halt rule. No LLM call makes any of these decisions
-- an LLM only classifies an error as infra-vs-code (classify_error) and
generates replacement code; everything about whether to keep going,
retry, or halt is decided here from plain counters.

Deliberately has zero Databricks/LLM imports so it can be unit-tested
with fake callables (see tests/test_batch_runner.py) -- the real
deploy/run/codegen functions get wired in by the caller.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field


@dataclass
class RunResult:
    success: bool
    is_infra_error: bool = False
    error: str | None = None
    logs: str | None = None
    error_summary: str | None = None  # short, one-line version of `error` for display


@dataclass
class JobOutcome:
    pipeline_id: str
    status: str  # COMPLETED | FAILED | INFRA_RETRY_EXHAUSTED
    attempts: list[dict] = field(default_factory=list)
    diagnosis: str | None = None
    error_summary: str | None = None  # short, one-line version of `diagnosis` for display
    spec_uncertain: bool = False
    java_uncertain: bool = False
    timing: dict = field(default_factory=dict)  # convert/run/total_duration_seconds, if available


def _hash_code(code: str) -> str:
    return hashlib.sha256(code.encode()).hexdigest()


def run_job_with_fix_cycles(
    pipeline_id: str,
    generate_code_fn,
    deploy_and_run_fn,
    max_fix_cycles: int = 2,
) -> JobOutcome:
    """Generate -> deploy -> run, with up to max_fix_cycles fix-and-rerun
    cycles (max_fix_cycles + 1 runs total). An infra error gets exactly
    one same-code retry and does NOT consume a fix cycle; if it persists,
    returns INFRA_RETRY_EXHAUSTED (the caller re-queues the job and feeds
    this into the batch's infra-failure streak -- it is never FAILED).

    generate_code_fn(previous_code: str | None, previous_error: str | None) -> str
    deploy_and_run_fn(code: str) -> RunResult
    """
    attempts: list[dict] = []
    seen_hashes: set[str] = set()
    code = generate_code_fn(None, None)
    previous_error: str | None = None

    fix_cycle = 0
    while True:
        code_hash = _hash_code(code)
        if code_hash in seen_hashes:
            # The fixer violated "never resubmit identical code" -- treat
            # as a failed fix attempt rather than looping forever on it.
            attempts.append({
                "fix_cycle": fix_cycle,
                "code_hash": code_hash,
                "result": "REJECTED_DUPLICATE_CODE",
            })
            return JobOutcome(
                pipeline_id, "FAILED", attempts,
                diagnosis="Fixer produced identical code to a previous attempt",
            )
        seen_hashes.add(code_hash)

        result = deploy_and_run_fn(code)
        attempts.append({
            "fix_cycle": fix_cycle,
            "code_hash": code_hash,
            "success": result.success,
            "is_infra_error": result.is_infra_error,
            "error": result.error,
        })

        if result.success:
            return JobOutcome(pipeline_id, "COMPLETED", attempts)

        if result.is_infra_error:
            retry_result = deploy_and_run_fn(code)
            attempts.append({
                "fix_cycle": fix_cycle,
                "code_hash": code_hash,
                "infra_retry": True,
                "success": retry_result.success,
                "is_infra_error": retry_result.is_infra_error,
                "error": retry_result.error,
            })
            if retry_result.success:
                return JobOutcome(pipeline_id, "COMPLETED", attempts)
            if retry_result.is_infra_error:
                return JobOutcome(
                    pipeline_id, "INFRA_RETRY_EXHAUSTED", attempts,
                    diagnosis=retry_result.error, error_summary=retry_result.error_summary,
                )
            # Infra retry came back as a code error instead -- fall through
            # and treat it like any other code failure, still on this same
            # fix_cycle (the infra hiccup itself never consumed a cycle).
            result = retry_result
            previous_error = result.error

        previous_error = result.error
        if fix_cycle >= max_fix_cycles:
            return JobOutcome(
                pipeline_id, "FAILED", attempts,
                diagnosis=previous_error, error_summary=result.error_summary,
            )

        fix_cycle += 1
        code = generate_code_fn(code, previous_error)


@dataclass
class BatchResult:
    outcomes: list[JobOutcome]
    completed_count: int
    failed_count: int
    halted: bool
    halt_reason: str | None


def run_batch(
    pipeline_ids: list[str],
    generate_code_fn_factory,
    deploy_and_run_fn_factory,
    max_fix_cycles: int = 2,
    halt_threshold: int = 3,
    infra_halt_threshold: int = 3,
) -> BatchResult:
    """Run one batch (already selected: exactly 5 PENDING jobs ordered by
    complexity_score, by the caller). Stops the whole batch -- and signals
    the process-level halt -- as soon as halt_threshold jobs FAIL, or
    infra_halt_threshold consecutive infra failures occur.

    generate_code_fn_factory(pipeline_id) -> generate_code_fn for that job
    deploy_and_run_fn_factory(pipeline_id) -> deploy_and_run_fn for that job
    (factories let the caller close over per-job spec/context without this
    module needing to know anything about jobs beyond their id)
    """
    outcomes: list[JobOutcome] = []
    failed_count = 0
    completed_count = 0
    infra_streak = 0
    halted = False
    halt_reason = None

    for pipeline_id in pipeline_ids:
        if halted:
            break

        outcome = run_job_with_fix_cycles(
            pipeline_id,
            generate_code_fn_factory(pipeline_id),
            deploy_and_run_fn_factory(pipeline_id),
            max_fix_cycles=max_fix_cycles,
        )
        outcomes.append(outcome)

        if outcome.status == "COMPLETED":
            completed_count += 1
            infra_streak = 0
        elif outcome.status == "FAILED":
            failed_count += 1
            infra_streak = 0
            if failed_count >= halt_threshold:
                halted = True
                halt_reason = f"{failed_count} jobs FAILED in this batch (threshold {halt_threshold})"
        elif outcome.status == "INFRA_RETRY_EXHAUSTED":
            infra_streak += 1
            if infra_streak >= infra_halt_threshold:
                halted = True
                halt_reason = f"{infra_streak} consecutive infra failures (threshold {infra_halt_threshold})"

    return BatchResult(outcomes, completed_count, failed_count, halted, halt_reason)
