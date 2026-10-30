"""Unit tests for the plain-code halt rule and fix-cycle cap. No
Databricks or LLM calls -- everything here is fake callables, per
src/orchestration/batch_runner.py's whole reason for having zero
external imports.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.orchestration.batch_runner import RunResult, run_batch, run_job_with_fix_cycles


def always_fails_code_error(previous_code, previous_error):
    return f"code attempt after error: {previous_error}"


def always_fails_deploy(code):
    return RunResult(success=False, is_infra_error=False, error="boom")


def test_fix_cycle_cap_is_three_runs_total():
    outcome = run_job_with_fix_cycles(
        "job_x", always_fails_code_error, always_fails_deploy, max_fix_cycles=2,
    )
    assert outcome.status == "FAILED"
    # initial run + 2 fix cycles = 3 attempts, no infra retries involved
    assert len(outcome.attempts) == 3


def test_succeeds_without_consuming_all_cycles():
    calls = {"n": 0}

    def deploy(code):
        calls["n"] += 1
        return RunResult(success=(calls["n"] == 2), error=None if calls["n"] == 2 else "fail once")

    outcome = run_job_with_fix_cycles(
        "job_y", always_fails_code_error, deploy, max_fix_cycles=2,
    )
    assert outcome.status == "COMPLETED"
    assert len(outcome.attempts) == 2


def test_duplicate_code_is_rejected_not_infinite_loop():
    def generate_same_code_always(previous_code, previous_error):
        return "identical code every time"

    outcome = run_job_with_fix_cycles(
        "job_dup", generate_same_code_always, always_fails_deploy, max_fix_cycles=2,
    )
    assert outcome.status == "FAILED"
    assert "identical" in outcome.diagnosis.lower()
    # Only one real attempt -- the second generation produced the same
    # hash and was rejected immediately rather than being deployed again.
    assert len(outcome.attempts) == 2


def test_infra_error_gets_one_retry_and_does_not_consume_fix_cycle():
    calls = {"n": 0}

    def deploy(code):
        calls["n"] += 1
        return RunResult(success=False, is_infra_error=True, error="cluster timeout")

    outcome = run_job_with_fix_cycles(
        "job_infra", always_fails_code_error, deploy, max_fix_cycles=2,
    )
    assert outcome.status == "INFRA_RETRY_EXHAUSTED"
    # exactly 2 deploy calls: the original + the one same-code infra retry
    assert calls["n"] == 2
    assert len(outcome.attempts) == 2


def test_infra_error_recovers_on_retry_without_fix_cycle():
    calls = {"n": 0}

    def deploy(code):
        calls["n"] += 1
        if calls["n"] == 1:
            return RunResult(success=False, is_infra_error=True, error="transient")
        return RunResult(success=True)

    outcome = run_job_with_fix_cycles(
        "job_infra_ok", always_fails_code_error, deploy, max_fix_cycles=2,
    )
    assert outcome.status == "COMPLETED"
    assert calls["n"] == 2


def test_halt_rule_trips_at_three_failures_in_batch_of_five():
    def gen_factory(pid):
        return always_fails_code_error

    def deploy_factory(pid):
        return always_fails_deploy  # every job fails every attempt

    result = run_batch(
        ["a", "b", "c", "d", "e"], gen_factory, deploy_factory,
        max_fix_cycles=2, halt_threshold=3,
    )
    assert result.halted is True
    assert result.failed_count == 3
    # batch stops early -- only 3 of the 5 jobs were even attempted
    assert len(result.outcomes) == 3


def test_halt_rule_does_not_trip_at_two_failures():
    def deploy_factory_for(pid):
        def deploy(code):
            # a and b fail, everything else succeeds immediately
            return RunResult(success=pid not in ("a", "b"))
        return deploy

    def gen_factory(pid):
        return always_fails_code_error

    result = run_batch(
        ["a", "b", "c", "d", "e"], gen_factory, deploy_factory_for,
        max_fix_cycles=2, halt_threshold=3,
    )
    assert result.halted is False
    assert result.failed_count == 2
    assert result.completed_count == 3
    assert len(result.outcomes) == 5


def test_three_consecutive_infra_failures_halt():
    def gen_factory(pid):
        return always_fails_code_error

    def deploy_factory(pid):
        def deploy(code):
            return RunResult(success=False, is_infra_error=True, error="infra down")
        return deploy

    result = run_batch(
        ["a", "b", "c", "d", "e"], gen_factory, deploy_factory,
        max_fix_cycles=2, halt_threshold=3, infra_halt_threshold=3,
    )
    assert result.halted is True
    assert "infra" in result.halt_reason.lower()
    assert result.failed_count == 0  # infra failures never count as FAILED
