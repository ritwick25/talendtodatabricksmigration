"""Thin shim so agent modules can use @trace(...) without a hard
dependency on mlflow being installed yet. Falls back to a no-op
decorator if mlflow isn't importable -- tracing is an observability
nice-to-have, not something that should block an agent from running.
"""
from __future__ import annotations

try:
    import os

    import mlflow

    try:
        mlflow.set_tracking_uri("databricks://dev")
        mlflow.set_experiment(
            os.environ.get("MLFLOW_EXPERIMENT_PATH", "/Shared/talend-migration-agents")
        )
    except Exception:
        # Falls back to mlflow's local default tracking store. Tracing is
        # observability, not correctness -- never let a tracking-backend
        # problem block an agent from running.
        pass

    def trace(name: str):
        return mlflow.trace(name=name)

except ImportError:
    def trace(name: str):
        def decorator(func):
            return func
        return decorator
