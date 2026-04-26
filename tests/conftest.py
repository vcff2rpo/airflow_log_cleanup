from __future__ import annotations

import importlib.util
import sys
import types
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, Literal, cast

import pytest

MODULE_RELATIVE_PATH = "log_clean.py"


class VariableStore:
    """Mutable stand-in for airflow.sdk.Variable."""

    values: dict[str, str] = {}

    @classmethod
    def get(cls, key: str, default: str = "") -> str:
        return cls.values.get(key, default)


class DummyConf:
    """Small airflow.configuration.conf replacement used by tests."""

    values: dict[tuple[str, str], str | None] = {}

    @classmethod
    def get(cls, section: str, key: str, fallback: str | None = None) -> str | None:
        return cls.values.get((section, key), fallback)


class AirflowSkipException(Exception):
    """Small stand-in for airflow.exceptions.AirflowSkipException."""


class DummyDAG:
    """Minimal context-manager compatible DAG stub."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    def __enter__(self) -> DummyDAG:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> Literal[False]:
        return False


@pytest.fixture()
def airflow_stub_modules(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Install lightweight Airflow stubs before importing the module under test."""
    current_context: dict[str, Any] = {"params": {}}

    def get_current_context() -> dict[str, Any]:
        return current_context

    def task(
        func: Callable[..., Any] | None = None,
        **_task_kwargs: Any,
    ) -> Callable[..., Any] | Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(inner_func: Callable[..., Any]) -> Callable[..., Any]:
            return inner_func

        if func is None:
            return decorator

        return func

    airflow_module = types.ModuleType("airflow")
    airflow_module_any = cast(Any, airflow_module)
    airflow_module_any.__path__ = []

    exceptions_module = types.ModuleType("airflow.exceptions")
    exceptions_module_any = cast(Any, exceptions_module)
    exceptions_module_any.AirflowSkipException = AirflowSkipException

    pendulum_module = types.ModuleType("pendulum")
    pendulum_module_any = cast(Any, pendulum_module)
    pendulum_module_any.datetime = lambda *args, **kwargs: {"args": args, "kwargs": kwargs}

    configuration_module = types.ModuleType("airflow.configuration")
    configuration_module_any = cast(Any, configuration_module)
    configuration_module_any.conf = DummyConf

    sdk_module = types.ModuleType("airflow.sdk")
    sdk_module_any = cast(Any, sdk_module)
    sdk_module_any.DAG = DummyDAG
    sdk_module_any.Variable = VariableStore
    sdk_module_any.get_current_context = get_current_context
    sdk_module_any.task = task

    sdk_exceptions_module = types.ModuleType("airflow.sdk.exceptions")
    sdk_exceptions_module_any = cast(Any, sdk_exceptions_module)
    sdk_exceptions_module_any.AirflowSkipException = AirflowSkipException

    monkeypatch.setitem(sys.modules, "airflow", airflow_module)
    monkeypatch.setitem(sys.modules, "pendulum", pendulum_module)
    monkeypatch.setitem(sys.modules, "airflow.configuration", configuration_module)
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk.exceptions", sdk_exceptions_module)

    return {
        "conf": DummyConf,
        "Variable": VariableStore,
        "current_context": current_context,
        "AirflowSkipException": AirflowSkipException,
    }


@pytest.fixture()
def import_log_clean(
    airflow_stub_modules: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[Any]:
    """Import a fresh copy of log_clean.py for each test with stubbed dependencies."""
    module_path = Path(__file__).resolve().parents[1] / MODULE_RELATIVE_PATH
    module_name = "log_clean"

    VariableStore.values = {}
    base_log_folder = tmp_path_factory.mktemp("import-base-log-folder")
    DummyConf.values = {("logging", "base_log_folder"): str(base_log_folder)}
    airflow_stub_modules["current_context"].clear()
    airflow_stub_modules["current_context"].update({"params": {}})

    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to create import spec for {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    try:
        yield module
    finally:
        sys.modules.pop(module_name, None)
