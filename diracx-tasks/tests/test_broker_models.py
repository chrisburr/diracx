"""Tests for broker message serialization and models."""

from __future__ import annotations

from diracx.tasks.plumbing.broker.models import (
    BrokerMessage,
    TaskMessage,
    TaskResult,
)


def test_task_message_roundtrip():
    msg = TaskMessage(
        task_id="abc123",
        task_name="test:MyTask",
        labels={"priority": "normal", "size": "medium"},
        args=[1, "hello", {"nested": True}],
        kwargs={},
    )

    # Serialize to BrokerMessage and back
    broker_msg = BrokerMessage.from_task_message(msg)
    assert broker_msg.task_id == "abc123"
    assert broker_msg.task_name == "test:MyTask"

    # Roundtrip
    recovered = broker_msg.to_task_message()
    assert recovered.task_id == msg.task_id
    assert recovered.task_name == msg.task_name
    assert recovered.args == msg.args
    assert recovered.labels == msg.labels


def test_task_result_from_value():
    result = TaskResult.from_value(value=42, execution_time=1.5)
    assert not result.is_err
    assert result.return_value == 42
    assert result.execution_time == 1.5
    assert result.error is None


def test_task_result_from_exception():
    try:
        raise ValueError("something went wrong")
    except ValueError as exc:
        result = TaskResult.from_exception(exc=exc, execution_time=0.1)

    assert result.is_err
    assert result.return_value is None
    assert result.error is not None
    assert result.error["type"] == "ValueError"
    assert "something went wrong" in result.error["message"]
    assert "traceback" in result.error


def test_task_result_raise_for_error():
    result = TaskResult.from_value(value="ok", execution_time=0.0)
    assert result.raise_for_error() is result

    import pytest

    error_result = TaskResult.from_exception(
        exc=RuntimeError("boom"), execution_time=0.0
    )
    with pytest.raises(Exception, match="RuntimeError: boom"):
        error_result.raise_for_error()
