#!/usr/bin/env python3
"""
Example usage of the JSONL IPC Worker library.
Shows different ways to use the worker in external modules.
"""

from typing import Dict, Callable, Any
from jsonlipc.worker import JSONLWorker, PureHandler
from jsonlipc.envelopes import make_log_envelope, make_log_message, make_progress_envelope, make_error_envelope, make_result_envelope
from jsonlipc.errors import InvalidParametersError, MethodNotFoundError


# Method 1: Using function-based handlers

def add(_method: str, _request_id: str, params: Dict[str, float]) -> dict:
    a = params.get("a")
    b = params.get("b")

    if a is None or b is None:
        raise InvalidParametersError(
            f"Missing required parameters 'a' and 'b'")

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise InvalidParametersError(f"Parameters 'a' and 'b' must be numbers")

    return {"sum": a + b}


def multiply(_method: str, _request_id: str, params: Dict[str, float]) -> dict:
    """Multiply two numbers."""
    a = params.get("a", 1)
    b = params.get("b", 1)

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    return {"product": a * b}


def handle_echo(_: str, request_id: str, params: dict) -> dict:
    """Echo handler that returns the input parameters."""
    return {"echo": params}


def handle_log(_: str, request_id: str, params: dict) -> dict:
    """Test handler that sends log messages."""
    messages = [
        make_log_message("info", "Starting log test"),
        make_log_message("warn", "This is a warning",
                         {"detail": "test warning"}),
        make_log_message("error", "This is an error", {"detail": "test error"})
    ]

    # Send session-level log (no request_id)
    session_log = make_log_envelope(
        [make_log_message("info", "Session log message")])
    worker.send_log(session_log)

    # Send request-level log (with request_id)
    request_log = make_log_envelope(messages, request_id=request_id)
    worker.send_log(request_log)

    return {"status": "logs_sent", "count": len(messages)}


def handle_progress(_: str, request_id: str, params: dict) -> dict:
    """Test handler that sends progress updates."""
    import time

    total_steps = params.get("steps", 5)
    delay = params.get("delay", 0.1)

    for i in range(total_steps + 1):
        ratio = i / total_steps
        progress_env = make_progress_envelope(
            request_id=request_id,
            ratio=ratio,
            current=float(i),
            total=float(total_steps),
            unit="steps",
            stage=f"step_{i}",
            message=f"Processing step {i} of {total_steps}"
        )
        worker.send_progress(request_id, progress_env)

        if i < total_steps:
            time.sleep(delay)

    return {"status": "progress_complete", "total_steps": total_steps}


def handle_default(method: str, request_id: str, params: dict) -> dict:
    """Default handler for unrecognized methods."""
    raise MethodNotFoundError(f"Method not found: {method}")
    return {}


# Create worker with initial handlers
worker = JSONLWorker({
    "add": add,
    "multiply": multiply,
    "echo": handle_echo,
    "log": handle_log,
    "progress": handle_progress,
    "default": handle_default,
})

# Method 2: Registering handlers after creation


def divide(_method: str, _request_id: str, params: Dict[str, float]) -> dict:
    """Divide two numbers."""
    a = params.get("a", 0)
    b = params.get("b", 1)

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    if b == 0:
        raise ZeroDivisionError("Division by zero")

    return {"quotient": a / b}


worker.register_handler("divide", divide)

if __name__ == "__main__":
    worker.run()
