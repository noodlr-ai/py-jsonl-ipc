#!/usr/bin/env python3
"""
Example usage of the JSONL IPC Worker library.
Shows different ways to use the worker in external modules.
"""

from typing import Dict, Callable, Any
from jsonlipc.worker import JSONLWorker
from jsonlipc.envelopes import make_log_envelope, make_log_message, make_progress_envelope, make_error_envelope, make_result_envelope
from jsonlipc.errors import InvalidParametersError, MethodNotFoundError


# Method 1: Using function-based handlers

def add(_method: str, _request_id: str, params: Dict[str, float]) -> float:
    a = params.get("a")
    b = params.get("b")

    if a is None or b is None:
        raise InvalidParametersError(
            f"Missing required parameters 'a' and 'b'")

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise InvalidParametersError(f"Parameters 'a' and 'b' must be numbers")

    return a + b


def multiply(_method: str, _request_id: str, params: Dict[str, float]) -> float:
    """Multiply two numbers."""
    a = params.get("a", 1)
    b = params.get("b", 1)

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    return a * b


def handle_echo(_: str, request_id: str, params: dict):
    """Echo handler that returns the input parameters."""
    return params


def handle_log(_: str, request_id: str, params: dict):
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


def handle_progress(_: str, request_id: str, params: dict):
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


def handle_default(method: str, request_id: str, params: dict):
    """Default handler for unrecognized methods."""
    raise MethodNotFoundError(f"Method not found: {method}")


def make_handler(func: Callable[..., Any]) -> Callable[[str, str, dict], None]:
    """A convenience function so handlers can remain pure"""
    def wrapper(method: str, request_id: str, params: dict) -> None:
        try:
            result = {"result": func(method, request_id, params)}
            env = make_result_envelope(request_id, result)
            worker.send_result(request_id, env)
        except ValueError as e:
            env = make_error_envelope(request_id, "valueError", str(e))
            worker.send_error(request_id, env)
        except TypeError as e:
            env = make_error_envelope(request_id, "typeError", str(e))
            worker.send_error(request_id, env)
        except ZeroDivisionError as e:
            env = make_error_envelope(request_id, "zeroDivisionError", str(e))
            worker.send_error(request_id, env)
        except RuntimeError as e:
            env = make_error_envelope(request_id, "runtimeError", str(e))
            worker.send_error(request_id, env)
        except MethodNotFoundError as e:
            env = make_error_envelope(request_id, "methodNotFound", str(e))
            worker.send_error(request_id, env)
        except InvalidParametersError as e:
            env = make_error_envelope(request_id, "invalidParameters", str(e))
            worker.send_error(request_id, env)
        except Exception as e:
            env = make_error_envelope(
                request_id, "internalError", f"Unexpected error: {str(e)}")
            worker.send_error(request_id, env)
    return wrapper


# Create worker with initial handlers
worker = JSONLWorker({
    "add": make_handler(add),
    "multiply": make_handler(multiply),
    "echo": make_handler(handle_echo),
    "log": make_handler(handle_log),
    "progress": make_handler(handle_progress),
    "default": make_handler(handle_default),
})

# Method 2: Registering handlers after creation


def divide(_method: str, _request_id: str, params: Dict[str, float]) -> float:
    """Divide two numbers."""
    a = params.get("a", 0)
    b = params.get("b", 1)

    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    if b == 0:
        raise ZeroDivisionError("Division by zero")

    return a / b


worker.register_handler("divide", make_handler(divide))

if __name__ == "__main__":
    worker.run()
