#!/usr/bin/env python3
"""
Example usage of the JSONL IPC Worker library.
Shows different ways to use the worker in external modules.
"""

from jsonlipc import JSONLWorker

# Method 1: Using function-based handlers


def handle_math_add(request_id, params):
    """Custom add handler with validation."""
    try:
        a = params.get("a")
        b = params.get("b")

        if a is None or b is None:
            worker.send_error(request_id, -32602,
                              "Missing required parameters 'a' and 'b'")
            return

        if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
            worker.send_error(request_id, -32602,
                              "Parameters 'a' and 'b' must be numbers")
            return

        result = a + b
        worker.send_result(request_id, result)
    except Exception as e:
        worker.send_error(request_id, -1, f"Calculation error: {str(e)}")


def handle_math_multiply(request_id, params):
    """Custom multiply handler."""
    try:
        a = params.get("a", 1)
        b = params.get("b", 1)
        result = a * b
        worker.send_result(request_id, result)
    except Exception as e:
        worker.send_error(request_id, -1, str(e))


def handle_echo(request_id, params):
    """Echo handler that returns the input parameters."""
    worker.send_result(request_id, params)


def handle_log_message(request_id, params):
    """Log a message and send confirmation."""
    message = params.get("message", "")
    level = params.get("level", "info")

    # Send event to parent process
    worker.send_event("log_received", {
        "level": level,
        "message": message,
        "timestamp": __import__("time").time()
    })

    # Send response
    worker.send_result(request_id, {"status": "logged"})


def handle_default(method, request_id, params):
    """Default handler for unrecognized methods."""
    worker.send_error(request_id, -32601,
                      f"(Default Handler) Method not found: {method}")


# Create worker with initial handlers
worker = JSONLWorker({
    "add": handle_math_add,
    "multiply": handle_math_multiply,
    "echo": handle_echo,
    "log": handle_log_message,
    "default": handle_default,
})

# Method 2: Registering handlers after creation


def handle_divide(request_id, params):
    """Division handler with zero-division protection."""
    try:
        a = params.get("a", 0)
        b = params.get("b", 1)

        if b == 0:
            worker.send_error(request_id, -32001, "Division by zero")
            return

        result = a / b
        worker.send_result(request_id, result)
    except Exception as e:
        worker.send_error(request_id, -1, str(e))


worker.register_handler("divide", handle_divide)

if __name__ == "__main__":
    worker.run()
