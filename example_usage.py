#!/usr/bin/env python3
"""
Example usage of the JSONL IPC Worker library.
Shows different ways to use the worker in external modules.
"""

from dataclasses import dataclass
from typing import Dict, Callable, Optional, TypeAlias, Type, Any
from inspect import signature, Signature, Parameter
from jsonlipc.worker import JSONLWorker, RequestMessage, NotificationMessage
from jsonlipc.envelopes import (
    make_log_envelope, make_log_message, make_progress_envelope,
    make_error_envelope, make_result_envelope, LogMessage
)
from jsonlipc.errors import InvalidParametersError, MethodNotFoundError


@dataclass
class HandlerContext:
    """Context object passed to all handlers."""
    method: str
    request_id: str
    params: dict
    _engine: 'Engine'

    def send_progress(self, ratio: float, current: float, total: float,
                      unit: str = "", stage: str = "", message: str = "") -> None:
        """Send a progress update."""
        self._engine._send_progress(
            self.request_id, ratio, current, total, unit, stage, message)

    def send_log(self, messages: list[LogMessage], session_level: bool = False) -> None:
        """Send log messages."""
        self._engine._send_log(self.request_id, messages, session_level)

    def log_info(self, message: str, data: Optional[dict] = None) -> None:
        """Convenience method for single info log."""
        self.send_log([make_log_message("info", message, data)])

    def log_warn(self, message: str, data: Optional[dict] = None) -> None:
        """Convenience method for single warning log."""
        self.send_log([make_log_message("warn", message, data)])

    def log_error(self, message: str, data: Optional[dict] = None) -> None:
        """Convenience method for single error log."""
        self.send_log([make_log_message("error", message, data)])


DEFAULT_ERROR_MAP: Dict[Type[Exception], str] = {
    ValueError: "valueError",
    TypeError: "typeError",
    ZeroDivisionError: "zeroDivisionError",
    RuntimeError: "runtimeError",
    InvalidParametersError: "invalidParameters",
    MethodNotFoundError: "methodNotFound",
}

# Handler type: flexible function that may or may not take context
Handler = Callable[..., Any | None]


@dataclass
class HandlerInfo:
    """Cached handler metadata to avoid runtime inspection."""
    func: Handler
    sig: Signature
    expects_ctx: bool
    param_names: set[str]
    is_async: bool  # True if wrapped for async execution


class Engine:
    def __init__(self, handlers: Optional[Dict[str, Handler]] = None):
        self.worker = JSONLWorker(self.route_request)
        self.handlers: Dict[str, HandlerInfo] = {}

        # Register initial handlers if provided
        if handlers:
            for method, handler in handlers.items():
                self.register_handler(method, handler)

        # Engine explicitly registers shutdown handler
        self.register_handler("shutdown", self._handle_shutdown)

        # Engine explicitly registers ping handler
        self.register_handler("ping", self._handle_ping)

    # ========================================================================
    # Internal methods - called by HandlerContext
    # ========================================================================

    def _send_progress(self, request_id: str, ratio: float, current: float,
                       total: float, unit: str, stage: str, message: str) -> None:
        """Internal: Send progress envelope."""
        envelope = make_progress_envelope(
            request_id=request_id,
            ratio=ratio,
            current=current,
            total=total,
            unit=unit,
            stage=stage,
            message=message
        )
        self.worker.send_progress(request_id, envelope)

    def _send_log(self, request_id: str, messages: list[LogMessage],
                  session_level: bool) -> None:
        """Internal: Send log messages."""
        if session_level:
            envelope = make_log_envelope(messages)
        else:
            envelope = make_log_envelope(messages, request_id=request_id)
        self.worker.send_log(envelope)

    def _get_error_code(self, exc: Exception) -> str:
        """Engine decides how to map exceptions to error codes."""
        for exc_type, code in DEFAULT_ERROR_MAP.items():
            if isinstance(exc, exc_type):
                return code
        return "internalError"

    def register_handler(self, method: str, handler: Callable):
        """Engine manages handler registration."""
        sig = signature(handler)
        params = sig.parameters

        # Check if handler expects ctx parameter
        expects_ctx = 'ctx' in params

        # Get all parameter names (excluding ctx)
        param_names = {name for name in params.keys()}

        # For now, is_async is always False (sync execution)
        handler_info = HandlerInfo(
            func=handler,
            sig=sig,
            expects_ctx=expects_ctx,
            param_names=param_names,
            is_async=False
        )

        self.handlers[method] = handler_info

    def route_request(self, message: RequestMessage | NotificationMessage):
        """Engine's routing logic."""
        method = message.get("method")
        request_id = message.get("id") or self.worker.get_session_id()
        params = message.get("params", {})

        # Create context
        ctx = HandlerContext(
            method=method,
            request_id=request_id,
            params=params,
            _engine=self
        )

        if method in self.handlers:
            try:
                handler_info = self.handlers[method]

                # Call handler with proper parameter spreading
                try:
                    if handler_info.expects_ctx and len(handler_info.param_names) == 1:
                        # Function only takes ctx, don't spread params
                        result = handler_info.func(ctx)
                    elif handler_info.expects_ctx:
                        # Function takes ctx + other params
                        result = handler_info.func(**ctx.params, ctx=ctx)
                    else:
                        # Function doesn't take ctx, just spread params
                        result = handler_info.func(**ctx.params)
                except TypeError as e:
                    # Enhance error message with parameter information
                    required_params = [
                        name for name, param in handler_info.sig.parameters.items()
                        if param.default == Parameter.empty and name != 'ctx'
                    ]
                    provided_params = list(ctx.params.keys())
                    missing_params = [
                        p for p in required_params if p not in provided_params]
                    extra_params = [
                        p for p in provided_params if p not in handler_info.param_names]

                    error_msg = f"Invalid parameters for method '{ctx.method}'"
                    if missing_params:
                        error_msg += f"\n  Missing required parameters: {sorted(missing_params)}"
                    if extra_params:
                        error_msg += f"\n  Unexpected parameters: {sorted(extra_params)}"
                    error_msg += f"\n  Expected parameters: {sorted(handler_info.param_names - {'ctx'})}"
                    error_msg += f"\n  Provided parameters: {sorted(provided_params)}"

                    raise InvalidParametersError(error_msg) from e

                # Engine automatically sends the result
                self.worker.send_result(
                    request_id,
                    make_result_envelope(request_id, result))
            except Exception as e:
                # Engine handles errors automatically
                error_code = self._get_error_code(e)
                self.worker.send_error(request_id, make_error_envelope(
                    request_id, error_code, ""))
        else:
            self.worker.send_error(request_id, make_error_envelope(
                request_id, self._get_error_code(MethodNotFoundError()), f"Method not found: {method}"))

    def _handle_shutdown(self, reason: str = "Unknown", ctx: Optional[HandlerContext] = None):
        """Engine's shutdown handler."""
        # Tell worker to shutdown (this will trigger the shutdown notification)
        self.worker.shutdown("Shutdown requested via IPC")

        # Return result dict - Engine will send it automatically
        return {"status": "shutting down"}

    def _handle_ping(self, ctx: Optional[HandlerContext] = None):
        """Engine's ping handler - can be customized."""
        return {"response": "pong"}

    def run(self):
        """Start the worker. This is a blocking call, wrap functions in thread/process if needed."""
        self.worker.run()


# Method 1: Using function-based handlers

def add(a: float, b: float, ctx: Optional[HandlerContext] = None) -> float:
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise InvalidParametersError(f"Parameters 'a' and 'b' must be numbers")

    return a + b


def multiply(a: float = 1, b: float = 1, ctx: Optional[HandlerContext] = None) -> float:
    """Multiply two numbers."""
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    return a * b


def handle_echo(ctx: HandlerContext) -> dict:
    """Echo handler that returns the input parameters."""
    return {"echo": ctx.params}


def handle_log(ctx: HandlerContext) -> dict:
    """Test handler that sends log messages."""
    # Send session-level log (no request_id)
    ctx.send_log([make_log_message("info", "Session log message")],
                 session_level=True)

    messages = [
        make_log_message("info", "Starting log test"),
        make_log_message("warn", "This is a warning",
                         {"detail": "test warning"}),
        make_log_message("error", "This is an error", {"detail": "test error"})
    ]

    # Send request-level log (with request_id)
    ctx.send_log(messages)

    return {"status": "logs_sent", "count": len(messages)}


def handle_progress(steps: int = 5, delay: float = 0.1, ctx: Optional[HandlerContext] = None) -> dict:
    """Test handler that sends progress updates."""
    import time

    total_steps = steps

    if ctx:
        for i in range(total_steps + 1):
            ctx.send_progress(
                ratio=i / total_steps,
                current=float(i),
                total=float(total_steps),
                unit="steps",
                stage=f"step_{i}",
                message=f"Processing step {i} of {total_steps}"
            )
            if i < total_steps:
                time.sleep(delay)

    return {"status": "progress_complete", "total_steps": total_steps}


def handle_noop(ctx: Optional[HandlerContext] = None) -> None:
    """Test handler that returns None."""
    # This handler does nothing and returns None
    return None


def handle_default(ctx: HandlerContext) -> None:
    """Default handler for unrecognized methods."""
    raise MethodNotFoundError(f"Method not found: {ctx.method}")


# Create worker with initial handlers
engine = Engine({
    "add": add,
    "multiply": multiply,
    "echo": handle_echo,
    "log": handle_log,
    "progress": handle_progress,
    "noop": handle_noop,
    "default": handle_default,
})

# Method 2: Registering handlers after creation


def divide(a: float = 0, b: float = 1, ctx: Optional[HandlerContext] = None) -> float:
    """Divide two numbers."""
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        raise TypeError("Parameters 'a' and 'b' must be numbers")

    if b == 0:
        raise ZeroDivisionError("Division by zero")

    return a / b


engine.register_handler("divide", divide)

if __name__ == "__main__":
    engine.run()
