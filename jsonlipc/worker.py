#!/usr/bin/env python3
"""
JSON Lines IPC Worker Library
Provides a reusable worker framework for handling JSON Lines IPC communication.
"""

import json
import sys
import signal
from typing import Dict, Callable, Optional, Any, TypeVar, TypeAlias, Type
import threading
import queue
import time

from .errors import InvalidParametersError, MethodNotFoundError

from .envelopes import (
    make_error_code,
    make_error_envelope,
    make_result_envelope,
    utcnow,
    ResultEnvelope,
    ErrorEnvelope,
    ProgressEnvelope,
    LogEnvelope,
    LogMessage,
    ErrorCode,
)

DEFAULT_ERROR_MAP: Dict[Type[Exception], str] = {
    ValueError: "valueError",
    TypeError: "typeError",
    ZeroDivisionError: "zeroDivisionError",
    RuntimeError: "runtimeError",
    InvalidParametersError: "invalidParameters",
    MethodNotFoundError: "methodNotFound",
}

# Pure handler that computes and returns data
PureHandler: TypeAlias = Callable[[str, str, dict], dict]

# Wrapped handler that sends its own envelopes
HandlerFunc: TypeAlias = Callable[[str, str, dict], None]


class JSONLWorker:
    """JSON Lines IPC Worker that can be extended with custom handlers."""

    def __init__(self, handlers: Optional[Dict[str, PureHandler]] = None):
        """
        Initialize the worker with optional custom handlers.

        Args:
            handlers: Dictionary mapping method names to handler functions.
                     Handler functions should accept (request_id, params) arguments.
        """
        self.running = True
        self.handlers: Dict[str, HandlerFunc] = {
            k: self.make_handler(v) for k, v in (handlers or {}).items()}

        # Add default handlers
        self.register_handler("ping", self._default_ping_handler)
        self.register_handler("shutdown", self._default_shutdown_handler)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # Setup read thread and message queue
        self.message_queue = queue.Queue()
        self.reader_thread = None

        # Generate timestamp-based session ID at nanosecond precision
        self.session_id = f"sess_{int(time.time_ns())}"
        self.seq = 0
        self.schema = "message/v1"
        self._req_seq: dict[str, int] = {}  # per-request envelope seq

    # Setup on a separate thread to read from stdin
    def _stdin_reader(self):
        """Read from stdin in a separate thread."""
        try:
            for line in sys.stdin:
                if line:
                    self.message_queue.put(line.strip())
                else:
                    break
        except:
            pass
        finally:
            self.message_queue.put(None)  # Signal EOF

    def _default_shutdown_handler(self, _method: str, _request_id: str, _params: dict):
        """Default shutdown handler."""
        self.stop("Shutdown requested via IPC")
        return {"status": "shutting down"}

    def _default_ping_handler(self, _method: str, _request_id: str, _params: dict):
        """Default ping handler."""
        return {"response": "pong"}

    def _signal_handler(self, signum, _frame):
        """Handle shutdown signals."""
        signal_names = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}
        signal_name = signal_names.get(signum, f"signal {signum}")
        self.stop(f"Received {signal_name}")

    def stop(self, msg: str = "Python worker stopped by parent process"):
        """Stop the worker."""
        self.running = False
        # Note: we can add sys.exit(1) if we want to indicate an error exit on shutdown back to the parent process

    def register_handler(
        self,
        method: str,
        handler: PureHandler,
        error_map: Dict[Type[Exception], str] | None = None
    ):
        """
        Register a pure handler for a specific method.

        The handler will be automatically wrapped to send results and handle errors.

        Args:
            method: The method name
            handler: Pure function that takes (method, request_id, params) and returns dict
            error_map: Optional custom error mapping
        """
        self.handlers[method] = self.make_handler(handler, error_map)

    def register_raw_handler(self, method: str, handler: HandlerFunc):
        """
        Register a pre-wrapped handler for advanced use cases.

        Handler must manage its own envelope sending.
        Most users should use register_handler() instead.
        """
        self.handlers[method] = handler

    def make_handler(
        self,
        func: PureHandler,
        error_map: Dict[Type[Exception], str] | None = None
    ) -> HandlerFunc:
        """
        Wrap a pure handler function to automatically send results and handle errors.

        Most users should use register_handler() which calls this automatically.
        """
        error_mapping = {**DEFAULT_ERROR_MAP, **(error_map or {})}

        def wrapper(method: str, request_id: str, params: dict) -> None:
            try:
                result = func(method, request_id, params)
                if not isinstance(result, dict):
                    raise TypeError(
                        f"Handler must return dict, got {type(result).__name__}")
                env = make_result_envelope(request_id, result)
                self.send_result(request_id, env)
            except Exception as e:
                error_code = None
                for exc_type, code in error_mapping.items():
                    if isinstance(e, exc_type):
                        error_code = code
                        break
                if error_code is None:
                    error_code = "internalError"
                env = make_error_envelope(request_id, error_code, str(e))
                self.send_error(request_id, env)

        return wrapper

    def unregister_handler(self, method: str):
        """Unregister a handler for a specific method."""
        if method in self.handlers:
            del self.handlers[method]

    def _send_message(self, msg: dict):
        """Send a JSON Lines message to stdout."""
        self.seq += 1
        msg.setdefault("ts", utcnow())
        msg.setdefault("seq", self.seq)
        msg.setdefault("schema", self.schema)
        json_line = json.dumps(msg)
        print(json_line, flush=True)

    def _send_response(self, request_id: str, data: Any):
        """Final or intermediate response (no transport error)."""
        self._send_message({
            "id": request_id,
            "type": "response",
            "data": data
        })

    def _send_notification(self, id: str, method: str, data: Any = None):
        """Send a notification message."""
        self._send_message({
            "id": id,
            "type": "notification",
            "method": method,
            "data": data
        })

    # ---------------------- Session Method Wrappers ---------------------------
    # Note: These methods should only be called by the session worker internally
    def _send_session_error(self, err: ErrorCode):
        """Send session error."""
        self._send_message({
            "id": self.session_id,
            "type": "notification",
            "method": "error",
            "error": err
        })

    def _send_request_error(self, request_id: str, err: ErrorCode):
        """Send request error."""
        self._send_message({
            "id": request_id,
            "type": "response",
            "error": err
        })

    def _send_session_messages(self, messages: list[LogMessage]):
        """Send session messages."""
        self._send_message({
            "id": self.session_id,
            "type": "response",
            "messages": messages
        })

    def _send_request_messages(self, request_id: str, messages: list[LogMessage]):
        """Send request messages."""
        self._send_message({
            "id": request_id,
            "type": "response",
            "messages": messages
        })

    def _send_session_log(self, envelope: LogEnvelope):
        """Send a session log message."""
        self._send_notification(self.session_id, "log", envelope)

    # ---------------------- Application Method Wrappers ----------------------
    # Note: These methods are how the application communicates with the worker

    def _next_req_seq(self, request_id: str) -> int:
        self._req_seq[request_id] = self._req_seq.get(request_id, 0) + 1
        return self._req_seq[request_id]

    T = TypeVar('T', bound=ProgressEnvelope | ResultEnvelope | LogEnvelope)

    def _inject_seq(self, request_id: str, envelope: T) -> T:
        """Add sequence number to envelope if it has a request_id (i.e., it's request-scoped)."""
        if "request_id" in envelope:
            envelope["seq"] = self._next_req_seq(request_id)
        return envelope

    # RESPONSES (request-specific, terminal errors)

    def send_result(self, request_id: str, envelope: ResultEnvelope):
        """Application final/partial result"""
        envelope = self._inject_seq(request_id, envelope)
        self._send_response(request_id, envelope)

    def send_error(self, request_id: str, envelope: ErrorEnvelope):
        """Application Error"""
        self._send_response(request_id, envelope)

    # NOTIFICATIONS (information, warnings, non-terminal terminal errors)
    def send_log(self, envelope: LogEnvelope, method: str = "log"):
        """Application Log"""
        id = envelope.get("request_id", self.session_id)
        if "request_id" in envelope:
            envelope = self._inject_seq(id, envelope)
        self._send_notification(id, method, envelope)

    def send_progress(self, request_id: str, envelope: ProgressEnvelope, method: str = "progress") -> None:
        """Application Progress"""
        envelope = self._inject_seq(request_id, envelope)
        self._send_notification(request_id, method, data=envelope)

    def handle_request(self, message):
        """Handle incoming request using registered handlers."""
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})

        if method in self.handlers:
            try:
                self.handlers[method](method, request_id, params)
            except Exception as e:
                self._send_request_error(
                    request_id, make_error_code("handleError", f"Handler error: {str(e)}"))
        elif "default" in self.handlers:
            # the default handler receives the method
            self.handlers["default"](method, request_id, params)
        else:
            self._send_request_error(
                request_id, make_error_code("methodNotFound", f"Method not found: {method}"))

    def handle_message(self, message):
        """Handle incoming message."""
        msg_type = message.get("type")

        if msg_type in ["request", "notification"]:
            self.handle_request(message)
        else:
            # Ignore other message types
            pass

    def run(self):
        """Main worker loop."""
        # Send a startup event
        self._send_notification(self.session_id, "ready")

        # Start stdin reader thread
        self.reader_thread = threading.Thread(target=self._stdin_reader)
        self.reader_thread.daemon = True
        self.reader_thread.start()

        try:
            while self.running:
                try:
                    # timeout allows us to check self.running periodically
                    line = self.message_queue.get(timeout=0.1)

                    if line is None:  # EOF/shutdown signal
                        break

                    if not line:
                        continue

                    try:
                        msg = json.loads(line)
                        self.handle_message(msg)
                    except json.JSONDecodeError as e:
                        self._send_session_error(make_error_code(
                            "invalidJSON", f"JSON decode error: {e}"))
                    except Exception as e:
                        self._send_session_error(make_error_code(
                            "internalError", f"Internal error: {e}"))

                except queue.Empty:
                    continue  # Timeout, check self.running again

        except KeyboardInterrupt:
            self.stop("KeyboardInterrupt")

        # Signal reader thread to stop
        self.running = False

        # Optional: wait briefly for thread cleanup
        # Note: there is a race condition in the thread where it needs to receive a message to then check if it should stop; therefore,
        # we let the daemon thread be cleaned up automatically
        # if self.reader_thread and self.reader_thread.is_alive():
        #     self.reader_thread.join(timeout=0.5)

        self._send_notification(self.session_id, "shutdown")
