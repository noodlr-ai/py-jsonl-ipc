#!/usr/bin/env python3
"""
JSON Lines IPC Worker Library
Provides a reusable worker framework for handling JSON Lines IPC communication.
"""

import json
import sys
import signal
from typing import Callable, Any, TypeVar, Literal, NotRequired, TypedDict, cast
import threading
import queue
import time

from .errors import InvalidParametersError, MethodNotFoundError

from .envelopes import (
    make_error_code,
    utcnow,
    ResultEnvelope,
    ErrorEnvelope,
    ProgressEnvelope,
    LogEnvelope,
    LogMessage,
    ErrorCode,
)


class RequestMessage(TypedDict):
    """Request message with required and optional fields."""
    type: Literal["request"]
    id: str
    method: str
    params: NotRequired[dict]  # Optional field


class NotificationMessage(TypedDict):
    """Notification message."""
    type: Literal["notification"]
    method: str
    params: NotRequired[dict]  # Optional field
    id: NotRequired[str]  # Optional field


class JSONLWorker:
    """JSON Lines IPC Worker that can be extended with custom handlers."""

    def __init__(self, request_handler: Callable[[RequestMessage | NotificationMessage], None]):
        """
        Initialize the worker with optional custom handlers.

        Args:
            request_handler: Function to handle incoming requests.
        """
        self.running = True
        self.request_handler = request_handler

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
                line = line.strip()  # Expecting to receive JSON Lines, remove trailing newline
                if line:
                    self.message_queue.put(line)
        except:
            pass
        finally:
            self.message_queue.put(None)  # Signal EOF

    def _notify_shutdown(self, reason: str):
        """
        Notify Engine of shutdown request via synthetic message.
        Used by both signal handlers and KeyboardInterrupt.
        """
        synthetic_message: RequestMessage = {
            "type": "request",
            "id": self.session_id,
            "method": "shutdown",
            "params": {"reason": reason}
        }

        try:
            self.request_handler(synthetic_message)
        except Exception:
            # If Engine can't handle it, force shutdown
            self.shutdown(f"{reason} - forced shutdown")

    def _signal_handler(self, signum, _frame):
        """
        Handle shutdown signals by notifying Engine via synthetic message.
        Engine decides whether/when to call worker.shutdown().
        """
        signal_names = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}
        signal_name = signal_names.get(signum, f"signal {signum}")
        self._notify_shutdown(f"Received {signal_name}")

    def shutdown(self, reason: str = "Shutdown requested"):
        """
        Public API for graceful shutdown.
        Called by Engine when it's ready to stop.
        """
        self.running = False
        # Note: we can add sys.exit(1) if we want to indicate an error exit on shutdown back to the parent process

    def get_session_id(self) -> str:
        """Get the current session ID."""
        return self.session_id

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

    def handle_message(self, message: dict):
        """Handle incoming message with protocol validation."""
        # Validate basic structure
        if not isinstance(message, dict):
            self._send_session_error(make_error_code(
                "invalidMessage", "Message must be a JSON object"))
            return

        msg_type = message.get("type")

        # Validate required fields based on type
        if msg_type == "request":
            if not self._validate_request(message):
                return
            self.request_handler(cast(RequestMessage, message))
        elif msg_type == "notification":
            if not self._validate_notification(message):
                return
            self.request_handler(cast(NotificationMessage, message))
        else:
            # Unknown type - ignore silently (could be future protocol extension)
            pass

    def _validate_request(self, message: dict) -> bool:
        """Validate request message structure."""
        if "id" not in message or not isinstance(message.get("id"), str):
            self._send_session_error(make_error_code(
                "invalidMessage", "Request must have string 'id' field"))
            return False

        if "method" not in message or not isinstance(message.get("method"), str):
            request_id = message.get("id", self.session_id)
            self._send_request_error(request_id, make_error_code(
                "invalidMessage", "Request must have string 'method' field"))
            return False

        # params is optional, but if present must be dict
        if "params" in message and not isinstance(message.get("params"), dict):
            request_id = message["id"]
            self._send_request_error(request_id, make_error_code(
                "invalidMessage", "Request 'params' must be an object"))
            return False

        return True

    def _validate_notification(self, message: dict) -> bool:
        """Validate notification message structure."""
        if "method" not in message or not isinstance(message.get("method"), str):
            self._send_session_error(make_error_code(
                "invalidMessage", "Notification must have string 'method' field"))
            return False

        if "params" in message and not isinstance(message.get("params"), dict):
            self._send_session_error(make_error_code(
                "invalidMessage", "Notification 'params' must be an object"))
            return False

        return True

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
            self._notify_shutdown("Received KeyboardInterrupt")

        # Signal reader thread to stop
        self.running = False

        # Optional: wait briefly for thread cleanup
        # Note: there is a race condition in the thread where it needs to receive a message to then check if it should stop; therefore,
        # we let the daemon thread be cleaned up automatically
        # if self.reader_thread and self.reader_thread.is_alive():
        #     self.reader_thread.join(timeout=0.5)

        self._send_notification(self.session_id, "shutdown")
