#!/usr/bin/env python3
"""
JSON Lines IPC Worker Library
Provides a reusable worker framework for handling JSON Lines IPC communication.
"""

import json
import sys
import signal
from typing import Dict, Callable, Optional, Any
import threading
import queue
import time
from datetime import datetime, timezone

def utcnow():
    return datetime.now(timezone.utc).isoformat()

class JSONLWorker:
    """JSON Lines IPC Worker that can be extended with custom handlers."""

    def __init__(self, handlers: Optional[Dict[str, Callable]] = None):
        """
        Initialize the worker with optional custom handlers.

        Args:
            handlers: Dictionary mapping method names to handler functions.
                     Handler functions should accept (request_id, params) arguments.
        """
        self.running = True
        self.handlers = handlers or {}

        # Add default handlers
        self.handlers.setdefault("ping", self._default_ping_handler)
        self.handlers.setdefault("shutdown", self._default_shutdown_handler)

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

        # Setup read thread and message queue
        self.message_queue = queue.Queue()
        self.reader_thread = None

        # Generate timestamp-based session ID at nanosecond precision
        self.session_id = f"sess_{int(time.time_ns())}"
        self.seq = 0
        self.data_schema = "message/v1"
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

    def _default_shutdown_handler(self, method: str, request_id: str, params: dict):
        """Default shutdown handler."""
        self.send_response(request_id, { "result": "shutting down" })
        self.stop("Shutdown requested via IPC")

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        signal_names = {signal.SIGTERM: "SIGTERM", signal.SIGINT: "SIGINT"}
        signal_name = signal_names.get(signum, f"signal {signum}")
        self.stop(f"Received {signal_name}")

    def stop(self, msg: str = "Python worker stopped by parent process"):
        """Stop the worker."""
        self.running = False
        # Note: we can add sys.exit(1) if we want to indicate an error exit on shutdown back to the parent process

    def _default_ping_handler(self, method: str, request_id: str, params: dict):
        """Default ping handler."""
        self.send_response(request_id, { "result": "pong" })

    def register_handler(self, method: str, handler: Callable):
        """Register a handler for a specific method."""
        self.handlers[method] = handler

    def unregister_handler(self, method: str):
        """Unregister a handler for a specific method."""
        if method in self.handlers:
            del self.handlers[method]

    def send_message(self, msg: dict):
        """Send a JSON Lines message to stdout."""
        self.seq += 1
        msg.setdefault("ts", utcnow())
        msg.setdefault("seq", self.seq)
        msg.setdefault("data_schema", self.data_schema)
        json_line = json.dumps(msg)
        print(json_line, flush=True)

    def send_response(self, request_id: str, data: Any):
        """Final or intermediate response (no transport error)."""
        self.send_message({
            "id": request_id,
            "type": "response",
            "data": data
        })

    def send_transport_error(self, request_id, code: str, message: str, details: dict | None = None):
        """Send an error message."""
        self.send_message({
            "id": request_id,
            "type": "response",
            "error": {
                "code": code,
                "message": message,
                "data": details
            }
        })

    def send_notification(self, id: str, method: str, data: Any = None):
        """Send a notification message."""
        self.send_message({
            "id": id,
            "type": "notification",
            "method": method,
            "data": data
        })

    # ---------- transport warnings (top-level) ----------
    @staticmethod
    def make_transport_warning(code: str, message: str, *, level: str = "warn", details: dict[str, Any] | None = None) -> dict:
        tw: dict[str, Any] = {"code": code, "level": level, "message": message}
        if details is not None: tw["details"] = details
        return tw

    def send_transport_warning_notify(self, request_id: str, code: str, message: str, method: str, level: str = "warn",
                                      details: dict[str, Any] | None = None):
        """Send a notification that carries ONLY transport warnings (no app payload)."""
        tw = self.make_transport_warning(code, message, level=level, details=details)
        self.send_notification(request_id, method, tw)

    
    # ---------- notification helpers ----------
    def _next_req_seq(self, request_id: str) -> int:
        self._req_seq[request_id] = self._req_seq.get(request_id, 0) + 1
        return self._req_seq[request_id]

    def make_envelope(self, request_id: str, *, kind: str,
                    data=None, progress: dict | None = None, error: dict | None = None,
                    final: bool = False, status: str | None = None, warnings: list | None = None):
        env = {
            "version": "envelope/v1",
            "request_id": request_id,
            "kind": kind,
            "seq": self._next_req_seq(request_id),   # per-request sequence
            "ts": utcnow(),
        }
        if data is not None: env["data"] = data
        if progress is not None: env["progress"] = progress
        if error is not None: env["error"] = error
        if final: env["final"] = True
        if status: env["status"] = status
        if warnings: env["warnings"] = warnings
        return env
    
    @staticmethod
    def make_app_warning(code: str, message: str, *, details: dict[str, Any] | None = None) -> dict:
        aw: dict[str, Any] = {"code": code, "message": message}
        if details is not None: aw["details"] = details
        return aw

    # ---- app warnings in NOTIFICATIONS (non-terminal) ----
    def send_app_warning_notify(self, request_id: str, method: str, code: str, message: str, details: dict[str, Any] | None = None, status: str | None = "running"):
        """Domain warning while work continues (e.g., '12 rows skipped')."""
        warn = self.make_app_warning(code, message, details=details)
        env = self.make_envelope(request_id, kind="log", warnings=[warn], status=status)
        self.send_notification(request_id, method, data=env)

    # Convenience wrappers for common envelope messages
    def send_progress(self, request_id: str, method: str, progress_data: dict[str, Any], status: str | None = "running", warnings: list[dict] | None = None):
        """Progress is a NOTIFICATION with a payload"""
        env = self.make_envelope(request_id, kind="progress", progress=progress_data, status=status, warnings=warnings)
        self.send_notification(request_id, method, data=env)

    def send_result(self, request_id: str, result_data: dict[str, Any], final: bool = True, warnings: list[dict] | None = None):
        """Final (or intermediate) result as a RESPONSE with envelope payload."""
        env = self.make_envelope(request_id, kind="result", data=result_data, final=final, warnings=warnings)
        self.send_response(request_id, env)

    def send_app_error_final(self, request_id: str, code: str, message: str, details: dict[str, Any] | None = None, warnings: list[dict] | None = None):
        """Application-level terminal error (ends the request): RESPONSE with envelope kind=error."""
        err: dict[str, Any] = {"code": code, "message": message}
        if details is not None: err["details"] = details
        env = self.make_envelope(request_id, kind="error", error=err, final=True, status="failed", warnings=warnings)
        self.send_response(request_id, env)

    def send_app_error_notify(self, request_id: str, method: str, code: str, message: str, details: dict | None = None, status: str | None = None):
        """Application-level non-terminal error (e.g., a node fails but pipeline continues): NOTIFICATION."""
        err: dict[str, Any] = {"code": code, "message": message}
        if details is not None: err["details"] = details
        env = self.make_envelope(request_id, kind="error", error=err, final=False, status=status)
        self.send_notification(request_id, method, data=env)

    def handle_request(self, message):
        """Handle incoming request using registered handlers."""
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})

        if method in self.handlers:
            try:
                self.handlers[method](method, request_id, params)
            except Exception as e:
                self.send_transport_error(request_id, "handlerError", f"Handler error: {str(e)}")
        elif "default" in self.handlers:
            # the default handler receives the method
            self.handlers["default"](method, request_id, params)
        else:
            self.send_transport_error(request_id, "methodNotFound", f"Method not found: {method}")

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
        self.send_notification(self.session_id, "ready")

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
                        self.send_notification(self.session_id, "log", { "message": f"JSON decode error: {e}" })
                    except Exception as e:
                        self.send_notification(self.session_id, "log", { "message": f"Error handling message: {e}" })

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

        self.send_notification(self.session_id, "shutdown")
