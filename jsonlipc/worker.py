#!/usr/bin/env python3
"""
JSON Lines IPC Worker Library
Provides a reusable worker framework for handling JSON Lines IPC communication.
"""

import json
import sys
import signal
from typing import Dict, Callable, Optional
import threading
import queue


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
        self.send_response(request_id, "shutting down")
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
        self.send_response(request_id, "pong")

    def register_handler(self, method: str, handler: Callable):
        """Register a handler for a specific method."""
        self.handlers[method] = handler

    def unregister_handler(self, method: str):
        """Unregister a handler for a specific method."""
        if method in self.handlers:
            del self.handlers[method]

    def send_message(self, message):
        """Send a JSON Lines message to stdout."""
        json_line = json.dumps(message)
        print(json_line, flush=True)

    def send_response(self, request_id, data):
        """Send a final response message."""
        self.send_message({
            "id": request_id,
            "type": "response",
            "data": data
        })

    def send_progress(self, request_id, progress_data):
        """Send a progress update for an ongoing request."""
        self.send_message({
            "id": request_id,
            "type": "progress",
            "data": progress_data
        })

    def send_result(self, request_id, data):
        """Alias for send_response to make intent clearer."""
        self.send_response(request_id, data)

    def send_error(self, request_id, code, message, data=None):
        """Send an error message."""
        self.send_message({
            "id": request_id,
            "type": "error",
            "error": {
                "code": code,
                "message": message,
                "data": data
            }
        })

    def send_event(self, method, data=None):
        """Send an event message."""
        self.send_message({
            "type": "event",
            "method": method,
            "data": data
        })

    def handle_request(self, message):
        """Handle incoming request using registered handlers."""
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})

        if method in self.handlers:
            try:
                self.handlers[method](method, request_id, params)
            except Exception as e:
                self.send_error(request_id, -1, f"Handler error: {str(e)}")
        elif "default" in self.handlers:
            # the default handler receives the method
            self.handlers["default"](method, request_id, params)
        else:
            self.send_error(request_id, -32601, f"Method not found: {method}")

    def handle_message(self, message):
        """Handle incoming message."""
        msg_type = message.get("type")

        if msg_type in ["request", "event"]:
            self.handle_request(message)
        else:
            # Ignore other message types
            pass

    def run(self):
        """Main worker loop."""
        # Send a startup event
        self.send_event("ready", "Python worker started")

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
                        message = json.loads(line)
                        self.handle_message(message)
                    except json.JSONDecodeError as e:
                        self.send_event("log", f"JSON decode error: {e}")
                    except Exception as e:
                        self.send_event("log", f"Error handling message: {e}")

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

        self.send_event("shutdown", "Worker stopped gracefully")
