#!/usr/bin/env python3
"""
JSON Lines IPC Worker Library
Provides a reusable worker framework for handling JSON Lines IPC communication.
"""

import json
import sys
import time
from typing import Dict, Callable, Any, Optional

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
    
    def _default_ping_handler(self, request_id, params):
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
    
    def send_response(self, request_id, result):
        """Send a response message."""
        self.send_message({
            "id": request_id,
            "type": "response",
            "result": result
        })
    
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
    
    def send_event(self, method, params=None):
        """Send an event message."""
        self.send_message({
            "type": "event",
            "method": method,
            "params": params
        })
    
    def handle_request(self, message):
        """Handle incoming request using registered handlers."""
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})
        
        if method in self.handlers:
            try:
                self.handlers[method](request_id, params)
            except Exception as e:
                self.send_error(request_id, -1, f"Handler error: {str(e)}")
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
        
        try:
            # Read JSON Lines from stdin
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    message = json.loads(line)
                    self.handle_message(message) # Note: this is all synchronous at the moment; it needs to be made asynchronous
                except json.JSONDecodeError as e:
                    self.send_event("log", f"JSON decode error: {e}")
                except Exception as e:
                    self.send_event("log", f"Error handling message: {e}")
        
        except KeyboardInterrupt:
            pass
        except EOFError:
            pass
        
        self.send_event("log", "Python worker shutting down")