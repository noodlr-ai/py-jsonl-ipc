#!/usr/bin/env python3
"""
Pytest-based tests for JSONL IPC communication.
"""

import json
import subprocess
import sys
import threading
import time
import pytest
from queue import Queue


class JSONLClient:
    """Simple client to test JSONL IPC workers."""

    def __init__(self, worker_script):
        self.worker_script = worker_script
        self.process = None
        self.message_queue = Queue()
        self.request_id = 0

    def start_worker(self):
        """Start the worker process."""
        self.process = subprocess.Popen(
            [sys.executable, self.worker_script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )

        # Start thread to read responses
        self.reader_thread = threading.Thread(target=self._read_responses)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def _read_responses(self):
        """Read responses from worker in separate thread."""
        while self.process and self.process.poll() is None:
            try:
                if self.process.stdout:
                    line = self.process.stdout.readline()
                    if line:
                        message = json.loads(line.strip())
                        self.message_queue.put(message)
            except Exception as e:
                print(f"Error reading response: {e}")
                break

    def send_request(self, method, params=None):
        """Send a request to the worker."""
        self.request_id += 1
        request = {
            "id": str(self.request_id),
            "type": "request",
            "method": method,
            "params": params or {}
        }

        json_line = json.dumps(request) + "\n"
        if self.process and self.process.stdin:
            self.process.stdin.write(json_line)
            self.process.stdin.flush()

        return str(self.request_id)

    def get_response(self, timeout=2):
        """Get the next response from the worker."""
        try:
            return self.message_queue.get(timeout=timeout)
        except:
            return None

    def get_all_messages(self, timeout=2, max_messages=10):
        """Get all available messages within the timeout period."""
        messages = []
        start_time = time.time()

        while len(messages) < max_messages and (time.time() - start_time) < timeout:
            try:
                message = self.message_queue.get(timeout=0.1)
                messages.append(message)
            except:
                # No more messages available
                break

        return messages

    def stop_worker(self):
        """Stop the worker process."""
        if self.process:
            self.process.terminate()
            return self.process.wait()
        return 0


@pytest.fixture
def worker_client():
    """Fixture to provide a configured JSONLClient for testing."""
    client = JSONLClient("example_usage.py")
    client.start_worker()

    # Wait for startup and consume startup message
    time.sleep(0.5)
    startup_msg = client.get_response()

    yield client

    # Cleanup
    client.stop_worker()


class TestJSONLIPC:
    """Test class for JSONL IPC worker functionality."""

    def test_worker_startup(self, worker_client):
        """Test that worker starts up correctly."""
        # The startup message was already consumed in the fixture
        # Just verify the process is running
        assert worker_client.process is not None
        assert worker_client.process.poll() is None, "Worker process should be running"

    def test_ping(self, worker_client):
        """Test the ping method."""
        req_id = worker_client.send_request("ping")
        response = worker_client.get_response()
        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["response"] == "pong", "Ping should return 'pong'"

    def test_add_method(self, worker_client):
        """Test the add method."""
        req_id = worker_client.send_request("add", {"a": 5, "b": 3})
        response = worker_client.get_response()
        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["sum"] == 8, "5 + 3 should equal 8"

    def test_echo_method(self, worker_client):
        """Test the echo method."""
        test_data = {"hello": "world", "test": 123}
        req_id = worker_client.send_request("echo", test_data)
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["echo"] == test_data, "Echo should return the same data"

    def test_multiply_method(self, worker_client):
        """Test the multiply method."""
        req_id = worker_client.send_request("multiply", {"a": 4, "b": 7})
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["product"] == 28, "4 * 7 should equal 28"

    def test_divide_method(self, worker_client):
        """Test the divide method."""
        req_id = worker_client.send_request("divide", {"a": 15, "b": 3})
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["quotient"] == 5, "15 / 3 should equal 5"

    def test_default_handler_unknown_method(self, worker_client):
        """Test the default handler with an unknown method."""
        req_id = worker_client.send_request("unknown_method", {"test": "data"})
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get("type") == "response", "Should be an error message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"
        data = response.get("data")
        assert data["kind"] == "error", "Kind should return 'error'"
        err = data.get("error")
        assert err.get(
            "code") == "methodNotFound", "Should return 'methodNotFound' error code"
        assert "unknown_method" in err.get(
            "message", ""), "Error message should mention the method"

    def test_default_handler_consistency(self, worker_client):
        """Test that the default handler works consistently for different unknown methods."""
        req_id = worker_client.send_request(
            "nonexistent_function", {"param1": "value1"})
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get("type") == "response", "Should be an error message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"

        data = response.get("data")
        assert data["kind"] == "error", "Kind should return 'error'"
        err = data.get("error")
        assert err.get(
            "code") == "methodNotFound", "Should return 'methodNotFound' error code"
        assert "nonexistent_function" in err.get(
            "message", ""), "Error message should mention the method"

    def test_add_method_validation_missing_params(self, worker_client):
        """Test add method with missing parameters."""
        req_id = worker_client.send_request("add", {"a": 5})  # Missing 'b'
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get("type") == "response", "Should be an error message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"

        data = response.get("data")
        assert data["kind"] == "error", "Kind should return 'error'"
        err = data.get("error")
        assert err.get(
            "code") == "invalidParameters", "Should return 'invalidParameters' error code"

    def test_add_method_validation_invalid_types(self, worker_client):
        """Test add method with invalid parameter types."""
        req_id = worker_client.send_request(
            "add", {"a": "not_a_number", "b": 3})
        response = worker_client.get_response()

        assert response is not None, "Should receive a response"
        assert response.get("type") == "response", "Should be an error message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"

        data = response.get("data")
        assert data["kind"] == "error", "Kind should return 'error'"
        err = data.get("error")
        assert err.get(
            "code") == "invalidParameters", "Should return 'invalidParameters' error code"

    def test_log_method(self, worker_client):
        """Test the log method that sends log messages."""
        req_id = worker_client.send_request("log", {})

        # Collect all messages (logs + final response)
        messages = worker_client.get_all_messages(timeout=3, max_messages=5)

        # Find the response message
        response = None
        log_messages = []
        for msg in messages:
            if msg.get("type") == "response" and msg.get("id") == req_id:
                response = msg
            elif msg.get("type") == "notification" and msg.get("method") == "log":
                log_messages.append(msg)

        # Verify final response
        assert response is not None, "Should receive a response"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"

        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["status"] == "logs_sent", "Should return logs_sent status"
        assert payload["count"] == 3, "Should have sent 3 log messages"

        # Verify log messages were sent
        assert len(
            log_messages) >= 2, "Should receive at least 2 log notifications (session + request)"

        # Check that we have both session and request-scoped logs
        session_logs = [msg for msg in log_messages if msg.get("id") != req_id]
        request_logs = [msg for msg in log_messages if msg.get("id") == req_id]

        assert len(session_logs) >= 1, "Should have at least one session log"
        assert len(request_logs) >= 1, "Should have at least one request log"

        # Verify log message structure
        for log_msg in log_messages:
            assert log_msg.get("method") == "log", "Should be log notification"
            log_data = log_msg.get("data")
            assert log_data.get(
                "kind") == "log", "Log data should have kind=log"
            assert "messages" in log_data, "Log data should contain messages"
            assert isinstance(log_data["messages"],
                              list), "Messages should be a list"

    def test_progress_method(self, worker_client):
        """Test the progress method that sends progress updates."""
        steps = 3
        req_id = worker_client.send_request(
            "progress", {"steps": steps, "delay": 0.05})

        # Collect all messages with a longer timeout since we have delays
        messages = worker_client.get_all_messages(timeout=3, max_messages=15)

        # Find the response message
        response = None
        progress_messages = []
        for msg in messages:
            if msg.get("type") == "response" and msg.get("id") == req_id:
                response = msg
            elif msg.get("type") == "notification" and msg.get("method") == "progress" and msg.get("id") == req_id:
                progress_messages.append(msg)

        # Verify final response
        assert response is not None, f"Should receive a response. Got {len(messages)} messages total"
        assert response.get(
            "type") == "response", "Should be a response message"
        assert response.get(
            "id") == req_id, "Response ID should match request ID"

        data = response.get("data")
        assert data["final"] == True, "Final flag should be True"
        payload = data.get("data")
        assert payload["status"] == "progress_complete", "Should return progress_complete status"
        assert payload[
            "total_steps"] == steps, f"Should have processed {steps} steps"

        # Verify progress messages were sent
        # Should receive steps+1 progress updates (0, 1, 2, 3 for steps=3)
        expected_progress_count = steps + 1
        assert len(
            progress_messages) == expected_progress_count, f"Should receive {expected_progress_count} progress updates, got {len(progress_messages)}"

        # Verify progress message structure and sequence
        for i, progress_msg in enumerate(progress_messages):
            assert progress_msg.get(
                "method") == "progress", "Should be progress notification"
            assert progress_msg.get(
                "id") == req_id, "Progress should be request-scoped"

            progress_data = progress_msg.get("data")
            assert progress_data.get(
                "kind") == "progress", "Progress data should have kind=progress"
            assert "progress" in progress_data, "Progress data should contain progress field"

            progress_info = progress_data["progress"]
            assert progress_info["current"] == float(
                i), f"Current should be {i}"
            assert progress_info["total"] == float(
                steps), f"Total should be {steps}"
            assert progress_info["unit"] == "steps", "Unit should be steps"
            assert abs(progress_info["ratio"] - (i / steps)
                       ) < 0.001, f"Ratio should be {i/steps}"

            # Check optional fields
            assert "stage" in progress_info, "Should have stage field"
            assert "message" in progress_info, "Should have message field"


class TestWorkerScriptValidity:
    """Test class for worker script validation."""

    def test_correct_worker_script(self):
        """Test that a correct worker script starts and responds properly."""
        client = JSONLClient("example_usage.py")

        try:
            client.start_worker()
            time.sleep(0.5)

            # Check for startup message
            response = client.get_response()
            assert response is not None, "Should receive startup message"
            assert response.get(
                "type") == "notification", "Startup should be an event"
            assert response.get("method") == "ready", "Should be a ready event"

            # Test a simple ping to verify it's working
            req_id = client.send_request("ping")
            response = client.get_response()
            assert response is not None, "Should receive ping response"
            assert response.get(
                "type") == "response", "Should be a response message"
            assert response.get(
                "id") == req_id, "Response ID should match request ID"
            data = response.get("data")
            assert data["final"] == True, "Final flag should be True"
            payload = data.get("data")
            assert payload["response"] == "pong", "Ping should return 'pong'"

        finally:
            client.stop_worker()

    def test_nonexistent_worker_script(self):
        """Test that a non-existent worker script fails to start."""
        client = JSONLClient("non_existent.py")

        try:
            client.start_worker()
            time.sleep(0.5)

            # The process should exit quickly with an error
            if client.process:
                exit_code = client.process.poll()
                if exit_code is None:
                    # Wait a bit more if process hasn't exited yet
                    time.sleep(1)
                    exit_code = client.process.poll()

                assert exit_code is not None, "Non-existent script process should exit"
                assert exit_code != 0, "Non-existent script should exit with error code"
            else:
                raise AssertionError("Process did not start")

        finally:
            if client.process:
                client.stop_worker()

    def test_invalid_worker_script(self):
        """Test that an invalid worker script (non-Python file) fails appropriately."""
        client = JSONLClient("README.md")

        try:
            client.start_worker()
            time.sleep(0.5)

            # The process should either fail to start or exit quickly
            if client.process:
                # Check if process has exited with error
                exit_code = client.process.poll()
                if exit_code is None:
                    # Process is still running, wait a bit more and check again
                    time.sleep(1)
                    exit_code = client.process.poll()

                assert exit_code is not None, "Invalid script should exit"
                assert exit_code != 0, "Invalid script should exit with error code"

        except Exception:
            # This is expected for invalid scripts
            pass
        finally:
            if client.process:
                client.stop_worker()


class TestWorkerShutdown:
    """Test class for worker shutdown functionality."""

    def test_graceful_shutdown(self):
        """Test that the worker shuts down gracefully when requested."""
        client = JSONLClient("example_usage.py")
        client.start_worker()

        try:
            # Wait for startup
            time.sleep(0.5)
            startup_msg = client.get_response()  # Consume startup message

            # Send shutdown request
            req_id = client.send_request("shutdown")
            response = client.get_response()

            # Wait for shutdown response
            assert response is not None, "Should receive shutdown response"
            assert response.get(
                "type") == "response", "Should be a response message"
            assert response.get(
                "id") == req_id, "Response ID should match request ID"
            data = response.get("data")
            assert data["final"] == True, "Final flag should be True"
            payload = data.get("data")
            assert payload["status"] == "shutting down", "Shutdown should return 'shutting down'"

            # Wait for shutdown notification
            shutdown_event = client.get_response()
            assert shutdown_event is not None, "Should receive shutdown event"
            assert shutdown_event.get(
                "type") == "notification", "Should be an event message"
            assert shutdown_event.get(
                "method") == "shutdown", "Should be a shutdown event"

        finally:
            client.stop_worker()


if __name__ == "__main__":
    # Run pytest when script is executed directly
    pytest.main([__file__, "-v"])


# LEFT-OFF: pushing and tagging, switching over to fix my py-engine now...
