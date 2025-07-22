#!/usr/bin/env python3
"""
Simple test client to demonstrate JSONL IPC communication.
"""

# TODO: This needs to be improved to use asserts and failures; pytest or something better.

import json
import subprocess
import sys
import threading
import time
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
            "id": self.request_id,
            "type": "request",
            "method": method,
            "params": params or {}
        }
        
        json_line = json.dumps(request) + "\n"
        
        self.process.stdin.write(json_line)
        self.process.stdin.flush()

        return self.request_id
    
    def get_response(self, timeout=5):
        """Get the next response from the worker."""
        try:
            return self.message_queue.get(timeout=timeout)
        except:
            return None
    
    def stop_worker(self):
        """Stop the worker process."""
        if self.process:
            self.process.terminate()
            return self.process.wait() # returns the return code of the process
        return 0

def test_worker(worker_script):
    """Test the worker with various requests."""
    client = JSONLClient(worker_script)
    client.start_worker()
    
    try:
        # Wait for startup
        time.sleep(0.5)

        # Wait for startup message
        response = client.get_response()
        print(f"Startup message: {response}")
        
        # Test ping
        print("Testing ping...")
        req_id = client.send_request("ping")
        response = client.get_response()
        print(f"Ping response: {response}")
        
        # Test add
        print("\nTesting add...")
        req_id = client.send_request("add", {"a": 5, "b": 3})
        response = client.get_response()
        print(f"Add response: {response}")

        # Test Echo
        print("\nTesting echo...")
        req_id = client.send_request("echo", {"hello": "world", "test": 123})
        response = client.get_response()
        print(f"Echo response: {response}")

        # Test multiply
        print("\nTesting multiply...")
        req_id = client.send_request("multiply", {"a": 4, "b": 7})
        response = client.get_response()
        print(f"Multiply response: {response}")

        # Test divide
        print("\nTesting divide...")
        req_id = client.send_request("divide", {"a": 15, "b": 3})
        response = client.get_response()
        print(f"Divide response: {response}")

        # Test unknown method
        print("\nTesting unknown method...")
        req_id = client.send_request("unknown_method", {"test": "data"})
        response = client.get_response()
        print(f"Unknown method response: {response}")

        # Test graceful shutdown
        print("\nTesting graceful shutdown...")
        client.send_request("shutdown")
        response = client.get_response()
        print(f"Shutdown response: {response}")

        # Wait for any additional messages
        time.sleep(1)
        while True:
            msg = client.get_response(timeout=0.1)
            if msg is None:
                break
            print(f"Additional message: {msg}")
        
    finally:
        print("\nStopping worker...")
        code = client.stop_worker()
        print(f"Worker stopped with exit code: {code}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        worker_script = sys.argv[1]
    else:
        worker_script = "example_usage.py"
    
    print(f"Testing worker: {worker_script}")
    test_worker(worker_script)
