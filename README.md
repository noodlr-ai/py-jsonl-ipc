# JSON Lines IPC Worker Library

A reusable Python library for creating JSON Lines Inter-Process Communication (IPC) workers that can communicate with parent processes through stdin/stdout.

## Features

- **Flexible Handler Registration**: Register custom handlers for different methods
- **Type Safety**: Built with type hints for better development experience
- **Error Handling**: Built-in error handling and JSON-RPC style error responses
- **Event Support**: Send events to parent processes
- **Easy Integration**: Simple API for integrating into existing projects

## Message Schema

### In

```
{
    id: string;
    method: string;
    params: any;
}
```

### Out

Response Message

```
{
    id: string;
    type: "response";
    data: any;
}
```

Progress Message

```
{
    id: string;
    type: "progress";
    data: any;
}
```

Event Message

```
{
    type: "event";
    method: string;
    data: any;
}
```

Error Message

```
{
    id: string;
    type: "error";
    error: {
        code: number;
        message: string;
        data: any;
    }
}

```

## Crib Sheet

Use `method` on:

- requests → imperative RPC name (e.g., "pipeline.run").
- notifications → descriptive topic/event (e.g., "pipeline.progress", "lifecycle.ready").

Do not use method on:

- responses → they’re correlated by id, so method is redundant.

All `messages` and `envelopes` send a timestamp. The `message` timestamp tells us when the message was sent, while the `envelope` timestamp tells us when
the business logic actually created the envelope.

The message `error` property is only set by the worker internally, all errors from the application are sent as a `data` payload
The message `warning` property is only set by the worker internally, all warnings from the application are sent as a `data` payload

**Response vs Notification Guidelines**

Use "response" for:

- Request-scoped messages that are directly answering a specific request
- Terminal messages that end a request (final results, errors)
- Partial results for long-running requests

Use "notification" for:

- Unsolicited updates not tied to a specific request
- Progress updates during request processing
- Log messages that are informational
- Events that happen independently of requests

## Testing

Full test suite
`pytest -v -s test_jsonl_ipc.py`

Individual test
`pytest -v -s test_jsonl_ipc.py::TestJSONLIPC::test_log_method`

## Pushing New Version

1. Update setup.py with new version number
2. git tag v0.0.5
3. git push origin v0.0.5

## Retag

1. git tag -f v0.0.5
2. git push origin v0.0.5 --force
