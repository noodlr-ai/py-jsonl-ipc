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
    method: stirng;
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

## Pushing New Version

1. Update setup.py with new version number
2. git tag v0.0.5
3. git push origin v0.0.5
