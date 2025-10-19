"""
JSON Lines IPC Envelope Utilities
Shared functions for creating envelopes and error/log structures.
"""

from typing import TypedDict, Any, Literal, NotRequired, Optional
from datetime import datetime, timezone

EnvelopeKind = Literal["progress", "result", "error", "log"]
Status = Literal["queued", "running", "succeeded",
                 "failed", "cancelled", "retrying"]
LogLevel = Literal["info", "error", "warn", "debug"]


class ErrorCode(TypedDict):
    code: str
    message: str
    # Optional properties
    details: NotRequired[dict[str, Any]]


class LogMessage(TypedDict):
    level: LogLevel
    message: str
    # Optional properties
    details: NotRequired[dict[str, Any] | ErrorCode]


class ProgressData(TypedDict):
    ratio: float              # 0.0..1.0
    current: float  # processed items
    total: float   # estimated/known
    unit: str      # "items","bytes"
    # Optional properties
    stage: NotRequired[str]  # "fetch", "transform, etc."
    message: NotRequired[str]  # optional message
    eta_ms: NotRequired[int]  # estimated time to completion in ms


class ResultEnvelope(TypedDict):
    schema: Literal["envelope/v1"]
    request_id: str
    kind: Literal["result"]
    ts: str
    data: dict[str, Any] | None
    final: bool
    messages: list[LogMessage]
    # Optional properties
    # Will be injected by the worker (useful for sending partial results as multiple messages)
    seq: NotRequired[int]


class ErrorEnvelope(TypedDict):
    schema: Literal["envelope/v1"]
    request_id: str
    kind: Literal["error"]
    ts: str
    error: ErrorCode
    final: bool
    status: Status
    messages: list[LogMessage]
    # Optional properties
    details: NotRequired[dict[str, Any]]
    seq: NotRequired[int]  # Will be injected by the worker


class LogEnvelope(TypedDict):
    schema: Literal["envelope/v1"]
    kind: Literal["log"]
    ts: str
    messages: list[LogMessage]
    # Optional properties
    request_id: NotRequired[str]
    seq: NotRequired[int]  # Will be injected by the worker


class ProgressEnvelope(TypedDict):
    schema: Literal["envelope/v1"]
    request_id: str
    kind: Literal["progress"]
    ts: str
    progress: ProgressData
    status: Status
    messages: list[LogMessage]
    # Optional properties
    seq: NotRequired[int]  # Will be injected by the worker


def utcnow():
    """Get current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()

# class Envelope(TypedDict, total=False):
#     schema: Required[Literal["envelope/v1"]]
#     kind: Required[EnvelopeKind]
#     request_id: str | None
#     ts: str | None
#     data: dict[str, Any] | None
#     progress: dict[str, Any] | None
#     error: ErrorCode | None
#     final: bool | None
#     status: str | None
#     warnings: list[dict[str, Any]] | None

# def make_envelope(
#     request_id: str,
#     *,
#     kind: EnvelopeKind,
#     data: dict[str, Any] | None = None,
#     progress: dict[str, Any] | None = None,
#     error: ErrorCode | None = None,
#     final: bool = False,
#     status: str | None = None,
#     warnings: list[dict[str, Any]] | None = None
# ) -> Envelope:
#     """Create a full envelope with sequence number and timestamp."""
#     env: Envelope = {
#         "schema": "envelope/v1",
#         "request_id": request_id,
#         "kind": kind,
#         "ts": utcnow(),
#     }
#     if data is not None: env["data"] = data
#     if progress is not None: env["progress"] = progress
#     if error is not None: env["error"] = error
#     if final: env["final"] = True
#     if status: env["status"] = status
#     if warnings: env["warnings"] = warnings
#     return env


def make_error_code(code: str, message: str, details: Optional[dict[str, Any]] = None) -> ErrorCode:
    """Create a standardized error structure."""
    error_code: ErrorCode = {
        "code": code,
        "message": message,
    }
    if details is not None:
        error_code["details"] = details
    return error_code


def make_log_message(level: LogLevel, message: str, details: Optional[dict[str, Any] | ErrorCode] = None) -> LogMessage:
    """Create a standardized log message structure."""
    log_message: LogMessage = {
        "level": level,
        "message": message,
    }
    if details is not None:
        log_message["details"] = details
    return log_message


def make_progress_data(ratio: float, current: float, total: float, unit: str, stage: Optional[str] = None, message: Optional[str] = None, eta_ms: Optional[int] = None) -> ProgressData:
    """Create standardized progress data"""
    data: ProgressData = {
        "ratio": ratio,
        "current": current,
        "total": total,
        "unit": unit,
    }
    if stage is not None:
        data["stage"] = stage
    if message is not None:
        data["message"] = message
    if eta_ms is not None:
        data["eta_ms"] = eta_ms
    return data

# High-level envelope creation functions


def make_result_envelope(
    request_id: str,
    result_data: dict[str, Any] | None,
    *,
    final: bool = True,
    messages: Optional[list[LogMessage]] = None
) -> ResultEnvelope:
    """Create a result envelope."""
    env: ResultEnvelope = {
        "schema": "envelope/v1",
        "request_id": request_id,
        "kind": "result",
        "ts": utcnow(),
        "data": result_data,
        "final": final,
        "messages": messages or []
    }
    return env


def make_error_envelope(
    request_id: str,
    code: str,
    message: str,
    *,
    details: Optional[dict[str, Any]] = None,
    messages: Optional[list[LogMessage]] = None,
    status: Status = "failed",
    final: bool = True
) -> ErrorEnvelope:
    """Create an error envelope (terminal)."""
    err = make_error_code(code, message, details)
    env: ErrorEnvelope = {
        "schema": "envelope/v1",
        "request_id": request_id,
        "kind": "error",
        "ts": utcnow(),
        "error": err,
        "final": final,
        "messages": messages or [],
        "status": status,
    }
    if details is not None:
        env["details"] = details
    return env


def make_progress_envelope(
    request_id: str,
    ratio: float,
    current: float,
    total: float,
    unit: str,
    stage: Optional[str] = None,
    message: Optional[str] = None,
    eta_ms: Optional[int] = None,
    *,
    status: Status = "running",
    messages: Optional[list[LogMessage]] = None
) -> ProgressEnvelope:
    progress_data: ProgressData = make_progress_data(
        ratio, current, total, unit, stage=stage, message=message, eta_ms=eta_ms)
    """Create a progress envelope."""
    return {
        "schema": "envelope/v1",
        "request_id": request_id,
        "kind": "progress",
        "ts": utcnow(),
        "progress": progress_data,
        "status": status,
        "messages": messages or [],
    }


def make_log_envelope(
    messages: list[LogMessage],
    *,
    request_id: Optional[str] = None
) -> LogEnvelope:
    """Create a log envelope."""
    env: LogEnvelope = {
        "schema": "envelope/v1",
        "kind": "log",
        "ts": utcnow(),
        "messages": messages,
    }
    if request_id is not None:
        env["request_id"] = request_id
    return env


def make_log_error_envelope(
    code: str,
    message: str,
    *,
    details: Optional[dict[str, Any]] = None,
    request_id: Optional[str] = None,
) -> LogEnvelope:
    """Create a log envelope that wraps an error (non-terminal)"""
    err = make_error_code(code, message, details)
    log = make_log_message("error", message, err)
    env: LogEnvelope = {
        "schema": "envelope/v1",
        "kind": "log",
        "ts": utcnow(),
        "messages": [log],
    }
    if request_id is not None:
        env["request_id"] = request_id
    return env
