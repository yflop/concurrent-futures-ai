"""Registered callable discovery and bounded in-process task lifecycle."""
import ast
from collections import Counter
from concurrent.futures import CancelledError
from dataclasses import dataclass, field
import inspect
import json
import math
import re
import threading
import time
import types
import typing
import uuid

from ..executor import BackpressureError, IntelligentExecutor
from ..policies import TaskHints


class TaskServiceError(Exception):
    """An intentionally safe public error, with no underlying exception text."""

    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(message)

    def as_dict(self):
        return {"code": self.code, "message": self.message}


def _error(code, message):
    return TaskServiceError(code, message)


def _json_text(value, maximum):
    """Validate exact JSON types, bounded size/depth, and detach mutable values.

    Refuse Python's permissive tuple, non-string key, NaN and custom-class
    conversions. Checking before encoding avoids calling object serialization
    hooks, and also puts a finite bound on traversing very large containers.
    """
    ancestors = set()
    nodes = 0

    def visit(item, depth):
        nonlocal nodes
        nodes += 1
        if nodes > maximum or depth > 64:
            raise ValueError("JSON size or nesting limit exceeded")
        kind = type(item)
        if kind in (str, int, float, bool) or item is None:
            if kind is str:
                if len(item) > maximum:
                    raise ValueError("JSON string exceeds size limit")
                # Escaping a lone UTF-16 surrogate makes syntactic JSON, but
                # downstream UTF-8 transports cannot serialize that value.
                item.encode("utf-8", errors="strict")
            if kind is int and item.bit_length() > maximum * 4:
                raise ValueError("JSON integer exceeds size limit")
            if kind is float and not math.isfinite(item):
                raise ValueError("JSON numbers must be finite")
            return
        if kind not in (list, dict):
            raise ValueError("unsupported JSON value")
        identity = id(item)
        if identity in ancestors or len(item) > maximum:
            raise ValueError("cyclic or oversized JSON container")
        ancestors.add(identity)
        try:
            if kind is dict:
                for key, child in item.items():
                    if type(key) is not str or len(key) > maximum:
                        raise ValueError("JSON object keys must be bounded strings")
                    key.encode("utf-8", errors="strict")
                    visit(child, depth + 1)
            else:
                for child in item:
                    visit(child, depth + 1)
        finally:
            ancestors.remove(identity)

    visit(value, 0)
    encoder = json.JSONEncoder(ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    chunks, size = [], 0
    for chunk in encoder.iterencode(value):
        size += len(chunk)  # ensure_ascii means character count equals UTF-8 bytes.
        if size > maximum:
            raise ValueError("JSON encoded size limit exceeded")
        chunks.append(chunk)
    return "".join(chunks)


def _string_schema(annotation):
    """Parse a small annotation grammar without eval/get_type_hints/imports."""
    try:
        tree = ast.parse(annotation, mode="eval").body
    except (SyntaxError, ValueError, RecursionError):
        raise ValueError("unsupported task parameter annotation") from None

    names = {
        "str": {"type": "string"}, "int": {"type": "integer"},
        "float": {"type": "number"}, "bool": {"type": "boolean"},
        "None": {"type": "null"}, "NoneType": {"type": "null"},
        "list": {"type": "array", "items": {}},
        "List": {"type": "array", "items": {}},
        "dict": {"type": "object", "additionalProperties": {}},
        "Dict": {"type": "object", "additionalProperties": {}},
        "Any": {},
    }

    def name(node):
        if isinstance(node, ast.Name):
            return node.id
        if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                and node.value.id == "typing"):
            return node.attr
        return None

    def parse(node, depth=0):
        if depth > 32:
            raise ValueError("task annotation nesting limit exceeded")
        label = name(node)
        if label in names:
            return names[label].copy()
        if isinstance(node, ast.Constant) and node.value is None:
            return {"type": "null"}
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return {"anyOf": [parse(node.left, depth + 1), parse(node.right, depth + 1)]}
        if isinstance(node, ast.Subscript):
            label = name(node.value)
            args = list(node.slice.elts) if isinstance(node.slice, ast.Tuple) else [node.slice]
            if label in ("list", "List") and len(args) == 1:
                return {"type": "array", "items": parse(args[0], depth + 1)}
            if label in ("dict", "Dict") and len(args) == 2 and name(args[0]) == "str":
                return {"type": "object", "additionalProperties": parse(args[1], depth + 1)}
            if label == "Optional" and len(args) == 1:
                return {"anyOf": [parse(args[0], depth + 1), {"type": "null"}]}
            if label == "Union" and len(args) >= 2:
                return {"anyOf": [parse(arg, depth + 1) for arg in args]}
        raise ValueError("unsupported task parameter annotation")

    return parse(tree)


def _schema(annotation):
    if annotation is inspect.Parameter.empty or annotation is typing.Any:
        return {}
    if type(annotation) is str:
        if len(annotation) > 4096:
            raise ValueError("task annotation exceeds size limit")
        return _string_schema(annotation)
    primitives = {str: "string", int: "integer", float: "number", bool: "boolean",
                  type(None): "null", None: "null"}
    # Identity comparisons avoid invoking hash/equality hooks on annotations.
    for primitive, kind in primitives.items():
        if annotation is primitive:
            return {"type": kind}
    if annotation is list:
        return {"type": "array", "items": {}}
    if annotation is dict:
        return {"type": "object", "additionalProperties": {}}
    origin, args = typing.get_origin(annotation), typing.get_args(annotation)
    if origin is list and len(args) == 1:
        return {"type": "array", "items": _schema(args[0])}
    if origin is dict and len(args) == 2 and args[0] is str:
        return {"type": "object", "additionalProperties": _schema(args[1])}
    if origin is typing.Union or origin is types.UnionType:
        return {"anyOf": [_schema(arg) for arg in args]}
    raise ValueError("unsupported task parameter annotation")


def _matches(value, schema):
    if "anyOf" in schema:
        return any(_matches(value, branch) for branch in schema["anyOf"])
    kind = schema.get("type")
    if kind is None:
        return True
    if kind == "null":
        return value is None
    if kind == "integer":
        return type(value) is int
    if kind == "number":
        return type(value) in (int, float)
    if kind == "boolean":
        return type(value) is bool
    if kind == "string":
        return type(value) is str
    if kind == "array":
        return type(value) is list and all(_matches(item, schema["items"]) for item in value)
    if kind == "object":
        return type(value) is dict and all(
            _matches(item, schema["additionalProperties"]) for item in value.values())
    return False


@dataclass(frozen=True)
class TaskDefinition:
    """Immutable public discovery metadata; input_schema is a detached copy."""
    name: str
    description: str
    _schema_json: str = field(repr=False)

    @property
    def input_schema(self):
        return json.loads(self._schema_json)

    def as_dict(self):
        return {"name": self.name, "description": self.description,
                "input_schema": self.input_schema}


@dataclass(frozen=True)
class _Registration:
    definition: TaskDefinition
    function: typing.Callable
    signature: inspect.Signature
    hints: TaskHints
    defaults_json: str


class TaskRegistry:
    """An explicit callable allowlist, never a module or function-name loader.

    Parameters can be unannotated (any JSON value), int, float, str, bool, None,
    list, dict, typed lists/string-keyed dicts, or unions of those types.
    Unsupported annotations, variadic and positional-only parameters fail at
    registration. Return annotations are informational and are not inspected;
    every returned value is checked for JSON safety and size at runtime.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._registrations = {}

    def register(self, name, function, *, description="", hints=None):
        if type(name) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]{0,63}", name) is None:
            raise ValueError("task name must be 1-64 ASCII identifier characters")
        if not callable(function):
            raise TypeError("task function must be callable")
        if inspect.iscoroutinefunction(function) or inspect.isgeneratorfunction(function):
            raise ValueError("task function must be synchronous")
        if type(description) is not str or len(description) > 4096:
            raise ValueError("description must be a string of at most 4096 characters")
        try:
            description.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ValueError("description must contain valid Unicode scalar values") from None
        if hints is None:
            hints = TaskHints(key=name)
        if not isinstance(hints, TaskHints):
            raise TypeError("hints must be TaskHints")
        try:
            hints.key.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ValueError("hints.key must contain valid Unicode scalar values") from None
        try:
            signature = inspect.signature(function, eval_str=False)
        except (TypeError, ValueError):
            raise ValueError("task function must have an inspectable signature") from None
        properties, required, defaults = {}, [], {}
        for parameter in signature.parameters.values():
            if parameter.kind not in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY):
                raise ValueError("task parameters must accept named arguments without variadics")
            schema = _schema(parameter.annotation)
            if parameter.default is parameter.empty:
                required.append(parameter.name)
            else:
                try:
                    default = json.loads(_json_text(parameter.default, 65536))
                except (ValueError, TypeError, OverflowError, RecursionError):
                    raise ValueError("task defaults must be bounded JSON values") from None
                if not _matches(default, schema):
                    raise ValueError("task default does not match its annotation")
                defaults[parameter.name] = default
                schema = dict(schema, default=default)
            properties[parameter.name] = schema
        schema = {"type": "object", "properties": properties,
                  "required": required, "additionalProperties": False}
        definition = TaskDefinition(name, description, json.dumps(schema, allow_nan=False))
        registration = _Registration(definition, function, signature, hints,
                                     _json_text(defaults, 65536))
        with self._lock:
            if name in self._registrations:
                raise ValueError("task name is already registered")
            self._registrations[name] = registration
        return definition

    def list(self):
        with self._lock:
            return [self._registrations[name].definition for name in sorted(self._registrations)]

    def _freeze(self):
        with self._lock:
            return dict(self._registrations)


@dataclass
class _Record:
    task_id: str
    name: str
    submitted_at: float
    status: str = "queued"
    started_at: typing.Optional[float] = None
    completed_at: typing.Optional[float] = None
    result_json: typing.Optional[str] = None
    error: typing.Optional[dict] = None


@dataclass(frozen=True)
class _Outcome:
    result_json: typing.Optional[str] = None
    error: typing.Optional[dict] = None


class TaskService:
    """Bounded retained records and physical in-flight work; threads only.

    Submission never waits for admission. max_pending includes cancelled jobs
    until their queue slots are actually drained, even if forgotten. Cancelling
    queued work prevents invocation; it cannot stop a running Python callable.
    max_tasks counts retained records, which are never silently evicted.

    Limits apply to compact, ASCII-escaped JSON encoded as UTF-8. All strings
    and object keys must be UTF-8 encodable Unicode scalar values; lone UTF-16
    surrogates are rejected even when JSON could escape them. Input values,
    including defaults, are detached before invocation. Results are stored only
    as bounded JSON; exception text and tracebacks are never returned or kept in
    retained records. Trusted callables can still allocate arbitrary memory or
    run forever: threads are not a sandbox or a hard timeout mechanism.
    """

    def __init__(self, registry, *, max_workers=4, max_pending=64, max_tasks=256,
                 max_input_bytes=65536, max_result_bytes=1048576):
        if not isinstance(registry, TaskRegistry):
            raise TypeError("registry must be TaskRegistry")
        for name, value in (("max_workers", max_workers), ("max_pending", max_pending),
                            ("max_tasks", max_tasks), ("max_input_bytes", max_input_bytes),
                            ("max_result_bytes", max_result_bytes)):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._definitions = registry._freeze()
        self._max_tasks = max_tasks
        self._max_pending = max_pending
        self._max_input_bytes = max_input_bytes
        self._max_result_bytes = max_result_bytes
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._records = {}
        self._active = {}
        self._closed = False
        self._counts = dict(submitted=0, completed=0, failed=0, cancelled=0,
                            rejected=0, forgotten=0)
        self._executor = IntelligentExecutor(max_workers=max_workers, process_workers=0,
            max_pending=max_pending, policy="threads", submit_timeout=0)

    def list_definitions(self):
        return [self._definitions[name].definition.as_dict() for name in sorted(self._definitions)]

    def _arguments(self, registration, arguments):
        if type(arguments) is not dict:
            raise _error("invalid_arguments", "Task arguments must be a JSON object.")
        try:
            detached = json.loads(_json_text(arguments, self._max_input_bytes))
            registration.signature.bind(**detached)
            schemas = registration.definition.input_schema["properties"]
            if any(not _matches(value, schemas[name]) for name, value in detached.items()):
                raise ValueError("argument type mismatch")
            values = json.loads(registration.defaults_json)
            values.update(detached)
            # Defaults also consume the per-invocation bound.
            return json.loads(_json_text(values, self._max_input_bytes))
        except (ValueError, TypeError, OverflowError, RecursionError, KeyError):
            raise _error("invalid_arguments", "Task arguments do not match the schema or JSON limits.") from None

    def submit(self, name, arguments):
        registration = self._definitions.get(name) if type(name) is str else None
        if registration is None:
            raise _error("unknown_task", "No task is registered with that name.")
        values = self._arguments(registration, arguments)
        with self._lock:
            if self._closed:
                raise _error("service_closed", "The task service is closed.")
            if len(self._records) >= self._max_tasks:
                self._counts["rejected"] += 1
                raise _error("task_capacity", "Retained task limit reached; forget completed tasks first.")
            if len(self._active) >= self._max_pending:
                self._counts["rejected"] += 1
                raise _error("busy", "Task admission limit reached; try again after work drains.")
            record = _Record(uuid.uuid4().hex, name, time.time())
            self._records[record.task_id] = record
            self._active[record.task_id] = record
            self._counts["submitted"] += 1
            try:
                future = self._executor.submit_task(self._run, record, registration, values,
                                                    hints=registration.hints)
            except BaseException as exc:
                del self._records[record.task_id]
                del self._active[record.task_id]
                self._counts["submitted"] -= 1
                self._counts["rejected"] += 1
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                code = "busy" if isinstance(exc, BackpressureError) else "submission_failed"
                raise _error(code, "The task could not be admitted.") from None
            future.add_done_callback(lambda done: self._done(record, done))
            return self._view(record)

    def _run(self, record, registration, values):
        with self._lock:
            if record.status == "cancelled":
                return _Outcome()
            record.status = "running"
            record.started_at = time.time()
        try:
            result = registration.function(**values)
        except BaseException:
            return _Outcome(error={"code": "execution_failed", "message": "The registered task failed."})
        try:
            return _Outcome(result_json=_json_text(result, self._max_result_bytes))
        except BaseException:
            # Avoid leaked unawaited-coroutine warnings for accidental async results.
            if inspect.iscoroutine(result):
                try:
                    result.close()
                except BaseException:
                    pass
            return _Outcome(error={"code": "invalid_result",
                                   "message": "Task result is not supported JSON or exceeds its limits."})

    def _done(self, record, future):
        with self._condition:
            self._active.pop(record.task_id, None)
            if record.status != "cancelled":
                try:
                    outcome = future.result()
                except CancelledError:
                    self._mark_cancelled(record)
                except BaseException:
                    record.error = {"code": "execution_failed", "message": "The registered task failed."}
                    record.status = "failed"
                    record.completed_at = time.time()
                    self._counts["failed"] += 1
                else:
                    record.error = outcome.error
                    record.result_json = outcome.result_json
                    record.status = "failed" if outcome.error is not None else "completed"
                    record.completed_at = time.time()
                    self._counts[record.status] += 1
            self._condition.notify_all()

    def _view(self, record):
        return {"task_id": record.task_id, "name": record.name, "status": record.status,
                "submitted_at": record.submitted_at, "started_at": record.started_at,
                "completed_at": record.completed_at,
                "result": json.loads(record.result_json) if record.result_json is not None else None,
                "error": dict(record.error) if record.error is not None else None}

    def _find(self, task_id):
        record = self._records.get(task_id) if type(task_id) is str else None
        if record is None:
            raise _error("task_not_found", "No retained task exists with that ID.")
        return record

    def get(self, task_id):
        with self._lock:
            return self._view(self._find(task_id))

    def _mark_cancelled(self, record):
        record.status = "cancelled"
        record.completed_at = time.time()
        self._counts["cancelled"] += 1

    def cancel(self, task_id):
        with self._lock:
            record = self._find(task_id)
            accepted = record.status in ("queued", "cancelled")
            if record.status == "queued":
                self._mark_cancelled(record)
                # Do not Future.cancel(): its physical work item would remain in
                # ThreadPoolExecutor's queue while releasing admission capacity.
            return dict(self._view(record), cancel_accepted=accepted)

    def forget(self, task_id):
        with self._lock:
            record = self._find(task_id)
            if record.status not in ("completed", "failed", "cancelled"):
                raise _error("task_not_finished", "Only finished task records can be forgotten.")
            del self._records[task_id]
            self._counts["forgotten"] += 1
            return {"task_id": task_id, "forgotten": True}

    def list_tasks(self, limit=100):
        """Return oldest retained records first, for bounded drain-and-forget."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise _error("invalid_limit", "Task list limit must be an integer from 1 to 100.")
        with self._lock:
            return [self._view(record) for record in list(self._records.values())[:limit]]

    def snapshot(self):
        with self._lock:
            return {"closed": self._closed, "retained": len(self._records),
                    "pending": len(self._active), "max_pending": self._max_pending,
                    "max_tasks": self._max_tasks, "max_input_bytes": self._max_input_bytes,
                    "max_result_bytes": self._max_result_bytes, "counts": dict(self._counts),
                    "statuses": dict(Counter(record.status for record in self._records.values())),
                    "executor": self._executor.snapshot()}

    def close(self, wait=True, cancel_futures=True):
        if type(wait) is not bool or type(cancel_futures) is not bool:
            raise ValueError("wait and cancel_futures must be booleans")
        with self._lock:
            self._closed = True
            if cancel_futures:
                for record in self._active.values():
                    if record.status == "queued":
                        self._mark_cancelled(record)
        # No service lock while waiting: wrappers and callbacks need it to finish.
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
        if wait:
            with self._condition:
                self._condition.wait_for(lambda: not self._active)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
