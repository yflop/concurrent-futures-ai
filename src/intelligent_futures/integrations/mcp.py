"""Optional, local stdio MCP adapter for an operator-owned task allowlist.

Only :func:`create_server` and :func:`run_stdio` load the MCP dependency. Clients
can submit registered names and JSON arguments, never source code or imports.
This module supplies no network listener, authentication, or sandbox. Registered
callables execute with the host process's privileges and may have side effects.
"""
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version
import json
from typing import TYPE_CHECKING

from ..catalog import ConceptRegistry
from ..framework import TaskServiceError

if TYPE_CHECKING:
    from ..framework import TaskService

__all__ = ["create_server", "run_stdio"]

_INSTALL = 'Install the MCP integration with: pip install "intelligent-futures[mcp]" (mcp>=2.3,<3).'
_CONCEPTS_URI = "intelligent-futures://concepts"
_TASKS_URI = "intelligent-futures://registered-tasks"


def _load_sdk():
    """Keep the standard-library-only core usable without any MCP imports."""
    try:
        release = version("mcp")
    except PackageNotFoundError as exc:
        raise ImportError(_INSTALL) from exc
    try:
        major, minor = (int(part) for part in release.split(".")[:2])
    except ValueError as exc:
        raise ImportError(_INSTALL) from exc
    if major != 2 or minor < 3:
        raise ImportError(f"Unsupported MCP SDK {release}. {_INSTALL}")
    try:
        from mcp import types
        from mcp.server import Server
        from mcp.shared.exceptions import MCPError
    except ImportError as exc:
        raise ImportError(_INSTALL) from exc
    return types, Server, MCPError


def _object_schema(properties=None, required=()):
    return {"type": "object", "properties": properties or {},
            "required": list(required), "additionalProperties": False}


def _validate(arguments, schema):
    """Validate this adapter's small schema without coercing strings or bools.

    Task callable arguments are validated separately by TaskService, against
    the registry's schema. The SDK low-level Server does not validate inputs.
    """
    if not isinstance(arguments, dict):
        raise TaskServiceError("invalid_arguments", "Tool arguments must be an object.")
    properties = schema["properties"]
    if any(key not in properties for key in arguments):
        raise TaskServiceError("invalid_arguments", "Unexpected tool argument.")
    if any(key not in arguments for key in schema["required"]):
        raise TaskServiceError("invalid_arguments", "Missing required tool argument.")
    for key, value in arguments.items():
        rule = properties[key]
        kind = rule["type"]
        if value is None and isinstance(kind, list) and "null" in kind:
            continue
        if isinstance(kind, list):
            kind = next(item for item in kind if item != "null")
        valid = ((kind == "string" and isinstance(value, str))
                 or (kind == "object" and isinstance(value, dict))
                 or (kind == "integer" and type(value) is int))
        if not valid:
            raise TaskServiceError("invalid_arguments", f"Invalid type for {key}.")
        if "minLength" in rule and len(value) < rule["minLength"]:
            raise TaskServiceError("invalid_arguments", f"{key} must not be empty.")
        if "minimum" in rule and value < rule["minimum"]:
            raise TaskServiceError("invalid_arguments", f"{key} is below its minimum.")
        if "maximum" in rule and value > rule["maximum"]:
            raise TaskServiceError("invalid_arguments", f"{key} exceeds its maximum.")
        if "enum" in rule and value not in rule["enum"]:
            raise TaskServiceError("invalid_arguments", f"Invalid value for {key}.")


def create_server(service: "TaskService"):
    """Build an official MCP SDK Server exposing one trusted TaskService.

    The server owns the service lifetime. Exiting its lifespan cancels queued
    work and closes admission. Already-running Python threads cannot be killed;
    they finish normally and may delay interpreter exit. Use one service/server
    per stdio connection. Creating the server starts no transport or task.

    The returned low-level SDK Server is useful with the official Client for
    testing. Only stdio is supported by this package's serving helper; exposing
    it on a network would require a separately designed security boundary.
    """
    types, Server, MCPError = _load_sdk()
    concepts = ConceptRegistry.bundled()
    identifier = {"type": "string", "minLength": 1}
    limit = {"type": "integer", "minimum": 1, "maximum": 100, "default": 100}
    schemas = {
        "list_registered_tasks": _object_schema(),
        "submit_task": _object_schema(
            {"name": identifier, "arguments": {"type": "object"}}, ("name", "arguments")),
        "get_task": _object_schema({"task_id": identifier}, ("task_id",)),
        "cancel_task": _object_schema({"task_id": identifier}, ("task_id",)),
        "forget_task": _object_schema({"task_id": identifier}, ("task_id",)),
        "list_tasks": _object_schema({"limit": limit}),
        "executor_metrics": _object_schema(),
        "search_concepts": _object_schema({
            "text": {"type": "string", "default": ""},
            "status": {"type": ["string", "null"],
                       "enum": ["implemented", "experimental", "planned", None]},
            "category": {"type": ["string", "null"]}, "limit": limit}),
    }
    descriptions = {
        "list_registered_tasks": "List the operator's allowed task names and JSON input schemas.",
        "submit_task": "Submit a registered task without waiting for completion. Returns a task ID and snapshot. "
                       "Tasks may have external side effects; never retry a submission automatically.",
        "get_task": "Read a task snapshot, result, or sanitized failure without waiting for completion.",
        "cancel_task": "Try to cancel a queued task. Running Python callables cannot be interrupted.",
        "forget_task": "Delete a terminal task's in-memory result and free retained-task capacity.",
        "list_tasks": "List retained task snapshots up to the requested limit.",
        "executor_metrics": "Read task-service capacity and executor metrics.",
        "search_concepts": "Search the read-only concept catalog. Planned concepts are not executable tasks.",
    }
    mutating = {"submit_task", "cancel_task", "forget_task"}
    tools = [types.Tool(
        name=name, description=descriptions[name], input_schema=schema,
        output_schema={"type": "object"},
        annotations=types.ToolAnnotations(
            read_only_hint=name not in mutating,
            destructive_hint=name in mutating,
            idempotent_hint=name != "submit_task",
            open_world_hint=name == "submit_task"),
    ) for name, schema in schemas.items()]

    def result(data, *, is_error=False):
        # Both representations have identical JSON content, including failures.
        encoded = json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        return types.CallToolResult(content=[types.TextContent(type="text", text=encoded)],
                                    structured_content=data, is_error=is_error)

    @asynccontextmanager
    async def lifespan(server):
        try:
            yield service
        finally:
            # No await here: cleanup still runs if the connection is cancelled.
            service.close(wait=False, cancel_futures=True)

    async def list_tools(ctx, params):
        return types.ListToolsResult(tools=tools)

    async def call_tool(ctx, params):
        try:
            name = params.name
            if name not in schemas:
                raise TaskServiceError("unknown_tool", "Unknown tool name.")
            arguments = {} if params.arguments is None else params.arguments
            _validate(arguments, schemas[name])
            if name == "list_registered_tasks":
                data = {"tasks": service.list_definitions()}
            elif name == "submit_task":
                data = service.submit(arguments["name"], arguments["arguments"])
            elif name == "get_task":
                data = service.get(arguments["task_id"])
            elif name == "cancel_task":
                data = service.cancel(arguments["task_id"])
            elif name == "forget_task":
                data = service.forget(arguments["task_id"])
            elif name == "list_tasks":
                data = {"tasks": service.list_tasks(limit=arguments.get("limit", 100))}
            elif name == "executor_metrics":
                data = service.snapshot()
            else:
                matches = concepts.search(arguments.get("text", ""),
                                          status=arguments.get("status"), category=arguments.get("category"))
                data = {"concepts": matches[:arguments.get("limit", 100)], "total": len(matches)}
            return result(data)
        except TaskServiceError as exc:
            return result({"error": exc.as_dict()}, is_error=True)
        except Exception:
            # Never expose a repr, traceback, raw callable exception, or argument.
            return result({"error": {"code": "internal_error", "message": "Task service failed."}}, is_error=True)

    resources = [
        types.Resource(uri=_CONCEPTS_URI, name="concept_catalog", mime_type="application/json",
                       description="Read-only concept catalog; catalog entries never execute code."),
        types.Resource(uri=_TASKS_URI, name="registered_tasks", mime_type="application/json",
                       description="Operator-registered task names and input schemas."),
    ]

    async def list_resources(ctx, params):
        return types.ListResourcesResult(resources=resources)

    async def read_resource(ctx, params):
        uri = str(params.uri)
        if uri == _CONCEPTS_URI:
            data = {"concepts": concepts.search()}
        elif uri == _TASKS_URI:
            data = {"tasks": service.list_definitions()}
        else:
            raise MCPError(-32002, "Unknown resource.")
        return types.ReadResourceResult(contents=[types.TextResourceContents(
            uri=uri, mime_type="application/json",
            text=json.dumps(data, ensure_ascii=False, allow_nan=False))])

    return Server(
        "intelligent-futures", lifespan=lifespan,
        instructions="Only operator-registered task names are executable. Submit once, then poll get_task. "
                     "Task IDs and results are local, in-memory, and lost when this server exits. "
                     "This is a trusted local stdio service, not a sandbox.",
        on_list_tools=list_tools, on_call_tool=call_tool,
        on_list_resources=list_resources, on_read_resource=read_resource,
        get_tool_input_schema=schemas.get,
    )


def run_stdio(service: "TaskService") -> None:
    """Serve until stdin closes; no socket or public listener is opened.

    Call from a normal synchronous main guard. The official SDK reserves stdin
    and stdout while serving and diverts task prints to stderr. The service is
    closed on disconnect, cancellation, or transport startup failure.
    """
    try:
        server = create_server(service)
        import anyio
        from mcp.server.stdio import stdio_server

        async def serve():
            async with stdio_server() as (read_stream, write_stream):
                await server.run(read_stream, write_stream, server.create_initialization_options())

        anyio.run(serve)
    finally:
        service.close(wait=False, cancel_futures=True)
