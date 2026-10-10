"""MCP integration tests use the official SDK and a real stdio subprocess."""
import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from intelligent_futures.framework import TaskRegistry, TaskService, TaskServiceError
from intelligent_futures.integrations.mcp import create_server, run_stdio

try:
    _release = tuple(int(part) for part in version("mcp").split(".")[:2])
    _HAS_MCP = (2, 3) <= _release < (3, 0)
except (PackageNotFoundError, ValueError):
    _HAS_MCP = False

if _HAS_MCP:
    from mcp import Client
    from mcp.client.session import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.shared.exceptions import MCPError


class OptionalDependencyTests(unittest.TestCase):
    def test_import_and_core_work_without_optional_dependencies(self):
        source = str(Path(__file__).resolve().parents[1] / "src")
        code = """
import sys
sys.path.insert(0, sys.argv[1])
from intelligent_futures.framework import TaskRegistry, TaskService
from intelligent_futures.integrations.mcp import create_server
assert not any(name == 'mcp' or name.startswith('mcp.') for name in sys.modules)
try:
    create_server(object())
except ImportError as exc:
    assert 'intelligent-futures[mcp]' in str(exc)
else:
    raise AssertionError('Missing optional SDK should be reported')
registry = TaskRegistry()
registry.register('answer', lambda: 42)
with TaskService(registry) as service:
    assert service.submit('answer', {})['task_id']
"""
        process = subprocess.run([sys.executable, "-S", "-c", code, source],
                                 capture_output=True, text=True, timeout=15)
        self.assertEqual(process.returncode, 0, process.stderr)

    def test_unsupported_sdk_has_install_hint(self):
        with patch("intelligent_futures.integrations.mcp.version", return_value="1.30.0"):
            with self.assertRaisesRegex(ImportError, r"mcp>=2.3,<3"):
                create_server(object())

    def test_run_stdio_closes_service_when_sdk_is_missing(self):
        registry = TaskRegistry()
        registry.register("answer", lambda: 42)
        service = TaskService(registry)
        with patch("intelligent_futures.integrations.mcp.version", side_effect=PackageNotFoundError("mcp")):
            with self.assertRaisesRegex(ImportError, r"intelligent-futures\[mcp\]"):
                run_stdio(service)
        self.assertTrue(service.snapshot()["closed"])
        with self.assertRaises(TaskServiceError) as error:
            service.submit("answer", {})
        self.assertEqual(error.exception.code, "service_closed")


@unittest.skipUnless(_HAS_MCP, 'Install intelligent-futures[mcp] to run real MCP protocol tests')
class MCPProtocolTests(unittest.IsolatedAsyncioTestCase):
    def parameters(self, marker=None, *, gate=None):
        fixture = Path(__file__).with_name("mcp_fixture.py")
        # Mirror this test interpreter's import path, not the parent's secrets.
        env = {"PYTHONPATH": os.pathsep.join(str(Path(p).resolve()) for p in sys.path if p)}
        args = [str(fixture)]
        if marker is not None or gate is not None:
            args.append(str(marker) if marker is not None else "-")
        if gate is not None:
            args.append(str(gate))
        return StdioServerParameters(command=sys.executable, args=args, env=env)

    @asynccontextmanager
    async def client(self, marker=None, *, mode="auto", gate=None):
        async with Client(self.parameters(marker, gate=gate), mode=mode, read_timeout_seconds=10, cache=None) as client:
            yield client

    def payload(self, response, *, error=False):
        self.assertEqual(response.is_error, error, response)
        self.assertEqual(len(response.content), 1)
        self.assertEqual(response.content[0].type, "text")
        value = json.loads(response.content[0].text)
        self.assertEqual(value, response.structured_content)
        return value

    async def terminal(self, client, task_id):
        deadline = asyncio.get_running_loop().time() + 8
        while asyncio.get_running_loop().time() < deadline:
            data = self.payload(await client.call_tool("get_task", {"task_id": task_id}))
            if data["status"] in {"completed", "failed", "cancelled"}:
                return data
            await asyncio.sleep(0.01)
        self.fail("Task did not reach a terminal state")

    async def running(self, client, task_id):
        deadline = asyncio.get_running_loop().time() + 8
        while asyncio.get_running_loop().time() < deadline:
            data = self.payload(await client.call_tool("get_task", {"task_id": task_id}))
            if data["status"] == "running":
                return data
            if data["status"] not in {"queued", "running"}:
                self.fail(f"Expected a running task, got {data['status']}")
            await asyncio.sleep(0.01)
        self.fail("Task did not start")

    async def test_real_initialize_discovery_and_resources(self):
        # Explicit legacy initialize tests the handshake existing MCP hosts use.
        with tempfile.TemporaryFile(mode="w+") as errlog:
            async with stdio_client(self.parameters(), errlog=errlog) as streams:
                async with ClientSession(*streams) as session:
                    initialized = await session.initialize()
                    self.assertEqual(initialized.server_info.name, "intelligent-futures")
                    self.assertIsNotNone(initialized.capabilities.tools)
                    self.assertIsNotNone(initialized.capabilities.resources)
                    tools = {tool.name: tool for tool in (await session.list_tools()).tools}
                    self.assertEqual(set(tools), {
                        "list_registered_tasks", "submit_task", "get_task", "cancel_task",
                        "forget_task", "list_tasks", "executor_metrics", "search_concepts"})
                    for name, tool in tools.items():
                        self.assertFalse(tool.input_schema["additionalProperties"])
                        self.assertEqual(tool.annotations.read_only_hint,
                                         name not in {"submit_task", "cancel_task", "forget_task"})
                    self.assertTrue(tools["submit_task"].annotations.open_world_hint)
                    self.assertTrue(tools["submit_task"].annotations.destructive_hint)
                    self.assertFalse(tools["submit_task"].annotations.idempotent_hint)
                    definitions = self.payload(await session.call_tool("list_registered_tasks", {}))["tasks"]
                    add = next(item for item in definitions if item["name"] == "add")
                    self.assertEqual(add["input_schema"]["properties"]["left"]["type"], "integer")
                    resources = await session.list_resources()
                    self.assertEqual({str(item.uri) for item in resources.resources}, {
                        "intelligent-futures://concepts", "intelligent-futures://registered-tasks"})
                    for resource in resources.resources:
                        result = await session.read_resource(str(resource.uri))
                        self.assertEqual(result.contents[0].mime_type, "application/json")
                        self.assertIsInstance(json.loads(result.contents[0].text), dict)
                    with self.assertRaises(MCPError) as failure:
                        await session.read_resource("intelligent-futures://missing")
                    self.assertEqual(failure.exception.code, -32002)

    async def test_modern_client_submit_poll_result_and_stdout(self):
        async with self.client() as client:
            self.assertGreaterEqual(client.protocol_version, "2026-07-28")
            submitted = self.payload(await client.call_tool("submit_task", {
                "name": "add", "arguments": {"left": 3, "right": 4}}))
            result = await self.terminal(client, submitted["task_id"])
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["result"], 7)
            noisy = self.payload(await client.call_tool("submit_task", {"name": "noisy", "arguments": {}}))
            self.assertEqual((await self.terminal(client, noisy["task_id"]))["result"], "printed")
            listed = self.payload(await client.call_tool("list_tasks", {"limit": 1}))
            self.assertEqual(len(listed["tasks"]), 1)
            metrics = self.payload(await client.call_tool("executor_metrics", {}))
            self.assertIsInstance(metrics, dict)
            concepts = self.payload(await client.call_tool("search_concepts", {"status": "planned", "limit": 1}))
            self.assertEqual(len(concepts["concepts"]), 1)
            self.assertGreaterEqual(concepts["total"], 1)
            self.assertEqual(concepts["concepts"][0]["status"], "planned")

    async def test_nonblocking_saturation_cancel_and_forget(self):
        with tempfile.TemporaryDirectory() as directory:
            await self.check_nonblocking_queue(Path(directory) / "release")

    async def check_nonblocking_queue(self, gate):
        async with self.client(gate=gate) as client:
            first = self.payload(await client.call_tool("submit_task", {"name": "hold", "arguments": {"seconds": 1.5}}))
            # A response while this task is running proves submit did not wait.
            await self.running(client, first["task_id"])
            queued = self.payload(await client.call_tool("submit_task", {"name": "add", "arguments": {"left": 2}}))
            self.assertEqual(queued["status"], "queued")
            saturated = self.payload(await client.call_tool("submit_task", {"name": "add", "arguments": {"left": 9}}), error=True)
            self.assertEqual(saturated["error"]["code"], "busy")
            unfinished = self.payload(await client.call_tool("forget_task", {"task_id": first["task_id"]}), error=True)
            self.assertEqual(unfinished["error"]["code"], "task_not_finished")
            cancelled = self.payload(await client.call_tool("cancel_task", {"task_id": queued["task_id"]}))
            self.assertTrue(cancelled["cancel_accepted"])
            self.assertEqual(cancelled["status"], "cancelled")
            running = self.payload(await client.call_tool("cancel_task", {"task_id": first["task_id"]}))
            self.assertFalse(running["cancel_accepted"])
            self.assertEqual(running["status"], "running")
            forgotten = self.payload(await client.call_tool("forget_task", {"task_id": queued["task_id"]}))
            self.assertTrue(forgotten["forgotten"])
            self.payload(await client.call_tool("get_task", {"task_id": queued["task_id"]}), error=True)
            gate.touch()
            self.assertEqual((await self.terminal(client, first["task_id"]))["result"], "finished")

    async def test_validation_and_unknown_targets_are_tool_errors(self):
        invalid = [
            ("no_such_tool", {}),
            ("get_task", {}),
            ("get_task", {"task_id": ""}),
            ("get_task", {"task_id": 42}),
            ("get_task", {"task_id": "missing"}),
            ("cancel_task", {"task_id": "missing"}),
            ("forget_task", {"task_id": "missing"}),
            ("executor_metrics", {"unexpected": True}),
            ("list_tasks", {"limit": True}),
            ("list_tasks", {"limit": 1.0}),
            ("list_tasks", {"limit": "1"}),
            ("list_tasks", {"limit": 0}),
            ("list_tasks", {"limit": 101}),
            ("search_concepts", {"status": "made-up"}),
            ("search_concepts", {"text": None}),
            ("submit_task", {"name": "add", "arguments": []}),
            ("submit_task", {"name": "add", "arguments": {"left": True}}),
            ("submit_task", {"name": "add", "arguments": {"left": "1"}}),
            ("submit_task", {"name": "add", "arguments": {"left": 1, "extra": 2}}),
            ("submit_task", {"name": "os.system", "arguments": {"command": "never execute"}}),
            ("submit_task", {"name": "add", "arguments": {"left": 1}, "source": "never execute"}),
            ("submit_task", {"name": "add", "arguments": {"left": "x" * 2048}}),
        ]
        async with self.client() as client:
            for name, arguments in invalid:
                with self.subTest(name=name, arguments=arguments):
                    result = self.payload(await client.call_tool(name, arguments), error=True)
                    self.assertIsInstance(result["error"]["code"], str)
                    self.assertIsInstance(result["error"]["message"], str)
            self.assertEqual(self.payload(await client.call_tool("list_tasks", {}))["tasks"], [])

    async def test_task_failure_and_serialization_are_sanitized(self):
        async with self.client() as client:
            for name in ("fail", "nonjson", "nonfinite", "surrogate", "surrogate_key", "oversized"):
                with self.subTest(name=name):
                    submitted = self.payload(await client.call_tool("submit_task", {"name": name, "arguments": {}}))
                    result = await self.terminal(client, submitted["task_id"])
                    self.assertEqual(result["status"], "failed")
                    self.assertIsNone(result["result"])
                    self.assertIsNotNone(result["error"])
                    self.assertEqual(result["error"]["code"], "execution_failed" if name == "fail" else "invalid_result")
                    self.assertNotIn("private-exception-value", json.dumps(result))
                    self.assertNotIn("object at 0x", json.dumps(result))

    async def test_retention_limit_requires_explicit_forget(self):
        async with self.client() as client:
            retained = []
            for number in range(8):
                task = self.payload(await client.call_tool("submit_task", {
                    "name": "add", "arguments": {"left": number}}))
                retained.append(task["task_id"])
                await self.terminal(client, task["task_id"])
            full = self.payload(await client.call_tool("submit_task", {
                "name": "add", "arguments": {"left": 8}}), error=True)
            self.assertEqual(full["error"]["code"], "task_capacity")
            listed = self.payload(await client.call_tool("list_tasks", {}))["tasks"]
            self.assertEqual([task["task_id"] for task in listed], retained)
            self.payload(await client.call_tool("forget_task", {"task_id": retained[0]}))
            accepted = self.payload(await client.call_tool("submit_task", {
                "name": "add", "arguments": {"left": 8}}))
            self.assertEqual((await self.terminal(client, accepted["task_id"]))["result"], 9)

    async def test_real_disconnect_closes_admission_and_cancels_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "closed.json"
            async with self.client(marker, gate=Path(directory) / "release") as client:
                first = self.payload(await client.call_tool("submit_task", {
                    "name": "hold", "arguments": {"seconds": 1.0}}))
                await self.running(client, first["task_id"])
                queued = self.payload(await client.call_tool("submit_task", {
                    "name": "add", "arguments": {"left": 10}}))
            self.assertTrue(marker.exists(), "Server lifespan must close service on stdin EOF")
            data = json.loads(marker.read_text(encoding="utf-8"))
            by_id = {task["task_id"]: task for task in data["tasks"]}
            self.assertEqual(by_id[queued["task_id"]]["status"], "cancelled")
            self.assertFalse(data["wait"])
            self.assertTrue(data["cancel_futures"])
            self.assertTrue(data["metrics"]["closed"])

    async def test_lifespan_closes_on_cancellation(self):
        registry = TaskRegistry()
        registry.register("answer", lambda: 42)
        service = TaskService(registry)
        server = create_server(service)
        started = asyncio.Event()

        async def cancelled_connection():
            async with server.lifespan(server):
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(cancelled_connection())
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(service.snapshot()["closed"])
        with self.assertRaises(TaskServiceError) as error:
            service.submit("answer", {})
        self.assertEqual(error.exception.code, "service_closed")

    async def test_run_stdio_closes_service_when_event_loop_entry_fails(self):
        service = TaskService(TaskRegistry())
        # A synchronous runner cannot nest inside an already-running loop.
        with self.assertRaises(RuntimeError):
            run_stdio(service)
        self.assertTrue(service.snapshot()["closed"])

    async def test_unexpected_service_error_is_a_sanitized_tool_result(self):
        service = TaskService(TaskRegistry())
        async with Client(create_server(service), cache=None) as client:
            with patch.object(service, "snapshot", side_effect=RuntimeError("private-error-detail")):
                data = self.payload(await client.call_tool("executor_metrics", {}), error=True)
            self.assertEqual(data["error"]["code"], "internal_error")
            self.assertNotIn("private-error-detail", json.dumps(data))


if __name__ == "__main__":
    unittest.main()
