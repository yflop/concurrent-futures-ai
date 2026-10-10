"""Original, loopback-only network workload replicas; see docs/workloads-network.md."""
from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib
import json
from pathlib import Path
import random
import re
import shutil
import socket
import tempfile
import threading
from urllib.parse import parse_qs, urlsplit

NAMES = ("http", "cached_http", "downloads", "discovery")
TIMEOUT = 3.0
REPRESENTATIVE_SIZES = (4096, 16384, 65536, 262144, 1048576, 4194304)
RESPONSE_CHUNKS = (1024, 8192, 65536)


def _dependency(name):
    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise ImportError(f"Network workload requires optional dependency {name!r}; "
                          "install requests and requests-cache") from exc


def _local_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
            or parsed.username or parsed.password or not parsed.port):
        raise ValueError("Only explicit http://127.0.0.1:<port> fixture URLs are allowed")
    return parsed


class _Server(ThreadingHTTPServer):
    daemon_threads = False
    block_on_close = True
    request_queue_size = 128


class _Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(TIMEOUT)

    def log_message(self, *_):
        pass

    def do_GET(self):
        path = urlsplit(self.path).path
        with self.server.counter_lock:
            self.server.requests[path] = self.server.requests.get(path, 0) + 1
        if path == "/blocked":
            # Fault injection only: release on teardown, never delay normal I/O.
            self.server.release_blocked.wait(3.0)
            return
        if path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "http://example.invalid/never-follow")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if path == "/truncated":
            self.send_response(200)
            self.send_header("Content-Length", "1024")
            self.end_headers()
            self.wfile.write(b"short")
            return
        if path == "/common/basic_info":
            content = b"ret=OK,pow=1,name=Local%20fixture"
        elif path == "/failure":
            self.send_error(503, "intentional fixture failure")
            return
        elif re.fullmatch(r"/data/[a-zA-Z0-9_.-]+", path):
            target = self.server.root / path.removeprefix("/data/")
            if not target.is_file():
                self.send_error(404)
                return
            content = target.read_bytes()
        else:
            self.send_error(404)
            return
        chunk_values = parse_qs(urlsplit(self.path).query).get("chunk", ["65536"])
        try:
            response_chunk = int(chunk_values[0])
        except (ValueError, IndexError):
            response_chunk = 0
        if response_chunk not in RESPONSE_CHUNKS:
            self.send_error(400, "invalid fixture chunk size")
            return
        length = len(content)
        status = 200
        range_header = self.headers.get("Range")
        if range_header:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_header)
            if match:
                start = int(match[1])
                end = min(int(match[2]) if match[2] else length - 1, length - 1)
            if not match or start >= length or end < start:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{length}")
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            status = 206
            content = content[start:end + 1]
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "public, max-age=3600")
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{length}")
        self.end_headers()
        try:
            for offset in range(0, len(content), response_chunk):
                self.wfile.write(content[offset:offset + response_chunk])
        except (BrokenPipeError, ConnectionResetError):
            pass


@contextmanager
def fixture(root: Path):
    """Own a parent-process HTTP server and a reserved, non-listening TCP port.

    Keep this context alive until all thread/process futures have completed.
    Do not nest fixtures using the same root. Yields server for request counters.
    """
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    marker = root / "network-fixture.json"
    if marker.exists():
        raise ValueError("Fixture root already active or has stale metadata")
    server = _Server(("127.0.0.1", 0), _Handler)
    server.root = root
    server.counter_lock = threading.Lock()
    server.requests = {}
    server.release_blocked = threading.Event()
    closed = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    closed.bind(("127.0.0.1", 0))  # Holding the socket prevents port-reuse races.
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        marker.write_text(json.dumps({"port": server.server_port,
                                     "closed_port": closed.getsockname()[1]}))
        yield server
    finally:
        server.release_blocked.set()
        server.shutdown()
        server.server_close()
        thread.join()
        closed.close()
        marker.unlink(missing_ok=True)


def prepare(name: str, root: Path, count: int, seed: int,
            scale: str = "smoke") -> list[dict]:
    """Prepare independent JSON/pickle-safe tasks; invoke once per measurement."""
    if name not in NAMES:
        raise ValueError(f"Unknown network workload: {name}")
    if isinstance(count, bool) or not isinstance(count, int) or not 0 <= count <= 10000:
        raise ValueError("count must be an integer between 0 and 10000")
    if scale not in ("smoke", "representative"):
        raise ValueError("scale must be smoke or representative")
    _dependency("requests")
    if name == "cached_http":
        _dependency("requests_cache")
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    fixture_root = next((p for p in (root, *root.parents)
                         if (p / "network-fixture.json").is_file()), None)
    if fixture_root is None:
        raise ValueError("prepare requires an active fixture in root or an ancestor")
    metadata = json.loads((fixture_root / "network-fixture.json").read_text())
    base = f"http://127.0.0.1:{metadata['port']}"
    work = Path(tempfile.mkdtemp(prefix=f"{name}-", dir=root))
    rng = random.Random(seed)
    tasks = []
    for i in range(count):
        task = {"name": name, "id": i, "url": base, "work": str(work / str(i))}
        if name == "discovery":
            task.update(port=metadata["port"] if i % 2 == 0 else metadata["closed_port"],
                        expected_open=i % 2 == 0)
            if scale == "representative":
                task["probes"] = 4
        else:
            size = (131072 if name == "downloads" else 8192) + rng.randrange(1024)
            if scale == "representative":
                size = REPRESENTATIVE_SIZES[i % len(REPRESENTATIVE_SIZES)]
            content = rng.randbytes(size)
            filename = f"{work.name}-{i}.bin"
            (fixture_root / filename).write_bytes(content)
            task.update(url=f"{base}/data/{filename}", size=size,
                        sha256=sha256(content).hexdigest(), parts=4)
            if scale == "representative":
                response_chunk = RESPONSE_CHUNKS[(i // 2) % len(RESPONSE_CHUNKS)]
                task.update(url=task["url"] + f"?chunk={response_chunk}",
                            response_chunk=response_chunk, parts=(2, 4, 8)[i % 3])
        tasks.append(task)
    return tasks


def _get(session, url, **kwargs):
    _local_url(url)
    response = session.get(url, timeout=TIMEOUT, allow_redirects=False, **kwargs)
    try:
        response.raise_for_status()
        if response.is_redirect:
            raise ValueError("Fixture redirects are not allowed")
    except BaseException:
        response.close()
        raise
    return response


def _verify(content, task):
    digest = sha256(content).hexdigest()
    if len(content) != task["size"] or digest != task["sha256"]:
        raise ValueError("Downloaded content does not match fixture manifest")
    return {"name": task["name"], "id": task["id"], "bytes": len(content), "sha256": digest}


def run_task(task: dict) -> dict:
    """Execute real I/O without internal executors, sleeps or external targets."""
    name = task["name"]
    if name not in NAMES:
        raise ValueError(f"Unknown network workload: {name}")
    _local_url(task["url"])
    requests = _dependency("requests")
    work = Path(task["work"])
    work.mkdir(parents=True, exist_ok=True)
    if name == "cached_http":
        cache = _dependency("requests_cache")
        # Fresh invocation-specific SQLite database: replay never becomes all-warm.
        with tempfile.TemporaryDirectory(prefix="cache-", dir=work) as directory:
            with cache.CachedSession(str(Path(directory) / "responses"),
                                     backend="sqlite", expire_after=3600) as session:
                session.trust_env = False
                with _get(session, task["url"]) as cold:
                    first = _verify(cold.content, task)
                    cold_hit = bool(cold.from_cache)
                with _get(session, task["url"]) as warm:
                    second = _verify(warm.content, task)
                    warm_hit = bool(warm.from_cache)
                if cold_hit or not warm_hit or first != second:
                    raise ValueError("Expected one cold miss followed by one verified cache hit")
                return dict(first, cold_misses=1, warm_hits=1)
    with requests.Session() as session:
        session.trust_env = False  # Ignore ambient proxies, credentials and netrc.
        if name == "discovery":
            port = task["port"]
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("Invalid fixture port")
            probes = task.get("probes", 1)
            if isinstance(probes, bool) or not isinstance(probes, int) or not 1 <= probes <= 32:
                raise ValueError("Invalid bounded discovery probe count")
            result = {"name": name, "id": task["id"]}
            for _ in range(probes):
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=TIMEOUT):
                        is_open = True
                except (ConnectionRefusedError, TimeoutError):
                    is_open = False
                result["open"] = is_open
                if is_open != task["expected_open"]:
                    raise ValueError("Discovery result disagrees with fixture manifest")
                if is_open:
                    with _get(session, f"http://127.0.0.1:{port}/common/basic_info") as response:
                        from urllib.parse import unquote
                        info = dict(item.split("=", 1) for item in response.text.split(","))
                        result["device"] = unquote(info["name"])
                        result["power"] = int(info["pow"])
            if probes != 1:
                result["probes"] = probes
            return result
        if name == "http":
            with _get(session, task["url"]) as response:
                return _verify(response.content, task)
        part_count = task["parts"]
        if not isinstance(part_count, int) or not 1 <= part_count <= task["size"]:
            raise ValueError("Invalid download part count")
        with tempfile.TemporaryDirectory(prefix="parts-", dir=work) as directory:
            directory = Path(directory)
            chunk = (task["size"] + part_count - 1) // part_count
            pieces = []
            for index, start in enumerate(range(0, task["size"], chunk)):
                end = min(start + chunk, task["size"]) - 1
                part = directory / str(index)
                with _get(session, task["url"], stream=True,
                          headers={"Range": f"bytes={start}-{end}"}) as response:
                    expected = f"bytes {start}-{end}/{task['size']}"
                    if response.status_code != 206 or response.headers.get("Content-Range") != expected:
                        raise ValueError("Server did not honor exact byte range")
                    with part.open("wb") as output:
                        for data in response.iter_content(8192):
                            output.write(data)
                if part.stat().st_size != end - start + 1:
                    raise ValueError("Truncated download range")
                pieces.append(part)
            merged = directory / "merged.bin"
            with merged.open("wb") as output:
                for part in pieces:
                    with part.open("rb") as source:
                        shutil.copyfileobj(source, output)
            return dict(_verify(merged.read_bytes(), task), parts=len(pieces))
