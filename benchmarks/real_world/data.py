"""Original bounded workload replicas; provenance and limits: docs/workloads-data.md.

Preparation is outside measurement. Tasks contain only JSON/pickle-safe values;
run_task is module-level and opens its own files/connections in each worker.
"""
from __future__ import annotations

from collections import defaultdict
from contextlib import closing
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import stat
import zlib

NAMES = ("geospatial", "pipeline", "dedup", "sqlite")
CHUNK = 65536
PREFIX = 4096


def _digest_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def _equal_files(left: str, right: str) -> bool:
    # No shallow metadata comparison or cached filecmp result: verify every byte.
    with open(left, "rb") as a, open(right, "rb") as b:
        while True:
            x, y = a.read(CHUNK), b.read(CHUNK)
            if x != y:
                return False
            if not x:
                return True


def find_duplicates(root: str | Path) -> list[list[str]]:
    """Size/prefix/full-hash filtering followed by exact byte equivalence.

    Stable fixture trees are required. Symlinks/nonregular files are ignored;
    inaccessible/disappearing files are skipped. This is not a filesystem snapshot
    or a destructive deduplicator. Returned paths are relative for portable results.
    """
    root = Path(root)
    sizes: dict[int, list[str]] = defaultdict(list)
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if not Path(directory, d).is_symlink())
        for filename in sorted(files):
            path = os.path.join(directory, filename)
            try:
                info = os.stat(path, follow_symlinks=False)
                if stat.S_ISREG(info.st_mode):
                    sizes[info.st_size].append(path)
            except OSError:
                continue
    result = []
    for paths in sizes.values():
        if len(paths) < 2:
            continue
        prefixes: dict[bytes, list[str]] = defaultdict(list)
        for path in paths:
            try:
                with open(path, "rb") as stream:
                    prefixes[stream.read(PREFIX)].append(path)
            except OSError:
                continue
        for candidates in prefixes.values():
            if len(candidates) < 2:
                continue
            hashes: dict[str, list[str]] = defaultdict(list)
            for path in candidates:
                try:
                    hashes[_digest_file(path)].append(path)
                except OSError:
                    continue
            for bucket in hashes.values():
                if len(bucket) < 2:
                    continue
                exact: list[list[str]] = []
                for path in sorted(bucket):
                    try:
                        # Establish readability even if this becomes a new representative.
                        with open(path, "rb"):
                            pass
                        for group in exact:
                            if _equal_files(path, group[0]):
                                group.append(path)
                                break
                        else:
                            exact.append([path])
                    except OSError:
                        continue
                result.extend(sorted(str(Path(p).relative_to(root)) for p in group)
                              for group in exact if len(group) > 1)
    return sorted(result, key=lambda group: group[0])


# Rounded, real city-centre geographic coordinates. Edges are an ORIGINAL
# illustrative transport topology, not measured roads, flight schedules or GIS data.
CITIES = {
    "Seattle": (47.6062, -122.3321), "Portland": (45.5152, -122.6784),
    "San Francisco": (37.7749, -122.4194), "Los Angeles": (34.0522, -118.2437),
    "Phoenix": (33.4484, -112.0740), "Denver": (39.7392, -104.9903),
    "Dallas": (32.7767, -96.7970), "Houston": (29.7604, -95.3698),
    "Minneapolis": (44.9778, -93.2650), "Chicago": (41.8781, -87.6298),
    "St Louis": (38.6270, -90.1994), "Atlanta": (33.7490, -84.3880),
    "Miami": (25.7617, -80.1918), "Washington": (38.9072, -77.0369),
    "New York": (40.7128, -74.0060), "Boston": (42.3601, -71.0589),
}


def haversine_km(a, b) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return 6371.0088 * 2 * math.asin(math.sqrt(min(1.0, h)))


def _networkx():
    try:
        import networkx
    except ImportError as exc:
        raise RuntimeError("geospatial requires optional dependency: pip install networkx>=3.2") from exc
    return networkx


def prepare(name: str, root: Path, count: int, seed: int, scale: str = "smoke") -> list[dict]:
    if name not in NAMES:
        raise ValueError(f"unknown data workload: {name}")
    if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 10000:
        raise ValueError("count must be an integer between 1 and 10000")
    if scale not in ("smoke", "representative"):
        raise ValueError("scale must be smoke or representative")
    if scale == "representative" and count > 64:
        raise ValueError("representative count must be at most 64 to bound fixture resources")
    representative = scale == "representative"
    root = Path(root).resolve() / name
    root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    tasks = []
    if name == "geospatial":
        _networkx()  # Fail before measuring or launching workers.
        cities = sorted(CITIES)
        edges = set()
        # A connected backbone plus each city's three closest neighbours.
        for i, city in enumerate(cities):
            if i:
                edges.add(tuple(sorted((cities[i-1], city))))
            for other in sorted((x for x in cities if x != city), key=lambda x: haversine_km(CITIES[city], CITIES[x]))[:3]:
                edges.add(tuple(sorted((city, other))))
        nodes = CITIES
        if representative:
            # Original fictional geographic grid, never imported road data.
            side = 32
            nodes = {f"grid-{r:02d}-{c:02d}": (35 + r*.025 + rng.uniform(-.004,.004),
                                                -120 + c*.025 + rng.uniform(-.004,.004))
                     for r in range(side) for c in range(side)}
            edges = set()
            for r in range(side):
                for c in range(side):
                    for dr, dc in ((0,1),(1,0),(1,1)):
                        if r+dr < side and c+dc < side and (dr != dc or (r+c)%3 == 0):
                            edges.add((f"grid-{r:02d}-{c:02d}", f"grid-{r+dr:02d}-{c+dc:02d}"))
            cities = sorted(nodes)
        graph = {"nodes": nodes, "edges": [[a, b, haversine_km(nodes[a], nodes[b])] for a, b in sorted(edges)]}
        path = root / "city-network.json"
        path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
        for i in range(count):
            tasks.append({"name": name, "id": i, "path": str(path), "origins": rng.sample(cities, (4,8,16)[i%3] if representative else 8), "destinations": rng.sample(cities, (32,64,128)[i%3]) if representative else cities})
    elif name == "dedup":
        for i in range(count):
            directory = root / f"tree-{i:05d}"
            directory.mkdir(exist_ok=True)
            nested = directory / "nested"
            nested.mkdir(exist_ok=True)
            if representative:
                # Keep aggregate file count <= 8192, even with many tasks.
                limit = min(512, 8192 // count)
                target_files = max(128, limit * (2 + i%3) // 4)
                groups = (target_files - 3) // 3
                for j in range(groups):
                    subdir = directory / f"bucket-{j%16:02d}"
                    subdir.mkdir(exist_ok=True)
                    size = (32, 257, 1024, PREFIX+64, 16384, CHUNK+37)[j%6]
                    content = rng.randbytes(size)
                    (subdir / f"original-{j:04d}.bin").write_bytes(content)
                    (nested / f"copy-{j:04d}.bin").write_bytes(content)
                    # Large files collide in size AND prefix, but not full content.
                    (subdir / f"different-{j:04d}.bin").write_bytes(content[:-1] + bytes([content[-1]^255]))
                (directory / "empty").write_bytes(b"")
                (nested / "empty").write_bytes(b"")
                (directory / "unique-size").write_bytes(b"unique")
                for extra in range(target_files - (3*groups+3)):
                    (directory / f"extra-unique-{extra}").write_bytes(b"x"*(7+extra))
                tasks.append({"name": name, "id": i, "path": str(directory)})
                continue
            prefix = rng.randbytes(PREFIX)
            for j in range(6):
                data = prefix + rng.randbytes(CHUNK + j)
                (directory / f"a{j}.bin").write_bytes(data)
                (nested / f"b{j}.bin").write_bytes(data)
                # Same prefix and size, unequal bytes: hashes must examine tail.
                (directory / f"different{j}.bin").write_bytes(data[:-1] + bytes([data[-1] ^ 255]))
            (directory / "empty").write_bytes(b"")
            (nested / "empty").write_bytes(b"")
            (directory / "unique-size").write_bytes(b"unique")
            tasks.append({"name": name, "id": i, "path": str(directory)})
    elif name == "pipeline":
        words = ("python", "executor", "stream", "concurrency", "Python", "data", "pythonista")
        for i in range(count):
            path = root / f"pages-{i:05d}.jsonl"
            with path.open("w", encoding="utf-8") as stream:
                pages = min((128,256,512)[i%3], 8192//count) if representative else 32
                for j in range(pages):
                    tokens = (64,128,256,512)[(i+j)%4] if representative else 256
                    text = " ".join(rng.choice(words) for _ in range(tokens))
                    stream.write(json.dumps({"id": j, "text": text}) + "\n")
            tasks.append({"name": name, "id": i, "path": str(path), "word": "python"})
    else:
        path = root / "market.sqlite3"
        with closing(sqlite3.connect(path)) as db:
            db.execute("PRAGMA journal_mode=WAL")
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS bars (symbol TEXT, day INTEGER, open INTEGER, high INTEGER, low INTEGER, close INTEGER, volume INTEGER, PRIMARY KEY(symbol,day))")
                db.execute("DELETE FROM bars")
                symbols = [f"DEMO_{chr(65+i)}" for i in range(12 if representative else 4)]
                days = 8192 if representative else 2048
                for symbol in symbols:
                    rows = []
                    price = 10000
                    for day in range(days):
                        close = max(100, price + rng.randrange(-100, 101))
                        rows.append((symbol, day, price, max(price,close)+20, min(price,close)-20, close, rng.randrange(1000,100000)))
                        price = close
                    db.executemany("INSERT INTO bars VALUES(?,?,?,?,?,?,?)", rows)
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        for i in range(count):
            if representative:
                length = (256,1024,4096)[i%3]
                tasks.append({"name": name, "id": i, "path": str(path), "symbol": symbols[i%len(symbols)],
                              "start": rng.randrange(0, days-length+1), "length": length,
                              "min_volume": (0,25000,50000)[i%3], "descending": bool(i%2)})
            else:
                tasks.append({"name": name, "id": i, "path": str(path), "symbol": f"DEMO_{'ABCD'[i % 4]}", "start": rng.randrange(0, 1024), "length": 1024})
    return tasks


def run_task(task: dict) -> dict:
    name = task["name"]
    if name == "dedup":
        groups = find_duplicates(task["path"])
        return {"id": task["id"], "groups": groups, "duplicate_files": sum(map(len, groups))}
    if name == "pipeline":
        # Lazy source -> JSON decode -> word transform -> JSON serialization ->
        # compression sink, consuming one bounded page at a time. No HTTP or sleep.
        digest = hashlib.sha256()
        total = pages = compressed_bytes = 0
        with open(task["path"], encoding="utf-8") as source:
            decoded = map(json.loads, source)
            transformed = ({"id": p["id"], "count": sum(w.casefold() == task["word"].casefold() for w in re.findall(r"\b\w+\b", p["text"]))} for p in decoded)
            for record in transformed:
                encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
                compressed = zlib.compress(encoded)
                if zlib.decompress(compressed) != encoded:
                    raise AssertionError("pipeline serialization roundtrip failed")
                digest.update(encoded)
                compressed_bytes += len(compressed)
                total += record["count"]
                pages += 1
        return {"id": task["id"], "pages": pages, "word_count": total, "compressed_bytes": compressed_bytes, "sha256": digest.hexdigest()}
    if name == "sqlite":
        # A fresh read-only connection and explicit snapshot per task; no connection
        # crosses a thread/process. Preparation commits all data before workers run.
        uri = Path(task["path"]).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            predicate = "symbol=? AND day>=? AND day<? AND volume>=?"
            parameters = (task["symbol"], task["start"], task["start"]+task["length"], task.get("min_volume", 0))
            ordering = "DESC" if task.get("descending", False) else "ASC"
            rows = db.execute("SELECT day, open, high, low, close, volume FROM bars WHERE " + predicate + " ORDER BY day " + ordering, parameters).fetchall()
            aggregate = db.execute("SELECT COUNT(*), SUM(close*volume), SUM(volume) FROM bars WHERE " + predicate, parameters).fetchone()
            db.commit()
        if aggregate != (len(rows), sum(r[4]*r[5] for r in rows), sum(r[5] for r in rows)):
            raise AssertionError("SQL aggregate differs from independent row reduction")
        return {"id": task["id"], "symbol": task["symbol"], "rows": len(rows), "price_volume": aggregate[1], "volume": aggregate[2], "sha256": hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()}
    if name == "geospatial":
        nx = _networkx()
        data = json.loads(Path(task["path"]).read_text(encoding="utf-8"))
        graph = nx.Graph()
        for city, (lat, lon) in data["nodes"].items():
            graph.add_node(city, latitude=lat, longitude=lon)
        graph.add_weighted_edges_from(data["edges"], weight="km")
        rows = []
        for origin in task["origins"]:
            distances, routes = nx.single_source_dijkstra(graph, origin, weight="km")
            for destination in task["destinations"]:
                route = routes[destination]
                actual = sum(graph[a][b]["km"] for a, b in zip(route, route[1:]))
                if not math.isclose(actual, distances[destination], rel_tol=1e-12):
                    raise AssertionError("route length differs from shortest-path result")
                rows.append([origin, destination, round(actual, 6), route])
        return {"id": task["id"], "routes": len(rows), "total_km": round(sum(r[2] for r in rows), 6), "sha256": hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()}
    raise ValueError(f"unknown data workload: {name}")
