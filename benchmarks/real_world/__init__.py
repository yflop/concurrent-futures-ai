"""Original, local-fixture replicas of eleven public workload patterns."""

NAMES = ("http", "images", "geospatial", "cached_http", "downloads", "pipeline",
         "ocr", "dedup", "sqlite", "discovery", "video")


def module_for(name):
    if name in {"http", "cached_http", "downloads", "discovery"}:
        from . import network
        return network
    if name in {"images", "ocr", "video"}:
        from . import media
        return media
    if name in {"geospatial", "pipeline", "dedup", "sqlite"}:
        from . import data
        return data
    raise ValueError(f"Unknown workload: {name}")


def execute_batch(payload):
    """Identical algorithm/chunk boundary in both executor families."""
    import time
    name, tasks = payload
    run = module_for(name).run_task
    rows = []
    for task in tasks:
        start = time.perf_counter()
        try:
            value = run(task)
            rows.append({"value": value, "error": None,
                         "service_seconds": time.perf_counter() - start})
        except Exception as exc:
            rows.append({"value": None, "error": type(exc).__name__ + ": " + str(exc),
                         "service_seconds": time.perf_counter() - start})
    return rows
