"""Consume a lazy input stream with bounded pending work and result buffering.

Run: python -m examples.bounded_pipeline
"""
import json
from intelligent_futures import IntelligentExecutor


def records(count: int):
    for index in range(count):
        yield json.dumps({"id": index, "amount_cents": index * 125})


def normalize(raw: str) -> dict:
    record = json.loads(raw)
    return {"id": record["id"], "amount_dollars": record["amount_cents"] / 100}


def main() -> None:
    executor = IntelligentExecutor(max_workers=4, max_pending=8, policy="threads")
    with executor:
        # Ordered streaming: no list of every input or Future is built.
        # buffersize bounds submitted-but-not-yielded results; max_pending bounds
        # accepted unfinished tasks across the executor, including other producers.
        stream = executor.map_bounded(normalize, records(100), buffersize=8)
        total = 0.0
        count = 0
        try:
            for row in stream:
                count += 1
                total += row["amount_dollars"]
        finally:
            # Also close when a downstream consumer stops early or raises.
            stream.close()
    print(f"Processed {count} rows; amount total=${total:.2f}")
    print(f"Peak unfinished tasks: {executor.snapshot()['counts']['peak_pending']}")


if __name__ == "__main__":
    main()
