"""Create a scoped evidence summary from final representative raw reports."""
import argparse
import json
from pathlib import Path
import statistics


def summarize(workload):
    phases = workload["summary"]["heldout_comparisons"]
    ratios = [p["T_best_tuned_stdlib_over_T_intelligent"] for p in phases]
    engines = {}
    for engine in ("stdlib", "intelligent"):
        rows = [r for r in workload["heldout"] if r["engine"] == engine]
        total = [r["cold_lifecycle_seconds"] + r["warm"]["seconds"] for r in rows]
        engines[engine] = {
            "configuration": workload["summary"]["chosen_from_calibration"][engine],
            "mean_cold_plus_warm_seconds": statistics.mean(total),
            "mean_lifecycle_cpu_seconds": statistics.mean(r["lifecycle_cpu_seconds"] for r in rows),
            "peak_sampled_rss_bytes": max(r["sampled_tree_peak_rss_bytes"] for r in rows),
            "max_batch_completion_p95_seconds": max(r[p]["batch_completion_latency_p95_seconds"] for r in rows for p in ("cold", "warm")),
            "total_task_errors": sum(len(r[p]["errors"]) for r in rows for p in ("cold", "warm")),
            "configuration_search_seconds": workload["configuration_selection"][engine]["elapsed_seconds"],
            "heldout_seeds": len(rows), "items_per_phase": rows[0]["cold"]["tasks"],
        }
    savings = engines["stdlib"]["mean_cold_plus_warm_seconds"] - engines["intelligent"]["mean_cold_plus_warm_seconds"]
    cost = engines["intelligent"]["configuration_search_seconds"]
    return {"name": workload["name"], "accepted": workload.get("accepted", False),
            "coverage_within_ten_percent": workload["summary"]["near_optimal_coverage"],
            "ratio_min": min(ratios), "ratio_median": statistics.median(ratios), "ratio_max": max(ratios),
            "engines": engines, "observed_mean_cycle_savings_seconds": savings,
            "illustrative_cycles_to_repay_intelligent_execution_search_only": cost / savings if savings > 0 else None,
            "amortization_limit": "Conditional arithmetic from observed mean, not demonstrated long-run repayment or statistical significance. Search cost excludes shared fixture generation, sequential references, hold-out validation, input curation and operational adoption. No positive saving means no throughput repayment established."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    results = []
    seen = set()
    for path in args.reports:
        report = json.loads(path.read_text())
        if report.get("failures"):
            print(f"WARNING: preserving failures from {path}: {report['failures']}")
        for workload in report["workloads"]:
            if workload["name"] in seen:
                raise ValueError("Choose one comparison mode per summary; duplicate workload " + workload["name"])
            seen.add(workload["name"])
            item = summarize(workload)
            item["source_report"] = str(path)
            results.append(item)
    output = {"scope": "Observed single-host generated-fixture evidence, not production certification or universal superiority.",
              "ratio_definition": "Calibration-selected stdlib time / calibration-selected intelligent time on held-out inputs.",
              "workloads": results}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2, allow_nan=False) + "\n")
    for item in results:
        print(f"{item['name']}: ratio median={item['ratio_median']:.3f}; <=10% coverage={item['coverage_within_ten_percent']:.0%}; accepted={item['accepted']}")


if __name__ == "__main__":
    main()
