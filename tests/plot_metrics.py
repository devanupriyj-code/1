import json
import os
import sys

"""
=============================================================================================
PROJECT 26: EVALUATION GRAPH GENERATOR
=============================================================================================
Generates presentation-ready and report-ready charts:
1. Latency Cumulative Distribution Function (CDF) and Histogram.
2. Percentile Response Latency Breakdown (Min, Mean, p50, p95, p99, Max).
3. Idempotency & Concurrency Verification Summary Bar Chart.
=============================================================================================
"""

BENCHMARK_FILE = os.path.join(os.path.dirname(__file__), "benchmark_results.json")
OUTPUT_IMAGE = os.path.join(os.path.dirname(__file__), "benchmark_plots.png")

def main():
    if not os.path.exists(BENCHMARK_FILE):
        print(f"Error: {BENCHMARK_FILE} not found. Please run tests/load_test_200.py first.", file=sys.stderr)
        sys.exit(1)

    try:
        import matplotlib
        matplotlib.use("Agg")  # Non-interactive backend for generating images
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("Error: matplotlib or numpy is not installed. Run: pip install matplotlib numpy", file=sys.stderr)
        sys.exit(1)

    with open(BENCHMARK_FILE, "r") as f:
        data = json.load(f)

    latencies = np.array(data["latencies_ms"])
    total_voters = data["total_voters"]
    accepted = data["accepted_votes"]
    duplicates = data["duplicate_ignored"]
    errors = data["errors"]

    # Set up Matplotlib styling
    plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
    fig, axes = plt.subplots(1, 3, figsize=(18, 5.5), dpi=300)
    fig.suptitle(
        f"Project 26: E-Voting / Live Polling System Under Synchronized Burst Load ({total_voters} Concurrent Voters)",
        fontsize=14,
        fontweight="bold",
        y=0.98
    )

    # -----------------------------------------------------------------------------------------
    # Plot 1: Latency Distribution & Histogram
    # -----------------------------------------------------------------------------------------
    ax1 = axes[0]
    n_bins = 25
    counts, bins, patches = ax1.hist(latencies, bins=n_bins, color="#3b82f6", edgecolor="#1d4ed8", alpha=0.75, density=False)
    ax1.axvline(data["mean_ms"], color="#ef4444", linestyle="--", linewidth=1.5, label=f"Mean: {data['mean_ms']:.2f}ms")
    ax1.axvline(data["p95_ms"], color="#f59e0b", linestyle="-.", linewidth=1.5, label=f"p95: {data['p95_ms']:.2f}ms")
    ax1.set_title("Response Time Histogram", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Round-Trip Latency (ms)", fontsize=10)
    ax1.set_ylabel("Vote Request Count", fontsize=10)
    ax1.legend(loc="upper right", frameon=True)
    ax1.grid(True, linestyle=":", alpha=0.6)

    # -----------------------------------------------------------------------------------------
    # Plot 2: Latency Percentiles (Min, p50, Mean, p95, p99, Max)
    # -----------------------------------------------------------------------------------------
    ax2 = axes[1]
    metrics_labels = ["Min", "Median (p50)", "Mean", "p95", "p99", "Max"]
    metrics_values = [
        data["min_ms"],
        data["p50_ms"],
        data["mean_ms"],
        data["p95_ms"],
        data["p99_ms"],
        data["max_ms"]
    ]
    colors = ["#10b981", "#3b82f6", "#6366f1", "#f59e0b", "#ec4899", "#ef4444"]

    bars = ax2.bar(metrics_labels, metrics_values, color=colors, edgecolor="#334155", width=0.55)
    ax2.set_title("Latency Percentile Breakdown", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Latency (ms)", fontsize=10)
    ax2.grid(True, axis="y", linestyle=":", alpha=0.6)

    for bar, val in zip(bars, metrics_values):
        height = bar.get_height()
        ax2.annotate(
            f"{val:.2f}ms",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8.5,
            fontweight="bold"
        )

    # -----------------------------------------------------------------------------------------
    # Plot 3: Idempotency & Strictly-Once Verification Breakdown
    # -----------------------------------------------------------------------------------------
    ax3 = axes[2]
    cat_labels = ["Verified Unique Votes", "Duplicate Retries Caught", "Errors"]
    cat_values = [accepted, duplicates, errors]
    cat_colors = ["#10b981", "#f59e0b", "#ef4444"]

    status_bars = ax3.bar(cat_labels, cat_values, color=cat_colors, edgecolor="#334155", width=0.45)
    ax3.set_title("Fault Tolerance & Idempotency Audit", fontsize=12, fontweight="bold")
    ax3.set_ylabel("Frames Processed", fontsize=10)
    ax3.grid(True, axis="y", linestyle=":", alpha=0.6)

    for bar, val in zip(status_bars, cat_values):
        height = bar.get_height()
        ax3.annotate(
            f"{val}",
            xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9.5,
            fontweight="bold"
        )

    plt.tight_layout()
    plt.subplots_adjust(top=0.88)
    plt.savefig(OUTPUT_IMAGE, dpi=300)
    plt.close()

    print(f"[+] Evaluation plots successfully generated at: {OUTPUT_IMAGE}")

if __name__ == "__main__":
    main()
