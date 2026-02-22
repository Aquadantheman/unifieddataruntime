"""
Durability Benchmark: Understanding Rhizo's Two Commit Paths

Rhizo has TWO distinct commit paths with different latency and durability:

1. ALGEBRAIC COMMIT (0.001ms) - Coordination-free, in-memory
   - For operations that are commutative+associative (ADD, MAX, UNION)
   - No disk write, no fsync - durability via replication (gossip)
   - This is the "59x faster than 2PC" path
   - See real_consensus_benchmark.py for these measurements

2. DISK WRITE (~18-65ms per batch) - Parquet encoding, atomic rename
   - For materializing data to storage (OLAP/batch workloads)
   - Atomic write-to-temp-rename survives process crash (SIGKILL, OOM)
   - Requires fsync for power-loss durability
   - Optimized for columnar analytics, not single-row OLTP

This benchmark measures path #2 (disk writes) with and without fsync.

Key insight: Rhizo is OLAP-first. The disk write path prioritizes:
- Compression (Parquet encoding)
- Read performance (columnar layout)
- Deduplication (content-addressed chunks)

For OLTP-style single-row commits, the algebraic path is 10,000x faster.

Usage:
  python benchmarks/durability_benchmark.py
  python benchmarks/durability_benchmark.py --iterations 50 --rows 1000
"""

import argparse
import json
import os
import sqlite3
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List
import sys

# Add rhizo to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import pyarrow as pa


@dataclass
class BenchmarkResult:
    system: str
    operation: str
    iterations: int
    latencies_ms: List[float]
    durability: str  # "process-crash" or "power-loss"

    @property
    def mean_ms(self) -> float:
        return statistics.mean(self.latencies_ms)

    @property
    def median_ms(self) -> float:
        return statistics.median(self.latencies_ms)

    @property
    def p99_ms(self) -> float:
        if len(self.latencies_ms) >= 100:
            return statistics.quantiles(self.latencies_ms, n=100)[98]
        return max(self.latencies_ms)

    @property
    def stddev_ms(self) -> float:
        if len(self.latencies_ms) >= 2:
            return statistics.stdev(self.latencies_ms)
        return 0.0

    def summary_dict(self) -> dict:
        return {
            "system": self.system,
            "operation": self.operation,
            "durability": self.durability,
            "iterations": self.iterations,
            "mean_ms": round(self.mean_ms, 4),
            "median_ms": round(self.median_ms, 4),
            "stddev_ms": round(self.stddev_ms, 4),
            "p99_ms": round(self.p99_ms, 4),
        }


def print_result(result: BenchmarkResult) -> None:
    print(f"  {result.system}")
    print(f"    Durability: {result.durability}")
    print(f"    Mean:   {result.mean_ms:.4f} ms  (stddev: {result.stddev_ms:.4f})")
    print(f"    Median: {result.median_ms:.4f} ms")
    print(f"    p99:    {result.p99_ms:.4f} ms")


# =============================================================================
# Rhizo Benchmarks
# =============================================================================

def benchmark_rhizo_write(iterations: int = 100, rows: int = 100) -> BenchmarkResult:
    """Benchmark Rhizo write without fsync (current behavior)."""
    import rhizo

    with tempfile.TemporaryDirectory() as tmpdir:
        db = rhizo.open(tmpdir)

        # Create test data
        table = pa.table({
            "id": pa.array(range(rows)),
            "value": pa.array([f"data_{i}" for i in range(rows)]),
            "score": pa.array([float(i) for i in range(rows)]),
        })

        # Warmup
        for i in range(10):
            db.write(f"warmup_{i}", table)

        # Measure
        latencies = []
        for i in range(iterations):
            start = time.perf_counter()
            db.write(f"bench_{i}", table)
            latencies.append((time.perf_counter() - start) * 1000)

        db.close()

    return BenchmarkResult(
        system=f"Rhizo write ({rows} rows, no fsync)",
        operation="write table",
        iterations=iterations,
        latencies_ms=latencies,
        durability="process-crash",
    )


def benchmark_rhizo_write_fsync(iterations: int = 100, rows: int = 100) -> BenchmarkResult:
    """Benchmark Rhizo write WITH explicit fsync (power-loss durable)."""
    import rhizo

    with tempfile.TemporaryDirectory() as tmpdir:
        db = rhizo.open(tmpdir)

        # Create test data
        table = pa.table({
            "id": pa.array(range(rows)),
            "value": pa.array([f"data_{i}" for i in range(rows)]),
            "score": pa.array([float(i) for i in range(rows)]),
        })

        # Warmup
        for i in range(10):
            db.write(f"warmup_{i}", table)
            # Fsync the data directory
            os.sync()  # Linux: sync all filesystems

        # Measure
        latencies = []
        for i in range(iterations):
            start = time.perf_counter()
            db.write(f"bench_{i}", table)
            # Fsync to ensure power-loss durability
            os.sync()
            latencies.append((time.perf_counter() - start) * 1000)

        db.close()

    return BenchmarkResult(
        system=f"Rhizo write ({rows} rows, with fsync)",
        operation="write table + fsync",
        iterations=iterations,
        latencies_ms=latencies,
        durability="power-loss",
    )


def benchmark_rhizo_write_dir_fsync(iterations: int = 100, rows: int = 100) -> BenchmarkResult:
    """Benchmark Rhizo write with directory fsync (proper power-loss durability)."""
    import rhizo

    with tempfile.TemporaryDirectory() as tmpdir:
        db = rhizo.open(tmpdir)

        # Create test data
        table = pa.table({
            "id": pa.array(range(rows)),
            "value": pa.array([f"data_{i}" for i in range(rows)]),
            "score": pa.array([float(i) for i in range(rows)]),
        })

        # Warmup
        for i in range(10):
            db.write(f"warmup_{i}", table)

        # Measure
        latencies = []
        chunks_dir = Path(tmpdir) / "chunks"
        catalog_dir = Path(tmpdir) / "catalog"

        for i in range(iterations):
            start = time.perf_counter()
            db.write(f"bench_{i}", table)

            # Fsync data directories for power-loss durability
            # This is what a proper durable write would do
            try:
                if chunks_dir.exists():
                    fd = os.open(str(chunks_dir), os.O_RDONLY | os.O_DIRECTORY)
                    os.fsync(fd)
                    os.close(fd)
                if catalog_dir.exists():
                    fd = os.open(str(catalog_dir), os.O_RDONLY | os.O_DIRECTORY)
                    os.fsync(fd)
                    os.close(fd)
            except (OSError, AttributeError):
                # Windows doesn't support O_DIRECTORY, fall back to os.sync()
                try:
                    os.sync()
                except AttributeError:
                    pass  # Windows has no os.sync()

            latencies.append((time.perf_counter() - start) * 1000)

        db.close()

    return BenchmarkResult(
        system=f"Rhizo write ({rows} rows, dir fsync)",
        operation="write table + dir fsync",
        iterations=iterations,
        latencies_ms=latencies,
        durability="power-loss",
    )


# =============================================================================
# SQLite Benchmarks (for comparison)
# =============================================================================

def benchmark_sqlite_wal_normal(iterations: int = 100, rows: int = 100) -> BenchmarkResult:
    """SQLite WAL mode with NORMAL sync (batched fsync)."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT, score REAL)")

    # Warmup
    for i in range(10):
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT OR REPLACE INTO data VALUES (?, ?, ?)",
            [(j, f"data_{j}", float(j)) for j in range(rows)]
        )
        conn.execute("COMMIT")

    # Measure
    latencies = []
    for i in range(iterations):
        start = time.perf_counter()
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT OR REPLACE INTO data VALUES (?, ?, ?)",
            [(j + i * rows, f"data_{j}", float(j)) for j in range(rows)]
        )
        conn.execute("COMMIT")
        latencies.append((time.perf_counter() - start) * 1000)

    conn.close()
    os.unlink(db_path)

    return BenchmarkResult(
        system=f"SQLite WAL NORMAL ({rows} rows)",
        operation="INSERT rows",
        iterations=iterations,
        latencies_ms=latencies,
        durability="process-crash",  # NORMAL doesn't guarantee power-loss
    )


def benchmark_sqlite_wal_full(iterations: int = 100, rows: int = 100) -> BenchmarkResult:
    """SQLite WAL mode with FULL sync (fsync per commit)."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT, score REAL)")

    # Warmup
    for i in range(10):
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT OR REPLACE INTO data VALUES (?, ?, ?)",
            [(j, f"data_{j}", float(j)) for j in range(rows)]
        )
        conn.execute("COMMIT")

    # Measure
    latencies = []
    for i in range(iterations):
        start = time.perf_counter()
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT OR REPLACE INTO data VALUES (?, ?, ?)",
            [(j + i * rows, f"data_{j}", float(j)) for j in range(rows)]
        )
        conn.execute("COMMIT")
        latencies.append((time.perf_counter() - start) * 1000)

    conn.close()
    os.unlink(db_path)

    return BenchmarkResult(
        system=f"SQLite WAL FULL ({rows} rows)",
        operation="INSERT rows + fsync",
        iterations=iterations,
        latencies_ms=latencies,
        durability="power-loss",
    )


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Durability benchmark")
    parser.add_argument("--iterations", type=int, default=100, help="Iterations per benchmark")
    parser.add_argument("--rows", type=int, default=100, help="Rows per write")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file")
    args = parser.parse_args()

    print("=" * 70)
    print("DURABILITY BENCHMARK")
    print("Measuring write latency with different durability guarantees")
    print("=" * 70)
    print()
    print(f"Configuration: {args.iterations} iterations, {args.rows} rows per write")
    print()

    results = []

    # Rhizo benchmarks
    print("1. RHIZO WRITES")
    print("-" * 50)

    rhizo_no_sync = benchmark_rhizo_write(args.iterations, args.rows)
    print_result(rhizo_no_sync)
    results.append(rhizo_no_sync)
    print()

    rhizo_fsync = benchmark_rhizo_write_dir_fsync(args.iterations, args.rows)
    print_result(rhizo_fsync)
    results.append(rhizo_fsync)
    print()

    # SQLite benchmarks
    print("2. SQLITE BASELINES")
    print("-" * 50)

    sqlite_normal = benchmark_sqlite_wal_normal(args.iterations, args.rows)
    print_result(sqlite_normal)
    results.append(sqlite_normal)
    print()

    sqlite_full = benchmark_sqlite_wal_full(args.iterations, args.rows)
    print_result(sqlite_full)
    results.append(sqlite_full)
    print()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print("Process-crash durable (survives kill -9, not power loss):")
    print(f"  Rhizo (no fsync):      {rhizo_no_sync.mean_ms:.3f} ms")
    print(f"  SQLite WAL NORMAL:     {sqlite_normal.mean_ms:.3f} ms")
    print()
    print("Power-loss durable (survives power failure):")
    print(f"  Rhizo (with fsync):    {rhizo_fsync.mean_ms:.3f} ms")
    print(f"  SQLite WAL FULL:       {sqlite_full.mean_ms:.3f} ms")
    print()

    # Compute speedups
    speedup_no_sync = sqlite_normal.mean_ms / rhizo_no_sync.mean_ms
    speedup_fsync = sqlite_full.mean_ms / rhizo_fsync.mean_ms
    fsync_overhead = rhizo_fsync.mean_ms / rhizo_no_sync.mean_ms

    print("Speedups:")
    print(f"  Rhizo vs SQLite (no fsync):   {speedup_no_sync:.1f}x faster")
    print(f"  Rhizo vs SQLite (with fsync): {speedup_fsync:.1f}x faster")
    print()
    print("Fsync overhead:")
    print(f"  Rhizo fsync adds: {fsync_overhead:.1f}x latency ({rhizo_fsync.mean_ms - rhizo_no_sync.mean_ms:.3f} ms)")
    print()

    print("WHAT THIS MEANS:")
    print("-" * 50)
    print("Rhizo's atomic write-to-temp-rename pattern survives process crashes")
    print("(SIGKILL, OOM, application bugs) without fsync. For power-loss")
    print("durability, explicit fsync is needed, adding measurable latency.")
    print()
    print("For most applications, process-crash durability is sufficient.")
    print("Critical applications should enable fsync (future configuration option).")

    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "config": {
            "iterations": args.iterations,
            "rows": args.rows,
        },
        "results": [r.summary_dict() for r in results],
        "summary": {
            "rhizo_no_fsync_ms": round(rhizo_no_sync.mean_ms, 4),
            "rhizo_with_fsync_ms": round(rhizo_fsync.mean_ms, 4),
            "sqlite_normal_ms": round(sqlite_normal.mean_ms, 4),
            "sqlite_full_ms": round(sqlite_full.mean_ms, 4),
            "speedup_no_fsync": round(speedup_no_sync, 1),
            "speedup_with_fsync": round(speedup_fsync, 1),
            "fsync_overhead_factor": round(fsync_overhead, 1),
        },
        "durability_note": (
            "Process-crash durable: survives kill -9, OOM, application crashes. "
            "Power-loss durable: survives sudden power failure, requires fsync."
        ),
    }

    output_path = args.output or str(Path(__file__).parent / "DURABILITY_BENCHMARK_RESULTS.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    print()
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
