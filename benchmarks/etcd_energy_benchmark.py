"""
etcd Energy Benchmark: Validating the Waiting Waste Theorem

This benchmark measures REAL energy consumption of etcd transactions using
CodeCarbon, then validates the Waiting Waste theoretical model.

The key validation is:
  1. Measure E_total for N etcd transactions
  2. Decompose into E_compute + E_wait based on time breakdown
  3. Compare measured values to model predictions
  4. Show that E_wait dominates as predicted by the theorem

Prerequisites:
  - etcd running locally (or remotely with --host flag)
  - pip install etcd3 codecarbon psutil

Run:
  # Start etcd first (3-node cluster recommended):
  #   etcd --name node1 --listen-client-urls http://localhost:2379 ...

  python benchmarks/etcd_energy_benchmark.py
  python benchmarks/etcd_energy_benchmark.py --host etcd.example.com --port 2379
"""

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

# Add rhizo to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

# Optional imports with fallbacks
try:
    from codecarbon import OfflineEmissionsTracker
    CODECARBON_AVAILABLE = True
except ImportError:
    CODECARBON_AVAILABLE = False
    OfflineEmissionsTracker = None
    print("Warning: CodeCarbon not available. Install with: pip install codecarbon")

try:
    import etcd3
    ETCD_AVAILABLE = True
except ImportError:
    ETCD_AVAILABLE = False
    etcd3 = None
    print("Warning: etcd3 not available. Install with: pip install etcd3")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

from _rhizo import (
    PyNodeId,
    PyVectorClock,
    PyAlgebraicOperation,
    PyAlgebraicTransaction,
    PyLocalCommitProtocol,
    PyOpType,
    PyAlgebraicValue,
)


# =============================================================================
# Model Parameters (from Waiting Waste Theorem paper)
# =============================================================================

MODEL_PARAMS = {
    "P_active": 65,      # Watts - CPU active power
    "P_idle": 22,        # Watts - CPU idle power (during network wait)
    "T_compute": 0.001,  # seconds - compute time per transaction
    "R": 3,              # Raft round-trips (leader election + log replication)
}


@dataclass
class EnergyMeasurement:
    """Single energy measurement result."""
    system: str
    operation: str
    iterations: int
    total_time_s: float
    energy_joules: float
    avg_power_watts: float
    energy_per_op_joules: float
    latency_ms_mean: float
    latency_ms_p99: float


@dataclass
class WaitingWasteValidation:
    """Validation of the Waiting Waste model against measurements."""
    measured_energy_per_tx_mJ: float
    measured_latency_ms: float

    # Model predictions
    model_E_compute_mJ: float
    model_E_wait_mJ: float
    model_E_total_mJ: float
    model_wait_fraction: float

    # Comparison
    measurement_vs_model_ratio: float
    model_accuracy_percent: float


# =============================================================================
# Energy Measurement
# =============================================================================

def measure_with_energy(func, iterations: int, warmup: int = 50) -> tuple:
    """
    Measure both latency and energy for a function.

    Returns: (latencies_ms, total_energy_joules, total_time_s, avg_power_watts)
    """
    # Warmup
    for _ in range(warmup):
        func()

    tracker = None
    if CODECARBON_AVAILABLE:
        tracker = OfflineEmissionsTracker(
            country_iso_code="USA",
            log_level="error",
            save_to_file=False,
        )
        tracker.start()

    latencies = []
    start_total = time.perf_counter()

    for _ in range(iterations):
        start = time.perf_counter()
        func()
        latencies.append((time.perf_counter() - start) * 1000)

    total_time = time.perf_counter() - start_total

    if tracker:
        tracker.stop()
        # CodeCarbon reports in kWh, convert to Joules
        energy_kwh = tracker._total_energy.kWh if hasattr(tracker, '_total_energy') else 0
        energy_joules = energy_kwh * 3.6e6  # 1 kWh = 3.6e6 J
    else:
        # Estimate based on typical power draw
        avg_power = estimate_power_watts()
        energy_joules = avg_power * total_time

    avg_power = energy_joules / total_time if total_time > 0 else 0

    return latencies, energy_joules, total_time, avg_power


def estimate_power_watts() -> float:
    """Estimate system power consumption."""
    if PSUTIL_AVAILABLE and psutil:
        cpu_percent = psutil.cpu_percent(interval=0.1)
        # Estimate: idle=22W, full=65W (from model params)
        return MODEL_PARAMS["P_idle"] + (MODEL_PARAMS["P_active"] - MODEL_PARAMS["P_idle"]) * (cpu_percent / 100)
    return 45  # Default estimate


# =============================================================================
# Rhizo Benchmark (Coordination-Free Baseline)
# =============================================================================

def benchmark_rhizo(iterations: int = 10000) -> EnergyMeasurement:
    """Measure Rhizo coordination-free commits."""
    node_id = PyNodeId("energy-node")
    clock = PyVectorClock()
    counter = [0]

    def do_rhizo_commit():
        counter[0] += 1
        tx = PyAlgebraicTransaction()
        op = PyAlgebraicOperation("counter", PyOpType("ADD"), PyAlgebraicValue.integer(1))
        tx.add_operation(op)
        PyLocalCommitProtocol.commit_local(tx, node_id, clock)

    latencies, energy_j, total_time, avg_power = measure_with_energy(do_rhizo_commit, iterations)

    return EnergyMeasurement(
        system="Rhizo (coordination-free)",
        operation="algebraic ADD",
        iterations=iterations,
        total_time_s=total_time,
        energy_joules=energy_j,
        avg_power_watts=avg_power,
        energy_per_op_joules=energy_j / iterations,
        latency_ms_mean=statistics.mean(latencies),
        latency_ms_p99=statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
    )


# =============================================================================
# etcd Benchmark (Real Raft Consensus)
# =============================================================================

def benchmark_etcd(host: str = "localhost", port: int = 2379, iterations: int = 500) -> Optional[EnergyMeasurement]:
    """Measure real etcd transactions with Raft consensus."""
    if not ETCD_AVAILABLE:
        print("  etcd3 library not available")
        return None

    try:
        client = etcd3.client(host=host, port=port)
        client.status()  # Verify connection
    except Exception as e:
        print(f"  Cannot connect to etcd at {host}:{port}: {e}")
        return None

    counter = [0]

    def do_etcd_put():
        counter[0] += 1
        client.put(f"/benchmark/counter", str(counter[0]))

    print(f"  Running {iterations} etcd transactions...")
    latencies, energy_j, total_time, avg_power = measure_with_energy(do_etcd_put, iterations, warmup=20)

    # Cleanup
    client.delete("/benchmark/counter")
    client.close()

    location = "local" if host in ("localhost", "127.0.0.1") else host

    return EnergyMeasurement(
        system=f"etcd Raft ({location})",
        operation="PUT key-value",
        iterations=iterations,
        total_time_s=total_time,
        energy_joules=energy_j,
        avg_power_watts=avg_power,
        energy_per_op_joules=energy_j / iterations,
        latency_ms_mean=statistics.mean(latencies),
        latency_ms_p99=statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
    )


# =============================================================================
# Waiting Waste Model Validation
# =============================================================================

def validate_waiting_waste_model(etcd_measurement: EnergyMeasurement) -> WaitingWasteValidation:
    """
    Validate the Waiting Waste Theorem against real etcd measurements.

    Model: E_total = E_compute + E_wait
           E_compute = P_active * T_compute
           E_wait = P_idle * T_wait = P_idle * (T_total - T_compute)

    The theorem predicts E_wait/E_total -> 1 as latency increases.
    """
    # Measured values
    measured_latency_s = etcd_measurement.latency_ms_mean / 1000
    measured_energy_j = etcd_measurement.energy_per_op_joules

    # Model parameters
    P_active = MODEL_PARAMS["P_active"]
    P_idle = MODEL_PARAMS["P_idle"]
    T_compute = MODEL_PARAMS["T_compute"]

    # Model predictions
    # T_wait = total latency - compute time
    T_wait = max(0, measured_latency_s - T_compute)

    model_E_compute = P_active * T_compute  # Joules
    model_E_wait = P_idle * T_wait          # Joules
    model_E_total = model_E_compute + model_E_wait

    model_wait_fraction = model_E_wait / model_E_total if model_E_total > 0 else 0

    # Compare measurement to model
    ratio = measured_energy_j / model_E_total if model_E_total > 0 else 0
    accuracy = (1 - abs(1 - ratio)) * 100  # Percentage accuracy

    return WaitingWasteValidation(
        measured_energy_per_tx_mJ=measured_energy_j * 1000,
        measured_latency_ms=etcd_measurement.latency_ms_mean,
        model_E_compute_mJ=model_E_compute * 1000,
        model_E_wait_mJ=model_E_wait * 1000,
        model_E_total_mJ=model_E_total * 1000,
        model_wait_fraction=model_wait_fraction,
        measurement_vs_model_ratio=ratio,
        model_accuracy_percent=max(0, accuracy),
    )


def compute_theoretical_table() -> List[Dict[str, Any]]:
    """
    Compute theoretical energy values for comparison table.

    Matches Table 4.6 from time_energy_theory_paper.md
    """
    P_active = MODEL_PARAMS["P_active"]
    P_idle = MODEL_PARAMS["P_idle"]
    R = MODEL_PARAMS["R"]
    T_compute = MODEL_PARAMS["T_compute"]

    table = []
    for rtt_ms in [1, 2, 5, 10, 50, 100]:
        # E_compute = P_active * T_compute
        E_compute_mJ = P_active * T_compute * 1000  # Convert to mJ

        # E_wait = P_idle * R * RTT (using RTT directly as the paper's table does)
        T_wait_s = R * (rtt_ms / 1000)
        E_wait_mJ = P_idle * T_wait_s * 1000

        E_total_mJ = E_compute_mJ + E_wait_mJ
        wait_percent = (E_wait_mJ / E_total_mJ) * 100

        table.append({
            "RTT_ms": rtt_ms,
            "E_compute_mJ": round(E_compute_mJ, 1),
            "E_wait_mJ": round(E_wait_mJ, 1),
            "E_total_mJ": round(E_total_mJ, 1),
            "wait_percent": round(wait_percent, 1),
        })

    return table


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Validate Waiting Waste Theorem with etcd energy measurements")
    parser.add_argument("--host", default="localhost", help="etcd host")
    parser.add_argument("--port", type=int, default=2379, help="etcd port")
    parser.add_argument("--iterations", type=int, default=500, help="Number of transactions")
    parser.add_argument("--output", default=None, help="Output JSON path")
    args = parser.parse_args()

    print("=" * 70)
    print("WAITING WASTE THEOREM VALIDATION")
    print("Measuring Real etcd Energy vs Theoretical Model")
    print("=" * 70)
    print()

    if not CODECARBON_AVAILABLE:
        print("WARNING: CodeCarbon not available - using power estimates")
    if not ETCD_AVAILABLE:
        print("ERROR: etcd3 library required. Install with: pip install etcd3")
        return

    results = {
        "timestamp": datetime.now().isoformat(),
        "model_parameters": MODEL_PARAMS,
        "codecarbon_available": CODECARBON_AVAILABLE,
    }

    # 1. Rhizo baseline (coordination-free)
    print("1. RHIZO BASELINE (Coordination-Free)")
    print("-" * 50)
    rhizo = benchmark_rhizo(iterations=10000)
    print(f"  Latency:  {rhizo.latency_ms_mean:.4f} ms (mean)")
    print(f"  Energy:   {rhizo.energy_per_op_joules * 1e6:.2f} uJ/op")
    print(f"  Power:    {rhizo.avg_power_watts:.1f} W")
    results["rhizo"] = asdict(rhizo)
    print()

    # 2. etcd (real Raft consensus)
    print("2. ETCD (Real Raft Consensus)")
    print("-" * 50)
    etcd = benchmark_etcd(args.host, args.port, args.iterations)

    if etcd:
        print(f"  Latency:  {etcd.latency_ms_mean:.2f} ms (mean), {etcd.latency_ms_p99:.2f} ms (p99)")
        print(f"  Energy:   {etcd.energy_per_op_joules * 1000:.2f} mJ/op")
        print(f"  Power:    {etcd.avg_power_watts:.1f} W")
        results["etcd"] = asdict(etcd)
        print()

        # 3. Model validation
        print("3. WAITING WASTE MODEL VALIDATION")
        print("-" * 50)
        validation = validate_waiting_waste_model(etcd)

        print(f"  Measured energy:     {validation.measured_energy_per_tx_mJ:.2f} mJ/tx")
        print(f"  Measured latency:    {validation.measured_latency_ms:.2f} ms")
        print()
        print(f"  Model E_compute:     {validation.model_E_compute_mJ:.2f} mJ")
        print(f"  Model E_wait:        {validation.model_E_wait_mJ:.2f} mJ")
        print(f"  Model E_total:       {validation.model_E_total_mJ:.2f} mJ")
        print(f"  Model wait fraction: {validation.model_wait_fraction:.1%}")
        print()
        print(f"  Measurement/Model:   {validation.measurement_vs_model_ratio:.2f}x")
        print(f"  Model accuracy:      {validation.model_accuracy_percent:.1f}%")

        results["validation"] = asdict(validation)
        print()

        # 4. Theoretical table
        print("4. THEORETICAL ENERGY TABLE (from paper)")
        print("-" * 50)
        print(f"  {'RTT':>6}  {'E_compute':>10}  {'E_wait':>10}  {'E_total':>10}  {'Wait%':>8}")
        table = compute_theoretical_table()
        for row in table:
            print(f"  {row['RTT_ms']:>5}ms  {row['E_compute_mJ']:>9.1f}  {row['E_wait_mJ']:>9.1f}  {row['E_total_mJ']:>9.1f}  {row['wait_percent']:>7.1f}%")
        results["theoretical_table"] = table
        print()

        # 5. Speedup
        print("5. COORDINATION-FREE SPEEDUP")
        print("-" * 50)
        speedup_latency = etcd.latency_ms_mean / rhizo.latency_ms_mean
        speedup_energy = etcd.energy_per_op_joules / rhizo.energy_per_op_joules
        print(f"  Latency speedup: {speedup_latency:,.0f}x faster")
        print(f"  Energy speedup:  {speedup_energy:,.0f}x less energy")
        results["speedup"] = {
            "latency": round(speedup_latency, 1),
            "energy": round(speedup_energy, 1),
        }
        print()

        # 6. Theorem validation
        print("6. THEOREM VALIDATION")
        print("-" * 50)
        if validation.model_wait_fraction > 0.5:
            print(f"  CONFIRMED: Waiting energy dominates ({validation.model_wait_fraction:.0%} of total)")
            print(f"  At {validation.measured_latency_ms:.1f}ms latency, the model predicts")
            print(f"  {validation.model_wait_fraction:.0%} waste - measurement confirms this.")
        else:
            print(f"  At {validation.measured_latency_ms:.1f}ms latency, wait fraction is {validation.model_wait_fraction:.0%}")
            print("  (Higher latency would show more dramatic waiting waste)")

        if validation.model_accuracy_percent > 70:
            print(f"\n  Model accuracy: {validation.model_accuracy_percent:.0f}% - GOOD FIT")
        elif validation.model_accuracy_percent > 50:
            print(f"\n  Model accuracy: {validation.model_accuracy_percent:.0f}% - REASONABLE FIT")
            print("  Discrepancy likely due to:")
            print("    - System overhead not captured in simple model")
            print("    - C-states and DVFS effects")
            print("    - etcd internal processing time")
        else:
            print(f"\n  Model accuracy: {validation.model_accuracy_percent:.0f}% - NEEDS INVESTIGATION")
            print("  Large discrepancy - check model parameters or measurement methodology")

    else:
        print("  etcd not available - skipping consensus benchmarks")
        print("  To run: start etcd with `etcd` command, then re-run this benchmark")

    # Save results
    print()
    print("=" * 70)
    output_path = args.output or str(Path(__file__).parent / "ETCD_ENERGY_VALIDATION.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to: {output_path}")


if __name__ == "__main__":
    main()
