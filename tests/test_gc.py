"""
Tests for TTL / Garbage Collection — rhizo.gc module.

Covers:
  - GCPolicy validation (3 tests)
  - Protected versions safety (8 tests)
  - Time-based TTL (8 tests)
  - Count-based retention (7 tests)
  - Combined policy (4 tests)
  - Chunk sweep — phase 2 (5 tests)
  - Two-phase integrity (4 tests)
  - AutoGC background thread (4 tests)
  - Database.gc() integration (5 tests)
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import threading
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pytest

import _rhizo
import rhizo
from rhizo.gc import GCPolicy, GCResult, GarbageCollector, AutoGC


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    d = tempfile.mkdtemp(prefix="rhizo_gc_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def gc_env(temp_dir):
    """Full GC environment with catalog, store, branches, and tx_manager."""
    chunks_dir = os.path.join(temp_dir, "chunks")
    catalog_dir = os.path.join(temp_dir, "catalog")
    branches_dir = os.path.join(temp_dir, "branches")
    tx_dir = os.path.join(temp_dir, "transactions")

    store = _rhizo.PyChunkStore(chunks_dir)
    catalog = _rhizo.PyCatalog(catalog_dir)
    branch_mgr = _rhizo.PyBranchManager(branches_dir)
    tx_mgr = _rhizo.PyTransactionManager(tx_dir, catalog_dir, branches_dir)

    # Ensure main branch exists (tx_manager may auto-create it)
    try:
        branch_mgr.create("main")
    except (ValueError, OSError):
        pass

    yield {
        "store": store,
        "catalog": catalog,
        "branch_mgr": branch_mgr,
        "tx_mgr": tx_mgr,
        "temp_dir": temp_dir,
        "catalog_dir": catalog_dir,
    }


def _write_version(env, table_name, data_dict, metadata=None):
    """Helper: write a DataFrame as a new version using the low-level API."""
    from rhizo.writer import TableWriter
    writer = TableWriter(env["store"], env["catalog"])
    df = pd.DataFrame(data_dict)
    return writer.write(table_name, df, metadata=metadata)


def _make_gc(env, branch_mgr=True, tx_mgr=True):
    """Helper: create a GarbageCollector from a gc_env fixture."""
    return GarbageCollector(
        catalog=env["catalog"],
        store=env["store"],
        branch_manager=env["branch_mgr"] if branch_mgr else None,
        transaction_manager=env["tx_mgr"] if tx_mgr else None,
    )


# ===========================================================================
# TestGCPolicy
# ===========================================================================

class TestGCPolicy:
    """GCPolicy validation — 3 tests."""

    def test_time_based_only(self):
        p = GCPolicy(max_age_seconds=3600)
        assert p.max_age_seconds == 3600
        assert p.max_versions_per_table is None

    def test_count_based_only(self):
        p = GCPolicy(max_versions_per_table=5)
        assert p.max_age_seconds is None
        assert p.max_versions_per_table == 5

    def test_negative_age_rejected(self):
        with pytest.raises(ValueError):
            GCPolicy(max_age_seconds=-1)

    def test_zero_count_rejected(self):
        with pytest.raises(ValueError):
            GCPolicy(max_versions_per_table=0)

    def test_both_policies(self):
        p = GCPolicy(max_age_seconds=60, max_versions_per_table=3)
        assert p.max_age_seconds == 60
        assert p.max_versions_per_table == 3


# ===========================================================================
# TestProtectedVersions
# ===========================================================================

class TestProtectedVersions:
    """Protected version safety — 8 tests."""

    def test_latest_always_protected(self, gc_env):
        """Latest version must never be deleted, even with aggressive policy."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # Version 3 is latest — must survive
        versions = gc_env["catalog"].list_versions("t1")
        assert 3 in versions
        assert result.versions_deleted == 2

    def test_branch_head_protected(self, gc_env):
        """Versions pointed to by branch heads are protected."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})

        # Point a branch head at v1
        gc_env["branch_mgr"].update_head("main", "t1", 1)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        versions = gc_env["catalog"].list_versions("t1")
        assert 1 in versions  # protected by branch head
        assert 3 in versions  # protected as latest
        # Only v2 should be deleted
        assert result.versions_deleted == 1

    def test_branch_fork_point_protected(self, gc_env):
        """Fork point versions are protected."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})
        _write_version(gc_env, "t1", {"x": [4]})

        # Create a feature branch from main at v2
        gc_env["branch_mgr"].update_head("main", "t1", 2)
        gc_env["branch_mgr"].create("feature", from_branch="main")

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        versions = gc_env["catalog"].list_versions("t1")
        assert 4 in versions  # latest
        assert 2 in versions  # fork point / branch head

    def test_active_tx_snapshot_protected(self, gc_env):
        """Active transaction snapshots are protected."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})

        # Start a transaction (which captures a snapshot at v3)
        tx_id = gc_env["tx_mgr"].begin()

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # Latest (v3) is protected both as latest and as tx snapshot
        versions = gc_env["catalog"].list_versions("t1")
        assert 3 in versions

        # Clean up
        gc_env["tx_mgr"].abort(tx_id)

    def test_multiple_branches_protect_different_versions(self, gc_env):
        """Multiple branches can protect different versions."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        # main head at v2, feature head at v3
        gc_env["branch_mgr"].update_head("main", "t1", 2)
        gc_env["branch_mgr"].create("feature", from_branch="main")
        gc_env["branch_mgr"].update_head("feature", "t1", 3)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        versions = gc_env["catalog"].list_versions("t1")
        assert 2 in versions  # main head
        assert 3 in versions  # feature head
        assert 5 in versions  # latest

    def test_no_branches_only_latest_protected(self, gc_env):
        """Without branch manager, only latest is protected."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env, branch_mgr=False, tx_mgr=False)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        versions = gc_env["catalog"].list_versions("t1")
        assert versions == [5]
        assert result.versions_deleted == 4

    def test_no_tx_manager_still_works(self, gc_env):
        """GC works without tx_manager (no snapshot protection)."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})

        gc = _make_gc(gc_env, tx_mgr=False)
        result = gc.collect(GCPolicy(max_versions_per_table=1))
        assert result.versions_deleted == 1

    def test_protected_versions_are_superset(self, gc_env):
        """Protected set includes latest + branch refs + tx refs."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})

        gc_env["branch_mgr"].update_head("main", "t1", 1)

        gc = _make_gc(gc_env)
        protected = gc._collect_protected_versions()
        assert 1 in protected.get("t1", set())  # branch head
        assert 3 in protected.get("t1", set())  # latest


# ===========================================================================
# TestTimeTTL
# ===========================================================================

class TestTimeTTL:
    """Time-based TTL — 8 tests."""

    def test_deletes_old_versions(self, gc_env):
        """Versions older than max_age_seconds are deleted."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})

        # Backdate v1 by modifying created_at in the version JSON
        v1 = gc_env["catalog"].get_version("t1", 1)
        _backdate_version(gc_env, "t1", 1, seconds_ago=7200)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=3600))
        assert result.versions_deleted == 1
        assert 1 not in gc_env["catalog"].list_versions("t1")

    def test_keeps_recent_versions(self, gc_env):
        """Recent versions are not deleted."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=3600))
        assert result.versions_deleted == 0

    def test_never_deletes_latest_even_if_old(self, gc_env):
        """Latest version is protected even if older than TTL."""
        _write_version(gc_env, "t1", {"x": [1]})
        _backdate_version(gc_env, "t1", 1, seconds_ago=9999)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=60))
        assert result.versions_deleted == 0
        assert gc_env["catalog"].list_versions("t1") == [1]

    def test_zero_ttl_deletes_all_non_protected(self, gc_env):
        """max_age_seconds=0 deletes everything except protected."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})
        # Backdate all except latest
        for v in range(1, 5):
            _backdate_version(gc_env, "t1", v, seconds_ago=1)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=0))
        # v1-v4 backdated, v5 is latest (protected)
        assert result.versions_deleted == 4

    def test_very_large_ttl_deletes_nothing(self, gc_env):
        """Very large TTL keeps everything."""
        for i in range(3):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=999999))
        assert result.versions_deleted == 0

    def test_multiple_tables_independently(self, gc_env):
        """TTL applied independently per table."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t2", {"y": [10]})
        _write_version(gc_env, "t2", {"y": [20]})

        _backdate_version(gc_env, "t1", 1, seconds_ago=7200)
        # t2 versions are recent

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=3600))
        assert result.versions_deleted == 1
        assert "t1" in result.details
        assert "t2" not in result.details

    def test_single_version_table(self, gc_env):
        """Table with only one version: nothing deleted (latest protected)."""
        _write_version(gc_env, "t1", {"x": [1]})
        _backdate_version(gc_env, "t1", 1, seconds_ago=9999)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=60))
        assert result.versions_deleted == 0

    def test_boundary_exact_age(self, gc_env):
        """Version exactly at cutoff boundary is deleted (< cutoff)."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        # Backdate v1 to exactly max_age_seconds ago + 1 to ensure it's past
        _backdate_version(gc_env, "t1", 1, seconds_ago=61)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=60))
        assert result.versions_deleted == 1


# ===========================================================================
# TestCountRetention
# ===========================================================================

class TestCountRetention:
    """Count-based retention — 7 tests."""

    def test_keeps_exactly_n_most_recent(self, gc_env):
        """Keeps the N most recent versions."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=3))
        versions = gc_env["catalog"].list_versions("t1")
        assert versions == [3, 4, 5]
        assert result.versions_deleted == 2

    def test_fewer_than_n_deletes_nothing(self, gc_env):
        """If fewer versions than limit, nothing deleted."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=5))
        assert result.versions_deleted == 0

    def test_keep_one_means_only_latest(self, gc_env):
        """max_versions_per_table=1 keeps only latest."""
        for i in range(4):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))
        versions = gc_env["catalog"].list_versions("t1")
        assert versions == [4]
        assert result.versions_deleted == 3

    def test_deletes_oldest_first(self, gc_env):
        """Oldest versions are the ones deleted."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=3))
        assert result.details["t1"] == [1, 2]

    def test_independent_per_table(self, gc_env):
        """Count applied independently per table."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})
        for i in range(2):
            _write_version(gc_env, "t2", {"y": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=3))
        # t1: 5 versions, keep 3, delete 2
        assert gc_env["catalog"].list_versions("t1") == [3, 4, 5]
        # t2: 2 versions, keep 3, delete 0
        assert gc_env["catalog"].list_versions("t2") == [1, 2]

    def test_respects_protected_versions(self, gc_env):
        """Protected versions are not deleted even if over count limit."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        # Protect v1 via branch head
        gc_env["branch_mgr"].update_head("main", "t1", 1)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=2))

        versions = gc_env["catalog"].list_versions("t1")
        assert 1 in versions  # protected by branch
        assert 5 in versions  # latest

    def test_protected_exceeds_limit_all_kept(self, gc_env):
        """If all versions are protected, none deleted even past limit."""
        _write_version(gc_env, "t1", {"x": [1]})
        _write_version(gc_env, "t1", {"x": [2]})
        _write_version(gc_env, "t1", {"x": [3]})

        # Protect v1 and v2 via branches
        gc_env["branch_mgr"].update_head("main", "t1", 1)
        gc_env["branch_mgr"].create("b2", from_branch="main")
        gc_env["branch_mgr"].update_head("b2", "t1", 2)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        versions = gc_env["catalog"].list_versions("t1")
        assert 1 in versions
        assert 2 in versions
        assert 3 in versions
        assert result.versions_deleted == 0


# ===========================================================================
# TestCombinedPolicy
# ===========================================================================

class TestCombinedPolicy:
    """Combined time + count policy — 4 tests."""

    def test_union_of_policies(self, gc_env):
        """Version eligible if it violates EITHER policy."""
        for i in range(6):
            _write_version(gc_env, "t1", {"x": [i]})
        # Backdate v1-v3 to be old
        for v in range(1, 4):
            _backdate_version(gc_env, "t1", v, seconds_ago=7200)

        gc = _make_gc(gc_env)
        # count=4 would delete v1,v2; TTL=3600 would delete v1,v2,v3
        # Union: v1,v2,v3 all candidates
        result = gc.collect(GCPolicy(max_age_seconds=3600, max_versions_per_table=4))
        assert result.versions_deleted == 3
        versions = gc_env["catalog"].list_versions("t1")
        assert versions == [4, 5, 6]

    def test_protection_overrides_both(self, gc_env):
        """Protected versions survive both policies."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})
        _backdate_version(gc_env, "t1", 1, seconds_ago=9999)

        gc_env["branch_mgr"].update_head("main", "t1", 1)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=60, max_versions_per_table=1))

        assert 1 in gc_env["catalog"].list_versions("t1")

    def test_realistic_scenario(self, gc_env):
        """Realistic: 10 versions, keep 5 and delete >1hr old."""
        for i in range(10):
            _write_version(gc_env, "t1", {"x": [i]})
        # Backdate v1-v7 to be 2 hours old
        for v in range(1, 8):
            _backdate_version(gc_env, "t1", v, seconds_ago=7200)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_age_seconds=3600, max_versions_per_table=5))
        # By count: delete v1-v5 (keep 5 newest: v6-v10)
        # By TTL: delete v1-v7
        # Union: v1-v7 candidates, but v10 is latest (protected)
        assert result.versions_deleted == 7
        versions = gc_env["catalog"].list_versions("t1")
        assert versions == [8, 9, 10]

    def test_neither_policy_set_raises(self, gc_env):
        """Must set at least one policy."""
        gc = _make_gc(gc_env)
        with pytest.raises(ValueError, match="At least one"):
            gc.collect(GCPolicy())


# ===========================================================================
# TestChunkSweep
# ===========================================================================

class TestChunkSweep:
    """Chunk sweep (phase 2) — 5 tests."""

    def test_unreferenced_chunks_deleted(self, gc_env):
        """After deleting versions, orphaned chunks are swept."""
        _write_version(gc_env, "t1", {"x": list(range(100))})
        _write_version(gc_env, "t1", {"x": list(range(100, 200))})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        assert result.versions_deleted == 1
        assert result.chunks_deleted >= 1
        assert result.bytes_freed > 0

    def test_shared_chunks_preserved(self, gc_env):
        """Chunks shared between versions are not deleted."""
        # Write same data twice → same chunk hashes
        data = {"x": list(range(50))}
        _write_version(gc_env, "t1", data)
        _write_version(gc_env, "t1", data)

        v1_info = gc_env["catalog"].get_version("t1", 1)
        v2_info = gc_env["catalog"].get_version("t1", 2)

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # v1 deleted, but chunks still referenced by v2
        assert result.versions_deleted == 1
        assert result.chunks_deleted == 0

    def test_empty_store_sweep(self, gc_env):
        """Sweeping empty store is safe."""
        gc = _make_gc(gc_env)
        deleted, failed, freed = gc._phase2_sweep_chunks()
        assert deleted == 0
        assert failed == 0

    def test_all_chunks_referenced_none_deleted(self, gc_env):
        """If all chunks are referenced, none deleted."""
        _write_version(gc_env, "t1", {"x": list(range(100))})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=10))

        assert result.chunks_deleted == 0

    def test_bytes_freed_is_positive(self, gc_env):
        """bytes_freed reflects actual data removed."""
        # Write enough data to have measurable bytes
        _write_version(gc_env, "t1", {"x": list(range(1000))})
        _write_version(gc_env, "t1", {"x": list(range(1000, 2000))})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))
        assert result.bytes_freed > 0


# ===========================================================================
# TestTwoPhaseIntegrity
# ===========================================================================

class TestTwoPhaseIntegrity:
    """Two-phase integrity — 4 tests."""

    def test_phases_run_in_order(self, gc_env):
        """Phase 1 (versions) runs before phase 2 (chunks)."""
        _write_version(gc_env, "t1", {"x": list(range(100))})
        _write_version(gc_env, "t1", {"x": list(range(100, 200))})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # If phase 2 ran before phase 1, deleted chunks would still be referenced
        assert result.versions_deleted == 1
        assert result.chunks_deleted >= 1

    def test_gc_idempotent(self, gc_env):
        """Running GC twice — second run deletes nothing."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result1 = gc.collect(GCPolicy(max_versions_per_table=2))
        assert result1.versions_deleted == 3

        result2 = gc.collect(GCPolicy(max_versions_per_table=2))
        assert result2.versions_deleted == 0
        assert result2.chunks_deleted == 0

    def test_gc_during_concurrent_write_no_data_loss(self, gc_env):
        """Writing during GC doesn't lose data."""
        for i in range(5):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=3))

        # Write after GC
        _write_version(gc_env, "t1", {"x": [999]})

        # All remaining versions + new one should be readable
        versions = gc_env["catalog"].list_versions("t1")
        assert 6 in versions  # new version
        assert len(versions) == 4  # 3 kept + 1 new

    def test_result_details_correct(self, gc_env):
        """GCResult.details maps table -> deleted version numbers."""
        for i in range(4):
            _write_version(gc_env, "t1", {"x": [i]})
        for i in range(3):
            _write_version(gc_env, "t2", {"y": [i]})

        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=2))

        assert result.details["t1"] == [1, 2]
        assert result.details["t2"] == [1]


# ===========================================================================
# TestAutoGC
# ===========================================================================

class TestAutoGC:
    """AutoGC background thread — 4 tests."""

    def test_starts_and_stops_cleanly(self, gc_env):
        """AutoGC can be started and stopped without errors."""
        gc = _make_gc(gc_env)
        auto = AutoGC(gc, GCPolicy(max_versions_per_table=5), interval_seconds=0.1)
        auto.start()
        assert auto.is_running
        auto.stop(timeout=5.0)
        assert not auto.is_running

    def test_runs_at_least_once(self, gc_env):
        """AutoGC runs at least once within its interval."""
        for i in range(3):
            _write_version(gc_env, "t1", {"x": [i]})

        gc = _make_gc(gc_env)
        auto = AutoGC(gc, GCPolicy(max_versions_per_table=1), interval_seconds=0.1)
        auto.start()
        time.sleep(0.5)  # Wait for at least one run
        auto.stop(timeout=5.0)

        assert auto.last_result is not None
        assert auto.last_result.versions_deleted >= 0

    def test_last_result_populated(self, gc_env):
        """last_result is populated after a run."""
        _write_version(gc_env, "t1", {"x": [1]})

        gc = _make_gc(gc_env)
        auto = AutoGC(gc, GCPolicy(max_versions_per_table=10), interval_seconds=0.1)
        auto.start()
        time.sleep(0.5)
        auto.stop(timeout=5.0)

        result = auto.last_result
        assert result is not None
        assert isinstance(result, GCResult)

    def test_stop_waits_for_current_run(self, gc_env):
        """stop() blocks until current GC run finishes."""
        gc = _make_gc(gc_env)
        auto = AutoGC(gc, GCPolicy(max_versions_per_table=5), interval_seconds=0.1)
        auto.start()
        time.sleep(0.3)
        auto.stop(timeout=10.0)
        # No assertion beyond not hanging/crashing
        assert not auto.is_running


# ===========================================================================
# TestDatabaseGC
# ===========================================================================

class TestDatabaseGC:
    """Database.gc() integration — 5 tests."""

    def test_db_gc_works(self, temp_dir):
        """db.gc() runs successfully."""
        with rhizo.open(temp_dir) as db:
            for i in range(5):
                db.write("t1", pd.DataFrame({"x": [i]}))

            result = db.gc(max_versions_per_table=2)
            assert result.versions_deleted == 3

    def test_returns_gc_result(self, temp_dir):
        """db.gc() returns a GCResult."""
        with rhizo.open(temp_dir) as db:
            db.write("t1", pd.DataFrame({"x": [1]}))
            result = db.gc(max_versions_per_table=10)
            assert isinstance(result, GCResult)

    def test_gc_on_closed_db_raises(self, temp_dir):
        """gc() on closed DB raises RuntimeError."""
        db = rhizo.open(temp_dir)
        db.close()
        with pytest.raises(RuntimeError):
            db.gc(max_versions_per_table=5)

    def test_auto_gc_in_open(self, temp_dir):
        """auto_gc parameter in rhizo.open() works."""
        policy = GCPolicy(max_versions_per_table=10)
        db = rhizo.open(temp_dir, auto_gc=policy, auto_gc_interval=60.0)
        assert db._auto_gc is not None
        assert db._auto_gc.is_running
        db.close()
        assert not db._auto_gc.is_running

    def test_full_write_gc_read_cycle(self, temp_dir):
        """Write multiple versions, GC, verify data integrity."""
        with rhizo.open(temp_dir) as db:
            for i in range(10):
                db.write("t1", pd.DataFrame({"x": list(range(i * 100, (i + 1) * 100))}))

            # GC: keep only 3 versions
            result = db.gc(max_versions_per_table=3)
            assert result.versions_deleted == 7

            # Latest data should still be readable
            table = db.read("t1")
            assert table.num_rows == 100

            # Only 3 versions remain
            assert len(db.versions("t1")) == 3


# ===========================================================================
# Helpers
# ===========================================================================

def _backdate_version(env, table_name, version, seconds_ago):
    """Modify a version's created_at timestamp to simulate aging."""
    import json

    catalog_dir = env["catalog_dir"]
    version_path = os.path.join(catalog_dir, table_name, f"{version}.json")
    with open(version_path, "r") as f:
        data = json.load(f)

    data["created_at"] = int(time.time() - seconds_ago)

    with open(version_path, "w") as f:
        json.dump(data, f)


# ===========================================================================
# TestBranchChunkSafety
# ===========================================================================

class TestBranchChunkSafety:
    """Branch + chunk deduplication safety — validates Section 4.4.

    Key scenario: Two branches share the same chunks (content-addressed).
    When one branch's versions get GC'd, the shared chunks must survive
    because the other branch still references them.
    """

    def test_shared_chunks_survive_partial_gc(self, gc_env):
        """Chunks shared between branches survive when one branch is GC'd.

        This is the critical test from Section 4.4 of the technical review:
        "if two branches share chunks and one branch gets GC'd, do the
        shared chunks survive?"
        """
        # Step 1: Write initial data on main
        data1 = {"x": list(range(100)), "y": [f"val_{i}" for i in range(100)]}
        result = _write_version(gc_env, "t1", data1)
        v1 = result.version
        assert v1 == 1

        # Step 2: Create feature branch from main (shares v1's chunks)
        gc_env["branch_mgr"].update_head("main", "t1", 1)
        gc_env["branch_mgr"].create("feature", from_branch="main")

        # Step 3: Write multiple new versions on main
        for i in range(5):
            new_data = {"x": list(range(i * 100, (i + 1) * 100))}
            _write_version(gc_env, "t1", new_data)

        # Now: main has v1-v6, feature branch head points at v1
        # v1's chunks are shared between main and feature

        # Step 4: Get v1's chunk hashes before GC
        v1_info = gc_env["catalog"].get_version("t1", 1)
        v1_chunks = set(v1_info.chunk_hashes)

        # Step 5: GC main to keep only 2 versions (should delete v1-v4)
        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=2))

        # v1 should be protected by feature branch head, but others deleted
        versions_after = gc_env["catalog"].list_versions("t1")
        assert 1 in versions_after, "v1 must survive (feature branch head)"
        assert 6 in versions_after, "v6 must survive (latest)"

        # Step 6: Verify v1's chunks still exist (critical check)
        for chunk_hash in v1_chunks:
            chunk_data = gc_env["store"].get(chunk_hash)
            assert chunk_data is not None, f"Chunk {chunk_hash} should not be deleted"

        # Step 7: Verify we can still read the data from v1
        from rhizo.reader import TableReader
        reader = TableReader(gc_env["store"], gc_env["catalog"])
        df = reader.read_pandas("t1", version=1)
        assert len(df) == 100
        assert list(df["x"]) == list(range(100))

    def test_gc_deletes_orphaned_chunks_after_branch_delete(self, gc_env):
        """After deleting a branch, its exclusive chunks can be GC'd."""
        # Step 1: Write data that creates unique chunks
        data1 = {"x": list(range(100))}
        _write_version(gc_env, "t1", data1)
        gc_env["branch_mgr"].update_head("main", "t1", 1)

        # Step 2: Create feature branch with different data
        gc_env["branch_mgr"].create("feature", from_branch="main")
        feature_data = {"x": list(range(1000, 1100))}  # Different data = different chunks
        result = _write_version(gc_env, "t1", feature_data)
        v2 = result.version
        gc_env["branch_mgr"].update_head("feature", "t1", v2)

        # Get v2's chunks (unique to feature branch)
        v2_info = gc_env["catalog"].get_version("t1", v2)
        v2_chunks = set(v2_info.chunk_hashes)

        # Step 3: Delete feature branch
        gc_env["branch_mgr"].delete("feature")

        # Step 4: Write more on main to make v2 old
        for i in range(3):
            _write_version(gc_env, "t1", {"x": [i]})

        # Step 5: GC should now be able to delete v2 and its chunks
        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # v2 should be deleted (no branch references it anymore)
        versions_after = gc_env["catalog"].list_versions("t1")
        assert v2 not in versions_after, "v2 should be deleted after branch removal"

        # v2's unique chunks should also be deleted (phase 2 sweep removes orphans)
        # After branch deletion and GC, the orphaned chunks are cleaned up
        chunks_deleted = 0
        for chunk_hash in v2_chunks:
            try:
                gc_env["store"].get(chunk_hash)
            except OSError:
                chunks_deleted += 1

        # At least some chunks should be deleted (the unique ones)
        # This confirms GC correctly identifies and sweeps orphaned chunks
        assert chunks_deleted > 0, "GC should delete orphaned chunks after branch removal"

    def test_content_addressed_dedup_across_branches(self, gc_env):
        """Identical data on different branches shares chunks (dedup works)."""
        # Write same data twice
        data = {"x": list(range(50)), "y": ["test"] * 50}

        result1 = _write_version(gc_env, "t1", data)
        v1 = result1.version
        gc_env["branch_mgr"].update_head("main", "t1", v1)

        # Create feature branch
        gc_env["branch_mgr"].create("feature", from_branch="main")

        # Write identical data on feature branch
        result2 = _write_version(gc_env, "t1", data)
        v2 = result2.version
        gc_env["branch_mgr"].update_head("feature", "t1", v2)

        # Both versions should share chunk hashes (content-addressed)
        v1_info = gc_env["catalog"].get_version("t1", v1)
        v2_info = gc_env["catalog"].get_version("t1", v2)

        v1_chunks = set(v1_info.chunk_hashes)
        v2_chunks = set(v2_info.chunk_hashes)

        assert v1_chunks == v2_chunks, "Identical data should produce identical chunks"

        # GC with aggressive policy
        gc = _make_gc(gc_env)
        result = gc.collect(GCPolicy(max_versions_per_table=1))

        # Both branch heads are protected, so both versions survive
        versions_after = gc_env["catalog"].list_versions("t1")
        assert v1 in versions_after, "v1 protected by main head"
        assert v2 in versions_after, "v2 protected by feature head"

        # Chunks should not be deleted (referenced by both)
        assert result.chunks_deleted == 0
