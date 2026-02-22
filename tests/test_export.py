"""
Comprehensive tests for the export functionality.

Tests Parquet, CSV, and JSON export at all API levels:
- ExportEngine (core)
- Database.export() (high-level)
- QueryEngine.export_table() / export_query()
- rhizo.export() (standalone)
"""

import json
import os
import shutil
import tempfile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rhizo
from rhizo.export import ExportEngine, ExportResult, detect_format


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db_path():
    """Create a temporary directory for the database."""
    tmpdir = tempfile.mkdtemp(prefix="rhizo_export_test_")
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def db_with_data(temp_db_path):
    """Create a database with sample data."""
    db = rhizo.open(temp_db_path)
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "score": [85.5, 92.0, 78.5, 95.0, 88.5],
        "active": [True, False, True, True, False],
    })
    db.write("users", df)
    yield db, temp_db_path
    db.close()


@pytest.fixture
def db_with_versions(temp_db_path):
    """Create a database with multiple versions of a table."""
    db = rhizo.open(temp_db_path)

    df_v1 = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    db.write("data", df_v1)

    df_v2 = pd.DataFrame({"id": [1, 2, 3], "value": ["x", "y", "z"]})
    db.write("data", df_v2)

    yield db, temp_db_path
    db.close()


@pytest.fixture
def export_dir(temp_db_path):
    """Create a subdirectory for export output files."""
    out = os.path.join(temp_db_path, "exports")
    os.makedirs(out)
    return out


# ---------------------------------------------------------------------------
# TestFormatDetection
# ---------------------------------------------------------------------------

class TestFormatDetection:
    """Tests for format auto-detection."""

    def test_parquet_extension(self):
        assert detect_format("out.parquet") == "parquet"

    def test_pq_extension(self):
        assert detect_format("out.pq") == "parquet"

    def test_csv_extension(self):
        assert detect_format("data.csv") == "csv"

    def test_json_extension(self):
        assert detect_format("data.json") == "json"

    def test_jsonl_extension(self):
        assert detect_format("data.jsonl") == "json"

    def test_ndjson_extension(self):
        assert detect_format("data.ndjson") == "json"

    def test_explicit_format_overrides_extension(self):
        """Explicit format= takes priority over file extension."""
        assert detect_format("out.csv", format="parquet") == "parquet"
        assert detect_format("out.parquet", format="json") == "json"

    def test_unknown_extension_raises(self):
        with pytest.raises(ValueError, match="Cannot detect export format"):
            detect_format("out.xlsx")

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError, match="Unsupported export format"):
            detect_format("out.parquet", format="avro")

    def test_case_insensitive_extension(self):
        assert detect_format("OUT.PARQUET") == "parquet"
        assert detect_format("data.CSV") == "csv"

    def test_case_insensitive_format(self):
        assert detect_format("out.txt", format="PARQUET") == "parquet"
        assert detect_format("out.txt", format="Json") == "json"


# ---------------------------------------------------------------------------
# TestParquetExport
# ---------------------------------------------------------------------------

class TestParquetExport:
    """Tests for Parquet export."""

    def test_basic_roundtrip(self, db_with_data, export_dir):
        """Export to Parquet, re-read, verify data matches."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "users.parquet")

        result = db.export("users", out_path)

        assert isinstance(result, ExportResult)
        assert result.format == "parquet"
        assert result.row_count == 5
        assert result.file_size_bytes > 0
        assert os.path.isfile(result.path)
        assert set(result.columns_exported) == {"id", "name", "score", "active"}

        # Re-read and verify
        exported = pq.read_table(out_path)
        original = db.read("users")
        assert exported.num_rows == original.num_rows
        assert set(exported.column_names) == set(original.column_names)
        assert exported.column("id").to_pylist() == original.column("id").to_pylist()
        assert exported.column("name").to_pylist() == original.column("name").to_pylist()

    def test_single_chunk_fast_path(self, db_with_data, export_dir):
        """Small table (1 chunk) uses the raw-bytes fast path."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "fast.parquet")

        result = db.export("users", out_path)
        assert result.row_count == 5

        # Verify it's a valid Parquet file
        pf = pq.ParquetFile(out_path)
        assert pf.metadata.num_rows == 5

    def test_multi_chunk_export(self, temp_db_path, export_dir):
        """Force multi-chunk by using small chunk_size_rows."""
        from _rhizo import PyChunkStore, PyCatalog
        from rhizo.writer import TableWriter
        from rhizo.reader import TableReader

        chunks_dir = os.path.join(temp_db_path, "mc_chunks")
        catalog_dir = os.path.join(temp_db_path, "mc_catalog")
        store = PyChunkStore(chunks_dir)
        catalog = PyCatalog(catalog_dir)

        # Write with very small chunks
        writer = TableWriter(store, catalog, chunk_size_rows=2)
        df = pd.DataFrame({"id": range(10), "val": range(10, 20)})
        write_result = writer.write("multi", df)
        assert write_result.chunk_count > 1  # Must be multi-chunk

        reader = TableReader(store, catalog)
        engine = ExportEngine(reader, store, catalog)

        out_path = os.path.join(export_dir, "multi.parquet")
        result = engine.export_table("multi", out_path)

        assert result.row_count == 10
        exported = pq.read_table(out_path)
        assert exported.num_rows == 10
        assert sorted(exported.column("id").to_pylist()) == list(range(10))

    def test_column_projection(self, db_with_data, export_dir):
        """Export only selected columns."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "projected.parquet")

        result = db.export("users", out_path, columns=["id", "name"])

        assert result.columns_exported == ["id", "name"]
        exported = pq.read_table(out_path)
        assert exported.column_names == ["id", "name"]
        assert exported.num_rows == 5

    def test_version_specific_export(self, db_with_versions, export_dir):
        """Export a specific version."""
        db, _ = db_with_versions
        out_v1 = os.path.join(export_dir, "v1.parquet")
        out_v2 = os.path.join(export_dir, "v2.parquet")

        db.export("data", out_v1, version=1)
        db.export("data", out_v2, version=2)

        v1 = pq.read_table(out_v1)
        v2 = pq.read_table(out_v2)
        assert v1.num_rows == 2
        assert v2.num_rows == 3

    def test_compression_snappy(self, db_with_data, export_dir):
        """Export with snappy compression."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "snappy.parquet")

        result = db.export("users", out_path, compression="snappy")
        assert result.file_size_bytes > 0

        pf = pq.ParquetFile(out_path)
        assert pf.metadata.num_rows == 5

    def test_compression_none(self, db_with_data, export_dir):
        """Export with no compression."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "none.parquet")

        result = db.export("users", out_path, compression="none")
        assert result.file_size_bytes > 0

    def test_schema_preservation(self, temp_db_path, export_dir):
        """Verify schema types survive the export roundtrip."""
        with rhizo.open(temp_db_path) as db:
            df = pd.DataFrame({
                "int_col": pd.array([1, 2, 3], dtype="int64"),
                "float_col": pd.array([1.1, 2.2, 3.3], dtype="float64"),
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            })
            db.write("typed", df)

            out_path = os.path.join(export_dir, "typed.parquet")
            db.export("typed", out_path)

        exported = pq.read_table(out_path)
        schema = exported.schema
        assert schema.field("int_col").type == pa.int64()
        assert schema.field("float_col").type == pa.float64()
        assert pa.types.is_string(schema.field("str_col").type) or pa.types.is_large_string(schema.field("str_col").type)
        assert schema.field("bool_col").type == pa.bool_()

    def test_pq_extension(self, db_with_data, export_dir):
        """The .pq extension is recognized as Parquet."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "users.pq")
        result = db.export("users", out_path)
        assert result.format == "parquet"
        assert pq.read_table(out_path).num_rows == 5


# ---------------------------------------------------------------------------
# TestCSVExport
# ---------------------------------------------------------------------------

class TestCSVExport:
    """Tests for CSV export."""

    def test_basic_roundtrip(self, db_with_data, export_dir):
        """Export to CSV, re-read with pandas, verify data."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "users.csv")

        result = db.export("users", out_path)

        assert result.format == "csv"
        assert result.row_count == 5
        assert result.file_size_bytes > 0

        df = pd.read_csv(out_path)
        assert len(df) == 5
        assert set(df.columns) >= {"id", "name", "score"}

    def test_column_projection(self, db_with_data, export_dir):
        """Export only selected columns to CSV."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "proj.csv")

        result = db.export("users", out_path, columns=["id", "name"])
        assert set(result.columns_exported) == {"id", "name"}

        df = pd.read_csv(out_path)
        assert list(df.columns) == ["id", "name"]

    def test_version_specific(self, db_with_versions, export_dir):
        """Export a specific version to CSV."""
        db, _ = db_with_versions
        out_path = os.path.join(export_dir, "v1.csv")

        db.export("data", out_path, version=1)
        df = pd.read_csv(out_path)
        assert len(df) == 2

    def test_explicit_format(self, db_with_data, export_dir):
        """Explicit format= works even with non-standard extension."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "data.txt")

        result = db.export("users", out_path, format="csv")
        assert result.format == "csv"
        df = pd.read_csv(out_path)
        assert len(df) == 5


# ---------------------------------------------------------------------------
# TestJSONExport
# ---------------------------------------------------------------------------

class TestJSONExport:
    """Tests for JSON export."""

    def test_basic_roundtrip(self, db_with_data, export_dir):
        """Export to JSON, re-read, verify data."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "users.json")

        result = db.export("users", out_path)

        assert result.format == "json"
        assert result.row_count == 5

        with open(out_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        assert len(records) == 5
        assert all("id" in r for r in records)
        assert all("name" in r for r in records)

    def test_column_projection(self, db_with_data, export_dir):
        """Export only selected columns to JSON."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "proj.json")

        result = db.export("users", out_path, columns=["id", "score"])

        with open(out_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        assert set(records[0].keys()) == {"id", "score"}

    def test_version_specific(self, db_with_versions, export_dir):
        """Export a specific version to JSON."""
        db, _ = db_with_versions
        out_path = os.path.join(export_dir, "v1.json")

        db.export("data", out_path, version=1)
        with open(out_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        assert len(records) == 2

    def test_jsonl_extension(self, db_with_data, export_dir):
        """The .jsonl extension is recognized as JSON."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "data.jsonl")
        result = db.export("users", out_path)
        assert result.format == "json"


# ---------------------------------------------------------------------------
# TestQueryExport
# ---------------------------------------------------------------------------

class TestQueryExport:
    """Tests for exporting SQL query results."""

    def test_filtered_query(self, db_with_data, export_dir):
        """Export a filtered query result."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "filtered.parquet")

        result = db.engine.export_query(
            "SELECT * FROM users WHERE score > 90",
            out_path,
        )

        exported = pq.read_table(out_path)
        assert exported.num_rows == 2  # Bob (92) and Dave (95)

    def test_aggregation_query(self, db_with_data, export_dir):
        """Export an aggregation query result."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "agg.csv")

        result = db.engine.export_query(
            "SELECT active, COUNT(*) as cnt, AVG(score) as avg_score FROM users GROUP BY active",
            out_path,
        )

        df = pd.read_csv(out_path)
        assert len(df) == 2  # true and false groups
        assert "cnt" in df.columns
        assert "avg_score" in df.columns

    def test_query_to_json(self, db_with_data, export_dir):
        """Export a query result to JSON."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "selected.json")

        result = db.engine.export_query(
            "SELECT name, score FROM users ORDER BY score DESC LIMIT 3",
            out_path,
        )

        with open(out_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        assert len(records) == 3
        # Should be ordered by score DESC
        assert records[0]["score"] >= records[1]["score"]


# ---------------------------------------------------------------------------
# TestDatabaseExport
# ---------------------------------------------------------------------------

class TestDatabaseExport:
    """Tests for the Database.export() and rhizo.export() APIs."""

    def test_database_export(self, db_with_data, export_dir):
        """Database.export() works correctly."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "db_export.parquet")

        result = db.export("users", out_path)
        assert isinstance(result, ExportResult)
        assert result.row_count == 5

    def test_standalone_export(self, temp_db_path, export_dir):
        """rhizo.export() standalone function works."""
        # Create a database with data first
        with rhizo.open(temp_db_path) as db:
            db.write("items", pd.DataFrame({"x": [10, 20, 30]}))

        out_path = os.path.join(export_dir, "standalone.parquet")
        result = rhizo.export(temp_db_path, "items", out_path)

        assert result.row_count == 3
        exported = pq.read_table(out_path)
        assert sorted(exported.column("x").to_pylist()) == [10, 20, 30]

    def test_export_after_close_raises(self, db_with_data, export_dir):
        """Exporting after close raises RuntimeError."""
        db, _ = db_with_data
        db.close()

        with pytest.raises(RuntimeError, match="closed"):
            db.export("users", os.path.join(export_dir, "fail.parquet"))


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Tests for error handling and edge cases."""

    def test_nonexistent_table_raises(self, db_with_data, export_dir):
        """Exporting a non-existent table raises IOError."""
        db, _ = db_with_data
        with pytest.raises((IOError, OSError)):
            db.export("nonexistent", os.path.join(export_dir, "fail.parquet"))

    def test_nonexistent_version_raises(self, db_with_data, export_dir):
        """Exporting a non-existent version raises IOError."""
        db, _ = db_with_data
        with pytest.raises((IOError, OSError)):
            db.export("users", os.path.join(export_dir, "fail.parquet"), version=999)

    def test_invalid_format_raises(self, db_with_data, export_dir):
        """Invalid format raises ValueError."""
        db, _ = db_with_data
        with pytest.raises(ValueError):
            db.export("users", os.path.join(export_dir, "out.parquet"), format="avro")

    def test_unknown_extension_raises(self, db_with_data, export_dir):
        """Unknown extension without explicit format raises ValueError."""
        db, _ = db_with_data
        with pytest.raises(ValueError):
            db.export("users", os.path.join(export_dir, "out.xlsx"))

    def test_creates_parent_directory(self, db_with_data, export_dir):
        """Export creates parent directories if needed."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "sub", "dir", "users.parquet")

        result = db.export("users", out_path)
        assert os.path.isfile(result.path)
        assert result.row_count == 5


# ---------------------------------------------------------------------------
# TestLargeExport
# ---------------------------------------------------------------------------

class TestLargeExport:
    """Tests for exporting larger datasets."""

    @pytest.mark.slow
    def test_1m_rows_parquet(self, temp_db_path, export_dir):
        """Export 1M rows to Parquet and verify integrity."""
        with rhizo.open(temp_db_path) as db:
            rng = np.random.default_rng(42)
            df = pd.DataFrame({
                "id": np.arange(1_000_000, dtype=np.int64),
                "value": rng.standard_normal(1_000_000),
                "category": rng.choice(["A", "B", "C", "D"], 1_000_000),
            })
            db.write("big", df)

            out_path = os.path.join(export_dir, "big.parquet")
            result = db.export("big", out_path)

        assert result.row_count == 1_000_000
        exported = pq.read_table(out_path)
        assert exported.num_rows == 1_000_000
        assert exported.column("id").to_pylist()[0] == 0
        assert exported.column("id").to_pylist()[-1] == 999_999

    @pytest.mark.slow
    def test_1m_rows_csv(self, temp_db_path, export_dir):
        """Export 1M rows to CSV and verify row count."""
        with rhizo.open(temp_db_path) as db:
            df = pd.DataFrame({
                "id": np.arange(1_000_000, dtype=np.int64),
                "value": np.arange(1_000_000, dtype=np.float64),
            })
            db.write("big", df)

            out_path = os.path.join(export_dir, "big.csv")
            result = db.export("big", out_path)

        assert result.row_count == 1_000_000
        # Verify by counting lines (header + 1M rows)
        with open(out_path, "r") as f:
            line_count = sum(1 for _ in f)
        assert line_count == 1_000_001  # header + data


# ---------------------------------------------------------------------------
# TestAtomicWrite
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    """Tests for atomic write behavior."""

    def test_no_partial_file_on_success(self, db_with_data, export_dir):
        """After successful export, no temp files remain."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "clean.parquet")

        db.export("users", out_path)

        # No .tmp files should remain
        for f in os.listdir(export_dir):
            assert not f.endswith(".rhizo_export_tmp"), f"Temp file not cleaned up: {f}"

    def test_file_is_complete(self, db_with_data, export_dir):
        """Exported file is a complete, valid Parquet file."""
        db, _ = db_with_data
        out_path = os.path.join(export_dir, "complete.parquet")

        db.export("users", out_path)

        # Should be readable without errors
        pf = pq.ParquetFile(out_path)
        table = pf.read()
        assert table.num_rows == 5
