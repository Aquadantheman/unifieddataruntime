"""
QueryEngine - SQL queries over versioned data with time travel and branching.

The engine provides:
1. SQL interface via DuckDB
2. Time travel - query any historical version
3. Git-like branching - isolated workspaces for data development
4. Cross-table ACID transactions with snapshot isolation
5. Table registration caching for performance
6. Multiple result formats (Arrow, pandas, dict)
"""

from __future__ import annotations

import os
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Dict, List, Any, Union, Generator

import duckdb
import pyarrow as pa

from .reader import TableReader
from .writer import TableWriter, WriteResult
from .olap_engine import OLAPEngine, is_datafusion_available
from .logging import get_logger
from .exceptions import TableNotFoundError, validate_table_name

_logger = get_logger(__name__)

# Default integrity verification: True for safety, override with RHIZO_VERIFY_INTEGRITY=false
_DEFAULT_VERIFY_INTEGRITY = os.environ.get("RHIZO_VERIFY_INTEGRITY", "true").lower() != "false"

if TYPE_CHECKING:
    import pandas as pd
    import _rhizo
    from .transaction import TransactionContext
    from .subscriber import Subscriber
    from .export import ExportResult


# Backwards-compatible alias for the shared validation function
_validate_table_name = validate_table_name


@dataclass
class RegisteredTable:
    """Tracks a table registered with DuckDB."""
    table_name: str
    version: int
    arrow_table: pa.Table


@dataclass
class QueryResult:
    """Result of a SQL query."""
    arrow_table: pa.Table
    row_count: int
    column_names: List[str]

    def to_pandas(self) -> "pd.DataFrame":
        """Convert result to pandas DataFrame."""
        return self.arrow_table.to_pandas()

    def to_dict(self) -> List[Dict[str, Any]]:
        """Convert result to list of dictionaries."""
        return self.arrow_table.to_pylist()

    def to_arrow(self) -> pa.Table:
        """Return as Arrow Table."""
        return self.arrow_table


class QueryEngine:
    """
    SQL query engine with time travel and branching support.

    Provides a DuckDB-based SQL interface over versioned Rhizo tables.
    Tables are loaded on-demand and cached for performance.

    Example:
        >>> from _rhizo import PyChunkStore, PyCatalog, PyBranchManager
        >>> from rhizo import QueryEngine
        >>>
        >>> store = PyChunkStore("./data/chunks")
        >>> catalog = PyCatalog("./data/catalog")
        >>> branches = PyBranchManager("./data/branches")
        >>> engine = QueryEngine(store, catalog, branch_manager=branches)
        >>>
        >>> # Query latest version on current branch (main by default)
        >>> result = engine.query("SELECT * FROM users WHERE age > 21")
        >>> df = result.to_pandas()
        >>>
        >>> # Switch to a feature branch
        >>> engine.checkout("feature/new-scoring")
        >>>
        >>> # Query on a specific branch without switching
        >>> result = engine.query(
        ...     "SELECT * FROM users",
        ...     branch="feature/experiment"
        ... )
        >>>
        >>> # Time travel - query version 5 of users table (overrides branch)
        >>> result = engine.query(
        ...     "SELECT * FROM users WHERE age > 21",
        ...     versions={"users": 5}
        ... )

    Note:
        Table names in queries are case-insensitive but stored lowercase.

    Version Resolution Order:
        1. Explicit version in `versions` dict (highest priority)
        2. Branch head pointer (from `branch` param or current_branch)
        3. Catalog latest (fallback for backward compatibility)
    """

    def __init__(
        self,
        store: "_rhizo.PyChunkStore",
        catalog: "_rhizo.PyCatalog",
        verify_integrity: bool = _DEFAULT_VERIFY_INTEGRITY,
        branch_manager: Optional["_rhizo.PyBranchManager"] = None,
        transaction_manager: Optional["_rhizo.PyTransactionManager"] = None,
        enable_olap: bool = True,
        olap_cache_size: int = 1_000_000_000,
        catalog_path: Optional[str] = None,
    ):
        """
        Initialize the QueryEngine.

        Args:
            store: PyChunkStore instance for content-addressable storage
            catalog: PyCatalog instance for version metadata
            verify_integrity: Verify chunk hashes on read (default: True for safety).
                             Set to False for faster reads in trusted environments.
            branch_manager: Optional PyBranchManager for branch-aware queries.
                           If None, operates in branchless mode (backward compatible).
            transaction_manager: Optional PyTransactionManager for ACID transactions.
                                If None, transaction() will raise RuntimeError.
            enable_olap: If True and DataFusion is available, use OLAPEngine for
                        faster analytical queries. Falls back to DuckDB if OLAP
                        fails or is unavailable. (default: True)
            olap_cache_size: Maximum cache size for OLAPEngine in bytes.
                            Only used if enable_olap=True. (default: 1GB)
        """
        self.store = store
        self.catalog = catalog
        self.reader = TableReader(store, catalog, verify_integrity)
        self.writer = TableWriter(store, catalog, catalog_path=catalog_path)
        self.branch_manager = branch_manager
        self.transaction_manager = transaction_manager

        # Current branch (only used if branch_manager is provided)
        self._current_branch: str = "main"

        # DuckDB connection (in-memory) + reentrant lock for thread safety.
        # RLock because _query_duckdb holds the lock while calling _ensure_registered.
        self._conn = duckdb.connect(":memory:")
        self._conn_lock = threading.RLock()

        # Cache of registered tables: (table_name, version) -> RegisteredTable
        self._registered: Dict[tuple, RegisteredTable] = {}

        # Active transaction tracking (for nested transaction prevention)
        self._active_tx: Optional[int] = None

        # OLAP Engine (DataFusion-based) for faster analytical queries
        self._olap: Optional[OLAPEngine] = None
        if enable_olap and is_datafusion_available():
            try:
                self._olap = OLAPEngine(
                    store=store,
                    catalog=catalog,
                    branch_manager=branch_manager,
                    max_cache_size_bytes=olap_cache_size,
                    verify_integrity=verify_integrity,
                )
            except Exception as e:
                # If OLAP initialization fails, continue without it (DuckDB fallback)
                _logger.warning("OLAP initialization failed: %s. Using DuckDB fallback.", e)
                self._olap = None

    def query(
        self,
        sql: str,
        versions: Optional[Dict[str, int]] = None,
        params: Optional[List[Any]] = None,
        branch: Optional[str] = None,
        use_olap: bool = True,
    ) -> QueryResult:
        """
        Execute a SQL query with optional time travel and branching.

        By default, uses the OLAPEngine (DataFusion) for faster analytical queries
        if available, with automatic fallback to DuckDB if OLAP fails or is unavailable.

        Args:
            sql: SQL query string
            versions: Dict mapping table names to specific versions.
                      If a table is not in this dict, branch head or latest is used.
            params: Optional query parameters for prepared statements.
                   Note: params are only supported with DuckDB (use_olap=False).
            branch: Branch to query from. If None, uses current_branch.
                   Only used if branch_manager is configured.
            use_olap: If True and OLAPEngine is available, use DataFusion for
                     faster execution. Falls back to DuckDB on failure.
                     Set to False to force DuckDB path. (default: True)

        Returns:
            QueryResult with Arrow table and metadata

        Raises:
            IOError: If a referenced table doesn't exist
            duckdb.Error: If SQL is invalid

        Example:
            >>> # Query latest on current branch (uses OLAP if available)
            >>> engine.query("SELECT * FROM users")
            >>>
            >>> # Force DuckDB path (e.g., for parameterized queries)
            >>> engine.query("SELECT * FROM users WHERE id = ?", params=[123], use_olap=False)
            >>>
            >>> # Query on specific branch
            >>> engine.query("SELECT * FROM users", branch="feature/test")
            >>>
            >>> # Time travel (overrides branch)
            >>> engine.query("SELECT * FROM users", versions={"users": 5})
        """
        versions = versions or {}
        effective_branch = branch or self._current_branch

        # Try OLAP path if enabled and available (params not supported in OLAP)
        if use_olap and self._olap is not None and params is None:
            try:
                arrow_table = self._olap.query(sql, versions=versions, branch=effective_branch)
                return QueryResult(
                    arrow_table=arrow_table,
                    row_count=arrow_table.num_rows,
                    column_names=arrow_table.column_names,
                )
            except Exception as e:
                # Fall back to DuckDB on any OLAP error
                _logger.debug("OLAP query failed: %s. Falling back to DuckDB.", e)

        # DuckDB path (fallback or explicit)
        return self._query_duckdb(sql, versions, params, effective_branch)

    def _query_duckdb(
        self,
        sql: str,
        versions: Dict[str, int],
        params: Optional[List[Any]],
        branch: str,
    ) -> QueryResult:
        """Execute query using DuckDB backend."""
        # Extract table names from query (pure regex, no conn access)
        table_names = self._extract_table_names(sql)

        with self._conn_lock:
            # Register each table with the appropriate version
            for table_name in table_names:
                version = versions.get(table_name)  # None means resolve via branch/latest
                self._ensure_registered(table_name, version, branch)

            # Execute with retry: if regex missed a table, DuckDB's error tells us
            # which table is missing so we can register it and retry once.
            try:
                result = self._conn.execute(sql, params or [])
            except Exception as e:
                missing = self._extract_missing_table(e)
                if missing:
                    self._ensure_registered(missing, versions.get(missing), branch)
                    result = self._conn.execute(sql, params or [])
                else:
                    raise

            arrow_table = result.fetch_arrow_table()

        return QueryResult(
            arrow_table=arrow_table,
            row_count=arrow_table.num_rows,
            column_names=arrow_table.column_names,
        )

    @staticmethod
    def _extract_missing_table(exc: Exception) -> Optional[str]:
        """Extract a missing table name from a DuckDB CatalogError."""
        msg = str(exc)
        # DuckDB format: 'Catalog Error: Table with name X does not exist!'
        m = re.search(r"Table with name (\w+) does not exist", msg, re.IGNORECASE)
        if m:
            return m.group(1).lower()
        return None

    def query_pandas(
        self,
        sql: str,
        versions: Optional[Dict[str, int]] = None,
        params: Optional[List[Any]] = None,
        branch: Optional[str] = None,
    ) -> "pd.DataFrame":
        """Execute query and return pandas DataFrame."""
        return self.query(sql, versions, params, branch).to_pandas()

    def query_arrow(
        self,
        sql: str,
        versions: Optional[Dict[str, int]] = None,
        params: Optional[List[Any]] = None,
        branch: Optional[str] = None,
    ) -> pa.Table:
        """Execute query and return Arrow Table."""
        return self.query(sql, versions, params, branch).to_arrow()

    # =========================================================================
    # OLAP Engine Operations
    # =========================================================================

    @property
    def olap_enabled(self) -> bool:
        """Check if OLAP engine is enabled and available."""
        return self._olap is not None

    def olap_query(
        self,
        sql: str,
        versions: Optional[Dict[str, int]] = None,
        branch: Optional[str] = None,
    ) -> pa.Table:
        """
        Execute query using OLAP engine (DataFusion) only.

        Unlike query(), this method does NOT fall back to DuckDB.
        Use this when you specifically need OLAP performance characteristics.

        Args:
            sql: SQL query string
            versions: Dict mapping table names to specific versions
            branch: Branch to query from (default: current branch)

        Returns:
            Arrow table with query results

        Raises:
            RuntimeError: If OLAP engine is not available
            Exception: If query execution fails

        Example:
            >>> # Force OLAP path for analytics
            >>> result = engine.olap_query('''
            ...     SELECT category, COUNT(*), AVG(amount)
            ...     FROM orders
            ...     GROUP BY category
            ... ''')
        """
        if self._olap is None:
            raise RuntimeError(
                "OLAP engine not available. "
                "Ensure DataFusion is installed and enable_olap=True."
            )

        effective_branch = branch or self._current_branch
        return self._olap.query(sql, versions=versions, branch=effective_branch)

    def query_time_travel(
        self,
        sql: str,
        branch: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute SQL with inline time travel and branch syntax.

        This is a powerful method that enables querying historical versions
        and comparing data across branches using extended SQL syntax.

        Supported Syntax:
        - `SELECT * FROM users VERSION 5` - Query version 5 of users
        - `SELECT * FROM users@feature` - Query users from 'feature' branch
        - `SELECT * FROM users@main VERSION 3` - Version 3 on main branch

        This enables powerful analytical queries like:

        ```sql
        -- Compare current vs historical data
        SELECT
            curr.id,
            curr.score AS current_score,
            old.score AS old_score,
            curr.score - old.score AS delta
        FROM users AS curr
        JOIN users VERSION 1 AS old ON curr.id = old.id
        WHERE curr.score != old.score
        ```

        ```sql
        -- Compare data across branches
        SELECT
            main.id,
            main.value AS main_value,
            feat.value AS feature_value
        FROM users@main AS main
        JOIN users@feature AS feat ON main.id = feat.id
        WHERE main.value != feat.value
        ```

        Args:
            sql: SQL query with VERSION and/or @branch syntax
            branch: Default branch for tables without @branch (default: current)

        Returns:
            QueryResult with the query results

        Raises:
            RuntimeError: If OLAP engine is not available
            IOError: If referenced tables/versions don't exist

        Example:
            >>> # Time travel to version 5
            >>> result = engine.query_time_travel(
            ...     "SELECT COUNT(*) FROM users VERSION 5"
            ... )
            >>>
            >>> # Cross-branch comparison
            >>> result = engine.query_time_travel('''
            ...     SELECT m.id, m.score AS main, f.score AS feature
            ...     FROM users@main m
            ...     JOIN users@experiment f ON m.id = f.id
            ... ''')
        """
        if self._olap is None:
            raise RuntimeError(
                "Time travel SQL syntax requires OLAP engine. "
                "Ensure DataFusion is installed and enable_olap=True."
            )

        effective_branch = branch or self._current_branch
        arrow_table = self._olap.query_time_travel(sql, branch=effective_branch)

        return QueryResult(
            arrow_table=arrow_table,
            row_count=arrow_table.num_rows,
            column_names=arrow_table.column_names,
        )

    def olap_stats(self) -> Dict[str, Any]:
        """
        Get OLAP engine cache statistics.

        Returns:
            Dict with cache metrics including:
            - enabled: Whether OLAP is enabled
            - hits: Number of cache hits
            - misses: Number of cache misses
            - hit_rate: Hit rate (0.0 to 1.0)
            - current_size_mb: Current cache size in MB
            - max_size_mb: Maximum cache size in MB
            - entry_count: Number of cached tables

        Example:
            >>> stats = engine.olap_stats()
            >>> print(f"Cache hit rate: {stats['hit_rate']:.1%}")
        """
        if self._olap is None:
            return {
                "enabled": False,
                "hits": 0,
                "misses": 0,
                "hit_rate": 0.0,
                "current_size_mb": 0.0,
                "max_size_mb": 0.0,
                "entry_count": 0,
            }

        stats = self._olap.cache_stats()
        stats["enabled"] = True
        return stats

    def olap_clear_cache(self, table_name: Optional[str] = None) -> None:
        """
        Clear the OLAP cache.

        Args:
            table_name: If specified, only clear this table's cache.
                       If None, clear entire cache.

        Raises:
            RuntimeError: If OLAP engine is not available
        """
        if self._olap is None:
            raise RuntimeError("OLAP engine not available")

        validated = validate_table_name(table_name) if table_name else None
        self._olap.clear_cache(validated)

    def olap_preload(
        self,
        table_name: str,
        version: Optional[int] = None,
        branch: Optional[str] = None,
    ) -> None:
        """
        Preload a table into the OLAP cache for faster queries.

        Args:
            table_name: Name of the table to preload
            version: Specific version (None for latest/branch head)
            branch: Branch to load from (default: current branch)

        Raises:
            RuntimeError: If OLAP engine is not available
        """
        if self._olap is None:
            raise RuntimeError("OLAP engine not available")

        validated_name = validate_table_name(table_name)
        effective_branch = branch or self._current_branch
        self._olap.preload(validated_name, version, effective_branch)

    def query_changelog(
        self,
        sql: str,
        since_tx_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
        tables: Optional[List[str]] = None,
        branch: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> QueryResult:
        """
        Execute SQL queries over the changelog (CDC/audit log).

        The changelog tracks all committed transactions. This method
        enables powerful analytics over change history:

        ```sql
        -- Get recent changes
        SELECT * FROM __changelog ORDER BY tx_id DESC LIMIT 10

        -- Find all changes to users table
        SELECT * FROM __changelog WHERE table_name = 'users'

        -- Count changes per table
        SELECT table_name, COUNT(*) as changes
        FROM __changelog
        GROUP BY table_name
        ORDER BY changes DESC

        -- Find new tables created
        SELECT DISTINCT table_name, committed_at
        FROM __changelog
        WHERE is_new_table = true
        ```

        Schema of __changelog:
        - tx_id: INT64 - Transaction ID
        - epoch_id: INT64 - Epoch ID
        - committed_at: INT64 - Unix timestamp of commit
        - branch: STRING - Branch name
        - table_name: STRING - Changed table
        - old_version: INT64 - Previous version (null if new)
        - new_version: INT64 - New version
        - is_new_table: BOOL - True if this created the table

        Args:
            sql: SQL query referencing __changelog table
            since_tx_id: Only include changes after this tx_id
            since_timestamp: Only include changes after this timestamp
            tables: Only include changes to these tables
            branch: Only include changes on this branch
            limit: Max changelog entries to load (for performance)

        Returns:
            QueryResult with changelog query results

        Raises:
            RuntimeError: If OLAP engine or transaction manager unavailable

        Example:
            >>> # Find tables that changed most
            >>> result = engine.query_changelog('''
            ...     SELECT table_name, COUNT(*) as changes
            ...     FROM __changelog
            ...     GROUP BY table_name
            ...     ORDER BY changes DESC
            ... ''')
            >>> print(result.to_pandas())
        """
        if self._olap is None:
            raise RuntimeError(
                "Changelog queries require OLAP engine. "
                "Ensure DataFusion is installed and enable_olap=True."
            )

        if self.transaction_manager is None:
            raise RuntimeError(
                "Changelog queries require transaction_manager. "
                "Initialize QueryEngine with transaction_manager parameter."
            )

        arrow_table = self._olap.query_changelog(
            sql,
            self.transaction_manager,
            since_tx_id=since_tx_id,
            since_timestamp=since_timestamp,
            tables=tables,
            branch=branch,
            limit=limit,
        )

        return QueryResult(
            arrow_table=arrow_table,
            row_count=arrow_table.num_rows,
            column_names=arrow_table.column_names,
        )

    def write_table(
        self,
        table_name: str,
        data: Union["pd.DataFrame", pa.Table],
        metadata: Optional[Dict[str, str]] = None,
        branch: Optional[str] = None,
        *,
        primary_key: Optional[List[str]] = None,
        schema_mode: Optional[str] = None,
    ) -> WriteResult:
        """
        Write data as a new version of a table.

        Convenience method that wraps TableWriter. If a branch_manager is
        configured, also updates the branch head pointer.

        Args:
            table_name: Name of the table (must be a valid SQL identifier)
            data: DataFrame or Arrow Table to write
            metadata: Optional metadata for this version
            branch: Branch to update. If None, uses current_branch.
                   Only used if branch_manager is configured.
            primary_key: Columns that form the primary key (set once, immutable).
            schema_mode: Schema evolution mode override ("additive" or "flexible").

        Returns:
            WriteResult with version info

        Raises:
            ValueError: If table_name is invalid
        """
        # Validate table name to prevent path traversal
        validated_name = validate_table_name(table_name)
        result = self.writer.write(
            validated_name, data, metadata,
            primary_key=primary_key, schema_mode=schema_mode,
        )

        # Update branch head if branch_manager is configured
        if self.branch_manager is not None:
            effective_branch = branch or self._current_branch
            self.branch_manager.update_head(effective_branch, validated_name, result.version)

        # Invalidate cache for this table (force reload on next query)
        self._invalidate_cache(validated_name)

        # Also invalidate OLAP cache
        if self._olap is not None:
            self._olap.clear_cache(validated_name)

        return result

    def list_tables(self) -> List[str]:
        """List all tables in the catalog."""
        return self.reader.list_tables()

    def list_versions(self, table_name: str) -> List[int]:
        """List all versions of a table."""
        validated_name = validate_table_name(table_name)
        return self.reader.list_versions(validated_name)

    # =========================================================================
    # Export Operations
    # =========================================================================

    def export_table(
        self,
        table_name: str,
        path: str,
        *,
        version: Optional[int] = None,
        columns: Optional[List[str]] = None,
        format: Optional[str] = None,
        compression: Optional[str] = None,
        branch: Optional[str] = None,
    ) -> "ExportResult":
        """
        Export a table version to Parquet, CSV, or JSON.

        Format is auto-detected from the file extension unless ``format``
        is specified explicitly.

        Args:
            table_name: Name of the table to export.
            path: Output file path (.parquet, .csv, .json, etc.).
            version: Table version to export (None for latest or branch head).
            columns: Columns to include (None for all).
            format: Explicit format override ('parquet', 'csv', 'json').
            compression: Parquet compression codec (default: 'zstd').
            branch: Branch to export from (None for current branch).

        Returns:
            ExportResult with path, row_count, file_size_bytes, etc.

        Raises:
            IOError: If table or version doesn't exist.
            ValueError: If format is unsupported or columns are invalid.

        Example:
            >>> result = engine.export_table("users", "users.parquet")
            >>> result = engine.export_table("users", "v3.csv", version=3)
        """
        from .export import ExportEngine

        # Resolve version from branch if needed
        if version is None and self.branch_manager is not None:
            effective_branch = branch or self._current_branch
            branch_version = self.branch_manager.get_table_version(
                effective_branch, table_name
            )
            if branch_version is not None:
                version = branch_version

        export_engine = ExportEngine(self.reader, self.store, self.catalog)
        return export_engine.export_table(
            table_name, path,
            version=version, columns=columns,
            format=format, compression=compression,
        )

    def export_query(
        self,
        sql: str,
        path: str,
        *,
        format: Optional[str] = None,
        compression: Optional[str] = None,
        versions: Optional[Dict[str, int]] = None,
        branch: Optional[str] = None,
    ) -> "ExportResult":
        """
        Execute a SQL query and export the results to a file.

        Args:
            sql: SQL query string.
            path: Output file path (.parquet, .csv, .json, etc.).
            format: Explicit format override ('parquet', 'csv', 'json').
            compression: Parquet compression codec (default: 'zstd').
            versions: Dict mapping table names to specific versions.
            branch: Branch to query from (None for current).

        Returns:
            ExportResult with path, row_count, file_size_bytes, etc.

        Example:
            >>> engine.export_query(
            ...     "SELECT * FROM users WHERE age > 21",
            ...     "adults.parquet"
            ... )
        """
        from .export import ExportEngine

        result = self.query(sql, versions=versions, branch=branch)
        export_engine = ExportEngine(self.reader, self.store, self.catalog)
        return export_engine.export_arrow(
            result.to_arrow(), path,
            format=format, compression=compression,
        )

    # =========================================================================
    # Transaction Operations
    # =========================================================================

    @contextmanager
    def transaction(
        self,
        branch: Optional[str] = None,
    ) -> Generator["TransactionContext", None, None]:
        """
        Create a transaction context for atomic multi-table operations.

        Provides ACID guarantees across multiple table writes:
        - Atomicity: All writes commit together or none do
        - Consistency: Conflict detection prevents inconsistent states
        - Isolation: Snapshot isolation - reads see consistent point-in-time
        - Durability: Committed changes are persisted

        Args:
            branch: Branch to operate on. If None, uses current_branch.

        Yields:
            TransactionContext for read/write operations

        Raises:
            RuntimeError: If transaction_manager is not configured
            RuntimeError: If a transaction is already active (no nesting)

        Example:
            >>> with engine.transaction() as tx:
            ...     # Read data (sees snapshot at transaction start)
            ...     users = tx.query("SELECT * FROM users")
            ...
            ...     # Buffer writes (not visible outside transaction)
            ...     tx.write_table("users", updated_df)
            ...     tx.write_table("audit_log", audit_df)
            ...
            ...     # Read-your-writes: query sees buffered data
            ...     result = tx.query("SELECT COUNT(*) FROM users")
            ...
            ...     # Auto-commits on successful exit
            ... # Auto-rollback if exception raised

        Note:
            Nested transactions are not supported. Attempting to start
            a transaction while one is active will raise RuntimeError.

        Future Extensions:
            - Savepoints for partial rollback
            - Distributed transactions
            - Custom isolation levels
        """
        # Import here to avoid circular import
        from .transaction import TransactionContext

        if self.transaction_manager is None:
            raise RuntimeError(
                "Cannot start transaction: transaction_manager not configured. "
                "Initialize QueryEngine with a PyTransactionManager."
            )

        if self._active_tx is not None:
            raise RuntimeError(
                "Nested transactions are not supported. "
                f"Transaction {self._active_tx} is already active."
            )

        effective_branch = branch or self._current_branch

        # Begin transaction through the Rust manager
        tx_id = self.transaction_manager.begin(effective_branch)
        self._active_tx = tx_id

        # Capture read snapshot (current versions of all known tables)
        snapshot = self._capture_snapshot(effective_branch)

        # Create the transaction context
        ctx = TransactionContext(self, tx_id, effective_branch, snapshot)

        try:
            yield ctx
            # Commit on successful exit (if not already committed/aborted)
            if ctx.is_active:
                ctx.commit()
        except Exception as e:
            # Rollback on exception (if not already committed/aborted)
            if ctx.is_active:
                ctx.abort(f"Exception: {e}")
            raise
        finally:
            self._active_tx = None
            # Invalidate cache - versions may have changed
            self._registered.clear()

    def _capture_snapshot(self, branch: str) -> Dict[str, int]:
        """
        Capture the current version snapshot for a transaction.

        Returns a mapping of table names to their current versions
        on the specified branch (or catalog latest if branchless).
        """
        snapshot: Dict[str, int] = {}

        # Get all tables from catalog
        tables = self.catalog.list_tables()

        for table_name in tables:
            version = None

            # Try branch head first
            if self.branch_manager is not None:
                version = self.branch_manager.get_table_version(branch, table_name)

            # Fall back to catalog latest
            if version is None:
                try:
                    metadata = self.reader.get_metadata(table_name)
                    version = metadata.version
                except TableNotFoundError:
                    # Explicit table not found - skip it
                    continue
                except OSError as e:
                    # Rust catalog raises OSError for "not found" - check message
                    if "not found" in str(e).lower():
                        continue
                    # Re-raise unexpected I/O errors (disk full, permissions, etc.)
                    raise

            snapshot[table_name.lower()] = version

        return snapshot

    # =========================================================================
    # Branch Operations
    # =========================================================================

    @property
    def current_branch(self) -> str:
        """Get the current branch name."""
        return self._current_branch

    def checkout(self, branch_name: str) -> None:
        """
        Switch to a different branch.

        Args:
            branch_name: Name of the branch to switch to

        Raises:
            IOError: If branch doesn't exist
            RuntimeError: If branch_manager is not configured
        """
        if self.branch_manager is None:
            raise RuntimeError("Cannot checkout: branch_manager not configured")

        # Verify branch exists (will raise if not found)
        self.branch_manager.get(branch_name)

        self._current_branch = branch_name

        # Invalidate all cached tables (versions may differ on new branch)
        self._registered.clear()

    def create_branch(
        self,
        name: str,
        from_branch: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Any:
        """
        Create a new branch.

        Args:
            name: Name of the new branch
            from_branch: Branch to create from (default: current branch)
            description: Optional description for the branch

        Returns:
            The created branch object

        Raises:
            RuntimeError: If branch_manager is not configured
            ValueError: If branch already exists
        """
        if self.branch_manager is None:
            raise RuntimeError("Cannot create branch: branch_manager not configured")

        source = from_branch or self._current_branch
        return self.branch_manager.create(name, from_branch=source, description=description)

    def list_branches(self) -> List[str]:
        """
        List all branch names.

        Returns:
            List of branch names

        Raises:
            RuntimeError: If branch_manager is not configured
        """
        if self.branch_manager is None:
            raise RuntimeError("Cannot list branches: branch_manager not configured")

        return self.branch_manager.list()

    def diff_branches(self, source: str, target: str) -> Dict[str, Any]:
        """
        Compare two branches.

        Args:
            source: Source branch name
            target: Target branch name

        Returns:
            Dict with comparison results including:
            - unchanged: Tables with same version on both branches
            - modified: Tables with different versions (table, source_ver, target_ver)
            - added_in_source: Tables only in source branch
            - added_in_target: Tables only in target branch
            - has_conflicts: True if branches have diverged

        Raises:
            RuntimeError: If branch_manager is not configured
        """
        if self.branch_manager is None:
            raise RuntimeError("Cannot diff branches: branch_manager not configured")

        diff = self.branch_manager.diff(source, target)

        return {
            "source_branch": diff.source_branch,
            "target_branch": diff.target_branch,
            "unchanged": diff.unchanged,
            "modified": diff.modified,
            "added_in_source": diff.added_in_source,
            "added_in_target": diff.added_in_target,
            "has_conflicts": diff.has_conflicts,
            "source_only_changes": diff.source_only_changes,
            "target_only_changes": diff.target_only_changes,
        }

    def merge_branch(self, source: str, into: Optional[str] = None) -> None:
        """
        Merge source branch into target branch (fast-forward only).

        Args:
            source: Branch to merge from
            into: Branch to merge into (default: current branch)

        Raises:
            RuntimeError: If branch_manager is not configured
            ValueError: If merge conflict (branches have diverged)
        """
        if self.branch_manager is None:
            raise RuntimeError("Cannot merge: branch_manager not configured")

        target = into or self._current_branch
        self.branch_manager.merge(source, into=target)

        # Invalidate cache if we merged into current branch
        if target == self._current_branch:
            self._registered.clear()

    def get_table_info(self, table_name: str, version: Optional[int] = None) -> Dict[str, Any]:
        """
        Get information about a table version.

        Args:
            table_name: Name of the table
            version: Specific version (None for latest)

        Returns:
            Dict with table metadata and schema info
        """
        validated_name = validate_table_name(table_name)
        metadata = self.reader.get_metadata(validated_name, version)

        # Load schema from first chunk
        arrow_table = self.reader.read_arrow(validated_name, version)

        return {
            "table_name": metadata.table_name,
            "version": metadata.version,
            "chunk_count": metadata.chunk_count,
            "row_count": arrow_table.num_rows,
            "created_at": metadata.created_at,
            "parent_version": metadata.parent_version,
            "schema": {
                field.name: str(field.type)
                for field in arrow_table.schema
            },
        }

    def diff_versions(
        self,
        table_name: str,
        version_a: int,
        version_b: int,
        key_columns: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Compare two versions of a table.

        Args:
            table_name: Name of the table
            version_a: First version to compare
            version_b: Second version to compare
            key_columns: Columns to use as primary key for row matching.
                        If None, compares aggregate statistics only.
                        Column names must be valid SQL identifiers (alphanumeric + underscore).

        Returns:
            Dict with comparison results

        Raises:
            ValueError: If key_columns contain invalid characters
        """
        validated_name = validate_table_name(table_name)
        table_a = self.reader.read_arrow(validated_name, version_a)
        table_b = self.reader.read_arrow(validated_name, version_b)

        result = {
            "table_name": validated_name,
            "version_a": version_a,
            "version_b": version_b,
            "rows_a": table_a.num_rows,
            "rows_b": table_b.num_rows,
            "row_diff": table_b.num_rows - table_a.num_rows,
            "schema_changed": table_a.schema != table_b.schema,
        }

        if key_columns:
            # Validate key_columns to prevent SQL injection
            valid_column_names = set(table_a.column_names) | set(table_b.column_names)
            for col in key_columns:
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                    raise ValueError(
                        f"Invalid column name '{col}': must be a valid SQL identifier "
                        "(start with letter or underscore, contain only alphanumerics and underscores)"
                    )
                if col not in valid_column_names:
                    raise ValueError(
                        f"Column '{col}' not found in table schema. "
                        f"Available columns: {sorted(valid_column_names)}"
                    )

            # Detailed row-level diff using DuckDB
            # Register both versions temporarily
            with self._conn_lock:
                self._conn.register("__diff_a", table_a)
                self._conn.register("__diff_b", table_b)

                try:
                    # Use quoted identifiers for safety
                    join_conditions = " AND ".join(f'a."{c}" = b."{c}"' for c in key_columns)

                    # Find added rows (in B but not in A)
                    added = self._conn.execute(f"""
                        SELECT * FROM __diff_b b
                        WHERE NOT EXISTS (
                            SELECT 1 FROM __diff_a a
                            WHERE {join_conditions}
                        )
                    """).fetch_arrow_table()

                    # Find removed rows (in A but not in B)
                    removed = self._conn.execute(f"""
                        SELECT * FROM __diff_a a
                        WHERE NOT EXISTS (
                            SELECT 1 FROM __diff_b b
                            WHERE {join_conditions}
                        )
                    """).fetch_arrow_table()

                    result["rows_added"] = added.num_rows
                    result["rows_removed"] = removed.num_rows
                finally:
                    # Always unregister temp tables
                    self._conn.unregister("__diff_a")
                    self._conn.unregister("__diff_b")

        return result

    def _ensure_registered(
        self,
        table_name: str,
        version: Optional[int] = None,
        branch: Optional[str] = None,
    ) -> None:
        """
        Ensure a table is registered with DuckDB at the specified version.

        Thread-safe: acquires _conn_lock (reentrant) for conn + cache access.

        Version resolution order:
        1. Explicit version parameter (highest priority)
        2. Branch head pointer (if branch_manager configured)
        3. Catalog latest (fallback)
        """
        # Resolve version (no conn access, safe outside lock)
        if version is None:
            # Try to resolve via branch head
            if self.branch_manager is not None and branch is not None:
                version = self.branch_manager.get_table_version(branch, table_name)

            # Fallback to catalog latest if branch doesn't have this table
            if version is None:
                metadata = self.reader.get_metadata(table_name)
                version = metadata.version

        cache_key = (table_name.lower(), version)

        with self._conn_lock:
            # Check if already registered at this version
            if cache_key in self._registered:
                return

            # Load the table (I/O, but must be under lock to avoid
            # races between check-and-register)
            arrow_table = self.reader.read_arrow(table_name, version)

            # Unregister any previous version of this table
            self._unregister_table(table_name)

            # Register with DuckDB
            self._conn.register(table_name.lower(), arrow_table)

            # Cache the registration
            self._registered[cache_key] = RegisteredTable(
                table_name=table_name.lower(),
                version=version,
                arrow_table=arrow_table,
            )

    def _unregister_table(self, table_name: str) -> None:
        """Unregister all versions of a table from DuckDB. Thread-safe."""
        table_lower = table_name.lower()

        with self._conn_lock:
            # Find and remove all cached versions
            to_remove = [k for k in self._registered if k[0] == table_lower]

            for key in to_remove:
                del self._registered[key]

            # Try to unregister from DuckDB (may not exist)
            try:
                self._conn.unregister(table_lower)
            except Exception as e:
                _logger.debug("Table %s not registered in DuckDB: %s", table_lower, e)

    def _invalidate_cache(self, table_name: str) -> None:
        """Invalidate cache for a table (called after writes)."""
        self._unregister_table(table_name)

    def _extract_table_names(self, sql: str) -> List[str]:
        """
        Extract table names from a SQL query.

        This is a regex-based approach that handles:
        - Quoted identifiers: "table_name" and `table_name`
        - Schema-qualified names: schema.table (extracts just table)
        - Comma-separated FROM lists: FROM a, b, c
        - Avoids extracting from string literals
        - Excludes SQL keywords, functions, and subquery aliases

        Over-extraction is safe (just registers tables that may not be used).
        Under-extraction is caught by _query_duckdb's retry mechanism.
        """
        # Step 1: Remove string literals to avoid extracting table names from them
        sql_no_strings = re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql)

        # Normalize whitespace
        sql_normalized = " ".join(sql_no_strings.split())

        # Identifier patterns
        identifier = r'(?:[a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)'
        table_ref = rf'(?:{identifier}\.)?({identifier})'

        # Keywords that introduce table references (single-table patterns)
        patterns = [
            rf'\bJOIN\s+{table_ref}',
            rf'\bINNER\s+JOIN\s+{table_ref}',
            rf'\bLEFT\s+(?:OUTER\s+)?JOIN\s+{table_ref}',
            rf'\bRIGHT\s+(?:OUTER\s+)?JOIN\s+{table_ref}',
            rf'\bFULL\s+(?:OUTER\s+)?JOIN\s+{table_ref}',
            rf'\bCROSS\s+JOIN\s+{table_ref}',
            rf'\bNATURAL\s+(?:LEFT\s+|RIGHT\s+|FULL\s+)?(?:OUTER\s+)?JOIN\s+{table_ref}',
            rf'\bUPDATE\s+{table_ref}',
            rf'\bINTO\s+{table_ref}',
        ]

        table_names = set()
        for pattern in patterns:
            matches = re.findall(pattern, sql_normalized, re.IGNORECASE)
            for match in matches:
                table_name = match.strip('"').strip('`')
                table_names.add(table_name.lower())

        # Handle comma-separated FROM lists: FROM a, b AS x, schema.c
        # Match everything after FROM until a keyword boundary
        from_clauses = re.finditer(
            r'\bFROM\s+((?:(?!\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bHAVING\b'
            r'|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|\bJOIN\b|\bINNER\b|\bLEFT\b'
            r'|\bRIGHT\b|\bFULL\b|\bCROSS\b|\bNATURAL\b|\bON\b|\bUSING\b'
            r'|\bINTO\b|\bSET\b|\bVALUES\b|\bRETURNING\b|\bFOR\b'
            r'|\bSELECT\b).)+)',
            sql_normalized,
            re.IGNORECASE,
        )
        for m in from_clauses:
            from_body = m.group(1)
            # Split on commas, then extract the table identifier from each item
            # Each item may be: table, schema.table, table AS alias, (subquery)
            for item in from_body.split(','):
                item = item.strip()
                if not item or item.startswith('('):
                    continue
                # Take the first token (possibly schema.table), ignore alias
                ref_match = re.match(
                    rf'(?:{identifier}\.)?({identifier})', item
                )
                if ref_match:
                    table_name = ref_match.group(1).strip('"').strip('`')
                    table_names.add(table_name.lower())

        # SQL keywords and built-in functions to exclude
        keywords = {
            'select', 'from', 'where', 'group', 'order', 'limit', 'offset',
            'union', 'except', 'intersect', 'all', 'distinct', 'as',
            'and', 'or', 'not', 'in', 'is', 'null', 'true', 'false',
            'case', 'when', 'then', 'else', 'end',
            'join', 'inner', 'outer', 'left', 'right', 'full', 'cross', 'natural',
            'on', 'using', 'having', 'by', 'asc', 'desc', 'nulls', 'first', 'last',
            'with', 'recursive', 'values', 'set', 'update', 'delete', 'insert', 'into',
            'create', 'drop', 'alter', 'table', 'view', 'index',
            'primary', 'key', 'foreign', 'references', 'constraint', 'unique',
            'default', 'check', 'between', 'like', 'ilike', 'similar', 'to',
            'exists', 'any', 'some', 'over', 'partition', 'window', 'rows', 'range',
            'unbounded', 'preceding', 'following', 'current', 'row',
            'lateral', 'unnest', 'ordinality',
            'count', 'sum', 'avg', 'min', 'max', 'first_value', 'last_value',
            'row_number', 'rank', 'dense_rank', 'ntile', 'lag', 'lead',
            'sample', 'tablesample', 'returning', 'conflict', 'nothing',
            'integer', 'bigint', 'smallint', 'real', 'double', 'precision',
            'varchar', 'char', 'text', 'boolean', 'date', 'time', 'timestamp',
            'interval', 'blob', 'json', 'array',
        }
        table_names -= keywords

        return list(table_names)

    # =========================================================================
    # Integrity & Recovery Operations
    # =========================================================================

    def verify_integrity(self) -> Dict[str, Any]:
        """
        Verify the integrity of the UDR storage system.

        Checks:
        - Transaction log consistency (if transaction_manager configured)
        - More checks can be added in the future (chunk integrity, catalog consistency)

        Returns:
            Dict with:
                - is_healthy: True if no issues found
                - issues: List of issue descriptions
                - transaction_issues: List of transaction-specific issues (if applicable)

        Example:
            >>> result = engine.verify_integrity()
            >>> if not result["is_healthy"]:
            ...     print(f"Issues found: {result['issues']}")
        """
        result: Dict[str, Any] = {
            "is_healthy": True,
            "issues": [],
            "transaction_issues": [],
        }

        # Check transaction system consistency
        if self.transaction_manager is not None:
            try:
                tx_issues = self.transaction_manager.verify_consistency()
                if tx_issues:
                    result["is_healthy"] = False
                    result["issues"].extend(tx_issues)
                    result["transaction_issues"] = tx_issues
            except Exception as e:
                result["is_healthy"] = False
                result["issues"].append(f"Transaction verification failed: {e}")

        return result

    def recover(self, apply: bool = True) -> Dict[str, Any]:
        """
        Run recovery to clean up incomplete transactions.

        This should be called on startup after a crash or unclean shutdown.
        Scans the transaction log and either marks pending transactions
        as aborted (if apply=True) or just reports what would be done.

        Args:
            apply: If True, mark pending transactions as aborted.
                   If False, just report what would be recovered (dry run).

        Returns:
            Dict with recovery results:
                - is_clean: True if no issues found
                - replayed: List of transaction IDs that were replayed
                - rolled_back: List of transaction IDs that were rolled back
                - warnings: List of warning messages
                - errors: List of error messages

        Raises:
            RuntimeError: If transaction_manager is not configured

        Example:
            >>> # On startup
            >>> report = engine.recover()
            >>> if not report["is_clean"]:
            ...     print(f"Recovered from crash: {report}")
        """
        if self.transaction_manager is None:
            raise RuntimeError("Cannot recover: transaction_manager not configured")

        if apply:
            report = self.transaction_manager.recover_and_apply()
        else:
            report = self.transaction_manager.recover()

        return {
            "is_clean": report.is_clean,
            "last_committed_epoch": report.last_committed_epoch,
            "replayed": list(report.replayed),
            "rolled_back": list(report.rolled_back),
            "already_aborted": list(report.already_aborted),
            "already_committed": list(report.already_committed),
            "warnings": list(report.warnings),
            "errors": list(report.errors),
        }

    # =========================================================================
    # Changelog & Subscription Operations
    # =========================================================================

    def get_changes(
        self,
        since_tx_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
        tables: Optional[List[str]] = None,
        branch: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get changelog entries since a specific point.

        This is the batch interface for querying what changed:
        - Batch state query: "What is the data?" -> engine.query()
        - Batch change query: "What changed?" -> engine.get_changes()
        - Stream changes: "Notify me of changes" -> engine.subscribe()

        Args:
            since_tx_id: Start from this transaction ID (exclusive)
            since_timestamp: Start from this Unix timestamp (inclusive)
            tables: Filter to specific tables (None = all tables)
            branch: Filter to specific branch (None = all branches)
            limit: Maximum entries to return (None = no limit)

        Returns:
            List of changelog entries as dicts, each containing:
            - tx_id: Transaction ID
            - epoch_id: Epoch the transaction was in
            - committed_at: Unix timestamp of commit
            - branch: Branch name
            - changes: List of table changes, each with:
              - table_name: Name of changed table
              - old_version: Previous version (None if new table)
              - new_version: New version after change

        Raises:
            RuntimeError: If transaction_manager is not configured

        Example:
            >>> # Get all changes since transaction 100
            >>> changes = engine.get_changes(since_tx_id=100)
            >>> for entry in changes:
            ...     for change in entry["changes"]:
            ...         print(f"{change['table_name']}: v{change['old_version']} -> v{change['new_version']}")

            >>> # Get changes to specific tables in last hour
            >>> import time
            >>> one_hour_ago = int(time.time()) - 3600
            >>> changes = engine.get_changes(
            ...     since_timestamp=one_hour_ago,
            ...     tables=["users", "orders"]
            ... )
        """
        if self.transaction_manager is None:
            raise RuntimeError(
                "Cannot get changes: transaction_manager not configured. "
                "Initialize QueryEngine with a PyTransactionManager."
            )

        entries = self.transaction_manager.get_changelog(
            since_tx_id=since_tx_id,
            since_timestamp=since_timestamp,
            tables=tables,
            branch=branch,
            limit=limit,
        )

        return [
            {
                "tx_id": e.tx_id,
                "epoch_id": e.epoch_id,
                "committed_at": e.committed_at,
                "branch": e.branch,
                "changes": [
                    {
                        "table_name": c.table_name,
                        "old_version": c.old_version,
                        "new_version": c.new_version,
                    }
                    for c in e.changes
                ],
            }
            for e in entries
        ]

    def subscribe(
        self,
        tables: Optional[List[str]] = None,
        since_tx_id: Optional[int] = None,
        branch: Optional[str] = None,
        poll_interval: float = 1.0,
    ) -> "Subscriber":
        """
        Create a subscriber for changelog events.

        This is the streaming interface for the unified batch/stream model.
        Returns a Subscriber that can be iterated or used with callbacks.

        Args:
            tables: Only receive events for these tables (None = all)
            since_tx_id: Start from this transaction (exclusive).
                        None = start from current latest (won't replay history)
            branch: Filter to specific branch (None = all branches)
            poll_interval: Seconds between polls when waiting (default: 1.0)

        Returns:
            Subscriber instance for processing change events

        Raises:
            RuntimeError: If transaction_manager is not configured

        Example (iterator):
            >>> for event in engine.subscribe(tables=["users"]):
            ...     print(f"{event.table_name}: v{event.old_version} -> v{event.new_version}")
            ...     if should_stop:
            ...         break

        Example (non-blocking poll):
            >>> subscriber = engine.subscribe()
            >>> events = subscriber.poll()  # Returns immediately
            >>> process_events(events)

        Example (background processing):
            >>> def on_change(event):
            ...     print(f"Changed: {event.table_name}")
            >>> subscriber = engine.subscribe()
            >>> subscriber.start_background(on_change)
            >>> # ... do other work ...
            >>> subscriber.stop()
        """
        # Import here to avoid circular import
        from .subscriber import Subscriber

        if self.transaction_manager is None:
            raise RuntimeError(
                "Cannot subscribe: transaction_manager not configured. "
                "Initialize QueryEngine with a PyTransactionManager."
            )

        return Subscriber(
            transaction_manager=self.transaction_manager,
            since_tx_id=since_tx_id,
            tables=tables,
            branch=branch,
            poll_interval=poll_interval,
        )

    def latest_tx_id(self) -> Optional[int]:
        """
        Get the latest committed transaction ID.

        Useful for establishing a checkpoint before processing,
        then using get_changes(since_tx_id=checkpoint) later.

        Returns:
            Latest transaction ID, or None if no transactions yet

        Raises:
            RuntimeError: If transaction_manager is not configured
        """
        if self.transaction_manager is None:
            raise RuntimeError(
                "Cannot get latest_tx_id: transaction_manager not configured"
            )

        return self.transaction_manager.latest_tx_id()

    def close(self) -> None:
        """Close the DuckDB connection. Thread-safe."""
        with self._conn_lock:
            self._conn.close()

    def __enter__(self) -> "QueryEngine":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
