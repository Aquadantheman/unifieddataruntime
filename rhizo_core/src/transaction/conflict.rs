//! Conflict detection for concurrent transactions.
//!
//! This module provides pluggable conflict detection strategies:
//! - `TableLevelConflictDetector` - Two transactions conflict if they write the same table
//! - `PartitionLevelConflictDetector` - Conflict on same partition
//! - `BloomFilterConflictDetector` - Row-level conflict detection via Bloom filter write-sets
//! - `AdaptiveConflictDetector` - Selects strategy based on write granularity
//!
//! # POAC Integration
//!
//! The `BloomFilterConflictDetector` implements Section 3 of the POAC paper:
//! - O(1) memory per transaction write-set (vs O(rows) for exact sets)
//! - Zero false negatives: conflicting transactions are always detected
//! - Bounded false positives: tunable via target FP rate parameter
//! - Uses BLAKE3 for hash functions (consistent with storage layer)
//!
//! The conflict detection strategy determines the concurrency/isolation trade-off.

use std::collections::HashSet;
use super::types::{TransactionRecord, WriteGranularity};

/// Represents a detected conflict between two transactions
#[derive(Debug, Clone)]
pub struct Conflict {
    /// Tables (or resources) that conflict
    pub tables: Vec<String>,

    /// First transaction ID
    pub tx1_id: u64,

    /// Second transaction ID
    pub tx2_id: u64,

    /// Description of the conflict
    pub description: String,

    /// Whether this conflict might be a false positive (Bloom filter detection)
    pub possibly_false_positive: bool,
}

impl Conflict {
    /// Create a new conflict (exact detection, no false positive possible)
    pub fn new(tables: Vec<String>, tx1_id: u64, tx2_id: u64) -> Self {
        let description = format!(
            "Write-write conflict on tables {:?} between tx {} and tx {}",
            tables, tx1_id, tx2_id
        );
        Self {
            tables,
            tx1_id,
            tx2_id,
            description,
            possibly_false_positive: false,
        }
    }

    /// Create a conflict that may be a Bloom filter false positive
    pub fn new_possibly_approximate(tables: Vec<String>, tx1_id: u64, tx2_id: u64) -> Self {
        let description = format!(
            "Possible write-write conflict on tables {:?} between tx {} and tx {} (Bloom filter)",
            tables, tx1_id, tx2_id
        );
        Self {
            tables,
            tx1_id,
            tx2_id,
            description,
            possibly_false_positive: true,
        }
    }

    /// Check if conflict involves a specific table
    pub fn involves_table(&self, table: &str) -> bool {
        self.tables.iter().any(|t| t == table)
    }
}

/// Trait for conflict detection strategies
pub trait ConflictDetector: Send + Sync {
    /// Detect conflicts between two transactions
    ///
    /// Returns `Some(Conflict)` if the transactions conflict, `None` otherwise.
    fn detect(&self, tx1: &TransactionRecord, tx2: &TransactionRecord) -> Option<Conflict>;

    /// Get the name of this detector for debugging
    fn name(&self) -> &'static str;
}

// =============================================================================
// Bloom Filter Write-Set (POAC Paper Section 3)
// =============================================================================

/// A Bloom filter for probabilistic set membership testing.
///
/// Implements the optimal parameters from the POAC paper:
///   m = -n * ln(p) / (ln 2)^2   (bits)
///   k = (m / n) * ln 2           (hash functions)
///
/// # Properties
/// - **Zero false negatives**: If an element was inserted, `contains` always returns true
/// - **Bounded false positives**: P(FP) = (1 - e^(-kn/m))^k <= target rate
/// - **Fixed memory**: O(1) regardless of element count (for given n, p)
///
/// # Hash Function Derivation
/// Uses BLAKE3 to derive k independent hash functions from a single hash.
/// BLAKE3 produces 256 bits; we partition into k slices, each giving a
/// bit index via modular reduction. For k <= 16 and m < 2^16, this provides
/// sufficient independence.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<u64>,
    /// Number of bits (m)
    num_bits: usize,
    /// Number of hash functions (k)
    num_hashes: u32,
    /// Number of elements inserted
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with optimal parameters.
    ///
    /// # Arguments
    /// * `expected_elements` - Expected number of elements (n)
    /// * `target_fp_rate` - Target false positive rate (p), e.g. 0.01 for 1%
    ///
    /// # Panics
    /// Panics if `expected_elements` is 0 or `target_fp_rate` is not in (0, 1).
    pub fn new(expected_elements: usize, target_fp_rate: f64) -> Self {
        assert!(expected_elements > 0, "expected_elements must be > 0");
        assert!(
            target_fp_rate > 0.0 && target_fp_rate < 1.0,
            "target_fp_rate must be in (0, 1)"
        );

        let n = expected_elements as f64;
        let p = target_fp_rate;

        // Optimal number of bits: m = -n * ln(p) / (ln 2)^2
        let ln2 = std::f64::consts::LN_2;
        let m = (-(n * p.ln()) / (ln2 * ln2)).ceil() as usize;
        let m = m.max(64); // Minimum 64 bits

        // Optimal number of hash functions: k = (m / n) * ln 2
        let k = ((m as f64 / n) * ln2).round() as u32;
        let k = k.clamp(1, 16); // Clamp to reasonable range

        // Allocate bit array (packed into u64s)
        let num_words = m.div_ceil(64);

        Self {
            bits: vec![0u64; num_words],
            num_bits: m,
            num_hashes: k,
            count: 0,
        }
    }

    /// Create a Bloom filter with explicit parameters (for testing/benchmarking).
    pub fn with_params(num_bits: usize, num_hashes: u32) -> Self {
        assert!(num_bits > 0, "num_bits must be > 0");
        assert!(num_hashes > 0, "num_hashes must be > 0");

        let num_words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes: num_hashes.min(16),
            count: 0,
        }
    }

    /// Insert an element into the filter.
    pub fn insert(&mut self, item: &[u8]) {
        let indices = self.hash_indices(item);
        for idx in indices {
            let word = idx / 64;
            let bit = idx % 64;
            self.bits[word] |= 1u64 << bit;
        }
        self.count += 1;
    }

    /// Insert a composite key (table_name + row_key).
    pub fn insert_key(&mut self, table: &str, key: &str) {
        // Combine table and key with a separator that won't appear in either
        let mut composite = Vec::with_capacity(table.len() + 1 + key.len());
        composite.extend_from_slice(table.as_bytes());
        composite.push(0xFF); // Separator byte
        composite.extend_from_slice(key.as_bytes());
        self.insert(&composite);
    }

    /// Check if an element might be in the filter.
    ///
    /// - Returns `true`: element is *possibly* present (may be false positive)
    /// - Returns `false`: element is *definitely not* present (zero false negatives)
    pub fn contains(&self, item: &[u8]) -> bool {
        let indices = self.hash_indices(item);
        indices.iter().all(|&idx| {
            let word = idx / 64;
            let bit = idx % 64;
            (self.bits[word] >> bit) & 1 == 1
        })
    }

    /// Check if a composite key might be in the filter.
    pub fn contains_key(&self, table: &str, key: &str) -> bool {
        let mut composite = Vec::with_capacity(table.len() + 1 + key.len());
        composite.extend_from_slice(table.as_bytes());
        composite.push(0xFF);
        composite.extend_from_slice(key.as_bytes());
        self.contains(&composite)
    }

    /// Check if any element in `other` might overlap with this filter.
    ///
    /// This performs a bitwise AND check: if any bit is set in both filters,
    /// there *might* be an overlapping element.
    ///
    /// - Returns `true`: filters *possibly* share elements
    /// - Returns `false`: filters *definitely* share no elements
    ///
    /// Note: This is a fast approximation. For exact overlap detection,
    /// iterate elements and check `contains()` individually.
    pub fn maybe_intersects(&self, other: &BloomFilter) -> bool {
        // Filters must have the same parameters for bitwise comparison
        if self.num_bits != other.num_bits {
            // Conservative: assume possible intersection
            return true;
        }
        self.bits
            .iter()
            .zip(other.bits.iter())
            .any(|(a, b)| a & b != 0)
    }

    /// Number of elements inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Number of bits in the filter (m).
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Number of hash functions (k).
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.bits.len() * 8
    }

    /// Theoretical false positive rate given current element count.
    ///
    /// P(FP) = (1 - e^(-kn/m))^k
    pub fn estimated_fp_rate(&self) -> f64 {
        let k = self.num_hashes as f64;
        let n = self.count as f64;
        let m = self.num_bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }

    /// Derive k bit indices from BLAKE3 hash of the item.
    ///
    /// Uses enhanced double hashing: index_i = (h1 + i*h2 + i*i) mod m
    /// where h1, h2 are derived from the BLAKE3 digest.
    /// This provides sufficient independence for practical Bloom filters [Kirsch & Mitzenmacher, 2006].
    fn hash_indices(&self, item: &[u8]) -> Vec<usize> {
        let hash = blake3::hash(item);
        let hash_bytes = hash.as_bytes(); // 32 bytes

        // Extract two 64-bit hashes from the BLAKE3 output
        let h1 = u64::from_le_bytes(hash_bytes[0..8].try_into().unwrap());
        let h2 = u64::from_le_bytes(hash_bytes[8..16].try_into().unwrap());

        let m = self.num_bits as u64;

        (0..self.num_hashes)
            .map(|i| {
                let i = i as u64;
                // Enhanced double hashing: h1 + i*h2 + i^2
                let combined = h1.wrapping_add(i.wrapping_mul(h2)).wrapping_add(i.wrapping_mul(i));
                (combined % m) as usize
            })
            .collect()
    }
}

// =============================================================================
// Write-Set: Wraps BloomFilter for Transaction Tracking
// =============================================================================

/// A transaction's write-set tracked via Bloom filter.
///
/// Encapsulates a Bloom filter configured for row-level conflict detection.
/// Each entry is a (table_name, row_key) pair.
#[derive(Debug, Clone)]
pub struct BloomWriteSet {
    filter: BloomFilter,
    /// Tables that have entries in this write-set (exact tracking for fast pre-check)
    tables: HashSet<String>,
}

impl BloomWriteSet {
    /// Create a new write-set with expected element count and target FP rate.
    pub fn new(expected_rows: usize, target_fp_rate: f64) -> Self {
        Self {
            filter: BloomFilter::new(expected_rows, target_fp_rate),
            tables: HashSet::new(),
        }
    }

    /// Record a row write.
    pub fn insert(&mut self, table: &str, row_key: &str) {
        self.tables.insert(table.to_string());
        self.filter.insert_key(table, row_key);
    }

    /// Check if a specific row might have been written.
    pub fn might_contain(&self, table: &str, row_key: &str) -> bool {
        // Fast path: if table not in write-set at all, definitely no conflict
        if !self.tables.contains(table) {
            return false;
        }
        self.filter.contains_key(table, row_key)
    }

    /// Check if two write-sets might overlap (fast bitwise check).
    pub fn maybe_conflicts_with(&self, other: &BloomWriteSet) -> bool {
        // Fast path: check table-level overlap first
        if self.tables.is_disjoint(&other.tables) {
            return false;
        }
        self.filter.maybe_intersects(&other.filter)
    }

    /// Tables in this write-set.
    pub fn tables(&self) -> &HashSet<String> {
        &self.tables
    }

    /// Number of rows recorded.
    pub fn count(&self) -> usize {
        self.filter.count()
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.filter.memory_bytes()
    }

    /// Current estimated false positive rate.
    pub fn estimated_fp_rate(&self) -> f64 {
        self.filter.estimated_fp_rate()
    }
}

// =============================================================================
// Conflict Detectors
// =============================================================================

/// Table-level conflict detection (Phase 5.0)
///
/// Two transactions conflict if they write to the same table.
/// This is the simplest and most conservative strategy.
#[derive(Debug, Default)]
pub struct TableLevelConflictDetector;

impl TableLevelConflictDetector {
    pub fn new() -> Self {
        Self
    }
}

impl ConflictDetector for TableLevelConflictDetector {
    fn detect(&self, tx1: &TransactionRecord, tx2: &TransactionRecord) -> Option<Conflict> {
        // Collect tables written by each transaction
        let tables1: HashSet<_> = tx1.writes.iter()
            .map(|w| w.table_name.as_str())
            .collect();
        let tables2: HashSet<_> = tx2.writes.iter()
            .map(|w| w.table_name.as_str())
            .collect();

        // Find intersection (tables written by both)
        let conflicts: Vec<String> = tables1
            .intersection(&tables2)
            .map(|s| s.to_string())
            .collect();

        if conflicts.is_empty() {
            None
        } else {
            Some(Conflict::new(conflicts, tx1.tx_id, tx2.tx_id))
        }
    }

    fn name(&self) -> &'static str {
        "TableLevelConflictDetector"
    }
}

/// Partition-level conflict detection (Phase 5.5)
///
/// Two transactions conflict only if they write to the same partition
/// of the same table.
#[derive(Debug, Default)]
pub struct PartitionLevelConflictDetector;

impl PartitionLevelConflictDetector {
    pub fn new() -> Self {
        Self
    }
}

impl ConflictDetector for PartitionLevelConflictDetector {
    fn detect(&self, tx1: &TransactionRecord, tx2: &TransactionRecord) -> Option<Conflict> {
        // For each table, check if writes overlap on partitions
        for write1 in &tx1.writes {
            for write2 in &tx2.writes {
                if write1.table_name != write2.table_name {
                    continue;
                }

                // Check partition overlap
                match (&write1.granularity, &write2.granularity) {
                    // Both whole-table writes -> conflict
                    (WriteGranularity::WholeTable, WriteGranularity::WholeTable) => {
                        return Some(Conflict::new(
                            vec![write1.table_name.clone()],
                            tx1.tx_id,
                            tx2.tx_id,
                        ));
                    }
                    // One is whole-table, other is partition -> conflict
                    (WriteGranularity::WholeTable, WriteGranularity::Partitions(_)) |
                    (WriteGranularity::Partitions(_), WriteGranularity::WholeTable) => {
                        return Some(Conflict::new(
                            vec![write1.table_name.clone()],
                            tx1.tx_id,
                            tx2.tx_id,
                        ));
                    }
                    // Both partition writes -> check overlap
                    (WriteGranularity::Partitions(p1), WriteGranularity::Partitions(p2)) => {
                        let parts1: HashSet<_> = p1.iter().collect();
                        let parts2: HashSet<_> = p2.iter().collect();
                        if parts1.intersection(&parts2).next().is_some() {
                            return Some(Conflict::new(
                                vec![write1.table_name.clone()],
                                tx1.tx_id,
                                tx2.tx_id,
                            ));
                        }
                    }
                    // Keys granularity - treat as whole table for now
                    _ => {
                        return Some(Conflict::new(
                            vec![write1.table_name.clone()],
                            tx1.tx_id,
                            tx2.tx_id,
                        ));
                    }
                }
            }
        }

        None
    }

    fn name(&self) -> &'static str {
        "PartitionLevelConflictDetector"
    }
}

/// Bloom filter row-level conflict detection (POAC Phase)
///
/// Uses Bloom filter write-sets for O(1) memory conflict detection with
/// zero false negatives and bounded false positives.
///
/// # Theory (POAC Paper Section 3)
///
/// Traditional row-level conflict detection requires O(rows) memory per
/// transaction. With Bloom filters:
/// - Memory: O(1) per transaction (fixed-size bit array)
/// - False negatives: 0 (safety preserved -- conflicts always detected)
/// - False positives: <= target rate (tunable, typically 1%)
///
/// # Usage
///
/// The detector operates on `BloomWriteSet` instances attached to transactions.
/// For transactions without Bloom write-sets, falls back to table-level detection.
///
/// ```ignore
/// let detector = BloomFilterConflictDetector::new(10_000, 0.01);
///
/// // Build write-sets during transaction execution
/// let mut ws1 = BloomWriteSet::new(10_000, 0.01);
/// ws1.insert("users", "user_123");
/// ws1.insert("users", "user_456");
///
/// let mut ws2 = BloomWriteSet::new(10_000, 0.01);
/// ws2.insert("users", "user_789");  // Different row -- no conflict
///
/// assert!(!ws1.maybe_conflicts_with(&ws2));
/// ```
#[derive(Debug)]
pub struct BloomFilterConflictDetector {
    /// Expected number of rows per transaction write-set
    expected_rows: usize,
    /// Target false positive rate
    target_fp_rate: f64,
}

impl BloomFilterConflictDetector {
    /// Create a new Bloom filter conflict detector.
    ///
    /// # Arguments
    /// * `expected_rows` - Expected rows per transaction (used to size filters)
    /// * `target_fp_rate` - Target false positive rate (e.g. 0.01 for 1%)
    pub fn new(expected_rows: usize, target_fp_rate: f64) -> Self {
        Self {
            expected_rows,
            target_fp_rate,
        }
    }

    /// Create a write-set for a new transaction.
    pub fn create_write_set(&self) -> BloomWriteSet {
        BloomWriteSet::new(self.expected_rows, self.target_fp_rate)
    }

    /// Detect conflicts between two Bloom filter write-sets.
    ///
    /// Returns `Some(Conflict)` if the write-sets might overlap.
    /// The conflict's `possibly_false_positive` flag indicates this was
    /// detected via Bloom filter and may be a false positive.
    pub fn detect_bloom(
        &self,
        tx1_id: u64,
        ws1: &BloomWriteSet,
        tx2_id: u64,
        ws2: &BloomWriteSet,
    ) -> Option<Conflict> {
        if ws1.maybe_conflicts_with(ws2) {
            // Determine which tables overlap
            let overlapping_tables: Vec<String> = ws1
                .tables()
                .intersection(ws2.tables())
                .cloned()
                .collect();

            if overlapping_tables.is_empty() {
                // Tables are disjoint -- no conflict possible
                None
            } else {
                Some(Conflict::new_possibly_approximate(
                    overlapping_tables,
                    tx1_id,
                    tx2_id,
                ))
            }
        } else {
            None
        }
    }
}

impl Default for BloomFilterConflictDetector {
    fn default() -> Self {
        Self::new(10_000, 0.01)
    }
}

impl ConflictDetector for BloomFilterConflictDetector {
    fn detect(&self, tx1: &TransactionRecord, tx2: &TransactionRecord) -> Option<Conflict> {
        // Without attached write-sets on the TransactionRecord, we fall back
        // to table-level detection. In practice, BloomWriteSets are maintained
        // alongside transactions and detect_bloom() is called directly.
        //
        // This trait impl exists for interface compatibility. The primary API
        // for Bloom filter detection is detect_bloom() with explicit write-sets.
        TableLevelConflictDetector.detect(tx1, tx2)
    }

    fn name(&self) -> &'static str {
        "BloomFilterConflictDetector"
    }
}

/// Row-level conflict detection via Bloom filters.
///
/// Alias for `BloomFilterConflictDetector` -- replaces the former stub.
pub type RowLevelConflictDetector = BloomFilterConflictDetector;

/// Multi-strategy conflict detector
///
/// Uses different detection strategies based on write granularity.
#[derive(Debug, Default)]
pub struct AdaptiveConflictDetector;

impl AdaptiveConflictDetector {
    pub fn new() -> Self {
        Self
    }
}

impl ConflictDetector for AdaptiveConflictDetector {
    fn detect(&self, tx1: &TransactionRecord, tx2: &TransactionRecord) -> Option<Conflict> {
        // Check if both transactions use partition granularity
        let has_partition_writes = tx1.writes.iter().chain(tx2.writes.iter())
            .any(|w| matches!(w.granularity, WriteGranularity::Partitions(_)));

        if has_partition_writes {
            PartitionLevelConflictDetector.detect(tx1, tx2)
        } else {
            TableLevelConflictDetector.detect(tx1, tx2)
        }
    }

    fn name(&self) -> &'static str {
        "AdaptiveConflictDetector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::types::TableWrite;

    // =========================================================================
    // Bloom Filter Unit Tests
    // =========================================================================

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.insert(b"hello");
        bf.insert(b"world");

        // Must find inserted elements (zero false negatives)
        assert!(bf.contains(b"hello"));
        assert!(bf.contains(b"world"));

        // Likely won't find non-inserted element (but could be false positive)
        // We just check count here
        assert_eq!(bf.count(), 2);
    }

    #[test]
    fn test_bloom_filter_zero_false_negatives() {
        // Insert 10,000 elements and verify ALL are found
        let n = 10_000;
        let mut bf = BloomFilter::new(n, 0.01);

        let elements: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("element_{}", i).into_bytes())
            .collect();

        for elem in &elements {
            bf.insert(elem);
        }

        // Every single inserted element MUST be found
        let mut false_negatives = 0;
        for elem in &elements {
            if !bf.contains(elem) {
                false_negatives += 1;
            }
        }

        assert_eq!(false_negatives, 0, "Bloom filter must have zero false negatives");
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        // Insert n elements, then test m non-elements
        let n = 10_000;
        let m = 100_000; // Test 100K non-elements for statistical significance
        let target_fp = 0.01;

        let mut bf = BloomFilter::new(n, target_fp);

        // Insert elements
        for i in 0..n {
            bf.insert(format!("inserted_{}", i).as_bytes());
        }

        // Test non-elements
        let mut false_positives = 0;
        for i in 0..m {
            if bf.contains(format!("not_inserted_{}", i).as_bytes()) {
                false_positives += 1;
            }
        }

        let actual_fp_rate = false_positives as f64 / m as f64;

        // Allow 2x target (statistical variance)
        assert!(
            actual_fp_rate < target_fp * 2.0,
            "FP rate {} exceeds 2x target {}",
            actual_fp_rate,
            target_fp
        );
    }

    #[test]
    fn test_bloom_filter_memory_savings() {
        // Compare Bloom filter memory vs exact HashSet
        let n = 1_000_000;
        let bf = BloomFilter::new(n, 0.01);

        let bloom_bytes = bf.memory_bytes();

        // Exact set: ~40 bytes per entry (table_id + row_id + HashSet overhead)
        let exact_bytes = n * 40;

        let savings = 1.0 - (bloom_bytes as f64 / exact_bytes as f64);

        assert!(
            savings > 0.95,
            "Expected >95% memory savings, got {:.1}%",
            savings * 100.0
        );
    }

    #[test]
    fn test_bloom_filter_optimal_params() {
        // Verify parameters match POAC paper formulas
        let n = 10_000;
        let p = 0.01;
        let bf = BloomFilter::new(n, p);

        let ln2 = std::f64::consts::LN_2;

        // Expected m = -n * ln(p) / (ln 2)^2
        let expected_m = (-(n as f64) * p.ln() / (ln2 * ln2)).ceil() as usize;

        // Expected k = (m/n) * ln 2
        let expected_k = ((expected_m as f64 / n as f64) * ln2).round() as u32;

        assert_eq!(bf.num_bits(), expected_m);
        assert_eq!(bf.num_hashes(), expected_k);
    }

    #[test]
    fn test_bloom_filter_composite_keys() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.insert_key("users", "user_123");
        bf.insert_key("orders", "order_456");

        assert!(bf.contains_key("users", "user_123"));
        assert!(bf.contains_key("orders", "order_456"));

        // Different table, same key suffix -- should not match
        // (probabilistically -- this tests the separator byte works)
        assert!(!bf.contains_key("users", "order_456"));
    }

    #[test]
    fn test_bloom_filter_estimated_fp_rate() {
        let mut bf = BloomFilter::new(1000, 0.01);

        // Empty filter should have 0 FP rate
        assert_eq!(bf.estimated_fp_rate(), 0.0);

        // Insert elements and check rate increases
        for i in 0..1000 {
            bf.insert(format!("elem_{}", i).as_bytes());
        }

        let rate = bf.estimated_fp_rate();
        assert!(rate > 0.0 && rate < 0.05, "FP rate {} outside expected range", rate);
    }

    // =========================================================================
    // BloomWriteSet Tests
    // =========================================================================

    #[test]
    fn test_write_set_disjoint_tables() {
        let mut ws1 = BloomWriteSet::new(1000, 0.01);
        ws1.insert("users", "u1");
        ws1.insert("users", "u2");

        let mut ws2 = BloomWriteSet::new(1000, 0.01);
        ws2.insert("orders", "o1");
        ws2.insert("orders", "o2");

        // Different tables -- fast path should catch this
        assert!(!ws1.maybe_conflicts_with(&ws2));
    }

    #[test]
    fn test_write_set_same_table_different_rows() {
        let mut ws1 = BloomWriteSet::new(1000, 0.01);
        ws1.insert("users", "user_1");

        let mut ws2 = BloomWriteSet::new(1000, 0.01);
        ws2.insert("users", "user_2");

        // Same table but different rows -- Bloom filter *might* say conflict
        // (false positive possible) but the table overlap is real
        // This tests that the system doesn't crash, not the specific result
        let _ = ws1.maybe_conflicts_with(&ws2);
    }

    #[test]
    fn test_write_set_overlapping_rows() {
        let mut ws1 = BloomWriteSet::new(1000, 0.01);
        ws1.insert("users", "user_123");

        let mut ws2 = BloomWriteSet::new(1000, 0.01);
        ws2.insert("users", "user_123"); // Same row!

        // Must detect as possible conflict (zero false negatives)
        assert!(ws1.maybe_conflicts_with(&ws2));
    }

    #[test]
    fn test_write_set_memory() {
        let ws = BloomWriteSet::new(1_000_000, 0.01);

        // 1M expected rows with 1% FP should use far less than 40MB
        assert!(
            ws.memory_bytes() < 2_000_000,
            "Write-set memory {} bytes too large for 1M expected rows",
            ws.memory_bytes()
        );
    }

    // =========================================================================
    // BloomFilterConflictDetector Tests
    // =========================================================================

    #[test]
    fn test_bloom_detector_no_conflict() {
        let detector = BloomFilterConflictDetector::new(1000, 0.01);

        let mut ws1 = detector.create_write_set();
        ws1.insert("users", "user_1");
        ws1.insert("users", "user_2");

        let mut ws2 = detector.create_write_set();
        ws2.insert("orders", "order_1");

        let conflict = detector.detect_bloom(1, &ws1, 2, &ws2);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_bloom_detector_definite_conflict() {
        let detector = BloomFilterConflictDetector::new(1000, 0.01);

        let mut ws1 = detector.create_write_set();
        ws1.insert("users", "user_123");

        let mut ws2 = detector.create_write_set();
        ws2.insert("users", "user_123"); // Same row

        let conflict = detector.detect_bloom(1, &ws1, 2, &ws2);
        assert!(conflict.is_some());

        let c = conflict.unwrap();
        assert!(c.possibly_false_positive); // Bloom detection flag
        assert!(c.involves_table("users"));
    }

    #[test]
    fn test_bloom_detector_false_positive_flag() {
        let detector = BloomFilterConflictDetector::new(1000, 0.01);

        let mut ws1 = detector.create_write_set();
        ws1.insert("users", "user_1");

        let mut ws2 = detector.create_write_set();
        ws2.insert("users", "user_2");

        // If detected as conflict, should be flagged as possibly false positive
        if let Some(conflict) = detector.detect_bloom(1, &ws1, 2, &ws2) {
            assert!(conflict.possibly_false_positive);
        }
    }

    // =========================================================================
    // Original Detector Tests (preserved)
    // =========================================================================

    fn create_tx_with_writes(tx_id: u64, tables: &[&str]) -> TransactionRecord {
        let mut tx = TransactionRecord::new(tx_id, 1, "main".to_string());
        for table in tables {
            tx.add_write(TableWrite::new(*table, 1, vec!["chunk".to_string()]));
        }
        tx
    }

    #[test]
    fn test_no_conflict_different_tables() {
        let detector = TableLevelConflictDetector::new();

        let tx1 = create_tx_with_writes(1, &["users"]);
        let tx2 = create_tx_with_writes(2, &["orders"]);

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_conflict_same_table() {
        let detector = TableLevelConflictDetector::new();

        let tx1 = create_tx_with_writes(1, &["users"]);
        let tx2 = create_tx_with_writes(2, &["users"]);

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_some());

        let conflict = conflict.unwrap();
        assert_eq!(conflict.tables, vec!["users"]);
        assert_eq!(conflict.tx1_id, 1);
        assert_eq!(conflict.tx2_id, 2);
    }

    #[test]
    fn test_conflict_multiple_tables() {
        let detector = TableLevelConflictDetector::new();

        let tx1 = create_tx_with_writes(1, &["users", "orders"]);
        let tx2 = create_tx_with_writes(2, &["orders", "products"]);

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_some());

        let conflict = conflict.unwrap();
        assert!(conflict.involves_table("orders"));
    }

    #[test]
    fn test_no_conflict_empty_writes() {
        let detector = TableLevelConflictDetector::new();

        let tx1 = TransactionRecord::new(1, 1, "main".to_string());
        let tx2 = TransactionRecord::new(2, 1, "main".to_string());

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_no_conflict_one_empty() {
        let detector = TableLevelConflictDetector::new();

        let tx1 = create_tx_with_writes(1, &["users"]);
        let tx2 = TransactionRecord::new(2, 1, "main".to_string());

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_partition_conflict_whole_table() {
        let detector = PartitionLevelConflictDetector::new();

        let tx1 = create_tx_with_writes(1, &["users"]);
        let tx2 = create_tx_with_writes(2, &["users"]);

        // Both WholeTable -> should conflict
        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_some());
    }

    #[test]
    fn test_partition_no_conflict_different_partitions() {
        let detector = PartitionLevelConflictDetector::new();

        let mut tx1 = TransactionRecord::new(1, 1, "main".to_string());
        tx1.add_write(TableWrite::new("users", 1, vec!["chunk".to_string()])
            .with_granularity(WriteGranularity::Partitions(vec!["p1".to_string()])));

        let mut tx2 = TransactionRecord::new(2, 1, "main".to_string());
        tx2.add_write(TableWrite::new("users", 1, vec!["chunk".to_string()])
            .with_granularity(WriteGranularity::Partitions(vec!["p2".to_string()])));

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_partition_conflict_same_partition() {
        let detector = PartitionLevelConflictDetector::new();

        let mut tx1 = TransactionRecord::new(1, 1, "main".to_string());
        tx1.add_write(TableWrite::new("users", 1, vec!["chunk".to_string()])
            .with_granularity(WriteGranularity::Partitions(vec!["p1".to_string()])));

        let mut tx2 = TransactionRecord::new(2, 1, "main".to_string());
        tx2.add_write(TableWrite::new("users", 1, vec!["chunk".to_string()])
            .with_granularity(WriteGranularity::Partitions(vec!["p1".to_string(), "p2".to_string()])));

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_some());
    }

    #[test]
    fn test_adaptive_detector() {
        let detector = AdaptiveConflictDetector::new();

        // Table-level writes
        let tx1 = create_tx_with_writes(1, &["users"]);
        let tx2 = create_tx_with_writes(2, &["users"]);

        let conflict = detector.detect(&tx1, &tx2);
        assert!(conflict.is_some());

        // Different tables
        let tx3 = create_tx_with_writes(3, &["orders"]);
        let conflict = detector.detect(&tx1, &tx3);
        assert!(conflict.is_none());
    }

    #[test]
    fn test_conflict_description() {
        let conflict = Conflict::new(
            vec!["users".to_string(), "orders".to_string()],
            1,
            2,
        );

        assert!(conflict.description.contains("users"));
        assert!(conflict.description.contains("orders"));
        assert!(conflict.description.contains("tx 1"));
        assert!(conflict.description.contains("tx 2"));
        assert!(!conflict.possibly_false_positive);
    }

    #[test]
    fn test_detector_names() {
        assert_eq!(TableLevelConflictDetector::new().name(), "TableLevelConflictDetector");
        assert_eq!(PartitionLevelConflictDetector::new().name(), "PartitionLevelConflictDetector");
        assert_eq!(BloomFilterConflictDetector::default().name(), "BloomFilterConflictDetector");
        assert_eq!(AdaptiveConflictDetector::new().name(), "AdaptiveConflictDetector");
    }

    // =========================================================================
    // POAC Paper Validation Tests (Section 3.4)
    // =========================================================================

    #[test]
    fn test_poac_table_1_1k_elements() {
        // Reproduce POAC paper Table 1: 1,000 elements, 1% target FP
        let n = 1_000;
        let target_fp = 0.01;
        let test_count = 100_000;

        let mut bf = BloomFilter::new(n, target_fp);
        for i in 0..n {
            bf.insert(format!("elem_{}", i).as_bytes());
        }

        // Verify zero false negatives
        for i in 0..n {
            assert!(bf.contains(format!("elem_{}", i).as_bytes()));
        }

        // Measure false positive rate
        let mut fps = 0;
        for i in 0..test_count {
            if bf.contains(format!("nonelem_{}", i).as_bytes()) {
                fps += 1;
            }
        }
        let fp_rate = fps as f64 / test_count as f64;

        // Paper claims <2% actual FP rate for 1% target
        assert!(fp_rate < 0.02, "FP rate {:.4} exceeds paper's 2% bound", fp_rate);

        // Paper claims >97% memory savings
        let exact_bytes = n * 40; // ~40 bytes per HashSet entry
        let bloom_bytes = bf.memory_bytes();
        let savings = 1.0 - (bloom_bytes as f64 / exact_bytes as f64);
        assert!(savings > 0.95, "Memory savings {:.1}% below paper's 97%", savings * 100.0);
    }

    #[test]
    fn test_poac_table_1_10k_elements() {
        // Reproduce POAC paper Table 1: 10,000 elements
        let n = 10_000;
        let target_fp = 0.01;
        let test_count = 100_000;

        let mut bf = BloomFilter::new(n, target_fp);
        for i in 0..n {
            bf.insert(format!("elem_{}", i).as_bytes());
        }

        // Zero false negatives
        let mut false_negatives = 0;
        for i in 0..n {
            if !bf.contains(format!("elem_{}", i).as_bytes()) {
                false_negatives += 1;
            }
        }
        assert_eq!(false_negatives, 0);

        // FP rate
        let mut fps = 0;
        for i in 0..test_count {
            if bf.contains(format!("nonelem_{}", i).as_bytes()) {
                fps += 1;
            }
        }
        let fp_rate = fps as f64 / test_count as f64;
        assert!(fp_rate < 0.02, "FP rate {:.4} exceeds 2% bound", fp_rate);
    }

    #[test]
    fn test_poac_table_1_100k_elements() {
        // Reproduce POAC paper Table 1: 100,000 elements
        let n = 100_000;
        let target_fp = 0.01;
        let test_count = 100_000;

        let mut bf = BloomFilter::new(n, target_fp);
        for i in 0..n {
            bf.insert(format!("elem_{}", i).as_bytes());
        }

        // Zero false negatives
        let mut false_negatives = 0;
        for i in 0..n {
            if !bf.contains(format!("elem_{}", i).as_bytes()) {
                false_negatives += 1;
            }
        }
        assert_eq!(false_negatives, 0);

        // FP rate
        let mut fps = 0;
        for i in 0..test_count {
            if bf.contains(format!("nonelem_{}", i).as_bytes()) {
                fps += 1;
            }
        }
        let fp_rate = fps as f64 / test_count as f64;
        assert!(fp_rate < 0.02, "FP rate {:.4} exceeds 2% bound", fp_rate);

        // Memory savings should exceed 90% (at 1e-7 FP rate, Bloom filter needs ~4MB for 1M elements)
        let exact_bytes = n * 40;
        let bloom_bytes = bf.memory_bytes();
        let savings = 1.0 - (bloom_bytes as f64 / exact_bytes as f64);
        assert!(savings > 0.97, "Memory savings {:.1}% below 97%", savings * 100.0);
    }

    #[test]
    fn test_poac_table_1_1m_elements() {
        // Reproduce POAC paper Table 1: 1,000,000 elements
        let n = 1_000_000;
        let target_fp = 0.01;
        let test_count = 100_000; // Fewer tests for speed

        let mut bf = BloomFilter::new(n, target_fp);
        for i in 0..n {
            bf.insert(format!("elem_{}", i).as_bytes());
        }

        // Spot-check false negatives (full check would be slow)
        for i in (0..n).step_by(100) {
            assert!(
                bf.contains(format!("elem_{}", i).as_bytes()),
                "False negative at element {}",
                i
            );
        }

        // FP rate
        let mut fps = 0;
        for i in 0..test_count {
            if bf.contains(format!("nonelem_{}", i).as_bytes()) {
                fps += 1;
            }
        }
        let fp_rate = fps as f64 / test_count as f64;
        assert!(fp_rate < 0.02, "FP rate {:.4} exceeds 2% bound", fp_rate);

        // Memory savings should exceed 90% (at 1e-7 FP rate, Bloom filter needs ~4MB for 1M elements)
        let exact_bytes = n * 40;
        let bloom_bytes = bf.memory_bytes();
        let savings = 1.0 - (bloom_bytes as f64 / exact_bytes as f64);
        assert!(savings > 0.90, "Memory savings {:.1}% below 90%", savings * 100.0);
    }
}
