//! Speculative execution for coordination-free transactions.
//!
//! This module implements the speculative execution protocol from the POAC paper,
//! with safety guarantees proven in `papers/speculative_safety_proof.md`.
//!
//! # Safety Guarantees
//!
//! The implementation maintains three invariants (from Speculative Safety Theorem):
//!
//! 1. **Detection Completeness**: Bloom filter conflict detection has zero false negatives
//! 2. **Visibility Invariant**: Only confirmed writes are visible; speculative writes are isolated
//! 3. **Commit Order**: Confirmed transactions have a total order via timestamps
//!
//! # Protocol
//!
//! 1. **Speculative Commit**: If conflict probability < threshold, commit to speculative buffer
//! 2. **Conflict Detection**: Background check against concurrent and newly-confirmed transactions
//! 3. **Resolution**: Confirm (promote to committed) or Rollback (discard from speculative buffer)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::algebraic::{AlgebraicValue, OpType};
use crate::distributed::VersionedUpdate;

/// Status of a speculative commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeculativeStatus {
    /// Awaiting confirmation (no conflict detected yet)
    Pending,
    /// Confirmed and promoted to committed state
    Confirmed,
    /// Aborted due to conflict detection
    Aborted,
}

/// A tentatively committed transaction awaiting confirmation.
#[derive(Debug, Clone)]
pub struct TentativeCommit {
    /// Unique identifier for this speculative commit
    id: u64,

    /// The versioned update containing operations
    update: VersionedUpdate,

    /// Estimated probability this will conflict [0.0, 1.0]
    conflict_probability: f64,

    /// Timestamp when speculative commit occurred (nanos since epoch)
    timestamp: u64,

    /// Current status
    status: SpeculativeStatus,

    /// Keys affected by this commit (for conflict checking)
    affected_keys: Vec<String>,
}

impl TentativeCommit {
    /// Create a new tentative commit.
    pub fn new(id: u64, update: VersionedUpdate, conflict_probability: f64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let affected_keys = update
            .operations()
            .iter()
            .map(|op| op.key().to_string())
            .collect();

        Self {
            id,
            update,
            conflict_probability,
            timestamp,
            status: SpeculativeStatus::Pending,
            affected_keys,
        }
    }

    /// Get the commit ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get the versioned update.
    pub fn update(&self) -> &VersionedUpdate {
        &self.update
    }

    /// Get the conflict probability.
    pub fn conflict_probability(&self) -> f64 {
        self.conflict_probability
    }

    /// Get the timestamp.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get the current status.
    pub fn status(&self) -> SpeculativeStatus {
        self.status
    }

    /// Get the affected keys.
    pub fn affected_keys(&self) -> &[String] {
        &self.affected_keys
    }

    /// Mark as confirmed.
    pub fn confirm(&mut self) {
        self.status = SpeculativeStatus::Confirmed;
    }

    /// Mark as aborted.
    pub fn abort(&mut self) {
        self.status = SpeculativeStatus::Aborted;
    }
}

/// Tracks conflict probability per key using exponential moving average.
///
/// This implements the adaptive threshold learning from POAC Section 4.3:
///
/// ```text
/// p̂_{t+1} = p̂_t + α × (observed_t − p̂_t)
/// ```
///
/// Where:
/// - `p̂_t` = estimated conflict probability at time t
/// - `α` = learning rate (controls adaptation speed)
/// - `observed_t` ∈ {0, 1} = whether a conflict was observed
#[derive(Debug, Clone)]
pub struct ConflictProbabilityTracker {
    /// Per-key conflict probability estimates
    key_probabilities: HashMap<String, f64>,

    /// Learning rate for EMA (α)
    learning_rate: f64,

    /// Default probability for unseen keys
    default_probability: f64,

    /// Total observations
    total_observations: u64,

    /// Total conflicts observed
    total_conflicts: u64,
}

impl ConflictProbabilityTracker {
    /// Create a new tracker with default parameters.
    pub fn new() -> Self {
        Self::with_params(0.1, 0.01)
    }

    /// Create a new tracker with custom parameters.
    ///
    /// # Arguments
    /// * `learning_rate` - EMA learning rate (α), typically 0.05-0.2
    /// * `default_probability` - Initial probability for unseen keys
    pub fn with_params(learning_rate: f64, default_probability: f64) -> Self {
        Self {
            key_probabilities: HashMap::new(),
            learning_rate,
            default_probability,
            total_observations: 0,
            total_conflicts: 0,
        }
    }

    /// Get the estimated conflict probability for a set of keys.
    ///
    /// Returns the maximum probability among all keys (conservative estimate).
    pub fn estimate_probability(&self, keys: &[String]) -> f64 {
        if keys.is_empty() {
            return self.default_probability;
        }

        keys.iter()
            .map(|k| self.key_probabilities.get(k).copied().unwrap_or(self.default_probability))
            .fold(0.0f64, f64::max)
    }

    /// Record an observation (conflict or no conflict) for keys.
    ///
    /// # Arguments
    /// * `keys` - Keys that were involved in the transaction
    /// * `conflict_occurred` - Whether a conflict was detected
    pub fn record_observation(&mut self, keys: &[String], conflict_occurred: bool) {
        let observed = if conflict_occurred { 1.0 } else { 0.0 };

        for key in keys {
            let current = self
                .key_probabilities
                .get(key)
                .copied()
                .unwrap_or(self.default_probability);

            // EMA update: p̂_{t+1} = p̂_t + α × (observed - p̂_t)
            let updated = current + self.learning_rate * (observed - current);
            self.key_probabilities.insert(key.clone(), updated);
        }

        self.total_observations += 1;
        if conflict_occurred {
            self.total_conflicts += 1;
        }
    }

    /// Get the global conflict rate.
    pub fn global_conflict_rate(&self) -> f64 {
        if self.total_observations == 0 {
            self.default_probability
        } else {
            self.total_conflicts as f64 / self.total_observations as f64
        }
    }

    /// Get the learning rate.
    pub fn learning_rate(&self) -> f64 {
        self.learning_rate
    }

    /// Set the learning rate.
    pub fn set_learning_rate(&mut self, rate: f64) {
        self.learning_rate = rate.clamp(0.01, 1.0);
    }

    /// Get number of keys being tracked.
    pub fn tracked_key_count(&self) -> usize {
        self.key_probabilities.len()
    }
}

impl Default for ConflictProbabilityTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for speculative execution.
#[derive(Debug, Clone)]
pub struct SpeculativeConfig {
    /// Threshold for speculation: speculate if P(conflict) < threshold
    pub speculation_threshold: f64,

    /// Learning rate for conflict probability EMA
    pub learning_rate: f64,

    /// Default conflict probability for new keys
    pub default_conflict_probability: f64,

    /// Maximum pending speculative commits before forcing eager commit
    pub max_pending_commits: usize,

    /// Whether speculative execution is enabled
    pub enabled: bool,
}

impl Default for SpeculativeConfig {
    fn default() -> Self {
        Self {
            // From POAC paper: speculate when P(conflict) < 83%
            // We use a more conservative default of 50%
            speculation_threshold: 0.5,
            learning_rate: 0.1,
            default_conflict_probability: 0.01,
            max_pending_commits: 100,
            enabled: true,
        }
    }
}

impl SpeculativeConfig {
    /// Create config optimized for low-conflict workloads.
    pub fn low_conflict() -> Self {
        Self {
            speculation_threshold: 0.8,
            learning_rate: 0.05,
            default_conflict_probability: 0.001,
            max_pending_commits: 200,
            enabled: true,
        }
    }

    /// Create config optimized for high-conflict workloads.
    pub fn high_conflict() -> Self {
        Self {
            speculation_threshold: 0.2,
            learning_rate: 0.2,
            default_conflict_probability: 0.1,
            max_pending_commits: 50,
            enabled: true,
        }
    }

    /// Disable speculative execution entirely.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

/// Speculative buffer holding tentative writes (isolated from confirmed state).
///
/// This implements the **Visibility Invariant** from the safety proof:
/// - Reads ONLY access confirmed state, never the speculative buffer
/// - This prevents dirty reads and cascading aborts
#[derive(Debug)]
pub struct SpeculativeBuffer {
    /// Tentative state: key -> (op_type, value, commit_id)
    state: HashMap<String, (OpType, AlgebraicValue, u64)>,

    /// Pending commits awaiting confirmation
    pending_commits: Vec<TentativeCommit>,

    /// Next commit ID
    next_id: AtomicU64,

    /// Conflict probability tracker
    conflict_tracker: ConflictProbabilityTracker,

    /// Configuration
    config: SpeculativeConfig,
}

impl SpeculativeBuffer {
    /// Create a new speculative buffer with default config.
    pub fn new() -> Self {
        Self::with_config(SpeculativeConfig::default())
    }

    /// Create a new speculative buffer with custom config.
    pub fn with_config(config: SpeculativeConfig) -> Self {
        Self {
            state: HashMap::new(),
            pending_commits: Vec::new(),
            next_id: AtomicU64::new(1),
            conflict_tracker: ConflictProbabilityTracker::with_params(
                config.learning_rate,
                config.default_conflict_probability,
            ),
            config,
        }
    }

    /// Generate next commit ID.
    fn next_commit_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Check if speculation is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Decide whether to speculate based on conflict probability.
    pub fn should_speculate(&self, keys: &[String]) -> bool {
        if !self.config.enabled {
            return false;
        }

        if self.pending_commits.len() >= self.config.max_pending_commits {
            return false;
        }

        let probability = self.conflict_tracker.estimate_probability(keys);
        probability < self.config.speculation_threshold
    }

    /// Add a speculative commit to the buffer.
    ///
    /// Returns the commit ID for later confirmation/rollback.
    pub fn add_speculative(&mut self, update: VersionedUpdate) -> u64 {
        let keys: Vec<String> = update
            .operations()
            .iter()
            .map(|op| op.key().to_string())
            .collect();

        let conflict_probability = self.conflict_tracker.estimate_probability(&keys);
        let commit_id = self.next_commit_id();

        // Add to pending commits
        let tentative = TentativeCommit::new(commit_id, update.clone(), conflict_probability);
        self.pending_commits.push(tentative);

        // Apply to speculative state
        for op in update.operations() {
            self.state.insert(
                op.key().to_string(),
                (op.op_type(), op.value().clone(), commit_id),
            );
        }

        commit_id
    }

    /// Check for conflicts between a pending commit and confirmed transactions.
    ///
    /// Returns true if conflict detected.
    pub fn check_conflict(&self, commit_id: u64, confirmed_keys: &[String]) -> bool {
        // Find the pending commit
        let commit = match self.pending_commits.iter().find(|c| c.id() == commit_id) {
            Some(c) => c,
            None => return false, // Already confirmed/aborted
        };

        // Check if any affected key overlaps with confirmed keys
        for key in commit.affected_keys() {
            if confirmed_keys.contains(key) {
                return true;
            }
        }

        false
    }

    /// Confirm a speculative commit (promote to confirmed state).
    ///
    /// Returns the versioned update to apply to confirmed state.
    pub fn confirm(&mut self, commit_id: u64) -> Option<VersionedUpdate> {
        // Find and update the commit
        let commit_idx = self
            .pending_commits
            .iter()
            .position(|c| c.id() == commit_id && c.status() == SpeculativeStatus::Pending)?;

        let commit = &mut self.pending_commits[commit_idx];
        commit.confirm();

        // Record successful observation
        self.conflict_tracker
            .record_observation(commit.affected_keys(), false);

        // Remove from speculative state (will be added to confirmed state by caller)
        for key in commit.affected_keys() {
            if let Some((_, _, cid)) = self.state.get(key) {
                if *cid == commit_id {
                    self.state.remove(key);
                }
            }
        }

        Some(commit.update().clone())
    }

    /// Rollback a speculative commit (discard from buffer).
    pub fn rollback(&mut self, commit_id: u64) -> bool {
        // Find and update the commit
        let commit_idx = self
            .pending_commits
            .iter()
            .position(|c| c.id() == commit_id && c.status() == SpeculativeStatus::Pending);

        let commit_idx = match commit_idx {
            Some(idx) => idx,
            None => return false,
        };

        let commit = &mut self.pending_commits[commit_idx];
        commit.abort();

        // Record conflict observation
        self.conflict_tracker
            .record_observation(commit.affected_keys(), true);

        // Remove from speculative state
        for key in commit.affected_keys() {
            if let Some((_, _, cid)) = self.state.get(key) {
                if *cid == commit_id {
                    self.state.remove(key);
                }
            }
        }

        true
    }

    /// Get all pending commit IDs.
    pub fn pending_commit_ids(&self) -> Vec<u64> {
        self.pending_commits
            .iter()
            .filter(|c| c.status() == SpeculativeStatus::Pending)
            .map(|c| c.id())
            .collect()
    }

    /// Get count of pending commits.
    pub fn pending_count(&self) -> usize {
        self.pending_commits
            .iter()
            .filter(|c| c.status() == SpeculativeStatus::Pending)
            .count()
    }

    /// Get the conflict tracker for statistics.
    pub fn conflict_tracker(&self) -> &ConflictProbabilityTracker {
        &self.conflict_tracker
    }

    /// Clean up old confirmed/aborted commits (garbage collection).
    pub fn cleanup(&mut self, max_age_nanos: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.pending_commits.retain(|c| {
            c.status() == SpeculativeStatus::Pending || (now - c.timestamp()) < max_age_nanos
        });
    }
}

impl Default for SpeculativeBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of a speculative commit operation.
#[derive(Debug, Clone)]
pub struct SpeculativeCommitResult {
    /// Commit ID for tracking
    pub commit_id: u64,

    /// Whether this was actually speculative (vs eager due to high conflict probability)
    pub is_speculative: bool,

    /// Estimated conflict probability at time of commit
    pub conflict_probability: f64,

    /// The versioned update (for propagation if eager)
    pub update: VersionedUpdate,
}

/// Metrics for speculative execution.
#[derive(Debug, Clone, Default)]
pub struct SpeculativeMetrics {
    /// Total speculative commits attempted
    pub total_speculated: u64,

    /// Total eager commits (skipped speculation)
    pub total_eager: u64,

    /// Successfully confirmed
    pub confirmed: u64,

    /// Rolled back due to conflict
    pub rolled_back: u64,

    /// Current pending count
    pub pending: u64,

    /// Global conflict rate
    pub conflict_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_tracker_ema() {
        let mut tracker = ConflictProbabilityTracker::with_params(0.5, 0.0);

        // Initially zero
        assert_eq!(tracker.estimate_probability(&["key1".to_string()]), 0.0);

        // Record a conflict
        tracker.record_observation(&["key1".to_string()], true);
        assert_eq!(tracker.estimate_probability(&["key1".to_string()]), 0.5);

        // Record no conflict
        tracker.record_observation(&["key1".to_string()], false);
        assert_eq!(tracker.estimate_probability(&["key1".to_string()]), 0.25);
    }

    #[test]
    fn test_speculative_buffer_add_and_confirm() {
        use crate::distributed::{AlgebraicOperation, AlgebraicTransaction, NodeId, VectorClock};

        let mut buffer = SpeculativeBuffer::new();

        // Create a mock update
        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(AlgebraicOperation::new(
            "counter",
            OpType::AbelianAdd,
            AlgebraicValue::integer(10),
        ));

        let mut clock = VectorClock::new();
        clock.tick(&NodeId::new("test"));
        let update = VersionedUpdate::new(tx.operations().to_vec(), clock, NodeId::new("test"));

        // Add speculative
        let commit_id = buffer.add_speculative(update);
        assert_eq!(buffer.pending_count(), 1);

        // Confirm
        let confirmed = buffer.confirm(commit_id);
        assert!(confirmed.is_some());
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn test_speculative_buffer_rollback() {
        use crate::distributed::{AlgebraicOperation, AlgebraicTransaction, NodeId, VectorClock};

        let mut buffer = SpeculativeBuffer::new();

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(AlgebraicOperation::new(
            "counter",
            OpType::AbelianAdd,
            AlgebraicValue::integer(10),
        ));

        let mut clock = VectorClock::new();
        clock.tick(&NodeId::new("test"));
        let update = VersionedUpdate::new(tx.operations().to_vec(), clock, NodeId::new("test"));

        let commit_id = buffer.add_speculative(update);
        assert!(buffer.rollback(commit_id));
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn test_should_speculate() {
        let mut buffer = SpeculativeBuffer::with_config(SpeculativeConfig {
            speculation_threshold: 0.5,
            ..Default::default()
        });

        // New key: should speculate (default probability is low)
        assert!(buffer.should_speculate(&["new_key".to_string()]));

        // Record many conflicts
        for _ in 0..10 {
            buffer
                .conflict_tracker
                .record_observation(&["hot_key".to_string()], true);
        }

        // Hot key: should NOT speculate (high conflict probability)
        assert!(!buffer.should_speculate(&["hot_key".to_string()]));
    }

    #[test]
    fn test_config_presets() {
        let low = SpeculativeConfig::low_conflict();
        assert!(low.speculation_threshold > 0.5);

        let high = SpeculativeConfig::high_conflict();
        assert!(high.speculation_threshold < 0.5);

        let disabled = SpeculativeConfig::disabled();
        assert!(!disabled.enabled);
    }
}
