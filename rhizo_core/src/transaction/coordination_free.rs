//! Coordination-free transaction support for algebraic operations.
//!
//! This module provides the bridge between the transaction system and the
//! distributed coordination-free protocol. It allows transactions containing
//! only algebraic operations to commit locally without coordination, producing
//! a `VersionedUpdate` that can be merged with updates from other nodes.
//!
//! # Theory
//!
//! For algebraic operations (commutative + associative), we can achieve
//! strong eventual consistency without coordination:
//!
//! - **Semilattice operations** (MAX, MIN, UNION): Idempotent merge
//! - **Abelian operations** (ADD, MULTIPLY): Combine deltas
//!
//! Vector clocks track causality to determine when merge is needed.
//!
//! # Usage
//!
//! ```ignore
//! use rhizo_core::transaction::CoordinationFreeManager;
//!
//! // Create a coordination-free manager
//! let cf_manager = CoordinationFreeManager::new(NodeId::new("node-1"));
//!
//! // Create an algebraic transaction
//! let mut tx = AlgebraicTransaction::new();
//! tx.add_operation(AlgebraicOperation::new(
//!     "page_views",
//!     OpType::AbelianAdd,
//!     AlgebraicValue::integer(100),
//! ));
//!
//! // Commit locally (no coordination!)
//! let update = cf_manager.commit_local(&tx)?;
//!
//! // Later, merge with updates from other nodes
//! let merged = cf_manager.merge_update(&update, &remote_update)?;
//! ```

use std::sync::RwLock;

use crate::algebraic::{AlgebraicSchemaRegistry, AlgebraicValue, OpType};
use crate::distributed::{
    AlgebraicTransaction, LocalCommitError, LocalCommitProtocol, NodeId, VectorClock,
    VersionedUpdate,
};
use super::speculative::{
    SpeculativeBuffer, SpeculativeConfig, SpeculativeCommitResult, SpeculativeMetrics,
};
use super::escrow::{EscrowConfig, EscrowManager, EscrowResult, EscrowAggregateStats};

/// Error type for coordination-free operations
#[derive(Debug, thiserror::Error)]
pub enum CoordinationFreeError {
    /// Transaction contains non-algebraic operations
    #[error("Transaction contains non-algebraic operations and cannot be committed coordination-free")]
    NotFullyAlgebraic,

    /// Local commit protocol error
    #[error("Local commit error: {0}")]
    LocalCommit(#[from] LocalCommitError),

    /// Lock acquisition failed
    #[error("Failed to acquire lock: {0}")]
    LockError(String),

    /// Schema validation error
    #[error("Schema validation error: {0}")]
    SchemaError(String),

    /// Merge error
    #[error("Merge error: {0}")]
    MergeError(String),
}

/// Configuration for coordination-free mode
#[derive(Debug, Clone)]
pub struct CoordinationFreeConfig {
    /// Require all operations to be algebraic (reject non-algebraic)
    pub require_fully_algebraic: bool,

    /// Optional schema registry for validation
    pub schema_registry: Option<AlgebraicSchemaRegistry>,
}

impl Default for CoordinationFreeConfig {
    fn default() -> Self {
        Self {
            require_fully_algebraic: true,
            schema_registry: None,
        }
    }
}

/// Manages coordination-free transactions for a single node.
///
/// This manager maintains:
/// - A vector clock for causality tracking
/// - The node's identity
/// - Local state for algebraic operations (confirmed)
/// - Speculative buffer for tentative commits (POAC speculative execution)
/// - Configuration for validation
///
/// # Speculative Execution
///
/// When enabled, the manager can speculatively commit transactions with low
/// conflict probability, confirming them asynchronously. This implements
/// POAC Paper Section 4 with safety guarantees from `speculative_safety_proof.md`.
pub struct CoordinationFreeManager {
    /// This node's identifier
    node_id: NodeId,

    /// Vector clock for causality tracking
    clock: RwLock<VectorClock>,

    /// Configuration
    config: CoordinationFreeConfig,

    /// Committed updates (for replay/recovery)
    committed_updates: RwLock<Vec<VersionedUpdate>>,

    /// Current local state (key -> (op_type, value)) - CONFIRMED state only
    /// This is the "confirmed store" from the visibility invariant
    local_state: RwLock<std::collections::HashMap<String, (OpType, AlgebraicValue)>>,

    /// Speculative buffer for tentative commits (POAC speculative execution)
    /// This is isolated from local_state to maintain the visibility invariant
    speculative_buffer: RwLock<SpeculativeBuffer>,

    /// Escrow manager for hot-spot resources (POAC escrow transactions)
    /// This enables linear horizontal scaling for quota-limited operations
    escrow: RwLock<Option<EscrowManager>>,
}

impl CoordinationFreeManager {
    /// Create a new coordination-free manager with default config
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, CoordinationFreeConfig::default())
    }

    /// Create a new coordination-free manager with custom config
    pub fn with_config(node_id: NodeId, config: CoordinationFreeConfig) -> Self {
        Self {
            node_id,
            clock: RwLock::new(VectorClock::new()),
            config,
            committed_updates: RwLock::new(Vec::new()),
            local_state: RwLock::new(std::collections::HashMap::new()),
            speculative_buffer: RwLock::new(SpeculativeBuffer::new()),
            escrow: RwLock::new(None),
        }
    }

    /// Create a new coordination-free manager with custom speculative config
    pub fn with_speculative_config(
        node_id: NodeId,
        config: CoordinationFreeConfig,
        spec_config: SpeculativeConfig,
    ) -> Self {
        Self {
            node_id,
            clock: RwLock::new(VectorClock::new()),
            config,
            committed_updates: RwLock::new(Vec::new()),
            local_state: RwLock::new(std::collections::HashMap::new()),
            speculative_buffer: RwLock::new(SpeculativeBuffer::with_config(spec_config)),
            escrow: RwLock::new(None),
        }
    }

    /// Create a new coordination-free manager with escrow support
    pub fn with_escrow(
        node_id: NodeId,
        config: CoordinationFreeConfig,
        escrow_config: EscrowConfig,
    ) -> Self {
        Self {
            node_id: node_id.clone(),
            clock: RwLock::new(VectorClock::new()),
            config,
            committed_updates: RwLock::new(Vec::new()),
            local_state: RwLock::new(std::collections::HashMap::new()),
            speculative_buffer: RwLock::new(SpeculativeBuffer::new()),
            escrow: RwLock::new(Some(EscrowManager::new(node_id, escrow_config))),
        }
    }

    /// Create a fully-configured coordination-free manager
    pub fn with_full_config(
        node_id: NodeId,
        config: CoordinationFreeConfig,
        spec_config: SpeculativeConfig,
        escrow_config: Option<EscrowConfig>,
    ) -> Self {
        let escrow = escrow_config.map(|c| EscrowManager::new(node_id.clone(), c));
        Self {
            node_id,
            clock: RwLock::new(VectorClock::new()),
            config,
            committed_updates: RwLock::new(Vec::new()),
            local_state: RwLock::new(std::collections::HashMap::new()),
            speculative_buffer: RwLock::new(SpeculativeBuffer::with_config(spec_config)),
            escrow: RwLock::new(escrow),
        }
    }

    /// Get this node's identifier
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get a copy of the current vector clock
    pub fn clock(&self) -> Result<VectorClock, CoordinationFreeError> {
        self.clock
            .read()
            .map(|c| c.clone())
            .map_err(|_| CoordinationFreeError::LockError("clock".to_string()))
    }

    /// Check if a transaction can be committed coordination-free
    pub fn can_commit_locally(&self, tx: &AlgebraicTransaction) -> bool {
        LocalCommitProtocol::can_commit_locally(tx)
    }

    /// Commit a transaction locally without coordination.
    ///
    /// This operation:
    /// 1. Validates that all operations are algebraic
    /// 2. Increments the local vector clock
    /// 3. Applies operations to local state
    /// 4. Returns a VersionedUpdate for propagation
    ///
    /// # Errors
    ///
    /// Returns error if transaction contains non-algebraic operations.
    pub fn commit_local(
        &self,
        tx: &AlgebraicTransaction,
    ) -> Result<VersionedUpdate, CoordinationFreeError> {
        // Validate transaction is fully algebraic
        if self.config.require_fully_algebraic && !tx.is_fully_algebraic() {
            return Err(CoordinationFreeError::NotFullyAlgebraic);
        }

        // Get write lock on clock
        let mut clock = self
            .clock
            .write()
            .map_err(|_| CoordinationFreeError::LockError("clock".to_string()))?;

        // Commit using LocalCommitProtocol
        let update = LocalCommitProtocol::commit_local(tx, &self.node_id, &mut clock)?;

        // Apply to local state
        self.apply_update_to_state(&update)?;

        // Store update for replay
        {
            let mut committed = self
                .committed_updates
                .write()
                .map_err(|_| CoordinationFreeError::LockError("committed_updates".to_string()))?;
            committed.push(update.clone());
        }

        Ok(update)
    }

    /// Receive and apply an update from another node.
    ///
    /// This merges the remote update with local state using algebraic rules.
    pub fn receive_update(
        &self,
        remote_update: &VersionedUpdate,
    ) -> Result<(), CoordinationFreeError> {
        // Merge clock
        {
            let mut clock = self
                .clock
                .write()
                .map_err(|_| CoordinationFreeError::LockError("clock".to_string()))?;
            clock.merge(remote_update.clock());
        }

        // Apply to local state
        self.apply_update_to_state(remote_update)?;

        Ok(())
    }

    /// Merge two updates using algebraic rules.
    ///
    /// This is a static operation that doesn't affect local state.
    pub fn merge_updates(
        &self,
        update1: &VersionedUpdate,
        update2: &VersionedUpdate,
    ) -> Result<VersionedUpdate, CoordinationFreeError> {
        LocalCommitProtocol::merge_updates(update1, update2)
            .map_err(CoordinationFreeError::LocalCommit)
    }

    /// Get the current value for a key in local state
    pub fn get_state(&self, key: &str) -> Result<Option<AlgebraicValue>, CoordinationFreeError> {
        let state = self
            .local_state
            .read()
            .map_err(|_| CoordinationFreeError::LockError("local_state".to_string()))?;
        Ok(state.get(key).map(|(_, v)| v.clone()))
    }

    /// Get all keys in local state
    pub fn keys(&self) -> Result<Vec<String>, CoordinationFreeError> {
        let state = self
            .local_state
            .read()
            .map_err(|_| CoordinationFreeError::LockError("local_state".to_string()))?;
        Ok(state.keys().cloned().collect())
    }

    /// Get number of committed updates
    pub fn update_count(&self) -> Result<usize, CoordinationFreeError> {
        let committed = self
            .committed_updates
            .read()
            .map_err(|_| CoordinationFreeError::LockError("committed_updates".to_string()))?;
        Ok(committed.len())
    }

    /// Apply an update to local state using algebraic merge rules
    fn apply_update_to_state(
        &self,
        update: &VersionedUpdate,
    ) -> Result<(), CoordinationFreeError> {
        use crate::algebraic::{AlgebraicMerger, MergeResult};

        let mut state = self
            .local_state
            .write()
            .map_err(|_| CoordinationFreeError::LockError("local_state".to_string()))?;

        for op in update.operations() {
            let key = op.key().to_string();

            if let Some((existing_op_type, existing_value)) = state.get(&key) {
                // Merge with existing value
                if *existing_op_type == op.op_type() {
                    let merge_result =
                        AlgebraicMerger::merge(op.op_type(), existing_value, op.value());
                    match merge_result {
                        MergeResult::Merged(merged_value) => {
                            state.insert(key, (op.op_type(), merged_value));
                        }
                        MergeResult::Conflict { reason, .. } => {
                            return Err(CoordinationFreeError::MergeError(format!(
                                "Conflict merging key '{}': {}",
                                op.key(),
                                reason
                            )));
                        }
                        MergeResult::TypeMismatch { type1, type2, .. } => {
                            return Err(CoordinationFreeError::MergeError(format!(
                                "Type mismatch merging key '{}': {:?} vs {:?}",
                                op.key(),
                                type1,
                                type2
                            )));
                        }
                    }
                } else {
                    // Different operation types on same key - this is a conflict
                    return Err(CoordinationFreeError::MergeError(format!(
                        "Operation type mismatch for key '{}': {:?} vs {:?}",
                        op.key(),
                        existing_op_type,
                        op.op_type()
                    )));
                }
            } else {
                // First value for this key
                state.insert(key, (op.op_type(), op.value().clone()));
            }
        }

        Ok(())
    }

    // =========================================================================
    // Speculative Execution (POAC Paper Section 4)
    // =========================================================================

    /// Commit a transaction, potentially speculatively.
    ///
    /// This method implements the POAC speculative execution protocol:
    /// 1. If conflict probability < threshold, commit speculatively
    /// 2. If conflict probability >= threshold, commit eagerly (immediately to confirmed)
    ///
    /// # Returns
    ///
    /// Returns a `SpeculativeCommitResult` indicating:
    /// - Whether the commit was speculative or eager
    /// - The commit ID (for tracking speculative commits)
    /// - The conflict probability estimate
    ///
    /// # Safety
    ///
    /// Speculative commits are isolated from reads (visibility invariant).
    /// Use `confirm_speculative()` or `rollback_speculative()` to resolve.
    pub fn commit_with_speculation(
        &self,
        tx: &AlgebraicTransaction,
    ) -> Result<SpeculativeCommitResult, CoordinationFreeError> {
        // Validate transaction is fully algebraic
        if self.config.require_fully_algebraic && !tx.is_fully_algebraic() {
            return Err(CoordinationFreeError::NotFullyAlgebraic);
        }

        // Get keys for probability estimation
        let keys: Vec<String> = tx.operations().iter().map(|op| op.key().to_string()).collect();

        // Check if we should speculate
        let should_speculate = {
            let buffer = self
                .speculative_buffer
                .read()
                .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;
            buffer.should_speculate(&keys)
        };

        // Get write lock on clock
        let mut clock = self
            .clock
            .write()
            .map_err(|_| CoordinationFreeError::LockError("clock".to_string()))?;

        // Create the versioned update
        let update = LocalCommitProtocol::commit_local(tx, &self.node_id, &mut clock)?;

        if should_speculate {
            // Speculative path: add to speculative buffer, don't apply to confirmed state
            let mut buffer = self
                .speculative_buffer
                .write()
                .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;

            let conflict_probability = buffer.conflict_tracker().estimate_probability(&keys);
            let commit_id = buffer.add_speculative(update.clone());

            Ok(SpeculativeCommitResult {
                commit_id,
                is_speculative: true,
                conflict_probability,
                update,
            })
        } else {
            // Eager path: commit directly to confirmed state
            self.apply_update_to_state(&update)?;

            // Store update for replay
            {
                let mut committed = self
                    .committed_updates
                    .write()
                    .map_err(|_| CoordinationFreeError::LockError("committed_updates".to_string()))?;
                committed.push(update.clone());
            }

            Ok(SpeculativeCommitResult {
                commit_id: 0, // Eager commits don't need tracking
                is_speculative: false,
                conflict_probability: 1.0, // High probability triggered eager
                update,
            })
        }
    }

    /// Confirm a speculative commit (promote to confirmed state).
    ///
    /// Call this when no conflict was detected for the speculative commit.
    ///
    /// # Returns
    ///
    /// Returns the versioned update on success, or None if commit_id not found.
    pub fn confirm_speculative(
        &self,
        commit_id: u64,
    ) -> Result<Option<VersionedUpdate>, CoordinationFreeError> {
        let update = {
            let mut buffer = self
                .speculative_buffer
                .write()
                .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;
            buffer.confirm(commit_id)
        };

        if let Some(ref update) = update {
            // Apply to confirmed state
            self.apply_update_to_state(update)?;

            // Store update for replay
            {
                let mut committed = self
                    .committed_updates
                    .write()
                    .map_err(|_| CoordinationFreeError::LockError("committed_updates".to_string()))?;
                committed.push(update.clone());
            }
        }

        Ok(update)
    }

    /// Rollback a speculative commit (discard from buffer).
    ///
    /// Call this when a conflict was detected for the speculative commit.
    ///
    /// # Returns
    ///
    /// Returns true if the commit was found and rolled back.
    pub fn rollback_speculative(&self, commit_id: u64) -> Result<bool, CoordinationFreeError> {
        let mut buffer = self
            .speculative_buffer
            .write()
            .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;
        Ok(buffer.rollback(commit_id))
    }

    /// Check pending speculative commits for conflicts against recent confirmed transactions.
    ///
    /// # Returns
    ///
    /// Returns a list of (commit_id, has_conflict) pairs.
    pub fn check_pending_conflicts(&self) -> Result<Vec<(u64, bool)>, CoordinationFreeError> {
        let buffer = self
            .speculative_buffer
            .read()
            .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;

        let state = self
            .local_state
            .read()
            .map_err(|_| CoordinationFreeError::LockError("local_state".to_string()))?;

        // Get confirmed keys
        let confirmed_keys: Vec<String> = state.keys().cloned().collect();

        // Check each pending commit
        let pending_ids = buffer.pending_commit_ids();
        let results: Vec<(u64, bool)> = pending_ids
            .iter()
            .map(|&id| {
                let has_conflict = buffer.check_conflict(id, &confirmed_keys);
                (id, has_conflict)
            })
            .collect();

        Ok(results)
    }

    /// Get metrics for speculative execution.
    pub fn speculative_metrics(&self) -> Result<SpeculativeMetrics, CoordinationFreeError> {
        let buffer = self
            .speculative_buffer
            .read()
            .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;

        let tracker = buffer.conflict_tracker();

        Ok(SpeculativeMetrics {
            total_speculated: 0, // TODO: Track this
            total_eager: 0,      // TODO: Track this
            confirmed: 0,        // TODO: Track this
            rolled_back: 0,      // TODO: Track this
            pending: buffer.pending_count() as u64,
            conflict_rate: tracker.global_conflict_rate(),
        })
    }

    /// Get the number of pending speculative commits.
    pub fn pending_speculative_count(&self) -> Result<usize, CoordinationFreeError> {
        let buffer = self
            .speculative_buffer
            .read()
            .map_err(|_| CoordinationFreeError::LockError("speculative_buffer".to_string()))?;
        Ok(buffer.pending_count())
    }

    // =========================================================================
    // Escrow Transactions (POAC Paper Section 5)
    // =========================================================================

    /// Check if escrow is enabled
    pub fn has_escrow(&self) -> bool {
        self.escrow
            .read()
            .map(|e| e.is_some())
            .unwrap_or(false)
    }

    /// Register a resource for escrow management.
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Unique identifier for the resource (e.g., "inventory:sku123")
    /// * `global_total` - Total amount available across all nodes
    /// * `initial_quota` - Initial quota for this node
    ///
    /// # Errors
    ///
    /// Returns error if escrow is not enabled or resource already exists.
    pub fn register_escrow_resource(
        &self,
        resource_id: impl Into<String>,
        global_total: i64,
        initial_quota: i64,
    ) -> Result<(), CoordinationFreeError> {
        let escrow = self
            .escrow
            .read()
            .map_err(|_| CoordinationFreeError::LockError("escrow".to_string()))?;

        match escrow.as_ref() {
            Some(mgr) => mgr
                .register_resource(resource_id, global_total, initial_quota)
                .map_err(|e| CoordinationFreeError::SchemaError(e.to_string())),
            None => Err(CoordinationFreeError::SchemaError(
                "Escrow not enabled".to_string(),
            )),
        }
    }

    /// Register a resource with automatically calculated optimal quota.
    ///
    /// Uses Poisson-based sizing: q* = F^-1_Poisson(1 - ε; λ_node)
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Unique identifier for the resource
    /// * `global_total` - Total amount available across all nodes
    /// * `expected_rate` - Expected request rate (requests per second)
    ///
    /// # Returns
    ///
    /// The calculated optimal quota.
    pub fn register_escrow_resource_auto(
        &self,
        resource_id: impl Into<String>,
        global_total: i64,
        expected_rate: f64,
    ) -> Result<i64, CoordinationFreeError> {
        let escrow = self
            .escrow
            .read()
            .map_err(|_| CoordinationFreeError::LockError("escrow".to_string()))?;

        match escrow.as_ref() {
            Some(mgr) => mgr
                .register_resource_auto_quota(resource_id, global_total, expected_rate)
                .map_err(|e| CoordinationFreeError::SchemaError(e.to_string())),
            None => Err(CoordinationFreeError::SchemaError(
                "Escrow not enabled".to_string(),
            )),
        }
    }

    /// Consume quota for a resource locally (no coordination).
    ///
    /// This implements POAC escrow transactions for hot-spot resources.
    /// Operations consume local quota without coordination. Only quota
    /// exhaustion triggers coordination.
    ///
    /// # Arguments
    ///
    /// * `resource_id` - The resource to consume from
    /// * `amount` - Amount to consume
    ///
    /// # Returns
    ///
    /// `EscrowResult::Success` if quota was available locally.
    /// `EscrowResult::QuotaExhausted` if coordination is needed.
    pub fn consume_escrow(&self, resource_id: &str, amount: i64) -> EscrowResult {
        let escrow = match self.escrow.read() {
            Ok(e) => e,
            Err(_) => return EscrowResult::ResourceNotFound,
        };

        match escrow.as_ref() {
            Some(mgr) => mgr.try_consume(resource_id, amount),
            None => EscrowResult::ResourceNotFound,
        }
    }

    /// Replenish escrow quota after coordination.
    ///
    /// Call this after coordinating with other nodes to obtain more quota.
    pub fn replenish_escrow(
        &self,
        resource_id: &str,
        amount: i64,
    ) -> Result<(), CoordinationFreeError> {
        let escrow = self
            .escrow
            .read()
            .map_err(|_| CoordinationFreeError::LockError("escrow".to_string()))?;

        match escrow.as_ref() {
            Some(mgr) => mgr
                .replenish(resource_id, amount)
                .map_err(|e| CoordinationFreeError::SchemaError(e.to_string())),
            None => Err(CoordinationFreeError::SchemaError(
                "Escrow not enabled".to_string(),
            )),
        }
    }

    /// Get escrow statistics for a resource.
    pub fn escrow_stats(
        &self,
        resource_id: &str,
    ) -> Result<Option<super::escrow::EscrowStats>, CoordinationFreeError> {
        let escrow = self
            .escrow
            .read()
            .map_err(|_| CoordinationFreeError::LockError("escrow".to_string()))?;

        match escrow.as_ref() {
            Some(mgr) => Ok(mgr.get_stats(resource_id)),
            None => Ok(None),
        }
    }

    /// Get aggregate escrow statistics.
    pub fn escrow_aggregate_stats(&self) -> Result<Option<EscrowAggregateStats>, CoordinationFreeError> {
        let escrow = self
            .escrow
            .read()
            .map_err(|_| CoordinationFreeError::LockError("escrow".to_string()))?;

        match escrow.as_ref() {
            Some(mgr) => mgr
                .aggregate_stats()
                .map(Some)
                .map_err(|e| CoordinationFreeError::SchemaError(e.to_string())),
            None => Ok(None),
        }
    }

    /// Get available escrow quota for a resource.
    pub fn available_escrow(&self, resource_id: &str) -> Option<i64> {
        let escrow = self.escrow.read().ok()?;
        escrow.as_ref()?.available_quota(resource_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::AlgebraicOperation;

    fn add_op(key: &str, value: i64) -> AlgebraicOperation {
        AlgebraicOperation::new(key, OpType::AbelianAdd, AlgebraicValue::integer(value))
    }

    fn max_op(key: &str, value: i64) -> AlgebraicOperation {
        AlgebraicOperation::new(key, OpType::SemilatticeMax, AlgebraicValue::integer(value))
    }

    #[test]
    fn test_create_manager() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));
        assert_eq!(manager.node_id().as_str(), "node-1");
    }

    #[test]
    fn test_commit_local() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 10));

        let update = manager.commit_local(&tx).unwrap();

        assert_eq!(update.origin_node().as_str(), "node-1");
        assert_eq!(update.operations().len(), 1);
    }

    #[test]
    fn test_local_state_updated() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 10));

        manager.commit_local(&tx).unwrap();

        let value = manager.get_state("counter").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(10));
    }

    #[test]
    fn test_multiple_commits_accumulate() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        // First commit: counter = 10
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 10));
        manager.commit_local(&tx1).unwrap();

        // Second commit: counter += 20
        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(add_op("counter", 20));
        manager.commit_local(&tx2).unwrap();

        // Should be 30
        let value = manager.get_state("counter").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(30));
    }

    #[test]
    fn test_receive_remote_update() {
        let manager1 = CoordinationFreeManager::new(NodeId::new("node-1"));
        let manager2 = CoordinationFreeManager::new(NodeId::new("node-2"));

        // Node 1 commits
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 10));
        let update1 = manager1.commit_local(&tx1).unwrap();

        // Node 2 commits
        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(add_op("counter", 20));
        manager2.commit_local(&tx2).unwrap();

        // Node 2 receives update from Node 1
        manager2.receive_update(&update1).unwrap();

        // Node 2 should have 30
        let value = manager2.get_state("counter").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(30));
    }

    #[test]
    fn test_merge_updates() {
        let manager1 = CoordinationFreeManager::new(NodeId::new("node-1"));
        let manager2 = CoordinationFreeManager::new(NodeId::new("node-2"));

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 10));
        let update1 = manager1.commit_local(&tx1).unwrap();

        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(add_op("counter", 20));
        let update2 = manager2.commit_local(&tx2).unwrap();

        let merged = manager1.merge_updates(&update1, &update2).unwrap();
        assert_eq!(merged.operations().len(), 1);

        // The merged value should be 30
        let merged_value = merged.operations()[0].value();
        assert_eq!(merged_value.as_integer(), Some(30));
    }

    #[test]
    fn test_max_operation() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(max_op("timestamp", 100));
        manager.commit_local(&tx1).unwrap();

        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(max_op("timestamp", 50)); // Less than 100
        manager.commit_local(&tx2).unwrap();

        // Should still be 100 (max)
        let value = manager.get_state("timestamp").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(100));

        let mut tx3 = AlgebraicTransaction::new();
        tx3.add_operation(max_op("timestamp", 200)); // Greater than 100
        manager.commit_local(&tx3).unwrap();

        // Now should be 200
        let value = manager.get_state("timestamp").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(200));
    }

    #[test]
    fn test_clock_advances() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let clock_before = manager.clock().unwrap();
        assert!(clock_before.is_empty());

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 10));
        manager.commit_local(&tx).unwrap();

        let clock_after = manager.clock().unwrap();
        assert!(!clock_after.is_empty());
        assert!(clock_after.get(&NodeId::new("node-1")) > 0);
    }

    #[test]
    fn test_non_algebraic_rejected() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(AlgebraicOperation::new(
            "data",
            OpType::GenericOverwrite, // Not algebraic
            AlgebraicValue::integer(42),
        ));

        let result = manager.commit_local(&tx);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoordinationFreeError::NotFullyAlgebraic
        ));
    }

    #[test]
    fn test_update_count() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        assert_eq!(manager.update_count().unwrap(), 0);

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 10));
        manager.commit_local(&tx).unwrap();

        assert_eq!(manager.update_count().unwrap(), 1);
    }

    // =========================================================================
    // Speculative Execution Tests
    // =========================================================================

    #[test]
    fn test_speculative_commit_low_conflict() {
        // With default config, new keys should speculate (low conflict probability)
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("new_key", 100));

        let result = manager.commit_with_speculation(&tx).unwrap();

        // Should be speculative (default probability is low)
        assert!(result.is_speculative);
        assert!(result.commit_id > 0);

        // Confirmed state should NOT have the value (visibility invariant)
        assert!(manager.get_state("new_key").unwrap().is_none());

        // Should have 1 pending commit
        assert_eq!(manager.pending_speculative_count().unwrap(), 1);
    }

    #[test]
    fn test_speculative_confirm() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 50));

        let result = manager.commit_with_speculation(&tx).unwrap();
        assert!(result.is_speculative);

        // Confirm the speculative commit
        let confirmed = manager.confirm_speculative(result.commit_id).unwrap();
        assert!(confirmed.is_some());

        // Now confirmed state should have the value
        let value = manager.get_state("counter").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(50));

        // No more pending commits
        assert_eq!(manager.pending_speculative_count().unwrap(), 0);
    }

    #[test]
    fn test_speculative_rollback() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 50));

        let result = manager.commit_with_speculation(&tx).unwrap();
        assert!(result.is_speculative);

        // Rollback the speculative commit
        let rolled_back = manager.rollback_speculative(result.commit_id).unwrap();
        assert!(rolled_back);

        // Confirmed state should NOT have the value
        assert!(manager.get_state("counter").unwrap().is_none());

        // No more pending commits
        assert_eq!(manager.pending_speculative_count().unwrap(), 0);
    }

    #[test]
    fn test_visibility_invariant() {
        // This test verifies the core safety property: speculative writes
        // are not visible to reads (get_state only reads confirmed store)
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));

        // Speculative commit
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("isolated", 100));
        let spec_result = manager.commit_with_speculation(&tx1).unwrap();
        assert!(spec_result.is_speculative);

        // Read should not see speculative value
        assert!(manager.get_state("isolated").unwrap().is_none());

        // Eager commit (use commit_local which always commits to confirmed)
        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(add_op("visible", 200));
        manager.commit_local(&tx2).unwrap();

        // Eager commit should be visible
        let value = manager.get_state("visible").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(200));

        // Speculative still not visible
        assert!(manager.get_state("isolated").unwrap().is_none());

        // After confirm, speculative becomes visible
        manager.confirm_speculative(spec_result.commit_id).unwrap();
        let value = manager.get_state("isolated").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(100));
    }

    #[test]
    fn test_speculative_with_custom_config() {
        use crate::transaction::speculative::SpeculativeConfig;

        // Disable speculation
        let disabled_config = SpeculativeConfig::disabled();
        let manager = CoordinationFreeManager::with_speculative_config(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            disabled_config,
        );

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("key", 10));

        let result = manager.commit_with_speculation(&tx).unwrap();

        // Should NOT be speculative (disabled)
        assert!(!result.is_speculative);

        // Value should be immediately visible
        let value = manager.get_state("key").unwrap().unwrap();
        assert_eq!(value.as_integer(), Some(10));
    }

    // =========================================================================
    // Escrow Integration Tests (POAC Paper Section 5)
    // =========================================================================

    #[test]
    fn test_escrow_not_enabled_by_default() {
        let manager = CoordinationFreeManager::new(NodeId::new("node-1"));
        assert!(!manager.has_escrow());
    }

    #[test]
    fn test_escrow_enabled_with_config() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );
        assert!(manager.has_escrow());
    }

    #[test]
    fn test_escrow_consume_local() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        // Register a resource
        manager.register_escrow_resource("inventory:sku1", 1000, 50).unwrap();

        // Consume quota
        let result = manager.consume_escrow("inventory:sku1", 10);
        assert!(matches!(result, EscrowResult::Success { remaining: 40 }));

        // Check remaining
        assert_eq!(manager.available_escrow("inventory:sku1"), Some(40));
    }

    #[test]
    fn test_escrow_quota_exhaustion_triggers_coordination() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        manager.register_escrow_resource("counter", 1000, 5).unwrap();

        // Consume all quota
        for _ in 0..5 {
            let result = manager.consume_escrow("counter", 1);
            assert!(result.is_success());
        }

        // Next consume should need coordination
        let result = manager.consume_escrow("counter", 1);
        assert!(result.needs_coordination());
    }

    #[test]
    fn test_escrow_replenish() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        manager.register_escrow_resource("counter", 1000, 10).unwrap();

        // Exhaust quota
        for _ in 0..10 {
            manager.consume_escrow("counter", 1);
        }
        assert_eq!(manager.available_escrow("counter"), Some(0));

        // Replenish
        manager.replenish_escrow("counter", 10).unwrap();
        assert_eq!(manager.available_escrow("counter"), Some(10));
    }

    #[test]
    fn test_escrow_auto_quota() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        // At 100 requests/second, optimal quota should be around 117
        let quota = manager.register_escrow_resource_auto("counter", 1000, 100.0).unwrap();
        assert!(quota >= 110 && quota <= 130, "Expected ~117, got {}", quota);
    }

    #[test]
    fn test_escrow_stats() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        manager.register_escrow_resource("counter", 1000, 10).unwrap();

        // Do some operations
        for _ in 0..8 {
            manager.consume_escrow("counter", 1);
        }

        let stats = manager.escrow_stats("counter").unwrap().unwrap();
        assert_eq!(stats.local_operations, 8);
        assert_eq!(stats.coordination_events, 0);
    }

    #[test]
    fn test_escrow_aggregate_stats() {
        let manager = CoordinationFreeManager::with_escrow(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            EscrowConfig::default(),
        );

        manager.register_escrow_resource("r1", 1000, 10).unwrap();
        manager.register_escrow_resource("r2", 2000, 20).unwrap();

        for _ in 0..5 {
            manager.consume_escrow("r1", 1);
            manager.consume_escrow("r2", 1);
        }

        let stats = manager.escrow_aggregate_stats().unwrap().unwrap();
        assert_eq!(stats.resource_count, 2);
        assert_eq!(stats.total_local_operations, 10);
    }

    #[test]
    fn test_full_config_with_escrow() {
        let manager = CoordinationFreeManager::with_full_config(
            NodeId::new("node-1"),
            CoordinationFreeConfig::default(),
            SpeculativeConfig::default(),
            Some(EscrowConfig::default()),
        );

        assert!(manager.has_escrow());
        manager.register_escrow_resource("test", 100, 10).unwrap();
        assert!(manager.consume_escrow("test", 5).is_success());
    }
}
