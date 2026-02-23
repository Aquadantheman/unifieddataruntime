//! Escrow transactions for linear horizontal scaling on hot spots.
//!
//! This module implements POAC Paper Section 5: Escrow Transactions.
//!
//! # Problem
//!
//! Hot spots (popular inventory items, global counters) serialize all access:
//!
//! ```text
//! Throughput_serialized = 1 / T_transaction
//! ```
//!
//! Even with fast transactions, single-row bottlenecks limit scalability.
//!
//! # Escrow Solution
//!
//! Pre-allocate quota to each node:
//!
//! ```text
//! Total = Σ quota_i + reserve
//! ```
//!
//! Operations consume local quota without coordination. Only quota exhaustion
//! triggers coordination.
//!
//! # Quota Sizing
//!
//! Requests follow a Poisson process. For interval T and n nodes, optimal quota:
//!
//! ```text
//! q* = F^-1_Poisson(1 - ε; λ_node)
//! ```
//!
//! Where:
//! - ε = target exhaustion rate (e.g., 0.01 for 1%)
//! - λ_node = (λ · T) / n = expected requests per node per interval
//!
//! With proper quota sizing, >99% of operations execute locally.
//!
//! # Usage
//!
//! ```ignore
//! use rhizo_core::transaction::{EscrowManager, EscrowConfig};
//! use rhizo_core::distributed::NodeId;
//!
//! // Create escrow manager with configuration
//! let config = EscrowConfig {
//!     target_exhaustion_rate: 0.01,  // 1% coordination rate
//!     replenish_interval_secs: 1.0,
//!     ..Default::default()
//! };
//! let mut manager = EscrowManager::new(NodeId::new("node-1"), config);
//!
//! // Register a resource with initial quota
//! manager.register_resource("inventory:sku123", 1000, 50);
//!
//! // Consume quota locally (no coordination)
//! match manager.try_consume("inventory:sku123", 1) {
//!     EscrowResult::Success { remaining } => {
//!         println!("Consumed locally, {} remaining", remaining);
//!     }
//!     EscrowResult::QuotaExhausted { requested, available } => {
//!         println!("Need coordination: requested {}, have {}", requested, available);
//!     }
//!     EscrowResult::ResourceNotFound => {
//!         println!("Resource not registered");
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

use crate::distributed::NodeId;
use serde::{Deserialize, Serialize};

/// Result of an escrow operation
#[derive(Debug, Clone, PartialEq)]
pub enum EscrowResult {
    /// Operation succeeded, quota consumed locally
    Success {
        /// Remaining quota after consumption
        remaining: i64,
    },

    /// Quota exhausted, coordination required
    QuotaExhausted {
        /// Amount requested
        requested: i64,
        /// Amount available
        available: i64,
    },

    /// Resource not registered with escrow manager
    ResourceNotFound,

    /// Cannot consume negative amounts
    InvalidAmount,
}

impl EscrowResult {
    /// Check if operation succeeded
    pub fn is_success(&self) -> bool {
        matches!(self, EscrowResult::Success { .. })
    }

    /// Check if coordination is needed
    pub fn needs_coordination(&self) -> bool {
        matches!(self, EscrowResult::QuotaExhausted { .. })
    }
}

/// Error type for escrow operations
#[derive(Debug, thiserror::Error)]
pub enum EscrowError {
    /// Lock acquisition failed
    #[error("Failed to acquire lock: {0}")]
    LockError(String),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    ConfigError(String),

    /// Resource already exists
    #[error("Resource already registered: {0}")]
    ResourceExists(String),

    /// Quota calculation failed
    #[error("Quota calculation error: {0}")]
    QuotaCalculationError(String),
}

/// Configuration for escrow quota sizing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscrowConfig {
    /// Target probability of quota exhaustion (ε)
    /// Default: 0.01 (1% coordination rate, 99% local)
    pub target_exhaustion_rate: f64,

    /// Interval between quota replenishment (seconds)
    /// Quota is sized for this interval's expected load
    pub replenish_interval_secs: f64,

    /// Minimum quota regardless of load estimates
    pub min_quota: i64,

    /// Maximum quota to prevent over-allocation
    pub max_quota: i64,

    /// Enable adaptive quota resizing based on observed load
    pub adaptive_sizing: bool,

    /// Learning rate for adaptive sizing (α in EMA)
    pub learning_rate: f64,
}

impl Default for EscrowConfig {
    fn default() -> Self {
        Self {
            target_exhaustion_rate: 0.01,   // 1% coordination rate
            replenish_interval_secs: 1.0,   // Size quota for 1 second
            min_quota: 10,                   // At least 10 operations
            max_quota: 10_000,              // Cap at 10k
            adaptive_sizing: true,
            learning_rate: 0.1,
        }
    }
}

impl EscrowConfig {
    /// Create config for high-throughput scenarios
    pub fn high_throughput() -> Self {
        Self {
            target_exhaustion_rate: 0.001,  // 0.1% coordination
            replenish_interval_secs: 0.5,
            min_quota: 100,
            max_quota: 100_000,
            adaptive_sizing: true,
            learning_rate: 0.05,
        }
    }

    /// Create config that disables escrow (always coordinates)
    pub fn disabled() -> Self {
        Self {
            target_exhaustion_rate: 1.0,    // Always exhaust
            replenish_interval_secs: 1.0,
            min_quota: 0,
            max_quota: 0,
            adaptive_sizing: false,
            learning_rate: 0.0,
        }
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), EscrowError> {
        if self.target_exhaustion_rate < 0.0 || self.target_exhaustion_rate > 1.0 {
            return Err(EscrowError::ConfigError(
                "target_exhaustion_rate must be in [0, 1]".to_string(),
            ));
        }
        if self.replenish_interval_secs <= 0.0 {
            return Err(EscrowError::ConfigError(
                "replenish_interval_secs must be positive".to_string(),
            ));
        }
        if self.min_quota > self.max_quota {
            return Err(EscrowError::ConfigError(
                "min_quota must not exceed max_quota".to_string(),
            ));
        }
        if self.learning_rate < 0.0 || self.learning_rate > 1.0 {
            return Err(EscrowError::ConfigError(
                "learning_rate must be in [0, 1]".to_string(),
            ));
        }
        Ok(())
    }
}

/// Statistics for an escrowed resource
#[derive(Debug, Clone, Default)]
pub struct EscrowStats {
    /// Total successful local operations
    pub local_operations: u64,

    /// Total coordination events (quota exhaustion)
    pub coordination_events: u64,

    /// Total amount consumed
    pub total_consumed: i64,

    /// Total amount replenished
    pub total_replenished: i64,

    /// Estimated request rate (λ per second)
    pub estimated_rate: f64,

    /// Current quota allocation
    pub current_quota: i64,

    /// Time of last replenishment
    pub last_replenish: Option<Instant>,
}

impl EscrowStats {
    /// Calculate local operation rate (percentage)
    pub fn local_rate(&self) -> f64 {
        let total = self.local_operations + self.coordination_events;
        if total == 0 {
            1.0 // No operations yet, assume all would be local
        } else {
            self.local_operations as f64 / total as f64
        }
    }
}

/// A single escrowed resource
#[derive(Debug)]
struct EscrowResource {
    /// Resource identifier (kept for debugging/logging)
    #[allow(dead_code)]
    resource_id: String,

    /// Current available quota
    available: i64,

    /// Allocated quota per interval
    allocated_quota: i64,

    /// Global total for this resource (for distributed validation)
    #[allow(dead_code)]
    global_total: i64,

    /// Statistics
    stats: EscrowStats,

    /// Operations since last rate estimate
    ops_since_estimate: u64,

    /// Time of last rate estimate
    last_estimate_time: Instant,
}

impl EscrowResource {
    fn new(resource_id: String, global_total: i64, initial_quota: i64) -> Self {
        let now = Instant::now();
        Self {
            resource_id,
            available: initial_quota,
            allocated_quota: initial_quota,
            global_total,
            stats: EscrowStats {
                current_quota: initial_quota,
                last_replenish: Some(now),
                ..Default::default()
            },
            ops_since_estimate: 0,
            last_estimate_time: now,
        }
    }

    /// Try to consume quota
    fn try_consume(&mut self, amount: i64) -> EscrowResult {
        if amount <= 0 {
            return EscrowResult::InvalidAmount;
        }

        // Track for rate estimation
        self.ops_since_estimate += 1;

        if self.available >= amount {
            self.available -= amount;
            self.stats.local_operations += 1;
            self.stats.total_consumed += amount;
            EscrowResult::Success {
                remaining: self.available,
            }
        } else {
            self.stats.coordination_events += 1;
            EscrowResult::QuotaExhausted {
                requested: amount,
                available: self.available,
            }
        }
    }

    /// Replenish quota (called after coordination or periodically)
    fn replenish(&mut self, amount: i64) {
        self.available = amount.min(self.allocated_quota);
        self.stats.total_replenished += amount;
        self.stats.last_replenish = Some(Instant::now());
    }

    /// Update rate estimate using EMA
    fn update_rate_estimate(&mut self, learning_rate: f64) {
        let elapsed = self.last_estimate_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            let observed_rate = self.ops_since_estimate as f64 / elapsed;
            self.stats.estimated_rate = self.stats.estimated_rate * (1.0 - learning_rate)
                + observed_rate * learning_rate;

            // Reset counters
            self.ops_since_estimate = 0;
            self.last_estimate_time = Instant::now();
        }
    }
}

/// Escrow manager for hot spot coordination
///
/// Manages pre-allocated quotas for resources, enabling local operations
/// without coordination when quota is available.
pub struct EscrowManager {
    /// This node's identifier
    node_id: NodeId,

    /// Configuration
    config: EscrowConfig,

    /// Escrowed resources
    resources: RwLock<HashMap<String, EscrowResource>>,

    /// Number of nodes in cluster (for quota division)
    node_count: usize,
}

impl EscrowManager {
    /// Create a new escrow manager
    pub fn new(node_id: NodeId, config: EscrowConfig) -> Self {
        Self {
            node_id,
            config,
            resources: RwLock::new(HashMap::new()),
            node_count: 1, // Single node by default
        }
    }

    /// Create with default configuration
    pub fn with_defaults(node_id: NodeId) -> Self {
        Self::new(node_id, EscrowConfig::default())
    }

    /// Get this node's identifier
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Set the number of nodes (for quota division)
    pub fn set_node_count(&mut self, count: usize) {
        self.node_count = count.max(1);
    }

    /// Register a resource for escrow management
    ///
    /// # Arguments
    ///
    /// * `resource_id` - Unique identifier for the resource
    /// * `global_total` - Total amount available across all nodes
    /// * `initial_quota` - Initial quota for this node
    pub fn register_resource(
        &self,
        resource_id: impl Into<String>,
        global_total: i64,
        initial_quota: i64,
    ) -> Result<(), EscrowError> {
        let resource_id = resource_id.into();
        let mut resources = self
            .resources
            .write()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        if resources.contains_key(&resource_id) {
            return Err(EscrowError::ResourceExists(resource_id));
        }

        let resource = EscrowResource::new(resource_id.clone(), global_total, initial_quota);
        resources.insert(resource_id, resource);
        Ok(())
    }

    /// Register a resource with automatically calculated optimal quota
    ///
    /// Uses Poisson-based sizing: q* = F^-1_Poisson(1 - ε; λ_node)
    pub fn register_resource_auto_quota(
        &self,
        resource_id: impl Into<String>,
        global_total: i64,
        expected_rate: f64,
    ) -> Result<i64, EscrowError> {
        let optimal_quota = self.calculate_optimal_quota(expected_rate)?;
        let resource_id = resource_id.into();

        let mut resources = self
            .resources
            .write()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        if resources.contains_key(&resource_id) {
            return Err(EscrowError::ResourceExists(resource_id));
        }

        let mut resource = EscrowResource::new(resource_id.clone(), global_total, optimal_quota);
        resource.stats.estimated_rate = expected_rate;
        resources.insert(resource_id, resource);

        Ok(optimal_quota)
    }

    /// Try to consume quota for a resource
    pub fn try_consume(&self, resource_id: &str, amount: i64) -> EscrowResult {
        let mut resources = match self.resources.write() {
            Ok(r) => r,
            Err(_) => return EscrowResult::ResourceNotFound,
        };

        match resources.get_mut(resource_id) {
            Some(resource) => resource.try_consume(amount),
            None => EscrowResult::ResourceNotFound,
        }
    }

    /// Replenish quota after coordination
    pub fn replenish(&self, resource_id: &str, amount: i64) -> Result<(), EscrowError> {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        if let Some(resource) = resources.get_mut(resource_id) {
            resource.replenish(amount);
            Ok(())
        } else {
            Err(EscrowError::LockError(format!(
                "Resource not found: {}",
                resource_id
            )))
        }
    }

    /// Get statistics for a resource
    pub fn get_stats(&self, resource_id: &str) -> Option<EscrowStats> {
        let resources = self.resources.read().ok()?;
        resources.get(resource_id).map(|r| r.stats.clone())
    }

    /// Get current available quota for a resource
    pub fn available_quota(&self, resource_id: &str) -> Option<i64> {
        let resources = self.resources.read().ok()?;
        resources.get(resource_id).map(|r| r.available)
    }

    /// Calculate optimal quota using Poisson inverse CDF
    ///
    /// Formula: q* = F^-1_Poisson(1 - ε; λ_node)
    ///
    /// Where:
    /// - ε = target exhaustion rate
    /// - λ_node = (λ · T) / n = expected requests per node per interval
    pub fn calculate_optimal_quota(&self, expected_rate: f64) -> Result<i64, EscrowError> {
        // λ_node = (λ · T) / n
        let lambda_node =
            (expected_rate * self.config.replenish_interval_secs) / self.node_count as f64;

        if lambda_node <= 0.0 {
            return Ok(self.config.min_quota);
        }

        // Calculate q* = F^-1_Poisson(1 - ε; λ_node)
        // This is the smallest q such that P(Poisson(λ) <= q) >= 1 - ε
        let target_cdf = 1.0 - self.config.target_exhaustion_rate;
        let quota = poisson_inverse_cdf(target_cdf, lambda_node);

        // Apply bounds
        let bounded_quota = quota
            .max(self.config.min_quota)
            .min(self.config.max_quota);

        Ok(bounded_quota)
    }

    /// Update rate estimates for all resources (call periodically)
    pub fn update_rate_estimates(&self) -> Result<(), EscrowError> {
        let mut resources = self
            .resources
            .write()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        for resource in resources.values_mut() {
            resource.update_rate_estimate(self.config.learning_rate);
        }

        Ok(())
    }

    /// Resize quotas based on current rate estimates (adaptive sizing)
    pub fn resize_quotas(&self) -> Result<Vec<(String, i64, i64)>, EscrowError> {
        if !self.config.adaptive_sizing {
            return Ok(vec![]);
        }

        let mut resources = self
            .resources
            .write()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        let mut resizes = Vec::new();

        for (id, resource) in resources.iter_mut() {
            let old_quota = resource.allocated_quota;
            let new_quota = self
                .calculate_optimal_quota_internal(resource.stats.estimated_rate)?
                .max(self.config.min_quota)
                .min(self.config.max_quota);

            if new_quota != old_quota {
                resource.allocated_quota = new_quota;
                resource.stats.current_quota = new_quota;
                resizes.push((id.clone(), old_quota, new_quota));
            }
        }

        Ok(resizes)
    }

    /// Internal quota calculation (doesn't need lock)
    fn calculate_optimal_quota_internal(&self, expected_rate: f64) -> Result<i64, EscrowError> {
        let lambda_node =
            (expected_rate * self.config.replenish_interval_secs) / self.node_count as f64;

        if lambda_node <= 0.0 {
            return Ok(self.config.min_quota);
        }

        let target_cdf = 1.0 - self.config.target_exhaustion_rate;
        Ok(poisson_inverse_cdf(target_cdf, lambda_node))
    }

    /// Get aggregate statistics across all resources
    pub fn aggregate_stats(&self) -> Result<EscrowAggregateStats, EscrowError> {
        let resources = self
            .resources
            .read()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;

        let mut total_local_operations = 0u64;
        let mut total_coordination_events = 0u64;
        let mut total_consumed = 0i64;

        for resource in resources.values() {
            total_local_operations += resource.stats.local_operations;
            total_coordination_events += resource.stats.coordination_events;
            total_consumed += resource.stats.total_consumed;
        }

        Ok(EscrowAggregateStats {
            resource_count: resources.len(),
            total_local_operations,
            total_coordination_events,
            total_consumed,
        })
    }

    /// Check if a resource needs coordination (quota below threshold)
    pub fn needs_replenishment(&self, resource_id: &str, threshold_percent: f64) -> Option<bool> {
        let resources = self.resources.read().ok()?;
        let resource = resources.get(resource_id)?;

        let threshold = (resource.allocated_quota as f64 * threshold_percent) as i64;
        Some(resource.available < threshold)
    }

    /// List all registered resource IDs
    pub fn resource_ids(&self) -> Result<Vec<String>, EscrowError> {
        let resources = self
            .resources
            .read()
            .map_err(|_| EscrowError::LockError("resources".to_string()))?;
        Ok(resources.keys().cloned().collect())
    }
}

/// Aggregate statistics across all escrowed resources
#[derive(Debug, Clone, Default)]
pub struct EscrowAggregateStats {
    /// Number of registered resources
    pub resource_count: usize,

    /// Total successful local operations
    pub total_local_operations: u64,

    /// Total coordination events
    pub total_coordination_events: u64,

    /// Total amount consumed
    pub total_consumed: i64,
}

impl EscrowAggregateStats {
    /// Calculate overall local operation rate
    pub fn local_rate(&self) -> f64 {
        let total = self.total_local_operations + self.total_coordination_events;
        if total == 0 {
            1.0
        } else {
            self.total_local_operations as f64 / total as f64
        }
    }
}

/// Calculate Poisson inverse CDF (quantile function)
///
/// Returns the smallest k such that P(X <= k) >= p where X ~ Poisson(λ)
///
/// Uses iterative CDF calculation for numerical stability.
fn poisson_inverse_cdf(p: f64, lambda: f64) -> i64 {
    if lambda <= 0.0 {
        return 0;
    }
    if p <= 0.0 {
        return 0;
    }
    if p >= 1.0 {
        // Return a large value (Poisson has unbounded support)
        return (lambda * 10.0) as i64;
    }

    // Calculate CDF iteratively: P(X <= k) = Σ e^-λ * λ^i / i!
    let mut cdf = 0.0;
    let mut pmf = (-lambda).exp(); // P(X = 0) = e^-λ
    let mut k = 0i64;

    while cdf < p {
        cdf += pmf;
        if cdf >= p {
            break;
        }
        k += 1;
        // P(X = k) = P(X = k-1) * λ / k
        pmf *= lambda / k as f64;

        // Safety bound to prevent infinite loop
        if k > (lambda * 20.0) as i64 + 1000 {
            break;
        }
    }

    k
}

/// Calculate Poisson CDF: P(X <= k) where X ~ Poisson(λ)
#[allow(dead_code)]
fn poisson_cdf(k: i64, lambda: f64) -> f64 {
    if k < 0 || lambda <= 0.0 {
        return 0.0;
    }

    let mut cdf = 0.0;
    let mut pmf = (-lambda).exp();

    for i in 0..=k {
        cdf += pmf;
        if i < k {
            pmf *= lambda / (i + 1) as f64;
        }
    }

    cdf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poisson_inverse_cdf() {
        // For λ = 10, P(X <= 15) ≈ 0.95
        let q = poisson_inverse_cdf(0.95, 10.0);
        assert!(q >= 14 && q <= 16, "Expected ~15, got {}", q);

        // For λ = 100, we need higher quantiles
        let q = poisson_inverse_cdf(0.99, 100.0);
        assert!(q >= 115 && q <= 125, "Expected ~120, got {}", q);

        // Edge cases
        assert_eq!(poisson_inverse_cdf(0.0, 10.0), 0);
        assert_eq!(poisson_inverse_cdf(0.5, 0.0), 0);
    }

    #[test]
    fn test_poisson_cdf() {
        // P(X <= 10) for λ = 10 should be around 0.58
        let p = poisson_cdf(10, 10.0);
        assert!(p > 0.5 && p < 0.7, "Expected ~0.58, got {}", p);

        // P(X <= 0) for λ = 5 = e^-5 ≈ 0.0067
        let p = poisson_cdf(0, 5.0);
        assert!((p - 0.0067).abs() < 0.001, "Expected ~0.0067, got {}", p);
    }

    #[test]
    fn test_escrow_manager_basic() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));

        // Register resource
        manager.register_resource("inventory:sku1", 1000, 50).unwrap();

        // Consume quota
        let result = manager.try_consume("inventory:sku1", 10);
        assert!(matches!(result, EscrowResult::Success { remaining: 40 }));

        // Check remaining
        assert_eq!(manager.available_quota("inventory:sku1"), Some(40));
    }

    #[test]
    fn test_escrow_quota_exhaustion() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 20).unwrap();

        // Consume all quota
        for _ in 0..20 {
            let result = manager.try_consume("counter", 1);
            assert!(result.is_success());
        }

        // Next consume should fail
        let result = manager.try_consume("counter", 1);
        assert!(result.needs_coordination());
        assert!(matches!(
            result,
            EscrowResult::QuotaExhausted {
                requested: 1,
                available: 0
            }
        ));
    }

    #[test]
    fn test_escrow_replenish() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 10).unwrap();

        // Exhaust quota
        for _ in 0..10 {
            manager.try_consume("counter", 1);
        }
        assert_eq!(manager.available_quota("counter"), Some(0));

        // Replenish
        manager.replenish("counter", 10).unwrap();
        assert_eq!(manager.available_quota("counter"), Some(10));
    }

    #[test]
    fn test_escrow_auto_quota() {
        let config = EscrowConfig {
            target_exhaustion_rate: 0.01,
            replenish_interval_secs: 1.0,
            min_quota: 10,
            max_quota: 10_000,
            ..Default::default()
        };
        let manager = EscrowManager::new(NodeId::new("node-1"), config);

        // At 100 requests/second, optimal quota should be around 117
        // (Poisson inverse CDF at 0.99 for λ=100)
        let quota = manager.register_resource_auto_quota("counter", 1000, 100.0).unwrap();
        assert!(quota >= 110 && quota <= 130, "Expected ~117, got {}", quota);
    }

    #[test]
    fn test_escrow_stats() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 10).unwrap();

        // Do some operations
        for _ in 0..8 {
            manager.try_consume("counter", 1);
        }
        // These will need coordination
        manager.try_consume("counter", 5);
        manager.try_consume("counter", 5);

        let stats = manager.get_stats("counter").unwrap();
        assert_eq!(stats.local_operations, 8);
        assert_eq!(stats.coordination_events, 2);
        assert!((stats.local_rate() - 0.8).abs() < 0.01);
    }

    #[test]
    fn test_escrow_multinode_quota() {
        let config = EscrowConfig {
            target_exhaustion_rate: 0.01,
            replenish_interval_secs: 1.0,
            min_quota: 10,
            max_quota: 10_000,
            ..Default::default()
        };
        let mut manager = EscrowManager::new(NodeId::new("node-1"), config);
        manager.set_node_count(5);

        // With 5 nodes and 500 requests/second total, each node gets 100/s
        // λ_node = 500 / 5 = 100
        let quota = manager.calculate_optimal_quota(500.0).unwrap();
        assert!(quota >= 110 && quota <= 130, "Expected ~117, got {}", quota);
    }

    #[test]
    fn test_escrow_config_validation() {
        let mut config = EscrowConfig::default();

        // Valid
        config.validate().unwrap();

        // Invalid exhaustion rate
        config.target_exhaustion_rate = 1.5;
        assert!(config.validate().is_err());
        config.target_exhaustion_rate = 0.01;

        // Invalid interval
        config.replenish_interval_secs = -1.0;
        assert!(config.validate().is_err());
        config.replenish_interval_secs = 1.0;

        // Invalid quota bounds
        config.min_quota = 100;
        config.max_quota = 10;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_escrow_invalid_amount() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 50).unwrap();

        let result = manager.try_consume("counter", 0);
        assert!(matches!(result, EscrowResult::InvalidAmount));

        let result = manager.try_consume("counter", -5);
        assert!(matches!(result, EscrowResult::InvalidAmount));
    }

    #[test]
    fn test_escrow_resource_not_found() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));

        let result = manager.try_consume("nonexistent", 1);
        assert!(matches!(result, EscrowResult::ResourceNotFound));
    }

    #[test]
    fn test_escrow_duplicate_resource() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 50).unwrap();

        let result = manager.register_resource("counter", 2000, 100);
        assert!(matches!(result, Err(EscrowError::ResourceExists(_))));
    }

    #[test]
    fn test_escrow_high_throughput_config() {
        let config = EscrowConfig::high_throughput();
        assert_eq!(config.target_exhaustion_rate, 0.001);
        assert_eq!(config.min_quota, 100);
        config.validate().unwrap();
    }

    #[test]
    fn test_escrow_disabled_config() {
        let config = EscrowConfig::disabled();
        assert_eq!(config.target_exhaustion_rate, 1.0);
        assert_eq!(config.max_quota, 0);
        config.validate().unwrap();
    }

    #[test]
    fn test_aggregate_stats() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("r1", 1000, 10).unwrap();
        manager.register_resource("r2", 2000, 20).unwrap();

        for _ in 0..5 {
            manager.try_consume("r1", 1);
            manager.try_consume("r2", 1);
        }

        let stats = manager.aggregate_stats().unwrap();
        assert_eq!(stats.resource_count, 2);
        assert_eq!(stats.total_local_operations, 10);
        assert_eq!(stats.total_consumed, 10);
    }

    #[test]
    fn test_needs_replenishment() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("counter", 1000, 100).unwrap();

        // Initially at 100% - no replenishment needed
        assert_eq!(manager.needs_replenishment("counter", 0.2), Some(false));

        // Consume 90 - now at 10%
        for _ in 0..90 {
            manager.try_consume("counter", 1);
        }
        assert_eq!(manager.needs_replenishment("counter", 0.2), Some(true));
    }

    #[test]
    fn test_resource_ids() {
        let manager = EscrowManager::with_defaults(NodeId::new("node-1"));
        manager.register_resource("a", 100, 10).unwrap();
        manager.register_resource("b", 200, 20).unwrap();
        manager.register_resource("c", 300, 30).unwrap();

        let mut ids = manager.resource_ids().unwrap();
        ids.sort();
        assert_eq!(ids, vec!["a", "b", "c"]);
    }
}
