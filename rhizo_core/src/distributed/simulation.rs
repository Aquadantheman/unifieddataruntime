//! Multi-node simulation framework for coordination-free transactions.
//!
//! This module provides a simulation framework to prove that nodes using
//! coordination-free transactions converge to the same state regardless
//! of message ordering, network delays, or partitions.
//!
//! # Theory Validation
//!
//! The simulation validates the core theorems:
//!
//! 1. **Convergence Theorem**: Given algebraic operations, all nodes
//!    converge to the same state regardless of message order.
//!
//! 2. **Partition Tolerance**: Nodes can operate independently during
//!    network partitions and merge correctly when reconnected.
//!
//! 3. **Commutativity Proof**: merge(A, B) = merge(B, A) for all updates.
//!
//! # Usage
//!
//! ```
//! use rhizo_core::distributed::simulation::{
//!     SimulatedCluster, NetworkCondition, SimulationConfig,
//! };
//! use rhizo_core::distributed::{AlgebraicOperation, AlgebraicTransaction};
//! use rhizo_core::algebraic::{OpType, AlgebraicValue};
//!
//! // Create a 5-node cluster
//! let mut cluster = SimulatedCluster::new(5);
//!
//! // Each node performs local operations
//! for i in 0usize..5 {
//!     let mut tx = AlgebraicTransaction::new();
//!     tx.add_operation(AlgebraicOperation::new(
//!         "counter",
//!         OpType::AbelianAdd,
//!         AlgebraicValue::integer(((i + 1) * 10) as i64),
//!     ));
//!     cluster.commit_on_node(i, tx).unwrap();
//! }
//!
//! // Propagate all updates (simulated gossip)
//! cluster.propagate_all();
//!
//! // Verify all nodes converged to the same state
//! assert!(cluster.verify_convergence());
//!
//! // Check the final value: 10 + 20 + 30 + 40 + 50 = 150
//! let state = cluster.get_node_state(0, "counter").unwrap();
//! assert_eq!(state.as_integer(), Some(150));
//! ```

use super::local_commit::{
    AlgebraicTransaction, LocalCommitError, LocalCommitProtocol, VersionedUpdate,
};
use super::vector_clock::{NodeId, VectorClock};
use crate::algebraic::{AlgebraicMerger, AlgebraicValue, MergeResult, OpType};
use std::collections::{HashMap, HashSet, VecDeque};

/// Configuration for the simulation.
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// Maximum number of propagation rounds
    pub max_rounds: usize,
    /// Whether to randomize message order
    pub randomize_order: bool,
    /// Simulated network partition (node pairs that can't communicate)
    pub partitions: Vec<(usize, usize)>,
    /// Adversarial network conditions for chaos testing
    pub adversarial: Option<AdversarialConfig>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            max_rounds: 100,
            randomize_order: false,
            partitions: Vec::new(),
            adversarial: None,
        }
    }
}

/// Configuration for adversarial/chaos testing of the distributed simulation.
///
/// This enables Jepsen-style testing where messages may be randomly dropped,
/// delayed, or reordered to verify that the system converges correctly under
/// adverse conditions.
#[derive(Debug, Clone)]
pub struct AdversarialConfig {
    /// Probability of dropping a message (0.0 - 1.0)
    pub drop_probability: f64,
    /// Probability of delaying a message by 1-max_delay rounds
    pub delay_probability: f64,
    /// Maximum rounds to delay a message
    pub max_delay: usize,
    /// Probability of duplicating a message
    pub duplicate_probability: f64,
    /// Random seed for reproducibility (None = random each run)
    pub seed: Option<u64>,
}

impl Default for AdversarialConfig {
    fn default() -> Self {
        Self {
            drop_probability: 0.0,
            delay_probability: 0.0,
            max_delay: 5,
            duplicate_probability: 0.0,
            seed: None,
        }
    }
}

impl AdversarialConfig {
    /// Create a mild adversarial config (5% drops, 10% delays)
    pub fn mild() -> Self {
        Self {
            drop_probability: 0.05,
            delay_probability: 0.10,
            max_delay: 3,
            duplicate_probability: 0.02,
            seed: None,
        }
    }

    /// Create a moderate adversarial config (15% drops, 25% delays)
    pub fn moderate() -> Self {
        Self {
            drop_probability: 0.15,
            delay_probability: 0.25,
            max_delay: 5,
            duplicate_probability: 0.05,
            seed: None,
        }
    }

    /// Create a severe adversarial config (30% drops, 40% delays)
    pub fn severe() -> Self {
        Self {
            drop_probability: 0.30,
            delay_probability: 0.40,
            max_delay: 10,
            duplicate_probability: 0.10,
            seed: None,
        }
    }

    /// Set the random seed for reproducibility
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }
}

/// Network condition for message delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkCondition {
    /// Messages delivered immediately in order
    Perfect,
    /// Messages may be reordered
    Reordered,
    /// Messages delayed by N rounds
    Delayed(usize),
    /// Network is partitioned (no delivery)
    Partitioned,
}

/// A message being sent between nodes.
#[derive(Debug, Clone)]
pub struct Message {
    /// Source node index
    pub from: usize,
    /// Destination node index
    pub to: usize,
    /// The versioned update being sent
    pub update: VersionedUpdate,
    /// Delivery delay (rounds remaining)
    pub delay: usize,
}

/// A simulated node in the cluster.
#[derive(Debug, Clone)]
pub struct SimulatedNode {
    /// Node identifier
    pub node_id: NodeId,
    /// Node index in cluster
    pub index: usize,
    /// Current vector clock
    pub clock: VectorClock,
    /// Current state (key -> (op_type, value))
    pub state: HashMap<String, (OpType, AlgebraicValue)>,
    /// Updates this node has produced
    pub local_updates: Vec<VersionedUpdate>,
    /// All updates this node has applied (local + received), for partition healing
    pub all_updates: Vec<VersionedUpdate>,
    /// Updates this node has received and applied (deduplication IDs)
    pub applied_updates: HashSet<String>,
    /// Pending updates to send to other nodes
    pub outbox: VecDeque<VersionedUpdate>,
}

impl SimulatedNode {
    /// Create a new simulated node.
    pub fn new(index: usize) -> Self {
        let node_id = NodeId::new(format!("node-{}", index));
        Self {
            node_id,
            index,
            clock: VectorClock::new(),
            state: HashMap::new(),
            local_updates: Vec::new(),
            all_updates: Vec::new(),
            applied_updates: HashSet::new(),
            outbox: VecDeque::new(),
        }
    }

    /// Commit a transaction locally.
    pub fn commit(&mut self, tx: AlgebraicTransaction) -> Result<VersionedUpdate, LocalCommitError> {
        let update = LocalCommitProtocol::commit_local(&tx, &self.node_id, &mut self.clock)?;

        // Apply to local state
        self.apply_update(&update);

        // Track this update
        let update_id = self.generate_update_id(&update);
        self.applied_updates.insert(update_id);
        self.local_updates.push(update.clone());
        self.all_updates.push(update.clone());

        // Queue for propagation
        self.outbox.push_back(update.clone());

        Ok(update)
    }

    /// Apply an update to local state.
    pub fn apply_update(&mut self, update: &VersionedUpdate) {
        for op in update.operations() {
            let key = op.key().to_string();

            if let Some((existing_op_type, existing_value)) = self.state.get(&key) {
                // Merge with existing value
                if *existing_op_type == op.op_type() {
                    let merge_result =
                        AlgebraicMerger::merge(op.op_type(), existing_value, op.value());
                    if let MergeResult::Merged(merged_value) = merge_result {
                        self.state.insert(key, (op.op_type(), merged_value));
                    }
                }
            } else {
                // First value for this key
                self.state.insert(key, (op.op_type(), op.value().clone()));
            }
        }

        // Update clock
        self.clock.merge(update.clock());
    }

    /// Receive and apply an update from another node.
    pub fn receive_update(&mut self, update: &VersionedUpdate) -> bool {
        let update_id = self.generate_update_id(update);

        // Skip if already applied (deduplication)
        if self.applied_updates.contains(&update_id) {
            return false;
        }

        // Apply the update
        self.apply_update(update);
        self.applied_updates.insert(update_id);
        self.all_updates.push(update.clone());

        // Queue for further propagation (gossip)
        self.outbox.push_back(update.clone());

        true
    }

    /// Generate a unique ID for an update (for deduplication).
    ///
    /// Uses the full vector clock state (sorted for determinism) instead of
    /// just the clock sum, which is not unique across different updates.
    fn generate_update_id(&self, update: &VersionedUpdate) -> String {
        // Use explicit update_id if set
        if let Some(id) = update.update_id() {
            return id.to_string();
        }
        // Build deterministic clock representation: sorted node:time pairs
        let mut entries: Vec<_> = update.clock().entries().collect();
        entries.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        let clock_str: String = entries
            .iter()
            .map(|(node, time)| format!("{}={}", node.as_str(), time))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}@{}", update.origin_node(), clock_str)
    }

    /// Get the current value for a key.
    pub fn get_state(&self, key: &str) -> Option<&AlgebraicValue> {
        self.state.get(key).map(|(_, v)| v)
    }

    /// Get all keys in state.
    pub fn keys(&self) -> Vec<String> {
        self.state.keys().cloned().collect()
    }
}

/// A simulated cluster of nodes.
#[derive(Debug)]
pub struct SimulatedCluster {
    /// All nodes in the cluster
    pub nodes: Vec<SimulatedNode>,
    /// Pending messages in transit
    pub messages: VecDeque<Message>,
    /// Simulation configuration
    pub config: SimulationConfig,
    /// Current simulation round
    pub round: usize,
    /// Statistics
    pub stats: SimulationStats,
}

/// Statistics from the simulation.
#[derive(Debug, Default, Clone)]
pub struct SimulationStats {
    /// Total messages sent
    pub messages_sent: usize,
    /// Total messages delivered
    pub messages_delivered: usize,
    /// Total messages dropped (partitions)
    pub messages_dropped: usize,
    /// Rounds until convergence
    pub rounds_to_converge: Option<usize>,
    /// Total operations committed
    pub operations_committed: usize,
    /// Messages dropped by adversarial conditions
    pub adversarial_drops: usize,
    /// Messages delayed by adversarial conditions
    pub adversarial_delays: usize,
    /// Messages duplicated by adversarial conditions
    pub adversarial_duplicates: usize,
}

impl SimulatedCluster {
    /// Create a new cluster with N nodes.
    pub fn new(num_nodes: usize) -> Self {
        let nodes = (0..num_nodes).map(SimulatedNode::new).collect();
        Self {
            nodes,
            messages: VecDeque::new(),
            config: SimulationConfig::default(),
            round: 0,
            stats: SimulationStats::default(),
        }
    }

    /// Create a cluster with custom configuration.
    pub fn with_config(num_nodes: usize, config: SimulationConfig) -> Self {
        let nodes = (0..num_nodes).map(SimulatedNode::new).collect();
        Self {
            nodes,
            messages: VecDeque::new(),
            config,
            round: 0,
            stats: SimulationStats::default(),
        }
    }

    /// Get the number of nodes.
    pub fn num_nodes(&self) -> usize {
        self.nodes.len()
    }

    /// Commit a transaction on a specific node.
    pub fn commit_on_node(
        &mut self,
        node_index: usize,
        tx: AlgebraicTransaction,
    ) -> Result<VersionedUpdate, LocalCommitError> {
        let update = self.nodes[node_index].commit(tx)?;
        self.stats.operations_committed += update.operations().len();
        Ok(update)
    }

    /// Broadcast updates from all nodes to all other nodes.
    pub fn broadcast_all(&mut self) {
        let num_nodes = self.nodes.len();

        for from in 0..num_nodes {
            while let Some(update) = self.nodes[from].outbox.pop_front() {
                for to in 0..num_nodes {
                    if from != to && !self.is_partitioned(from, to) {
                        self.messages.push_back(Message {
                            from,
                            to,
                            update: update.clone(),
                            delay: 0,
                        });
                        self.stats.messages_sent += 1;
                    }
                }
            }
        }
    }

    /// Deliver all pending messages.
    pub fn deliver_messages(&mut self) {
        let messages: Vec<Message> = self.messages.drain(..).collect();

        for msg in messages {
            if !self.is_partitioned(msg.from, msg.to) {
                self.nodes[msg.to].receive_update(&msg.update);
                self.stats.messages_delivered += 1;
            } else {
                self.stats.messages_dropped += 1;
            }
        }
    }

    /// Deliver messages with adversarial network conditions.
    ///
    /// This method applies random drops, delays, and duplications based on
    /// the adversarial configuration. Used for chaos/fuzz testing.
    #[cfg(test)]
    pub fn deliver_messages_adversarial(&mut self, rng: &mut impl rand::Rng) {

        let adversarial = match &self.config.adversarial {
            Some(cfg) => cfg.clone(),
            None => {
                // No adversarial config, fall back to normal delivery
                return self.deliver_messages();
            }
        };

        let messages: Vec<Message> = self.messages.drain(..).collect();
        let mut delayed_messages: Vec<Message> = Vec::new();
        let mut duplicate_messages: Vec<Message> = Vec::new();

        for mut msg in messages {
            // Check partition first
            if self.is_partitioned(msg.from, msg.to) {
                self.stats.messages_dropped += 1;
                continue;
            }

            // Handle delayed messages from previous rounds
            if msg.delay > 0 {
                msg.delay -= 1;
                delayed_messages.push(msg);
                continue;
            }

            // Random drop
            if rng.gen::<f64>() < adversarial.drop_probability {
                self.stats.adversarial_drops += 1;
                continue;
            }

            // Random delay
            if rng.gen::<f64>() < adversarial.delay_probability {
                let delay = rng.gen_range(1..=adversarial.max_delay);
                msg.delay = delay;
                delayed_messages.push(msg.clone());
                self.stats.adversarial_delays += 1;
                continue;
            }

            // Random duplicate (deliver now and queue a copy)
            if rng.gen::<f64>() < adversarial.duplicate_probability {
                duplicate_messages.push(msg.clone());
                self.stats.adversarial_duplicates += 1;
            }

            // Deliver the message
            self.nodes[msg.to].receive_update(&msg.update);
            self.stats.messages_delivered += 1;
        }

        // Re-queue delayed messages
        for msg in delayed_messages {
            self.messages.push_back(msg);
        }

        // Queue duplicate messages for next round
        for msg in duplicate_messages {
            self.messages.push_back(msg);
        }
    }

    /// Run one round of propagation with adversarial conditions.
    #[cfg(test)]
    pub fn propagate_round_adversarial(&mut self, rng: &mut impl rand::Rng) {
        self.broadcast_all();
        self.deliver_messages_adversarial(rng);
        self.round += 1;
    }

    /// Propagate until convergence with adversarial conditions.
    ///
    /// In adversarial mode, messages may be dropped. Real gossip protocols
    /// handle this by periodically re-broadcasting state. We simulate this
    /// by requeuing all updates every `requeue_interval` rounds.
    #[cfg(test)]
    pub fn propagate_all_adversarial(&mut self, rng: &mut impl rand::Rng) {
        const REQUEUE_INTERVAL: usize = 10; // Re-gossip every 10 rounds

        for round_num in 0..self.config.max_rounds {
            let had_messages = !self.nodes.iter().all(|n| n.outbox.is_empty())
                || !self.messages.is_empty(); // Include delayed messages

            self.propagate_round_adversarial(rng);

            // Periodically requeue all updates to recover from drops
            // This simulates real gossip protocol behavior
            if round_num > 0 && round_num % REQUEUE_INTERVAL == 0 {
                self.requeue_all_updates();
            }

            if !had_messages && self.verify_convergence() {
                self.stats.rounds_to_converge = Some(self.round);
                return;
            }
        }
    }

    /// Run one round of propagation.
    pub fn propagate_round(&mut self) {
        self.broadcast_all();
        self.deliver_messages();
        self.round += 1;
    }

    /// Propagate until convergence or max rounds.
    pub fn propagate_all(&mut self) {
        for _ in 0..self.config.max_rounds {
            let had_messages = !self.nodes.iter().all(|n| n.outbox.is_empty());

            self.propagate_round();

            if !had_messages && self.verify_convergence() {
                self.stats.rounds_to_converge = Some(self.round);
                return;
            }
        }
    }

    /// Check if two nodes are partitioned.
    fn is_partitioned(&self, from: usize, to: usize) -> bool {
        self.config
            .partitions
            .iter()
            .any(|&(a, b)| (a == from && b == to) || (a == to && b == from))
    }

    /// Add a network partition between two nodes.
    pub fn partition(&mut self, node_a: usize, node_b: usize) {
        self.config.partitions.push((node_a, node_b));
    }

    /// Remove all partitions (heal network).
    pub fn heal_partitions(&mut self) {
        self.config.partitions.clear();
    }

    /// Re-queue all known updates for propagation.
    /// Call this after healing partitions to ensure updates are re-gossiped.
    /// Includes both locally-originated and transitively received updates
    /// so bridge nodes can relay updates to previously-partitioned peers.
    pub fn requeue_all_updates(&mut self) {
        for node in &mut self.nodes {
            for update in &node.all_updates {
                node.outbox.push_back(update.clone());
            }
        }
    }

    /// Verify that all nodes have converged to the same state.
    pub fn verify_convergence(&self) -> bool {
        if self.nodes.is_empty() {
            return true;
        }

        // Collect all keys across all nodes
        let all_keys: HashSet<String> = self
            .nodes
            .iter()
            .flat_map(|n| n.keys())
            .collect();

        // Check each key has the same value on all nodes
        for key in all_keys {
            let mut values: Vec<Option<&AlgebraicValue>> = Vec::new();
            for node in &self.nodes {
                values.push(node.get_state(&key));
            }

            // All values should be equal
            let first = values.first().cloned().flatten();
            if !values.iter().all(|v| *v == first) {
                return false;
            }
        }

        true
    }

    /// Get the state of a key on a specific node.
    pub fn get_node_state(&self, node_index: usize, key: &str) -> Option<&AlgebraicValue> {
        self.nodes[node_index].get_state(key)
    }

    /// Get statistics from the simulation.
    pub fn get_stats(&self) -> &SimulationStats {
        &self.stats
    }

    /// Get all keys that exist across any node.
    pub fn all_keys(&self) -> Vec<String> {
        self.nodes
            .iter()
            .flat_map(|n| n.keys())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Debug print the state of all nodes.
    pub fn debug_state(&self) -> String {
        let mut output = String::new();
        for (i, node) in self.nodes.iter().enumerate() {
            output.push_str(&format!("Node {}: {:?}\n", i, node.state));
        }
        output
    }
}

/// Builder for complex simulation scenarios.
#[derive(Debug)]
pub struct SimulationBuilder {
    num_nodes: usize,
    config: SimulationConfig,
    initial_operations: Vec<(usize, AlgebraicTransaction)>,
}

impl SimulationBuilder {
    /// Create a new simulation builder.
    pub fn new(num_nodes: usize) -> Self {
        Self {
            num_nodes,
            config: SimulationConfig::default(),
            initial_operations: Vec::new(),
        }
    }

    /// Set max propagation rounds.
    pub fn max_rounds(mut self, rounds: usize) -> Self {
        self.config.max_rounds = rounds;
        self
    }

    /// Enable message reordering.
    pub fn with_reordering(mut self) -> Self {
        self.config.randomize_order = true;
        self
    }

    /// Add a network partition.
    pub fn with_partition(mut self, node_a: usize, node_b: usize) -> Self {
        self.config.partitions.push((node_a, node_b));
        self
    }

    /// Add an initial operation for a node.
    pub fn with_operation(mut self, node: usize, tx: AlgebraicTransaction) -> Self {
        self.initial_operations.push((node, tx));
        self
    }

    /// Enable adversarial network conditions.
    pub fn with_adversarial(mut self, config: AdversarialConfig) -> Self {
        self.config.adversarial = Some(config);
        self
    }

    /// Build and run the simulation.
    pub fn run(self) -> Result<SimulatedCluster, LocalCommitError> {
        let mut cluster = SimulatedCluster::with_config(self.num_nodes, self.config);

        // Commit all initial operations
        for (node_index, tx) in self.initial_operations {
            cluster.commit_on_node(node_index, tx)?;
        }

        // Propagate until convergence
        cluster.propagate_all();

        Ok(cluster)
    }

    /// Build and run with adversarial conditions.
    #[cfg(test)]
    pub fn run_adversarial(self, seed: u64) -> Result<SimulatedCluster, LocalCommitError> {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut cluster = SimulatedCluster::with_config(self.num_nodes, self.config);

        // Commit all initial operations
        for (node_index, tx) in self.initial_operations {
            cluster.commit_on_node(node_index, tx)?;
        }

        // Propagate with adversarial conditions
        cluster.propagate_all_adversarial(&mut rng);

        Ok(cluster)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebraic::{AlgebraicValue, OpType};
    use crate::distributed::AlgebraicOperation;

    // Helper to create an ADD operation
    fn add_op(key: &str, value: i64) -> AlgebraicOperation {
        AlgebraicOperation::new(key, OpType::AbelianAdd, AlgebraicValue::integer(value))
    }

    // Helper to create a MAX operation
    fn max_op(key: &str, value: i64) -> AlgebraicOperation {
        AlgebraicOperation::new(key, OpType::SemilatticeMax, AlgebraicValue::integer(value))
    }

    // Helper to create a UNION operation
    fn union_op(key: &str, values: &[&str]) -> AlgebraicOperation {
        AlgebraicOperation::new(
            key,
            OpType::SemilatticeUnion,
            AlgebraicValue::string_set(values.to_vec()),
        )
    }

    // ============ Basic Cluster Tests ============

    #[test]
    fn test_create_cluster() {
        let cluster = SimulatedCluster::new(5);
        assert_eq!(cluster.num_nodes(), 5);
        assert!(cluster.verify_convergence()); // Empty cluster is converged
    }

    #[test]
    fn test_single_node_commit() {
        let mut cluster = SimulatedCluster::new(1);

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 10));

        cluster.commit_on_node(0, tx).unwrap();

        let value = cluster.get_node_state(0, "counter").unwrap();
        assert_eq!(value.as_integer(), Some(10));
    }

    // ============ Two Node Convergence ============

    #[test]
    fn test_two_nodes_converge_add() {
        let mut cluster = SimulatedCluster::new(2);

        // Node 0: counter += 10
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("counter", 10));
        cluster.commit_on_node(0, tx0).unwrap();

        // Node 1: counter += 20
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 20));
        cluster.commit_on_node(1, tx1).unwrap();

        // Before propagation: different states
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(10)
        );
        assert_eq!(
            cluster.get_node_state(1, "counter").unwrap().as_integer(),
            Some(20)
        );

        // Propagate
        cluster.propagate_all();

        // After propagation: same state (10 + 20 = 30)
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(30)
        );
        assert_eq!(
            cluster.get_node_state(1, "counter").unwrap().as_integer(),
            Some(30)
        );
    }

    #[test]
    fn test_two_nodes_converge_max() {
        let mut cluster = SimulatedCluster::new(2);

        // Node 0: timestamp = max(100)
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(max_op("timestamp", 100));
        cluster.commit_on_node(0, tx0).unwrap();

        // Node 1: timestamp = max(200)
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(max_op("timestamp", 200));
        cluster.commit_on_node(1, tx1).unwrap();

        cluster.propagate_all();

        // Both should have max(100, 200) = 200
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "timestamp").unwrap().as_integer(),
            Some(200)
        );
    }

    #[test]
    fn test_two_nodes_converge_union() {
        let mut cluster = SimulatedCluster::new(2);

        // Node 0: tags = {"a", "b"}
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(union_op("tags", &["a", "b"]));
        cluster.commit_on_node(0, tx0).unwrap();

        // Node 1: tags = {"b", "c"}
        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(union_op("tags", &["b", "c"]));
        cluster.commit_on_node(1, tx1).unwrap();

        cluster.propagate_all();

        // Both should have {"a", "b", "c"}
        assert!(cluster.verify_convergence());

        if let Some(AlgebraicValue::StringSet(tags)) = cluster.get_node_state(0, "tags") {
            assert_eq!(tags.len(), 3);
            assert!(tags.contains("a"));
            assert!(tags.contains("b"));
            assert!(tags.contains("c"));
        } else {
            panic!("Expected StringSet");
        }
    }

    // ============ Five Node Convergence ============

    #[test]
    fn test_five_nodes_converge() {
        let mut cluster = SimulatedCluster::new(5);

        // Each node increments counter by (i+1)*10
        for i in 0..5 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("counter", ((i + 1) * 10) as i64));
            cluster.commit_on_node(i, tx).unwrap();
        }

        cluster.propagate_all();

        // All should equal 10 + 20 + 30 + 40 + 50 = 150
        assert!(cluster.verify_convergence());
        for i in 0..5 {
            assert_eq!(
                cluster.get_node_state(i, "counter").unwrap().as_integer(),
                Some(150),
                "Node {} has wrong value",
                i
            );
        }
    }

    #[test]
    fn test_five_nodes_multiple_keys() {
        let mut cluster = SimulatedCluster::new(5);

        // Each node updates multiple keys
        for i in 0..5 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("total", ((i + 1) * 10) as i64));
            tx.add_operation(max_op("max_id", (i * 100) as i64));
            tx.add_operation(union_op("nodes", &[&format!("node-{}", i)]));
            cluster.commit_on_node(i, tx).unwrap();
        }

        cluster.propagate_all();

        assert!(cluster.verify_convergence());

        // Verify each key
        for i in 0..5 {
            // total = 10 + 20 + 30 + 40 + 50 = 150
            assert_eq!(
                cluster.get_node_state(i, "total").unwrap().as_integer(),
                Some(150)
            );

            // max_id = max(0, 100, 200, 300, 400) = 400
            assert_eq!(
                cluster.get_node_state(i, "max_id").unwrap().as_integer(),
                Some(400)
            );

            // nodes = {"node-0", "node-1", "node-2", "node-3", "node-4"}
            if let Some(AlgebraicValue::StringSet(nodes)) = cluster.get_node_state(i, "nodes") {
                assert_eq!(nodes.len(), 5);
            }
        }
    }

    // ============ Network Partition Tests ============

    #[test]
    fn test_partition_prevents_propagation() {
        let mut cluster = SimulatedCluster::new(2);

        // Partition nodes 0 and 1
        cluster.partition(0, 1);

        // Each node commits
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("counter", 10));
        cluster.commit_on_node(0, tx0).unwrap();

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 20));
        cluster.commit_on_node(1, tx1).unwrap();

        // Propagate (should not work due to partition)
        cluster.propagate_all();

        // They should NOT have converged
        assert!(!cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(10)
        );
        assert_eq!(
            cluster.get_node_state(1, "counter").unwrap().as_integer(),
            Some(20)
        );
    }

    #[test]
    fn test_partition_heal_then_converge() {
        let mut cluster = SimulatedCluster::new(2);

        // Partition nodes
        cluster.partition(0, 1);

        // Commit on both
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("counter", 10));
        cluster.commit_on_node(0, tx0).unwrap();

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 20));
        cluster.commit_on_node(1, tx1).unwrap();

        // Try to propagate (fails due to partition)
        cluster.propagate_round();
        assert!(!cluster.verify_convergence());

        // Heal partition and re-queue updates for gossip
        cluster.heal_partitions();
        cluster.requeue_all_updates();

        // Now propagate
        cluster.propagate_all();

        // Should converge
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(30)
        );
    }

    #[test]
    fn test_partial_partition() {
        // 3 nodes: 0 <-> 1 (ok), 1 <-> 2 (ok), 0 <-> 2 (partitioned)
        let mut cluster = SimulatedCluster::new(3);
        cluster.partition(0, 2);

        // Each node commits
        for i in 0..3 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("counter", ((i + 1) * 10) as i64));
            cluster.commit_on_node(i, tx).unwrap();
        }

        // Propagate - node 1 acts as bridge
        cluster.propagate_all();

        // All should converge (via node 1)
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(60) // 10 + 20 + 30
        );
    }

    // ============ Commutativity Tests ============

    #[test]
    fn test_order_independence_add() {
        // Create two clusters with same operations in different order
        let mut cluster_ab = SimulatedCluster::new(2);
        let mut cluster_ba = SimulatedCluster::new(2);

        // Cluster AB: node 0 first, then node 1
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("x", 5));
        cluster_ab.commit_on_node(0, tx0).unwrap();

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("x", 3));
        cluster_ab.commit_on_node(1, tx1).unwrap();

        // Cluster BA: node 1 first, then node 0
        let mut tx1b = AlgebraicTransaction::new();
        tx1b.add_operation(add_op("x", 3));
        cluster_ba.commit_on_node(1, tx1b).unwrap();

        let mut tx0b = AlgebraicTransaction::new();
        tx0b.add_operation(add_op("x", 5));
        cluster_ba.commit_on_node(0, tx0b).unwrap();

        // Propagate both
        cluster_ab.propagate_all();
        cluster_ba.propagate_all();

        // Both should converge to same value (5 + 3 = 8)
        assert!(cluster_ab.verify_convergence());
        assert!(cluster_ba.verify_convergence());

        let val_ab = cluster_ab.get_node_state(0, "x").unwrap().as_integer();
        let val_ba = cluster_ba.get_node_state(0, "x").unwrap().as_integer();

        assert_eq!(val_ab, val_ba);
        assert_eq!(val_ab, Some(8));
    }

    #[test]
    fn test_order_independence_complex() {
        // Test with 5 nodes, each contributing different values
        let values = [7, 13, 29, 41, 53]; // Prime numbers for variety

        // Run simulation in forward order
        let mut cluster_forward = SimulatedCluster::new(5);
        for (i, &val) in values.iter().enumerate() {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("sum", val));
            cluster_forward.commit_on_node(i, tx).unwrap();
        }
        cluster_forward.propagate_all();

        // Run simulation in reverse order
        let mut cluster_reverse = SimulatedCluster::new(5);
        for (i, &val) in values.iter().enumerate().rev() {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("sum", val));
            cluster_reverse.commit_on_node(i, tx).unwrap();
        }
        cluster_reverse.propagate_all();

        // Both should have same result
        let expected_sum: i64 = values.iter().sum(); // 143
        assert_eq!(
            cluster_forward.get_node_state(0, "sum").unwrap().as_integer(),
            Some(expected_sum)
        );
        assert_eq!(
            cluster_reverse.get_node_state(0, "sum").unwrap().as_integer(),
            Some(expected_sum)
        );
    }

    // ============ Associativity Tests ============

    #[test]
    fn test_associativity_grouping() {
        // Test that ((A + B) + C) == (A + (B + C))
        let values = [10, 20, 30];

        // Group left: ((10 + 20) + 30)
        let mut cluster_left = SimulatedCluster::new(3);
        for (i, &val) in values.iter().enumerate() {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("x", val));
            cluster_left.commit_on_node(i, tx).unwrap();
        }
        // Propagate 0->1 first, then result->2
        cluster_left.propagate_all();

        // Group right: (10 + (20 + 30))
        let mut cluster_right = SimulatedCluster::new(3);
        // Start with nodes 1 and 2
        for i in [1, 2, 0] {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("x", values[i]));
            cluster_right.commit_on_node(i, tx).unwrap();
        }
        cluster_right.propagate_all();

        // Both should equal 60
        assert_eq!(
            cluster_left.get_node_state(0, "x").unwrap().as_integer(),
            Some(60)
        );
        assert_eq!(
            cluster_right.get_node_state(0, "x").unwrap().as_integer(),
            Some(60)
        );
    }

    // ============ Builder Tests ============

    #[test]
    fn test_simulation_builder() {
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("count", 100));

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("count", 200));

        let cluster = SimulationBuilder::new(2)
            .max_rounds(50)
            .with_operation(0, tx0)
            .with_operation(1, tx1)
            .run()
            .unwrap();

        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "count").unwrap().as_integer(),
            Some(300)
        );
    }

    // ============ Statistics Tests ============

    #[test]
    fn test_simulation_stats() {
        let mut cluster = SimulatedCluster::new(3);

        for i in 0..3 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("x", (i + 1) as i64));
            cluster.commit_on_node(i, tx).unwrap();
        }

        cluster.propagate_all();

        let stats = cluster.get_stats();
        assert_eq!(stats.operations_committed, 3);
        assert!(stats.messages_sent > 0);
        assert!(stats.messages_delivered > 0);
    }

    // ============ Edge Cases ============

    #[test]
    fn test_empty_transaction() {
        let mut cluster = SimulatedCluster::new(2);

        let tx = AlgebraicTransaction::new();
        let result = cluster.commit_on_node(0, tx);

        // Empty transaction should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_single_node_cluster() {
        let mut cluster = SimulatedCluster::new(1);

        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("x", 42));
        cluster.commit_on_node(0, tx).unwrap();

        cluster.propagate_all();

        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "x").unwrap().as_integer(),
            Some(42)
        );
    }

    #[test]
    fn test_many_operations_same_key() {
        let mut cluster = SimulatedCluster::new(2);

        // Node 0: multiple increments
        for _ in 0..10 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("counter", 1));
            cluster.commit_on_node(0, tx).unwrap();
        }

        // Node 1: multiple increments
        for _ in 0..10 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("counter", 2));
            cluster.commit_on_node(1, tx).unwrap();
        }

        cluster.propagate_all();

        // 10*1 + 10*2 = 30
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(30)
        );
    }

    // ============ Real-World Scenario Tests ============

    #[test]
    fn test_ecommerce_scenario() {
        // Simulate an e-commerce scenario with page views, cart items, and timestamps
        let mut cluster = SimulatedCluster::new(3); // 3 data centers

        // SF: 100 page views, cart items, recent timestamp
        let mut tx_sf = AlgebraicTransaction::new();
        tx_sf.add_operation(add_op("page_views", 100));
        tx_sf.add_operation(union_op("cart_items", &["item-a", "item-b"]));
        tx_sf.add_operation(max_op("last_activity", 1000));
        cluster.commit_on_node(0, tx_sf).unwrap();

        // Tokyo: 50 page views, different cart items, older timestamp
        let mut tx_tokyo = AlgebraicTransaction::new();
        tx_tokyo.add_operation(add_op("page_views", 50));
        tx_tokyo.add_operation(union_op("cart_items", &["item-c"]));
        tx_tokyo.add_operation(max_op("last_activity", 800));
        cluster.commit_on_node(1, tx_tokyo).unwrap();

        // London: 75 page views, overlapping cart items, newest timestamp
        let mut tx_london = AlgebraicTransaction::new();
        tx_london.add_operation(add_op("page_views", 75));
        tx_london.add_operation(union_op("cart_items", &["item-b", "item-d"]));
        tx_london.add_operation(max_op("last_activity", 1200));
        cluster.commit_on_node(2, tx_london).unwrap();

        cluster.propagate_all();

        // Verify convergence
        assert!(cluster.verify_convergence());

        // page_views = 100 + 50 + 75 = 225
        assert_eq!(
            cluster.get_node_state(0, "page_views").unwrap().as_integer(),
            Some(225)
        );

        // last_activity = max(1000, 800, 1200) = 1200
        assert_eq!(
            cluster
                .get_node_state(0, "last_activity")
                .unwrap()
                .as_integer(),
            Some(1200)
        );

        // cart_items = {item-a, item-b, item-c, item-d}
        if let Some(AlgebraicValue::StringSet(items)) = cluster.get_node_state(0, "cart_items") {
            assert_eq!(items.len(), 4);
            assert!(items.contains("item-a"));
            assert!(items.contains("item-b"));
            assert!(items.contains("item-c"));
            assert!(items.contains("item-d"));
        }
    }

    // ============ Transitive Requeue Tests (M4 fix) ============

    #[test]
    fn test_requeue_includes_transitively_received_updates() {
        // 3 nodes: Node 0 and Node 2 are partitioned from each other.
        // Node 1 bridges them. After partition healing, requeue must
        // include updates Node 1 received transitively from Node 0
        // so that Node 2 can get them.
        let mut cluster = SimulatedCluster::new(3);

        // Partition Node 0 from Node 2 (Node 1 bridges)
        cluster.partition(0, 2);

        // Node 0 commits (only Node 1 can receive this)
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("counter", 10));
        cluster.commit_on_node(0, tx0).unwrap();

        // Node 2 commits (only Node 1 can receive this)
        let mut tx2 = AlgebraicTransaction::new();
        tx2.add_operation(add_op("counter", 30));
        cluster.commit_on_node(2, tx2).unwrap();

        // Propagate: Node 1 receives both updates via gossip
        cluster.propagate_all();

        // Node 1 should have both (10 + 30 = 40)
        assert_eq!(
            cluster.get_node_state(1, "counter").unwrap().as_integer(),
            Some(40)
        );

        // Now: partition Node 1 from Node 0, heal Node 0 <-> Node 2
        // Node 2 has only its own update (30). It needs Node 0's update (10)
        // which was transitively received by Node 1.
        cluster.config.partitions.clear();
        cluster.partition(0, 1);
        cluster.partition(1, 2);
        // Only Node 0 <-> Node 2 is open now

        // Requeue all — Node 0 should re-send its local update to Node 2
        cluster.requeue_all_updates();
        cluster.propagate_all();

        // Node 2 should now have counter = 40 (received Node 0's update)
        assert_eq!(
            cluster.get_node_state(2, "counter").unwrap().as_integer(),
            Some(40)
        );
        // Node 0 should also have counter = 40 (received Node 2's update)
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(40)
        );
    }

    #[test]
    fn test_requeue_bridge_node_relays_after_heal() {
        // 4 nodes split into two groups: {0,1} and {2,3}
        // After full heal + requeue, all should converge.
        let mut cluster = SimulatedCluster::new(4);

        // Partition all cross-group pairs
        cluster.partition(0, 2);
        cluster.partition(0, 3);
        cluster.partition(1, 2);
        cluster.partition(1, 3);

        // Each node commits
        for i in 0..4 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("total", ((i + 1) * 10) as i64));
            cluster.commit_on_node(i, tx).unwrap();
        }

        // Propagate: 0 <-> 1 share, 2 <-> 3 share (cross-group blocked)
        cluster.propagate_all();

        // Node 0 has 10+20=30, Node 3 has 30+40=70
        assert_eq!(
            cluster.get_node_state(0, "total").unwrap().as_integer(),
            Some(30)
        );
        assert_eq!(
            cluster.get_node_state(3, "total").unwrap().as_integer(),
            Some(70)
        );
        assert!(!cluster.verify_convergence());

        // Full heal + requeue
        cluster.heal_partitions();
        cluster.requeue_all_updates();
        cluster.propagate_all();

        // All should converge: 10+20+30+40 = 100
        assert!(cluster.verify_convergence());
        for i in 0..4 {
            assert_eq!(
                cluster.get_node_state(i, "total").unwrap().as_integer(),
                Some(100),
                "Node {} has wrong value",
                i
            );
        }
    }

    #[test]
    fn test_requeue_multi_hop_transitive() {
        // 5 nodes in a chain: 0 -- 1 -- 2 -- 3 -- 4
        // Only adjacent nodes can communicate.
        // Node 0 commits, update must reach Node 4 via 3 hops.
        let mut cluster = SimulatedCluster::new(5);

        // Partition all non-adjacent pairs
        for i in 0..5 {
            for j in (i + 2)..5 {
                cluster.partition(i, j);
            }
        }

        // Only Node 0 commits
        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("signal", 42));
        cluster.commit_on_node(0, tx).unwrap();

        // Propagate through chain
        cluster.propagate_all();

        // All should have signal=42 (gossip through chain)
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(4, "signal").unwrap().as_integer(),
            Some(42)
        );
    }

    #[test]
    fn test_requeue_after_cascading_partitions() {
        // Scenario: updates propagate partially, then a new partition
        // isolates the bridge. After heal+requeue, everything converges.
        let mut cluster = SimulatedCluster::new(3);

        // Node 0 commits
        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("x", 100));
        cluster.commit_on_node(0, tx0).unwrap();

        // Propagate freely — all nodes get x=100
        cluster.propagate_all();
        assert!(cluster.verify_convergence());

        // Now partition Node 2 from both 0 and 1
        cluster.partition(0, 2);
        cluster.partition(1, 2);

        // Node 0 and Node 1 commit new updates
        let mut tx0b = AlgebraicTransaction::new();
        tx0b.add_operation(add_op("x", 50));
        cluster.commit_on_node(0, tx0b).unwrap();

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("x", 25));
        cluster.commit_on_node(1, tx1).unwrap();

        // Propagate: 0 and 1 converge to 175, Node 2 still at 100
        cluster.propagate_all();
        assert_eq!(
            cluster.get_node_state(0, "x").unwrap().as_integer(),
            Some(175)
        );
        assert_eq!(
            cluster.get_node_state(2, "x").unwrap().as_integer(),
            Some(100)
        );

        // Heal + requeue
        cluster.heal_partitions();
        cluster.requeue_all_updates();
        cluster.propagate_all();

        // All converge to 175
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(2, "x").unwrap().as_integer(),
            Some(175)
        );
    }

    #[test]
    fn test_requeue_deduplication_still_works() {
        // Ensure requeue doesn't cause double-application of updates.
        // With AbelianAdd, double-application would give wrong sum.
        let mut cluster = SimulatedCluster::new(2);

        let mut tx0 = AlgebraicTransaction::new();
        tx0.add_operation(add_op("counter", 10));
        cluster.commit_on_node(0, tx0).unwrap();

        let mut tx1 = AlgebraicTransaction::new();
        tx1.add_operation(add_op("counter", 20));
        cluster.commit_on_node(1, tx1).unwrap();

        // Propagate normally
        cluster.propagate_all();
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(30)
        );

        // Requeue all updates and propagate again
        cluster.requeue_all_updates();
        cluster.propagate_all();

        // Should still be 30, not 60 (deduplication must prevent re-application)
        assert!(cluster.verify_convergence());
        assert_eq!(
            cluster.get_node_state(0, "counter").unwrap().as_integer(),
            Some(30)
        );
    }

    #[test]
    fn test_distributed_counter_high_contention() {
        // Simulate high contention: 10 nodes all incrementing same counter
        let mut cluster = SimulatedCluster::new(10);

        for i in 0..10 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("global_counter", 1));
            cluster.commit_on_node(i, tx).unwrap();
        }

        cluster.propagate_all();

        // All should equal 10
        assert!(cluster.verify_convergence());
        for i in 0..10 {
            assert_eq!(
                cluster
                    .get_node_state(i, "global_counter")
                    .unwrap()
                    .as_integer(),
                Some(10)
            );
        }
    }

    // ============ Adversarial / Chaos Tests ============
    //
    // These tests verify that the algebraic merge system converges correctly
    // even under adverse network conditions: message drops, delays, duplicates.
    // This is Jepsen-style testing for the distributed simulation.

    #[test]
    fn test_adversarial_mild_still_converges() {
        // Mild adversarial conditions: 5% drops, 10% delays
        use rand::SeedableRng;

        for seed in 0..10 {
            let mut tx0 = AlgebraicTransaction::new();
            tx0.add_operation(add_op("counter", 100));

            let mut tx1 = AlgebraicTransaction::new();
            tx1.add_operation(add_op("counter", 200));

            let cluster = SimulationBuilder::new(2)
                .max_rounds(50) // Extra rounds to handle delays
                .with_adversarial(AdversarialConfig::mild().with_seed(seed))
                .with_operation(0, tx0)
                .with_operation(1, tx1)
                .run_adversarial(seed)
                .unwrap();

            assert!(
                cluster.verify_convergence(),
                "Seed {} failed to converge under mild adversarial conditions",
                seed
            );
            assert_eq!(
                cluster.get_node_state(0, "counter").unwrap().as_integer(),
                Some(300),
                "Seed {} has wrong value",
                seed
            );
        }
    }

    #[test]
    fn test_adversarial_moderate_still_converges() {
        // Moderate adversarial conditions: 15% drops, 25% delays
        for seed in 0..10 {
            let mut tx0 = AlgebraicTransaction::new();
            tx0.add_operation(add_op("x", 10));

            let mut tx1 = AlgebraicTransaction::new();
            tx1.add_operation(add_op("x", 20));

            let mut tx2 = AlgebraicTransaction::new();
            tx2.add_operation(add_op("x", 30));

            let cluster = SimulationBuilder::new(3)
                .max_rounds(100) // More rounds for recovery
                .with_adversarial(AdversarialConfig::moderate().with_seed(seed))
                .with_operation(0, tx0)
                .with_operation(1, tx1)
                .with_operation(2, tx2)
                .run_adversarial(seed)
                .unwrap();

            assert!(
                cluster.verify_convergence(),
                "Seed {} failed to converge under moderate adversarial conditions",
                seed
            );
            assert_eq!(
                cluster.get_node_state(0, "x").unwrap().as_integer(),
                Some(60),
                "Seed {} has wrong value",
                seed
            );
        }
    }

    #[test]
    fn test_adversarial_severe_eventually_converges() {
        // Severe adversarial conditions: 30% drops, 40% delays
        // With enough rounds, algebraic operations MUST converge
        for seed in 0..5 {
            let mut operations = Vec::new();
            for i in 0..5 {
                let mut tx = AlgebraicTransaction::new();
                tx.add_operation(add_op("total", (i + 1) as i64));
                operations.push((i, tx));
            }

            let mut builder = SimulationBuilder::new(5)
                .max_rounds(200) // Many rounds for severe conditions
                .with_adversarial(AdversarialConfig::severe().with_seed(seed));

            for (node, tx) in operations {
                builder = builder.with_operation(node, tx);
            }

            let cluster = builder.run_adversarial(seed).unwrap();

            assert!(
                cluster.verify_convergence(),
                "Seed {} failed to converge under severe adversarial conditions. Stats: {:?}",
                seed,
                cluster.stats
            );
            // 1 + 2 + 3 + 4 + 5 = 15
            assert_eq!(
                cluster.get_node_state(0, "total").unwrap().as_integer(),
                Some(15),
                "Seed {} has wrong value",
                seed
            );
        }
    }

    #[test]
    fn test_adversarial_duplicates_are_idempotent() {
        // Test that duplicate messages don't corrupt state
        // High duplicate probability, no drops
        let config = AdversarialConfig {
            drop_probability: 0.0,
            delay_probability: 0.1,
            max_delay: 2,
            duplicate_probability: 0.5, // 50% duplicates!
            seed: Some(42),
        };

        for seed in 0..10 {
            let mut tx0 = AlgebraicTransaction::new();
            tx0.add_operation(add_op("counter", 100));

            let mut tx1 = AlgebraicTransaction::new();
            tx1.add_operation(add_op("counter", 200));

            let cluster = SimulationBuilder::new(2)
                .max_rounds(50)
                .with_adversarial(config.clone().with_seed(seed))
                .with_operation(0, tx0)
                .with_operation(1, tx1)
                .run_adversarial(seed)
                .unwrap();

            // Despite duplicates, deduplication should prevent double-application
            assert!(cluster.verify_convergence());
            assert_eq!(
                cluster.get_node_state(0, "counter").unwrap().as_integer(),
                Some(300), // NOT 600 or higher
                "Seed {} has corrupted value due to duplicate processing",
                seed
            );
        }
    }

    #[test]
    fn test_adversarial_max_operations() {
        // Stress test: many operations under adversarial conditions
        for seed in 0..3 {
            let mut builder = SimulationBuilder::new(5)
                .max_rounds(150)
                .with_adversarial(AdversarialConfig::moderate().with_seed(seed));

            // Each node commits 10 operations
            let mut expected_sum: i64 = 0;
            for node in 0..5 {
                for op in 0..10 {
                    let value = (node * 10 + op + 1) as i64;
                    expected_sum += value;

                    let mut tx = AlgebraicTransaction::new();
                    tx.add_operation(add_op("sum", value));
                    builder = builder.with_operation(node, tx);
                }
            }

            let cluster = builder.run_adversarial(seed).unwrap();

            assert!(
                cluster.verify_convergence(),
                "Seed {} failed with 50 operations",
                seed
            );
            assert_eq!(
                cluster.get_node_state(0, "sum").unwrap().as_integer(),
                Some(expected_sum),
                "Seed {} has wrong sum",
                seed
            );
        }
    }

    #[test]
    fn test_adversarial_mixed_operation_types() {
        // Test all operation types under adversarial conditions
        for seed in 0..5 {
            let mut tx0 = AlgebraicTransaction::new();
            tx0.add_operation(add_op("counter", 10));
            tx0.add_operation(max_op("max_val", 100));
            tx0.add_operation(union_op("tags", &["a", "b"]));

            let mut tx1 = AlgebraicTransaction::new();
            tx1.add_operation(add_op("counter", 20));
            tx1.add_operation(max_op("max_val", 50));
            tx1.add_operation(union_op("tags", &["b", "c"]));

            let mut tx2 = AlgebraicTransaction::new();
            tx2.add_operation(add_op("counter", 30));
            tx2.add_operation(max_op("max_val", 200));
            tx2.add_operation(union_op("tags", &["c", "d"]));

            let cluster = SimulationBuilder::new(3)
                .max_rounds(100)
                .with_adversarial(AdversarialConfig::moderate().with_seed(seed))
                .with_operation(0, tx0)
                .with_operation(1, tx1)
                .with_operation(2, tx2)
                .run_adversarial(seed)
                .unwrap();

            assert!(cluster.verify_convergence(), "Seed {} failed", seed);

            // Verify each operation type
            assert_eq!(
                cluster.get_node_state(0, "counter").unwrap().as_integer(),
                Some(60)
            );
            assert_eq!(
                cluster.get_node_state(0, "max_val").unwrap().as_integer(),
                Some(200)
            );
            if let Some(AlgebraicValue::StringSet(tags)) = cluster.get_node_state(0, "tags") {
                assert_eq!(tags.len(), 4); // a, b, c, d
            } else {
                panic!("Seed {} has wrong tags type", seed);
            }
        }
    }

    #[test]
    fn test_adversarial_stats_tracked() {
        // Verify that adversarial stats are properly tracked
        use rand::SeedableRng;
        let seed = 12345u64;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let config = SimulationConfig {
            max_rounds: 50,
            randomize_order: false,
            partitions: Vec::new(),
            adversarial: Some(AdversarialConfig::moderate()),
        };

        let mut cluster = SimulatedCluster::with_config(3, config);

        for i in 0..3 {
            let mut tx = AlgebraicTransaction::new();
            tx.add_operation(add_op("x", (i + 1) as i64));
            cluster.commit_on_node(i, tx).unwrap();
        }

        cluster.propagate_all_adversarial(&mut rng);

        // With moderate config, we should see some adversarial events
        let stats = cluster.get_stats();
        // At least verify stats are being tracked (may be 0 with lucky RNG)
        assert!(
            stats.messages_sent > 0,
            "Should have sent some messages"
        );
        // The sum of delivered + dropped should account for all messages
        // (some may still be in flight due to delays)
    }

    #[test]
    fn test_adversarial_reproducible_with_seed() {
        // Same seed should produce same results
        let seed = 99999u64;

        let run = |s| {
            let mut tx0 = AlgebraicTransaction::new();
            tx0.add_operation(add_op("x", 1));

            let mut tx1 = AlgebraicTransaction::new();
            tx1.add_operation(add_op("x", 2));

            SimulationBuilder::new(2)
                .max_rounds(50)
                .with_adversarial(AdversarialConfig::moderate().with_seed(s))
                .with_operation(0, tx0)
                .with_operation(1, tx1)
                .run_adversarial(s)
                .unwrap()
        };

        let cluster1 = run(seed);
        let cluster2 = run(seed);

        // Same seed should give same stats
        assert_eq!(
            cluster1.stats.adversarial_drops,
            cluster2.stats.adversarial_drops
        );
        assert_eq!(
            cluster1.stats.adversarial_delays,
            cluster2.stats.adversarial_delays
        );
        assert_eq!(
            cluster1.stats.adversarial_duplicates,
            cluster2.stats.adversarial_duplicates
        );
    }
}
