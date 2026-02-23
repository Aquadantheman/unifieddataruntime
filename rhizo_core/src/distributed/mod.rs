//! Distributed systems primitives for coordination-free transactions.
//!
//! This module provides the building blocks for distributed Rhizo deployments
//! where algebraic operations can commit locally without coordination.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Distributed Module                      │
//! ├─────────────────────────────────────────────────────────────┤
//! │  VectorClock     - Causality tracking                       │
//! │  LocalCommit     - Coordination-free commit protocol        │
//! │  Simulation      - Multi-node convergence testing           │
//! │  (Future) Gossip        - Anti-entropy propagation          │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Theory
//!
//! Traditional distributed databases require consensus (Paxos/Raft) for
//! strong consistency. However, for algebraic operations (commutative +
//! associative), we can achieve strong eventual consistency without
//! coordination:
//!
//! - **Semilattice operations** (MAX, MIN, UNION): Idempotent merge
//! - **Abelian operations** (ADD, MULTIPLY): Combine deltas
//!
//! Vector clocks track causality to determine when merge is needed.
//!
//! # Example: Vector Clocks
//!
//! ```
//! use rhizo_core::distributed::{VectorClock, NodeId, CausalOrder};
//! use rhizo_core::algebraic::{OpType, AlgebraicMerger, AlgebraicValue};
//!
//! let node_a = NodeId::new("sf");
//! let node_b = NodeId::new("tokyo");
//!
//! // Node A increments counter
//! let mut clock_a = VectorClock::new();
//! clock_a.tick(&node_a);
//! let delta_a = AlgebraicValue::integer(5);
//!
//! // Node B increments counter (concurrent)
//! let mut clock_b = VectorClock::new();
//! clock_b.tick(&node_b);
//! let delta_b = AlgebraicValue::integer(3);
//!
//! // Detect concurrent updates
//! assert_eq!(clock_a.compare(&clock_b), CausalOrder::Concurrent);
//!
//! // Merge using algebraic properties (order doesn't matter!)
//! let merged = AlgebraicMerger::merge(
//!     OpType::AbelianAdd,
//!     &delta_a,
//!     &delta_b
//! );
//!
//! // Result: 5 + 3 = 8
//! if let rhizo_core::algebraic::MergeResult::Merged(v) = merged {
//!     assert_eq!(v, AlgebraicValue::integer(8));
//! }
//! ```
//!
//! # Example: Coordination-Free Local Commit
//!
//! ```
//! use rhizo_core::distributed::{
//!     AlgebraicOperation, AlgebraicTransaction, LocalCommitProtocol,
//!     NodeId, VectorClock,
//! };
//! use rhizo_core::algebraic::{OpType, AlgebraicValue};
//!
//! // Two nodes perform concurrent operations
//! let node_sf = NodeId::new("san-francisco");
//! let node_tokyo = NodeId::new("tokyo");
//! let mut clock_sf = VectorClock::new();
//! let mut clock_tokyo = VectorClock::new();
//!
//! // SF increments page_views by 100
//! let mut tx_sf = AlgebraicTransaction::new();
//! tx_sf.add_operation(AlgebraicOperation::new(
//!     "page_views",
//!     OpType::AbelianAdd,
//!     AlgebraicValue::integer(100),
//! ));
//!
//! // Tokyo increments page_views by 50 (concurrently!)
//! let mut tx_tokyo = AlgebraicTransaction::new();
//! tx_tokyo.add_operation(AlgebraicOperation::new(
//!     "page_views",
//!     OpType::AbelianAdd,
//!     AlgebraicValue::integer(50),
//! ));
//!
//! // Both can commit locally without coordination
//! let update_sf = LocalCommitProtocol::commit_local(&tx_sf, &node_sf, &mut clock_sf).unwrap();
//! let update_tokyo = LocalCommitProtocol::commit_local(&tx_tokyo, &node_tokyo, &mut clock_tokyo).unwrap();
//!
//! // Later, merge the concurrent updates
//! let merged = LocalCommitProtocol::merge_updates(&update_sf, &update_tokyo).unwrap();
//!
//! // Result: 100 + 50 = 150 (order doesn't matter!)
//! let page_views = merged.operations()[0].value().as_integer();
//! assert_eq!(page_views, Some(150));
//! ```

mod local_commit;
pub mod network;
pub mod simulation;
mod vector_clock;

pub use local_commit::{
    AlgebraicOperation, AlgebraicTransaction, LocalCommitError, LocalCommitProtocol,
    VersionedUpdate,
};
pub use network::{NetworkConfig, NetworkError, NetworkMessage, NetworkNode, NetworkStats};
pub use simulation::{
    Message, NetworkCondition, SimulatedCluster, SimulatedNode, SimulationBuilder,
    SimulationConfig, SimulationStats,
};
pub use vector_clock::{CausalOrder, NodeId, VectorClock};
