//! Transaction module for cross-table ACID transactions.
//!
//! This module provides:
//! - `TransactionManager` - Coordinates transactions with conflict detection
//! - `TransactionRecord` - Complete transaction state and metadata
//! - `TransactionLog` - Persistent storage for transaction records
//! - `EpochConfig` / `EpochMetadata` - Epoch-based organization
//! - `ConflictDetector` - Pluggable conflict detection strategies
//! - `CoordinationFreeManager` - Coordination-free mode for algebraic operations
//! - `SpeculativeBuffer` - Speculative execution with conflict probability tracking
//! - `EscrowManager` - Escrow transactions for linear horizontal scaling on hot spots

mod types;
mod epoch;
mod error;
mod log;
mod conflict;
mod manager;
mod recovery;
mod coordination_free;
mod speculative;
mod escrow;

pub use types::{
    TxId, EpochId, TransactionStatus, WriteGranularity,
    TableWrite, TransactionRecord, TransactionMode,
};
pub use epoch::{EpochConfig, EpochStatus, EpochMetadata};
pub use error::TransactionError;
pub use log::TransactionLog;
pub use conflict::{
    Conflict, ConflictDetector, TableLevelConflictDetector,
    PartitionLevelConflictDetector, BloomFilter, BloomWriteSet,
    BloomFilterConflictDetector, RowLevelConflictDetector, AdaptiveConflictDetector,
};
pub use manager::TransactionManager;
pub use recovery::{RecoveryReport, RecoveryManager};
pub use coordination_free::{
    CoordinationFreeConfig, CoordinationFreeError, CoordinationFreeManager,
};
pub use speculative::{
    SpeculativeStatus, TentativeCommit, ConflictProbabilityTracker,
    SpeculativeConfig, SpeculativeBuffer, SpeculativeCommitResult, SpeculativeMetrics,
};
pub use escrow::{
    EscrowResult, EscrowError, EscrowConfig, EscrowStats, EscrowManager, EscrowAggregateStats,
};
