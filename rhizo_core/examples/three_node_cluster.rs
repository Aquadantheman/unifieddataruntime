//! Three-node TCP cluster demonstration.
//!
//! This example starts 3 Rhizo nodes on localhost, each performing
//! concurrent operations, and verifies that all nodes converge to
//! the same state through the coordination-free gossip protocol.
//!
//! Run with:
//!     cargo run --example three_node_cluster
//!
//! Expected output:
//!     - All 3 nodes start and connect
//!     - Each node commits local operations
//!     - After gossip, all nodes have the same state
//!     - Final counter value = 10 + 20 + 30 = 60

use rhizo_core::algebraic::{AlgebraicValue, OpType};
use rhizo_core::distributed::{
    AlgebraicOperation, AlgebraicTransaction, NetworkConfig, NetworkNode,
};
use std::time::Duration;

fn add_op(key: &str, value: i64) -> AlgebraicOperation {
    AlgebraicOperation::new(key, OpType::AbelianAdd, AlgebraicValue::integer(value))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for debug output
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== Rhizo 3-Node TCP Cluster Demo ===\n");

    // Start 3 nodes
    println!("Starting nodes...");

    let node1 = NetworkNode::start(NetworkConfig::localhost_cluster(0)).await?;
    println!("  Node 1 started on port 9001");

    let node2 = NetworkNode::start(NetworkConfig::localhost_cluster(1)).await?;
    println!("  Node 2 started on port 9002");

    let node3 = NetworkNode::start(NetworkConfig::localhost_cluster(2)).await?;
    println!("  Node 3 started on port 9003");

    // Wait for connections to establish
    println!("\nWaiting for peer connections...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Each node commits a local operation
    println!("\nCommitting operations on each node:");

    let mut tx1 = AlgebraicTransaction::new();
    tx1.add_operation(add_op("counter", 10));
    node1.commit_and_broadcast(tx1).await?;
    println!("  Node 1: counter += 10");

    let mut tx2 = AlgebraicTransaction::new();
    tx2.add_operation(add_op("counter", 20));
    node2.commit_and_broadcast(tx2).await?;
    println!("  Node 2: counter += 20");

    let mut tx3 = AlgebraicTransaction::new();
    tx3.add_operation(add_op("counter", 30));
    node3.commit_and_broadcast(tx3).await?;
    println!("  Node 3: counter += 30");

    // Wait for gossip to propagate
    println!("\nWaiting for gossip propagation...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check convergence
    println!("\nChecking convergence:");
    let v1 = node1.get_state("counter");
    let v2 = node2.get_state("counter");
    let v3 = node3.get_state("counter");

    println!("  Node 1 counter: {:?}", v1.as_ref().map(|v| v.as_integer()));
    println!("  Node 2 counter: {:?}", v2.as_ref().map(|v| v.as_integer()));
    println!("  Node 3 counter: {:?}", v3.as_ref().map(|v| v.as_integer()));

    // Verify convergence
    let expected = Some(60); // 10 + 20 + 30
    let converged = v1.as_ref().and_then(|v| v.as_integer()) == expected
        && v2.as_ref().and_then(|v| v.as_integer()) == expected
        && v3.as_ref().and_then(|v| v.as_integer()) == expected;

    println!("\n=== Result ===");
    if converged {
        println!("SUCCESS: All nodes converged to counter = 60");
    } else {
        println!("PENDING: Nodes have not yet converged (may need more time)");
        println!("  Expected: 60 (10 + 20 + 30)");
    }

    // Print stats
    println!("\n=== Network Statistics ===");
    let stats1 = node1.stats().await;
    let stats2 = node2.stats().await;
    let stats3 = node3.stats().await;
    println!("  Node 1: sent={}, received={}, updates_in={}",
             stats1.messages_sent, stats1.messages_received, stats1.updates_received);
    println!("  Node 2: sent={}, received={}, updates_in={}",
             stats2.messages_sent, stats2.messages_received, stats2.updates_received);
    println!("  Node 3: sent={}, received={}, updates_in={}",
             stats3.messages_sent, stats3.messages_received, stats3.updates_received);

    // Shutdown
    println!("\nShutting down nodes...");
    node1.shutdown().await;
    node2.shutdown().await;
    node3.shutdown().await;

    println!("Done.");
    Ok(())
}
