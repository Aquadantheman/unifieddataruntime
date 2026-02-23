//! TCP network layer for real distributed deployment.
//!
//! This module provides TCP-based networking for multi-node Rhizo clusters.
//! It enables real network deployment of the coordination-free protocol that
//! was previously only available in simulation.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        NetworkNode                               │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  CoordinationFreeManager  │  TCP Listener  │  Peer Connections  │
//! │  (local state + escrow)   │  (incoming)    │  (outgoing)        │
//! └─────────────────────────────────────────────────────────────────┘
//!                                    ↕
//!                           NetworkMessage (JSON)
//!                                    ↕
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Other Nodes                               │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use rhizo_core::distributed::network::{NetworkNode, NetworkConfig};
//! use rhizo_core::distributed::NodeId;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = NetworkConfig {
//!         node_id: NodeId::new("node-1"),
//!         listen_addr: "127.0.0.1:9001".parse().unwrap(),
//!         peers: vec![
//!             "127.0.0.1:9002".parse().unwrap(),
//!             "127.0.0.1:9003".parse().unwrap(),
//!         ],
//!     };
//!
//!     let node = NetworkNode::new(config).await.unwrap();
//!
//!     // Commit locally and broadcast to peers
//!     let mut tx = AlgebraicTransaction::new();
//!     tx.add_operation(add_op("counter", 10));
//!     node.commit_and_broadcast(tx).await.unwrap();
//! }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock};

use super::local_commit::{AlgebraicTransaction, VersionedUpdate};
use super::vector_clock::NodeId;
use crate::transaction::CoordinationFreeManager;

/// Network protocol messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkMessage {
    /// Announce this node's identity
    Hello {
        node_id: NodeId,
    },

    /// A versioned update to propagate
    Update(VersionedUpdate),

    /// Acknowledge receipt of an update
    Ack {
        update_id: String,
    },

    /// Request full state sync (used on reconnection)
    SyncRequest,

    /// Response to sync request with all updates
    SyncResponse {
        updates: Vec<VersionedUpdate>,
    },

    /// Heartbeat to keep connection alive
    Ping,

    /// Heartbeat response
    Pong,
}

/// Configuration for a network node.
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// This node's unique identifier
    pub node_id: NodeId,

    /// Address to listen on for incoming connections
    pub listen_addr: SocketAddr,

    /// Addresses of peer nodes to connect to
    pub peers: Vec<SocketAddr>,

    /// Connection timeout in milliseconds
    pub connect_timeout_ms: u64,

    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
}

impl NetworkConfig {
    /// Create a config for localhost testing with 3 nodes.
    pub fn localhost_cluster(node_index: usize) -> Self {
        let ports = [9001, 9002, 9003];
        let my_port = ports[node_index];
        let peer_ports: Vec<_> = ports.iter().filter(|&&p| p != my_port).collect();

        Self {
            node_id: NodeId::new(format!("node-{}", node_index + 1)),
            listen_addr: format!("127.0.0.1:{}", my_port).parse().unwrap(),
            peers: peer_ports
                .iter()
                .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
                .collect(),
            connect_timeout_ms: 5000,
            heartbeat_interval_ms: 10000,
        }
    }
}

/// Error type for network operations.
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Node not ready")]
    NotReady,

    #[error("Coordination error: {0}")]
    Coordination(String),

    #[error("Channel error")]
    ChannelError,
}

/// Statistics for network operations.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total messages sent
    pub messages_sent: u64,

    /// Total messages received
    pub messages_received: u64,

    /// Total updates propagated
    pub updates_propagated: u64,

    /// Total updates received from peers
    pub updates_received: u64,

    /// Number of connected peers
    pub connected_peers: usize,
}

/// A network-connected Rhizo node.
///
/// Wraps `CoordinationFreeManager` with TCP networking for real distributed deployment.
pub struct NetworkNode {
    /// Configuration
    config: NetworkConfig,

    /// The underlying coordination-free manager
    manager: Arc<CoordinationFreeManager>,

    /// Sender for broadcasting updates to peers
    update_tx: broadcast::Sender<VersionedUpdate>,

    /// Network statistics
    stats: Arc<RwLock<NetworkStats>>,

    /// Shutdown signal
    shutdown_tx: mpsc::Sender<()>,
}

impl NetworkNode {
    /// Create and start a new network node.
    pub async fn start(config: NetworkConfig) -> Result<Self, NetworkError> {
        let manager = Arc::new(CoordinationFreeManager::new(config.node_id.clone()));
        let (update_tx, _) = broadcast::channel(1000);
        let stats = Arc::new(RwLock::new(NetworkStats::default()));
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        // Start TCP listener
        let listener = TcpListener::bind(&config.listen_addr).await?;
        tracing::info!("Node {} listening on {}", config.node_id.as_str(), config.listen_addr);

        // Spawn listener task
        let listener_manager = Arc::clone(&manager);
        let listener_stats = Arc::clone(&stats);
        let listener_update_tx = update_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                tracing::info!("Accepted connection from {}", addr);
                                let mgr = Arc::clone(&listener_manager);
                                let stats = Arc::clone(&listener_stats);
                                let tx = listener_update_tx.clone();
                                tokio::spawn(handle_connection(stream, mgr, stats, tx));
                            }
                            Err(e) => {
                                tracing::error!("Accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Listener shutting down");
                        break;
                    }
                }
            }
        });

        // Connect to peers
        let node = Self {
            config,
            manager,
            update_tx,
            stats,
            shutdown_tx,
        };

        node.connect_to_peers().await;

        Ok(node)
    }

    /// Connect to all configured peers.
    async fn connect_to_peers(&self) {
        for peer_addr in &self.config.peers {
            let peer_addr = *peer_addr;
            let node_id = self.config.node_id.clone();
            let manager = Arc::clone(&self.manager);
            let stats = Arc::clone(&self.stats);
            let mut update_rx = self.update_tx.subscribe();

            tokio::spawn(async move {
                loop {
                    match TcpStream::connect(&peer_addr).await {
                        Ok(stream) => {
                            tracing::info!("Connected to peer {}", peer_addr);
                            if let Err(e) = handle_peer_connection(
                                stream,
                                node_id.clone(),
                                Arc::clone(&manager),
                                Arc::clone(&stats),
                                &mut update_rx,
                            )
                            .await
                            {
                                tracing::warn!("Peer connection error: {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to connect to {}: {}", peer_addr, e);
                        }
                    }
                    // Reconnect after delay
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            });
        }
    }

    /// Get this node's ID.
    pub fn node_id(&self) -> &NodeId {
        &self.config.node_id
    }

    /// Get the underlying manager.
    pub fn manager(&self) -> &CoordinationFreeManager {
        &self.manager
    }

    /// Commit a transaction locally and broadcast to peers.
    pub async fn commit_and_broadcast(
        &self,
        tx: AlgebraicTransaction,
    ) -> Result<VersionedUpdate, NetworkError> {
        // Commit locally
        let update = self
            .manager
            .commit_local(&tx)
            .map_err(|e| NetworkError::Coordination(e.to_string()))?;

        // Broadcast to peers
        let _ = self.update_tx.send(update.clone());

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.updates_propagated += 1;
        }

        Ok(update)
    }

    /// Get current network statistics.
    pub async fn stats(&self) -> NetworkStats {
        self.stats.read().await.clone()
    }

    /// Get the current value for a key.
    pub fn get_state(&self, key: &str) -> Option<crate::algebraic::AlgebraicValue> {
        self.manager.get_state(key).ok().flatten()
    }

    /// Shutdown the node.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(()).await;
    }
}

/// Handle an incoming connection.
async fn handle_connection(
    stream: TcpStream,
    manager: Arc<CoordinationFreeManager>,
    stats: Arc<RwLock<NetworkStats>>,
    update_tx: broadcast::Sender<VersionedUpdate>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => break, // Connection closed
            Ok(_) => {
                match serde_json::from_str::<NetworkMessage>(&line) {
                    Ok(msg) => {
                        // Update stats
                        {
                            let mut s = stats.write().await;
                            s.messages_received += 1;
                        }

                        match msg {
                            NetworkMessage::Hello { node_id } => {
                                tracing::debug!("Received hello from {}", node_id.as_str());
                            }
                            NetworkMessage::Update(update) => {
                                tracing::debug!(
                                    "Received update from {}",
                                    update.origin_node().as_str()
                                );
                                // Apply update to local state
                                if let Err(e) = manager.receive_update(&update) {
                                    tracing::warn!("Failed to apply update: {}", e);
                                } else {
                                    let mut s = stats.write().await;
                                    s.updates_received += 1;
                                }
                                // Forward to other connections
                                let _ = update_tx.send(update);
                            }
                            NetworkMessage::Ping => {
                                let pong = NetworkMessage::Pong;
                                if let Ok(json) = serde_json::to_string(&pong) {
                                    let _ = writer.write_all(json.as_bytes()).await;
                                    let _ = writer.write_all(b"\n").await;
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Failed to parse message: {}", e);
                    }
                }
            }
            Err(e) => {
                tracing::debug!("Read error: {}", e);
                break;
            }
        }
    }
}

/// Handle outgoing connection to a peer.
async fn handle_peer_connection(
    stream: TcpStream,
    node_id: NodeId,
    manager: Arc<CoordinationFreeManager>,
    stats: Arc<RwLock<NetworkStats>>,
    update_rx: &mut broadcast::Receiver<VersionedUpdate>,
) -> Result<(), NetworkError> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    // Send hello
    let hello = NetworkMessage::Hello {
        node_id: node_id.clone(),
    };
    let json = serde_json::to_string(&hello)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;

    {
        let mut s = stats.write().await;
        s.messages_sent += 1;
        s.connected_peers += 1;
    }

    let mut line = String::new();

    loop {
        tokio::select! {
            // Receive from peer
            result = reader.read_line(&mut line) => {
                match result {
                    Ok(0) => break, // Connection closed
                    Ok(_) => {
                        if let Ok(msg) = serde_json::from_str::<NetworkMessage>(&line) {
                            let mut s = stats.write().await;
                            s.messages_received += 1;
                            drop(s);

                            if let NetworkMessage::Update(update) = msg {
                                // Only apply if not from ourselves
                                if update.origin_node() != &node_id {
                                    if let Err(e) = manager.receive_update(&update) {
                                        tracing::warn!("Failed to apply update: {}", e);
                                    } else {
                                        let mut s = stats.write().await;
                                        s.updates_received += 1;
                                    }
                                }
                            }
                        }
                        line.clear();
                    }
                    Err(e) => {
                        return Err(NetworkError::Io(e));
                    }
                }
            }
            // Send updates to peer
            result = update_rx.recv() => {
                match result {
                    Ok(update) => {
                        // Don't send updates back to their origin
                        if update.origin_node() != &node_id {
                            continue;
                        }
                        let msg = NetworkMessage::Update(update);
                        let json = serde_json::to_string(&msg)?;
                        writer.write_all(json.as_bytes()).await?;
                        writer.write_all(b"\n").await?;

                        let mut s = stats.write().await;
                        s.messages_sent += 1;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    {
        let mut s = stats.write().await;
        s.connected_peers = s.connected_peers.saturating_sub(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebraic::{AlgebraicValue, OpType};
    use crate::distributed::AlgebraicOperation;

    fn add_op(key: &str, value: i64) -> AlgebraicOperation {
        AlgebraicOperation::new(key, OpType::AbelianAdd, AlgebraicValue::integer(value))
    }

    #[tokio::test]
    async fn test_network_config_localhost() {
        let config = NetworkConfig::localhost_cluster(0);
        assert_eq!(config.node_id.as_str(), "node-1");
        assert_eq!(config.listen_addr.port(), 9001);
        assert_eq!(config.peers.len(), 2);
    }

    #[tokio::test]
    async fn test_network_message_serialization() {
        let update = VersionedUpdate::new(
            vec![add_op("counter", 10)],
            crate::distributed::VectorClock::new(),
            NodeId::new("test"),
        );

        let msg = NetworkMessage::Update(update);
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: NetworkMessage = serde_json::from_str(&json).unwrap();

        match parsed {
            NetworkMessage::Update(u) => {
                assert_eq!(u.origin_node().as_str(), "test");
            }
            _ => panic!("Expected Update message"),
        }
    }

    #[tokio::test]
    async fn test_single_node_start() {
        let config = NetworkConfig {
            node_id: NodeId::new("test-node"),
            listen_addr: "127.0.0.1:0".parse().unwrap(), // Random port
            peers: vec![],
            connect_timeout_ms: 1000,
            heartbeat_interval_ms: 10000,
        };

        let node = NetworkNode::start(config).await.unwrap();
        assert_eq!(node.node_id().as_str(), "test-node");

        // Commit a transaction
        let mut tx = AlgebraicTransaction::new();
        tx.add_operation(add_op("counter", 42));
        let update = node.commit_and_broadcast(tx).await.unwrap();

        assert_eq!(update.operations().len(), 1);

        // Check state
        let value = node.get_state("counter").unwrap();
        assert_eq!(value.as_integer(), Some(42));

        node.shutdown().await;
    }
}
