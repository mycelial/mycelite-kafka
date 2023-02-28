//! Kafka to mycelite bridge.
//! Consume kafka topics into SQLite with mycelite extension enabled.

mod bridge;
mod topic_poller;

pub use bridge::{KafkaMyceliteBridge, KafkaMyceliteBridgeHandle};
pub use topic_poller::{TopicPoller, TopicPollerHandle};
