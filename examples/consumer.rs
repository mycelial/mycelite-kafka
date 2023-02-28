#![allow(dead_code)]
#![allow(unused)]

use anyhow::Result;
use clap::Parser;
use mycelite_kafka::{TopicPoller, KafkaMyceliteBridge};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Parser)]
struct Config {
    #[clap(short, long, default_value = "localhost:9092")]
    brokers: String,
    #[clap(short, long, default_value = "db.sqlite3")]
    database: String,
    #[clap(short, long, default_value = "target/debug/libmycelite")]
    extension_path: String,
    #[clap(short, long, default_value = "mycelite_bridge")]
    group_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    env_logger::init();
    let bridge_handle = KafkaMyceliteBridge::try_new(&cfg.brokers, &cfg.group_id, &cfg.database, &cfg.extension_path, &[]).await?.spawn();
    let topic_handle = TopicPoller::new(&cfg.brokers, bridge_handle.clone()).spawn();
    bridge_handle.wait().await;
    topic_handle.wait().await;
    Ok(())
}
