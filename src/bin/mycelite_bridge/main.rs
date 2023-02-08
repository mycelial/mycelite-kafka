#![allow(dead_code)]
#![allow(unused)]

use anyhow::Result;
use clap::Parser;
use mycelite_kafka::{MyceliteBridge, MyceliteBridgeHandle};
use rdkafka::client::Client;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Parser)]
struct Config {
    #[clap(short, long, default_value = "localhost:9092")]
    brokers: String,
    #[clap(short, long, default_value = "db.sqlite3")]
    database: String,
    #[clap(short, long, default_value = "target/debug/libmycelite")]
    extension_path: String
}

fn init_logger() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init()
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    init_logger();

    // poll topics and launch consumers periodically
    let mut consumers: HashMap<String, MyceliteBridgeHandle> = HashMap::new();

    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cfg.brokers.as_str())
        .create()?;
    loop {
        match client.fetch_metadata(None, Duration::from_secs(5)) {
            Ok(metadata) => {
                let topics = metadata
                    .topics()
                    .iter()
                    .map(|topic| topic.name().to_string());
                for topic in topics {
                    // FIXME: 'private topics'
                    if topic.starts_with("__") {
                        continue
                    }
                    match consumers.get(&topic) {
                        Some(handle) => {
                            if handle.alive() {
                                continue
                            }
                        },
                        None => (),
                    }
                    let res = MyceliteBridge::try_new(
                        cfg.brokers.as_str(),
                        "mycelite_bridge",
                        topic.as_str(),
                        cfg.database.as_str(),
                        cfg.extension_path.as_str()
                    )
                    .await;
                    match res {
                        Ok(bridge) => {
                            consumers.insert(topic, bridge.spawn());
                        }
                        Err(e) => {
                            log::error!("failed to start mycelite bridge from topic {topic}: {e:?}")
                        }
                    }
                }
            }
            Err(e) => log::error!("failed to fetch metadata: {:?}", e),
        };
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    Ok(())
}
