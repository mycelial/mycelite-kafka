#![allow(dead_code)]
#![allow(unused)]

use anyhow::Result;
use clap::Parser;
use mycelite_kafka::TopicSupervisor;
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    env_logger::init();
    TopicSupervisor::new(&cfg.brokers, &cfg.extension_path, &cfg.database)
        .spawn()
        .join()
        .await?;
    Ok(())
}
