use anyhow::Result;
use std::time::Duration;

use clap::Parser;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

#[derive(Debug, Parser)]
struct Config {
    #[clap(short, long, default_value = "1")]
    topics: usize,
    #[clap(short, long, default_value = "3")]
    rate: usize,
    #[clap(short, long, default_value = "localhost:9092")]
    brokers: String,
}

struct XorShift {
    state: u64,
}

impl XorShift {
    fn new() -> Self {
        let seed = std::time::UNIX_EPOCH
            .elapsed()
            .expect("failed to get unix time");
        Self {
            state: (seed.as_millis() as u64).max(1),
        }
    }

    fn next(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        self.state
    }
}

async fn producer(topic_name: String, rate: usize, brokers: &str) {
    let delay = std::time::Duration::from_millis((1000.0 / rate as f32) as u64);
    let client: &FutureProducer = &ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("failed to create producer");

    let mut rng = XorShift::new();
    loop {
        let payload = format!("{}", rng.next());
        let message = FutureRecord::to(topic_name.as_str())
            .key(payload.as_bytes())
            .payload(payload.as_bytes());
        match client.send(message, Timeout::Never).await {
            Ok(_) => (),
            Err((e, _)) => return println!("error: {:?}", e),
        }
        tokio::time::sleep(delay).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::parse();
    let brokers: &'static str = unsafe { std::mem::transmute(cfg.brokers.as_str()) };

    let mut handles = (0..cfg.topics)
        .map(|topic| {
            let topic_name = format!("topic-{}", topic);
            tokio::spawn(async move { producer(topic_name, cfg.rate, brokers).await })
        })
        .collect::<Vec<_>>();
    while let Some(handle) = handles.pop() {
        handle.await?;
    }
    Ok(())
}
