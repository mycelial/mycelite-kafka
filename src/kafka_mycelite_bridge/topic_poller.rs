use crate::kafka_mycelite_bridge::KafkaMyceliteBridgeHandle;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};

pub struct TopicPoller {
    topics: HashSet<String>,
    brokers: String,
    handle: KafkaMyceliteBridgeHandle,
}

#[derive(Debug, Clone)]
pub struct TopicPollerHandle {
    tx: UnboundedSender<Message>,
}

impl TopicPollerHandle {
    pub fn alive(&self) -> bool {
        !self.tx.is_closed()
    }

    pub async fn wait(&self) {
        let (tx, rx) = oneshot_channel();
        self.tx.send(Message::Wait(tx)).ok();
        rx.await.ok();
    }

    pub async fn quit(&self) {
        self.tx.send(Message::Quit).ok();
        self.wait().await;
    }
}

enum Message {
    Wait(OneshotSender<()>),
    Quit,
}

impl TopicPoller {
    pub fn new(brokers: &str, handle: KafkaMyceliteBridgeHandle) -> Self {
        Self {
            topics: HashSet::new(),
            brokers: brokers.into(),
            handle,
        }
    }

    pub fn spawn(mut self) -> TopicPollerHandle {
        let (tx, mut rx) = unbounded_channel::<Message>();
        tokio::spawn(async move {
            self.enter_loop(&mut rx).await;
        });
        TopicPollerHandle { tx }
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<Message>) {
        let client: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.as_str())
            .create()
            .unwrap();
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        let mut waiters = vec![];
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    log::info!("checking topics");
                    match client.fetch_metadata(None, Duration::from_secs(5)) {
                        Ok(metadata) => {
                            for topic in metadata
                                .topics()
                                .iter()
                                .filter(|topic| !(topic.name().starts_with("__") || self.topics.contains(topic.name())))
                                .map(|topic| topic.name().to_string())
                                .collect::<Vec<_>>()
                            {
                                self.handle.add_topic(&topic).ok();
                                self.topics.insert(topic);
                            }
                        }
                        Err(e) => log::error!("failed to fetch metadata: {:?}", e),
                    };
                },
                res = rx.recv() => {
                    if res.is_none() {
                        return
                    }
                    match res.unwrap() {
                        Message::Quit => {
                            return
                        },
                        Message::Wait(tx) => {
                            waiters.push(tx)
                        },
                    }
                }
            }
        }
    }
}
