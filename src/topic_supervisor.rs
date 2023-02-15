use crate::mycelite_bridge::{MyceliteBridge, MyceliteBridgeHandle};
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct TopicSupervisor {
    consumers: BTreeMap<String, MyceliteBridgeHandle>,
    brokers: String,
    extension_path: String,
    database_path: String,
}

pub struct TopicSupervisorHandle {
    join_handle: tokio::task::JoinHandle<()>,
    tx: UnboundedSender<Message>,
}

impl TopicSupervisorHandle {
    pub async fn join(self) -> Result<(), tokio::task::JoinError> {
        self.join_handle.await
    }

    pub fn alive(&self) -> bool {
        !self.tx.is_closed()
    }

    pub async fn abort(self) -> Result<(), tokio::task::JoinError> {
        self.tx.send(Message::Quit).ok();
        self.join_handle.abort();
        self.join_handle.await
    }
}

enum Message {
    Quit,
}

impl TopicSupervisor {
    pub fn new(brokers: &str, extension_path: &str, database_path: &str) -> Self {
        Self {
            consumers: BTreeMap::new(),
            brokers: brokers.into(),
            extension_path: extension_path.into(),
            database_path: database_path.into(),
        }
    }

    pub fn spawn(mut self) -> TopicSupervisorHandle {
        let (tx, mut rx) = unbounded_channel::<Message>();
        let join_handle = tokio::spawn(async move {
            self.enter_loop(&mut rx).await;
        });
        TopicSupervisorHandle { join_handle, tx }
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<Message>) {
        let client: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.brokers.as_str())
            .create()
            .unwrap();
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    log::info!("checking topics");
                    match client.fetch_metadata(None, Duration::from_secs(5)) {
                        Ok(metadata) => {
                            let topics = metadata.topics().iter().map(|topic| topic.name().to_string()).collect::<Vec<_>>();
                            for topic in topics {
                                // FIXME: 'private topics'
                                if topic.starts_with("__") {
                                    continue;
                                }
                                if let Some(handle) = self.consumers.get(&topic) {
                                    if handle.alive() {
                                        continue;
                                    }
                                }
                                let res = MyceliteBridge::try_new(
                                    self.brokers.as_str(),
                                    "mycelite_bridge",
                                    topic.as_str(),
                                    self.database_path.as_str(),
                                    self.extension_path.as_str(),
                                )
                                    .await;
                                match res {
                                    Ok(bridge) => {
                                        self.consumers.insert(topic.to_string(), bridge.spawn());
                                    }
                                    Err(e) => {
                                        log::error!("failed to start mycelite bridge from topic {topic}: {e:?}")
                                    }
                                }
                            }
                        }
                        Err(e) => log::error!("failed to fetch metadata: {:?}", e),
                    };
                },
                msg = rx.recv() => {
                    match msg {
                        Some(Message::Quit) => {
                            return
                        },
                        None => {
                            // channel was closed
                            return
                        }
                    }
                }
            }
        }
    }
}
