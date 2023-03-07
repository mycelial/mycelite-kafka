use anyhow::Result;
use rdkafka::consumer::{
    CommitMode, Consumer,
    stream_consumer::StreamConsumer,
};
use rdkafka::error::KafkaError;
use rdkafka::message::{BorrowedMessage, Headers, Message as _};
use rdkafka::{ClientConfig, TopicPartitionList, Offset};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode},
    ConnectOptions,
};
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};

type Connection = SqliteConnection;

async fn sqlite_connection(uri: &str, extension_path: &str) -> Result<Connection> {
    let _conn = SqliteConnectOptions::from_str(uri)?
        .extension(extension_path.to_owned())
        .journal_mode(SqliteJournalMode::Delete)
        .create_if_missing(true)
        .connect()
        .await?;
    let conn = SqliteConnectOptions::from_str(uri)?
        .extension_with_entrypoint(extension_path.to_owned(), "mycelite_config")
        .vfs("mycelite_writer")
        .connect()
        .await?;
    Ok(conn)
}

/// bridge between topic and mycelite
pub struct KafkaMyceliteBridge {
    brokers: String,
    group_id: String,
    topics: HashSet<String>,
    sqlite_conn: Connection,
    topic_partition_list: TopicPartitionList,
    transaction_started: bool,
    insert_count: u64,
    last_insert_count_tick: u64,
    batch_size: u64,
}

impl KafkaMyceliteBridge {
    pub async fn try_new(
        brokers: &str,
        group_id: &str,
        sqlite_db: &str,
        extension_path: &str,
        topics: &[&str],
    ) -> Result<Self> {
        Ok(Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topics: HashSet::from_iter(topics.iter().map(|s| s.to_string())),
            sqlite_conn: sqlite_connection(sqlite_db, extension_path).await?,
            topic_partition_list: TopicPartitionList::new(),
            transaction_started: false,
            insert_count: 0,
            last_insert_count_tick: 0,
            batch_size: 8192
        })
    }

    pub fn spawn(mut self) -> KafkaMyceliteBridgeHandle {
        let (tx, mut rx) = unbounded_channel::<Message>();
        tokio::spawn(async move {
            match self.enter_loop(&mut rx).await {
                Ok(()) => log::info!("mycelite bridge done"),
                Err(e) => log::error!("failed with error: {:?}", e),
            };
        });
        KafkaMyceliteBridgeHandle { tx }
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<Message>) -> Result<()> {
        let consumer = self.setup_consumer()?;
        consumer.subscribe(&self.get_topics()).ok();
        let mut waiters = vec![];
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                res = rx.recv() => {
                    let message = match res {
                        None => {
                            log::info!("mycelite bridge handle was dropped, quitting");
                            return Ok(())
                        },
                        Some(m) => m
                    };
                    match message {
                        Message::Quit => {
                            log::info!("received quit message, quitting");
                            return Ok(())
                        },
                        Message::AddTopic(topic) => {
                            log::info!("adding topic '{topic}'");
                            self.create_table(&topic).await?;
                            self.topics.insert(topic);
                            consumer.subscribe(&self.get_topics()).ok();
                        },
                        Message::RemoveTopic(topic) => {
                            log::info!("removing topic {topic}");
                            self.topics.remove(&topic);
                            consumer.subscribe(&self.get_topics()).ok();
                        },
                        Message::ListTopics(tx) => {
                            tx.send(self.topics.iter().cloned().collect()).ok();
                        },
                        Message::Wait(tx) => {
                            waiters.push(tx);
                        },
                    }
                },
                _tick = interval.tick() => {
                    if self.last_insert_count_tick == self.insert_count {
                        self.commit(&consumer).await?;
                    } else {
                        self.last_insert_count_tick = self.insert_count
                    }
                },
                res = consumer.recv() => {
                    match res {
                        Err(e) => {
                            match e {
                                KafkaError::MessageConsumption(_) => (),
                                _ => log::error!("failed to receive message: {e:?}")
                            };
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        },
                        Ok(message) => {
                            let ignore = match message.headers() {
                                None => false,
                                Some(headers) => {
                                    headers
                                        .iter()
                                        .any(|header| header.key == "mycelite" && header.value == Some("ignore".as_bytes()))
                                },
                            };
                            if !ignore {
                                log::info!(
                                    "new message, topic: {}, offset: {}, partion: {}, key: {:?}, value: {:?}",
                                    message.topic(), message.offset(), message.partition(), message.key(), message.payload()
                                );
                                self.begin().await?;
                                self.store(&message).await?;
                                if self.insert_count >= self.batch_size {
                                    self.commit(&consumer).await?;
                                }
                            } else {
                                log::info!(
                                    "restreamed message ignores: topic: {}, offset: {}, partion: {}, key: {:?}, value: {:?}",
                                    message.topic(), message.offset(), message.partition(), message.key(), message.payload()
                                );
                            }
                        }
                    }
                },
            }
        }
    }

    fn get_topics(&self) -> Vec<&str> {
        self.topics.iter().map(|s| s.as_str()).collect()
    }

    fn setup_consumer(&self) -> Result<StreamConsumer> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", self.group_id.as_str())
            .set("bootstrap.servers", self.brokers.as_str())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        Ok(consumer)
    }

    async fn create_table(&mut self, table: &str) -> Result<()> {
        sqlx::query(&format!(
            r#"CREATE TABLE IF NOT EXISTS "{table}" (
                    partition INT NOT NULL,
                    offset INT NOT NULL,
                    key BLOB,
                    payload BLOB,
                    PRIMARY KEY (partition, offset)
                )"#,
        ))
        .fetch_all(&mut self.sqlite_conn)
        .await?;
        Ok(())
    }

    async fn begin(&mut self) -> Result<()> {
        if self.transaction_started {
            return Ok(())
        }
        sqlx::query("BEGIN")
            .execute(&mut self.sqlite_conn)
            .await?;
        self.transaction_started = true;
        Ok(())
    }

    async fn commit(&mut self, consumer: &StreamConsumer) -> Result<()> {
        if !self.transaction_started || self.insert_count == 0 {
            return Ok(())
        }
        sqlx::query("COMMIT")
            .execute(&mut self.sqlite_conn)
            .await?;
        self.transaction_started = false;
        self.insert_count = 0;
        consumer.commit(&self.topic_partition_list, CommitMode::Async)?;
        Ok(())
    }

    async fn store<'a, 'b: 'a>(&mut self, message: &'a BorrowedMessage<'b>) -> Result<()> {
        sqlx::query(&format!(
            r#"INSERT OR IGNORE INTO "{}" (partition, offset, key, payload) VALUES(?, ?, ?, ?)"#,
            message.topic(),
        ))
        .bind(message.partition())
        .bind(message.offset())
        .bind(message.key().unwrap_or(&[]))
        .bind(message.payload().unwrap_or(&[]))
        .execute(&mut self.sqlite_conn)
        .await?;
        self.insert_count += 1;
        self.topic_partition_list.add_partition_offset(
            message.topic(), message.partition(), Offset::Offset(message.offset() + 1)
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct KafkaMyceliteBridgeHandle {
    tx: UnboundedSender<Message>,
}

impl KafkaMyceliteBridgeHandle {
    pub fn alive(&self) -> bool {
        !self.tx.is_closed()
    }

    pub async fn quit(&self) {
        self.tx.send(Message::Quit).ok();
        self.wait().await;
    }

    pub fn add_topic<S: Into<String>>(&self, topic: S) -> Result<()> {
        Ok(self.tx.send(Message::AddTopic(topic.into()))?)
    }

    pub fn remove_topic<S: Into<String>>(&self, topic: S) -> Result<()> {
        Ok(self.tx.send(Message::RemoveTopic(topic.into()))?)
    }

    pub async fn wait(&self) {
        let (tx, rx) = oneshot_channel();
        self.tx.send(Message::Wait(tx)).ok();
        rx.await.ok();
    }

    pub async fn list_topics(&self) -> Result<Vec<String>> {
        let (tx, rx) = oneshot_channel();
        self.tx.send(Message::ListTopics(tx))?;
        Ok(rx.await?)
    }
}

#[derive(Debug)]
enum Message {
    AddTopic(String),
    RemoveTopic(String),
    ListTopics(OneshotSender<Vec<String>>),
    Wait(OneshotSender<()>),
    Quit,
}
