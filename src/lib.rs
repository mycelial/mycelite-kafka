#![allow(dead_code)]
#![allow(unused)]

use anyhow::Result;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Message as _, BorrowedMessage};
use rdkafka::ClientConfig;
use rdkafka::ClientContext;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePool},
    ConnectOptions,
};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

type Pool = SqlitePool;
type Connection = SqliteConnection;

async fn sqlite_connection(uri: &str, extension_path: &str) -> Result<Connection> {
    let conn = SqliteConnectOptions::from_str(uri)?
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
pub struct MyceliteBridge {
    brokers: String,
    group_id: String,
    topic: String,
    sqlite_conn: Connection,
}


impl MyceliteBridge {
    pub async fn try_new(
        brokers: &str,
        group_id: &str,
        topic: &str,
        sqlite_db: &str,
        extension_path: &str,
    ) -> Result<Self> {
        Ok(Self {
            brokers: brokers.into(),
            group_id: group_id.into(),
            topic: topic.into(),
            sqlite_conn: sqlite_connection(sqlite_db, extension_path).await?,
        })
    }

    pub fn spawn(mut self) -> MyceliteBridgeHandle {
        let (tx, mut rx) = unbounded_channel::<Message>();
        MyceliteBridgeHandle {
            join_handle: tokio::spawn(async move {
                match self.enter_loop(&mut rx).await {
                    Ok(()) => (),
                    Err(e) => log::error!("({}) consumer failed with error: {:?}", self.topic, e)
                };
            }),
            tx,
        }
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<Message>) -> Result<()> {
        self.create_table().await?;
        let mut consumer = self.setup_consumer()?;
        loop {
            tokio::select! {
                res = rx.recv() => {
                    if let Some(message) = res {
                        match message {
                            Message::Quit => {
                                log::info!("({}) received quit message, quitting", self.topic);
                                return Ok(())
                            }
                        }
                    } else {
                        log::info!("({}) mycelite bridge handle was dropped, quitting", self.topic);
                        return Ok(())
                    }
                },
                res = consumer.recv() => {
                    match res {
                        Err(e) => {
                            log::error!("({}) failed to receive message: {e:?}", self.topic);
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        },
                        Ok(message) => {
                            log::info!(
                                "({}) new message, offset: {}, partion: {}, key: {:?}, value: {:?}",
                                self.topic,
                                message.offset(), message.partition(), message.key(), message.payload()
                            );
                            self.store(&message).await?;
                            //consumer.commit_message(&message, CommitMode::Async)?;
                        }
                    }
                }
            }
        }
    }

    fn setup_consumer(&self) -> Result<StreamConsumer> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", self.group_id.as_str())
            .set("bootstrap.servers", self.brokers.as_str())
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;
        consumer.subscribe(&[self.topic.as_str()])?;
        Ok(consumer)
    }

    async fn create_table(&mut self) -> Result<()> {
        // FIXME: cache query?
        let query = format!(
            r#"CREATE TABLE IF NOT EXISTS '{}' (
                partition INT NOT NULL,
                offset INT NOT NULL,
                key BLOB,
                payload BLOB,
                PRIMARY KEY (partition, offset)
            )"#,
            self.topic
        );

        sqlx::query(query.as_str())
            .fetch_all(&mut self.sqlite_conn)
            .await?;
        Ok(())
    }

    async fn store<'a>(&mut self, message: &'a BorrowedMessage<'a>) -> Result<()> {
        // FIXME: cache query?
        let query = format!("INSERT OR IGNORE INTO '{}'(partition, offset, key, payload) VALUES(?, ?, ?, ?)", self.topic);
        sqlx::query(query.as_str())
            .bind(message.partition())
            .bind(message.offset())
            .bind(message.key().unwrap_or(&[]))
            .bind(message.payload().unwrap_or(&[]))
            .execute(&mut self.sqlite_conn)
            .await?;
        Ok(())
    }
}

pub struct MyceliteBridgeHandle {
    join_handle: tokio::task::JoinHandle<()>,
    tx: UnboundedSender<Message>,
}

impl MyceliteBridgeHandle {
    pub fn alive(&self) -> bool {
        !self.tx.is_closed()
    }
}


enum Message {
    Quit
}
