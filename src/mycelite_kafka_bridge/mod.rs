use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteConnection},
    ConnectOptions, Row,
};
use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use tokio::sync::mpsc::{
    error::TryRecvError, unbounded_channel, UnboundedReceiver, UnboundedSender,
};
use tokio::sync::oneshot::{channel as oneshot_channel, Sender as OneshotSender};
use futures::StreamExt;

#[derive(Debug)]
pub struct MyceliteKafkaBridge {
    brokers: String,
    sqlite_conn: Connection,
    tx: UnboundedSender<Message>,
    rx: UnboundedReceiver<Message>,
}

#[derive(Debug, Clone)]
pub struct MyceliteKafkaBridgeHandle {
    tx: UnboundedSender<Message>,
}

type Connection = SqliteConnection;

async fn sqlite_connection(uri: &str) -> Result<Connection> {
    let conn = SqliteConnectOptions::from_str(uri)?
        .vfs("unix") // FIXME: hardcoded value
        .create_if_missing(true)
        .connect()
        .await?;
    Ok(conn)
}

#[derive(Debug)]
enum Message {
    /// Restream data from database back to kafka
    Restream(OneshotSender<()>, String),
    /// Restreamer done with db
    Done(String),
    /// Store new offset
    StoreOffset(String, String, i64),
    /// Wait for 'actor' to quit
    Wait(OneshotSender<()>),
    /// Ask 'actor' to quit
    Quit,
}

impl MyceliteKafkaBridge {
    pub async fn new(brokers: &str, database_path: &str) -> Result<Self> {
        let (tx, rx) = unbounded_channel();
        Ok(Self {
            brokers: brokers.into(),
            sqlite_conn: sqlite_connection(database_path).await?,
            tx,
            rx,
        })
    }

    pub fn spawn(mut self) -> MyceliteKafkaBridgeHandle {
        let handle = self.handle();
        tokio::spawn(async move {
            if let Err(e) = self.enter_loop().await {
                log::error!("mycelite kafka bridge loop failed: {e:?}")
            }
        });
        handle
    }

    async fn enter_loop(&mut self) -> Result<()> {
        let mut waiters = vec![];
        self.create_table().await?;
        let mut queue: HashMap<String, VecDeque<OneshotSender<()>>> = HashMap::new();
        let mut restreamers: HashMap<String, (RestreamerHandle, OneshotSender<()>)> =
            HashMap::new();
        while let Some(message) = self.rx.recv().await {
            match message {
                Message::Wait(tx) => waiters.push(tx),
                Message::Restream(tx, db_path) => {
                    // FIXME:
                    match restreamers.get(&db_path) {
                        Some(_) => {
                            queue.entry(db_path).and_modify(|q| q.push_back(tx));
                        }
                        None => match self.spawn_restreamer(&db_path).await {
                            Ok(restreamer) => {
                                restreamers.insert(db_path, (restreamer, tx));
                            }
                            Err(e) => {
                                log::error!("failed to spawn restreamer for {db_path}: {e:?}")
                            }
                        },
                    }
                }
                Message::StoreOffset(db_path, topic, offset) => {
                    self.store_offset(&db_path, &topic, offset).await?
                }
                Message::Done(db_path) => {
                    if let Some((_, tx)) = restreamers.remove(&db_path) {
                        tx.send(()).ok();
                    };
                    // FIXME:
                    if let Some(q) = queue.get_mut(&db_path) {
                        if let Some(tx) = q.pop_front() {
                            match self.spawn_restreamer(&db_path).await {
                                Ok(restreamer) => {
                                    restreamers.insert(db_path, (restreamer, tx));
                                }
                                Err(e) => {
                                    log::error!("failed to spawn restreamer for {db_path}: {e:?}")
                                }
                            }
                        }
                    }
                }
                Message::Quit => break,
            }
        }
        Ok(())
    }

    async fn spawn_restreamer(&mut self, db_path: &str) -> Result<RestreamerHandle> {
        let offsets = sqlx::query("SELECT topic, offset FROM offsets WHERE database = ?")
            .bind(db_path)
            .fetch_all(&mut self.sqlite_conn)
            .await?;

        let offsets = offsets.iter().fold(HashMap::new(), |mut acc, row| {
            let topic = row.get::<String, _>(0);
            let offset = row.get::<i64, _>(1);
            acc.insert(topic, offset);
            acc
        });
        let handle = self.handle();
        Ok(
            Restreamer::new(self.brokers.as_str(), db_path, offsets, handle)
                .await?
                .spawn(),
        )
    }

    fn handle(&self) -> MyceliteKafkaBridgeHandle {
        MyceliteKafkaBridgeHandle {
            tx: self.tx.clone(),
        }
    }

    async fn create_table(&mut self) -> Result<()> {
        sqlx::query("CREATE TABLE IF NOT EXISTS offsets (database TEXT, topic TEXT, offset int, PRIMARY KEY(database, topic))")
            .execute(&mut self.sqlite_conn)
            .await?;
        Ok(())
    }

    async fn store_offset(&mut self, db: &str, topic: &str, offset: i64) -> Result<()> {
        sqlx::query("INSERT OR REPLACE INTO offsets(database, topic, offset) VALUES(?, ?, ?)")
            .bind(db)
            .bind(topic)
            .bind(offset)
            .execute(&mut self.sqlite_conn)
            .await?;
        Ok(())
    }
}

impl MyceliteKafkaBridgeHandle {
    pub async fn quit(&self) {
        self.tx.send(Message::Quit).ok();
    }

    pub async fn wait(&self) {
        let (tx, rx) = oneshot_channel();
        {
            self.tx.send(Message::Wait(tx)).ok();
        }
        rx.await.ok();
    }

    pub async fn restream<S: Into<String>>(&self, db_path: S) -> Result<()> {
        let (tx, rx) = oneshot_channel();
        self.tx.send(Message::Restream(tx, db_path.into())).ok();
        Ok(rx.await?)
    }

    async fn done<S: Into<String>>(&self, db: S) {
        self.tx.send(Message::Done(db.into())).ok();
    }

    async fn store_offset(&self, db: &str, topic: &str, offset: i64) {
        self.tx
            .send(Message::StoreOffset(db.into(), topic.into(), offset))
            .ok();
    }
}

struct Restreamer {
    brokers: String,
    db_path: String,
    sql_conn: Connection,
    offsets: HashMap<String, i64>,
    handle: MyceliteKafkaBridgeHandle,
    limit: usize,
}

impl Restreamer {
    async fn new(
        brokers: &str,
        db_path: &str,
        offsets: HashMap<String, i64>,
        handle: MyceliteKafkaBridgeHandle,
    ) -> Result<Self> {
        Ok(Self {
            brokers: brokers.into(),
            db_path: db_path.into(),
            sql_conn: sqlite_connection(db_path).await?,
            offsets,
            handle,
            limit: 2048,
        })
    }

    fn spawn(mut self) -> RestreamerHandle {
        let (tx, mut rx) = unbounded_channel();
        tokio::spawn(async move {
            if let Err(e) = self.enter_loop(&mut rx).await {
                log::error!("restreamer failed: {e:?}");
            };
            self.handle.done(&self.db_path).await;
        });
        RestreamerHandle { tx }
    }

    async fn get_tables(&mut self) -> Result<Vec<String>> {
        let tables = sqlx::query("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
            .fetch_all(&mut self.sql_conn)
            .await?;

        Ok(tables
            .iter()
            .map(|row| row.get::<String, _>(0))
            .collect::<Vec<_>>())
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<()>) -> Result<()> {
        let client: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", self.brokers.as_str())
            .set("message.timeout.ms", "5000")
            .create()
            .expect("failed to create producer");

        for table in self.get_tables().await? {
            let table = &table;

            let mut offset = self.offsets.get(table).copied().unwrap_or(0_i64);
            loop {

                let (last_rowid, mut futures) = sqlx::query(&format!(
                    r#"SELECT rowid, key, payload FROM "{table}" WHERE rowid > ? LIMIT ?"#
                ))
                .bind(offset)
                .bind(self.limit as i64)
                .fetch_all(&mut self.sql_conn)
                .await?
                .iter()
                .fold((None, futures::stream::FuturesUnordered::new()), |(_, mut futures), row| {
                    let rowid = row.get::<i64, _>(0);
                    let key = row.get::<Vec<u8>, _>(1);
                    let payload = row.get::<Vec<u8>, _>(2);

                    futures.push(async move {
                        let message = FutureRecord::to(&table)
                            .key(key.as_slice())
                            .payload(payload.as_slice())
                            .headers(OwnedHeaders::new().insert(Header {
                                key: "mycelite",
                                value: Some("ignore".as_bytes()),
                            }));
                        client.send(message, Timeout::Never).await
                    });

                    (Some(rowid), futures)
                });

                if futures.is_empty() {
                    break
                }

                while let Some(res) = futures.next().await {
                    res.map_err(|e| e.0)?;
                }

                if let Some(value) = last_rowid {
                    self.handle.store_offset(&self.db_path, &table, value).await;
                    offset = value;
                };
            }
        }
        Ok(())
    }
}

#[allow(unused)]
struct RestreamerHandle {
    tx: UnboundedSender<()>,
}
