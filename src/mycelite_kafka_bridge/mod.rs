use anyhow::Result;
use tokio::sync::mpsc::{
    unbounded_channel,
    UnboundedSender,
    UnboundedReceiver,
};
use tokio::sync::oneshot::{
    channel as oneshot_channel,
    Sender as OneshotSender,
};

pub struct MyceliteKafkaBridge {
    brokers: String,
    database_path: String,
}

pub struct MyceliteKafkaBridgeHandle {
    tx: UnboundedSender<Message>
}

#[derive(Debug)]
enum Message {
    /// Restream data from database back to kafka
    Restream(String, OneshotSender<()>),
    /// Wait for 'actor' to quit
    Wait(OneshotSender<()>),
    /// Ask 'actor' to quit
    Quit
}

impl MyceliteKafkaBridge {
    pub fn new(brokers: &str, database_path: &str) -> Self {
        Self {
            brokers: brokers.into(),
            database_path: database_path.into()
        }
    }

    pub fn spawn(mut self) -> MyceliteKafkaBridgeHandle {
        let (tx, mut rx) = unbounded_channel();
        tokio::spawn(async move {
            self.enter_loop(&mut rx).await;
        });
        MyceliteKafkaBridgeHandle { tx }
    }

    async fn enter_loop(&mut self, rx: &mut UnboundedReceiver<Message>) {
        let mut waiters = vec![];
        loop {
            let res = rx.recv().await;
            if res.is_none() {
                return
            }
            match res.unwrap() {
                Message::Wait(tx) => waiters.push(tx),
                Message::Restream(from, tx) => {
                    // TODO:
                    tx.send(()).ok();
                },
                Message::Quit => return,
            }

        }
    }
}


impl MyceliteKafkaBridgeHandle {
    pub async fn quit(&self) {
        self.tx.send(Message::Quit).ok();
    }

    pub async fn wait(&self) {
        let (tx, rx) = oneshot_channel();
        { self.tx.send(Message::Wait(tx)).ok(); }
        rx.await.ok();
    }

    pub async fn restream<S: Into<String>>(&self, from: S) -> Result<()> {
        let (tx, rx) = oneshot_channel();
        self.tx.send(Message::Restream(from.into(), tx)).ok();
        Ok(rx.await?)
    }
}


#[cfg(test)]
mod test {
    use super::*;

}
