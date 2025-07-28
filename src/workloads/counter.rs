use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use tokio::sync::mpsc;
use tokio_postgres::Transaction;

use crate::{events::*, pool::ConnectionPool};

use super::{Inserts, Workload};

#[derive(Parser, Debug, Clone)]
pub struct CounterArgs {}

#[derive(Clone)]
pub struct Counter {
    pub args: CounterArgs,
    q: String,
}

impl Counter {
    pub fn new(args: CounterArgs) -> Self {
        let q = format!("UPDATE counter SET value = value + 1 WHERE id = 1");
        Self { args, q }
    }
}

#[async_trait]
impl Workload for Counter {
    type T = Inserts;

    async fn setup(&self, pool: ConnectionPool, tx: mpsc::Sender<Message>) -> Result<()> {
        let client = pool.borrow().await?;

        tx.table_creating("counter").await?;
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS counter (
    id INT PRIMARY KEY,
    value INT
);",
                &[],
            )
            .await?;
        tx.table_created("counter").await?;

        tx.table_loading("counter", 1).await?;
        client
            .execute(
                "INSERT INTO counter VALUES (1, 0) ON CONFLICT DO NOTHING",
                &[],
            )
            .await?;
        tx.table_loaded("counter", 1).await?;

        Ok(())
    }

    async fn transaction(
        &self,
        transaction: &Transaction<'_>,
        _tx: mpsc::Sender<Message>,
    ) -> Result<Inserts> {
        // FIXME: prepare a statement
        transaction.execute(&self.q, &[]).await?;
        Ok(Inserts {
            // FIXME: Not an insert
            rows_inserted: 1,
            logical_bytes_written: 12,
        })
    }
}
