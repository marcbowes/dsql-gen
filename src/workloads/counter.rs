use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;

use crate::pool::ClientHandle;

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

    async fn setup(&self, client: ClientHandle) -> Result<()> {
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS counter (
    id INT PRIMARY KEY,
    value INT
);",
                &[],
            )
            .await?;
        client
            .execute(
                "INSERT INTO counter VALUES (1, 0) ON CONFLICT DO NOTHING",
                &[],
            )
            .await?;
        Ok(())
    }

    async fn transaction(&self, client: ClientHandle) -> Result<Inserts> {
        let s = client.statement("q", &self.q).await?;
        client.execute(&s, &[]).await?;
        Ok(Inserts {
            // FIXME: Not an insert
            rows_inserted: 1,
            logical_bytes_written: 12,
        })
    }
}
