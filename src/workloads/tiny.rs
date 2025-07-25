use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use tokio::sync::mpsc;
use tokio_postgres::Transaction;

use crate::{events::*, pool::ClientHandle};

use super::{Inserts, Workload};

#[derive(Parser, Debug, Clone)]
pub struct TinyRowsArgs {
    #[arg(short, long)]
    pub rows_per_transaction: usize,
}

#[derive(Clone)]
pub struct TinyRows {
    pub args: TinyRowsArgs,
    q: String,
}

impl TinyRows {
    pub fn new(args: TinyRowsArgs) -> Self {
        let q = format!(
            "INSERT INTO tiny (content) SELECT md5(random()::text) FROM generate_series(1, {})",
            args.rows_per_transaction
        );
        Self { args, q }
    }
}

#[async_trait]
impl Workload for TinyRows {
    type T = Inserts;

    async fn setup(&self, client: ClientHandle, tx: mpsc::Sender<Message>) -> Result<()> {
        tx.table_creating("tiny").await?;
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS tiny (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content text
);",
                &[],
            )
            .await?;
        tx.table_created("tiny").await?;

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
            rows_inserted: self.args.rows_per_transaction,
            logical_bytes_written: self.args.rows_per_transaction * 68,
        })
    }
}
