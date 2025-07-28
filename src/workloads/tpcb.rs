use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_postgres::Transaction;

use crate::{
    events::*,
    pool::{ClientHandle, ConnectionPool},
};

use super::{Inserts, Workload};

const NUM_TELLERS: usize = 10;
const NUM_ACCOUNTS: usize = 100_000;
const ROWS_PER_TX: usize = 1000;

#[derive(Parser, Debug, Clone)]
pub struct TpcbArgs {
    /// Scale factor (number of branches)
    #[arg(short, long, default_value_t = 1)]
    pub scale: usize,

    /// Skip creating and populating tables?
    #[arg(short, long, default_value_t = false)]
    pub no_initialize: bool,

    /// Skip dropping existing tables?
    #[arg(long, default_value_t = false)]
    pub no_deinitialize: bool,
}

#[derive(Clone)]
pub struct Tpcb {
    pub args: TpcbArgs,
}

impl Tpcb {
    pub fn new(args: TpcbArgs) -> Self {
        Self { args }
    }

    /// Calculate number of branches for the given scale
    pub fn num_branches(&self) -> usize {
        self.args.scale
    }

    /// Calculate number of tellers for the given scale
    pub fn num_tellers(&self) -> usize {
        self.args.scale * NUM_TELLERS
    }

    /// Calculate number of accounts for the given scale
    pub fn num_accounts(&self) -> usize {
        self.args.scale * NUM_ACCOUNTS
    }
}

#[async_trait]
impl Workload for Tpcb {
    type T = Inserts;

    async fn setup(&self, pool: ConnectionPool, tx: mpsc::Sender<Message>) -> Result<()> {
        let client = pool.borrow().await?;

        if !self.args.no_deinitialize {
            drop_tables(&client, tx.clone()).await?;
        }

        if !self.args.no_initialize {
            let mut j = JoinSet::new();

            j.spawn(initialize_branches(
                self.num_branches(),
                pool.clone(),
                tx.clone(),
            ));
            j.spawn(initialize_tellers(
                self.num_tellers(),
                pool.clone(),
                tx.clone(),
            ));
            j.spawn(initialize_accounts(
                self.num_accounts(),
                pool.clone(),
                tx.clone(),
            ));
            j.spawn(initialize_history(pool.clone(), tx.clone()));

            while let Some(r) = j.join_next().await {
                _ = r??;
            }
        }

        Ok(())
    }

    async fn transaction(
        &self,
        transaction: &Transaction<'_>,
        _tx: mpsc::Sender<Message>,
    ) -> Result<Self::T> {
        let aid = rand::random_range(1..=self.num_accounts()) as i32;
        let bid = rand::random_range(1..=self.num_branches()) as i32;
        let tid = rand::random_range(1..=self.num_tellers()) as i32;
        let delta = rand::random_range(-5000..=5000);

        let mut rows = transaction
            .execute(
                "UPDATE pgbench_accounts SET abalance = abalance + $1 WHERE aid = $2",
                &[&delta, &aid],
            )
            .await?;
        assert_eq!(1, rows, "account {aid} does not exist");

        // XXX: the balance doesn't matter; this simply matches the number of
        // queries/load to the service that pgbench makes, and will panic if
        // there is a correctness bug.
        let _ = transaction
            .query_one(
                "SELECT abalance FROM pgbench_accounts WHERE aid = $1",
                &[&aid],
            )
            .await?;

        rows += transaction
            .execute(
                "UPDATE pgbench_tellers SET tbalance = tbalance + $1 WHERE tid = $2",
                &[&delta, &tid],
            )
            .await?;
        assert_eq!(2, rows, "teller {tid} does not exist");

        rows += transaction
            .execute(
                "UPDATE pgbench_branches SET bbalance = bbalance + $1 WHERE bid = $2",
                &[&delta, &bid],
            )
            .await?;
        assert_eq!(3, rows, "branch {bid} does not exist");

        rows += transaction
            .execute(
                "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)",
                &[&tid, &bid, &aid, &delta],
            )
            .await?;
        assert_eq!(4, rows);

        // FIXME: not an insert, wrong lb
        Ok(Inserts {
            rows_inserted: rows as usize,
            logical_bytes_written: 100,
        })
    }
}

/// Drop all pgbench tables
async fn drop_tables(client: &ClientHandle, tx: mpsc::Sender<Message>) -> Result<()> {
    tx.table_dropping("pgbench_history").await?;
    client.ddl("DROP TABLE IF EXISTS pgbench_history").await?;
    tx.table_dropped("pgbench_history").await?;

    tx.table_dropping("pgbench_accounts").await?;
    client.ddl("DROP TABLE IF EXISTS pgbench_accounts").await?;
    tx.table_dropped("pgbench_accounts").await?;

    tx.table_dropping("pgbench_tellers").await?;
    client.ddl("DROP TABLE IF EXISTS pgbench_tellers").await?;
    tx.table_dropped("pgbench_tellers").await?;

    tx.table_dropping("pgbench_branches").await?;
    client.ddl("DROP TABLE IF EXISTS pgbench_branches").await?;
    tx.table_dropped("pgbench_branches").await?;

    Ok(())
}

async fn initialize_branches(
    num_branches: usize,
    pool: ConnectionPool,
    tx: mpsc::Sender<Message>,
) -> Result<bool> {
    let client = pool.borrow().await?;

    let exists = client
        ._query_one("SELECT to_regclass('pgbench_branches') IS NOT NULL", &[])
        .await?;

    if exists.get::<'_, _, bool>(0) == true {
        return Ok(false);
    }

    tx.table_creating("pgbench_branches").await?;
    let _ = client
        .ddl(
            "CREATE TABLE pgbench_branches (
    bid INTEGER PRIMARY KEY,
    bbalance INTEGER,
    filler CHAR(88)
);",
        )
        .await?;
    tx.table_created("pgbench_branches").await?;

    populate_branches(num_branches, &client, tx).await?;

    Ok(true)
}

async fn populate_branches(
    num_branches: usize,
    client: &ClientHandle,
    tx: mpsc::Sender<Message>,
) -> Result<()> {
    let mut remaining = num_branches;
    tx.table_loading("pgbench_branches", remaining).await?;
    let mut start = 1;

    while remaining > 0 {
        let rows = remaining.min(ROWS_PER_TX);
        let stop = start + rows - 1; // Subtract 1 to avoid overlap
        client
            .execute(
                &format!(
                    "INSERT INTO pgbench_branches (bid, bbalance)
SELECT bid, 0 FROM generate_series({start}, {stop}) as bid
"
                ),
                &[],
            )
            .await?;
        tx.table_loaded("pgbench_branches", rows).await?;
        remaining -= rows;
        start = stop + 1; // Start from the next number
    }

    Ok(())
}

async fn initialize_tellers(
    num_tellers: usize,
    pool: ConnectionPool,
    tx: mpsc::Sender<Message>,
) -> Result<bool> {
    let client = pool.borrow().await?;

    let exists = client
        ._query_one("SELECT to_regclass('pgbench_tellers') IS NOT NULL", &[])
        .await?;

    if exists.get::<'_, _, bool>(0) == true {
        return Ok(false);
    }

    tx.table_creating("pgbench_tellers").await?;
    let _ = client
        .ddl(
            "CREATE TABLE pgbench_tellers (
    tid INTEGER PRIMARY KEY,
    bid INTEGER,
    tbalance INTEGER,
    filler CHAR(84)
);",
        )
        .await?;

    let row = client
        ._query_one(
            "CREATE INDEX ASYNC pgbench_tellers_bid_idx ON pgbench_tellers (bid)",
            &[],
        )
        .await?;

    let job_id = row.get::<'_, _, String>("job_id");
    client
        .execute("CALL sys.wait_for_job($1)", &[&job_id])
        .await?;

    tx.table_created("pgbench_tellers").await?;

    populate_tellers(num_tellers, &client, tx).await?;

    Ok(true)
}

async fn populate_tellers(
    num_tellers: usize,
    client: &ClientHandle,
    tx: mpsc::Sender<Message>,
) -> Result<()> {
    let mut remaining = num_tellers;
    tx.table_loading("pgbench_tellers", remaining).await?;
    let mut start = 1;

    while remaining > 0 {
        let rows = remaining.min(ROWS_PER_TX);
        let stop = start + rows - 1; // Subtract 1 to avoid overlap
        client
            .execute(
                &format!(
                    "INSERT INTO pgbench_tellers (tid, bid, tbalance)
SELECT tid, (tid - 1) / {NUM_TELLERS} + 1, 0 FROM generate_series({start}, {stop}) as tid
",
                ),
                &[],
            )
            .await?;
        tx.table_loaded("pgbench_tellers", rows).await?;
        remaining -= rows;
        start = stop + 1; // Start from the next number
    }

    Ok(())
}

async fn initialize_accounts(
    num_accounts: usize,
    pool: ConnectionPool,
    tx: mpsc::Sender<Message>,
) -> Result<bool> {
    let client = pool.borrow().await?;

    let exists = client
        ._query_one("SELECT to_regclass('pgbench_accounts') IS NOT NULL", &[])
        .await?;

    if exists.get::<'_, _, bool>(0) == true {
        return Ok(false);
    }

    tx.table_creating("pgbench_accounts").await?;

    let _ = client
        .ddl(
            "CREATE TABLE pgbench_accounts (
    aid INTEGER PRIMARY KEY,
    bid INTEGER,
    abalance INTEGER,
    filler CHAR(84)
);",
        )
        .await?;

    let row = client
        ._query_one(
            "CREATE INDEX ASYNC pgbench_accounts_bid_idx ON pgbench_accounts (bid)",
            &[],
        )
        .await?;

    let job_id = row.get::<'_, _, String>("job_id");
    client
        .execute("CALL sys.wait_for_job($1)", &[&job_id])
        .await?;

    tx.table_created("pgbench_accounts").await?;

    populate_accounts(num_accounts, &client, tx).await?;

    Ok(true)
}

async fn populate_accounts(
    num_accounts: usize,
    client: &ClientHandle,
    tx: mpsc::Sender<Message>,
) -> Result<()> {
    let mut remaining = num_accounts;
    tx.table_loading("pgbench_accounts", remaining).await?;
    let mut start = 1;

    while remaining > 0 {
        let rows = remaining.min(ROWS_PER_TX);
        let stop = start + rows - 1; // Subtract 1 to avoid overlap
        client
            .execute(
                &format!(
                    "INSERT INTO pgbench_accounts (aid, bid, abalance)
SELECT aid, (aid - 1) / {NUM_ACCOUNTS} + 1, 0 FROM generate_series({start}, {stop}) as aid
"
                ),
                &[],
            )
            .await?;
        tx.table_loaded("pgbench_accounts", rows).await?;
        remaining -= rows;
        start = stop + 1; // Start from the next number
    }

    Ok(())
}

async fn initialize_history(pool: ConnectionPool, tx: mpsc::Sender<Message>) -> Result<bool> {
    let client = pool.borrow().await?;

    let exists = client
        ._query_one("SELECT to_regclass('pgbench_history') IS NOT NULL", &[])
        .await?;

    if exists.get::<'_, _, bool>(0) == true {
        return Ok(false);
    }

    tx.table_creating("pgbench_history").await?;
    let _ = client
        .ddl(
            "CREATE TABLE pgbench_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tid INTEGER,
    bid INTEGER,
    aid INTEGER,
    delta INTEGER,
    mtime TIMESTAMP,
    filler CHAR(22)
)",
        )
        .await?;
    tx.table_created("pgbench_history").await?;

    Ok(true)
}
