use std::{num::NonZero, time::Duration};

use anyhow::{anyhow, Result};
use async_rate_limiter::RateLimiter;
use async_trait::async_trait;
use aws_config::SdkConfig;
use tokio::{sync::mpsc, time::sleep};
use tokio_postgres::Transaction;
use ui::Ui;

use crate::{
    cli::{WorkloadArgs, WorkloadCommands},
    events::Message,
    pool::{Bundle, ClientHandle, ConnectionPool, PoolConfig},
    runner::WorkloadRunner,
};

pub mod counter;
pub mod onekib;
pub mod tiny;
pub mod tpcb;
pub mod ui;

pub struct Inserts {
    pub rows_inserted: usize,
    pub logical_bytes_written: usize,
}

#[async_trait]
pub trait Workload {
    type T;

    async fn setup(&self, client: ClientHandle, tx: mpsc::Sender<Message>) -> Result<()>;
    async fn transaction(
        &self,
        transaction: &Transaction<'_>,
        tx: mpsc::Sender<Message>,
    ) -> Result<Self::T>;
}

pub async fn run_load_generator(
    identifier: String,
    sdk_config: SdkConfig,
    args: WorkloadArgs,
) -> Result<()> {
    let (workload, executor) = args.workload.build();

    let (tx, rx) = mpsc::channel(1000);
    let mut ui = Ui::new(rx);

    let mut config = tokio_postgres::Config::new();
    config.host(args.endpoint(&identifier, &sdk_config)?);
    config.user(args.user);
    config.dbname("postgres");
    config.ssl_mode(tokio_postgres::config::SslMode::Require);
    config.ssl_negotiation(tokio_postgres::config::SslNegotiation::Direct);

    let concurrency =
        NonZero::new(args.concurrency).ok_or_else(|| anyhow!("concurrency must be non-zero"))?;

    tracing::info!("launching pool");
    ui.on_before_pool_create();
    let rate_limiter = RateLimiter::new(10);
    rate_limiter.burst(1000);

    let (pool, mut telemetry) = ConnectionPool::launch(
        Bundle::new_with_sdk_config(config, sdk_config)?,
        PoolConfig {
            desired: concurrency,
            concurrent: concurrency,
            rate_limiter,
        },
    )
    .await?;
    // XXX: For UI testing.
    // sleep(Duration::from_secs(3)).await;
    ui.on_after_pool_create();

    let _telem = tx.clone();
    tokio::spawn(async move {
        while let Some(t) = telemetry.recv().await {
            _telem.send(Message::PoolTelemetry(t)).await?;
        }
        anyhow::Ok(())
    });

    // Create the workload runner
    let runner = WorkloadRunner::new(
        pool,
        executor,
        concurrency,
        args.batches,
        tx.clone(),
        args.always_rollback,
    )
    .await?;

    ui.on_before_setup();
    let conn = runner.pool.borrow().await?;
    workload.setup(conn, tx.clone()).await?;
    ui.on_after_setup();

    if args.setup_only {
        return Ok(());
    }

    // Run in headless mode
    let rows_per_tx = match &args.workload {
        WorkloadCommands::Tiny(tiny_args) => Some(tiny_args.rows_per_transaction),
        WorkloadCommands::OneKib(onekib_args) => Some(onekib_args.rows_per_transaction),
        WorkloadCommands::Counter(_) => None,
        WorkloadCommands::Tpcb(_) => Some(4), // FIXME: Make this automatically correctly
    };

    let workload_name = match &args.workload {
        WorkloadCommands::Tiny(_) => "tiny".to_string(),
        WorkloadCommands::OneKib(_) => "onekib".to_string(),
        WorkloadCommands::Counter(_) => "counter".to_string(),
        WorkloadCommands::Tpcb(_) => "tcpb".to_string(),
    };

    // Close the message channel to signal all background tasks to exit
    drop(tx);

    // Give background tasks more time to clean up (network connections need time)
    sleep(Duration::from_millis(1000)).await;

    Ok(())
}
