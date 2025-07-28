use std::num::NonZero;

use anyhow::{Result, anyhow};
use async_rate_limiter::RateLimiter;
use async_trait::async_trait;
use aws_config::SdkConfig;
use tokio::sync::mpsc;
use tokio_postgres::Transaction;
use ui::Ui;

use crate::{
    cli::WorkloadArgs,
    events::Message,
    pool::{Bundle, ConnectionPool, PoolConfig},
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

    async fn setup(&self, pool: ConnectionPool, tx: mpsc::Sender<Message>) -> Result<()>;
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
    config.user(&args.user);
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
        pool.clone(),
        executor,
        concurrency,
        args.batches,
        tx.clone(),
        args.always_rollback,
    )
    .await?;

    ui.on_before_setup();
    workload.setup(pool, tx).await?;
    ui.on_after_setup();

    if args.setup_only {
        return Ok(());
    }

    ui.run(runner, args).await?;

    Ok(())
}
