use std::fs::File;
use std::num::NonZero;
use std::time::Duration;

use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use clap::Parser;
use dsql_gen::events::Message;
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use dsql_gen::runner::WorkloadRunner;
use dsql_gen::ui::MonitorUI;
use dsql_gen::usage::{self, Usage, UsageCalculator};
use tokio::time::sleep;
use tracing::Level;
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Run the load generator
    Run(RunArgs),
    /// Print usage information for a cluster
    Usage(UsageArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: Option<String>,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    batches: usize,

    /// Workload name
    #[arg(short, long)]
    workload: String,

    /// Workload rows
    #[arg(short = 'n', long, default_value_t = 0)]
    rows: usize,
}

impl RunArgs {
    fn endpoint(&self, sdk_config: &SdkConfig) -> Result<String> {
        let region = sdk_config
            .region()
            .ok_or_else(|| anyhow!("no region set"))?;
        Ok(format!(
            "{}.dsql.{}.on.aws",
            self.identifier,
            region.as_ref()
        ))
    }

    async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        loader.load().await
    }
}

impl UsageArgs {
    async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        loader.load().await
    }
}

#[derive(Parser, Debug)]
struct UsageArgs {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: Option<String>,
}

async fn run_load_generator(args: RunArgs) -> Result<()> {
    let sdk_config = args.sdk_config().await;

    if args.rows == 0 {
        println!("setup complete, no rows requested");
        return Ok(());
    }

    let (tx, rx) = mpsc::channel(1000);

    let calc = UsageCalculator::new(args.identifier.clone(), &sdk_config);

    // Create the workload runner
    let runner = WorkloadRunner::new(
        args.endpoint(&sdk_config)?,
        sdk_config,
        args.workload,
        args.rows,
        NonZero::new(args.concurrency).ok_or_else(|| anyhow!("concurrency must be non-zero"))?,
        args.batches,
        tx.clone(),
    )
    .await?;

    // make sure this is after the setup runs.
    let initial_usage = calc.get_initial_usage().await?;
    usage::print(&initial_usage);
    let watch_usage = calc.spawn_monitor(initial_usage);

    // notify the UI of usage updates.
    let tx_usage = tx.clone();
    let mut _watch_usage = watch_usage.clone();
    tx_usage.send(Message::InitialUsage(initial_usage)).await?;
    let _send_usage: JoinHandle<Result<()>> = tokio::spawn(async move {
        loop {
            _watch_usage.changed().await?;
            let usage = *_watch_usage.borrow();
            tx_usage.send(Message::UsageUpdated(usage)).await?;
        }
    });

    // Start the UI in a separate task
    let ui = task::spawn(async move {
        let mut ui = MonitorUI::new(rx)?;
        ui.run(runner).await?;
        anyhow::Ok(())
    });

    if let Err(err) = ui.await? {
        eprintln!("{err:?}");
    }

    println!("waiting for final usage");
    sleep(Duration::from_secs(90)).await;
    let final_usage = *watch_usage.borrow();

    let usage_diff = final_usage - initial_usage;
    usage::print_with_diff(&final_usage, &usage_diff);

    Ok(())
}

async fn print_cluster_usage(args: UsageArgs) -> anyhow::Result<()> {
    let sdk_config = args.sdk_config().await;
    let cal = UsageCalculator::new(args.identifier.clone(), &sdk_config);

    println!(
        "Fetching usage information for cluster: {}",
        args.identifier
    );

    let dpu_metrics = cal.dpus_this_month().await?;
    let storage_metrics = cal.current_storage_usage().await?;
    let cost_estimate = usage::calculate_costs(&dpu_metrics, &storage_metrics);
    let usage = Usage {
        dpu_metrics,
        storage_metrics,
        cost_estimate,
    };

    usage::print(&usage);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let file = File::create("/tmp/dsql-gen.log")?;
    let (non_blocking, _guard) = NonBlocking::new(file);

    let env_filter = EnvFilter::builder()
        .with_default_directive(Level::INFO.into())
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_writer(non_blocking)
        .with_env_filter(env_filter)
        .init();

    let args = Args::parse();

    match args.command {
        Commands::Run(run_args) => run_load_generator(run_args).await?,
        Commands::Usage(print_args) => print_cluster_usage(print_args).await?,
    }

    Ok(())
}
