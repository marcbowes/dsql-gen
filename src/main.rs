use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use clap::Parser;
use dsql_gen::runner::{WorkloadRunner, print_usage, print_usage_and_diff};
use dsql_gen::ui::MonitorUI;
use dsql_gen::usage::{self, Usage, UsageCalculator};
use tokio::sync::mpsc;
use tokio::task;

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
    concurrency: u32,

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

    // Create the workload runner
    let runner = WorkloadRunner::new(
        args.identifier.clone(),
        args.endpoint(&sdk_config)?,
        sdk_config,
        args.workload,
        args.rows,
        args.concurrency,
        args.batches,
    )
    .await?;

    print_usage(&runner.initial_usage);

    // Create channels for coordination
    let (ui_done_tx, mut ui_done_rx) = mpsc::channel::<()>(1);
    let (workload_done_tx, mut workload_done_rx) = mpsc::channel::<()>(1);

    // Start the workload in a separate task
    let runner_clone = runner.clone();
    let concurrency = args.concurrency;
    let batches = args.batches;
    let workload_handle = task::spawn(async move {
        runner_clone.run(concurrency, batches).await?;
        let _ = workload_done_tx.send(()).await;
        Ok::<(), anyhow::Error>(())
    });

    // Start the UI in a separate task
    let runner_clone = runner.clone();
    let ui_handle = task::spawn(async move {
        let mut ui = MonitorUI::new()?;
        ui.run(&runner_clone).await?;
        let _ = ui_done_tx.send(()).await;
        Ok::<(), anyhow::Error>(())
    });

    // Wait for either the workload to complete or the UI to be closed
    tokio::select! {
        _ = ui_done_rx.recv() => {
            // UI was closed, abort the workload
            workload_handle.abort();
        }
        _ = workload_done_rx.recv() => {
            // Workload completed normally
        }
    }

    // Wait for the UI to close if it hasn't already
    let _ = ui_handle.await;

    // Cleanup and get final usage
    let (initial_usage, final_usage) = runner.cleanup().await?;
    
    let usage_diff = final_usage - initial_usage;
    print_usage_and_diff(&final_usage, &usage_diff);

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

    print_usage(&usage);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Run(run_args) => run_load_generator(run_args).await?,
        Commands::Usage(print_args) => print_cluster_usage(print_args).await?,
    }

    Ok(())
}
