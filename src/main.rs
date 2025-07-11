use std::fs::File;
use std::num::NonZero;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use clap::Parser;
use dsql_gen::events::Message;
use dsql_gen::workloads::counter::{Counter, CounterArgs};
use dsql_gen::workloads::onekib::{OneKibRows, OneKibRowsArgs};
use dsql_gen::workloads::tiny::{TinyRows, TinyRowsArgs};
use tokio::sync::mpsc;
use tokio::task::{self, JoinHandle};

use dsql_gen::runner::{InsertsExecutor, SharedExecutor, SharedWorkload, WorkloadRunner};
use dsql_gen::ui::{HeadlessMonitor, MonitorUI};
use dsql_gen::usage::{self, Usage, UsageCalculator};
use tracing::Level;
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

impl Args {
    async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        loader.load().await
    }
}

#[derive(Parser, Debug)]
enum Commands {
    /// Run the load generator
    Workload(WorkloadArgs),
    /// Print usage information for a cluster
    Usage(UsageArgs),
}

#[derive(Parser, Debug)]
struct WorkloadArgs {
    /// An optional endpoint override to use. The endpoint is usually
    /// automatically built from the identifier and region.
    #[arg(short, long)]
    endpoint: Option<String>,

    /// DB user
    #[arg(short, long, default_value = "admin")]
    user: String,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    batches: usize,

    /// Whether to watch CloudWatch metrics or not.
    #[arg(long, default_value_t = false)]
    no_usage: bool,

    /// Workload to run
    #[command(subcommand)]
    workload: WorkloadCommands,

    /// Always rollback transactions (used for specific testing scenarios)
    #[arg(long, default_value_t = false)]
    always_rollback: bool,

    /// Run without terminal UI, show final stats only
    #[arg(long, default_value_t = false)]
    no_ui: bool,
}

#[derive(Parser, Debug, Clone)]
enum WorkloadCommands {
    /// Run the tiny insert workload
    Tiny(TinyRowsArgs),
    /// Run the 1KiB insert workload
    OneKib(OneKibRowsArgs),
    /// Run the counter workload
    Counter(CounterArgs),
}

impl WorkloadCommands {
    fn build(&self) -> (SharedWorkload, SharedExecutor) {
        match self {
            WorkloadCommands::Tiny(args) => {
                let w = Arc::new(TinyRows::new(args.clone()));
                (w.clone(), Arc::new(InsertsExecutor(w)))
            }
            WorkloadCommands::OneKib(args) => {
                let w = Arc::new(OneKibRows::new(args.clone()));
                (w.clone(), Arc::new(InsertsExecutor(w)))
            }
            WorkloadCommands::Counter(args) => {
                let w = Arc::new(Counter::new(args.clone()));
                (w.clone(), Arc::new(InsertsExecutor(w)))
            }
        }
    }
}

impl WorkloadArgs {
    fn endpoint(&self, identifier: impl AsRef<str>, sdk_config: &SdkConfig) -> Result<String> {
        if let Some(endpoint) = &self.endpoint {
            return Ok(endpoint.clone());
        }

        let region = sdk_config
            .region()
            .ok_or_else(|| anyhow!("no region set"))?;
        Ok(format!(
            "{}.dsql.{}.on.aws",
            identifier.as_ref(),
            region.as_ref()
        ))
    }
}

#[derive(Parser, Debug)]
struct UsageArgs;

async fn run_load_generator(
    identifier: String,
    sdk_config: SdkConfig,
    args: WorkloadArgs,
) -> Result<()> {
    let (workload, executor) = args.workload.build();

    let (tx, rx) = mpsc::channel(1000);

    let calc = UsageCalculator::new(identifier.clone(), &sdk_config);

    // Create the workload runner
    let runner = WorkloadRunner::new(
        args.endpoint(&identifier, &sdk_config)?,
        args.user,
        sdk_config,
        workload,
        executor,
        NonZero::new(args.concurrency).ok_or_else(|| anyhow!("concurrency must be non-zero"))?,
        args.batches,
        tx.clone(),
        args.always_rollback,
    )
    .await?;

    let mut usage_task = None::<JoinHandle<Result<()>>>;

    if !args.no_usage {
        // make sure this is after the setup runs.
        let initial_usage = calc.get_initial_usage().await?;
        usage::print(&initial_usage);
        let watch_usage = calc.spawn_monitor(initial_usage);

        // notify the UI of usage updates.
        let tx_usage = tx.clone();
        let mut _watch_usage = watch_usage.clone();
        tx_usage.send(Message::InitialUsage(initial_usage)).await?;
        let send_usage_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            loop {
                _watch_usage.changed().await?;
                let usage = *_watch_usage.borrow();
                if tx_usage.send(Message::UsageUpdated(usage)).await.is_err() {
                    break; // Channel closed, exit loop
                }
            }
            Ok(())
        });
        usage_task = Some(send_usage_handle);
    }

    if args.no_ui {
        // Run in headless mode
        let mut headless = HeadlessMonitor::new(rx);
        headless.run(runner).await?;
        headless.print_final_stats();

        // Cleanup background tasks
        if let Some(task) = usage_task {
            task.abort();
        }

        // Close the message channel to signal all background tasks to exit
        drop(tx);

        // Give background tasks more time to clean up (network connections need time)
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    } else {
        // Start the UI in a separate task
        let ui = task::spawn(async move {
            let mut ui = MonitorUI::new(rx)?;
            ui.run(runner).await?;
            anyhow::Ok(())
        });

        if let Err(err) = ui.await? {
            eprintln!("{err:?}");
        }

        // Cleanup background tasks
        if let Some(task) = usage_task {
            task.abort();
        }

        // Close the message channel to signal all background tasks to exit
        drop(tx);

        // Give background tasks more time to clean up (network connections need time)
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    }

    // if args.usage {
    //     println!("waiting for final usage");
    //     sleep(Duration::from_secs(90)).await;

    //     let final_usage = *watch_usage.borrow();
    //     let usage_diff = final_usage - initial_usage;
    //     usage::print_with_diff(&final_usage, &usage_diff);
    // }

    Ok(())
}

async fn print_cluster_usage(
    identifier: String,
    sdk_config: SdkConfig,
    _args: UsageArgs,
) -> anyhow::Result<()> {
    let cal = UsageCalculator::new(identifier.clone(), &sdk_config);

    println!("Fetching usage information for cluster: {identifier}");

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
    let sdk_config = args.sdk_config().await;

    match args.command {
        Commands::Workload(run_args) => {
            run_load_generator(args.identifier, sdk_config, run_args).await?
        }
        Commands::Usage(print_args) => {
            print_cluster_usage(args.identifier, sdk_config, print_args).await?
        }
    }

    Ok(())
}
