use std::fs::File;

use anyhow::Result;
use aws_config::SdkConfig;
use clap::Parser;
use dsql_gen::cli::{Args, Commands, UsageArgs};
use dsql_gen::workloads;

use dsql_gen::usage::{self, Usage, UsageCalculator};
use tracing::Level;
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::EnvFilter;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
            workloads::run_load_generator(args.identifier, sdk_config, run_args).await?
        }
        Commands::Usage(print_args) => {
            print_cluster_usage(args.identifier, sdk_config, print_args).await?
        }
    }

    Ok(())
}
