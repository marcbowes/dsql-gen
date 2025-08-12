use std::sync::Arc;

use anyhow::{Result, anyhow};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use clap::Parser;

use crate::{
    runner::{InsertsExecutor, SharedExecutor, SharedWorkload},
    workloads::{
        counter::{Counter, CounterArgs},
        onekib::{OneKibRows, OneKibRowsArgs},
        tiny::{TinyRows, TinyRowsArgs},
        tpcb::{Tpcb, TpcbArgs},
    },
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    pub identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    pub region: Option<String>,

    /// AWS profile
    #[arg(short, long, env = "AWS_PROFILE")]
    pub profile: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

impl Args {
    pub async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        if let Some(p) = &self.profile {
            loader = loader.profile_name(p);
        }
        loader.load().await
    }
}

#[derive(Parser, Debug)]
pub enum Commands {
    /// Run the load generator
    Workload(WorkloadArgs),
    /// Print usage information for a cluster
    Usage(UsageArgs),
}

#[derive(Parser, Debug)]
pub struct WorkloadArgs {
    /// An optional endpoint override to use. The endpoint is usually
    /// automatically built from the identifier and region.
    #[arg(short, long)]
    pub endpoint: Option<String>,

    /// DB user
    #[arg(short, long, default_value = "admin")]
    pub user: String,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    pub concurrency: usize,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    pub batches: usize,

    /// Whether to watch CloudWatch metrics or not.
    #[arg(long, default_value_t = false)]
    pub no_usage: bool,

    /// Whether to quit after running the setup code
    #[arg(long, default_value_t = false)]
    pub setup_only: bool,

    /// Workload to run
    #[command(subcommand)]
    pub workload: WorkloadCommands,

    /// Always rollback transactions (used for specific testing scenarios)
    #[arg(long, default_value_t = false)]
    pub always_rollback: bool,
}

#[derive(Parser, Debug, Clone)]
pub enum WorkloadCommands {
    /// Run the tiny insert workload
    Tiny(TinyRowsArgs),
    /// Run the 1KiB insert workload
    OneKib(OneKibRowsArgs),
    /// Run the counter workload
    Counter(CounterArgs),
    /// Run the TPC-B workload
    Tpcb(TpcbArgs),
}

impl WorkloadCommands {
    pub fn build(&self) -> (SharedWorkload, SharedExecutor) {
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
            WorkloadCommands::Tpcb(args) => {
                let w = Arc::new(Tpcb::new(args.clone()));
                (w.clone(), Arc::new(InsertsExecutor(w)))
            }
        }
    }
}

impl WorkloadArgs {
    pub fn endpoint(&self, identifier: impl AsRef<str>, sdk_config: &SdkConfig) -> Result<String> {
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
pub struct UsageArgs;
