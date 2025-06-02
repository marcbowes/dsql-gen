use std::{
    ops,
    time::{Duration, SystemTime},
};

use anyhow::{Result, anyhow};
use aws_config::SdkConfig;
use aws_sdk_cloudwatch::{Client as CloudWatchClient, types::Dimension};
use byte_unit::{Byte, Unit};
use chrono::{Datelike, Timelike, Utc};
use tokio::{sync::watch, time::sleep};

/// Calculates usage based on CloudWatch metrics.
///
/// CloudWatch usage metrics are delayed, and there is no guarantee that the
/// usage metrics will precisely match with billing. This is the best way to
/// watch your estimated usage in real time.
pub struct UsageCalculator {
    cluster_id: String,
    cloudwatch_client: CloudWatchClient,
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct DpuMetrics {
    pub total: f64,
    pub compute: f64,
    pub read: f64,
    pub write: f64,
}

impl ops::Sub for DpuMetrics {
    type Output = DpuMetrics;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total: self.total - rhs.total,
            compute: self.compute - rhs.compute,
            read: self.read - rhs.read,
            write: self.write - rhs.write,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct DpuCost {
    pub total: f64,
    pub compute: f64,
    pub read: f64,
    pub write: f64,
}

impl ops::Sub for DpuCost {
    type Output = DpuCost;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total: self.total - rhs.total,
            compute: self.compute - rhs.compute,
            read: self.read - rhs.read,
            write: self.write - rhs.write,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct StorageMetrics {
    pub size_bytes: f64,
}

impl ops::Sub for StorageMetrics {
    type Output = StorageMetrics;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            size_bytes: self.size_bytes - rhs.size_bytes,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct StorageCost {
    pub gb_month: f64,
}

impl ops::Sub for StorageCost {
    type Output = StorageCost;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            gb_month: self.gb_month - rhs.gb_month,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Usage {
    pub dpu_metrics: DpuMetrics,
    pub storage_metrics: StorageMetrics,
    pub cost_estimate: CostEstimate,
}

impl ops::Sub for Usage {
    type Output = Usage;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            dpu_metrics: self.dpu_metrics - rhs.dpu_metrics,
            storage_metrics: self.storage_metrics - rhs.storage_metrics,
            cost_estimate: self.cost_estimate - rhs.cost_estimate,
        }
    }
}

impl Usage {
    pub fn set_dpu_metrics(&mut self, updated: DpuMetrics) -> bool {
        if self.dpu_metrics == updated {
            return false;
        }

        self.dpu_metrics = updated;
        self.recalculate();
        true
    }

    pub fn set_storage_metrics(&mut self, updated: StorageMetrics) -> bool {
        if self.storage_metrics == updated {
            return false;
        }

        self.storage_metrics = updated;
        self.recalculate();
        true
    }

    fn recalculate(&mut self) {
        self.cost_estimate = calculate_costs(&self.dpu_metrics, &self.storage_metrics);
    }
}

/// An estimate of cost, in dollars.
///
/// !! WARNING !!
///
/// This estimate should not be depended upon for real world calculations. It is
/// written entirely for the purpose of this simulator.
///
/// In this program, data is being ingested. We look at the total number of DPUs
/// consumed in a window of time and the _current_ storage size. The assumption
/// is we're loading data quickly, then leaving the data set alone. And so the
/// storage cost will be dominated by the latest datapoint stored for the rest
/// of the month.
///
/// If you want to adapt this calculator for your own purpose, please make sure
/// you understand pricing, and your use-case.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct CostEstimate {
    pub total_dpus: DpuCost,
    pub latest_storage: StorageCost,
}

impl ops::Sub for CostEstimate {
    type Output = CostEstimate;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            total_dpus: self.total_dpus - rhs.total_dpus,
            latest_storage: self.latest_storage - rhs.latest_storage,
        }
    }
}

impl UsageCalculator {
    pub fn new(cluster_id: String, sdk_config: &SdkConfig) -> Self {
        let cloudwatch_client = CloudWatchClient::new(sdk_config);

        Self {
            cluster_id,
            cloudwatch_client,
        }
    }

    pub async fn dpus_this_month(&self) -> Result<DpuMetrics> {
        let (start_time, end_time) = this_month_to_now();
        self.get_dpu_metrics(&start_time, &end_time).await
    }

    pub async fn current_storage_usage(&self) -> Result<StorageMetrics> {
        let end = Utc::now();
        let start = end - chrono::Duration::hours(1);

        let dimension = Dimension::builder()
            .name("ClusterId")
            .value(&self.cluster_id)
            .build();

        let response = self
            .cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/AuroraDSQL")
            .metric_name("ClusterStorageSize")
            .dimensions(dimension)
            .start_time(SystemTime::from(start).into())
            .end_time(SystemTime::from(end).into())
            .period(60)
            .statistics(aws_sdk_cloudwatch::types::Statistic::Average)
            .send()
            .await?;

        let size_bytes = response
            .datapoints()
            .iter()
            .filter_map(|datapoint| {
                if let Some(timestamp) = datapoint.timestamp() {
                    if let Some(average) = datapoint.average() {
                        return Some((timestamp, average));
                    }
                }
                None
            })
            .max_by(|a, b| a.0.cmp(b.0))
            .map(|(_, avg)| avg);

        Ok(StorageMetrics {
            size_bytes: size_bytes.ok_or_else(|| anyhow!("no datapoints"))?,
        })
    }

    async fn get_dpu_metrics(
        &self,
        start_time: &SystemTime,
        end_time: &SystemTime,
    ) -> Result<DpuMetrics> {
        let total_dpu = self
            .get_dpu_metric("TotalDPU", start_time, end_time)
            .await?;
        let compute_dpu = self
            .get_dpu_metric("ComputeDPU", start_time, end_time)
            .await?;
        let read_dpu = self.get_dpu_metric("ReadDPU", start_time, end_time).await?;
        let write_dpu = self
            .get_dpu_metric("WriteDPU", start_time, end_time)
            .await?;

        Ok(DpuMetrics {
            total: total_dpu,
            compute: compute_dpu,
            read: read_dpu,
            write: write_dpu,
        })
    }

    async fn get_dpu_metric(
        &self,
        metric_name: &str,
        start_time: &SystemTime,
        end_time: &SystemTime,
    ) -> Result<f64> {
        let dimension = Dimension::builder()
            .name("ClusterId")
            .value(&self.cluster_id)
            .build();

        let response = self
            .cloudwatch_client
            .get_metric_statistics()
            .namespace("AWS/AuroraDSQL")
            .metric_name(metric_name)
            .dimensions(dimension)
            .start_time((*start_time).into())
            .end_time((*end_time).into())
            .period(86400) // 1 day period
            .statistics(aws_sdk_cloudwatch::types::Statistic::Sum)
            .send()
            .await?;

        let sum = response
            .datapoints()
            .iter()
            .filter_map(|datapoint| datapoint.sum())
            .sum::<f64>();

        Ok(sum)
    }

    /// Get the initial usage
    pub async fn get_initial_usage(&self) -> Result<Usage> {
        let mut dpus = None;
        let mut storage = None;

        loop {
            if let Ok(it) = self.dpus_this_month().await {
                dpus = Some(it)
            }
            if let Ok(it) = self.current_storage_usage().await {
                storage = Some(it)
            };

            if dpus.is_some() && storage.is_some() {
                break;
            }

            // XXX: New clusters will always have metrics due to the initial table
            // creation. Wait for them to come through.
            println!("no metrics yet, assuming this is a new cluster, retrying..");
            sleep(Duration::from_secs(30)).await;
        }

        let dpu_metrics = dpus.unwrap();
        let storage_metrics = storage.unwrap();
        let cost_estimate = calculate_costs(&dpu_metrics, &storage_metrics);
        Ok(Usage {
            dpu_metrics,
            storage_metrics,
            cost_estimate,
        })
    }

    pub fn spawn_monitor(self, initial_usage: Usage) -> watch::Receiver<Usage> {
        let calc = self;
        let (tx, rx) = watch::channel(initial_usage);

        tokio::spawn(async move {
            loop {
                if tx.receiver_count() == 0 {
                    break;
                }

                if let Ok(it) = calc.dpus_this_month().await {
                    tx.send_if_modified(move |usage| usage.set_dpu_metrics(it));
                }

                if let Ok(it) = calc.current_storage_usage().await {
                    tx.send_if_modified(move |usage| usage.set_storage_metrics(it));
                }

                sleep(Duration::from_secs(30)).await;
            }
        });

        rx
    }
}

/// Print usage information to the console
pub fn print(latest_usage: &Usage) {
    let storage = byte_unit::Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);
    let total_cost = latest_usage.cost_estimate.total_dpus.total
        + latest_usage.cost_estimate.latest_storage.gb_month;

    println!("\n{:=^70}", " Usage & Cost ");
    println!("{:<15} {:>15} {:>15}", "", "Usage", "Cost");
    println!("{:-<15} {:-^15} {:-^15}", "", "", "");
    println!(
        "{:<15} {:>15.2} {:>15}",
        "Total DPUs:",
        latest_usage.dpu_metrics.total,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.total)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Compute:",
        latest_usage.dpu_metrics.compute,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.compute)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Read:",
        latest_usage.dpu_metrics.read,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.read)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Write:",
        latest_usage.dpu_metrics.write,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.write)
    );
    println!(
        "{:<15} {:>15} {:>15}",
        "Storage:",
        storage.to_string(),
        format!("${:.2}", latest_usage.cost_estimate.latest_storage.gb_month)
    );
    println!("{:═<15} {:═^15} {:═^15}", "", "", "");
    println!(
        "{:<15} {:>15} {:>15}",
        "TOTAL COST:",
        "",
        format!("${:.2}", total_cost)
    );
    println!("{:=^70}", "");
}

/// Print usage information and difference to the console
pub fn print_with_diff(latest_usage: &Usage, usage_diff: &Usage) {
    let storage_current = byte_unit::Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);
    let storage_diff = byte_unit::Byte::from_u64(usage_diff.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);

    let total_cost_current = latest_usage.cost_estimate.total_dpus.total
        + latest_usage.cost_estimate.latest_storage.gb_month;
    let total_cost_diff = usage_diff.cost_estimate.total_dpus.total
        + usage_diff.cost_estimate.latest_storage.gb_month;

    println!("\n{:=^90}", " Usage & Cost ");
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "", "Usage", "Delta", "Cost", "Delta"
    );
    println!(
        "{:-<15} {:-^15} {:-^15} {:-^15} {:-^15}",
        "", "", "", "", ""
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "Total DPUs:",
        latest_usage.dpu_metrics.total,
        format!("(+{:.2})", usage_diff.dpu_metrics.total),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.total),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.total)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Compute:",
        latest_usage.dpu_metrics.compute,
        format!("(+{:.2})", usage_diff.dpu_metrics.compute),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.compute),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.compute)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Read:",
        latest_usage.dpu_metrics.read,
        format!("(+{:.2})", usage_diff.dpu_metrics.read),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.read),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.read)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Write:",
        latest_usage.dpu_metrics.write,
        format!("(+{:.2})", usage_diff.dpu_metrics.write),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.write),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.write)
    );
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "Storage:",
        storage_current.to_string(),
        format!("(+{})", storage_diff),
        format!("${:.2}", latest_usage.cost_estimate.latest_storage.gb_month),
        format!(
            "(+${:.2})",
            usage_diff.cost_estimate.latest_storage.gb_month
        )
    );
    println!(
        "{:═<15} {:═^15} {:═^15} {:═^15} {:═^15}",
        "", "", "", "", ""
    );
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "TOTAL COST:",
        "",
        "",
        format!("${:.2}", total_cost_current),
        format!("(+${:.2})", total_cost_diff)
    );
    println!("{:=^90}", "");
}

pub fn calculate_costs(dpu_metrics: &DpuMetrics, storage_info: &StorageMetrics) -> CostEstimate {
    let total_dpus = DpuCost {
        total: dpu_metrics.total * 8.0 / 1_000_000.0,
        compute: dpu_metrics.compute * 8.0 / 1_000_000.0,
        read: dpu_metrics.read * 8.9 / 1_000_000.0,
        write: dpu_metrics.write * 8.9 / 1_000_000.0,
    };

    let gb = Byte::from_u64(storage_info.size_bytes as u64)
        .get_adjusted_unit(Unit::GB)
        .get_value();

    let latest_storage = StorageCost {
        gb_month: gb * 0.33,
    };

    CostEstimate {
        total_dpus,
        latest_storage,
    }
}

fn this_month_to_now() -> (SystemTime, SystemTime) {
    _this_month_to_now().expect("there is always a start of the month")
}

fn _this_month_to_now() -> Option<(SystemTime, SystemTime)> {
    let now = Utc::now();
    let end_time = SystemTime::from(now);

    // Start time: first day of current month
    let start_time_chrono = now
        .with_day(1)?
        .with_hour(0)?
        .with_minute(0)?
        .with_second(0)?
        .with_nanosecond(0)?;
    let start_time = SystemTime::from(start_time_chrono);

    Some((start_time, end_time))
}
