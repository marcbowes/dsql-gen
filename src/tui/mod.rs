//! TUI components for the DSQL load generator monitor
//!
//! This module contains all the UI components for the terminal user interface,
//! organized as separate modules for each section of the UI.

pub mod progress;
pub mod performance;
pub mod latency;
pub mod usage_cost;
pub mod errors;

// Re-export commonly used types
pub use progress::ProgressWidget;
pub use performance::PerformanceWidget;
pub use latency::{LatencyWidget, StatefulLatencyWidget};
pub use usage_cost::UsageCostWidget;
pub use errors::ErrorsWidget;

use ratatui::prelude::*;
use std::collections::VecDeque;
use std::default::Default;
use crate::runner::MetricsInner;
use crate::usage::{Usage, DpuMetrics, StorageMetrics, CostEstimate, DpuCost, StorageCost};
use hdrhistogram::Histogram;

// Default implementations for Usage-related types
impl Default for DpuMetrics {
    fn default() -> Self {
        Self {
            total: 0.0,
            compute: 0.0,
            read: 0.0,
            write: 0.0,
        }
    }
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            size_bytes: 0.0,
        }
    }
}

impl Default for DpuCost {
    fn default() -> Self {
        Self {
            total: 0.0,
            compute: 0.0,
            read: 0.0,
            write: 0.0,
        }
    }
}

impl Default for StorageCost {
    fn default() -> Self {
        Self {
            gb_month: 0.0,
        }
    }
}

impl Default for CostEstimate {
    fn default() -> Self {
        Self {
            total_dpus: DpuCost::default(),
            latest_storage: StorageCost::default(),
        }
    }
}

impl Default for Usage {
    fn default() -> Self {
        Self {
            dpu_metrics: DpuMetrics::default(),
            storage_metrics: StorageMetrics::default(),
            cost_estimate: CostEstimate::default(),
        }
    }
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            completed_batches: 0,
            completed_since_last_tick: 0,
            error_count: 0,
            last_errors: VecDeque::with_capacity(3),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
        }
    }
}

/// The main model containing all UI state
#[derive(Clone)]
pub struct Model {
    pub metrics: MetricsInner,
    pub total_batches: usize,
    pub progress_pct: f64,
    pub tps: f64,
    pub rps: f64,
    pub bps_formatted: String,
    pub pool_size: usize,
    pub pool_idle: usize,
    pub latest_latency_histogram: Histogram::<u64>,
    pub latest_usage: Usage,
    pub usage_diff: Usage,
    pub usage_diff_from_start: Usage,
    pub tps_history: VecDeque<f64>,
    pub latency_histogram_history: VecDeque<Histogram::<u64>>, // Store full histograms
    pub error_history: VecDeque<f64>, // errors per second
}

impl Model {
    /// Create a new model with default values
    pub fn new(total_batches: usize) -> Self {
        Self {
            metrics: MetricsInner::default(),
            total_batches,
            progress_pct: 0.0,
            tps: 0.0,
            rps: 0.0,
            bps_formatted: "0 B".to_string(),
            pool_size: 0,
            pool_idle: 0,
            latest_latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
            latest_usage: Usage::default(),
            usage_diff: Usage::default(),
            usage_diff_from_start: Usage::default(),
            tps_history: VecDeque::with_capacity(300), // 5 minutes at 1s intervals
            latency_histogram_history: VecDeque::with_capacity(300),
            error_history: VecDeque::with_capacity(300),
        }
    }
}

/// The main UI function that renders all components
pub fn draw(f: &mut Frame, model: &Model) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),  // Progress
            Constraint::Length(6),  // Performance
            Constraint::Length(12), // Latency (increased for better chart visibility)
            Constraint::Length(12), // Usage & Cost
            Constraint::Min(0),     // Errors
        ])
        .split(f.area());

    // Render each component
    f.render_widget(ProgressWidget::new(model), chunks[0]);
    f.render_widget(PerformanceWidget::new(model), chunks[1]);
    
    // Use the stateful latency widget with chart
    let latency_widget = StatefulLatencyWidget::new(model);
    latency_widget.render_with_chart(chunks[2], f.buffer_mut());
    
    f.render_widget(UsageCostWidget::new(model), chunks[3]);
    f.render_widget(ErrorsWidget::new(model), chunks[4]);
}
