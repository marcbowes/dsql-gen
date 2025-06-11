//! TUI components for the DSQL load generator monitor
//!
//! This module contains all the UI components for the terminal user interface,
//! organized as separate modules for each section of the UI.

use std::{collections::VecDeque, time::SystemTime};

use hdrhistogram::Histogram;
use ratatui::prelude::*;

pub mod errors;
pub mod latency;
pub mod performance;
pub mod progress;
pub mod usage_cost;

use errors::{ErrorState, ErrorWidget};
use latency::{LatencyState, LatencyWidget};
use performance::{PerformanceState, PerformanceWidget};
use progress::{ProgressWidget, ProgressWidgetState};
use usage_cost::{UsageCostState, UsageCostWidget};

use crate::runner::WorkloadRunner;

/// The main model containing all UI state
pub struct Model {
    pub runner: WorkloadRunner,
    pub metrics: Metrics,
    pub progress_pct: f64,
    pub usage_cost: UsageCostState,
    pub progress: ProgressWidgetState,
    pub latency_state: LatencyState,
    pub performance_state: PerformanceState,
    pub error_state: ErrorState,
}

impl Model {
    pub fn new(runner: WorkloadRunner) -> Self {
        Self {
            runner,
            metrics: Metrics::default(),
            progress_pct: 0.0,
            usage_cost: UsageCostState::default(),
            progress: ProgressWidgetState {
                completed: 0,
                total: 0,
                pct: 0.0,
            },
            latency_state: LatencyState::new(),
            performance_state: PerformanceState::new(),
            error_state: ErrorState::new(),
        }
    }
}

#[derive(Clone)]
pub struct ErrorEntry {
    pub timestamp: SystemTime,
    pub message: String,
}

#[derive(Clone)]
pub struct Metrics {
    pub completed_batches: usize,
    pub completed_since_last_tick: usize,
    pub error_count: usize,
    pub errors_since_last_tick: usize,
    pub last_errors: VecDeque<ErrorEntry>,
    pub latency_histogram: Histogram<u64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            completed_batches: 0,
            completed_since_last_tick: 0,
            error_count: 0,
            errors_since_last_tick: 0,
            last_errors: VecDeque::with_capacity(5),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
        }
    }
}

impl Metrics {
    pub fn get_and_reset(&mut self) -> (usize, usize, Histogram<u64>) {
        let completed = self.completed_since_last_tick;
        let errors = self.errors_since_last_tick;
        self.completed_since_last_tick = 0;
        self.errors_since_last_tick = 0;
        let histogram = self.latency_histogram.clone();
        self.latency_histogram.reset();
        (completed, errors, histogram)
    }
}

/// The main UI function that renders all components
pub fn draw(f: &mut Frame, model: &mut Model) {
    // Calculate available height after fixed sections
    let total_height = f.area().height.saturating_sub(2); // Account for margin
    let fixed_height = 3 + 12 + 12 + 1; // Progress + Errors + Usage & Cost + Quit message
    let remaining_height = total_height.saturating_sub(fixed_height);

    // Split remaining height between Performance and Latency
    let perf_latency_height = remaining_height / 2;

    // Error if not enough space for charts
    if perf_latency_height < 12 {
        // Render error message if terminal too small
        let error_msg = format!(
            "Terminal too small! Need at least {} lines, have {}",
            fixed_height + 24, // 24 = 12 lines each for perf and latency
            total_height
        );
        let error_para = ratatui::widgets::Paragraph::new(error_msg)
            .block(
                ratatui::widgets::Block::default()
                    .title("Error")
                    .borders(ratatui::widgets::Borders::ALL),
            )
            .style(Style::default().fg(Color::Red));
        error_para.render(f.area(), f.buffer_mut());
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),                   // Progress
            Constraint::Length(perf_latency_height), // Performance (dynamic)
            Constraint::Length(perf_latency_height), // Latency (dynamic)
            Constraint::Length(12),                  // Errors
            Constraint::Length(12),                  // Usage & Cost
            Constraint::Length(1),                   // Quit message
        ])
        .split(f.area());

    // Render each component
    f.render_stateful_widget(ProgressWidget, chunks[0], &mut model.progress);

    // Use the new stateful performance widget with chart
    let performance_widget = PerformanceWidget::new(&model.performance_state);
    performance_widget.render_with_chart(chunks[1], f.buffer_mut());

    // Use the stateful latency widget with chart
    let latency_widget = LatencyWidget::new(&model.latency_state);
    latency_widget.render_with_chart(chunks[2], f.buffer_mut());

    // Use the new stateful error widget with chart
    let error_widget = ErrorWidget::new(&model.error_state);
    error_widget.render_with_chart(chunks[3], f.buffer_mut());

    f.render_stateful_widget(UsageCostWidget, chunks[4], &mut model.usage_cost);

    // Render quit message at the bottom without borders
    let quit_msg = ratatui::widgets::Paragraph::new(
        "Press 'q' or ESC to quit", // | Press 'w' for workload settings
    )
    .style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::ITALIC),
    )
    .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(quit_msg, chunks[5]);
}
