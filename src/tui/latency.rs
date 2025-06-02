//! Latency stats component for the TUI

use std::time::Duration;

use hdrhistogram::Histogram;
use ratatui::{
    prelude::*,
    symbols,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
};

use crate::history::{
    TimestampedHistory,
    bucketing::{BucketConfig, bucket_data},
};

/// State for the latency widget containing its history data
#[derive(Clone)]
pub struct LatencyState {
    pub latest_latency_histogram: Histogram<u64>,
    pub latency_histogram_history: TimestampedHistory<Histogram<u64>>,
}

impl Default for LatencyState {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyState {
    /// Create a new latency state with default values
    pub fn new() -> Self {
        Self {
            latest_latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
            latency_histogram_history: TimestampedHistory::new(Duration::from_secs(300)),
        }
    }

    /// Update the latency state with a new histogram
    pub fn update(&mut self, histogram: Histogram<u64>) {
        self.latest_latency_histogram = histogram.clone();
        self.latency_histogram_history.push(histogram);
    }

    /// Record an individual latency value
    pub fn record(&mut self, latency_ms: u64) {
        self.latest_latency_histogram
            .record(latency_ms)
            .expect("latency value should be within histogram bounds");

        // Clone the current histogram and add it to the history
        let histogram_clone = self.latest_latency_histogram.clone();
        self.latency_histogram_history.push(histogram_clone);
    }
}

/// Stateful latency widget that can render both simple stats and charts
pub struct LatencyWidget<'a> {
    state: &'a LatencyState,
}

impl<'a> LatencyWidget<'a> {
    /// Create a new latency widget with the given state
    pub fn new(state: &'a LatencyState) -> Self {
        Self { state }
    }

    /// Render with split layout for chart integration
    pub fn render_with_chart(&self, area: Rect, buf: &mut Buffer) {
        // Split horizontally: left for stats, right for chart
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30), // Fixed width for stats (consistent with performance)
                Constraint::Min(40),    // Remaining width for chart
            ])
            .split(area);

        // Render stats on the left
        self.render_stats(chunks[0], buf);

        // Render chart on the right
        self.render_chart(chunks[1], buf);
    }

    fn render_stats(&self, area: Rect, buf: &mut Buffer) {
        let p50 = self.state.latest_latency_histogram.value_at_quantile(0.50) as f64;
        let p90 = self.state.latest_latency_histogram.value_at_quantile(0.90) as f64;
        let p99 = self.state.latest_latency_histogram.value_at_quantile(0.99) as f64;
        let p999 = self.state.latest_latency_histogram.value_at_quantile(0.999) as f64;

        let latency_text = [
            format!("p50:   {:.1} ms", p50),
            format!("p90:   {:.1} ms", p90),
            format!("p99:   {:.1} ms", p99),
            format!("p99.9: {:.1} ms", p999),
        ]
        .join("\n");

        let latency_para = Paragraph::new(latency_text)
            .block(Block::default().title("Latency").borders(Borders::ALL));

        latency_para.render(area, buf);
    }

    fn render_chart(&self, area: Rect, buf: &mut Buffer) {
        if self.state.latency_histogram_history.is_empty() {
            // No data yet, just render empty block
            let block = Block::default()
                .title("Latency Chart (5 min)")
                .borders(Borders::ALL);
            block.render(area, buf);
            return;
        }

        // Use bucketing to get p50 and p99 data over time
        let config = BucketConfig::new(Duration::from_secs(1), 300); // 300 seconds = 5 minutes

        // Get p50 buckets
        let p50_buckets = bucket_data(
            self.state.latency_histogram_history.data(),
            config.clone(),
            |histograms| {
                if histograms.is_empty() {
                    0.0
                } else {
                    // Take the most recent histogram in the bucket
                    let latest_histogram = histograms.last().unwrap();
                    latest_histogram.value_at_quantile(0.50) as f64
                }
            },
            0.0, // default value for empty buckets
        );

        // Get p99 buckets
        let p99_buckets = bucket_data(
            self.state.latency_histogram_history.data(),
            config,
            |histograms| {
                if histograms.is_empty() {
                    0.0
                } else {
                    // Take the most recent histogram in the bucket
                    let latest_histogram = histograms.last().unwrap();
                    latest_histogram.value_at_quantile(0.99) as f64
                }
            },
            0.0, // default value for empty buckets
        );

        // Prepare data for chart
        let mut p50_data = Vec::with_capacity(p50_buckets.len());
        let mut p99_data = Vec::with_capacity(p99_buckets.len());

        // Calculate max value for Y-axis scaling
        let mut max_latency: f64 = 0.0;

        for (i, (&p50, &p99)) in p50_buckets.iter().zip(p99_buckets.iter()).enumerate() {
            let x = i as f64;
            p50_data.push((x, p50));
            p99_data.push((x, p99));
            max_latency = max_latency.max(p50).max(p99);
        }

        // Add some padding to max value
        max_latency = (max_latency * 1.1).max(10.0_f64);

        // Create datasets
        let datasets = vec![
            Dataset::default()
                .name("p50")
                .marker(symbols::Marker::Dot)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Cyan))
                .data(&p50_data),
            Dataset::default()
                .name("p99")
                .marker(symbols::Marker::Dot)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Yellow))
                .data(&p99_data),
        ];

        // Calculate X-axis labels (time)
        let x_labels = vec![
            Span::raw("-5m"),
            Span::raw("-4m"),
            Span::raw("-3m"),
            Span::raw("-2m"),
            Span::raw("-1m"),
            Span::raw("now"),
        ];

        // Calculate Y-axis labels
        let y_labels = vec![
            Span::raw("0"),
            Span::raw(format!("{:.0}", max_latency / 2.0)),
            Span::raw(format!("{:.0}", max_latency)),
        ];

        // Create chart
        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title("Latency Chart (5 min)")
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .title("Time")
                    .style(Style::default().fg(Color::Gray))
                    .labels(x_labels)
                    .bounds([0.0, 300.0]),
            )
            .y_axis(
                Axis::default()
                    .title("Latency (ms)")
                    .style(Style::default().fg(Color::Gray))
                    .labels(y_labels)
                    .bounds([0.0, max_latency]),
            )
            .hidden_legend_constraints((Constraint::Percentage(50), Constraint::Percentage(50)));

        chart.render(area, buf);
    }
}

impl<'a> Widget for LatencyWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.render_stats(area, buf);
    }
}
