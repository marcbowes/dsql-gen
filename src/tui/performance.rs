//! Performance stats component for the TUI

use std::time::{Duration, Instant};

use byte_unit::{Byte, UnitType};
use ratatui::{
    prelude::*,
    symbols,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
};

use crate::{
    events::QueryOk,
    history::{
        bucketing::{bucket_data, BucketConfig},
        TimestampedHistory,
    },
};

/// State for the performance widget containing its history data
#[derive(Clone)]
pub struct PerformanceState {
    pub tps_history: TimestampedHistory<QueryOk>,
    pub open: usize,
}

impl PerformanceState {
    /// Create a new performance state with default values
    pub fn new() -> Self {
        Self {
            tps_history: TimestampedHistory::new(Duration::from_secs(300)),
            open: 0,
        }
    }

    /// Update the performance state with new data
    pub fn update(&mut self, ok: QueryOk) {
        self.tps_history.push(ok);
    }
}

/// Stateful performance widget that can render both simple stats and charts
pub struct PerformanceWidget<'a> {
    state: &'a PerformanceState,
}

impl<'a> PerformanceWidget<'a> {
    /// Create a new performance widget with the given state
    pub fn new(state: &'a PerformanceState) -> Self {
        Self { state }
    }

    /// Render with split layout for chart integration
    pub fn render_with_chart(&self, area: Rect, buf: &mut Buffer) {
        // Split horizontally: left for stats, right for chart
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30), // Fixed width for stats
                Constraint::Min(40),    // Remaining width for chart
            ])
            .split(area);

        // Render stats on the left
        self.render_stats(chunks[0], buf);

        // Render chart on the right
        self.render_chart(chunks[1], buf);
    }

    fn render_stats(&self, area: Rect, buf: &mut Buffer) {
        let secs = 5;
        let ago = Instant::now() - Duration::from_secs(secs);
        let data = self
            .state
            .tps_history
            .data()
            .iter()
            .filter(|dp| dp.timestamp >= ago)
            .collect::<Vec<_>>();
        let tps = data.len() as f64 / secs as f64;
        let rps = data.iter().map(|ok| ok.value.rows_inserted).sum::<usize>() as f64 / secs as f64;
        let bps = data
            .iter()
            .map(|ok| ok.value.rows_inserted * ok.value.per_row_logical_bytes_written)
            .sum::<usize>() as f64
            / secs as f64;
        let bps_unit = Byte::from(bps as u64).get_appropriate_unit(UnitType::Binary);

        let throughput_text = [
            format!("Transactions/sec: {}", tps as u64),
            format!("Rows/sec: {}", rps as u64),
            format!(
                "Throughput: {:.2} {}/sec",
                bps_unit.get_value(),
                bps_unit.get_unit()
            ),
            format!("Pool: {} open", self.state.open),
        ]
        .join("\n");

        let throughput_para = Paragraph::new(throughput_text).block(
            Block::default()
                .title(format!("Performance ({secs} sec)"))
                .borders(Borders::ALL),
        );

        throughput_para.render(area, buf);
    }

    fn render_chart(&self, area: Rect, buf: &mut Buffer) {
        if self.state.tps_history.is_empty() {
            // No data yet, just render empty block
            let block = Block::default()
                .title("TPS Chart (5 min)")
                .borders(Borders::ALL);
            block.render(area, buf);
            return;
        }

        // Use bucketing to convert timestamped data to chart data
        let config = BucketConfig::new(Duration::from_secs(1), 300); // 300 seconds = 5 minutes
        let buckets = bucket_data(
            self.state.tps_history.data(),
            config,
            |values| values.len() as f64,
            0.0, // default value for empty buckets
        );

        // Prepare data for chart
        let mut tps_data = Vec::with_capacity(buckets.len());
        let mut max_tps: f64 = 0.0;

        for (i, &tps) in buckets.iter().enumerate() {
            let x = i as f64;
            tps_data.push((x, tps));
            max_tps = max_tps.max(tps);
        }

        // Add some padding to max value
        max_tps = (max_tps * 1.1).max(10.0);

        // Create dataset
        let datasets = vec![Dataset::default()
            .name("TPS")
            .marker(symbols::Marker::Dot)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(Color::Green))
            .data(&tps_data)];

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
            Span::raw(format!("{:.0}", max_tps / 2.0)),
            Span::raw(format!("{:.0}", max_tps)),
        ];

        // Create chart
        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title("TPS Chart (5 min)")
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
                    .title("Transactions/sec")
                    .style(Style::default().fg(Color::Gray))
                    .labels(y_labels)
                    .bounds([0.0, max_tps]),
            )
            .hidden_legend_constraints((Constraint::Percentage(50), Constraint::Percentage(50)));

        chart.render(area, buf);
    }
}

impl<'a> Widget for PerformanceWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.render_stats(area, buf);
    }
}
