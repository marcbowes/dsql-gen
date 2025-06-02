//! Performance stats component for the TUI

use ratatui::{
    prelude::*,
    symbols,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
};

use crate::tui::Model;

/// Widget for displaying performance statistics
pub struct PerformanceWidget<'a> {
    model: &'a Model,
}

impl<'a> PerformanceWidget<'a> {
    /// Create a new performance widget with the given model
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
}

impl<'a> Widget for PerformanceWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let throughput_text = vec![
            format!("Transactions/sec: {}", self.model.tps as u64),
            format!("Rows/sec: {}", self.model.rps as u64),
            format!("Throughput: {}", self.model.bps_formatted),
            format!("Pool: {} open, {} idle", self.model.pool_size, self.model.pool_idle),
        ].join("\n");
        
        let throughput_para = Paragraph::new(throughput_text)
            .block(Block::default().title("Performance").borders(Borders::ALL));
        
        throughput_para.render(area, buf);
    }
}

/// Placeholder for future stateful performance widget with chart
pub struct StatefulPerformanceWidget<'a> {
    model: &'a Model,
}

impl<'a> StatefulPerformanceWidget<'a> {
    /// Create a new stateful performance widget
    pub fn new(model: &'a Model) -> Self {
        Self { model }
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
        let throughput_text = vec![
            format!("Transactions/sec: {}", self.model.tps as u64),
            format!("Rows/sec: {}", self.model.rps as u64),
            format!("Throughput: {}", self.model.bps_formatted),
            format!("Pool: {} open, {} idle", self.model.pool_size, self.model.pool_idle),
        ].join("\n");
        
        let throughput_para = Paragraph::new(throughput_text)
            .block(Block::default().title("Performance").borders(Borders::ALL));
        
        throughput_para.render(area, buf);
    }
    
    fn render_chart(&self, area: Rect, buf: &mut Buffer) {
        if self.model.tps_history.is_empty() {
            // No data yet, just render empty block
            let block = Block::default()
                .title("TPS Chart (5 min)")
                .borders(Borders::ALL);
            block.render(area, buf);
            return;
        }

        // Prepare data for chart
        let data_len = self.model.tps_history.len();
        let mut tps_data = Vec::with_capacity(data_len);
        let mut max_tps: f64 = 0.0;
        
        for (i, &tps) in self.model.tps_history.iter().enumerate() {
            let x = i as f64;
            tps_data.push((x, tps));
            max_tps = max_tps.max(tps);
        }
        
        // Add some padding to max value
        max_tps = (max_tps * 1.1).max(10.0);
        
        // Create dataset
        let datasets = vec![
            Dataset::default()
                .name("TPS")
                .marker(symbols::Marker::Dot)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Green))
                .data(&tps_data),
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
            Span::raw(format!("{:.0}", max_tps / 2.0)),
            Span::raw(format!("{:.0}", max_tps)),
        ];
        
        // Create chart
        let chart = Chart::new(datasets)
            .block(Block::default()
                .title("TPS Chart (5 min)")
                .borders(Borders::ALL))
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
