//! Progress bar component for the TUI

use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Gauge},
};

use crate::tui::Model;

/// Widget for displaying the progress bar
pub struct ProgressWidget<'a> {
    model: &'a Model,
}

impl<'a> ProgressWidget<'a> {
    /// Create a new progress widget with the given model
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
}

impl<'a> Widget for ProgressWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let progress_gauge = Gauge::default()
            .block(Block::default().title("Progress").borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Cyan))
            .percent(self.model.progress_pct as u16)
            .label(format!(
                "{}/{} ({:.1}%)",
                self.model.metrics.completed_batches,
                self.model.runner.batches(),
                self.model.progress_pct
            ));

        progress_gauge.render(area, buf);
    }
}
