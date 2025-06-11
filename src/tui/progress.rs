//! Progress bar component for the TUI

use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Gauge},
};

pub struct ProgressWidgetState {
    pub completed: usize,
    pub total: usize,
    pub pct: f64,
}

/// Widget for displaying the progress bar
pub struct ProgressWidget;

impl StatefulWidget for ProgressWidget {
    type State = ProgressWidgetState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        let progress_gauge = Gauge::default()
            .block(Block::default().title("Progress").borders(Borders::ALL))
            .gauge_style(Style::default().fg(Color::Cyan))
            .percent(state.pct as u16)
            .label(format!(
                "{}/{} ({:.1}%)",
                state.completed, state.total, state.pct
            ));

        progress_gauge.render(area, buf);
    }
}
