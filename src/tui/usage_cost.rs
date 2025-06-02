//! Usage & Cost table component for the TUI

use byte_unit::{Byte, UnitType};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Cell, Row, Table},
};

use crate::tui::Model;

/// Widget for displaying usage and cost information
pub struct UsageCostWidget<'a> {
    model: &'a Model,
}

impl<'a> UsageCostWidget<'a> {
    /// Create a new usage & cost widget with the given model
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
}

impl<'a> Widget for UsageCostWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let m = &self.model.metrics;
        let latest_usage = &self.model.latest_usage;
        let usage_diff_from_start = &self.model.usage_diff_from_start;

        // Format storage sizes
        let storage_current = Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
            .get_appropriate_unit(UnitType::Decimal);

        // Calculate total costs
        let total_cost_current = latest_usage.cost_estimate.total_dpus.total
            + latest_usage.cost_estimate.latest_storage.gb_month;
        let total_cost_diff_from_start = usage_diff_from_start.cost_estimate.total_dpus.total
            + usage_diff_from_start.cost_estimate.latest_storage.gb_month;

        let rows = vec![
            // Header row
            Row::new(vec![
                Cell::from(""),
                Cell::from("Month"),
                Cell::from("Month"),
                Cell::from("Delta"),
                Cell::from("Delta"),
            ]),
            Row::new(vec![
                Cell::from("Total DPUs:"),
                Cell::from(format!("{:.2}", latest_usage.dpu_metrics.total,)),
                Cell::from(format!(
                    "${:.2}",
                    latest_usage.cost_estimate.total_dpus.total
                )),
                Cell::from(format!("+{:.2}", usage_diff_from_start.dpu_metrics.total,)),
                Cell::from(format!(
                    "+${:.2}",
                    usage_diff_from_start.cost_estimate.total_dpus.total
                )),
            ]),
            Row::new(vec![
                Cell::from("  Compute:"),
                Cell::from(format!("{:.2}", latest_usage.dpu_metrics.compute)),
                Cell::from(format!(
                    "${:.2}",
                    latest_usage.cost_estimate.total_dpus.compute
                )),
                Cell::from(format!("+{:.2}", usage_diff_from_start.dpu_metrics.compute)),
                Cell::from(format!(
                    "+${:.2}",
                    usage_diff_from_start.cost_estimate.total_dpus.compute
                )),
            ]),
            Row::new(vec![
                Cell::from("  Read:"),
                Cell::from(format!("{:.2}", latest_usage.dpu_metrics.read)),
                Cell::from(format!(
                    "${:.2}",
                    latest_usage.cost_estimate.total_dpus.read
                )),
                Cell::from(format!("+{:.2}", usage_diff_from_start.dpu_metrics.read)),
                Cell::from(format!(
                    "+${:.2}",
                    usage_diff_from_start.cost_estimate.total_dpus.read
                )),
            ]),
            Row::new(vec![
                Cell::from("  Write:"),
                Cell::from(format!("{:.2}", latest_usage.dpu_metrics.write)),
                Cell::from(format!(
                    "${:.2}",
                    latest_usage.cost_estimate.total_dpus.write
                )),
                Cell::from(format!("+{:.2}", usage_diff_from_start.dpu_metrics.write,)),
                Cell::from(format!(
                    "+${:.2}",
                    usage_diff_from_start.cost_estimate.total_dpus.write
                )),
            ]),
            Row::new(vec![
                Cell::from("Storage:"),
                Cell::from(format!(
                    "{:.2} {}",
                    storage_current.get_value(),
                    storage_current.get_unit(),
                )),
                Cell::from(format!(
                    "${:.2}",
                    latest_usage.cost_estimate.latest_storage.gb_month
                )),
                Cell::from(format!(
                    "+{}",
                    Byte::from_u64(usage_diff_from_start.storage_metrics.size_bytes as u64)
                        .get_appropriate_unit(UnitType::Decimal)
                )),
                Cell::from(format!(
                    "+${:.2}",
                    usage_diff_from_start.cost_estimate.latest_storage.gb_month
                )),
            ]),
            Row::new(vec![
                Cell::from("═══════════"),
                Cell::from("═══════════"),
                Cell::from("═══════════"),
                Cell::from("═══════════"),
                Cell::from("═══════════"),
            ]),
            Row::new(vec![
                Cell::from("TOTAL COST:").style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from(""),
                Cell::from(format!("${:.2}", total_cost_current)).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Cell::from(""),
                Cell::from(format!("(+${:.2})", total_cost_diff_from_start)).style(
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
        ];

        let title = if m.completed_batches >= self.model.runner.batches() {
            "Usage & Cost - waiting for final values"
        } else {
            "Usage & Cost"
        };
        let mut block = Block::default().title(title).borders(Borders::ALL);
        if m.completed_batches >= self.model.runner.batches() {
            block = block.title_style(Style::default().add_modifier(Modifier::RAPID_BLINK));
        }

        let widths = [
            Constraint::Length(12),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
            Constraint::Length(20),
        ];
        let table = Table::new(rows, widths).block(block);

        // Render the table directly to the buffer
        use ratatui::widgets::Widget;
        Widget::render(table, area, buf);
    }
}
