use std::time::Duration;

use tokio::sync::mpsc;

use crate::{pool, tui::Model, usage::Usage};

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok(QueryOk),
    Err(QueryErr),
}

#[derive(Debug, Clone)]
pub struct QueryOk {
    pub duration: Duration,
    pub rows_inserted: usize,
    pub logical_bytes_written: usize,
}

#[derive(Debug, Clone)]
pub struct QueryErr {
    pub duration: Duration,
    pub msg: String,
}

/// Messages sent from the runner to the UI
#[derive(Debug)]
pub enum Message {
    /// A batch completed successfully or failed
    QueryResult(QueryResult),
    /// Initial usage
    InitialUsage(Usage),
    /// Usage information was updated
    UsageUpdated(Usage),
    /// All batches have completed
    WorkloadComplete,
    /// Pool telemetry
    PoolTelemetry(pool::Telemetry),
}

/// Runner event listener that processes events and updates UI state
pub struct EventListener {
    /// Channel receiver for runner events
    pub rx: mpsc::Receiver<Message>,
    /// Whether all batches have completed
    pub completed: bool,
}

impl EventListener {
    /// Create a new event listener with the specified channel capacity
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self {
            rx,
            completed: false,
        }
    }

    /// Process an incoming message and update the provided model
    pub fn process_message(&mut self, message: Message, model: &mut Model) {
        match message {
            Message::QueryResult(result) => {
                match result {
                    QueryResult::Ok(ok) => {
                        model.latency_state.record(ok.duration.as_millis() as u64);
                        model.metrics.completed_batches += 1;
                        model.performance_state.update(ok);
                    }
                    QueryResult::Err(err) => model.error_state.record_error(err.msg),
                }

                // FIXME: Do this with less coupling.
                model.progress.total = model.runner.batches();
                model.progress.completed = model.metrics.completed_batches;
                model.progress.pct =
                    (100.0 * model.progress.completed as f64) / model.progress.total as f64;
            }
            Message::InitialUsage(usage) => {
                model.usage_cost.initial = usage;
            }
            Message::UsageUpdated(usage) => {
                model.usage_cost.latest = usage;
            }
            Message::WorkloadComplete => {
                self.completed = true;
            }
            Message::PoolTelemetry(telemetry) => match telemetry {
                pool::Telemetry::Connected(_) => model.performance_state.open += 1,
                pool::Telemetry::Disconnected(_) => model.performance_state.open -= 1,
                pool::Telemetry::Err { err, .. } => model.error_state.record_error(err.to_string()),
            },
        }
    }

    /// Process all available messages without waiting
    pub async fn process_available_messages(&mut self, model: &mut Model) {
        while let Ok(message) = self.rx.try_recv() {
            self.process_message(message, model);
        }
    }
}
