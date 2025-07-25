use std::{
    fmt::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use console::Emoji;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::events::Message;

pub struct Ui {
    state: Arc<State>,
    events: JoinHandle<()>,
}

impl Drop for Ui {
    fn drop(&mut self) {
        self.events.abort();
    }
}

struct State {
    pool: ProgressBar,
    setup: SetupState,
}

struct SetupState {
    schema: MultiProgress,
    schemas: Mutex<IndexMap<String, ProgressBar>>,
    load: MultiProgress,
    loads: Mutex<IndexMap<String, ProgressBar>>,
}

impl Default for SetupState {
    fn default() -> Self {
        let schema = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let schemas = Mutex::new(IndexMap::new());
        let load = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let loads = Mutex::new(IndexMap::new());

        Self {
            schema,
            schemas,
            load,
            loads,
        }
    }
}

impl SetupState {
    fn schema_progressbar(&self, name: impl Into<String>) -> ProgressBar {
        let mut schemas = self.loads.lock().expect("never poisoned");

        match schemas.entry(name.into()) {
            indexmap::map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            indexmap::map::Entry::Vacant(vacant_entry) => {
                let pb = self.schema.add(ProgressBar::no_length());
                pb.set_style(spinner_template());
                vacant_entry.insert(pb.clone());
                pb
            }
        }
    }

    fn load_progressbar(&self, name: impl Into<String>) -> ProgressBar {
        let mut loads = self.loads.lock().expect("never poisoned");

        match loads.entry(name.into()) {
            indexmap::map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            indexmap::map::Entry::Vacant(vacant_entry) => {
                let pb = self.load.add(ProgressBar::new(0));
                pb.set_style(
                    ProgressStyle::with_template(
                        "{prefix:.bold.dim} {spinner} {wide_msg} [{elapsed_precise}] [{bar:.cyan/blue}] {pos}/{len} rows ({eta})",
                    )
                    .unwrap()
                    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                         write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                    })
                    .progress_chars("#>-")
                );
                vacant_entry.insert(pb.clone());
                pb
            }
        }
    }
}

impl Ui {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        let state = Arc::new(State {
            pool: ProgressBar::hidden(),
            setup: SetupState::default(),
        });

        let events = tokio::spawn(process_events(rx, state.clone()));

        Ui { state, events }
    }

    pub fn on_before_pool_create(&mut self) {
        let pool = &self.state.pool;
        pool.set_draw_target(ProgressDrawTarget::stderr());
        pool.set_style(spinner_template());
        pool.set_message("connecting");
        pool.set_prefix(phased(1));
        pool.enable_steady_tick(Duration::from_millis(100));
    }

    pub fn on_after_pool_create(&mut self) {
        let pool = &self.state.pool;
        pool.disable_steady_tick();
        pool.finish_with_message("connected");
    }

    pub fn on_before_setup(&mut self) {
        let SetupState { schema, load, .. } = &self.state.setup;
        schema.set_draw_target(ProgressDrawTarget::stderr());
        load.set_draw_target(ProgressDrawTarget::stderr());
    }

    pub fn on_after_setup(&mut self) {
        let SetupState {
            schema,
            schemas,
            load,
            loads,
        } = &self.state.setup;
        let mut schemas = schemas.lock().unwrap();
        for (_, pb) in schemas.iter_mut() {
            pb.disable_steady_tick();
            pb.finish_and_clear();
        }
        _ = schema.clear();

        let mut loads = loads.lock().unwrap();
        for (_, pb) in loads.iter_mut() {
            pb.disable_steady_tick();
            pb.finish_and_clear();
        }
        _ = load.clear();
    }
}

fn phased(phase: usize) -> String {
    let icon = match phase {
        1 => Emoji("🔗", ""),
        2 => Emoji("🚧", ""),
        3 => Emoji("💾", ""),
        _ => unimplemented!(),
    };

    format!("{icon}")
}

fn spinner_template() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}").unwrap()
}

async fn process_events(mut rx: mpsc::Receiver<Message>, state: Arc<State>) {
    while let Some(event) = rx.recv().await {
        match event {
            Message::QueryResult(_query_result) => {}
            Message::InitialUsage(_usage) => {}
            Message::UsageUpdated(_usage) => {}
            Message::TableDropping(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(2));
                pb.set_message(format!("dropping: {t}"));
            }
            Message::TableDropped(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.disable_steady_tick();
                pb.set_message(format!("dropped: {t}"));
            }
            Message::TableCreating(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(2));
                pb.set_message(format!("creating table: {t}"));
            }
            Message::TableCreated(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.disable_steady_tick();
                pb.finish_with_message(format!("created table: {t}"));
            }
            Message::TableLoading(t, rows) => {
                let pb = &state.setup.load_progressbar(&t);
                pb.reset();
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(3));
                pb.set_length(rows as u64);
                pb.set_message(format!("loading: {t}"));
            }
            Message::TableLoaded(t, rows) => {
                let pb = &state.setup.load_progressbar(&t);
                pb.inc(rows as u64);

                if Some(pb.position()) == pb.length() {
                    pb.disable_steady_tick();
                    pb.finish_with_message(format!("loaded: {t}"));
                }
            }
            Message::LoadComplete => {
                let load = &state.setup.load;
                _ = load.clear();
            }
            Message::WorkloadComplete => {}
            Message::PoolTelemetry(telemetry) => match telemetry {
                crate::pool::Telemetry::Connected(_) => {}
                crate::pool::Telemetry::Disconnected(_) => {}
                crate::pool::Telemetry::Err { .. } => {}
            },
        }
    }
}
