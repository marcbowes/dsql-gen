use anyhow::Result;
use async_trait::async_trait;

use crate::pool::ClientHandle;

pub mod onekib;
pub mod tiny;

pub struct Inserts {
    pub rows_inserted: usize,
    pub logical_bytes_written: usize,
}

#[async_trait]
pub trait Workload {
    type T;

    async fn setup(&self, client: ClientHandle) -> Result<()>;
    async fn transaction(&self, client: ClientHandle) -> Result<Self::T>;
}

// // FIXME: this doesn't fit in well with the other workloads
// pub fn counter(n: usize) -> Workload {
//     Workload {
//         name: "counter".to_string(),
//         setup: "CREATE TABLE IF NOT EXISTS counter (
//     id INT PRIMARY KEY,
//     value int
// );"
//         .to_string(),
//         single_statement: format!("UPDATE counter SET value = value + 1 WHERE id = {n}"),
//         rows_inserted: 1,
//         per_row_logical_bytes_written: 12,
//     }
// }
