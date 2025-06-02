use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Workload {
    /// The name of the workload
    pub name: String,
    /// Idempotent DDL to initialize the schema
    pub setup: String,
    /// Single statement to run (in it's own transaction)
    pub single_statement: String,
    /// How many rows were inserted
    pub rows_inserted: usize,
    /// How many bytes were inserted per row
    pub per_row_logical_bytes_written: usize,
}

pub fn load_all(n: usize) -> HashMap<String, Workload> {
    let mut all = HashMap::new();
    for wl in [tiny_rows(n), one_kib_rows(n)] {
        all.insert(wl.name.clone(), wl);
    }
    all
}

pub fn tiny_rows(n: usize) -> Workload {
    Workload {
        name: "tiny".to_string(),
        setup: "CREATE TABLE IF NOT EXISTS test (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content text
);"
        .to_string(),
        single_statement: format!(
            "INSERT INTO test (content) SELECT md5(random()::text) FROM generate_series(1, {n})"
        ),
        rows_inserted: n,
        per_row_logical_bytes_written: 68,
    }
}

pub fn one_kib_rows(n: usize) -> Workload {
    Workload {
        name: "1KiB".to_string(),
        setup: "CREATE TABLE IF NOT EXISTS test (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    content text
);"
        .to_string(),
        single_statement: format!(
            "INSERT INTO test (content) SELECT substring(repeat(md5(random()::text), 32), 1, 988) FROM generate_series(1, {n})"
        ),
        rows_inserted: n,
        per_row_logical_bytes_written: 1024,
    }
}
