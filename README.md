# DSQL data generator tool

A Rust load generator for AWS DSQL with a terminal UI for real-time monitoring. Features configurable workloads, automatic retry on failures, and usage tracking.

## Features

- **Terminal UI**: Real-time monitoring with ratatui showing:
  - Progress bar with completion percentage
  - Performance metrics (TPS, rows/sec, throughput)
  - Latency statistics (p50, p90, p99, p99.9)
  - DPU usage and cost tracking
  - Error monitoring
- **Configurable workloads**: Multiple pre-defined workloads
- **Automatic retry**: Built-in retry logic with exponential backoff
- **Connection pooling**: Efficient connection management
- **Usage tracking**: Monitor DPU consumption and storage costs

## Usage

```bash
# Run a workload
cargo run --release -- run \
  --identifier your-cluster-id \
  --workload workload-name \
  --rows number-of-rows

# Check cluster usage
cargo run --release -- usage \
  --identifier your-cluster-id

# Get help
cargo run --release -- --help
cargo run --release -- run --help
cargo run --release -- usage --help
```

## Examples

```bash
# Run default workload (2000 batches, 10 concurrent connections)
cargo run --release -- run \
  --identifier my-cluster \
  --workload default \
  --rows 1000

# Run with custom settings
cargo run --release -- run \
  --identifier my-cluster \
  --workload heavy-write \
  --rows 5000 \
  --concurrency 20 \
  --batches 5000

# Check usage for a cluster
cargo run --release -- usage \
  --identifier my-cluster
```

## Terminal UI

During workload execution, the terminal displays a real-time dashboard. Press `q` or `ESC` to gracefully stop the workload.

```
┌─ Progress ─────────────────────────────────┐
│ ████████████████░░░░░░  1250/2000 (62.5%) │
└────────────────────────────────────────────┘
┌─ Performance ──────────────────────────────┐
│ Transactions/sec: 125                      │
│ Rows/sec: 1250                             │
│ Throughput: 12.5 MiB/sec                   │
│ Pool: 10 open, 2 idle                      │
└────────────────────────────────────────────┘
```
