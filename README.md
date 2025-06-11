# DSQL data generator tool

A Rust load generator for AWS DSQL with a terminal UI for real-time monitoring. Features configurable workloads, automatic retry on failures, and usage tracking.

## Usage

```bash
# Run a workload
cargo run --release -- workload --identifier your-cluster-id \
  --batches 10 --concurrency 1 \
  tiny --rows-per-transaction 1
```
