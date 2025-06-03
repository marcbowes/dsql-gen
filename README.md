# DSQL data generator tool

A Rust load generator for AWS DSQL with a terminal UI for real-time monitoring. Features configurable workloads, automatic retry on failures, and usage tracking.

## Usage

```bash
# Run a workload
cargo run --release -- run \
  --identifier your-cluster-id \
  --workload tiny \
  --batches 10 \
  --concurrency 1 \
  --rows 1
```
