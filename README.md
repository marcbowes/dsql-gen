# DSQL data generator tool

A simple Rust program to insert rows into an AWS DSQL table with configurable
concurrency and automatic retry on failures.

## Usage

```bash
# Run with default settings (2000 batches, 10 concurrent connections)
cargo run --release -- --endpoint your-cluster-endpoint.amazonaws.com

# Run with custom settings
cargo run --release -- \
  --endpoint your-cluster-endpoint.amazonaws.com \
  --region the-region \
  --concurrency 20 \
  --batches 2000 \
  --sql "INSERT INTO test (content) SELECT md5(random()::text) FROM generate_series(1, 1000)"

# Get help
cargo run --release -- --help
```

## Examples

```bash
# Insert 2 million rows into test table (default)
cargo run --release -- --endpoint my-cluster.dsql.us-east-1.on.aws

# Insert 5 million rows with 50 concurrent connections
cargo run --release -- \
  --endpoint my-cluster.dsql.us-east-1.on.aws \
  --concurrency 50 \
  --batches 5000

# Custom SQL with 100 rows per batch, 10k total rows
cargo run --release -- \
  --endpoint my-cluster.dsql.us-east-1.on.aws \
  --sql "INSERT INTO users (name) SELECT 'user_' || generate_series(1, 100)" \
  --batches 100
