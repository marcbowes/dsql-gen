
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dsql::{
    auth_token::{AuthTokenGenerator, Config},
    error::BoxError,
};
use clap::Parser;
use tokio::task::JoinSet;
use tokio_retry::{
    Retry,
    strategy::{ExponentialBackoff, jitter},
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// AWS DSQL cluster endpoint
    #[arg(short, long)]
    endpoint: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: String,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,

    /// SQL query to execute per batch
    #[arg(
        short,
        long,
        default_value = "INSERT INTO test (content) SELECT md5(random()::text) FROM generate_series(1, 1000)"
    )]
    sql: String,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    batches: usize,
}

async fn generate_token(endpoint: &str, region: &str) -> Result<String, BoxError> {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let signer = AuthTokenGenerator::new(
        Config::builder()
            .hostname(endpoint)
            .region(Region::new(region.to_string()))
            .build()?,
    );
    Ok(signer
        .db_connect_admin_auth_token(&sdk_config)
        .await?
        .to_string())
}

async fn insert_with_retry(
    endpoint: String,
    region: String,
    sql: String,
    batch_id: usize,
) -> anyhow::Result<()> {
    let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);
    Retry::spawn(retry_strategy, || {
        insert(&endpoint, &region, sql.clone(), batch_id)
    })
    .await
}

async fn insert(endpoint: &str, region: &str, sql: String, batch_id: usize) -> anyhow::Result<()> {
    match _insert(endpoint, region, sql, batch_id).await {
        Ok(it) => {
            println!("Batch {batch_id} completed");
            Ok(it)
        }
        Err(err) => {
            println!("Batch {batch_id} err: {err}");
            Err(err)
        }
    }
}

async fn _insert(
    endpoint: &str,
    region: &str,
    sql: String,
    _batch_id: usize,
) -> anyhow::Result<()> {
    let token = generate_token(endpoint, region)
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    let conn_str = format!(
        "host={} port=5432 dbname=postgres user=admin password={} sslmode=require",
        endpoint, token
    );

    let connector = native_tls::TlsConnector::new().unwrap();
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(&conn_str, connector).await?;

    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("connection error: {err:?}");
        }
    });
    client.execute(&sql, &[]).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut set = JoinSet::new();

    for i in 0..args.batches {
        while set.len() >= args.concurrency {
            set.join_next().await;
        }
        set.spawn(insert_with_retry(
            args.endpoint.clone(),
            args.region.clone(),
            args.sql.clone(),
            i,
        ));
    }

    while set.join_next().await.is_some() {}

    println!("Completed {} batches successfully", args.batches);
    Ok(())
}
