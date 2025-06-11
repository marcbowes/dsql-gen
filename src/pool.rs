use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZero,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Result};
use aws_config::SdkConfig;
use aws_sdk_dsql::auth_token::{self, AuthTokenGenerator};
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinSet,
};
use tokio_postgres::{Config, Statement};

pub struct Bundle {
    pub config: Config,
    pub sdk_config: SdkConfig,
    pub signer: AuthTokenGenerator,
    pub connector: postgres_native_tls::MakeTlsConnector,
}

impl Bundle {
    pub fn new_with_sdk_config(mut config: Config, sdk_config: SdkConfig) -> Result<Bundle> {
        let endpoint = match &config.get_hosts() {
            [tokio_postgres::config::Host::Tcp(hostname)] => hostname.clone(),
            _ => bail!("you must specify precisely one host by hostname"),
        };

        if config.get_user().is_none() {
            bail!("you must specify a user");
        }

        // FIXME: Temporary hack for testing against rds
        let connector = if let Ok(pgpass) = std::env::var("PGPASSWORD") {
            config.password(pgpass);
            config.ssl_negotiation(tokio_postgres::config::SslNegotiation::Postgres);

            let connector = native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()?;
            let connector = postgres_native_tls::MakeTlsConnector::new(connector);
            connector
        } else {
            let connector = native_tls::TlsConnector::builder()
                .request_alpns(&["postgresql"])
                .build()?;
            let connector = postgres_native_tls::MakeTlsConnector::new(connector);
            connector
        };

        let signer = AuthTokenGenerator::new(
            auth_token::Config::builder()
                .hostname(endpoint)
                .build()
                .map_err(|err| anyhow!("Failed to build signer config: {err}"))?,
        );

        Ok(Self {
            config,
            sdk_config,
            signer,
            connector,
        })
    }

    async fn connect(&self) -> Result<Client> {
        let mut config = self.config.clone();
        config.password(
            match config.get_user() {
                Some("admin") => self
                    .signer
                    .db_connect_admin_auth_token(&self.sdk_config)
                    .await
                    .map_err(|err| anyhow!("signer failed: {err}")),
                Some(_) => self
                    .signer
                    .db_connect_auth_token(&self.sdk_config)
                    .await
                    .map_err(|err| anyhow!("signer failed: {err}")),
                None => Err(anyhow!("invalid config")),
            }?
            .as_str(),
        );

        let (client, connection) = config.connect(self.connector.clone()).await?;

        // TODO: Notices
        // let (tx, rx) = mpsc::channel(1);
        _ = tokio::spawn(async move {
            // TODO
            // loop {
            //     match poll_fn(|cx| connection.poll_message(cx)).await {
            //         None => break,
            //         Some(Err(err)) => {
            //             tx.send(Err(err));
            //             break;
            //         }
            //         Some(Ok(msg)) => tx.send(Ok(msg)).await?,
            //     }
            // }

            anyhow::Ok(connection.await?)
        });
        // Clients start without any prepared statements
        let statements = Mutex::new(HashMap::new());

        Ok(Client { client, statements })
    }
}

pub struct Client {
    client: tokio_postgres::Client,
    statements: Mutex<HashMap<&'static str, Statement>>,
}

impl Client {
    /// Get the statement if it's already been prepared, or prepare the
    /// statement.
    pub async fn statement(
        &self,
        name: &'static str,
        query: impl AsRef<str>,
    ) -> Result<Statement, tokio_postgres::Error> {
        let mut known = self.statements.lock().await;
        match known.get(name) {
            Some(statement) => Ok(statement.clone()),
            None => {
                let statement = self.prepare(query.as_ref()).await?;
                known.insert(name, statement.clone());
                Ok(statement)
            }
        }
    }
}

impl Deref for Client {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("client", &self.client)
            .finish()
    }
}

#[derive(Debug)]
pub enum Telemetry {
    Connected(Duration),
    Disconnected(Duration),
    Err {
        err: anyhow::Error,
        elapsed: Duration,
    },
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub desired: NonZero<usize>,
    pub concurrent: NonZero<usize>,
}

#[derive(Clone)]
pub struct ConnectionPool {
    inner: Arc<Mutex<PoolInner>>,
}

impl ConnectionPool {
    /// Creates a new pool.
    ///
    /// The pool will fail to initialize if at least one connection cannot be
    /// opened, which is intended to help you diagnose configuration problems.
    ///
    /// Once one connection succeeds, the telemetry channel can be used for
    /// monitoring changes in pool size or errors connecting.
    pub async fn launch(
        bundle: Bundle,
        config: PoolConfig,
    ) -> Result<(Self, mpsc::Receiver<Telemetry>)> {
        let initial = bundle.connect().await?;

        let (obtain_tx, obtain_rx) = mpsc::unbounded_channel();
        let (release_tx, release_rx) = mpsc::unbounded_channel();
        let (telemetry_tx, telemetry_rx) = mpsc::channel(1000);

        tokio::spawn(maintain(
            obtain_tx,
            release_rx,
            telemetry_tx,
            bundle,
            config,
        ));

        release_tx.send(Some(initial))?;

        let inner = PoolInner {
            obtain: obtain_rx,
            release: release_tx,
        };

        let pool = Self {
            inner: Arc::new(Mutex::new(inner)),
        };

        Ok((pool, telemetry_rx))
    }

    pub async fn borrow(&self) -> ClientHandle {
        self.inner.lock().await.borrow().await
    }
}

struct PoolInner {
    obtain: mpsc::UnboundedReceiver<Client>,
    release: mpsc::UnboundedSender<Option<Client>>,
}

impl PoolInner {
    pub async fn borrow(&mut self) -> ClientHandle {
        ClientHandle {
            client: self.obtain.recv().await,
            release: self.release.clone(),
        }
    }
}

async fn maintain(
    obtain: mpsc::UnboundedSender<Client>,
    mut release: mpsc::UnboundedReceiver<Option<Client>>,
    // XXX: Telemetry is lossy. A bounded receiver is used here in case the
    // consumer doesn't consume telemetry (or drops the receiver).
    telemetry: mpsc::Sender<Telemetry>,
    bundle: Bundle,
    config: PoolConfig,
) -> Result<()> {
    let mut connected = 0;
    let mut inflight = 0;
    let mut connecting = JoinSet::new();
    let bundle = Arc::new(bundle);

    loop {
        // first launch any reconnect attempts we need
        while inflight + connected < config.desired.get() && inflight < config.concurrent.get() {
            inflight += 1;
            let _b = bundle.clone();
            connecting.spawn(async move {
                let start = Instant::now();
                (_b.connect().await, start.elapsed())
            });
        }

        // now watch for events - either from the `drop` queue returning or
        // notifying of a connection failure or from one of our connect attempts
        tokio::select! {
            handle = connecting.join_next(), if !connecting.is_empty() => {
                inflight -= 1;
                match handle.expect("a task")? {
                    (Ok(client), elapsed) => {
                        connected += 1;
                        _ = telemetry.try_send(Telemetry::Connected(elapsed));
                        obtain.send(client)?;
                    }
                    (Err(err), elapsed) => {
                        _ = telemetry.try_send(Telemetry::Err { err, elapsed });
                    }
                }
            },
            release = release.recv() => {
                match release {
                    Some(Some(client)) => {
                        if client.is_closed() {
                            connected -= 1;
                        }
                        obtain.send(client)?;
                    },
                    Some(None) => { // sender wanted to consume the connection so make a new one
                        connected -= 1;
                    }
                    None => {
                        break; // all senders dropped
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct ClientHandle {
    client: Option<Client>,
    release: mpsc::UnboundedSender<Option<Client>>,
}

impl Deref for ClientHandle {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().unwrap()
    }
}

impl DerefMut for ClientHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().unwrap()
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        let _ = self.release.send(self.client.take());
    }
}
