use std::{fmt::Display, hash::Hash, marker::PhantomData, time::Duration};

use async_nats::{
    Client, HeaderMap,
    jetstream::{self, Context},
};
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use outbox_core::{
    error::OutboxError, model::Message, publisher::Publisher, repository::Repository,
};
use std::fmt::Debug;
use tracing::{debug, error};

const MESSAGE_ID: &str = "Nats-Msg-Id";

pub struct NATSPublisher<R, Msg, Identifier> {
    client: Client,
    jetstream: Context,
    ack_timeout: Duration,
    _marker: PhantomData<(R, Msg, Identifier)>,
}

impl<R, Msg, Identifier> NATSPublisher<R, Msg, Identifier>
where
    R: Repository<Msg, Identifier>,
    Msg: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Send + Sync,
{
    pub async fn new(
        nats_url: String,
        nats_credentials: String,
        ack_timeout: Duration,
    ) -> Result<Self, OutboxError> {
        // Determine if TLS is required based on the URL scheme
        let needs_tls = nats_url.starts_with("wss://") || nats_url.starts_with("nats+tls://");

        // Connect to NATS with or without credentials
        let client: Client = if nats_credentials.trim().is_empty() {
            debug!("Connecting to NATS without credentials");
            async_nats::connect(&nats_url).await
        } else {
            debug!("Connecting to NATS with base64-encoded credentials");

            // Decode base64 credentials
            let decoded_base64_bytes = BASE64_STANDARD
                .decode(nats_credentials.trim())
                .expect("Failed to decode base64 NATS credentials - ensure NATS_USER_CREDS_BASE64 is valid base64");

            // Convert to UTF-8 string (NATS credentials are text-based .creds files)
            let credentials = String::from_utf8(decoded_base64_bytes).expect(
                "Failed to convert NATS credentials to UTF-8 - credentials may be corrupted",
            );

            // Parse and connect with credentials
            async_nats::ConnectOptions::with_credentials(&credentials)
                .expect("Failed to parse NATS credentials - ensure the credentials format is valid")
                .require_tls(needs_tls)
                .connect(&nats_url)
                .await
        }.map_err(|e| OutboxError::PublisherError(e.to_string()))?;
        let jetstream: Context = jetstream::new(client.clone());
        Ok(Self {
            client,
            jetstream,
            ack_timeout,
            _marker: PhantomData,
        })
    }

    async fn publish_and_await_ack(&self, message: Msg) -> Result<(), OutboxError> {
        let headers = {
            let mut headers = HeaderMap::new();
            headers.insert(MESSAGE_ID, message.id().to_string());
            headers
        };

        let json_bytes = serde_json::to_vec(message.payload()).unwrap();
        let bytes = Bytes::from(json_bytes);

        let ack_future = self
            .jetstream
            .publish_with_headers(message.subject().to_string(), headers, bytes)
            .await
            .map_err(|e| OutboxError::PublisherError(e.kind().to_string()))?;

        tokio::time::timeout(self.ack_timeout, ack_future)
            .await
            .map_err(|_| OutboxError::PublisherError("Acknowledgment took too long".into()))?
            .map_err(|e| OutboxError::PublisherError(e.kind().to_string()))?;
        Ok(())
    }
}
#[async_trait]
impl<R, Msg, Identifier> Publisher<Msg> for NATSPublisher<R, Msg, Identifier>
where
    R: Repository<Msg, Identifier>,
    Msg: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Send + Sync,
{
    async fn publish(&self, message: Msg) -> Result<(), OutboxError> {
        self.publish_and_await_ack(message).await?;
        Ok(())
    }

    async fn shutdown(&self) {
        match self.client.drain().await {
            Ok(_) => {
                debug!("NATS shutdown successful")
            }
            Err(error) => {
                error!("NATS Shutdown Error: {}", error.to_string())
            }
        }
    }
}
