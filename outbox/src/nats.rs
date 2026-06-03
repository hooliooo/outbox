//! [`NATSPublisher`] is an implemntation of the [`Publisher`](crate::publisher::Publisher) that uses the async-nats crate
//!
use std::{fmt::Display, hash::Hash, marker::PhantomData, time::Duration};

use crate::{error::OutboxError, model::Message, publisher::Publisher};
use async_nats::{
    Client, HeaderMap,
    jetstream::{self, Context, context::PublishAckFuture, publish::PublishAck},
};
use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use tracing::{debug, error};

const MESSAGE_ID: &str = "Nats-Msg-Id";
const ACK_TOOK_TOO_LONG: &str = "Acknowledgment took too long";

pub struct NATSPublisher<Msg, Identifier> {
    client: Client,
    jetstream: Context,
    ack_timeout: Duration,
    _marker: PhantomData<(Msg, Identifier)>,
}

impl<Msg, Identifier> NATSPublisher<Msg, Identifier>
where
    Msg: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
{
    pub async fn new(client: Client, ack_timeout: Duration) -> Result<Self, OutboxError> {
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

        let ack_future: PublishAckFuture = self
            .jetstream
            .publish_with_headers(message.subject().to_string(), headers, bytes)
            .await
            .map_err(|e| OutboxError::PublisherError(e.kind().to_string()))?;
        let _ack: PublishAck = tokio::time::timeout(self.ack_timeout, ack_future)
            .await
            .map_err(|_| OutboxError::PublisherError(ACK_TOOK_TOO_LONG.into()))?
            .map_err(|e| OutboxError::PublisherError(e.kind().to_string()))?;
        Ok(())
    }
}
#[async_trait]
impl<Msg, Identifier> Publisher<Msg> for NATSPublisher<Msg, Identifier>
where
    Msg: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
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

#[cfg(all(test, feature = "nats"))]
mod tests {
    use std::{assert_matches, time::Duration};

    use async_nats::Client;
    use base64::{Engine, prelude::BASE64_STANDARD};
    use dtor::dtor;
    use testcontainers::{
        ContainerAsync, GenericImage, ImageExt,
        core::{ContainerPort, WaitFor},
        runners::AsyncRunner,
    };
    use tokio::sync::OnceCell;
    use uuid::Uuid;

    use crate::{
        error::OutboxError,
        model::{Message, MessageStatus},
        nats::{ACK_TOOK_TOO_LONG, NATSPublisher},
        publisher::Publisher,
    };

    #[derive(Clone, Eq, Hash, PartialEq, Debug)]
    struct TestMessage {
        id: Uuid,
        status: MessageStatus,
        subject: String,
        payload: serde_json::Value,
    }

    impl Message<Uuid> for TestMessage {
        fn id(&self) -> &Uuid {
            &self.id
        }

        fn status(&self) -> crate::model::MessageStatus {
            self.status.clone()
        }

        fn subject(&self) -> &str {
            &self.subject
        }

        fn payload(&self) -> &serde_json::Value {
            &self.payload
        }

        fn name() -> &'static str {
            "test_message"
        }
    }

    struct NATSEnvironment {
        nats_port: u16,
        _nats: ContainerAsync<GenericImage>,
    }

    static NATS_CONTAINER: OnceCell<NATSEnvironment> = OnceCell::const_new();

    async fn nats_env() -> &'static NATSEnvironment {
        NATS_CONTAINER
            .get_or_init(|| async {
                let nats = GenericImage::new("nats", "latest")
                    .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
                    .with_mapped_port(4222, ContainerPort::Tcp(4222))
                    .with_mapped_port(8222, ContainerPort::Tcp(8222))
                    .with_cmd(["-js", "-sd", "/data", "-m", "8222"])
                    .start()
                    .await
                    .expect("Could not start NATS container");
                let nats_port = nats.get_host_port_ipv4(4222).await.unwrap();
                NATSEnvironment {
                    nats_port,
                    _nats: nats,
                }
            })
            .await
    }

    #[dtor(unsafe)]
    fn clean_up() {
        if let Some(env) = NATS_CONTAINER.get() {
            let id = env._nats.id();
            std::process::Command::new("docker")
                .args(["container", "rm", "-f", id])
                .output()
                .expect("failed to stop testcontainer");
        }
    }

    #[tokio::test]
    async fn test() {
        let env = nats_env().await;
        let url = format!("nats://localhost:{}", env.nats_port);
        let nats_client = async_nats::connect(&url).await.unwrap();
        let jetstream = async_nats::jetstream::new(nats_client);

        // Determine if TLS is required based on the URL scheme
        let needs_tls = url.starts_with("wss://") || url.starts_with("nats+tls://");
        let nats_credentials = String::new();

        // Connect to NATS with or without credentials
        let client: Client = if nats_credentials.trim().is_empty() {
            async_nats::connect(&url).await
        } else {

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
                .connect(&url)
                .await
        }.unwrap();

        let publisher: NATSPublisher<TestMessage, Uuid> =
            NATSPublisher::new(client.clone(), Duration::from_secs(10))
                .await
                .unwrap();

        let stream_name = "test-pending-publish";
        let subject = "com.test.pending.domain-events.created";

        let _ = jetstream.delete_stream(stream_name).await;

        jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: stream_name.to_string(),
                subjects: vec![subject.to_string()],
                max_messages: 2,
                discard: async_nats::jetstream::stream::DiscardPolicy::New,
                ..Default::default()
            })
            .await
            .expect("Failed to create stream");

        let message = TestMessage {
            id: Uuid::now_v7(),
            status: MessageStatus::PENDING,
            subject: subject.to_string(),
            payload: serde_json::json!({
                "id": "test",
                "aggregate_type": "user"
            }),
        };
        publisher.publish(message).await.unwrap();

        let mut stream = jetstream.get_stream(stream_name).await.unwrap();
        let last_sequence = stream.info().await.unwrap().state.last_sequence;
        assert!(
            last_sequence >= 1,
            "At least one message should be in the stream"
        );

        let message = TestMessage {
            id: Uuid::now_v7(),
            status: MessageStatus::PENDING,
            subject: subject.to_string(),
            payload: serde_json::json!({
                "id": "test2",
                "aggregate_type": "user"
            }),
        };
        publisher.publish(message).await.unwrap();
        let last_sequence = stream.info().await.unwrap().state.last_sequence;

        assert!(
            last_sequence >= 2,
            "At least two message should be in the stream"
        );

        let message = TestMessage {
            id: Uuid::now_v7(),
            status: MessageStatus::PENDING,
            subject: subject.to_string(),
            payload: serde_json::json!({
                "id": "test3",
                "aggregate_type": "user"
            }),
        };
        let result = publisher.publish(message).await;
        assert_matches!(result.unwrap_err(), OutboxError::PublisherError(_));

        let message = TestMessage {
            id: Uuid::now_v7(),
            status: MessageStatus::PENDING,
            subject: subject.to_string(),
            payload: serde_json::json!({
                "id": "test4",
                "aggregate_type": "user"
            }),
        };

        let induce_ack_fail_publisher: NATSPublisher<TestMessage, Uuid> =
            NATSPublisher::new(client.clone(), Duration::from_nanos(1))
                .await
                .unwrap();

        let result = induce_ack_fail_publisher.publish(message).await;
        assert_matches!(result.unwrap_err(), OutboxError::PublisherError(error) if error == ACK_TOOK_TOO_LONG);
    }
}
