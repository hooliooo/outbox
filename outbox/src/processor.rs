//! [`Processor`] is the worker that executes the work loop at every interval:
//! It operates in three typestates: [`Pending`], [`Failed`], and [`CleanUp`].
//!
//! The [`Pending`] [`Processor`] fetches a batch of messages with the status of [`MessageStatus::Pending`] and attempts to
//! publish them.
//!
//! The [`Failed`] [`Processor`] resets messages that have been set to [`MessageStatus::Processing`] longer
//! than the [`OutboxConfig::stale_threshold_in_secs`] and fetches a batch of messages with the
//! status of [`MessageStatus::Failed`] and attempts to publish them.
//!
//! The [`CleanUp`] [`Processor`] deletes messages that have been set to [`MessageStatus::Published`]
//! and is older than the [`OutboxConfig::retention_in_secs`]
//!

use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::marker::PhantomData;

use futures::StreamExt;
use tracing::{debug, error, info, warn};

use crate::config::OutboxConfig;
use crate::error::OutboxError;
use crate::model::{Message, MessageStatus, PublishOutcome};
use crate::publisher::Publisher;
use crate::repository::Repository;

/// Reads messages from the [`Repository`] and publishes the messages to the message queue.
pub struct Processor<State, R, E, Id, P> {
    config: OutboxConfig,
    repository: R,
    publisher: P,
    _marker: PhantomData<(State, E, Id)>,
}

/// State for the [`Processor`] to read and act on pending messages
pub struct Pending;
/// State for the [`Processor`] to read and act on failed messages
pub struct Failed;
/// State for the [`Processor`] to read and act messages that have reached their retention limit
pub struct CleanUp;

impl<State, R, OutboxMessage, Identifier, P> Processor<State, R, OutboxMessage, Identifier, P>
where
    R: Repository<OutboxMessage, Identifier>,
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
    P: Publisher<OutboxMessage>,
{
    /// Creates a Processor
    pub fn new(config: OutboxConfig, repository: R, publisher: P) -> Self {
        Self {
            config,
            repository,
            publisher,
            _marker: PhantomData,
        }
    }

    /// Publishes the messages in a concurrent manner
    /// # Arguments
    /// * `messages` The messages to be published concurrently
    async fn publish_messages(&self, messages: Vec<OutboxMessage>) {
        let count = self.config.publisher_batch_size as usize;
        futures::stream::iter(messages)
            .for_each_concurrent(count, |message| async move {
                let id = message.id().clone();
                let subject = message.subject().to_owned();

                let lease = match message.lease_token() {
                    Some(token) => token.to_owned(),
                    None => {
                        error!(%id, %subject, "Claimed message has no lease token; skipping to avoid an un-fenceable publish (row will be recovered)");
                        return;
                    }
                };

                #[cfg(feature = "metrics")]
                let start = std::time::Instant::now();
                match self.publisher.publish(message).await {
                    Ok(()) => {
                        debug!(%id, %subject, "Message successfully published");
                        let result = self
                            .repository
                            .update_status_by_outcome(id.clone(), lease,PublishOutcome::Published)
                            .await;

                        #[cfg(feature = "metrics")]
                        let elapsed = start.elapsed().as_secs_f64();

                        match result {
                            Ok(true) => {
                                debug!("Message status updated to PUBLISHED");
                                #[cfg(feature = "metrics")]
                                {
                                    metrics::counter!("outbox.published_total", "subject" => subject.clone()).increment(1);
                                    metrics::histogram!("outbox.publish_duration_in_secs", "subject" => subject).record(elapsed);
                                };
                            }
                            Ok(false) => {
                                warn!(%id, %subject, "Published to message broker but the lease was lost before marking PUBLISHED; row will be reprocessed (dedup handles it)");
                                #[cfg(feature = "metrics")]
                                {
                                    metrics::counter!("outbox.failed_to_update_after_publish_total", "subject" => subject.clone()).increment(1);
                                    metrics::histogram!("outbox.failed_to_update_after_publish_duration_in_secs", "subject" => subject.clone()).record(elapsed);
                                };
                            }
                            Err(err) => {
                                error!("Message status not updated: {:?}", err);
                                #[cfg(feature = "metrics")]
                                {
                                    metrics::counter!("outbox.failed_to_update_after_publish_total", "subject" => subject.clone()).increment(1);
                                    metrics::histogram!("outbox.failed_to_update_after_publish_duration_in_secs", "subject" => subject).record(elapsed);
                                };
                            },
                        }
                    }
                    Err(err) => {
                        error!("Failed to publish message {:?}", err);
                        #[cfg(feature = "metrics")]
                        {
                            let elapsed = start.elapsed().as_secs_f64();
                            metrics::counter!("outbox.failed_to_publish_total", "subject" => subject.clone()).increment(1);
                            metrics::histogram!("outbox.failed_to_publish_duration_in_secs", "subject" => subject.clone()).record(elapsed);
                        };

                        let result = self
                            .repository
                            .update_status_by_outcome(id.clone(), lease, PublishOutcome::Failed(err.to_string()))
                            .await;

                        match result {
                            Ok(true) => debug!(%id, %subject, "Message status updated to FAILED"),
                            Ok(false) => warn!(%id, %subject, "Lease was lost before marking FAILED; row was reset or reclaimed and will be reprocessed."),
                            Err(err) => error!(%id, %subject, error = ?err, "Failed to update message status to FAILED"),
                        }
                    }
                }
            })
            .await;
    }
}

impl<R, OutboxMessage, Identifier, P> Processor<Pending, R, OutboxMessage, Identifier, P>
where
    R: Repository<OutboxMessage, Identifier>,
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
    P: Publisher<OutboxMessage>,
{
    /// Processes a batch of pending events.
    ///
    /// Fetches and claims up to `repository_batch_size` PENDING messages via [`Repository::fetch_next_to_process_by_status`], then publishes them.
    pub async fn process(&self) -> Result<usize, OutboxError> {
        let messages = self
            .repository
            .fetch_next_to_process_by_status(
                MessageStatus::Pending,
                self.config.repository_batch_size,
                self.config.max_publish_attempts,
            )
            .await?;
        if messages.is_empty() {
            debug!("No PENDING messages to publish");
            return Ok(0);
        }
        let count = messages.len();
        self.publish_messages(messages).await;
        Ok(count)
    }
}

impl<R, OutboxMessage, Identifier, P> Processor<Failed, R, OutboxMessage, Identifier, P>
where
    R: Repository<OutboxMessage, Identifier>,
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
    P: Publisher<OutboxMessage>,
{
    /// Recovers stale PROCESSING messages, then processes a batch of failed events.
    ///
    /// Messages that have been stuck in PROCESSING longer than
    /// [`OutboxConfig::stale_threshold_in_secs`](crate::config::OutboxConfig) are reset
    /// to PENDING so the pending processor can retry them. After recovery, up to
    /// `repository_batch_size` FAILED messages are fetched, claimed, and republished.
    pub async fn process(&self) -> Result<usize, OutboxError> {
        match self
            .repository
            .recover_stale(self.config.stale_threshold_in_secs)
            .await
        {
            Ok(recovered) if recovered > 0 => {
                debug!(
                    "{} stale PROCESSING message(s) recovered to PENDING",
                    recovered
                );

                #[cfg(feature = "metrics")]
                metrics::counter!("outbox.events_reverted_from_processing_to_pending_total")
                    .increment(recovered);
            }
            Ok(_) => {
                debug!("No stale messages");
            }
            Err(err) => {
                error!("Failed to recover stale PROCESSING messages: {:?}", err);
            }
        }

        let messages = self
            .repository
            .fetch_next_to_process_by_status(
                MessageStatus::Failed,
                self.config.repository_batch_size,
                self.config.max_publish_attempts,
            )
            .await?;
        if messages.is_empty() {
            debug!("No FAILED messages to publish");
            return Ok(0);
        }
        let count = messages.len();
        self.publish_messages(messages).await;
        Ok(count)
    }
}

impl<R, OutboxMessage, Identifier, P> Processor<CleanUp, R, OutboxMessage, Identifier, P>
where
    R: Repository<OutboxMessage, Identifier>,
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
    P: Publisher<OutboxMessage>,
{
    /// Processes a batch of events that have reached the retention period.
    ///
    /// Fetches the messages that have reached the `retention_in_days` defined in the [`OutboxConfig`]
    pub async fn process(&self) -> Result<(), OutboxError> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        let deleted = self
            .repository
            .clean_up(
                self.config.retention_in_secs,
                self.config.clean_up_batch_size,
            )
            .await?;

        if deleted >= self.config.clean_up_batch_size as u64 {
            warn!(
                deleted,
                batch_size = self.config.clean_up_batch_size,
                "Cleanup hit the batch limit; expired messages likely remain for the next interval"
            )
        } else if deleted > 0 {
            info!(deleted, "Deleted published messages past retention.");
        }

        #[cfg(feature = "metrics")]
        {
            let elapsed = start.elapsed().as_secs_f64();
            metrics::histogram!("outbox.message_clean_up_duration_in_secs").record(elapsed);
        };
        Ok(())
    }
}
