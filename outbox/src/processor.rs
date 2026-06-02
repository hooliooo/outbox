//! [`Processor`] is the worker that executes the work loop at every interval:
//! With a state of [`Pending`] or [`Failed`] it fetches a batch of messages with  
//! the respective status, attempts to publish them, and update the messages' status
//! based on the result of attempt.
//!

use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::marker::PhantomData;

use futures::StreamExt;
use tracing::{debug, error};

use crate::config::OutboxConfig;
use crate::error::OutboxError;
use crate::model::{Message, MessageStatus};
use crate::publisher::Publisher;
use crate::repository::Repository;

/// Reads messages from the [`Repository`](crate::repository::Repository) and
/// publishes the messages to the message queue.
///
pub struct Processor<State, R, E, Id, P> {
    config: OutboxConfig,
    repository: R,
    publisher: P,
    _marker: PhantomData<(State, E, Id)>,
}

/// State for the [`Processor`](crate::processor::Processor) to read and act
/// on pending messages
pub struct Pending;
/// State for the [`Processor`](crate::processor::Processor) to read and act
/// on failed messages
pub struct Failed;
/// State for the [`Processor`](crate::processor::Processor) to read and act
/// messages that have reached their retention limit
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

    async fn publish_messages(&self, messages: Vec<OutboxMessage>) {
        let count = self.config.publisher_batch_size as usize;
        futures::stream::iter(messages)
            .for_each_concurrent(count, |message| async move {
                let id = message.id().clone();
                let subject = message.subject().to_owned();
                
                #[cfg(feature = "metrics")] 
                let start = std::time::Instant::now();
                match self.publisher.publish(message).await {
                    Ok(()) => {
                        debug!("Message successfully published");
                        let result = self
                            .repository
                            .update_status(id, MessageStatus::PUBLISHED, None)
                            .await;

                        #[cfg(feature = "metrics")] 
                        let elapsed = start.elapsed().as_secs_f64();

                        match result {
                            Ok(_) => {
                                debug!("Message status updated to PUBLISHED");
                                #[cfg(feature = "metrics")] 
                                {
                                    metrics::counter!("outbox.published_total", "subject" => subject.clone()).increment(1); 
                                    metrics::histogram!("outbox.publish_duration_in_secs", "subject" => subject).record(elapsed);
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
                            metrics::histogram!("outbox.failed_to_publish_duration_in_secs", "subject" => subject).record(elapsed);
                        };

                        let result = self
                            .repository
                            .update_status(id, MessageStatus::FAILED, Some(err.to_string()))
                            .await;

                        match result {
                            Ok(_) => debug!("Message status updated to FAILED"),
                            Err(err) => error!("Message status not updated: {:?}", err),
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
    /// Fetches and claims up to `repository_batch_size` PENDING messages via
    /// [`Repository::fetch_and_claim`](crate::repository::Repository::fetch_and_claim),
    /// then publishes them.
    pub async fn process(&self) -> Result<usize, OutboxError> {
        let messages = self
            .repository
            .fetch_and_claim(MessageStatus::PENDING, self.config.repository_batch_size)
            .await?;
        if messages.is_empty() {
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
                metrics::counter!("outbox.events_reverted_from_processing_to_pending_total").increment(recovered); 
            }
            Ok(_) => {}
            Err(err) => {
                error!("Failed to recover stale PROCESSING messages: {:?}", err);
            }
        }

        let messages = self
            .repository
            .fetch_and_claim(MessageStatus::FAILED, self.config.repository_batch_size)
            .await?;
        if messages.is_empty() {
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
    /// Fetches the messages that have reached the `retention_in_days` defined in the
    /// [`OutboxConfig`](crate::config::OutboxConfig)
    pub async fn process(&self) -> Result<(), OutboxError> {
        self.repository
            .clean_up(self.config.retention_in_days)
            .await?;
        Ok(())
    }
}
