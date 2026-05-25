//! [`Processor`] is the worker that executes the work loop at every interval:
//! With a state of [`Pending`] or [`Failed`] it fetches a batch of messages with  
//! the respective status, attempts to publish them, and update the messages' status
//! based on the result of attempt.
//!

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use futures::StreamExt;
use tracing::error;

use crate::config::OutboxConfig;
use crate::error::OutboxError;
use crate::model::Identifiable;
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
    OutboxMessage: Clone + Debug + Identifiable<Identifier>,
    Identifier: Eq + Hash + PartialEq,
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
                match self.publisher.publish(message).await {
                    Ok(()) => {}
                    Err(err) => {
                        error!("Failed to publish message {:?}", err);
                    }
                }
            })
            .await;
    }
}

impl<R, OutboxMessage, Identifier, P> Processor<Pending, R, OutboxMessage, Identifier, P>
where
    R: Repository<OutboxMessage, Identifier>,
    OutboxMessage: Clone + Debug + Identifiable<Identifier>,
    Identifier: Eq + Hash + PartialEq,
    P: Publisher<OutboxMessage>,
{
    /// Processes a batch of pending events.
    ///
    /// Fetches up to the [`OutboxConfig`](crate::config::OutboxConfig) `batch_size` configuration from
    /// the [`Repository`](crate::config::Repository)
    pub async fn process(&self) -> Result<usize, OutboxError> {
        let messages = self
            .repository
            .fetch_pending(self.config.repository_batch_size)
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
    OutboxMessage: Clone + Debug + Identifiable<Identifier>,
    Identifier: Eq + Hash + PartialEq,
    P: Publisher<OutboxMessage>,
{
    /// Processes a batch of failed events.
    ///
    /// Fetches up to the [`OutboxConfig`](crate::config::OutboxConfig) `batch_size` configuration from
    /// the [`Repository`](crate::config::Repository)
    pub async fn process(&self) -> Result<usize, OutboxError> {
        let messages = self
            .repository
            .fetch_failed(self.config.repository_batch_size)
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
    OutboxMessage: Clone + Debug + Identifiable<Identifier>,
    Identifier: Eq + Hash + PartialEq,
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
