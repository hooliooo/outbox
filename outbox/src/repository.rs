//! A [`Repository`] is the abstraction over the persistence layer used to store and read
//! outbox messages.
use std::fmt::{Debug, Display};
use std::hash::Hash;

use async_trait::async_trait;

use crate::model::{MessageStatus, PublishOutcome};
use crate::{error::OutboxError, model::Message};

/// Reads and updates the outbox messages from the persistence layer
#[async_trait]
pub trait Repository<OutboxMessage, Identifier>: Send + Sync
where
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
{
    /// Claims a batch of rows up to the `limit` that are not already claimed by `status`
    ///
    /// Implementations are expected to lock the rows it fetches (e.g. `SELECT … FOR UPDATE SKIP LOCKED`
    /// in PostgreSQL) so concurrent workers cannot fetch the same rows and are expected to atomically
    /// switch the status to [`MessageStatus::PROCESSING`]
    ///
    /// # Arguments
    /// - `status` The status of the messages to fetch
    /// - `limit` The maximum number of messages to fetch
    /// - `max_publish_attempts` Only claim messages with `retry_count < max_publish_attempts`
    ///
    /// # Errors
    /// Returns an [`OutboxError`] if the database operation fails
    async fn fetch_next_to_process_by_status(
        &self,
        status: MessageStatus,
        limit: u32,
        max_publish_attempts: u32,
    ) -> Result<Vec<OutboxMessage>, OutboxError>;

    /// Resets messages that have been stuck in
    /// [`MessageStatus::PROCESSING`](crate::model::MessageStatus) longer than
    /// `stale_threshold_in_secs` back to [`MessageStatus::PENDING`](crate::model::MessageStatus)
    /// so they can be retried.
    ///
    /// Returns the number of messages recovered.
    ///
    /// # Arguments
    /// - `stale_threshold_in_secs` How long a message may remain in PROCESSING before recovery
    async fn recover_stale(&self, stale_threshold_in_secs: u32) -> Result<u64, OutboxError>;

    /// Removes outbox messages with a status of
    /// [`MessageStatus::PUBLISHED`](crate::model::MessageStatus) and older than the retention period
    /// # Arguments
    /// - `retention_in_secs` The number of seconds published outbox messages are retained
    async fn clean_up(&self, retention_in_secs: u32, limit: u32) -> Result<u64, OutboxError>;

    /// Updates the status of the outbox message
    /// # Arguments
    /// - `id` The identifier of the message
    /// - `status` The updated status of the message
    async fn update_status_by_outcome(
        &self,
        id: Identifier,
        lease_token: String,
        outcome: PublishOutcome,
    ) -> Result<bool, OutboxError>;
}
