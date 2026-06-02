//! A [`Repository`] is the abstraction over the persistence layer used to store and read
//! outbox messages.
use std::collections::HashSet;
use std::fmt::{Debug, Display};
use std::hash::Hash;

use async_trait::async_trait;

use crate::model::MessageStatus;
use crate::{error::OutboxError, model::Message};

/// Reads and updates the outbox messages from the persistence layer
#[async_trait]
pub trait Repository<OutboxMessage, Identifier>: Send + Sync
where
    OutboxMessage: Clone + Debug + Message<Identifier> + Send + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
{
    /// Fetches outbox messages matching the given status. This is a plain
    /// read — no locking or claiming is performed.
    ///
    /// # Arguments
    /// - `status` The status of the messages to query
    /// - `limit` The maximum number of messages fetched
    async fn fetch_by_status(
        &self,
        status: MessageStatus,
        limit: u32,
    ) -> Result<Vec<OutboxMessage>, OutboxError>;

    /// Attempts to claim messages by atomically transitioning them from
    /// `expected_status` to [`MessageStatus::PROCESSING`]. Returns the
    /// identifiers of messages that were successfully claimed.
    ///
    /// This is the universal concurrency primitive: a conditional update
    /// (compare-and-swap) that every persistence layer can implement.
    ///
    /// # Arguments
    /// - `ids` The identifiers of the messages to claim
    /// - `expected_status` Only claim messages whose current status matches this value
    async fn claim(
        &self,
        ids: Vec<Identifier>,
        expected_status: MessageStatus,
    ) -> Result<Vec<Identifier>, OutboxError>;

    /// Fetches and claims a batch of messages in one logical operation.
    ///
    /// The default implementation calls [`fetch_by_status`](Self::fetch_by_status)
    /// followed by [`claim`](Self::claim), filtering the result to only include
    /// successfully claimed messages.
    ///
    /// Adapters may override this with an optimized atomic implementation
    /// (e.g. `SELECT … FOR UPDATE SKIP LOCKED` in PostgreSQL) to avoid the
    /// extra round-trip and wasted fetches under contention.
    ///
    /// # Arguments
    /// - `status` The status of the messages to fetch and claim
    /// - `limit` The maximum number of messages to fetch
    async fn fetch_and_claim(
        &self,
        status: MessageStatus,
        limit: u32,
    ) -> Result<Vec<OutboxMessage>, OutboxError> {
        let messages = self.fetch_by_status(status.clone(), limit).await?;
        if messages.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<Identifier> = messages.iter().map(|m| m.id().clone()).collect();
        let claimed_ids = self.claim(ids, status).await?;
        let claimed_set: HashSet<Identifier> = claimed_ids.into_iter().collect();
        Ok(messages
            .into_iter()
            .filter(|m| claimed_set.contains(m.id()))
            .collect())
    }

    /// Resets messages that have been stuck in
    /// [`MessageStatus::PROCESSING`](crate::model::MessageStatus) longer than
    /// `stale_threshold_in_secs` back to [`MessageStatus::PENDING`](crate::model::MessageStatus)
    /// so they can be retried.
    ///
    /// Returns the number of messages recovered.
    ///
    /// # Arguments
    /// - `stale_threshold_in_secs` How long a message may remain in PROCESSING before recovery
    async fn recover_stale(&self, stale_threshold_in_secs: u64) -> Result<u64, OutboxError>;

    /// Removes outbox messages with a status of
    /// [`MessageStatus::PUBLISHED`](crate::model::MessageStatus) and older than the retention period
    /// # Arguments
    /// - `retention_in_days` The number of days published outbox messages from be retained
    async fn clean_up(&self, retention_in_days: u32) -> Result<(), OutboxError>;

    /// Updates the status of the outbox message
    /// # Arguments
    /// - `id` The identifier of the message
    /// - `status` The updated status of the message
    async fn update_status(
        &self,
        id: Identifier,
        status: MessageStatus,
        last_error: Option<String>,
    ) -> Result<(), OutboxError>;
}
