//! A [`Repository`] is the abstraction over the persistence layer used to store and read
//! outbox messages.
use std::fmt::Debug;
use std::hash::Hash;

use async_trait::async_trait;

use crate::{error::OutboxError, model::Identifiable};

/// Reads and updates the outbox messages from the persistence layer
#[async_trait]
pub trait Repository<OutboxMessage, Identifier>: Send + Sync
where
    OutboxMessage: Clone + Debug + Identifiable<Identifier>,
    Identifier: Eq + Hash + PartialEq,
{
    /// Fetches outbox messages with a status of
    /// ['MessageStatus::PENDING`](crate::model::MessageStatus)
    /// # Arguments
    /// - `limit` The maximum number of messages fetched
    async fn fetch_pending(&self, limit: u32) -> Result<Vec<OutboxMessage>, OutboxError>;

    /// Fetches outbox messages with a status of
    /// ['MessageStatus::FAILED`](crate::model::MessageStatus)
    /// # Arguments
    /// - `limit` The maximum number of messages fetched
    async fn fetch_failed(&self, limit: u32) -> Result<Vec<OutboxMessage>, OutboxError>;

    /// Removes outbox messages with a status of
    /// ['MessageStatus::PUBLISHED`](crate::model::MessageStatus) and older than the retention period
    /// # Arguments
    /// - `retention_in_days` The number of days published outbox messages from be retained
    async fn clean_up(&self, retention_in_days: u32) -> Result<(), OutboxError>;
}
