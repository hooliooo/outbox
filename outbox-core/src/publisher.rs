//! A [`Publisher`] sends the outbox message to the message broker, message queue or distributed
//! event streaming platofrm
use async_trait::async_trait;
use std::fmt::Debug;

use crate::error::OutboxError;

/// A [`Publisher`] is the component that sends the outbox message to the message infrastructure
#[async_trait]
pub trait Publisher<E>
where
    E: Clone + Debug,
{
    /// Sends the outbox message to the message infrastructure
    /// # Arguments
    /// - `message` The outbox message to be sent
    async fn publish(&self, message: E) -> Result<(), OutboxError>;
}
