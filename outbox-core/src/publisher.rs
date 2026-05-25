use async_trait::async_trait;
use std::fmt::Debug;

use crate::error::OutboxError;

#[async_trait]
pub trait Publisher<E>
where
    E: Clone + Debug,
{
    async fn publish(&self, message: E) -> Result<(), OutboxError>;
}
