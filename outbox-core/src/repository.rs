use std::fmt::Debug;

use async_trait::async_trait;

use crate::error::OutboxError;

#[async_trait]
pub trait Repository<Entity>: Send + Sync
where
    Entity: Clone + Debug,
{
    async fn fetch_pending(&self, limit: u32) -> Result<Vec<Entity>, OutboxError>;

    async fn fetch_failed(&self, limit: u32) -> Result<Vec<Entity>, OutboxError>;

    async fn clean_up(&self, retention_in_days: u32) -> Result<(), OutboxError>;
}
