//! [`OutboxError`] represents the group of errors one might encounter using the outbox crate
use thiserror::Error;

/// Encapsulates the kind of errors one might encounter when using the outbox crate
#[derive(Debug, Error)]
pub enum OutboxError {
    /// An error related to the configuration of the outbox crate
    #[error("Configuration error: {0}")]
    ConfigError(String),
    /// An error related to the database operations of the outbox crate
    #[error("Database error: {0}")]
    DatabaseError(String),
    /// An error related to the publish operations of the outbox crate
    #[error("Publisher error: {0}")]
    PublisherError(String),
}
