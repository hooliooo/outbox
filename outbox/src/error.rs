use thiserror::Error;

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Publisher error: {0}")]
    PublisherError(String),
}
