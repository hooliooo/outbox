//! Runtime configuration for the outbox
//! [`OutboxConfig`] carries the options of the outbox functionality
//!

/// Runtime configuration for the outbox
#[derive(Clone)]
pub struct OutboxConfig {
    /// The maximum number of messages fetched to be processed
    pub repository_batch_size: u32,
    /// The maximum number of messages fetched to be processed
    pub publisher_batch_size: u32,
    /// How long (in days) events are kept before being deleted
    pub retention_in_days: u32,
    /// The interval (in secs) between clean up of messages in the database
    pub clean_up_interval_in_secs: u32,
    /// The interval (in secs) between polling of messages to publish
    pub polling_interval_in_secs: u32,
}

impl Default for OutboxConfig {
    /// Returns a configuration with the following default values
    ///
    /// | Field                        | Value |
    /// |------------------------------|-------|
    /// | `repository_batch_size`      | 50    |
    /// | `publisher_batch_size`       | 20    |
    /// | `retention_in_days`          | 7     |
    /// | `clean_up_interval_in_secs`  | 3600  |
    /// | `polling_interval_in_secs`   | 10    |
    ///
    fn default() -> Self {
        Self {
            repository_batch_size: 50,
            publisher_batch_size: 20,
            retention_in_days: 7,
            clean_up_interval_in_secs: 3600,
            polling_interval_in_secs: 10,
        }
    }
}
