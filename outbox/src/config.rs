//! Runtime configuration for the outbox
//! [`OutboxConfig`] carries the configuration of the outbox processor's functionality
//!

/// Runtime configuration for the outbox
#[derive(Clone)]
pub struct OutboxConfig {
    /// The maximum number of messages fetched to be processed
    pub repository_batch_size: u32,
    /// The maximum number of messages fetched to be processed
    pub publisher_batch_size: u32,
    /// The maximum number of messages deleted in a single clean up run
    pub clean_up_batch_size: u32,
    /// How long (in days) events are kept before being deleted
    pub retention_in_secs: u32,
    /// The interval (in secs) between clean up of messages in the database
    pub clean_up_interval_in_secs: u32,
    /// The interval (in secs) between polling of messages to publish
    pub polling_interval_in_secs: u32,
    /// The interval (in secs) between polling of failed messages to publish
    pub failed_interval_in_secs: u32,
    /// How long (in secs) a message can remain in PROCESSING before it is
    /// considered stale and reset to PENDING for reprocessing
    pub stale_threshold_in_secs: u32,
    /// The maximum character limit of the error recorded on the `last_error` column of the
    /// outboc message database table
    pub max_last_error_length: usize,
    /// The maximum number of publish attempts made for a message
    /// A message can be claimed as long as `retry_count` < `max_publish_attempts`
    pub max_publish_attempts: u32,
}

impl Default for OutboxConfig {
    /// Returns a configuration with the following default values
    ///
    /// | Field                        | Value |
    /// |------------------------------|-------|
    /// | `repository_batch_size`      | 50    |
    /// | `publisher_batch_size`       | 20    |
    /// | `clean_up_batch_size`        | 1000  |
    /// | `retention_in_days`          | 7     |
    /// | `clean_up_interval_in_secs`  | 3600  |
    /// | `polling_interval_in_secs`   | 10    |
    /// | `failed_interval_in_secs`    | 20    |
    /// | `stale_threshold_in_secs`    | 300   |
    /// | `max_last_error_length`      | 1000  |
    /// | `max_publish_attempts`       | 10    |
    ///
    fn default() -> Self {
        Self {
            repository_batch_size: 50,
            publisher_batch_size: 20,
            clean_up_batch_size: 1000,
            retention_in_secs: 7,
            clean_up_interval_in_secs: 3600,
            polling_interval_in_secs: 10,
            failed_interval_in_secs: 20,
            stale_threshold_in_secs: 300,
            max_last_error_length: 1000,
            max_publish_attempts: 10,
        }
    }
}
