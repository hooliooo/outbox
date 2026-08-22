//! [`Message`] is the trait that encapsulates the minimum interface required for the outbox crate to
//! fetch information about the message from the persistence layer to publish to the message
//! infrastructure
use std::{
    fmt::{Display, Error, Formatter},
    hash::Hash,
};

use serde_json::Value;

/// The trait the outbox message entity must adopt to integrate with the
/// [`Repository`](crate::repository::Repository) properly. It contains the minimum interface
/// required for the outbox crate to fetch the message from the persistence layer to publish to the
/// messge infrastructure with the [`Publisher`](crate::publisher::Publisher).
///
/// There should be a defined struct that contains the complete definition of the outbox message
/// from the persistence layer and that struct should implement this trait.
pub trait Message<Id>
where
    Id: Eq + Hash + PartialEq + Display,
{
    /// The identifier of the outbox message
    fn id(&self) -> &Id;

    /// The status of the outbox message
    fn status(&self) -> MessageStatus;

    /// The subject or topic of the message
    fn subject(&self) -> &str;

    /// The payload sent to be sent
    fn payload(&self) -> &Value;

    /// The lease token stamped when the message is claimed. It is `None`
    /// when not claimed
    fn lease_token(&self) -> Option<&str>;

    /// The name of the outbox message schema
    fn name() -> &'static str;
}

/// The possible statuses of an outbox message
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(feature = "postgres", sqlx(rename_all = "UPPERCASE"))]
pub enum MessageStatus {
    /// The outbox message is waiting to be published
    Pending,
    /// The outbox message has been claimed by a processor instance and is being published
    Processing,
    /// The outbox message has been published
    Published,
    /// The outbox message was not published due to a failure
    Failed,
}

impl Display for MessageStatus {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let string = match self {
            MessageStatus::Pending => "PENDING",
            MessageStatus::Processing => "PROCESSING",
            MessageStatus::Published => "PUBLISHED",
            MessageStatus::Failed => "FAILED",
        };
        write!(f, "{}", string)
    }
}

impl TryFrom<String> for MessageStatus {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "PENDING" => Ok(MessageStatus::Pending),
            "PROCESSING" => Ok(MessageStatus::Processing),
            "PUBLISHED" => Ok(MessageStatus::Published),
            "FAILED" => Ok(MessageStatus::Failed),
            _ => Err(format!("Invalid outbox status string: {}", value)),
        }
    }
}

/// The possible outcomes of a publish attempt
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The message was published successfully.
    Published,
    /// The message failed to publish; carries the error detail to persist.
    Failed(String),
}
