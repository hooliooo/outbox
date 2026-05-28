use std::{
    fmt::{Display, Error, Formatter},
    hash::Hash,
};

use sqlx::types::JsonValue;

/// The trait the outbox message entity must adopt to integrate with the
/// [`Repository`](crate::repository::Repository) properly
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
    fn payload(&self) -> &JsonValue;

    /// The name of the outbox message schema
    fn name() -> &'static str;
}

/// The possible statuses of an outbox message
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(rename_all = "UPPERCASE"))]
pub enum MessageStatus {
    /// The outbox message is waiting to be published
    PENDING,
    /// The outbox message has been published
    PUBLISHED,
    /// The outbox message was not published due to a failure
    FAILED,
}

impl Display for MessageStatus {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        let string = match self {
            MessageStatus::PENDING => "PENDING",
            MessageStatus::PUBLISHED => "PUBLISHED",
            MessageStatus::FAILED => "FAILED",
        };
        write!(f, "{}", string)
    }
}

impl TryFrom<String> for MessageStatus {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "PENDING" => Ok(MessageStatus::PENDING),
            "PUBLISHED" => Ok(MessageStatus::PUBLISHED),
            "FAILED" => Ok(MessageStatus::FAILED),
            _ => Err(format!("Invalid outbox status string: {}", value)),
        }
    }
}
