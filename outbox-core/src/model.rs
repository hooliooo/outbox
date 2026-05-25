use std::{
    fmt::{Display, Error, Formatter},
    hash::Hash,
};

pub trait Identifiable<Id>
where
    Id: Eq + Hash + PartialEq,
{
    fn id(&self) -> &Id;

    fn status(&self) -> MessageStatus;

    fn name() -> &'static str;
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "sqlx", derive(sqlx::Type))]
#[cfg_attr(feature = "sqlx", sqlx(rename_all = "UPPERCASE"))]
pub enum MessageStatus {
    PENDING,
    PUBLISHED,
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
