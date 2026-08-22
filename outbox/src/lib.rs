pub mod config;
pub mod error;
pub mod model;
pub mod processor;
pub mod publisher;
pub mod repository;

#[cfg(all(feature = "chrono-postgres", feature = "time-postgres"))]
compile_error!(
    "The features 'chrono-postgres' and 'time-postgres' are mutually exclusive. Please choose only one."
);

#[cfg(not(any(feature = "chrono-postgres", feature = "time-postgres")))]
compile_error!("You must enable either 'chrono-postgres' or 'time-postgres' feature.");

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(feature = "postgres")]
pub mod postgres;

pub trait TimestampExt {
    fn utc_now() -> Self;
}

#[cfg(feature = "chrono-postgres")]
pub type Timestamp = chrono::DateTime<chrono::Utc>;

#[cfg(feature = "chrono-postgres")]
impl TimestampExt for Timestamp {
    fn utc_now() -> Self {
        chrono::Utc::now()
    }
}

#[cfg(feature = "time-postgres")]
pub type Timestamp = time::OffsetDateTime;

#[cfg(feature = "time-postgres")]
impl TimestampExt for Timestamp {
    fn utc_now() -> Self {
        time::OffsetDateTime::now_utc()
    }
}

pub trait DurationExt {
    fn days(days: i64) -> Self;
    fn minutes(minutes: i64) -> Self;
    fn seconds(seconds: i64) -> Self;
}

#[cfg(feature = "chrono-postgres")]
pub type Duration = chrono::Duration;

#[cfg(feature = "chrono-postgres")]
impl DurationExt for Duration {
    fn days(days: i64) -> Self {
        chrono::Duration::days(days)
    }

    fn minutes(minutes: i64) -> Self {
        chrono::Duration::minutes(minutes)
    }

    fn seconds(seconds: i64) -> Self {
        chrono::Duration::seconds(seconds)
    }
}

#[cfg(feature = "time-postgres")]
pub type Duration = time::Duration;

#[cfg(feature = "time-postgres")]
impl DurationExt for Duration {
    fn days(days: i64) -> Self {
        time::Duration::days(days)
    }

    fn minutes(minutes: i64) -> Self {
        time::Duration::minutes(minutes)
    }

    fn seconds(seconds: i64) -> Self {
        time::Duration::seconds(seconds)
    }
}
