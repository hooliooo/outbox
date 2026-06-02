pub mod config;
pub mod error;
pub mod model;
pub mod processor;
pub mod publisher;
pub mod repository;

#[cfg(feature = "nats")]
#[path = "nats/nats.rs"]
pub mod nats;

#[cfg(feature = "postgres")]
#[path = "postgres/postgres.rs"]
pub mod postgres;
