# Outbox

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A transactional outbox pattern implmentation written in Rust. Thrives for at-least-once delivery within distributed systems

## Key Features
* **Async** Uses the async features of Rust
* **Trait-based Architecture** Leverages traits to be persistence and message infrastructure agnostic
* **Concurrency Safe** Designed to handle horizontal scaling
* **Automatic Clean Up** Built-in functionality to clean up published messages

## To Dos
* **Metrics** Add metrics to measure performance like publish counter and duration histogram
* **Dead Letter Queue** Add dead letter queue handling for chronically failed messages
