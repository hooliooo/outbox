//! The [`SqlxRespository`] is an implementation of the trait [`Repository`](crate::repository::Repository) that uses the sqlx crate
//!
use std::{
    fmt::{Debug, Display},
    future::Future,
    hash::Hash,
    marker::PhantomData,
    pin::Pin,
};

use async_trait::async_trait;
use sqlx::{AssertSqlSafe, PgPool};
use time::OffsetDateTime;

use crate::{
    error::OutboxError,
    model::{Message, MessageStatus},
    repository::Repository,
};

/// A sqlx implemenation of the [`Repository`](outbox_core::repository::Repository)
pub struct SqlxRespository<Msg, Identifier> {
    pool: PgPool,
    _marker: PhantomData<(Msg, Identifier)>,
}

impl<Msg, Identifier> SqlxRespository<Msg, Identifier>
where
    Msg: Clone
        + Debug
        + Message<Identifier>
        + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>
        + Unpin
        + Send
        + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Send + Sync,
{
    /// Creates a new instance of the SqlxRespository
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            _marker: PhantomData,
        }
    }

    pub async fn with_transaction<T, F>(&self, f: F) -> Result<T, OutboxError>
    where
        T: Send,
        F: for<'c> FnOnce(
            &'c mut sqlx::postgres::PgConnection,
        )
            -> Pin<Box<dyn Future<Output = Result<T, OutboxError>> + Send + 'c>>,
    {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        let result = f(&mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(result)
    }
}

#[async_trait]
impl<Msg, Identifier> Repository<Msg, Identifier> for SqlxRespository<Msg, Identifier>
where
    Msg: Clone
        + Debug
        + Message<Identifier>
        + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>
        + Unpin
        + Send
        + Sync,
    Identifier: Eq + Hash + PartialEq + Display + Clone + Send + Sync,
{
    async fn fetch_by_status(
        &self,
        status: MessageStatus,
        limit: u32,
    ) -> Result<Vec<Msg>, OutboxError> {
        let query = AssertSqlSafe(format!(
            "SELECT * FROM {} WHERE status = $1 ORDER BY created_at ASC LIMIT {}",
            Msg::name(),
            limit
        ));
        let results: Vec<Msg> = sqlx::query_as(query)
            .bind(status.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
        Ok(results)
    }

    async fn claim(
        &self,
        ids: Vec<Identifier>,
        expected_status: MessageStatus,
    ) -> Result<Vec<Identifier>, OutboxError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let id_strings: Vec<String> = ids.iter().map(|id| id.to_string()).collect();

        let rows: Vec<(String,)> = self
            .with_transaction(|conn| {
                Box::pin(async move {
                    let update_query = AssertSqlSafe(format!(
                        "UPDATE {} SET status = $1 WHERE id = ANY($2) AND status = $3",
                        Msg::name()
                    ));
                    sqlx::query(update_query)
                        .bind(MessageStatus::PROCESSING.to_string())
                        .bind(&id_strings)
                        .bind(expected_status.to_string())
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

                    let select_query = AssertSqlSafe(format!(
                        "SELECT id FROM {} WHERE id = ANY($1) AND status = $2",
                        Msg::name()
                    ));
                    let rows: Vec<(String,)> = sqlx::query_as(select_query)
                        .bind(&id_strings)
                        .bind(MessageStatus::PROCESSING.to_string())
                        .fetch_all(&mut *conn)
                        .await
                        .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
                    Ok(rows)
                })
            })
            .await?;

        let claimed_strings: std::collections::HashSet<String> =
            rows.into_iter().map(|(id,)| id).collect();
        Ok(ids
            .into_iter()
            .filter(|id| claimed_strings.contains(&id.to_string()))
            .collect())
    }

    /// Optimized Postgres override using `SELECT … FOR UPDATE SKIP LOCKED`
    /// to atomically fetch and claim in a single transaction with no wasted
    /// reads under contention.
    async fn fetch_and_claim(
        &self,
        status: MessageStatus,
        limit: u32,
    ) -> Result<Vec<Msg>, OutboxError> {
        let results = self.with_transaction(|conn| {
            Box::pin(async move {
                let select_query = AssertSqlSafe(format!(
                    "SELECT * FROM {} WHERE status = $1 ORDER BY created_at ASC LIMIT {} FOR UPDATE SKIP LOCKED",
                    Msg::name(),
                    limit
                ));
                let results: Vec<Msg> = sqlx::query_as(select_query)
                    .bind(status.to_string())
                    .fetch_all(&mut *conn)
                    .await
                    .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

                if !results.is_empty() {
                    let ids: Vec<String> = results.iter().map(|m| m.id().to_string()).collect();
                    let update_query = AssertSqlSafe(format!(
                        "UPDATE {} SET status = $1 WHERE id = ANY($2)",
                        Msg::name()
                    ));
                    sqlx::query(update_query)
                        .bind(MessageStatus::PROCESSING.to_string())
                        .bind(&ids)
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
                }

                Ok(results)
            })
        }).await?;

        Ok(results)
    }

    async fn recover_stale(&self, stale_threshold_in_secs: u64) -> Result<u64, OutboxError> {
        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "UPDATE {} SET status = $1 WHERE status = $2 AND created_at < now() - (INTERVAL '1 second' * $3)",
            Msg::name()
        ));
        let result = sqlx::query(query)
            .bind(MessageStatus::PENDING.to_string())
            .bind(MessageStatus::PROCESSING.to_string())
            .bind(stale_threshold_in_secs as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn clean_up(&self, retention_in_days: u32) -> Result<u64, OutboxError> {
        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "
            DELETE FROM {} 
            WHERE id IN (
                SELECT id FROM {}
                WHERE status = 'PUBLISHED'
                AND created_at < now() - (INTERVAL '1 day' * $1)
                LIMIT 1000
            )",
            Msg::name(),
            Msg::name(),
        ));
        let result = sqlx::query(query)
            .bind(retention_in_days as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn update_status(
        &self,
        id: Identifier,
        status: MessageStatus,
        last_error: Option<String>,
    ) -> Result<(), OutboxError> {
        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "
            UPDATE {}
            SET status = $1, published_at = $2, last_error = $3
            WHERE id = $4
            ",
            Msg::name()
        ));

        let published_at: Option<OffsetDateTime> = match status {
            MessageStatus::PENDING | MessageStatus::PROCESSING | MessageStatus::FAILED => None,
            MessageStatus::PUBLISHED => Some(OffsetDateTime::now_utc()),
        };

        sqlx::query(query)
            .bind(status.to_string())
            .bind(published_at)
            .bind(last_error)
            .bind(id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {

    use crate::model::{Message, MessageStatus};
    use crate::repository::Repository;
    use dtor::dtor;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::Row;
    use sqlx::types::JsonValue;
    use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres::Postgres;
    use time::{Duration, OffsetDateTime};
    use tokio::sync::OnceCell;
    use uuid::Uuid;

    use crate::postgres::SqlxRespository;

    static CONTAINER: OnceCell<ContainerAsync<Postgres>> = OnceCell::const_new();
    static POOL: OnceCell<PgPool> = OnceCell::const_new();

    #[dtor(unsafe)]
    fn clean_up() {
        let container_id = CONTAINER
            .get()
            .map(|c| c.id())
            .expect("failed to get container id");
        std::process::Command::new("docker")
            .args(["container", "rm", "-f", container_id])
            .output()
            .expect("failed to stop testcontainer");
    }

    async fn get_pool() -> &'static PgPool {
        POOL.get_or_init(|| async {
            let container = CONTAINER
                .get_or_init(|| async {
                    Postgres::default()
                        .start()
                        .await
                        .expect("Cannot create Docker container with Postgres")
                })
                .await;

            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("Cannot get port from container");
            let connection_string =
                format!("postgres://postgres:postgres@localhost:{}/postgres", port);
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&connection_string)
                .await
                .expect("Failed to connect to test database");

            // Create the outbox_message_entity table
            sqlx::query(
                r#"
                    CREATE TABLE IF NOT EXISTS outbox_message (
                        id VARCHAR NOT NULL PRIMARY KEY,
                        aggregate_id VARCHAR NOT NULL,
                        aggregate_name VARCHAR NOT NULL,
                        subject VARCHAR NOT NULL,
                        payload JSONB NOT NULL,
                        status VARCHAR NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        published_at TIMESTAMPTZ,
                        retry_count INT NOT NULL,
                        last_error VARCHAR
                    )
                "#,
            )
            .execute(&pool)
            .await
            .expect("Failed to create outbox_message table");
            pool
        })
        .await
    }

    #[allow(dead_code)]
    #[derive(Clone, FromRow, Debug)]
    struct OutboxMessage {
        #[sqlx(try_from = "String")]
        pub id: Uuid,
        pub aggregate_id: String,
        pub aggregate_name: String,
        pub subject: String,
        pub payload: JsonValue,
        #[sqlx(try_from = "String")]
        pub status: MessageStatus,
        pub created_at: OffsetDateTime,
        pub published_at: Option<OffsetDateTime>,
        pub retry_count: i32,
        pub last_error: Option<String>,
    }

    impl Message<Uuid> for OutboxMessage {
        fn id(&self) -> &Uuid {
            &self.id
        }

        fn status(&self) -> MessageStatus {
            self.status.clone()
        }

        fn subject(&self) -> &str {
            &self.subject
        }

        fn payload(&self) -> &JsonValue {
            &self.payload
        }

        fn name() -> &'static str {
            "outbox_message"
        }
    }

    async fn truncate_table(pool: &PgPool) {
        sqlx::query("TRUNCATE TABLE outbox_message")
            .execute(pool)
            .await
            .expect("Failed to truncate outbox_message table");
    }

    async fn create_message(
        pool: &PgPool,
        subject: &'static str,
        status: MessageStatus,
        now: Option<OffsetDateTime>,
        published_at: Option<OffsetDateTime>,
    ) -> Uuid {
        let subject = format!("some.event.prefix.{}", subject);
        let message_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let payload = json!({
            "id": aggregate_id,
            "name": "test"
        });

        let now = now.unwrap_or(OffsetDateTime::now_utc());
        let published_at: Option<OffsetDateTime> = published_at.or(None);

        sqlx::query(
            r#"
            INSERT INTO outbox_message
            (id, aggregate_id, aggregate_name, subject, payload, status, created_at, published_at, retry_count)
            VALUES 
            ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(message_id.to_string())
        .bind(aggregate_id.to_string())
        .bind("user")
        .bind(&subject)
        .bind(&payload)
        .bind(status.to_string())
        .bind(now)
        .bind(published_at)
        .bind(0i64)
        .execute(pool)
        .await
        .unwrap();
        message_id
    }

    async fn get_all_messages_by_status_and_ids(
        pool: &PgPool,
        status: MessageStatus,
        ids: Vec<Uuid>,
    ) -> Vec<OutboxMessage> {
        let ids: Vec<String> = ids.into_iter().map(|x| x.to_string()).collect();
        sqlx::query_as(
            r"
            SELECT * FROM outbox_message 
            WHERE status = $1 
            AND id = ANY($2)
            ",
        )
        .bind(status.to_string())
        .bind(&ids)
        .fetch_all(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn test_database_setup() {
        let pool = get_pool().await;

        // Test that we can query the database
        let result = sqlx::query("SELECT 1").fetch_one(pool).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_fetch_by_status_returns_matching_rows_without_claiming() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut ids: Vec<Uuid> = Vec::with_capacity(3);
        for _ in 0..3 {
            let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;
            ids.push(id);
        }
        create_message(pool, "test-subject", MessageStatus::FAILED, None, None).await;

        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let results = repo
            .fetch_by_status(MessageStatus::PENDING, 10)
            .await
            .unwrap();

        assert_eq!(results.len(), 3);

        // Status should still be PENDING — fetch_by_status does not claim
        let still_pending =
            get_all_messages_by_status_and_ids(pool, MessageStatus::PENDING, ids).await;
        assert_eq!(still_pending.len(), 3);
    }

    #[tokio::test]
    #[serial]
    async fn test_claim_transitions_matching_rows_to_processing() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut ids: Vec<Uuid> = Vec::with_capacity(3);
        for _ in 0..3 {
            let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;
            ids.push(id);
        }

        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let claimed = repo
            .claim(ids.clone(), MessageStatus::PENDING)
            .await
            .unwrap();

        assert_eq!(claimed.len(), 3);

        let now_processing =
            get_all_messages_by_status_and_ids(pool, MessageStatus::PROCESSING, ids).await;
        assert_eq!(now_processing.len(), 3);
    }

    #[tokio::test]
    #[serial]
    async fn test_claim_ignores_rows_with_wrong_status() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let id = create_message(pool, "test-subject", MessageStatus::PUBLISHED, None, None).await;

        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let claimed = repo.claim(vec![id], MessageStatus::PENDING).await.unwrap();

        assert!(claimed.is_empty());

        // Row should still be PUBLISHED
        let still_published =
            get_all_messages_by_status_and_ids(pool, MessageStatus::PUBLISHED, vec![id]).await;
        assert_eq!(still_published.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_fetch_and_claim_pending() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(11);
        for _ in 0..=10 {
            let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;
            pending_message_ids.push(id);
        }
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let limit = 10;
        let pending_messages = repo
            .fetch_and_claim(MessageStatus::PENDING, limit)
            .await
            .unwrap();

        assert_eq!(pending_messages.len(), limit as usize);

        let still_pending = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::PENDING,
            pending_message_ids.clone(),
        )
        .await;
        assert_eq!(still_pending.len(), 1);

        let now_processing = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::PROCESSING,
            pending_message_ids.clone(),
        )
        .await;
        assert_eq!(now_processing.len(), limit as usize);

        for (idx, message) in pending_messages.iter().enumerate() {
            assert_eq!(message.id, pending_message_ids[idx]);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_fetch_and_claim_second_call_gets_remaining() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(11);
        for _ in 0..=10 {
            let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;
            pending_message_ids.push(id);
        }
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());

        let first_batch = repo
            .fetch_and_claim(MessageStatus::PENDING, 10)
            .await
            .unwrap();
        assert_eq!(first_batch.len(), 10);

        let second_batch = repo
            .fetch_and_claim(MessageStatus::PENDING, 10)
            .await
            .unwrap();
        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].id, pending_message_ids[10]);
    }

    #[tokio::test]
    #[serial]
    async fn test_fetch_and_claim_failed() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut failed_message_ids: Vec<Uuid> = Vec::with_capacity(11);
        for _ in 0..=10 {
            let id = create_message(pool, "test-subject", MessageStatus::FAILED, None, None).await;
            failed_message_ids.push(id);
        }
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let limit = 10;
        let failed_messages = repo
            .fetch_and_claim(MessageStatus::FAILED, limit)
            .await
            .unwrap();

        assert_eq!(failed_messages.len(), limit as usize);

        let still_failed = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::FAILED,
            failed_message_ids.clone(),
        )
        .await;
        assert_eq!(still_failed.len(), 1);

        let now_processing = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::PROCESSING,
            failed_message_ids.clone(),
        )
        .await;
        assert_eq!(now_processing.len(), limit as usize);

        for (idx, message) in failed_messages.iter().enumerate() {
            assert_eq!(message.id, failed_message_ids[idx]);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_clean_up() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(10);
        for _ in 0..=9 {
            let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;
            pending_message_ids.push(id);
        }
        let two_days = Duration::days(2);
        let two_days_ago = OffsetDateTime::now_utc() - two_days;

        let mut published_message_ids: Vec<Uuid> = Vec::with_capacity(4);
        for _ in 0..=3 {
            let id = create_message(
                pool,
                "test-subject",
                MessageStatus::PUBLISHED,
                Some(two_days_ago),
                Some(two_days_ago),
            )
            .await;
            published_message_ids.push(id);
        }
        let all_ids: Vec<String> = [published_message_ids, pending_message_ids]
            .concat()
            .into_iter()
            .map(|x| x.to_string())
            .collect();
        let sql_query = r#"
            SELECT COUNT(id) 
            FROM outbox_message 
            WHERE id = ANY($1::TEXT[])
            "#;
        let count = sqlx::query(sql_query)
            .bind(all_ids.clone())
            .fetch_one(pool)
            .await
            .unwrap();
        let count: i64 = count.get(0);
        assert_eq!(count, 14);
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let count = repo.clean_up(1).await.unwrap();
        assert_eq!(count, 4);

        let count = sqlx::query(sql_query)
            .bind(all_ids)
            .fetch_one(pool)
            .await
            .unwrap();
        let count: i64 = count.get(0);
        assert_eq!(count, 10);
    }

    #[tokio::test]
    #[serial]
    async fn test_update_status() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let query = "SELECT * FROM outbox_message WHERE id = $1";
        let id = create_message(pool, "test-subject", MessageStatus::PENDING, None, None).await;

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.published_at.is_none());

        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());

        repo.update_status(id, MessageStatus::PUBLISHED, None)
            .await
            .unwrap();

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.published_at.is_some());
        assert!(message.last_error.is_none());

        repo.update_status(id, MessageStatus::FAILED, Some("test error".to_owned()))
            .await
            .unwrap();

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.published_at.is_none());
        assert_eq!(message.last_error.unwrap(), "test error".to_owned());

        repo.update_status(id, MessageStatus::PENDING, None)
            .await
            .unwrap();

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.published_at.is_none());
        assert!(message.last_error.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_recover_stale_resets_processing_to_pending() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        let ten_minutes_ago = OffsetDateTime::now_utc() - Duration::minutes(10);
        let id = create_message(
            pool,
            "test-subject",
            MessageStatus::PROCESSING,
            Some(ten_minutes_ago),
            None,
        )
        .await;

        // A threshold of 300s (5min) should recover this 10-minute-old row
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let recovered = repo.recover_stale(300).await.unwrap();
        assert_eq!(recovered, 1);

        let messages =
            get_all_messages_by_status_and_ids(pool, MessageStatus::PENDING, vec![id]).await;
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_recover_stale_ignores_recent_processing() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        // Created just now — should NOT be recovered with a 300s threshold
        let id = create_message(pool, "test-subject", MessageStatus::PROCESSING, None, None).await;

        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let recovered = repo.recover_stale(300).await.unwrap();
        assert_eq!(recovered, 0);

        let messages =
            get_all_messages_by_status_and_ids(pool, MessageStatus::PROCESSING, vec![id]).await;
        assert_eq!(messages.len(), 1);
    }
}
