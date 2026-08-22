//! The [`SqlxRepository`] is an implementation of the trait [`Repository`] that uses the sqlx crate
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

use crate::{
    Duration, Timestamp, TimestampExt,
    error::OutboxError,
    model::{Message, MessageStatus, PublishOutcome},
    repository::Repository,
};

/// A sqlx implemenation of the [`Repository`]
pub struct SqlxRepository<Msg, Identifier> {
    pool: PgPool,
    _marker: PhantomData<(Msg, Identifier)>,
}

impl<Msg, Identifier> SqlxRepository<Msg, Identifier>
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
impl<Msg, Identifier> Repository<Msg, Identifier> for SqlxRepository<Msg, Identifier>
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
    async fn fetch_next_to_process_by_status(
        &self,
        status: MessageStatus,
        limit: u32,
        max_publish_attempts: u32,
    ) -> Result<Vec<Msg>, OutboxError> {
        let query = AssertSqlSafe(format!(
            "UPDATE {table}
                SET status = $1,
                processing_started_at = $2,
                lease_token = gen_random_uuid()::text
             WHERE id IN (
                 SELECT id FROM {table}
                 WHERE status = $3
                 AND retry_count < $4
                 ORDER BY created_at ASC
                 LIMIT {limit}
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING *",
            table = Msg::name(),
        ));

        let results: Vec<Msg> = sqlx::query_as::<sqlx::Postgres, Msg>(query)
            .bind(MessageStatus::Processing.to_string())
            .bind(utc_now())
            .bind(status.to_string())
            .bind(max_publish_attempts as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(results)
    }

    async fn recover_stale(&self, stale_threshold_in_secs: u32) -> Result<u64, OutboxError> {
        let cutoff = utc_now() - Duration::seconds(stale_threshold_in_secs as i64);
        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "UPDATE {table}
                SET status = $1,
                processing_started_at = NULL,
                lease_token = NULL
            WHERE status = $2
            AND (processing_started_at IS NULL OR processing_started_at < $3)",
            table = Msg::name()
        ));
        let result = sqlx::query::<sqlx::Postgres>(query)
            .bind(MessageStatus::Pending.to_string())
            .bind(MessageStatus::Processing.to_string())
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn clean_up(&self, retention_in_days: u32, limit: u32) -> Result<u64, OutboxError> {
        let cutoff = utc_now() - Duration::seconds(retention_in_days as i64);
        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "
            DELETE FROM {table}
            WHERE id IN (
                SELECT id FROM {table}
                WHERE status = 'PUBLISHED'
                AND published_at < $1
                LIMIT $2
            )",
            table = Msg::name(),
        ));
        let result = sqlx::query::<sqlx::Postgres>(query)
            .bind(cutoff)
            .bind(limit as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn update_status_by_outcome(
        &self,
        id: Identifier,
        lease_token: String,
        outcome: PublishOutcome,
    ) -> Result<bool, OutboxError> {
        let (status, published_at, last_error, retry_count): (
            MessageStatus,
            Option<Timestamp>,
            Option<String>,
            &'static str,
        ) = match outcome {
            PublishOutcome::Published => (MessageStatus::Published, Some(utc_now()), None, "0"),
            PublishOutcome::Failed(error) => {
                (MessageStatus::Failed, None, Some(error), "retry_count + 1")
            }
        };

        let query: AssertSqlSafe<String> = AssertSqlSafe(format!(
            "
            UPDATE {table}
            SET status = $1,
                published_at = $2,
                last_error = $3,
                retry_count = {retry_count},
                processing_started_at = NULL,
                lease_token = NULL
            WHERE id = $4 AND lease_token =$5 AND status = $6
            ",
            table = Msg::name()
        ));

        let result = sqlx::query::<sqlx::Postgres>(query)
            .bind(status.to_string())
            .bind(published_at)
            .bind(last_error)
            .bind(id.to_string())
            .bind(lease_token)
            .bind(MessageStatus::Processing.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }
}

/// The current UTC instant as a `PrimitiveDateTime`
fn utc_now() -> Timestamp {
    Timestamp::utc_now()
}

#[cfg(all(test, feature = "postgres"))]
mod tests {

    use crate::model::{Message, MessageStatus, PublishOutcome};
    use crate::repository::Repository;
    use crate::{Duration, Timestamp, TimestampExt};
    use dtor::dtor;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::Row;
    use sqlx::types::JsonValue;
    use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
    use testcontainers::ImageExt;
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres::Postgres;
    use tokio::sync::OnceCell;
    use uuid::Uuid;

    use crate::postgres::SqlxRepository;

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
                        .with_tag("18-alpine")
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
                        last_error VARCHAR,
                        processing_started_at TIMESTAMPTZ,
                        lease_token VARCHAR
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
        pub created_at: Timestamp,
        pub published_at: Option<Timestamp>,
        pub retry_count: i32,
        pub last_error: Option<String>,
        pub processing_started_at: Option<Timestamp>,
        pub lease_token: Option<String>,
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

        fn lease_token(&self) -> Option<&str> {
            self.lease_token.as_deref()
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
        now: Option<Timestamp>,
        published_at: Option<Timestamp>,
    ) -> Uuid {
        let subject = format!("some.event.prefix.{}", subject);
        let message_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let payload = json!({
            "id": aggregate_id,
            "name": "test"
        });

        let now = now.unwrap_or(Timestamp::utc_now());
        let published_at: Option<Timestamp> = published_at.or(None);
        // A PROCESSING row models one that has already been claimed, so its lease started at `now`
        // and it carries a fencing token (mirrors what `claim`/`fetch_and_claim` stamp).
        let processing_started_at: Option<Timestamp> =
            matches!(status, MessageStatus::Processing).then_some(now);
        let lease_token: Option<String> =
            matches!(status, MessageStatus::Processing).then(|| Uuid::now_v7().to_string());

        sqlx::query(
            r#"
            INSERT INTO outbox_message
            (id, aggregate_id, aggregate_name, subject, payload, status, created_at, published_at, retry_count, processing_started_at, lease_token)
            VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
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
        .bind(processing_started_at)
        .bind(lease_token)
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
    async fn test_fetch_next_to_process_by_status_pending() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(11);
        for _ in 0..=10 {
            let id = create_message(pool, "test-subject", MessageStatus::Pending, None, None).await;
            pending_message_ids.push(id);
        }
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());
        let limit = 10;
        let pending_messages = repo
            .fetch_next_to_process_by_status(MessageStatus::Pending, limit, 1)
            .await
            .unwrap();

        assert_eq!(pending_messages.len(), limit as usize);

        let still_pending = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::Pending,
            pending_message_ids.clone(),
        )
        .await;
        assert_eq!(still_pending.len(), 1);

        let now_processing = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::Processing,
            pending_message_ids.clone(),
        )
        .await;
        assert_eq!(now_processing.len(), limit as usize);

        let returned: Vec<Uuid> = {
            let mut ids: Vec<Uuid> = pending_messages.iter().map(|m| m.id).collect();
            ids.sort();
            ids
        };

        let expected: Vec<Uuid> = {
            let mut ids = pending_message_ids[..limit as usize].to_vec();
            ids.sort();
            ids
        };

        assert_eq!(returned, expected);
    }

    #[tokio::test]
    #[serial]
    async fn test_fetch_and_claim_second_call_gets_remaining() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(11);
        for _ in 0..=10 {
            let id = create_message(pool, "test-subject", MessageStatus::Pending, None, None).await;
            pending_message_ids.push(id);
        }
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());

        let first_batch = repo
            .fetch_next_to_process_by_status(MessageStatus::Pending, 10, 1)
            .await
            .unwrap();
        assert_eq!(first_batch.len(), 10);

        let second_batch = repo
            .fetch_next_to_process_by_status(MessageStatus::Pending, 10, 1)
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
            let id = create_message(pool, "test-subject", MessageStatus::Failed, None, None).await;
            failed_message_ids.push(id);
        }
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());
        let limit = 10;
        let failed_messages = repo
            .fetch_next_to_process_by_status(MessageStatus::Failed, limit, 1)
            .await
            .unwrap();

        assert_eq!(failed_messages.len(), limit as usize);

        let still_failed = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::Failed,
            failed_message_ids.clone(),
        )
        .await;
        assert_eq!(still_failed.len(), 1);

        let now_processing = get_all_messages_by_status_and_ids(
            pool,
            MessageStatus::Processing,
            failed_message_ids.clone(),
        )
        .await;
        assert_eq!(now_processing.len(), limit as usize);

        let returned: Vec<Uuid> = {
            let mut ids: Vec<Uuid> = failed_messages.iter().map(|m| m.id).collect();
            ids.sort();
            ids
        };

        let expected: Vec<Uuid> = {
            let mut ids = failed_message_ids[..limit as usize].to_vec();
            ids.sort();
            ids
        };

        assert_eq!(returned, expected);
    }

    #[tokio::test]
    #[serial]
    async fn test_clean_up() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        let mut pending_message_ids: Vec<Uuid> = Vec::with_capacity(10);
        for _ in 0..=9 {
            let id = create_message(pool, "test-subject", MessageStatus::Pending, None, None).await;
            pending_message_ids.push(id);
        }
        let two_days = Duration::days(2);
        let two_days_ago = Timestamp::utc_now() - two_days;

        let mut published_message_ids: Vec<Uuid> = Vec::with_capacity(4);
        for _ in 0..=3 {
            let id = create_message(
                pool,
                "test-subject",
                MessageStatus::Published,
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
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());
        let count = repo.clean_up(1, 1000).await.unwrap();
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
    async fn test_update_status_by_outcome_published() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let query = "SELECT * FROM outbox_message WHERE id = $1";
        // Start from PROCESSING so the row carries a lease — the outcome must clear it on exit.
        let id = create_message(pool, "test-subject", MessageStatus::Processing, None, None).await;

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.published_at.is_none());
        assert!(message.processing_started_at.is_some());
        let lease = message
            .lease_token
            .clone()
            .expect("a PROCESSING row carries a lease token");

        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());

        let updated = repo
            .update_status_by_outcome(id, lease, PublishOutcome::Published)
            .await
            .unwrap();
        // The row still carried the token we claimed with, so the fenced update applied.
        assert!(updated);

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        // Success stamps the publish timestamp, carries no error, and releases the lease + token.
        assert!(message.published_at.is_some());
        assert!(message.last_error.is_none());
        assert!(message.processing_started_at.is_none());
        assert!(message.lease_token.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_update_status_by_outcome_failed() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let query = "SELECT * FROM outbox_message WHERE id = $1";
        // Start from PROCESSING so the row carries a lease that this failing outcome must clear.
        let id = create_message(pool, "test-subject", MessageStatus::Processing, None, None).await;

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        assert!(message.processing_started_at.is_some());
        let lease = message
            .lease_token
            .clone()
            .expect("a PROCESSING row carries a lease token");

        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());

        let updated = repo
            .update_status_by_outcome(id, lease, PublishOutcome::Failed("test error".to_owned()))
            .await
            .unwrap();
        // The row still carried the token we claimed with, so the fenced update applied.
        assert!(updated);

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(id.to_string())
            .fetch_one(pool)
            .await
            .unwrap();

        // A failure leaves the publish timestamp NULL, records the error, and releases the lease + token.
        assert!(message.published_at.is_none());
        assert_eq!(message.last_error.unwrap(), "test error".to_owned());
        assert!(message.processing_started_at.is_none());
        assert!(message.lease_token.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_update_status_by_outcome_is_a_no_op_when_lease_is_lost() {
        let pool = get_pool().await;
        truncate_table(pool).await;
        let query = "SELECT * FROM outbox_message WHERE id = $1";
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());

        // Case 1 — the row is still PROCESSING, but under a DIFFERENT token: models another worker
        // having re-claimed it after our lease was recovered. The fence must reject a mismatched token
        // and leave the new owner's row untouched.
        let reclaimed =
            create_message(pool, "test-subject", MessageStatus::Processing, None, None).await;
        let new_owner_token = sqlx::query_as::<_, OutboxMessage>(query)
            .bind(reclaimed.to_string())
            .fetch_one(pool)
            .await
            .unwrap()
            .lease_token
            .expect("a PROCESSING row carries a lease token");

        let applied = repo
            .update_status_by_outcome(
                reclaimed,
                "not-the-current-token".to_owned(),
                PublishOutcome::Published,
            )
            .await
            .unwrap();
        assert!(
            !applied,
            "a mismatched lease token must not finalize the row"
        );

        let message: OutboxMessage = sqlx::query_as(query)
            .bind(reclaimed.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(message.status(), MessageStatus::Processing);
        assert!(message.published_at.is_none());
        // The new owner's token is still in place — we did not clobber its lease.
        assert_eq!(
            message.lease_token.as_deref(),
            Some(new_owner_token.as_str())
        );

        // Case 2 — a PENDING row: stale recovery already reset it and cleared its token, so no token
        // can match. (A NULL column never equals a bound value in SQL.)
        let reset = create_message(pool, "test-subject", MessageStatus::Pending, None, None).await;
        let reset_applied = repo
            .update_status_by_outcome(reset, new_owner_token.clone(), PublishOutcome::Published)
            .await
            .unwrap();
        assert!(!reset_applied);
        let message: OutboxMessage = sqlx::query_as(query)
            .bind(reset.to_string())
            .fetch_one(pool)
            .await
            .unwrap();
        assert_eq!(message.status(), MessageStatus::Pending);

        // Case 3 — a missing row likewise reports "not applied" rather than erroring.
        let missing = repo
            .update_status_by_outcome(
                Uuid::now_v7(),
                new_owner_token,
                PublishOutcome::Failed("nope".to_owned()),
            )
            .await
            .unwrap();
        assert!(!missing);
    }

    #[tokio::test]
    #[serial]
    async fn test_recover_stale_resets_processing_to_pending() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        let ten_minutes_ago = Timestamp::utc_now() - Duration::minutes(10);
        let id = create_message(
            pool,
            "test-subject",
            MessageStatus::Processing,
            Some(ten_minutes_ago),
            None,
        )
        .await;

        // A threshold of 300s (5min) should recover this 10-minute-old row
        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());
        let recovered = repo.recover_stale(300).await.unwrap();
        assert_eq!(recovered, 1);

        let messages =
            get_all_messages_by_status_and_ids(pool, MessageStatus::Pending, vec![id]).await;
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_recover_stale_ignores_recent_processing() {
        let pool = get_pool().await;
        truncate_table(pool).await;

        // Created just now — should NOT be recovered with a 300s threshold
        let id = create_message(pool, "test-subject", MessageStatus::Processing, None, None).await;

        let repo: SqlxRepository<OutboxMessage, Uuid> = SqlxRepository::new(pool.clone());
        let recovered = repo.recover_stale(300).await.unwrap();
        assert_eq!(recovered, 0);

        let messages =
            get_all_messages_by_status_and_ids(pool, MessageStatus::Processing, vec![id]).await;
        assert_eq!(messages.len(), 1);
    }
}
