use async_trait::async_trait;
use outbox_core::error::OutboxError;
use outbox_core::model::MessageStatus;
use outbox_core::{model::Identifiable, repository::Repository};
use sqlx::{AssertSqlSafe, PgPool};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

/// A sqlx implemenation of the [`Repository`](outbox_core::repository::Repository)
pub struct SqlxRespository<Entity, Id> {
    pool: PgPool,
    _marker: PhantomData<(Entity, Id)>,
}

impl<Entity, Id> SqlxRespository<Entity, Id>
where
    Entity: Clone
        + Debug
        + Identifiable<Id>
        + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>
        + Unpin
        + Send
        + Sync,
    Id: Eq + Hash + PartialEq + Send + Sync,
{
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            _marker: PhantomData,
        }
    }

    async fn fetch_messages_by_status(
        &self,
        limit: u32,
        status: MessageStatus,
    ) -> Result<Vec<Entity>, OutboxError> {
        let query = AssertSqlSafe(format!(
            "SELECT * FROM {} WHERE status = $1 LIMIT {}",
            Entity::name(),
            limit
        ));
        let results: Vec<Entity> = sqlx::query_as(query)
            .bind(status.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
        Ok(results)
    }
}

#[async_trait]
impl<Entity, Id> Repository<Entity> for SqlxRespository<Entity, Id>
where
    Entity: Clone
        + Debug
        + Identifiable<Id>
        + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>
        + Unpin
        + Send
        + Sync,
    Id: Eq + Hash + PartialEq + Send + Sync,
{
    async fn fetch_pending(&self, limit: u32) -> Result<Vec<Entity>, OutboxError> {
        self.fetch_messages_by_status(limit, MessageStatus::PENDING)
            .await
    }

    async fn fetch_failed(&self, limit: u32) -> Result<Vec<Entity>, OutboxError> {
        self.fetch_messages_by_status(limit, MessageStatus::FAILED)
            .await
    }

    async fn clean_up(&self, retention_in_days: u32) -> Result<(), OutboxError> {
        let query = AssertSqlSafe(format!(
            "
            DELETE FROM {} 
            WHERE id IN (
                SELECT id FROM {}
                WHERE status = 'PUBLISHED'
                AND created_at < now() - (INTERVAL '1 day' * $1)
                LIMIT 1000
            )",
            Entity::name(),
            Entity::name(),
        ));
        sqlx::query(query)
            .bind(retention_in_days as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| OutboxError::DatabaseError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use dtor::dtor;
    use futures::future::join_all;
    use outbox_core::model::{Identifiable, MessageStatus};
    use outbox_core::repository::Repository;
    use serde_json::json;
    use sqlx::types::JsonValue;
    use sqlx::{FromRow, PgPool, postgres::PgPoolOptions};
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::postgres::Postgres;
    use time::OffsetDateTime;
    use tokio::sync::OnceCell;
    use uuid::Uuid;

    use crate::SqlxRespository;

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

    impl Identifiable<Uuid> for OutboxMessage {
        fn id(&self) -> &Uuid {
            &self.id
        }

        fn status(&self) -> MessageStatus {
            self.status.clone()
        }

        fn name() -> &'static str {
            "outbox_message"
        }
    }

    async fn create_message(pool: &PgPool, subject: &'static str, status: MessageStatus) -> Uuid {
        let subject = format!("some.event.prefix.{}", subject);
        let message_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let payload = json!({
            "id": aggregate_id,
            "name": "test"
        });

        let now = OffsetDateTime::now_utc();
        let published_at: Option<OffsetDateTime> = None;

        let db_id: String = sqlx::query_scalar(
            r#"
            INSERT INTO outbox_message
            (id, aggregate_id, aggregate_name, subject, payload, status, created_at, published_at, retry_count)
            VALUES 
            ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
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
        .fetch_one(pool)
        .await
        .unwrap();
        Uuid::from_str(&db_id).unwrap()
    }

    async fn get_all_messages_by_status(
        pool: &PgPool,
        status: MessageStatus,
    ) -> Vec<OutboxMessage> {
        sqlx::query_as(r"SELECT * FROM outbox_message WHERE status = $1")
            .bind(status.to_string())
            .fetch_all(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_database_setup() {
        let pool = get_pool().await;

        // Test that we can query the database
        let result = sqlx::query("SELECT 1").fetch_one(pool).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_fetch_pending() {
        let pool = get_pool().await;
        let pending_message_ids: Vec<Uuid> = join_all(
            (0..=10).map(|_| create_message(pool, "test-subject", MessageStatus::PENDING)),
        )
        .await;
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let limit = 10;
        let pending_messages = repo.fetch_pending(limit).await.unwrap();
        let all_pending_messages = get_all_messages_by_status(pool, MessageStatus::PENDING).await;
        assert_eq!(pending_messages.len(), limit as usize);
        assert_eq!(all_pending_messages.len(), 11);

        for (idx, message) in pending_messages.iter().enumerate() {
            assert_eq!(message.id, pending_message_ids[idx]);
        }
    }

    #[tokio::test]
    async fn test_fetch_failed() {
        let pool = get_pool().await;
        let failed_message_ids: Vec<Uuid> =
            join_all((0..=10).map(|_| create_message(pool, "test-subject", MessageStatus::FAILED)))
                .await;
        let repo: SqlxRespository<OutboxMessage, Uuid> = SqlxRespository::new(pool.clone());
        let limit = 10;
        let failed_messages = repo.fetch_failed(limit).await.unwrap();
        let all_failed_messages = get_all_messages_by_status(pool, MessageStatus::FAILED).await;
        assert_eq!(failed_messages.len(), limit as usize);
        assert_eq!(all_failed_messages.len(), 11);

        for (idx, message) in failed_messages.iter().enumerate() {
            assert_eq!(message.id, failed_message_ids[idx]);
        }
    }
}
