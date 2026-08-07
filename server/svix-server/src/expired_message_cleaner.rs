// SPDX-FileCopyrightText: © 2022 Svix Authors
// SPDX-License-Identifier: MIT

use std::time::{Duration, Instant};

use chrono::Utc;
use sea_orm::{
    ConnectionTrait, DatabaseConnection, DbErr, ExecResult, Statement, TransactionTrait,
    UpdateResult, Value,
};

use crate::{
    cfg::Configuration,
    core::types::{BaseId, MessageId},
    error::Result,
};

type DbResult<T> = std::result::Result<T, DbErr>;

async fn exec_without_timeout(pool: &DatabaseConnection, stmt: Statement) -> DbResult<ExecResult> {
    let increase_timeout = Statement::from_string(
        pool.get_database_backend(),
        "SET LOCAL statement_timeout=0;",
    );
    let tx = pool.begin().await?;
    let _ = tx.execute(increase_timeout).await?;
    let res = tx.execute(stmt).await?;
    tx.commit().await?;
    Ok(res)
}

/// Deletes `messagecontent` rows whose own `expiration` (settable per-message via the API's
/// `pruneRetentionPeriod`) has passed or that belong to a message older than `older_than`, and
/// permanently prunes `messageattempt` and `message` rows older than `older_than`. `limit` sets
/// how many rows to delete at a time, per query.
pub async fn clean_expired_messages(
    pool: &DatabaseConnection,
    limit: i32,
    older_than: chrono::DateTime<Utc>,
) -> DbResult<UpdateResult> {
    let batch_limit: Value = limit.into();
    let cutoff: Value = MessageId::start_id(older_than).to_string().into();

    // `messagecontent.id` *is* the message id, so `id < $cutoff` here means the same thing as it
    // does in the row-pruning queries below: the message is past the operator's retention period.
    //
    // Both criteria are needed, OR'd. `messagecontent` has no FK/cascade tying it to `message`, so
    // the row-pruning queries below never touch it - without `expiration <= now()` a payload asked
    // to be wiped early (privacy) would linger until the global cutoff, and without `id < $cutoff`
    // any content whose own `expiration` outlives that cutoff would be orphaned once its `message`
    // row is pruned out from under it.
    let content_stmt = Statement::from_sql_and_values(
        pool.get_database_backend(),
        r#"
        DELETE FROM messagecontent WHERE id = any(
            array(
                SELECT id FROM messagecontent
                WHERE
                    expiration <= now()
                    OR id < $1
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
        )
    "#,
        [cutoff.clone(), batch_limit.clone()],
    );
    let expired_content = pool.execute(content_stmt).await?.rows_affected();

    // Permanently prune `messageattempt` and `message` rows older than the retention period.
    let attempt_stmt = Statement::from_sql_and_values(
        pool.get_database_backend(),
        r#"
        DELETE FROM messageattempt WHERE id IN (
            SELECT id FROM messageattempt WHERE msg_id < $1 ORDER BY msg_id LIMIT $2
        )
    "#,
        [cutoff.clone(), batch_limit.clone()],
    );
    let pruned_attempts = exec_without_timeout(pool, attempt_stmt).await?.rows_affected();

    let message_stmt = Statement::from_sql_and_values(
        pool.get_database_backend(),
        r#"
        DELETE FROM message WHERE id IN (
            SELECT id FROM message WHERE id < $1 ORDER BY id LIMIT $2
        )
    "#,
        [cutoff, batch_limit],
    );
    let pruned_messages = exec_without_timeout(pool, message_stmt).await?.rows_affected();

    Ok(UpdateResult {
        rows_affected: expired_content + pruned_attempts + pruned_messages,
    })
}

/// Polls the database for `messagecontent` rows to delete once their own `expiration` passes,
/// and for `message`/`messageattempt`/`messagecontent` rows to permanently prune once past the
/// configured retention period.
///
/// Uses a variable polling schedule, based on affected row counts each iteration of the loop.
pub async fn expired_message_cleaner_loop(
    cfg: &Configuration,
    pool: &DatabaseConnection,
) -> Result<()> {
    const ON_ERROR: Duration = Duration::from_secs(10);
    let batch_size = cfg.expired_message_cleaner_batch_size;
    let retention_period_days = cfg.prune_retention_period_days;
    // When fewer rows than the batch size have been pruned, take a nap for this long.
    let idle = Duration::from_secs(cfg.expired_message_cleaner_idle_sleep_secs as u64);
    let mut sleep_time = None;
    while !crate::is_shutting_down() {
        if let Some(duration) = sleep_time {
            if crate::shutting_down_token()
                .run_until_cancelled_owned(tokio::time::sleep(duration))
                .await
                .is_none()
            {
                return Ok(());
            }
        }

        let older_than = Utc::now() - chrono::Duration::days(retention_period_days.into());

        let start = Instant::now();
        match clean_expired_messages(pool, batch_size, older_than).await {
            Err(err) => {
                tracing::error!("{}", err);
                sleep_time = Some(ON_ERROR);
            }
            Ok(UpdateResult { rows_affected }) => {
                if rows_affected > 0 {
                    tracing::debug!(elapsed =? start.elapsed(), "expired/pruned {} row(s)", rows_affected);
                }

                sleep_time = if rows_affected < (batch_size as _) {
                    Some(idle)
                } else {
                    // When we see full batches, don't sleep at all.
                    None
                };
            }
        }
    }

    Ok(())
}
