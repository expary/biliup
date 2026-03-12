use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{
    ITEM_STATUS_DOWNLOADED, ITEM_STATUS_FAILED, ITEM_STATUS_META_READY, ITEM_STATUS_READY_UPLOAD,
    ITEM_STATUS_TRANSCODED, ITEM_STATUS_UPLOADED, JOB_STATUS_IDLE, JOB_STATUS_PAUSED,
    JOB_STATUS_QUEUED, JOB_STATUS_RUNNING, NewYouTubeJob, UpdateYouTubeJob, YouTubeGlobalItem,
    YouTubeGlobalItemListResponse, YouTubeItem, YouTubeItemListResponse, YouTubeItemsQuery,
    YouTubeJob, YouTubeJobLog, YouTubeJobsResponse, YouTubeJobsSummary,
};
use chrono::Utc;
use error_stack::ResultExt;
use ormlite::Model;
use sqlx::{Row, sqlite::SqliteRow};

fn now_ts() -> i64 {
    Utc::now().timestamp()
}

pub const GLOBAL_READY_UPLOAD_RETRY_COOLDOWN_SECONDS: i64 = 600;

fn scheduled_job_status(enabled: i64, next_sync_at: Option<i64>) -> &'static str {
    if enabled != 1 {
        return JOB_STATUS_PAUSED;
    }

    let now = now_ts();
    if next_sync_at.is_none_or(|next| next <= now) {
        JOB_STATUS_QUEUED
    } else {
        JOB_STATUS_IDLE
    }
}

fn row_to_job(row: SqliteRow) -> Result<YouTubeJob, sqlx::Error> {
    Ok(YouTubeJob {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        source_url: row.try_get("source_url")?,
        source_type: row.try_get("source_type")?,
        upload_streamer_id: row.try_get("upload_streamer_id")?,
        enabled: row.try_get("enabled")?,
        sync_interval_seconds: row.try_get("sync_interval_seconds")?,
        auto_publish: row.try_get("auto_publish")?,
        backfill_mode: row.try_get("backfill_mode")?,
        status: row.try_get("status")?,
        last_sync_at: row.try_get("last_sync_at")?,
        next_sync_at: row.try_get("next_sync_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        item_total: row.try_get("item_total").ok(),
        item_pending: row.try_get("item_pending").ok(),
        item_failed: row.try_get("item_failed").ok(),
        item_uploaded: row.try_get("item_uploaded").ok(),
    })
}

pub async fn create_job(pool: &ConnectionPool, payload: NewYouTubeJob) -> AppResult<YouTubeJob> {
    let now = now_ts();
    let enabled = payload.enabled.unwrap_or(true) as i64;
    let auto_publish = payload.auto_publish.unwrap_or(true) as i64;
    let sync_interval = payload.sync_interval_seconds.unwrap_or(1800).max(60);
    let status = scheduled_job_status(enabled, Some(now));

    let result = sqlx::query(
        r#"
        INSERT INTO youtube_jobs
        (name, source_url, source_type, upload_streamer_id, enabled, sync_interval_seconds, auto_publish, backfill_mode, status, last_sync_at, next_sync_at, last_error, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'all', ?8, NULL, ?9, NULL, ?10, ?10)
        "#,
    )
    .bind(payload.name)
    .bind(payload.source_url)
    .bind(payload.source_type)
    .bind(payload.upload_streamer_id)
    .bind(enabled)
    .bind(sync_interval)
    .bind(auto_publish)
    .bind(status)
    .bind(now)
    .bind(now)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;

    let id = result.last_insert_rowid();

    get_job(pool, id).await
}

pub async fn update_job(pool: &ConnectionPool, payload: UpdateYouTubeJob) -> AppResult<YouTubeJob> {
    let now = now_ts();
    let existing = get_job(pool, payload.id).await?;
    let enabled = payload.enabled as i64;
    let status = if !payload.enabled {
        JOB_STATUS_PAUSED
    } else if existing.status == JOB_STATUS_RUNNING {
        JOB_STATUS_RUNNING
    } else {
        scheduled_job_status(enabled, existing.next_sync_at)
    };

    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET name = ?1,
            source_url = ?2,
            source_type = ?3,
            upload_streamer_id = ?4,
            enabled = ?5,
            sync_interval_seconds = ?6,
            auto_publish = ?7,
            status = ?8,
            updated_at = ?9
        WHERE id = ?10
        "#,
    )
    .bind(payload.name)
    .bind(payload.source_url)
    .bind(payload.source_type)
    .bind(payload.upload_streamer_id)
    .bind(enabled)
    .bind(payload.sync_interval_seconds.max(60))
    .bind(payload.auto_publish as i64)
    .bind(status)
    .bind(now)
    .bind(payload.id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;

    get_job(pool, payload.id).await
}

pub async fn get_job(pool: &ConnectionPool, id: i64) -> AppResult<YouTubeJob> {
    let row = sqlx::query(
        r#"
        SELECT id, name, source_url, source_type, upload_streamer_id, enabled, sync_interval_seconds,
               auto_publish, backfill_mode, status, last_sync_at, next_sync_at, last_error, created_at, updated_at
        FROM youtube_jobs
        WHERE id = ?1
        "#,
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .change_context(AppError::Unknown)?;
    row_to_job(row).change_context(AppError::Unknown)
}

pub async fn list_jobs(pool: &ConnectionPool) -> AppResult<YouTubeJobsResponse> {
    let rows = sqlx::query(
        r#"
        SELECT id, name, source_url, source_type, upload_streamer_id, enabled, sync_interval_seconds,
               auto_publish, backfill_mode, status, last_sync_at, next_sync_at, last_error, created_at, updated_at,
               (SELECT COUNT(1) FROM youtube_items WHERE job_id = youtube_jobs.id) AS item_total,
               (SELECT COUNT(1) FROM youtube_items WHERE job_id = youtube_jobs.id AND status IN ('discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload')) AS item_pending,
               (SELECT COUNT(1) FROM youtube_items WHERE job_id = youtube_jobs.id AND status = 'failed') AS item_failed,
               (SELECT COUNT(1) FROM youtube_items WHERE job_id = youtube_jobs.id AND status = 'uploaded') AS item_uploaded
        FROM youtube_jobs
        ORDER BY CASE status
            WHEN 'running' THEN 0
            WHEN 'queued' THEN 1
            WHEN 'error' THEN 2
            WHEN 'idle' THEN 3
            WHEN 'paused' THEN 4
            ELSE 5
          END ASC,
          COALESCE(next_sync_at, 9223372036854775807) ASC,
          id DESC
        "#,
    )
    .fetch_all(pool)
    .await
    .change_context(AppError::Unknown)?;
    let jobs = rows
        .into_iter()
        .map(row_to_job)
        .collect::<Result<Vec<_>, _>>()
        .change_context(AppError::Unknown)?;

    let pending_items: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(1) FROM youtube_items
        WHERE status IN ('discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload')
        "#,
    )
    .fetch_one(pool)
    .await
    .change_context(AppError::Unknown)?;

    let failed_items: i64 =
        sqlx::query_scalar(r#"SELECT COUNT(1) FROM youtube_items WHERE status = 'failed'"#)
            .fetch_one(pool)
            .await
            .change_context(AppError::Unknown)?;

    let bug_items: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(1) FROM youtube_items
        WHERE status = 'failed' OR (last_error IS NOT NULL AND trim(last_error) <> '')
        "#,
    )
    .fetch_one(pool)
    .await
    .change_context(AppError::Unknown)?;

    let uploaded_items: i64 =
        sqlx::query_scalar(r#"SELECT COUNT(1) FROM youtube_items WHERE status = 'uploaded'"#)
            .fetch_one(pool)
            .await
            .change_context(AppError::Unknown)?;

    Ok(YouTubeJobsResponse {
        summary: YouTubeJobsSummary {
            total_jobs: jobs.len() as i64,
            pending_items,
            failed_items,
            bug_items,
            uploaded_items,
        },
        jobs,
    })
}

pub async fn pause_or_resume_job(pool: &ConnectionPool, id: i64) -> AppResult<YouTubeJob> {
    let job = get_job(pool, id).await?;
    let enabled = if job.enabled == 1 { 0 } else { 1 };
    let status = scheduled_job_status(enabled, job.next_sync_at);
    let now = now_ts();

    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET enabled = ?1, status = ?2, updated_at = ?3
        WHERE id = ?4
        "#,
    )
    .bind(enabled)
    .bind(status)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    get_job(pool, id).await
}

pub async fn force_pause_job(pool: &ConnectionPool, id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET enabled = 0, status = ?1, updated_at = ?2
        WHERE id = ?3
        "#,
    )
    .bind(JOB_STATUS_PAUSED)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn trigger_job_now(pool: &ConnectionPool, id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET next_sync_at = ?1,
            updated_at = ?1,
            enabled = 1,
            status = CASE
              WHEN status = ?2 THEN ?2
              ELSE ?3
            END,
            last_error = NULL
        WHERE id = ?4
        "#,
    )
    .bind(now)
    .bind(JOB_STATUS_RUNNING)
    .bind(JOB_STATUS_QUEUED)
    .bind(id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn trigger_job_collect_once(pool: &ConnectionPool, id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET next_sync_at = ?1,
            updated_at = ?1,
            status = ?2,
            last_error = NULL
        WHERE id = ?3
        "#,
    )
    .bind(now)
    .bind(JOB_STATUS_QUEUED)
    .bind(id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn delete_job(pool: &ConnectionPool, id: i64) -> AppResult<()> {
    let result = sqlx::query("DELETE FROM youtube_jobs WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await
        .change_context(AppError::Unknown)?;
    if result.rows_affected() == 0 {
        return Err(AppError::Custom(format!("任务不存在: {}", id)).into());
    }
    Ok(())
}

pub async fn fetch_due_jobs(pool: &ConnectionPool, limit: i64) -> AppResult<Vec<YouTubeJob>> {
    let now = now_ts();
    let rows = sqlx::query(
        r#"
        SELECT id, name, source_url, source_type, upload_streamer_id, enabled, sync_interval_seconds,
               auto_publish, backfill_mode, status, last_sync_at, next_sync_at, last_error, created_at, updated_at
        FROM youtube_jobs
        WHERE enabled = 1
          AND (next_sync_at IS NULL OR next_sync_at <= ?1)
        ORDER BY COALESCE(next_sync_at, 0) ASC, id ASC
        LIMIT ?2
        "#,
    )
    .bind(now)
    .bind(limit)
    .fetch_all(pool)
    .await
    .change_context(AppError::Unknown)?;
    rows.into_iter()
        .map(row_to_job)
        .collect::<Result<Vec<_>, _>>()
        .change_context(AppError::Unknown)
}

pub async fn recover_running_jobs(pool: &ConnectionPool) -> AppResult<i64> {
    let now = now_ts();
    let result = sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET status = CASE
              WHEN enabled = 1 THEN ?1
              ELSE ?2
            END,
            updated_at = ?3
        WHERE status = ?4
        "#,
    )
    .bind(JOB_STATUS_QUEUED)
    .bind(JOB_STATUS_PAUSED)
    .bind(now)
    .bind(JOB_STATUS_RUNNING)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(result.rows_affected() as i64)
}

pub async fn set_job_running(pool: &ConnectionPool, job_id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET status = ?1, last_error = NULL, updated_at = ?2
        WHERE id = ?3
        "#,
    )
    .bind(JOB_STATUS_RUNNING)
    .bind(now)
    .bind(job_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn set_job_finished(pool: &ConnectionPool, job_id: i64, status: &str) -> AppResult<()> {
    let now = now_ts();
    let row = sqlx::query("SELECT sync_interval_seconds FROM youtube_jobs WHERE id = ?1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .change_context(AppError::Unknown)?;
    let interval: i64 = row.try_get("sync_interval_seconds").unwrap_or(1800);
    let next_sync = now + interval.max(60);

    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET status = ?1, last_sync_at = ?2, next_sync_at = ?3, updated_at = ?2
        WHERE id = ?4
        "#,
    )
    .bind(status)
    .bind(now)
    .bind(next_sync)
    .bind(job_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn set_job_error(pool: &ConnectionPool, job_id: i64, err: &str) -> AppResult<()> {
    let now = now_ts();
    let row = sqlx::query("SELECT sync_interval_seconds FROM youtube_jobs WHERE id = ?1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .change_context(AppError::Unknown)?;
    let interval: i64 = row.try_get("sync_interval_seconds").unwrap_or(1800);
    let next_sync = now + interval.max(60);

    sqlx::query(
        r#"
        UPDATE youtube_jobs
        SET status = ?1, last_error = ?2, last_sync_at = ?3, next_sync_at = ?4, updated_at = ?3
        WHERE id = ?5
        "#,
    )
    .bind(crate::server::infrastructure::models::youtube::JOB_STATUS_ERROR)
    .bind(err)
    .bind(now)
    .bind(next_sync)
    .bind(job_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn upsert_discovered_item(
    pool: &ConnectionPool,
    job_id: i64,
    video_id: &str,
    video_url: &str,
    source_title: Option<&str>,
    upload_date: Option<&str>,
    channel_id: Option<&str>,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        INSERT INTO youtube_items
        (job_id, video_id, video_url, channel_id, source_title, upload_date, status, created_at, updated_at)
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
        ON CONFLICT(job_id, video_id)
        DO UPDATE SET
          video_url = excluded.video_url,
          channel_id = COALESCE(excluded.channel_id, youtube_items.channel_id),
          source_title = COALESCE(excluded.source_title, youtube_items.source_title),
          upload_date = COALESCE(excluded.upload_date, youtube_items.upload_date),
          updated_at = excluded.updated_at
        "#,
    )
    .bind(job_id)
    .bind(video_id)
    .bind(video_url)
    .bind(channel_id)
    .bind(source_title)
    .bind(upload_date)
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(now)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn list_items_for_processing(
    pool: &ConnectionPool,
    job_id: i64,
) -> AppResult<Vec<YouTubeItem>> {
    sqlx::query_as::<_, YouTubeItem>(
        r#"
        SELECT id, job_id, video_id, video_url, channel_id, source_title, source_description, source_tags,
               thumbnail_url, upload_date, duration_sec, raw_metadata, generated_title, generated_description,
               generated_tags, local_file_path, transcoded_file_path, status, retry_count, last_error,
               bili_aid, bili_bvid, created_at, updated_at, uploaded_at
        FROM youtube_items
        WHERE job_id = ?1
          AND status IN (?2, ?3, ?4, ?5, ?6)
        ORDER BY id ASC
        "#,
    )
    .bind(job_id)
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(ITEM_STATUS_META_READY)
    .bind(ITEM_STATUS_DOWNLOADED)
    .bind(ITEM_STATUS_TRANSCODED)
    .bind(ITEM_STATUS_READY_UPLOAD)
    .fetch_all(pool)
    .await
    .change_context(AppError::Unknown)
}

pub async fn fetch_next_global_item(pool: &ConnectionPool) -> AppResult<Option<YouTubeItem>> {
    let retry_ready_upload_before = now_ts() - GLOBAL_READY_UPLOAD_RETRY_COOLDOWN_SECONDS.max(60);

    sqlx::query_as::<_, YouTubeItem>(
        r#"
        SELECT i.id, i.job_id, i.video_id, i.video_url, i.channel_id, i.source_title, i.source_description,
               i.source_tags, i.thumbnail_url, i.upload_date, i.duration_sec, i.raw_metadata,
               i.generated_title, i.generated_description, i.generated_tags, i.local_file_path,
               i.transcoded_file_path, i.status, i.retry_count, i.last_error, i.bili_aid, i.bili_bvid,
               i.created_at, i.updated_at, i.uploaded_at
        FROM youtube_items i
        INNER JOIN youtube_jobs j ON j.id = i.job_id
        WHERE i.status IN (?1, ?2, ?3, ?4)
           OR (
                i.status = ?5
                AND j.auto_publish = 1
                AND i.updated_at <= ?6
           )
        ORDER BY i.created_at ASC, i.id ASC
        LIMIT 1
        "#,
    )
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(ITEM_STATUS_META_READY)
    .bind(ITEM_STATUS_DOWNLOADED)
    .bind(ITEM_STATUS_TRANSCODED)
    .bind(ITEM_STATUS_READY_UPLOAD)
    .bind(retry_ready_upload_before)
    .fetch_optional(pool)
    .await
    .change_context(AppError::Unknown)
}

pub async fn update_item_metadata(
    pool: &ConnectionPool,
    item_id: i64,
    source_title: Option<&str>,
    source_description: Option<&str>,
    source_tags_json: Option<&str>,
    thumbnail_url: Option<&str>,
    upload_date: Option<&str>,
    duration_sec: Option<i64>,
    channel_id: Option<&str>,
    raw_metadata_json: &str,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET source_title = COALESCE(?1, source_title),
            source_description = ?2,
            source_tags = ?3,
            thumbnail_url = ?4,
            upload_date = COALESCE(?5, upload_date),
            duration_sec = ?6,
            channel_id = COALESCE(?7, channel_id),
            raw_metadata = ?8,
            status = ?9,
            updated_at = ?10
        WHERE id = ?11
        "#,
    )
    .bind(source_title)
    .bind(source_description)
    .bind(source_tags_json)
    .bind(thumbnail_url)
    .bind(upload_date)
    .bind(duration_sec)
    .bind(channel_id)
    .bind(raw_metadata_json)
    .bind(ITEM_STATUS_META_READY)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn update_item_generated(
    pool: &ConnectionPool,
    item_id: i64,
    generated_title: &str,
    generated_description: &str,
    generated_tags_json: &str,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET generated_title = ?1,
            generated_description = ?2,
            generated_tags = ?3,
            status = ?4,
            updated_at = ?5
        WHERE id = ?6
        "#,
    )
    .bind(generated_title)
    .bind(generated_description)
    .bind(generated_tags_json)
    .bind(ITEM_STATUS_META_READY)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn update_item_downloaded(
    pool: &ConnectionPool,
    item_id: i64,
    local_file_path: &str,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET local_file_path = ?1, status = ?2, updated_at = ?3
        WHERE id = ?4
        "#,
    )
    .bind(local_file_path)
    .bind(ITEM_STATUS_DOWNLOADED)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn update_item_transcoded(
    pool: &ConnectionPool,
    item_id: i64,
    transcoded_file_path: Option<&str>,
    status: &str,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET transcoded_file_path = ?1, status = ?2, updated_at = ?3
        WHERE id = ?4
        "#,
    )
    .bind(transcoded_file_path)
    .bind(status)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn mark_item_failed(pool: &ConnectionPool, item_id: i64, err: &str) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1, retry_count = retry_count + 1, last_error = ?2, updated_at = ?3
        WHERE id = ?4
        "#,
    )
    .bind(ITEM_STATUS_FAILED)
    .bind(err)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn mark_item_status(pool: &ConnectionPool, item_id: i64, status: &str) -> AppResult<()> {
    let now = now_ts();
    sqlx::query("UPDATE youtube_items SET status = ?1, updated_at = ?2 WHERE id = ?3")
        .bind(status)
        .bind(now)
        .bind(item_id)
        .execute(pool)
        .await
        .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn mark_item_retry_later(
    pool: &ConnectionPool,
    item_id: i64,
    status: &str,
    err: &str,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1,
            last_error = ?2,
            updated_at = ?3
        WHERE id = ?4
        "#,
    )
    .bind(status)
    .bind(err)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn mark_item_uploaded(
    pool: &ConnectionPool,
    item_id: i64,
    bili_aid: Option<i64>,
    bili_bvid: Option<&str>,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1,
            bili_aid = ?2,
            bili_bvid = ?3,
            uploaded_at = ?4,
            updated_at = ?4
        WHERE id = ?5
        "#,
    )
    .bind(ITEM_STATUS_UPLOADED)
    .bind(bili_aid)
    .bind(bili_bvid)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn clear_item_files(pool: &ConnectionPool, item_id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET local_file_path = NULL,
            transcoded_file_path = NULL,
            updated_at = ?1
        WHERE id = ?2
        "#,
    )
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn is_video_uploaded(pool: &ConnectionPool, video_id: &str) -> AppResult<bool> {
    let id: Option<String> =
        sqlx::query_scalar("SELECT video_id FROM youtube_uploaded_videos WHERE video_id = ?1")
            .bind(video_id)
            .fetch_optional(pool)
            .await
            .change_context(AppError::Unknown)?;
    Ok(id.is_some())
}

pub async fn insert_uploaded_video(
    pool: &ConnectionPool,
    video_id: &str,
    youtube_item_id: i64,
    bili_aid: Option<i64>,
    bili_bvid: Option<&str>,
) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        INSERT INTO youtube_uploaded_videos
        (video_id, youtube_item_id, bili_aid, bili_bvid, uploaded_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(video_id)
        DO UPDATE SET
          youtube_item_id = excluded.youtube_item_id,
          bili_aid = COALESCE(excluded.bili_aid, youtube_uploaded_videos.bili_aid),
          bili_bvid = COALESCE(excluded.bili_bvid, youtube_uploaded_videos.bili_bvid),
          uploaded_at = excluded.uploaded_at
        "#,
    )
    .bind(video_id)
    .bind(youtube_item_id)
    .bind(bili_aid)
    .bind(bili_bvid)
    .bind(now)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn get_upload_streamer_for_job(
    pool: &ConnectionPool,
    job_id: i64,
) -> AppResult<UploadStreamer> {
    let row = sqlx::query("SELECT upload_streamer_id FROM youtube_jobs WHERE id = ?1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .change_context(AppError::Unknown)?;
    let upload_streamer_id: i64 = row.try_get("upload_streamer_id").unwrap_or_default();
    UploadStreamer::select()
        .where_("id = ?")
        .bind(upload_streamer_id)
        .fetch_one(pool)
        .await
        .change_context(AppError::Unknown)
}

pub async fn list_job_items(
    pool: &ConnectionPool,
    job_id: i64,
    query: YouTubeItemsQuery,
) -> AppResult<YouTubeItemListResponse> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).clamp(1, 1000);
    let offset = (page - 1) * page_size;

    let (items, total) = if let Some(status) = query.status.filter(|v| !v.trim().is_empty()) {
        let items = sqlx::query_as::<_, YouTubeItem>(
            r#"
            SELECT id, job_id, video_id, video_url, channel_id, source_title, source_description, source_tags,
                   thumbnail_url, upload_date, duration_sec, raw_metadata, generated_title, generated_description,
                   generated_tags, local_file_path, transcoded_file_path, status, retry_count, last_error,
                   bili_aid, bili_bvid, created_at, updated_at, uploaded_at
            FROM youtube_items
            WHERE job_id = ?1 AND status = ?2
            ORDER BY id DESC
            LIMIT ?3 OFFSET ?4
            "#,
        )
        .bind(job_id)
        .bind(status.clone())
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .change_context(AppError::Unknown)?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COUNT(1) FROM youtube_items WHERE job_id = ?1 AND status = ?2",
        )
        .bind(job_id)
        .bind(status)
        .fetch_one(pool)
        .await
        .change_context(AppError::Unknown)?;
        (items, total)
    } else {
        let items = sqlx::query_as::<_, YouTubeItem>(
            r#"
            SELECT id, job_id, video_id, video_url, channel_id, source_title, source_description, source_tags,
                   thumbnail_url, upload_date, duration_sec, raw_metadata, generated_title, generated_description,
                   generated_tags, local_file_path, transcoded_file_path, status, retry_count, last_error,
                   bili_aid, bili_bvid, created_at, updated_at, uploaded_at
            FROM youtube_items
            WHERE job_id = ?1
            ORDER BY id DESC
            LIMIT ?2 OFFSET ?3
            "#,
        )
        .bind(job_id)
        .bind(page_size)
        .bind(offset)
        .fetch_all(pool)
        .await
        .change_context(AppError::Unknown)?;
        let total: i64 = sqlx::query_scalar("SELECT COUNT(1) FROM youtube_items WHERE job_id = ?1")
            .bind(job_id)
            .fetch_one(pool)
            .await
            .change_context(AppError::Unknown)?;
        (items, total)
    };

    Ok(YouTubeItemListResponse {
        items,
        total,
        page,
        page_size,
    })
}

pub async fn list_global_items(
    pool: &ConnectionPool,
    query: YouTubeItemsQuery,
) -> AppResult<YouTubeGlobalItemListResponse> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(100).clamp(1, 1000);
    let offset = (page - 1) * page_size;

    let status_filter = query
        .status
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_string());

    let order_by = r#"
        ORDER BY CASE
            WHEN qm.queue_position IS NOT NULL THEN 0
            WHEN i.status = 'failed' THEN 1
            WHEN i.status = 'uploaded' THEN 2
            WHEN i.status = 'skipped_duplicate' THEN 3
            ELSE 4
          END ASC,
          qm.queue_position ASC NULLS LAST,
          i.created_at DESC,
          i.id DESC
    "#;

    let select_sql = format!(
        r#"
        WITH queue_meta AS (
            SELECT
                id,
                ROW_NUMBER() OVER (ORDER BY created_at ASC, id ASC) AS queue_position,
                COUNT(1) OVER () AS queue_total
            FROM youtube_items
            WHERE status IN ('discovered', 'meta_ready', 'downloaded', 'transcoded', 'ready_upload')
        )
        SELECT i.id, i.job_id, j.name AS job_name, j.source_type AS job_source_type,
               qm.queue_position, qm.queue_total,
               i.video_id, i.video_url, i.channel_id, i.source_title, i.source_description,
               i.source_tags, i.thumbnail_url, i.upload_date, i.duration_sec, i.raw_metadata,
               i.generated_title, i.generated_description, i.generated_tags, i.local_file_path,
               i.transcoded_file_path, i.status, i.retry_count, i.last_error, i.bili_aid, i.bili_bvid,
               i.created_at, i.updated_at, i.uploaded_at
        FROM youtube_items i
        INNER JOIN youtube_jobs j ON j.id = i.job_id
        LEFT JOIN queue_meta qm ON qm.id = i.id
        {where_clause}
        {order_by}
        LIMIT ?1 OFFSET ?2
        "#,
        where_clause = if status_filter.is_some() {
            "WHERE i.status = ?3"
        } else {
            ""
        },
        order_by = order_by,
    );

    let count_sql = format!(
        "SELECT COUNT(1) FROM youtube_items i {}",
        if status_filter.is_some() {
            "WHERE i.status = ?1"
        } else {
            ""
        }
    );

    let items = if let Some(status) = status_filter.clone() {
        sqlx::query_as::<_, YouTubeGlobalItem>(&select_sql)
            .bind(page_size)
            .bind(offset)
            .bind(status)
            .fetch_all(pool)
            .await
            .change_context(AppError::Unknown)?
    } else {
        sqlx::query_as::<_, YouTubeGlobalItem>(&select_sql)
            .bind(page_size)
            .bind(offset)
            .fetch_all(pool)
            .await
            .change_context(AppError::Unknown)?
    };

    let total = if let Some(status) = status_filter {
        sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(status)
            .fetch_one(pool)
            .await
            .change_context(AppError::Unknown)?
    } else {
        sqlx::query_scalar::<_, i64>(&count_sql)
            .fetch_one(pool)
            .await
            .change_context(AppError::Unknown)?
    };

    Ok(YouTubeGlobalItemListResponse {
        items,
        total,
        page,
        page_size,
    })
}

pub async fn get_item(pool: &ConnectionPool, item_id: i64) -> AppResult<YouTubeItem> {
    sqlx::query_as::<_, YouTubeItem>(
        r#"
        SELECT id, job_id, video_id, video_url, channel_id, source_title, source_description, source_tags,
               thumbnail_url, upload_date, duration_sec, raw_metadata, generated_title, generated_description,
               generated_tags, local_file_path, transcoded_file_path, status, retry_count, last_error,
               bili_aid, bili_bvid, created_at, updated_at, uploaded_at
        FROM youtube_items
        WHERE id = ?1
        "#,
    )
    .bind(item_id)
    .fetch_one(pool)
    .await
    .change_context(AppError::Unknown)
}

pub async fn delete_item(pool: &ConnectionPool, item_id: i64) -> AppResult<YouTubeItem> {
    let item = get_item(pool, item_id).await?;
    let result = sqlx::query("DELETE FROM youtube_items WHERE id = ?1")
        .bind(item_id)
        .execute(pool)
        .await
        .change_context(AppError::Unknown)?;
    if result.rows_affected() == 0 {
        return Err(AppError::Custom(format!("视频任务不存在: {}", item_id)).into());
    }
    Ok(item)
}

pub async fn append_job_log(pool: &ConnectionPool, job_id: i64, message: &str) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        INSERT INTO youtube_job_logs (job_id, message, created_at)
        VALUES (?1, ?2, ?3)
        "#,
    )
    .bind(job_id)
    .bind(message)
    .bind(now)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn list_job_logs(
    pool: &ConnectionPool,
    job_id: i64,
    limit: i64,
) -> AppResult<Vec<String>> {
    Ok(list_job_log_entries(pool, job_id, limit)
        .await?
        .into_iter()
        .map(|row| row.message)
        .collect())
}

pub async fn list_job_log_entries(
    pool: &ConnectionPool,
    job_id: i64,
    limit: i64,
) -> AppResult<Vec<YouTubeJobLog>> {
    let mut logs = sqlx::query_as::<_, YouTubeJobLog>(
        r#"
        SELECT id, job_id, message, created_at
        FROM youtube_job_logs
        WHERE job_id = ?1
        ORDER BY id DESC
        LIMIT ?2
        "#,
    )
    .bind(job_id)
    .bind(limit.clamp(1, 2000))
    .fetch_all(pool)
    .await
    .change_context(AppError::Unknown)?;
    logs.reverse();
    Ok(logs)
}

pub async fn list_item_log_entries(
    pool: &ConnectionPool,
    job_id: i64,
    video_id: &str,
    limit: i64,
) -> AppResult<Vec<YouTubeJobLog>> {
    let needle = format!("vid={}", video_id.trim());
    let mut logs = sqlx::query_as::<_, YouTubeJobLog>(
        r#"
        SELECT id, job_id, message, created_at
        FROM youtube_job_logs
        WHERE job_id = ?1 AND instr(message, ?2) > 0
        ORDER BY id DESC
        LIMIT ?3
        "#,
    )
    .bind(job_id)
    .bind(needle)
    .bind(limit.clamp(1, 5000))
    .fetch_all(pool)
    .await
    .change_context(AppError::Unknown)?;
    logs.reverse();
    Ok(logs)
}

pub async fn retry_item(pool: &ConnectionPool, item_id: i64) -> AppResult<()> {
    let now = now_ts();
    sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1, last_error = NULL, created_at = ?2, updated_at = ?2
        WHERE id = ?3
        "#,
    )
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(now)
    .bind(item_id)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(())
}

pub async fn retry_failed_items_for_job(pool: &ConnectionPool, job_id: i64) -> AppResult<i64> {
    let now = now_ts();
    let result = sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1,
            last_error = NULL,
            local_file_path = NULL,
            transcoded_file_path = NULL,
            created_at = ?2,
            updated_at = ?2
        WHERE job_id = ?3
          AND status = ?4
        "#,
    )
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(now)
    .bind(job_id)
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_FAILED)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(result.rows_affected() as i64)
}

pub async fn retry_all_failed_items(pool: &ConnectionPool) -> AppResult<i64> {
    let now = now_ts();
    let result = sqlx::query(
        r#"
        UPDATE youtube_items
        SET status = ?1,
            last_error = NULL,
            local_file_path = NULL,
            transcoded_file_path = NULL,
            created_at = ?2,
            updated_at = ?2
        WHERE status = ?3
        "#,
    )
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_DISCOVERED)
    .bind(now)
    .bind(crate::server::infrastructure::models::youtube::ITEM_STATUS_FAILED)
    .execute(pool)
    .await
    .change_context(AppError::Unknown)?;
    Ok(result.rows_affected() as i64)
}
