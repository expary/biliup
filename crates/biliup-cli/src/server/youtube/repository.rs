use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{
    ITEM_STATUS_DOWNLOADED, ITEM_STATUS_FAILED, ITEM_STATUS_META_READY, ITEM_STATUS_READY_UPLOAD,
    ITEM_STATUS_TRANSCODED, ITEM_STATUS_UPLOADED, JOB_STATUS_IDLE, JOB_STATUS_PAUSED,
    JOB_STATUS_RUNNING, NewYouTubeJob, UpdateYouTubeJob, YouTubeItem, YouTubeItemListResponse,
    YouTubeItemsQuery, YouTubeJob, YouTubeJobLog, YouTubeJobsResponse, YouTubeJobsSummary,
};
use chrono::Utc;
use error_stack::ResultExt;
use ormlite::Model;
use sqlx::{Row, sqlite::SqliteRow};

fn now_ts() -> i64 {
    Utc::now().timestamp()
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
    })
}

pub async fn create_job(pool: &ConnectionPool, payload: NewYouTubeJob) -> AppResult<YouTubeJob> {
    let now = now_ts();
    let enabled = payload.enabled.unwrap_or(true) as i64;
    let auto_publish = payload.auto_publish.unwrap_or(true) as i64;
    let sync_interval = payload.sync_interval_seconds.unwrap_or(1800).max(60);

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
    .bind(if enabled == 1 {
        JOB_STATUS_IDLE
    } else {
        JOB_STATUS_PAUSED
    })
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
    let enabled = payload.enabled as i64;
    let status = if payload.enabled {
        JOB_STATUS_IDLE
    } else {
        JOB_STATUS_PAUSED
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
               auto_publish, backfill_mode, status, last_sync_at, next_sync_at, last_error, created_at, updated_at
        FROM youtube_jobs
        ORDER BY id DESC
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
            uploaded_items,
        },
        jobs,
    })
}

pub async fn pause_or_resume_job(pool: &ConnectionPool, id: i64) -> AppResult<YouTubeJob> {
    let job = get_job(pool, id).await?;
    let enabled = if job.enabled == 1 { 0 } else { 1 };
    let status = if enabled == 1 {
        JOB_STATUS_IDLE
    } else {
        JOB_STATUS_PAUSED
    };
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
            status = ?2,
            last_error = NULL
        WHERE id = ?3
        "#,
    )
    .bind(now)
    .bind(JOB_STATUS_RUNNING)
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
    .bind(JOB_STATUS_IDLE)
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
        SET status = ?1, last_error = NULL, updated_at = ?2
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
