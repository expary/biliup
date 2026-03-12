use crate::server::errors::{AppError, report_to_response};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::youtube::{
    NewYouTubeJob, UpdateYouTubeJob, YouTubeGlobalItemListResponse, YouTubeItemListResponse,
    YouTubeItemsQuery, YouTubeJob, YouTubeItemLogsResponse, YouTubeJobLogEntry,
    YouTubeJobLogsResponse, YouTubeJobsResponse,
};
use crate::server::youtube::manager::{YouTubeJobManager, normalize_source_type};
use crate::server::youtube::logging::parse_job_log_message;
use crate::server::youtube::repository;
use axum::http::StatusCode;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Deserialize)]
pub struct YouTubeActiveQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct YouTubeActiveTask {
    pub job_id: i64,
    pub job_name: String,
    pub stage: String,
    pub video_id: Option<String>,
    pub message: String,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct YouTubeActiveTasksResponse {
    pub ts_ms: i64,
    pub items: Vec<YouTubeActiveTask>,
}

pub async fn get_youtube_jobs_endpoint(
    State(pool): State<ConnectionPool>,
) -> Result<Json<YouTubeJobsResponse>, Response> {
    let result = repository::list_jobs(&pool)
        .await
        .map_err(report_to_response)?;
    Ok(Json(result))
}

pub async fn post_youtube_jobs_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Json(mut payload): Json<NewYouTubeJob>,
) -> Result<Json<YouTubeJob>, Response> {
    ensure_upload_streamer_exists(&pool, payload.upload_streamer_id).await?;
    payload.source_type = normalize_source_type(&payload.source_url, &payload.source_type);
    let result = repository::create_job(&pool, payload)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(result))
}

pub async fn put_youtube_jobs_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
    Json(mut payload): Json<UpdateYouTubeJob>,
) -> Result<Json<YouTubeJob>, Response> {
    ensure_upload_streamer_exists(&pool, payload.upload_streamer_id).await?;
    payload.id = id;
    payload.source_type = normalize_source_type(&payload.source_url, &payload.source_type);
    let result = repository::update_job(&pool, payload)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(result))
}

pub async fn run_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    repository::trigger_job_now(&pool, id)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn sync_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    repository::trigger_job_collect_once(&pool, id)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn pause_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<YouTubeJob>, Response> {
    let result = repository::pause_or_resume_job(&pool, id)
        .await
        .map_err(report_to_response)?;
    if result.enabled == 0 {
        manager.cancel_job(id).await;
        manager.cancel_processing_job(id).await;
    }
    manager.wakeup();
    Ok(Json(result))
}

pub async fn delete_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    if manager.is_job_running(id).await {
        let _ = repository::force_pause_job(&pool, id).await;
        manager.cancel_job(id).await;

        let deadline = Instant::now() + Duration::from_secs(5);
        while manager.is_job_running(id).await && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if manager.is_job_running(id).await {
            return Err(report_to_response(AppError::Http {
                status: StatusCode::CONFLICT,
                message: "任务正在停止中，请稍后再试".to_string(),
            }));
        }
    }

    if manager.is_processing_job(id).await {
        manager.cancel_processing_job(id).await;

        let deadline = Instant::now() + Duration::from_secs(5);
        while manager.is_processing_job(id).await && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if manager.is_processing_job(id).await {
            return Err(report_to_response(AppError::Http {
                status: StatusCode::CONFLICT,
                message: "该任务的视频正在全局队列处理中，请稍后再试".to_string(),
            }));
        }
    }

    repository::delete_job(&pool, id)
        .await
        .map_err(report_to_response)?;

    let job_dir = PathBuf::from(format!("data/youtube/{id}"));
    if let Err(err) = tokio::fs::remove_dir_all(&job_dir).await
        && err.kind() != ErrorKind::NotFound
    {
        return Err(report_to_response(AppError::Custom(format!(
            "删除任务目录失败: {}",
            err
        ))));
    }

    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn get_youtube_job_items_endpoint(
    State(pool): State<ConnectionPool>,
    Path(id): Path<i64>,
    Query(query): Query<YouTubeItemsQuery>,
) -> Result<Json<YouTubeItemListResponse>, Response> {
    let result = repository::list_job_items(&pool, id, query)
        .await
        .map_err(report_to_response)?;
    Ok(Json(result))
}

pub async fn get_youtube_global_items_endpoint(
    State(pool): State<ConnectionPool>,
    Query(query): Query<YouTubeItemsQuery>,
) -> Result<Json<YouTubeGlobalItemListResponse>, Response> {
    let result = repository::list_global_items(&pool, query)
        .await
        .map_err(report_to_response)?;
    Ok(Json(result))
}

pub async fn delete_youtube_item_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(item_id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    if manager.is_item_processing(item_id).await {
        manager.cancel_processing_item(item_id).await;

        let deadline = Instant::now() + Duration::from_secs(5);
        while manager.is_item_processing(item_id).await && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if manager.is_item_processing(item_id).await {
            return Err(report_to_response(AppError::Http {
                status: StatusCode::CONFLICT,
                message: "该视频任务正在执行中，请稍后再试".to_string(),
            }));
        }
    }

    let item = repository::delete_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;

    let item_dir = PathBuf::from(format!("data/youtube/{}/{}", item.job_id, item.video_id));
    if let Err(err) = tokio::fs::remove_dir_all(&item_dir).await
        && err.kind() != ErrorKind::NotFound
    {
        return Err(report_to_response(AppError::Custom(format!(
            "删除视频任务目录失败: {}",
            err
        ))));
    }

    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn retry_youtube_item_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(item_id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    repository::retry_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn run_youtube_item_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(item_id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    let item = repository::get_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;

    if matches!(item.status.as_str(), "uploaded" | "skipped_duplicate") {
        return Err(report_to_response(AppError::Http {
            status: StatusCode::CONFLICT,
            message: "该视频任务已完成，无需再次执行".to_string(),
        }));
    }

    if item.status == "failed" {
        repository::retry_item(&pool, item_id)
            .await
            .map_err(report_to_response)?;
    }

    let _ = repository::append_job_log(
        &pool,
        item.job_id,
        &format!("[任务] vid={} 已从任务列表触发执行", item.video_id),
    )
    .await;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn pause_youtube_item_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(item_id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    let item = repository::get_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;

    if manager.cancel_processing_item(item_id).await {
        let _ = repository::append_job_log(
            &pool,
            item.job_id,
            &format!("[任务] vid={} 已从任务列表暂停执行", item.video_id),
        )
        .await;
    }

    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn retry_failed_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    ensure_youtube_job_exists(&pool, id).await?;
    let retried_count = repository::retry_failed_items_for_job(&pool, id)
        .await
        .map_err(report_to_response)?;
    let _ = repository::append_job_log(
        &pool,
        id,
        &format!("[任务] 批量重试失败项: {}", retried_count),
    )
    .await;
    manager.wakeup();
    Ok(Json(
        serde_json::json!({"ok": true, "retried_count": retried_count}),
    ))
}

pub async fn get_youtube_job_logs_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<YouTubeJobLogsResponse>, Response> {
    let entries = manager.log_entries_of(id).await;
    let logs = entries.iter().map(|it| it.raw.clone()).collect::<Vec<_>>();
    Ok(Json(YouTubeJobLogsResponse {
        job_id: id,
        logs,
        entries,
    }))
}

pub async fn get_youtube_item_logs_endpoint(
    State(pool): State<ConnectionPool>,
    Path(item_id): Path<i64>,
) -> Result<Json<YouTubeItemLogsResponse>, Response> {
    let item = repository::get_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;
    let rows = repository::list_item_log_entries(&pool, item.job_id, &item.video_id, 2000)
        .await
        .map_err(report_to_response)?;
    let entries = rows
        .into_iter()
        .map(|row| {
            let (stage, video_id, message) = parse_job_log_message(&row.message);
            YouTubeJobLogEntry {
                id: Some(row.id),
                created_at: row.created_at,
                stage,
                video_id,
                message,
                raw: row.message,
            }
        })
        .collect();
    Ok(Json(YouTubeItemLogsResponse { item, entries }))
}

pub async fn get_youtube_manager_health_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<serde_json::Value>, Response> {
    let running = manager.sync_running_jobs_count().await;
    let item_worker_active = manager.is_item_worker_active().await;
    let item_worker_paused = manager.is_queue_paused();
    Ok(Json(crate::server::youtube::manager::manager_health_json(
        running,
        item_worker_active,
        item_worker_paused,
    )))
}

pub async fn run_youtube_queue_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<serde_json::Value>, Response> {
    manager.set_queue_paused(false);
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn pause_youtube_queue_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<serde_json::Value>, Response> {
    manager.set_queue_paused(true);
    let _ = manager.cancel_active_processing().await;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn retry_failed_youtube_queue_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<serde_json::Value>, Response> {
    let retried_count = repository::retry_all_failed_items(&pool)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(serde_json::json!({
        "ok": true,
        "retried_count": retried_count
    })))
}

pub async fn get_youtube_active_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
    Query(query): Query<YouTubeActiveQuery>,
) -> Result<Json<YouTubeActiveTasksResponse>, Response> {
    let ts_ms = now_ms();
    let limit = query.limit.unwrap_or(10).clamp(1, 50);

    let mut snapshots = manager.runtime_snapshots().await;
    snapshots.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));

    let items = snapshots
        .into_iter()
        .take(limit)
        .map(|snapshot| YouTubeActiveTask {
            job_id: snapshot.job_id,
            job_name: snapshot.job_name,
            stage: snapshot.stage,
            video_id: snapshot.video_id,
            message: snapshot.message,
            updated_at_ms: snapshot.updated_at_ms,
        })
        .collect();

    Ok(Json(YouTubeActiveTasksResponse { ts_ms, items }))
}

async fn ensure_upload_streamer_exists(pool: &ConnectionPool, id: i64) -> Result<(), Response> {
    let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM uploadstreamers WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| report_to_response(AppError::Custom(e.to_string())))?;
    if exists.is_none() {
        return Err(report_to_response(AppError::Custom(format!(
            "upload_streamer_id 不存在: {}",
            id
        ))));
    }
    Ok(())
}

async fn ensure_youtube_job_exists(pool: &ConnectionPool, id: i64) -> Result<(), Response> {
    let exists: Option<i64> = sqlx::query_scalar("SELECT id FROM youtube_jobs WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| report_to_response(AppError::Custom(e.to_string())))?;
    if exists.is_none() {
        return Err(report_to_response(AppError::Http {
            status: StatusCode::NOT_FOUND,
            message: format!("任务不存在: {}", id),
        }));
    }
    Ok(())
}
