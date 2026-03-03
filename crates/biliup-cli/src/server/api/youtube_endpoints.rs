use crate::server::errors::{AppError, report_to_response};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::youtube::{
    NewYouTubeJob, UpdateYouTubeJob, YouTubeItemListResponse, YouTubeItemsQuery, YouTubeJob,
    YouTubeJobsResponse,
};
use crate::server::youtube::manager::{YouTubeJobManager, normalize_source_type};
use crate::server::youtube::repository;
use axum::Json;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;

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

pub async fn pause_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<YouTubeJob>, Response> {
    let result = repository::pause_or_resume_job(&pool, id)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(result))
}

pub async fn delete_youtube_job_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    if manager.is_job_running(id).await {
        return Err(report_to_response(AppError::Custom(
            "任务正在运行中，请稍后再删除".to_string(),
        )));
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

pub async fn retry_youtube_item_endpoint(
    State(pool): State<ConnectionPool>,
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(item_id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    let item = repository::get_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;
    repository::retry_item(&pool, item_id)
        .await
        .map_err(report_to_response)?;
    repository::trigger_job_now(&pool, item.job_id)
        .await
        .map_err(report_to_response)?;
    manager.wakeup();
    Ok(Json(serde_json::json!({"ok": true})))
}

pub async fn get_youtube_job_logs_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Response> {
    let logs = manager.logs_of(id).await;
    Ok(Json(serde_json::json!({ "job_id": id, "logs": logs })))
}

pub async fn get_youtube_manager_health_endpoint(
    State(manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<serde_json::Value>, Response> {
    let running = manager.running_jobs_count().await;
    Ok(Json(crate::server::youtube::manager::manager_health_json(
        running,
    )))
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
