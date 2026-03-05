use crate::server::common::upload::{build_studio, submit_to_bilibili, upload};
use crate::server::common::util::Recorder;
use crate::server::config::Config;
use crate::server::core::download_manager::DownloadManager;
use crate::server::errors::{AppError, report_to_response};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::context::{Stage, WorkerMetrics, WorkerStatus};
use crate::server::infrastructure::dto::LiveStreamerResponse;
use crate::server::infrastructure::models::live_streamer::{InsertLiveStreamer, LiveStreamer};
use crate::server::infrastructure::models::upload_streamer::{
    InsertUploadStreamer, UploadStreamer,
};
use crate::server::infrastructure::models::{
    Configuration, FileItem, InsertConfiguration, StreamerInfo,
};
use crate::server::infrastructure::repositories::{
    del_streamer, get_all_streamer, get_upload_config,
};
use crate::server::infrastructure::service_register::ServiceRegister;
use crate::server::youtube::manager::YouTubeJobManager;
use crate::{LogHandle, UploadLine};
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use biliup::credential::Credential;
use chrono::Utc;
use clap::ValueEnum;
use error_stack::{Report, ResultExt};
use ormlite::{Insert, Model};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub async fn get_streamers_endpoint(
    State(pool): State<ConnectionPool>,
    State(managers): State<Arc<DownloadManager>>,
) -> Result<Json<Vec<LiveStreamerResponse>>, Response> {
    let live_streamers = get_all_streamer(&pool).await.map_err(report_to_response)?;
    let mut results = Vec::new();
    let workers = managers.get_rooms().await;
    for x in live_streamers {
        let option = workers
            .clone()
            .into_iter()
            .find(|worker| worker.live_streamer.id == x.id);

        let status = match option.as_ref() {
            Some(t) => format!("{:?}", *t.downloader_status.read().unwrap()),
            None => String::new(),
        };

        results.push(LiveStreamerResponse {
            status,
            inner: x,
            upload_status: option
                .map(|t| format!("{:?}", *t.uploader_status.read().unwrap()))
                .unwrap_or_default(),
        });
    }
    Ok(Json(results))
}

pub async fn post_streamers_endpoint(
    State(service_register): State<ServiceRegister>,
    State(managers): State<Arc<DownloadManager>>,
    State(pool): State<ConnectionPool>,
    Json(payload): Json<InsertLiveStreamer>,
) -> Result<Json<LiveStreamer>, Response> {
    let url = &payload.url.clone();
    // You can insert the model directly.
    let live_streamers = payload
        .insert(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    let upload_config = get_upload_config(&pool, live_streamers.id)
        .await
        .map_err(report_to_response)?;
    let Some(_) = managers
        .add_room(service_register.worker(live_streamers.clone(), upload_config))
        .await
    else {
        info!("not supported url: {}", url);
        return Err((StatusCode::BAD_REQUEST, "Not supported url").into_response());
    };

    info!(url = url, "successfully inserted new live streamers");
    Ok(Json(live_streamers))
}

pub async fn put_streamers_endpoint(
    State(service_register): State<ServiceRegister>,
    State(managers): State<Arc<DownloadManager>>,
    State(pool): State<ConnectionPool>,
    Json(payload): Json<LiveStreamer>,
) -> Result<Json<LiveStreamer>, Response> {
    let streamer = payload
        .update_all_fields(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    let id = streamer.id;
    managers.del_room(id).await;

    let upload_config = get_upload_config(&pool, id)
        .await
        .map_err(report_to_response)?;

    managers
        .add_room(service_register.worker(streamer.clone(), upload_config))
        .await
        .ok_or(AppError::Unknown)
        .map_err(report_to_response)?;

    info!(id = id, "successfully update live streamers");
    Ok(Json(streamer))
}

pub async fn delete_streamers_endpoint(
    State(managers): State<Arc<DownloadManager>>,
    State(pool): State<ConnectionPool>,
    Path(id): Path<i64>,
) -> Result<Json<LiveStreamer>, Response> {
    managers.del_room(id).await;

    let live_streamers = del_streamer(&pool, id).await.map_err(report_to_response)?;
    info!(workers=?live_streamers, "successfully inserted new live streamers");
    Ok(Json(live_streamers))
}

// #[axum::debug_handler(state = ServiceRegister)]
pub async fn pause_streamers_endpoint(
    State(managers): State<Arc<DownloadManager>>,
    Path(id): Path<i64>,
) -> Result<Json<()>, Response> {
    let worker = managers.get_room_by_id(id).await;
    if let Some(w) = worker {
        let worker_status = w.downloader_status.read().unwrap().clone();
        match worker_status {
            WorkerStatus::Working(_) => {
                w.change_status(Stage::Download, WorkerStatus::Pause).await;
                info!(url=?&w.live_streamer.url, "successfully pause live streamers");
                managers.make_waker(id).await;
            }
            WorkerStatus::Pause => {
                w.change_status(Stage::Download, WorkerStatus::Idle).await;
                managers.wake_waker(id).await;
                info!(url=?&w.live_streamer.url, "successfully start live streamers");
            }
            WorkerStatus::Pending => {
                w.change_status(Stage::Download, WorkerStatus::Pause).await;
                managers.make_waker(id).await;
                info!(url=?&w.live_streamer.url, "successfully pause live streamers");
            }
            WorkerStatus::Idle => {
                w.change_status(Stage::Download, WorkerStatus::Pause).await;
                managers.make_waker(id).await;
                info!(url=?&w.live_streamer.url, "successfully pause live streamers");
            }
        };
    }

    Ok(Json(()))
}

pub async fn get_configuration(
    State(config): State<Arc<RwLock<Config>>>,
) -> Result<Json<Config>, Response> {
    Ok(Json(config.read().unwrap().clone()))
}

// #[axum_macros::debug_handler(state = ServiceRegister)]
pub async fn put_configuration(
    State(config): State<Arc<RwLock<Config>>>,
    State(pool): State<ConnectionPool>,
    State(log_handle): State<LogHandle>,
    Json(json_data): Json<Config>,
) -> Result<Json<Config>, Response> {
    // 将 JSON 序列化为 TEXT 存库
    let value_txt = serde_json::to_string(&json_data)
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    let mut tx = pool
        .begin()
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    // 最多取 2 条判断是否多行
    let ids: Vec<i64> =
        sqlx::query_scalar::<_, i64>("SELECT id FROM configuration WHERE key = ?1 LIMIT 2")
            .bind("config")
            .fetch_all(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?;

    let saved: Configuration = if ids.is_empty() {
        // 插入
        sqlx::query("INSERT INTO configuration (key, value) VALUES (?1, ?2)")
            .bind("config")
            .bind(&value_txt)
            .execute(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?;

        // 取 last_insert_rowid 并读回整行
        let id: i64 = sqlx::query_scalar::<_, i64>("SELECT last_insert_rowid()")
            .fetch_one(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?;

        sqlx::query_as::<_, Configuration>("SELECT id, key, value FROM configuration WHERE id = ?1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?
    } else if ids.len() == 1 {
        // 更新
        let id = ids[0];
        sqlx::query("UPDATE configuration SET value = ?1 WHERE id = ?2")
            .bind(&value_txt)
            .bind(id)
            .execute(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?;

        sqlx::query_as::<_, Configuration>("SELECT id, key, value FROM configuration WHERE id = ?1")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?
    } else {
        // 多行报错
        return Err(report_to_response(Report::new(AppError::Custom(
            format!("有多个空间配置同时存在 (key='config'): {} 行", ids.len()).to_string(),
        ))));
    };

    tx.commit()
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    // 提交后从 DB 重新加载配置
    let saved_config: Config = serde_json::from_str(&saved.value)
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    *config.write().unwrap() = saved_config;
    let guard = config.read().unwrap();
    if let Some(loggers_level) = &guard.loggers_level {
        let new_filter = EnvFilter::try_new(loggers_level)
            .change_context(AppError::Custom(String::from("Invalid log level format")))
            .map_err(report_to_response)?;

        log_handle
            .modify(|filter| *filter = new_filter)
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?;
    }

    Ok(Json(guard.clone()))
}

pub async fn get_streamer_info(
    // Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    State(pool): State<ConnectionPool>,
) -> Result<Json<Vec<StreamerInfo>>, Response> {
    let streamer_infos = StreamerInfo::select()
        .fetch_all(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    Ok(Json(streamer_infos))
}

pub async fn get_streamer_info_files(
    // Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    State(pool): State<ConnectionPool>,
    Path(id): Path<i64>,
) -> Result<Json<Vec<FileItem>>, Response> {
    let file_items = FileItem::select()
        .where_("streamer_info_id = ?")
        .bind(id)
        .fetch_all(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    Ok(Json(file_items))
}

pub async fn get_upload_streamers_endpoint(
    // Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    State(pool): State<ConnectionPool>,
) -> Result<Json<Vec<UploadStreamer>>, Response> {
    let uploader_streamers = UploadStreamer::select()
        .fetch_all(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    Ok(Json(uploader_streamers))
}

pub async fn add_upload_streamer_endpoint(
    // Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    State(pool): State<ConnectionPool>,
    Json(upload_streamer): Json<InsertUploadStreamer>,
) -> Result<Json<serde_json::Value>, Response> {
    if upload_streamer.id.is_none() {
        Ok(Json(
            serde_json::to_value(
                ormlite::Insert::insert(upload_streamer, &pool)
                    .await
                    .change_context(AppError::Unknown)
                    .map_err(report_to_response)?,
            )
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?,
        ))
    } else {
        Ok(Json(
            serde_json::to_value(
                upload_streamer
                    .update_all_fields(&pool)
                    .await
                    .change_context(AppError::Unknown)
                    .map_err(report_to_response)?,
            )
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?,
        ))
    }
}

pub async fn get_upload_streamer_endpoint(
    State(pool): State<ConnectionPool>,
    Path(id): Path<i64>,
) -> Result<Json<UploadStreamer>, Response> {
    let uploader_streamers = UploadStreamer::select()
        .where_("id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    Ok(Json(uploader_streamers))
}
pub async fn delete_template_endpoint(
    State(pool): State<ConnectionPool>,
    Path(id): Path<i64>,
) -> Result<Json<()>, Response> {
    let uploader_streamers = UploadStreamer::select()
        .where_("id = ?")
        .bind(id)
        .fetch_one(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    Ok(Json(
        uploader_streamers
            .delete(&pool)
            .await
            .change_context(AppError::Unknown)
            .map_err(report_to_response)?,
    ))
}

pub async fn get_users_endpoint(
    State(pool): State<ConnectionPool>,
) -> Result<Json<Vec<serde_json::Value>>, Response> {
    let configurations = Configuration::select()
        .where_("key = 'bilibili-cookies'")
        .fetch_all(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    let mut res = Vec::new();
    for cookies in configurations {
        res.push(json!({
            "id": cookies.id,
            "name": cookies.value,
            "value": cookies.value,
            "platform": cookies.key,
        }))
    }
    Ok(Json(res))
}

pub async fn add_user_endpoint(
    State(pool): State<ConnectionPool>,
    Json(user): Json<InsertConfiguration>,
) -> Result<Json<Configuration>, Response> {
    let res = user
        .insert(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    Ok(Json(res))
}

pub async fn delete_user_endpoint(
    Path(id): Path<i64>,
    State(pool): State<ConnectionPool>,
) -> Result<Json<()>, Response> {
    let x = sqlx::query("DELETE FROM configuration WHERE id = ?")
        .bind(id)
        .execute(&pool)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    info!("{:?}", x);
    Ok(Json(()))
}

pub async fn get_qrcode() -> Result<Json<serde_json::Value>, Response> {
    let qrcode = Credential::new(None)
        .get_qrcode()
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    Ok(Json(qrcode))
}

pub async fn login_by_qrcode(
    Json(value): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, Response> {
    let info = tokio::time::timeout(
        Duration::from_secs(300),
        Credential::new(None).login_by_qrcode(value),
        // std::future::pending::<AppResult<LoginInfo>>(),
    )
    .await
    .change_context(AppError::Custom("deadline has elapsed".to_string()))
    .map_err(report_to_response)?
    .change_context(AppError::Unknown)
    .map_err(report_to_response)?;

    // extract mid
    let mid = info.token_info.mid;
    let filename = format!("data/{}.json", mid);

    let mut file = fs::File::create(&filename)
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;
    file.write_all(&serde_json::to_vec_pretty(&info).unwrap())
        .await
        .change_context(AppError::Unknown)
        .map_err(report_to_response)?;

    Ok(Json(json!({ "filename": filename })))
}

pub async fn get_videos() -> Result<Json<Vec<serde_json::Value>>, Response> {
    let media_extensions = [".mp4", ".flv", ".3gp", ".webm", ".mkv", ".ts"];
    let blacklist = ["next-env.d.ts"];

    let mut file_list = Vec::new();
    let mut index = 1;

    // **use tokio::fs::read_dir**
    if let Ok(mut entries) = fs::read_dir(".").await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            let file_name = entry.file_name().to_string_lossy().into_owned();

            if blacklist.contains(&file_name.as_str()) {
                continue;
            }

            if let Some(ext) = path.extension().and_then(|e| e.to_str())
                && media_extensions
                    .iter()
                    .any(|allowed| ext == allowed.trim_start_matches('.'))
                && let Ok(metadata) = entry.metadata().await
            {
                let mtime = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                file_list.push(serde_json::json!({
                    "key": index,
                    "name": file_name,
                    "updateTime": mtime,
                    "size": metadata.len(),
                }));
                index += 1;
            }
        }
    }
    Ok(Json(file_list))
}

// #[axum::debug_handler(state = ServiceRegister)]
pub async fn get_status(
    State(_service_register): State<ServiceRegister>,
    State(managers): State<Arc<DownloadManager>>,
    State(config): State<Arc<RwLock<Config>>>,
) -> Result<Json<serde_json::Value>, Response> {
    let workers = managers.get_rooms().await;

    let mut sw = Vec::new();
    for worker in &workers {
        sw.push(serde_json::json!({
            "downloader_status": format!("{:?}", worker.downloader_status.read()),
            "uploader_status": format!("{:?}", worker.uploader_status.read().unwrap()),
            "live_streamer": worker.live_streamer,
            "upload_streamer": worker.upload_streamer,
        }));
    }

    Ok(Json(serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "rooms": sw,
        "download_semaphore": managers.d_kills.len(),
        "update_semaphore": managers.u_kills.len(),
        "config": config,
    })))
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlCenterGlobalMetrics {
    pub active_downloads: usize,
    pub active_uploads: usize,
    pub total_download_bytes: u64,
    pub total_upload_bytes: u64,
    pub avg_download_bps: u64,
    pub avg_upload_bps: u64,
    pub avg_upload_file_duration_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlCenterTaskMetrics {
    pub key: String,
    pub kind: String,
    pub id: i64,
    pub name: String,
    pub url: String,
    pub stage: Option<String>,
    pub message: Option<String>,
    pub download_status: String,
    pub upload_status: String,
    pub cleanup_status: String,
    pub metrics: WorkerMetrics,
    pub download_progress: Option<f64>,
    pub upload_progress: Option<f64>,
    pub ffmpeg_progress: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlCenterMetricsResponse {
    pub ts_ms: i64,
    pub global: ControlCenterGlobalMetrics,
    pub tasks: Vec<ControlCenterTaskMetrics>,
}

pub async fn get_metrics(
    State(managers): State<Arc<DownloadManager>>,
    State(youtube_manager): State<Arc<YouTubeJobManager>>,
) -> Result<Json<ControlCenterMetricsResponse>, Response> {
    let workers = managers.get_rooms().await;
    let ts_ms: i64 = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64))
        .unwrap_or(0);

    let mut tasks = Vec::with_capacity(workers.len());

    let mut total_download_bytes: u64 = 0;
    let mut total_upload_bytes: u64 = 0;
    let mut active_download_bytes: u64 = 0;
    let mut active_downloads: usize = 0;
    let mut active_uploads: usize = 0;

    // 计算“全局平均下载速度”：对活跃下载会话做加权（总 bytes / 总时长）
    let mut sum_download_session_ms: u64 = 0;
    // 计算“全局平均上传速度”：按累计上传耗时做加权（总 bytes / 总耗时）
    let mut sum_upload_duration_ms: u64 = 0;
    let mut sum_upload_files: u64 = 0;

    for worker in &workers {
        let metrics = worker.metrics_snapshot();
        total_download_bytes = total_download_bytes.saturating_add(metrics.download.total_bytes);
        total_upload_bytes = total_upload_bytes.saturating_add(metrics.upload.total_bytes);

        if metrics.download.active {
            active_downloads += 1;
            active_download_bytes = active_download_bytes.saturating_add(metrics.download.total_bytes);
            if let Some(start) = metrics.download.started_at_ms
                && ts_ms >= start
            {
                sum_download_session_ms = sum_download_session_ms.saturating_add((ts_ms - start) as u64);
            }
        }
        if metrics.upload.active {
            active_uploads += 1;
        }

        sum_upload_duration_ms =
            sum_upload_duration_ms.saturating_add(metrics.upload.total_duration_ms);
        sum_upload_files = sum_upload_files.saturating_add(metrics.upload.total_files);

        let download_progress = metrics
            .download
            .segment_started_at_ms
            .zip(metrics.download.segment_time_sec)
            .and_then(|(start, seg_sec)| {
                if !metrics.download.active || seg_sec == 0 {
                    return None;
                }
                let seg_ms = seg_sec.saturating_mul(1000);
                let elapsed = ts_ms.saturating_sub(start);
                Some(((elapsed as f64) / (seg_ms as f64)).clamp(0.0, 1.0))
            });

        let upload_progress = metrics
            .upload
            .current_file_total_bytes
            .and_then(|total| {
                if total == 0 {
                    return None;
                }
                Some(((metrics.upload.current_file_sent_bytes as f64) / (total as f64)).clamp(0.0, 1.0))
            });

        let ffmpeg_progress = metrics.ffmpeg.out_time_ms.and_then(|out_ms| {
            if !metrics.ffmpeg.active {
                return None;
            }
            let Some(seg_sec) = metrics.download.segment_time_sec else {
                return None;
            };
            if seg_sec == 0 {
                return None;
            }
            let seg_ms = seg_sec.saturating_mul(1000);
            let within = out_ms % seg_ms;
            Some(((within as f64) / (seg_ms as f64)).clamp(0.0, 1.0))
        });

        tasks.push(ControlCenterTaskMetrics {
            key: format!("streamer-{}", worker.id()),
            kind: "streamer".to_string(),
            id: worker.id(),
            name: worker.live_streamer.remark.clone(),
            url: worker.live_streamer.url.clone(),
            stage: None,
            message: None,
            download_status: format!("{:?}", *worker.downloader_status.read().unwrap()),
            upload_status: format!("{:?}", *worker.uploader_status.read().unwrap()),
            cleanup_status: format!("{:?}", *worker.cleanup_status.read().unwrap()),
            metrics,
            download_progress,
            upload_progress,
            ffmpeg_progress,
        });
    }

    for job in youtube_manager.runtime_snapshots().await {
        total_download_bytes = total_download_bytes.saturating_add(job.metrics.download.total_bytes);
        total_upload_bytes = total_upload_bytes.saturating_add(job.metrics.upload.total_bytes);

        if job.metrics.download.active {
            active_downloads += 1;
            active_download_bytes =
                active_download_bytes.saturating_add(job.metrics.download.total_bytes);
            if let Some(start) = job.metrics.download.started_at_ms
                && ts_ms >= start
            {
                sum_download_session_ms =
                    sum_download_session_ms.saturating_add((ts_ms - start) as u64);
            }
        }
        if job.metrics.upload.active {
            active_uploads += 1;
        }

        sum_upload_duration_ms =
            sum_upload_duration_ms.saturating_add(job.metrics.upload.total_duration_ms);
        sum_upload_files = sum_upload_files.saturating_add(job.metrics.upload.total_files);

        let stage = job.stage.clone();
        let download_status = if stage == "下载" {
            "Working"
        } else {
            "Idle"
        };
        let upload_status = if stage == "上传" || stage == "投稿" {
            "Pending"
        } else {
            "Idle"
        };
        let cleanup_status = if stage == "清理" { "Pending" } else { "Idle" };

        tasks.push(ControlCenterTaskMetrics {
            key: format!("youtube-{}", job.job_id),
            kind: "youtube".to_string(),
            id: job.job_id,
            name: format!("YouTube：{}", job.job_name),
            url: job
                .video_url
                .clone()
                .unwrap_or_else(|| job.source_url.clone()),
            stage: Some(job.stage),
            message: Some(job.message),
            download_status: download_status.to_string(),
            upload_status: upload_status.to_string(),
            cleanup_status: cleanup_status.to_string(),
            metrics: job.metrics,
            download_progress: job.download_progress,
            upload_progress: job.upload_progress,
            ffmpeg_progress: job.ffmpeg_progress,
        });
    }

    let avg_download_bps = if sum_download_session_ms > 0 {
        (active_download_bytes as f64 / (sum_download_session_ms as f64 / 1000.0)).round() as u64
    } else {
        0
    };
    let avg_upload_bps = if sum_upload_duration_ms > 0 {
        (total_upload_bytes as f64 / (sum_upload_duration_ms as f64 / 1000.0)).round() as u64
    } else {
        0
    };
    let avg_upload_file_duration_ms = if sum_upload_files > 0 {
        (sum_upload_duration_ms / sum_upload_files) as u64
    } else {
        0
    };

    Ok(Json(ControlCenterMetricsResponse {
        ts_ms,
        global: ControlCenterGlobalMetrics {
            active_downloads,
            active_uploads,
            total_download_bytes,
            total_upload_bytes,
            avg_download_bps,
            avg_upload_bps,
            avg_upload_file_duration_ms,
        },
        tasks,
    }))
}

#[derive(Deserialize)]
pub struct PostUploads {
    files: Vec<PathBuf>,
    params: UploadStreamer,
}

// #[debug_handler]
pub async fn post_uploads(
    State(config): State<Arc<RwLock<Config>>>,
    Json(json_data): Json<PostUploads>,
) -> Result<Json<serde_json::Value>, Response> {
    let upload_config = json_data.params;
    let (line, limit, submit_api, config_snapshot) = {
        let config = config.read().unwrap();
        let line = UploadLine::from_str(&config.lines, true).ok();
        let limit = config.threads;
        let submit_api = config.submit_api.clone();
        (line, limit, submit_api, config.clone())
    };
    info!("通过页面开始上传");
    tokio::spawn(async move {
        let (bilibili, videos) = upload(
            upload_config
                .user_cookie
                .as_deref()
                .unwrap_or("cookies.json"),
            None,
            line,
            &json_data.files,
            limit as usize,
        )
        .await?;
        if !videos.is_empty() {
            let recorder = Recorder::new(
                upload_config.title.clone(),
                StreamerInfo::new(
                    &upload_config.template_name,
                    "stream_title",
                    "",
                    Utc::now(),
                    "",
                ),
            );
            let studio = build_studio(
                &config_snapshot,
                &upload_config,
                &bilibili,
                videos,
                &recorder,
            )
            .await?;
            let response_data =
                submit_to_bilibili(&bilibili, &studio, submit_api.as_deref()).await?;
            info!("通过页面上传成功 {:?}", response_data);
        }
        Ok::<_, Report<AppError>>(())
    });

    Ok(Json(serde_json::json!({})))
}
