use crate::server::common::upload::UploadProgressHook;
use crate::server::config::Config;
use crate::server::core::downloader::ffmpeg_downloader::{FfmpegProgress, ProgressSink};
use crate::server::core::downloader::ytdlp::{
    Backend, DownloadConfig, YouTubeDownloader, YtDlpProgress, YtDlpProgressSink,
};
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::context::{
    DownloadMetrics, FfmpegMetrics, UploadMetrics, WorkerMetrics,
};
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{
    ITEM_STATUS_DISCOVERED, ITEM_STATUS_DOWNLOADED, ITEM_STATUS_META_READY,
    ITEM_STATUS_READY_UPLOAD, ITEM_STATUS_SKIPPED_DUPLICATE, ITEM_STATUS_TRANSCODED,
    JOB_STATUS_IDLE, YouTubeItem, YouTubeJob, YouTubeJobLogEntry,
};
use crate::server::youtube::collector;
use crate::server::youtube::logging::parse_job_log_message;
use crate::server::youtube::metadata;
use crate::server::youtube::repository;
use crate::server::youtube::transcode;
use crate::server::youtube::uploader;
use axum::http::StatusCode;
use chrono::Utc;
use error_stack::ResultExt;
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::fs;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

const SYNC_CONCURRENCY: usize = 1;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone)]
struct InMemoryJobLog {
    created_at: i64,
    message: String,
}

#[derive(Debug, Clone, Default)]
struct JobRuntimeState {
    job_id: i64,
    job_name: String,
    source_url: String,
    stage: String,
    video_id: Option<String>,
    video_url: Option<String>,
    message: String,
    updated_at_ms: i64,

    metrics: WorkerMetrics,
    download_progress: Option<f64>,
    upload_progress: Option<f64>,
    ffmpeg_progress: Option<f64>,

    ffmpeg_duration_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct YouTubeJobRuntimeSnapshot {
    pub job_id: i64,
    pub job_name: String,
    pub source_url: String,
    pub stage: String,
    pub video_id: Option<String>,
    pub video_url: Option<String>,
    pub message: String,
    pub updated_at_ms: i64,
    pub metrics: WorkerMetrics,
    pub download_progress: Option<f64>,
    pub upload_progress: Option<f64>,
    pub ffmpeg_progress: Option<f64>,
}

#[derive(Debug, Clone)]
struct ActiveItemProcess {
    job_id: i64,
    item_id: i64,
    cancel_token: CancellationToken,
}

#[derive(Clone)]
pub struct YouTubeJobManager {
    pool: ConnectionPool,
    config: Arc<std::sync::RwLock<Config>>,
    wakeup: Arc<Notify>,
    running_jobs: Arc<Mutex<HashSet<i64>>>,
    semaphore: Arc<Semaphore>,
    cancel_tokens: Arc<Mutex<HashMap<i64, CancellationToken>>>,
    active_item_process: Arc<Mutex<Option<ActiveItemProcess>>>,
    queue_paused: Arc<AtomicBool>,
    logs: Arc<RwLock<HashMap<i64, VecDeque<InMemoryJobLog>>>>,
    runtime: Arc<RwLock<HashMap<i64, JobRuntimeState>>>,
}

impl YouTubeJobManager {
    pub fn new(pool: ConnectionPool, config: Arc<std::sync::RwLock<Config>>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            config,
            wakeup: Arc::new(Notify::new()),
            running_jobs: Arc::new(Mutex::new(HashSet::new())),
            semaphore: Arc::new(Semaphore::new(SYNC_CONCURRENCY)),
            cancel_tokens: Arc::new(Mutex::new(HashMap::new())),
            active_item_process: Arc::new(Mutex::new(None)),
            queue_paused: Arc::new(AtomicBool::new(false)),
            logs: Arc::new(RwLock::new(HashMap::new())),
            runtime: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn runtime_snapshots(&self) -> Vec<YouTubeJobRuntimeSnapshot> {
        let guard = self.runtime.read().await;
        guard
            .values()
            .cloned()
            .map(|state| YouTubeJobRuntimeSnapshot {
                job_id: state.job_id,
                job_name: state.job_name,
                source_url: state.source_url,
                stage: state.stage,
                video_id: state.video_id,
                video_url: state.video_url,
                message: state.message,
                updated_at_ms: state.updated_at_ms,
                metrics: state.metrics,
                download_progress: state.download_progress,
                upload_progress: state.upload_progress,
                ffmpeg_progress: state.ffmpeg_progress,
            })
            .collect()
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            match repository::recover_running_jobs(&self.pool).await {
                Ok(recovered) if recovered > 0 => {
                    warn!(
                        recovered,
                        "youtube manager recovered interrupted running jobs"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    error!(error = ?e, "youtube manager failed to recover running jobs");
                }
            }
            loop {
                if let Err(e) = self.tick().await {
                    error!(error = ?e, "youtube manager tick failed");
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                    _ = self.wakeup.notified() => {}
                }
            }
        });
    }

    pub fn wakeup(&self) {
        self.wakeup.notify_one();
    }

    pub async fn logs_of(&self, job_id: i64) -> Vec<String> {
        if let Ok(logs) = repository::list_job_logs(&self.pool, job_id, 500).await
            && !logs.is_empty()
        {
            return logs;
        }
        let guard = self.logs.read().await;
        guard
            .get(&job_id)
            .map(|q| q.iter().map(|it| it.message.clone()).collect())
            .unwrap_or_default()
    }

    pub async fn log_entries_of(&self, job_id: i64) -> Vec<YouTubeJobLogEntry> {
        if let Ok(rows) = repository::list_job_log_entries(&self.pool, job_id, 500).await
            && !rows.is_empty()
        {
            return rows
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
        }

        let guard = self.logs.read().await;
        guard
            .get(&job_id)
            .map(|q| {
                q.iter()
                    .map(|row| {
                        let (stage, video_id, message) = parse_job_log_message(&row.message);
                        YouTubeJobLogEntry {
                            id: None,
                            created_at: row.created_at,
                            stage,
                            video_id,
                            message,
                            raw: row.message.clone(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn latest_log_entry_of(&self, job_id: i64) -> Option<YouTubeJobLogEntry> {
        let guard = self.logs.read().await;
        let queue = guard.get(&job_id)?;
        let row = queue.back()?;
        let (stage, video_id, message) = parse_job_log_message(&row.message);
        Some(YouTubeJobLogEntry {
            id: None,
            created_at: row.created_at,
            stage,
            video_id,
            message,
            raw: row.message.clone(),
        })
    }

    async fn ensure_runtime_job(&self, job: &YouTubeJob) {
        let now = now_ms();
        let mut guard = self.runtime.write().await;
        guard.entry(job.id).or_insert_with(|| JobRuntimeState {
            job_id: job.id,
            job_name: job.name.clone(),
            source_url: job.source_url.clone(),
            stage: "任务".to_string(),
            message: "运行中".to_string(),
            updated_at_ms: now,
            metrics: WorkerMetrics {
                download: DownloadMetrics {
                    active: false,
                    ..Default::default()
                },
                upload: UploadMetrics {
                    active: false,
                    ..Default::default()
                },
                ffmpeg: FfmpegMetrics {
                    active: false,
                    ..Default::default()
                },
            },
            ..Default::default()
        });
    }

    async fn remove_runtime_job(&self, job_id: i64) {
        let mut guard = self.runtime.write().await;
        guard.remove(&job_id);
    }

    async fn update_runtime_stage(
        &self,
        job_id: i64,
        stage: &str,
        video_id: Option<&str>,
        message: &str,
    ) {
        let now = now_ms();
        let mut guard = self.runtime.write().await;
        if let Some(state) = guard.get_mut(&job_id) {
            state.stage = stage.trim().to_string();
            state.video_id = video_id.map(|v| v.to_string());
            state.message = message.trim().to_string();
            state.updated_at_ms = now;
        }
    }

    async fn update_runtime_item(&self, job: &YouTubeJob, item: &YouTubeItem) {
        let now = now_ms();
        let mut guard = self.runtime.write().await;
        if let Some(state) = guard.get_mut(&job.id) {
            state.job_name = job.name.clone();
            state.source_url = job.source_url.clone();
            state.video_id = Some(item.video_id.clone());
            state.video_url = Some(item.video_url.clone());
            state.updated_at_ms = now;
        }
    }

    pub async fn running_jobs_count(&self) -> usize {
        let sync_jobs = self.running_jobs.lock().await.len();
        let item_processing = self.active_item_process.lock().await.is_some() as usize;
        sync_jobs + item_processing
    }

    pub async fn sync_running_jobs_count(&self) -> usize {
        self.running_jobs.lock().await.len()
    }

    pub async fn is_item_worker_active(&self) -> bool {
        self.active_item_process.lock().await.is_some()
    }

    pub fn is_queue_paused(&self) -> bool {
        self.queue_paused.load(Ordering::SeqCst)
    }

    pub fn set_queue_paused(&self, paused: bool) {
        self.queue_paused.store(paused, Ordering::SeqCst);
    }

    pub async fn is_job_running(&self, job_id: i64) -> bool {
        self.running_jobs.lock().await.contains(&job_id)
    }

    pub async fn is_processing_job(&self, job_id: i64) -> bool {
        self.active_item_process
            .lock()
            .await
            .as_ref()
            .is_some_and(|state| state.job_id == job_id)
    }

    pub async fn is_item_processing(&self, item_id: i64) -> bool {
        self.active_item_process
            .lock()
            .await
            .as_ref()
            .is_some_and(|state| state.item_id == item_id)
    }

    pub async fn cancel_job(&self, job_id: i64) -> bool {
        let guard = self.cancel_tokens.lock().await;
        let Some(token) = guard.get(&job_id) else {
            return false;
        };
        token.cancel();
        true
    }

    pub async fn cancel_processing_job(&self, job_id: i64) -> bool {
        let guard = self.active_item_process.lock().await;
        let Some(state) = guard.as_ref() else {
            return false;
        };
        if state.job_id != job_id {
            return false;
        }
        state.cancel_token.cancel();
        true
    }

    pub async fn cancel_processing_item(&self, item_id: i64) -> bool {
        let guard = self.active_item_process.lock().await;
        let Some(state) = guard.as_ref() else {
            return false;
        };
        if state.item_id != item_id {
            return false;
        }
        state.cancel_token.cancel();
        true
    }

    pub async fn cancel_active_processing(&self) -> bool {
        let guard = self.active_item_process.lock().await;
        let Some(state) = guard.as_ref() else {
            return false;
        };
        state.cancel_token.cancel();
        true
    }

    async fn append_log_internal(
        &self,
        job_id: i64,
        message: impl Into<String>,
        update_runtime: bool,
    ) {
        let message = message.into();
        let created_at = Utc::now().timestamp();
        let mut guard = self.logs.write().await;
        let queue = guard.entry(job_id).or_insert_with(VecDeque::new);
        queue.push_back(InMemoryJobLog {
            created_at,
            message: message.clone(),
        });
        while queue.len() > 200 {
            queue.pop_front();
        }
        drop(guard);

        if update_runtime {
            let (stage, video_id, msg) = parse_job_log_message(&message);
            self.update_runtime_stage(job_id, &stage, video_id.as_deref(), &msg)
                .await;
        }

        if let Err(err) = repository::append_job_log(&self.pool, job_id, &message).await {
            warn!(job_id, error = ?err, "append youtube job log failed");
        }
    }

    async fn append_log(&self, job_id: i64, message: impl Into<String>) {
        self.append_log_internal(job_id, message, true).await;
    }

    async fn append_log_no_runtime(&self, job_id: i64, message: impl Into<String>) {
        self.append_log_internal(job_id, message, false).await;
    }

    async fn log_stage(
        &self,
        job_id: i64,
        stage: &str,
        video_id: Option<&str>,
        message: impl Into<String>,
    ) {
        let stage = stage.trim();
        let mut prefix = if stage.is_empty() {
            "[日志]".to_string()
        } else {
            format!("[{stage}]")
        };
        if let Some(video_id) = video_id.filter(|v| !v.trim().is_empty()) {
            prefix.push(' ');
            prefix.push_str("vid=");
            prefix.push_str(video_id.trim());
        }
        let message = message.into();
        let trimmed = message.trim();
        if trimmed.is_empty() {
            self.append_log(job_id, prefix).await;
            self.update_runtime_stage(job_id, stage, video_id, "").await;
        } else {
            self.append_log(job_id, format!("{prefix} {trimmed}")).await;
            self.update_runtime_stage(job_id, stage, video_id, trimmed)
                .await;
        }
    }

    async fn log_stage_no_runtime(
        &self,
        job_id: i64,
        stage: &str,
        video_id: Option<&str>,
        message: impl Into<String>,
    ) {
        let stage = stage.trim();
        let mut prefix = if stage.is_empty() {
            "[日志]".to_string()
        } else {
            format!("[{stage}]")
        };
        if let Some(video_id) = video_id.filter(|v| !v.trim().is_empty()) {
            prefix.push(' ');
            prefix.push_str("vid=");
            prefix.push_str(video_id.trim());
        }
        let message = message.into();
        let trimmed = message.trim();
        if trimmed.is_empty() {
            self.append_log_no_runtime(job_id, prefix).await;
        } else {
            self.append_log_no_runtime(job_id, format!("{prefix} {trimmed}"))
                .await;
        }
    }

    async fn tick(self: &Arc<Self>) -> AppResult<()> {
        self.tick_sync_jobs().await?;
        self.tick_global_queue().await?;
        Ok(())
    }

    async fn tick_sync_jobs(self: &Arc<Self>) -> AppResult<()> {
        let due_jobs = repository::fetch_due_jobs(&self.pool, 32).await?;
        for job in due_jobs {
            let permit = match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let mut running = self.running_jobs.lock().await;
            if running.contains(&job.id) {
                continue;
            }
            running.insert(job.id);
            drop(running);

            let cancel_token = CancellationToken::new();
            {
                let mut guard = self.cancel_tokens.lock().await;
                guard.insert(job.id, cancel_token.clone());
            }

            let manager = Arc::clone(self);
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = manager.run_job(job.clone(), cancel_token.clone()).await {
                    error!(job_id = job.id, error = ?e, "run youtube job failed");
                }
                {
                    let mut guard = manager.cancel_tokens.lock().await;
                    guard.remove(&job.id);
                }
                let mut running = manager.running_jobs.lock().await;
                running.remove(&job.id);
                drop(running);
                manager.wakeup();
            });
        }
        Ok(())
    }

    async fn tick_global_queue(self: &Arc<Self>) -> AppResult<()> {
        if self.is_queue_paused() {
            return Ok(());
        }
        if self.active_item_process.lock().await.is_some() {
            return Ok(());
        }

        let Some(item) = repository::fetch_next_global_item(&self.pool).await? else {
            return Ok(());
        };

        let cancel_token = CancellationToken::new();
        {
            let mut guard = self.active_item_process.lock().await;
            if guard.is_some() {
                return Ok(());
            }
            *guard = Some(ActiveItemProcess {
                job_id: item.job_id,
                item_id: item.id,
                cancel_token: cancel_token.clone(),
            });
        }

        let manager = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = manager
                .run_global_queue_item(item.clone(), cancel_token.clone())
                .await
            {
                error!(
                    job_id = item.job_id,
                    item_id = item.id,
                    error = ?e,
                    "run youtube global queue item failed"
                );
            }
            {
                let mut guard = manager.active_item_process.lock().await;
                if matches!(guard.as_ref(), Some(state) if state.item_id == item.id) {
                    *guard = None;
                }
            }
            manager.wakeup();
        });

        Ok(())
    }

    async fn run_job(
        self: &Arc<Self>,
        job: YouTubeJob,
        cancel_token: CancellationToken,
    ) -> AppResult<()> {
        let sync_logs_update_runtime = !self.is_processing_job(job.id).await;

        if let Ok(latest) = repository::get_job(&self.pool, job.id).await
            && latest.enabled != 1
        {
            if sync_logs_update_runtime {
                self.log_stage(
                    job.id,
                    "任务",
                    None,
                    format!("跳过: 任务已暂停: {}", latest.name),
                )
                .await;
            } else {
                self.log_stage_no_runtime(
                    job.id,
                    "任务",
                    None,
                    format!("跳过: 任务已暂停: {}", latest.name),
                )
                .await;
            }
            return Ok(());
        }

        if sync_logs_update_runtime {
            self.ensure_runtime_job(&job).await;
        }
        repository::set_job_running(&self.pool, job.id).await?;
        if sync_logs_update_runtime {
            self.log_stage(job.id, "任务", None, format!("开始: {}", job.name))
                .await;
        } else {
            self.log_stage_no_runtime(job.id, "任务", None, format!("开始: {}", job.name))
                .await;
        }

        let run_result: AppResult<()> = async {
            let cfg_snapshot = self.config.read().unwrap().clone();
            let entries = tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Err(AppError::Custom("任务已暂停".to_string()).into());
                }
                result = collector::collect_entries(&job.source_url, cfg_snapshot.proxy.as_deref()) => result,
            }?;
            if sync_logs_update_runtime {
                self.log_stage(job.id, "采集", None, format!("采集到 {} 条候选视频", entries.len()))
                    .await;
            } else {
                self.log_stage_no_runtime(
                    job.id,
                    "采集",
                    None,
                    format!("采集到 {} 条候选视频", entries.len()),
                )
                .await;
            }

            for entry in entries {
                if cancel_token.is_cancelled() {
                    if sync_logs_update_runtime {
                        self.log_stage(job.id, "任务", None, "已暂停：停止采集入库".to_string())
                            .await;
                    } else {
                        self.log_stage_no_runtime(
                            job.id,
                            "任务",
                            None,
                            "已暂停：停止采集入库".to_string(),
                        )
                        .await;
                    }
                    return Ok(());
                }
                repository::upsert_discovered_item(
                    &self.pool,
                    job.id,
                    &entry.video_id,
                    &entry.video_url,
                    entry.title.as_deref(),
                    entry.upload_date.as_deref(),
                    entry.channel_id.as_deref(),
                )
                .await?;
            }

            if sync_logs_update_runtime {
                self.log_stage(job.id, "采集", None, "采集完成：已交给全局视频队列".to_string())
                    .await;
            } else {
                self.log_stage_no_runtime(
                    job.id,
                    "采集",
                    None,
                    "采集完成：已交给全局视频队列".to_string(),
                )
                .await;
            }
            Ok(())
        }
        .await;

        let enabled_now = match repository::get_job(&self.pool, job.id).await {
            Ok(latest) => latest.enabled == 1,
            Err(_) => true,
        };
        let cancelled = cancel_token.is_cancelled();
        let final_status = if enabled_now {
            JOB_STATUS_IDLE
        } else {
            crate::server::infrastructure::models::youtube::JOB_STATUS_PAUSED
        };

        match run_result {
            Ok(_) => {
                repository::set_job_finished(&self.pool, job.id, final_status).await?;
                if !enabled_now {
                    if sync_logs_update_runtime {
                        self.log_stage(job.id, "任务", None, "已暂停".to_string())
                            .await;
                    } else {
                        self.log_stage_no_runtime(job.id, "任务", None, "已暂停".to_string())
                            .await;
                    }
                } else {
                    if sync_logs_update_runtime {
                        self.log_stage(job.id, "任务", None, "完成".to_string())
                            .await;
                    } else {
                        self.log_stage_no_runtime(job.id, "任务", None, "完成".to_string())
                            .await;
                    }
                }
                if !self.is_processing_job(job.id).await {
                    self.remove_runtime_job(job.id).await;
                }
            }
            Err(err) => {
                if cancelled || !enabled_now {
                    repository::set_job_finished(&self.pool, job.id, final_status).await?;
                    if enabled_now {
                        if sync_logs_update_runtime {
                            self.log_stage(job.id, "任务", None, "已停止".to_string())
                                .await;
                        } else {
                            self.log_stage_no_runtime(job.id, "任务", None, "已停止".to_string())
                                .await;
                        }
                    } else {
                        if sync_logs_update_runtime {
                            self.log_stage(job.id, "任务", None, "已暂停".to_string())
                                .await;
                        } else {
                            self.log_stage_no_runtime(job.id, "任务", None, "已暂停".to_string())
                                .await;
                        }
                    }
                    if !self.is_processing_job(job.id).await {
                        self.remove_runtime_job(job.id).await;
                    }
                    return Ok(());
                }
                let msg = err.to_string();
                repository::set_job_error(&self.pool, job.id, &msg).await?;
                if sync_logs_update_runtime {
                    self.log_stage(job.id, "错误", None, format!("任务失败: {msg}"))
                        .await;
                } else {
                    self.log_stage_no_runtime(job.id, "错误", None, format!("任务失败: {msg}"))
                        .await;
                }
                if !self.is_processing_job(job.id).await {
                    self.remove_runtime_job(job.id).await;
                }
                return Err(err);
            }
        }

        Ok(())
    }

    async fn run_global_queue_item(
        self: &Arc<Self>,
        item: YouTubeItem,
        cancel_token: CancellationToken,
    ) -> AppResult<()> {
        let job = match repository::get_job(&self.pool, item.job_id).await {
            Ok(job) => job,
            Err(err) => {
                warn!(
                    job_id = item.job_id,
                    item_id = item.id,
                    error = ?err,
                    "skip youtube queue item because job no longer exists"
                );
                return Ok(());
            }
        };
        let upload_cfg = repository::get_upload_streamer_for_job(&self.pool, job.id).await?;

        self.ensure_runtime_job(&job).await;
        self.update_runtime_item(&job, &item).await;
        self.log_stage(
            job.id,
            "队列",
            Some(&item.video_id),
            "开始全局处理".to_string(),
        )
        .await;

        let result = self
            .process_item(&job, &upload_cfg, &item, &cancel_token)
            .await;

        match result {
            Ok(_) => {
                self.remove_runtime_job(job.id).await;
                Ok(())
            }
            Err(err) => {
                let handled = self
                    .handle_process_item_error(&job, &item, &cancel_token, &err)
                    .await?;
                self.remove_runtime_job(job.id).await;
                if handled {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn handle_process_item_error(
        &self,
        job: &YouTubeJob,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
        err: &error_stack::Report<AppError>,
    ) -> AppResult<bool> {
        if cancel_token.is_cancelled() {
            self.log_stage(
                job.id,
                "任务",
                Some(&item.video_id),
                "已停止：全局队列取消".to_string(),
            )
            .await;
            return Ok(true);
        }

        if is_rate_limited_error(err) {
            let msg = err.to_string();
            repository::mark_item_retry_later(&self.pool, item.id, ITEM_STATUS_READY_UPLOAD, &msg)
                .await?;
            self.log_stage(
                job.id,
                "限流",
                Some(&item.video_id),
                format!(
                    "{msg}，已暂缓回到全局队列，{} 秒后再试",
                    repository::GLOBAL_READY_UPLOAD_RETRY_COOLDOWN_SECONDS
                ),
            )
            .await;
            return Ok(true);
        }

        let msg = err.to_string();
        repository::mark_item_failed(&self.pool, item.id, &msg).await?;
        if let Ok(failed_item) = repository::get_item(&self.pool, item.id).await
            && let Err(cleanup_err) = self.cleanup_item_artifacts(job.id, &failed_item).await
        {
            self.log_stage(
                job.id,
                "清理",
                Some(&item.video_id),
                format!("失败后清理文件异常: {}", cleanup_err),
            )
            .await;
        }
        self.log_stage(
            job.id,
            "错误",
            Some(&item.video_id),
            format!("处理失败: {}", err),
        )
        .await;
        Ok(true)
    }

    async fn process_item(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<()> {
        if cancel_token.is_cancelled() {
            return Err(AppError::Custom("任务已暂停".to_string()).into());
        }
        self.ensure_runtime_job(job).await;
        self.update_runtime_item(job, item).await;
        self.log_stage(job.id, "任务", Some(&item.video_id), "开始处理".to_string())
            .await;

        let mut current = item.clone();
        if current.status == ITEM_STATUS_DISCOVERED
            || (current.status == ITEM_STATUS_META_READY && missing_generated_metadata(&current))
        {
            current = self
                .stage_fetch_and_generate(job, upload_cfg, &current, cancel_token)
                .await?;
        }
        if current.status == ITEM_STATUS_META_READY {
            current = self.stage_download(job, &current, cancel_token).await?;
        }
        if current.status == ITEM_STATUS_DOWNLOADED {
            current = self.stage_transcode(job, &current, cancel_token).await?;
        }
        if current.status == ITEM_STATUS_TRANSCODED || current.status == ITEM_STATUS_READY_UPLOAD {
            self.stage_upload(job, upload_cfg, &current, cancel_token)
                .await?;
        }
        Ok(())
    }

    async fn stage_fetch_and_generate(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<YouTubeItem> {
        if cancel_token.is_cancelled() {
            return Err(AppError::Custom("任务已暂停".to_string()).into());
        }
        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            if cancel_token.is_cancelled() {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            self.log_stage(
                job.id,
                "元数据",
                Some(&item.video_id),
                format!("开始（第 {attempt} 次）：拉取来源信息并生成投稿元数据"),
            )
            .await;
            match self
                .try_stage_fetch_and_generate(job, upload_cfg, item, cancel_token)
                .await
            {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!(
                            "[AI] vid={} 元数据生成第 {} 次失败: {}",
                            item.video_id, attempt, msg
                        ),
                    )
                    .await;
                    if attempt < 3 {
                        if cancel_token.is_cancelled() {
                            return Err(AppError::Custom("任务已暂停".to_string()).into());
                        }
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
            }
        }
        Err(AppError::Custom(last_err.unwrap_or_else(|| "元数据生成失败".to_string())).into())
    }

    async fn try_stage_fetch_and_generate(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<YouTubeItem> {
        let cfg_snapshot = self.config.read().unwrap().clone();
        self.log_stage(
            job.id,
            "元数据",
            Some(&item.video_id),
            "拉取来源元数据".to_string(),
        )
        .await;
        let fetched = tokio::select! {
            _ = cancel_token.cancelled() => {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            result = collector::fetch_video_metadata(&item.video_url, cfg_snapshot.proxy.as_deref()) => result,
        }?;
        let source_tags_json = serde_json::to_string(&fetched.tags)
            .change_context(AppError::Custom("序列化原始 tags 失败".to_string()))?;
        let raw_json = serde_json::to_string(&fetched.raw)
            .change_context(AppError::Custom("序列化原始元数据失败".to_string()))?;
        repository::update_item_metadata(
            &self.pool,
            item.id,
            fetched.title.as_deref(),
            fetched.description.as_deref(),
            Some(&source_tags_json),
            fetched.thumbnail.as_deref(),
            fetched.upload_date.as_deref(),
            fetched.duration_sec,
            fetched.channel_id.as_deref(),
            &raw_json,
        )
        .await?;

        let (source_title, source_description, source_tags) =
            metadata::metadata_from_source(&fetched);
        let tail_policy = metadata::DescriptionTailPolicy {
            is_self_made: upload_cfg.copyright.unwrap_or(2) == 1,
            include_source_link: upload_cfg.youtube_mark_source_link.unwrap_or_default() == 1,
            include_source_channel: upload_cfg.youtube_mark_source_channel.unwrap_or_default() == 1,
        };
        self.log_stage(
            job.id,
            "AI",
            Some(&item.video_id),
            "开始生成：标题/简介/标签".to_string(),
        )
        .await;
        let generated = tokio::select! {
            _ = cancel_token.cancelled() => {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            result = metadata::generate_metadata(
                &cfg_snapshot,
                &source_title,
                &source_description,
                &source_tags,
                &item.video_url,
                fetched
                    .channel_name
                    .as_deref()
                    .or(fetched.channel_id.as_deref()),
                tail_policy,
            ) => result,
        }?;
        self.log_stage(
            job.id,
            "AI",
            Some(&item.video_id),
            format!(
                "生成完成：标题 {} 字，标签 {} 个",
                generated.title.chars().count(),
                generated.tags.len()
            ),
        )
        .await;
        let generated_tags_json = serde_json::to_string(&generated.tags)
            .change_context(AppError::Custom("序列化生成 tags 失败".to_string()))?;
        repository::update_item_generated(
            &self.pool,
            item.id,
            &generated.title,
            &generated.description,
            &generated_tags_json,
        )
        .await?;
        self.log_stage(job.id, "元数据", Some(&item.video_id), "已就绪".to_string())
            .await;
        repository::get_item(&self.pool, item.id).await
    }

    async fn stage_download(
        self: &Arc<Self>,
        job: &YouTubeJob,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<YouTubeItem> {
        if cancel_token.is_cancelled() {
            return Err(AppError::Custom("任务已暂停".to_string()).into());
        }
        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            if cancel_token.is_cancelled() {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            let cfg_snapshot = self.config.read().unwrap().clone();
            let proxy = cfg_snapshot.proxy.clone().unwrap_or_default();
            let proxy_hint = if proxy.trim().is_empty() {
                "无代理".to_string()
            } else {
                format!("proxy={proxy}")
            };
            let cookie_hint = cfg_snapshot
                .user
                .as_ref()
                .and_then(|user| user.youtube_cookie.as_ref())
                .map(|path| format!("cookie={}", path.display()))
                .unwrap_or_else(|| "cookie=无".to_string());
            let max_h_hint = cfg_snapshot
                .youtube_max_resolution
                .map(|h| format!("max_h={h}"))
                .unwrap_or_else(|| "max_h=默认".to_string());
            let max_size_hint = cfg_snapshot
                .youtube_max_videosize
                .clone()
                .filter(|v| !v.trim().is_empty())
                .map(|v| format!("max_size={v}"))
                .unwrap_or_else(|| "max_size=默认".to_string());
            self.append_log(
                job.id,
                format!(
                    "[下载] vid={} 开始（第 {} 次）: {} | {} | {} | {} | {}",
                    item.video_id,
                    attempt,
                    item.video_url,
                    proxy_hint,
                    cookie_hint,
                    max_h_hint,
                    max_size_hint
                ),
            )
            .await;
            match self.try_stage_download(job, item, cancel_token).await {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!(
                            "[下载] vid={} 第 {} 次失败: {}",
                            item.video_id, attempt, msg
                        ),
                    )
                    .await;
                    if attempt < 3 {
                        if cancel_token.is_cancelled() {
                            return Err(AppError::Custom("任务已暂停".to_string()).into());
                        }
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
            }
        }
        Err(AppError::Custom(last_err.unwrap_or_else(|| "下载失败".to_string())).into())
    }

    async fn try_stage_download(
        self: &Arc<Self>,
        job: &YouTubeJob,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<YouTubeItem> {
        let cfg_snapshot = self.config.read().unwrap().clone();
        let work_dir = PathBuf::from(format!("data/youtube/{}/{}", job.id, item.video_id));
        std::fs::create_dir_all(&work_dir).change_context(AppError::Unknown)?;

        let mut download_cfg = DownloadConfig::default();
        download_cfg.webpage_url = item.video_url.clone();
        download_cfg.download_url = Some(item.video_url.clone());
        download_cfg.filename = item.video_id.clone();
        download_cfg.suffix = "mkv".to_string();
        download_cfg.working_dir = work_dir.clone();
        download_cfg.temp_root = work_dir.join("temp");
        download_cfg.use_temp_dir_for_ytdlp = false;
        download_cfg.backend = Backend::YtDlp;
        download_cfg.is_live = false;
        download_cfg.use_live_cover = false;
        download_cfg.cookies_file = cfg_snapshot
            .user
            .as_ref()
            .and_then(|user| user.youtube_cookie.clone());
        download_cfg.proxy = cfg_snapshot.proxy.clone();
        download_cfg.prefer_vcodec = cfg_snapshot.youtube_prefer_vcodec.clone();
        download_cfg.prefer_acodec = cfg_snapshot.youtube_prefer_acodec.clone();
        download_cfg.max_filesize = cfg_snapshot.youtube_max_videosize.clone();
        download_cfg.max_height = cfg_snapshot.youtube_max_resolution;
        download_cfg.download_archive = None;

        let downloader = YouTubeDownloader::new(download_cfg);
        {
            let now = now_ms();
            let mut guard = self.runtime.write().await;
            if let Some(state) = guard.get_mut(&job.id) {
                state.metrics.download = DownloadMetrics {
                    active: true,
                    started_at_ms: Some(now),
                    segment_started_at_ms: Some(now),
                    segment_time_sec: None,
                    total_bytes: 0,
                    total_segments: 0,
                    last_segment_bytes: 0,
                    last_segment_duration_ms: None,
                    last_bps: None,
                    avg_bps: None,
                };
                state.download_progress = Some(0.0);
                state.updated_at_ms = now;
            }
        }
        let manager = Arc::clone(self);
        let job_id = job.id;
        let sink: YtDlpProgressSink = Arc::new(move |p: YtDlpProgress| {
            let Ok(mut guard) = manager.runtime.try_write() else {
                return;
            };
            let Some(state) = guard.get_mut(&job_id) else {
                return;
            };

            let now = now_ms();
            if !state.metrics.download.active {
                state.metrics.download.active = true;
                state.metrics.download.started_at_ms = Some(now);
                state.metrics.download.segment_started_at_ms = Some(now);
                state.metrics.download.segment_time_sec = None;
            }

            if let Some(downloaded) = p.downloaded_bytes {
                state.metrics.download.total_bytes = downloaded;
            }
            if let Some(speed) = p.speed_bps {
                state.metrics.download.last_bps = Some(speed);
            }
            state.metrics.download.avg_bps = state
                .metrics
                .download
                .started_at_ms
                .and_then(|start| now.checked_sub(start))
                .map(|ms| ms.max(0) as u64)
                .filter(|ms| *ms > 0)
                .map(|ms| state.metrics.download.total_bytes as f64 / (ms as f64 / 1000.0));

            state.download_progress = p
                .percent
                .map(|pct| (pct / 100.0).clamp(0.0, 1.0))
                .or(state.download_progress);
            state.updated_at_ms = now;
        });
        let start = Instant::now();
        let download_result = tokio::select! {
            _ = cancel_token.cancelled() => {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            result = downloader.download_with_progress(Some(sink)) => result,
        };
        if let Err(err) = download_result {
            let now = now_ms();
            let mut guard = self.runtime.write().await;
            if let Some(state) = guard.get_mut(&job.id) {
                state.metrics.download.active = false;
                state.metrics.download.segment_started_at_ms = None;
                state.updated_at_ms = now;
            }
            return Err(err);
        }
        {
            let now = now_ms();
            let mut guard = self.runtime.write().await;
            if let Some(state) = guard.get_mut(&job.id) {
                state.metrics.download.active = false;
                state.metrics.download.segment_started_at_ms = None;
                if state.download_progress.unwrap_or(0.0) < 1.0 {
                    state.download_progress = Some(1.0);
                }
                state.updated_at_ms = now;
            }
        }
        let downloaded = find_downloaded_file(&work_dir, &item.video_id)?;
        let size_bytes = fs::metadata(&downloaded)
            .await
            .map(|m| m.len())
            .unwrap_or_default();
        repository::update_item_downloaded(&self.pool, item.id, &downloaded.to_string_lossy())
            .await?;
        let elapsed = start.elapsed().as_secs_f64().max(0.001);
        {
            let now = now_ms();
            let mut guard = self.runtime.write().await;
            if let Some(state) = guard.get_mut(&job.id) {
                state.metrics.download.total_bytes = size_bytes;
                state.metrics.download.avg_bps = Some(size_bytes as f64 / elapsed);
                state.metrics.download.last_bps = Some(size_bytes as f64 / elapsed);
                state.download_progress = Some(1.0);
                state.updated_at_ms = now;
            }
        }
        let size_mb = size_bytes as f64 / 1024.0 / 1024.0;
        self.log_stage(
            job.id,
            "下载",
            Some(&item.video_id),
            format!(
                "完成: {} | {:.1}MB | 耗时 {:.2}s | 平均 {:.2}MB/s",
                downloaded.display(),
                size_mb,
                elapsed,
                size_mb / elapsed
            ),
        )
        .await;
        repository::get_item(&self.pool, item.id).await
    }

    async fn stage_transcode(
        self: &Arc<Self>,
        job: &YouTubeJob,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<YouTubeItem> {
        if cancel_token.is_cancelled() {
            return Err(AppError::Custom("任务已暂停".to_string()).into());
        }
        let input_path = item
            .local_file_path
            .as_ref()
            .ok_or_else(|| AppError::Custom("缺少下载文件路径".to_string()))?;
        let path = PathBuf::from(input_path);

        let mut base_output = path.clone();
        let input_probe = tokio::select! {
            _ = cancel_token.cancelled() => {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            result = transcode::probe_summary(&path) => result,
        }?;
        self.log_stage(
            job.id,
            "探测",
            Some(&item.video_id),
            format!("输入: {}", input_probe.pretty()),
        )
        .await;
        {
            let now = now_ms();
            let duration_ms = (input_probe.duration_sec.max(0.0) * 1000.0).round() as u64;
            let mut guard = self.runtime.write().await;
            if let Some(state) = guard.get_mut(&job.id) {
                state.ffmpeg_duration_ms = (duration_ms > 0).then_some(duration_ms);
                state.metrics.ffmpeg = FfmpegMetrics {
                    active: false,
                    ..Default::default()
                };
                state.ffmpeg_progress = None;
                state.updated_at_ms = now;
            }
        }

        let need_transcode = transcode::need_transcode(&input_probe);
        let manager = Arc::clone(self);
        let job_id = job.id;
        let ffmpeg_sink: ProgressSink = Arc::new(move |p: FfmpegProgress| {
            let Ok(mut guard) = manager.runtime.try_write() else {
                return;
            };
            let Some(state) = guard.get_mut(&job_id) else {
                return;
            };
            let now = now_ms();

            state.metrics.ffmpeg.active = true;
            state.metrics.ffmpeg.out_time_ms = p.out_time_ms;
            state.metrics.ffmpeg.total_size = p.total_size;
            state.metrics.ffmpeg.speed = p.speed.clone();
            state.metrics.ffmpeg.updated_at_ms = Some(now);

            if p.progress.as_deref() == Some("end") {
                state.metrics.ffmpeg.active = false;
            }

            if let (Some(out_ms), Some(dur_ms)) = (p.out_time_ms, state.ffmpeg_duration_ms) {
                if dur_ms > 0 {
                    state.ffmpeg_progress =
                        Some(((out_ms as f64) / (dur_ms as f64)).clamp(0.0, 1.0));
                }
            }
            state.updated_at_ms = now;
        });
        if need_transcode {
            self.log_stage(
                job.id,
                "转码",
                Some(&item.video_id),
                "开始标准转码（H.264/AAC，preset=slow，码率尽量接近原文件）".to_string(),
            )
            .await;
            let mut last_err: Option<String> = None;
            for attempt in 1..=3 {
                if cancel_token.is_cancelled() {
                    return Err(AppError::Custom("任务已暂停".to_string()).into());
                }
                {
                    let now = now_ms();
                    let mut guard = self.runtime.write().await;
                    if let Some(state) = guard.get_mut(&job.id) {
                        state.metrics.ffmpeg = FfmpegMetrics {
                            active: true,
                            out_time_ms: Some(0),
                            total_size: None,
                            speed: None,
                            updated_at_ms: Some(now),
                        };
                        state.ffmpeg_progress = Some(0.0);
                        state.updated_at_ms = now;
                    }
                }
                let report = tokio::select! {
                    _ = cancel_token.cancelled() => {
                        return Err(AppError::Custom("任务已暂停".to_string()).into());
                    }
                    result = transcode::transcode_with_report(&item.video_id, &path, Some(ffmpeg_sink.clone())) => result,
                };
                match report {
                    Ok(report) => {
                        {
                            let now = now_ms();
                            let mut guard = self.runtime.write().await;
                            if let Some(state) = guard.get_mut(&job.id) {
                                state.metrics.ffmpeg.active = false;
                                state.ffmpeg_progress = Some(1.0);
                                state.updated_at_ms = now;
                            }
                        }
                        base_output = report.output.clone();
                        let (in_mb, out_mb) = (
                            report.input.size_bytes as f64 / 1024.0 / 1024.0,
                            report.output_summary.size_bytes as f64 / 1024.0 / 1024.0,
                        );
                        let out_ratio = if in_mb > 0.0 { out_mb / in_mb } else { 0.0 };
                        self.log_stage(
                            job.id,
                            "转码",
                            Some(&item.video_id),
                            format!(
                                "完成: {} | 耗时 {:.2}s | 目标码率 v={}kbps | 输入 {:.1}MB -> 输出 {:.1}MB ({:.2}x)",
                                base_output.display(),
                                report.elapsed_ms as f64 / 1000.0,
                                (report.target_video_bps / 1000).max(1),
                                in_mb,
                                out_mb,
                                out_ratio
                            ),
                        )
                        .await;
                        if base_output != path {
                            match fs::remove_file(&path).await {
                                Ok(_) => {
                                    self.log_stage(
                                        job.id,
                                        "清理",
                                        Some(&item.video_id),
                                        format!("删除原始下载文件: {}", path.display()),
                                    )
                                    .await;
                                }
                                Err(err) if err.kind() == ErrorKind::NotFound => {}
                                Err(err) => {
                                    self.log_stage(
                                        job.id,
                                        "清理",
                                        Some(&item.video_id),
                                        format!("删除原始下载文件失败: {} ({err})", path.display()),
                                    )
                                    .await;
                                }
                            }
                            repository::update_item_downloaded(
                                &self.pool,
                                item.id,
                                &base_output.to_string_lossy(),
                            )
                            .await?;
                        }
                        last_err = None;
                        break;
                    }
                    Err(err) => {
                        {
                            let now = now_ms();
                            let mut guard = self.runtime.write().await;
                            if let Some(state) = guard.get_mut(&job.id) {
                                state.metrics.ffmpeg.active = false;
                                state.updated_at_ms = now;
                            }
                        }
                        let msg = err.to_string();
                        last_err = Some(msg.clone());
                        self.append_log(
                            job.id,
                            format!(
                                "[转码] vid={} 第 {} 次失败: {}",
                                item.video_id, attempt, msg
                            ),
                        )
                        .await;
                        if attempt < 3 {
                            if cancel_token.is_cancelled() {
                                return Err(AppError::Custom("任务已暂停".to_string()).into());
                            }
                            tokio::time::sleep(backoff_delay(attempt)).await;
                        }
                    }
                }
            }
            if let Some(err) = last_err {
                return Err(AppError::Custom(err).into());
            }
        } else {
            self.log_stage(
                job.id,
                "转码",
                Some(&item.video_id),
                "符合直传条件，跳过标准转码".to_string(),
            )
            .await;
        }

        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            if cancel_token.is_cancelled() {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            if attempt == 1 {
                self.log_stage(
                    job.id,
                    "处理",
                    Some(&item.video_id),
                    "开始视频处理（局部随机黑点，GOP 分段重组，preset=veryfast）".to_string(),
                )
                .await;
            }
            {
                let now = now_ms();
                let mut guard = self.runtime.write().await;
                if let Some(state) = guard.get_mut(&job.id) {
                    state.metrics.ffmpeg = FfmpegMetrics {
                        active: true,
                        out_time_ms: Some(0),
                        total_size: None,
                        speed: None,
                        updated_at_ms: Some(now),
                    };
                    state.ffmpeg_progress = Some(0.0);
                    state.updated_at_ms = now;
                }
            }
            let report = tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Err(AppError::Custom("任务已暂停".to_string()).into());
                }
                result = transcode::apply_upload_effects_with_report(&item.video_id, &base_output, Some(ffmpeg_sink.clone())) => result,
            };
            match report {
                Ok(report) => {
                    {
                        let now = now_ms();
                        let mut guard = self.runtime.write().await;
                        if let Some(state) = guard.get_mut(&job.id) {
                            state.metrics.ffmpeg.active = false;
                            state.ffmpeg_progress = Some(1.0);
                            state.updated_at_ms = now;
                        }
                    }
                    let out = report.output.clone();
                    repository::update_item_transcoded(
                        &self.pool,
                        item.id,
                        Some(&out.to_string_lossy()),
                        ITEM_STATUS_TRANSCODED,
                    )
                    .await?;
                    let (in_mb, out_mb) = (
                        report.input.size_bytes as f64 / 1024.0 / 1024.0,
                        report.output_summary.size_bytes as f64 / 1024.0 / 1024.0,
                    );
                    let out_ratio = if in_mb > 0.0 { out_mb / in_mb } else { 0.0 };
                    let audio_label = match report.audio_plan {
                        transcode::AudioPlan::Copy => "copy".to_string(),
                        transcode::AudioPlan::Aac { bps } => {
                            format!("aac {}kbps", (bps / 1000).max(1))
                        }
                        transcode::AudioPlan::None => "无音频".to_string(),
                    };
                    let dots = report
                        .filter
                        .as_deref()
                        .map(|v| v.matches("drawbox=").count())
                        .unwrap_or(0);
                    self.append_log(
                        job.id,
                        format!(
                            "[处理] vid={} 完成（局部随机黑点）: {} | 耗时 {:.2}s | v={}kbps | a={} | 输入 {:.1}MB -> 输出 {:.1}MB ({:.2}x) | drawbox={}",
                            item.video_id,
                            out.display(),
                            report.elapsed_ms as f64 / 1000.0,
                            (report.target_video_bps / 1000).max(1),
                            audio_label,
                            in_mb,
                            out_mb,
                            out_ratio,
                            dots
                        ),
                    )
                    .await;
                    if base_output != out {
                        match fs::remove_file(&base_output).await {
                            Ok(_) => {
                                self.log_stage(
                                    job.id,
                                    "清理",
                                    Some(&item.video_id),
                                    format!("删除转码输入文件: {}", base_output.display()),
                                )
                                .await;
                            }
                            Err(err) if err.kind() == ErrorKind::NotFound => {}
                            Err(err) => {
                                self.log_stage(
                                    job.id,
                                    "清理",
                                    Some(&item.video_id),
                                    format!(
                                        "删除转码输入文件失败: {} ({err})",
                                        base_output.display()
                                    ),
                                )
                                .await;
                            }
                        }
                    }
                    return repository::get_item(&self.pool, item.id).await;
                }
                Err(err) => {
                    {
                        let now = now_ms();
                        let mut guard = self.runtime.write().await;
                        if let Some(state) = guard.get_mut(&job.id) {
                            state.metrics.ffmpeg.active = false;
                            state.updated_at_ms = now;
                        }
                    }
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!(
                            "[处理] vid={} 第 {} 次失败: {}",
                            item.video_id, attempt, msg
                        ),
                    )
                    .await;
                    if attempt < 3 {
                        if cancel_token.is_cancelled() {
                            return Err(AppError::Custom("任务已暂停".to_string()).into());
                        }
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
            }
        }

        Err(AppError::Custom(last_err.unwrap_or_else(|| "视频处理失败".to_string())).into())
    }

    async fn stage_upload(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
        cancel_token: &CancellationToken,
    ) -> AppResult<()> {
        if cancel_token.is_cancelled() {
            return Err(AppError::Custom("任务已暂停".to_string()).into());
        }
        if repository::is_video_uploaded(&self.pool, &item.video_id).await? {
            repository::mark_item_status(&self.pool, item.id, ITEM_STATUS_SKIPPED_DUPLICATE)
                .await?;
            if let Err(cleanup_err) = self.cleanup_item_artifacts(job.id, item).await {
                self.log_stage(
                    job.id,
                    "清理",
                    Some(&item.video_id),
                    format!("跳过后清理文件异常: {}", cleanup_err),
                )
                .await;
            }
            self.log_stage(
                job.id,
                "跳过",
                Some(&item.video_id),
                "已存在，跳过上传".to_string(),
            )
            .await;
            return Ok(());
        }

        if job.auto_publish == 0 {
            repository::mark_item_status(&self.pool, item.id, ITEM_STATUS_READY_UPLOAD).await?;
            self.log_stage(
                job.id,
                "投稿",
                Some(&item.video_id),
                "已就绪，等待手动发布".to_string(),
            )
            .await;
            return Ok(());
        }

        let upload_path = item
            .transcoded_file_path
            .as_ref()
            .or(item.local_file_path.as_ref())
            .ok_or_else(|| AppError::Custom("缺少上传文件路径".to_string()))?;

        let upload_size_bytes = fs::metadata(upload_path)
            .await
            .map(|m| m.len())
            .unwrap_or_default();
        let upload_size_mb = upload_size_bytes as f64 / 1024.0 / 1024.0;
        let upload_file_name = Path::new(upload_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("video");

        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            if cancel_token.is_cancelled() {
                return Err(AppError::Custom("任务已暂停".to_string()).into());
            }
            self.log_stage(
                job.id,
                "上传",
                Some(&item.video_id),
                format!(
                    "开始（第 {attempt} 次）：{upload_file_name} | {:.1}MB",
                    upload_size_mb
                ),
            )
            .await;
            let cfg_snapshot = self.config.read().unwrap().clone();
            {
                let now = now_ms();
                let mut guard = self.runtime.write().await;
                if let Some(state) = guard.get_mut(&job.id) {
                    if !state.metrics.upload.active {
                        state.metrics.upload.active = true;
                        state.metrics.upload.started_at_ms = Some(now);
                    }
                    state.metrics.upload.current_file = Some(upload_file_name.to_string());
                    state.metrics.upload.current_file_total_bytes = Some(upload_size_bytes);
                    state.metrics.upload.current_file_sent_bytes = 0;
                    state.metrics.upload.current_started_at_ms = Some(now);
                    state.metrics.upload.current_bps = Some(0.0);
                    state.upload_progress = Some(0.0);
                    state.updated_at_ms = now;
                }
            }
            let manager = Arc::clone(self);
            let job_id = job.id;
            let progress_hook: UploadProgressHook =
                Arc::new(move |file_name, total_bytes, sent_bytes| {
                    let Ok(mut guard) = manager.runtime.try_write() else {
                        return;
                    };
                    let Some(state) = guard.get_mut(&job_id) else {
                        return;
                    };
                    let now = now_ms();

                    if !state.metrics.upload.active {
                        state.metrics.upload.active = true;
                        state.metrics.upload.started_at_ms = Some(now);
                    }

                    if state.metrics.upload.current_file.as_deref() != Some(file_name) {
                        state.metrics.upload.current_file = Some(file_name.to_string());
                        state.metrics.upload.current_file_total_bytes = Some(total_bytes);
                        state.metrics.upload.current_file_sent_bytes = 0;
                        state.metrics.upload.current_started_at_ms = Some(now);
                    }
                    state.metrics.upload.current_file_total_bytes = Some(total_bytes);
                    state.metrics.upload.current_file_sent_bytes = sent_bytes;
                    state.metrics.upload.current_bps = state
                        .metrics
                        .upload
                        .current_started_at_ms
                        .and_then(|start| now.checked_sub(start))
                        .map(|ms| ms.max(0) as u64)
                        .filter(|d| *d > 0)
                        .map(|d| sent_bytes as f64 / (d as f64 / 1000.0));

                    state.upload_progress = (total_bytes > 0)
                        .then(|| ((sent_bytes as f64) / (total_bytes as f64)).clamp(0.0, 1.0));
                    state.updated_at_ms = now;
                });
            let start = Instant::now();
            let result = tokio::select! {
                _ = cancel_token.cancelled() => {
                    return Err(AppError::Custom("任务已暂停".to_string()).into());
                }
                result = uploader::upload_video(&cfg_snapshot, job, item, upload_cfg, upload_path, Some(progress_hook)) => result,
            };
            match result {
                Ok(result) => {
                    let elapsed = start.elapsed().as_secs_f64().max(0.001);
                    {
                        let now = now_ms();
                        let elapsed_ms = start.elapsed().as_millis() as u64;
                        let mut guard = self.runtime.write().await;
                        if let Some(state) = guard.get_mut(&job.id) {
                            state.metrics.upload.total_files =
                                state.metrics.upload.total_files.saturating_add(1);
                            state.metrics.upload.total_bytes = state
                                .metrics
                                .upload
                                .total_bytes
                                .saturating_add(upload_size_bytes);
                            state.metrics.upload.total_duration_ms = state
                                .metrics
                                .upload
                                .total_duration_ms
                                .saturating_add(elapsed_ms);
                            state.metrics.upload.avg_file_duration_ms =
                                (state.metrics.upload.total_files > 0).then(|| {
                                    state.metrics.upload.total_duration_ms as f64
                                        / state.metrics.upload.total_files as f64
                                });
                            state.metrics.upload.avg_bps =
                                (state.metrics.upload.total_duration_ms > 0).then(|| {
                                    state.metrics.upload.total_bytes as f64
                                        / (state.metrics.upload.total_duration_ms as f64 / 1000.0)
                                });

                            state.metrics.upload.active = false;
                            state.metrics.upload.current_file = None;
                            state.metrics.upload.current_file_total_bytes = None;
                            state.metrics.upload.current_file_sent_bytes = 0;
                            state.metrics.upload.current_started_at_ms = None;
                            state.metrics.upload.current_bps = None;
                            state.upload_progress = Some(1.0);
                            state.updated_at_ms = now;
                        }
                    }
                    self.log_stage(
                        job.id,
                        "上传",
                        Some(&item.video_id),
                        format!(
                            "完成：耗时 {:.2}s | 平均 {:.2}MB/s",
                            elapsed,
                            upload_size_mb / elapsed
                        ),
                    )
                    .await;
                    repository::mark_item_uploaded(
                        &self.pool,
                        item.id,
                        result.aid,
                        result.bvid.as_deref(),
                    )
                    .await?;
                    repository::insert_uploaded_video(
                        &self.pool,
                        &item.video_id,
                        item.id,
                        result.aid,
                        result.bvid.as_deref(),
                    )
                    .await?;
                    self.append_log(
                        job.id,
                        format!(
                            "[投稿] vid={} 成功 aid={:?}, bvid={:?}, 上传文件={}, 封面={}",
                            item.video_id,
                            result.aid,
                            result.bvid,
                            result.upload_file_name,
                            result.cover_file_path.unwrap_or_else(|| "无".to_string())
                        ),
                    )
                    .await;
                    let uploaded_item = repository::get_item(&self.pool, item.id).await?;
                    if let Err(cleanup_err) =
                        self.cleanup_item_artifacts(job.id, &uploaded_item).await
                    {
                        self.log_stage(
                            job.id,
                            "清理",
                            Some(&item.video_id),
                            format!("上传后清理文件异常: {}", cleanup_err),
                        )
                        .await;
                    }
                    return Ok(());
                }
                Err(err) => {
                    {
                        let now = now_ms();
                        let mut guard = self.runtime.write().await;
                        if let Some(state) = guard.get_mut(&job.id) {
                            state.metrics.upload.active = false;
                            state.metrics.upload.current_started_at_ms = None;
                            state.metrics.upload.current_bps = None;
                            state.updated_at_ms = now;
                        }
                    }
                    if is_rate_limited_error(&err) {
                        self.append_log(
                            job.id,
                            format!("[投稿] vid={} 触发限流: {}", item.video_id, err),
                        )
                        .await;
                        return Err(err);
                    }
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!(
                            "[投稿] vid={} 第 {} 次失败: {}",
                            item.video_id, attempt, msg
                        ),
                    )
                    .await;
                    if attempt < 3 {
                        if cancel_token.is_cancelled() {
                            return Err(AppError::Custom("任务已暂停".to_string()).into());
                        }
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
            }
        }
        Err(AppError::Custom(last_err.unwrap_or_else(|| "投稿失败".to_string())).into())
    }
}

impl YouTubeJobManager {
    async fn cleanup_item_artifacts(&self, job_id: i64, item: &YouTubeItem) -> AppResult<()> {
        let artifact_paths = collect_item_artifact_paths(item);
        let artifact_dirs = collect_item_artifact_dirs(item);
        if artifact_paths.is_empty() {
            return Ok(());
        }

        let mut removed_count = 0usize;
        for path in artifact_paths {
            if try_remove_file(&path).await? {
                removed_count += 1;
            }
        }
        for dir in artifact_dirs {
            let _ = try_remove_dir(&dir).await?;
        }
        if removed_count > 0 {
            self.log_stage(
                job_id,
                "清理",
                Some(&item.video_id),
                format!("已清理 {} 个本地文件", removed_count),
            )
            .await;
        }
        repository::clear_item_files(&self.pool, item.id).await?;
        Ok(())
    }
}

fn is_rate_limited_error(err: &error_stack::Report<AppError>) -> bool {
    matches!(
        err.current_context(),
        AppError::Http {
            status: StatusCode::TOO_MANY_REQUESTS,
            ..
        }
    )
}

fn collect_item_artifact_paths(item: &YouTubeItem) -> Vec<PathBuf> {
    let mut paths = HashSet::new();
    let local_path = item.local_file_path.as_ref().map(PathBuf::from);
    let transcoded_path = item.transcoded_file_path.as_ref().map(PathBuf::from);

    if let Some(path) = &local_path {
        paths.insert(path.clone());
    }
    if let Some(path) = &transcoded_path {
        paths.insert(path.clone());
    }

    if let Some(base_dir) = local_path
        .as_ref()
        .or(transcoded_path.as_ref())
        .and_then(|path| path.parent().map(Path::to_path_buf))
    {
        paths.insert(base_dir.join(format!("{}.upload.mp4", item.video_id)));
        paths.insert(base_dir.join(format!("{}.fx.mp4", item.video_id)));
        paths.insert(base_dir.join(format!("{}.cover.jpg", item.video_id)));
    }

    paths.into_iter().collect()
}

fn collect_item_artifact_dirs(item: &YouTubeItem) -> Vec<PathBuf> {
    let mut dirs = HashSet::new();
    if let Some(path) = item.local_file_path.as_deref() {
        if let Some(parent) = Path::new(path).parent() {
            dirs.insert(parent.to_path_buf());
        }
    }
    if let Some(path) = item.transcoded_file_path.as_deref() {
        if let Some(parent) = Path::new(path).parent() {
            dirs.insert(parent.to_path_buf());
        }
    }
    dirs.into_iter().collect()
}

fn missing_generated_metadata(item: &YouTubeItem) -> bool {
    item.generated_title
        .as_deref()
        .map_or(true, |value| value.trim().is_empty())
        || item
            .generated_description
            .as_deref()
            .map_or(true, |value| value.trim().is_empty())
        || item
            .generated_tags
            .as_deref()
            .map_or(true, |value| value.trim().is_empty())
}

async fn try_remove_file(path: &Path) -> AppResult<bool> {
    match fs::metadata(path).await {
        Ok(metadata) => {
            if metadata.is_file() {
                fs::remove_file(path)
                    .await
                    .change_context(AppError::Unknown)?;
                if let Some(parent) = path.parent() {
                    let _ = fs::remove_dir(parent).await;
                }
                return Ok(true);
            }
            Ok(false)
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err(AppError::Unknown.into()),
    }
}

async fn try_remove_dir(path: &Path) -> AppResult<bool> {
    match fs::remove_dir_all(path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err(AppError::Unknown.into()),
    }
}

fn backoff_delay(attempt: usize) -> std::time::Duration {
    match attempt {
        1 => std::time::Duration::from_secs(2),
        2 => std::time::Duration::from_secs(10),
        _ => std::time::Duration::from_secs(30),
    }
}

fn find_downloaded_file(dir: &Path, video_id: &str) -> AppResult<PathBuf> {
    let mut candidates = Vec::new();
    let read_dir = std::fs::read_dir(dir).change_context(AppError::Unknown)?;
    for entry in read_dir {
        let entry = entry.change_context(AppError::Unknown)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        if !file_name.starts_with(video_id) {
            continue;
        }
        if file_name.ends_with(".part") || file_name.ends_with(".ytdl") {
            continue;
        }
        candidates.push(path);
    }

    if candidates.is_empty() {
        return Err(AppError::Custom(format!("未找到下载结果文件: {}", dir.display())).into());
    }
    candidates.sort();
    Ok(candidates.remove(0))
}

pub fn normalize_source_type(source_url: &str, given: &str) -> String {
    let given_trimmed = given.trim().to_ascii_lowercase();
    if matches!(given_trimmed.as_str(), "channel" | "playlist" | "shorts") {
        return given_trimmed;
    }
    collector::detect_source_type(source_url)
}

pub fn manager_health_json(
    sync_running_count: usize,
    item_worker_active: bool,
    item_worker_paused: bool,
) -> serde_json::Value {
    json!({
        "running_sync_jobs": sync_running_count,
        "sync_concurrency": SYNC_CONCURRENCY,
        "item_worker_active": item_worker_active,
        "item_worker_paused": item_worker_paused,
        "item_worker_concurrency": 1
    })
}
