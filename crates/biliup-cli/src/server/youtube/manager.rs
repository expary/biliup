use crate::server::config::Config;
use crate::server::core::downloader::ytdlp::{Backend, DownloadConfig, YouTubeDownloader};
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{
    ITEM_STATUS_DISCOVERED, ITEM_STATUS_DOWNLOADED, ITEM_STATUS_META_READY, ITEM_STATUS_READY_UPLOAD,
    ITEM_STATUS_SKIPPED_DUPLICATE, ITEM_STATUS_TRANSCODED, JOB_STATUS_IDLE, YouTubeItem,
    YouTubeJob,
};
use crate::server::youtube::collector;
use crate::server::youtube::metadata;
use crate::server::youtube::repository;
use crate::server::youtube::transcode;
use crate::server::youtube::uploader;
use error_stack::ResultExt;
use serde_json::json;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock, Semaphore};
use tracing::{error, warn};

#[derive(Clone)]
pub struct YouTubeJobManager {
    pool: ConnectionPool,
    config: Arc<std::sync::RwLock<Config>>,
    wakeup: Arc<Notify>,
    running_jobs: Arc<Mutex<HashSet<i64>>>,
    semaphore: Arc<Semaphore>,
    logs: Arc<RwLock<HashMap<i64, VecDeque<String>>>>,
}

impl YouTubeJobManager {
    pub fn new(pool: ConnectionPool, config: Arc<std::sync::RwLock<Config>>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            config,
            wakeup: Arc::new(Notify::new()),
            running_jobs: Arc::new(Mutex::new(HashSet::new())),
            semaphore: Arc::new(Semaphore::new(2)),
            logs: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
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
        self.wakeup.notify_waiters();
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
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn running_jobs_count(&self) -> usize {
        self.running_jobs.lock().await.len()
    }

    async fn append_log(&self, job_id: i64, message: impl Into<String>) {
        let message = message.into();
        let mut guard = self.logs.write().await;
        let queue = guard.entry(job_id).or_insert_with(VecDeque::new);
        queue.push_back(message.clone());
        while queue.len() > 200 {
            queue.pop_front();
        }
        drop(guard);

        if let Err(err) = repository::append_job_log(&self.pool, job_id, &message).await {
            warn!(job_id, error = ?err, "append youtube job log failed");
        }
    }

    async fn tick(self: &Arc<Self>) -> AppResult<()> {
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

            let manager = Arc::clone(self);
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = manager.run_job(job.clone()).await {
                    error!(job_id = job.id, error = ?e, "run youtube job failed");
                }
                let mut running = manager.running_jobs.lock().await;
                running.remove(&job.id);
            });
        }
        Ok(())
    }

    async fn run_job(self: &Arc<Self>, job: YouTubeJob) -> AppResult<()> {
        repository::set_job_running(&self.pool, job.id).await?;
        self.append_log(job.id, format!("任务开始: {}", job.name)).await;

        let run_result = async {
            let entries = collector::collect_entries(&job.source_url).await?;
            self.append_log(job.id, format!("采集到 {} 条候选视频", entries.len()))
                .await;
            for entry in entries {
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
            self.process_job_items(&job).await?;
            AppResult::Ok(())
        }
        .await;

        match run_result {
            Ok(_) => {
                repository::set_job_finished(&self.pool, job.id, JOB_STATUS_IDLE).await?;
                self.append_log(job.id, "任务完成".to_string()).await;
            }
            Err(err) => {
                let msg = err.to_string();
                repository::set_job_error(&self.pool, job.id, &msg).await?;
                self.append_log(job.id, format!("任务失败: {msg}")).await;
                return Err(err);
            }
        }

        Ok(())
    }

    async fn process_job_items(self: &Arc<Self>, job: &YouTubeJob) -> AppResult<()> {
        let upload_cfg = repository::get_upload_streamer_for_job(&self.pool, job.id).await?;
        let items = repository::list_items_for_processing(&self.pool, job.id).await?;
        for item in items {
            if let Err(err) = self.process_item(job, &upload_cfg, &item).await {
                let msg = err.to_string();
                repository::mark_item_failed(&self.pool, item.id, &msg).await?;
                self.append_log(
                    job.id,
                    format!("视频 {} 处理失败: {}", item.video_id, err),
                )
                .await;
            }
        }
        Ok(())
    }

    async fn process_item(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
    ) -> AppResult<()> {
        self.append_log(job.id, format!("处理视频 {}", item.video_id)).await;

        let mut current = item.clone();
        if current.status == ITEM_STATUS_DISCOVERED {
            current = self.stage_fetch_and_generate(job, upload_cfg, &current).await?;
        }
        if current.status == ITEM_STATUS_META_READY {
            current = self.stage_download(job, &current).await?;
        }
        if current.status == ITEM_STATUS_DOWNLOADED {
            current = self.stage_transcode(job, &current).await?;
        }
        if current.status == ITEM_STATUS_TRANSCODED || current.status == ITEM_STATUS_READY_UPLOAD {
            self.stage_upload(job, upload_cfg, &current).await?;
        }
        Ok(())
    }

    async fn stage_fetch_and_generate(
        self: &Arc<Self>,
        job: &YouTubeJob,
        upload_cfg: &UploadStreamer,
        item: &YouTubeItem,
    ) -> AppResult<YouTubeItem> {
        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            match self.try_stage_fetch_and_generate(job, upload_cfg, item).await {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!("视频 {} 元数据生成第 {} 次失败: {}", item.video_id, attempt, msg),
                    )
                    .await;
                    if attempt < 3 {
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
    ) -> AppResult<YouTubeItem> {
        let fetched = collector::fetch_video_metadata(&item.video_url).await?;
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

        let cfg_snapshot = self.config.read().unwrap().clone();
        let (source_title, source_description, source_tags) = metadata::metadata_from_source(&fetched);
        let tail_policy = metadata::DescriptionTailPolicy {
            is_self_made: upload_cfg.copyright.unwrap_or(2) == 1,
            include_source_link: upload_cfg.youtube_mark_source_link.unwrap_or_default() == 1,
            include_source_channel: upload_cfg.youtube_mark_source_channel.unwrap_or_default() == 1,
        };
        let generated = metadata::generate_metadata(
            &cfg_snapshot,
            &source_title,
            &source_description,
            &source_tags,
            &item.video_url,
            fetched.channel_name.as_deref().or(fetched.channel_id.as_deref()),
            tail_policy,
        )
        .await?;
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
        self.append_log(job.id, format!("视频 {} 元数据已就绪", item.video_id))
            .await;
        repository::get_item(&self.pool, item.id).await
    }

    async fn stage_download(
        self: &Arc<Self>,
        job: &YouTubeJob,
        item: &YouTubeItem,
    ) -> AppResult<YouTubeItem> {
        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            match self.try_stage_download(job, item).await {
                Ok(updated) => return Ok(updated),
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!("视频 {} 下载第 {} 次失败: {}", item.video_id, attempt, msg),
                    )
                    .await;
                    if attempt < 3 {
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
        download_cfg.prefer_vcodec = cfg_snapshot.youtube_prefer_vcodec.clone();
        download_cfg.prefer_acodec = cfg_snapshot.youtube_prefer_acodec.clone();
        download_cfg.max_filesize = cfg_snapshot.youtube_max_videosize.clone();
        download_cfg.max_height = cfg_snapshot.youtube_max_resolution;
        download_cfg.download_archive = None;

        let downloader = YouTubeDownloader::new(download_cfg);
        downloader.download().await?;
        let downloaded = find_downloaded_file(&work_dir, &item.video_id)?;
        repository::update_item_downloaded(&self.pool, item.id, &downloaded.to_string_lossy()).await?;
        self.append_log(
            job.id,
            format!("视频 {} 下载完成: {}", item.video_id, downloaded.display()),
        )
        .await;
        repository::get_item(&self.pool, item.id).await
    }

    async fn stage_transcode(
        self: &Arc<Self>,
        job: &YouTubeJob,
        item: &YouTubeItem,
    ) -> AppResult<YouTubeItem> {
        let input_path = item
            .local_file_path
            .as_ref()
            .ok_or_else(|| AppError::Custom("缺少下载文件路径".to_string()))?;
        let path = PathBuf::from(input_path);

        let mut base_output = path.clone();
        let need_transcode = transcode::should_transcode(&path).await?;
        if need_transcode {
            let mut last_err: Option<String> = None;
            for attempt in 1..=3 {
                match transcode::transcode(&item.video_id, &path).await {
                    Ok(out) => {
                        base_output = out;
                        self.append_log(
                            job.id,
                            format!("视频 {} 转码完成: {}", item.video_id, base_output.display()),
                        )
                        .await;
                        last_err = None;
                        break;
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        last_err = Some(msg.clone());
                        self.append_log(
                            job.id,
                            format!("视频 {} 转码第 {} 次失败: {}", item.video_id, attempt, msg),
                        )
                        .await;
                        if attempt < 3 {
                            tokio::time::sleep(backoff_delay(attempt)).await;
                        }
                    }
                }
            }
            if let Some(err) = last_err {
                return Err(AppError::Custom(err).into());
            }
        } else {
            self.append_log(job.id, format!("视频 {} 符合直传条件，跳过标准转码", item.video_id))
                .await;
        }

        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            match transcode::apply_upload_effects(&item.video_id, &base_output).await {
                Ok(out) => {
                    repository::update_item_transcoded(
                        &self.pool,
                        item.id,
                        Some(&out.to_string_lossy()),
                        ITEM_STATUS_TRANSCODED,
                    )
                    .await?;
                    self.append_log(
                        job.id,
                        format!(
                            "视频 {} 处理完成（随机抽帧+黑点）: {}",
                            item.video_id,
                            out.display()
                        ),
                    )
                    .await;
                    return repository::get_item(&self.pool, item.id).await;
                }
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!("视频 {} 处理第 {} 次失败: {}", item.video_id, attempt, msg),
                    )
                    .await;
                    if attempt < 3 {
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
    ) -> AppResult<()> {
        if repository::is_video_uploaded(&self.pool, &item.video_id).await? {
            repository::mark_item_status(&self.pool, item.id, ITEM_STATUS_SKIPPED_DUPLICATE).await?;
            self.append_log(job.id, format!("视频 {} 已存在，跳过上传", item.video_id))
                .await;
            return Ok(());
        }

        if job.auto_publish == 0 {
            repository::mark_item_status(&self.pool, item.id, ITEM_STATUS_READY_UPLOAD).await?;
            self.append_log(job.id, format!("视频 {} 已就绪，等待手动发布", item.video_id))
                .await;
            return Ok(());
        }

        let cfg_snapshot = self.config.read().unwrap().clone();
        let upload_path = item
            .transcoded_file_path
            .as_ref()
            .or(item.local_file_path.as_ref())
            .ok_or_else(|| AppError::Custom("缺少上传文件路径".to_string()))?;

        let mut last_err: Option<String> = None;
        for attempt in 1..=3 {
            match uploader::upload_video(&cfg_snapshot, job, item, upload_cfg, upload_path).await {
                Ok(result) => {
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
                            "视频 {} 投稿成功 aid={:?}, bvid={:?}, 上传文件={}, 封面={}",
                            item.video_id,
                            result.aid,
                            result.bvid,
                            result.upload_file_name,
                            result.cover_file_path.unwrap_or_else(|| "无".to_string())
                        ),
                    )
                    .await;
                    return Ok(());
                }
                Err(err) => {
                    let msg = err.to_string();
                    last_err = Some(msg.clone());
                    self.append_log(
                        job.id,
                        format!("视频 {} 投稿第 {} 次失败: {}", item.video_id, attempt, msg),
                    )
                    .await;
                    if attempt < 3 {
                        tokio::time::sleep(backoff_delay(attempt)).await;
                    }
                }
            }
        }
        Err(AppError::Custom(last_err.unwrap_or_else(|| "投稿失败".to_string())).into())
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

pub fn manager_health_json(running_count: usize) -> serde_json::Value {
    json!({
        "running_jobs": running_count,
        "max_concurrency": 2
    })
}
