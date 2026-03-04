use crate::server::common::download::DownloadTask;
use crate::server::common::util::Recorder;
use crate::server::config::{Config, default_segment_time};
use crate::server::core::downloader::DownloadConfig;
use crate::server::core::downloader::ffmpeg_downloader::FfmpegProgress;
use crate::server::core::plugin::StreamInfoExt;
use crate::server::infrastructure::connection_pool::ConnectionPool;
use crate::server::infrastructure::models::StreamerInfo;
use crate::server::infrastructure::models::live_streamer::LiveStreamer;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use biliup::client::StatelessClient;
use core::fmt;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use struct_patch::Patch;
use tracing::{error, info};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DownloadMetrics {
    pub active: bool,
    pub started_at_ms: Option<i64>,
    pub segment_started_at_ms: Option<i64>,
    pub segment_time_sec: Option<u64>,

    pub total_bytes: u64,
    pub total_segments: u64,

    pub last_segment_bytes: u64,
    pub last_segment_duration_ms: Option<u64>,
    pub last_bps: Option<f64>,
    pub avg_bps: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct UploadMetrics {
    pub active: bool,
    pub started_at_ms: Option<i64>,

    pub total_bytes: u64,
    pub total_files: u64,
    pub total_duration_ms: u64,
    pub avg_bps: Option<f64>,
    pub avg_file_duration_ms: Option<f64>,

    pub current_file: Option<String>,
    pub current_file_total_bytes: Option<u64>,
    pub current_file_sent_bytes: u64,
    pub current_started_at_ms: Option<i64>,
    pub current_bps: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FfmpegMetrics {
    pub active: bool,
    pub out_time_ms: Option<u64>,
    pub total_size: Option<u64>,
    pub speed: Option<String>,
    pub updated_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WorkerMetrics {
    pub download: DownloadMetrics,
    pub upload: UploadMetrics,
    pub ffmpeg: FfmpegMetrics,
}

/// 应用程序上下文，包含工作器和扩展信息
#[derive(Debug, Clone)]
pub struct Context {
    id: i64,
    /// 工作器实例
    worker: Arc<Worker>,
    stream_info: StreamInfoExt,
    pool: ConnectionPool,
}

impl Context {
    /// 创建新的上下文实例
    ///
    /// # 参数
    /// * `worker` - 工作器实例的Arc引用
    pub fn new(
        id: i64,
        worker: Arc<Worker>,
        pool: ConnectionPool,
        stream_info: StreamInfoExt,
    ) -> Self {
        Self {
            id,
            worker,
            stream_info,
            pool,
        }
    }

    pub fn worker_id(&self) -> i64 {
        self.worker.id()
    }

    pub fn worker_arc(&self) -> Arc<Worker> {
        self.worker.clone()
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn live_streamer(&self) -> &LiveStreamer {
        &self.worker.get_streamer()
    }

    pub fn stateless_client(&self) -> &StatelessClient {
        &self.worker.client
    }

    pub fn config(&self) -> Config {
        self.worker.get_config()
    }

    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    pub async fn change_status(&self, stage: Stage, status: WorkerStatus) {
        self.worker.change_status(stage, status).await;
    }

    pub fn status(&self, stage: Stage) -> WorkerStatus {
        match stage {
            Stage::Download => self.worker.downloader_status.read().unwrap().clone(),
            Stage::Upload => self.worker.uploader_status.read().unwrap().clone(),
            Stage::Cleanup => self.worker.cleanup_status.read().unwrap().clone(),
        }
    }

    pub fn upload_config(&self) -> &Option<UploadStreamer> {
        self.worker.get_upload_config()
    }

    pub fn metrics_snapshot(&self) -> WorkerMetrics {
        self.worker.metrics_snapshot()
    }

    pub fn on_download_segment_completed(&self, segment_path: &std::path::Path) {
        self.worker.on_download_segment_completed(segment_path);
    }

    pub fn on_upload_file_started(&self, file_name: &str, total_bytes: u64) {
        self.worker.on_upload_file_started(file_name, total_bytes);
    }

    pub fn on_upload_progress(&self, file_name: &str, total_bytes: u64, sent_bytes: u64) {
        self.worker.on_upload_progress(file_name, total_bytes, sent_bytes);
    }

    pub fn on_upload_file_completed(&self, file_name: &str, total_bytes: u64, elapsed_ms: u64) {
        self.worker
            .on_upload_file_completed(file_name, total_bytes, elapsed_ms);
    }

    pub fn recorder(&self, streamer_info: StreamerInfo) -> Recorder {
        // 创建录制器
        Recorder::new(
            self.live_streamer()
                .filename_prefix
                .clone()
                .or(self.config().filename_prefix.clone()),
            streamer_info,
        )
    }

    pub fn stream_info_ext(&self) -> &StreamInfoExt {
        &self.stream_info
    }

    pub fn download_config(&self, ext: &StreamInfoExt) -> DownloadConfig {
        let config = self.config();
        // 确定文件格式后缀
        let suffix = self
            .live_streamer()
            .format
            .clone()
            .unwrap_or_else(|| ext.suffix.to_string());
        DownloadConfig {
            // 流URL
            url: ext.raw_stream_url.to_string(),
            segment_time: config.segment_time.or_else(default_segment_time),
            file_size: Some(config.file_size), // 2GB
            headers: ext.stream_headers.clone(),
            recorder: self.recorder(ext.streamer_info.clone()),
            // output_dir: PathBuf::from("./downloads")
            output_dir: PathBuf::from("."),
            suffix,
        }
    }
}

/// 工作器结构体，管理单个主播的录制和上传任务
#[derive(Debug)]
pub struct Worker {
    /// 下载器状态
    pub downloader_status: RwLock<WorkerStatus>,
    /// 上传器状态
    pub uploader_status: RwLock<WorkerStatus>,
    /// 清理状态
    pub cleanup_status: RwLock<WorkerStatus>,
    metrics: RwLock<WorkerMetrics>,
    /// 直播主播信息
    pub live_streamer: LiveStreamer,
    /// 上传配置（可选）
    pub upload_streamer: Option<UploadStreamer>,
    /// 全局配置
    config: Arc<RwLock<Config>>,
    /// HTTP客户端
    pub client: StatelessClient,
}

impl Worker {
    /// 创建新的工作器实例
    ///
    /// # 参数
    /// * `live_streamer` - 直播主播信息
    /// * `upload_streamer` - 上传配置（可选）
    /// * `config` - 全局配置的Arc引用
    /// * `client` - HTTP客户端
    pub fn new(
        live_streamer: LiveStreamer,
        upload_streamer: Option<UploadStreamer>,
        config: Arc<RwLock<Config>>,
        client: StatelessClient,
    ) -> Self {
        Self {
            downloader_status: RwLock::new(Default::default()),
            uploader_status: RwLock::new(Default::default()),
            cleanup_status: RwLock::new(Default::default()),
            metrics: RwLock::new(Default::default()),
            live_streamer,
            upload_streamer,
            config,
            client,
        }
    }

    pub fn id(&self) -> i64 {
        self.live_streamer.id
    }

    /// 获取主播信息
    /// 返回当前工作器关联的直播主播信息
    pub fn get_streamer(&self) -> &LiveStreamer {
        &self.live_streamer
    }

    /// 获取上传配置
    /// 返回当前工作器的上传配置（如果存在）
    pub fn get_upload_config(&self) -> &Option<UploadStreamer> {
        &self.upload_streamer
    }

    /// 获取覆写配置
    /// 返回当前的配置副本
    pub fn get_config(&self) -> Config {
        let mut cfg = self.config.read().unwrap().clone();

        if let Some(cfg_p) = self.live_streamer.override_cfg.clone() {
            cfg.apply(cfg_p)
        }
        cfg
    }

    pub fn metrics_snapshot(&self) -> WorkerMetrics {
        self.metrics.read().unwrap().clone()
    }

    fn reset_download_metrics(&self) {
        let cfg = self.get_config();
        let segment_time_sec = cfg
            .segment_time
            .as_deref()
            .map(crate::server::common::util::parse_time)
            .map(|d| d.as_secs());
        let now = now_ms();
        let mut metrics = self.metrics.write().unwrap();
        metrics.download = DownloadMetrics {
            active: true,
            started_at_ms: Some(now),
            segment_started_at_ms: Some(now),
            segment_time_sec,
            ..Default::default()
        };
        metrics.ffmpeg = Default::default();
    }

    fn stop_download_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.download.active = false;
        metrics.download.segment_started_at_ms = None;
        metrics.ffmpeg.active = false;
    }

    fn reset_upload_metrics(&self) {
        let now = now_ms();
        let mut metrics = self.metrics.write().unwrap();
        metrics.upload = UploadMetrics {
            active: true,
            started_at_ms: Some(now),
            ..Default::default()
        };
    }

    fn stop_upload_metrics(&self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.upload.active = false;
        metrics.upload.current_file = None;
        metrics.upload.current_file_total_bytes = None;
        metrics.upload.current_file_sent_bytes = 0;
        metrics.upload.current_started_at_ms = None;
        metrics.upload.current_bps = None;
    }

    pub fn on_download_segment_completed(&self, segment_path: &std::path::Path) {
        let now = now_ms();
        let size = std::fs::metadata(segment_path).map(|m| m.len()).unwrap_or(0);
        let mut metrics = self.metrics.write().unwrap();
        if !metrics.download.active {
            return;
        }

        metrics.download.total_segments = metrics.download.total_segments.saturating_add(1);
        metrics.download.total_bytes = metrics.download.total_bytes.saturating_add(size);

        let segment_duration_ms = metrics
            .download
            .segment_started_at_ms
            .and_then(|start| now.checked_sub(start))
            .map(|ms| ms.max(0) as u64);
        metrics.download.last_segment_bytes = size;
        metrics.download.last_segment_duration_ms = segment_duration_ms;
        metrics.download.last_bps = segment_duration_ms
            .filter(|d| *d > 0)
            .map(|d| size as f64 / (d as f64 / 1000.0));

        metrics.download.avg_bps = metrics
            .download
            .started_at_ms
            .and_then(|start| now.checked_sub(start))
            .map(|ms| ms.max(0) as u64)
            .filter(|d| *d > 0)
            .map(|d| metrics.download.total_bytes as f64 / (d as f64 / 1000.0));

        metrics.download.segment_started_at_ms = Some(now);
    }

    pub fn on_upload_file_started(&self, file_name: &str, total_bytes: u64) {
        let now = now_ms();
        let mut metrics = self.metrics.write().unwrap();
        if !metrics.upload.active {
            // 允许在未显式进入 Pending 时也能显示上传进度
            metrics.upload.active = true;
            metrics.upload.started_at_ms = Some(now);
        }
        metrics.upload.current_file = Some(file_name.to_string());
        metrics.upload.current_file_total_bytes = Some(total_bytes);
        metrics.upload.current_file_sent_bytes = 0;
        metrics.upload.current_started_at_ms = Some(now);
        metrics.upload.current_bps = Some(0.0);
    }

    pub fn on_upload_progress(&self, file_name: &str, total_bytes: u64, sent_bytes: u64) {
        let now = now_ms();
        let mut metrics = self.metrics.write().unwrap();
        if metrics.upload.current_file.as_deref() != Some(file_name) {
            metrics.upload.current_file = Some(file_name.to_string());
            metrics.upload.current_file_total_bytes = Some(total_bytes);
            metrics.upload.current_file_sent_bytes = 0;
            metrics.upload.current_started_at_ms = Some(now);
        }
        metrics.upload.current_file_total_bytes = Some(total_bytes);
        metrics.upload.current_file_sent_bytes = sent_bytes;
        metrics.upload.current_bps = metrics
            .upload
            .current_started_at_ms
            .and_then(|start| now.checked_sub(start))
            .map(|ms| ms.max(0) as u64)
            .filter(|d| *d > 0)
            .map(|d| sent_bytes as f64 / (d as f64 / 1000.0));
    }

    pub fn on_upload_file_completed(&self, file_name: &str, total_bytes: u64, elapsed_ms: u64) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.upload.total_files = metrics.upload.total_files.saturating_add(1);
        metrics.upload.total_bytes = metrics.upload.total_bytes.saturating_add(total_bytes);
        metrics.upload.total_duration_ms = metrics
            .upload
            .total_duration_ms
            .saturating_add(elapsed_ms);
        metrics.upload.avg_file_duration_ms = (metrics.upload.total_files > 0).then(|| {
            metrics.upload.total_duration_ms as f64 / metrics.upload.total_files as f64
        });
        metrics.upload.avg_bps = (metrics.upload.total_duration_ms > 0)
            .then(|| metrics.upload.total_bytes as f64 / (metrics.upload.total_duration_ms as f64 / 1000.0));

        // 如果完成的是当前文件，保留文件名但清空进度（下一文件会覆盖）
        if metrics.upload.current_file.as_deref() == Some(file_name) {
            metrics.upload.current_file = None;
            metrics.upload.current_file_sent_bytes = 0;
            metrics.upload.current_file_total_bytes = None;
            metrics.upload.current_started_at_ms = None;
            metrics.upload.current_bps = None;
        }
    }

    pub fn on_ffmpeg_progress(&self, progress: FfmpegProgress) {
        let now = now_ms();
        let mut metrics = self.metrics.write().unwrap();
        metrics.ffmpeg.active = true;
        metrics.ffmpeg.out_time_ms = progress.out_time_ms;
        metrics.ffmpeg.total_size = progress.total_size;
        metrics.ffmpeg.speed = progress.speed;
        metrics.ffmpeg.updated_at_ms = Some(now);
        if progress.progress.as_deref() == Some("end") {
            metrics.ffmpeg.active = false;
        }
    }

    /// 更改工作器状态
    ///
    /// # 参数
    /// * `stage` - 工作阶段（下载或上传）
    /// * `status` - 新的工作状态
    pub async fn change_status(&self, stage: Stage, status: WorkerStatus) {
        match stage {
            Stage::Download => {
                let prev = self.downloader_status.read().unwrap().clone();
                let task = if let WorkerStatus::Working(task) = &prev
                    && !matches!(status, WorkerStatus::Working(_))
                {
                    Some(task.clone())
                } else {
                    None
                };

                if matches!(status, WorkerStatus::Working(_))
                    && !matches!(prev, WorkerStatus::Working(_))
                {
                    self.reset_download_metrics();
                } else if matches!(prev, WorkerStatus::Working(_))
                    && !matches!(status, WorkerStatus::Working(_))
                {
                    self.stop_download_metrics();
                }

                *self.downloader_status.write().unwrap() = status;

                if let Some(task) = task
                    && let Err(e) = task.stop().await
                {
                    error!(error = ?e, "Failed to stop downloader");
                }
            }
            Stage::Upload => {
                let prev = self.uploader_status.read().unwrap().clone();
                if matches!(status, WorkerStatus::Pending) && !matches!(prev, WorkerStatus::Pending)
                {
                    self.reset_upload_metrics();
                } else if matches!(prev, WorkerStatus::Pending)
                    && matches!(status, WorkerStatus::Idle)
                {
                    self.stop_upload_metrics();
                }
                *self.uploader_status.write().unwrap() = status;
            }
            Stage::Cleanup => {
                *self.cleanup_status.write().unwrap() = status;
            }
        }
    }
}

pub fn find_worker(workers: &[Arc<Worker>], id: i64) -> Option<&Arc<Worker>> {
    workers.iter().find(|worker| worker.live_streamer.id == id)
}

impl Drop for Worker {
    /// 工作器销毁时的清理逻辑
    fn drop(&mut self) {
        info!("Dropping worker {}", self.live_streamer.id);
    }
}

impl PartialEq for Worker {
    /// 比较两个工作器是否相等（基于主播ID）
    fn eq(&self, other: &Self) -> bool {
        self.live_streamer.id == other.live_streamer.id
    }
}

impl Eq for Worker {}

/// 工作阶段枚举
#[derive(Debug)]
pub enum Stage {
    /// 下载阶段
    Download,
    /// 上传阶段
    Upload,
    /// 清理阶段
    Cleanup,
}

/// 工作器状态枚举
#[derive(Default, Clone)]
pub enum WorkerStatus {
    /// 正在工作
    Working(Arc<DownloadTask>),
    /// 等待中
    Pending,
    /// 空闲状态（默认）
    #[default]
    Idle,
    /// 下载暂停中
    Pause,
}

// 简单 Debug：打印状态名，忽略内部 downloader
impl fmt::Debug for WorkerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            WorkerStatus::Working(_) => "Working",
            WorkerStatus::Pending => "Pending",
            WorkerStatus::Idle => "Idle",
            WorkerStatus::Pause => "Pause",
        };
        f.write_str(name)
    }
}

/// 应用程序上下文，包含工作器和扩展信息
#[derive(Debug, Clone)]
pub struct PluginContext {
    /// 工作器实例
    worker: Arc<Worker>,
    pool: ConnectionPool,
}

impl PluginContext {
    pub fn new(worker: Arc<Worker>, pool: ConnectionPool) -> Self {
        Self { worker, pool }
    }

    pub fn to_context(&self, id: i64, stream_info: StreamInfoExt) -> Context {
        Context::new(id, self.worker.clone(), self.pool.clone(), stream_info)
    }

    pub fn config(&self) -> Config {
        self.worker.get_config()
    }

    pub fn live_streamer(&self) -> &LiveStreamer {
        &self.worker.get_streamer()
    }

    pub fn upload_config(&self) -> &Option<UploadStreamer> {
        self.worker.get_upload_config()
    }

    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    pub fn client(&self) -> reqwest::Client {
        self.worker.client.client.clone()
    }
}
