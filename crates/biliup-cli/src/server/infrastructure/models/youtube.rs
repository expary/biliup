use serde::{Deserialize, Serialize};
use sqlx::FromRow;

pub const SOURCE_TYPE_CHANNEL: &str = "channel";
pub const SOURCE_TYPE_PLAYLIST: &str = "playlist";
pub const SOURCE_TYPE_SHORTS: &str = "shorts";

pub const JOB_STATUS_IDLE: &str = "idle";
pub const JOB_STATUS_QUEUED: &str = "queued";
pub const JOB_STATUS_RUNNING: &str = "running";
pub const JOB_STATUS_PAUSED: &str = "paused";
pub const JOB_STATUS_ERROR: &str = "error";

pub const ITEM_STATUS_DISCOVERED: &str = "discovered";
pub const ITEM_STATUS_META_READY: &str = "meta_ready";
pub const ITEM_STATUS_DOWNLOADED: &str = "downloaded";
pub const ITEM_STATUS_TRANSCODED: &str = "transcoded";
pub const ITEM_STATUS_READY_UPLOAD: &str = "ready_upload";
pub const ITEM_STATUS_UPLOADED: &str = "uploaded";
pub const ITEM_STATUS_SKIPPED_DUPLICATE: &str = "skipped_duplicate";
pub const ITEM_STATUS_FAILED: &str = "failed";

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct YouTubeJob {
    pub id: i64,
    pub name: String,
    pub source_url: String,
    pub source_type: String,
    pub upload_streamer_id: i64,
    pub enabled: i64,
    pub sync_interval_seconds: i64,
    pub auto_publish: i64,
    pub backfill_mode: String,
    pub status: String,
    pub last_sync_at: Option<i64>,
    pub next_sync_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub item_total: Option<i64>,
    pub item_pending: Option<i64>,
    pub item_failed: Option<i64>,
    pub item_uploaded: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewYouTubeJob {
    pub name: String,
    pub source_url: String,
    pub source_type: String,
    pub upload_streamer_id: i64,
    pub enabled: Option<bool>,
    pub sync_interval_seconds: Option<i64>,
    pub auto_publish: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateYouTubeJob {
    pub id: i64,
    pub name: String,
    pub source_url: String,
    pub source_type: String,
    pub upload_streamer_id: i64,
    pub enabled: bool,
    pub sync_interval_seconds: i64,
    pub auto_publish: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct YouTubeItem {
    pub id: i64,
    pub job_id: i64,
    pub video_id: String,
    pub video_url: String,
    pub channel_id: Option<String>,
    pub source_title: Option<String>,
    pub source_description: Option<String>,
    pub source_tags: Option<String>,
    pub thumbnail_url: Option<String>,
    pub upload_date: Option<String>,
    pub duration_sec: Option<i64>,
    pub raw_metadata: Option<String>,
    pub generated_title: Option<String>,
    pub generated_description: Option<String>,
    pub generated_tags: Option<String>,
    pub local_file_path: Option<String>,
    pub transcoded_file_path: Option<String>,
    pub status: String,
    pub retry_count: i64,
    pub last_error: Option<String>,
    pub bili_aid: Option<i64>,
    pub bili_bvid: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub uploaded_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct YouTubeUploadedVideo {
    pub video_id: String,
    pub youtube_item_id: Option<i64>,
    pub bili_aid: Option<i64>,
    pub bili_bvid: Option<String>,
    pub uploaded_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeJobsSummary {
    pub total_jobs: i64,
    pub pending_items: i64,
    pub failed_items: i64,
    pub bug_items: i64,
    pub uploaded_items: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeJobsResponse {
    pub summary: YouTubeJobsSummary,
    pub jobs: Vec<YouTubeJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeItemsQuery {
    pub status: Option<String>,
    pub page: Option<i64>,
    pub page_size: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeItemListResponse {
    pub items: Vec<YouTubeItem>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct YouTubeGlobalItem {
    pub id: i64,
    pub job_id: i64,
    pub job_name: String,
    pub job_source_type: String,
    pub queue_position: Option<i64>,
    pub queue_total: Option<i64>,
    pub video_id: String,
    pub video_url: String,
    pub channel_id: Option<String>,
    pub source_title: Option<String>,
    pub source_description: Option<String>,
    pub source_tags: Option<String>,
    pub thumbnail_url: Option<String>,
    pub upload_date: Option<String>,
    pub duration_sec: Option<i64>,
    pub raw_metadata: Option<String>,
    pub generated_title: Option<String>,
    pub generated_description: Option<String>,
    pub generated_tags: Option<String>,
    pub local_file_path: Option<String>,
    pub transcoded_file_path: Option<String>,
    pub status: String,
    pub retry_count: i64,
    pub last_error: Option<String>,
    pub bili_aid: Option<i64>,
    pub bili_bvid: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub uploaded_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeGlobalItemListResponse {
    pub items: Vec<YouTubeGlobalItem>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct YouTubeJobLog {
    pub id: i64,
    pub job_id: i64,
    pub message: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeJobLogEntry {
    pub id: Option<i64>,
    pub created_at: i64,
    pub stage: String,
    pub video_id: Option<String>,
    pub message: String,
    pub raw: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeJobLogsResponse {
    pub job_id: i64,
    pub logs: Vec<String>,
    pub entries: Vec<YouTubeJobLogEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YouTubeItemLogsResponse {
    pub item: YouTubeItem,
    pub entries: Vec<YouTubeJobLogEntry>,
}
