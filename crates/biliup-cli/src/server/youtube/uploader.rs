use crate::UploadLine;
use crate::server::common::upload::{build_studio, submit_to_bilibili, upload};
use crate::server::common::util::Recorder;
use crate::server::common::util::normalize_proxy;
use crate::server::config::Config;
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::models::StreamerInfo;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{YouTubeItem, YouTubeJob};
use crate::server::youtube::metadata;
use biliup::bilibili::ResponseData;
use chrono::Utc;
use clap::ValueEnum;
use error_stack::ResultExt;
use rand::Rng;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::io::Cursor;
use tokio::fs;

#[derive(Debug, Clone)]
pub struct UploadResult {
    pub aid: Option<i64>,
    pub bvid: Option<String>,
    pub upload_file_name: String,
    pub cover_file_path: Option<String>,
}

#[derive(Debug)]
struct PreparedUploadVideo {
    path: PathBuf,
    file_name: String,
    temporary: bool,
}

pub async fn upload_video(
    config: &Config,
    job: &YouTubeJob,
    item: &YouTubeItem,
    upload_config: &UploadStreamer,
    upload_path: &str,
) -> AppResult<UploadResult> {
    let mut upload_cfg = upload_config.clone();
    upload_cfg.youtube_title_strategy = None;
    upload_cfg.youtube_title_strategy_prompt = None;

    if let Some(generated_title) = &item.generated_title {
        upload_cfg.title = Some(generated_title.clone());
    }
    if let Some(generated_description) = &item.generated_description {
        upload_cfg.description = Some(generated_description.clone());
    }
    if let Some(generated_tags) = &item.generated_tags {
        upload_cfg.tags = serde_json::from_str(generated_tags).unwrap_or_default();
    }
    upload_cfg.title = upload_cfg
        .title
        .as_deref()
        .map(metadata::sanitize_title)
        .filter(|title| !title.is_empty());
    upload_cfg.description = upload_cfg
        .description
        .as_deref()
        .map(metadata::sanitize_description)
        .filter(|description| !description.is_empty());
    upload_cfg.tags = metadata::sanitize_submit_tags(upload_cfg.tags.clone());

    if upload_cfg.title.is_none() {
        let fallback = metadata::sanitize_title(
            item.generated_title
                .as_deref()
                .or(item.source_title.as_deref())
                .unwrap_or(""),
        );
        upload_cfg.title = Some(if fallback.is_empty() {
            "精选视频内容分享".to_string()
        } else {
            fallback
        });
    }
    if upload_cfg
        .copyright_source
        .as_deref()
        .unwrap_or("")
        .is_empty()
    {
        upload_cfg.copyright_source = Some(item.video_url.clone());
    }

    let source_cover = prepare_source_cover_file(config.proxy.as_deref(), item).await?;
    if let Some(cover_path) = &source_cover {
        upload_cfg.cover_path = Some(cover_path.to_string_lossy().to_string());
    }

    let prepared_video = prepare_upload_video(item, upload_path).await?;
    let video_paths = vec![prepared_video.path.clone()];

    let line = UploadLine::from_str(&config.lines, true).ok();
    let upload_result = upload(
        upload_cfg.user_cookie.as_deref().unwrap_or("cookies.json"),
        None,
        line,
        &video_paths,
        config.threads as usize,
    )
    .await;

    if prepared_video.temporary
        && let Err(err) = fs::remove_file(&prepared_video.path).await
    {
        tracing::warn!(
            path = %prepared_video.path.display(),
            error = ?err,
            "cleanup temporary upload file failed"
        );
    }

    let (bilibili, videos) = upload_result?;
    if videos.is_empty() {
        return Err(AppError::Custom("上传视频文件阶段未得到稿件分P".to_string()).into());
    }

    let recorder = Recorder::new(
        upload_cfg.title.clone(),
        StreamerInfo::new(
            &job.name,
            &item.video_url,
            item.source_title.as_deref().unwrap_or(""),
            Utc::now(),
            "",
        ),
    );
    let studio = build_studio(config, &upload_cfg, &bilibili, videos, &recorder).await?;
    let submit_api = config.submit_api.clone();
    let response = submit_to_bilibili(&bilibili, &studio, submit_api.as_deref()).await?;
    let (aid, bvid) = parse_submit_result(response);
    Ok(UploadResult {
        aid,
        bvid,
        upload_file_name: prepared_video.file_name,
        cover_file_path: source_cover.map(|path| path.to_string_lossy().to_string()),
    })
}

fn parse_submit_result(response: ResponseData<Value>) -> (Option<i64>, Option<String>) {
    let Some(data) = response.data else {
        return (None, None);
    };
    let aid = data.get("aid").and_then(|v| v.as_i64()).or_else(|| {
        data.get("archive")
            .and_then(|v| v.get("aid"))
            .and_then(|v| v.as_i64())
    });
    let bvid = data
        .get("bvid")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            data.get("archive")
                .and_then(|v| v.get("bvid"))
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });
    (aid, bvid)
}

async fn prepare_source_cover_file(proxy: Option<&str>, item: &YouTubeItem) -> AppResult<Option<PathBuf>> {
    let Some(cover_url) = item.thumbnail_url.as_deref() else {
        return Ok(None);
    };
    if cover_url.trim().is_empty() {
        return Ok(None);
    }

    let cover_path = PathBuf::from(format!(
        "data/youtube/{}/{}/{}.cover.jpg",
        item.job_id, item.video_id, item.video_id
    ));
    if let Some(parent) = cover_path.parent() {
        fs::create_dir_all(parent)
            .await
            .change_context(AppError::Unknown)?;
    }

    let mut client_builder = reqwest::Client::builder();
    if let Some(proxy) = normalize_proxy(proxy) {
        let proxy = reqwest::Proxy::all(&proxy)
            .change_context(AppError::Custom("代理配置格式错误".to_string()))?;
        client_builder = client_builder.proxy(proxy);
    }
    let client = client_builder
        .build()
        .change_context(AppError::Custom("创建 HTTP 客户端失败".to_string()))?;

    let response = client
        .get(cover_url)
        .send()
        .await
        .change_context(AppError::Custom("下载来源封面失败".to_string()))?;
    if !response.status().is_success() {
        return Err(
            AppError::Custom(format!("下载来源封面失败: HTTP {}", response.status())).into(),
        );
    }
    let bytes = response
        .bytes()
        .await
        .change_context(AppError::Custom("读取来源封面失败".to_string()))?;

    let jpeg_bytes = match image::load_from_memory(&bytes) {
        Ok(img) => {
            let mut cursor = Cursor::new(Vec::new());
            let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut cursor, 95);
            if let Err(err) = encoder.encode_image(&img) {
                tracing::warn!(error = ?err, "encode cover image as jpeg failed, fallback to original bytes");
                bytes.to_vec()
            } else {
                cursor.into_inner()
            }
        }
        Err(err) => {
            tracing::warn!(error = ?err, "decode cover image failed, fallback to original bytes");
            bytes.to_vec()
        }
    };

    fs::write(&cover_path, &jpeg_bytes)
        .await
        .change_context(AppError::Custom("保存来源封面失败".to_string()))?;
    Ok(Some(cover_path))
}

async fn prepare_upload_video(
    item: &YouTubeItem,
    source_path: &str,
) -> AppResult<PreparedUploadVideo> {
    let source = PathBuf::from(source_path);
    let metadata = fs::metadata(&source)
        .await
        .change_context(AppError::Custom(format!(
            "上传文件不存在: {}",
            source.display()
        )))?;
    if !metadata.is_file() {
        return Err(AppError::Custom(format!("上传路径不是文件: {}", source.display())).into());
    }

    let ext = source
        .extension()
        .and_then(|ext| ext.to_str())
        .filter(|ext| !ext.trim().is_empty())
        .unwrap_or("mp4");
    let title_seed = item
        .generated_title
        .as_deref()
        .or(item.source_title.as_deref())
        .unwrap_or(&item.video_id);
    let clean_title = sanitize_file_stem(title_seed);
    let short_title = truncate_chars(&clean_title, 24);
    let random_suffix: u16 = rand::thread_rng().gen_range(1000..=9999);
    let ts = Utc::now().timestamp();
    let upload_file_name = format!(
        "{short_title}_{}_{}_{}_upload.{ext}",
        item.video_id, ts, random_suffix
    )
    .replace("..", ".");
    let target = source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(&upload_file_name);

    if target == source {
        return Ok(PreparedUploadVideo {
            path: source,
            file_name: upload_file_name,
            temporary: false,
        });
    }
    if fs::try_exists(&target).await.unwrap_or(false) {
        let _ = fs::remove_file(&target).await;
    }

    if let Err(link_err) = fs::hard_link(&source, &target).await {
        tracing::warn!(
            source = %source.display(),
            target = %target.display(),
            error = ?link_err,
            "hard link failed, fallback to file copy"
        );
        fs::copy(&source, &target)
            .await
            .change_context(AppError::Custom(format!(
                "创建上传临时文件失败: {} -> {}",
                source.display(),
                target.display()
            )))?;
    }

    Ok(PreparedUploadVideo {
        path: target,
        file_name: upload_file_name,
        temporary: true,
    })
}

fn sanitize_file_stem(raw: &str) -> String {
    let mut output = raw
        .chars()
        .map(|ch| match ch {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect::<String>()
        .trim()
        .to_string();
    if output.is_empty() {
        output = "video".to_string();
    }
    output
}

fn truncate_chars(s: &str, max_len: usize) -> String {
    s.chars().take(max_len).collect()
}
