use crate::UploadLine;
use crate::server::common::upload::{build_studio, submit_to_bilibili, upload};
use crate::server::common::util::Recorder;
use crate::server::config::Config;
use crate::server::errors::{AppError, AppResult};
use crate::server::infrastructure::models::StreamerInfo;
use crate::server::infrastructure::models::upload_streamer::UploadStreamer;
use crate::server::infrastructure::models::youtube::{YouTubeItem, YouTubeJob};
use biliup::bilibili::ResponseData;
use chrono::Utc;
use clap::ValueEnum;
use serde_json::Value;
use std::path::PathBuf;

pub async fn upload_video(
    config: &Config,
    job: &YouTubeJob,
    item: &YouTubeItem,
    upload_config: &UploadStreamer,
    upload_path: &str,
) -> AppResult<(Option<i64>, Option<String>)> {
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
    if upload_cfg.copyright_source.as_deref().unwrap_or("").is_empty() {
        upload_cfg.copyright_source = Some(item.video_url.clone());
    }

    let video_paths = vec![PathBuf::from(upload_path)];
    let line = UploadLine::from_str(&config.lines, true).ok();
    let (bilibili, videos) = upload(
        upload_cfg.user_cookie.as_deref().unwrap_or("cookies.json"),
        None,
        line,
        &video_paths,
        config.threads as usize,
    )
    .await?;
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
    Ok(parse_submit_result(response))
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
