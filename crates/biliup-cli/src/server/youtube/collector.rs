use crate::server::errors::{AppError, AppResult};
use crate::server::common::util::normalize_proxy;
use error_stack::ResultExt;
use serde_json::Value;
use tokio::process::Command;
use url::Url;

#[derive(Debug, Clone)]
pub struct CollectedEntry {
    pub video_id: String,
    pub video_url: String,
    pub title: Option<String>,
    pub upload_date: Option<String>,
    pub channel_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VideoMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub tags: Vec<String>,
    pub thumbnail: Option<String>,
    pub upload_date: Option<String>,
    pub duration_sec: Option<i64>,
    pub channel_id: Option<String>,
    pub channel_name: Option<String>,
    pub raw: Value,
}

pub fn detect_source_type(url: &str) -> String {
    let normalized = url.to_ascii_lowercase();
    if normalized.contains("list=") || normalized.contains("/playlist") {
        return "playlist".to_string();
    }
    if normalized.contains("/shorts") {
        return "shorts".to_string();
    }
    "channel".to_string()
}

pub async fn collect_entries(source_url: &str, proxy: Option<&str>) -> AppResult<Vec<CollectedEntry>> {
    let collect_source_url = normalize_collect_source_url(source_url);
    let mut cmd = Command::new("yt-dlp");
    cmd.kill_on_drop(true)
        .arg("--flat-playlist")
        .arg("--ignore-errors")
        .arg("--dump-json");
    if let Some(proxy) = normalize_proxy(proxy) {
        cmd.arg("--proxy").arg(proxy);
    }
    let output = cmd.arg(&collect_source_url).output().await
        .change_context(AppError::Custom(
            "执行 yt-dlp 采集失败，请确认已安装 yt-dlp".to_string(),
        ))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("yt-dlp 采集失败: {stderr}")).into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut result = Vec::new();
    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let video_id = value
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();
        if video_id.is_empty() {
            continue;
        }
        let video_url = format!("https://www.youtube.com/watch?v={video_id}");
        result.push(CollectedEntry {
            video_id,
            video_url,
            title: value
                .get("title")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            upload_date: value
                .get("upload_date")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
            channel_id: value
                .get("channel_id")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string()),
        });
    }

    if result.is_empty()
        && let Some(video_id) = extract_video_id(source_url)
    {
        result.push(CollectedEntry {
            video_id: video_id.clone(),
            video_url: format!("https://www.youtube.com/watch?v={video_id}"),
            title: None,
            upload_date: None,
            channel_id: None,
        });
    }

    Ok(result)
}

pub async fn fetch_video_metadata(video_url: &str, proxy: Option<&str>) -> AppResult<VideoMetadata> {
    let mut cmd = Command::new("yt-dlp");
    cmd.kill_on_drop(true)
        .arg("--dump-single-json")
        .arg("--no-playlist")
        .arg("--skip-download");
    if let Some(proxy) = normalize_proxy(proxy) {
        cmd.arg("--proxy").arg(proxy);
    }
    let output = cmd.arg(video_url).output().await
        .change_context(AppError::Custom(
            "执行 yt-dlp 元数据抓取失败，请确认已安装 yt-dlp".to_string(),
        ))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("yt-dlp 元数据抓取失败: {stderr}")).into());
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let raw: Value = serde_json::from_str(stdout.trim())
        .change_context(AppError::Custom("yt-dlp 元数据 JSON 解析失败".to_string()))?;

    let tags = raw
        .get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|x| x.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(VideoMetadata {
        title: raw
            .get("title")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        description: raw
            .get("description")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        tags,
        thumbnail: raw
            .get("thumbnail")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        upload_date: raw
            .get("upload_date")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        duration_sec: raw.get("duration").and_then(|v| v.as_i64()),
        channel_id: raw
            .get("channel_id")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        channel_name: raw
            .get("channel")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        raw,
    })
}

fn normalize_collect_source_url(source_url: &str) -> String {
    if is_watch_url(source_url) {
        return source_url.to_string();
    }
    if let Some(list_id) = extract_query_param(source_url, "list")
        && !list_id.trim().is_empty()
    {
        return format!("https://www.youtube.com/playlist?list={list_id}");
    }
    source_url.to_string()
}

fn extract_query_param(source_url: &str, key: &str) -> Option<String> {
    let parsed = Url::parse(source_url).ok()?;
    parsed
        .query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.to_string())
}

fn extract_video_id(source_url: &str) -> Option<String> {
    let parsed = Url::parse(source_url).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    if host.contains("youtu.be") {
        let id = parsed.path().trim_matches('/').to_string();
        if !id.is_empty() {
            return Some(id);
        }
    }
    if host.contains("youtube.com") || host.contains("music.youtube.com") {
        if let Some(v) = extract_query_param(source_url, "v")
            && !v.trim().is_empty()
        {
            return Some(v);
        }
        let segments = parsed
            .path_segments()
            .map(|s| s.collect::<Vec<_>>())
            .unwrap_or_default();
        if segments.first() == Some(&"shorts")
            && let Some(id) = segments.get(1)
            && !id.trim().is_empty()
        {
            return Some((*id).to_string());
        }
    }
    None
}

fn is_watch_url(source_url: &str) -> bool {
    Url::parse(source_url)
        .ok()
        .map(|url| {
            let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
            let path = url.path().to_ascii_lowercase();
            (host.contains("youtube.com") || host.contains("music.youtube.com"))
                && path.contains("/watch")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{detect_source_type, extract_video_id, normalize_collect_source_url};

    #[test]
    fn detect_playlist_source() {
        assert_eq!(
            detect_source_type("https://www.youtube.com/playlist?list=PLxxxx"),
            "playlist"
        );
    }

    #[test]
    fn detect_shorts_source() {
        assert_eq!(
            detect_source_type("https://www.youtube.com/shorts/abc123"),
            "shorts"
        );
    }

    #[test]
    fn detect_channel_source_default() {
        assert_eq!(
            detect_source_type("https://www.youtube.com/@channel_name"),
            "channel"
        );
    }

    #[test]
    fn keep_watch_with_list_for_mix_playlist() {
        let source = "https://www.youtube.com/watch?v=WJyV6WoWmnc&list=RDWJyV6WoWmnc&start_radio=1";
        let normalized = normalize_collect_source_url(source);
        assert_eq!(normalized, source);
    }

    #[test]
    fn extract_video_id_from_watch_url() {
        let source = "https://www.youtube.com/watch?v=WJyV6WoWmnc&list=RDWJyV6WoWmnc";
        assert_eq!(extract_video_id(source).as_deref(), Some("WJyV6WoWmnc"));
    }
}
