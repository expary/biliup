use crate::server::errors::{AppError, AppResult};
use error_stack::ResultExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use tokio::process::Command;

pub async fn should_transcode(path: &Path) -> AppResult<bool> {
    let value = probe_value(path).await?;
    Ok(need_transcode_from_probe(&value))
}

fn need_transcode_from_probe(value: &Value) -> bool {
    let format_name = value
        .get("format")
        .and_then(|v| v.get("format_name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let supported_container = format_name
        .split(',')
        .any(|name| matches!(name, "mp4" | "mov" | "matroska"));
    let streams = value
        .get("streams")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let video_stream = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(|v| v.as_str()) == Some("video"));
    let audio_stream = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(|v| v.as_str()) == Some("audio"));

    let has_h264_video = video_stream
        .and_then(|v| v.get("codec_name"))
        .and_then(|v| v.as_str())
        == Some("h264");
    let has_yuv420p = video_stream
        .and_then(|v| v.get("pix_fmt"))
        .and_then(|v| v.as_str())
        == Some("yuv420p");
    let has_aac_audio = audio_stream.is_none()
        || audio_stream
            .and_then(|v| v.get("codec_name"))
            .and_then(|v| v.as_str())
            == Some("aac");

    !(supported_container && has_h264_video && has_yuv420p && has_aac_audio)
}

pub async fn transcode(video_id: &str, input: &Path) -> AppResult<PathBuf> {
    let output = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{video_id}.upload.mp4"));

    let result = Command::new("ffmpeg")
        .kill_on_drop(true)
        .arg("-y")
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0?")
        .arg("-c:v")
        .arg("libx264")
        .arg("-preset")
        .arg("medium")
        .arg("-crf")
        .arg("23")
        .arg("-pix_fmt")
        .arg("yuv420p")
        .arg("-c:a")
        .arg("aac")
        .arg("-b:a")
        .arg("192k")
        .arg("-movflags")
        .arg("+faststart")
        .arg(&output)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffmpeg 转码失败，请确认已安装 ffmpeg".to_string(),
        ))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(AppError::Custom(format!("ffmpeg 转码失败: {stderr}")).into());
    }
    Ok(output)
}

pub async fn apply_upload_effects(video_id: &str, input: &Path) -> AppResult<PathBuf> {
    let probe = probe_value(input).await?;
    let (width, height, duration_sec) = probe_video_info(&probe);
    let filter = build_effect_filter(video_id, width, height, duration_sec);
    let output = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("{video_id}.fx.mp4"));

    let result = Command::new("ffmpeg")
        .kill_on_drop(true)
        .arg("-y")
        .arg("-i")
        .arg(input)
        .arg("-map")
        .arg("0:v:0")
        .arg("-map")
        .arg("0:a:0?")
        .arg("-vf")
        .arg(filter)
        .arg("-c:v")
        .arg("libx264")
        .arg("-preset")
        .arg("medium")
        .arg("-crf")
        .arg("23")
        .arg("-pix_fmt")
        .arg("yuv420p")
        .arg("-c:a")
        .arg("aac")
        .arg("-b:a")
        .arg("192k")
        .arg("-movflags")
        .arg("+faststart")
        .arg(&output)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffmpeg 视频处理失败，请确认已安装 ffmpeg".to_string(),
        ))?;

    if !result.status.success() {
        let stderr = String::from_utf8_lossy(&result.stderr);
        return Err(AppError::Custom(format!("ffmpeg 视频处理失败: {stderr}")).into());
    }
    Ok(output)
}

async fn probe_value(path: &Path) -> AppResult<Value> {
    let output = Command::new("ffprobe")
        .kill_on_drop(true)
        .arg("-v")
        .arg("quiet")
        .arg("-print_format")
        .arg("json")
        .arg("-show_streams")
        .arg("-show_format")
        .arg(path)
        .output()
        .await
        .change_context(AppError::Custom(
            "执行 ffprobe 失败，请确认已安装 ffmpeg/ffprobe".to_string(),
        ))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::Custom(format!("ffprobe 失败: {stderr}")).into());
    }

    serde_json::from_slice::<Value>(&output.stdout)
        .change_context(AppError::Custom("ffprobe 输出解析失败".to_string()))
}

fn probe_video_info(value: &Value) -> (u32, u32, f64) {
    let streams = value
        .get("streams")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let video = streams
        .iter()
        .find(|stream| stream.get("codec_type").and_then(|v| v.as_str()) == Some("video"));

    let width = video
        .and_then(|v| v.get("width"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1920) as u32;
    let height = video
        .and_then(|v| v.get("height"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1080) as u32;
    let duration_sec = value
        .get("format")
        .and_then(|v| v.get("duration"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(60.0)
        .max(5.0);
    (width.max(64), height.max(64), duration_sec)
}

fn build_effect_filter(video_id: &str, width: u32, height: u32, duration_sec: f64) -> String {
    let mut hasher = DefaultHasher::new();
    video_id.hash(&mut hasher);
    width.hash(&mut hasher);
    height.hash(&mut hasher);
    duration_sec.to_bits().hash(&mut hasher);
    let seed = hasher.finish();
    let mut rng = StdRng::seed_from_u64(seed);

    let drop_interval = rng.gen_range(120..=260);
    let drop_offset = rng.gen_range(1..drop_interval);
    let dots = rng.gen_range(2..=4);

    let mut filters = vec![
        format!("select='not(eq(mod(n\\,{drop_interval}),{drop_offset}))'"),
        "setpts=N/FRAME_RATE/TB".to_string(),
    ];

    for _ in 0..dots {
        let max_w = (width / 40).max(6);
        let min_w = (width / 160).max(2);
        let max_h = (height / 40).max(6);
        let min_h = (height / 160).max(2);
        let box_w = rng.gen_range(min_w..=max_w).min(width.saturating_sub(1));
        let box_h = rng.gen_range(min_h..=max_h).min(height.saturating_sub(1));
        let x_max = width.saturating_sub(box_w).max(1);
        let y_max = height.saturating_sub(box_h).max(1);
        let x = rng.gen_range(0..x_max);
        let y = rng.gen_range(0..y_max);
        let start = rng.gen_range(0.2..(duration_sec * 0.92).max(0.3));
        let span = rng.gen_range(0.2..0.9);
        let end = (start + span).min(duration_sec - 0.05);
        filters.push(format!(
            "drawbox=x={x}:y={y}:w={box_w}:h={box_h}:color=black@1:t=fill:enable='between(t,{start:.2},{end:.2})'"
        ));
    }

    filters.join(",")
}

#[cfg(test)]
mod tests {
    use super::{build_effect_filter, need_transcode_from_probe};
    use serde_json::json;

    #[test]
    fn no_transcode_when_bilibili_friendly() {
        let value = json!({
            "format": { "format_name": "mov,mp4,m4a,3gp,3g2,mj2" },
            "streams": [
                { "codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p" },
                { "codec_type": "audio", "codec_name": "aac" }
            ]
        });
        assert!(!need_transcode_from_probe(&value));
    }

    #[test]
    fn transcode_when_video_codec_not_h264() {
        let value = json!({
            "format": { "format_name": "mp4" },
            "streams": [
                { "codec_type": "video", "codec_name": "hevc", "pix_fmt": "yuv420p" },
                { "codec_type": "audio", "codec_name": "aac" }
            ]
        });
        assert!(need_transcode_from_probe(&value));
    }

    #[test]
    fn no_audio_is_allowed() {
        let value = json!({
            "format": { "format_name": "matroska,webm" },
            "streams": [{ "codec_type": "video", "codec_name": "h264", "pix_fmt": "yuv420p" }]
        });
        assert!(!need_transcode_from_probe(&value));
    }

    #[test]
    fn effect_filter_contains_frame_drop_and_drawbox() {
        let filter = build_effect_filter("video1", 1920, 1080, 210.0);
        assert!(filter.contains("select='not(eq(mod(n\\,"));
        assert!(filter.contains("setpts=N/FRAME_RATE/TB"));
        assert!(filter.contains("drawbox="));
    }
}
