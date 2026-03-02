use crate::server::errors::{AppError, AppResult};
use error_stack::ResultExt;
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::process::Command;

pub async fn should_transcode(path: &Path) -> AppResult<bool> {
    let output = Command::new("ffprobe")
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

    let value: Value = serde_json::from_slice(&output.stdout)
        .change_context(AppError::Custom("ffprobe 输出解析失败".to_string()))?;
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

#[cfg(test)]
mod tests {
    use super::need_transcode_from_probe;
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
}
